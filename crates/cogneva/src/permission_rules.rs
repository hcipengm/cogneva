//! 人答过一次「总是这样」之后，那条规则住在哪里。
//!
//! 规则按「这是谁的范围」分桶：一个人的规则一个键，一段对话的规则一个键，桶里装的是
//! 一整份 JSON 数组。不做「一条规则一个键」——键值存储没有扫描，列不出前缀下的所有
//! 键，一条一桶就永远读不回「我手上还有哪些规则」，而撤销的第一步正是把那份名单摆出来。
//!
//! 写是「读整桶、改、写回」。两个并发的「总是」落进同一个桶时后写的会盖掉先写的，
//! 代价是那条规则没留下、下一回同样的调用再问一次——往安全的方向丢，不是往放行的
//! 方向丢。要消掉这个窗口得给存储加一次比较交换，为一个几十秒才发生一次的写不值得。

use std::sync::Arc;

use cog_agent::permission::{PermissionRule, PermissionRuleSet, PermissionScope};

use cog_core::KeyValueStore;

/// 规则桶的键前缀。整棵树里只有这一处拼这个键。
pub const RULE_KEY_PREFIX: &str = "permission_rules";

/// 一个范围落在存储里的键。人范围的规则需要一个属主名才落得下去；没有登录身份的
/// 连接没有属主，`None` 表示「这个范围在这一条连接上存不了」。
fn bucket_key(scope: &PermissionScope, owner: Option<&str>) -> Option<String> {
    match scope {
        PermissionScope::Owner => owner
            .map(str::trim)
            .filter(|owner| !owner.is_empty())
            .map(|owner| format!("{RULE_KEY_PREFIX}:owner:{owner}")),
        PermissionScope::Session { session_id } => {
            Some(format!("{RULE_KEY_PREFIX}:session:{session_id}"))
        }
    }
}

/// 读一个桶。读不到、读回来不是 JSON、不是一组规则，都按「桶里没有规则」处理：
/// 那个后果是多问一句（可见、可补），而不是凭一份读不懂的东西静默放行。
async fn read_bucket(kv: &Arc<dyn KeyValueStore>, key: &str) -> PermissionRuleSet {
    match kv.get(key).await {
        Ok(Some(text)) => match serde_json::from_str::<PermissionRuleSet>(&text) {
            Ok(set) => set,
            Err(error) => {
                tracing::warn!(key, %error, "权限规则的桶读不懂，按没有规则处理");
                PermissionRuleSet::default()
            }
        },
        Ok(None) => PermissionRuleSet::default(),
        Err(error) => {
            tracing::warn!(key, %error, "权限规则的桶读不到，按没有规则处理");
            PermissionRuleSet::default()
        }
    }
}

/// 这一段对话现在生效的全部规则：这个人的 + 这一段的。会话范围要靠调用方在评估时
/// 再筛一次（[`PermissionRuleSet::evaluate`] 按会话号判），所以两桶可以直接并在一起。
pub async fn load(
    kv: &Arc<dyn KeyValueStore>,
    owner: Option<&str>,
    session_id: &str,
) -> PermissionRuleSet {
    let session = read_bucket(kv, &format!("{RULE_KEY_PREFIX}:session:{session_id}")).await;
    let owner_key = bucket_key(&PermissionScope::Owner, owner);
    let mut merged = PermissionRuleSet::default();
    for rule in session.rules() {
        merged.insert(rule.clone());
    }
    if let Some(key) = owner_key {
        for rule in read_bucket(kv, &key).await.rules() {
            merged.insert(rule.clone());
        }
    }
    merged
}

/// 把一个范围里的整份规则写回去。
async fn write_bucket(
    kv: &Arc<dyn KeyValueStore>,
    owner: Option<&str>,
    scope: &PermissionScope,
    set: &PermissionRuleSet,
) -> cog_core::SFResult<()> {
    let Some(key) = bucket_key(scope, owner) else {
        return Err(cog_core::SFError::Validation(
            "这条规则要挂在某个属主名下，而这条连接没有登录身份".into(),
        ));
    };
    if set.is_empty() {
        return kv.del(&key).await;
    }
    let text = serde_json::to_string(set)
        .map_err(|error| cog_core::SFError::Internal(format!("规则没法序列化: {error}")))?;
    kv.set(&key, &text).await
}

/// 记下一条规则。同一条写第二次是空操作：规则的身份是内容。
pub async fn remember(
    kv: &Arc<dyn KeyValueStore>,
    owner: Option<&str>,
    rule: PermissionRule,
) -> cog_core::SFResult<PermissionRule> {
    let scope = rule.scope.clone();
    let key = bucket_key(&scope, owner).ok_or_else(|| {
        cog_core::SFError::Validation("这条规则要挂在某个属主名下，而这条连接没有登录身份".into())
    })?;
    let mut set = read_bucket(kv, &key).await;
    if set.insert(rule.clone()) {
        write_bucket(kv, owner, &scope, &set).await?;
    }
    Ok(rule)
}

/// 撤掉一条规则，返回还剩什么。
///
/// 从手上的两个桶里都试一遍：规则的号里带着范围，但直接按两个桶找更省事，也不会
/// 因为一个号里的冒号被解析错而撤不掉。两个桶都撤不掉就是「本来就没有这条」。
pub async fn revoke(
    kv: &Arc<dyn KeyValueStore>,
    owner: Option<&str>,
    session_id: &str,
    rule_id: &str,
) -> PermissionRuleSet {
    for scope in [
        PermissionScope::Session {
            session_id: session_id.to_string(),
        },
        PermissionScope::Owner,
    ] {
        let Some(key) = bucket_key(&scope, owner) else {
            continue;
        };
        let mut set = read_bucket(kv, &key).await;
        if set.remove(rule_id) {
            if let Err(error) = write_bucket(kv, owner, &scope, &set).await {
                tracing::warn!(%error, rule = rule_id, "撤销规则没写回去");
            }
        }
    }
    load(kv, owner, session_id).await
}

/// 只在测试里用的内存 KV：这一层要证明的是「写进去、读回来、撤掉之后没了」，
/// 不是某一种驱动的行为。
#[cfg(test)]
#[derive(Default)]
pub(crate) struct MemoryKv {
    rows: std::sync::Mutex<std::collections::HashMap<String, String>>,
}

#[cfg(test)]
#[async_trait::async_trait]
impl KeyValueStore for MemoryKv {
    async fn get(&self, key: &str) -> cog_core::SFResult<Option<String>> {
        Ok(self.rows.lock().unwrap().get(key).cloned())
    }
    async fn set(&self, key: &str, value: &str) -> cog_core::SFResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    async fn set_ex(&self, key: &str, value: &str, _ttl: u64) -> cog_core::SFResult<()> {
        self.set(key, value).await
    }
    async fn del(&self, key: &str) -> cog_core::SFResult<()> {
        self.rows.lock().unwrap().remove(key);
        Ok(())
    }
    async fn expire(&self, _key: &str, _ttl: i64) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn ttl(&self, _key: &str) -> cog_core::SFResult<i64> {
        Ok(-1)
    }
    async fn incr(&self, _key: &str, _by: i64) -> cog_core::SFResult<i64> {
        Ok(0)
    }
    async fn decr(&self, _key: &str, _by: i64) -> cog_core::SFResult<i64> {
        Ok(0)
    }
    async fn incr_ex(&self, _key: &str, _by: i64, _ttl: u64) -> cog_core::SFResult<i64> {
        Ok(0)
    }
    async fn sadd(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn srem(&self, _key: &str, _member: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn smembers(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        Ok(Vec::new())
    }
    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn hget(&self, _key: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        Ok(None)
    }
    async fn hdel(&self, _key: &str, _field: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn hvals(&self, _key: &str) -> cog_core::SFResult<Vec<String>> {
        Ok(Vec::new())
    }
    async fn hgetall(&self, _key: &str) -> cog_core::SFResult<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    async fn hlen(&self, _key: &str) -> cog_core::SFResult<u64> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(allow: bool, tool: &str, subject: Option<&str>) -> PermissionRule {
        PermissionRule {
            tool: tool.into(),
            subject: subject.map(str::to_string),
            allow,
            scope: PermissionScope::Owner,
        }
    }

    /// 写进去的规则要能按属主读回来，且只读到这一个人的。
    #[tokio::test]
    async fn a_rule_is_read_back_for_its_owner_only() {
        let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryKv::default());
        remember(
            &kv,
            Some("alice"),
            owned(true, "run_command", Some("cargo test")),
        )
        .await
        .expect("写得进去");
        let hers = load(&kv, Some("alice"), "s1").await;
        assert_eq!(hers.rules().len(), 1);
        assert!(load(&kv, Some("bob"), "s1").await.is_empty());
    }

    /// 没有登录身份的连接把「总是」记成这一段对话的规则，而不是假装记到了某个人名下。
    #[tokio::test]
    async fn a_connection_without_an_owner_keeps_the_rule_in_its_session() {
        let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryKv::default());
        let rule = PermissionRule {
            tool: "run_command".into(),
            subject: Some("cargo test".into()),
            allow: true,
            scope: PermissionScope::Session {
                session_id: "s9".into(),
            },
        };
        remember(&kv, None, rule).await.expect("会话范围不需要属主");
        assert_eq!(load(&kv, None, "s9").await.rules().len(), 1);
        assert!(load(&kv, None, "s10").await.is_empty());

        // 人范围的规则没有属主就落不下去——报错，而不是悄悄记进一个不属于谁的位置。
        let err = remember(&kv, None, owned(true, "write_file", None)).await;
        assert!(err.is_err());
    }

    /// 撤销之后桶里就空了：下一次同样的调用会重新问（规则评估给出 `None`）。
    #[tokio::test]
    async fn revoking_empties_the_bucket() {
        let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryKv::default());
        let rule = owned(false, "run_command", Some("rm -rf"));
        remember(&kv, Some("alice"), rule.clone()).await.unwrap();
        assert_eq!(load(&kv, Some("alice"), "s1").await.rules().len(), 1);
        let left = revoke(&kv, Some("alice"), "s1", &rule.id()).await;
        assert!(left.is_empty());
        // 撤一条本来就没有的不是错，也不造一条空桶。
        let again = revoke(&kv, Some("alice"), "s1", &rule.id()).await;
        assert!(again.is_empty());
    }
}
