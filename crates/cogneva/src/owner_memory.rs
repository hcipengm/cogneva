//! 属主写下的约定住在哪里。
//!
//! 一个人一个桶，桶里是一整份 JSON 数组。不做「一条一个键」：键值存储没有扫
//! 描，列不出前缀下的所有键，一条一桶就永远读不回「我手上写了哪几句」——而
//! 撤销与「这一轮加载了什么」的第一步都是把那份名单摆出来。同一个理由与权限
//! 规则那一本一样，键的形状也照它。
//!
//! 读不到、读回来不是 JSON、不是一组条目，一律按「这个人还没写过」处理。后果
//! 是这一轮少了几句话，不是这一轮停下来：约定是附加的，不是这一轮的前提。
//!
//! 容量两条是 `override`，不是导出的读数：「一句约定多长会让模型照做、多长会
//! 被它当成背景噪声」今天没有实测曲线；写下来的东西进的是**每一轮**的系统前
//! 缀，所以它必须有个界，而那个界今天只能是钉死的。要改就改这里一处，等有一
//! 条「约定长度 → 遵从率」的读数之后再换成导出值。

use std::sync::Arc;

use async_trait::async_trait;

use cog_agent::memory::{MemoryEntry, OwnerMemory};
use cog_core::{KeyValueStore, SFError, SFResult};

/// 条目桶的键前缀。整棵树里只有这一处拼这个键。
pub const MEMORY_KEY_PREFIX: &str = "owner_memory";

/// 一条约定的上限（字符）。`override`：见文件头。
// cap: bound — 人定的（override，见文件头）：一句约定是一句话，不是一篇文章；它进的是每一轮的系统前缀，所以放长一段就放大每一次调用
pub const MEMORY_ENTRY_LIMIT_CHARS: usize = 2_000;

/// 一个人全部约定的上限（字符）。`override`：见文件头。桶满之后再记一条会被
/// 拒绝并说明，而不是悄悄挤掉最早的那条——被挤掉的那句不会再有人读到，而写的
/// 人以为它还在。
// cap: bound — 人定的（override，见文件头）：一个人合计八千字，按一条满额算是四条；满之后再记被拒并要求先撤旧的，所以这个界就是「一个人手上能同时持有多少字」的答案
pub const MEMORY_BUCKET_LIMIT_CHARS: usize = 8_000;

/// 存进桶里的一行。`at` 是写下这一刻，界面上按时间排、也让人认出哪句是老的。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredEntry {
    id: String,
    text: String,
    at: String,
}

/// 桶的键。没有登录身份就没有属主，`None` 表示「这一条在这条连接上存不了」。
fn bucket_key(owner: Option<&str>) -> Option<String> {
    owner
        .map(str::trim)
        .filter(|owner| !owner.is_empty())
        .map(|owner| format!("{MEMORY_KEY_PREFIX}:owner:{owner}"))
}

/// 一条约定的号由内容算出来：同一句话写第二次是空操作，不是两条一模一样的
/// 约定被反复交给模型。号短是为了在界面上按得住，取内容哈希前 12 位。
fn id_for(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    digest
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn read_bucket(kv: &Arc<dyn KeyValueStore>, key: &str) -> Vec<StoredEntry> {
    match kv.get(key).await {
        Ok(Some(text)) => match serde_json::from_str::<Vec<StoredEntry>>(&text) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(key, %error, "属主约定的桶读不懂，按还没写过处理");
                Vec::new()
            }
        },
        Ok(None) => Vec::new(),
        Err(error) => {
            tracing::warn!(key, %error, "属主约定的桶读不到，按还没写过处理");
            Vec::new()
        }
    }
}

async fn write_bucket(
    kv: &Arc<dyn KeyValueStore>,
    key: &str,
    entries: &[StoredEntry],
) -> SFResult<()> {
    if entries.is_empty() {
        return kv.del(key).await;
    }
    let text = serde_json::to_string(entries)
        .map_err(|error| SFError::Internal(format!("约定没法序列化: {error}")))?;
    kv.set(key, &text).await
}

/// 这个人写下的全部约定，按写下的顺序（老的在前）。
pub async fn list(kv: &Arc<dyn KeyValueStore>, owner: Option<&str>) -> Vec<MemoryEntry> {
    let Some(key) = bucket_key(owner) else {
        return Vec::new();
    };
    read_bucket(kv, &key)
        .await
        .into_iter()
        .map(|entry| MemoryEntry {
            id: entry.id,
            text: entry.text,
        })
        .collect()
}

/// 记下一条约定，返回记完之后的名单。
///
/// 同一句话写第二次是空操作：约定的身份是它的内容，不是写入次数。
pub async fn remember(
    kv: &Arc<dyn KeyValueStore>,
    owner: Option<&str>,
    text: &str,
    at: chrono::DateTime<chrono::Utc>,
) -> SFResult<Vec<MemoryEntry>> {
    let text = text.trim();
    if text.is_empty() {
        return Err(SFError::Validation(
            "一条约定要有内容：空白的一句写下去，下一轮会在提示词里读到一条空行".into(),
        ));
    }
    if text.chars().count() > MEMORY_ENTRY_LIMIT_CHARS {
        return Err(SFError::Validation(format!(
            "这条约定有 {} 个字，超过一条的上限 {MEMORY_ENTRY_LIMIT_CHARS} 个字",
            text.chars().count()
        )));
    }
    let Some(key) = bucket_key(owner) else {
        return Err(SFError::Validation(
            "约定要挂在某个属主名下，而这条连接没有登录身份".into(),
        ));
    };
    let mut entries = read_bucket(kv, &key).await;
    let id = id_for(text);
    if !entries.iter().any(|entry| entry.id == id) {
        let used: usize = entries.iter().map(|entry| entry.text.chars().count()).sum();
        let after = used + text.chars().count();
        if after > MEMORY_BUCKET_LIMIT_CHARS {
            return Err(SFError::Validation(format!(
                "这个人的约定一共 {used} 个字，再加这一条会超过上限 \
                 {MEMORY_BUCKET_LIMIT_CHARS} 个字：先撤掉几句旧的，再写这一句"
            )));
        }
        entries.push(StoredEntry {
            id,
            text: text.to_string(),
            at: at.to_rfc3339(),
        });
        write_bucket(kv, &key, &entries).await?;
    }
    Ok(entries
        .into_iter()
        .map(|entry| MemoryEntry {
            id: entry.id,
            text: entry.text,
        })
        .collect())
}

/// 撤掉一条，返回还剩什么。撤一条本来就没有的号不是错误：名单里没有它，就是
/// 撤完的样子。
pub async fn forget(
    kv: &Arc<dyn KeyValueStore>,
    owner: Option<&str>,
    id: &str,
) -> SFResult<Vec<MemoryEntry>> {
    let Some(key) = bucket_key(owner) else {
        return Ok(Vec::new());
    };
    let mut entries = read_bucket(kv, &key).await;
    let before = entries.len();
    entries.retain(|entry| entry.id != id);
    if entries.len() != before {
        write_bucket(kv, &key, &entries).await?;
    }
    Ok(entries
        .into_iter()
        .map(|entry| MemoryEntry {
            id: entry.id,
            text: entry.text,
        })
        .collect())
}

/// 这一轮读谁写的约定：装配层拿这个挂到运行时上。
pub struct OwnerMemoryBucket {
    kv: Arc<dyn KeyValueStore>,
    owner: String,
}

impl OwnerMemoryBucket {
    pub fn new(kv: Arc<dyn KeyValueStore>, owner: impl Into<String>) -> Self {
        Self {
            kv,
            owner: owner.into(),
        }
    }
}

#[async_trait]
impl OwnerMemory for OwnerMemoryBucket {
    async fn entries(&self) -> SFResult<Vec<MemoryEntry>> {
        // 读面不报「读不到」以外的错：一个坏桶按还没写过处理，这一轮照跑。
        Ok(list(&self.kv, Some(&self.owner)).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 与权限规则那一本共用同一张内存 KV：这一层要证明的是桶的形状与三条
    /// 规则，不是某一种驱动的行为。
    use crate::permission_rules::MemoryKv;

    fn kv() -> Arc<dyn KeyValueStore> {
        Arc::new(MemoryKv::default())
    }

    fn at() -> chrono::DateTime<chrono::Utc> {
        chrono::TimeZone::timestamp_opt(&chrono::Utc, 1_760_000_000, 0).unwrap()
    }

    #[tokio::test]
    async fn what_the_owner_wrote_reads_back_on_the_next_turn() {
        let kv = kv();
        assert!(list(&kv, Some("root")).await.is_empty());
        remember(&kv, Some("root"), "提交信息用英文", at())
            .await
            .unwrap();
        let entries = list(&kv, Some("root")).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].text, "提交信息用英文");
        // 另一个人读不到这一句：桶按人分开。
        assert!(list(&kv, Some("someone-else")).await.is_empty());
    }

    #[tokio::test]
    async fn the_same_sentence_written_twice_stays_one_entry() {
        let kv = kv();
        remember(&kv, Some("root"), "缩进两个空格", at())
            .await
            .unwrap();
        let entries = remember(&kv, Some("root"), "缩进两个空格", at())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn blank_and_oversized_conventions_are_refused_with_a_reason() {
        let kv = kv();
        assert!(remember(&kv, Some("root"), "   ", at()).await.is_err());
        let long = "字".repeat(MEMORY_ENTRY_LIMIT_CHARS + 1);
        assert!(remember(&kv, Some("root"), &long, at()).await.is_err());
        // 没有身份就没有地方挂这一句。
        assert!(remember(&kv, None, "随便写一句", at()).await.is_err());
    }

    #[tokio::test]
    async fn the_bucket_has_a_ceiling_and_says_so_instead_of_dropping_the_oldest() {
        let kv = kv();
        // 每条正好 1000 字：数字前缀一位，其余补满——八条正好压到上限。
        let chunk = "字".repeat(999);
        for round in 0..(MEMORY_BUCKET_LIMIT_CHARS / 1_000) {
            remember(&kv, Some("root"), &format!("{round}{chunk}"), at())
                .await
                .unwrap();
        }
        let overflow = remember(&kv, Some("root"), "再来一条", at()).await;
        assert!(
            overflow.is_err(),
            "the bucket accepted an entry past its ceiling"
        );
        // 满了之后旧的那几句都还在——拒绝，而不是悄悄挤掉最早的一条。
        assert_eq!(
            list(&kv, Some("root")).await.len(),
            MEMORY_BUCKET_LIMIT_CHARS / 1_000
        );
    }

    #[tokio::test]
    async fn forgetting_one_leaves_the_rest_and_a_missing_id_is_not_an_error() {
        let kv = kv();
        remember(&kv, Some("root"), "第一句", at()).await.unwrap();
        let entries = remember(&kv, Some("root"), "第二句", at()).await.unwrap();
        let first = entries[0].id.clone();
        let left = forget(&kv, Some("root"), &first).await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].text, "第二句");
        assert_eq!(
            forget(&kv, Some("root"), "no-such-id").await.unwrap().len(),
            1
        );
    }
}
