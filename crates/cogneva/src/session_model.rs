//! 一段对话用哪只模型，住在哪里。
//!
//! 选择属于「谁在这段对话里说话」的那一格，所以键里带人：同一个人开着两段对话，
//! 一段要快、一段要深，换的是一段的属性。带上人还有第二个后果——别人拿同一个
//! 会话号说话的资格已经在归属那一面判过了（见网关的 `sessions::access`），这里
//! 再按人分桶，一段对话里的选择不会被另一段同号的东西读走。
//!
//! 存的是模型名本身，不存「第几个档」：清单是平台配的那一份，换一次配置档位会
//! 跟着挪，而人是照着名字选的那一只。
//!
//! 读取分三种结局，界面读得出区别：`Ok(Some(name))` 选过，`Ok(None)` 没选过
//! （走上游配置的那一只），`Err` 读不到键值存储——那时网关把这一轮按默认跑，
//! 而不是把「没读出来」当成「没选过」写回一份默认值。

use std::sync::Arc;

use cog_core::KeyValueStore;

/// 键前缀。整棵树里只有这一处拼这个键。
pub const KEY_PREFIX: &str = "session_model";

/// 这个人这一段对话落在存储里的键。没有登录身份时没有「这个人」，也就没有键。
pub fn key(owner: &str, session_id: &str) -> Option<String> {
    let owner = owner.trim();
    if owner.is_empty() {
        return None;
    }
    Some(format!("{KEY_PREFIX}:{owner}:{session_id}"))
}

/// 这一段现在选的那只模型。
pub async fn load(
    kv: &Arc<dyn KeyValueStore>,
    owner: &str,
    session_id: &str,
) -> Result<Option<String>, String> {
    let Some(key) = key(owner, session_id) else {
        // 没有身份的一段对话存不住选择：它不属于任何人，也就没有「这一段用哪只」
        // 可以留给下一次。这不是读失败，是根本没得读。
        return Ok(None);
    };
    let raw = kv.get(&key).await.map_err(|e| e.to_string())?;
    Ok(raw.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()))
}

/// 把选择写下来。空名字是「改回默认」。
pub async fn save(
    kv: &Arc<dyn KeyValueStore>,
    owner: &str,
    session_id: &str,
    model: Option<&str>,
) -> Result<Option<String>, String> {
    let Some(key) = key(owner, session_id) else {
        return Err(
            "这段对话没有登录身份，选择存不下来：换模型要记在谁的这一段上，而这条连接答不出「谁」。"
                .into(),
        );
    };
    match model.map(str::trim).filter(|m| !m.is_empty()) {
        Some(model) => kv.set(&key, model).await.map_err(|e| e.to_string())?,
        // 改回默认就是删掉这一格：留一个空串在存储里，「没选过」与「选了空的」
        // 在之后每一次读的时候都要再判一次，而它们本来就是同一件事。
        None => kv.del(&key).await.map_err(|e| e.to_string())?,
    }
    load(kv, owner, session_id).await
}

#[cfg(test)]
mod tests {
    use super::{key, load, save};
    use crate::permission_rules::MemoryKv;
    use cog_core::KeyValueStore;
    use std::sync::Arc;

    /// 选了就记住，下一轮读回同一只；没选过时读回 `None`，不是一份默认名字。
    #[tokio::test]
    async fn a_choice_is_read_back_by_the_next_turn() {
        let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryKv::default());
        assert_eq!(load(&kv, "alice", "s-1").await, Ok(None));
        assert_eq!(
            save(&kv, "alice", "s-1", Some("fast-model")).await,
            Ok(Some("fast-model".into()))
        );
        assert_eq!(
            load(&kv, "alice", "s-1").await,
            Ok(Some("fast-model".into()))
        );
    }

    /// 选择跟着人走：别人用同一个会话号读不到它，改回默认也删的是自己那一格。
    #[tokio::test]
    async fn a_choice_belongs_to_the_person_who_made_it() {
        let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryKv::default());
        save(&kv, "alice", "s-1", Some("deep-model")).await.unwrap();
        assert_eq!(load(&kv, "bob", "s-1").await, Ok(None));
        assert_eq!(
            save(&kv, "alice", "s-1", None).await,
            Ok(None),
            "改回默认之后这一格应当读作没选过"
        );
    }

    /// 没有身份的一段存不住选择，而且要说出来：静默成功会让界面显示一次没发生的切换。
    #[tokio::test]
    async fn a_session_without_an_owner_cannot_remember_a_choice() {
        let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryKv::default());
        assert!(key("", "s-1").is_none());
        assert!(save(&kv, "  ", "s-1", Some("m")).await.is_err());
        assert_eq!(load(&kv, "", "s-1").await, Ok(None));
    }
}
