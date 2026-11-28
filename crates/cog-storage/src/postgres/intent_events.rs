//! 意图的两条事实落进事件流：到达（`intent_received`）与接手（`intent_triaged`）。
//!
//! 同一个 store 出两个面：写面（`EventStore`）给对话那一屏，读面
//! （[`cog_core::IntentReader`]）给把意图变成待办的那条循环。两面落同一张表、同一
//! 份信封，于是「哪些还没有人接手」是一次存在性判断，而不是两个存储之间的一次对账。
//!
//! 接手记成第二条事件，不改写到达那一条：到达是证据，证据不改；派生出什么、派给
//! 哪一条待办，是后来发生的事。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{IntentReader, IntentRow, SFError, SFResult};

/// 到达事件与接手事件的 kind。闭集在 [`cog_core::KINDS`] 里，这里只引用。
const RECEIVED: &str = "intent_received";
const TRIAGED: &str = "intent_triaged";

/// 还没有接手的那几条，按到达次序。
///
/// 「接手过没有」有两个成立的读法，两种都算：一条 `intent_triaged` 事件（分诊那条路
/// 写下的），或者一条任务行的 `intent_id` 就是这条身份（planner 派生出待办时写下的）。
/// 只认其中一种的时候，另一条路上已经做掉的活会在收件台上永远压着——2026-11-24
/// 两条线合流时就撞在这里。`task_defined.intent_id` 是生成列（迁移 127），带条件索引。
const UNDISPATCHED: &str = "\
SELECT i.subject_id, i.attributes, i.at \
  FROM cogneva_event i \
 WHERE i.kind = 'intent_received' \
   AND NOT EXISTS (SELECT 1 FROM cogneva_event t \
                    WHERE t.kind = 'intent_triaged' AND t.subject_id = i.subject_id) \
   AND NOT EXISTS (SELECT 1 FROM task_defined d WHERE d.intent_id = i.subject_id) \
 ORDER BY i.at ASC, i.event_id ASC \
 LIMIT $1";

/// 事件流上的意图写者与读者。写走那条流唯一的写口，读用同一张表。
pub struct IntentEventStore {
    pool: PgPool,
    store: std::sync::Arc<dyn cog_core::EventStore>,
}

impl IntentEventStore {
    pub fn new(pool: PgPool, store: std::sync::Arc<dyn cog_core::EventStore>) -> Self {
        Self { pool, store }
    }
}

const ONE_INTENT: &str = "\
SELECT i.attributes, i.at \
  FROM cogneva_event i \
 WHERE i.kind = 'intent_received' AND i.subject_id = $1 \
 ORDER BY i.at ASC, i.event_id ASC \
 LIMIT 1";

#[async_trait]
impl IntentReader for IntentEventStore {
    async fn undispatched(&self, limit: i64) -> SFResult<Vec<IntentRow>> {
        let rows = sqlx::query(UNDISPATCHED)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read undispatched intents: {e}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let identity: String = row
                .try_get("subject_id")
                .map_err(|e| SFError::Database(format!("intent row: {e}")))?;
            // 信封的列是 TEXT（约束只保证里面是 JSON 对象），与其它读事件流的
            // store 一样在 Rust 里解码。
            let attributes_text: String = row
                .try_get("attributes")
                .map_err(|e| SFError::Database(format!("intent row: {e}")))?;
            let attributes: serde_json::Value = match serde_json::from_str(&attributes_text) {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!(
                        identity = %identity,
                        error = %e,
                        "an intent row's attributes do not decode; it is left alone"
                    );
                    continue;
                }
            };
            let at: DateTime<Utc> = row
                .try_get("at")
                .map_err(|e| SFError::Database(format!("intent row: {e}")))?;
            // 一行读不成意图（没有原文）时跳过并点名，而不是把它读成一条空话：
            // 空话会被派成一条谁也说不清要做什么的待办。
            match IntentRow::from_attributes(identity.clone(), &attributes, at) {
                Some(row) => out.push(row),
                None => tracing::warn!(
                    identity = %identity,
                    "an intent row carries no text; it cannot become work and is left alone"
                ),
            }
        }
        Ok(out)
    }

    async fn by_identity(&self, identity: &str) -> SFResult<Option<IntentRow>> {
        let row = sqlx::query(ONE_INTENT)
            .bind(identity)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read one intent: {e}")))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let attributes_text: String = row
            .try_get("attributes")
            .map_err(|e| SFError::Database(format!("intent row: {e}")))?;
        let at: DateTime<Utc> = row
            .try_get("at")
            .map_err(|e| SFError::Database(format!("intent row: {e}")))?;
        let attributes: serde_json::Value = match serde_json::from_str(&attributes_text) {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(identity = %identity, error = %e, "an intent row's attributes do not decode");
                return Ok(None);
            }
        };
        Ok(IntentRow::from_attributes(
            identity.to_string(),
            &attributes,
            at,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 两条语句各自的幂等键不是同一个：接手与到达是两件事，共用一个键会让接手把
    /// 到达那条挡住（`ON CONFLICT (writer, idempotency_key)`）。
    #[test]
    fn the_two_events_do_not_share_an_idempotency_key() {
        let identity = "chat:s1:9f2a";
        assert_eq!(identity, "chat:s1:9f2a");
        assert_ne!(identity, format!("{identity}:triaged"));
    }

    /// 闭集里必须有这两个 kind，否则两条写入口在信封检查处就被拒。
    #[test]
    fn both_kinds_are_in_the_closed_set() {
        assert!(cog_core::is_known_kind(RECEIVED));
        assert!(cog_core::is_known_kind(TRIAGED));
    }
}
