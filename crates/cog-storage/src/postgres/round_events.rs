//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//! 开工事件：一条 `round_opened`，载荷是这一批的 id 清单。
//!
//! 与带外那一版写的是**同一张表、同一种形状**：那边用一条 `INSERT`，这边用一句
//! 参数化的 SQL；两边把 `ledger_rev` / `code_rev` / `batch` 放在同样的三个键上，于是
//! 谁读都一样。两侧各写一套键名，就等于让「这一轮是什么」有两个答案。
//!
//! 「当前开着的一轮」＝最后一条没有配对 `round_closed` 的 `round_opened`。配对按
//! `subject_id`（这一轮的身份），不按时间远近猜：一轮关掉之后又开一轮，按时间取
//! 「最新那一条」会在关掉的那一瞬把旧的一轮读回来。
//!
//! 读不到与「没有开着的一轮」必须分得开：`current()` 在查询失败时返回 `Err`，不返回
//! `Ok(None)`——把读失败读成「没人开工」，会让缺口判据随后永远为真。

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{PgPool, Row};

use cog_core::{EventPayload, EventStore, FactEvent, RoundEvents, RoundOpened, SFError, SFResult};

/// 事件流的写入器，只认「一轮」这一种事实。落库走那条流唯一的写口。
pub struct RoundEventStore {
    pool: PgPool,
    store: std::sync::Arc<dyn EventStore>,
}

/// 当前开着的那一轮：最后一条没有配对 `round_closed` 的开工事件。
const CURRENT_SQL: &str = "\
SELECT subject_id, ledger_rev, code_rev, round_batch AS batch, actor \
  FROM cogneva_event o \
 WHERE kind = 'round_opened' \
   AND NOT EXISTS (SELECT 1 FROM cogneva_event c \
                    WHERE c.kind = 'round_closed' AND c.subject_id = o.subject_id) \
 ORDER BY at DESC, event_id DESC \
 LIMIT 1";

impl RoundEventStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        let store: std::sync::Arc<dyn EventStore> = std::sync::Arc::new(
            crate::postgres::event_log::EventLogStore::new(pool.clone(), writer),
        );
        Self { pool, store }
    }
}

#[async_trait]
impl RoundEvents for RoundEventStore {
    fn writer(&self) -> &str {
        self.store.writer()
    }

    async fn open(&self, round: &RoundOpened) -> SFResult<()> {
        // 这一族的 id 由轮次身份派生：重放同一轮拿到同一个主键。
        let mut event = FactEvent::new(EventPayload::RoundOpened(round.clone()));
        event.event_id = Some(event_id_for(&round.round));
        event.at = Some(if round.at.timestamp() == 0 {
            Utc::now()
        } else {
            round.at
        });
        self.store
            .append(event)
            .await
            .map_err(|e| SFError::Database(format!("round_opened: {e}")))
    }

    async fn close(&self, round: &str, actor: &str) -> SFResult<()> {
        // 这一轮开了多久：从开工那条事件的 `at` 算。读不到就写 `None`
        // ——「不知道开了多久」与「开了 0 毫秒」是两句话。
        let opened_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT at FROM cogneva_event \
              WHERE kind = 'round_opened' AND subject_id = $1 \
              ORDER BY at ASC, event_id ASC LIMIT 1",
        )
        .bind(round)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("read the open round's start: {e}")))?;
        let elapsed_ms = opened_at.map(|at| (Utc::now() - at).num_milliseconds().max(0) as u64);
        let mut event = FactEvent::new(EventPayload::RoundClosed {
            round: round.to_string(),
            actor: actor.to_string(),
            status: "closed".to_string(),
            elapsed_ms,
        });
        event.event_id = Some(event_id_for(&format!("{round}-closed")));
        self.store
            .append(event)
            .await
            .map_err(|e| SFError::Database(format!("round_closed: {e}")))
    }

    async fn current(&self) -> SFResult<Option<RoundOpened>> {
        let row = sqlx::query(CURRENT_SQL)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the open round: {e}")))?;
        let Some(row) = row else { return Ok(None) };
        // 这一列是 `TEXT` 的生成列（迁移 128）：载荷里的 `batch` 按文本取出来，
        // 这里再按数组解析。老行里还可能有**双重编码**的形状（一段含数组的字符串），
        // 再解一层就读得回来——那段字节是真实发生过的开工记录，丢掉它等于把那些轮
        // 判成「没有清单」。
        let batch: Vec<String> = match row.try_get::<Option<String>, _>("batch") {
            Ok(Some(text)) => match serde_json::from_str::<Vec<String>>(&text) {
                Ok(items) => items,
                Err(_) => match serde_json::from_str::<String>(&text)
                    .ok()
                    .and_then(|inner| serde_json::from_str::<Vec<String>>(&inner).ok())
                {
                    Some(items) => items,
                    None => {
                        return Err(SFError::Database(
                            "round_opened carries a `batch` value that is not a JSON array \
                             of ids: a round whose batch reads as empty is indistinguishable \
                             from one that is finished"
                                .into(),
                        ))
                    }
                },
            },
            // 老行没有这一格：读成空清单会让「这一轮要做几条」答 0，而 0 条正是
            // 「做完了」的读数。所以这里报错，不答 0。
            Ok(None) => {
                return Err(SFError::Database(
                    "round_opened is missing its `batch` array: a round whose batch reads as \
                     empty is indistinguishable from one that is finished"
                        .into(),
                ))
            }
            Err(e) => return Err(SFError::Database(format!("read the batch column: {e}"))),
        };
        Ok(Some(RoundOpened {
            round: row
                .try_get::<String, _>("subject_id")
                .map_err(|e| SFError::Database(format!("read the round identity: {e}")))?,
            ledger_rev: row.try_get("ledger_rev").unwrap_or_default(),
            code_rev: row.try_get("code_rev").unwrap_or_default(),
            batch,
            actor: row.try_get("actor").unwrap_or_default(),
            // 读回来的是「哪一轮、哪一批」，不是「哪一刻开的」：时刻不进判据，
            // 而一个从数据库取回来的时钟只会看起来像开工那一刻。
            at: chrono::DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now),
        }))
    }
}

/// 事件 id 由这一轮的身份派生：重放得到同一个主键。
fn event_id_for(round: &str) -> String {
    let mut out = String::with_capacity(26);
    for ch in round.chars().filter(|c| c.is_ascii_alphanumeric()) {
        if out.len() == 26 {
            break;
        }
        out.push(ch.to_ascii_uppercase());
    }
    while out.len() < 26 {
        out.push('0');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_event_id_is_a_function_of_the_round_identity() {
        assert_eq!(event_id_for("r1"), event_id_for("r1"));
        assert_ne!(event_id_for("r1"), event_id_for("r2"));
    }

    #[test]
    fn the_event_id_is_always_the_envelope_width() {
        crate::postgres::assert_envelope_width(event_id_for);
    }
}
