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

use cog_core::{
    check as check_envelope, EnvelopeFields, RoundEvents, RoundOpened, SFError, SFResult,
};

/// 事件流的写入器，只认「一轮开工」这一种事实。
pub struct RoundEventStore {
    pool: PgPool,
    writer: String,
}

/// kind 与 subject_kind 写死在这一句里（这一族只写一种），其余列从载荷来。
const INSERT_SQL: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, \
    attributes, at) \
VALUES ($1, 'round_opened', 'round', $2, $2, 'open', 'ok', 1, $3, NULL, NULL, $4, $5, \
        'round', 1, $6, $7) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

/// 关一轮。幂等键是「这一轮的身份＋closed」，与开工那条不是同一个键。
const CLOSE_SQL: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, \
    attributes, at) \
VALUES ($1, 'round_closed', 'round', $2, $2, 'close', 'ok', 1, $3, NULL, NULL, $4, $5, \
        'round', 1, '{}', NOW()) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

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
        Self {
            pool,
            writer: writer.into(),
        }
    }
}

#[async_trait]
impl RoundEvents for RoundEventStore {
    fn writer(&self) -> &str {
        &self.writer
    }

    async fn open(&self, round: &RoundOpened) -> SFResult<()> {
        let event_id = event_id_for(&round.round);
        let attributes = round.attributes().to_string();
        check_envelope(&EnvelopeFields {
            event_id: event_id.clone(),
            kind: "round_opened".into(),
            subject_kind: "round".into(),
            subject_id: round.round.clone(),
            unit_of_work: round.round.clone(),
            step: "open".into(),
            outcome: "ok".into(),
            idempotency_key: round.idempotency_key(),
            actor: round.actor.clone(),
            writer: self.writer.clone(),
            scope: "round".into(),
            attributes: attributes.clone(),
        })
        .map_err(|defect| {
            SFError::Database(format!(
                "round_opened refused before it was written: {}",
                defect.as_str()
            ))
        })?;
        sqlx::query(INSERT_SQL)
            .bind(&event_id)
            .bind(&round.round)
            .bind(round.idempotency_key())
            .bind(&round.actor)
            .bind(&self.writer)
            .bind(attributes)
            .bind(if round.at.timestamp() == 0 {
                Utc::now()
            } else {
                round.at
            })
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("append round_opened: {e}")))?;
        Ok(())
    }

    async fn close(&self, round: &str, actor: &str) -> SFResult<()> {
        let event_id = event_id_for(&format!("{round}-closed"));
        sqlx::query(CLOSE_SQL)
            .bind(&event_id)
            .bind(round)
            .bind(format!("{round}:closed"))
            .bind(actor)
            .bind(&self.writer)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("append round_closed: {e}")))?;
        Ok(())
    }

    async fn current(&self) -> SFResult<Option<RoundOpened>> {
        let row = sqlx::query(CURRENT_SQL)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the open round: {e}")))?;
        let Some(row) = row else { return Ok(None) };
        let batch: Vec<String> = match row.try_get::<Option<serde_json::Value>, _>("batch") {
            Ok(Some(serde_json::Value::Array(items))) => items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            // 2026-11-22 之前带外那一版把清单写成**一段含数组的字符串**，
            // 这一侧读到的是 `String`。那一段字节是真实发生过的开工记录，丢掉它等于
            // 把 33 轮判成「没有清单」——所以读得懂的旧形状就地读出来，写在注释里。
            Ok(Some(serde_json::Value::String(text))) => {
                match serde_json::from_str::<Vec<String>>(&text) {
                    Ok(items) => items,
                    Err(_) => {
                        return Err(SFError::Database(
                            "round_opened carries a `batch` string that is not a JSON array \
                             of ids: a round whose batch reads as empty is indistinguishable \
                             from one that is finished"
                                .into(),
                        ))
                    }
                }
            }
            // 老行没有这一格、或者它不是数组：读成空清单会让「这一轮要做几条」答 0，
            // 而 0 条正是「做完了」的读数。所以这里报错，不答 0。
            Ok(_) => {
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
