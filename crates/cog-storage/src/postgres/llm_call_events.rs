//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//! 把一次调用写成事件流里的一行。这是那一族的**写入口**，读侧还没切过来。
//!
//! 三条形状上的讲究，都与 `cogneva_event` 的信封有关：
//!
//! - 信封的每一列都点名写，不靠默认值：默认值只盖住「服务端时刻」与「第一次尝试」，
//!   其余每一列缺了都少一个能力（迁移 117 的逐列注释写了每一个）。
//! - `on conflict (writer, idempotency_key) do nothing`：同一次调用重放写的是同一条。
//! - `attributes` 是 text 列，存的是载荷本身的 JSON，不是它的再次编码——一条事件读回来
//!   时那一格应当是能直接解析的对象，而不是一个含 JSON 的字符串。这一点由这里的
//!   `to_string()` 与读侧的 `from_str` 一对保证，写错的方向是「看起来存进去了」。

use async_trait::async_trait;
use sqlx::PgPool;

use cog_core::{
    check as check_envelope, EnvelopeFields, LlmCall, LlmCallEvents, SFError, SFResult,
};

/// 事件流的写入器，只认这一种事实。
pub struct LlmCallEventStore {
    pool: PgPool,
    /// 这一条连接代表哪个写者（部署名）。与幂等键一起构成唯一约束的两半。
    writer: String,
}

/// 写一行事件的语句。
///
/// `event_id` 由调用方给（ULID 由事件流那一层的约定决定），这里只把它写进去；
/// 其余枚举列照信封的取值写死在这条语句里，因为这一族只写一种 kind。
const INSERT_SQL: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, \
    attributes, at) \
VALUES ($1, 'llm_call', 'llm_call', $2, $2, 'call', 'ok', 1, $3, NULL, $9, $4, $5, $6, 1, $7, $8) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

impl LlmCallEventStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        Self {
            pool,
            writer: writer.into(),
        }
    }
}

#[async_trait]
impl LlmCallEvents for LlmCallEventStore {
    fn writer(&self) -> &str {
        &self.writer
    }

    async fn append(&self, call: &LlmCall) -> SFResult<()> {
        let event_id = ulid_like_id(&call.call_id);
        // 信封先过判据再写：空着的那一格与「不适用」在读数上同形，写进去就分不开了。
        check_envelope(&EnvelopeFields {
            event_id: event_id.clone(),
            kind: "llm_call".into(),
            subject_kind: "llm_call".into(),
            subject_id: call.call_id.clone(),
            unit_of_work: call.call_id.clone(),
            step: "call".into(),
            outcome: "ok".into(),
            idempotency_key: call.idempotency_key(),
            actor: call.actor.clone(),
            writer: self.writer.clone(),
            scope: call.model.clone(),
            attributes: call.attributes().to_string(),
        })
        .map_err(|defect| {
            SFError::Database(format!(
                "llm_call event refused before it was written: {}",
                defect.as_str()
            ))
        })?;
        sqlx::query(INSERT_SQL)
            .bind(&event_id)
            .bind(&call.call_id)
            .bind(call.idempotency_key())
            .bind(&call.actor)
            .bind(&self.writer)
            .bind(&call.model)
            .bind(call.attributes().to_string())
            .bind(call.batch_id.clone())
            .bind(call.at)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("append llm_call event: {e}")))?;
        Ok(())
    }
}

/// 事件 id 由调用身份派生，同一身份重放得到同一个 id。
///
/// 不做成随机 ULID：那样重放会造出第二个 id，而 id 是主键——两个 id 指着同一件事，
/// 读到的人要自己去猜哪条是真的。派生之后，`(writer, idempotency_key)` 与主键说的是
/// 同一件事，两条保护不会互相打架。
fn ulid_like_id(call_id: &str) -> String {
    // 26 个字符的 Crockford base32 形状，取自调用身份的前 26 个合法字符，不足则补 0。
    // 这不是 ULID 的时间序，只是同一个 26 字符外形——信封要的是「够长且稳定」，而
    // 「按 id 排序即按时间排序」这条约定属于**入账事件**，不是这一族。
    let mut out = String::with_capacity(26);
    for ch in call_id.chars().filter(|c| c.is_ascii_alphanumeric()) {
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

    /// 同一个调用身份派生出同一个 id：重放不会造出第二个主键。
    #[test]
    fn the_event_id_is_a_function_of_the_call_identity() {
        assert_eq!(ulid_like_id("call-abc"), ulid_like_id("call-abc"));
        assert_ne!(ulid_like_id("call-abc"), ulid_like_id("call-abd"));
    }

    /// id 恒为 26 个字符：信封那一列是定长的，短的会被拒或者被补齐到别人的形状。
    #[test]
    fn the_event_id_is_always_the_envelope_width() {
        for call_id in ["", "x", &"y".repeat(40)] {
            assert_eq!(ulid_like_id(call_id).len(), 26, "{call_id:?}");
        }
        assert!(
            ulid_like_id("").chars().all(|c| c == '0'),
            "an empty identity has to pad rather than fall short"
        );
    }
}
