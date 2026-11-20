//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//! 一次工具调用写成事件流里的一行。
//!
//! 与 `llm_call` 同一套信封、同一条幂等约束（`(writer, idempotency_key)`，键取调用身份），
//! 差别只在载荷：工具名、参数的**哈希**、结果的**哈希**、耗时、成败。正文不进事件——
//! 超限的正文进内容寻址存储，事件里存哈希；这样「同一次调用的两份记录」不会再出现：
//! 事件是记录，正文是载体。
//!
//! 为什么哈希而不是正文：事件要能被重放，而重放需要的是「那一次调用的结果是什么」的
//! 可校验指针，不是几兆字节本身。哈希与字节一致这一条由写入口保证——它先算哈希再写，
//! 字节与哈希不会分叉；读侧按哈希去载体取，取回来的字节再算一次，对不上就是载体坏了。

use async_trait::async_trait;
use sqlx::PgPool;

use cog_core::{
    check as check_envelope, EnvelopeFields, SFError, SFResult, ToolCall, ToolCallEvents,
};

/// 事件流的写入器，只认工具调用这一种事实。
pub struct ToolCallEventStore {
    pool: PgPool,
    writer: String,
}

/// 写一行工具调用事件的语句。kind 与 subject_kind 写死在这一句里（这一族只写一种），
/// 其余列从载荷来。
const INSERT_SQL: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, \
    attributes, at) \
VALUES ($1, 'tool_call', 'tool_call', $2, $3, 'call', $4, 1, $5, NULL, NULL, $6, $7, $8, 1, $9, $10) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

impl ToolCallEventStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        Self {
            pool,
            writer: writer.into(),
        }
    }
}

#[async_trait]
impl ToolCallEvents for ToolCallEventStore {
    fn writer(&self) -> &str {
        &self.writer
    }

    async fn append(&self, call: &ToolCall) -> SFResult<()> {
        let event_id = event_id_for(&call.call_id);
        let attributes = call.attributes().to_string();
        check_envelope(&EnvelopeFields {
            event_id: event_id.clone(),
            kind: "tool_call".into(),
            subject_kind: "tool_call".into(),
            subject_id: call.call_id.clone(),
            unit_of_work: call.unit_of_work.clone(),
            step: "call".into(),
            outcome: call.outcome.clone(),
            idempotency_key: call.idempotency_key(),
            actor: call.actor.clone(),
            writer: self.writer.clone(),
            scope: call.tool.clone(),
            attributes: attributes.clone(),
        })
        .map_err(|defect| {
            SFError::Database(format!(
                "tool_call event refused before it was written: {}",
                defect.as_str()
            ))
        })?;
        sqlx::query(INSERT_SQL)
            .bind(&event_id)
            .bind(&call.call_id)
            .bind(&call.unit_of_work)
            .bind(&call.outcome)
            .bind(call.idempotency_key())
            .bind(&call.actor)
            .bind(&self.writer)
            .bind(&call.tool)
            .bind(attributes)
            .bind(call.at)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("append tool_call event: {e}")))?;
        Ok(())
    }
}

/// 事件 id 由调用身份派生：重放得到同一个主键。
fn event_id_for(call_id: &str) -> String {
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

    #[test]
    fn the_event_id_is_a_function_of_the_call_identity() {
        assert_eq!(event_id_for("call-1"), event_id_for("call-1"));
        assert_ne!(event_id_for("call-1"), event_id_for("call-2"));
    }

    #[test]
    fn the_event_id_is_always_the_envelope_width() {
        for id in ["", "x", &"y".repeat(40)] {
            assert_eq!(event_id_for(id).len(), 26, "{id:?}");
        }
    }
}
