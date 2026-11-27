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

use cog_core::{EventPayload, EventStore, FactEvent, SFError, SFResult, ToolCallEvent};

/// 事件流的写入器，只认工具调用这一种事实。落库走那条流唯一的写口。
pub struct ToolCallEventStore {
    store: std::sync::Arc<dyn EventStore>,
}

impl ToolCallEventStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        let store: std::sync::Arc<dyn EventStore> =
            std::sync::Arc::new(crate::postgres::event_log::EventLogStore::new(pool, writer));
        Self { store }
    }
}

impl ToolCallEventStore {
    /// 写一条工具调用事实：与其他族同一条流、同一个写口。
    pub async fn append(&self, call: &ToolCallEvent) -> SFResult<()> {
        let mut event = FactEvent::new(EventPayload::ToolCall(call.clone()));
        event.event_id = Some(event_id_for(&call.call_id));
        event.at = Some(call.at);
        self.store
            .append(event)
            .await
            .map_err(|e| SFError::Database(format!("tool_call: {e}")))
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

/// 一条工作碰过哪些文件：从它自己的 `tool_call` 事件里取，不由模型自报。
///
/// 取的是载荷里那一格 `path`（S37：给文件相关的调用单独一格），所以这条读取不需要
/// 回源去载体——回源读不到时清单会静默变空，而那正是要防的形状。
///
/// 工具调用的 `scope` 列存的是工具名，文件在参数里——而参数只存哈希（正文在载体里）。
/// 所以这条读取不解析参数正文：它按**工具名 + 时间**列出调用，再由调用方按自己的
/// 载体取正文。这是有意的：一个要解析正文才能回答的读取，会在正文缺失时变成
/// 「什么都没碰过」，而那正是要防的形状。
pub const TOOL_CALLS_FOR_WORK_SQL: &str = "\
SELECT tool_path AS path, at \
  FROM cogneva_event \
 WHERE kind = 'tool_call' AND unit_of_work = $1 \
   AND tool_path IS NOT NULL \
 ORDER BY at, subject_id";

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
        crate::postgres::assert_envelope_width(event_id_for);
    }
}
