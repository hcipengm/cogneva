//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//! 一次工具调用的正本是一条事件，不是对象存储里的一个对象。
//!
//! 现实现把每次工具调用写成对象存储里的一个对象（走内存 API 的一条 HTTP），而事件流里
//! **没有** `tool_call` 这一族——那份对象于是成了一份没人校验的记录：它在不在、是不是
//! 那一次调用的、有没有被改过，都没有第二条读数能回答。事件流是只追加的正本，所以一次
//! 调用先落一条 `tool_call`，对象存储那份降为**正文载体**（按内容哈希取），不再是记录。
//!
//! 载荷里放哈希而不是正文：事件要能被重放，重放要的是「那次结果是什么」的可校验指针。
//! 参数与结果各一个哈希，加字节数——两个哈希都不一样却同长，读者只看得见长度；所以长度
//! 与哈希一起给，是为了让「换了一个同样长的结果」在读数上分得开。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// 一次工具调用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    /// 调用身份：上游给的 tool call id，或平台自己生成的。
    pub call_id: String,
    /// 这次调用属于哪一条工作（与事件的 `unit_of_work` 同义）。
    pub unit_of_work: String,
    /// 工具名。
    pub tool: String,
    /// 参数字节的哈希（sha256 hex）。正文不进事件。
    pub arguments_sha256: String,
    /// 参数字节数。
    pub arguments_bytes: u64,
    /// 结果字节的哈希；没有结果（失败）时为 `None`。
    pub result_sha256: Option<String>,
    /// 结果字节数。
    pub result_bytes: u64,
    /// 结局词：`ok` 是唯一的完成词，其余都是「这一步还没过」。
    pub outcome: String,
    /// 这次调用花了多久；拿不到时为 `None`（不写 0：0 毫秒是一次真实读数）。
    pub latency_ms: Option<u64>,
    /// 发起方身份。
    pub actor: String,
    /// 服务端时刻，由写入口取自己的钟。
    pub at: DateTime<Utc>,
}

impl ToolCall {
    /// 幂等键：调用身份。重放的是同一次调用，不是第二次。
    pub fn idempotency_key(&self) -> String {
        self.call_id.clone()
    }

    /// 事件载荷。
    pub fn attributes(&self) -> serde_json::Value {
        serde_json::json!({
            "tool": self.tool,
            "arguments_sha256": self.arguments_sha256,
            "arguments_bytes": self.arguments_bytes,
            "result_sha256": self.result_sha256,
            "result_bytes": self.result_bytes,
            "outcome": self.outcome,
            "latency_ms": self.latency_ms,
        })
    }
}

/// 往事件流里追加一条工具调用。实现必须幂等，写不进去要如实报错。
#[async_trait]
pub trait ToolCallEvents: Send + Sync {
    fn writer(&self) -> &str;

    async fn append(&self, call: &ToolCall) -> SFResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> ToolCall {
        ToolCall {
            call_id: "c1".into(),
            unit_of_work: "u1".into(),
            tool: "run_command".into(),
            arguments_sha256: "aa".into(),
            arguments_bytes: 10,
            result_sha256: Some("bb".into()),
            result_bytes: 20,
            outcome: "ok".into(),
            latency_ms: Some(5),
            actor: "a".into(),
            at: Utc::now(),
        }
    }

    /// 载荷里是哈希与长度，不是正文：事件可重放，但它自己不是正文。
    #[test]
    fn the_payload_carries_hashes_and_lengths_not_bodies() {
        let attrs = call().attributes();
        assert_eq!(attrs["arguments_sha256"], "aa");
        assert_eq!(attrs["arguments_bytes"], 10);
        assert_eq!(attrs["result_sha256"], "bb");
        assert_eq!(attrs["result_bytes"], 20);
        assert!(attrs.get("arguments").is_none());
        assert!(attrs.get("result").is_none());
    }

    /// 缺耗时不写 0：0 毫秒是一次真实读数，与「不知道」同形。
    #[test]
    fn a_missing_latency_stays_absent() {
        let mut c = call();
        c.latency_ms = None;
        assert!(c.attributes()["latency_ms"].is_null());
    }

    /// 幂等键是调用身份。
    #[test]
    fn the_idempotency_key_is_the_call_identity() {
        assert_eq!(call().idempotency_key(), "c1");
    }
}
