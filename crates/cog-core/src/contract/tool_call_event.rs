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
pub struct ToolCallEvent {
    /// 调用身份：上游给的 tool call id，或平台自己生成的。
    pub call_id: String,
    /// 这次调用属于哪一条工作（与事件的 `unit_of_work` 同义）。
    pub unit_of_work: String,
    /// 工具名。
    pub tool: String,
    /// 模型当时看到的工具说明。工具会改，回放要用当时那一份，不是今天的。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_description: Option<String>,
    /// 模型当时看到的参数 schema 的 sha256。schema 正文可从注册表按这一格核对。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_sha256: Option<String>,
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
    /// 这次调用点名的**文件路径**，当它与文件有关时。
    ///
    /// 参数在载荷里只有哈希，所以「看过哪些文件」不在事件里，除非这里单独给一格。
    /// 不给它，就得按哈希去载体取正文再解析——那是把一次读数变成一次回源，而回源
    /// 读不到时整份清单会静默变空。给一格之后，清单是事件自己回答的。
    pub path: Option<String>,
    /// 正文的存放处（`artifact://…`）；`None` 表示这一份正文没有可取的载体。
    ///
    /// 事件里存的是哈希，哈希只证明「取回来的字节对不对」，不告诉人去哪取。
    /// 这一格是那个指针：没有它，一次调用的参数与结果只能永远留在触发它的
    /// 那个进程里。
    pub artifact: Option<String>,
    /// 这一次调用属于哪一段会话；`None` 是「不是从会话里发起的」。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// 这一次调用的因果根：把它触发了的那条意图事件 id。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation: Option<String>,
    /// 发起方身份。
    pub actor: String,
    /// 服务端时刻，由写入口取自己的钟。
    pub at: DateTime<Utc>,
}

impl ToolCallEvent {
    /// 幂等键：调用身份。重放的是同一次调用，不是第二次。
    pub fn idempotency_key(&self) -> String {
        self.call_id.clone()
    }

    /// 事件载荷。
    pub fn attributes(&self) -> serde_json::Value {
        serde_json::json!({
            "tool": self.tool,
            "tool_description": self.tool_description,
            "schema_sha256": self.schema_sha256,
            "arguments_sha256": self.arguments_sha256,
            "arguments_bytes": self.arguments_bytes,
            "result_sha256": self.result_sha256,
            "result_bytes": self.result_bytes,
            "outcome": self.outcome,
            "latency_ms": self.latency_ms,
            "path": self.path,
            "artifact": self.artifact,
            "session": self.session,
            "causation": self.causation,
        })
    }
}

impl ToolCallEvent {
    /// 从**字节**造一条调用记录：哈希在这里算，所以「哈希与字节一致」不是一条要记着
    /// 维持的不变式，而是一处计算的结果。参数与结果各一个哈希、各一个长度——两个不同的
    /// 结果可以一样长，只给长度分不开，只给哈希看不出大小。
    #[allow(clippy::too_many_arguments)]
    pub fn from_bytes(
        call_id: impl Into<String>,
        unit_of_work: impl Into<String>,
        tool: impl Into<String>,
        definition: Option<(&str, &serde_json::Value)>,
        actor: impl Into<String>,
        arguments: &[u8],
        result: Option<&[u8]>,
        outcome: impl Into<String>,
        latency_ms: Option<u64>,
        path: Option<String>,
        artifact: Option<String>,
        session: Option<String>,
        causation: Option<String>,
        at: DateTime<Utc>,
    ) -> Self {
        use sha2::{Digest, Sha256};
        let hex = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
        let (tool_description, schema_sha256) = match definition {
            Some((description, parameters)) => {
                let schema = serde_json::to_string(parameters).unwrap_or(Default::default());
                (
                    Some(description.to_string()),
                    Some(format!("{:x}", Sha256::digest(schema.as_bytes()))),
                )
            }
            None => (None, None),
        };
        Self {
            call_id: call_id.into(),
            unit_of_work: unit_of_work.into(),
            tool: tool.into(),
            tool_description,
            schema_sha256,
            arguments_sha256: hex(arguments),
            arguments_bytes: arguments.len() as u64,
            result_sha256: result.map(hex),
            result_bytes: result.map_or(0, |b| b.len() as u64),
            outcome: outcome.into(),
            latency_ms,
            path,
            artifact,
            session,
            causation,
            actor: actor.into(),
            at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> ToolCallEvent {
        ToolCallEvent {
            call_id: "c1".into(),
            unit_of_work: "u1".into(),
            tool: "run_command".into(),
            tool_description: None,
            schema_sha256: None,
            arguments_sha256: "aa".into(),
            arguments_bytes: 10,
            result_sha256: Some("bb".into()),
            result_bytes: 20,
            outcome: "ok".into(),
            latency_ms: Some(5),
            path: None,
            artifact: None,
            session: None,
            causation: None,
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

#[cfg(test)]
mod from_bytes_tests {
    use super::*;

    fn call(args: &[u8], result: Option<&[u8]>) -> ToolCallEvent {
        ToolCallEvent::from_bytes(
            "c1",
            "u1",
            "run_command",
            None,
            "a",
            args,
            result,
            "ok",
            Some(3),
            None,
            None,
            None,
            None,
            Utc::now(),
        )
    }

    /// 哈希与字节一致：同一段字节算两次一样，改一个字节就变。
    #[test]
    fn the_hash_is_a_function_of_the_bytes() {
        let one = call(b"{\"cmd\":\"ls\"}", Some(b"ok"));
        let two = call(b"{\"cmd\":\"ls\"}", Some(b"ok"));
        let other = call(b"{\"cmd\":\"rm\"}", Some(b"ok"));
        assert_eq!(one.arguments_sha256, two.arguments_sha256);
        assert_ne!(one.arguments_sha256, other.arguments_sha256);
        assert_eq!(one.arguments_bytes, 12, "`{{\"cmd\":\"ls\"}}` 是 12 个字节");
    }

    /// 没有结果时两个结果字段都空着：哈希 `None`、长度 0——而不是一个空串的哈希。
    #[test]
    fn a_call_without_a_result_has_no_result_hash() {
        let failed = call(b"{}", None);
        assert_eq!(failed.result_sha256, None);
        assert_eq!(failed.result_bytes, 0);
    }

    /// 长度与哈希一起给：两个一样长但内容不同的结果，只靠长度分不开。
    #[test]
    fn two_results_of_equal_length_are_distinguished_by_their_hashes() {
        let a = call(b"{}", Some(b"abcd"));
        let b = call(b"{}", Some(b"abce"));
        assert_eq!(a.result_bytes, b.result_bytes);
        assert_ne!(a.result_sha256, b.result_sha256);
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    /// 路径单独一格：参数只有哈希，不给这一格，「看过哪些文件」就答不出来，
    /// 只能回源去载体取正文——回源读不到时整份清单会静默变空。
    #[test]
    fn the_path_travels_in_the_payload_when_the_call_names_a_file() {
        let mut c = ToolCallEvent {
            call_id: "c1".into(),
            unit_of_work: "u1".into(),
            tool: "read_file".into(),
            tool_description: None,
            schema_sha256: None,
            arguments_sha256: "aa".into(),
            arguments_bytes: 1,
            result_sha256: None,
            result_bytes: 0,
            outcome: "ok".into(),
            latency_ms: None,
            path: Some("crates/cog-core/src/lib.rs".into()),
            artifact: None,
            session: None,
            causation: None,
            actor: "a".into(),
            at: Utc::now(),
        };
        assert_eq!(c.attributes()["path"], "crates/cog-core/src/lib.rs");
        c.path = None;
        assert!(c.attributes()["path"].is_null(), "与文件无关的调用不带路径");
    }
    /// 写出去的键与目录一致：载荷存 text，没有类型替人记住它有什么。
    #[test]
    fn the_written_keys_are_the_catalogued_keys() {
        let call = ToolCallEvent::from_bytes(
            "c1",
            "u1",
            "read_file",
            None,
            "a",
            b"{}",
            Some(b"ok"),
            "ok",
            None,
            None,
            None,
            None,
            None,
            Utc::now(),
        );
        let written: std::collections::BTreeSet<String> = call
            .attributes()
            .as_object()
            .expect("a payload is an object")
            .keys()
            .cloned()
            .collect();
        let catalogued: std::collections::BTreeSet<String> = crate::payload_fields("tool_call", 1)
            .expect("tool_call v1 is catalogued")
            .iter()
            .map(|(field, _)| (*field).to_string())
            .collect();
        assert_eq!(written, catalogued, "写者与目录不是同一组键");
    }
}
