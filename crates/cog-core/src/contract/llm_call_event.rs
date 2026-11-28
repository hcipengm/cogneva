//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//! 一次 LLM 调用的**正本**是一条事件，不是一行汇总。
//!
//! `gateway_llm_usage` 今天记着「这次调用花了多少」——它是唯一的一份，却只是「方便查」
//! 的形状：口径一变就得回填，行丢了就永远读不回来。事件流是只追加的正本，投影随时可
//! 重算，所以调用这件事该落进流里，汇总表是它的一道投影。
//!
//! 这一层只定义契约：网关进程只依赖 `cog-core`（契约层），连不到 `cog-storage`，所以
//! 「往流里追加一条调用事实」必须先是一个 trait，实现由装配层交进来。
//!
//! 两条形状上的讲究：
//!
//! - **幂等键取调用身份**，不是取时间：同一次调用重放（进程在写与答复之间没死成、
//!   重试打进来）写的是同一条事件，不是两条花费。`(writer, idempotency_key)` 上已有
//!   唯一约束，追加走 `DO NOTHING`。
//! - **事件带 `at`，但它是服务端时刻**，由写入口取自己的钟，不由调用方传。一个由被
//!   记账方自己填的时刻，回填与重放都会让它漂。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// 一次上游调用，作为一条事实。
///
/// 字段是「这次调用花了多少、为谁花的、成没成」这几件事本身；**正文不在这里**——
/// 请求与响应的字节在留底那一面（S45／S47 之后才搬进库），事件只指向它。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmCall {
    /// 调用身份。幂等键由它导出：同一次调用无论重放几次，账上只有一条。
    pub call_id: String,
    /// 服务这一次调用的上游池身份。
    pub upstream: String,
    /// 线协议族（`openai` / `anthropic`）。
    pub api_style: String,
    /// 上游配置里那个真模型名。
    pub model: String,
    /// 平台的结果词（`ok`、`refused_by_budget`、`error` …），与用量行同一套。
    pub result: String,
    /// 输入 token。
    pub input_tokens: u64,
    /// 输出 token。
    pub output_tokens: u64,
    /// 命中前缀缓存的 token。读侧按人/按工作区判花销时要它（缓存便宜得多）。
    pub cached_tokens: u64,
    /// **写进**缓存的那部分 token（Anthropic 的 cache_creation）。旧表的
    /// `tokens_cache_created` 列有它，事件流原先没带——读切换后那一半读数会掉成
    /// 「没人量过」，而它是命中率的分母之一。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_created_tokens: Option<u64>,
    /// 调用来源（`interactive`／`gateway_pool`／…）。读侧按来源分桶时要它。
    pub origin: String,
    /// 这次调用记在谁头上；`None` 是这条连接没有身份。按人判预算要它。
    pub owner: Option<String>,
    /// 这一次调用花了多久，毫秒；`None` 表示落账那一刻它已经不在手里了。
    ///
    /// 不用 0 顶替：0 毫秒是一次真实读数（本地拒调用就是 0），把它与「不知道」
    /// 写成同一个数，正是这一族最忌的形状。拿到答复的那一处才有这个数，落账的那
    /// 一处未必有。
    pub latency_ms: Option<u64>,
    /// 这次调用是为哪个任务花的；`None` 表示不是任务发起的。
    pub task_id: Option<String>,
    /// 发起方身份。
    pub actor: String,
    /// 这一笔花掉的钱（微单位），`None` 表示没有定价过。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_micros: Option<i64>,
    /// 用哪本价目表的哪一档算的（`<book>@<currency>`）。没有它就算不出币种。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_book: Option<String>,
    /// 这一笔调用属于哪一批（批量合并调用那一路打的戳）；`None` 是单独一次调用。
    ///
    /// 它写在**信封**上而不是载荷里：`batch_id` 本来就是信封的一列，问「这一批一起花了
    /// 多少」是它的用途；放进 `attributes` 会让读者要么按列、要么按 JSON 取键，两条路
    /// 各读一半（2026-11-22 实测：写的人放载荷、读的人按列，恒答「这一批没有记录」）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    /// 这一次调用属于哪一段会话；`None` 是这条连接没有会话。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// 配速那一拍的读数，JSON 文本（`measured_upstreams / allowance / spent /
    /// reserve / window_start`）。它原先只存在汇总表的列里，而那正是「停写汇总表
    /// 就会丢掉的东西」——搬进事件之后，行才真的只是它的投影。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pace: Option<String>,

    // ---- 用量行上本来就有、载荷此前没有的几格（EV15）。停写行之前必须先在这里。 ----
    /// 输出里有多少是模型想出来的；`None` 是上游没说。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_reasoning: Option<u64>,
    /// 拒绝的原因分类（配额/配速/熔断…）；只有拒绝行有。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal_kind: Option<String>,
    /// 「什么时候可以再来」；只有拒绝行有。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<u64>,
    /// 这次调用的计量结局（`read`/`absent`/…）：读面靠它分清「上游说零」与「上游没开口」。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_outcome: Option<String>,
    /// 发请求前的输入估算；`None` 是没有估算。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_input: Option<u64>,
    /// 哪一级额度拒的（人/工作区/任务）；只有拒绝行有。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused_scope: Option<String>,
    /// 模型侧为什么停了（`stop`／`length`／`tool_calls`／`content_filter` …）。
    /// 平台的结果词说的是调用成没成，这一格说的是模型自己停在哪。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    /// 首字时延（毫秒）；`None` 是没量到（非流式或没有内容帧）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttft_ms: Option<u64>,
    /// 提示清单：这一次发出去的每段 sha256（JSON 数组）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_manifest: Option<String>,
    /// 装配时挤掉了多少条更早的消息。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_dropped: Option<u32>,
    /// 写入口取自己的钟填进来的时刻。
    pub at: DateTime<Utc>,
}

/// 用量行与调用事件里同一格的两种键名。
///
/// 行用 `tokens_*`／`task`，事件用 `*_tokens`／`task_id`。逐格相等的判据按这张表
/// 取值——按名字直接比，这五格会被读成「事件里没有」。表只有这一处。
pub const USAGE_CELL_MAP: &[(&str, &str)] = &[
    ("tokens_input", "input_tokens"),
    ("tokens_output", "output_tokens"),
    ("tokens_cached", "cached_tokens"),
    ("tokens_cache_created", "cache_created_tokens"),
    ("task", "task_id"),
];

/// 行字段名 → 事件键名。
pub fn event_key_for_row_field(row_field: &str) -> Option<&'static str> {
    USAGE_CELL_MAP
        .iter()
        .find(|(row, _)| *row == row_field)
        .map(|(_, key)| *key)
}

/// 事件键名 → 行字段名。
pub fn row_field_for_event_key(key: &str) -> Option<&'static str> {
    USAGE_CELL_MAP
        .iter()
        .find(|(_, event)| *event == key)
        .map(|(row, _)| *row)
}

/// 这一次请求发出去的提示由哪些段组成：每段一条 sha256，按发送顺序。
///
/// 段是请求体里的每一条 message（系统提示、历史、工具结果各算一段）。哈希让「当时
/// 模型看到的是哪几段」在留底过期之后仍答得出来——前提是每一段的载体永久（见
/// `EVIDENCE_CARRIERS`）。`None` 是这条路径手上没有请求体（探测类调用自己拼 JSON）。
pub fn prompt_manifest(request_body: &[u8]) -> Option<String> {
    let body: serde_json::Value = serde_json::from_slice(request_body).ok()?;
    let body = body.as_object()?;
    // 没有 `messages` 的请求（探测类调用自己拼的 JSON）没有前缀可言。
    body.get("messages")?.as_array()?;
    // 清单的算法只有一处（`prompt_prefix::manifest`）：写的人与读的人算同一份，
    // 否则「前缀没断」与「读的人算短了一截」在读数上同形。
    Some(super::prompt_prefix::manifest_json(body))
}

/// 一笔调用的身份：由它落下的那一行派生。
///
/// live 落账与回填都走这一个拼法——两条路各拼一次时，同一行会被写成两条事件、
/// 记两笔钱。行先写、事件后写，回填手上也只有行，所以身份只能从行来。
pub fn llm_call_id(row_id: &str) -> String {
    format!("row:{row_id}")
}

impl LlmCall {
    /// 幂等键：**调用身份**，不是时刻也不是内容哈希。
    ///
    /// 重放保护要的是「同一次调用只记一条」。用内容哈希做不到——同样的提示词打两次
    /// 是真花了两次钱；用时刻做不到——重试那一刻与第一次不是同一个数。
    pub fn idempotency_key(&self) -> String {
        self.call_id.clone()
    }

    /// 载荷：事件正文，`attributes` 列直接存它的 JSON。
    pub fn attributes(&self) -> serde_json::Value {
        serde_json::json!({
            "upstream": self.upstream,
            "api_style": self.api_style,
            "model": self.model,
            "result": self.result,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cached_tokens": self.cached_tokens,
            "cache_created_tokens": self.cache_created_tokens,
            "origin": self.origin,
            "owner": self.owner,
            "latency_ms": self.latency_ms,
            "task_id": self.task_id,
            "pace": self.pace,
            "cost_micros": self.cost_micros,
            "price_book": self.price_book,
            "session": self.session,
            "batch_id": self.batch_id,
            "tokens_reasoning": self.tokens_reasoning,
            "refusal_kind": self.refusal_kind,
            "retry_after_secs": self.retry_after_secs,
            "usage_outcome": self.usage_outcome,
            "estimated_input": self.estimated_input,
            "refused_scope": self.refused_scope,
            "finish_reason": self.finish_reason,
            "ttft_ms": self.ttft_ms,
            "prompt_manifest": self.prompt_manifest,
            "prompt_dropped": self.prompt_dropped,
        })
    }
}

impl LlmCall {
    /// 从流里读回来：`attributes` 那一列就是 [`Self::attributes`] 写下去的 JSON。
    ///
    /// 与写侧同一个字段表：加一格时两边一起改，漏一边的代价是——读回来少一格
    /// 就是 `None`，而 `None` 在读数上读作「这次调用没有这一格」，不是一个
    /// 解析错误。所以读不出来的整个调用返回 `None`，由调用方如实说「读不回来」。
    pub fn from_attributes(
        call_id: String,
        actor: String,
        attributes: &serde_json::Value,
        at: DateTime<Utc>,
    ) -> Option<Self> {
        let text = |key: &str| {
            attributes
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        let number = |key: &str| attributes.get(key).and_then(serde_json::Value::as_u64);
        Some(Self {
            call_id,
            upstream: text("upstream")?,
            api_style: text("api_style")?,
            model: text("model")?,
            result: text("result")?,
            input_tokens: number("input_tokens")?,
            output_tokens: number("output_tokens")?,
            cached_tokens: number("cached_tokens")?,
            cache_created_tokens: number("cache_created_tokens"),
            origin: text("origin")?,
            owner: text("owner"),
            latency_ms: number("latency_ms"),
            task_id: text("task_id"),
            actor,
            cost_micros: attributes
                .get("cost_micros")
                .and_then(serde_json::Value::as_i64),
            price_book: text("price_book"),
            batch_id: text("batch_id"),
            session: text("session"),
            pace: text("pace"),
            tokens_reasoning: number("tokens_reasoning"),
            refusal_kind: text("refusal_kind"),
            retry_after_secs: number("retry_after_secs"),
            usage_outcome: text("usage_outcome"),
            estimated_input: number("estimated_input"),
            refused_scope: text("refused_scope"),
            finish_reason: text("finish_reason"),
            ttft_ms: number("ttft_ms"),
            prompt_manifest: text("prompt_manifest"),
            prompt_dropped: number("prompt_dropped").map(|n| n as u32),
            at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> LlmCall {
        LlmCall {
            call_id: "call-1".into(),
            upstream: "anthropic".into(),
            api_style: "anthropic".into(),
            model: "claude".into(),
            result: "ok".into(),
            input_tokens: 120,
            output_tokens: 30,
            cached_tokens: 80,
            cache_created_tokens: Some(40),
            origin: "interactive".into(),
            owner: Some("alice".into()),
            latency_ms: Some(900),
            task_id: Some("t-1".into()),
            actor: "employee-1".into(),
            cost_micros: Some(-12),
            price_book: Some("default@USD".into()),
            batch_id: Some("t-1".into()),
            session: Some("s-1".into()),
            pace: Some("{\"spent\":1}".into()),
            tokens_reasoning: None,
            refusal_kind: None,
            retry_after_secs: None,
            usage_outcome: None,
            estimated_input: None,
            refused_scope: None,
            finish_reason: None,
            ttft_ms: None,
            prompt_manifest: None,
            prompt_dropped: None,
            at: Utc::now(),
        }
    }

    /// 写下去的那份载荷读得回来，逐格相同：读侧不必再拼一份键名表。
    #[test]
    fn a_written_call_reads_back_the_same() {
        let original = call();
        let back = LlmCall::from_attributes(
            original.call_id.clone(),
            original.actor.clone(),
            &original.attributes(),
            original.at,
        )
        .expect("这一份载荷该读得回来");
        assert_eq!(back, original);
    }

    /// 缺一格必填就是读不回来——返回 `None`，不拿默认值把它补成一次正常的调用。
    #[test]
    fn a_payload_missing_a_required_cell_is_not_a_call() {
        let mut attributes = call().attributes();
        attributes.as_object_mut().unwrap().remove("model");
        assert!(
            LlmCall::from_attributes("c".into(), "a".into(), &attributes, Utc::now()).is_none()
        );
    }
}
