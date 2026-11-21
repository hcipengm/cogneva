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
    /// 写入口取自己的钟填进来的时刻。
    pub at: DateTime<Utc>,
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
        })
    }
}

/// 往事件流里追加一条调用事实。
///
/// 实现必须**幂等**：同一个 `call_id` 追加两次，账上只有一条。写不进去要如实报错，
/// 不许把失败咽下去——调用方按这条错误记「这一笔没进账」，而不是把它读成成功。
#[async_trait]
pub trait LlmCallEvents: Send + Sync {
    /// 这条事实属于哪个写者（部署名），与幂等键一起构成唯一约束。
    fn writer(&self) -> &str;

    async fn append(&self, call: &LlmCall) -> SFResult<()>;
}
