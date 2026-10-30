use crate::{
    AssistantMessageEvent, ContentBlock, EventStream, EventStreamProducer, Message, RawLogger,
    SFError, SFResult, StopReason, ToolDefinition,
};
use async_trait::async_trait;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub type AssistantMessageEventStream = EventStream<AssistantMessageEvent, ChatResponse>;
pub type AssistantMessageEventProducer = EventStreamProducer<AssistantMessageEvent, ChatResponse>;

/// Inbound header carrying the calling component identity through the LLM
/// proxy. The gateway turns it into the bounded `actor` dimension on token
/// metrics so per-actor consumption (e.g. self-review) is observable.
pub const LLM_ACTOR_HEADER: &str = "x-cogneva-actor";

/// Inbound header carrying the environment a call was made from, alongside
/// [`LLM_ACTOR_HEADER`] on the same proxied request.
///
/// The two headers answer different questions and are read side by side:
/// `actor` says which component burned the tokens, this one says where it ran,
/// and the same component runs in more than one environment, so neither can be
/// derived from the other.
///
/// The closed set of values lives with the reader that normalises them; the
/// constant lives here because every crate that *makes* a call declares this
/// header, and those crates depend on the contract layer, not on the reader.
/// A writer that could not name the header would have to spell the string
/// itself, and a second spelling is a second value.
pub const LLM_ORIGIN_HEADER: &str = "x-cogneva-origin";

/// Inbound header carrying the identity of the unit of work a call is spent
/// for, alongside the actor and origin headers on the same proxied request.
///
/// The other two axes answer "which component" and "which environment"; neither
/// answers "for which job". A component runs many jobs in one environment, and
/// spend is judged per job -- was this change worth its tokens, what does one
/// task cost -- so the caller has to say which job, because only it knows. The
/// value is the same identity the caller reports its own per-run totals under,
/// which is what lets the two be reconciled instead of trusted separately.
///
/// Absent is a supported state and means "not attributed to a job"; it must not
/// be defaulted to a placeholder, because a placeholder would become the group
/// every unattributed call lands in and would read as one very expensive job.
pub const LLM_TASK_HEADER: &str = "x-cogneva-task";

/// Inbound header carrying the person a call's spend is charged to.
///
/// The three axes already on this request answer which component, which
/// environment and which job; none of them answers "whose budget is this".
/// A person's daily ceiling can only be enforced against the last one, and a
/// caller's own actor name is not it: every WebUI conversation is `web_chat`.
///
/// The value has to be an identity this platform authenticated, not a name the
/// caller chose. A ceiling a caller can rename itself out of is not a ceiling,
/// which is why the only writer today is the gateway's own WebUI chat, reading
/// the id out of the websocket's verified token.
///
/// Absent is a supported state and means "not attributed to a person" -- most
/// callers are components with no human behind them. It must not be defaulted
/// to a placeholder: that placeholder would become the group every
/// unattributed call lands in, and a ceiling set on it would silently cap the
/// whole platform.
pub const LLM_OWNER_HEADER: &str = "x-cogneva-owner";

/// The standard header an upstream uses to say when it can serve us again.
pub const RETRY_AFTER_HEADER: &str = "retry-after";

/// The wait an upstream asked for, in seconds, read off [`RETRY_AFTER_HEADER`].
///
/// Both legal forms are accepted — a bare number of seconds, and an HTTP-date —
/// because upstreams use both. A value that parses as neither is `None`: an
/// unreadable hint must not be guessed at, and a wrong wait is worse than no
/// wait at all, since it aims the next attempt at a moment the upstream never
/// named.
///
/// The date form is resolved against the local clock, so a skewed clock shifts
/// the answer; a past date yields zero, which is the honest reading of "already
/// due" and leaves the policy delay in charge.
pub fn parse_retry_after_secs(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs);
    }
    DateTime::parse_from_rfc2822(raw)
        .ok()
        .map(|t| (t.timestamp() - chrono::Utc::now().timestamp()).max(0) as u64)
}

/// [`parse_retry_after_secs`] applied to a response's headers.
///
/// Field names are compared case-insensitively: HTTP header names are, and the
/// concrete client's map preserves whatever casing the wire carried.
pub fn retry_after_hint(headers: &HashMap<String, String>) -> Option<u64> {
    crate::header_value(headers, RETRY_AFTER_HEADER).and_then(parse_retry_after_secs)
}

/// Outbound header the LLM proxy sets on a refusal to name the cause it
/// classified the refusal under.
///
/// The status code alone is lossy for a proxy fronting a pool: a spent
/// allowance and a rejected credential both arrive as 401/403, and a
/// subscription the provider has stopped honouring arrives as 400 — the same
/// shape as a request of ours that is malformed. The proxy holds the upstream's
/// own statement about when it will serve again, which the caller cannot see, so
/// it declares the cause here and the caller prefers it over the status.
///
/// A value that names no cause in [`UpstreamFailure`]'s closed set is ignored,
/// so an unknown token reads as "nothing was declared" and the caller falls back
/// to the status rather than guessing.
pub const UPSTREAM_CAUSE_HEADER: &str = "x-cogneva-upstream-cause";

/// The cause a refusal declared on [`UPSTREAM_CAUSE_HEADER`], or `None` when the
/// header is absent or names no cause.
///
/// Header names are compared case-insensitively for the same reason
/// [`retry_after_hint`] does: the concrete client's map preserves whatever
/// casing the wire carried.
pub fn declared_upstream_cause(headers: &HashMap<String, String>) -> Option<UpstreamFailure> {
    crate::header_value(headers, UPSTREAM_CAUSE_HEADER)
        .and_then(|value| UpstreamFailure::from_name(value.trim()))
}

/// The token a rendered refusal carries its stated wait under, and the reader
/// for it.
///
/// The wait starts life as a measurement beside a typed cause, and the cause
/// has a reader for the prose it is rendered into ([`UpstreamFailure::named_in`])
/// because the layers between the transport and the retry decision flatten the
/// error into a string. The wait needs the same reader for the same reason: a
/// stated wait that reaches the retry decision as `None` lets the policy's own
/// backoff govern, and a policy backoff of seconds against an upstream that
/// asked for minutes spends an entire retry budget inside one outage.
///
/// A reader that recovers the number from prose cannot trust its magnitude the
/// way a typed field can, so what it reads is capped: a number that came out of
/// free text must not be able to park a task indefinitely. The cap is a ceiling
/// on the wait, not a judgement about the failure — the policy delay still wins
/// when it is longer.
pub const RETRY_AFTER_MARKER: &str = "[retry_after_secs=";

/// The longest wait a reader will accept out of rendered text. Matches the
/// ceiling the gateway's own circuit breaker puts on the interval it advertises,
/// so a recovered wait cannot outlast the source that produced it.
// cap: derived — 网关自己的熔断器给它公布的间隔也封在同一高度，所以恢复出来的等待不会长过产生它的源头
pub const MAX_RECOVERED_RETRY_AFTER_SECS: u64 = 6 * 60 * 60;

/// Render `secs` as the token [`retry_after_hint_in`] reads back.
pub fn render_retry_after(secs: u64) -> String {
    format!("{RETRY_AFTER_MARKER}{secs}]")
}

/// The wait a rendered refusal states, when the text carries one.
///
/// Read from the last occurrence: the token is appended by whoever rendered the
/// refusal, so anything that wraps the text again puts more prose after it, and
/// a reason that itself quotes an earlier rendering must not shadow the outer
/// one. Absent means the upstream never named a wait, which is not zero — a
/// caller must fall back to its own policy rather than to "no delay".
pub fn retry_after_hint_in(text: &str) -> Option<u64> {
    let rest = &text[text.rfind(RETRY_AFTER_MARKER)? + RETRY_AFTER_MARKER.len()..];
    let (digits, _) = rest.split_once(']')?;
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .map(|secs| secs.min(MAX_RECOVERED_RETRY_AFTER_SECS))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    #[default]
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheRetention {
    #[default]
    None,
    Short,
    Long,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Transport {
    #[default]
    Auto,
    Sse,
    Websocket,
}

#[derive(Debug, Clone, Default)]
pub struct LlmModelInfo {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub base_url: String,
}

#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct ChatOptions {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub tools: Option<Vec<ToolDefinition>>,
    pub response_format: ResponseFormat,
    pub reasoning: Option<ThinkingLevel>,
    pub api_key: Option<String>,
    pub session_id: Option<String>,
    pub headers: HashMap<String, String>,
    pub on_payload: Option<
        Arc<dyn Fn(serde_json::Value, &LlmModelInfo) -> SFResult<serde_json::Value> + Send + Sync>,
    >,
    pub transport: Transport,
    pub cache_retention: CacheRetention,
    pub max_retry_delay_ms: Option<u64>,
    pub metadata: HashMap<String, String>,
    pub abort_signal: Option<Arc<AtomicBool>>,
    pub raw_logger: Option<Arc<dyn RawLogger>>,
}

impl std::fmt::Debug for ChatOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatOptions")
            .field("model", &self.model)
            .field("temperature", &self.temperature)
            .field("max_tokens", &self.max_tokens)
            .field("tools", &self.tools.as_ref().map(|t| t.len()))
            .field("response_format", &self.response_format)
            .field("reasoning", &self.reasoning)
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("session_id", &self.session_id)
            .field("headers", &self.headers)
            .field("on_payload", &self.on_payload.as_ref().map(|_| "[closure]"))
            .field("transport", &self.transport)
            .field("cache_retention", &self.cache_retention)
            .field("max_retry_delay_ms", &self.max_retry_delay_ms)
            .field("metadata", &self.metadata)
            .field("abort_signal", &self.abort_signal)
            .field("raw_logger", &self.raw_logger.as_ref().map(|_| "[logger]"))
            .finish()
    }
}

impl ChatOptions {
    /// Tag every outbound HTTP request of this call with the calling component
    /// identity (e.g. "self_review", "agent:generator") for token attribution.
    pub fn with_actor(mut self, actor: &str) -> Self {
        self.headers
            .insert(LLM_ACTOR_HEADER.to_string(), actor.to_string());
        self
    }

    /// Declare the environment this call is made from, overriding whatever the
    /// process declared for its calls in general.
    ///
    /// Most callers never need this: the environment a process runs in is a
    /// property of where it is deployed, so the transport layer states it once
    /// for every call the process makes. It is here for the process that hosts
    /// more than one environment — a control plane answering a human and
    /// driving its own pool is one process with two answers.
    pub fn with_origin(mut self, origin: &str) -> Self {
        self.headers
            .insert(LLM_ORIGIN_HEADER.to_string(), origin.to_string());
        self
    }

    /// Declare the unit of work this call is spent for.
    ///
    /// Unlike the environment, this is a property of the call rather than of the
    /// process: one process runs many jobs one after another, so the caller
    /// states it per run and a run's calls carry the same value. Blank is
    /// treated as undeclared -- a task id that names nothing must not become a
    /// group of its own on the ledger.
    pub fn with_task(mut self, task: &str) -> Self {
        let task = task.trim();
        if !task.is_empty() {
            self.headers
                .insert(LLM_TASK_HEADER.to_string(), task.to_string());
        }
        self
    }

    /// Declare the person this call is spent for.
    ///
    /// Also a property of the call rather than of the process, and also blank-
    /// tolerant: the person's daily ceiling is judged on this value, so an
    /// empty one must not become a bucket with a ceiling of its own. Only a
    /// caller that authenticated a human can state it -- a component naming one
    /// would be attributing its own spend to somebody who did not ask for it.
    pub fn with_owner(mut self, owner: &str) -> Self {
        let owner = owner.trim();
        if !owner.is_empty() {
            self.headers
                .insert(LLM_OWNER_HEADER.to_string(), owner.to_string());
        }
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum ResponseFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, Default)]
pub struct CompleteOptions {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub api_key: Option<String>,
}

/// Why an upstream call was refused, taken from the transport signal (an HTTP
/// status) rather than from the provider's own prose.
///
/// Every consumer that has to decide something about a failure — failover,
/// per-intent backoff, pausing — reads this instead of pattern-matching the
/// error text. Matching on text is how a quota outage gets recorded as a
/// content defect: the words differ per upstream and per locale, while the
/// status code does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamFailure {
    /// The upstream asked us to slow down. Retrying after the advertised delay
    /// can succeed on its own.
    RateLimited,
    /// The allowance is spent or payment is required. No retry helps until an
    /// external window resets or billing changes.
    QuotaExhausted,
    /// The upstream rejected our credentials. No retry helps until the key
    /// changes.
    Auth,
    /// The upstream itself faulted. Transient by nature.
    ServerError,
    /// The upstream rejected the request as malformed. This is our request
    /// being wrong, not the environment — it must never be reported as an
    /// environment failure.
    BadRequest,
    /// No HTTP status was obtained at all (connection, DNS, timeout).
    Transport,
}

impl UpstreamFailure {
    /// Translate a status code into the cause. Codes outside the recognised
    /// set are treated as a request-side fault, which keeps an unknown
    /// condition from being credited to the environment.
    pub fn from_status(status: u16) -> Self {
        match status {
            429 => Self::RateLimited,
            402 => Self::QuotaExhausted,
            401 | 403 => Self::Auth,
            400 | 404 | 405 | 415 | 422 => Self::BadRequest,
            500..=599 => Self::ServerError,
            _ => Self::BadRequest,
        }
    }

    /// Whether re-issuing the identical request could succeed with no external
    /// change. Quota and credentials only move when a window resets or a key is
    /// replaced, so retrying those on a timer buys nothing.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::QuotaExhausted | Self::Auth)
    }

    /// Whether the environment failed to serve us, as opposed to the request
    /// being malformed. A malformed request is a defect on our side and must be
    /// reported as one.
    pub fn is_environment_failure(self) -> bool {
        !matches!(self, Self::BadRequest)
    }

    /// Every cause, in the order their names render. The list and [`Self::name`]
    /// are two views of one closed set; a reader that recovers a cause from a
    /// rendered reason walks the list rather than re-typing the names, and
    /// `every_cause_reads_back_from_its_own_name` pairs them.
    pub const ALL: [Self; 6] = [
        Self::RateLimited,
        Self::QuotaExhausted,
        Self::Auth,
        Self::ServerError,
        Self::BadRequest,
        Self::Transport,
    ];

    /// The name this cause renders under and [`Self::from_name`] reads back.
    /// Single source: `Display` and the parser both go through it, so a name
    /// cannot mean one variant on the wire and another to a reader.
    pub fn name(self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::QuotaExhausted => "quota_exhausted",
            Self::Auth => "auth_rejected",
            Self::ServerError => "server_error",
            Self::BadRequest => "bad_request",
            Self::Transport => "transport",
        }
    }

    /// The cause rendered under `name`, or `None` when `name` is not one of
    /// them.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|cause| cause.name() == name)
    }

    /// The cause a rendered upstream refusal names, when `text` carries one.
    ///
    /// The refusal travels as prose once an error type has rendered it —
    /// `SFError::Upstream` writes `LLM upstream refused (<name>): <reason>` —
    /// but the name inside the parentheses is this crate's own closed
    /// vocabulary, not the provider's. A reader one or more crates away from
    /// the transport can therefore recover the typed cause instead of
    /// pattern-matching the upstream's own wording, which differs per provider
    /// and per locale. `every_cause_reads_back_from_its_own_name` pins this
    /// reader to that rendering.
    pub fn named_in(text: &str) -> Option<Self> {
        let (_, rest) = text.split_once("LLM upstream refused (")?;
        let (name, _) = rest.split_once(')')?;
        Self::from_name(name)
    }
}

impl std::fmt::Display for UpstreamFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Serialised through [`UpstreamFailure::name`] rather than by deriving from the
/// variant names.
///
/// The derive produced `auth` where every rendered reason, the cause header and
/// the reader that recovers a cause from prose all say `auth_rejected`. Nothing
/// compared the two, so the same cause could leave one surface under one word
/// and be looked for on the other under another — a mismatch that reads as
/// "nobody declared a cause" rather than as a disagreement.
impl Serialize for UpstreamFailure {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

/// The word this cause was persisted under before [`UpstreamFailure::name`] became
/// the wire form, when it differs.
///
/// Tasks stored by an older build carry `error_cause` as the variant's own
/// spelling, and the reader loads every row of a workspace in one pass: refusing
/// one of them fails the whole read, not just that row. So a legacy word still
/// parses — and is never written again, because the writer only ever emits
/// [`UpstreamFailure::name`]. There is exactly one such word, and it is listed
/// here rather than guessed at parse time.
fn from_legacy_name(name: &str) -> Option<UpstreamFailure> {
    match name {
        "auth" => Some(UpstreamFailure::Auth),
        _ => None,
    }
}

/// A word outside [`UpstreamFailure::ALL`] (and the legacy spelling above) is an
/// error rather than a default: an unrecognised cause and a request-side fault
/// mean different things, and folding one into the other would report a
/// vocabulary mismatch as our bug.
impl<'de> Deserialize<'de> for UpstreamFailure {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Self::from_name(&name)
            .or_else(|| from_legacy_name(&name))
            .ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "unknown upstream cause {name:?}; declared names are {:?}",
                    UpstreamFailure::ALL.map(|cause| cause.name())
                ))
            })
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ChatResponse {
    pub content: Vec<ContentBlock>,
    pub api: String,
    pub provider: String,
    pub model: String,
    pub response_id: Option<String>,
    pub usage: Usage,
    pub stop_reason: StopReason,
    pub error_message: Option<String>,
    /// Typed cause of [`Self::error_message`], when the transport supplied a
    /// signal to derive it from. `None` means the failure was described in
    /// prose only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_failure: Option<UpstreamFailure>,
    /// How long the upstream said to wait before trying again, when the refusal
    /// carried a [`RETRY_AFTER_HEADER`].
    ///
    /// Separate from [`Self::upstream_failure`] because it is a measurement, not
    /// a cause: the cause is a closed enum consumers match on, while this is
    /// present only when the upstream volunteered it. A caller that retries must
    /// let the longer of this and its own policy delay win.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<u64>,
    pub timestamp: DateTime<chrono::Utc>,
}

/// Token/cost accounting for one LLM response.
///
/// `Deserialize` is here for the callers that carry a measured usage back across a
/// process boundary — an evaluation rig collecting a scaffold's output compares
/// cells by what was actually spent, and a re-hydrated [`crate::Usage`] must mean
/// the same thing as the one the provider produced.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct Usage {
    pub input: u32,
    pub output: u32,
    pub cache_read: u32,
    pub cache_write: u32,
    pub total_tokens: u32,
    pub cost: crate::message::Cost,
}

/// What one attempt ended as, as far as the usage ledger is concerned.
///
/// The ledger keeps this in a `result` column and every reader predicates on
/// one of the four words: the window readers sum the served rows, the failure
/// readers count everything else, and the rollups split them. The spellings
/// therefore have to agree between two crates that cannot see each other -- the
/// gateway writes the rows, the observability crate queries them -- so the set
/// lives here rather than beside either one.
///
/// Four of the six are refusals we made, not answers the upstream gave.
/// `refused_by_circuit` means no upstream was even tried (the pool was shut),
/// `refused_by_pace` means the window's allowance was spent, and
/// `refused_by_budget` means the unit of work spent the ceiling someone
/// declared for it. Recording any of them as `error` would read as "the
/// upstream answered wrong" and point the repair at the wrong place -- and each
/// of the three has a different repair, which is why they are three words and
/// not one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageResult {
    /// The call was served.
    Ok,
    /// An upstream was tried and the call did not complete.
    Error,
    /// The pool circuit refused before any upstream was selected.
    RefusedByCircuit,
    /// The window pacer refused before the request was sent.
    RefusedByPace,
    /// 这件工作自己那份当日预算用完了，请求没发出去。
    ///
    /// 与上一条分开：配速拒的是「这一窗摊到此刻不够再开一件」，等窗口翻页会自己好；
    /// 这一条拒的是「有人给这件任务声明了上限，它用完了」，等的不是窗口而是那个上限
    /// ——而它多半该被读成「这件工作该收尾了」，不是「晚点再来」。两者合成一个词，
    /// 修的人会去查窗口，而那窗口一点问题都没有。
    RefusedByBudget,
    /// 这个人今天的额度用完了，请求没发出去。
    ///
    /// 与上一条分开，理由同上一条与配速分开的理由：上一条的上限挂在「这件工作」
    /// 上，修法是收尾这件工作或抬它的上限；这一条的上限挂在「这个人今天能花
    /// 多少」上，修法是等日界翻页、或由属主调整这个人的额度。两者合成一个词，
    /// 读的人会去找错的那个人问。
    RefusedByOwnerBudget,
}

impl UsageResult {
    /// Every value, in the order a reader lists them.
    ///
    /// The ledger's partial indexes and predicates name these words. A query
    /// that spells one itself agrees with this enum only until a variant is
    /// renamed, and then it matches no rows -- which reads exactly like a
    /// ledger with nothing in it.
    pub const ALL: [UsageResult; 6] = [
        UsageResult::Ok,
        UsageResult::Error,
        UsageResult::RefusedByCircuit,
        UsageResult::RefusedByPace,
        UsageResult::RefusedByBudget,
        UsageResult::RefusedByOwnerBudget,
    ];

    /// The word this outcome is stored under.
    pub fn as_str(self) -> &'static str {
        match self {
            UsageResult::Ok => "ok",
            UsageResult::Error => "error",
            UsageResult::RefusedByCircuit => "refused_by_circuit",
            UsageResult::RefusedByPace => "refused_by_pace",
            UsageResult::RefusedByBudget => "refused_by_budget",
            UsageResult::RefusedByOwnerBudget => "refused_by_owner_budget",
        }
    }

    /// Read one back. A value outside the set is refused rather than mapped to
    /// a neighbour: an unknown word means the row was written by a build that
    /// disagrees with this one.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|outcome| outcome.as_str() == s)
    }
}

impl std::str::FromStr for UsageResult {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s).ok_or_else(|| format!("unknown usage result: {s}"))
    }
}

/// 一次拒绝的成因，从上游的正文里读出来。
///
/// 它落在台账的 `refusal_kind` 列上，而那一列有一个部分索引与一条读数按
/// `quota_cycle` 取行——写它的与读它的不在同一个 crate，所以这个闭集摆在两者都
/// 依赖的这一层，而不是摆在写它的那一侧。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalKind {
    /// 配额／用量类：窗口走完会自己好。
    QuotaCycle,
    /// 资格类：这台账号对上游没有使用权。等不会让它好。
    Entitlement,
    /// 有拒绝，但正文里读不出成因。
    Unclassified,
}

impl RefusalKind {
    /// 闭集里的全部取值。发布时**每一拍按这三种各发一条**，当前那一种 1、其余 0：
    /// 只发当前那一种，上一拍那种会永远停在 1 上，而「这条上游是资格类」是一个
    /// 会结束的状态。
    pub const ALL: [RefusalKind; 3] = [Self::QuotaCycle, Self::Entitlement, Self::Unclassified];

    /// 这个成因存进台账时用的那个词。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QuotaCycle => "quota_cycle",
            Self::Entitlement => "entitlement",
            Self::Unclassified => "unclassified",
        }
    }

    /// 从它自己发布出去的名字读回来（跨进程载荷里存的就是这个名字）。
    ///
    /// 认不出的一律 `None`，不落到 `Unclassified`：认不出说明写它的那一代与读它的
    /// 这一代对不齐，那是"这一格没有值"，而 `Unclassified` 是"上游写了话但我们没
    /// 读懂"——把前者写成后者，会让一次版本错位看起来像上游回答得含糊。
    pub fn from_name(name: &str) -> Option<Self> {
        if name.is_empty() {
            return None;
        }
        Self::ALL.into_iter().find(|k| k.as_str() == name)
    }
}

impl std::str::FromStr for RefusalKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_name(s).ok_or_else(|| format!("unknown refusal kind: {s}"))
    }
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream>;

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream>;

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse>;

    async fn health_check(&self) -> bool;
}

/// Execute an LLM request that expects a structured JSON response matching
/// the given JSON Schema.
/// The function injects a system prompt instructing the model to produce valid
/// JSON conforming to the schema, validates the response, and returns a parsed
/// value.
/// # Example
/// ```
/// use serde::Deserialize;
/// use schemars::JsonSchema;
/// use cog_core::{LlmClient, SFResult, Message, ChatOptions, execute_structured};
/// #[derive(Debug, Deserialize, JsonSchema)]
/// struct DecomposeResult {
///     tasks: Vec<String>,
/// }
/// # async fn example(provider: &dyn LlmClient) -> SFResult<()> {
/// let result: DecomposeResult = execute_structured(
///     provider,
///     &[Message::user("Break down: deploy a service")],
///     &ChatOptions::default(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn execute_structured<T>(
    provider: &dyn LlmClient,
    messages: &[Message],
    options: &ChatOptions,
) -> SFResult<T>
where
    T: schemars::JsonSchema + serde::de::DeserializeOwned,
{
    let root_schema = schemars::gen::SchemaGenerator::default().root_schema_for::<T>();
    let schema_json = serde_json::to_string_pretty(&root_schema).map_err(SFError::Serialization)?;

    let system_msg = Message::system(format!(
        "Respond with valid JSON conforming to this JSON Schema. \
         Do not output markdown fences or explanatory text.\n\n{schema_json}"
    ));

    let mut msgs = vec![system_msg];
    msgs.extend_from_slice(messages);

    let mut opts = options.clone();
    opts.response_format = ResponseFormat::Json;

    let response = provider.chat(&msgs, &opts).await?;
    // A backend that could not serve the request still arrives as `Ok`: the
    // reason travels in `error_message` with `stop_reason: Error`, and the
    // content is empty. Parsing that empty content reports a JSON syntax error
    // and drops the cause, so an unreachable upstream reads as "the model
    // emitted garbage" — a defect in the model rather than in the environment.
    // Callers classifying on the error text act on the difference, so the
    // provider's own reason is what has to come out of here.
    if response.error_message.is_some() || response.stop_reason == StopReason::Error {
        return Err(SFError::LLM(response.error_message.unwrap_or_else(|| {
            "provider reported an error without a message".into()
        })));
    }
    let text = response
        .content
        .iter()
        .filter_map(|b| b.as_text())
        .collect::<String>();

    // No text and no error signal is not a malformed answer: it is no answer.
    // Parsing it reports a JSON syntax error at column 0, which reads as "the
    // model emitted garbage" — a defect in the model rather than an upstream
    // that returned nothing. The variants carry that distinction, so the empty
    // body has to take the one meant for a call the environment did not serve.
    if text.trim().is_empty() {
        return Err(SFError::LLM(format!(
            "provider answered with no content and no error signal (stop_reason={:?})",
            response.stop_reason
        )));
    }

    let value: serde_json::Value = serde_json::from_str(&text).map_err(SFError::Serialization)?;

    let schema_value = serde_json::to_value(&root_schema).map_err(SFError::Serialization)?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|e| SFError::Validation(format!("invalid schema: {e}")))?;

    let errors: Vec<String> = validator
        .iter_errors(&value)
        .map(|e| e.to_string())
        .collect();
    if !errors.is_empty() {
        return Err(SFError::Validation(format!(
            "JSON schema validation failed: {}",
            errors.join("; ")
        )));
    }

    let parsed: T = serde_json::from_value(value).map_err(SFError::Serialization)?;
    Ok(parsed)
}

// ─── Protocol faces ────────────────────────────────────────────────────────
//
// Three words name the wire shapes this platform speaks: the chat-completions
// shape, the Anthropic messages shape, and the newer OpenAI responses shape. A
// backend config, a pool entry and an inbound request each carry one of these
// words, and the dispatcher picks a provider by comparing them — so they are
// vocabulary two crates have to agree on, not a detail of the crate that
// happens to implement the providers. Spelled out separately, one renamed copy
// turns a declared face into one nothing recognizes and the entry falls through
// to whichever branch is the catch-all.

/// The `api_style` word for the chat-completions face.
pub const OPENAI_API_STYLE: &str = "openai";
/// The `api_style` word for the Anthropic messages face.
pub const ANTHROPIC_API_STYLE: &str = "anthropic";
/// The `api_style` word for the OpenAI responses face.
pub const RESPONSES_API_STYLE: &str = "openai-responses";

/// Whether a declared style belongs to the OpenAI protocol family.
///
/// The family is what keeps a vendor that answers `/responses` from dropping out
/// of the chat-completions face: serving the newer shape says nothing about
/// having stopped serving the older one, and a pool entry is listed under the
/// face it was *verified* on, not the only face it can serve.
pub fn is_openai_family(style: &str) -> bool {
    style == OPENAI_API_STYLE || style == RESPONSES_API_STYLE
}

/// The endpoint a base URL names on the chat-completions face.
///
/// The platform appends exactly one path element, so the base carries
/// everything before it, version segment included (`…/v1`,
/// `…/api/paas/v4`, `…/compatible-mode/v1`). A base that stops short of the
/// version segment composes an address the vendor does not serve.
pub fn chat_completions_url(base_url: &str) -> String {
    format!("{}/chat/completions", base_url.trim_end_matches('/'))
}

/// The endpoint a base URL names on the Anthropic messages face.
pub fn messages_url(base_url: &str) -> String {
    format!("{}/messages", base_url.trim_end_matches('/'))
}

/// The endpoint a base URL names on the responses face.
pub fn responses_url(base_url: &str) -> String {
    format!("{}/responses", base_url.trim_end_matches('/'))
}

/// The reason recorded when an inbound request names a face no member of the
/// pool serves.
pub const FACE_WITHOUT_A_MEMBER: &str = "face_has_no_serving_member";

/// 拒绝的成因：这件工作自己那份当日预算用完了。
///
/// 与 [`FACE_WITHOUT_A_MEMBER`] 同一类东西——写在响应正文里、给调用侧的故障转移与
/// 告警规则读的那一格。同一个 429 也来自配速，两者靠这一格分开：配速说「池子紧，
/// 等一等」，这一条说「这件工作该收尾了」。写成常量而不是两处字面量，是因为写它的
/// （网关）与读它的（告警规则、WebUI）不在同一个 crate 里。
pub const REFUSED_BY_BUDGET: &str = "task_budget_spent";

/// 拒绝的成因：这个人今天的额度用完了。
///
/// 与 [`REFUSED_BY_BUDGET`] 同一类东西、同一个理由要分开：那一条的修法是收尾这件
/// 工作，这一条的修法是等日界或由属主调整这个人的额度。两句话要问的不是同一个人。
pub const REFUSED_BY_OWNER_BUDGET: &str = "owner_budget_spent";

/// Which field names the token ceiling for an upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxTokensField {
    /// The newer spelling, which reasoning models insist on.
    #[default]
    MaxCompletionTokens,
    /// The original spelling, which older compatible vendors only accept.
    MaxTokens,
}

/// What the platform believes about one upstream, as far as request shaping is
/// concerned.
///
/// Narrower than the full compatibility image on purpose: this is the part two
/// crates have to agree on — the gateway, which rewrites an outbound body, and
/// the provider crate, which reads the same document on its own path. A field
/// that only one side acts on stays with that side.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderCompat {
    /// Whether the upstream supports the `store` field.
    pub supports_store: bool,
    /// Whether the upstream supports `reasoning_effort`.
    pub supports_reasoning_effort: bool,
    /// Whether the upstream supports `stream_options: { include_usage: true }`.
    pub supports_usage_in_streaming: bool,
    /// Whether the upstream supports the `strict` field on tool definitions.
    pub supports_strict_mode: bool,
    /// Whether the upstream only accepts `temperature = 1.0`.
    pub requires_temperature_one: bool,
    /// Which field names the token ceiling.
    pub max_tokens_field: MaxTokensField,
}

impl ProviderCompat {
    /// The image for an upstream nobody registered: it is told the newest field
    /// shapes and nothing is stripped from its requests.
    ///
    /// This is the direction the fallback has to fall: an upstream that answers
    /// a shape it does not know with a 400 is a request rejected before its
    /// first byte, and the caller reads it as this platform refusing them. An
    /// upstream that quietly ignores a field loses nothing.
    pub fn permissive() -> Self {
        Self {
            supports_store: true,
            supports_reasoning_effort: true,
            supports_usage_in_streaming: true,
            supports_strict_mode: true,
            requires_temperature_one: false,
            max_tokens_field: MaxTokensField::MaxCompletionTokens,
        }
    }
}

/// The compatibility image of an upstream, looked up by the address it is
/// reached at.
///
/// The table itself is vendor knowledge and stays with the crate that owns the
/// providers; what crosses is the question ("what do we believe about this
/// address") and the answer's shape, so a caller can shape a request without
/// linking the provider implementations.
pub trait ProviderCompatTable: Send + Sync {
    /// What we believe about the upstream at `base_url`.
    fn detect(&self, base_url: &str) -> ProviderCompat;
}

#[cfg(test)]
mod upstream_failure_name_tests {
    use super::*;

    /// The rendered name and the parser are one vocabulary: every cause must
    /// read back from the exact string the transport writes. A name that
    /// renders but does not parse would leave a reader one crate away treating
    /// a typed refusal as prose.
    #[test]
    fn every_cause_reads_back_from_its_own_name() {
        for cause in UpstreamFailure::ALL {
            assert_eq!(UpstreamFailure::from_name(cause.name()), Some(cause));
            assert_eq!(cause.to_string(), cause.name());
        }
    }

    /// The wire form is the rendered name, not the variant's own spelling.
    ///
    /// The derived form said `auth` while everything a reader sees says
    /// `auth_rejected`; a consumer that recorded one and looked for the other
    /// found nothing and read that as "no cause was declared".
    #[test]
    fn every_cause_survives_the_wire_under_the_name_it_renders() {
        let mut names = std::collections::BTreeSet::new();
        for cause in UpstreamFailure::ALL {
            assert!(
                names.insert(cause.name()),
                "two causes render under {:?}; a reader cannot tell them apart",
                cause.name()
            );
            let json = serde_json::to_string(&cause).unwrap();
            assert_eq!(json, format!("\"{}\"", cause.name()));
            assert_eq!(
                serde_json::from_str::<UpstreamFailure>(&json).unwrap(),
                cause
            );
        }
        // The legacy spelling still loads — a task row written before this
        // change carries it, and one unreadable row fails the whole workspace
        // read — but it is never written again.
        assert_eq!(
            serde_json::from_str::<UpstreamFailure>("\"auth\"").unwrap(),
            UpstreamFailure::Auth
        );
        assert_ne!(
            serde_json::to_string(&UpstreamFailure::Auth).unwrap(),
            "\"auth\""
        );
        assert_eq!(UpstreamFailure::from_name("auth"), None);
        assert!(serde_json::from_str::<UpstreamFailure>("\"forbidden\"").is_err());
    }

    /// The reader runs against the real error type rather than a hand-built
    /// string: the rendering `named_in` depends on is `SFError::Upstream`'s, so
    /// the pairing has to be proven through it — and through the wrappers the
    /// reason actually travels under.
    #[test]
    fn a_rendered_refusal_carries_its_typed_cause() {
        for cause in UpstreamFailure::ALL {
            let err = SFError::upstream_refused(cause, "detail");
            assert_eq!(UpstreamFailure::named_in(&err.to_string()), Some(cause));
            let nested = SFError::Agent(err.to_string()).to_string();
            assert_eq!(UpstreamFailure::named_in(&nested), Some(cause));
        }
    }

    /// A reason with no rendered refusal is not read as one.
    #[test]
    fn prose_that_names_no_refusal_yields_none() {
        assert_eq!(UpstreamFailure::named_in("upstream is unreachable"), None);
        assert_eq!(UpstreamFailure::named_in(""), None);
        assert_eq!(UpstreamFailure::from_name("nonsense"), None);
    }
}

#[cfg(test)]
mod retry_after_tests {
    use super::*;

    /// 两种合法形态都要认：秒数是常态，HTTP-date 是 RFC 允许的另一种写法。
    #[test]
    fn both_legal_forms_parse() {
        assert_eq!(parse_retry_after_secs("120"), Some(120));
        assert_eq!(parse_retry_after_secs("  30  "), Some(30));
        assert_eq!(
            retry_after_hint(&HashMap::from([("Retry-After".into(), "45".into())])),
            Some(45)
        );
    }

    /// 请求头名大小写不敏感，查表也不能敏感：具体客户端保留线上的原始大小写。
    #[test]
    fn the_header_lookup_ignores_case() {
        for name in ["retry-after", "Retry-After", "RETRY-AFTER"] {
            assert_eq!(
                retry_after_hint(&HashMap::from([(name.into(), "7".into())])),
                Some(7),
                "{name}"
            );
        }
    }

    /// 网关在回复头上声明的分类按名字读回；闭集之外的名字当作没声明，交给状态码
    /// 兜底，不猜。
    #[test]
    fn a_declared_cause_reads_back_and_an_unknown_name_is_no_declaration() {
        let named = |v: &str| HashMap::from([(UPSTREAM_CAUSE_HEADER.to_string(), v.to_string())]);
        assert_eq!(
            declared_upstream_cause(&named("rate_limited")),
            Some(UpstreamFailure::RateLimited)
        );
        assert_eq!(
            declared_upstream_cause(&HashMap::from([(
                UPSTREAM_CAUSE_HEADER.to_uppercase(),
                "quota_exhausted".to_string()
            )])),
            Some(UpstreamFailure::QuotaExhausted)
        );
        assert_eq!(declared_upstream_cause(&named("kinda_slow")), None);
        assert_eq!(declared_upstream_cause(&HashMap::new()), None);
    }

    /// 认不出来就是没有：猜一个不存在的时间点，等于把下一次尝试瞄准上游从没
    /// 说过的时刻。空头、乱码、负数的处理都一样——不带出任何提示。
    #[test]
    fn an_unreadable_hint_is_no_hint() {
        assert_eq!(parse_retry_after_secs(""), None);
        assert_eq!(parse_retry_after_secs("soon"), None);
        assert_eq!(parse_retry_after_secs("-5"), None);
        assert_eq!(retry_after_hint(&HashMap::new()), None);
        assert_eq!(
            retry_after_hint(&HashMap::from([("Retry-After".into(), "n/a".into())])),
            None
        );
    }

    /// 渲染与读回是一对：写出去多少，读回来就是多少，且中间被别的散文包过
    /// 也不影响。
    #[test]
    fn a_rendered_wait_reads_back_through_a_wrapper() {
        let rendered = format!(
            "environment_error: LLM upstream refused (server_error): HTTP 503 {}",
            render_retry_after(282)
        );
        assert_eq!(retry_after_hint_in(&rendered), Some(282));

        // 外面再套一层（PGE 的终止性标记、SFError 的 Agent 包装），读出来的
        // 还是同一个数；反过来，什么都没写就是 None，不是 0。
        assert_eq!(
            retry_after_hint_in(&format!("terminal_env_failure: {rendered}")),
            Some(282)
        );
        assert_eq!(retry_after_hint_in("refused with no stated wait"), None);
        assert_eq!(retry_after_hint_in("[retry_after_secs=]"), None);
        assert_eq!(retry_after_hint_in("[retry_after_secs=soon]"), None);
    }

    /// 出现过两次时取外层那一次：内层可能是原因自己在引用更早的一次渲染，
    /// 而外层是刚刚写上去的。取内层等于拿旧的时刻安排新的尝试。
    #[test]
    fn the_last_rendered_wait_wins() {
        let inner = format!("first {}", render_retry_after(60));
        let outer = format!("{inner} then {}", render_retry_after(282));
        assert_eq!(retry_after_hint_in(&outer), Some(282));
    }

    /// 从散文里读出来的数不能比它可能的来源活得更久：真有天文数字也只按上限
    /// 计，否则一段文本就能把任务停在那里不动。
    #[test]
    fn a_wait_read_out_of_text_is_capped() {
        assert_eq!(
            retry_after_hint_in(&render_retry_after(MAX_RECOVERED_RETRY_AFTER_SECS + 1)),
            Some(MAX_RECOVERED_RETRY_AFTER_SECS)
        );
        assert_eq!(
            retry_after_hint_in(&render_retry_after(90)),
            Some(90),
            "上限只压超出它的那些，正常值原样返回"
        );
    }

    /// 已过去的时间点是"现在就能重试"，不是无穷等待；读数取地板 0，剩下的交给
    /// 调用方的策略退避。
    #[test]
    fn a_past_date_reads_as_due_now() {
        let past = (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc2822();
        assert_eq!(parse_retry_after_secs(&past), Some(0));
    }
}

#[cfg(test)]
mod structured_output_tests {
    use super::*;

    #[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
    struct Probe {
        n: u32,
    }

    /// A provider that answers exactly what the test hands it. The failure
    /// shapes below differ only in what the transport reported, so a stub is
    /// the only way to reach them without an unreachable upstream.
    struct StubProvider {
        response: ChatResponse,
    }

    #[async_trait]
    impl LlmClient for StubProvider {
        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            Ok(self.response.clone())
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            let (stream, mut producer) = EventStream::with_capacity(1);
            producer.end(self.response.clone());
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &CompleteOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            self.chat_stream(&[], &ChatOptions::default()).await
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn answering(text: &str, stop_reason: StopReason, error: Option<&str>) -> ChatResponse {
        ChatResponse {
            content: if text.is_empty() {
                Vec::new()
            } else {
                vec![ContentBlock::Text {
                    text: text.to_string(),
                    text_signature: None,
                }]
            },
            stop_reason,
            error_message: error.map(str::to_string),
            ..Default::default()
        }
    }

    async fn ask(provider: &StubProvider) -> SFResult<Probe> {
        execute_structured::<Probe>(
            provider,
            &[Message::user("count them")],
            &ChatOptions::default(),
        )
        .await
    }

    /// An upstream that could not serve the call reports it in-band, and that
    /// reason is what the caller classifies on. Parsing the empty body instead
    /// reports a JSON syntax error, which puts the failure on the model.
    #[tokio::test]
    async fn a_failed_call_arrives_as_the_upstreams_own_reason() {
        let provider = StubProvider {
            response: answering("", StopReason::Error, Some("upstream returned 503")),
        };
        let err = ask(&provider)
            .await
            .expect_err("a failed call is not an answer");
        assert!(
            matches!(err, SFError::LLM(_)),
            "an unserved call is an LLM failure, got: {err:?}"
        );
        assert!(
            err.to_string().contains("upstream returned 503"),
            "the upstream's own reason has to survive, got: {err}"
        );
    }

    /// A call that produced no text and reported nothing is still not an
    /// answer: nothing was answered. Reading it as a parse failure says the
    /// model emitted garbage, and the two call for different repairs.
    #[tokio::test]
    async fn an_answer_with_no_content_is_not_a_malformed_answer() {
        let provider = StubProvider {
            response: answering("   \n", StopReason::Stop, None),
        };
        let err = ask(&provider)
            .await
            .expect_err("an empty answer is not an answer");
        assert!(
            matches!(err, SFError::LLM(_)),
            "no content is an unserved call, not a malformed one, got: {err:?}"
        );
    }

    /// The control for the stub itself: an ordinary answer still comes back as
    /// the parsed value. Without it the two failure tests would pass on a
    /// provider whose wire path never worked at all.
    #[tokio::test]
    async fn an_answer_that_arrives_parses() {
        let provider = StubProvider {
            response: answering("{\"n\": 7}", StopReason::Stop, None),
        };
        let parsed = ask(&provider).await.expect("a well-formed answer parses");
        assert_eq!(parsed.n, 7);
    }

    /// The control: a body that arrived and is not JSON is the model's own
    /// defect, and must keep reading as one. A blanket "any unparsable body is
    /// an environment failure" would hide it and retry a message that can never
    /// come out right.
    #[tokio::test]
    async fn a_body_that_is_not_json_is_still_the_models_defect() {
        let provider = StubProvider {
            response: answering("not json at all", StopReason::Stop, None),
        };
        let err = ask(&provider)
            .await
            .expect_err("a non-JSON body cannot parse");
        assert!(
            matches!(err, SFError::Serialization(_)),
            "a delivered body that does not parse is a serialization failure, got: {err:?}"
        );
    }
}

#[cfg(test)]
mod task_header_tests {
    use super::*;

    /// The job identity is copied onto the wire unchanged, so the ledger's
    /// column holds the exact string the caller reports its own totals under.
    /// A normalisation here -- lowercase, a prefix, a hash -- would still look
    /// like an id and would join to nothing.
    #[test]
    fn the_declared_task_travels_verbatim() {
        let options = ChatOptions::default().with_task("ralph-pipeline-abc");
        assert_eq!(
            options.headers.get(LLM_TASK_HEADER).map(String::as_str),
            Some("ralph-pipeline-abc")
        );
    }

    /// Blank is undeclared, not a task named "": the ledger treats absent as
    /// "not attributed", and an empty string would be a group of its own.
    #[test]
    fn a_blank_task_declares_nothing() {
        for blank in ["", "   "] {
            let options = ChatOptions::default().with_task(blank);
            assert!(
                !options.headers.contains_key(LLM_TASK_HEADER),
                "a blank task must leave the header off entirely"
            );
        }
    }

    /// Declaring a task must not disturb the axes read beside it.
    #[test]
    fn actor_origin_and_task_coexist() {
        let options = ChatOptions::default()
            .with_actor("agent:planner")
            .with_origin("gateway_pool")
            .with_task("task-1");
        assert_eq!(
            options.headers.get(LLM_ACTOR_HEADER).map(String::as_str),
            Some("agent:planner")
        );
        assert_eq!(
            options.headers.get(LLM_ORIGIN_HEADER).map(String::as_str),
            Some("gateway_pool")
        );
        assert_eq!(
            options.headers.get(LLM_TASK_HEADER).map(String::as_str),
            Some("task-1")
        );
    }
}

#[cfg(test)]
mod owner_header_tests {
    use super::*;

    /// The person's identity is copied onto the wire unchanged: a ceiling is
    /// enforced on this value, so a normalisation here would enforce a ceiling
    /// against an id the management face never wrote.
    #[test]
    fn the_declared_owner_travels_verbatim() {
        let options = ChatOptions::default().with_owner("user-7f3a");
        assert_eq!(
            options.headers.get(LLM_OWNER_HEADER).map(String::as_str),
            Some("user-7f3a")
        );
    }

    /// Blank is undeclared, not a person named "": a placeholder would become
    /// the bucket every unattributed call lands in, and a ceiling set anywhere
    /// near it would silently cap the platform.
    #[test]
    fn a_blank_owner_declares_nothing() {
        for blank in ["", "   "] {
            let options = ChatOptions::default().with_owner(blank);
            assert!(
                !options.headers.contains_key(LLM_OWNER_HEADER),
                "a blank owner must leave the header off entirely"
            );
        }
    }

    /// The person and the job are two answers to two different questions, and
    /// one call carries both: the ceiling on the job and the ceiling on the
    /// person are judged separately against separate limits.
    #[test]
    fn owner_and_task_coexist_without_overwriting_each_other() {
        let options = ChatOptions::default()
            .with_task("task-1")
            .with_owner("user-7f3a");
        assert_eq!(
            options.headers.get(LLM_TASK_HEADER).map(String::as_str),
            Some("task-1")
        );
        assert_eq!(
            options.headers.get(LLM_OWNER_HEADER).map(String::as_str),
            Some("user-7f3a")
        );
    }

    /// Every result word is its own: a reader that selects rows by state has to
    /// be able to tell "this person is out for the day" from "this job is
    /// done", so the two words must not collide.
    #[test]
    fn the_person_ceiling_has_a_word_of_its_own() {
        assert_ne!(
            UsageResult::RefusedByOwnerBudget.as_str(),
            UsageResult::RefusedByBudget.as_str()
        );
        assert!(UsageResult::ALL.contains(&UsageResult::RefusedByOwnerBudget));
        assert_eq!(
            UsageResult::parse(UsageResult::RefusedByOwnerBudget.as_str()),
            Some(UsageResult::RefusedByOwnerBudget)
        );
        assert_ne!(
            REFUSED_BY_OWNER_BUDGET, REFUSED_BY_BUDGET,
            "两种拒绝的响应成因同值的话，调用侧只能得出一个处置，而它们要问的不是同一个人"
        );
    }
}
