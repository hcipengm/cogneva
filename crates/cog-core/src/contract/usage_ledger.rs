//! The spend ledger's records, and the faces that write and read them.
//!
//! One process meters every LLM call the platform makes; another owns the
//! table those meterings land in. Neither may name the other's crate, so the
//! row's shape and the questions asked of it live here, and the table itself
//! stays with whoever owns the database.
//!
//! The row is data, not behaviour: a writer fills it from what it saw on the
//! wire, a reader folds it into windows. Keeping the fields in one place is
//! what stops the writer and the reader from drifting into two opinions about
//! what a column means — a drift neither side can see, because each compiles on
//! its own copy.

use chrono::{DateTime, Utc};

/// The environment a metered call was made in, as a closed set.
///
/// Closed because it is read as counts and a reader selects rows by it: a value
/// outside the set is not a new environment, it is a caller that did not say.
/// [`Self::declared`] is the only way in, and [`Self::Unknown`] is deliberately
/// outside the declared set -- an undeclared caller is not attributable to any
/// environment, and folding it into one would credit that environment with
/// tokens it did not burn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LlmOrigin {
    /// The platform's own task pool driving agents (planner, generator, ...).
    GatewayPool,
    /// A long-lived resident worker session.
    ResidentWorker,
    /// An interactive, human-driven session.
    Interactive,
    /// A sandbox executor run.
    Sandbox,
    /// The evaluation harness driving scaffolds over a benchmark. It spends on
    /// the same agents the pool does, but for a measurement rather than for
    /// output, and the two cannot be told apart afterwards if they share an
    /// origin: a reader asking what a benchmark cost would have to subtract it
    /// from production spend by hand.
    Eval,
    /// No usable declaration arrived. Not a place a call comes from, so it is
    /// kept out of [`Self::DECLARED`] and out of any environment's total.
    Unknown,
}

impl LlmOrigin {
    /// The values an environment may declare, in the order the ledger reads
    /// them. [`Self::Unknown`] is not one of them.
    pub const DECLARED: [LlmOrigin; 5] = [
        LlmOrigin::GatewayPool,
        LlmOrigin::ResidentWorker,
        LlmOrigin::Interactive,
        LlmOrigin::Sandbox,
        LlmOrigin::Eval,
    ];

    /// The origin a caller declared, or [`Self::Unknown`] when it declared none
    /// or named one outside the set.
    ///
    /// An unrecognised value reads as "nothing was declared" rather than being
    /// stored as free text: the column is a closed set a reader groups by, and
    /// letting an arbitrary token in would make every new misspelling its own
    /// series. Trimming is the only repair -- a value that names an environment
    /// after trimming is accepted.
    pub fn declared(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some("gateway_pool") => LlmOrigin::GatewayPool,
            Some("resident_worker") => LlmOrigin::ResidentWorker,
            Some("interactive") => LlmOrigin::Interactive,
            Some("sandbox") => LlmOrigin::Sandbox,
            Some("eval") => LlmOrigin::Eval,
            _ => LlmOrigin::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LlmOrigin::GatewayPool => "gateway_pool",
            LlmOrigin::ResidentWorker => "resident_worker",
            LlmOrigin::Interactive => "interactive",
            LlmOrigin::Sandbox => "sandbox",
            LlmOrigin::Eval => "eval",
            LlmOrigin::Unknown => "unknown",
        }
    }
}

/// What reading a finished response found, as a closed set.
///
/// The token counts on a ledger row are what the upstream said. This says
/// whether it said anything at all, which the counts cannot: a call that came
/// back `ok` carrying no usage frame lands `0 / 0`, and so does a call that
/// really served nothing. Summed into a window, the two are indistinguishable
/// and the silent one reads as a free call — so which of the two happened has
/// to travel on the row itself.
///
/// Closed, like [`LlmOrigin`]: the column is read as counts, and a value
/// outside the set is not a new way to be silent, it is a writer that drifted.
/// [`Self::Absent`] and [`Self::NotAsked`] both mean no number arrived and are
/// kept apart because the repair differs — the first is the upstream's silence
/// on a call that did ask, the second is our own request that never asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeteringOutcome {
    /// A usage frame arrived. It counts even when it reported zeros: a named
    /// zero is a reading, not a silence.
    Read,
    /// The call asked for usage and the upstream answered without it.
    Absent,
    /// The request carried no `stream_options`, so no usage frame was ever due.
    NotAsked,
    /// The response never finished. A call fault, not a metering one, kept
    /// apart so a stream that died cannot be read as an upstream that answers
    /// quietly.
    Interrupted,
    /// Our caller stopped reading before the end and no frame had arrived. The
    /// silence belongs to neither side: the upstream may have been about to
    /// speak, and the request did ask.
    Abandoned,
}

impl MeteringOutcome {
    /// The five cells, in the order the ledger reads them. Every successful
    /// call lands in exactly one, so a count over these is the whole of the
    /// calls that were metered at all.
    pub const ALL: [MeteringOutcome; 5] = [
        MeteringOutcome::Read,
        MeteringOutcome::Absent,
        MeteringOutcome::NotAsked,
        MeteringOutcome::Interrupted,
        MeteringOutcome::Abandoned,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            MeteringOutcome::Read => "read",
            MeteringOutcome::Absent => "absent",
            MeteringOutcome::NotAsked => "not_asked",
            MeteringOutcome::Interrupted => "interrupted",
            MeteringOutcome::Abandoned => "abandoned",
        }
    }

    /// The cell a stored word names, or `None` for anything outside the set —
    /// including the empty string a `NULL` column reads back as. `None` is the
    /// honest answer for a row written before this column existed, and folding
    /// it into [`Self::Read`] would credit that row with a reading nobody took.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "read" => Some(MeteringOutcome::Read),
            "absent" => Some(MeteringOutcome::Absent),
            "not_asked" => Some(MeteringOutcome::NotAsked),
            "interrupted" => Some(MeteringOutcome::Interrupted),
            "abandoned" => Some(MeteringOutcome::Abandoned),
            _ => None,
        }
    }
}

/// 一次窗口配速拒绝的证据：这一拍的数与拒的是谁，和拒绝本身落在同一行。
///
/// 单独一组列而不是复用 `estimated_input` / `refusal_kind` / `upstream`：那三格
/// 各有各的语义（估算的 prompt、上游拒的原因、这一行的主语），拿任何一个装配速
/// 的额度缺口都是第二个语义，而两个语义共用一格，读的人就再也分不开它们。
///
/// 标量是**池级报表**（各已量到窗口的上游相加），与 429 body 上是同一组数；
/// 真正判定的是 `upstreams` 里逐家各自的尺子——只留标量，一个"池里有额度、
/// 但每一家自己的窗口都装不下"的拒绝会读成"整池没额度"。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PaceEvidence {
    /// 这一拍量到窗口的上游家数。它与 `allowance_tokens` 必须一起读：没有它，
    /// 0 额度分不清"量了一家且烧穿"与"一家都没量到"。
    pub measured_upstreams: u32,
    pub allowance_tokens: u64,
    pub spent_tokens: u64,
    pub reserve_tokens: u64,
    /// 这一窗的起点。不能省：`allowance_tokens = 0` 本身分不清"窗口还没开张"
    /// 与"已经烧到零"，边界时刻必须自带。
    pub window_start_unix: i64,
    /// 参与判定且各自都被自己那把尺子拦下的候选。空集不会出现在拒绝行上。
    pub upstreams: Vec<PaceUpstreamEvidence>,
}

/// 一个候选自己被判拦那一刻的读数，键与健康表同形（`base_url|model`）。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PaceUpstreamEvidence {
    pub upstream: String,
    pub allowance_tokens: u64,
    pub spent_tokens: u64,
    pub reserve_tokens: u64,
    pub window_start_unix: i64,
    /// 这一家自己最早什么时候承接得下下一件任务。
    pub wait_secs: u64,
}

/// One metered LLM call.
#[derive(Debug, Clone)]
pub struct LlmUsageRecord {
    /// Pool identity of the upstream that served the call.
    pub upstream: String,
    /// Wire protocol family (`openai` / `anthropic`).
    pub api_style: String,
    /// Real model name as configured on the upstream.
    pub model: String,
    /// One of the platform's result words. The four refusals carry zero tokens
    /// but keep the attempt visible, and they are kept apart from `error`
    /// because the words name opposite repairs: an `error` row is an upstream
    /// that was tried and did not serve, `refused_by_circuit` is a call no
    /// upstream was ever offered (the pool was known unusable),
    /// `refused_by_pace` is one this platform held back on purpose,
    /// `refused_by_budget` is one the job itself declared a ceiling for and has
    /// now reached, and `refused_by_owner_budget` is one the person it would be
    /// charged to has no allowance left for today. Each reads as its own state
    /// rather than as "no traffic" -- and the last two are the only ones whose
    /// repair is not "wait for capacity" but "finish this job or raise its
    /// ceiling" and "wait for the day to turn, or have the allowance raised".
    /// On a refusal row `upstream` and `model` are `-`: no single upstream is
    /// its subject, so both columns say so rather than naming a candidate.
    pub result: String,
    /// Calling component as normalized by the gateway (`self_review`,
    /// `agent:<role>`, `unknown`); lets per-actor token spend be audited.
    pub actor: String,
    /// Environment the call was made from, as one of [`LlmOrigin`]'s values --
    /// the gateway's reading of the caller's own [`crate::LLM_ORIGIN_HEADER`].
    /// Answers a question `actor` cannot: the same component runs in more than
    /// one environment, so "who spent this" and "where it was spent" are two
    /// axes.
    pub origin: String,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// Input tokens the upstream served from its own prefix cache. Its own
    /// column rather than a fold into `tokens_input`: the ratio of the two is
    /// the hit rate, and that ratio is the only reading a change to prompt
    /// prefix stability can be accepted on, so a durable audit of token spend
    /// that cannot answer it is missing the one number it exists for. How the
    /// two relate follows the wire protocol (on an OpenAI-compatible upstream
    /// cached is a subset of input, on Anthropic the two are disjoint), which
    /// is why they stay apart here instead of being netted out.
    pub tokens_cached: u64,
    /// Input tokens the upstream wrote into its prefix cache on this call.
    ///
    /// Separate from [`Self::tokens_cached`] because the two answer different
    /// questions about the same feature, and a ledger holding only the read
    /// half cannot tell them apart. A cache that was written and never read
    /// again says the prefix changes between calls; a cache that was never even
    /// written says the request never asked the upstream to keep anything. The
    /// first repair is an ordering change in whichever component assembles the
    /// prompt, the second is a missing field on the wire. With `tokens_cached`
    /// alone both read as "cached: 0" and the repair is a guess.
    ///
    /// Upstreams on the OpenAI-compatible face do not report this at all -- they
    /// cache without being asked and say nothing about what they wrote -- so a
    /// zero there is "not reported" rather than "nothing written", and the
    /// protocol column is what a reader has to pair it with.
    pub tokens_cache_created: u64,
    /// What reading the response found, when this row came from a call that was
    /// answered at all.
    ///
    /// The three counts above are what the upstream said; this is whether it
    /// said anything. Without it a successful call that carried no usage frame
    /// is stored as `0 / 0 / 0` — the same row a call that truly served nothing
    /// produces — and every total summed from these columns silently treats the
    /// unmetered call as free. Measured on this deployment over the 30 days to
    /// 2026-10-30: 892 of 19,687 `ok` rows carry zero tokens, and which of the
    /// two situations that is cannot be recovered from the row.
    ///
    /// `None` is "no reading was taken", not a reading: the failure and refusal
    /// paths land rows about attempts that never got a body, and rows written
    /// before this column existed keep it empty. Folding those into
    /// [`MeteringOutcome::Read`] would invent a measurement.
    pub usage_outcome: Option<MeteringOutcome>,
    /// What this platform's own estimator priced the request at, before any
    /// upstream counted the prompt.
    ///
    /// The row's other input figure is the upstream's truth; this one is the
    /// estimate the window budget is judged against, so one row carries both
    /// rulers and the gap between them is readable. The estimate is a
    /// deliberate lower bound ([`crate::estimate_tokens`], the unit the agent's
    /// context window trims by) over the serialized body this gateway sent, so
    /// `estimated_input` below `tokens_input` is expected and its *size* is the
    /// finding: a gap that tracks the content mix is the estimator's, a request
    /// the estimator also called over budget is the window's. Without both
    /// numbers on the same row those two read alike, which is why this is a
    /// column and not a counter of over-budget calls.
    ///
    /// `None` on rows landed by a path that does not price a request (failed
    /// attempts and pooled refusals). Absent is not zero here: a row priced at
    /// zero would read as an empty prompt, and the two have opposite repairs.
    pub estimated_input: Option<u64>,
    /// Why the upstream refused, as the gateway classified the refusal body:
    /// `quota_cycle`, `entitlement`, or `unclassified`.
    ///
    /// Nullable, and `NULL` means either "this row is not a refusal" (a
    /// successful call, a transport failure with no body to read, a pooled
    /// refusal decided before any upstream was tried) or a row written before
    /// this column existed -- the reader cannot tell those apart after the
    /// fact, which is why the value is recorded at the refusal site and never
    /// reconstructed. The distinction is the point: `quota_cycle` says this
    /// window is spent and will come back, `entitlement` says it never existed,
    /// and the two call for opposite actions. Folding both into `error` makes
    /// "we have no capacity" and "we never bought this model" one cell.
    pub refusal_kind: Option<String>,
    /// 这次熔断拒绝让调用方等多久（秒）。
    ///
    /// 只有 `refused_by_circuit` 的行有：熔断在拒绝的那一刻就已经说出下一次值得
    /// 再试的时刻，而**这个数的量级正是两种熔断的分界**——窗口翻页黑障是几十秒到
    /// 几分钟，额度烧穿是几千秒（通常就是上游自述的 reset）。不落这一格，两种情形
    /// 在账上都只是 `refused_by_circuit`，读者得回头去读窗口相位才能分开，而那
    /// 份读数在事后往往已经滚过去了。
    ///
    /// `None` 不是 0：0 会读成「立刻可以重试」，而这行说的恰恰是「现在不能」。
    pub retry_after_secs: Option<u64>,
    /// The unit of work this call was spent for, as the caller declared it --
    /// the same identity the caller reports its own per-run totals under.
    ///
    /// Nullable on purpose, like `estimated_input` and for the same reason:
    /// `NULL` is "this call was not attributed to a job", which is a different
    /// statement from "it was spent for job `unknown`". A NOT NULL default would
    /// put every unattributed call in one group and read as a single very
    /// expensive job, so rows landed by paths that have no job identity keep it
    /// empty rather than inventing one.
    pub task: Option<String>,
    /// The person this call is charged to, as the caller declared it.
    ///
    /// Beside `task`, not instead of it: the two answer different questions and
    /// lead to different readers. `task` is the unit of work whose ceiling the
    /// call is judged against; this is who pays for it, and it is the same
    /// identity the gateway's owner check and the person's daily counters use,
    /// so a refused row names the person it refused rather than leaving that to
    /// be inferred from the spend counters -- which move only on calls that
    /// actually ran.
    ///
    /// Nullable on the same terms as `task`: `NULL` is "no owner was declared",
    /// not an owner named `unknown`. Every path that cannot name one keeps it
    /// empty rather than gathering unattributed calls into one placeholder
    /// person.
    pub owner: Option<String>,
    /// Wall time of the call; for streams this is first-byte to last-byte.
    pub latency_ms: u64,
    /// 窗口配速那一拍的读数，只在 `result = "refused_by_pace"` 的行上有。
    ///
    /// `None` 在非配速行上是"不适用"，不是"缺口为 0"——`NULL ≠ 0` 是这组列
    /// 存在的全部理由：配速拒绝的缺口按 `spent + reserve − allowance` 复算，
    /// 而别的行上没有这一拍的数，给它们写 0 会让缺口读成"正好不欠额度"。
    pub pace: Option<PaceEvidence>,
}

/// One LLM exchange, bodies included.
///
/// The usage record beside it answers "how much"; this answers "what was
/// asked and what came back". They are separate rows in separate tables
/// because the two are written by the same call but read by different
/// consumers: pacing and billing read the usage row on every window boundary
/// and must not pay for text they never look at, while a person asking why a
/// run burned 850k tokens reads one exchange and nothing else.
#[derive(Debug, Clone)]
pub struct LlmExchangeRecord {
    pub upstream: String,
    pub api_style: String,
    pub model: String,
    pub actor: String,
    pub origin: String,
    pub task: Option<String>,
    /// `ok`, `abandoned` (the caller dropped the stream mid-flight), or
    /// `error` (the upstream answered with a non-2xx body).
    pub result: String,
    pub latency_ms: u64,
    /// SHA-256 of the exact bytes sent upstream, before redaction.
    pub request_sha256: String,
    /// SHA-256 of the response bytes; `None` when no response body existed.
    pub response_sha256: Option<String>,
    pub request_bytes: u64,
    pub response_bytes: u64,
    /// Redacted text, capped by the caller and marked in `truncated` when it
    /// was cut. `None` is the shape a caller uses when there were no bytes at
    /// all -- a request path that builds no body of its own, for instance --
    /// and is therefore distinct from `Some("")`, which is a body that was
    /// empty. Both the size and the hash are taken before the cut, so a reader
    /// can tell a short body from a long one that was clipped.
    pub request_body: Option<String>,
    pub response_body: Option<String>,
    /// Which side was dropped for size: `""`, `request`, `response`, `both`.
    pub truncated: String,
}

/// One upstream's quota window as the ledger tells it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpstreamWindowReading {
    /// Last instant this upstream served a call; `None` when it never has.
    pub last_ok_unix: Option<i64>,
    /// Instant of the most recent refusal that said the window was spent;
    /// `None` when no such refusal is on record.
    pub last_quota_refusal_unix: Option<i64>,
    /// Latest instant serving resumed after a spent-cycle refusal, `None` when
    /// it never has.
    ///
    /// This is the only observation of a reset that an upstream which names no
    /// instant leaves behind: a call cannot succeed while the window is spent,
    /// so the first success after a wall bounds the instant the quota returned
    /// — the error is the retry delay, and for a pacer that re-reads every tick
    /// that is seconds. The instant is a **window start**: windows repeat from
    /// it, so it anchors the phase even while a later wall has no success after
    /// it yet. The refusal itself is not that instant: a window can be spent on
    /// its first day.
    pub last_resume_unix: Option<i64>,
    /// What the window ending at the last classified refusal served; zero when
    /// there is no such refusal, because nothing measured it.
    pub burned_window_tokens: u64,
}

/// One upstream's window measurement, as durable state.
///
/// `source` names **which observation** the boundary came from, not what kind
/// of code wrote the row: `named_reset` is the instant an upstream named in its
/// refusal, `last_resume` is the first successful call after the wall, which
/// bounds the reset from above by the retry delay, and `observed_reset` is a
/// live reading that classified the outstanding refusal as a spent cycle and
/// named when the quota returns. A reader that sees only the instant cannot
/// tell an exact promise from a bounded one; the three are not equally strong.
/// Rows written before this vocabulary say `wall_row`: that spelling named the
/// path the capacity was summed on, not the boundary, so it reads as
/// `provenance unknown` until the next measurement rewrites the row.
///
/// `declared_reset_unix` is the upstream's own statement, kept verbatim. It can
/// differ from `boundary_unix` when the named instant was rejected as belonging
/// to another cycle, so the pair answers "what did it say" and "what did we
/// pace from" separately. `None` means the upstream named no instant at all —
/// it is never the boundary copied across.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredWindow {
    pub upstream: String,
    pub capacity_tokens: u64,
    pub window_secs: u64,
    pub boundary_unix: i64,
    pub source: String,
    pub declared_reset_unix: Option<i64>,
}

/// How the calls in one range ended, one line per `(result, refusal_kind)`.
///
/// The spend report says "these tokens, by this actor"; it cannot say why a
/// range has no tokens at all, because a call that was refused before it left
/// the process still lands a row and that row carries its own verdict. Reading
/// the verdicts back is what turns a flat zero — an idle pool and a pool whose
/// circuit is open look identical on every other face — into a sentence.
///
/// `refusal_kind` is empty for rows that were not refused (`ok`, `error`); it
/// is carried rather than folded into `result` because two refusals with
/// different kinds want different repairs, and a count that merges them names
/// no one to fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeCount {
    /// `ok` / `error` / `refused_by_*`, as written by the meter.
    pub result: String,
    /// The narrower reason a refusal names, or empty.
    pub refusal_kind: String,
    pub calls: i64,
}

/// How many successful calls in a range carried a usage reading, by what the
/// reading found.
///
/// One line per cell of [`MeteringOutcome`], plus a line with an empty outcome
/// for calls whose row predates the column. This is the trustworthiness of the
/// token totals beside it: an `absent`/`not_asked` line means those calls are
/// counted in the spend with zeros, and a window made mostly of them is a
/// window whose token total is a floor rather than a measurement.
///
/// Only successful calls are counted — the failure and refusal paths never got
/// a body to read, and their rows are unmetered by construction rather than by
/// fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeteringCount {
    /// One of [`MeteringOutcome::ALL`]'s words, or empty for a row written
    /// before the column existed.
    pub outcome: String,
    pub calls: i64,
}

/// One site's savings in a range, as the saved-call ledger recorded them.
///
/// The spend ledger answers what was paid for. This answers what was *not* paid
/// for and on purpose, which is a different fact and comes from a different
/// table: a stage that spent nothing looks the same whether it decided not to
/// or was never reached, and only the recorded saving separates them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedWorkLine {
    /// The stage that chose not to call (a role name, a self-review stage).
    pub stage: String,
    /// Why it chose that. Free text from the caller, so it is grouped and shown
    /// rather than interpreted here.
    pub reason: String,
    pub skips: i64,
}

/// One line of the spend verdict projection: how one site's windows were judged.
///
/// A verdict is the only face that says a silence was *unexplained*. The spend
/// report shows zero, the outcomes show refused calls, and the saved-call
/// ledger shows the savings that were recorded — none of them can say "this
/// site used to spend here and nothing accounts for it stopping".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendVerdictLine {
    /// The declared call site this cell belongs to.
    pub actor: String,
    /// `necessary` / `documented_skip` / `unexpected_absent` / `unsettled`.
    /// Carried as written rather than as an enum: the column is what a reader
    /// selects by, and a value this crate does not know is a value the writer
    /// wrote, not an error to swallow.
    pub verdict: String,
    /// The saving the cell was judged against, or empty when it had none.
    pub skip_reason: String,
    /// How many hourly windows share this judgement.
    pub windows: i64,
    pub skips: i64,
    pub tokens_input: i64,
    pub tokens_output: i64,
}

/// The writing and window-reading face of the spend ledger.
///
/// Separate from the reporting face ([`crate::LlmUsageReader`]) because the two
/// have different callers and different failure budgets: a caller landing a row
/// is on the request path and must be able to give up quietly, while a report
/// reader is answering a person and must be able to say why it could not. One
/// trait holding both would let a backend be half-implemented and still compile
/// at every call site.
///
/// Errors are strings rather than a driver error type: the callers are crates
/// that never see the database, and a failure here is only ever logged.
#[async_trait::async_trait]
pub trait LlmUsageLedger: Send + Sync {
    /// Land one metered call.
    async fn record(&self, record: &LlmUsageRecord) -> Result<(), String>;

    /// Land one exchange's bodies.
    async fn record_exchange(&self, record: &LlmExchangeRecord) -> Result<(), String>;

    /// What the ledger knows about one upstream's window phase.
    async fn upstream_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> Result<UpstreamWindowReading, String>;

    /// Tokens served for `upstream` in `[since_unix, until_unix)`.
    async fn burned_between(
        &self,
        upstream: &str,
        since_unix: i64,
        until_unix: i64,
    ) -> Result<u64, String>;

    /// Persist one window measurement.
    async fn record_window(&self, window: &StoredWindow) -> Result<(), String>;

    /// The last persisted measurement for `(upstream, window_secs)`.
    async fn stored_window(
        &self,
        upstream: &str,
        window_secs: u64,
    ) -> Result<Option<StoredWindow>, String>;

    /// Tokens served for `upstream` since `since_unix`, successful calls only.
    async fn spent_since(&self, upstream: &str, since_unix: i64) -> Result<u64, String>;

    /// Today's token totals, split into the pool total and one entry per actor.
    async fn spend_by_actor_since(
        &self,
        since_unix: i64,
    ) -> Result<(u64, std::collections::BTreeMap<String, u64>), String>;

    /// What one job costs, from the jobs that finished inside the last `days`:
    /// `(cost at `quantile`, how many jobs that was, when the oldest one was)`.
    ///
    /// `None` is "no job finished in the window" — a different answer from a
    /// zero cost, and the caller has to be able to tell them apart or it will
    /// reserve nothing for every job it is about to start.
    async fn task_cost_quantile(
        &self,
        days: i64,
        quantile: f64,
    ) -> Result<Option<(u64, u64, i64)>, String>;

    /// Whether the named task has ever landed a call on this ledger.
    async fn task_has_started(&self, task: &str) -> Result<bool, String>;

    /// Tokens spent by each of the named tasks, `(task_id, tokens)`.
    ///
    /// Tasks that never spent are absent rather than zero: a job that has not
    /// started and a job that cost nothing are different answers to "what has
    /// this run cost so far", and only the caller knows which one it asked.
    async fn spend_by_task(&self, task_ids: &[String]) -> Result<Vec<(String, u64)>, String>;

    /// Tokens spent in `[since, until)` that no task in the graph owns.
    ///
    /// The rows are calls that really were paid for, so dropping them would
    /// understate what a project cost; but no task can carry them, so they are
    /// reported as one window total instead of being folded into any task.
    async fn unattributed_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, String>;

    /// How the calls in `[since, until)` ended, grouped by `(result,
    /// refusal_kind)`.
    ///
    /// Answered from the per-call ledger rather than from the rollup: the fold
    /// keeps `calls` and `failed_calls`, which say *that* calls failed, and the
    /// verdict that says *why* is a column the fold does not carry. A range
    /// with zero successful calls is exactly when that column is worth reading,
    /// so the read is allowed to scan the ledger — it is bounded by the range,
    /// not by the table.
    ///
    /// An empty answer means no call was attempted in the range, which is not
    /// the same as "every call succeeded": that comes back as one `ok` line.
    async fn outcomes_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<OutcomeCount>, String>;

    /// What reading the response found for the successful calls in
    /// `[since, until)`, grouped by cell.
    ///
    /// The companion to [`Self::outcomes_between`] and read from the same
    /// rows: that one says how attempts ended, this one says how much of the
    /// resulting spend was actually measured. A window of `ok` calls that all
    /// landed `absent` spends tokens the totals record as zero, so a reader
    /// that only sees the totals has no way to know its number is a floor.
    ///
    /// A range with no successful call answers empty, which is not the same as
    /// every call having been read.
    async fn metering_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<MeteringCount>, String>;

    /// The savings recorded in `[since, until)`, one line per `(stage, reason)`.
    ///
    /// Read beside the spend and the outcomes because the three together are
    /// the only complete answer to "why did this range cost nothing": some
    /// calls were refused, and some work was never attempted. This is the
    /// second half, and without it a deliberate saving and a stalled planner
    /// produce the same screen.
    ///
    /// An empty answer means nothing chose to skip, which is not the same as
    /// "nothing was skipped": a saved-call ledger nothing writes to also
    /// answers empty. The caller is the one that knows whether its writer is
    /// alive.
    async fn skips_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<SkippedWorkLine>, String>;

    /// The spend verdicts judged for windows inside `[since, until)`, summed
    /// per `(actor, verdict, skip_reason)`.
    ///
    /// The loop that judges windows writes these rows and moves on, so until
    /// something reads them back the judgement is made and never delivered. A
    /// site whose windows read `unexpected_absent` — it was live, it had been
    /// spending, and it stopped with no saving on the record — is exactly the
    /// case the other two faces cannot name, so this is the face that has to
    /// carry it.
    async fn verdicts_between(
        &self,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<SpendVerdictLine>, String>;
}

/// A process's analytics sink: fire-and-forget events, one call per event.
///
/// `&self` with no result because the caller is on a request path and the sink
/// is a queue: an event that could not be queued is the sink's problem to log,
/// not something the request should turn into an error.
pub trait AnalyticsSink: Send + Sync {
    fn send(&self, event: crate::AnalyticsEvent);
}

/// A single analytics event.
///
/// The shape moved here because two crates fill it and neither may name the
/// other: the producing process builds the properties it witnessed, and the
/// sink that stores them reads the same key names. A `HashMap` of JSON values
/// rather than typed fields -- an event's properties are added by the call site
/// that knows them, and a type per event would put a new event type behind a
/// core change.
#[derive(Debug, Clone)]
pub struct AnalyticsEvent {
    pub event_id: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub properties: std::collections::HashMap<String, serde_json::Value>,
}

impl AnalyticsEvent {
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: event_type.into(),
            timestamp: Utc::now(),
            user_id: None,
            session_id: None,
            task_id: None,
            agent_id: None,
            properties: std::collections::HashMap::new(),
        }
    }

    pub fn user_id(mut self, id: impl Into<String>) -> Self {
        self.user_id = Some(id.into());
        self
    }

    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.session_id = Some(id.into());
        self
    }

    pub fn task_id(mut self, id: impl Into<String>) -> Self {
        self.task_id = Some(id.into());
        self
    }

    pub fn agent_id(mut self, id: impl Into<String>) -> Self {
        self.agent_id = Some(id.into());
        self
    }

    pub fn property(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_spelling_of_an_environment_is_no_declaration() {
        assert_eq!(
            LlmOrigin::declared(Some("interactive")),
            LlmOrigin::Interactive
        );
        assert_eq!(LlmOrigin::declared(Some("  eval ")), LlmOrigin::Eval);
        assert_eq!(LlmOrigin::declared(Some("qa")), LlmOrigin::Unknown);
        assert_eq!(LlmOrigin::declared(None), LlmOrigin::Unknown);
    }

    #[test]
    fn the_undeclared_origin_is_not_one_a_caller_may_claim() {
        assert!(!LlmOrigin::DECLARED.contains(&LlmOrigin::Unknown));
        assert_eq!(LlmOrigin::DECLARED.len(), 5);
    }

    #[test]
    fn an_event_carries_the_type_it_was_named_with() {
        let event = AnalyticsEvent::new("llm_call").property("tokens", serde_json::json!(12));
        assert_eq!(event.event_type, "llm_call");
        assert_eq!(event.properties["tokens"], serde_json::json!(12));
        assert!(!event.event_id.is_empty(), "an event needs an identity");
    }
}
