use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::ApiError;
use crate::GatewayState;

/// Query parameters for event stream filtering.
#[derive(Debug, Deserialize, Default)]
pub struct EventFilterQuery {
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub squad_id: Option<String>,
    pub event_types: Option<String>,
    pub level: Option<String>,
    /// How far back the caller asked to read, when it said so.
    ///
    /// A question wider than the store's floor widens the floor: the sweep
    /// refuses to delete rows inside the longest span anyone has asked for,
    /// so this number is both what this answer covers and the input the next
    /// sweep beat recomputes against.
    pub window_secs: Option<u64>,
}

/// Query parameters for paginated log fetching.
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

// ─── Cluster Overview ───

/// How many firing alerts to fetch when all the verdict needs is whether any
/// exist. The question is boolean, so one row settles it and the read stays
/// bounded no matter how much is firing.
// cap: bound — 探测只取一条；有界，不随正在告警的数量增长
const FIRING_PROBE_LIMIT: i64 = 1;

/// Verdict over the firing alerts an evidence source reported.
///
/// The scope is exactly that evidence: `healthy` means the source answered and
/// nothing was firing, not that every component was verified working. Severity
/// plays no part — the alert vocabulary spends `info` on resolutions, so any
/// row still firing is a claim that something is wrong.
fn cluster_health_of(firing: &[cog_core::PersistedAlert]) -> cog_core::ClusterHealth {
    if firing.is_empty() {
        cog_core::ClusterHealth::Healthy
    } else {
        cog_core::ClusterHealth::Degraded
    }
}

/// Fill the overview's verdict from whichever alert source this process has.
///
/// The storage layer holds no alert evidence, so it answers `unknown`; only a
/// surface that can actually read the alerts may replace that word. The verdict
/// stays `unknown` both when there is no source and when the source's read
/// failed — "I did not look" and "nothing is firing" must not arrive as the
/// same string, and a query that errored is not evidence of quiet.
async fn apply_alert_verdict(
    overview: &mut cog_core::observability::ClusterOverview,
    source: Option<&Arc<dyn cog_core::ActiveAlertSource>>,
) {
    let Some(source) = source else {
        return;
    };
    let Some(firing) = source.list_active_alerts(FIRING_PROBE_LIMIT).await else {
        return;
    };
    overview.cluster_health = cluster_health_of(&firing);
}

/// Fill the agent counts from the registry that actually holds presence.
///
/// The storage layer answers these two cells from a table no process writes,
/// so it reports `None`; only a surface that holds the agent registry may
/// replace that with a count. The registry is where an agent announces itself
/// and keeps beating, and it drops a record whose beats stop — so its listing
/// is live units, not names that were once taken. Both cells carry that one
/// count: the index behind it keeps retired names forever, and counting the
/// index would report tombstones as agents, disagreeing with the roster
/// printed next to it. A failed read leaves both `None`, because "unknown" and
/// "zero" are the two answers this exists to keep apart.
async fn apply_agent_census(
    overview: &mut cog_core::observability::ClusterOverview,
    registry: Option<&Arc<dyn cog_core::AgentRegistry>>,
) {
    let Some(registry) = registry else {
        return;
    };
    let Ok(live) = registry.list().await else {
        return;
    };
    let count = live.len();
    overview.total_agents = Some(count);
    overview.active_agents = Some(count);
}

pub async fn cluster_overview_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<ClusterOverviewResponse>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let mut overview = gateway
        .get_cluster_overview()
        .await
        .map_err(|e| ApiError::internal(format!("failed to get cluster overview: {e}")))?;
    apply_alert_verdict(&mut overview, state.active_alert_source.as_ref()).await;
    apply_agent_census(&mut overview, state.agent_registry.as_ref()).await;
    let llm_pool = llm_pool_view(&state).await;
    Ok(Json(ClusterOverviewResponse { overview, llm_pool }))
}

/// The cluster's counts, and what the model plane can currently do.
///
/// The pool is a separate cell rather than a field of the overview because it
/// is a different kind of fact: the counts describe this gateway's own books,
/// and the pool is a snapshot another process published. `None` is not a
/// healthy pool — it is "there is no snapshot to read", which a reader must
/// not mistake for "the upstreams are fine".
#[derive(Debug, Serialize)]
pub struct ClusterOverviewResponse {
    #[serde(flatten)]
    pub overview: cog_core::observability::ClusterOverview,
    pub llm_pool: Option<LlmPoolView>,
}

/// What the LLM pool can do right now, as its own writer published it.
///
/// Deliberately counts and instants only. The upstream identities in the
/// snapshot carry vendor hosts and model names, and the business plane is
/// meant to be vendor-neutral: an operator who needs to know which account to
/// top up reads that on the gateway that holds the credentials. What the rest
/// of the platform legitimately needs is whether anything can serve, how many
/// members the pool has, and when it may come back.
#[derive(Debug, Serialize)]
pub struct LlmPoolView {
    /// Whether the pool as a whole can serve, as the writing process latched
    /// it. Not re-derived here: the latch is cleared only by an upstream
    /// actually answering, and a reader that recomputed it from the suspect
    /// list would clear it whenever a probe window expired.
    pub available: bool,
    /// How many upstreams the pool is configured with; zero when the writer
    /// did not say, never a pool of no upstreams.
    pub pool_size: usize,
    /// How many must serve for the pool to count as able to carry
    /// LLM-dependent work, when the operator declared a floor.
    pub min_healthy: Option<usize>,
    pub unavailable_count: usize,
    /// The longest quota window a suspect upstream stated, when it stated one.
    /// An upstream that only says its weekly window has to run out names no
    /// instant, and this is then the only statement of how long the outage
    /// lasts.
    pub quota_window_secs: u64,
    /// The earliest instant an upstream said its quota resets.
    pub recovery_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The earliest instant our own probe will try an upstream again.
    pub next_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When the writing process last saw an upstream actually answer. Per
    /// process: a freshly started writer reports nothing, which is "no
    /// evidence yet" rather than "nothing has served".
    pub last_served_at: Option<chrono::DateTime<chrono::Utc>>,
    /// What the current window has allowed and what has been spent against it,
    /// when at least one upstream has a measured window. `None` is "nothing
    /// measured yet", never a window allowing nothing.
    pub pace: Option<LlmPaceView>,
}

/// The quota window's own account: how much it allowed, how much went out.
///
/// The spend faces answer "how much was spent". None of them can answer "how
/// much was there to spend", and on a deployment whose upstreams are
/// subscription plans there is no price to multiply by tokens — the window's
/// measured capacity is the whole of the cost statement. Without this, a
/// window that is nearly gone and a window that is barely touched read the
/// same on every other face, because the tokens spent are the only number any
/// of them carries.
///
/// Every figure here comes from an observation, not from a published quota:
/// nothing upstream tells us its limit, so the capacity is what it served
/// before it refused the window as spent, and the allowance is that capacity
/// spread linearly over the window's length. `window_start_at` is when the
/// current window began — its end is `window_start_at + window_secs`, left to
/// the reader so the wire carries no second copy of arithmetic.
#[derive(Debug, Serialize)]
pub struct LlmPaceView {
    /// How many upstreams' measured windows went into these numbers. The
    /// allowance is the sum across them, so a reading measured on one member
    /// out of several is a smaller allowance, not a broken one.
    pub measured_upstreams: usize,
    /// Tokens the current window allows by now, summed over those upstreams.
    ///
    /// A sum, not one window's figure: the members keep their own windows and
    /// the total is what they allow between them. It is therefore nonzero
    /// while `window_secs`/`window_start_at` are absent, whenever the members
    /// do not share one window — a reader must not pair the two into a rate.
    pub allowance_tokens: u64,
    /// Tokens the current window has served, as the ledger counted them.
    pub spent_tokens: u64,
    /// Tokens one whole job takes, measured over finished jobs; `0` when no
    /// job has finished recently enough to be measured.
    pub reserve_tokens: u64,
    /// How many finished jobs the reserve was measured over. `0` means the
    /// reserve has no population behind it, and a number measured over five
    /// jobs is not the same claim as one measured over five hundred.
    pub reserve_population: u64,
    /// How long the window is, seconds; `0` when the measured members are not
    /// all in one window, where the length describes nothing they share.
    pub window_secs: u64,
    /// When the current window started. `None` when the snapshot is not paced
    /// and, equally, when the measured members are in different windows: the
    /// reader then has no single start to pair with `window_secs`, and no end
    /// to compute.
    pub window_start_at: Option<chrono::DateTime<chrono::Utc>>,
}

fn instant(unix: i64) -> Option<chrono::DateTime<chrono::Utc>> {
    if unix <= 0 {
        return None;
    }
    chrono::DateTime::from_timestamp(unix, 0)
}

/// The snapshot's allowance account, or `None` when no window was measured.
///
/// The zero case is why this is an `Option` rather than a struct of zeros: a
/// payload with no measured window carries `measured_upstreams == 0` and every
/// amount at zero, and a face that published those would say the window allows
/// nothing — the opposite of "we have not measured anything", and enough to
/// make a reader believe the quota is spent.
fn pace_view(pace: &cog_core::LlmWindowPace) -> Option<LlmPaceView> {
    if pace.measured_upstreams == 0 {
        return None;
    }
    Some(LlmPaceView {
        measured_upstreams: pace.measured_upstreams,
        allowance_tokens: pace.allowance_tokens,
        spent_tokens: pace.spent_tokens,
        reserve_tokens: pace.reserve_tokens,
        reserve_population: pace.reserve_population,
        window_secs: pace.window_secs,
        window_start_at: instant(pace.window_start_unix),
    })
}

/// Project a pool snapshot onto the reading the business plane gets.
///
/// One function so the two readers that will exist cannot disagree: the route
/// reads it now, and anything that later reuses the snapshot reads the same
/// projection rather than reshuffling the fields itself.
fn pool_view(status: &cog_core::LlmPoolStatus) -> LlmPoolView {
    LlmPoolView {
        available: !status.unavailable,
        pool_size: status.pool_size,
        min_healthy: status.declared_min_healthy,
        unavailable_count: status.unavailable_upstreams.len(),
        quota_window_secs: status.quota_window_secs,
        recovery_at: instant(status.evidenced_recovery_unix),
        next_attempt_at: instant(status.next_attempt_unix),
        last_served_at: instant(status.last_success_unix),
        pace: pace_view(&status.pace),
    }
}

/// Read the pool snapshot the credential-holding gateway publishes.
///
/// Absent store, absent key, or a payload that does not parse all answer
/// `None` for one reason: each of them means nothing here can say whether the
/// pool can serve, and a fabricated `false` would be as wrong as a fabricated
/// `true`.
async fn llm_pool_view(state: &GatewayState) -> Option<LlmPoolView> {
    let store = state.key_value.as_ref()?;
    let raw = store
        .get(cog_core::LLM_POOL_STATUS_KEY)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "the LLM pool snapshot could not be read");
        })
        .ok()??;
    let status: cog_core::LlmPoolStatus = serde_json::from_str(&raw)
        .map_err(|e| {
            tracing::warn!(error = %e, "the LLM pool snapshot did not parse");
        })
        .ok()?;
    Some(pool_view(&status))
}

// ─── Squad State ───

pub async fn squad_state_handler(
    State(state): State<Arc<GatewayState>>,
    Path(squad_id): Path<String>,
) -> Result<Json<cog_core::observability::SquadState>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let squad = gateway
        .get_squad_state(&squad_id)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get squad state: {e}")))?;
    Ok(Json(squad))
}

// ─── Agent State ───

pub async fn agent_state_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Result<Json<cog_core::AgentState>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let agent_state = gateway
        .get_agent_state(&agent_id)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get agent state: {e}")))?;
    Ok(Json(agent_state))
}

// ─── Token spend by actor (durable rollup) ───

/// Query parameters for [`token_usage_by_actor_handler`].
#[derive(Debug, Deserialize)]
pub struct TokenUsageQuery {
    /// Size of the trailing window, in hours. Defaults to one day.
    #[serde(default = "default_usage_hours")]
    pub hours: i64,
}

fn default_usage_hours() -> i64 {
    24
}

/// 投入那一格与花销面读的必须是同一段窗口。
///
/// 两处各写一个数就是两个承诺：工作台说「本窗」24 小时、花销面说别的长度时，
/// 点进去看到的总额与格上的数字对不上，而两个数各自都说得通。所以这里不给工作台
/// 第二个常量，让它走同一条读数。
pub(crate) fn spend_window_hours() -> i64 {
    default_usage_hours()
}

/// How far back a spend read may reach, in hours, taken from the ledger's own
/// retention rather than written down here.
///
/// A constant here and a knob there are two promises, and the day they disagree
/// nothing reports it: the sweep keeps thirty days, the read answers seven, and
/// both look healthy — twenty-three days of rows sit in the table and no caller
/// can reach them. Deriving the ceiling from
/// [`cog_core::retention::llm_usage_keep_days`] makes the two statements one
/// statement. It is evaluated per call rather than cached in a `const` so a
/// deployment that changes the knob changes the read with it.
fn usage_reach_hours() -> i64 {
    cog_core::retention::llm_usage_keep_days().max(1) * 24
}

/// Cap on the *per-window* read's trailing span.
///
/// This one is genuinely about the response, not about the data: that read
/// returns one row per window per `(actor, origin, upstream, protocol)`, so the
/// range *is* the response size — the fold held 55,996 rows over its first five
/// weeks and a chart cannot draw them. The summed reads have no such limit and
/// derive theirs from the retention ([`usage_reach_hours`]); only this one is
/// bounded by how much can be drawn.
// cap: bound — 逐窗口那一读的响应大小就是区间本身，界的是画得出来多少格
const MAX_SERIES_HOURS: i64 = 24 * 7;

/// The instant a spend request asks the fold to sum from.
///
/// `hours <= 0` is how a caller asks for everything the fold holds rather than
/// for a trailing window. It is a separate answer from clamping to the reach:
/// the reach bounds one scan, while "all of it" is the only way to read a
/// cumulative total at all, and a reader that wanted the total and silently got
/// a bounded span would publish a number that looks like the total and is not.
/// The honest bound still travels: every read reports the range the fold
/// actually covered, so the cumulative figure arrives with the day it starts
/// on.
///
/// The floor is the epoch rather than "now minus something large" because the
/// fold's oldest window is not known here — asking from before any ledger row
/// can exist needs no knowledge of when the platform started, and an empty
/// answer still carries `covered_since: None`.
fn usage_since(hours: i64, until: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    if hours <= 0 {
        return chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
    }
    until - chrono::Duration::hours(hours.clamp(1, usage_reach_hours()))
}

/// The instant a *per-window* read starts from: always a bounded trailing span.
///
/// The summed read above answers "what has this account spent" and can answer it
/// over the whole fold, because it returns one row per actor however wide the
/// range is. This one returns one row per window per combination, so the range
/// *is* the response size — the fold held 55,996 rows over its first five weeks,
/// and a chart cannot draw them. Asking for everything therefore comes back as
/// the cap here, and that is not a silent substitution: the answer's own
/// `covered_since` names the first window it drew, so a reader that asked for
/// more than seven days can see where the drawing starts. `hours <= 0` keeps the
/// same reading as [`usage_since`] — "as wide as this read goes" — and the two
/// reach different distances on purpose: this read's limit is how much can be
/// drawn, that one's is how much the ledger still holds.
fn series_since(hours: i64, until: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    if hours <= 0 {
        return until - chrono::Duration::hours(MAX_SERIES_HOURS);
    }
    until - chrono::Duration::hours(hours.clamp(1, MAX_SERIES_HOURS))
}

/// The rolled spend, with the range of windows it was summed over.
///
/// The endpoints are the ones the rollup answered for, not the ones the request
/// named, and they are `null` together when no folded window fell inside the
/// requested range. Both halves matter: the fold lands one window per closed
/// hour, so the trailing window a caller reaches for first is exactly the one
/// that is not folded yet, and echoing the request back would certify "we
/// covered that" over an empty list — the same response a genuinely idle
/// platform returns. The two are told apart by the range the read covered.
#[derive(Debug, serde::Serialize)]
pub struct TokenUsageResponse {
    pub covered_since: Option<String>,
    pub covered_until: Option<String>,
    pub actors: Vec<cog_core::ActorUsage>,
}

/// Token spend per actor × upstream × protocol over the trailing window, read
/// from the durable rollup rather than the per-call ledger.
pub async fn token_usage_by_actor_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<TokenUsageQuery>,
) -> Result<Json<TokenUsageResponse>, ApiError> {
    let reader = state
        .llm_usage_reader
        .as_ref()
        .ok_or_else(|| ApiError::internal("token usage reader not configured"))?;
    let until = chrono::Utc::now();
    let since = usage_since(q.hours, until);
    let rolled = reader
        .usage_by_actor_rolled(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read token usage: {e}")))?;
    Ok(Json(token_usage_response(rolled)))
}

/// Project a rolled read onto the response face.
///
/// Takes the read alone, so the request's own window is not in scope here and
/// cannot be echoed as if it were coverage; every endpoint the response carries
/// is one the rollup named.
fn token_usage_response(rolled: cog_core::RolledUsage) -> TokenUsageResponse {
    TokenUsageResponse {
        covered_since: rolled.covered_since.map(|t| t.to_rfc3339()),
        covered_until: rolled.covered_until.map(|t| t.to_rfc3339()),
        actors: rolled.actors,
    }
}

/// Query parameters for [`token_series_handler`]. Same window, same cap.
#[derive(Debug, Deserialize)]
pub struct TokenSeriesQuery {
    #[serde(default = "default_usage_hours")]
    pub hours: i64,
}

/// The fold left at window granularity: what was spent in each closed window,
/// per `(actor, origin, upstream, protocol)`, over the range the fold covered.
///
/// The rows travel unsplit — one per window and combination, not summed into
/// either — because the face that reads this draws a time axis, and a sum here
/// would leave the reader with a single point and no way to recover the
/// intermediate ones.
#[derive(Debug, serde::Serialize)]
pub struct TokenSeriesResponse {
    pub covered_since: Option<String>,
    pub covered_until: Option<String>,
    pub rows: Vec<cog_core::WindowSpend>,
}

/// Token spend per window per actor × upstream × protocol over the trailing
/// window, read from the durable rollup rather than the per-call ledger.
pub async fn token_series_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<TokenSeriesQuery>,
) -> Result<Json<TokenSeriesResponse>, ApiError> {
    let reader = state
        .llm_usage_reader
        .as_ref()
        .ok_or_else(|| ApiError::internal("token usage reader not configured"))?;
    let until = chrono::Utc::now();
    let since = series_since(q.hours, until);
    let series = reader
        .usage_series(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read usage series: {e}")))?;
    Ok(Json(token_series_response(series)))
}

/// Project a series read onto the response face.
///
/// Same rule as [`token_usage_response`]: the range is the one the fold named,
/// never the one the request asked for. Rows are passed through one for one —
/// the projection's only job is the endpoint format.
fn token_series_response(series: cog_core::UsageSeries) -> TokenSeriesResponse {
    TokenSeriesResponse {
        covered_since: series.covered_since.map(|t| t.to_rfc3339()),
        covered_until: series.covered_until.map(|t| t.to_rfc3339()),
        rows: series.rows,
    }
}

// ─── What was spent, in money, over a range the caller chooses ───

/// Query parameters for [`token_spend_handler`].
///
/// Two forms, never mixed: `since` + `until` (RFC3339, both, exclusive end) or
/// a trailing `hours`. Mixing them is refused rather than resolved by
/// precedence — a caller that sent both believes something about which one wins,
/// and guessing wrong hands them a number for a range they did not ask about
/// while every field in the answer still looks plausible.
#[derive(Debug, Deserialize)]
pub struct TokenSpendQuery {
    pub hours: Option<i64>,
    pub since: Option<String>,
    pub until: Option<String>,
}

/// One slice of the ledger that shares a price book.
#[derive(Debug, Serialize)]
pub struct SpendLine {
    pub actor: String,
    pub origin: String,
    pub upstream: String,
    pub api_style: String,
    /// The price list these calls were priced from. `null` on the rows that are
    /// the whole point of [`Self::priced_calls`] — money nobody computed.
    pub price_book: Option<String>,
    /// The book's currency code (`CNY` / `USD`), read off the book's label.
    /// `null` exactly when `price_book` is.
    pub currency: Option<String>,
    pub calls: i64,
    pub failed_calls: i64,
    /// The uncached half of the prompt.
    pub tokens_input: i64,
    /// The cached half. `tokens_input + tokens_cached` is the whole prompt.
    pub tokens_cached: i64,
    pub tokens_output: i64,
    /// In [`Self::currency`]'s millionths, over [`Self::priced_calls`] calls.
    pub cost_micros: Option<i64>,
    pub priced_calls: i64,
}

/// Money per currency, summed over the rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CurrencyTotal {
    pub currency: String,
    pub cost_micros: i64,
    pub priced_calls: i64,
}

/// The two halves of a spend total that must not be added together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpendTotals {
    /// One line per currency. Two currencies are two totals, not an exchange
    /// rate: this platform bills in whatever each upstream's price list says,
    /// and converting here would invent a rate nobody checked.
    pub by_currency: Vec<CurrencyTotal>,
    /// Calls in the range that carried no price at all. Read beside the money:
    /// a total whose unpriced count is not zero is a floor, not a measurement.
    pub unpriced_calls: i64,
}

/// What a range of the ledger cost, at call granularity.
#[derive(Debug, Serialize)]
pub struct TokenSpendResponse {
    /// What the request asked for, echoed so the answer is self-describing.
    pub requested_since: String,
    pub requested_until: String,
    /// The oldest and newest rows the ledger actually held inside that range.
    /// `null` together when nothing was there. A `covered_since` later than
    /// `requested_since` is the retention having swept the earlier part —
    /// reported, because a zero would say "we spent nothing then".
    pub covered_since: Option<String>,
    pub covered_until: Option<String>,
    /// The knob this read's ceiling came from, so a refused wider range can be
    /// traced to the deployment rather than to a magic number.
    pub retention_days: i64,
    pub rows: Vec<SpendLine>,
    pub totals: SpendTotals,
}

/// Resolve the requested range, or say why it cannot be answered.
///
/// The ceiling is the ledger's own retention
/// ([`cog_core::retention::llm_usage_keep_days`]), not a constant: a fixed one
/// drifts the day somebody changes the retention, and the drift is invisible —
/// both the sweep and the read keep working, one of them over a range the other
/// has already deleted. Asking wider than the retention is an error rather than
/// a silent clamp, because the clamp returns the same rows a genuinely quiet
/// month returns.
fn spend_range(
    q: &TokenSpendQuery,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>), ApiError> {
    let days = cog_core::retention::llm_usage_keep_days();
    let reach = chrono::Duration::hours(days.max(1) * 24);
    let ceiling = format!(
        "{} 天（{}={}）",
        days.max(1),
        cog_core::retention::LLM_USAGE_KEEP_DAYS_ENV,
        days
    );
    let parse = |name: &str, raw: &str| {
        chrono::DateTime::parse_from_rfc3339(raw)
            .map(|t| t.with_timezone(&chrono::Utc))
            .map_err(|e| {
                ApiError::bad_request(format!(
                    "{name}={raw} 不是 RFC3339 时刻（例如 2026-11-06T00:00:00+08:00）：{e}"
                ))
            })
    };
    let (since, until) = match (&q.since, &q.until) {
        (Some(s), Some(u)) => (parse("since", s)?, parse("until", u)?),
        (None, None) => {
            let hours = q.hours.unwrap_or_else(default_usage_hours);
            // `hours <= 0` keeps the reading the other spend faces give it: as
            // far back as this read can go. Here that is the retention, because
            // anything older has been swept and would come back empty.
            let span = if hours <= 0 {
                reach
            } else {
                chrono::Duration::hours(hours)
            };
            (now - span, now)
        }
        _ => {
            return Err(ApiError::bad_request(
                "since 与 until 必须成对给出：只给一个时另一半只能靠猜，而猜错的那一半不会有任何一列看得出来",
            ))
        }
    };
    if q.hours.is_some() && (q.since.is_some() || q.until.is_some()) {
        return Err(ApiError::bad_request(
            "hours 与 since/until 只能二选一：两个都给时用哪个都要靠约定，而约定看不见",
        ));
    }
    if until <= since {
        return Err(ApiError::bad_request(format!(
            "until 必须晚于 since：{}, {}",
            since.to_rfc3339(),
            until.to_rfc3339()
        )));
    }
    if until - since > reach {
        return Err(ApiError::bad_request(format!(
            "区间 {} 超过用量账的保留期 {ceiling}；更早的行已经被清扫删掉，再往外读只会答成一个看起来像零的缺口",
            until - since
        )));
    }
    Ok((since, until))
}

/// The currency a price book's label names: the part before `@`.
///
/// The label is written once, by `PriceBook::label`, as `<currency>@<date>`.
/// Reading the prefix rather than storing a second column keeps the two from
/// disagreeing — there is one string, and the currency is derivable from it.
fn book_currency(book: &str) -> String {
    book.split('@').next().unwrap_or(book).to_string()
}

/// What the calls in a caller-chosen range cost, read from the per-call ledger.
pub async fn token_spend_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<TokenSpendQuery>,
) -> Result<Json<TokenSpendResponse>, ApiError> {
    let ledger = state
        .llm_usage
        .as_ref()
        .ok_or_else(|| ApiError::internal("token usage ledger not configured"))?;
    let now = chrono::Utc::now();
    let (since, until) = spend_range(&q, now)?;
    let spend = ledger
        .priced_spend_between(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read the spend ledger: {e}")))?;
    Ok(Json(spend_response(since, until, spend)))
}

/// Sum a spend read into money per currency, plus the calls that carried none.
///
/// One implementation for every surface that shows a total: the spend face and the
/// workbench's invest cell both read this, so the cell and the page it links to
/// cannot disagree about what the same window cost. Totals are accumulated from the
/// same rows the response carries, not from a second query — a separate `SUM` is a
/// second answer to the same question, and the two disagree the first time one of
/// them gains a filter.
pub(crate) fn spend_totals(spend: &cog_core::PricedSpend) -> SpendTotals {
    let mut by_currency: Vec<CurrencyTotal> = Vec::new();
    let mut unpriced_calls = 0i64;
    for r in &spend.rows {
        // Unpriced is counted from the two numbers the ledger itself produced: the
        // calls and the ones that carried money. A row with a book whose window did
        // not cover these calls has `cost_micros: null` and `priced_calls: 0`, so it
        // lands here too — which is right, because "priced from a book that does not
        // cover this hour" is exactly as unpriced as "no book at all".
        unpriced_calls += r.calls - r.priced_calls;
        let currency = r.price_book.as_deref().map(book_currency);
        if let (Some(c), Some(micros)) = (currency, r.cost_micros) {
            match by_currency.iter_mut().find(|t| t.currency == c) {
                Some(t) => {
                    t.cost_micros += micros;
                    t.priced_calls += r.priced_calls;
                }
                None => by_currency.push(CurrencyTotal {
                    currency: c,
                    cost_micros: micros,
                    priced_calls: r.priced_calls,
                }),
            }
        }
    }
    SpendTotals {
        by_currency,
        unpriced_calls,
    }
}

/// Project a ledger spend read onto the response face.
fn spend_response(
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
    spend: cog_core::PricedSpend,
) -> TokenSpendResponse {
    let totals = spend_totals(&spend);
    let rows: Vec<SpendLine> = spend
        .rows
        .into_iter()
        .map(|r| {
            let currency = r.price_book.as_deref().map(book_currency);
            SpendLine {
                actor: r.actor,
                origin: r.origin,
                upstream: r.upstream,
                api_style: r.api_style,
                price_book: r.price_book,
                currency,
                calls: r.calls,
                failed_calls: r.failed_calls,
                tokens_input: r.tokens_input,
                tokens_cached: r.tokens_cached,
                tokens_output: r.tokens_output,
                cost_micros: r.cost_micros,
                priced_calls: r.priced_calls,
            }
        })
        .collect();
    TokenSpendResponse {
        requested_since: since.to_rfc3339(),
        requested_until: until.to_rfc3339(),
        covered_since: spend.covered_since.map(|t| t.to_rfc3339()),
        covered_until: spend.covered_until.map(|t| t.to_rfc3339()),
        retention_days: cog_core::retention::llm_usage_keep_days(),
        rows,
        totals,
    }
}

// ─── Why nothing was spent ───

/// Query parameters for [`token_outcomes_handler`].
///
/// Same trailing window as the spend reads, and the same `hours <= 0` reading:
/// the whole ledger. That is answerable here because the response is one line
/// per verdict kind, not one line per window — so "everything" stays a handful
/// of rows no matter how far back the ledger goes.
#[derive(Debug, Deserialize)]
pub struct TokenOutcomesQuery {
    #[serde(default = "default_usage_hours")]
    pub hours: i64,
}

/// One line of the verdict breakdown.
#[derive(Debug, Serialize)]
pub struct OutcomeLine {
    /// `ok` / `error` / `refused_by_*`.
    pub result: String,
    /// The narrower reason a refusal names, empty when the row was not refused.
    pub refusal_kind: String,
    pub calls: i64,
}

/// One cell of what reading the response found, over the successful calls.
#[derive(Debug, Serialize)]
pub struct MeteringLine {
    /// A cell of the metering vocabulary, or empty for a call whose row was
    /// written before the column existed and so has no reading to report.
    pub outcome: String,
    pub calls: i64,
}

/// How the calls in a range ended, with the range that was actually read.
///
/// This is the face a flat zero needs. Every other reading on the token page
/// answers "how much"; when the answer is nothing, the only useful question is
/// whether the platform was idle or whether every attempt was turned away
/// before it could spend — and those two look identical on a spend face.
///
/// `metering` is the same question asked of the calls that *did* succeed: a
/// successful call whose response carried no usage frame is written with zero
/// tokens, so it counts toward the spend as nothing at all. Reading the cells
/// back is what lets a reader see that a window's token total is a floor rather
/// than a measurement — a fact the total itself cannot carry.
#[derive(Debug, Serialize)]
pub struct TokenOutcomesResponse {
    pub since: String,
    pub until: String,
    pub outcomes: Vec<OutcomeLine>,
    /// One line per metering cell. Empty means no call succeeded in the range,
    /// which is not the same as every call having been metered.
    pub metering: Vec<MeteringLine>,
}

/// How the calls in the trailing window ended, per `(result, refusal_kind)`.
pub async fn token_outcomes_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<TokenOutcomesQuery>,
) -> Result<Json<TokenOutcomesResponse>, ApiError> {
    let ledger = state
        .llm_usage
        .as_ref()
        .ok_or_else(|| ApiError::internal("token usage ledger not configured"))?;
    let until = chrono::Utc::now();
    let since = usage_since(q.hours, until);
    let outcomes = ledger
        .outcomes_between(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read call outcomes: {e}")))?;
    // A failed read is a failure, not "every call was metered": the second
    // answer says the spend beside it is exact, and an empty list would claim
    // that for a range nobody read.
    let metering = ledger
        .metering_between(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read metering outcomes: {e}")))?;
    Ok(Json(outcomes_response(since, until, outcomes, metering)))
}

/// Project the verdict breakdown onto the response face.
///
/// The range is echoed because this read is from the ledger and answers exactly
/// the range it was given: unlike the folded faces there is no coverage to
/// discover, so restating the window is honest here — and a reader that asked
/// for `hours=0` can see the epoch it really started from.
fn outcomes_response(
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
    outcomes: Vec<cog_core::OutcomeCount>,
    metering: Vec<cog_core::MeteringCount>,
) -> TokenOutcomesResponse {
    TokenOutcomesResponse {
        since: since.to_rfc3339(),
        until: until.to_rfc3339(),
        outcomes: outcomes
            .into_iter()
            .map(|row| OutcomeLine {
                result: row.result,
                refusal_kind: row.refusal_kind,
                calls: row.calls,
            })
            .collect(),
        metering: metering
            .into_iter()
            .map(|row| MeteringLine {
                outcome: row.outcome,
                calls: row.calls,
            })
            .collect(),
    }
}

// ─── What the platform decided not to spend ───

/// Query parameters for [`token_account_handler`]. Same trailing window and the
/// same `hours <= 0` reading as the other two reads.
#[derive(Debug, Deserialize)]
pub struct TokenAccountQuery {
    #[serde(default = "default_usage_hours")]
    pub hours: i64,
}

/// One stage's savings: what it chose not to call, and the reason it recorded.
#[derive(Debug, Serialize)]
pub struct SkipLine {
    pub stage: String,
    pub reason: String,
    pub skips: i64,
}

/// One site's windows as the verdict fold judged them.
#[derive(Debug, Serialize)]
pub struct VerdictLine {
    pub actor: String,
    pub verdict: String,
    pub skip_reason: String,
    pub windows: i64,
    pub skips: i64,
    pub tokens_input: i64,
    pub tokens_output: i64,
}

/// The platform's own account of a range it spent nothing in.
///
/// The outcome read says which calls were turned away. It cannot say what was
/// never attempted: a planner that refused to plan because no upstream was
/// healthy makes no call at all, so it lands on the outcome read as nothing and
/// on the spend read as nothing, and the two silences together look like a
/// quiet platform. The savings and the verdicts are the only two faces that
/// know the difference, and until this route existed neither was ever read
/// back — they were written for an account nobody opened.
#[derive(Debug, Serialize)]
pub struct TokenAccountResponse {
    pub since: String,
    pub until: String,
    pub skips: Vec<SkipLine>,
    pub verdicts: Vec<VerdictLine>,
}

/// What the platform recorded deciding not to spend, and how its windows were
/// judged, over the trailing window.
pub async fn token_account_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<TokenAccountQuery>,
) -> Result<Json<TokenAccountResponse>, ApiError> {
    let ledger = state
        .llm_usage
        .as_ref()
        .ok_or_else(|| ApiError::internal("token usage ledger not configured"))?;
    let until = chrono::Utc::now();
    let since = usage_since(q.hours, until);
    // A read that fails is a failure, not an empty list. Both halves answer with
    // "no line" for a state that has a meaning -- nothing was saved, or no
    // window was judged -- and those meanings are exactly what this face exists
    // to distinguish from a silent zero. Degrading a failed read to an empty
    // list would make the platform assert one of them on no evidence, and the
    // caller would have no way to tell.
    let skips = ledger
        .skips_between(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read the recorded savings: {e}")))?;
    let verdicts = ledger
        .verdicts_between(since, until)
        .await
        .map_err(|e| ApiError::internal(format!("failed to read the window verdicts: {e}")))?;
    Ok(Json(account_response(since, until, skips, verdicts)))
}

/// Project the savings and the verdicts onto the response face.
fn account_response(
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
    skips: Vec<cog_core::SkippedWorkLine>,
    verdicts: Vec<cog_core::SpendVerdictLine>,
) -> TokenAccountResponse {
    TokenAccountResponse {
        since: since.to_rfc3339(),
        until: until.to_rfc3339(),
        skips: skips
            .into_iter()
            .map(|row| SkipLine {
                stage: row.stage,
                reason: row.reason,
                skips: row.skips,
            })
            .collect(),
        verdicts: verdicts
            .into_iter()
            .map(|row| VerdictLine {
                actor: row.actor,
                verdict: row.verdict,
                skip_reason: row.skip_reason,
                windows: row.windows,
                skips: row.skips,
                tokens_input: row.tokens_input,
                tokens_output: row.tokens_output,
            })
            .collect(),
    }
}

// ─── Who is spending today ───

/// Query parameters for [`token_spenders_handler`].
///
/// There is deliberately no window parameter: this read answers "who is close
/// to a ceiling", and a ceiling is counted per UTC day by
/// [`cog_core::day::day_start_unix`]. A trailing-hours window would answer a
/// different question and read as if it were the same one — a person who spent
/// 90% of their allowance in the last hour and one who spent it over three days
/// would look identical.
#[derive(Debug, Deserialize)]
pub struct TokenSpendersQuery {
    /// `user` or `ws`, the two levels a call can be charged to.
    #[serde(default = "default_spenders_scope")]
    pub scope: String,
}

fn default_spenders_scope() -> String {
    cog_core::QuotaScope::User.as_str().to_string()
}

/// One subject's day: what the ledger charged it, and the ceiling it declared.
#[derive(Debug, Serialize)]
pub struct SpenderLine {
    /// The person or workspace id, exactly as the ledger resolved it.
    pub id: String,
    /// Tokens the ledger charged this subject today.
    ///
    /// Zero is two different days: with refusals beside it, every call this
    /// subject made was turned away before it could spend anything; without
    /// them, the calls that landed charged nothing — an unmetered response
    /// lands as zero. The refusal column is what tells the two apart.
    pub tokens: u64,
    /// Calls today that this subject's own ceiling turned away.
    ///
    /// A refusal spends nothing, so it moves no counter anywhere; this is the
    /// only number on the row that says a ceiling actually bit. Only the
    /// refusals charged to this subject count — a job's own budget running out
    /// is a different wall with a different remedy, and it is not in here.
    pub refusals: u64,
    /// The ceiling somebody declared for this subject; `null` means nobody did,
    /// which is not the same as "unlimited" — the judge then falls back to the
    /// manager's default. Read [`TokenSpendersResponse::ceilings_readable`]
    /// before taking `null` to mean "nobody declared one".
    pub hard_limit: Option<u64>,
    pub soft_limit: Option<u64>,
    /// `hard_limit - tokens`, floored at zero; `null` when no ceiling is
    /// declared or the ceilings could not be read.
    ///
    /// Computed from the ledger, so it is exact for this read. The judge reads a
    /// counter that the projection refreshes on a period, so the two can differ
    /// by whatever was spent since the last projection pass — a subject can show
    /// ten tokens of room here and still be refused on the next call.
    pub remaining: Option<u64>,
}

/// Everyone the ledger charged today, at one level.
#[derive(Debug, Serialize)]
pub struct TokenSpendersResponse {
    /// The level asked for: `user` or `ws`.
    pub scope: &'static str,
    pub since: String,
    pub until: String,
    /// Tokens charged today across every subject at this level — the sum of
    /// every row, not of the rows a caller might choose to look at first.
    pub total: u64,
    /// Whether `hard_limit` / `soft_limit` mean anything on these rows. `false`
    /// means this deployment has no hierarchy manager, so no ceiling could be
    /// read and every row's null says "unread", not "undeclared".
    pub ceilings_readable: bool,
    /// Descending by tokens, then by refusals, then by id — every tie-break is
    /// read from the row itself, so two reads of the same day return the same
    /// order.
    pub spenders: Vec<SpenderLine>,
}

/// Today's spend and refusals per person or per workspace, beside the ceilings
/// those subjects declared.
///
/// This is the read that closes the operator loop for per-subject quotas: the
/// write face takes an id, but nobody knows which ids to write ceilings for
/// until someone can see who is spending — and nobody learns that a ceiling is
/// too low until someone can see who is being refused against one. Without it
/// the only way to find either is to query the ledger table by hand, which is
/// exactly the manual reconciliation the platform is not supposed to need.
///
/// Only the two levels a call is actually charged to can be asked. The list of
/// those levels comes from the same judge the write face uses, so a level that
/// this route accepts but the proxy never charges would be caught as a
/// mismatch rather than reported as an empty day.
pub async fn token_spenders_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<TokenSpendersQuery>,
) -> Result<Json<TokenSpendersResponse>, ApiError> {
    let scope = cog_core::QuotaScope::parse(&q.scope).ok_or_else(|| {
        let declared: Vec<&str> = cog_core::QuotaScope::cascade_order()
            .iter()
            .filter(|s| crate::scope_enforcement(**s).is_some())
            .map(|s| s.as_str())
            .collect();
        ApiError::bad_request(format!(
            "unknown quota scope {:?}; spend is recorded for {}",
            q.scope,
            declared.join(", ")
        ))
        .with_code("unknown_quota_scope")
    })?;
    if crate::scope_enforcement(scope).is_none() {
        // No call is ever charged to these levels, so there are no spenders to
        // list. Answering an empty list would assert "nobody spent", which is
        // the one thing this platform cannot know — the truth is that nothing
        // records it.
        return Err(ApiError::conflict(format!(
            "no call is ever charged to a {:?} level, so there is no day to read",
            scope
        ))
        .with_code("quota_scope_not_enforced"));
    }

    let ledger = state
        .llm_usage
        .as_ref()
        .ok_or_else(|| ApiError::internal("token usage ledger not configured"))?;
    let until = chrono::Utc::now();
    let since_unix = cog_core::day::day_start_unix(until.timestamp());
    let rows = match scope {
        cog_core::QuotaScope::User => ledger.spend_by_person_since(since_unix).await,
        cog_core::QuotaScope::Workspace => ledger.spend_by_workspace_since(since_unix).await,
        // 全局只有一个主体，所以它列出来正好一行：整个平台今天花掉的量。这一行
        // 必须出现（哪怕值为 0），因为读它的人要拿它去比「配过的上限还剩多少」，
        // 而空列表会读成「今天没人花」，那与「后台工作全停了」同形。
        cog_core::QuotaScope::Global => ledger
            .spend_total_since(since_unix)
            .await
            .map(|total| vec![(crate::GLOBAL_QUOTA_TARGET.to_string(), total)]),
        // Refused above by the same judge that accepted the write face; a new
        // enforceable level has to pass through `scope_enforcement` first.
        cog_core::QuotaScope::Team | cog_core::QuotaScope::Organization => {
            unreachable!("refused by scope_enforcement")
        }
    }
    .map_err(|e| ApiError::internal(format!("failed to read today's spend: {e}")))?;
    // The refusals are a second read of the same day and the same rows: a
    // subject whose calls were all turned away has nothing in the spend fold,
    // so a list built from spend alone would leave out exactly the people the
    // ceilings are biting. A failed read is a failure here for the same reason
    // it is above — an empty list would claim nobody was refused.
    let refusals = match scope {
        cog_core::QuotaScope::User => ledger.refusals_by_person_since(since_unix).await,
        cog_core::QuotaScope::Workspace => ledger.refusals_by_workspace_since(since_unix).await,
        // 全局那一格的拒绝行自己带着 `refused_scope = 'global'`，不需要再折叠一次
        // 身份；今天的拒绝总数就是这一行。
        cog_core::QuotaScope::Global => ledger
            .refusals_total_since(since_unix)
            .await
            .map(|n| vec![(crate::GLOBAL_QUOTA_TARGET.to_string(), n)]),
        cog_core::QuotaScope::Team | cog_core::QuotaScope::Organization => {
            unreachable!("refused by scope_enforcement")
        }
    }
    .map_err(|e| ApiError::internal(format!("failed to read today's refusals: {e}")))?;

    let mut days: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
    for (id, tokens) in rows {
        days.entry(id).or_default().0 = tokens;
    }
    for (id, refusals) in refusals {
        days.entry(id).or_default().1 = refusals;
    }

    let mut spenders = Vec::with_capacity(days.len());
    let mut ceilings_readable = state.hierarchy_manager.is_some();
    for (id, (tokens, refusals)) in days {
        // A ceiling read that fails is a failure, not "nobody declared one":
        // the second answer would tell an operator there is room to spare.
        let limits = match state.hierarchy_manager.as_ref() {
            Some(hierarchy) => hierarchy.limits(scope, &id).await.map_err(|e| {
                ApiError::internal(format!("failed to read the ceiling for {id}: {e}"))
            })?,
            None => {
                ceilings_readable = false;
                None
            }
        };
        spenders.push(SpenderLine {
            id,
            tokens,
            refusals,
            remaining: limits.as_ref().map(|l| l.hard_limit.saturating_sub(tokens)),
            hard_limit: limits.as_ref().map(|l| l.hard_limit),
            soft_limit: limits.as_ref().map(|l| l.soft_limit),
        });
    }

    Ok(Json(spenders_response(
        scope,
        chrono::DateTime::<chrono::Utc>::from_timestamp(since_unix, 0)
            .unwrap_or(chrono::DateTime::UNIX_EPOCH),
        until,
        spenders,
        ceilings_readable,
    )))
}

/// Order and total the rows onto the response face.
///
/// The total is taken from every row before anything is ordered: it is the
/// day's spend, and a caller that reads only the first page of `spenders` still
/// has to be able to tell how much the rest accounts for.
fn spenders_response(
    scope: cog_core::QuotaScope,
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
    mut spenders: Vec<SpenderLine>,
    ceilings_readable: bool,
) -> TokenSpendersResponse {
    let total = spenders.iter().map(|row| row.tokens).sum();
    // Ties break on refusals before the id: among the subjects that were
    // turned away without spending anything, the one refused most often is the
    // one the reader is looking for.
    spenders.sort_by(|a, b| {
        b.tokens
            .cmp(&a.tokens)
            .then_with(|| b.refusals.cmp(&a.refusals))
            .then_with(|| a.id.cmp(&b.id))
    });
    TokenSpendersResponse {
        scope: scope.as_str(),
        since: since.to_rfc3339(),
        until: until.to_rfc3339(),
        total,
        ceilings_readable,
        spenders,
    }
}

// ─── Task Metrics ───

pub async fn task_metrics_handler(
    State(state): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
) -> Result<Json<cog_core::observability::TaskMetrics>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let metrics = gateway
        .get_task_metrics(&task_id)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get task metrics: {e}")))?;
    Ok(Json(metrics))
}

// ─── Task Logs ───

pub async fn task_logs_handler(
    State(state): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
    Query(query): Query<LogsQuery>,
) -> Result<Json<Vec<cog_core::observability::LogEntry>>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let logs = gateway
        .get_task_logs(&task_id, query.limit)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get task logs: {e}")))?;
    Ok(Json(logs))
}

// ─── Events (Historical query) ───

pub async fn events_handler(
    State(state): State<Arc<GatewayState>>,
    Query(filter): Query<EventFilterQuery>,
) -> Result<Json<Vec<cog_core::AgentEvent>>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;

    let event_types = filter.event_types.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .collect::<Vec<_>>()
    });

    let level_filter = filter.level.as_deref();

    let mut event_filter = cog_core::EventFilter {
        agent_id: filter.agent_id,
        task_id: filter.task_id,
        squad_id: filter.squad_id,
        event_types,
        since: None,
    };

    // A query without a window asks for what the store promises: the
    // deployment's declared floor, widened by the longest question already
    // asked. A query that asks wider moves that floor, and the next sweep beat
    // takes the new number — the reader's need is the input, not a constant
    // copied beside it.
    let knob = cog_core::retention::OBSERVABILITY_EVENTS_READ_WINDOW_KNOB;
    let seed = cog_core::retention::read_window_seed(
        std::env::var(cog_core::retention::OBSERVABILITY_EVENTS_READ_WINDOW_ENV)
            .ok()
            .as_deref(),
        cog_core::retention::OBSERVABILITY_EVENTS_READ_WINDOW_DEFAULT_SECS,
    );
    let (floor, _) =
        cog_core::retention::read_window_secs(seed, cog_core::retention::longest_read_span(knob));
    let asked = filter.window_secs.filter(|secs| *secs > 0).unwrap_or(floor);
    cog_core::retention::record_read_span(knob, asked);
    event_filter.since =
        Some(chrono::Utc::now() - chrono::Duration::seconds(asked.min(i64::MAX as u64) as i64));

    let mut stream = gateway
        .subscribe_events(event_filter)
        .await
        .map_err(|e| ApiError::internal(format!("failed to subscribe to events: {e}")))?;

    // Collect up to 1000 events from the stream (bounded historical query).
    let mut events = Vec::new();
    let mut count = 0usize;
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                if event_matches_level(&event, level_filter) {
                    events.push(event);
                    count += 1;
                    if count >= 1000 {
                        break;
                    }
                }
            }
            Err(e) => {
                tracing::warn!("event stream error: {e}");
                break;
            }
        }
    }

    Ok(Json(events))
}

// ─── Events Live (WebSocket) ───

pub async fn events_live_handler(
    State(state): State<Arc<GatewayState>>,
    ws: WebSocketUpgrade,
    Query(filter): Query<EventFilterQuery>,
) -> Response {
    let gateway = match state.observability_gateway.clone() {
        Some(g) => g,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "observability gateway not configured"})),
            )
                .into_response();
        }
    };

    let event_types = filter.event_types.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .collect::<Vec<_>>()
    });
    let level_filter = filter.level;

    let event_filter = cog_core::EventFilter {
        agent_id: filter.agent_id,
        task_id: filter.task_id,
        squad_id: filter.squad_id,
        event_types,
        since: None,
    };

    ws.on_upgrade(move |socket| handle_events_live_ws(socket, gateway, event_filter, level_filter))
}

use axum::extract::ws::{Message as WsMessage, WebSocket};

async fn handle_events_live_ws(
    mut socket: WebSocket,
    gateway: Arc<dyn cog_core::ObservabilityGateway>,
    filter: cog_core::EventFilter,
    level_filter: Option<String>,
) {
    let mut stream = match gateway.subscribe_events(filter).await {
        Ok(s) => s,
        Err(e) => {
            let _ = socket
                .send(WsMessage::Text(
                    format!("{{\"error\":\"failed to subscribe: {e}\"}}").into(),
                ))
                .await;
            return;
        }
    };

    let level = level_filter.as_deref();
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => {
                if !event_matches_level(&event, level) {
                    continue;
                }
                let json = match serde_json::to_string(&event) {
                    Ok(j) => j,
                    Err(e) => {
                        tracing::warn!("event serialization error: {e}");
                        continue;
                    }
                };
                if socket.send(WsMessage::Text(json.into())).await.is_err() {
                    break; // Client disconnected
                }
            }
            Err(e) => {
                tracing::warn!("event stream error: {e}");
                let _ = socket
                    .send(WsMessage::Text(
                        format!("{{\"error\":\"stream error: {e}\"}}").into(),
                    ))
                    .await;
            }
        }
    }
}

// ─── Task Checkpoint ───

pub async fn task_checkpoint_handler(
    State(state): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let checkpoint = gateway
        .get_task_checkpoint(&task_id)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get task checkpoint: {e}")))?;
    match checkpoint {
        Some(cp) => Ok(Json(serde_json::json!(cp))),
        None => Ok(Json(
            serde_json::json!({"task_id": task_id, "checkpoint": null}),
        )),
    }
}

// ─── Snapshot URL ───

pub async fn snapshot_url_handler(
    State(state): State<Arc<GatewayState>>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let url = gateway
        .get_snapshot_url(&snapshot_id)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get snapshot url: {e}")))?;
    Ok(Json(
        serde_json::json!({"snapshot_id": snapshot_id, "url": url}),
    ))
}

// ─── Raw Log Index ───

#[derive(Debug, Deserialize)]
pub struct RawLogIndexQuery {
    pub stream: String,
    pub date: String,
}

pub async fn raw_log_index_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<RawLogIndexQuery>,
) -> Result<Json<Vec<cog_core::observability::RawLogIndex>>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let date =
        chrono::NaiveDate::parse_from_str(&query.date, cog_core::DAY_KEY_FORMAT).map_err(|e| {
            ApiError::bad_request(format!("invalid date format (expected YYYY-MM-DD): {e}"))
        })?;
    let index = gateway
        .get_raw_log_index(&query.stream, date)
        .await
        .map_err(|e| ApiError::internal(format!("failed to get raw log index: {e}")))?;
    Ok(Json(index))
}

// ─── Event Severity Helpers ───

/// Map an AgentEvent to a severity level string for filtering.
pub fn agent_event_level(event: &cog_core::AgentEvent) -> &'static str {
    use cog_core::AgentEvent;
    match event {
        AgentEvent::AgentError { severity, .. } => match severity {
            cog_core::ErrorSeverity::Warning => "warn",
            cog_core::ErrorSeverity::Critical | cog_core::ErrorSeverity::Fatal => "error",
        },
        AgentEvent::ResourceAlert { .. } => "warn",
        // Both comparisons read the wire spelling: the values in these frames
        // are the snake_case names (`AgentState` and `TaskStatus` are both
        // `#[serde(rename_all = "snake_case")]`, and every producer writes
        // them that way), so a PascalCase literal here matches nothing and the
        // filter quietly stops marking the one level it exists to mark. The
        // tests below derive both spellings from the enums instead of
        // restating them.
        AgentEvent::StateChange { to, .. } if to == "dead" => "error",
        AgentEvent::TaskStatusChange { status, .. } if status == "failed" => "error",
        _ => "info",
    }
}

/// Check if an event matches the requested level filter.
pub fn event_matches_level(event: &cog_core::AgentEvent, level: Option<&str>) -> bool {
    let Some(level) = level else { return true };
    let event_level = agent_event_level(event);
    match level {
        "error" => event_level == "error",
        "warn" => event_level == "warn" || event_level == "error",
        "info" => true,
        _ => true,
    }
}

// ─── Search ───

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    #[serde(default = "default_search_index")]
    pub index: String,
}

fn default_search_limit() -> usize {
    20
}

fn default_search_index() -> String {
    "cogneva".into()
}

pub async fn search_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let backend = state
        .search_backend
        .as_ref()
        .ok_or_else(|| ApiError::internal("search backend not configured"))?;
    let index = query.index.clone();
    let results = backend
        .search(&[index], &query.q, query.limit)
        .await
        .map_err(|e| ApiError::internal(format!("search failed: {e}")))?;
    Ok(Json(serde_json::json!({
        "query": query.q,
        "index": query.index,
        "results": results,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{ActiveAlertSource, PersistedAlert};

    fn firing(severity: &str) -> PersistedAlert {
        PersistedAlert {
            rule: "llm_upstream_pool_down".into(),
            dedup_key: "llm_upstream_pool_down".into(),
            severity: severity.into(),
            state: "firing".into(),
            message: "pool down".into(),
            labels: serde_json::json!({}),
            fired_at: chrono::Utc::now(),
            last_seen_at: None,
            acknowledged_at: None,
            acknowledged_by: None,
        }
    }

    fn overview(health: cog_core::ClusterHealth) -> cog_core::observability::ClusterOverview {
        cog_core::observability::ClusterOverview {
            total_agents: None,
            active_agents: None,
            total_tasks: None,
            active_tasks: None,
            queued_tasks: None,
            failed_tasks: None,
            avg_task_duration_ms: None,
            cluster_health: health,
            timestamp: chrono::Utc::now(),
            total_squads: None,
            active_squads: None,
        }
    }

    /// Answers with a fixed row set, or with a failed read when `firing` is
    /// `None`.
    struct StubSource {
        firing: Option<Vec<PersistedAlert>>,
    }

    #[async_trait::async_trait]
    impl ActiveAlertSource for StubSource {
        async fn list_active_alerts(&self, _limit: i64) -> Option<Vec<PersistedAlert>> {
            self.firing.clone()
        }

        async fn acknowledge_alert(
            &self,
            _dedup_key: &str,
            _by: &str,
        ) -> Result<Option<chrono::DateTime<chrono::Utc>>, String> {
            Err("the health verdict never acknowledges anything".into())
        }
    }

    // An empty answer and a failed read must stay apart: only a source that
    // looked and found nothing earns `healthy`.
    #[tokio::test]
    async fn a_source_that_answered_with_nothing_reads_healthy() {
        let source: Arc<dyn ActiveAlertSource> = Arc::new(StubSource {
            firing: Some(vec![]),
        });
        let mut o = overview(cog_core::ClusterHealth::Unknown);
        apply_alert_verdict(&mut o, Some(&source)).await;
        assert_eq!(o.cluster_health, cog_core::ClusterHealth::Healthy);
    }

    // A firing row is the claim that something is wrong, whatever its severity
    // — `info` is the resolution vocabulary and does not appear in firing rows.
    #[tokio::test]
    async fn anything_firing_reads_degraded_whatever_its_severity() {
        for severity in ["critical", "warning"] {
            let source: Arc<dyn ActiveAlertSource> = Arc::new(StubSource {
                firing: Some(vec![firing(severity)]),
            });
            let mut o = overview(cog_core::ClusterHealth::Unknown);
            apply_alert_verdict(&mut o, Some(&source)).await;
            assert_eq!(
                o.cluster_health,
                cog_core::ClusterHealth::Degraded,
                "severity {severity}"
            );
        }
    }

    // No source is no evidence: it must not be promoted to `healthy`, or
    // "never looked" and "looked, all clear" arrive as the same string.
    #[tokio::test]
    async fn without_a_source_the_verdict_stays_unknown() {
        let mut o = overview(cog_core::ClusterHealth::Unknown);
        apply_alert_verdict(&mut o, None).await;
        assert_eq!(o.cluster_health, cog_core::ClusterHealth::Unknown);
    }

    // A source whose query errored is not evidence of quiet either. This is the
    // path that makes the difference observable: the only production source
    // reports a failed lookup as `None`.
    #[tokio::test]
    async fn a_source_that_could_not_read_leaves_the_verdict_unknown() {
        let source: Arc<dyn ActiveAlertSource> = Arc::new(StubSource { firing: None });
        let mut o = overview(cog_core::ClusterHealth::Unknown);
        apply_alert_verdict(&mut o, Some(&source)).await;
        assert_eq!(o.cluster_health, cog_core::ClusterHealth::Unknown);
    }

    /// The endpoints on the wire are the range the rollup answered for.
    ///
    /// Not restating the request: the read is the only input this projection
    /// has, and the test hands it endpoints that no request could have produced
    /// (a range that stops an hour short of now) so that an echoed request
    /// window cannot pass.
    #[test]
    fn the_endpoints_on_the_wire_are_the_range_the_read_covered() {
        let covered_since = chrono::Utc::now() - chrono::Duration::hours(3);
        let covered_until = chrono::Utc::now() - chrono::Duration::hours(1);
        let response = token_usage_response(cog_core::RolledUsage {
            covered_since: Some(covered_since),
            covered_until: Some(covered_until),
            actors: vec![],
        });
        assert_eq!(
            response.covered_since.as_deref(),
            Some(covered_since.to_rfc3339().as_str())
        );
        assert_eq!(
            response.covered_until.as_deref(),
            Some(covered_until.to_rfc3339().as_str())
        );
    }

    /// A request no folded window falls inside answers with no endpoints at
    /// all, rather than with the window that was asked for.
    ///
    /// This is the shape the trailing-hour request hits: the fold lands closed
    /// hours, so the newest hour is by construction not covered yet. Reporting
    /// it as covered would make an unfolded hour and an idle one the same
    /// answer.
    #[test]
    fn an_uncovered_range_answers_without_endpoints() {
        let response = token_usage_response(cog_core::RolledUsage {
            covered_since: None,
            covered_until: None,
            actors: vec![],
        });
        assert!(
            response.covered_since.is_none() && response.covered_until.is_none(),
            "a range the fold never reached must not be reported as a covered one"
        );
        let wire = serde_json::to_value(&response).expect("the response serializes");
        assert!(
            wire["covered_since"].is_null() && wire["covered_until"].is_null(),
            "both endpoints read as null on the wire, got: {wire}"
        );
    }

    /// Asking for everything must not come back as the cap.
    ///
    /// The defect this pins is a cumulative figure that is really seven days:
    /// clamping turns the request into a number nobody can tell from a total,
    /// and the only thing that would say otherwise is the coverage the response
    /// carries — which a caller that asked for "all of it" has no reason to
    /// re-check. The two answers have to differ before they leave this function.
    #[test]
    fn asking_for_everything_does_not_come_back_as_the_cap() {
        let until = chrono::DateTime::parse_from_rfc3339("2026-10-30T00:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc);
        assert_eq!(
            usage_since(0, until),
            chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
            "`hours=0` means the whole fold; answering it with the cap would publish a \
             seven-day sum under the name of a cumulative total"
        );
        assert!(
            usage_since(-5, until) < usage_since(usage_reach_hours(), until),
            "a negative window means the same thing as zero and must not fall back to a \
             trailing one"
        );
        assert_eq!(
            usage_since(10_000, until),
            until - chrono::Duration::hours(usage_reach_hours()),
            "a trailing window past the reach is answered at the reach — and the reach is the \
             ledger's own retention, not a number written here"
        );
        // 上限必须来自保留期：账本留着多久，这一读就能问多久。写死的七日上限
        // 在保留期被改成三十天之后仍会说七天，而两边的读数各自都正常。
        assert_eq!(
            usage_reach_hours(),
            cog_core::retention::llm_usage_keep_days().max(1) * 24
        );
        assert_eq!(
            usage_since(6, until),
            until - chrono::Duration::hours(6),
            "a window inside the cap is answered as asked"
        );
    }

    /// 两个读法在 `hours <= 0` 上故意不同：求和那一面返回的是每个执行者一行，
    /// 全史也只有几十行，可以问「一共花了多少」；逐窗口那一面返回的是每个窗口
    /// 的每一行，区间有多宽响应就有多大，全史是五万多行、画不出来。把前者那条
    /// 「0 即全史」照搬到后者，就是让一个 60 秒轮询的端点把整本折叠账拉出来。
    #[test]
    fn the_per_window_read_answers_an_unbounded_ask_at_the_widest_it_can_draw() {
        let until = chrono::DateTime::parse_from_rfc3339("2026-10-30T00:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc);
        assert_eq!(
            series_since(0, until),
            until - chrono::Duration::hours(MAX_SERIES_HOURS),
            "asking for everything on the per-window read is answered at the widest span \
             the bars can be drawn for"
        );
        assert_eq!(series_since(-5, until), series_since(0, until));
        assert_eq!(
            series_since(24, until),
            until - chrono::Duration::hours(24),
            "and a bounded window is the same window on both reads"
        );
        assert_eq!(series_since(10_000, until), series_since(0, until));
    }

    /// The handler has no way to put the request's window into the answer: the
    /// response is built in one place, and that place is handed the read only.
    ///
    /// Read off the sources rather than asserted about the projection alone,
    /// because the defect this replaces was not a wrong endpoint: the handler
    /// computed the window from its own arguments and echoed it, and a test of
    /// the projection's arithmetic passes just as well while the handler builds
    /// the response itself.
    #[test]
    fn the_handler_answers_from_the_read_not_from_its_own_window() {
        let src = include_str!("observability.rs");
        let production = match src.rfind("\n#[cfg(test)]") {
            Some(at) => &src[..at],
            None => src,
        };
        let handler_start = production
            .find("pub async fn token_usage_by_actor_handler")
            .expect("the token usage handler is in this file");
        let handler = &production[handler_start..];
        let handler = match handler.find("\n}\n") {
            Some(end) => &handler[..end],
            None => handler,
        };
        assert!(
            handler.contains("token_usage_response(rolled)"),
            "the handler must project the read through the one place that builds the response"
        );
        assert!(
            !handler.contains("to_rfc3339") && !handler.contains("TokenUsageResponse {"),
            "the handler builds the response itself; the range it echoes would be its own request \
             window, which is not the range the rollup answered for"
        );
    }

    /// 结局分布不能把两种拒绝合成一行：`refused_by_circuit` 与
    /// `error` 各有自己的修法，而 `refusal_kind` 是它们各自唯一的细分。
    ///
    /// 这条测的是投影：分区在 SQL 里做，但投影一旦把 `refusal_kind` 丢掉，
    /// 两行会显示成同一件事，读数面就再也说不出「哪一种」。
    #[test]
    fn the_outcome_projection_keeps_the_kind_that_tells_two_refusals_apart() {
        let until = chrono::DateTime::parse_from_rfc3339("2026-10-30T00:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc);
        let since = until - chrono::Duration::hours(6);
        let response = outcomes_response(
            since,
            until,
            vec![
                cog_core::OutcomeCount {
                    result: "error".into(),
                    refusal_kind: "quota_cycle".into(),
                    calls: 3,
                },
                cog_core::OutcomeCount {
                    result: "error".into(),
                    refusal_kind: "entitlement".into(),
                    calls: 1,
                },
            ],
            // This test is about the refusal kinds, so the metering half is
            // present but empty: an empty half must not make the other half
            // vanish from the wire.
            Vec::new(),
        );
        let wire = serde_json::to_value(&response).expect("the response serializes");
        assert_eq!(
            response.since,
            since.to_rfc3339(),
            "this read answers the range it was given, so it echoes it"
        );
        assert_eq!(response.until, until.to_rfc3339());
        assert_eq!(
            wire["outcomes"].as_array().map(|a| a.len()),
            Some(2),
            "two kinds of the same result stay two lines"
        );
        assert_eq!(wire["outcomes"][0]["refusal_kind"], "quota_cycle");
        assert_eq!(wire["outcomes"][1]["refusal_kind"], "entitlement");
    }

    /// 成功的调用里，有多少真的量到了：这一格是紧挨着的 token 合计的成色。
    ///
    /// 一次 `ok` 的调用如果没有带回用量帧，落库时就是三个 0——和一次真的没花
    /// 钱的调用在账上是同一行。合计算出来的两个窗口因此一模一样，而这个数在
    /// 两处都读作「零花销」。所以计量的一格必须单独出现在读数面上：`absent`
    /// 或 `not_asked` 越长，旁边的 token 合计就越是一个下界而不是一个测量。
    #[test]
    fn the_outcome_projection_carries_the_calls_whose_numbers_never_came() {
        let until = chrono::DateTime::parse_from_rfc3339("2026-10-30T00:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc);
        let since = until - chrono::Duration::hours(6);
        let response = outcomes_response(
            since,
            until,
            vec![cog_core::OutcomeCount {
                result: "ok".into(),
                refusal_kind: String::new(),
                calls: 10,
            }],
            vec![
                cog_core::MeteringCount {
                    outcome: "read".into(),
                    calls: 7,
                },
                cog_core::MeteringCount {
                    outcome: "absent".into(),
                    calls: 3,
                },
            ],
        );
        let wire = serde_json::to_value(&response).expect("the response serializes");
        assert_eq!(
            wire["metering"].as_array().map(|a| a.len()),
            Some(2),
            "a call the upstream answered silently stays its own line"
        );
        assert_eq!(wire["metering"][0]["outcome"], "read");
        assert_eq!(wire["metering"][1]["outcome"], "absent");
        assert_eq!(wire["metering"][1]["calls"], 3);
        // 空的计量面必须还是 `[]`，不能省掉：省掉之后「这一窗没有一次成功调用」
        // 与「这一读根本没装」在 JSON 上是同一个形状。
        let empty = outcomes_response(since, until, Vec::new(), Vec::new());
        let wire = serde_json::to_value(&empty).expect("the response serializes");
        assert_eq!(wire["metering"], serde_json::json!([]));
        assert_eq!(wire["outcomes"], serde_json::json!([]));
    }

    /// 账本的两半必须各自出现：省下的与判定过的是两种事实，一栏空了不代表
    /// 另一栏也空。把空数组当成「没有可说的」会在两种情况下读成同一个零：
    /// 一次都没省（没有 savings），与一次都没判（verdict 面没在跑）。
    ///
    /// 判定里那两种缺失也得分得开：`unexpected_absent` 是「它本来在花、现在
    /// 无声无息地停了」，`documented_skip` 是「停得有记录」——合成一行就等于
    /// 把要修的那一种藏进正常的那一种里。
    #[test]
    fn the_account_projection_keeps_savings_and_judgements_apart() {
        let until = chrono::DateTime::parse_from_rfc3339("2026-10-30T00:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc);
        let since = until - chrono::Duration::hours(24);
        let response = account_response(
            since,
            until,
            vec![cog_core::SkippedWorkLine {
                stage: "planner".into(),
                reason: "upstream_unavailable".into(),
                skips: 31_151,
            }],
            vec![
                cog_core::SpendVerdictLine {
                    actor: "agent:planner".into(),
                    verdict: "documented_skip".into(),
                    skip_reason: "upstream_unavailable".into(),
                    windows: 24,
                    skips: 31_151,
                    tokens_input: 0,
                    tokens_output: 0,
                },
                cog_core::SpendVerdictLine {
                    actor: "agent:generator".into(),
                    verdict: "unexpected_absent".into(),
                    skip_reason: String::new(),
                    windows: 3,
                    skips: 0,
                    tokens_input: 0,
                    tokens_output: 0,
                },
            ],
        );
        let wire = serde_json::to_value(&response).expect("the response serializes");
        assert_eq!(response.since, since.to_rfc3339());
        assert_eq!(response.until, until.to_rfc3339());
        assert_eq!(wire["skips"].as_array().map(|a| a.len()), Some(1));
        assert_eq!(wire["skips"][0]["reason"], "upstream_unavailable");
        assert_eq!(
            wire["verdicts"].as_array().map(|a| a.len()),
            Some(2),
            "the two silences stay two lines"
        );
        assert_eq!(wire["verdicts"][0]["verdict"], "documented_skip");
        assert_eq!(wire["verdicts"][1]["verdict"], "unexpected_absent");
        assert_eq!(wire["verdicts"][1]["skip_reason"], "");
    }

    fn spend_row(
        actor: &str,
        window_start: chrono::DateTime<chrono::Utc>,
        input: i64,
    ) -> cog_core::WindowSpend {
        cog_core::WindowSpend {
            window_start,
            window_end: window_start + chrono::Duration::hours(1),
            actor: actor.into(),
            origin: "resident_worker".into(),
            upstream: "pool".into(),
            api_style: "openai".into(),
            calls: 1,
            failed_calls: 0,
            tokens_input: input,
            tokens_output: 0,
            tokens_cached: 0,
            // 这一行是 openai 兼容面：那一家不报写缓存那一半，缺读数就是 None。
            tokens_cache_created: None,
        }
    }

    /// The series' endpoints come from the read, exactly as the summed read's
    /// do — the two faces answer different questions but cover the same range.
    #[test]
    fn the_series_endpoints_are_the_range_the_read_covered() {
        let covered_since = chrono::Utc::now() - chrono::Duration::hours(5);
        let covered_until = chrono::Utc::now() - chrono::Duration::minutes(10);
        let response = token_series_response(cog_core::UsageSeries {
            covered_since: Some(covered_since),
            covered_until: Some(covered_until),
            rows: vec![],
        });
        assert_eq!(
            response.covered_since.as_deref(),
            Some(covered_since.to_rfc3339().as_str())
        );
        assert_eq!(
            response.covered_until.as_deref(),
            Some(covered_until.to_rfc3339().as_str())
        );
    }

    /// Two actors in the same window stay two rows.
    ///
    /// The defect this rules out is silent: summing here would answer the
    /// question the *other* endpoint exists for, and the face drawing a time
    /// axis would lose every intermediate point while still looking plausible —
    /// one row per window, all correct totals.
    #[test]
    fn a_series_keeps_one_row_per_actor_within_a_window() {
        let window = chrono::Utc::now() - chrono::Duration::hours(2);
        let response = token_series_response(cog_core::UsageSeries {
            covered_since: Some(window),
            covered_until: Some(window + chrono::Duration::hours(1)),
            rows: vec![
                spend_row("self_review", window, 10),
                spend_row("agent:dev", window, 90),
            ],
        });
        assert_eq!(
            response.rows.len(),
            2,
            "rows from one window must not be merged into a single line"
        );
        assert_eq!(
            response.rows.iter().map(|r| r.tokens_input).sum::<i64>(),
            100,
            "the projection passes the counts through unchanged"
        );
        assert!(
            response.rows.iter().all(|r| r.window_start == window),
            "each row keeps the window it was folded in"
        );
    }

    /// The series handler, like the summed one, cannot echo its own request
    /// window: it hands the read to the single place that builds the response.
    #[test]
    fn the_series_handler_answers_from_the_read_not_from_its_own_window() {
        let src = include_str!("observability.rs");
        let production = match src.rfind("\n#[cfg(test)]") {
            Some(at) => &src[..at],
            None => src,
        };
        let handler_start = production
            .find("pub async fn token_series_handler")
            .expect("the series handler is in this file");
        let handler = &production[handler_start..];
        let handler = match handler.find("\n}\n") {
            Some(end) => &handler[..end],
            None => handler,
        };
        assert!(
            handler.contains("token_series_response(series)"),
            "the handler must project the read through the one place that builds the response"
        );
        assert!(
            !handler.contains("to_rfc3339") && !handler.contains("TokenSeriesResponse {"),
            "the handler builds the response itself; the range it echoes would be its own request \
             window, which is not the range the rollup answered for"
        );
    }

    /// The two states this filter calls "error" are matched by the spelling the
    /// frame carries, and that spelling comes from the enum's serde name --
    /// not from `Debug`. Deriving the expected string here means a change to
    /// either convention fails this test instead of leaving a comparison that
    /// matches nothing: nothing about a literal that never matches is visible
    /// at runtime, the filter just stops marking the level it exists for.
    #[test]
    fn the_error_levels_are_matched_by_the_spelling_the_frames_carry() {
        let dead = serde_json::to_value(cog_core::AgentState::Dead)
            .expect("plain enum")
            .as_str()
            .expect("unit variant")
            .to_string();
        let failed = serde_json::to_value(cog_core::TaskStatus::Failed)
            .expect("plain enum")
            .as_str()
            .expect("unit variant")
            .to_string();

        let dead_frame = cog_core::AgentEvent::StateChange {
            agent_id: "a-1".into(),
            from: "active".into(),
            to: dead,
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        };
        let failed_frame = cog_core::AgentEvent::TaskStatusChange {
            task_id: "t-1".into(),
            status: failed,
            agent_id: None,
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        };

        assert_eq!(agent_event_level(&dead_frame), "error");
        assert_eq!(agent_event_level(&failed_frame), "error");
    }

    /// 池子读出来的那一面要带上额度窗的账：允许了多少、花出去多少。
    ///
    /// 这一条测的是「账真的上了线」：快照里 `pace` 已经有了，投影一旦丢掉
    /// 它，花销面就只剩「花了多少」，答不出「这一窗还有多少可用」——而对
    /// 订阅制的上游来说，后者才是唯一存在的成本读数。
    #[test]
    fn a_measured_window_reaches_the_reading_with_its_allowance_and_its_spend() {
        let status = cog_core::LlmPoolStatus {
            unavailable: false,
            evidenced_recovery_unix: 0,
            next_attempt_unix: 0,
            unavailable_upstreams: Vec::new(),
            pool_size: 2,
            declared_min_healthy: None,
            quota_window_secs: 2_592_000,
            upstream_evidence: Vec::new(),
            last_success_unix: 0,
            pace: cog_core::LlmWindowPace {
                measured_upstreams: 2,
                allowance_tokens: 16_620_054,
                spent_tokens: 4_000_000,
                reserve_tokens: 358_943,
                reserve_population: 533,
                window_secs: 2_592_000,
                window_start_unix: 1_793_028_477,
            },
        };
        let wire = serde_json::to_value(pool_view(&status)).expect("the view serializes");
        let pace = &wire["pace"];
        assert_eq!(pace["measured_upstreams"], 2);
        assert_eq!(pace["allowance_tokens"], 16_620_054);
        assert_eq!(pace["spent_tokens"], 4_000_000);
        assert_eq!(
            pace["reserve_population"], 533,
            "预留量测自几个整活，要跟着预留量一起走"
        );
        let start: chrono::DateTime<chrono::Utc> =
            serde_json::from_value(pace["window_start_at"].clone()).expect("一个时刻");
        assert_eq!(
            Some(start),
            chrono::DateTime::from_timestamp(1_793_028_477, 0),
            "窗口起点是读者算出「这一窗还有多久」的唯一依据"
        );
    }

    /// 没量过整窗时，额度那一格必须是「没有读数」，不是「额度为 0」。
    ///
    /// 两者在界面上差一个词，在含义上差一个结论：零额度会让读者以为配额已
    /// 经用光、池子本该停摆，而真相是这台进程还没有见过一个烧穿的窗口。
    #[test]
    fn a_pool_that_has_measured_no_window_reports_no_reading_not_a_zero_allowance() {
        let status = cog_core::LlmPoolStatus {
            unavailable: false,
            evidenced_recovery_unix: 0,
            next_attempt_unix: 0,
            unavailable_upstreams: Vec::new(),
            pool_size: 2,
            declared_min_healthy: None,
            quota_window_secs: 0,
            upstream_evidence: Vec::new(),
            last_success_unix: 0,
            pace: cog_core::LlmWindowPace::default(),
        };
        let wire = serde_json::to_value(pool_view(&status)).expect("the view serializes");
        assert!(wire["pace"].is_null(), "没有量过就是没有读数：{wire}");
    }

    /// 各家窗不一样时，报表带着额度但不带窗：读者拿不到一个起点去加窗长。
    ///
    /// 一条 7 天窗、一条 30 天窗同时量到时，「最早起点 + 最长窗长」会拼出一个
    /// 两条都不在的窗，而读者拿它算出来的窗尾是一个没人观测过的时刻。额度那一
    /// 半仍然有效（它是各家摊平量之和），所以两格必须分开读：有额度、没有窗。
    #[test]
    fn a_report_over_members_in_different_windows_carries_the_allowance_and_no_window() {
        let status = cog_core::LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 1_793_807_999,
            next_attempt_unix: 0,
            unavailable_upstreams: Vec::new(),
            pool_size: 2,
            declared_min_healthy: None,
            quota_window_secs: 2_592_000,
            upstream_evidence: Vec::new(),
            last_success_unix: 0,
            pace: cog_core::LlmWindowPace {
                measured_upstreams: 2,
                allowance_tokens: 16_737_629,
                spent_tokens: 0,
                reserve_tokens: 358_943,
                reserve_population: 533,
                window_secs: 0,
                window_start_unix: 0,
            },
        };
        let wire = serde_json::to_value(pool_view(&status)).expect("the view serializes");
        let pace = &wire["pace"];
        assert_eq!(pace["allowance_tokens"], 16_737_629);
        assert_eq!(pace["window_secs"], 0);
        assert!(
            pace["window_start_at"].is_null(),
            "没有共同的窗就没有起点，读者不能拿它加窗长：{wire}"
        );
    }

    // ─── 花多少钱：区间与金额 ───

    fn spend_query(
        hours: Option<i64>,
        since: Option<&str>,
        until: Option<&str>,
    ) -> TokenSpendQuery {
        TokenSpendQuery {
            hours,
            since: since.map(str::to_string),
            until: until.map(str::to_string),
        }
    }

    /// 两种写法互斥。两个都给时没有哪一个是「显然」该赢的，而选错的那一次
    /// 返回的是另一个区间的钱——每一列都还在、都对，只是答的不是它问的。
    #[test]
    fn a_range_and_a_trailing_window_are_refused_together() {
        let now = chrono::Utc::now();
        let err = spend_range(
            &spend_query(
                Some(6),
                Some("2026-11-06T00:00:00Z"),
                Some("2026-11-06T06:00:00Z"),
            ),
            now,
        )
        .expect_err("两种写法同时给必须被拒");
        assert!(err.message.contains("二选一"), "{}", err.message);
    }

    /// 只给一半同样拒。另一半只能靠猜，而猜出来的区间与真区间在响应里长得一样。
    #[test]
    fn half_a_range_is_refused() {
        let now = chrono::Utc::now();
        for q in [
            spend_query(None, Some("2026-11-06T00:00:00Z"), None),
            spend_query(None, None, Some("2026-11-06T06:00:00Z")),
        ] {
            let err = spend_range(&q, now).expect_err("半个区间必须被拒");
            assert!(err.message.contains("成对"), "{}", err.message);
        }
    }

    /// 倒着的区间拒绝，不自动交换：交换会把一个写错的区间悄悄答成另一个正确的
    /// 区间，而调用方永远不知道自己的输入曾经是错的。
    #[test]
    fn a_backwards_range_is_refused_rather_than_swapped() {
        let now = chrono::Utc::now();
        let err = spend_range(
            &spend_query(
                None,
                Some("2026-11-06T06:00:00Z"),
                Some("2026-11-06T00:00:00Z"),
            ),
            now,
        )
        .expect_err("倒着的区间必须被拒");
        assert!(err.message.contains("晚于"), "{}", err.message);
    }

    /// 读得比保留期还宽时要拒，并且拒的理由里必须出现那个旋钮的名字。
    ///
    /// 上限不是常量：它由 `COGNEVA_LLM_USAGE_KEEP_DAYS` 算出来。写死的上限在
    /// 保留期被改的那天就开始撒谎，而两边各自的读数都正常——账本留三十天、接口
    /// 只覆盖七天就是这样长出来的。这条测试盯着的是「理由里有没有那个变量名」，
    /// 所以把上限改回一个数字会让它红。
    #[test]
    fn asking_past_the_retention_names_the_knob_that_bounds_it() {
        let now = chrono::Utc::now();
        let days = cog_core::retention::llm_usage_keep_days();
        let since = now - chrono::Duration::hours(days * 24 + 1);
        let err = spend_range(
            &spend_query(None, Some(&since.to_rfc3339()), Some(&now.to_rfc3339())),
            now,
        )
        .expect_err("超出保留期必须被拒");
        let text = err.message;
        assert!(
            text.contains(cog_core::retention::LLM_USAGE_KEEP_DAYS_ENV),
            "拒的理由里要点出是哪个旋钮界的这个范围：{text}"
        );
        // 恰好等于保留期是合法的：清扫删的是更早的行，不是边界本身。
        let edge = now - chrono::Duration::hours(days * 24);
        assert!(
            spend_range(
                &spend_query(None, Some(&edge.to_rfc3339()), Some(&now.to_rfc3339())),
                now
            )
            .is_ok(),
            "恰好一个保留期宽的区间应当可读"
        );
    }

    /// `hours <= 0` 在本进程的其他花销读法里是「能读多远读多远」。这里能读多远
    /// 就是保留期——再往前是被清扫删掉的行，答成一零读起来像那段没人用。
    #[test]
    fn hours_zero_reaches_back_exactly_one_retention() {
        let now = chrono::Utc::now();
        let days = cog_core::retention::llm_usage_keep_days();
        let (since, until) = spend_range(&spend_query(Some(0), None, None), now).expect("必须可读");
        assert_eq!(until, now);
        assert_eq!(since, now - chrono::Duration::hours(days * 24));
    }

    /// 一组里所有调用都没价时，钱那一格是 `null`，不是 0。
    ///
    /// 0 会被读成「这一组没花钱」，而真相是「这一组的价钱没人算过」。两种情形
    /// 的补救方向相反：一个去看价目配没配，一个什么都不用做。
    #[test]
    fn a_group_with_no_price_carries_null_money_and_counts_its_unpriced_calls() {
        let since = chrono::Utc::now() - chrono::Duration::hours(1);
        let until = chrono::Utc::now();
        let response = spend_response(
            since,
            until,
            cog_core::PricedSpend {
                covered_since: Some(since),
                covered_until: Some(until),
                rows: vec![cog_core::PricedSpendRow {
                    actor: "generator".into(),
                    origin: "resident_worker".into(),
                    upstream: "u".into(),
                    api_style: "anthropic".into(),
                    price_book: None,
                    calls: 7,
                    failed_calls: 0,
                    tokens_input: 100,
                    tokens_cached: 900,
                    tokens_output: 50,
                    cost_micros: None,
                    priced_calls: 0,
                }],
            },
        );
        assert_eq!(response.rows[0].cost_micros, None);
        assert_eq!(response.rows[0].currency, None);
        assert_eq!(response.totals.unpriced_calls, 7);
        assert!(response.totals.by_currency.is_empty());
    }

    /// 一组里只有一部分调用有价时，钱盖的是那几次，缺口必须单独数出来。
    /// 把缺口并进总额，等于让没定价的调用按零元结算。
    #[test]
    fn money_covers_only_the_priced_calls_and_the_gap_is_counted() {
        let since = chrono::Utc::now() - chrono::Duration::hours(1);
        let until = chrono::Utc::now();
        let response = spend_response(
            since,
            until,
            cog_core::PricedSpend {
                covered_since: Some(since),
                covered_until: Some(until),
                rows: vec![
                    cog_core::PricedSpendRow {
                        actor: "generator".into(),
                        origin: "resident_worker".into(),
                        upstream: "u".into(),
                        api_style: "openai".into(),
                        price_book: Some("CNY@2026-11-05".into()),
                        calls: 10,
                        failed_calls: 1,
                        tokens_input: 1_000,
                        tokens_cached: 9_000,
                        tokens_output: 500,
                        cost_micros: Some(12_345),
                        priced_calls: 8,
                    },
                    cog_core::PricedSpendRow {
                        actor: "planner".into(),
                        origin: "resident_worker".into(),
                        upstream: "u".into(),
                        api_style: "openai".into(),
                        price_book: Some("CNY@2026-11-05".into()),
                        calls: 2,
                        failed_calls: 0,
                        tokens_input: 100,
                        tokens_cached: 0,
                        tokens_output: 10,
                        cost_micros: Some(755),
                        priced_calls: 2,
                    },
                ],
            },
        );
        assert_eq!(response.rows[0].currency.as_deref(), Some("CNY"));
        assert_eq!(
            response.totals.unpriced_calls, 2,
            "10+2 次调用里只有 10 次有价"
        );
        assert_eq!(response.totals.by_currency.len(), 1);
        assert_eq!(response.totals.by_currency[0].currency, "CNY");
        assert_eq!(response.totals.by_currency[0].cost_micros, 12_345 + 755);
        assert_eq!(response.totals.by_currency[0].priced_calls, 10);
    }

    /// 两种币种各自合计，不相加。相加要一个汇率，而汇率没有任何一处核对过；
    /// 加起来的那一个数会被当成「总共花了多少」。
    #[test]
    fn two_currencies_are_two_totals() {
        let since = chrono::Utc::now() - chrono::Duration::hours(1);
        let until = chrono::Utc::now();
        let row = |book: &str, micros: i64| cog_core::PricedSpendRow {
            actor: "a".into(),
            origin: "o".into(),
            upstream: "u".into(),
            api_style: "p".into(),
            price_book: Some(book.into()),
            calls: 1,
            failed_calls: 0,
            tokens_input: 1,
            tokens_cached: 0,
            tokens_output: 1,
            cost_micros: Some(micros),
            priced_calls: 1,
        };
        let response = spend_response(
            since,
            until,
            cog_core::PricedSpend {
                covered_since: Some(since),
                covered_until: Some(until),
                rows: vec![row("CNY@2026-11-05", 100), row("USD@2026-11-01", 200)],
            },
        );
        assert_eq!(response.totals.by_currency.len(), 2);
        assert_eq!(response.totals.unpriced_calls, 0);
    }

    /// 区间里一行都没有时，覆盖区间两端一起是 `null`，不是把请求的区间回显成
    /// 覆盖。回显会让「这段没人用」与「这段没读到」变成一个答案。
    #[test]
    fn an_empty_range_reports_no_coverage_rather_than_echoing_the_request() {
        let since = chrono::Utc::now() - chrono::Duration::hours(6);
        let until = chrono::Utc::now();
        let response = spend_response(
            since,
            until,
            cog_core::PricedSpend {
                covered_since: None,
                covered_until: None,
                rows: vec![],
            },
        );
        assert_eq!(response.covered_since, None);
        assert_eq!(response.covered_until, None);
        assert_eq!(response.requested_since, since.to_rfc3339());
        assert_eq!(response.totals.unpriced_calls, 0);
    }
}
