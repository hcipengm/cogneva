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
// cap: bound
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
}

fn instant(unix: i64) -> Option<chrono::DateTime<chrono::Utc>> {
    if unix <= 0 {
        return None;
    }
    chrono::DateTime::from_timestamp(unix, 0)
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
    Some(LlmPoolView {
        available: !status.unavailable,
        pool_size: status.pool_size,
        min_healthy: status.declared_min_healthy,
        unavailable_count: status.unavailable_upstreams.len(),
        quota_window_secs: status.quota_window_secs,
        recovery_at: instant(status.evidenced_recovery_unix),
        next_attempt_at: instant(status.next_attempt_unix),
        last_served_at: instant(status.last_success_unix),
    })
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

/// Cap on the trailing window one request may ask for.
///
/// Bounded so a single refresh cannot ask the fold for a range wider than the
/// ledger's own history; a request past the fold's reach is answered as an
/// uncovered range either way, so the cap bounds the scan and not the honesty
/// of the answer.
// cap: bound
const MAX_USAGE_HOURS: i64 = 24 * 7;

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
    let hours = q.hours.clamp(1, MAX_USAGE_HOURS);
    let until = chrono::Utc::now();
    let since = until - chrono::Duration::hours(hours);
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
    let hours = q.hours.clamp(1, MAX_USAGE_HOURS);
    let until = chrono::Utc::now();
    let since = until - chrono::Duration::hours(hours);
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

    // For historical queries without a "since" parameter, we default to
    // the last 24 hours to avoid unbounded result sets.
    event_filter.since = Some(chrono::Utc::now() - chrono::Duration::hours(24));

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
}
