use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use futures::StreamExt;
use serde::Deserialize;
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
const FIRING_PROBE_LIMIT: i64 = 1;

/// Verdict over the firing alerts an evidence source reported.
///
/// The scope is exactly that evidence: `healthy` means the source answered and
/// nothing was firing, not that every component was verified working. Severity
/// plays no part — the alert vocabulary spends `info` on resolutions, so any
/// row still firing is a claim that something is wrong.
fn cluster_health_of(firing: &[cog_core::PersistedAlert]) -> &'static str {
    if firing.is_empty() {
        "healthy"
    } else {
        "degraded"
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
    overview.cluster_health = cluster_health_of(&firing).to_string();
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
) -> Result<Json<cog_core::observability::ClusterOverview>, ApiError> {
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
    Ok(Json(overview))
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
    let date = chrono::NaiveDate::parse_from_str(&query.date, "%Y-%m-%d").map_err(|e| {
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
        AgentEvent::StateChange { to, .. } if to == "Dead" => "error",
        AgentEvent::TaskStatusChange { status, .. } if status == "Failed" => "error",
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

    fn overview(health: &str) -> cog_core::observability::ClusterOverview {
        cog_core::observability::ClusterOverview {
            total_agents: None,
            active_agents: None,
            total_tasks: None,
            active_tasks: None,
            queued_tasks: None,
            failed_tasks: None,
            avg_task_duration_ms: None,
            cluster_health: health.into(),
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
        let mut o = overview("unknown");
        apply_alert_verdict(&mut o, Some(&source)).await;
        assert_eq!(o.cluster_health, "healthy");
    }

    // A firing row is the claim that something is wrong, whatever its severity
    // — `info` is the resolution vocabulary and does not appear in firing rows.
    #[tokio::test]
    async fn anything_firing_reads_degraded_whatever_its_severity() {
        for severity in ["critical", "warning"] {
            let source: Arc<dyn ActiveAlertSource> = Arc::new(StubSource {
                firing: Some(vec![firing(severity)]),
            });
            let mut o = overview("unknown");
            apply_alert_verdict(&mut o, Some(&source)).await;
            assert_eq!(o.cluster_health, "degraded", "severity {severity}");
        }
    }

    // No source is no evidence: it must not be promoted to `healthy`, or
    // "never looked" and "looked, all clear" arrive as the same string.
    #[tokio::test]
    async fn without_a_source_the_verdict_stays_unknown() {
        let mut o = overview("unknown");
        apply_alert_verdict(&mut o, None).await;
        assert_eq!(o.cluster_health, "unknown");
    }

    // A source whose query errored is not evidence of quiet either. This is the
    // path that makes the difference observable: the only production source
    // reports a failed lookup as `None`.
    #[tokio::test]
    async fn a_source_that_could_not_read_leaves_the_verdict_unknown() {
        let source: Arc<dyn ActiveAlertSource> = Arc::new(StubSource { firing: None });
        let mut o = overview("unknown");
        apply_alert_verdict(&mut o, Some(&source)).await;
        assert_eq!(o.cluster_health, "unknown");
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
}
