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

pub async fn cluster_overview_handler(
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<cog_core::observability::ClusterOverview>, ApiError> {
    let gateway = state
        .observability_gateway
        .as_ref()
        .ok_or_else(|| ApiError::internal("observability gateway not configured"))?;
    let overview = gateway
        .get_cluster_overview()
        .await
        .map_err(|e| ApiError::internal(format!("failed to get cluster overview: {e}")))?;
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
