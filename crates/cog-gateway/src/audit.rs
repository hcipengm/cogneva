//! Audit replay HTTP handlers.
//! Provides endpoints for querying archived hook events and replaying
//! them for audit, debugging, and downstream analytics.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::sync::Arc;

use crate::error::ApiError;
use crate::GatewayState;

/// Query parameters for audit event listing.
#[derive(Debug, Deserialize, Default)]
pub struct AuditQuery {
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub trigger_type: Option<String>,
    pub since: Option<DateTime<Utc>>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// List archived hook events.
pub async fn list_audit_events_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<Vec<cog_core::HookArchiveEntry>>, ApiError> {
    let archive = state
        .hook_archive
        .as_ref()
        .ok_or_else(|| ApiError::internal("hook archive not configured"))?;

    let rows = archive
        .query(
            query.agent_id.as_deref(),
            query.task_id.as_deref(),
            query.trigger_type.as_deref(),
            query.since,
            query.limit,
        )
        .await
        .map_err(|e| ApiError::internal(format!("audit query failed: {e}")))?;

    Ok(Json(rows))
}

/// Replay a specific archived event by its database id.
pub async fn replay_audit_event_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<i32>,
) -> Result<Response, ApiError> {
    let archive = state
        .hook_archive
        .as_ref()
        .ok_or_else(|| ApiError::internal("hook archive not configured"))?;

    // Query the specific event by id with a high limit and then filter.
    let rows = archive
        .query(None, None, None, None, 10_000)
        .await
        .map_err(|e| ApiError::internal(format!("audit replay query failed: {e}")))?;

    let event = rows.into_iter().find(|r| r.id == id);

    match event {
        Some(e) => Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "id": e.id,
                "trigger_type": e.trigger_type,
                "agent_id": e.agent_id,
                "task_id": e.task_id,
                "crew_id": e.crew_id,
                "squad_id": e.squad_id,
                "payload": e.payload,
                "timestamp": e.timestamp,
                "replayed": true,
            })),
        )
            .into_response()),
        None => Err(ApiError::not_found(format!("audit event {id} not found"))),
    }
}
