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

/// 平台自己那本账的查询参数（哈希链，不是 hook 归档）。
///
/// 与 `/api/v1/audit/events` 是两本账：那一本记 hook 归档（谁触发、payload 是什么），
/// 这一本记平台自己做过的事——配额拒绝、授权、变更、以及上游池被谁改成什么样。
/// 两本混在一个端点下，读的人就分不清一条记录是"某次回调来过"还是"平台做了一件事"。
#[derive(Debug, Deserialize, Default)]
pub struct AuditStreamQuery {
    /// 动作前缀，如 `llm.` 或 `quota.`。
    pub action: Option<String>,
    /// 操作主体的全名（精确匹配）。
    pub actor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// 读平台自己的审计链，最新的在前。
///
/// 只读、只追加的那一条链上的一条切片。`read_all` 是这条链唯一给出的读法（增量
/// 游标不在契约里），所以这里按条数截断再返回——一次查询的代价随链长增长，这是
/// 一张"事后追查"的表应有的代价，而不是把它藏起来不让人查。
///
/// 链本身的可校验性不在这里判：`verify()` 要读整链，那是另一个动作、另一份读数。
pub async fn list_audit_stream_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<AuditStreamQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let stream = state
        .audit_stream
        .as_ref()
        .ok_or_else(|| ApiError::internal("audit stream not configured"))?;
    let events = stream
        .read_all()
        .await
        .map_err(|e| ApiError::internal(format!("audit stream read failed: {e}")))?;
    let matched: Vec<&cog_core::AuditEvent> = events
        .iter()
        .filter(|e| {
            query
                .action
                .as_deref()
                .is_none_or(|prefix| e.action.starts_with(prefix))
        })
        .filter(|e| query.actor.as_deref().is_none_or(|actor| e.actor == actor))
        .collect();
    let limit = query.limit.clamp(1, 1000);
    let items: Vec<&cog_core::AuditEvent> = matched.iter().rev().take(limit).copied().collect();
    Ok(Json(serde_json::json!({
        // 匹配到几条与这一页给了几条分开写：只回报回来的那几条，"还有多少没看见"
        // 就读不出来，而"最近的都是这些"与"最近 1000 条都是这些"是两件事。
        "matched": matched.len(),
        "items": items,
    })))
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
