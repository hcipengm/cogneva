//! These endpoints expose the **conversation session** concept (a continuous
//! AI chat thread) to mobile / web clients. Persistent storage and message
//! history will be wired up once the `sf-session` adapter lands; for now
//! these handlers validate auth, accept the documented request shapes, and
//! return canned responses so the client SDKs can develop against the real
//! routing surface.
//! Routes wired in [`crate::create_router`]:
//! - `GET    /api/v1/sessions`                 — list sessions
//! - `POST   /api/v1/sessions`                 — create session
//! - `GET    /api/v1/sessions/{id}`            — get session detail
//! - `PUT    /api/v1/sessions/{id}`            — update session
//! - `DELETE /api/v1/sessions/{id}`            — delete (soft) session
//! - `GET    /api/v1/sessions/{id}/messages`   — fetch message history
//! - `POST   /api/v1/sessions/{id}/messages`   — send message (stream task)
//! - `POST   /api/v1/sessions/{id}/messages/{message_id}/regenerate`
//!
//! Session management handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::GatewayState;

#[derive(Debug, Default, Deserialize)]
pub struct ListSessionsQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionPayload {
    #[serde(default)]
    pub title: Option<String>,
    pub workspace_id: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSessionPayload {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListMessagesQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub direction: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessagePayload {
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
}

pub async fn list_sessions_handler(
    State(_state): State<Arc<GatewayState>>,
    Query(_q): Query<ListSessionsQuery>,
) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "code": "ok",
            "data": {
                "items": [],
                "next_cursor": null,
                "has_more": false,
            }
        })),
    )
        .into_response()
}

pub async fn create_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Json(payload): Json<CreateSessionPayload>,
) -> Response {
    let session_id = uuid::Uuid::new_v4().to_string();
    (
        StatusCode::CREATED,
        Json(json!({
            "code": "ok",
            "data": {
                "session_id": session_id,
                "title": payload.title.unwrap_or_else(|| "新对话".to_string()),
                "workspace_id": payload.workspace_id,
                "agent_id": payload.agent_id,
                "created_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn get_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "code": "not_found",
            "error": "session_not_found",
            "session_id": session_id,
        })),
    )
        .into_response()
}

pub async fn update_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<UpdateSessionPayload>,
) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "code": "ok",
            "data": {
                "session_id": session_id,
                "title": payload.title,
                "status": payload.status,
                "updated_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn delete_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "code": "ok",
            "data": {
                "session_id": session_id,
                "deleted_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn list_messages_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    Query(_q): Query<ListMessagesQuery>,
) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "code": "ok",
            "data": {
                "session_id": session_id,
                "items": [],
                "next_cursor": null,
                "has_more": false,
            }
        })),
    )
        .into_response()
}

pub async fn send_message_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<SendMessagePayload>,
) -> Response {
    let message_id = format!("msg-{}", uuid::Uuid::new_v4());
    let task_id = format!("task-{}", uuid::Uuid::new_v4());
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "code": "ok",
            "data": {
                "message_id": message_id,
                "session_id": session_id,
                "role": "user",
                "content": payload.content,
                "task_id": task_id,
                "timestamp": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn regenerate_message_handler(
    State(_state): State<Arc<GatewayState>>,
    Path((session_id, message_id)): Path<(String, String)>,
) -> Response {
    let task_id = format!("task-{}", uuid::Uuid::new_v4());
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "code": "ok",
            "data": {
                "session_id": session_id,
                "source_message_id": message_id,
                "task_id": task_id,
                "timestamp": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}
