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
            "code": crate::error::simple_code::OK,
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
            "code": crate::error::simple_code::OK,
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
            "code": crate::error::simple_code::NOT_FOUND,
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
            "code": crate::error::simple_code::OK,
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
            "code": crate::error::simple_code::OK,
            "data": {
                "session_id": session_id,
                "deleted_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

/// The lines of one conversation, oldest first.
///
/// This used to answer `items: []` unconditionally while the chat socket was
/// happily streaming deltas: the transcript existed in `chat_turns` and the one
/// endpoint named "message history" could not see it. A reopened page showed an
/// empty conversation and the platform looked like it had forgotten what it was
/// told. The rows are read here instead of answered from a canned body.
///
/// `failure` rides on the row rather than being folded into `content`: a turn
/// whose answer never arrived is still a turn, and a reader that cannot tell
/// "no answer yet" from "no answer ever" re-asks a question that already failed.
pub async fn list_messages_handler(
    State(state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    Query(q): Query<ListMessagesQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(200).clamp(1, 500) as usize;
    let Some(store) = state.chat_turns.as_ref() else {
        // No transcript store in this deployment: an empty list is the truthful
        // answer, and the client still has the live socket.
        return (
            StatusCode::OK,
            Json(json!({
                "code": crate::error::simple_code::OK,
                "data": {
                    "session_id": session_id,
                    "items": [],
                    "next_cursor": null,
                    "has_more": false,
                }
            })),
        )
            .into_response();
    };

    match store.transcript(&session_id, limit).await {
        Ok(turns) => {
            let items: Vec<serde_json::Value> = turns
                .into_iter()
                .map(|turn| {
                    json!({
                        "id": format!("turn-{}", turn.id),
                        "role": turn.role.as_str(),
                        "content": turn.content,
                        "failure": turn.failure,
                        // 交出去的这一轮带着目标身份回来：重新打开这段对话时，
                        // 「这条是能关掉的问答，还是一条还在服务器上跑的目标」
                        // 只能从这一格读——实时流在页面关掉时就断了。
                        "goal_id": turn.goal,
                        "created_at": turn.created_at,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "code": crate::error::simple_code::OK,
                    "data": {
                        "session_id": session_id,
                        "items": items,
                        "next_cursor": null,
                        "has_more": false,
                    }
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "code": "error",
                "message": format!("读不到这段对话的记录：{e}"),
            })),
        )
            .into_response(),
    }
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
            "code": crate::error::simple_code::OK,
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
            "code": crate::error::simple_code::OK,
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
