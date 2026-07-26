//! HTTP handlers for the HookEngine management endpoints.
//! Exposes:
//! - `GET  /api/v1/hooks`    — list currently registered hooks
//! - `POST /api/v1/hooks`    — register a new hook dynamically
//! - `DELETE /api/v1/hooks/{id}` — remove a hook by id

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::GatewayState;
use cog_core::HookDef;

#[derive(Debug, Serialize)]
pub struct ListHooksResponse {
    pub hooks: Vec<HookDef>,
}

pub async fn list_hooks_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let Some(engine) = state.hook_engine.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "hook engine not configured"})),
        )
            .into_response();
    };

    let hooks = engine.list_hooks().await;
    (StatusCode::OK, Json(ListHooksResponse { hooks })).into_response()
}

pub async fn create_hook_handler(
    State(state): State<Arc<GatewayState>>,
    Json(def): Json<HookDef>,
) -> Response {
    let Some(engine) = state.hook_engine.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "hook engine not configured"})),
        )
            .into_response();
    };

    engine.register(def).await;
    (
        StatusCode::CREATED,
        Json(serde_json::json!({"status": "registered"})),
    )
        .into_response()
}

pub async fn delete_hook_handler(
    State(state): State<Arc<GatewayState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(engine) = state.hook_engine.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "hook engine not configured"})),
        )
            .into_response();
    };

    // Atomically replace the hook list, filtering out the target id.
    let current = engine.list_hooks().await;
    let filtered: Vec<HookDef> = current.into_iter().filter(|h| h.id != id).collect();
    engine.replace_hooks(filtered).await;

    (StatusCode::NO_CONTENT, "").into_response()
}
