//! HTTP handlers for the Agent registration / heartbeat endpoints.
//! Wires the [`cog_core::AgentRegistry`] trait to the gateway, exposing:
//! - `POST /api/v1/agents/register` — register a new Agent
//! - `POST /api/v1/agents/{id}/heartbeat` — renew TTL
//! - `DELETE /api/v1/agents/{id}` — graceful deregister
//! - `GET /api/v1/agents/{id}` — fetch a registration
//! - `GET /api/v1/agents` — list live Agents

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;
use cog_core::{AgentRegistration, ResourceInfo};

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub hostname: String,
    pub pod_ip: String,
    pub role: String,
    pub workspace_id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub resources: ResourceInfo,
    /// Optional client-supplied uuid for deterministic agent_id.  When `None`
    /// we generate a fresh `uuid_v4` so multiple instances of the same pod
    /// don't collide on `blake3(hostname|ip|role)`.
    #[serde(default)]
    pub uuid: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub agent_id: String,
    pub registration: AgentRegistration,
}

pub async fn register_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<RegisterRequest>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };

    let uuid = req.uuid.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let agent_id = cog_core::generate_agent_id(&req.hostname, &req.pod_ip, &req.role, &uuid);
    let now = chrono::Utc::now();
    let registration = AgentRegistration {
        agent_id: agent_id.clone(),
        role: req.role,
        workspace_id: req.workspace_id,
        capabilities: req.capabilities,
        resources: req.resources,
        hostname: req.hostname,
        pod_ip: req.pod_ip,
        registered_at: now,
        last_heartbeat: now,
    };

    match registry.register(&registration).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(RegisterResponse {
                agent_id,
                registration,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn heartbeat_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.heartbeat(&agent_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn deregister_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.deregister(&agent_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn get_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.get(&agent_id).await {
        Ok(Some(reg)) => (StatusCode::OK, Json(reg)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "agent not registered"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn list_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.list().await {
        Ok(regs) => (StatusCode::OK, Json(serde_json::json!({"agents": regs}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Agent Lifecycle Control Commands ───
// These POST endpoints dispatch lifecycle commands through the Supervisor
// trait, which emits events on the Supervisor broadcast channel for
// downstream consumption (gRPC server, hook engine, audit log, etc.).

#[derive(Debug, Deserialize)]
pub struct CheckpointQuery {
    /// Optional task ID.  When omitted the caller should supply it in the
    /// request body of a future revision; for now we require it.
    pub task_id: String,
}

pub async fn kill_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.kill_agent(&agent_id, "user-requested").await {
            Ok(true) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "dispatched",
                })),
            )
                .into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "agent not found",
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "kill",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

pub async fn restart_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.restart_agent(&agent_id, false).await {
            Ok(true) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "dispatched",
                })),
            )
                .into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "agent not found",
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "restart",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

pub async fn checkpoint_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<CheckpointQuery>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.checkpoint_agent(&agent_id, &query.task_id).await {
            Ok(checkpoint_id) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "checkpoint",
                    "status": "dispatched",
                    "checkpoint_id": checkpoint_id,
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "checkpoint",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "checkpoint",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}
