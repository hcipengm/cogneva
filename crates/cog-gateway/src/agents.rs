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

/// Put one command on a named agent's inbox.
///
/// The inbox subscription and the command semantics existed, but nothing in
/// the platform ever produced a message for them, so an agent could be
/// addressed only from inside its own process. This is the producer that
/// closes the path: the body is the wire form of [`cog_core::InboxMessage`].
///
/// A `reply_stream` named by the caller is refused here. The platform ends a
/// reply stream once the ask it belongs to is over -- that ending is the only
/// thing that keeps one from outliving its question -- and a name supplied
/// from outside belongs to a reader this process cannot see, so nothing here
/// may delete it. The stream is also not something an outside caller can read:
/// the store is not reachable from the client side. Accepting the field would
/// therefore answer 202 for a key that can stay in the store for good, with no
/// way to drain it. An agent asking another agent gets an answer through the
/// `send_to_agent` tool with `expect_reply`, whose stream the platform opens
/// and ends by construction.
///
/// Two delivery paths, tried in that order:
///
/// - an agent running in *this* process, addressed directly, so the command
///   reaches its execution path without waiting for a poll;
/// - a name held by a process elsewhere, which this one cannot see and cannot
///   call into. The registry is the only thing that knows it exists, and it
///   answers with evidence of life rather than with a row that merely exists,
///   so a name it returns belongs to a running consumer group. The command
///   goes onto that agent's inbox stream, which is the same stream the
///   consumer reads by construction -- the name comes from
///   [`cog_core::agent_inbox_stream`], not from a second copy of the format.
///
/// Accepted, not processed: the agent's consumer owns execution, and the
/// answer goes to `reply_stream` rather than to this response. A name that
/// resolves to no live agent anywhere is a 404, because queueing a command for
/// a process that will never read it is indistinguishable from losing it.
pub async fn send_message_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Json(msg): Json<cog_core::InboxMessage>,
) -> Response {
    // Checked before either delivery path: both of them hand the command to a
    // handler that would publish the answer into that stream and leave it
    // there, because ending it is the asker's job and this route has no asker
    // it knows.
    if let Some(stream) = caller_named_reply_stream(&msg) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "this route does not take a caller-named reply_stream: \
                          the platform cannot read or reclaim a stream whose owner \
                          it does not know. An agent asking another agent gets its \
                          answer through the send_to_agent tool with expect_reply, \
                          whose stream the platform opens and ends",
                "reply_stream": stream,
                "agent_id": agent_id,
            })),
        )
            .into_response();
    }

    // Local first. A miss is not an answer -- the same name may be held by
    // another process, which is the whole point of the registry.
    if let Some(pool) = state.agent_pool.as_ref() {
        match pool.get_agent(&agent_id).await {
            Ok(Some(agent)) => {
                return match agent.receive_message(msg).await {
                    Ok(()) => (
                        StatusCode::ACCEPTED,
                        Json(serde_json::json!({
                            "queued": true,
                            "agent_id": agent_id,
                            "via": "agent_pool",
                        })),
                    )
                        .into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            }
            Ok(None) => {}
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response()
            }
        }
    }

    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.get(&agent_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "no live agent under this name"})),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }

    // A name with a live holder but no queue to put the command on is a
    // deployment fault, not a delivery: say so instead of answering 202 for a
    // message that was never written anywhere.
    let Some(backend) = state.message_backend.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "message_backend not configured"})),
        )
            .into_response();
    };
    let payload = match serde_json::to_vec(&msg) {
        Ok(payload) => payload,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };
    let stream = cog_core::agent_inbox_stream(&agent_id);
    match backend.publish(&stream, &payload).await {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "queued": true,
                "agent_id": agent_id,
                "via": "inbox",
                "stream": stream,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string(), "stream": stream})),
        )
            .into_response(),
    }
}

/// The reply stream the caller put in the body, if any.
///
/// Split out so the refusal can be judged without a gateway state: what is
/// being decided is which bodies this route will take, and that depends on the
/// body alone.
fn caller_named_reply_stream(msg: &cog_core::InboxMessage) -> Option<&str> {
    match msg {
        cog_core::InboxMessage::Prompt {
            reply_stream: Some(stream),
            ..
        } => Some(stream),
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::caller_named_reply_stream;
    use cog_core::InboxMessage;

    /// A reply stream the caller names is taken out of the body rather than
    /// honoured, and the other two shapes -- a prompt with no reply stream and
    /// a command that is not a prompt at all -- go through. Reading only the
    /// refusal would not say the route still accepts anything.
    #[test]
    fn only_a_caller_named_reply_stream_is_refused() {
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Prompt {
                goal: serde_json::json!({"goal": "run the tests"}),
                reply_stream: Some("caller:reply:1".to_string()),
            }),
            Some("caller:reply:1")
        );
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Prompt {
                goal: serde_json::json!({"goal": "run the tests"}),
                reply_stream: None,
            }),
            None
        );
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Abort),
            None,
            "only a prompt can name a reply stream"
        );
    }
}
