//! Workspace HTTP handlers — real persistence via in-memory store.
//! Workspace = a multi-tenant container for sessions, agents, and quotas.
//! Routes wired in [`crate::create_router`]:
//! - `GET /api/v1/workspaces`                       — list user's workspaces
//! - `GET /api/v1/workspaces/{id}`                  — workspace detail
//! - `GET /api/v1/workspaces/{id}/members`          — workspace members

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::GatewayState;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceMember {
    pub user_id: String,
    pub role: String, // "owner", "admin", "member"
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub owner_id: String,
    pub members: Vec<WorkspaceMember>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub async fn list_workspaces_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let store = match state.workspace_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "code": "ok",
                    "data": {
                        "items": []
                    }
                })),
            )
                .into_response();
        }
    };

    let workspaces = match store.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "lock_poisoned"})),
            )
                .into_response();
        }
    };

    let items: Vec<serde_json::Value> = workspaces
        .values()
        .map(|w| {
            json!({
                "id": w.id,
                "name": w.name,
                "owner_id": w.owner_id,
                "created_at": w.created_at.to_rfc3339(),
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "code": "ok",
            "data": {
                "items": items
            }
        })),
    )
        .into_response()
}

pub async fn get_workspace_handler(
    State(state): State<Arc<GatewayState>>,
    Path(workspace_id): Path<String>,
) -> Response {
    let store = match state.workspace_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "code": "not_found",
                    "error": "workspace_not_found",
                    "workspace_id": workspace_id,
                })),
            )
                .into_response();
        }
    };

    let workspaces = match store.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "lock_poisoned"})),
            )
                .into_response();
        }
    };

    match workspaces.get(&workspace_id) {
        Some(w) => (
            StatusCode::OK,
            Json(json!({
                "code": "ok",
                "data": {
                    "id": w.id,
                    "name": w.name,
                    "owner_id": w.owner_id,
                    "members": w.members.iter().map(|m| json!({
                        "user_id": m.user_id,
                        "role": m.role,
                    })).collect::<Vec<_>>(),
                    "created_at": w.created_at.to_rfc3339(),
                }
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": "not_found",
                "error": "workspace_not_found",
                "workspace_id": workspace_id,
            })),
        )
            .into_response(),
    }
}

pub async fn list_workspace_members_handler(
    State(state): State<Arc<GatewayState>>,
    Path(workspace_id): Path<String>,
) -> Response {
    let store = match state.workspace_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "code": "ok",
                    "data": {
                        "workspace_id": workspace_id,
                        "items": []
                    }
                })),
            )
                .into_response();
        }
    };

    let workspaces = match store.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "lock_poisoned"})),
            )
                .into_response();
        }
    };

    let members = workspaces
        .get(&workspace_id)
        .map(|w| {
            w.members
                .iter()
                .map(|m| {
                    json!({
                        "user_id": m.user_id,
                        "role": m.role,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    (
        StatusCode::OK,
        Json(json!({
            "code": "ok",
            "data": {
                "workspace_id": workspace_id,
                "items": members
            }
        })),
    )
        .into_response()
}
