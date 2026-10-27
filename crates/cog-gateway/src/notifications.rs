//! Notification HTTP handlers — backed by any [`cog_core::NotificationStore`].
//! Routes wired in [`crate::create_router`]:
//! - `GET  /api/v1/notifications`                  — list notifications
//! - `POST /api/v1/notifications/{id}/read`        — mark single as read
//! - `POST /api/v1/notifications/read-all`         — mark all as read

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
pub struct ListNotificationsQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub unread_only: Option<bool>,
}

/// The owner every handler here answers for.
///
/// These routes are behind the auth layer, so claims are normally present; the
/// `Option` is what the layer's own contract gives us. Absent claims are
/// answered with 401 rather than a default id: a notification list is a
/// question about one person, and there is no person to ask it about.
fn owner_of(claims: &Option<axum::Extension<cog_core::Claims>>) -> Option<String> {
    claims
        .as_ref()
        .map(|axum::Extension(claims)| claims.sub.clone())
}

fn unauthenticated() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "authentication required"})),
    )
        .into_response()
}

pub async fn list_notifications_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(q): Query<ListNotificationsQuery>,
) -> Response {
    let Some(owner) = owner_of(&claims) else {
        return unauthenticated();
    };
    let store = match state.notification_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "code": crate::error::simple_code::OK,
                    "data": {
                        "items": [],
                        "unread_count": 0,
                        "next_cursor": null,
                        "has_more": false,
                    }
                })),
            )
                .into_response();
        }
    };

    let default_limit = state.config.read().unwrap().notification_limit as usize;
    let filter = cog_core::NotificationFilter {
        unread_only: q.unread_only == Some(true),
        limit: q
            .limit
            .map(|l| l as usize)
            .unwrap_or(default_limit)
            .min(1000),
        cursor: q.cursor,
    };

    let list = match store.list(&owner, filter).await {
        Ok(l) => l,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("store error: {}", e)})),
            )
                .into_response();
        }
    };

    let json_items: Vec<serde_json::Value> = list
        .items
        .into_iter()
        .map(|n| {
            json!({
                "id": n.id,
                "title": n.title,
                "body": n.body,
                "is_read": n.is_read,
                "created_at": n.created_at.to_rfc3339(),
                "read_at": n.read_at.map(|t| t.to_rfc3339()),
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "code": crate::error::simple_code::OK,
            "data": {
                "items": json_items,
                "unread_count": list.unread_count,
                "next_cursor": list.next_cursor,
                "has_more": list.has_more,
            }
        })),
    )
        .into_response()
}

pub async fn mark_read_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(notification_id): Path<String>,
) -> Response {
    let Some(owner) = owner_of(&claims) else {
        return unauthenticated();
    };
    let store = match state.notification_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "code": crate::error::simple_code::NOT_FOUND,
                    "error": "notification_not_found",
                    "notification_id": notification_id,
                })),
            )
                .into_response();
        }
    };

    match store.mark_read(&owner, &notification_id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({
                "code": crate::error::simple_code::OK,
                "data": {
                    "notification_id": notification_id,
                    "is_read": true,
                    "read_at": chrono::Utc::now().to_rfc3339(),
                }
            })),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": crate::error::simple_code::NOT_FOUND,
                "error": "notification_not_found",
                "notification_id": notification_id,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("store error: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn mark_all_read_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
) -> Response {
    let Some(owner) = owner_of(&claims) else {
        return unauthenticated();
    };
    let store = match state.notification_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "code": crate::error::simple_code::OK,
                    "data": {
                        "is_read": true,
                        "read_at": chrono::Utc::now().to_rfc3339(),
                    }
                })),
            )
                .into_response();
        }
    };

    match store.mark_all_read(&owner).await {
        Ok(_count) => (
            StatusCode::OK,
            Json(json!({
                "code": crate::error::simple_code::OK,
                "data": {
                    "is_read": true,
                    "read_at": chrono::Utc::now().to_rfc3339(),
                }
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("store error: {}", e)})),
        )
            .into_response(),
    }
}
