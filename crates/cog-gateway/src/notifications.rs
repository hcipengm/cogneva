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

pub async fn list_notifications_handler(
    State(state): State<Arc<GatewayState>>,
    Query(q): Query<ListNotificationsQuery>,
) -> Response {
    let store = match state.notification_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "code": "ok",
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

    let list = match store.list(filter).await {
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
            "code": "ok",
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
    Path(notification_id): Path<String>,
) -> Response {
    let store = match state.notification_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "code": "not_found",
                    "error": "notification_not_found",
                    "notification_id": notification_id,
                })),
            )
                .into_response();
        }
    };

    match store.mark_read(&notification_id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({
                "code": "ok",
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
                "code": "not_found",
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

pub async fn mark_all_read_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let store = match state.notification_store {
        Some(ref s) => s,
        None => {
            return (
                StatusCode::OK,
                Json(json!({
                    "code": "ok",
                    "data": {
                        "is_read": true,
                        "read_at": chrono::Utc::now().to_rfc3339(),
                    }
                })),
            )
                .into_response();
        }
    };

    match store.mark_all_read().await {
        Ok(_count) => (
            StatusCode::OK,
            Json(json!({
                "code": "ok",
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
