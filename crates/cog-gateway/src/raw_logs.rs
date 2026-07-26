//! Query API for the `raw_log_index` table.
//! Backed by `Arc<dyn cog_core::RawLogIndexStore>` on the gateway state.
//! When the store is not configured the handler returns 503 so callers can
//! distinguish "no data" from "feature disabled".

use axum::response::IntoResponse;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;
use cog_core::{RawLogIndexEntry, RawLogQuery, StorageTier};

#[derive(Debug, Deserialize)]
pub struct RawLogsParams {
    pub stream: Option<String>,
    pub start: Option<chrono::DateTime<chrono::Utc>>,
    pub end: Option<chrono::DateTime<chrono::Utc>>,
    pub tier: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct RawLogsResponse {
    pub count: usize,
    pub entries: Vec<RawLogIndexEntry>,
}

pub async fn list_raw_logs_handler(
    State(state): State<Arc<GatewayState>>,
    Query(params): Query<RawLogsParams>,
) -> axum::response::Response {
    let Some(ref store) = state.raw_log_index_store else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "raw_log_index store not configured",
            })),
        )
            .into_response();
    };

    let q = RawLogQuery {
        hour: None,
        stream: params.stream,
        start: params.start,
        end: params.end,
        tier: params
            .tier
            .as_deref()
            .and_then(|s| s.parse::<StorageTier>().ok()),
        limit: params.limit,
    };

    match store.query(&q).await {
        Ok(entries) => (
            StatusCode::OK,
            Json(RawLogsResponse {
                count: entries.len(),
                entries,
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
