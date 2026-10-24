//! The dataset catalog's read face.
//!
//! The catalog is written by whatever stores the bytes (the upload route today,
//! any producer through `DatasetStore` tomorrow). These routes are how a person
//! or a restore asks it what exists: which versions a dataset has, and what one
//! version says about itself -- size, digest, coverage, source.
//!
//! No route here returns the bytes. Reading a dataset back is a different
//! decision from reading its card: one is a restore step that has to verify a
//! digest, the other is a listing. Keeping them apart means a listing can cost
//! nothing and a restore can never be mistaken for one.

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

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// How many versions to return, newest first. Defaults to 20 rather than
    /// unlimited: a catalog's whole history is not what a page asks for.
    pub limit: Option<usize>,
}

fn entry_json(entry: &cog_storage::DatasetEntry) -> serde_json::Value {
    json!({
        "name": entry.name,
        "version": entry.version,
        "object_key": entry.object_key,
        "content_sha256": entry.content_sha256,
        "bytes": entry.bytes,
        // 没有这一格就是没有，不是一个 0——不可数和非空表是两件事。
        "row_count": entry.row_count,
        "coverage_from": entry.coverage_from.map(|t| t.to_rfc3339()),
        "coverage_to": entry.coverage_to.map(|t| t.to_rfc3339()),
        "source": entry.source,
        "media_type": entry.media_type,
        "created_at": entry.created_at.to_rfc3339(),
    })
}

pub async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    Path(name): Path<String>,
    Query(query): Query<ListQuery>,
) -> Response {
    let Some(ref store) = state.datasets else {
        return unavailable();
    };
    let limit = query.limit.unwrap_or(20);
    match store.versions(&name, limit).await {
        Ok(versions) => {
            let rows: Vec<_> = versions.iter().map(entry_json).collect();
            (
                StatusCode::OK,
                Json(json!({
                    "code": "ok",
                    "data": {
                        "name": name,
                        "count": rows.len(),
                        "versions": rows,
                    }
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(dataset = %name, error = %e, "listing a dataset's versions failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": "internal_error",
                    "error": "catalog_read_failed",
                    "message": e.to_string(),
                })),
            )
                .into_response()
        }
    }
}

pub async fn get_handler(
    State(state): State<Arc<GatewayState>>,
    Path((name, version)): Path<(String, String)>,
) -> Response {
    let Some(ref store) = state.datasets else {
        return unavailable();
    };
    match store.find(&name, &version).await {
        Ok(Some(entry)) => (
            StatusCode::OK,
            Json(json!({
                "code": "ok",
                "data": entry_json(&entry),
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": "not_found",
                "error": "dataset_version_not_found",
                "name": name,
                "version": version,
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(dataset = %name, version = %version, error = %e, "reading a dataset version failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": "internal_error",
                    "error": "catalog_read_failed",
                    "message": e.to_string(),
                })),
            )
                .into_response()
        }
    }
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "code": "service_unavailable",
            "error": "dataset_catalog_not_configured",
            "message": "No dataset catalog is configured on this deployment",
        })),
    )
        .into_response()
}
