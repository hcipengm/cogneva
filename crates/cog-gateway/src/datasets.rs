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

fn entry_json(entry: &cog_core::DatasetEntry) -> serde_json::Value {
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
                    "code": crate::error::simple_code::OK,
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
                    "code": crate::error::simple_code::INTERNAL_ERROR,
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
                "code": crate::error::simple_code::OK,
                "data": entry_json(&entry),
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": crate::error::simple_code::NOT_FOUND,
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
                    "code": crate::error::simple_code::INTERNAL_ERROR,
                    "error": "catalog_read_failed",
                    "message": e.to_string(),
                })),
            )
                .into_response()
        }
    }
}

/// Verify every catalogued version against the bytes in the store.
///
/// The migration gate. A restore that finished and a restore that carried
/// nothing look identical from outside -- both leave a process answering
/// requests -- so this route reports what was actually compared: how many
/// versions were checked, and for each one whether its bytes are still the ones
/// the catalog recorded. An empty catalog answers `all_verified: false`, because
/// a pass that verified nothing has not passed.
pub async fn verify_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<ListQuery>,
) -> Response {
    let Some(ref store) = state.datasets else {
        return unavailable();
    };
    // A cap rather than no bound: this walks object storage, and a request that
    // reads every object in the bucket is a request that blocks for as long as
    // the bucket is large.
    let limit = query.limit.unwrap_or(10_000);
    match store.verify(limit).await {
        Ok(report) => {
            let checks: Vec<_> = report
                .checks
                .iter()
                .map(|check| {
                    let (outcome, detail) = match &check.outcome {
                        cog_core::DatasetOutcome::Verified => ("verified", json!({})),
                        cog_core::DatasetOutcome::Mismatched {
                            recorded,
                            actual,
                            recorded_bytes,
                            actual_bytes,
                        } => (
                            "mismatched",
                            json!({
                                "recorded_sha256": recorded,
                                "actual_sha256": actual,
                                "recorded_bytes": recorded_bytes,
                                "actual_bytes": actual_bytes,
                            }),
                        ),
                        cog_core::DatasetOutcome::Missing => ("missing", json!({})),
                        cog_core::DatasetOutcome::Unreadable(e) => {
                            ("unreadable", json!({ "error": e }))
                        }
                    };
                    let mut row = json!({
                        "name": check.name,
                        "version": check.version,
                        "object_key": check.object_key,
                        "outcome": outcome,
                    });
                    if let (Some(row), serde_json::Value::Object(detail)) =
                        (row.as_object_mut(), detail)
                    {
                        row.extend(detail);
                    }
                    row
                })
                .collect();
            let all_verified = report.all_verified();
            let status = if all_verified {
                StatusCode::OK
            } else {
                // 一次没通过的校验不是一个「处理成功」的请求：调用方读状态码就够了，
                // 不必去解析正文才知道迁移没成功。
                StatusCode::CONFLICT
            };
            (
                status,
                Json(json!({
                    "code": if all_verified { "ok" } else { "verification_failed" },
                    "data": {
                        "checked": report.checked(),
                        "verified": report.verified(),
                        "failed": report.failed(),
                        "all_verified": all_verified,
                        "checks": checks,
                    }
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, "verifying the dataset catalog failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": crate::error::simple_code::INTERNAL_ERROR,
                    "error": "catalog_verify_failed",
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
            "code": crate::error::simple_code::SERVICE_UNAVAILABLE,
            "error": "dataset_catalog_not_configured",
            "message": "No dataset catalog is configured on this deployment",
        })),
    )
        .into_response()
}
