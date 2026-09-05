//! Gateway handlers for the self-evolution admin API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Response, Sse,
    },
    Json,
};
use cog_core::EvolutionMetricsSnapshot;
use serde_json::json;
use std::sync::Arc;

/// List all known evolution artifacts.
pub async fn list_changes_handler(State(state): State<Arc<crate::GatewayState>>) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.list_changes().await {
            Ok(changes) => (StatusCode::OK, Json(json!({ "changes": changes }))).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "list_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// Apply a single change and run the workspace test suite.
pub async fn apply_change_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Path(change_id): Path<String>,
) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.apply_change(&change_id).await {
            Ok(result) => (StatusCode::OK, Json(result)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "apply_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// Evaluate an artifact-level policy candidate (产物级进化 §14.3).
/// An Adopt verdict stages the candidate at AwaitingReview; approving the
/// returned change id hot-swaps the policy version.
pub async fn evaluate_policy_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(req): Json<cog_core::PolicyEvalRequest>,
) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.evaluate_policy(req).await {
            Ok(info) => (StatusCode::OK, Json(info)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "policy_eval_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// Approve a change held at AwaitingReview (manual_approve human gate),
/// then commit, build, and optionally switch to it.
pub async fn approve_change_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Path(change_id): Path<String>,
) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.approve_change(&change_id).await {
            Ok(result) => (StatusCode::OK, Json(result)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "approve_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// Commit, build, and optionally switch to a change.
pub async fn deploy_change_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Path(change_id): Path<String>,
) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.deploy_change(&change_id).await {
            Ok(result) => (StatusCode::OK, Json(result)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "deploy_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// Return the current self-evolution counters from the D5 observable metrics.
pub async fn metrics_handler(State(state): State<Arc<crate::GatewayState>>) -> Response {
    let mut snapshot = EvolutionMetricsSnapshot {
        events_total: 0,
        events_failed: 0,
        changes_applied: 0,
        changes_failed: 0,
    };

    for observable in &state.observables {
        match observable.collect_metrics("D5").await {
            Ok(metrics) => {
                for m in metrics {
                    match m.name.as_str() {
                        "evolution_event_total" => snapshot.events_total = m.value as u64,
                        "evolution_event_failed_total" => snapshot.events_failed = m.value as u64,
                        "evolution_change_applied_total" => {
                            snapshot.changes_applied = m.value as u64
                        }
                        "evolution_change_failed_total" => snapshot.changes_failed = m.value as u64,
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to collect D5 metrics from observable");
            }
        }
    }

    (StatusCode::OK, Json(snapshot)).into_response()
}

/// SSE stream of change-row changes for the takeover console.
///
/// `EventSource` cannot set headers, so the JWT is accepted via the `token`
/// query parameter (same convention as `/ws`); the Authorization header is
/// also honored. Emits one JSON-encoded `EvolutionChangeInfo` per event.
pub async fn stream_handler(
    State(state): State<Arc<crate::GatewayState>>,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("");
    let query_params = crate::parse_query_params(query);

    let token = query_params.get("token").cloned().or_else(|| {
        req.headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|auth| {
                let (scheme, value) = auth.split_once(' ')?;
                if scheme.eq_ignore_ascii_case("bearer") {
                    Some(value.to_string())
                } else {
                    None
                }
            })
    });

    let Some(token) = token else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing_token", "message": "JWT token required"})),
        )
            .into_response();
    };

    if let Err(e) = state.jwt_manager.verify_token(&token).await {
        tracing::warn!("evolution stream JWT verification failed: {}", e);
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid_token", "message": "JWT token invalid or expired"})),
        )
            .into_response();
    }

    let Some(tx) = state.evolution_stream.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_stream_not_configured"})),
        )
            .into_response();
    };

    let rx = tx.subscribe();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(row) => {
                    let data = serde_json::to_string(&row).unwrap_or_default();
                    let event = Ok::<_, std::convert::Infallible>(Event::default().data(data));
                    return Some((event, rx));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "evolution stream lagged, dropping stale rows");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("ping"),
        )
        .into_response()
}

/// Roll back to the previously deployed binary and restart.
pub async fn rollback_handler(State(state): State<Arc<crate::GatewayState>>) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.rollback().await {
            Ok(result) => (StatusCode::OK, Json(result)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "rollback_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// Query parameters for the events endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct EventsQuery {
    /// Maximum number of events to return (default 50).
    pub limit: Option<usize>,
}

/// List recent evolution events (newest first).
pub async fn events_handler(
    State(state): State<Arc<crate::GatewayState>>,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(50);
    match state.evolution_admin {
        Some(ref admin) => match admin.list_events(limit).await {
            Ok(events) => (StatusCode::OK, Json(json!({ "events": events }))).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "events_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// 自动晋级运行时开关快照（一键暂停状态）。
pub async fn promotion_switch_handler(State(state): State<Arc<crate::GatewayState>>) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.promotion_switch().await {
            Ok(info) => (StatusCode::OK, Json(info)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "promotion_switch_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// 一键暂停/恢复请求体。
#[derive(Debug, serde::Deserialize)]
pub struct PromotionSwitchRequest {
    pub paused: bool,
    #[serde(default)]
    pub note: String,
}

/// 设置自动晋级运行时暂停标志：立即生效，重启后回落到配置文件值。
pub async fn set_promotion_switch_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(req): Json<PromotionSwitchRequest>,
) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.set_promotion_paused(req.paused, &req.note).await {
            Ok(info) => (StatusCode::OK, Json(info)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "promotion_switch_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// 晋级台账历史（新在前），接管台晋级历史页数据源。
pub async fn promotions_handler(
    State(state): State<Arc<crate::GatewayState>>,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(100);
    match state.evolution_admin {
        Some(ref admin) => match admin.list_promotions(limit).await {
            Ok(promotions) => {
                (StatusCode::OK, Json(json!({ "promotions": promotions }))).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "promotions_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}

/// 最新晋级周报（eval 长期趋势），含趋势向下告警。
pub async fn promotion_trend_handler(State(state): State<Arc<crate::GatewayState>>) -> Response {
    match state.evolution_admin {
        Some(ref admin) => match admin.promotion_trend().await {
            Ok(report) => (StatusCode::OK, Json(report)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "promotion_trend_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "evolution_admin_not_configured"})),
        )
            .into_response(),
    }
}
