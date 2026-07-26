use axum::{
    body::{to_bytes, Body},
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

/// Extract actual token usage and cost from a JSON response body.
fn extract_usage_from_response(body: &[u8]) -> (u64, f64) {
    let json: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return (0, 0.0),
    };

    let usage = match json.get("usage") {
        Some(u) => u,
        None => return (0, 0.0),
    };

    let total_tokens = usage
        .get("total_tokens")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            let input = usage
                .get("input_tokens")
                .or_else(|| usage.get("prompt_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let output = usage
                .get("output_tokens")
                .or_else(|| usage.get("completion_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            Some(input + output)
        })
        .unwrap_or(0);

    let cost = usage
        .get("cost")
        .and_then(|c| c.get("total"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    (total_tokens, cost)
}

/// Estimate tokens from the request body.
/// Empty bodies (typical for GET/HEAD status queries) cost 0 tokens so that
/// polling task status does not drain the daily quota.
fn estimate_tokens_from_body(body: &[u8]) -> u64 {
    if body.is_empty() {
        return 0;
    }

    if let Ok(json) = serde_json::from_slice::<Value>(body) {
        if let Some(tokens) = json.get("estimated_tokens").and_then(|v| v.as_u64()) {
            return tokens;
        }

        if let Some(messages) = json.get("messages").and_then(|v| v.as_array()) {
            let total_chars: usize = messages
                .iter()
                .filter_map(|m| m.get("content").and_then(|c| c.as_str()))
                .map(|c| c.len())
                .sum();
            return ((total_chars / 4) as u64).max(1);
        }

        if let Some(prompt) = json.get("prompt").and_then(|v| v.as_str()) {
            return ((prompt.len() / 4) as u64).max(1);
        }
    }

    // Fallback: estimate from raw body length without inflating to the default.
    // The default is only meaningful when the body cannot be inspected at all;
    // here we have a non-empty body, so a length heuristic is sufficient.
    ((body.len() / 4) as u64).max(1)
}

/// Axum middleware that enforces quota checks.
/// Expects `user_id` and optionally `workspace_id` to be set in request extensions
/// by an upstream auth middleware.
pub async fn quota_middleware(
    manager: Arc<dyn cog_core::QuotaManager>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path.starts_with("/health") {
        return next.run(request).await;
    }

    let user_id = request
        .extensions()
        .get::<cog_core::Claims>()
        .map(|c| c.sub.clone())
        .unwrap_or_default();

    let workspace_id = request
        .extensions()
        .get::<(String, String)>()
        .map(|(_, ws_id)| ws_id.clone());

    if user_id.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "user_id not found in request extensions" })),
        )
            .into_response();
    }

    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "failed to read request body" })),
            )
                .into_response();
        }
    };

    let estimated_tokens = estimate_tokens_from_body(&bytes);

    let ws_id_ref = workspace_id.as_deref();
    let pre_check = manager
        .pre_check(&user_id, ws_id_ref, estimated_tokens)
        .await;

    if !pre_check.allowed {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": "quota exceeded",
                "remaining": pre_check.remaining,
                "estimated_tokens": estimated_tokens
            })),
        )
            .into_response();
    }

    let body = Body::from(bytes.clone());
    let request = Request::from_parts(parts, body);

    let response = next.run(request).await;

    let (mut parts, body) = response.into_parts();
    let resp_bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            let _ = manager
                .finalize(&user_id, ws_id_ref, estimated_tokens, estimated_tokens)
                .await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to read response body" })),
            )
                .into_response();
        }
    };

    let (actual_tokens, actual_cost) = extract_usage_from_response(&resp_bytes);
    let actual_tokens = if actual_tokens > 0 {
        actual_tokens
    } else {
        estimated_tokens
    };

    let _ = manager
        .finalize(&user_id, ws_id_ref, estimated_tokens, actual_tokens)
        .await;

    if actual_cost > 0.0 {
        if let Ok(header_value) = axum::http::HeaderValue::from_str(&format!("{:.6}", actual_cost))
        {
            parts.headers.insert(
                axum::http::HeaderName::from_static("x-actual-cost"),
                header_value,
            );
        }
    }

    let body = Body::from(resp_bytes);
    Response::from_parts(parts, body)
}
