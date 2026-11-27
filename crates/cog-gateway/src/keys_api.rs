//! 密钥页的读写面：列、签发、轮换、吊销。
//!
//! 私钥不经过这里：页面交出的是公钥与指纹（私钥在 KMS/密钥库里生成），事件里只
//! 留下公钥、指纹与动作。服务没接上时如实答 503，不假装成功。

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": "密钥服务没有接上" })),
    )
        .into_response()
}

pub async fn list_keys_handler(State(state): State<Arc<crate::GatewayState>>) -> Response {
    let Some(keys) = state.security_keys.clone() else {
        return unavailable();
    };
    match keys.list().await {
        Ok(rows) => (StatusCode::OK, Json(json!({ "keys": rows }))).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct IssueKeyBody {
    pub key_id: String,
    pub purpose: String,
    pub public_key: String,
    pub fingerprint: String,
    pub actor: String,
}

pub async fn issue_key_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(body): Json<IssueKeyBody>,
) -> Response {
    let Some(keys) = state.security_keys.clone() else {
        return unavailable();
    };
    match keys
        .issue(
            &body.key_id,
            &body.purpose,
            &body.public_key,
            &body.fingerprint,
            &body.actor,
            chrono::Utc::now(),
        )
        .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "issued": body.key_id }))).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct RotateKeyBody {
    pub key_id: String,
    pub previous_key_id: String,
    pub public_key: String,
    pub fingerprint: String,
    pub overlap_until: Option<String>,
    pub actor: String,
}

pub async fn rotate_key_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(body): Json<RotateKeyBody>,
) -> Response {
    let Some(keys) = state.security_keys.clone() else {
        return unavailable();
    };
    match keys
        .rotate(
            &body.key_id,
            &body.previous_key_id,
            &body.public_key,
            &body.fingerprint,
            body.overlap_until,
            &body.actor,
            chrono::Utc::now(),
        )
        .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "rotated": body.key_id }))).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct RevokeKeyBody {
    pub key_id: String,
    pub reason: String,
    pub actor: String,
}

pub async fn revoke_key_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(body): Json<RevokeKeyBody>,
) -> Response {
    let Some(keys) = state.security_keys.clone() else {
        return unavailable();
    };
    match keys
        .revoke(&body.key_id, &body.reason, &body.actor, chrono::Utc::now())
        .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "revoked": body.key_id }))).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}
