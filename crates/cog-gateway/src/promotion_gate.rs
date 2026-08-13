//! Admin API for the config-level auto-promotion gate.
//!
//! The runtime pause switch cannot cross the config gate — the config file
//! is the deliberate safety boundary. This endpoint gives the WebUI a
//! sanctioned path to flip that one flag: it edits
//! `self_evolution.promotion.enabled` inside the `cogneva-json` ConfigMap
//! and rolls the main app so the new value is loaded. All other gate
//! settings (quota, breakers, soak, path classes) keep their defaults.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

use crate::llm_admin::KubeClient;

const CONFIGMAP_NAME: &str = "cogneva-json";
const CONFIGMAP_KEY: &str = "cogneva.json";
const MAIN_DEPLOYMENT: &str = "cogneva";

#[derive(Debug, Deserialize)]
pub struct PromotionGateRequest {
    pub enabled: bool,
}

fn err(status: StatusCode, code: &str, message: String) -> Response {
    (status, Json(json!({"error": code, "message": message}))).into_response()
}

pub async fn set_promotion_gate_handler(Json(req): Json<PromotionGateRequest>) -> Response {
    let client = match KubeClient::in_cluster() {
        Ok(c) => c,
        Err(e) => return err(StatusCode::SERVICE_UNAVAILABLE, "not_in_cluster", e),
    };
    let cm_path = format!(
        "/api/v1/namespaces/{}/configmaps/{}",
        client.namespace(),
        CONFIGMAP_NAME
    );
    let cm = match client.get_json(&cm_path).await {
        Ok(v) => v,
        Err(e) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "configmap_read_failed",
                e,
            )
        }
    };
    let raw = cm
        .get("data")
        .and_then(|d| d.get(CONFIGMAP_KEY))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let mut root: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(e) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "config_parse_failed",
                format!("{CONFIGMAP_KEY} 不是合法 JSON: {e}"),
            )
        }
    };

    if !root.get("self_evolution").is_some_and(|v| v.is_object()) {
        root["self_evolution"] = json!({});
    }
    let se = &mut root["self_evolution"];
    if !se.get("promotion").is_some_and(|v| v.is_object()) {
        se["promotion"] = json!({});
    }
    se["promotion"]["enabled"] = json!(req.enabled);

    let rendered = match serde_json::to_string_pretty(&root) {
        Ok(s) => s,
        Err(e) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "config_render_failed",
                e.to_string(),
            )
        }
    };
    if let Err(e) = client
        .patch(&cm_path, json!({"data": {CONFIGMAP_KEY: rendered}}))
        .await
    {
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "configmap_write_failed",
            e,
        );
    }

    // Config is read at process start; roll the main app to pick it up.
    let restarted_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_default();
    if let Err(e) = client
        .patch(
            &format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                client.namespace(),
                MAIN_DEPLOYMENT
            ),
            json!({"spec": {"template": {
                "metadata": {"annotations": {"cogneva.io/restartedAt": restarted_at}},
            }}}),
        )
        .await
    {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "restart_failed", e);
    }

    Json(json!({"ok": true, "enabled": req.enabled})).into_response()
}
