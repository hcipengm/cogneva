//! Admin API for LLM provider configuration.
//!
//! The WebUI setup wizard uses these endpoints to connect the system to an
//! LLM after installation. Saving writes the API key into the
//! `cogneva-secrets` Secret, updates the `cogneva-json` ConfigMap's
//! `llm_routing.backends[0]`, and rolls the LLM-consuming deployments so the
//! new configuration takes effect.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const API_BASE: &str = "https://kubernetes.default.svc";
const SECRET_NAME: &str = "cogneva-secrets";
const SECRET_KEY: &str = "llm-api-key";
const CONFIGMAP_NAME: &str = "cogneva-json";
const CONFIGMAP_KEY: &str = "cogneva.json";
const KEY_PLACEHOLDER: &str = "${KIMI_API_KEY}";
const VALID_API_STYLES: [&str; 4] = ["openai", "anthropic", "google", "ollama"];
const RESTART_DEPLOYMENTS: [&str; 3] = ["cogneva", "cogneva-evolution", "cogneva-security-gateway"];

#[derive(Debug, Deserialize)]
pub struct LlmConfigRequest {
    pub provider: String,
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    pub api_style: Option<String>,
}

/// GET /api/v1/admin/llm-status — which backends exist and whether any
/// enabled one has a resolvable API key. Never returns key material.
pub async fn llm_status_handler(State(_state): State<Arc<crate::GatewayState>>) -> Response {
    let path = std::env::var("COGNEVA_CONFIG_PATH")
        .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".to_string());
    let backends = match std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .and_then(|cfg| cfg.get("llm_routing")?.get("backends")?.as_array().cloned())
    {
        Some(list) => list
            .iter()
            .map(|b| {
                let key = b.get("api_key").and_then(|k| k.as_str()).unwrap_or("");
                json!({
                    "provider": b.get("provider").and_then(|v| v.as_str()).unwrap_or(""),
                    "base_url": b.get("base_url").and_then(|v| v.as_str()).unwrap_or(""),
                    "model": b.get("model").and_then(|v| v.as_str()).unwrap_or(""),
                    "api_style": b.get("api_style").and_then(|v| v.as_str()).unwrap_or("openai"),
                    "enabled": b.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false),
                    "has_key": key_resolves(key),
                })
            })
            .collect::<Vec<_>>(),
        None => Vec::new(),
    };
    let configured = backends
        .iter()
        .any(|b| b["enabled"] == json!(true) && b["has_key"] == json!(true));
    (
        StatusCode::OK,
        Json(json!({ "configured": configured, "backends": backends })),
    )
        .into_response()
}

/// A key "resolves" when it is either a literal non-empty value or a
/// `${VAR}` reference whose environment variable is set and non-empty.
fn key_resolves(raw: &str) -> bool {
    let trimmed = raw.trim();
    if let Some(var) = trimmed.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        return std::env::var(var).map(|v| !v.is_empty()).unwrap_or(false);
    }
    !trimmed.is_empty()
}

/// POST /api/v1/admin/llm-config — persist provider settings and roll the
/// deployments that consume them.
pub async fn llm_config_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<LlmConfigRequest>,
) -> Response {
    let provider = req.provider.trim();
    let base_url = req.base_url.trim();
    let model = req.model.trim();
    let api_key = req.api_key.trim();
    let api_style = req
        .api_style
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("openai");

    if provider.is_empty() || model.is_empty() || api_key.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": "invalid_request", "message": "provider、model、api_key 不能为空"}),
            ),
        )
            .into_response();
    }
    if !(base_url.starts_with("https://") || base_url.starts_with("http://")) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_request", "message": "base_url 必须是 http(s):// 开头的完整地址"})),
        )
            .into_response();
    }
    if !VALID_API_STYLES.contains(&api_style) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_request", "message": format!("api_style 只支持 {}", VALID_API_STYLES.join("/"))})),
        )
            .into_response();
    }

    let kube = match KubeClient::in_cluster() {
        Ok(k) => k,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "not_in_cluster", "message": e})),
            )
                .into_response();
        }
    };

    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/{}",
                kube.namespace, SECRET_NAME
            ),
            json!({"stringData": {SECRET_KEY: api_key}}),
        )
        .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
        )
            .into_response();
    }

    if let Err(e) = kube
        .update_llm_backend(provider, base_url, model, api_style)
        .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "configmap_patch_failed", "message": e})),
        )
            .into_response();
    }

    let mut restarted = Vec::new();
    let mut failed = Vec::new();
    for name in RESTART_DEPLOYMENTS {
        let body = json!({
            "spec": {"template": {"metadata": {"annotations": {
                "kubectl.kubernetes.io/restartedAt": chrono::Utc::now().to_rfc3339()
            }}}}
        });
        match kube
            .patch(
                &format!(
                    "/apis/apps/v1/namespaces/{}/deployments/{}",
                    kube.namespace, name
                ),
                body,
            )
            .await
        {
            Ok(()) => restarted.push(name),
            Err(e) => {
                tracing::warn!(deployment = name, error = %e, "rollout restart failed");
                failed.push(name);
            }
        }
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "restarted": restarted,
            "failed_restarts": failed,
            "message": "配置已保存，相关组件正在滚动重启，约一分钟后生效",
        })),
    )
        .into_response()
}

/// Minimal in-cluster Kubernetes API client (service account token + CA).
struct KubeClient {
    http: reqwest::Client,
    token: String,
    namespace: String,
}

impl KubeClient {
    fn in_cluster() -> Result<Self, String> {
        let read = |name: &str| {
            std::fs::read(format!("{SA_DIR}/{name}"))
                .map_err(|e| format!("无法读取 {SA_DIR}/{name}（不在 K8s 集群内？）: {e}"))
        };
        let token = String::from_utf8_lossy(&read("token")?).trim().to_string();
        let namespace = read("namespace")
            .map(|b| String::from_utf8_lossy(&b).trim().to_string())
            .unwrap_or_else(|_| "cogneva".to_string());
        let ca = reqwest::Certificate::from_pem(&read("ca.crt")?)
            .map_err(|e| format!("集群 CA 证书解析失败: {e}"))?;
        let http = reqwest::Client::builder()
            .add_root_certificate(ca)
            .build()
            .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?;
        Ok(Self {
            http,
            token,
            namespace,
        })
    }

    async fn patch(&self, path: &str, body: serde_json::Value) -> Result<(), String> {
        let resp = self
            .http
            .patch(format!("{API_BASE}{path}"))
            .bearer_auth(&self.token)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/strategic-merge-patch+json",
            )
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("请求 apiserver 失败: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("apiserver 返回 {status}: {text}"));
        }
        Ok(())
    }

    /// Rewrite `llm_routing.backends[0]` inside the cogneva.json ConfigMap,
    /// keeping the `${KIMI_API_KEY}` placeholder (the key itself lives in the
    /// Secret and is injected via env).
    async fn update_llm_backend(
        &self,
        provider: &str,
        base_url: &str,
        model: &str,
        api_style: &str,
    ) -> Result<(), String> {
        let cm_path = format!(
            "/api/v1/namespaces/{}/configmaps/{}",
            self.namespace, CONFIGMAP_NAME
        );
        let resp = self
            .http
            .get(format!("{API_BASE}{cm_path}"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("读取 configmap 失败: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("读取 configmap 返回 {status}: {text}"));
        }
        let cm: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("configmap 响应解析失败: {e}"))?;
        let raw = cm
            .get("data")
            .and_then(|d| d.get(CONFIGMAP_KEY))
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("configmap 缺少 data.{CONFIGMAP_KEY}"))?;
        let mut cfg: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| format!("cogneva.json 解析失败: {e}"))?;

        let new_backend = json!({
            "provider": provider,
            "api_key": KEY_PLACEHOLDER,
            "base_url": base_url,
            "model": model,
            "api_style": api_style,
            "weight": 1,
            "enabled": true,
        });
        let routing = cfg
            .as_object_mut()
            .ok_or("cogneva.json 不是 JSON 对象")?
            .entry("llm_routing")
            .or_insert_with(|| json!({}));
        let backends = routing
            .as_object_mut()
            .ok_or("llm_routing 不是 JSON 对象")?
            .entry("backends")
            .or_insert_with(|| json!([]));
        match backends.as_array_mut() {
            Some(arr) if !arr.is_empty() => arr[0] = new_backend,
            Some(arr) => arr.push(new_backend),
            None => *backends = json!([new_backend]),
        }

        let rendered = serde_json::to_string_pretty(&cfg)
            .map_err(|e| format!("cogneva.json 序列化失败: {e}"))?;
        self.patch(&cm_path, json!({"data": {CONFIGMAP_KEY: rendered}}))
            .await
    }
}
