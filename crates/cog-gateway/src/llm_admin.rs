//! Admin API for LLM provider configuration.
//!
//! The WebUI setup wizard uses these endpoints to connect the system to an
//! LLM after installation. Saving writes the upstream pool (one entry for a
//! single LLM, more for failover) into the `cogneva-secrets` Secret as the
//! `llm-upstreams` JSON entry — the gateway's only upstream source. Only the
//! gateway holds credentials; the main app and sandbox talk to the gateway,
//! so rolling the gateway is the only restart needed. The gateway fails over
//! across the pool on 429 (rate limit) / 402 (quota exhausted).

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
/// 上游池条目：JSON 数组，每元素 {api_style, base_url, model, api_key}。
const SECRET_UPSTREAMS_KEY: &str = "llm-upstreams";
const GATEWAY_DEPLOYMENT: &str = "cogneva-security-gateway";

#[derive(Debug, Deserialize)]
pub struct LlmUpstreamRequest {
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    /// 协议面首猜（openai/anthropic）：探测按此顺序优先，失败自动换另一种。
    pub api_style: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LlmConfigRequest {
    /// 可选，仅作展示；网关按协议面路由，不消费该字段。
    pub provider: Option<String>,
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    /// 协议面首猜（openai/anthropic）：探测按此顺序优先，失败自动换另一种。
    pub api_style: Option<String>,
    /// 探测失败后的"仍然保存"：跳过连通验证直接落库。
    pub skip_verify: Option<bool>,
    /// 追加的故障转移上游：主上游 429/402 时按声明顺序顶上。
    pub extra_upstreams: Option<Vec<LlmUpstreamRequest>>,
}

/// GET /api/v1/admin/llm-status — whether the security gateway holds a
/// configured upstream pool. Never returns key material. In-cluster the
/// source of truth is the `llm-upstreams` Secret entry (the main app is
/// credential-free); outside the cluster we fall back to inspecting local
/// cogneva.json.
pub async fn llm_status_handler(State(_state): State<Arc<crate::GatewayState>>) -> Response {
    if let Ok(kube) = KubeClient::in_cluster() {
        if let Ok(configured) = kube.secret_has_key().await {
            return (
                StatusCode::OK,
                Json(json!({ "configured": configured, "backends": local_backends() })),
            )
                .into_response();
        }
    }
    let backends = local_backends();
    let configured = backends
        .iter()
        .any(|b| b["enabled"] == json!(true) && b["has_key"] == json!(true));
    (
        StatusCode::OK,
        Json(json!({ "configured": configured, "backends": backends })),
    )
        .into_response()
}

/// Backends declared in the local cogneva.json (informational only — after
/// credential narrowing the primary backend points at the security gateway).
fn local_backends() -> Vec<serde_json::Value> {
    let path = std::env::var("COGNEVA_CONFIG_PATH")
        .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".to_string());
    match std::fs::read_to_string(&path)
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
    }
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
    let skip_verify = req.skip_verify.unwrap_or(false);
    let primary = LlmUpstreamRequest {
        base_url: req.base_url.clone(),
        model: req.model.clone(),
        api_key: req.api_key.clone(),
        api_style: req.api_style.clone(),
    };
    let mut requests = vec![primary];
    requests.extend(req.extra_upstreams.unwrap_or_default());

    // 逐条校验 + 实证探测：任一上游验证失败则整单拒绝，避免落下半残池。
    let mut pool: Vec<serde_json::Value> = Vec::with_capacity(requests.len());
    for item in &requests {
        match resolve_upstream(item, skip_verify).await {
            Ok(entry) => pool.push(entry),
            Err(resp) => return resp,
        }
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

    let pool_json = match serde_json::to_string(&pool) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "serialize_failed", "message": e.to_string()})),
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
            json!({"stringData": {SECRET_UPSTREAMS_KEY: pool_json}}),
        )
        .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
        )
            .into_response();
    }

    if let Err(e) = kube.restart_gateway().await {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "gateway_patch_failed", "message": e})),
        )
            .into_response();
    }

    // restartedAt 注解触发网关滚动重启，新 Pod 启动时从 Secret 读
    // COGNEVA_LLM_UPSTREAMS 拿到完整上游池。
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "restarted": [GATEWAY_DEPLOYMENT],
            "upstreams": pool.len(),
            "message": "配置已保存，安全网关正在滚动重启，约一分钟后生效",
        })),
    )
        .into_response()
}

/// 校验单条上游并定协议面：返回可写入池的 JSON 条目；验证失败返回
/// 直接可回给前端的错误响应。
async fn resolve_upstream(
    item: &LlmUpstreamRequest,
    skip_verify: bool,
) -> Result<serde_json::Value, Response> {
    let base_url = item.base_url.trim();
    let model = item.model.trim();
    let api_key = item.api_key.trim();
    let style_hint = item
        .api_style
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if model.is_empty() || api_key.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_request", "message": "model、api_key 不能为空"})),
        )
            .into_response());
    }
    if !(base_url.starts_with("https://") || base_url.starts_with("http://")) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_request", "message": "base_url 必须是 http(s):// 开头的完整地址"})),
        )
            .into_response());
    }

    // 双协议实证探测：按首猜顺序先试，失败自动换另一种协议面，
    // 胜出者即为写入网关的 api_style（Kimi 这类 base_url 看不出协议的
    // 端点也能当场测出来）。skip_verify 时采信首猜，缺省 openai。
    let api_style = if skip_verify {
        match style_hint {
            Some("anthropic") => "anthropic",
            _ => "openai",
        }
    } else {
        match detect_api_style(base_url, model, api_key, style_hint).await {
            Ok(style) => style,
            Err(message) => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    Json(json!({"error": "llm_verify_failed", "message": message})),
                )
                    .into_response());
            }
        }
    };

    Ok(json!({
        "api_style": api_style,
        "base_url": base_url,
        "model": model,
        "api_key": api_key,
    }))
}

/// 保存前用用户手填的真 key 做实证协议探测（每种协议超时 8 秒）：
/// 按首猜顺序先试 openai 风格 `/models`，失败换 anthropic 风格
/// `/v1/messages`（max_tokens=1 的 ping），哪个通就返回哪个协议面。
/// base_url 看不出协议的端点（如 Kimi coding）也能当场测出。
/// 401/403 判定为密钥无效，连接失败判定为端点不可达，其余非 2xx 透传
/// 上游状态。两种都失败时返回最后一次错误；前端可带 skip_verify 重试
/// （部分企业网关会挡 /models 等探测路径）。
async fn detect_api_style(
    base_url: &str,
    model: &str,
    api_key: &str,
    style_hint: Option<&str>,
) -> Result<&'static str, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?;
    let base = base_url.trim_end_matches('/');
    let first_openai = style_hint != Some("anthropic");
    let mut last_err = String::new();
    for style in if first_openai {
        ["openai", "anthropic"]
    } else {
        ["anthropic", "openai"]
    } {
        let result = match style {
            "openai" => {
                client
                    .get(format!("{base}/models"))
                    .bearer_auth(api_key)
                    .send()
                    .await
            }
            _ => {
                client
                    .post(format!("{base}/v1/messages"))
                    .header("x-api-key", api_key)
                    .header("anthropic-version", "2023-06-01")
                    .json(&json!({
                        "model": model,
                        "max_tokens": 1,
                        "messages": [{"role": "user", "content": "ping"}]
                    }))
                    .send()
                    .await
            }
        };
        match result {
            Ok(resp) if resp.status().is_success() => return Ok(style),
            Ok(resp) => {
                let status = resp.status();
                last_err = match status.as_u16() {
                    401 | 403 => {
                        format!("密钥被 {base_url} 拒绝（HTTP {status}），请检查 API Key")
                    }
                    _ => format!("{base_url} 返回 HTTP {status}，请检查地址与模型名"),
                };
                // 认证类错误换协议面重试无意义，直接失败
                if status.as_u16() == 401 || status.as_u16() == 403 {
                    return Err(last_err);
                }
            }
            Err(e) => {
                last_err = format!("无法连接 {base_url}（{e}），请检查地址与网络");
                // 连接都建立不了，换协议面也连不上，直接失败
                return Err(last_err);
            }
        }
    }
    Err(last_err)
}

/// Minimal in-cluster Kubernetes API client (service account token + CA).
pub(crate) struct KubeClient {
    http: reqwest::Client,
    token: String,
    namespace: String,
}

impl KubeClient {
    pub(crate) fn in_cluster() -> Result<Self, String> {
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

    /// GET an apiserver path and return the parsed JSON body.
    pub(crate) async fn get_json(&self, path: &str) -> Result<serde_json::Value, String> {
        let resp = self
            .http
            .get(format!("{API_BASE}{path}"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("请求 apiserver 失败: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("apiserver 返回 {}", resp.status()));
        }
        resp.json()
            .await
            .map_err(|e| format!("apiserver 响应解析失败: {e}"))
    }

    /// 当前命名空间。
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) async fn patch(&self, path: &str, body: serde_json::Value) -> Result<(), String> {
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

    /// 上游池是否已写入集群 Secret（网关凭证的唯一事实源）。
    async fn secret_has_key(&self) -> Result<bool, String> {
        let resp = self
            .http
            .get(format!(
                "{API_BASE}/api/v1/namespaces/{}/secrets/{}",
                self.namespace, SECRET_NAME
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("读取 secret 失败: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("读取 secret 返回 {}", resp.status()));
        }
        let secret: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("secret 响应解析失败: {e}"))?;
        let has = secret
            .get("data")
            .and_then(|d| d.get(SECRET_UPSTREAMS_KEY))
            .and_then(|v| v.as_str())
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        Ok(has)
    }

    /// 滚动重启安全网关：上游池整体在 Secret 里（env 引用 Secret 条目，
    /// 内容变化不会触发重启），restartedAt 注解保证每次保存都滚出新 Pod
    /// 读取新池。
    async fn restart_gateway(&self) -> Result<(), String> {
        let restarted_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default();
        self.patch(
            &format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                self.namespace, GATEWAY_DEPLOYMENT
            ),
            json!({
                "spec": {"template": {
                    "metadata": {"annotations": {"cogneva.io/restartedAt": restarted_at}},
                }}
            }),
        )
        .await
    }
}
