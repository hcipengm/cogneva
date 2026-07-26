//! GitHub webhook 事件入口（discovery_mode=events/both 时启用）。
//!
//! 接收 GitHub webhook POST，验证 `X-Hub-Signature-256`（HMAC-SHA256）后，
//! 将 issue 事件（issues opened/edited/reopened、issue_comment created）
//! 驱动给共享的 [`GitHubDiscoveryLoop`] 处理。与轮询模式互补：
//! 轮询兜底，事件提供秒级响应。
//!
//! 安全约定：secret 只从环境变量读取（主进程解析，永不进沙盒）；
//! 未配置 secret 时拒绝启动（不接受伪造事件）。

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::discovery_loop::GitHubDiscoveryLoop;
use crate::provider::CiFailureEvent;

type HmacSha256 = Hmac<Sha256>;

/// 共享给 webhook handler 的状态。
#[derive(Clone)]
pub struct WebhookState {
    /// 轮询与事件共享的 discovery loop。
    pub discovery_loop: Arc<tokio::sync::Mutex<GitHubDiscoveryLoop>>,
    /// HMAC-SHA256 签名验证 secret（仅主进程内存持有）。
    pub secret: Arc<str>,
}

/// 验证 GitHub webhook 签名：`sha256=<hex HMAC-SHA256(secret, body)>`。
pub fn verify_signature(secret: &str, body: &[u8], signature_header: &str) -> bool {
    let Some(hex_sig) = signature_header.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(expected) = hex::decode(hex_sig) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

/// 从事件类型 + action + payload 提取需要处理的 issue 号。
pub fn extract_issue_number(event: &str, action: &str, payload: &serde_json::Value) -> Option<u64> {
    match (event, action) {
        ("issues", "opened" | "edited" | "reopened") => payload["issue"]["number"].as_u64(),
        // 评论是澄清对话的回复信号，重跑该 issue 的管线。
        ("issue_comment", "created") => payload["issue"]["number"].as_u64(),
        _ => None,
    }
}

/// 从 workflow_run completed 事件提取 CI 失败信息（conclusion=failure）。
pub fn extract_ci_failure(
    event: &str,
    action: &str,
    payload: &serde_json::Value,
) -> Option<CiFailureEvent> {
    if (event, action) != ("workflow_run", "completed") {
        return None;
    }
    let run = &payload["workflow_run"];
    if run["conclusion"].as_str() != Some("failure") {
        return None;
    }
    Some(CiFailureEvent {
        run_id: run["id"].as_u64()?,
        workflow_name: run["name"].as_str().unwrap_or_default().to_string(),
        head_sha: run["head_sha"].as_str().unwrap_or_default().to_string(),
        head_branch: run["head_branch"].as_str().unwrap_or_default().to_string(),
        html_url: run["html_url"].as_str().unwrap_or_default().to_string(),
    })
}

async fn webhook_handler(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let event = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    if !verify_signature(&state.secret, &body, signature) {
        tracing::warn!(event = %event, "Webhook 签名验证失败，已拒绝");
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    // ping 事件：GitHub 创建 webhook 时的连通性检查。
    if event == "ping" {
        return (StatusCode::OK, "pong").into_response();
    }

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "Webhook payload 解析失败");
            return (StatusCode::BAD_REQUEST, "invalid payload").into_response();
        }
    };
    let action = payload["action"].as_str().unwrap_or_default().to_string();

    if let Some(issue_number) = extract_issue_number(&event, &action, &payload) {
        tracing::info!(issue = issue_number, event = %event, action = %action, "Webhook 事件驱动 issue 处理");
        let mut loop_ = state.discovery_loop.lock().await;
        return match loop_.process_issue_event(issue_number).await {
            Ok(true) => (StatusCode::OK, "processed").into_response(),
            Ok(false) => (StatusCode::OK, "issue not found").into_response(),
            Err(e) => {
                tracing::warn!(issue = issue_number, error = %e, "Webhook 事件处理失败");
                (StatusCode::INTERNAL_SERVER_ERROR, "processing failed").into_response()
            }
        };
    }

    if let Some(ci_event) = extract_ci_failure(&event, &action, &payload) {
        let run_id = ci_event.run_id;
        tracing::info!(run_id, workflow = %ci_event.workflow_name, "Webhook 事件驱动 CI 失败处理");
        let mut loop_ = state.discovery_loop.lock().await;
        return match loop_.process_ci_failure(ci_event).await {
            Ok(true) => (StatusCode::OK, "processed").into_response(),
            Ok(false) => (StatusCode::OK, "duplicate or unsubmittable").into_response(),
            Err(e) => {
                tracing::warn!(run_id, error = %e, "CI 失败事件处理失败");
                (StatusCode::INTERNAL_SERVER_ERROR, "processing failed").into_response()
            }
        };
    }

    (StatusCode::OK, "ignored").into_response()
}

/// 构建 webhook 路由（POST {path}）。
pub fn webhook_router(state: WebhookState, path: &str) -> Router {
    Router::new()
        .route(path, post(webhook_handler))
        .with_state(state)
}

/// 启动 webhook 监听服务（阻塞至服务退出）。
pub async fn run_webhook_server(
    state: WebhookState,
    port: u16,
    path: String,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(addr = %addr, path = %path, "GitHub webhook 事件入口启动");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, webhook_router(state, &path))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
}

/// 从环境变量解析 webhook secret；缺失时返回 None（调用方应拒绝启动）。
pub fn resolve_secret(secret_env: &str) -> Option<String> {
    std::env::var(secret_env).ok().filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn signature_verification() {
        let body = br#"{"action":"opened"}"#;
        let sig = sign("s3cret", body);
        assert!(verify_signature("s3cret", body, &sig));
        assert!(!verify_signature("wrong", body, &sig));
        assert!(!verify_signature("s3cret", b"tampered", &sig));
        assert!(!verify_signature("s3cret", body, "sha256=zzzz"));
        assert!(!verify_signature("s3cret", body, "no-prefix"));
    }

    #[test]
    fn issue_number_extraction() {
        let payload = serde_json::json!({"action": "opened", "issue": {"number": 42}});
        assert_eq!(extract_issue_number("issues", "opened", &payload), Some(42));
        assert_eq!(extract_issue_number("issues", "closed", &payload), None);
        let comment = serde_json::json!({"action": "created", "issue": {"number": 7}});
        assert_eq!(
            extract_issue_number("issue_comment", "created", &comment),
            Some(7)
        );
        assert_eq!(extract_issue_number("push", "created", &comment), None);
    }

    #[test]
    fn ci_failure_extraction() {
        let payload = serde_json::json!({
            "action": "completed",
            "workflow_run": {
                "id": 30201171944u64,
                "name": "CI",
                "conclusion": "failure",
                "head_sha": "935f667",
                "head_branch": "main",
                "html_url": "https://github.com/o/r/actions/runs/30201171944"
            }
        });
        let ev = extract_ci_failure("workflow_run", "completed", &payload).unwrap();
        assert_eq!(ev.run_id, 30201171944);
        assert_eq!(ev.workflow_name, "CI");
        assert_eq!(ev.head_branch, "main");

        // 非 failure 结论不触发。
        let mut success = payload.clone();
        success["workflow_run"]["conclusion"] = serde_json::json!("success");
        assert_eq!(
            extract_ci_failure("workflow_run", "completed", &success),
            None
        );

        // 其他事件类型 / action 不触发。
        assert_eq!(extract_ci_failure("check_run", "completed", &payload), None);
        assert_eq!(
            extract_ci_failure("workflow_run", "requested", &payload),
            None
        );

        // 缺少 run id 时安全返回 None。
        let no_id = serde_json::json!({
            "action": "completed",
            "workflow_run": {"conclusion": "failure"}
        });
        assert_eq!(
            extract_ci_failure("workflow_run", "completed", &no_id),
            None
        );
    }
}
