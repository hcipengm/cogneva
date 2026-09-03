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

/// 从 Gitee 事件提取需要处理的 issue id（trait u64 承载数值 id）。
/// X-Gitee-Event 形如 "Issue Hook"/"Note Hook"，规范化后匹配。
pub fn extract_gitee_issue_number(
    event: &str,
    action: &str,
    payload: &serde_json::Value,
) -> Option<u64> {
    let ev = event.trim_end_matches(" Hook").to_ascii_lowercase();
    match (ev.as_str(), action) {
        ("issue", "open" | "update" | "reopen") => payload["issue"]["id"].as_u64(),
        // 评论（Note Hook）是澄清对话的回复信号，重跑该 issue 的管线。
        ("note", "comment") if payload["noteable_type"].as_str() == Some("Issue") => {
            payload["issue"]["id"].as_u64()
        }
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

/// 事件平台：决定 issue 号提取方式与是否有 CI 信号。
#[derive(Clone, Copy)]
enum PlatformKind {
    Github,
    Gitee,
}

/// 验签通过后的事件分发（legacy 直连模式与网关验签模式共用）。
async fn dispatch_platform_event(
    discovery_loop: &Arc<tokio::sync::Mutex<GitHubDiscoveryLoop>>,
    platform: PlatformKind,
    event: &str,
    body: &[u8],
) -> axum::response::Response {
    // ping 事件：GitHub 创建 webhook 时的连通性检查。
    if matches!(platform, PlatformKind::Github) && event == "ping" {
        return (StatusCode::OK, "pong").into_response();
    }

    let payload: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "Webhook payload 解析失败");
            return (StatusCode::BAD_REQUEST, "invalid payload").into_response();
        }
    };
    let action = payload["action"].as_str().unwrap_or_default().to_string();

    let issue_number = match platform {
        PlatformKind::Github => extract_issue_number(event, &action, &payload),
        PlatformKind::Gitee => extract_gitee_issue_number(event, &action, &payload),
    };
    if let Some(issue_number) = issue_number {
        let mut loop_ = discovery_loop.lock().await;
        // 机器人自己发评论也会触发 webhook；忽略这种自发事件，避免 bot
        // 应答自己导致重复追问。
        if loop_.is_self_comment_event(
            matches!(platform, PlatformKind::Gitee),
            event,
            &action,
            &payload,
        ) {
            tracing::info!(issue = issue_number, "忽略机器人自发评论事件");
            return (StatusCode::OK, "self event ignored").into_response();
        }
        tracing::info!(issue = issue_number, event = %event, action = %action, "Webhook 事件驱动 issue 处理");
        return match loop_.process_issue_event(issue_number).await {
            Ok(true) => (StatusCode::OK, "processed").into_response(),
            Ok(false) => (StatusCode::OK, "issue not found").into_response(),
            Err(e) => {
                tracing::warn!(issue = issue_number, error = %e, "Webhook 事件处理失败");
                (StatusCode::INTERNAL_SERVER_ERROR, "processing failed").into_response()
            }
        };
    }

    if matches!(platform, PlatformKind::Github) {
        if let Some(ci_event) = extract_ci_failure(event, &action, &payload) {
            let run_id = ci_event.run_id;
            tracing::info!(run_id, workflow = %ci_event.workflow_name, "Webhook 事件驱动 CI 失败处理");
            let mut loop_ = discovery_loop.lock().await;
            return match loop_.process_ci_failure(ci_event).await {
                Ok(true) => (StatusCode::OK, "processed").into_response(),
                Ok(false) => (StatusCode::OK, "duplicate or unsubmittable").into_response(),
                Err(e) => {
                    tracing::warn!(run_id, error = %e, "CI 失败事件处理失败");
                    (StatusCode::INTERNAL_SERVER_ERROR, "processing failed").into_response()
                }
            };
        }
    }

    (StatusCode::OK, "ignored").into_response()
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

    dispatch_platform_event(&state.discovery_loop, PlatformKind::Github, &event, &body).await
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

/// Gitee 事件入口固定路径（与网关转发路径约定一致）。
pub const GITEE_WEBHOOK_PATH: &str = "/webhooks/gitee";

/// 内部转发签名头：网关验平台签名后用它转发，主应用只认这个头。
pub const INTERNAL_SIGNATURE_HEADER: &str = "x-cogneva-signature-256";

/// 网关验签模式的共享状态：主应用只验内部 HMAC，平台 secret 不出网关。
#[derive(Clone)]
pub struct VerifiedWebhookState {
    /// GitHub 侧 discovery loop（github_integration 启用时存在）。
    pub github_loop: Option<Arc<tokio::sync::Mutex<GitHubDiscoveryLoop>>>,
    /// Gitee 侧 discovery loop（gitee_integration 启用时存在）。
    pub gitee_loop: Option<Arc<tokio::sync::Mutex<GitHubDiscoveryLoop>>>,
    /// 网关→主应用内部转发 HMAC secret（仅主进程内存持有）。
    pub internal_secret: Arc<str>,
}

async fn verified_github_handler(
    State(state): State<VerifiedWebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let event = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let signature = headers
        .get(INTERNAL_SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !verify_signature(&state.internal_secret, &body, signature) {
        tracing::warn!(event = %event, "GitHub 事件内部签名验证失败，已拒绝");
        return (StatusCode::UNAUTHORIZED, "invalid internal signature").into_response();
    }
    let Some(loop_) = &state.github_loop else {
        return (StatusCode::NOT_FOUND, "github integration disabled").into_response();
    };
    dispatch_platform_event(loop_, PlatformKind::Github, &event, &body).await
}

async fn verified_gitee_handler(
    State(state): State<VerifiedWebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let event = headers
        .get("x-gitee-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let signature = headers
        .get(INTERNAL_SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !verify_signature(&state.internal_secret, &body, signature) {
        tracing::warn!(event = %event, "Gitee 事件内部签名验证失败，已拒绝");
        return (StatusCode::UNAUTHORIZED, "invalid internal signature").into_response();
    }
    let Some(loop_) = &state.gitee_loop else {
        return (StatusCode::NOT_FOUND, "gitee integration disabled").into_response();
    };
    dispatch_platform_event(loop_, PlatformKind::Gitee, &event, &body).await
}

/// 构建网关验签模式的 webhook 路由（GitHub + Gitee 两路）。
pub fn verified_webhook_router(
    state: VerifiedWebhookState,
    github_path: &str,
    gitee_path: &str,
) -> Router {
    Router::new()
        .route(github_path, post(verified_github_handler))
        .route(gitee_path, post(verified_gitee_handler))
        .with_state(state)
}

/// 启动网关验签模式的 webhook 监听服务（阻塞至服务退出）。
pub async fn run_verified_webhook_server(
    state: VerifiedWebhookState,
    port: u16,
    github_path: String,
    gitee_path: String,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(addr = %addr, github = %github_path, gitee = %gitee_path, "网关验签 webhook 事件入口启动");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        verified_webhook_router(state, &github_path, &gitee_path),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown.changed().await;
    })
    .await
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
    fn gitee_issue_number_extraction() {
        let issue =
            serde_json::json!({"action": "open", "issue": {"id": 123456, "number": "I1A2B3"}});
        assert_eq!(
            extract_gitee_issue_number("Issue Hook", "open", &issue),
            Some(123456)
        );
        assert_eq!(
            extract_gitee_issue_number("Issue Hook", "update", &issue),
            Some(123456)
        );
        assert_eq!(
            extract_gitee_issue_number("Issue Hook", "close", &issue),
            None
        );
        // 评论（Note Hook）仅当挂在 Issue 上时触发。
        let note = serde_json::json!({
            "action": "comment",
            "noteable_type": "Issue",
            "issue": {"id": 77}
        });
        assert_eq!(
            extract_gitee_issue_number("Note Hook", "comment", &note),
            Some(77)
        );
        let note_on_pr = serde_json::json!({
            "action": "comment",
            "noteable_type": "PullRequest",
            "issue": {"id": 77}
        });
        assert_eq!(
            extract_gitee_issue_number("Note Hook", "comment", &note_on_pr),
            None
        );
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
