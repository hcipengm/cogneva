//! Admin API for the contribution channel (wizard step 2).
//!
//! Lets a user connect Cogneva to a public code platform (GitHub by default,
//! Gitee for users behind the GFW) without hand-editing tokens. On GitHub the
//! default path is the OAuth device-authorization flow: the wizard opens the
//! verification page with the user code pre-filled, the gateway polls for the
//! token, then stores it. Gitee has no device flow, so a manually pasted PAT
//! is the fallback (and the PAT fallback also covers GitHub before an OAuth
//! App is registered).
//!
//! Credentials follow the same rule as the LLM pool: the gateway is the only
//! holder. A connected token is written to the `cogneva-secrets` Secret
//! (`github-token` / `gitee-token`) which the gateway injects on egress for
//! both the platform API and git-over-proxy; the main app and sandbox stay
//! credential-free. An Ed25519 SSH keypair is generated for optional SSH
//! transport — the public key is uploaded to the account, the private key is
//! stored in the same Secret (`git-ssh-private-key`).

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use base64::Engine;
use serde::Deserialize;
use serde_json::json;

use crate::llm_admin::KubeClient;

const SECRET_GITHUB_TOKEN: &str = "github-token";
const SECRET_GITEE_TOKEN: &str = "gitee-token";
const SECRET_SSH_KEY: &str = "git-ssh-private-key";
const SECRET_CONTRIB_CONFIG: &str = "contribution-config";

const GITHUB_DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
const GITHUB_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const GITHUB_API: &str = "https://api.github.com";
const GITEE_API: &str = "https://gitee.com/api/v5";

/// OAuth scopes for the device-flow token: open PRs (`repo`) and upload the
/// SSH public key (`write:public_key`).
const GITHUB_DEVICE_SCOPE: &str = "repo write:public_key read:public_key";

/// A generated Ed25519 keypair in OpenSSH formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshKeyPair {
    /// `-----BEGIN OPENSSH PRIVATE KEY-----` PEM (unencrypted).
    pub private_pem: String,
    /// `ssh-ed25519 AAAA… comment` authorized-keys line (no trailing newline).
    pub public_line: String,
}

fn ssh_string(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
}

/// Serialize an Ed25519 public key (32 bytes) as the OpenSSH authorized-keys
/// line: `ssh-ed25519 <base64(blob)> <comment>`.
pub fn encode_ed25519_public_line(pubkey32: &[u8; 32], comment: &str) -> String {
    let mut blob = Vec::with_capacity(35 + 35);
    ssh_string(&mut blob, b"ssh-ed25519");
    ssh_string(&mut blob, pubkey32);
    let encoded = base64::engine::general_purpose::STANDARD.encode(&blob);
    format!("ssh-ed25519 {encoded} {comment}")
}

/// Serialize an Ed25519 keypair (32-byte seed + 32-byte public) as an
/// unencrypted OpenSSH v1 private-key PEM block. Deterministic given the
/// checkint so output is stable for a fixed seed in tests.
pub fn encode_ed25519_private_pem(seed32: &[u8; 32], pubkey32: &[u8; 32], comment: &str) -> String {
    let mut pub_blob = Vec::new();
    ssh_string(&mut pub_blob, b"ssh-ed25519");
    ssh_string(&mut pub_blob, pubkey32);

    // Plaintext private section: two matching check ints, then the key blob.
    let check: u32 = 0x0c06_0e0a; // arbitrary but fixed; the two copies must match
    let mut plain = Vec::new();
    plain.extend_from_slice(&check.to_be_bytes());
    plain.extend_from_slice(&check.to_be_bytes());
    ssh_string(&mut plain, b"ssh-ed25519");
    ssh_string(&mut plain, pubkey32); // raw public key
    let mut key64 = Vec::with_capacity(64);
    key64.extend_from_slice(seed32);
    key64.extend_from_slice(pubkey32);
    ssh_string(&mut plain, &key64); // private = seed(32) || public(32)
    ssh_string(&mut plain, comment.as_bytes());
    // Block size for cipher "none" is 8; padding is bytes 1,2,3,…
    let mut pad: u8 = 1;
    while plain.len() % 8 != 0 {
        plain.push(pad);
        pad = pad.wrapping_add(1);
    }

    let mut body = Vec::new();
    body.extend_from_slice(b"openssh-key-v1\0");
    ssh_string(&mut body, b"none"); // ciphername
    ssh_string(&mut body, b"none"); // kdfname
    ssh_string(&mut body, b""); // kdf options
    body.extend_from_slice(&1u32.to_be_bytes()); // number of keys
    ssh_string(&mut body, &pub_blob); // public key section
    ssh_string(&mut body, &plain); // (un)encrypted private section

    let b64 = base64::engine::general_purpose::STANDARD.encode(&body);
    let mut pem = String::from("-----BEGIN OPENSSH PRIVATE KEY-----\n");
    for chunk in b64.as_bytes().chunks(70) {
        pem.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        pem.push('\n');
    }
    pem.push_str("-----END OPENSSH PRIVATE KEY-----\n");
    pem
}

/// Generate a fresh Ed25519 keypair and render it in OpenSSH formats.
pub fn generate_ssh_keypair(comment: &str) -> SshKeyPair {
    use ed25519_dalek::SigningKey;
    use rand_core::OsRng;
    let signing = SigningKey::generate(&mut OsRng);
    let seed = signing.to_bytes();
    let pubkey = signing.verifying_key().to_bytes();
    SshKeyPair {
        private_pem: encode_ed25519_private_pem(&seed, &pubkey, comment),
        public_line: encode_ed25519_public_line(&pubkey, comment),
    }
}

/// Outcome of one device-flow token poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DevicePoll {
    /// User has not yet authorized; keep polling after `interval` seconds.
    Pending(u64),
    /// GitHub asked to slow down; poll again after `interval` seconds.
    SlowDown(u64),
    /// Authorized; access token returned.
    Token(String),
    /// The device code expired or was denied — the flow must restart.
    Failed(String),
}

/// Parse a GitHub device-flow token endpoint response.
pub fn parse_device_token(json: &serde_json::Value) -> DevicePoll {
    if let Some(token) = json.get("access_token").and_then(|v| v.as_str()) {
        if !token.is_empty() {
            return DevicePoll::Token(token.to_string());
        }
    }
    let interval = json.get("interval").and_then(|v| v.as_u64()).unwrap_or(5);
    match json.get("error").and_then(|v| v.as_str()).unwrap_or("") {
        "authorization_pending" => DevicePoll::Pending(interval),
        "slow_down" => {
            // GitHub adds 5s on slow_down even if `interval` is absent.
            let next = json
                .get("interval")
                .and_then(|v| v.as_u64())
                .unwrap_or(interval + 5);
            DevicePoll::SlowDown(next)
        }
        "expired_token" => DevicePoll::Failed("设备码已过期，请重新连接".into()),
        "access_denied" => DevicePoll::Failed("授权被拒绝".into()),
        other => DevicePoll::Failed(format!("设备流错误: {other}")),
    }
}

/// Fields the frontend needs to render the "open this page / enter this code"
/// step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub expires_in: u64,
    pub interval: u64,
}

/// Parse a GitHub device-code endpoint response.
pub fn parse_device_start(json: &serde_json::Value) -> Result<DeviceStart, String> {
    let get = |k: &str| json.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let device_code = get("device_code").ok_or_else(|| "响应缺少 device_code".to_string())?;
    let user_code = get("user_code").ok_or_else(|| "响应缺少 user_code".to_string())?;
    let verification_uri =
        get("verification_uri").ok_or_else(|| "响应缺少 verification_uri".to_string())?;
    // Pre-fill the code so the user only clicks "Authorize".
    let verification_uri = format!("{verification_uri}?user_code={user_code}");
    Ok(DeviceStart {
        device_code,
        user_code,
        verification_uri,
        expires_in: json
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(900),
        interval: json.get("interval").and_then(|v| v.as_u64()).unwrap_or(5),
    })
}

#[derive(Debug, Deserialize)]
pub struct ContributionConfigRequest {
    /// `github` | `gitee`.
    pub provider: String,
    /// Manual PAT for the manual_token fallback (Gitee, or GitHub pre-OAuth).
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DeviceStartRequest {
    /// Optional OAuth App client id; falls back to the configured env one.
    pub client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DevicePollRequest {
    pub client_id: String,
    pub device_code: String,
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .user_agent("cogneva-contribution")
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

fn oauth_client_id(explicit: Option<&str>) -> Option<String> {
    explicit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("COGNEVA_GITHUB_OAUTH_CLIENT_ID").ok())
        .filter(|s| !s.trim().is_empty())
}

/// Verify a token and return the account login it authenticates as.
async fn verify_token(provider: &str, token: &str) -> Result<String, String> {
    let client = http_client();
    match provider {
        "github" => {
            let resp = client
                .get(format!("{GITHUB_API}/user"))
                .bearer_auth(token)
                .header("Accept", "application/vnd.github+json")
                .send()
                .await
                .map_err(|e| format!("无法连接 GitHub（{e}）；国内网络可改用 Gitee 通道"))?;
            if !resp.status().is_success() {
                return Err(format!(
                    "GitHub 拒绝该令牌（HTTP {}），请检查权限",
                    resp.status()
                ));
            }
            let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
            Ok(body
                .get("login")
                .and_then(|v| v.as_str())
                .unwrap_or("github-user")
                .to_string())
        }
        "gitee" => {
            let resp = client
                .get(format!("{GITEE_API}/user"))
                .query(&[("access_token", token)])
                .send()
                .await
                .map_err(|e| format!("无法连接 Gitee（{e}）"))?;
            if !resp.status().is_success() {
                return Err(format!(
                    "Gitee 拒绝该令牌（HTTP {}），请检查权限",
                    resp.status()
                ));
            }
            let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
            Ok(body
                .get("login")
                .and_then(|v| v.as_str())
                .unwrap_or("gitee-user")
                .to_string())
        }
        other => Err(format!("未知平台: {other}")),
    }
}

/// Best-effort upload of the SSH public key to the account. A failure (e.g. a
/// token lacking the key-write scope) only warns — API and git-over-proxy
/// still work with the token.
async fn upload_public_key(provider: &str, token: &str, public_line: &str) -> Result<(), String> {
    let client = http_client();
    let title = "cogneva-contribution";
    match provider {
        "github" => {
            let resp = client
                .post(format!("{GITHUB_API}/user/keys"))
                .bearer_auth(token)
                .header("Accept", "application/vnd.github+json")
                .json(&json!({"title": title, "key": public_line}))
                .send()
                .await
                .map_err(|e| e.to_string())?;
            // 201 created; 422 = key already present (fine).
            if resp.status().as_u16() == 201 || resp.status().as_u16() == 422 {
                Ok(())
            } else {
                Err(format!("GitHub 上传公钥返回 HTTP {}", resp.status()))
            }
        }
        "gitee" => {
            let resp = client
                .post(format!("{GITEE_API}/user/keys"))
                .query(&[("access_token", token)])
                .json(&json!({"title": title, "key": public_line}))
                .send()
                .await
                .map_err(|e| e.to_string())?;
            if resp.status().is_success() {
                Ok(())
            } else {
                Err(format!("Gitee 上传公钥返回 HTTP {}", resp.status()))
            }
        }
        other => Err(format!("未知平台: {other}")),
    }
}

/// Persist a connected token (plus an Ed25519 SSH keypair) to the gateway
/// Secret and roll the gateway so it picks the new credentials up.
async fn persist_connected(
    kube: &KubeClient,
    provider: &str,
    token: &str,
    account: &str,
) -> Response {
    let keypair = generate_ssh_keypair("cogneva-contribution");
    let ssh_result = upload_public_key(provider, token, &keypair.public_line).await;

    let token_key = if provider == "gitee" {
        SECRET_GITEE_TOKEN
    } else {
        SECRET_GITHUB_TOKEN
    };
    let config_json = serde_json::to_string(&json!({
        "provider": provider,
        "mode": "manual_or_device",
        "account": account,
    }))
    .unwrap_or_else(|_| "{}".to_string());

    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({ "stringData": {
                token_key: token,
                SECRET_SSH_KEY: keypair.private_pem,
                SECRET_CONTRIB_CONFIG: config_json,
            }}),
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
            Json(json!({"error": "gateway_restart_failed", "message": e})),
        )
            .into_response();
    }

    let ssh_note = match ssh_result {
        Ok(()) => "SSH 公钥已上传到账户".to_string(),
        Err(e) => format!("令牌已保存；SSH 公钥未上传（{e}），不影响 API 与代理推送"),
    };
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "status": "connected",
            "provider": provider,
            "account": account,
            "ssh": ssh_note,
            "message": "贡献通道已连接，安全网关正在滚动重启，约一分钟后生效",
        })),
    )
        .into_response()
}

/// GET /api/v1/admin/contribution-status — whether a contribution token is
/// configured. Never returns token material.
pub async fn contribution_status_handler(
    State(_state): State<Arc<crate::GatewayState>>,
) -> Response {
    if let Ok(kube) = KubeClient::in_cluster() {
        if let Ok(secret) = kube
            .get_json(&format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ))
            .await
        {
            let has = |key: &str| {
                secret
                    .get("data")
                    .and_then(|d| d.get(key))
                    .and_then(|v| v.as_str())
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            };
            let github = has(SECRET_GITHUB_TOKEN);
            let gitee = has(SECRET_GITEE_TOKEN);
            let ssh = has(SECRET_SSH_KEY);
            let provider = if github {
                "github"
            } else if gitee {
                "gitee"
            } else {
                "none"
            };
            return (
                StatusCode::OK,
                Json(json!({
                    "configured": github || gitee,
                    "provider": provider,
                    "github": {"configured": github},
                    "gitee": {"configured": gitee},
                    "ssh": {"key_present": ssh},
                    "device_flow_available": oauth_client_id(None).is_some(),
                })),
            )
                .into_response();
        }
    }
    (
        StatusCode::OK,
        Json(json!({
            "configured": false,
            "provider": "none",
            "note": "not_in_cluster",
            "device_flow_available": oauth_client_id(None).is_some(),
        })),
    )
        .into_response()
}

/// POST /api/v1/admin/contribution-config — connect via a manually pasted
/// PAT (Gitee default, or GitHub fallback before an OAuth App is registered).
pub async fn contribution_config_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<ContributionConfigRequest>,
) -> Response {
    let provider = req.provider.trim();
    if provider != "github" && provider != "gitee" {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": "invalid_provider", "message": "provider 必须是 github 或 gitee"}),
            ),
        )
            .into_response();
    }
    let token = match req.token.as_deref().map(str::trim) {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing_token", "message": "请粘贴访问令牌（PAT）"})),
            )
                .into_response();
        }
    };

    let account = match verify_token(provider, &token).await {
        Ok(login) => login,
        Err(message) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "token_verify_failed", "message": message})),
            )
                .into_response();
        }
    };

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
    persist_connected(&kube, provider, &token, &account).await
}

/// POST /api/v1/admin/contribution/device/start — begin the GitHub device
/// flow. Returns the user code and a pre-filled verification URL.
pub async fn device_start_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<DeviceStartRequest>,
) -> Response {
    let Some(client_id) = oauth_client_id(req.client_id.as_deref()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "no_oauth_app",
                "message": "尚未配置 GitHub OAuth App client_id；请先用手动 PAT 通道连接，或由管理员设置 COGNEVA_GITHUB_OAUTH_CLIENT_ID"
            })),
        )
            .into_response();
    };

    let client = http_client();
    let resp = match client
        .post(GITHUB_DEVICE_CODE_URL)
        .header("Accept", "application/json")
        .json(&json!({"client_id": client_id, "scope": GITHUB_DEVICE_SCOPE}))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "device_start_failed",
                    "message": format!("无法连接 GitHub（{e}）；国内网络可改用 Gitee 通道")})),
            )
                .into_response();
        }
    };
    let body: serde_json::Value = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "device_start_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };
    match parse_device_start(&body) {
        Ok(start) => (
            StatusCode::OK,
            Json(json!({
                "client_id": client_id,
                "device_code": start.device_code,
                "user_code": start.user_code,
                "verification_uri": start.verification_uri,
                "expires_in": start.expires_in,
                "interval": start.interval,
            })),
        )
            .into_response(),
        Err(message) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "device_start_failed", "message": message})),
        )
            .into_response(),
    }
}

/// POST /api/v1/admin/contribution/device/poll — exchange a device code for a
/// token. The frontend polls this on the returned interval until it resolves.
pub async fn device_poll_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<DevicePollRequest>,
) -> Response {
    let client = http_client();
    let resp = match client
        .post(GITHUB_TOKEN_URL)
        .header("Accept", "application/json")
        .json(&json!({
            "client_id": req.client_id,
            "device_code": req.device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        }))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "device_poll_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };
    let body: serde_json::Value = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "device_poll_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };

    match parse_device_token(&body) {
        DevicePoll::Token(token) => {
            let account = match verify_token("github", &token).await {
                Ok(login) => login,
                Err(message) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        Json(json!({"error": "token_verify_failed", "message": message})),
                    )
                        .into_response();
                }
            };
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
            persist_connected(&kube, "github", &token, &account).await
        }
        DevicePoll::Pending(interval) => (
            StatusCode::OK,
            Json(json!({"status": "pending", "interval": interval})),
        )
            .into_response(),
        DevicePoll::SlowDown(interval) => (
            StatusCode::OK,
            Json(json!({"status": "slow_down", "interval": interval})),
        )
            .into_response(),
        DevicePoll::Failed(message) => (
            StatusCode::OK,
            Json(json!({"status": "error", "message": message})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_key_line_has_ed25519_prefix_and_base64_blob() {
        let pubkey = [9u8; 32];
        let line = encode_ed25519_public_line(&pubkey, "cogneva");
        assert!(line.starts_with("ssh-ed25519 "));
        assert!(line.ends_with(" cogneva"));
        // The base64 blob must decode and contain the 32-byte key.
        let b64 = line.split_whitespace().nth(1).unwrap();
        let blob = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert!(blob.windows(32).any(|w| w == pubkey));
    }

    #[test]
    fn private_pem_is_openssh_format_with_matching_pubkey() {
        let seed = [1u8; 32];
        let pubkey = [2u8; 32];
        let pem = encode_ed25519_private_pem(&seed, &pubkey, "cogneva");
        assert!(pem.contains("-----BEGIN OPENSSH PRIVATE KEY-----"));
        assert!(pem.contains("-----END OPENSSH PRIVATE KEY-----"));
        // Decode the base64 body and assert it carries the auth magic and the
        // public key (round-trip integrity without an SSH parser).
        let b64: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
        let body = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();
        assert!(body.starts_with(b"openssh-key-v1\0"));
        assert!(body.windows(32).any(|w| w == pubkey));
        assert!(body.windows(32).any(|w| w == seed));
    }

    #[test]
    fn generated_keypair_pair_is_consistent() {
        let kp = generate_ssh_keypair("cogneva");
        assert!(kp.public_line.starts_with("ssh-ed25519 "));
        assert!(kp.private_pem.contains("BEGIN OPENSSH PRIVATE KEY"));
        // The authorized-keys blob (type + key) must appear verbatim inside
        // the private PEM body's public-key section, proving the two halves
        // are the same keypair.
        let pub_blob = base64::engine::general_purpose::STANDARD
            .decode(kp.public_line.split_whitespace().nth(1).unwrap())
            .unwrap();
        let priv_b64: String = kp
            .private_pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        let priv_body = base64::engine::general_purpose::STANDARD
            .decode(priv_b64)
            .unwrap();
        assert!(priv_body
            .windows(pub_blob.len())
            .any(|w| w == pub_blob.as_slice()));
    }

    #[test]
    fn device_token_pending_then_token() {
        let pending = parse_device_token(&json!({"error": "authorization_pending", "interval": 5}));
        assert_eq!(pending, DevicePoll::Pending(5));
        let slow = parse_device_token(&json!({"error": "slow_down"}));
        assert_eq!(slow, DevicePoll::SlowDown(10));
        let ok = parse_device_token(&json!({"access_token": "gho_abc", "token_type": "bearer"}));
        assert_eq!(ok, DevicePoll::Token("gho_abc".to_string()));
        let expired = parse_device_token(&json!({"error": "expired_token"}));
        assert!(matches!(expired, DevicePoll::Failed(_)));
    }

    #[tokio::test]
    #[ignore = "needs the ssh-keygen binary; run manually"]
    async fn generated_private_pem_is_readable_by_ssh_keygen() {
        let kp = generate_ssh_keypair("cogneva");
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("id_ed25519");
        std::fs::write(&key_path, &kp.private_pem).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perm = std::fs::metadata(&key_path).unwrap().permissions();
            perm.set_mode(0o600);
            std::fs::set_permissions(&key_path, perm).unwrap();
        }
        let out = tokio::process::Command::new("ssh-keygen")
            .arg("-y")
            .arg("-f")
            .arg(&key_path)
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "ssh-keygen rejected the PEM: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let derived = String::from_utf8_lossy(&out.stdout);
        assert!(derived.starts_with("ssh-ed25519 "));
        assert_eq!(
            derived.split_whitespace().nth(1),
            kp.public_line.split_whitespace().nth(1)
        );
    }

    #[test]
    fn device_start_parses_and_prefills_code() {
        let start = parse_device_start(&json!({
            "device_code": "dev123",
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://github.com/login/device",
            "expires_in": 899,
            "interval": 5
        }))
        .unwrap();
        assert_eq!(start.user_code, "ABCD-EFGH");
        assert_eq!(
            start.verification_uri,
            "https://github.com/login/device?user_code=ABCD-EFGH"
        );
        assert_eq!(start.expires_in, 899);
        assert!(parse_device_start(&json!({"user_code": "x"})).is_err());
    }
}
