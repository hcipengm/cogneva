//! Admin API for the contribution channel (wizard step 2).
//!
//! Lets a user connect Cogneva to a public code platform (GitHub by default,
//! Gitee for users behind the GFW) without hand-editing tokens. On GitHub the
//! default path is the OAuth device-authorization flow: the wizard opens the
//! verification page with the user code pre-filled, the gateway polls for the
//! token, then stores it. Gitee has no device flow; its path is the OAuth
//! authorization-code flow: the wizard opens the authorize page, Gitee
//! redirects back to the gateway callback (or the user pastes the redirect
//! URL), the gateway exchanges the code for a token pair and stores it.
//! Gitee access tokens expire in 24h, so a background refresher rotates the
//! pair via the refresh token well before expiry. A manually pasted PAT
//! remains as the last-resort fallback on both platforms.
//!
//! Credentials follow the same rule as the LLM pool: the gateway is the only
//! holder. A connected token is written to the `cogneva-secrets` Secret
//! (`github-token` / `gitee-token`) which the gateway injects on egress for
//! both the platform API and git-over-proxy; the main app and sandbox stay
//! credential-free. An Ed25519 SSH keypair is generated for optional SSH
//! transport — the public key is uploaded to the account, the private key is
//! stored in the same Secret (`git-ssh-private-key`).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Json,
};
use base64::Engine;
use rand_core::RngCore;
use serde::Deserialize;
use serde_json::json;

use crate::llm_admin::KubeClient;

const SECRET_GITHUB_TOKEN: &str = "github-token";
const SECRET_GITEE_TOKEN: &str = "gitee-token";
const SECRET_GITEE_REFRESH: &str = "gitee-refresh-token";
const SECRET_SSH_KEY: &str = "git-ssh-private-key";
const SECRET_CONTRIB_CONFIG: &str = "contribution-config";
const SECRET_GITHUB_APP_ID: &str = "github-app-id";
const SECRET_GITHUB_APP_INSTALLATION_ID: &str = "github-app-installation-id";
const SECRET_GITHUB_APP_KEY: &str = "github-app-private-key";

const GITHUB_DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
const GITHUB_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
pub(crate) const GITHUB_API: &str = "https://api.github.com";
pub(crate) const GITEE_API: &str = "https://gitee.com/api/v5";
const GITEE_AUTHORIZE_URL: &str = "https://gitee.com/oauth/authorize";
const GITEE_TOKEN_URL: &str = "https://gitee.com/oauth/token";

/// OAuth scopes for the device-flow token: open PRs (`repo`) and upload the
/// SSH public key (`write:public_key`).
const GITHUB_DEVICE_SCOPE: &str = "repo write:public_key read:public_key";

/// Gitee OAuth states are single-use and expire quickly: the user is mid-flow
/// in another tab, anything older than this is abandoned.
const OAUTH_STATE_TTL: Duration = Duration::from_secs(15 * 60);
/// Gitee access tokens expire in 24h; refresh when less than this remains so
/// a failed refresh still has hours of runway to retry on the next tick.
const GITEE_REFRESH_THRESHOLD_SECS: u64 = 4 * 3600;
const GITEE_REFRESH_INTERVAL_SECS: u64 = 3600;

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

pub(crate) fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .user_agent("cogneva-contribution")
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

pub(crate) fn oauth_client_id(explicit: Option<&str>) -> Option<String> {
    explicit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("COGNEVA_GITHUB_OAUTH_CLIENT_ID").ok())
        .filter(|s| !s.trim().is_empty())
}

/// 安全网关内部端点基址（同命名空间 Service）。Gitee OAuth 应用凭证与授权码
/// 兑换都发生在网关进程内，主应用只经此地址借用，本进程零持有 client_secret。
pub(crate) fn oauth_gateway_base() -> String {
    std::env::var("COGNEVA_OAUTH_GATEWAY_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://cogneva-security-gateway:8081".to_string())
}

/// 向网关借 Gitee OAuth App 的 client_id（公开标识）。未配置或网关不可达返回
/// None，调用方据此走"未配置 OAuth App"的引导。判据只有网关这一个来源，
/// 主应用不另立近似判据。
pub(crate) async fn gitee_oauth_client_id() -> Option<String> {
    let url = format!("{}/v1/oauth/gitee/app", oauth_gateway_base());
    let body: serde_json::Value = http_client()
        .get(url)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    body.get("client_id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

pub(crate) async fn gitee_oauth_available() -> bool {
    gitee_oauth_client_id().await.is_some()
}

/// 调用网关的 OAuth 端点：成功返回 token 端点响应体，失败返回可展示的错误。
async fn oauth_gateway_call(
    path: &str,
    payload: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = format!("{}{path}", oauth_gateway_base());
    let resp = http_client()
        .post(url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("无法连接安全网关（{e}）"))?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        let msg = body
            .get("error_description")
            .or_else(|| body.get("message"))
            .and_then(|v| v.as_str())
            .unwrap_or("安全网关拒绝了授权请求");
        return Err(msg.to_string());
    }
    Ok(body)
}

/// Fixed callback override for the Gitee OAuth App. When unset the wizard
/// passes its own origin so the redirect lands back on this gateway.
pub(crate) fn gitee_oauth_redirect_override() -> Option<String> {
    std::env::var("COGNEVA_GITEE_OAUTH_REDIRECT_URI")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Decode a base64 entry from a fetched Secret's `data` map.
fn decode_secret_key(secret: &serde_json::Value, key: &str) -> Option<String> {
    let b64 = secret.get("data")?.get(key)?.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    String::from_utf8(bytes).ok()
}

/// Fetch and parse the `contribution-config` JSON document from the cluster
/// Secret. Missing / unparsable yields an empty object (policy defaults to
/// `auto` downstream).
pub(crate) async fn read_contrib_config(kube: &KubeClient) -> serde_json::Value {
    let secret = match kube
        .get_json(&format!(
            "/api/v1/namespaces/{}/secrets/cogneva-secrets",
            kube.namespace()
        ))
        .await
    {
        Ok(s) => s,
        Err(_) => return json!({}),
    };
    decode_secret_key(&secret, SECRET_CONTRIB_CONFIG)
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_else(|| json!({}))
}

/// The owner policy stored in the config document; absent → `Auto` (default
/// 自动回流).
pub(crate) fn policy_from_config(config: &serde_json::Value) -> cog_core::ContributionPolicy {
    config
        .get("policy")
        .and_then(|v| v.as_str())
        .and_then(cog_core::ContributionPolicy::parse)
        .unwrap_or_default()
}

/// In-flight OAuth authorization states, keyed by the opaque state string.
/// The value is the redirect_uri used at start — Gitee requires the same
/// redirect_uri again when exchanging the code.
fn oauth_states() -> &'static std::sync::Mutex<HashMap<String, (Instant, String)>> {
    static STATES: OnceLock<std::sync::Mutex<HashMap<String, (Instant, String)>>> = OnceLock::new();
    STATES.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Create and remember a fresh single-use OAuth state for `redirect_uri`.
pub fn new_oauth_state(redirect_uri: &str) -> String {
    let mut bytes = [0u8; 16];
    rand_core::OsRng.fill_bytes(&mut bytes);
    let state = hex::encode(bytes);
    if let Ok(mut map) = oauth_states().lock() {
        map.retain(|_, (created, _)| created.elapsed() < OAUTH_STATE_TTL);
        map.insert(state.clone(), (Instant::now(), redirect_uri.to_string()));
    }
    state
}

/// Consume a state: returns the remembered redirect_uri iff the state exists
/// and is fresh. A state can only be consumed once.
pub(crate) fn take_oauth_state(state: &str) -> Option<String> {
    let mut map = oauth_states().lock().ok()?;
    match map.get(state) {
        Some((created, _)) if created.elapsed() < OAUTH_STATE_TTL => {
            map.remove(state).map(|(_, uri)| uri)
        }
        Some(_) => {
            map.remove(state);
            None
        }
        None => None,
    }
}

/// A Gitee OAuth token pair with the metadata the refresher needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GiteeTokenSet {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
    pub obtained_at: u64,
}

/// Parse a Gitee token-endpoint response. `now` is injected for tests.
pub fn parse_gitee_token(json: &serde_json::Value, now: u64) -> Result<GiteeTokenSet, String> {
    let access_token = json
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            let desc = json
                .get("error_description")
                .or_else(|| json.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or("响应缺少 access_token");
            format!("Gitee 授权失败: {desc}")
        })?;
    Ok(GiteeTokenSet {
        access_token: access_token.to_string(),
        refresh_token: json
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        expires_in: json
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(86400),
        obtained_at: now,
    })
}

/// Build the Gitee authorize URL. Scopes are fixed at OAuth App registration
/// (user_info / projects / pull_requests / keys); Gitee's authorize endpoint
/// does not take a scope parameter.
pub fn build_gitee_authorize_url(client_id: &str, redirect_uri: &str, state: &str) -> String {
    format!(
        "{GITEE_AUTHORIZE_URL}?client_id={client_id}&redirect_uri={}&response_type=code&state={state}",
        urlencoding_encode(redirect_uri),
    )
}

/// Minimal percent-encoding for query parameter values (redirect_uri).
fn urlencoding_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Extract the authorization code from either a bare code or a full redirect
/// URL the user pasted from the browser address bar.
pub fn extract_gitee_code(code: Option<&str>, redirect_url: Option<&str>) -> Option<String> {
    if let Some(c) = code.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(c.to_string());
    }
    let url = redirect_url?.trim().to_string();
    let query = url.split_once('?')?.1;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == "code" && !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Gitee 授权码兑换：client_id/secret 由调用方给出，只有安全网关进程持有。
/// Gitee 的 token 端点要求参数走 query string 的 POST。
pub(crate) async fn exchange_gitee_code_with(
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
) -> Result<GiteeTokenSet, String> {
    let resp = http_client()
        .post(GITEE_TOKEN_URL)
        .query(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("client_secret", client_secret),
        ])
        .send()
        .await
        .map_err(|e| format!("无法连接 Gitee（{e}）"))?;
    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    parse_gitee_token(&body, unix_now())
}

/// Gitee refresh token 换新：凭证由调用方给出，只有安全网关进程持有。
/// refresh token 每次使用都会轮换。
pub(crate) async fn refresh_gitee_token_with(
    client_id: Option<&str>,
    client_secret: Option<&str>,
    refresh_token: &str,
) -> Result<GiteeTokenSet, String> {
    let mut params = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
    ];
    if let (Some(id), Some(secret)) = (client_id, client_secret) {
        params.push(("client_id", id));
        params.push(("client_secret", secret));
    }
    let resp = http_client()
        .post(GITEE_TOKEN_URL)
        .query(&params)
        .send()
        .await
        .map_err(|e| format!("无法连接 Gitee（{e}）"))?;
    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    parse_gitee_token(&body, unix_now())
}

/// 授权码兑换：经安全网关执行，本进程不接触应用凭证。
pub(crate) async fn exchange_gitee_code(
    code: &str,
    redirect_uri: &str,
) -> Result<GiteeTokenSet, String> {
    let body = oauth_gateway_call(
        "/v1/oauth/gitee/exchange",
        json!({"code": code, "redirect_uri": redirect_uri}),
    )
    .await?;
    parse_gitee_token(&body, unix_now())
}

/// Refresh a Gitee token pair through the gateway. The refresh token rotates on
/// every use.
pub async fn refresh_gitee_token(refresh_token: &str) -> Result<GiteeTokenSet, String> {
    let body = oauth_gateway_call(
        "/v1/oauth/gitee/refresh",
        json!({"refresh_token": refresh_token}),
    )
    .await?;
    parse_gitee_token(&body, unix_now())
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
pub(crate) async fn persist_connected(
    kube: &KubeClient,
    provider: &str,
    token: &str,
    account: &str,
    oauth: Option<&GiteeTokenSet>,
) -> Response {
    let keypair = generate_ssh_keypair("cogneva-contribution");
    let ssh_result = upload_public_key(provider, token, &keypair.public_line).await;

    let token_key = if provider == "gitee" {
        SECRET_GITEE_TOKEN
    } else {
        SECRET_GITHUB_TOKEN
    };
    let mut config_json_val = json!({
        "provider": provider,
        "mode": if oauth.is_some() { "oauth" } else { "manual_or_device" },
        "account": account,
    });
    // 重新连接不清空属主的回流策略：从既有配置继承 policy 字段。
    let existing_policy = read_contrib_config(kube)
        .await
        .get("policy")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if let Some(policy) = existing_policy {
        config_json_val["policy"] = json!(policy);
    }
    let mut string_data = json!({
        token_key: token,
        SECRET_SSH_KEY: keypair.private_pem,
    });
    if let Some(set) = oauth {
        config_json_val["obtained_at"] = json!(set.obtained_at);
        config_json_val["expires_in"] = json!(set.expires_in);
        if let Some(refresh) = &set.refresh_token {
            string_data[SECRET_GITEE_REFRESH] = json!(refresh);
        }
    }
    string_data[SECRET_CONTRIB_CONFIG] =
        json!(serde_json::to_string(&config_json_val).unwrap_or_else(|_| "{}".to_string()));

    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({ "stringData": string_data }),
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
/// configured, plus the owner policy and the staged backlog size. Never
/// returns token material.
pub async fn contribution_status_handler(
    State(state): State<Arc<crate::GatewayState>>,
) -> Response {
    let pending_count = match &state.contribution_control {
        Some(control) => control.pending().await.map(|v| v.len()).unwrap_or(0),
        None => 0,
    };
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
            // App 三件套齐全才算配置好；只读布尔位，私钥绝不回传。
            let github_app = has(SECRET_GITHUB_APP_ID)
                && has(SECRET_GITHUB_APP_INSTALLATION_ID)
                && has(SECRET_GITHUB_APP_KEY);
            let provider = if github {
                "github"
            } else if gitee {
                "gitee"
            } else {
                "none"
            };
            let config = decode_secret_key(&secret, SECRET_CONTRIB_CONFIG)
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                .unwrap_or_else(|| json!({}));
            let policy = policy_from_config(&config);
            return (
                StatusCode::OK,
                Json(json!({
                    "configured": github || gitee,
                    "provider": provider,
                    "github": {"configured": github},
                    "gitee": {"configured": gitee},
                    "ssh": {"key_present": ssh},
                    "github_app_configured": github_app,
                    "policy": policy.as_str(),
                    "pending_count": pending_count,
                    "device_flow_available": oauth_client_id(None).is_some(),
                    "gitee_oauth_available": gitee_oauth_available().await,
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
            "github_app_configured": false,
            "policy": state
                .contribution_control
                .as_ref()
                .map(|c| c.policy().as_str())
                .unwrap_or("auto"),
            "pending_count": pending_count,
            "device_flow_available": oauth_client_id(None).is_some(),
            "gitee_oauth_available": gitee_oauth_available().await,
        })),
    )
        .into_response()
}

/// GitHub App 凭证配置请求（运营者通道：手动粘贴或 Manifest 流回填共用）。
#[derive(Deserialize)]
pub struct GitHubAppConfigRequest {
    pub app_id: String,
    pub installation_id: String,
    pub private_key: String,
}

/// POST /api/v1/admin/contribution/github-app — 把 GitHub App 凭证
/// （App ID、installation ID、私钥 PEM）写入网关 Secret 并滚动网关。
/// 落盘前先做"数字 ID + 私钥可签名"校验（fail-closed）；私钥只进安全
/// 网关，状态接口只回布尔位、绝不回读私钥。未配置 App 时网关出口回退
/// 静态 token，属主主流程不依赖 App。
pub async fn github_app_config_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<GitHubAppConfigRequest>,
) -> Response {
    let app_id = req.app_id.trim();
    let installation_id = req.installation_id.trim();
    let private_key = req.private_key.trim();
    if crate::github_app::GitHubAppCreds::parse(app_id, installation_id, private_key).is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_app_credentials",
                "message": "App 凭证校验失败：App ID 与安装 ID 须为数字，私钥须为可签名的 RSA PEM",
            })),
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
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({ "stringData": {
                SECRET_GITHUB_APP_ID: app_id,
                SECRET_GITHUB_APP_INSTALLATION_ID: installation_id,
                SECRET_GITHUB_APP_KEY: private_key,
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
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "status": "github_app_configured",
            "message": "GitHub App 凭证已写入安全网关，滚动重启约一分钟后出口以 App bot 身份发出",
        })),
    )
        .into_response()
}

/// DELETE /api/v1/admin/contribution/github-app — 移除 GitHub App 凭证，
/// 网关出口回退静态 token（属主主流程不依赖 App，移除无功能损失）。
pub async fn github_app_clear_handler(State(_state): State<Arc<crate::GatewayState>>) -> Response {
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
    // Strategic merge patch: null values delete the keys from `data`.
    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({ "data": {
                SECRET_GITHUB_APP_ID: null,
                SECRET_GITHUB_APP_INSTALLATION_ID: null,
                SECRET_GITHUB_APP_KEY: null,
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
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "status": "github_app_cleared",
            "message": "GitHub App 凭证已移除，安全网关出口回退静态令牌",
        })),
    )
        .into_response()
}

/// Manifest 流回填请求：GitHub 创建 App 后回跳落地页带回的一次性 code。
#[derive(Deserialize)]
pub struct GitHubAppManifestRequest {
    pub code: String,
}

/// POST /api/v1/admin/contribution/github-app/manifest — App Manifest 流的
/// 服务端兑换：浏览器在 GitHub 创建 App 后回跳带回一次性 code，网关用 code
/// 调 conversions 接口接回 app_id 与 PEM 私钥并直写 Secret。私钥从 GitHub
/// 到网关不经浏览器/人手；conversions 无需预认证（code 一次性、几分钟有效）。
/// 创建时 App 尚无安装，installation id 待安装后由 discover 接口补入。
pub async fn github_app_manifest_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<GitHubAppManifestRequest>,
) -> Response {
    let code = req.code.trim();
    if code.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing_code", "message": "缺少 GitHub 创建 App 回跳的 code"})),
        )
            .into_response();
    }
    let url = format!("{GITHUB_API}/app-manifests/{code}/conversions");
    let resp = match http_client()
        .post(&url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "cogneva-security-gateway")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "manifest_exchange_failed", "message": format!("请求 GitHub 失败: {e}")})),
            )
                .into_response();
        }
    };
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let body: String = body.chars().take(300).collect();
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "manifest_exchange_failed", "message": format!("GitHub 返回 {status}: {body}")})),
        )
            .into_response();
    }
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "manifest_exchange_failed", "message": format!("解析 GitHub 响应失败: {e}")})),
            )
                .into_response();
        }
    };
    let app_id = v.get("id").and_then(|x| x.as_u64()).map(|n| n.to_string());
    let pem = v.get("pem").and_then(|x| x.as_str()).map(str::to_string);
    let html_url = v
        .get("html_url")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
    let (Some(app_id), Some(pem)) = (app_id, pem) else {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "manifest_exchange_failed", "message": "GitHub 响应缺少 App ID 或私钥"})),
        )
            .into_response();
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
    // 新 App 与旧 installation 不匹配：清掉旧 installation id，安装后再 discover。
    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({
                "stringData": {
                    SECRET_GITHUB_APP_ID: app_id.clone(),
                    SECRET_GITHUB_APP_KEY: pem,
                },
                "data": { SECRET_GITHUB_APP_INSTALLATION_ID: null },
            }),
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
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "app_id": app_id,
            "html_url": html_url,
            "installed": false,
            "message": "GitHub App 已创建，私钥已写入安全网关；请把 App 安装到仓库后点「发现安装」补全安装 ID",
        })),
    )
        .into_response()
}

/// POST /api/v1/admin/contribution/github-app/discover — App 安装后补全
/// installation id：用已存的 App 私钥签 JWT 调 GitHub 列出本 App 的安装，
/// 取首个安装写入 Secret。运营者在 GitHub 完成「安装到仓库」后点一次即可。
pub async fn github_app_discover_handler(
    State(_state): State<Arc<crate::GatewayState>>,
) -> Response {
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
    let secret = match kube
        .get_json(&format!(
            "/api/v1/namespaces/{}/secrets/cogneva-secrets",
            kube.namespace()
        ))
        .await
    {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "secret_read_failed", "message": e})),
            )
                .into_response();
        }
    };
    let app_id_raw = decode_secret_key(&secret, SECRET_GITHUB_APP_ID);
    let pem = decode_secret_key(&secret, SECRET_GITHUB_APP_KEY);
    let (Some(app_id_raw), Some(pem)) = (app_id_raw, pem) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "app_not_created",
                "message": "尚未创建/配置 GitHub App：请先一键注册或手动粘贴 App 凭证",
            })),
        )
            .into_response();
    };
    let Ok(app_id_num) = app_id_raw.trim().parse::<u64>() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "app_id_invalid", "message": "Secret 中的 App ID 不是数字"})),
        )
            .into_response();
    };
    let jwt = match crate::github_app::mint_app_jwt(app_id_num, &pem) {
        Ok(j) => j,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "jwt_failed", "message": e})),
            )
                .into_response();
        }
    };
    let resp = match http_client()
        .get(format!("{GITHUB_API}/app/installations"))
        .bearer_auth(jwt)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "cogneva-security-gateway")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "installations_lookup_failed", "message": format!("请求 GitHub 失败: {e}")})),
            )
                .into_response();
        }
    };
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let body: String = body.chars().take(300).collect();
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "installations_lookup_failed", "message": format!("GitHub 返回 {status}: {body}")})),
        )
            .into_response();
    }
    let installs: Vec<serde_json::Value> = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "installations_lookup_failed", "message": format!("解析响应失败: {e}")})),
            )
                .into_response();
        }
    };
    let Some(first) = installs.first() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "no_installation",
                "message": "GitHub 上还没有任何安装：请先在 App 安装页把它装到仓库（可只选公版仓库），再回来点发现",
            })),
        )
            .into_response();
    };
    let Some(installation_id) = first.get("id").and_then(|x| x.as_u64()) else {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "installations_lookup_failed", "message": "安装记录缺少 id"})),
        )
            .into_response();
    };
    let account = first
        .get("account")
        .and_then(|a| a.get("login"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({ "stringData": {
                SECRET_GITHUB_APP_INSTALLATION_ID: installation_id.to_string(),
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
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "installation_id": installation_id,
            "account": account,
            "installation_count": installs.len(),
            "message": "已发现 GitHub App 安装，安全网关重启后出口以 App bot 身份发出",
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
    persist_connected(&kube, provider, &token, &account, None).await
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
            persist_connected(&kube, "github", &token, &account, None).await
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

#[derive(Debug, Deserialize)]
pub struct GiteeOAuthStartRequest {
    /// The origin the wizard is served from (e.g. `http://localhost:8080`),
    /// used to build the callback URL unless an override is configured.
    pub redirect_origin: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GiteeOAuthExchangeRequest {
    pub state: String,
    /// Bare authorization code…
    pub code: Option<String>,
    /// …or the full redirect URL pasted from the browser address bar.
    pub redirect_url: Option<String>,
}

/// Resolve the redirect_uri for a start request.
fn resolve_redirect_uri(origin: Option<&str>) -> Result<String, String> {
    if let Some(fixed) = gitee_oauth_redirect_override() {
        return Ok(fixed);
    }
    let origin = origin
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or("缺少 redirect_origin，且未配置 COGNEVA_GITEE_OAUTH_REDIRECT_URI")?;
    if !origin.starts_with("http://") && !origin.starts_with("https://") {
        return Err("redirect_origin 必须是 http(s) 地址".to_string());
    }
    Ok(format!(
        "{}/api/v1/admin/contribution/gitee/oauth/callback",
        origin.trim_end_matches('/')
    ))
}

/// POST /api/v1/admin/contribution/gitee/oauth/start — begin the Gitee
/// authorization-code flow. Returns the authorize URL and state.
pub async fn gitee_oauth_start_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<GiteeOAuthStartRequest>,
) -> Response {
    let Some(client_id) = gitee_oauth_client_id().await else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "no_oauth_app",
                "message": "尚未配置 Gitee OAuth App；请先用手动令牌通道连接，或由管理员为安全网关配置应用凭证"
            })),
        )
            .into_response();
    };
    let redirect_uri = match resolve_redirect_uri(req.redirect_origin.as_deref()) {
        Ok(u) => u,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "bad_redirect", "message": message})),
            )
                .into_response();
        }
    };
    let state = new_oauth_state(&redirect_uri);
    (
        StatusCode::OK,
        Json(json!({
            "authorize_url": build_gitee_authorize_url(&client_id, &redirect_uri, &state),
            "state": state,
            "redirect_uri": redirect_uri,
        })),
    )
        .into_response()
}

/// Shared tail of both Gitee OAuth entry points: consume the state, exchange
/// the code, verify the token, and prepare the cluster client for persisting.
type GiteeOAuthMaterial = (KubeClient, GiteeTokenSet, String);

async fn complete_gitee_oauth(state: &str, code: &str) -> Result<GiteeOAuthMaterial, String> {
    let redirect_uri =
        take_oauth_state(state).ok_or("授权链接已过期或已使用，请回到接管台重新发起授权")?;
    let set = exchange_gitee_code(code, &redirect_uri).await?;
    let account = verify_token("gitee", &set.access_token).await?;
    let kube = KubeClient::in_cluster()?;
    Ok((kube, set, account))
}

/// POST /api/v1/admin/contribution/gitee/oauth/exchange — finish the flow
/// from the wizard with a pasted code / redirect URL (admin-authenticated).
pub async fn gitee_oauth_exchange_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<GiteeOAuthExchangeRequest>,
) -> Response {
    let Some(code) = extract_gitee_code(req.code.as_deref(), req.redirect_url.as_deref()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing_code",
                "message": "请粘贴授权码，或授权后浏览器地址栏里的完整网址"})),
        )
            .into_response();
    };
    match complete_gitee_oauth(&req.state, &code).await {
        Ok((kube, set, account)) => {
            persist_connected(&kube, "gitee", &set.access_token, &account, Some(&set)).await
        }
        Err(message) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "oauth_exchange_failed", "message": message})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct GiteeOAuthCallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

pub(crate) fn oauth_result_page(title: &str, detail: &str, ok: bool) -> Html<String> {
    let color = if ok { "#3fb950" } else { "#f85149" };
    Html(format!(
        "<!DOCTYPE html><html lang=\"zh\"><head><meta charset=\"utf-8\"><title>{title}</title>\
         <style>body{{background:#0d1117;color:#e6edf3;font-family:ui-monospace,Menlo,Consolas,monospace;\
         display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}\
         .card{{max-width:420px;padding:28px;border:1px solid #30363d;border-radius:12px;background:#161b22}}\
         h1{{font-size:18px;margin:0 0 10px;color:{color}}}p{{font-size:13px;color:#8b949e;line-height:1.7;margin:0}}</style></head>\
         <body><div class=\"card\"><h1>{title}</h1><p>{detail}</p></div>\
         <script>if (window.opener) {{ setTimeout(() => window.close(), 1500); }}</script></body></html>"
    ))
}

/// GET /api/v1/admin/contribution/gitee/oauth/callback — the redirect target
/// when Gitee can reach this gateway directly (e.g. localhost installs). This
/// route is intentionally public: the browser redirect carries no admin
/// token, and the single-use state parameter is the CSRF proof.
pub async fn gitee_oauth_callback_handler(Query(q): Query<GiteeOAuthCallbackQuery>) -> Response {
    if let Some(err) = q.error {
        let detail = q.error_description.unwrap_or_default();
        return oauth_result_page(
            "授权未完成",
            &format!("Gitee 返回错误：{err} {detail}。请回到接管台重试。"),
            false,
        )
        .into_response();
    }
    let (Some(state), Some(code)) = (q.state, q.code) else {
        return oauth_result_page("授权未完成", "回调缺少 code 或 state 参数。", false)
            .into_response();
    };
    match complete_gitee_oauth(&state, &code).await {
        Ok((kube, set, account)) => {
            let resp =
                persist_connected(&kube, "gitee", &set.access_token, &account, Some(&set)).await;
            if resp.status().is_success() {
                oauth_result_page(
                    "授权成功",
                    &format!("已连接 Gitee 账号 {account}，安全网关正在滚动重启（约一分钟）。本页可关闭，回到接管台即可看到状态更新。"),
                    true,
                )
                .into_response()
            } else {
                oauth_result_page(
                    "保存失败",
                    "令牌换取成功但写入集群 Secret 失败，请回到接管台重试。",
                    false,
                )
                .into_response()
            }
        }
        Err(message) => oauth_result_page("授权未完成", &message, false).into_response(),
    }
}

// ── 回流策略（属主三档）与断开 ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ContributionPolicyRequest {
    /// `auto` | `ask` | `local`。
    pub policy: String,
}

/// POST /api/v1/admin/contribution-policy — switch the owner policy. Takes
/// effect immediately in-process (the platform plugin holds the live gate)
/// and is persisted into the `contribution-config` document so the choice
/// survives restarts. No gateway roll is needed: the token env is unchanged.
pub async fn contribution_policy_set_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(req): Json<ContributionPolicyRequest>,
) -> Response {
    let Some(policy) = cog_core::ContributionPolicy::parse(req.policy.trim()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_policy",
                "message": "policy 必须是 auto（自动回流）/ ask（每次问我）/ local（只在本地）"})),
        )
            .into_response();
    };
    if let Some(control) = &state.contribution_control {
        control.set_policy(policy);
    }
    let persisted = match KubeClient::in_cluster() {
        Ok(kube) => {
            let mut config = read_contrib_config(&kube).await;
            config["policy"] = json!(policy.as_str());
            let raw = serde_json::to_string(&config).unwrap_or_else(|_| "{}".to_string());
            kube.patch(
                &format!(
                    "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                    kube.namespace()
                ),
                json!({ "stringData": { SECRET_CONTRIB_CONFIG: raw } }),
            )
            .await
            .is_ok()
        }
        Err(_) => false,
    };
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "policy": policy.as_str(),
            "persisted": persisted,
        })),
    )
        .into_response()
}

/// GET /api/v1/admin/contribution/pending — staged changes awaiting a
/// publish decision (oldest first). Content bodies are not returned.
pub async fn contribution_pending_handler(
    State(state): State<Arc<crate::GatewayState>>,
) -> Response {
    let Some(control) = &state.contribution_control else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "control_unavailable",
                "message": "平台集成未启用，无法列出暂存变更"})),
        )
            .into_response();
    };
    match control.pending().await {
        Ok(changes) => {
            let items: Vec<serde_json::Value> = changes
                .iter()
                .map(|c| {
                    json!({
                        "change_id": c.change_id,
                        "goal": c.goal,
                        "affected_files": c.affected_files,
                        "issue_number": c.issue_number,
                        "pge_mode": c.pge_mode,
                        "self_review_score": c.self_review_score,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({"ok": true, "pending": items, "count": items.len()})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "pending_read_failed", "message": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct ContributionFlushRequest {
    /// Submit only this staged change; omitted flushes the whole backlog.
    pub change_id: Option<String>,
}

/// POST /api/v1/admin/contribution/flush — the owner's explicit approval:
/// staged changes are submitted as PRs through the live channel, bypassing
/// the policy gate. Errors when the channel is not connected.
pub async fn contribution_flush_handler(
    State(state): State<Arc<crate::GatewayState>>,
    Json(req): Json<ContributionFlushRequest>,
) -> Response {
    let Some(control) = &state.contribution_control else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "control_unavailable",
                "message": "平台集成未启用，无法提交暂存变更"})),
        )
            .into_response();
    };
    match control.flush_pending(req.change_id.as_deref()).await {
        Ok(flushed) => (
            StatusCode::OK,
            Json(json!({"ok": true, "flushed": flushed})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "flush_failed", "message": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/v1/admin/contribution/disconnect — revoke the channel: token
/// material, the SSH deploy key and the config document are removed from the
/// cluster Secret and the security gateway rolls so egress stops injecting
/// credentials. Staged changes on disk are kept; reconnecting picks them up.
pub async fn contribution_disconnect_handler(
    State(_state): State<Arc<crate::GatewayState>>,
) -> Response {
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
    // Strategic merge patch: null values delete the keys from `data`.
    if let Err(e) = kube
        .patch(
            &format!(
                "/api/v1/namespaces/{}/secrets/cogneva-secrets",
                kube.namespace()
            ),
            json!({ "data": {
                SECRET_GITHUB_TOKEN: null,
                SECRET_GITEE_TOKEN: null,
                SECRET_GITEE_REFRESH: null,
                SECRET_SSH_KEY: null,
                SECRET_CONTRIB_CONFIG: null,
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
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "status": "disconnected",
            "message": "贡献通道已断开，安全网关正在滚动重启；已暂存的变更保留在本机，重新连接后可继续提交",
        })),
    )
        .into_response()
}

/// Hourly refresher for Gitee OAuth tokens: the access token expires in 24h,
/// so when less than GITEE_REFRESH_THRESHOLD_SECS remains the loop exchanges
/// the refresh token for a new pair, patches the Secret and rolls the gateway
/// so egress picks the fresh token up. PAT-mode installs have no refresh
/// material and are skipped.
pub fn spawn_gitee_token_refresher(
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !gitee_oauth_available().await {
            return;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(GITEE_REFRESH_INTERVAL_SECS));
        interval.tick().await; // first tick is immediate; skip it
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                _ = interval.tick() => {
                    if let Err(e) = gitee_refresh_tick().await {
                        tracing::warn!(error = %e, "gitee token refresh tick failed");
                    }
                }
            }
        }
    })
}

async fn gitee_refresh_tick() -> Result<(), String> {
    let kube = KubeClient::in_cluster()?;
    let secret = kube
        .get_json(&format!(
            "/api/v1/namespaces/{}/secrets/cogneva-secrets",
            kube.namespace()
        ))
        .await?;
    let decode = |key: &str| -> Option<String> {
        let b64 = secret.get("data")?.get(key)?.as_str()?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
        String::from_utf8(bytes).ok()
    };
    let config_raw = decode(SECRET_CONTRIB_CONFIG).unwrap_or_default();
    let config: serde_json::Value = serde_json::from_str(&config_raw).unwrap_or_else(|_| json!({}));
    if config.get("provider").and_then(|v| v.as_str()) != Some("gitee")
        || config.get("mode").and_then(|v| v.as_str()) != Some("oauth")
    {
        return Ok(());
    }
    let obtained_at = config
        .get("obtained_at")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let expires_in = config
        .get("expires_in")
        .and_then(|v| v.as_u64())
        .unwrap_or(86400);
    let remaining = (obtained_at + expires_in).saturating_sub(unix_now());
    if remaining > GITEE_REFRESH_THRESHOLD_SECS {
        return Ok(());
    }
    let refresh = decode(SECRET_GITEE_REFRESH)
        .ok_or("contribution-config 声明 oauth 模式但缺少 gitee-refresh-token")?;
    let set = refresh_gitee_token(&refresh).await?;
    let account = config
        .get("account")
        .and_then(|v| v.as_str())
        .unwrap_or("gitee-user")
        .to_string();
    let new_config = serde_json::to_string(&json!({
        "provider": "gitee",
        "mode": "oauth",
        "account": account,
        "obtained_at": set.obtained_at,
        "expires_in": set.expires_in,
    }))
    .unwrap_or_else(|_| "{}".to_string());
    let mut string_data = json!({
        SECRET_GITEE_TOKEN: set.access_token,
        SECRET_CONTRIB_CONFIG: new_config,
    });
    if let Some(new_refresh) = &set.refresh_token {
        string_data[SECRET_GITEE_REFRESH] = json!(new_refresh);
    }
    kube.patch(
        &format!(
            "/api/v1/namespaces/{}/secrets/cogneva-secrets",
            kube.namespace()
        ),
        json!({ "stringData": string_data }),
    )
    .await?;
    // The gateway reads the token from env at pod start; roll it so egress
    // picks up the fresh pair. The new pod sees a young token and idles.
    kube.restart_gateway().await
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

    #[test]
    fn gitee_token_parses_full_response() {
        let set = parse_gitee_token(
            &json!({
                "access_token": "at-1",
                "refresh_token": "rt-1",
                "expires_in": 86400,
                "token_type": "bearer",
                "scope": "user_info projects"
            }),
            1000,
        )
        .unwrap();
        assert_eq!(set.access_token, "at-1");
        assert_eq!(set.refresh_token.as_deref(), Some("rt-1"));
        assert_eq!(set.expires_in, 86400);
        assert_eq!(set.obtained_at, 1000);
    }

    #[test]
    fn gitee_token_defaults_and_errors() {
        // expires_in absent → 24h default; refresh_token absent → None.
        let set = parse_gitee_token(&json!({"access_token": "at"}), 5).unwrap();
        assert_eq!(set.expires_in, 86400);
        assert!(set.refresh_token.is_none());
        // Error payloads surface the provider's description.
        let err = parse_gitee_token(
            &json!({"error": "invalid_grant", "error_description": "code expired"}),
            0,
        )
        .unwrap_err();
        assert!(err.contains("code expired"));
        assert!(parse_gitee_token(&json!({}), 0).is_err());
    }

    #[test]
    fn gitee_authorize_url_encodes_redirect() {
        let url = build_gitee_authorize_url(
            "cid",
            "http://localhost:8080/api/v1/admin/contribution/gitee/oauth/callback",
            "st",
        );
        assert!(url.starts_with("https://gitee.com/oauth/authorize?client_id=cid"));
        assert!(url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A8080%2Fapi"));
        assert!(url.contains("response_type=code"));
        assert!(url.ends_with("state=st"));
    }

    #[test]
    fn extract_code_prefers_bare_then_url() {
        assert_eq!(
            extract_gitee_code(Some("abc"), Some("http://x/?code=zzz")),
            Some("abc".to_string())
        );
        assert_eq!(
            extract_gitee_code(None, Some("http://localhost:8080/cb?state=s&code=c123")),
            Some("c123".to_string())
        );
        assert_eq!(
            extract_gitee_code(Some("  "), Some("http://x/?code=q")),
            Some("q".to_string())
        );
        assert_eq!(extract_gitee_code(None, Some("http://x/no-query")), None);
        assert_eq!(extract_gitee_code(None, Some("http://x/?state=s")), None);
        assert_eq!(extract_gitee_code(None, None), None);
    }

    #[test]
    fn oauth_state_is_single_use() {
        let state = new_oauth_state("http://localhost/cb");
        assert_eq!(
            take_oauth_state(&state),
            Some("http://localhost/cb".to_string())
        );
        assert_eq!(take_oauth_state(&state), None);
        assert_eq!(take_oauth_state("never-issued"), None);
    }
}
