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
use cog_core::HttpRequest;
use rand_core::RngCore;
use serde::Deserialize;
use serde_json::json;

use crate::llm_admin::KubeClient;

const SECRET_GITHUB_TOKEN: &str = "github-token";
const SECRET_GITEE_TOKEN: &str = "gitee-token";
const SECRET_GITEE_REFRESH: &str = "gitee-refresh-token";
/// 与 `git_identity` 读回的那个键是同一个键：这里写入、那边确认可用，两个
/// 名字写同一个字面量时，改一处不会让另一处红。
const SECRET_SSH_KEY: &str = crate::git_identity::SECRET_SSH_PRIVATE_KEY;
const SECRET_CONTRIB_CONFIG: &str = "contribution-config";
const SECRET_GITHUB_APP_ID: &str = "github-app-id";
const SECRET_GITHUB_APP_INSTALLATION_ID: &str = "github-app-installation-id";
const SECRET_GITHUB_APP_KEY: &str = "github-app-private-key";
/// Project OAuth App client secret (Gitee authorization-code exchange). Only
/// the platform console can issue it, so nothing in the cluster can derive it
/// and the repository must never carry it — the wizard writes this key, the
/// security gateway reads it from its environment.
const SECRET_GITEE_OAUTH_CLIENT_SECRET: &str = "gitee-oauth-client-secret";
/// Project OAuth App client secret for GitHub's authorization-code channel,
/// same class as the Gitee one: the platform console issues it, the security
/// gateway reads it from its environment, the wizard writes it here.
const SECRET_GITHUB_OAUTH_CLIENT_SECRET: &str = "github-oauth-client-secret";

const GITHUB_DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
const GITHUB_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
pub(crate) const GITHUB_API: &str = "https://api.github.com";
pub(crate) const GITEE_API: &str = "https://gitee.com/api/v5";
const GITEE_AUTHORIZE_URL: &str = "https://gitee.com/oauth/authorize";
const GITEE_TOKEN_URL: &str = "https://gitee.com/oauth/token";

/// OAuth scopes for the device-flow token: open PRs (`repo`) and upload the
/// SSH public key (`write:public_key`).
const GITHUB_DEVICE_SCOPE: &str = "repo write:public_key read:public_key";

/// 循环名：Gitee OAuth token 刷新循环，报在 `cogneva_loop_*` 的 `loop` 标签上。
pub const GITEE_TOKEN_REFRESHER_LOOP: &str = "gateway_gitee_token_refresher";

/// Gitee OAuth states are single-use and expire quickly: the user is mid-flow
/// in another tab, anything older than this is abandoned.
const OAUTH_STATE_TTL: Duration = Duration::from_secs(15 * 60);

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

/// 本模块所有出站请求用的客户端，由装配层在进程启动时交进来。
///
/// 契约（[`cog_core::HttpClient`]）在这里，具体实现（reqwest、超时、连接池、
/// 代理）在装配层——网关插件与安全网关进程各自持有那一份，并把它交进来。于是
/// 本模块不认实现，而**本进程的出站客户端只有一份设置**：此前这里自己从
/// `http_client` 段造一个，读的是同一份配置、造出来的却是第二个客户端，改一处
/// 不影响另一处。
///
/// 没装配就是 Err，不回落到一个自己现造的客户端：回落的那个与配置好的那份在
/// 调用方看来一模一样，于是「装配漏了」读起来和「装配好了」一样。调用方本来就
/// 有「连不上」分支，这个错误就报在那里。
pub(crate) fn http_client() -> Result<Arc<dyn cog_core::HttpClient>, String> {
    INJECTED_HTTP
        .get()
        .cloned()
        .ok_or_else(|| "出站客户端尚未装配（本进程还没把 cog_core::HttpClient 交进来）".to_string())
}

/// 本进程交进来的那一份，进程内只有一个。
static INJECTED_HTTP: OnceLock<Arc<dyn cog_core::HttpClient>> = OnceLock::new();

/// 装配层调用一次，交出本进程的出站客户端。
///
/// 第二次调用返回 Err 而不是把前一个静默顶掉：出现了两个装配点、两份客户端时，
/// 「用哪一个」不该由调用顺序决定，而该被说出来。
pub(crate) fn install_http_client(client: Arc<dyn cog_core::HttpClient>) -> Result<(), String> {
    INJECTED_HTTP
        .set(client)
        .map_err(|_| "出站客户端已经装配过".to_string())
}

pub(crate) fn oauth_client_id(explicit: Option<&str>) -> Option<String> {
    explicit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var(crate::security_gateway::GITHUB_OAUTH_CLIENT_ID_ENV).ok())
        .filter(|s| !s.trim().is_empty())
}

/// 安全网关内部端点基址（同命名空间 Service）。
///
/// 这个地址有两个买家：Gitee OAuth 应用凭证与授权码兑换（网关进程内完成，
/// 主应用只借用，本进程零持有 client_secret），以及 Secret 写入的转发。两处
/// 各写一个默认值就是同一个事实的第二份副本——网关端口在 values 里挪一次，
/// 只会挪动其中一条路，另一条继续敲一扇关了的门。
///
/// 没声明就报错，不猜集群内地址：猜错的症状是「向导说网关不可达」，而真正
/// 的原因在清单里，两个面隔着一层，读的人会先去查那个进程。
pub(crate) fn security_gateway_base() -> Result<String, String> {
    std::env::var("COGNEVA_SECURITY_GATEWAY_BASE")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "没有声明安全网关端点（COGNEVA_SECURITY_GATEWAY_BASE）：本进程不猜集群内地址，\
             猜错读起来像「网关挂了」"
                .to_string()
        })
}

/// 向网关借 Gitee OAuth App 的 client_id（公开标识）。
///
/// 三种结果必须分开：`Ok(Some)` 是问到了、`Ok(None)` 是网关答「没配」、
/// `Err` 是**问都没问到**。把后两者合成一个 `None`，就等于在自己的进程里
/// 把「网关暂时连不上」读成「这个部署没有 OAuth App」——调用方据此把授权按钮
/// 置灰、或者劝用户改走粘贴令牌，而配置其实好好的。这一族已经付过一次代价：
/// 续期循环的启动门就是这么把「监听口还没绑好」读成「没配」，静默不启动，
/// 24 小时后令牌过期、egress 开始 401。
///
/// 主应用不持有应用凭证（零带外凭证），所以「有没有配」这个问题只有网关能答；
/// 本函数只负责把它的答复原样带回来，不替它下结论。
pub(crate) async fn gitee_oauth_client_id_from(base: &str) -> Result<Option<String>, String> {
    let url = format!("{}/v1/oauth/gitee/app", base.trim_end_matches('/'));
    let resp = http_client()?
        .execute(HttpRequest::get(url))
        .await
        .map_err(|e| format!("问不到安全网关（{e}）"))?;
    let body: serde_json::Value = resp
        .json()
        .map_err(|e| format!("安全网关的答复读不成 JSON（{e}）"))?;
    Ok(client_id_in_body(&body))
}

/// 网关答复里那一格：有值就是配了，缺键或空串就是它自己说没配。
fn client_id_in_body(body: &serde_json::Value) -> Option<String> {
    body.get("client_id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

pub(crate) async fn gitee_oauth_client_id_result() -> Result<Option<String>, String> {
    let base = security_gateway_base()?;
    gitee_oauth_client_id_from(&base).await
}

/// 调用网关的 OAuth 端点：成功返回 token 端点响应体，失败返回可展示的错误。
async fn oauth_gateway_call(
    path: &str,
    payload: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let url = format!("{}{path}", security_gateway_base()?);
    let request = HttpRequest::post(url)
        .json(&payload)
        .map_err(|e| format!("请求体序列化失败（{e}）"))?;
    let resp = http_client()?
        .execute(request)
        .await
        .map_err(|e| format!("无法连接安全网关（{e}）"))?;
    let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
    if !resp.is_success() {
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
pub(crate) fn decode_secret_key(secret: &serde_json::Value, key: &str) -> Option<String> {
    let b64 = secret.get("data")?.get(key)?.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    String::from_utf8(bytes).ok()
}

/// Fetch and parse the `contribution-config` JSON document from the cluster
/// Secret.
///
/// `None` means the document **could not be read** — apiserver refused, the
/// request failed, or the stored payload no longer parses. `Some` object with
/// no keys means it was read and simply holds nothing yet. The two must not be
/// collapsed: a writer that reads "could not read" as "empty" overwrites every
/// field it did not set itself, and a reader that does so reports a restored
/// policy it never saw.
pub(crate) async fn read_contrib_config() -> Option<serde_json::Value> {
    let kube = KubeClient::in_cluster().ok()?;
    // 这份文档只住内部片，读一片就够；合成视图在这里只会多一次可能失败的请求，
    // 而"读不到"与"读到空的"在这里是两件事（见上面）。
    let secret = kube
        .get_json(&crate::secret_layout::internal_secret_api_path(
            kube.namespace(),
        ))
        .await
        .ok()?;
    match decode_secret_key(&secret, SECRET_CONTRIB_CONFIG) {
        // 还没写过这份文档：那是「空的」，不是「没读到」。
        None => Some(json!({})),
        // 存了但解析不出来：当没读到——猜一份出来写回去就是覆盖。
        Some(raw) => serde_json::from_str(&raw).ok(),
    }
}

/// Merge a new policy into the config document read back from the Secret.
///
/// `None` in, `None` out: without the current document there is nothing to
/// merge into, and the caller must refuse the write rather than persist a
/// document containing only the policy — that would drop the provider, the
/// account and the token's lifetime, which the refresher reads to know when to
/// renew.
pub(crate) fn merge_policy(
    existing: Option<serde_json::Value>,
    policy: cog_core::ContributionPolicy,
) -> Option<serde_json::Value> {
    let mut config = existing?;
    config["policy"] = json!(policy.as_str());
    Some(config)
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

/// A query string (`k=v&k=v`) with both sides escaped by the one encoder above.
///
/// The endpoints that take parameters in the URL are OAuth token ones, whose
/// values are secrets; escaping them at the call site is how a `+` or `/` in a
/// client secret silently truncates the parameter instead of authenticating.
pub(crate) fn encode_query(params: &[(&str, &str)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding_encode(k), urlencoding_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
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
    let url = format!(
        "{GITEE_TOKEN_URL}?{}",
        encode_query(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("client_secret", client_secret),
        ])
    );
    let resp = http_client()?
        .execute(HttpRequest::post(url))
        .await
        .map_err(|e| format!("无法连接 Gitee（{e}）"))?;
    let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
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
    let url = format!("{GITEE_TOKEN_URL}?{}", encode_query(&params));
    let resp = http_client()?
        .execute(HttpRequest::post(url))
        .await
        .map_err(|e| format!("无法连接 Gitee（{e}）"))?;
    let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
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

/// Exchange a GitHub authorization code with credentials supplied by the
/// caller. Only the security gateway holds them; the main application reaches
/// this over `/v1/oauth/github/exchange` instead of reading the secret itself.
pub(crate) async fn exchange_github_code_with(
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
) -> Result<String, String> {
    let request = HttpRequest::post(GITHUB_TOKEN_URL)
        .header("Accept", cog_core::CONTENT_TYPE_JSON)
        .json(&json!({
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }))
        .map_err(|e| format!("请求体序列化失败（{e}）"))?;
    let resp = http_client()?
        .execute(request)
        .await
        .map_err(|e| format!("无法连接 GitHub（{e}）"))?;
    let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        let desc = body
            .get("error_description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return Err(format!("GitHub 授权失败：{err} {desc}"));
    }
    body.get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| "GitHub 令牌响应缺少 access_token".to_string())
}

/// Borrow a GitHub access token through the security gateway, the only process
/// holding the OAuth App client secret.
pub(crate) async fn exchange_github_code(code: &str, redirect_uri: &str) -> Result<String, String> {
    let body = oauth_gateway_call(
        "/v1/oauth/github/exchange",
        json!({"code": code, "redirect_uri": redirect_uri}),
    )
    .await?;
    body.get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| "安全网关响应缺少 access_token".to_string())
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
    let client = http_client()?;
    match provider {
        crate::GITHUB_PLATFORM => {
            let resp = client
                .execute(
                    HttpRequest::get(format!("{GITHUB_API}/user"))
                        .header("Authorization", format!("Bearer {token}"))
                        .header("Accept", "application/vnd.github+json"),
                )
                .await
                .map_err(|e| format!("无法连接 GitHub（{e}）；国内网络可改用 Gitee 通道"))?;
            if !resp.is_success() {
                return Err(format!(
                    "GitHub 拒绝该令牌（HTTP {}），请检查权限",
                    resp.status
                ));
            }
            let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
            Ok(body
                .get("login")
                .and_then(|v| v.as_str())
                .unwrap_or("github-user")
                .to_string())
        }
        crate::GITEE_PLATFORM => {
            let url = format!(
                "{GITEE_API}/user?{}",
                encode_query(&[("access_token", token)])
            );
            let resp = client
                .execute(HttpRequest::get(url))
                .await
                .map_err(|e| format!("无法连接 Gitee（{e}）"))?;
            if !resp.is_success() {
                return Err(format!(
                    "Gitee 拒绝该令牌（HTTP {}），请检查权限",
                    resp.status
                ));
            }
            let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
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
    let client = http_client()?;
    let title = "cogneva-contribution";
    match provider {
        crate::GITHUB_PLATFORM => {
            let request = HttpRequest::post(format!("{GITHUB_API}/user/keys"))
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/vnd.github+json")
                .json(&json!({"title": title, "key": public_line}))
                .map_err(|e| e.to_string())?;
            let resp = client.execute(request).await.map_err(|e| e.to_string())?;
            // 201 created; 422 = key already present (fine).
            if resp.status == 201 || resp.status == 422 {
                Ok(())
            } else {
                Err(format!("GitHub 上传公钥返回 HTTP {}", resp.status))
            }
        }
        crate::GITEE_PLATFORM => {
            let url = format!(
                "{GITEE_API}/user/keys?{}",
                encode_query(&[("access_token", token)])
            );
            let request = HttpRequest::post(url)
                .json(&json!({"title": title, "key": public_line}))
                .map_err(|e| e.to_string())?;
            let resp = client.execute(request).await.map_err(|e| e.to_string())?;
            if resp.is_success() {
                Ok(())
            } else {
                Err(format!("Gitee 上传公钥返回 HTTP {}", resp.status))
            }
        }
        other => Err(format!("未知平台: {other}")),
    }
}

/// 重新连接：把一枚新令牌（连同新生成的 SSH 密钥对）落进网关 Secret 并滚动。
///
/// 整个动作在网关进程里做。它要读既有配置里的 policy（重连不该把属主设的
/// local/ask 退回 auto）、要生成并上传一对 SSH 密钥，最后写的是两片 Secret——
/// 这些都是「凭证持有者」的事，而主应用只是把用户刚交的令牌递过去。
pub(crate) async fn persist_connected(
    provider: &str,
    token: &str,
    account: &str,
    oauth: Option<&GiteeTokenSet>,
) -> Response {
    let body = json!({
        "provider": provider,
        "token": token,
        "account": account,
        "oauth": oauth.map(|set| json!({
            "obtained_at": set.obtained_at,
            "expires_in": set.expires_in,
            "refresh_token": set.refresh_token,
        })),
    });
    match crate::secret_write::forward_internal_raw(
        crate::security_gateway::INTERNAL_CONTRIBUTION_CONNECT_PATH,
        serde_json::to_vec(&body).unwrap_or_default(),
    )
    .await
    {
        Ok((status, value)) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(value),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": crate::SECURITY_GATEWAY_UNREACHABLE, "message": e})),
        )
            .into_response(),
    }
}

/// 网关侧：解析主应用转来的连接请求并执行。
///
/// 令牌与 OAuth 三件套走这条请求体——它们本来就要交给网关落盘，这里只是把
/// 「谁写」与「写什么」放在同一侧，免得主应用为了合并旧策略先去读一遍 Secret。
pub(crate) async fn connect_from_json(body: &[u8]) -> (StatusCode, serde_json::Value) {
    #[derive(serde::Deserialize)]
    struct OAuthPayload {
        obtained_at: u64,
        expires_in: u64,
        #[serde(default)]
        refresh_token: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct ConnectRequest {
        provider: String,
        token: String,
        account: String,
        #[serde(default)]
        oauth: Option<OAuthPayload>,
    }
    let req: ConnectRequest = match serde_json::from_slice(body) {
        Ok(req) => req,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                json!({"error": "invalid_body", "message": e.to_string()}),
            );
        }
    };
    let oauth = req.oauth.map(|o| GiteeTokenSet {
        // access_token 只在 refresh 那条路径上用；连接时它就在请求体里，
        // 而落盘用的是上面的 token 字段。
        access_token: String::new(),
        refresh_token: o.refresh_token,
        expires_in: o.expires_in,
        obtained_at: o.obtained_at,
    });
    connect_contribution(&req.provider, &req.token, &req.account, oauth.as_ref()).await
}

/// 网关侧：解析并落盘一次策略变更。
pub(crate) async fn set_policy_from_json(body: &[u8]) -> (StatusCode, serde_json::Value) {
    #[derive(serde::Deserialize)]
    struct PolicyRequest {
        policy: String,
    }
    let req: PolicyRequest = match serde_json::from_slice(body) {
        Ok(req) => req,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                json!({"error": "invalid_body", "message": e.to_string()}),
            );
        }
    };
    let Some(policy) = cog_core::ContributionPolicy::parse(req.policy.trim()) else {
        return (
            StatusCode::BAD_REQUEST,
            json!({
                "error": "invalid_policy",
                "message": "policy 必须是 auto（自动回流）/ ask（每次问我）/ local（只在本地）",
            }),
        );
    };
    (StatusCode::OK, persist_policy(policy).await)
}

/// 网关侧：连接动作本体。参数是操作者刚交付的令牌与它验证出的账号名。
pub(crate) async fn connect_contribution(
    provider: &str,
    token: &str,
    account: &str,
    oauth: Option<&GiteeTokenSet>,
) -> (StatusCode, serde_json::Value) {
    let keypair = generate_ssh_keypair("cogneva-contribution");
    let ssh_result = upload_public_key(provider, token, &keypair.public_line).await;

    let token_key = if provider == crate::GITEE_PLATFORM {
        SECRET_GITEE_TOKEN
    } else {
        SECRET_GITHUB_TOKEN
    };
    let mut config_json_val = json!({
        "provider": provider,
        "mode": if oauth.is_some() { "oauth" } else { "manual_or_device" },
        "account": account,
    });
    // 重新连接不清空属主的回流策略：从既有配置继承 policy 字段。读不到就只剩
    // 默认值，所以这次降级必须留痕——重连会顺手把属主设的 local/ask 退回 auto。
    let existing_policy = match read_contrib_config().await {
        Some(config) => config
            .get("policy")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        None => {
            tracing::warn!(
                "contribution reconnect: stored config unreadable, the previous policy is not carried over"
            );
            None
        }
    };
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

    if let Err(e) = crate::secret_write::apply(string_data, false, true).await {
        return (
            StatusCode::BAD_GATEWAY,
            json!({"error": "secret_patch_failed", "message": e}),
        );
    }

    let ssh_note = match ssh_result {
        Ok(()) => "SSH 公钥已上传到账户".to_string(),
        Err(e) => format!("令牌已保存；SSH 公钥未上传（{e}），不影响 API 与代理推送"),
    };
    (
        StatusCode::OK,
        json!({
            "ok": true,
            "status": "connected",
            "provider": provider,
            "account": account,
            "ssh": ssh_note,
            "message": "贡献通道已连接，安全网关正在滚动重启，约一分钟后生效",
        }),
    )
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
    // 这一屏的每一个布尔位都来自那两片 Secret，而 Secret 只该由网关读。主应用
    // 保留的是它自己才知道的那几格（待回流条数、本进程的授权码能力），其余由
    // 网关折好送过来。
    let mut reading = match forwarded_contribution_status().await {
        Ok(value) => value,
        Err(e) => unreachable_status_json(&state, &e),
    };
    reading["pending_count"] = json!(pending_count);
    reading["device_flow_available"] = json!(oauth_client_id(None).is_some());
    // 三态：`true` 可用、`false` 网关答「没配」、`null` 是没问出答复。前两个是
    // 平台的实情，第三个是这一刻的读数缺口——把它折叠进 `false`，设置屏就会在
    // 网关滚动的那一分钟里劝人去「补 client_secret」，而那一格本来就填着。
    match gitee_oauth_client_id_result().await {
        Ok(found) => reading["gitee_oauth_available"] = json!(found.is_some()),
        Err(e) => {
            reading["gitee_oauth_available"] = serde_json::Value::Null;
            reading["gitee_oauth_error"] = json!(e);
        }
    }
    (StatusCode::OK, Json(reading)).into_response()
}

/// 网关侧：设置屏要的那份派生读数。
///
/// 只给布尔位与策略名——私钥、token 一个字都不回传。这一屏以前由主应用自己读
/// Secret 拼出来，那要求零凭证的进程对整个对象有 get；把折好的读数搬进网关之后
/// 那条授权才删得掉。
pub(crate) async fn status_block_json() -> serde_json::Value {
    let kube = match KubeClient::in_cluster() {
        Ok(kube) => kube,
        Err(e) => {
            return json!({
                "configured": false,
                "provider": "none",
                "note": "not_in_cluster",
                // 读不到 Secret 时身份状态是**未知**，不是"没配"：在集群外跑的面板
                // 不该据此说"网关没有身份"。
                "git_identity": {"state": crate::git_identity::IdentityState::Unknown.as_str(), "note": "not_in_cluster"},
                "read_error": e,
                "github_app_configured": false,
                // 读不到 Secret 就不知道密钥在不在，报 false 而不是"没配"的措辞。
                "gitee_oauth_secret_configured": false,
                "github_oauth_secret_configured": false,
                "policy": cog_core::ContributionPolicy::default().as_str(),
            });
        }
    };
    let secret = match crate::secret_layout::load_data_view(&kube).await {
        Ok(secret) => secret,
        Err(e) => {
            return json!({
                "configured": false,
                "provider": "none",
                "note": "secret_read_failed",
                "git_identity": {"state": crate::git_identity::IdentityState::Unknown.as_str(), "note": "secret_read_failed"},
                "read_error": e,
                "github_app_configured": false,
                "gitee_oauth_secret_configured": false,
                "github_oauth_secret_configured": false,
                "policy": cog_core::ContributionPolicy::default().as_str(),
            });
        }
    };
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
        crate::GITHUB_PLATFORM
    } else if gitee {
        crate::GITEE_PLATFORM
    } else {
        "none"
    };
    let config = decode_secret_key(&secret, SECRET_CONTRIB_CONFIG)
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .unwrap_or_else(|| json!({}));
    let policy = policy_from_config(&config);
    json!({
        "configured": github || gitee,
        "provider": provider,
        crate::GITHUB_PLATFORM: {"configured": github},
        crate::GITEE_PLATFORM: {"configured": gitee},
        "ssh": {"key_present": ssh},
        // 网关自己的 git 身份：装机时没人给它配密钥，所以它自举；
        // 这一块告诉面板走到了哪一步、缺什么。
        "git_identity": crate::git_identity::status_block(&secret),
        "github_app_configured": github_app,
        "policy": policy.as_str(),
        // 授权码通道要 client_id 与 client_secret 两件都齐；前者是公开标识
        // （随清单走），后者只能由向导投递，所以分开报。
        "gitee_oauth_secret_configured": has(SECRET_GITEE_OAUTH_CLIENT_SECRET),
        "github_oauth_secret_configured": has(SECRET_GITHUB_OAUTH_CLIENT_SECRET),
    })
}

/// 主应用侧：读到那一份派生读数，读不到就把原因写进去。
pub(crate) async fn forwarded_contribution_status() -> Result<serde_json::Value, String> {
    crate::secret_write::forward_internal(
        crate::security_gateway::INTERNAL_CONTRIBUTION_STATUS_PATH,
        Vec::new(),
    )
    .await
}

/// 网关侧：把一次性存取的回流策略并进既有文档落盘。
///
/// 读不到既有文档就不写：这次写入是整文档覆盖，`{}` 打底会把 provider /
/// account / token 时效一起抹掉，刷新循环随后静默失效。
pub(crate) async fn persist_policy(policy: cog_core::ContributionPolicy) -> serde_json::Value {
    let (persisted, persist_error) = match read_contrib_config().await {
        None => (
            false,
            Some("读不到现有贡献配置，未写入以免覆盖其余字段".to_string()),
        ),
        Some(config) => match merge_policy(Some(config), policy) {
            None => (false, Some("既有贡献配置不是对象，未写入".to_string())),
            Some(merged) => {
                let raw = serde_json::to_string(&merged).unwrap_or_else(|_| "{}".to_string());
                match crate::secret_write::apply(
                    json!({ SECRET_CONTRIB_CONFIG: raw }),
                    false,
                    false,
                )
                .await
                {
                    Ok(_) => (true, None),
                    Err(e) => (false, Some(e)),
                }
            }
        },
    };
    json!({
        "policy": policy.as_str(),
        "persisted": persisted,
        "persist_error": persist_error,
    })
}

/// 网关侧：读出存着的回流策略。`None` 表示读不到（不是「没设过」）。
pub(crate) async fn stored_policy() -> Option<cog_core::ContributionPolicy> {
    read_contrib_config()
        .await
        .map(|config| policy_from_config(&config))
}

/// 主应用侧：从网关要存着的回流策略。
///
/// 主应用进程启动时要把策略灌回活档，而这份文档住在 Secret 里。读不到的两种
/// 情形（网关不可达 / 文档读不出）在这里都返回 `None`，调用方按「没恢复」留痕。
pub(crate) async fn forwarded_policy() -> Option<cog_core::ContributionPolicy> {
    let value = crate::secret_write::forward_internal(
        crate::security_gateway::INTERNAL_CONTRIBUTION_POLICY_READ_PATH,
        Vec::new(),
    )
    .await
    .ok()?;
    if value.get("read").and_then(|v| v.as_bool()) != Some(true) {
        return None;
    }
    value
        .get("policy")
        .and_then(|v| v.as_str())
        .and_then(cog_core::ContributionPolicy::parse)
}

/// 主应用问不到网关时的那份读数。
///
/// 它说的是「不知道」，不是「没配」：两者在页面上是两种不同的补救（去接通道 /
/// 去看网关）。策略取本进程活档里的值——那一格不来自 Secret。
fn unreachable_status_json(state: &crate::GatewayState, error: &str) -> serde_json::Value {
    json!({
        "configured": false,
        "provider": "none",
        "note": crate::SECURITY_GATEWAY_UNREACHABLE,
        "read_error": error,
        "git_identity": {"state": crate::git_identity::IdentityState::Unknown.as_str(), "note": crate::SECURITY_GATEWAY_UNREACHABLE},
        "github_app_configured": false,
        "gitee_oauth_secret_configured": false,
        "github_oauth_secret_configured": false,
        "policy": state
            .contribution_control
            .as_ref()
            .map(|c| c.policy().as_str())
            .unwrap_or("auto"),
    })
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
    if let Err(e) = crate::secret_write::forward(
        json!({
            SECRET_GITHUB_APP_ID: app_id,
            SECRET_GITHUB_APP_INSTALLATION_ID: installation_id,
            SECRET_GITHUB_APP_KEY: private_key,
        }),
        false,
        true,
    )
    .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
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
    // Strategic merge patch: null values delete the keys from `data`.
    if let Err(e) = crate::secret_write::forward(
        json!({
            SECRET_GITHUB_APP_ID: null,
            SECRET_GITHUB_APP_INSTALLATION_ID: null,
            SECRET_GITHUB_APP_KEY: null,
        }),
        true,
        true,
    )
    .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
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
    let client = match http_client() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "manifest_exchange_failed", "message": e})),
            )
                .into_response();
        }
    };
    let request = HttpRequest::post(&url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", cog_core::topology::SECURITY_GATEWAY);
    let resp = match client.execute(request).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "manifest_exchange_failed", "message": format!("请求 GitHub 失败: {e}")})),
            )
                .into_response();
        }
    };
    let status = resp.status;
    if !resp.is_success() {
        let body = resp.text().unwrap_or_default();
        let body: String = body.chars().take(300).collect();
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "manifest_exchange_failed", "message": format!("GitHub 返回 {status}: {body}")})),
        )
            .into_response();
    }
    let v: serde_json::Value = match resp.json() {
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
    // 新 App 与旧 installation 不匹配：清掉旧 installation id，安装后再 discover。
    // 三个字段同一次写入：App ID／私钥是这次创建的值，installation id 置空等
    // discover 回填——`null` 在这条路径上表示删键。
    if let Err(e) = crate::secret_write::forward(
        json!({
            SECRET_GITHUB_APP_ID: app_id.clone(),
            SECRET_GITHUB_APP_KEY: pem,
            SECRET_GITHUB_APP_INSTALLATION_ID: null,
        }),
        true,
        true,
    )
    .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
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
    // 这一步要拿 App 私钥签 JWT。私钥只注入网关，所以签名与调用都在网关里做；
    // 主应用只转达结论（安装 ID 与账号名，两者都不是凭证）。
    match crate::secret_write::forward_internal_raw(
        crate::security_gateway::INTERNAL_GITHUB_APP_DISCOVER_PATH,
        Vec::new(),
    )
    .await
    {
        Ok((status, value)) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(value),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": crate::SECURITY_GATEWAY_UNREACHABLE, "message": e})),
        )
            .into_response(),
    }
}

/// 网关侧：用已存的 App 私钥签 JWT、列出安装、把首个安装 ID 补进 Secret。
pub(crate) async fn discover_github_app() -> (StatusCode, serde_json::Value) {
    let kube = match KubeClient::in_cluster() {
        Ok(k) => k,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "not_in_cluster", "message": e}),
            );
        }
    };
    let secret = match crate::secret_layout::load_data_view(&kube).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                json!({"error": "secret_read_failed", "message": e}),
            );
        }
    };
    let app_id_raw = decode_secret_key(&secret, SECRET_GITHUB_APP_ID);
    let pem = decode_secret_key(&secret, SECRET_GITHUB_APP_KEY);
    let (Some(app_id_raw), Some(pem)) = (app_id_raw, pem) else {
        return (
            StatusCode::CONFLICT,
            json!({
                "error": "app_not_created",
                "message": "尚未创建/配置 GitHub App：请先一键注册或手动粘贴 App 凭证",
            }),
        );
    };
    let Ok(app_id_num) = app_id_raw.trim().parse::<u64>() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "app_id_invalid", "message": "Secret 中的 App ID 不是数字"}),
        );
    };
    let jwt = match crate::github_app::mint_app_jwt(app_id_num, &pem) {
        Ok(j) => j,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "jwt_failed", "message": e}),
            );
        }
    };
    let client = match http_client() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "installations_lookup_failed", "message": e}),
            );
        }
    };
    let request = HttpRequest::get(format!("{GITHUB_API}/app/installations"))
        .header("Authorization", format!("Bearer {jwt}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", cog_core::topology::SECURITY_GATEWAY);
    let resp = match client.execute(request).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                json!({"error": "installations_lookup_failed", "message": format!("请求 GitHub 失败: {e}")}),
            );
        }
    };
    let status = resp.status;
    if !resp.is_success() {
        let body = resp.text().unwrap_or_default();
        let body: String = body.chars().take(300).collect();
        return (
            StatusCode::BAD_GATEWAY,
            json!({"error": "installations_lookup_failed", "message": format!("GitHub 返回 {status}: {body}")}),
        );
    }
    let installs: Vec<serde_json::Value> = match resp.json() {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                json!({"error": "installations_lookup_failed", "message": format!("解析响应失败: {e}")}),
            );
        }
    };
    let Some(first) = installs.first() else {
        return (
            StatusCode::NOT_FOUND,
            json!({
                "error": "no_installation",
                "message": "GitHub 上还没有任何安装：请先在 App 安装页把它装到仓库（可只选公版仓库），再回来点发现",
            }),
        );
    };
    let Some(installation_id) = first.get("id").and_then(|x| x.as_u64()) else {
        return (
            StatusCode::BAD_GATEWAY,
            json!({"error": "installations_lookup_failed", "message": "安装记录缺少 id"}),
        );
    };
    let account = first
        .get("account")
        .and_then(|a| a.get("login"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if let Err(e) = crate::secret_write::apply(
        json!({ SECRET_GITHUB_APP_INSTALLATION_ID: installation_id.to_string() }),
        false,
        true,
    )
    .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            json!({"error": "secret_patch_failed", "message": e}),
        );
    }
    (
        StatusCode::OK,
        json!({
            "ok": true,
            "installation_id": installation_id,
            "account": account,
            "installation_count": installs.len(),
            "message": "已发现 GitHub App 安装，安全网关重启后出口以 App bot 身份发出",
        }),
    )
}

/// POST /api/v1/admin/contribution-config — connect via a manually pasted
/// PAT (Gitee default, or GitHub fallback before an OAuth App is registered).
pub async fn contribution_config_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<ContributionConfigRequest>,
) -> Response {
    let provider = req.provider.trim();
    if provider != crate::GITHUB_PLATFORM && provider != crate::GITEE_PLATFORM {
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

    persist_connected(provider, &token, &account, None).await
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

    let client = match http_client() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "device_start_failed", "message": e})),
            )
                .into_response();
        }
    };
    let request = HttpRequest::post(GITHUB_DEVICE_CODE_URL)
        .header("Accept", cog_core::CONTENT_TYPE_JSON)
        .json(&json!({"client_id": client_id, "scope": GITHUB_DEVICE_SCOPE}));
    let request = match request {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "device_start_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };
    let resp = match client.execute(request).await {
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
    let body: serde_json::Value = match resp.json() {
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
    let client = match http_client() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "device_poll_failed", "message": e})),
            )
                .into_response();
        }
    };
    let request = HttpRequest::post(GITHUB_TOKEN_URL)
        .header("Accept", cog_core::CONTENT_TYPE_JSON)
        .json(&json!({
            "client_id": req.client_id,
            "device_code": req.device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        }));
    let request = match request {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "device_poll_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };
    let resp = match client.execute(request).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "device_poll_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };
    let body: serde_json::Value = match resp.json() {
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
            let account = match verify_token(crate::GITHUB_PLATFORM, &token).await {
                Ok(login) => login,
                Err(message) => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        Json(json!({"error": "token_verify_failed", "message": message})),
                    )
                        .into_response();
                }
            };
            persist_connected(crate::GITHUB_PLATFORM, &token, &account, None).await
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
    // 问不到与没配分开报：前者是网关这一刻没答（滚动、监听口没起来），后者是
    // 真的没配。同一句话盖住两种情况，用户会去改一份本来就对的配置。
    let client_id = match gitee_oauth_client_id_result().await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "no_oauth_app",
                    "message": "尚未配置 Gitee OAuth App；请先用手动令牌通道连接，或由管理员为安全网关配置应用凭证"
                })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "error": "gateway_unreachable",
                    "message": format!("读不到安全网关的 Gitee OAuth 配置：{e}。这不是「没配」，请稍后重试")
                })),
            )
                .into_response();
        }
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
/// the code and verify the token.
type GiteeOAuthMaterial = (GiteeTokenSet, String);

async fn complete_gitee_oauth(state: &str, code: &str) -> Result<GiteeOAuthMaterial, String> {
    let redirect_uri =
        take_oauth_state(state).ok_or("授权链接已过期或已使用，请回到接管台重新发起授权")?;
    let set = exchange_gitee_code(code, &redirect_uri).await?;
    let account = verify_token(crate::GITEE_PLATFORM, &set.access_token).await?;
    Ok((set, account))
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
        Ok((set, account)) => {
            persist_connected(
                crate::GITEE_PLATFORM,
                &set.access_token,
                &account,
                Some(&set),
            )
            .await
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
        Ok((set, account)) => {
            let resp = persist_connected(
                crate::GITEE_PLATFORM,
                &set.access_token,
                &account,
                Some(&set),
            )
            .await;
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
    // 活档已经在上一步改过了；落盘这一步交给网关——它读得到既有文档、也持有写权。
    let (persisted, persist_error) = match crate::secret_write::forward_internal(
        crate::security_gateway::INTERNAL_CONTRIBUTION_POLICY_PATH,
        serde_json::to_vec(&json!({ "policy": policy.as_str() })).unwrap_or_default(),
    )
    .await
    {
        Ok(value) => (
            value
                .get("persisted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            value
                .get("persist_error")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        ),
        Err(e) => (false, Some(e)),
    };
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "policy": policy.as_str(),
            "persisted": persisted,
            "persist_error": persist_error,
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
    // Strategic merge patch: null values delete the keys from `data`. 这五个键横跨
    // 两片（token/私钥在带外片、配置文档在内部片），按键表分派后是两笔 patch：
    // 一笔删带外那四个，一笔删内部那一个。
    if let Err(e) = crate::secret_write::forward(
        json!({
            SECRET_GITHUB_TOKEN: null,
            SECRET_GITEE_TOKEN: null,
            SECRET_GITEE_REFRESH: null,
            SECRET_SSH_KEY: null,
            SECRET_CONTRIB_CONFIG: null,
        }),
        true,
        true,
    )
    .await
    {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "secret_patch_failed", "message": e})),
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

/// Maps a wizard-facing provider name to the Secret key that has a consumer.
///
/// Only providers whose key is actually read are listed: storing a key nothing
/// consumes would report success and change nothing. Both platforms qualify
/// now — each has an authorization-code exchange in the security gateway that
/// needs the app secret. GitHub's device flow needs no secret, so a deployment
/// that only uses device flow simply leaves this key empty; the exchange is
/// what fails closed, not the account channel.
/// Returns the canonical provider name together with that key, so the response
/// can echo the name the mapping actually matched instead of a second copy of
/// the literal that could drift from it.
fn oauth_app_secret_key(provider: &str) -> Option<(&'static str, &'static str)> {
    match provider.trim().to_ascii_lowercase().as_str() {
        crate::GITEE_PLATFORM => Some((crate::GITEE_PLATFORM, SECRET_GITEE_OAUTH_CLIENT_SECRET)),
        crate::GITHUB_PLATFORM => Some((crate::GITHUB_PLATFORM, SECRET_GITHUB_OAUTH_CLIENT_SECRET)),
        _ => None,
    }
}

/// Trims a pasted secret and rejects the empty result.
///
/// The consumer treats an empty secret as "not configured" (`gitee_oauth_creds`
/// returns `None`), so storing one would silently close the authorization-code
/// channel while reporting success to the operator who just pasted a value.
fn clean_oauth_app_secret(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[derive(Deserialize)]
pub struct OAuthAppSecretRequest {
    pub provider: String,
    pub client_secret: String,
}

/// POST /api/v1/admin/contribution/oauth-app — store an OAuth App client
/// secret and roll the process that reads it.
///
/// The value can only be issued by the platform console; this endpoint is the
/// *delivery* path, not the source. That matters for the failure modes: the
/// write is reported separately from the roll, because a failed roll means
/// "stored, not yet in effect" (the consumer holds it in an environment
/// variable, which only a new process reads), not "lost".
pub async fn oauth_app_secret_handler(
    State(_state): State<Arc<crate::GatewayState>>,
    Json(req): Json<OAuthAppSecretRequest>,
) -> Response {
    let Some((provider, key)) = oauth_app_secret_key(&req.provider) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "unsupported_provider",
                "message": "只有 GitHub/Gitee 的授权码通道需要客户端密钥",
            })),
        )
            .into_response();
    };
    let Some(secret) = clean_oauth_app_secret(&req.client_secret) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "missing_client_secret",
                "message": "请粘贴 OAuth 应用客户端密钥",
            })),
        )
            .into_response();
    };
    let rolled = match crate::secret_write::forward(json!({ key: secret }), false, true).await {
        Ok(outcome) => outcome,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "secret_patch_failed", "message": e})),
            )
                .into_response();
        }
    };
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "provider": provider,
            "stored": true,
            "restarted": rolled.rolled(),
            "message": if rolled.rolled() {
                "已写入 Secret，安全网关正在滚动重启；重启完成后授权码通道即可用"
            } else {
                "已写入 Secret，但安全网关滚动重启失败；值已保存，待该 Deployment 下次重启生效"
            },
        })),
    )
        .into_response()
}

/// 本进程能不能碰那两个凭证 Secret 对象——**只看能力**。
///
/// 「集群里配了 Gitee OAuth 应用」是每个进程都能问到的集群级事实，不足以回答
/// 这个问题；判据取 apiserver 自己的权限判决。
///
/// 探的是内部那一片：写者同时写两片，而拿到授权的身份两片都能碰，所以探哪片答的
/// 都是同一个问题；取内部那片是因为它的名字不随分片而变。
///
/// 判不出来（建不出 in-cluster 客户端、探测本身报错）按**能碰**处理：那可能是
/// 启动瞬间的抖动，不该因此被静默停掉；后续每次写入仍会照常报告。
pub(crate) async fn is_contribution_secret_owner() -> bool {
    let kube = match KubeClient::in_cluster() {
        Ok(kube) => kube,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "gitee token refresher: no in-cluster client to probe secret access; starting the loop"
            );
            return true;
        }
    };
    match kube
        .can_get(&crate::secret_layout::internal_secret_api_path(
            kube.namespace(),
        ))
        .await
    {
        Ok(allowed) => allowed,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "gitee token refresher: secret access probe failed; starting the loop"
            );
            true
        }
    }
}

/// 本进程是不是那批**必须读写凭证 Secret 的后台循环**的属主。
///
/// 判据由两半合成，顺序是先声明后能力：
///
/// - **声明**（`COGNEVA_CREDENTIAL_LOOP_OWNER`，缺省 false）由部署给出，恰好一个
///   工作负载置真。为什么光有能力判据不够：能力是身份的属性，而**两个**身份都持有
///   那两个对象的今天，两边能力判据都答"能"，同一件事就被两个进程各做一遍——
///   git 身份自举会各生成一对密钥、各登记一次部署密钥，Gitee 续期会各换一次
///   refresh token（而 refresh token 是一次性的，第二次兑换会把第一次换出来的那
///   对作废）。能力面分不出该由谁做，只有部署面的声明能。
/// - **能力**（[`is_contribution_secret_owner`]）确认那个声明真的做得到。声明说真而
///   授权没跟上时，这里答"不是属主"并留下一行日志，而不是让循环每轮对着 403 空转。
///
/// 所以这个判据只会**收窄**：置真但无授权 ⇒ 不是属主；没置真 ⇒ 不是属主。
///
/// 循环搬进网关是逐个进行的，没搬的那些（今天还有 Gitee 续期）仍按能力判定，
/// 所以这里管的是「已搬走的那批」，不是一个永久的名字。
pub(crate) async fn is_credential_loop_owner() -> bool {
    if !declared_credential_loop_owner() {
        return false;
    }
    is_contribution_secret_owner().await
}

/// 部署面给本进程的属主声明。只认显式的真值：读不到、读成空串、读成别的字面量
/// 都按 false——一个"非空即真"的判据会让 `COGNEVA_CREDENTIAL_LOOP_OWNER=false`
/// 这类写法反转成属主，而它读起来正好是"关掉"。
fn declared_credential_loop_owner() -> bool {
    matches!(
        std::env::var("COGNEVA_CREDENTIAL_LOOP_OWNER").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("True")
    )
}

/// Periodic refresher for Gitee OAuth tokens: the access token expires in 24h,
/// so when less than `refresh_threshold_secs` remains the loop exchanges the
/// refresh token for a new pair, patches the Secret and rolls the gateway so
/// egress picks the fresh token up. PAT-mode installs have no refresh material
/// and are skipped.
///
/// Both the interval and the threshold come from the config surface; the
/// caller passes the effective values so the loop and the line that reports it
/// cannot disagree about what is running.
///
/// Spawned from the security gateway itself: the loop reads the refresh token out
/// of the Secret and writes the rotated pair back, so it can only run where the
/// Secret is readable — and only one process may run it, because every refresh
/// rotates a single-use token.
///
/// `metrics` is where the two readings go that say whether rotation is working
/// at all. The loop's own liveness is not it: a loop whose every exchange fails
/// still cycles, so its tick age stays young and its death counter stays at
/// zero all the way to the hour the token expires and egress starts answering
/// 401. The expiry gauge and the threshold that acts on it are the pair that
/// separates "rotating" from "trying and failing".
///
/// `app_configured` is the caller's own config fact, not a question the gate
/// asks over HTTP. The gate used to call the gateway's `/v1/oauth/gitee/app`,
/// and that call fails in exactly the place it has to work: the refresher is
/// spawned while this process is still starting, before it binds its listener,
/// so the only answer available is "unreachable" — which the same `Option`
/// return reports as "not configured". On 2026-10-29 that removed the loop from
/// a deployment whose OAuth app was configured all along: one INFO line saying
/// there was nothing to rotate, and a day later egress began answering 401 with
/// nothing left to rotate the token back.
pub fn spawn_gitee_token_refresher(
    shutdown: cog_core::shutdown::ShutdownSignal,
    interval_secs: u64,
    refresh_threshold_secs: u64,
    metrics: Arc<dyn cog_core::MetricsBackend>,
    app_configured: bool,
) -> tokio::task::JoinHandle<()> {
    // The task below is the deployment gate, not the loop: it ends by starting
    // the loop, so a caller must not read its completion as the loop's fate.
    // What it decides it decides from arguments alone, so the verdict cannot
    // depend on how far along this process' own startup happens to be.
    tokio::spawn(async move {
        // 每一支退出都带原因。只报「起来了」是不够的：拿 OAuth App 配置当门时，
        // App 被撤掉那一支会静默返回，于是「有轮换材料、但不再刷新」与「没东西
        // 可刷」在日志上同形，只有 24h 后 egress 开始 401 才区分得开。
        if !app_configured {
            tracing::info!(
                "gitee token refresher not started: no Gitee OAuth app is configured for the security gateway"
            );
            return;
        }
        // 起循环的进程必须自报。少了这一行，部署里就没有任何一行说得出「刷新
        // 循环正在跑」——它消失时要等 24h 后 token 过期、egress 开始 401 才
        // 知道。周期与阈值一起报出来，读数才有决定面。
        tracing::info!(
            interval_secs,
            refresh_threshold_secs,
            "gitee token refresher started: this process owns the contribution secret"
        );
        // 阈值先落一次：规则拿它当界，而它只在起循环时读得到。写成一条常数的
        // 话，改配置之后规则会继续按旧数等下去——那是静默，不是安全。
        if let Err(e) = metrics
            .record_gauge(
                cog_core::metric_names::GITEE_TOKEN_REFRESH_THRESHOLD_SECONDS,
                refresh_threshold_secs as f64,
                std::collections::HashMap::new(),
            )
            .await
        {
            // 写不出去只让规则少一臂，不影响刷新本身，所以不阻断循环。
            tracing::warn!(error = %e, "gitee token refresh threshold reading could not be written");
        }
        // 登记在这一行之后：属主与轮换材料两道门都在前面，没过的进程不是「刷新
        // 循环死了」，而是这个部署里本来就没有这个循环，上面那两行日志已经把它
        // 为什么没有说清了。登记之后再退出会被记成一次死亡，而那说的不是这里
        // 发生的事。
        drop(cog_core::loop_health::spawn(
            GITEE_TOKEN_REFRESHER_LOOP,
            cog_core::loop_health::Cadence::Periodic(Duration::from_secs(interval_secs)),
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let shutdown = shutdown.clone();
                let metrics = metrics.clone();
                async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
                    interval.tick().await; // first tick is immediate; skip it
                    loop {
                        beat.beat();
                        tokio::select! {
                            biased;
                            _ = shutdown.wait() => return,
                            _ = interval.tick() => {}
                        }
                        if let Err(e) = gitee_refresh_tick(refresh_threshold_secs, &metrics).await {
                            tracing::warn!(error = %e, "gitee token refresh tick failed");
                        }
                    }
                }
            },
        ));
    })
}

async fn gitee_refresh_tick(
    refresh_threshold_secs: u64,
    metrics: &Arc<dyn cog_core::MetricsBackend>,
) -> Result<(), String> {
    let kube = KubeClient::in_cluster()?;
    // 这一次 tick 要读的键横跨两片：配置文档在内部片，refresh token 在带外片。
    let secret = crate::secret_layout::load_data_view(&kube).await?;
    let decode = |key: &str| -> Option<String> {
        let b64 = secret.get("data")?.get(key)?.as_str()?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
        String::from_utf8(bytes).ok()
    };
    let config_raw = decode(SECRET_CONTRIB_CONFIG).unwrap_or_default();
    let config: serde_json::Value = serde_json::from_str(&config_raw).unwrap_or_else(|_| json!({}));
    // 读数在判之前就写：这一拍刷不刷新、刷没刷成，都先留下「此刻还剩多少」。
    // 只在刷新成功时写的话，失败路径上的读数是上一次成功时的旧值——而
    // 「一直没刷新过」正是这条读数要抓的状态。
    let now = unix_now() as i64;
    if let Some(seconds) = expiry_reading(&config, now) {
        publish_expiry(metrics, seconds).await;
    }
    if config.get("provider").and_then(|v| v.as_str()) != Some(crate::GITEE_PLATFORM)
        || config.get("mode").and_then(|v| v.as_str()) != Some("oauth")
    {
        return Ok(());
    }
    // 判定沿用老口径：读不出 `obtained_at` 时按「早已到期」处理，去换一次新的。
    // 那与读数是两件事——读数在那种配置下什么都不写（年龄未知），这里仍然要动作。
    let remaining = expiry_reading(&config, now).unwrap_or(0).max(0) as u64;
    if remaining > refresh_threshold_secs {
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
        "provider": crate::GITEE_PLATFORM,
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
    // The gateway reads the token from env at pod start; roll it so egress
    // picks up the fresh pair. The new pod sees a young token and idles.
    crate::secret_write::apply(string_data, false, true)
        .await
        .map(|_| ())?;
    // 写回去之后立刻改写读数：下一次 tick 要等一个周期，这中间它若继续指着旧
    // token 的剩余，规则看到的就是一次刷新之后还剩四分钟。写在 `apply` 之后，
    // 是因为写失败时新 token 并不在 Secret 里，那时报一个满的剩余是假读数。
    publish_expiry(metrics, set.expires_in as i64).await;
    Ok(())
}

/// 这份贡献配置对应的「令牌还剩多少秒」，没有可量的东西时为 `None`。
///
/// 三种情形返回 `None`，彼此原因不同：
/// - 不是 Gitee OAuth 模式：个人访问令牌没有过期时间，这一格不该有；
/// - 配置里没有 `obtained_at`：令牌的年龄未知，编一个剩余比不报更糟——
///   它会读成一个确切的数，而那个数没有任何来路；
/// - 配置读不出来（空对象）：同上，什么都不知道。
///
/// 返回值可以为负：负数是「已经过期多久」，它比 0 更有用，0 与「一秒都不剩」
/// 同形，而「过期一小时」与「刚好到期」要修的急迫程度不同。
pub fn expiry_reading(config: &serde_json::Value, now_unix: i64) -> Option<i64> {
    if config.get("provider").and_then(|v| v.as_str()) != Some(crate::GITEE_PLATFORM)
        || config.get("mode").and_then(|v| v.as_str()) != Some("oauth")
    {
        return None;
    }
    let obtained_at = config.get("obtained_at").and_then(|v| v.as_i64())?;
    let expires_in = config
        .get("expires_in")
        .and_then(|v| v.as_i64())
        .unwrap_or(86_400);
    Some((obtained_at + expires_in) - now_unix)
}

/// 把剩余寿命写进读数面。写不出去只记一行日志：读数是这一面的眼睛，而刷新本身
/// 是它的手——让一次写指标失败把刷新标成失败，会把「看不见」与「没换成」读成
/// 同一件事。
async fn publish_expiry(metrics: &Arc<dyn cog_core::MetricsBackend>, seconds: i64) {
    if let Err(e) = metrics
        .record_gauge(
            cog_core::metric_names::GITEE_TOKEN_SECONDS_TO_EXPIRY,
            seconds as f64,
            std::collections::HashMap::new(),
        )
        .await
    {
        tracing::warn!(error = %e, "gitee token expiry reading could not be written");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The OAuth token endpoints carry their parameters in the URL, and the
    // values are secrets. A `+` or `/` left unescaped turns "authenticate" into
    // "the parameter was truncated", which reads as a rejected credential and
    // sends the reader to check the wrong thing; an unescaped `&` splits one
    // parameter into two. Both sides get escaped, so the only `&` in the output
    // is the separator.
    #[test]
    fn encode_query_escapes_both_sides_and_keeps_the_separators() {
        let encoded = encode_query(&[
            ("client_secret", "a+b/c=d%e&f g"),
            ("refresh_token", "x?y#z"),
        ]);
        assert_eq!(
            encoded,
            "client_secret=a%2Bb%2Fc%3Dd%25e%26f%20g&refresh_token=x%3Fy%23z"
        );
        assert_eq!(encoded.split('&').count(), 2);
        assert!(!encoded.contains(' '), "a space must not be sent literally");
    }

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
    fn policy_merge_keeps_the_rest_of_the_document_and_refuses_to_invent_one() {
        // 读到既有文档：只动 policy，别的字段原样留着。
        let existing = json!({
            "provider": crate::GITEE_PLATFORM,
            "mode": "oauth",
            "account": "hcipengm",
            "expires_in": 86400,
        });
        let merged = merge_policy(Some(existing), cog_core::ContributionPolicy::Ask).unwrap();
        assert_eq!(merged["policy"], json!("ask"));
        assert_eq!(merged["provider"], json!("gitee"));
        assert_eq!(merged["account"], json!("hcipengm"));
        assert_eq!(merged["expires_in"], json!(86400));
        // 读不到：不产出文档，让调用方拒绝这次写入。产出 `{"policy": …}` 会把
        // 上面这些字段连同刷新循环所依赖的时效信息一起覆盖掉。
        assert!(merge_policy(None, cog_core::ContributionPolicy::Ask).is_none());
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

    #[test]
    fn oauth_app_secret_key_only_covers_providers_with_a_consumer() {
        // Both client secrets have a reader: the security gateway's
        // authorization-code exchange for that platform.
        assert_eq!(
            oauth_app_secret_key("gitee"),
            Some(("gitee", SECRET_GITEE_OAUTH_CLIENT_SECRET))
        );
        assert_eq!(
            oauth_app_secret_key("  Gitee "),
            Some(("gitee", SECRET_GITEE_OAUTH_CLIENT_SECRET))
        );
        assert_eq!(
            oauth_app_secret_key("GitHub"),
            Some(("github", SECRET_GITHUB_OAUTH_CLIENT_SECRET))
        );
        // A platform with no exchange and no key of its own has nowhere for
        // the value to go: accepting it would report success and change
        // nothing.
        assert_eq!(oauth_app_secret_key(""), None);
        assert_eq!(oauth_app_secret_key("bitbucket"), None);
    }

    /// 读数是「还剩多少」，三种没有可量的东西时它是 `None` 而不是一个数。
    ///
    /// 这一格最容易做错的方向是把「不知道」写成 0：0 读作「此刻刚好到期」，
    /// 而一条规则会因此在一个配置根本没有过期概念的部署上点火，或者在一个
    /// 读不出年龄的部署上永远报「已到期」。
    #[test]
    fn the_expiry_reading_is_a_number_only_when_there_is_a_token_to_measure() {
        let now = 1_700_000_000;
        let oauth = json!({
            "provider": crate::GITEE_PLATFORM,
            "mode": "oauth",
            "obtained_at": now - 3600,
            "expires_in": 86400,
        });
        assert_eq!(expiry_reading(&oauth, now), Some(86400 - 3600));

        // 已经过期：负数是「过期多久」，比 0 多一层信息。
        let stale = json!({
            "provider": crate::GITEE_PLATFORM,
            "mode": "oauth",
            "obtained_at": now - 90000,
            "expires_in": 86400,
        });
        assert_eq!(expiry_reading(&stale, now), Some(-3600));

        // 个人访问令牌没有过期时间：这一格不该有。
        let pat = json!({"provider": crate::GITEE_PLATFORM, "mode": "pat"});
        assert_eq!(expiry_reading(&pat, now), None);

        // 别的平台、空配置：同上。
        assert_eq!(
            expiry_reading(
                &json!({"provider": crate::GITHUB_PLATFORM, "mode": "oauth"}),
                now
            ),
            None
        );
        assert_eq!(expiry_reading(&json!({}), now), None);

        // oauth 模式但没有 `obtained_at`：令牌的年龄未知，编一个剩余会读成一个
        // 确切的数，而那个数没有任何来路。
        let ageless = json!({
            "provider": crate::GITEE_PLATFORM,
            "mode": "oauth",
            "expires_in": 86400,
        });
        assert_eq!(expiry_reading(&ageless, now), None);

        // `expires_in` 缺席时按 Gitee 的 24 小时算，与刷新那条路的口径一致。
        let no_ttl = json!({
            "provider": crate::GITEE_PLATFORM,
            "mode": "oauth",
            "obtained_at": now,
        });
        assert_eq!(expiry_reading(&no_ttl, now), Some(86400));
    }

    #[test]
    fn oauth_app_secret_is_trimmed_and_blank_is_rejected() {
        // A secret pasted with surrounding whitespace must land as the value
        // itself, or the exchange fails with an error that points nowhere.
        assert_eq!(
            clean_oauth_app_secret("  s3cr3t\n"),
            Some("s3cr3t".to_string())
        );
        // Blank would be read as "not configured" by the consumer while this
        // endpoint reported success.
        assert_eq!(clean_oauth_app_secret("   "), None);
        assert_eq!(clean_oauth_app_secret(""), None);
    }

    // 续期循环的准入判据必须是调用方手上那份配置，不能是一次自问自答的 HTTP：
    // 循环在监听口绑上之前就起，那一刻「问自己配没配」得到的必然是连不上，而
    // 「连不上」在这一族的返回值里与「没配」同形。2026-10-29 就是这样丢掉整个
    // 循环的——App 早就配好，它留一行「没配」退出，一天后 token 过期、egress
    // 开始 401，而部署里再没有谁换得动那枚令牌。
    //
    // 两半放在同一个测试里，是因为登记表是本进程的全局表：分开写，第二个测试
    // 会在第一个已登记之后断言「没有」，结果取决于执行顺序。
    #[tokio::test]
    async fn the_refresher_start_gate_reads_config_not_a_self_http_call() {
        let registry = cog_core::loop_health::registry();
        let registered = || {
            registry
                .names()
                .iter()
                .any(|n| n == GITEE_TOKEN_REFRESHER_LOOP)
        };

        // 没配：一行日志退出，不登记。
        let shutdown = cog_core::ShutdownSignal::new();
        let metrics = Arc::new(crate::test_metrics::TestMetrics::default());
        spawn_gitee_token_refresher(shutdown.clone(), 3600, 600, metrics, false)
            .await
            .expect("the deployment gate task itself must not panic");
        assert!(
            !registered(),
            "没配 OAuth App 时不该起续期循环，但登记表里出现了它"
        );

        // 配了：起。这一半没有网络可走——判据退回一次自问，测试进程里必然连不上，
        // 于是这里就会红。
        let shutdown = cog_core::ShutdownSignal::new();
        let metrics = Arc::new(crate::test_metrics::TestMetrics::default());
        let gate = spawn_gitee_token_refresher(shutdown.clone(), 3600, 600, metrics, true);
        gate.await
            .expect("the deployment gate task itself must not panic");
        // 登记在起循环的那个任务里做，所以它可能比 `gate` 这一笔晚一点。
        for _ in 0..50 {
            if registered() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let started = registered();
        shutdown.trigger();
        assert!(
            started,
            "OAuth App 配好了却没起续期循环：准入判据去问了本进程的监听口，而它此刻还没起来"
        );
    }

    /// 一条问不到的答复不许被读成「这个部署没配 OAuth App」。判的是分支：`Err`
    /// 那一支走了，`Ok(None)` 那一支没走。无论问不到的原因是客户端没装配、连接被
    /// 拒、还是答复不是 JSON，结论都必须是「不知道」，而不是「没有」——设置屏与
    /// 授权按钮都按这个分支决定是劝人补配置，还是说一句「稍后重试」。
    #[tokio::test]
    async fn a_gateway_that_cannot_be_asked_is_not_read_as_no_oauth_app() {
        match gitee_oauth_client_id_from("http://127.0.0.1:1").await {
            Err(e) => assert!(!e.trim().is_empty(), "问不到也要说清为什么"),
            Ok(v) => panic!("连不上的网关被读成了答复 {v:?}"),
        }
    }

    /// 反过来那一半：网关答得出来时，「答了没有」必须如实读成 `None`，不能因为
    /// 上面那条门而对真的没配也含糊。
    #[test]
    fn an_answer_without_a_client_id_is_read_as_not_configured() {
        let answered = serde_json::json!({ "available": false });
        assert_eq!(client_id_in_body(&answered), None);
        let empty = serde_json::json!({ "available": true, "client_id": "" });
        assert_eq!(client_id_in_body(&empty), None);
        let present = serde_json::json!({ "available": true, "client_id": "abc" });
        assert_eq!(client_id_in_body(&present).as_deref(), Some("abc"));
    }
}
