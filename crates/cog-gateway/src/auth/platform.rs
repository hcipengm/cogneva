//! Platform-account login: connecting a GitHub/Gitee account IS the login.
//!
//! Three public entry points share one core (`login_with_platform_token`):
//! - PAT / manual token (`/login`)
//! - OAuth authorization-code flow (`/start` + `/exchange`, plus a
//!   browser-reachable `/callback/{provider}` for installs where the platform
//!   can redirect straight back to this gateway)
//!
//! Every successful login upserts the local account (first connector becomes
//! Admin), signs a JWT, and best-effort persists the platform token into the
//! secure-gateway Secret so the contribution channel rides the same
//! authorization.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tracing::{info, warn};

use cog_core::PlatformIdentityStore;

use super::{permissions_for_user_type, AuthResponse, UserResponse};
use crate::contribution_admin as contrib;
use crate::GatewayState;

/// Profile fetched from the platform with the user's token.
#[derive(Debug, Clone)]
pub struct PlatformProfile {
    pub id: String,
    pub login: String,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
}

/// Fetch the platform profile for a token. Errors are user-facing Chinese
/// strings, consistent with the contribution channel.
pub async fn fetch_platform_profile(
    provider: &str,
    token: &str,
) -> Result<PlatformProfile, String> {
    let client = contrib::http_client();
    let body: serde_json::Value = match provider {
        "github" => {
            let resp = client
                .get(format!("{}/user", contrib::GITHUB_API))
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
            resp.json().await.map_err(|e| e.to_string())?
        }
        "gitee" => {
            let resp = client
                .get(format!("{}/user", contrib::GITEE_API))
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
            resp.json().await.map_err(|e| e.to_string())?
        }
        other => return Err(format!("未知平台: {other}")),
    };
    parse_profile(&body)
}

/// Extract a [`PlatformProfile`] from a platform `/user` response body.
fn parse_profile(body: &serde_json::Value) -> Result<PlatformProfile, String> {
    let login = body
        .get("login")
        .and_then(|v| v.as_str())
        .ok_or("平台资料缺少 login 字段")?
        .to_string();
    let id = body
        .get("id")
        .map(|v| match v {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s.clone(),
            _ => String::new(),
        })
        .filter(|s| !s.is_empty())
        .ok_or("平台资料缺少 id 字段")?;
    Ok(PlatformProfile {
        id,
        login,
        name: body
            .get("name")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        avatar_url: body
            .get("avatar_url")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    })
}

fn json_err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({"error": code, "message": message.into()})),
    )
        .into_response()
}

#[allow(clippy::result_large_err)] // Response is the handler-native error type here
fn require_identity_store(
    state: &GatewayState,
) -> Result<Arc<dyn PlatformIdentityStore>, Response> {
    state.platform_identities.clone().ok_or_else(|| {
        json_err(
            StatusCode::SERVICE_UNAVAILABLE,
            "account_store_unavailable",
            "账号库未就绪（需要 PostgreSQL）；离线/演示环境请用管理员密码或 demo 开关登录",
        )
    })
}

/// Shared core: verify token → upsert account → sign JWT → persist the token
/// into the gateway Secret (best-effort; the login itself must not fail when
/// the cluster Secret write is unavailable).
async fn login_with_platform_token(
    state: &GatewayState,
    provider: &str,
    access_token: &str,
    gitee_refresh: Option<&str>,
) -> Response {
    let store = match require_identity_store(state) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let profile = match fetch_platform_profile(provider, access_token).await {
        Ok(p) => p,
        Err(message) => return json_err(StatusCode::BAD_GATEWAY, "token_verify_failed", message),
    };

    let token_ref = format!("secret:cogneva-secrets/{provider}_token");
    let refresh_ref = gitee_refresh.map(|_| format!("secret:cogneva-secrets/{provider}_refresh"));
    let (user, created) = match store
        .find_or_create_by_identity(
            provider,
            &profile.id,
            &profile.login,
            profile.name.clone(),
            profile.avatar_url.clone(),
            Some(token_ref),
            refresh_ref,
        )
        .await
    {
        Ok(v) => v,
        Err(e) => {
            return json_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "account_upsert_failed",
                e.to_string(),
            )
        }
    };

    let token_pair = match state
        .jwt_manager
        .generate_token(
            &user,
            vec!["default".into()],
            permissions_for_user_type(user.user_type),
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            return json_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "token_sign_failed",
                e.to_string(),
            )
        }
    };

    // Dual-use: the same token opens the contribution channel. Best-effort —
    // outside a cluster (dev runs) there is no Secret to write.
    let mut contribution_synced = false;
    if let Ok(kube) = crate::llm_admin::KubeClient::in_cluster() {
        let resp = contrib::persist_connected(
            &kube,
            provider,
            access_token,
            &profile.login,
            gitee_refresh
                .map(|r| contrib::GiteeTokenSet {
                    access_token: access_token.to_string(),
                    refresh_token: Some(r.to_string()),
                    expires_in: 0,
                    obtained_at: 0,
                })
                .as_ref(),
        )
        .await;
        contribution_synced = resp.status().is_success();
        if !contribution_synced {
            warn!("platform login: contribution secret persist failed for {provider}");
        }
    }

    info!(
        provider,
        login = %profile.login,
        created, "platform login"
    );
    (
        StatusCode::OK,
        Json(json!({
            "auth": AuthResponse {
                access_token: token_pair.access_token,
                refresh_token: token_pair.refresh_token,
                token_type: "Bearer".into(),
                expires_in: super::access_token_expires_in(state),
                user: UserResponse::from(&user),
                session_token: None,
            },
            "account_created": created,
            "contribution_synced": contribution_synced,
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// PAT / manual token login
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PlatformLoginRequest {
    /// `github` | `gitee`.
    pub provider: String,
    pub access_token: String,
}

/// POST /api/v1/auth/platform/login — PAT login also creates the local
/// account (offline/intranet escape hatch).
pub async fn platform_login_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<PlatformLoginRequest>,
) -> Response {
    let provider = req.provider.trim().to_lowercase();
    if provider != "github" && provider != "gitee" {
        return json_err(
            StatusCode::BAD_REQUEST,
            "invalid_provider",
            "provider 必须是 github 或 gitee",
        );
    }
    let token = req.access_token.trim();
    if token.is_empty() {
        return json_err(StatusCode::BAD_REQUEST, "missing_token", "请粘贴访问令牌");
    }
    login_with_platform_token(&state, &provider, token, None).await
}

// ---------------------------------------------------------------------------
// OAuth authorization-code flow
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct OAuthStartRequest {
    pub provider: String,
    /// Origin of this gateway as the user's browser reaches it
    /// (e.g. http://127.0.0.1:8080) — the callback lands back here.
    pub redirect_origin: String,
}

#[allow(clippy::result_large_err)] // handler-native error type, same as above
fn oauth_redirect_uri(origin: &str, provider: &str) -> Result<String, Response> {
    if !origin.starts_with("http://") && !origin.starts_with("https://") {
        return Err(json_err(
            StatusCode::BAD_REQUEST,
            "invalid_redirect_origin",
            "redirect_origin 必须是 http(s) 地址",
        ));
    }
    Ok(format!(
        "{}/api/v1/auth/platform/callback/{}",
        origin.trim_end_matches('/'),
        provider
    ))
}

/// POST /api/v1/auth/platform/start — begin the OAuth flow, returning the
/// platform authorize URL the frontend opens in a new tab.
pub async fn oauth_start_handler(Json(req): Json<OAuthStartRequest>) -> Response {
    let provider = req.provider.trim().to_lowercase();
    let redirect_uri = match oauth_redirect_uri(&req.redirect_origin, &provider) {
        Ok(u) => u,
        Err(r) => return r,
    };
    let state = contrib::new_oauth_state(&redirect_uri);
    let url = match provider.as_str() {
        "gitee" => {
            let Some(client_id) = contrib::gitee_oauth_client_id().await else {
                return json_err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "oauth_not_configured",
                    "尚未配置 Gitee OAuth 应用；请改用访问令牌（PAT）登录",
                );
            };
            let redirect = contrib::gitee_oauth_redirect_override().unwrap_or(redirect_uri);
            contrib::build_gitee_authorize_url(&client_id, &redirect, &state)
        }
        "github" => {
            let Some(client_id) = contrib::oauth_client_id(None) else {
                return json_err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "oauth_not_configured",
                    "尚未配置 GitHub OAuth client_id；请改用访问令牌（PAT）或设备流登录",
                );
            };
            format!(
                "https://github.com/login/oauth/authorize?client_id={client_id}&redirect_uri={}&scope={}&state={state}",
                urlencoded(&redirect_uri),
                urlencoded("public_repo read:user"),
            )
        }
        _ => {
            return json_err(
                StatusCode::BAD_REQUEST,
                "invalid_provider",
                "provider 必须是 github 或 gitee",
            )
        }
    };
    (
        StatusCode::OK,
        Json(json!({ "authorize_url": url, "state": state })),
    )
        .into_response()
}

fn urlencoded(value: &str) -> String {
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

/// Exchange a GitHub authorization code for an access token.
async fn exchange_github_code(code: &str, redirect_uri: &str) -> Result<String, String> {
    let client_id = contrib::oauth_client_id(None).ok_or("未配置 GitHub OAuth client_id")?;
    let client_secret = std::env::var("COGNEVA_GITHUB_OAUTH_CLIENT_SECRET")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .ok_or("未配置 GitHub OAuth client_secret")?;
    let resp = contrib::http_client()
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .json(&json!({
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }))
        .send()
        .await
        .map_err(|e| format!("无法连接 GitHub（{e}）"))?;
    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
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

#[derive(Debug, Deserialize)]
pub struct OAuthExchangeRequest {
    pub provider: String,
    pub state: String,
    /// Bare authorization code…
    pub code: Option<String>,
    /// …or the full redirect URL pasted from the browser address bar.
    pub redirect_url: Option<String>,
}

async fn exchange_code(
    provider: &str,
    state: &str,
    code: Option<&str>,
    redirect_url: Option<&str>,
) -> Result<(String, Option<String>), String> {
    let code = contrib::extract_gitee_code(code, redirect_url)
        .ok_or("请粘贴授权码，或授权后浏览器地址栏里的完整网址")?;
    let redirect_uri =
        contrib::take_oauth_state(state).ok_or("授权链接已过期或已使用，请重新发起登录")?;
    match provider {
        "gitee" => {
            let set = contrib::exchange_gitee_code(&code, &redirect_uri).await?;
            Ok((set.access_token, set.refresh_token))
        }
        "github" => exchange_github_code(&code, &redirect_uri)
            .await
            .map(|t| (t, None)),
        other => Err(format!("未知平台: {other}")),
    }
}

/// POST /api/v1/auth/platform/exchange — finish the OAuth flow from the
/// wizard with a pasted code / redirect URL.
pub async fn oauth_exchange_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<OAuthExchangeRequest>,
) -> Response {
    let provider = req.provider.trim().to_lowercase();
    if provider != "github" && provider != "gitee" {
        return json_err(
            StatusCode::BAD_REQUEST,
            "invalid_provider",
            "provider 必须是 github 或 gitee",
        );
    }
    match exchange_code(
        &provider,
        &req.state,
        req.code.as_deref(),
        req.redirect_url.as_deref(),
    )
    .await
    {
        Ok((token, refresh)) => {
            login_with_platform_token(&state, &provider, &token, refresh.as_deref()).await
        }
        Err(message) => json_err(StatusCode::BAD_GATEWAY, "oauth_exchange_failed", message),
    }
}

#[derive(Debug, Deserialize)]
pub struct OAuthCallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    pub error_description: Option<String>,
}

/// Render the post-login landing page: stash the tokens where the web app
/// reads them, then jump back to the app root.
fn login_landing_page(payload: &serde_json::Value) -> Html<String> {
    let data = serde_json::to_string(payload).unwrap_or_else(|_| "{}".into());
    Html(format!(
        "<!DOCTYPE html><html lang=\"zh\"><head><meta charset=\"utf-8\"><title>登录成功</title>\
         <style>body{{background:#0d1117;color:#e6edf3;font-family:ui-monospace,Menlo,Consolas,monospace;\
         display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}\
         .card{{max-width:420px;padding:28px;border:1px solid #30363d;border-radius:12px;background:#161b22}}\
         h1{{font-size:18px;margin:0 0 10px;color:#3fb950}}p{{font-size:13px;color:#8b949e;line-height:1.7;margin:0}}</style></head>\
         <body><div class=\"card\"><h1>登录成功</h1><p>正在回到 Cogneva…</p></div>\
         <script>try {{ const d = {data}; \
         localStorage.setItem('cogneva_token', d.auth.access_token); \
         localStorage.setItem('cogneva_user', JSON.stringify(d.auth.user)); \
         if (window.opener) {{ window.opener.postMessage({{ type: 'cogneva-login', data: d }}, location.origin); window.close(); }} \
         else {{ window.location.href = '/'; }} }} catch (e) {{ document.querySelector('p').textContent = '登录状态写入失败：' + e; }}</script>\
         </body></html>"
    ))
}

/// GET /api/v1/auth/platform/callback/{provider} — the redirect target when
/// the platform can reach this gateway directly (localhost installs). Public
/// by design: the single-use state parameter is the CSRF proof.
pub async fn oauth_callback_handler(
    State(state): State<Arc<GatewayState>>,
    Path(provider): Path<String>,
    Query(q): Query<OAuthCallbackQuery>,
) -> Response {
    let provider = provider.to_lowercase();
    if let Some(err) = q.error {
        let detail = q.error_description.unwrap_or_default();
        return contrib::oauth_result_page(
            "登录未完成",
            &format!("平台返回错误：{err} {detail}。请回到登录页重试。"),
            false,
        )
        .into_response();
    }
    let (Some(state_str), Some(code)) = (q.state, q.code) else {
        return contrib::oauth_result_page("登录未完成", "回调缺少 code 或 state 参数。", false)
            .into_response();
    };
    match exchange_code(&provider, &state_str, Some(&code), None).await {
        Ok((token, refresh)) => {
            // Same core, but render HTML instead of JSON for the browser.
            let store = match require_identity_store(&state) {
                Ok(s) => s,
                Err(r) => return r,
            };
            let profile = match fetch_platform_profile(&provider, &token).await {
                Ok(p) => p,
                Err(message) => {
                    return contrib::oauth_result_page("登录未完成", &message, false)
                        .into_response()
                }
            };
            let token_ref = format!("secret:cogneva-secrets/{provider}_token");
            let refresh_ref = refresh.map(|_| format!("secret:cogneva-secrets/{provider}_refresh"));
            let (user, created) = match store
                .find_or_create_by_identity(
                    &provider,
                    &profile.id,
                    &profile.login,
                    profile.name.clone(),
                    profile.avatar_url.clone(),
                    Some(token_ref),
                    refresh_ref,
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    return contrib::oauth_result_page("登录未完成", &e.to_string(), false)
                        .into_response()
                }
            };
            let token_pair = match state
                .jwt_manager
                .generate_token(
                    &user,
                    vec!["default".into()],
                    permissions_for_user_type(user.user_type),
                )
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    return contrib::oauth_result_page("登录未完成", &e.to_string(), false)
                        .into_response()
                }
            };
            if let Ok(kube) = crate::llm_admin::KubeClient::in_cluster() {
                let resp =
                    contrib::persist_connected(&kube, &provider, &token, &profile.login, None)
                        .await;
                if !resp.status().is_success() {
                    warn!("platform login callback: contribution secret persist failed");
                }
            }
            info!(provider, login = %profile.login, created, "platform login (callback)");
            login_landing_page(&json!({
                "auth": AuthResponse {
                    access_token: token_pair.access_token,
                    refresh_token: token_pair.refresh_token,
                    token_type: "Bearer".into(),
                    expires_in: super::access_token_expires_in(&state),
                    user: UserResponse::from(&user),
                    session_token: None,
                },
                "account_created": created,
            }))
            .into_response()
        }
        Err(message) => contrib::oauth_result_page("登录未完成", &message, false).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_profile_full_github_shape() {
        let body = serde_json::json!({
            "id": 12345,
            "login": "octocat",
            "name": "The Octocat",
            "avatar_url": "https://avatars.example/u/12345"
        });
        let p = parse_profile(&body).expect("parse");
        assert_eq!(p.id, "12345");
        assert_eq!(p.login, "octocat");
        assert_eq!(p.name.as_deref(), Some("The Octocat"));
        assert!(p.avatar_url.is_some());
    }

    #[test]
    fn parse_profile_string_id_and_empty_optionals() {
        let body = serde_json::json!({
            "id": "6789",
            "login": "gitee_user",
            "name": "",
            "avatar_url": ""
        });
        let p = parse_profile(&body).expect("parse");
        assert_eq!(p.id, "6789");
        assert_eq!(p.name, None);
        assert_eq!(p.avatar_url, None);
    }

    #[test]
    fn parse_profile_missing_login_or_id_fails() {
        assert!(parse_profile(&serde_json::json!({"id": 1})).is_err());
        assert!(parse_profile(&serde_json::json!({"login": "x"})).is_err());
        assert!(parse_profile(&serde_json::json!({"login": "x", "id": ""})).is_err());
        assert!(parse_profile(&serde_json::json!({"login": "x", "id": true})).is_err());
    }

    #[test]
    fn urlencoded_escapes_only_when_needed() {
        assert_eq!(urlencoded("abc-XYZ_09.~"), "abc-XYZ_09.~");
        assert_eq!(
            urlencoded("http://127.0.0.1:8080/cb?a=b&c=中"),
            "http%3A%2F%2F127.0.0.1%3A8080%2Fcb%3Fa%3Db%26c%3D%E4%B8%AD"
        );
    }

    #[test]
    fn oauth_redirect_uri_validates_scheme() {
        assert!(oauth_redirect_uri("http://127.0.0.1:8080", "gitee").is_ok());
        assert!(oauth_redirect_uri("https://example.com/", "github").is_ok());
        assert!(oauth_redirect_uri("ftp://x", "gitee").is_err());
        assert!(oauth_redirect_uri("127.0.0.1:8080", "gitee").is_err());
        let u = oauth_redirect_uri("http://a/", "github").unwrap();
        assert_eq!(u, "http://a/api/v1/auth/platform/callback/github");
    }
}
