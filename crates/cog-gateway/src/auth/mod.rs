//! Authentication handlers: registration, login, refresh, and profile.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

pub mod error;
pub mod middleware;
mod password;
pub mod platform;
pub use self::{
    error::AuthError,
    password::{hash_password, verify_password},
};
use cog_core::{Claims, Permission, SessionInfo, User, UserStatus, UserType};

use crate::GatewayState;

// ---------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct RegisterPayload {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub phone: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoginPayload {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct RefreshPayload {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateProfilePayload {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub phone: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub user: UserResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: String,
    pub username: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub status: String,
    pub user_type: String,
    pub created_at: DateTime<Utc>,
}

impl From<&User> for UserResponse {
    fn from(u: &User) -> Self {
        Self {
            id: u.id.to_string(),
            username: u.username.clone(),
            email: u.email.clone(),
            phone: u.phone.clone(),
            display_name: u.display_name.clone(),
            avatar_url: u.avatar_url.clone(),
            status: format!("{:?}", u.status).to_lowercase(),
            user_type: format!("{:?}", u.user_type).to_lowercase(),
            created_at: u.created_at,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory user store
// ---------------------------------------------------------------------------

/// Internal stored user record (includes password hash).
#[derive(Debug, Clone)]
pub struct StoredUser {
    pub user: User,
    pub password_hash: String,
}

/// Thread-safe in-memory user storage.
#[derive(Debug, Default)]
pub struct InMemoryUserStore {
    users: RwLock<HashMap<Uuid, StoredUser>>,
    username_index: RwLock<HashMap<String, Uuid>>,
    email_index: RwLock<HashMap<String, Uuid>>,
    phone_index: RwLock<HashMap<String, Uuid>>,
}

impl InMemoryUserStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(
        &self,
        username: String,
        password_hash: String,
        email: Option<String>,
        phone: Option<String>,
        display_name: Option<String>,
    ) -> Result<User, AuthError> {
        // Check uniqueness
        {
            let idx = self.username_index.read().await;
            if idx.contains_key(&username) {
                return Err(AuthError::UserAlreadyExists(username));
            }
        }
        if let Some(ref e) = email {
            let idx = self.email_index.read().await;
            if idx.contains_key(e) {
                return Err(AuthError::UserAlreadyExists(e.clone()));
            }
        }
        if let Some(ref p) = phone {
            let idx = self.phone_index.read().await;
            if idx.contains_key(p) {
                return Err(AuthError::UserAlreadyExists(p.clone()));
            }
        }

        let id = Uuid::new_v4();
        let now = Utc::now();
        // The first user of an instance is its owner, and every later one is
        // Standard. The persistent store decides this the same way; a dev
        // store that gave everyone Standard would be a deployment where
        // `find_owner` finds nobody and the two stores tell different stories.
        let user_type = if self.users.read().await.is_empty() {
            UserType::Admin
        } else {
            UserType::Standard
        };
        let user = User {
            id,
            phone: phone.clone(),
            email: email.clone(),
            username: username.clone(),
            display_name: display_name.clone(),
            avatar_url: None,
            status: UserStatus::Active,
            user_type,
            created_at: now,
            updated_at: now,
        };

        let stored = StoredUser {
            user: user.clone(),
            password_hash,
        };

        self.users.write().await.insert(id, stored);
        self.username_index.write().await.insert(username, id);
        if let Some(e) = email {
            self.email_index.write().await.insert(e, id);
        }
        if let Some(p) = phone {
            self.phone_index.write().await.insert(p, id);
        }

        Ok(user)
    }

    pub async fn find_by_username(&self, username: &str) -> Option<StoredUser> {
        let idx = self.username_index.read().await;
        let id = idx.get(username).copied()?;
        drop(idx);
        let users = self.users.read().await;
        users.get(&id).cloned()
    }

    pub async fn find_by_email(&self, email: &str) -> Option<StoredUser> {
        let idx = self.email_index.read().await;
        let id = idx.get(email).copied()?;
        drop(idx);
        let users = self.users.read().await;
        users.get(&id).cloned()
    }

    pub async fn find_by_id(&self, id: Uuid) -> Option<StoredUser> {
        self.users.read().await.get(&id).cloned()
    }

    pub async fn update(&self, id: Uuid, updates: UpdateProfilePayload) -> Option<User> {
        let mut users = self.users.write().await;
        let stored = users.get_mut(&id)?;

        if let Some(display_name) = updates.display_name {
            stored.user.display_name = Some(display_name);
        }
        if let Some(avatar_url) = updates.avatar_url {
            stored.user.avatar_url = Some(avatar_url);
        }
        if let Some(email) = updates.email {
            // Update email index
            if let Some(old_email) = stored.user.email.clone() {
                let mut idx = self.email_index.write().await;
                idx.remove(&old_email);
                idx.insert(email.clone(), id);
            } else {
                let mut idx = self.email_index.write().await;
                idx.insert(email.clone(), id);
            }
            stored.user.email = Some(email);
        }
        if let Some(phone) = updates.phone {
            if let Some(old_phone) = stored.user.phone.clone() {
                let mut idx = self.phone_index.write().await;
                idx.remove(&old_phone);
                idx.insert(phone.clone(), id);
            } else {
                let mut idx = self.phone_index.write().await;
                idx.insert(phone.clone(), id);
            }
            stored.user.phone = Some(phone);
        }
        stored.user.updated_at = Utc::now();

        Some(stored.user.clone())
    }
}

#[async_trait::async_trait]
impl cog_core::UserStore for InMemoryUserStore {
    async fn create(
        &self,
        username: String,
        password_hash: String,
        email: Option<String>,
        phone: Option<String>,
        display_name: Option<String>,
    ) -> Result<cog_core::User, cog_core::SFError> {
        self.create(username, password_hash, email, phone, display_name)
            .await
            .map_err(|e| cog_core::SFError::Auth(e.to_string()))
    }

    async fn find_by_username(
        &self,
        username: &str,
    ) -> Result<Option<cog_core::User>, cog_core::SFError> {
        Ok(self.find_by_username(username).await.map(|s| s.user))
    }

    async fn find_by_username_with_password(
        &self,
        username: &str,
    ) -> Result<Option<(cog_core::User, String)>, cog_core::SFError> {
        Ok(self
            .find_by_username(username)
            .await
            .map(|s| (s.user, s.password_hash)))
    }

    async fn find_by_email(
        &self,
        email: &str,
    ) -> Result<Option<cog_core::User>, cog_core::SFError> {
        Ok(self.find_by_email(email).await.map(|s| s.user))
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<cog_core::User>, cog_core::SFError> {
        Ok(self.find_by_id(id).await.map(|s| s.user))
    }

    async fn update(
        &self,
        id: Uuid,
        updates: cog_core::UserUpdate,
    ) -> Result<Option<cog_core::User>, cog_core::SFError> {
        let payload = UpdateProfilePayload {
            display_name: updates.display_name,
            avatar_url: updates.avatar_url,
            email: updates.email,
            phone: updates.phone,
        };
        Ok(self.update(id, payload).await)
    }

    async fn find_owner(&self) -> Result<Option<cog_core::User>, cog_core::SFError> {
        let users = self.users.read().await;
        // Same rule as the persistent store: `admin` names the owner, because
        // `create` gives that marker to the first user and to nobody else.
        // `created_at` decides only which admin, if there is ever more than
        // one, and `id` keeps that answer from moving between calls.
        Ok(users
            .values()
            .filter(|stored| stored.user.user_type == UserType::Admin)
            .min_by_key(|stored| (stored.user.created_at, stored.user.id))
            .map(|stored| stored.user.clone()))
    }
}

// ---------------------------------------------------------------------------
// Login rate limiter (Redis-backed)
// ---------------------------------------------------------------------------

/// Failed-login attempts allowed per identifier inside the window before
/// the endpoint answers 429.
pub const LOGIN_MAX_ATTEMPTS: u32 = 5;
/// Sliding window (seconds) for failed-login counting.
pub const LOGIN_WINDOW_SECONDS: u64 = 300;

/// Rate-limits failed login attempts per identifier.
#[derive(Clone)]
pub struct LoginRateLimiter {
    store: Arc<dyn cog_core::KeyValueStore>,
    max_attempts: u32,
    window_seconds: u64,
}

impl std::fmt::Debug for LoginRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoginRateLimiter")
            .field("store", &"key-value store")
            .field("max_attempts", &self.max_attempts)
            .field("window_seconds", &self.window_seconds)
            .finish()
    }
}

impl LoginRateLimiter {
    pub fn new(
        store: Arc<dyn cog_core::KeyValueStore>,
        max_attempts: u32,
        window_seconds: u64,
    ) -> Self {
        Self {
            store,
            max_attempts,
            window_seconds,
        }
    }

    fn key(identifier: &str) -> String {
        format!("login_attempts:{}", identifier)
    }

    /// Check if the identifier is currently rate-limited.
    pub async fn is_limited(&self, identifier: &str) -> bool {
        self.count(&Self::key(identifier)).await >= self.max_attempts
    }

    /// Record a failed login attempt.
    pub async fn record_failure(&self, identifier: &str) {
        self.record_attempt(Self::key(identifier)).await;
    }

    /// Clear attempts on successful login.
    pub async fn clear(&self, identifier: &str) {
        let _ = self.store.del(&Self::key(identifier)).await;
    }

    /// Generic rate-limit check with a custom prefix.
    pub async fn is_limited_prefix(&self, prefix: &str, identifier: &str) -> bool {
        self.count(&format!("{}:{}", prefix, identifier)).await >= self.max_attempts
    }

    /// Record a generic attempt with a custom prefix.
    pub async fn record_attempt_prefix(&self, prefix: &str, identifier: &str) {
        self.record_attempt(format!("{}:{}", prefix, identifier))
            .await;
    }

    /// The counter behind an identifier, or zero. An unreadable counter is
    /// reported as zero rather than as "limited": failing closed here would turn
    /// a store outage into an outage of the login path as well, and the limiter
    /// exists to slow an attacker down, not to be the thing that decides whether
    /// anyone can sign in.
    async fn count(&self, key: &str) -> u32 {
        self.store
            .get(key)
            .await
            .ok()
            .flatten()
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(0)
    }

    async fn record_attempt(&self, key: String) {
        // The counter carries its own expiry, set in the same round trip that
        // creates it: a separate expiry would leave a window in which the key
        // exists without one, and a crash inside that window locks the
        // identifier out permanently.
        let _ = self.store.incr_ex(&key, 1, self.window_seconds).await;
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Permissions granted per user type. Mirrors the role mapping in cog-auth
/// so the JWT claim set matches what the role implies; previously every real
/// user was hardcoded to read-only while the demo path got everything.
fn permissions_for_user_type(user_type: UserType) -> Vec<Permission> {
    match user_type {
        UserType::Admin => vec![
            Permission::AgentRead,
            Permission::AgentWrite,
            Permission::QuotaRead,
            Permission::QuotaAdmin,
            Permission::UserAdmin,
            Permission::WorkspaceManageMembers,
            Permission::WorkspaceConfig,
        ],
        UserType::Standard => vec![
            Permission::AgentRead,
            Permission::AgentWrite,
            Permission::QuotaRead,
        ],
        UserType::Guest => vec![Permission::AgentRead],
    }
}

/// Access-token lifetime in seconds reported to clients — derived from the
/// same gateway config the JWT manager signs with, so the advertised
/// `expires_in` never drifts from the real token TTL.
fn access_token_expires_in(state: &GatewayState) -> u64 {
    state
        .config
        .read()
        .map(|c| c.effective_access_token_ttl_minutes())
        .unwrap_or(cog_core::DEFAULT_ACCESS_TOKEN_TTL_MINUTES)
        .saturating_mul(60)
}

/// Best-effort client IP for the session audit record: first
/// X-Forwarded-For hop, else X-Real-IP, else "unknown".
fn client_ip(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "unknown".into())
}

pub async fn register_handler(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<RegisterPayload>,
) -> Response {
    // Rate-limit check
    if let Some(ref limiter) = state.login_rate_limiter {
        if limiter
            .is_limited_prefix("register", &payload.username)
            .await
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "rate_limited",
                    "message": "Too many registration attempts. Please try again later."
                })),
            )
                .into_response();
        }
    }

    // Validation
    if payload.username.len() < 3 || payload.username.len() > 64 {
        if let Some(ref limiter) = state.login_rate_limiter {
            limiter
                .record_attempt_prefix("register", &payload.username)
                .await;
        }
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_username",
                "message": "Username must be between 3 and 64 characters"
            })),
        )
            .into_response();
    }
    if payload.password.len() < 8 {
        if let Some(ref limiter) = state.login_rate_limiter {
            limiter
                .record_attempt_prefix("register", &payload.username)
                .await;
        }
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid_password",
                "message": "Password must be at least 8 characters"
            })),
        )
            .into_response();
    }

    let password_hash = match hash_password(&payload.password) {
        Ok(h) => h,
        Err(e) => {
            warn!("Password hashing failed: {}", e);
            return e.into_response();
        }
    };

    let store = match state.user_store.as_ref() {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "auth_unavailable",
                    "message": "Authentication service is not configured"
                })),
            )
                .into_response();
        }
    };

    let user = match store
        .create(
            payload.username.clone(),
            password_hash,
            payload.email.clone(),
            payload.phone.clone(),
            payload.display_name.clone(),
        )
        .await
    {
        Ok(u) => u,
        Err(e) => {
            if let Some(ref limiter) = state.login_rate_limiter {
                limiter
                    .record_attempt_prefix("register", &payload.username)
                    .await;
            }
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "registration_failed", "message": e.to_string()})),
            )
                .into_response();
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
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    info!("User registered: {}", payload.username);
    (
        StatusCode::CREATED,
        Json(AuthResponse {
            access_token: token_pair.access_token,
            refresh_token: token_pair.refresh_token,
            token_type: "Bearer".into(),
            expires_in: access_token_expires_in(&state),
            user: UserResponse::from(&user),
            session_token: None,
        }),
    )
        .into_response()
}

/// Login without a configured user store (single-node / demo deployments).
/// Escape hatches in priority order: the bootstrap admin password
/// (`COGNEVA_ADMIN_PASSWORD`, explicitly provisioned by the operator), then
/// the demo switch (`gateway.demo_login_enabled`). Both issue an admin token;
/// with neither configured the endpoint fails closed.
async fn login_without_user_store(state: &GatewayState, payload: &LoginPayload) -> Response {
    let admin_password = bootstrap_admin_password();
    let demo_enabled = state
        .config
        .read()
        .map(|c| c.demo_login_enabled)
        .unwrap_or(false);

    let authorized = if let Some(expected) = admin_password {
        payload.username == "admin"
            && password::constant_time_eq(payload.password.as_bytes(), expected.as_bytes())
    } else if demo_enabled {
        warn!(
            "demo login enabled; granting admin token to '{}'",
            payload.username
        );
        true
    } else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "auth_unavailable",
                "message": "No user store configured. Set COGNEVA_ADMIN_PASSWORD for the bootstrap admin login, or enable gateway.demo_login_enabled for throwaway demos."
            })),
        )
            .into_response();
    };
    if !authorized {
        warn!("bootstrap admin login rejected for '{}'", payload.username);
        return AuthError::InvalidCredentials.into_response();
    }

    issue_bootstrap_admin(state, &payload.username).await
}

/// 引导管理员的令牌签发：身份是 `Uuid::nil()`、不属于任何用户行。
/// 有用户库与无用户库两条入口共用它，差别只在「谁被允许走到这里」。
async fn issue_bootstrap_admin(state: &GatewayState, username: &str) -> Response {
    let admin_user = User {
        id: Uuid::nil(),
        phone: None,
        email: None,
        username: username.to_string(),
        display_name: Some(username.to_string()),
        avatar_url: None,
        status: UserStatus::Active,
        user_type: UserType::Admin,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let token_pair = match state
        .jwt_manager
        .generate_token(
            &admin_user,
            vec!["default".into()],
            permissions_for_user_type(UserType::Admin),
        )
        .await
    {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    info!("Bootstrap login: {}", username);
    (
        StatusCode::OK,
        Json(AuthResponse {
            access_token: token_pair.access_token,
            refresh_token: token_pair.refresh_token,
            token_type: "Bearer".into(),
            expires_in: access_token_expires_in(state),
            user: UserResponse::from(&admin_user),
            session_token: None,
        }),
    )
        .into_response()
}

/// 这个部署有没有配引导管理员口令。装机时随机生成、只从 Secret 注入；
/// 仓库与 values 里永远为空，所以「没配」是常态，「配了」才是有人开了这扇门。
fn bootstrap_admin_password() -> Option<String> {
    std::env::var("COGNEVA_ADMIN_PASSWORD")
        .ok()
        .filter(|p| !p.is_empty())
}

pub async fn login_handler(
    State(state): State<Arc<GatewayState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<LoginPayload>,
) -> Response {
    let store = match state.user_store.as_ref() {
        Some(s) => s,
        None => return login_without_user_store(&state, &payload).await,
    };

    // Rate-limit check
    if let Some(ref limiter) = state.login_rate_limiter {
        if limiter.is_limited(&payload.username).await {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "rate_limited",
                    "message": "Too many failed login attempts. Please try again later."
                })),
            )
                .into_response();
        }
    }

    // 有用户库时的引导管理员：正式部署里用户库一直在，而首个管理员除了外部平台
    // 账号（要外网、要 OAuth 应用）没有本地入口——装完登录不进去。口令由安装脚本
    // 随机写进 Secret，只有部署方能读到；口令不对时**照样往下走用户库**，
    // 这样一个真名叫 `admin` 的本地账号仍能用自己的哈希登录，不会被人一配环境变量就顶掉。
    if let Some(expected) = bootstrap_admin_password() {
        if payload.username == "admin"
            && password::constant_time_eq(payload.password.as_bytes(), expected.as_bytes())
        {
            return issue_bootstrap_admin(&state, "admin").await;
        }
    }

    let (user, password_hash) = match store
        .find_by_username_with_password(&payload.username)
        .await
    {
        Ok(Some(pair)) => pair,
        Ok(None) => {
            if let Some(ref limiter) = state.login_rate_limiter {
                limiter.record_failure(&payload.username).await;
            }
            return AuthError::InvalidCredentials.into_response();
        }
        Err(e) => {
            warn!("User lookup error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "lookup_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };

    // Check account status
    match user.status {
        UserStatus::Disabled => {
            return AuthError::AccountDisabled.into_response();
        }
        UserStatus::Locked => {
            return AuthError::AccountLocked.into_response();
        }
        _ => {}
    }

    let valid = match verify_password(&payload.password, &password_hash) {
        Ok(v) => v,
        Err(e) => {
            warn!("Password verification error: {}", e);
            return e.into_response();
        }
    };

    if !valid {
        if let Some(ref limiter) = state.login_rate_limiter {
            limiter.record_failure(&payload.username).await;
        }
        return AuthError::InvalidCredentials.into_response();
    }

    // Clear rate-limit on success
    if let Some(ref limiter) = state.login_rate_limiter {
        limiter.clear(&payload.username).await;
    }

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
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Create Redis session if session manager is available
    let session_token = if let Some(ref sm) = state.session_manager {
        let session_info = SessionInfo {
            user_id: user.id,
            workspace_id: None,
            login_method: "password".into(),
            login_ip: client_ip(&headers),
            login_at: chrono::Utc::now(),
            last_active: chrono::Utc::now(),
            device_info: None,
        };
        match sm.create(session_info).await {
            Ok(sid) => Some(sid.to_string()),
            Err(e) => {
                warn!(
                    "Session creation failed for user {}: {}",
                    payload.username, e
                );
                None
            }
        }
    } else {
        None
    };

    let cookie = format!(
        "refresh_token={}; HttpOnly; Secure; SameSite=Strict; Path=/api/v1/auth/refresh; Max-Age=604800",
        token_pair.refresh_token
    );

    info!("User logged in: {}", payload.username);
    (
        StatusCode::OK,
        [(axum::http::header::SET_COOKIE, cookie)],
        Json(AuthResponse {
            access_token: token_pair.access_token,
            refresh_token: token_pair.refresh_token,
            token_type: "Bearer".into(),
            expires_in: access_token_expires_in(&state),
            user: UserResponse::from(&user),
            session_token,
        }),
    )
        .into_response()
}

pub async fn refresh_handler(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<RefreshPayload>,
) -> Response {
    let new_access = match state
        .jwt_manager
        .refresh_access_token(&payload.refresh_token)
        .await
    {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    (
        StatusCode::OK,
        Json(json!({
            "access_token": new_access,
            "token_type": "Bearer",
            "expires_in": access_token_expires_in(&state)
        })),
    )
        .into_response()
}

pub async fn get_me_handler(
    State(state): State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> Response {
    let claims = match request.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => {
            return AuthError::MissingAuthHeader.into_response();
        }
    };

    let user_id = match claims.user_id() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let store = match state.user_store.as_ref() {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "auth_unavailable",
                    "message": "Authentication service is not configured"
                })),
            )
                .into_response();
        }
    };

    let user = match store.find_by_id(user_id).await {
        Ok(Some(u)) => u,
        Ok(None) => return AuthError::UserNotFound(user_id.to_string()).into_response(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "lookup_failed", "message": e.to_string()})),
            )
                .into_response()
        }
    };

    (StatusCode::OK, Json(UserResponse::from(&user))).into_response()
}

pub async fn update_me_handler(
    State(state): State<Arc<GatewayState>>,
    req: axum::extract::Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => {
            return AuthError::MissingAuthHeader.into_response();
        }
    };

    let user_id = match claims.user_id() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Buffer and parse the body manually since Request consumes the body
    let (_parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_body", "message": "Failed to read request body" })),
            )
                .into_response();
        }
    };

    let payload: UpdateProfilePayload = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_json", "message": e.to_string() })),
            )
                .into_response();
        }
    };

    let store = match state.user_store.as_ref() {
        Some(s) => s,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "auth_unavailable",
                    "message": "Authentication service is not configured"
                })),
            )
                .into_response();
        }
    };

    let updates = cog_core::UserUpdate {
        display_name: payload.display_name,
        avatar_url: payload.avatar_url,
        email: payload.email,
        phone: payload.phone,
    };
    let user = match store.update(user_id, updates).await {
        Ok(Some(u)) => u,
        Ok(None) => return AuthError::UserNotFound(user_id.to_string()).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    info!("User profile updated: {}", user_id);
    (StatusCode::OK, Json(UserResponse::from(&user))).into_response()
}

// ---------------------------------------------------------------------------
// Session handlers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct LogoutPayload {
    #[serde(default)]
    pub session_token: Option<String>,
}

pub async fn logout_handler(
    State(state): State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> Response {
    let claims = match request.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => {
            return AuthError::MissingAuthHeader.into_response();
        }
    };

    let user_id = match claims.user_id() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let Some(ref sm) = state.session_manager else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "session_unavailable",
                "message": "Session manager is not configured"
            })),
        )
            .into_response();
    };

    // If a specific session token is provided via X-Session-Token header, destroy only that session.
    // Otherwise destroy all sessions for the user.
    let session_token = request
        .headers()
        .get("x-session-token")
        .and_then(|h| h.to_str().ok());

    if let Some(token_str) = session_token {
        match uuid::Uuid::parse_str(token_str) {
            Ok(session_id) => {
                if let Err(e) = sm.destroy(user_id, session_id).await {
                    warn!("Session destroy failed for {}: {}", session_id, e);
                }
                info!("User {} logged out session {}", user_id, session_id);
            }
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "invalid_session_token",
                        "message": "Invalid session token format"
                    })),
                )
                    .into_response();
            }
        }
    } else {
        if let Err(e) = sm.destroy_all(user_id).await {
            warn!("Session destroy_all failed for {}: {}", user_id, e);
        }
        info!("User {} logged out all sessions", user_id);
    }

    (StatusCode::OK, Json(json!({ "status": "logged_out" }))).into_response()
}

// ---------------------------------------------------------------------------
// Device registration (push tokens)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct DevicePayload {
    pub platform: String, // ios | android | harmonyos
    #[serde(default)]
    pub device_model: Option<String>,
    #[serde(default)]
    pub os_version: Option<String>,
    #[serde(default)]
    pub app_version: Option<String>,
    pub push_token: String,
    pub push_provider: String, // apns | fcm | hms
    pub device_id: String,
}

/// Register or update a push device for the authenticated user.
/// **Status:** Stub — accepts the payload, validates auth, and returns OK.
/// Persistent device storage will be wired up once the device registry
pub async fn device_handler(
    State(_state): State<Arc<GatewayState>>,
    req: axum::extract::Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => return AuthError::MissingAuthHeader.into_response(),
    };

    let user_id = match claims.user_id() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Buffer and parse the body manually
    let (_parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_body", "message": "Failed to read request body" })),
            )
                .into_response();
        }
    };

    let payload: DevicePayload = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_json", "message": e.to_string() })),
            )
                .into_response();
        }
    };

    info!(
        "Device registered: user={} device_id={} platform={} provider={}",
        user_id, payload.device_id, payload.platform, payload.push_provider
    );

    (
        StatusCode::OK,
        Json(json!({
            "status": "registered",
            "device_id": payload.device_id,
            "user_id": user_id.to_string(),
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Biometric login (register / verify)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct BiometricPayload {
    pub action: String, // register | verify
    #[serde(default)]
    pub biometric_public_key: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    pub device_id: String,
}

/// Bind or verify a biometric credential (Touch ID / Face ID / fingerprint).
/// **Status:** Not implemented — validates the payload's shape and answers
/// `501`. There is no key store behind this route, so there is nothing to
/// enrol into and nothing to check a signature against; answering success
/// would hand the caller a receipt for a credential that does not exist.
pub async fn biometric_handler(
    State(_state): State<Arc<GatewayState>>,
    req: axum::extract::Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => return AuthError::MissingAuthHeader.into_response(),
    };

    let user_id = match claims.user_id() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Buffer and parse the body manually
    let (_parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_body", "message": "Failed to read request body" })),
            )
                .into_response();
        }
    };

    let payload: BiometricPayload = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_json", "message": e.to_string() })),
            )
                .into_response();
        }
    };

    let (status, body) = biometric_reply(&payload, &user_id.to_string());
    (status, Json(body)).into_response()
}

/// What this build can tell a biometric caller.
///
/// Nothing -- and it has to say so. The handler used to answer `registered` and
/// `verified` after writing a log line: a client that enrolled a key got a
/// receipt for a key the platform never stored, and found out at the first
/// unlock. A stub that reports success is worse than a missing route, because
/// the caller has no way to tell the two apart. The shape checks stay so a
/// caller learns what a real request looks like, and the answer to a
/// well-formed one is `501`: the credential is not kept anywhere, so there is
/// nothing to accept.
fn biometric_reply(payload: &BiometricPayload, user_id: &str) -> (StatusCode, serde_json::Value) {
    match payload.action.as_str() {
        "register" if payload.biometric_public_key.is_none() => (
            StatusCode::BAD_REQUEST,
            json!({
                "error": "missing_public_key",
                "message": "biometric_public_key is required for action=register"
            }),
        ),
        "verify" if payload.signature.is_none() => (
            StatusCode::BAD_REQUEST,
            json!({
                "error": "missing_signature",
                "message": "signature is required for action=verify"
            }),
        ),
        "register" | "verify" => {
            info!(
                user = user_id,
                device = %payload.device_id,
                action = %payload.action,
                "biometric request refused: no key store exists in this build"
            );
            (
                StatusCode::NOT_IMPLEMENTED,
                json!({
                    "error": "not_implemented",
                    "message": "biometric registration and verification are not implemented; \
                                no credential was stored",
                }),
            )
        }
        other => (
            StatusCode::BAD_REQUEST,
            json!({
                "error": "invalid_action",
                "message": format!("action must be 'register' or 'verify', got: {}", other)
            }),
        ),
    }
}

pub async fn validate_session_handler(
    State(state): State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> Response {
    let claims = match request.extensions().get::<Claims>() {
        Some(c) => c.clone(),
        None => {
            return AuthError::MissingAuthHeader.into_response();
        }
    };

    let user_id = match claims.user_id() {
        Ok(id) => id,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let session_token = request
        .headers()
        .get("x-session-token")
        .and_then(|h| h.to_str().ok());

    let Some(ref sm) = state.session_manager else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "session_unavailable",
                "message": "Session manager is not configured"
            })),
        )
            .into_response();
    };

    match session_token {
        Some(token_str) => {
            match uuid::Uuid::parse_str(token_str) {
                Ok(session_id) => {
                    match sm.get(user_id, session_id).await {
                        Ok(Some(session)) => {
                            // Optionally refresh the session on validation
                            let _ = sm.refresh(user_id, session_id).await;
                            (
                                StatusCode::OK,
                                Json(json!({
                                    "valid": true,
                                    "user_id": user_id.to_string(),
                                    "login_at": session.login_at,
                                    "last_active": session.last_active,
                                })),
                            )
                                .into_response()
                        }
                        Ok(None) => (
                            StatusCode::UNAUTHORIZED,
                            Json(json!({
                                "valid": false,
                                "error": "session_not_found",
                                "message": "Session expired or invalid"
                            })),
                        )
                            .into_response(),
                        Err(e) => {
                            warn!("Session validation error: {}", e);
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "session_validation_failed", "message": e.to_string()})),
                            )
                                .into_response()
                        }
                    }
                }
                Err(_) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "valid": false,
                        "error": "invalid_session_token",
                        "message": "Invalid session token format"
                    })),
                )
                    .into_response(),
            }
        }
        None => {
            // No session token header — fall back to JWT-only validation
            (
                StatusCode::OK,
                Json(json!({
                    "valid": true,
                    "user_id": user_id.to_string(),
                    "session_checked": false,
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod biometric_tests {
    use super::*;

    fn payload(action: &str) -> BiometricPayload {
        BiometricPayload {
            action: action.into(),
            biometric_public_key: Some("pk".into()),
            signature: Some("sig".into()),
            device_id: "device-1".into(),
        }
    }

    #[test]
    fn a_well_formed_biometric_request_is_refused_instead_of_reported_as_done() {
        // The endpoint used to answer `registered`/`verified` and store nothing:
        // a client enrolled a key and got a receipt for a key the platform does
        // not have. Both actions have to say they are not implemented.
        for action in ["register", "verify"] {
            let (status, body) = biometric_reply(&payload(action), "user-1");
            assert_eq!(
                status,
                StatusCode::NOT_IMPLEMENTED,
                "{action} answered {body}"
            );
            assert_eq!(body["error"], "not_implemented");
        }
    }

    #[test]
    fn a_malformed_request_is_told_what_it_is_missing() {
        let mut register = payload("register");
        register.biometric_public_key = None;
        let (status, body) = biometric_reply(&register, "user-1");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "missing_public_key");

        let mut verify = payload("verify");
        verify.signature = None;
        let (status, body) = biometric_reply(&verify, "user-1");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "missing_signature");

        let (status, body) = biometric_reply(&payload("enrol"), "user-1");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_action");
    }
}
