use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// User account status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(rename_all = "snake_case")]
pub enum UserStatus {
    Active,
    Inactive,
    Disabled,
    Locked,
}

/// User type / tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(rename_all = "snake_case")]
pub enum UserType {
    Admin,
    Standard,
    Guest,
}

/// Core user entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub username: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub status: UserStatus,
    pub user_type: UserType,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// RBAC roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    SuperAdmin,
    OrgAdmin,
    Owner,
    Member,
    Visitor,
}

/// Fine-grained permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    AgentRead,
    AgentWrite,
    WorkspaceManageMembers,
    WorkspaceConfig,
    QuotaRead,
    QuotaAdmin,
    UserAdmin,
}

impl Role {
    pub fn is_admin(&self) -> bool {
        matches!(self, Role::SuperAdmin | Role::OrgAdmin)
    }

    pub fn is_operator(&self) -> bool {
        matches!(
            self,
            Role::SuperAdmin | Role::OrgAdmin | Role::Owner | Role::Member
        )
    }
}

/// Simplified role requirement for middleware checks.
#[derive(Debug, Clone, Copy)]
pub enum RoleRequirement {
    Admin,
    Operator,
    Viewer,
}

impl RoleRequirement {
    pub fn satisfied_by(&self, role: Role) -> bool {
        match self {
            RoleRequirement::Admin => role.is_admin(),
            RoleRequirement::Operator => role.is_operator(),
            RoleRequirement::Viewer => true,
        }
    }

    pub fn satisfied_by_any(&self, roles: &[Role]) -> bool {
        roles.iter().any(|r| self.satisfied_by(*r))
    }
}

/// JWT claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub exp: i64,
    pub iat: i64,
    pub jti: String,
    pub preferred_username: String,
    pub user_type: UserType,
    pub workspace_ids: Vec<String>,
    pub permissions: Vec<Permission>,
    pub roles: Vec<Role>,
}

impl Claims {
    /// Extract the user UUID from `sub`.
    pub fn user_id(&self) -> crate::SFResult<Uuid> {
        Uuid::parse_str(&self.sub)
            .map_err(|e| crate::SFError::Validation(format!("invalid user id: {e}")))
    }
}

/// Session information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub user_id: Uuid,
    pub workspace_id: Option<String>,
    pub login_method: String,
    pub login_ip: String,
    pub login_at: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
    pub device_info: Option<String>,
}

/// Authentication method type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(rename_all = "snake_case")]
pub enum AuthType {
    Phone,
    Wechat,
    EnterpriseWechat,
    Ldap,
    Email,
}

// ---------------------------------------------------------------------------
// Auth traits — defined in core so assembly layer can inject implementations.
// ---------------------------------------------------------------------------

/// Token pair returned by [`AuthProvider::generate_token`].
#[derive(Debug, Clone)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
}

/// Core authentication contract.
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    /// Generate an access token and a refresh token for `user`.
    async fn generate_token(
        &self,
        user: &User,
        workspace_ids: Vec<String>,
        permissions: Vec<Permission>,
    ) -> crate::SFResult<TokenPair>;

    /// Verify an access or refresh token and return its claims.
    async fn verify_token(&self, token: &str) -> crate::SFResult<Claims>;

    /// Refresh an access token using a valid refresh token.
    async fn refresh_access_token(&self, refresh_token: &str) -> crate::SFResult<String>;
}

/// Session management contract.
#[async_trait::async_trait]
pub trait SessionManager: Send + Sync {
    /// Create a new session and return its session ID.
    async fn create(&self, session: SessionInfo) -> crate::SFResult<uuid::Uuid>;

    /// Retrieve a session by user ID and session ID.
    async fn get(
        &self,
        user_id: uuid::Uuid,
        session_id: uuid::Uuid,
    ) -> crate::SFResult<Option<SessionInfo>>;

    /// Destroy a session.
    async fn destroy(&self, user_id: uuid::Uuid, session_id: uuid::Uuid) -> crate::SFResult<()>;

    /// Refresh the TTL of an existing session and update `last_active`.
    async fn refresh(&self, user_id: uuid::Uuid, session_id: uuid::Uuid) -> crate::SFResult<()>;

    /// Destroy all sessions for a user.
    async fn destroy_all(&self, user_id: uuid::Uuid) -> crate::SFResult<()>;
}

/// User profile update payload.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct UserUpdate {
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
}

/// User storage contract.
#[async_trait::async_trait]
pub trait UserStore: Send + Sync {
    /// Create a new user.
    async fn create(
        &self,
        username: String,
        password_hash: String,
        email: Option<String>,
        phone: Option<String>,
        display_name: Option<String>,
    ) -> crate::SFResult<User>;

    /// Find a user by username.
    async fn find_by_username(&self, username: &str) -> crate::SFResult<Option<User>>;

    /// Find a user by username, returning the user and password hash if found.
    async fn find_by_username_with_password(
        &self,
        username: &str,
    ) -> crate::SFResult<Option<(User, String)>>;

    /// Find a user by email.
    async fn find_by_email(&self, email: &str) -> crate::SFResult<Option<User>>;

    /// Find a user by ID.
    async fn find_by_id(&self, id: uuid::Uuid) -> crate::SFResult<Option<User>>;

    /// Update a user's profile.
    async fn update(&self, id: uuid::Uuid, updates: UserUpdate) -> crate::SFResult<Option<User>>;
}
