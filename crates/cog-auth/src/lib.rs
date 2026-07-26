//! Cogneva Authentication & Authorization
//! Provides JWT-based authentication, session management (Redis),
//! RBAC permission control, and Keycloak OIDC integration.

pub mod error;
pub mod jwt;
pub mod keycloak;
pub mod middleware;
pub mod password;
pub mod rbac;
pub mod session;
pub mod user;

pub use error::AuthError;
pub use jwt::JwtManager;
pub use keycloak::{
    resolve_roles, ClientAccess, KeycloakAuthProvider, KeycloakAuthResult, KeycloakClient,
    KeycloakConfig, KeycloakTokenResponse, KeycloakUserInfo, RealmAccess, TokenIntrospection,
};
pub use middleware::{auth_middleware, require_permission, require_role};
pub use password::{hash_password, verify_password};
pub use rbac::RoleChecker;
pub use session::SessionManager;
pub use user::{UserAuthMethod, UserRepository};

pub mod plugin;

// Re-export auth types from cog-core so consumers see them under cog_auth::* as before.
