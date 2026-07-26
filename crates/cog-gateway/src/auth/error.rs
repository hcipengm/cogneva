use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Authentication and authorization errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Token expired")]
    TokenExpired,

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Hash error: {0}")]
    HashError(String),

    #[error("Missing authorization header")]
    MissingAuthHeader,

    #[error("Invalid authorization format")]
    InvalidAuthFormat,

    #[error("Token generation failed: {0}")]
    TokenGenerationFailed(String),

    #[error("Redis error: {0}")]
    RedisError(String),

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("User already exists: {0}")]
    UserAlreadyExists(String),

    #[error("Account disabled")]
    AccountDisabled,

    #[error("Account locked")]
    AccountLocked,

    #[error("Internal error: {0}")]
    Internal(String),
}

impl AuthError {
    /// HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            AuthError::InvalidToken(_) => StatusCode::UNAUTHORIZED,
            AuthError::TokenExpired => StatusCode::UNAUTHORIZED,
            AuthError::SessionNotFound(_) => StatusCode::UNAUTHORIZED,
            AuthError::UserNotFound(_) => StatusCode::NOT_FOUND,
            AuthError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            AuthError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::HashError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::MissingAuthHeader => StatusCode::UNAUTHORIZED,
            AuthError::InvalidAuthFormat => StatusCode::UNAUTHORIZED,
            AuthError::TokenGenerationFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::RedisError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthError::UserAlreadyExists(_) => StatusCode::CONFLICT,
            AuthError::AccountDisabled => StatusCode::FORBIDDEN,
            AuthError::AccountLocked => StatusCode::FORBIDDEN,
            AuthError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Machine-readable error code.
    pub fn error_code(&self) -> &'static str {
        match self {
            AuthError::InvalidToken(_) => "invalid_token",
            AuthError::TokenExpired => "token_expired",
            AuthError::SessionNotFound(_) => "session_not_found",
            AuthError::UserNotFound(_) => "user_not_found",
            AuthError::PermissionDenied(_) => "permission_denied",
            AuthError::DatabaseError(_) => "database_error",
            AuthError::HashError(_) => "hash_error",
            AuthError::MissingAuthHeader => "missing_auth_header",
            AuthError::InvalidAuthFormat => "invalid_auth_format",
            AuthError::TokenGenerationFailed(_) => "token_generation_failed",
            AuthError::RedisError(_) => "redis_error",
            AuthError::InvalidCredentials => "invalid_credentials",
            AuthError::UserAlreadyExists(_) => "user_already_exists",
            AuthError::AccountDisabled => "account_disabled",
            AuthError::AccountLocked => "account_locked",
            AuthError::Internal(_) => "internal_error",
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = Json(json!({
            "error": self.error_code(),
            "message": self.to_string(),
        }));
        (status, body).into_response()
    }
}

impl From<redis::RedisError> for AuthError {
    fn from(e: redis::RedisError) -> Self {
        AuthError::RedisError(e.to_string())
    }
}

impl From<cog_core::SFError> for AuthError {
    fn from(e: cog_core::SFError) -> Self {
        match e {
            cog_core::SFError::Auth(msg) => AuthError::InvalidToken(msg),
            other => AuthError::Internal(other.to_string()),
        }
    }
}

pub type AuthResult<T> = Result<T, AuthError>;
