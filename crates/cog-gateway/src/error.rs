use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

/// 统一 API 错误响应体。
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
}

impl ApiError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: "NotFound".into(),
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: "BadRequest".into(),
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            error: "InternalServerError".into(),
            message: message.into(),
        }
    }

    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            error: "TooManyRequests".into(),
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            error: "Forbidden".into(),
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.error.as_str() {
            "NotFound" => StatusCode::NOT_FOUND,
            "BadRequest" => StatusCode::BAD_REQUEST,
            "Forbidden" => StatusCode::FORBIDDEN,
            "TooManyRequests" => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(self)).into_response()
    }
}

impl From<cog_core::SFError> for ApiError {
    fn from(e: cog_core::SFError) -> Self {
        match e {
            cog_core::SFError::Validation(msg) => ApiError::bad_request(msg),
            cog_core::SFError::Serialization(err) => ApiError::bad_request(err.to_string()),
            cog_core::SFError::TaskFailed { task_id, reason } => {
                ApiError::bad_request(format!("task {} failed: {}", task_id, reason))
            }
            cog_core::SFError::Backpressure => ApiError {
                error: "ServiceUnavailable".into(),
                message: "backpressure: channel capacity exceeded".into(),
            },
            cog_core::SFError::Timeout => ApiError {
                error: "GatewayTimeout".into(),
                message: "operation timed out".into(),
            },
            cog_core::SFError::Aborted => ApiError {
                error: "Conflict".into(),
                message: "operation aborted".into(),
            },
            other => ApiError::internal(other.to_string()),
        }
    }
}
