use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

/// 统一 API 错误响应体。
///
/// 三个格子各管一件事：`error` 是粗粒度类别（映射到状态码），`code` 是稳定
/// 标识（客户端按它分支），`message` 是给人看的解释。把 `code` 单独拎出来，
/// 是因为 `message` 会随着写它的人换措辞——客户端的判词只该长在 `code` 上。
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
    pub code: String,
}

impl ApiError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: "NotFound".into(),
            message: message.into(),
            code: "not_found".into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: "BadRequest".into(),
            message: message.into(),
            code: "bad_request".into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            error: "InternalServerError".into(),
            message: message.into(),
            code: "internal".into(),
        }
    }

    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            error: "TooManyRequests".into(),
            message: message.into(),
            code: "too_many_requests".into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            error: "Forbidden".into(),
            message: message.into(),
            code: "forbidden".into(),
        }
    }

    /// 这个部署现在没有能力回答，重试也不会变——缺的是一份配置或一条连接。
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            error: "ServiceUnavailable".into(),
            message: message.into(),
            code: "service_unavailable".into(),
        }
    }

    /// 一次比较交换输给了别的写者：请求本身没错，是它脚下的状态变了。
    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            error: "Conflict".into(),
            message: message.into(),
            code: "conflict".into(),
        }
    }

    /// A submitted row names an owner the face it serves does not have.
    ///
    /// Its own error name rather than plain `Conflict`: the caller has to
    /// change the unit or the face, not retry the same request after the
    /// other writer settles.
    pub fn owner_mismatch(message: impl Into<String>) -> Self {
        Self {
            error: "owner_mismatch".into(),
            message: message.into(),
            code: "owner_mismatch".into(),
        }
    }

    /// 把状态码级别的 `code` 换成一个更细的稳定标识。
    ///
    /// 两个都是 400 的拒绝——「父节点在别的项目里」和「依赖不存在」——对写
    /// 客户端的人来说是完全不同的事：一个要换父节点，一个要补依赖。状态码
    /// 说不出这个区别，`code` 可以。
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = code.into();
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.error.as_str() {
            "NotFound" => StatusCode::NOT_FOUND,
            "BadRequest" => StatusCode::BAD_REQUEST,
            "Forbidden" => StatusCode::FORBIDDEN,
            // 一轮 CAS 输给了别的写者：状态已经变了，重读再决定，不是这个请求本身错了。
            "Conflict" => StatusCode::CONFLICT,
            "owner_mismatch" => StatusCode::CONFLICT,
            "TooManyRequests" => StatusCode::TOO_MANY_REQUESTS,
            "ServiceUnavailable" => StatusCode::SERVICE_UNAVAILABLE,
            // 平台在等下游，不是自己错了：504 让客户端知道这个请求可能值得
            // 重试，500 只会让它以为是自己发错了。
            "GatewayTimeout" => StatusCode::GATEWAY_TIMEOUT,
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
                code: "backpressure".into(),
            },
            cog_core::SFError::Timeout => ApiError {
                error: "GatewayTimeout".into(),
                message: "operation timed out".into(),
                code: "timeout".into(),
            },
            cog_core::SFError::Aborted => ApiError {
                error: "Conflict".into(),
                message: "operation aborted".into(),
                code: "aborted".into(),
            },
            cog_core::SFError::Conflict(msg) => ApiError {
                error: "Conflict".into(),
                message: msg,
                code: "conflict".into(),
            },
            other => ApiError::internal(other.to_string()),
        }
    }
}
