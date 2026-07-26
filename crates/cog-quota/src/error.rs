/// Quota module error type.
#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("Quota exceeded: {resource}")]
    Exceeded { resource: String },

    #[error("Insufficient quota: required {required}, remaining {remaining}")]
    InsufficientQuota { required: u64, remaining: u64 },

    #[error("Invalid model: {0}")]
    InvalidModel(String),

    #[error("Invalid task type: {0}")]
    InvalidTaskType(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Redis error: {0}")]
    RedisError(String),

    #[error("Billing record error: {0}")]
    BillingError(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type QuotaResult<T> = Result<T, QuotaError>;

impl From<sqlx::Error> for QuotaError {
    fn from(e: sqlx::Error) -> Self {
        QuotaError::DatabaseError(e.to_string())
    }
}

impl From<redis::RedisError> for QuotaError {
    fn from(e: redis::RedisError) -> Self {
        QuotaError::RedisError(e.to_string())
    }
}
