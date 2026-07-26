/// 统一错误类型。参考 pi-ai 的契约：所有错误编码在流中，不直接抛出。
#[derive(Debug, thiserror::Error)]
pub enum SFError {
    #[error("LLM provider error: {0}")]
    LLM(String),

    #[error("Agent execution error: {0}")]
    Agent(String),

    #[error("Dag-executor error: {0}")]
    DagExecutor(String),

    #[error("Adapter error ({provider}): {message}")]
    Adapter { provider: String, message: String },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("IO error: {0}")]
    IO(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Redis error: {0}")]
    Redis(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Task execution failed: {task_id} - {reason}")]
    TaskFailed { task_id: String, reason: String },

    #[error("Backpressure: channel capacity exceeded")]
    Backpressure,

    #[error("Timeout")]
    Timeout,

    #[error("Aborted")]
    Aborted,

    #[error("Auth error: {0}")]
    Auth(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type SFResult<T> = Result<T, SFError>;

impl From<std::io::Error> for SFError {
    fn from(e: std::io::Error) -> Self {
        SFError::IO(e.to_string())
    }
}
