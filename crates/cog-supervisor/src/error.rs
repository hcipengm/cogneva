use thiserror::Error;

/// Errors raised by Supervisor components.
#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("state backend error: {0}")]
    StateBackend(String),

    #[error("orchestrator error: {0}")]
    Orchestrator(String),

    #[error("quota error: {0}")]
    Quota(String),

    #[error("observability error: {0}")]
    Observability(String),

    #[error("registry error: {0}")]
    Registry(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type SupervisorResult<T> = Result<T, SupervisorError>;

impl From<cog_core::SFError> for SupervisorError {
    fn from(err: cog_core::SFError) -> Self {
        SupervisorError::StateBackend(err.to_string())
    }
}
