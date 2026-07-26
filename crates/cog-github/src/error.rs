//! Error types for `cog-github`.

use thiserror::Error;

/// Errors returned by `cog-github`.
#[derive(Error, Debug)]
pub enum CogGitHubError {
    /// No GitHub token could be resolved for the account.
    #[error("github account has no token: account={0}")]
    MissingToken(String),

    /// An expected environment variable is not set.
    #[error("environment variable not set: {0}")]
    MissingEnvVar(String),

    /// An invalid account kind was encountered.
    #[error("invalid account kind: {0}")]
    InvalidAccountKind(String),

    /// The GitHub integration configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// The GitHub API provider returned an error.
    #[error("provider error: {0}")]
    Provider(String),

    /// An HTTP request failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// A serialization/deserialization operation failed.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type alias for `cog-github`.
pub type Result<T> = std::result::Result<T, CogGitHubError>;
