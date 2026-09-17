//! Error types for `cog-github`.

use cog_core::SFError;
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

    /// The platform core refused work this loop handed it, with the original
    /// error kept instead of flattened to prose. The *type* is the cause —
    /// quota, credentials, transport — and it is the only thing a retry
    /// decision is allowed to read.
    #[error("upstream failure: {0}")]
    Upstream(#[from] SFError),

    /// The GitHub API provider returned an error.
    #[error("provider error: {0}")]
    Provider(String),

    /// A change was rejected before publishing because it touches paths
    /// outside the contribution whitelist (configs, secrets, deploy
    /// manifests, business data, or anything not under the allowed
    /// generic-code paths).
    #[error("contribution rejected by privacy gate: {0}")]
    PrivacyRejected(String),

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

impl CogGitHubError {
    /// Whether re-issuing this work next tick is known, by type, to be futile:
    /// the same request cannot succeed until an external window resets or
    /// credentials change.
    ///
    /// Only [`Self::Upstream`] can answer yes. A [`Self::Provider`] error
    /// carries prose and nothing else, so nothing can be concluded from it —
    /// deciding otherwise would mean matching on an upstream's wording, which
    /// differs per provider and per locale and silently stops matching the day
    /// one of them rewords.
    pub fn is_terminal_upstream_failure(&self) -> bool {
        match self {
            Self::Upstream(e) => e.is_terminal_upstream_failure(),
            _ => false,
        }
    }
}

/// Result type alias for `cog-github`.
pub type Result<T> = std::result::Result<T, CogGitHubError>;
