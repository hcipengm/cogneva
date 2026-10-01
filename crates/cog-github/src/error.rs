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

    /// Which paths a change touches could not be read from its diff at all.
    ///
    /// A separate variant from [`Self::PrivacyRejected`] on purpose. Both hold
    /// the change back before anything is pushed, but they say opposite things
    /// about it: a rejection names paths that are known and unacceptable, while
    /// this one says the paths are unknown. Only the first is a property of the
    /// change. Merging them lets a diff that merely failed to parse be read as
    /// a change that violated the whitelist, and a caller acting on that takes
    /// the change out of the queue -- the one place a re-serialised diff could
    /// still have reached the branch from.
    #[error("diff could not be read: {0}")]
    DiffUnreadable(String),

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

    /// The typed cause, when the failure crossed the DAG bus with one. `None`
    /// means only prose survived — callers must not classify from that.
    pub fn upstream_failure(&self) -> Option<cog_core::UpstreamFailure> {
        match self {
            Self::Upstream(e) => e.upstream_failure(),
            _ => None,
        }
    }
}

/// Result type alias for `cog-github`.
pub type Result<T> = std::result::Result<T, CogGitHubError>;
