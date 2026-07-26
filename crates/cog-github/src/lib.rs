//! `cog-github` — GitHub integration for Cogneva self-evolution.
//!
//! This crate implements the autonomous GitHub sensor loop described in
//! `docs/2026-06-28_github_issue_to_pr_integration_design.md`:
//!
//! ```text
//! IssueDiscovery → IssueTriage → IssueConversation → Task
//!   → (Collaboration generates patch) → PrPublisher → MergeDecider
//!   → OutcomeRecorder → ReflectionEngine
//! ```
//!
//! Configuration contract types (`GitHubIntegrationConfig`, `GitHubAccount`,
//! …) live in `cog-core` so the central `cogneva.json` can carry the
//! `github_integration` section; they are re-exported here for convenience.

#![deny(missing_docs)]

pub mod conversation;
pub mod discovery;
pub mod discovery_loop;
pub mod error;
pub mod merge_decider;
pub mod outcome_recorder;
pub mod plugin;
pub mod pr_publisher;
pub mod provider;
pub mod triage;
pub mod webhook;

pub use cog_core::{
    AutoMergePolicy, BotAccount, BotIdentityConfig, ConversationConfig, GitHubAccount,
    GitHubIntegrationConfig, HumanAccount, WebhookConfig,
};
pub use error::{CogGitHubError, Result};
pub use webhook::{run_webhook_server, verify_signature, webhook_router, WebhookState};

pub use conversation::{ConversationState, ConversationTurn, IssueConversation};
pub use discovery::IssueDiscovery;
pub use discovery_loop::GitHubDiscoveryLoop;
pub use merge_decider::{MergeDecider, MergeDecision};
pub use outcome_recorder::OutcomeRecorder;
pub use pr_publisher::GitHubPrPublisher;
pub use provider::github::GitHubProvider;
pub use provider::{
    CiFailureEvent, CiJobLog, CodePlatformProvider, CreatePullRequest, PlatformComment,
    PlatformIssue, PlatformPullRequest, PullRequestDetail,
};
pub use triage::{IssueTriage, TriageDecision};

use tracing::debug;

/// Build a default GitHub provider from the integration config.
///
/// The token is resolved from the primary account's environment variable or
/// inline field and is held only in the calling process's memory.  It is not
/// passed to the sandbox.
pub fn default_provider(config: &GitHubIntegrationConfig) -> Result<Box<dyn CodePlatformProvider>> {
    let account = config
        .primary_account()
        .map_err(|e| CogGitHubError::InvalidConfig(e.to_string()))?;
    debug!(
        "initializing GitHub provider for account={} kind={}",
        account.username(),
        if account.is_bot() { "bot" } else { "human" }
    );
    Ok(Box::new(GitHubProvider::new(account, &config.repo)?))
}
