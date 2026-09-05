//! `cog-github` — GitHub integration for Cogneva self-evolution.
//!
//! This crate implements the autonomous GitHub sensor loop:
//!
//! ```text
//! IssueDiscovery → IssueTriage → IssueConversation → Task
//!   → (Collaboration generates change) → PrPublisher → MergeDecider
//!   → OutcomeRecorder → ReflectionEngine
//! ```
//!
//! Configuration contract types (`GitHubIntegrationConfig`, `GitHubAccount`,
//! …) live in `cog-core` so the central `cogneva.json` can carry the
//! `github_integration` section; they are re-exported here for convenience.

#![deny(missing_docs)]

pub mod config;
pub mod conversation;
pub mod cross_validation;
pub mod discovery;
pub mod discovery_loop;
pub mod error;
pub mod identity;
pub mod merge_decider;
pub mod outcome_recorder;
pub mod plugin;
pub mod pr_publisher;
pub mod provider;
pub mod triage;
pub mod webhook;

pub use error::{CogGitHubError, Result};
pub use webhook::{run_webhook_server, verify_signature, webhook_router, WebhookState};

pub use conversation::{ConversationState, ConversationTurn, IssueConversation};
pub use discovery::IssueDiscovery;
pub use discovery_loop::GitHubDiscoveryLoop;
pub use identity::{machine_fingerprint, InstanceIdentity, NAME_POOL};
pub use merge_decider::{MergeDecider, MergeDecision};
pub use outcome_recorder::OutcomeRecorder;
pub use pr_publisher::{GitHubChangeSink, GitHubPrPublisher};
pub use provider::gitee::GiteeProvider;
pub use provider::github::GitHubProvider;
pub use provider::{
    CiFailureEvent, CiJobLog, CodePlatformProvider, CreatePullRequest, PlatformComment,
    PlatformIssue, PlatformPullRequest, PullRequestDetail,
};
pub use triage::{IssueTriage, TriageDecision};

use tracing::debug;

/// Build a default GitHub provider from the integration config.
///
/// When `config.api_base` points at the security gateway passthrough the
/// provider is built token-free (the gateway injects credentials on egress);
/// otherwise the token is resolved from the primary account's environment
/// variable or inline field and held only in the calling process's memory.
pub fn default_provider(config: &GitHubIntegrationConfig) -> Result<Box<dyn CodePlatformProvider>> {
    let account = config
        .primary_account()
        .map_err(|e| CogGitHubError::InvalidConfig(e.to_string()))?;
    debug!(
        "initializing GitHub provider for account={} kind={} via_gateway={}",
        account.username(),
        if account.is_bot() { "bot" } else { "human" },
        config.api_base.is_some(),
    );
    Ok(Box::new(GitHubProvider::new(
        account,
        &config.repo,
        config.api_base.as_deref(),
    )?))
}

/// Build a Gitee provider from the gitee_integration config：网关模式
/// （api_base 已设）零 token；直连模式才解析 token_env。
pub fn gitee_provider(
    config: &config::GiteeIntegrationConfig,
) -> Result<Box<dyn CodePlatformProvider>> {
    let token = match config.api_base {
        Some(_) => None,
        None => config
            .token_env
            .as_deref()
            .and_then(|env| std::env::var(env).ok())
            .filter(|s| !s.is_empty()),
    };
    Ok(Box::new(GiteeProvider::new(
        &config.repo,
        config.api_base.as_deref(),
        token,
    )?))
}

pub use config::{
    AutoMergePolicy, BotAccount, BotIdentityConfig, ConversationConfig, GitHubAccount,
    GitHubIntegrationConfig, GiteeIntegrationConfig, HumanAccount, WebhookConfig,
};
