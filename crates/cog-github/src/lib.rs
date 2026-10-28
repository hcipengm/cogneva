//! `cog-github` — GitHub integration for Cogneva self-evolution.
//!
//! This crate implements the autonomous GitHub sensor loop:
//!
//! ```text
//! IssueDiscovery → IssueTriage → IssueConversation → Task
//!   → (Collaboration generates change) → OutcomeRecorder → ReflectionEngine
//! ```
//!
//! Configuration contract types (`GitHubIntegrationConfig`, `GitHubAccount`,
//! …) live in `cog-core` so the central `cogneva.json` can carry the
//! `github_integration` section; they are re-exported here for convenience.

#![deny(missing_docs)]

/// 读数按「哪一类」归属时用的标签键。
pub const LABEL_CATEGORY: &str = "category";

/// 读数按「什么意图」归属时用的标签键。
pub const LABEL_INTENT: &str = "intent";

/// 读数按「这条需求走到哪儿了」归属时用的标签键。
pub const LABEL_FATE: &str = "fate";

/// 读数按「漏斗的哪一段」归属时用的标签键。
pub const LABEL_STAGE: &str = "stage";

/// 读数带上「为什么是这个结局」时用的标签键。
pub const LABEL_REASON: &str = "reason";

pub mod change_funnel;
pub mod ci_refusals;
pub mod config;
pub mod contribution;
pub mod conversation;
pub mod cross_validation;
pub mod discovery;
pub mod discovery_loop;
pub mod error;
pub mod identity;
pub mod landing;
pub mod outcome_recorder;
pub mod pending_changes;
pub mod plugin;
pub mod provider;
pub mod redrive_budget;
pub mod triage;
pub mod webhook;

/// The word this crate calls GitHub by, in every place the word is a value.
///
/// It is a routing key: the provider reports it as its platform kind, the
/// landing targets carry it as the remote and the slug, the gateway puts it in
/// a URL path and in the stored contribution config. Nothing compares those
/// spellings, and a changed copy does not fail loudly -- it selects a platform
/// nobody else names, which reads as "this provider was never asked".
///
/// The gateway keeps its own copy under the same identifier (the two crates do
/// not depend on each other); a gate compares the spellings.
pub const GITHUB_PLATFORM: &str = "github";

/// The word this crate calls Gitee by. See [`GITHUB_PLATFORM`].
pub const GITEE_PLATFORM: &str = "gitee";

pub use error::{CogGitHubError, Result};
pub use webhook::{run_webhook_server, verify_signature, webhook_router, WebhookState};

pub use contribution::ContributionController;
pub use conversation::{ConversationState, ConversationTurn, IssueConversation};
pub use discovery::IssueDiscovery;
pub use discovery_loop::GitHubDiscoveryLoop;
pub use identity::{machine_fingerprint, InstanceIdentity, NAME_POOL};
pub use landing::{LandingRecord, LandingState, MainChannel};
pub use outcome_recorder::OutcomeRecorder;
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
pub fn default_provider(
    config: &GitHubIntegrationConfig,
    http: std::sync::Arc<dyn cog_core::HttpClient>,
) -> Result<Box<dyn CodePlatformProvider>> {
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
        &config.base_branch,
        config.api_base.as_deref(),
        http,
    )?))
}

/// Build a Gitee provider from the gitee_integration config：网关模式
/// （api_base 已设）零 token；直连模式才解析 token_env。
pub fn gitee_provider(
    config: &config::GiteeIntegrationConfig,
    http: std::sync::Arc<dyn cog_core::HttpClient>,
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
        http,
    )?))
}

pub use config::{
    BotAccount, BotIdentityConfig, ConversationConfig, GitHubAccount, GitHubIntegrationConfig,
    GiteeIntegrationConfig, HumanAccount, LandingPolicy, WebhookConfig,
};

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;
