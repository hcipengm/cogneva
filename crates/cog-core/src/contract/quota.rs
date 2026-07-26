//!Quota and hierarchy management types.

use crate::SFResult;
use serde::{Deserialize, Serialize};

/// Scope levels for the 5-level quota hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaScope {
    User,
    Workspace,
    Team,
    Organization,
    Global,
}

impl QuotaScope {
    pub fn cascade_order() -> [QuotaScope; 5] {
        [
            QuotaScope::User,
            QuotaScope::Workspace,
            QuotaScope::Team,
            QuotaScope::Organization,
            QuotaScope::Global,
        ]
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            QuotaScope::User => "user",
            QuotaScope::Workspace => "ws",
            QuotaScope::Team => "team",
            QuotaScope::Organization => "org",
            QuotaScope::Global => "global",
        }
    }
}

/// Soft / hard limit pair for a single scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaLimits {
    pub soft_limit: u64,
    pub hard_limit: u64,
}

impl QuotaLimits {
    pub fn new(soft_limit: u64, hard_limit: u64) -> Self {
        Self {
            soft_limit: soft_limit.min(hard_limit),
            hard_limit,
        }
    }

    pub fn from_hard(hard_limit: u64, soft_ratio: f64) -> Self {
        Self {
            soft_limit: ((hard_limit as f64) * soft_ratio) as u64,
            hard_limit,
        }
    }
}

impl Default for QuotaLimits {
    fn default() -> Self {
        Self {
            soft_limit: 80_000,
            hard_limit: 100_000,
        }
    }
}

/// Status of a single scope in the hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeStatus {
    pub scope: QuotaScope,
    pub target_id: String,
    pub remaining: u64,
    pub used_today: u64,
    pub limits: QuotaLimits,
    pub blocking: bool,
    pub warning: bool,
}

/// Context for a hierarchy quota check.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaContext {
    pub user_id: Option<String>,
    pub workspace_id: Option<String>,
    pub team_id: Option<String>,
    pub organization_id: Option<String>,
    pub global_id: Option<String>,
}

impl QuotaContext {
    pub fn target(&self, scope: QuotaScope) -> Option<&str> {
        match scope {
            QuotaScope::User => self.user_id.as_deref(),
            QuotaScope::Workspace => self.workspace_id.as_deref(),
            QuotaScope::Team => self.team_id.as_deref(),
            QuotaScope::Organization => self.organization_id.as_deref(),
            QuotaScope::Global => self.global_id.as_deref(),
        }
    }
}

/// Summary of quota usage for a target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaSummary {
    pub target_id: String,
    pub target_type: String,
    pub total_quota: u64,
    pub remaining: u64,
    pub used_today: u64,
}

/// Result of a pre-check quota request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PreCheckResult {
    pub allowed: bool,
    pub remaining: u64,
    pub estimated_cost: f64,
}

/// Aggregate result of a hierarchical check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchyDecision {
    pub allowed: bool,
    pub warnings: Vec<ScopeStatus>,
    pub blocked_by: Vec<ScopeStatus>,
    pub scopes: Vec<ScopeStatus>,
}

/// Single daily usage record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageHistoryEntry {
    pub date: String,
    pub tokens_used: u64,
}

/// Token quota manager.
#[async_trait::async_trait]
pub trait QuotaManager: Send + Sync {
    async fn pre_check(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
    ) -> PreCheckResult;

    async fn finalize(
        &self,
        user_id: &str,
        workspace_id: Option<&str>,
        estimated_tokens: u64,
        actual_tokens: u64,
    ) -> SFResult<()>;

    async fn get_remaining(&self, user_id: &str) -> u64;
    async fn get_workspace_remaining(&self, workspace_id: &str) -> u64;
    async fn get_used_today(&self, user_id: &str) -> u64;
    async fn get_workspace_used_today(&self, workspace_id: &str) -> u64;
    async fn get_request_count_today(&self, user_id: &str) -> u64;
    async fn get_user_summary(&self, user_id: &str) -> QuotaSummary;
    async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary;

    async fn recharge(
        &self,
        user_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<()>;

    async fn recharge_workspace(
        &self,
        workspace_id: &str,
        tokens: u64,
        valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<()>;
}

/// 5-level quota hierarchy manager.
#[async_trait::async_trait]
pub trait HierarchyManager: Send + Sync {
    async fn check(&self, ctx: &QuotaContext, tokens: u64) -> HierarchyDecision;
    async fn consume(&self, ctx: &QuotaContext, tokens: u64) -> SFResult<HierarchyDecision>;
    async fn refund(&self, ctx: &QuotaContext, tokens: u64) -> SFResult<()>;
    async fn pre_deduct(&self, ctx: &QuotaContext, tokens: u64) -> SFResult<HierarchyDecision>;
    async fn history(
        &self,
        scope: QuotaScope,
        target_id: &str,
        days: u32,
    ) -> SFResult<Vec<UsageHistoryEntry>>;
}

/// Per-workspace quota source used by the supervisor.
#[async_trait::async_trait]
pub trait WorkspaceQuotaSource: Send + Sync {
    async fn workspace_remaining(&self, workspace_id: &str) -> u64;
}
