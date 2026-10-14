//! Cogneva Quota Management
//! Provides token quota pre-check, real-time consumption tracking,
//! multi-model billing, and billing record management.
//! ## Features
//! - 5-level hierarchy (`user → workspace → team → organization → global`)
//!   with per-scope soft (warning) and hard (block) limits — see
//!   [`hierarchy::HierarchyManager`].
//! - Per-day usage history with TTL-based daily resets at UTC midnight.
//! - Two-phase quota consumption (`pre_deduct` → `consume` / `refund`).
//! - Prometheus metrics via [`metrics::QuotaMetrics`].
//!
//! In-flight enforcement has one implementation and it is not here: the axum
//! middleware lives in `cog_gateway::quota_middleware`, and it reaches this
//! crate's manager through [`cog_core::QuotaManager`] rather than by depending
//! on this crate. A second copy used to live here, concrete-typed
//! (`Arc<QuotaManager>`), which is why no contract-based consumer could call
//! it: it was reachable only from something that already depended on this
//! crate, and nothing does.

pub mod agent_budget;
pub mod billing;
pub mod budget_manager;
pub mod error;
pub mod hierarchy;
pub mod metrics;
pub mod model;
pub mod quota;
mod store;

pub use agent_budget::{AgentBudget, BudgetAction, BudgetResult, BudgetTemplates};
pub use billing::{BillingRecord, BillingRepository, RechargeRecord};
pub use budget_manager::ContextBudgetManager;
pub use error::{QuotaError, QuotaResult};
pub use hierarchy::HierarchyManager;
pub use metrics::QuotaMetrics;
pub use model::{ModelConfig, ModelRegistry, TaskType};
pub use quota::{DailyUsage, QuotaChecker, QuotaManager};

// Re-export core DTOs so consumers see them under cog_quota::* as before.
use cog_core::{
    HierarchyDecision, PreCheckResult, QuotaContext, QuotaLimits, QuotaScope, QuotaSummary,
    ScopeStatus, UsageHistoryEntry,
};

pub mod plugin;
