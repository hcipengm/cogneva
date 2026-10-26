//! Cogneva Quota Management
//! Provides token quota pre-check, real-time consumption tracking,
//! multi-model billing, and billing record management.
//! ## Features
//! - 5-level hierarchy (`user → workspace → team → organization → global`)
//!   with per-scope soft (warning) and hard (block) limits — see
//!   [`hierarchy::HierarchyManager`].
//! - Per-day usage history with TTL-based daily resets at UTC midnight.
//! - Two-phase quota consumption (`pre_deduct` → `consume` / `refund`).
//! - The two facts a refused caller needs from that reset: the UTC day boundary
//!   as a pure function ([`day`]) and the named refusal with its wait
//!   ([`hierarchy::refusal_for`]), so a request-facing caller can answer with
//!   `Retry-After` without re-deriving either.
//! - Prometheus families via [`metrics::QuotaMetrics`]: per-account, and so
//!   per-account keys are unbounded — which is why no periodic scrape carries
//!   them and nothing serves them today.
//! - The decisions themselves, at the process metrics face:
//!   [`observable::QuotaObservable`] publishes how many checks ran and how many
//!   were refused.
//!
//! Nothing charges this manager today, and that is deliberate. The one caller
//! was an axum middleware on the gateway's four management route groups, which
//! charged each request by the length of its body: those calls spend no model
//! tokens, so the figure it deducted had nothing to do with what the platform
//! spends for that user, and once the balance reached zero it refused the
//! request that reads the balance. It is gone. What a user's work actually
//! spends is in the gateway's LLM ledger, per call, and attributing that to a
//! user needs the task → owner join this crate does not have — so the
//! pre-check surface stays present and inert until that attribution lands,
//! rather than enforcing a number nobody measured.

pub mod agent_budget;
pub mod billing;
pub mod budget_manager;
pub mod day;
pub mod error;
pub mod hierarchy;
pub mod metrics;
pub mod model;
pub mod observable;
pub mod quota;
mod store;

pub use agent_budget::{AgentBudget, BudgetAction, BudgetResult};
pub use billing::{BillingRecord, BillingRepository, RechargeRecord};
pub use budget_manager::ContextBudgetManager;
pub use day::{day_start_unix, seconds_to_next_day};
pub use error::{QuotaError, QuotaResult};
pub use hierarchy::{refusal_for, DailyCapRefusal, HierarchyManager};
pub use metrics::QuotaMetrics;
pub use model::{ModelConfig, ModelRegistry, TaskType};
pub use observable::{QuotaObservable, QuotaReadings};
pub use quota::{DailyUsage, QuotaChecker, QuotaManager};

// Re-export core DTOs so consumers see them under cog_quota::* as before.
use cog_core::{
    HierarchyDecision, PreCheckResult, QuotaContext, QuotaLimits, QuotaScope, QuotaSummary,
    ScopeStatus, UsageHistoryEntry,
};

pub mod plugin;
