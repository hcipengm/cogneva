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
//! What charges this manager is model spend, and only model spend. The one
//! caller it used to have was an axum middleware on the gateway's four
//! management route groups, which charged each request by the length of its
//! body: those calls spend no model tokens, so the figure it deducted had
//! nothing to do with what the platform spends for that user, and once the
//! balance reached zero it refused the request that reads the balance. It is
//! gone. Spend is charged instead where it happens — the gateway's LLM proxy,
//! by the tokens the upstream reported — and the person axis is the first one
//! with a writer: a call that declared an owner is charged to that person and
//! judged against whatever ceiling an operator declared for them, in the same
//! daily counters the management face reads.
//!
//! The workspace, team and organization levels above it still have no writer,
//! and that stays deliberate: nothing on the platform says which of those a
//! given call belongs to, so a figure charged against them would be a number
//! nobody measured. A level with a declared ceiling and no spend is inert, and
//! reads as exactly that.

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
pub mod task_budget;

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
pub use task_budget::{verdict as task_budget_verdict, TaskBudgetDecision, TaskBudgetManager};

// Re-export core DTOs so consumers see them under cog_quota::* as before.
use cog_core::{
    HierarchyDecision, PreCheckResult, QuotaContext, QuotaLimits, QuotaScope, QuotaSummary,
    ScopeStatus, UsageHistoryEntry,
};

pub mod plugin;
