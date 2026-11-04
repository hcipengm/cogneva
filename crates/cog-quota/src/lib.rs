//! Cogneva Quota Management
//! Provides token quota pre-check, real-time consumption tracking,
//! multi-model billing, and billing record management.
//! ## Features
//! - 5-level hierarchy (`user → workspace → team → organization → global`)
//!   with per-scope soft (warning) and hard (block) limits — see
//!   [`hierarchy::HierarchyManager`].
//! - Per-day usage history with TTL-based daily resets at UTC midnight.
//! - One writer per counter: "used today" is a **projection** of the token
//!   ledger ([`hierarchy::HierarchyManager::project_used`]), overwritten on a
//!   period rather than added to, so a lost key heals and the cell cannot drift
//!   from the ledger it is computed from.
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
//! gone. Spend is written instead where it happens — the gateway's LLM proxy
//! lands one ledger row per call, by the tokens the upstream reported — and the
//! person axis is computed from those rows: the counted cell is rewritten from
//! the ledger on a period, and a call is judged against whatever ceiling an
//! operator declared for the person whose names that ledger resolves to. The
//! cell and the ledger are one fact, so there is nothing to reconcile between
//! them. The routes that report a balance read that same cell
//! ([`quota::QuotaManager`]); they keep no balance of their own, so what a
//! customer is shown cannot drift from what enforcement allowed.
//!
//! The workspace level is projected the same way, out of the same rows: a call
//! belongs to the workspace of the task it was made for, and the task graph is
//! where a piece of work records the tenant it runs in. That projection is what
//! makes a declared workspace ceiling bite — the supervisor's pause gate reads
//! the same cell, and before anything wrote it that gate could not tell a spent
//! workspace from an untouched one. A workspace nobody declared a ceiling for
//! is never cut off by the default pair: the gate asks "is there a limit", and
//! the default is what a cell reads as, not a decision anyone made.
//!
//! The team and organization levels above it still have no writer, and that
//! stays deliberate: a task names a workspace, not a team, so a figure charged
//! against those levels would be a number nobody measured. A level with a
//! declared ceiling and no spend is inert, and reads as exactly that.

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
pub use quota::{QuotaChecker, QuotaManager};
pub use task_budget::{verdict as task_budget_verdict, TaskBudgetDecision, TaskBudgetManager};

// Re-export core DTOs so consumers see them under cog_quota::* as before.
use cog_core::{
    HierarchyDecision, PreCheckResult, QuotaContext, QuotaLimits, QuotaScope, QuotaSummary,
    ScopeStatus, UsageHistoryEntry,
};

pub mod plugin;
