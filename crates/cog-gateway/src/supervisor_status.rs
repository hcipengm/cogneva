use chrono::{DateTime, Utc};
use serde::Serialize;

/// Snapshot of Supervisor runtime state for Dashboard consumption.
#[derive(Debug, Clone, Serialize)]
pub struct SupervisorStatusSnapshot {
    pub cycle: u64,
    pub healthy_agents: usize,
    pub dead_agents: usize,
    pub suspect_agents: usize,
    pub stuck_agents: usize,
    pub pending_handoffs: usize,
    pub scheduler_paused: bool,
    /// When the supervisor last emitted a rebalance plan, or `None` before any.
    ///
    /// Read off the supervisor's event channel: the timestamp of the last
    /// [`cog_core::SupervisorEvent::Rebalance`]. That event has two producers —
    /// the rebalance pass, when the plan it built is non-empty, and the health
    /// pass, when a checkpoint recovery for dead agents produced one.
    ///
    /// This is not the control plane's `SupervisorStatus.last_rebalance`, which
    /// reads a mutex set on every successful rebalance pass (each
    /// `rebalance_interval`, 300s). The two answer different questions: this
    /// one "when did a plan last go out", that one "when did the pass last run
    /// clean". Both names are right for their own fact; they are deliberately
    /// not the same name here.
    pub last_plan_emitted_at: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
}
