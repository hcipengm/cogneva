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
    pub last_rebalance: Option<DateTime<Utc>>,
    pub timestamp: DateTime<Utc>,
}
