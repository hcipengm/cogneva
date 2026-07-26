use chrono::{DateTime, Utc};
use serde::Serialize;

/// Statistics for the Supervisor's autonomous queues.
#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub pending_handoffs: usize,
    pub retry_count: usize,
    pub dlq_count: usize,
    pub cycle: u64,
    pub timestamp: DateTime<Utc>,
}

/// Current state of the scheduler gate.
#[derive(Debug, Clone, Serialize)]
pub struct GateStatus {
    pub paused: bool,
    pub timestamp: DateTime<Utc>,
}

/// Last known control plane report status.
#[derive(Debug, Clone, Serialize)]
pub struct ControlPlaneStatus {
    pub enabled: bool,
    pub last_report: Option<DateTime<Utc>>,
    pub last_success: Option<bool>,
    pub endpoint: Option<String>,
}
