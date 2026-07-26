use serde::Serialize;

/// Single heartbeat history entry.
#[derive(Debug, Clone, Serialize)]
pub struct HeartbeatHistoryEntry {
    pub agent_id: String,
    pub timestamp: String,
    pub status: String,
    pub load_score: f32,
    pub task_count: u32,
}
