use serde::Serialize;

/// Alert entry exposed via the Gateway API.
#[derive(Debug, Clone, Serialize)]
pub struct AlertEntry {
    pub id: String,
    pub severity: String,
    pub event_type: String,
    pub message: String,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub crew_id: Option<String>,
    pub timestamp: String,
    pub resolved: bool,
}
