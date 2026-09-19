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
    /// Which half of the alert surface this came from. The two halves differ in
    /// ways a reader has to know about: `supervisor` entries are the
    /// process-local health events (emptied by a restart), `durable` entries are
    /// rows in the persistent state machine. One list without this field would
    /// make the two kinds of evidence indistinguishable.
    pub source: &'static str,
}
