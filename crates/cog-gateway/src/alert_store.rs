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
    /// When the condition started. History: it never moves.
    pub timestamp: String,
    /// When the condition was last seen still true, when the source maintains
    /// such a clock. `timestamp` alone cannot separate an alert someone is
    /// actively re-confirming from a row whose producer died — both show the
    /// same start time — so the sighting is reported next to the edge rather
    /// than merged into it. `None` means the source has no such reading, not
    /// that the condition is unconfirmed.
    pub last_seen_at: Option<String>,
    pub resolved: bool,
    /// Which half of the alert surface this came from. The two halves differ in
    /// ways a reader has to know about: `supervisor` entries are the
    /// process-local health events (emptied by a restart), `durable` entries are
    /// rows in the persistent state machine. One list without this field would
    /// make the two kinds of evidence indistinguishable.
    pub source: &'static str,
}
