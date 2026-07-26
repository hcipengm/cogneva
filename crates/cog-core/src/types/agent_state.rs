use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Agent lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Init,
    Registered,
    Active,
    Idle,
    Completing,
    Inactive,
    Suspect,
    Dead,
}

/// Checkpoint for task recovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskCheckpoint {
    pub task_id: String,
    pub snapshot_id: String,
    pub event_offset: u64,
    pub timestamp: DateTime<Utc>,
}

/// Context board for shared task state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ContextBoard {
    pub task_id: String,
    pub fields: HashMap<String, String>,
    pub updated_at: DateTime<Utc>,
}

/// Event for event sourcing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub offset: u64,
    pub task_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}
