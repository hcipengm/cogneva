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

/// The board field under which one role's resume point is recorded.
///
/// The writer is the process that holds the task (it writes checkpoints on a
/// cadence) and the reader is the dispatcher that builds the task's agents, so
/// the spelling cannot live in either crate: two spellings of a field name do
/// not fail, they leave the reader looking at a board that never carries a
/// pointer, and the whole chain reads as "nothing was ever resumable".
///
/// The field is keyed by role because a task's progress is per agent — each
/// role's context is its own — and the role is the part of the agent id
/// (`{task_id}-{role}`) that says which one a checkpoint belongs to.
pub const CHECKPOINT_FIELD_PREFIX: &str = "checkpoint:";

/// The board field holding `role`'s resume point for a task.
pub fn checkpoint_field(role: &str) -> String {
    format!("{CHECKPOINT_FIELD_PREFIX}{role}")
}

/// The name a squad running `task_id` is known by.
pub const SQUAD_ID_PREFIX: &str = "squad:";

/// The squad name for a task.
pub fn squad_id_for(task_id: &str) -> String {
    format!("{SQUAD_ID_PREFIX}{task_id}")
}

/// The name of the agent that plays `role` for `task_id`.
///
/// Task and agent are related by this name alone — there is no mapping table —
/// and the two ends that depend on it live in different crates: the task holder
/// derives it to decide which agents are its own, and the dispatcher derives it
/// to build those agents. Two spellings do not fail; they leave each end looking
/// at a set of agents the other never had, which reads exactly like a task that
/// had nothing to resume from.
pub fn agent_id_for(task_id: &str, role: &str) -> String {
    format!("{}-{role}", squad_id_for(task_id))
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
