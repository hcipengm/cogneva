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

/// 数字员工名册：人名的唯一来源。
///
/// 列表只管"好不好认"：名字的唯一性由 [`display_name_for`] 尾部那 8 位短码
/// 承担，因此名册不需要很长。**长度不能随手改**——选取是 `哈希 % 名字数`，
/// 增删一个名字会让所有员工换名，按旧名记下的归属就读不回同一个人。
pub const NAME_POOL: &[&str] = &[
    "Alice", "Ralph", "Nova", "Kai", "Vera", "Orion", "Sage", "Echo", "Iris", "Leo", "Mira",
    "Quinn", "Ada", "Felix", "Luna", "Max",
];

/// 任意 agent 的展示名，形如 `Nova#3f2a91c7`。
///
/// 路由用的 id 必须由 (task, role) 推出来（[`agent_id_for`]）——两端各算一次
/// 还得对上，所以它不能带人名。展示名反过来由 id 推出：纯函数、同一个 id 永远
/// 是同一个人、不同 id 撞名概率是 2^-32 量级。这样"谁在干这件事"在名册、
/// 日志、提交里都读得成一个名字，而路由那一侧一个字节都不用动。
pub fn display_name_for(agent_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(agent_id.as_bytes());
    let selector = u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8]));
    let persona = NAME_POOL[(selector as usize) % NAME_POOL.len()];
    let short = digest[8..12]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    format!("{persona}#{short}")
}

#[cfg(test)]
mod naming_tests {
    use super::*;

    /// 一个 id 永远对应同一个名字：名字是读出来的一列，不是存下来的状态，
    /// 重算一次就得对上，否则名册里同一个人会有两个名字。
    #[test]
    fn a_display_name_is_a_function_of_the_id() {
        let id = agent_id_for("task-1", "planner");
        assert_eq!(display_name_for(&id), display_name_for(&id));
        assert_ne!(
            display_name_for(&id),
            display_name_for(&agent_id_for("task-2", "planner"))
        );
    }

    /// 名册里的每个单元都要有自己的名字：撞名等于读的人分不出是谁在干。
    #[test]
    fn distinct_agents_get_distinct_names() {
        let mut names = std::collections::HashSet::new();
        for t in 0..200 {
            for role in ["planner", "generator", "evaluator"] {
                let id = agent_id_for(&format!("task-{t}"), role);
                assert!(
                    names.insert(display_name_for(&id)),
                    "two agents share a name: {id}"
                );
            }
        }
    }

    #[test]
    fn the_display_name_reads_as_a_person() {
        let name = display_name_for(&agent_id_for("task-1", "planner"));
        let (persona, short) = name.split_once('#').expect("handle carries a #");
        assert!(
            NAME_POOL.contains(&persona),
            "persona {persona} not in the pool"
        );
        assert_eq!(short.len(), 8);
        assert!(short.chars().all(|c| c.is_ascii_hexdigit()));
    }
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
