//! The two reads whose row shapes two stores share.
//!
//! `PostgresStateBackend` is the state backend; `PostgresObservabilityGateway`
//! reads the same two tables, because what observability shows is what the state
//! backend wrote. Each spelled the statement and the row-to-struct mapping
//! itself, so "which table answers this" and "which column is which field" were
//! copies -- and neither copy could notice the other drifting.
//!
//! That drift is quiet by construction: `sqlx::query_as` maps a row to a tuple
//! *positionally*, so swapping two columns of the same type in one statement
//! (`snapshot_id` and `event_offset` are `String` and `i64`, but a reordered
//! projection of two text columns is invisible) still compiles, still answers,
//! and answers the wrong thing. The reader that reports a checkpoint reads a
//! snapshot id out of the offset column and resumes the task somewhere it never
//! stopped -- with no error to distinguish it from a checkpoint that was saved
//! that way.
//!
//! `gate`: `crates/cogneva/tests/a_row_two_stores_read_is_mapped_once_test.rs`
//! counts how many production lines in this crate spell each statement, each
//! mapping and the state decode, and fails on any second one.

use chrono::{DateTime, Utc};
use cog_core::{AgentState, SFError, SFResult, TaskCheckpoint};

/// One agent's state, as the state backend keeps it: a JSON document in a text
/// column, keyed by the agent.
pub(crate) const AGENT_STATE_SQL: &str = "SELECT state FROM cog_agent_states WHERE agent_id = $1";

/// The row [`AGENT_STATE_SQL`] answers with.
pub(crate) type AgentStateRow = (String,);

/// One task's checkpoint: where in that task's own event sequence it stopped.
///
/// Named rather than inlined because it is read through two code paths with one
/// retry policy each; a statement written twice is a statement only one of them
/// is measured against.
pub(crate) const CHECKPOINT_SQL: &str =
    "SELECT snapshot_id, event_offset, timestamp FROM cog_checkpoints WHERE task_id = $1";

/// The row [`CHECKPOINT_SQL`] answers with.
pub(crate) type CheckpointRow = (String, i64, DateTime<Utc>);

/// The state a stored document decodes to.
///
/// Three stores keep agent state as JSON -- the postgres state backend, the
/// observability gateway that reads the same table, and the redis backend --
/// and all three have to answer an unreadable document the same way: report it,
/// never fall back to a default. An agent started over from an empty state looks
/// exactly like an agent that had never run, which is the one reading a decoder
/// must not manufacture.
pub(crate) fn agent_state_from_json(document: &str) -> SFResult<AgentState> {
    let state: AgentState = serde_json::from_str(document).map_err(SFError::Serialization)?;
    Ok(state)
}

/// The checkpoint a stored row names, or the reason the row cannot be one.
///
/// `event_offset` is a page cursor into the task's event sequence, and the
/// writer never produces a negative one. Folding a negative into a `u64` turns
/// it into the largest cursor there is, and the task resumes as though it had
/// consumed every event it will ever have -- a corrupt row reading as a finished
/// task. Reported instead, the way the plan reader reports a negative budget.
pub(crate) fn checkpoint_from_row(task_id: &str, row: CheckpointRow) -> SFResult<TaskCheckpoint> {
    let (snapshot_id, event_offset, timestamp) = row;
    let event_offset = u64::try_from(event_offset).map_err(|_| {
        SFError::Database(format!(
            "checkpoint row {task_id} holds a negative event_offset ({event_offset}); \
             a task that has consumed nothing is 0, not below it"
        ))
    })?;
    Ok(TaskCheckpoint {
        task_id: task_id.into(),
        snapshot_id,
        event_offset,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn row(offset: i64) -> CheckpointRow {
        (
            "snap-1".to_string(),
            offset,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        )
    }

    #[test]
    fn a_saved_checkpoint_reads_back_as_the_position_it_names() {
        let checkpoint = checkpoint_from_row("task-1", row(42)).unwrap();
        assert_eq!(checkpoint.task_id, "task-1");
        assert_eq!(checkpoint.snapshot_id, "snap-1");
        assert_eq!(checkpoint.event_offset, 42);
    }

    #[test]
    fn a_negative_offset_is_reported_rather_than_read_as_the_end_of_the_sequence() {
        let err = checkpoint_from_row("task-1", row(-1)).unwrap_err();
        let rendered = format!("{err}");
        assert!(
            rendered.contains("task-1") && rendered.contains("-1"),
            "读到负游标要说清是哪一行、值是多少：{rendered}"
        );
    }

    #[test]
    fn an_unreadable_state_document_is_reported_rather_than_defaulted() {
        // 写入侧写的是枚举的 JSON 形式；空对象不是任何一档生命周期。
        assert!(agent_state_from_json("{}").is_err());
        assert!(agent_state_from_json(r#""no-such-state""#).is_err());
        assert_eq!(
            agent_state_from_json(r#""active""#).unwrap(),
            AgentState::Active
        );
    }
}
