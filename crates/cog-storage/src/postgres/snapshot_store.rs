use async_trait::async_trait;
use chrono::Utc;
use serde_json;
use sqlx::PgPool;

use cog_core::{AgentCheckpoint, CheckpointStore, SFError, SFResult};

/// PostgreSQL-backed snapshot store.
/// Stores [`cog_core::AgentCheckpoint`] objects as JSON text in a single table,
/// keyed by `snapshot_id`.
pub struct PostgresSnapshotStore {
    pool: PgPool,
}

/// This store's slice of the schema. The same statements are the ones a
/// restore replays, so the table cannot exist for the app and be missing for
/// the drill.
pub const DDL: &str = r#"
            CREATE TABLE IF NOT EXISTS cog_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                agent_state TEXT NOT NULL,
                context_window TEXT NOT NULL,
                event_offset BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_cog_snapshots_task_id ON cog_snapshots(task_id);
            -- The capacity sweep deletes oldest-first below a cutoff, which is
            -- one ordered scan of this index rather than a sort of the table.
            CREATE INDEX IF NOT EXISTS idx_cog_snapshots_timestamp ON cog_snapshots(timestamp);
            "#;

impl PostgresSnapshotStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the required table and index if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        crate::schema_ddl::apply(&self.pool, DDL).await
    }
}

/// One row as the store wrote it, decoded back into a checkpoint.
///
/// Both text columns are read with `?`. `agent_state` used to fall back to
/// `Value::Null` on a parse failure: the load then handed back a checkpoint
/// whose state was gone, `restore` found no known key in it, and the agent
/// resumed from the default state while the log said "Agent state restored
/// from checkpoint". A row that cannot be decoded is not a checkpoint to
/// resume from; a caller that cannot resume must say so.
#[allow(clippy::too_many_arguments)]
fn checkpoint_from_row(
    checkpoint_id: String,
    task_id: String,
    agent_state: String,
    context_window: String,
    event_offset: i64,
    timestamp: chrono::DateTime<Utc>,
) -> SFResult<AgentCheckpoint> {
    let agent_state: serde_json::Value =
        serde_json::from_str(&agent_state).map_err(SFError::Serialization)?;
    let context_window: Vec<cog_core::Message> =
        serde_json::from_str(&context_window).map_err(SFError::Serialization)?;
    Ok(AgentCheckpoint {
        checkpoint_id,
        task_id,
        agent_state,
        context_window,
        event_offset: event_offset as u64,
        timestamp,
    })
}

// ==========================================================================
// CheckpointStore implementation
// ==========================================================================

#[async_trait]
impl CheckpointStore for PostgresSnapshotStore {
    async fn save(&self, checkpoint: &AgentCheckpoint) -> SFResult<String> {
        let agent_state = serde_json::to_string(&checkpoint.agent_state)?;
        let context_window = serde_json::to_string(&checkpoint.context_window)?;

        sqlx::query(
            r#"
            INSERT INTO cog_snapshots (snapshot_id, task_id, agent_state, context_window, event_offset, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (snapshot_id) DO UPDATE SET
                task_id = EXCLUDED.task_id,
                agent_state = EXCLUDED.agent_state,
                context_window = EXCLUDED.context_window,
                event_offset = EXCLUDED.event_offset,
                timestamp = EXCLUDED.timestamp
            "#,
        )
        .bind(&checkpoint.checkpoint_id)
        .bind(&checkpoint.task_id)
        .bind(agent_state)
        .bind(context_window)
        .bind(checkpoint.event_offset as i64)
        .bind(checkpoint.timestamp)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(checkpoint.checkpoint_id.clone())
    }

    async fn load(&self, checkpoint_id: &str) -> SFResult<Option<AgentCheckpoint>> {
        let row =
            sqlx::query_as::<_, (String, String, String, String, i64, chrono::DateTime<Utc>)>(
                r#"
            SELECT snapshot_id, task_id, agent_state, context_window, event_offset, timestamp
            FROM cog_snapshots
            WHERE snapshot_id = $1
            "#,
            )
            .bind(checkpoint_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;

        match row {
            Some((
                checkpoint_id,
                task_id,
                agent_state,
                context_window,
                event_offset,
                timestamp,
            )) => Ok(Some(checkpoint_from_row(
                checkpoint_id,
                task_id,
                agent_state,
                context_window,
                event_offset,
                timestamp,
            )?)),
            None => Ok(None),
        }
    }

    async fn delete(&self, checkpoint_id: &str) -> SFResult<()> {
        sqlx::query("DELETE FROM cog_snapshots WHERE snapshot_id = $1")
            .bind(checkpoint_id)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentCheckpoint>> {
        let rows =
            sqlx::query_as::<_, (String, String, String, String, i64, chrono::DateTime<Utc>)>(
                r#"
            SELECT snapshot_id, task_id, agent_state, context_window, event_offset, timestamp
            FROM cog_snapshots
            ORDER BY timestamp DESC
            LIMIT $1
            "#,
            )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;

        let mut cps = Vec::new();
        for (checkpoint_id, task_id, agent_state, context_window, event_offset, timestamp) in rows {
            cps.push(checkpoint_from_row(
                checkpoint_id,
                task_id,
                agent_state,
                context_window,
                event_offset,
                timestamp,
            )?);
        }
        Ok(cps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(agent_state: &str, context_window: &str) -> SFResult<AgentCheckpoint> {
        checkpoint_from_row(
            "snap-1".into(),
            "task-1".into(),
            agent_state.into(),
            context_window.into(),
            7,
            Utc::now(),
        )
    }

    #[test]
    fn a_row_decodes_into_the_checkpoint_that_was_saved() {
        let cp = row(r#"{"state":"running","agent_id":"a-1"}"#, "[]").unwrap();
        assert_eq!(cp.checkpoint_id, "snap-1");
        assert_eq!(cp.agent_state["state"], "running");
        assert_eq!(cp.event_offset, 7);
    }

    /// The state column used to fall back to `Value::Null`: the checkpoint
    /// came back looking whole, `restore` found none of its keys, and the agent
    /// resumed from the default state with a log line saying it had been
    /// restored. A row that does not decode is not one to resume from.
    #[test]
    fn a_snapshot_whose_state_cannot_be_parsed_is_not_resumed_from() {
        let err = row("{ not json", "[]")
            .expect_err("an undecodable state must not read as an empty one");
        assert!(
            matches!(err, SFError::Serialization(_)),
            "the row's own bytes are the problem, not the database: {err}"
        );
    }

    #[test]
    fn a_snapshot_whose_context_cannot_be_parsed_is_not_resumed_from() {
        let err = row("{}", "[ not messages")
            .expect_err("an undecodable context window must not be skipped");
        assert!(matches!(err, SFError::Serialization(_)), "{err}");
    }
}
