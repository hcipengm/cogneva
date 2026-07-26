use async_trait::async_trait;
use chrono::Utc;
use serde_json;
use sqlx::PgPool;

use cog_core::{AgentCheckpoint, CheckpointStore, SFError, SFResult};

/// PostgreSQL-backed snapshot store.
/// Stores [`Snapshot`] objects as JSONB rows in a single table,
/// keyed by `snapshot_id`.
pub struct PostgresSnapshotStore {
    pool: PgPool,
}

impl PostgresSnapshotStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the required table and index if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                agent_state JSONB NOT NULL,
                context_window JSONB NOT NULL,
                event_offset BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_snapshots_task_id ON cog_snapshots(task_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }
}

// ==========================================================================
// CheckpointStore implementation
// ==========================================================================

#[async_trait]
impl CheckpointStore for PostgresSnapshotStore {
    async fn save(&self, checkpoint: &AgentCheckpoint) -> SFResult<String> {
        let agent_state = serde_json::to_value(&checkpoint.agent_state)?;
        let context_window = serde_json::to_value(&checkpoint.context_window)?;

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
        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                serde_json::Value,
                serde_json::Value,
                i64,
                chrono::DateTime<Utc>,
            ),
        >(
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
            )) => {
                let context_window: Vec<cog_core::Message> =
                    serde_json::from_value(context_window).map_err(SFError::Serialization)?;
                Ok(Some(AgentCheckpoint {
                    checkpoint_id,
                    task_id,
                    agent_state,
                    context_window,
                    event_offset: event_offset as u64,
                    timestamp,
                }))
            }
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
        let rows = sqlx::query_as::<
            _,
            (
                String,
                String,
                serde_json::Value,
                serde_json::Value,
                i64,
                chrono::DateTime<Utc>,
            ),
        >(
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
            let context_window: Vec<cog_core::Message> =
                serde_json::from_value(context_window).map_err(SFError::Serialization)?;
            cps.push(AgentCheckpoint {
                checkpoint_id,
                task_id,
                agent_state,
                context_window,
                event_offset: event_offset as u64,
                timestamp,
            });
        }
        Ok(cps)
    }
}
