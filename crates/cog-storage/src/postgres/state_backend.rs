use async_trait::async_trait;
use chrono::Utc;
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{AgentState, ContextBoard, Event, SFError, SFResult, StateBackend, TaskCheckpoint};

/// PostgreSQL-backed state backend.
pub struct PostgresStateBackend {
    pool: PgPool,
}

impl PostgresStateBackend {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Retry a database operation with exponential backoff.
    async fn retry<T, F, Fut>(&self, mut op: F) -> SFResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
    {
        let mut last_err = None;
        for attempt in 0..3 {
            match op().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            200 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        Err(SFError::Database(
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown database error".into()),
        ))
    }

    /// Auto-create the required tables and indexes if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_agent_states (
                agent_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_checkpoints (
                task_id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
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
            CREATE TABLE IF NOT EXISTS cog_events (
                id SERIAL PRIMARY KEY,
                task_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                offset_num BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_events_task_id ON cog_events(task_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_context_boards (
                task_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (task_id, field_name)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_dag_state (
                workspace_id TEXT PRIMARY KEY,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl StateBackend for PostgresStateBackend {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<AgentState>> {
        let row: Option<(String,)> = self
            .retry(|| async {
                sqlx::query_as("SELECT state FROM cog_agent_states WHERE agent_id = $1")
                    .bind(agent_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        match row {
            Some((s,)) => {
                let state: AgentState = serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn set_agent_state(&self, agent_id: &str, state: &AgentState) -> SFResult<()> {
        let value = serde_json::to_string(state)?;
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_agent_states (agent_id, state, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (agent_id) DO UPDATE SET state = EXCLUDED.state, updated_at = NOW()
                "#,
            )
            .bind(agent_id)
            .bind(value.clone())
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &AgentState,
        new: &AgentState,
    ) -> SFResult<bool> {
        let expected_json = serde_json::to_string(expected)?;
        let new_json = serde_json::to_string(new)?;

        let result = self
            .retry(|| async {
                sqlx::query(
                    r#"
                    UPDATE cog_agent_states
                    SET state = $1, updated_at = NOW()
                    WHERE agent_id = $2 AND state = $3
                    "#,
                )
                .bind(new_json.clone())
                .bind(agent_id)
                .bind(expected_json.clone())
                .execute(&self.pool)
                .await
            })
            .await?;

        Ok(result.rows_affected() == 1)
    }

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let row: Option<(String, i64, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(
                    "SELECT snapshot_id, event_offset, timestamp FROM cog_checkpoints WHERE task_id = $1",
                )
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await
            })
            .await?;

        match row {
            Some((snapshot_id, event_offset, timestamp)) => Ok(Some(TaskCheckpoint {
                task_id: task_id.into(),
                snapshot_id,
                event_offset: event_offset as u64,
                timestamp,
            })),
            None => Ok(None),
        }
    }

    async fn save_checkpoint(&self, checkpoint: &TaskCheckpoint) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_checkpoints (task_id, snapshot_id, event_offset, timestamp)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (task_id) DO UPDATE SET
                    snapshot_id = EXCLUDED.snapshot_id,
                    event_offset = EXCLUDED.event_offset,
                    timestamp = EXCLUDED.timestamp
                "#,
            )
            .bind(&checkpoint.task_id)
            .bind(&checkpoint.snapshot_id)
            .bind(checkpoint.event_offset as i64)
            .bind(checkpoint.timestamp)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_checkpoints WHERE task_id = $1")
                .bind(task_id)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn append_event(&self, task_id: &str, event: &Event) -> SFResult<u64> {
        let payload = serde_json::to_value(event)?;
        let event_type = event.event_type.clone();

        let count: (i64,) = self
            .retry(|| async {
                sqlx::query_as("SELECT COUNT(*) FROM cog_events WHERE task_id = $1")
                    .bind(task_id)
                    .fetch_one(&self.pool)
                    .await
            })
            .await?;

        let offset_num = count.0 + 1;

        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_events (task_id, event_type, payload, offset_num, created_at)
                VALUES ($1, $2, $3, $4, NOW())
                "#,
            )
            .bind(task_id)
            .bind(event_type.clone())
            .bind(payload.clone())
            .bind(offset_num)
            .execute(&self.pool)
            .await
        })
        .await?;

        Ok(offset_num as u64)
    }

    async fn get_events(&self, task_id: &str, offset: u64, limit: usize) -> SFResult<Vec<Event>> {
        let rows: Vec<(String, serde_json::Value, i64, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT event_type, payload, offset_num, created_at
                    FROM cog_events
                    WHERE task_id = $1 AND offset_num >= $2
                    ORDER BY offset_num ASC
                    LIMIT $3
                    "#,
                )
                .bind(task_id)
                .bind(offset as i64)
                .bind(limit as i64)
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        let mut events = Vec::with_capacity(rows.len());
        for (event_type, payload, offset_num, created_at) in rows {
            events.push(Event {
                offset: offset_num as u64,
                task_id: task_id.into(),
                event_type,
                payload,
                timestamp: created_at,
            });
        }
        Ok(events)
    }

    async fn get_board(&self, task_id: &str) -> SFResult<Option<ContextBoard>> {
        let rows: Vec<(String, String, chrono::DateTime<Utc>)> = self
            .retry(|| async {
                sqlx::query_as(
                    r#"
                    SELECT field_name, field_value, updated_at
                    FROM cog_context_boards
                    WHERE task_id = $1
                    "#,
                )
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
            })
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut fields = HashMap::with_capacity(rows.len());
        let mut updated_at = Utc::now();
        for (field_name, field_value, row_updated_at) in rows {
            fields.insert(field_name, field_value);
            if row_updated_at > updated_at {
                updated_at = row_updated_at;
            }
        }

        Ok(Some(ContextBoard {
            task_id: task_id.into(),
            fields,
            updated_at,
        }))
    }

    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_context_boards (task_id, field_name, field_value, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (task_id, field_name) DO UPDATE SET
                    field_value = EXCLUDED.field_value,
                    updated_at = NOW()
                "#,
            )
            .bind(task_id)
            .bind(field)
            .bind(value)
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn delete_board(&self, task_id: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_context_boards WHERE task_id = $1")
                .bind(task_id)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query("DELETE FROM cog_context_boards WHERE task_id = $1 AND field_name = $2")
                .bind(task_id)
                .bind(field)
                .execute(&self.pool)
                .await
        })
        .await?;
        Ok(())
    }

    async fn save_dag_state(&self, workspace_id: &str, state: &serde_json::Value) -> SFResult<()> {
        self.retry(|| async {
            sqlx::query(
                r#"
                INSERT INTO cog_dag_state (workspace_id, state, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (workspace_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    updated_at = NOW()
                "#,
            )
            .bind(workspace_id)
            .bind(state.clone())
            .execute(&self.pool)
            .await
        })
        .await?;
        Ok(())
    }

    async fn load_dag_state(&self, workspace_id: &str) -> SFResult<Option<serde_json::Value>> {
        let row: Option<(serde_json::Value,)> = self
            .retry(|| async {
                sqlx::query_as("SELECT state FROM cog_dag_state WHERE workspace_id = $1")
                    .bind(workspace_id)
                    .fetch_optional(&self.pool)
                    .await
            })
            .await?;

        Ok(row.map(|r| r.0))
    }
}
