use async_trait::async_trait;
use chrono::Utc;
use serde_json;
use sqlx::PgPool;

use cog_core::{HookArchive, SFError, SFResult};

/// PostgreSQL-backed archive for Hook events.
/// Stores every emitted hook event in a single append-only table for
/// audit, replay, and long-term analytics.
pub struct PostgresHookArchive {
    pool: PgPool,
}

impl PostgresHookArchive {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the required table and index if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_hook_events (
                id SERIAL PRIMARY KEY,
                trigger_type TEXT NOT NULL,
                dedup_key TEXT,
                agent_id TEXT,
                task_id TEXT,
                crew_id TEXT,
                squad_id TEXT,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_hook_events_agent_id ON cog_hook_events(agent_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_hook_events_task_id ON cog_hook_events(task_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_hook_events_timestamp ON cog_hook_events(timestamp)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }

    /// Archive a single hook event.
    #[allow(clippy::too_many_arguments)]
    pub async fn archive(
        &self,
        trigger_type: &str,
        dedup_key: Option<&str>,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        crew_id: Option<&str>,
        squad_id: Option<&str>,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_hook_events (trigger_type, dedup_key, agent_id, task_id, crew_id, squad_id, payload, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            "#,
        )
        .bind(trigger_type)
        .bind(dedup_key)
        .bind(agent_id)
        .bind(task_id)
        .bind(crew_id)
        .bind(squad_id)
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    /// Query archived hook events with optional filters.
    pub async fn query(
        &self,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        trigger_type: Option<&str>,
        since: Option<chrono::DateTime<Utc>>,
        limit: usize,
    ) -> SFResult<Vec<HookArchiveRow>> {
        let mut query_str = String::from(
            "SELECT id, trigger_type, dedup_key, agent_id, task_id, crew_id, squad_id, payload, timestamp FROM cog_hook_events WHERE 1=1"
        );
        if agent_id.is_some() {
            query_str.push_str(" AND agent_id = $1");
        }
        if task_id.is_some() {
            query_str.push_str(" AND task_id = $2");
        }
        if trigger_type.is_some() {
            query_str.push_str(" AND trigger_type = $3");
        }
        if since.is_some() {
            query_str.push_str(" AND timestamp >= $4");
        }
        query_str.push_str(" ORDER BY timestamp DESC LIMIT $5");

        let mut q = sqlx::query_as::<_, HookArchiveRow>(&query_str);
        q = q.bind(agent_id.unwrap_or(""));
        q = q.bind(task_id.unwrap_or(""));
        q = q.bind(trigger_type.unwrap_or(""));
        q = q.bind(since.unwrap_or(chrono::DateTime::UNIX_EPOCH));
        q = q.bind(limit as i64);

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(rows)
    }
}

#[async_trait]
impl HookArchive for PostgresHookArchive {
    async fn archive(
        &self,
        trigger_type: &str,
        dedup_key: Option<&str>,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        crew_id: Option<&str>,
        squad_id: Option<&str>,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        self.archive(
            trigger_type,
            dedup_key,
            agent_id,
            task_id,
            crew_id,
            squad_id,
            payload,
        )
        .await
    }

    async fn query(
        &self,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        trigger_type: Option<&str>,
        since: Option<chrono::DateTime<Utc>>,
        limit: usize,
    ) -> SFResult<Vec<cog_core::HookArchiveEntry>> {
        let rows = self
            .query(agent_id, task_id, trigger_type, since, limit)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| cog_core::HookArchiveEntry {
                id: r.id,
                trigger_type: r.trigger_type,
                dedup_key: r.dedup_key,
                agent_id: r.agent_id,
                task_id: r.task_id,
                crew_id: r.crew_id,
                squad_id: r.squad_id,
                payload: r.payload,
                timestamp: r.timestamp,
            })
            .collect())
    }
}

/// A single row from the hook event archive.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct HookArchiveRow {
    pub id: i32,
    pub trigger_type: String,
    pub dedup_key: Option<String>,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub crew_id: Option<String>,
    pub squad_id: Option<String>,
    pub payload: serde_json::Value,
    pub timestamp: chrono::DateTime<Utc>,
}
