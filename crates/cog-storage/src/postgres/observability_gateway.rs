use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

use futures::StreamExt;
use tokio::sync::broadcast;

use cog_core::{
    AgentEvent, AgentState, ClusterOverview, EventFilter, LogEntry, ObservabilityGateway,
    RawLogIndex, SFError, SFResult, SquadState, SquadStatus, TaskCheckpoint, TaskMetrics,
};

// ─── DDL ───

const OBSERVABILITY_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cog_observability_events (
    id          SERIAL PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    agent_id    TEXT,
    task_id     TEXT,
    crew_id     TEXT,
    squad_id    TEXT,
    payload     JSONB       NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cog_obs_events_agent_id  ON cog_observability_events(agent_id);
CREATE INDEX IF NOT EXISTS idx_cog_obs_events_task_id   ON cog_observability_events(task_id);
CREATE INDEX IF NOT EXISTS idx_cog_obs_events_crew_id   ON cog_observability_events(crew_id);
CREATE INDEX IF NOT EXISTS idx_cog_obs_events_squad_id  ON cog_observability_events(squad_id);
CREATE INDEX IF NOT EXISTS idx_cog_obs_events_timestamp ON cog_observability_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_cog_obs_events_type      ON cog_observability_events(event_type);

CREATE TABLE IF NOT EXISTS cog_observability_logs (
    id          SERIAL PRIMARY KEY,
    task_id     TEXT        NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    level       TEXT        NOT NULL,
    source      TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    metadata    JSONB       NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_cog_obs_logs_task_id     ON cog_observability_logs(task_id);
CREATE INDEX IF NOT EXISTS idx_cog_obs_logs_timestamp   ON cog_observability_logs(timestamp);

CREATE TABLE IF NOT EXISTS cog_observability_metrics (
    task_id             TEXT PRIMARY KEY,
    total_tokens        BIGINT      NOT NULL,
    prompt_tokens       BIGINT      NOT NULL,
    completion_tokens   BIGINT      NOT NULL,
    tool_calls          INTEGER     NOT NULL,
    iterations          INTEGER     NOT NULL,
    duration_ms         BIGINT      NOT NULL,
    timestamp           TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS cog_observability_raw_index (
    id            SERIAL PRIMARY KEY,
    stream        TEXT        NOT NULL,
    date          DATE        NOT NULL,
    file_path     TEXT        NOT NULL,
    encoding      TEXT        NOT NULL,
    record_count  BIGINT      NOT NULL,
    byte_size     BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cog_obs_raw_stream_date ON cog_observability_raw_index(stream, date);

CREATE TABLE IF NOT EXISTS cog_observability_snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    url           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cog_observability_squads (
    squad_id        TEXT PRIMARY KEY,
    task_id         TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    agents          JSONB       NOT NULL,
    completion_pct  REAL        NOT NULL,
    retry_count     INTEGER     NOT NULL,
    snapshot_id     TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cog_obs_squads_task_id ON cog_observability_squads(task_id);
"#;

// ─── Struct ───

/// PostgreSQL-backed observability gateway.
/// Stores all observability data in PostgreSQL for durability across restarts.
/// Real-time event subscription uses an in-memory broadcast channel (same
/// mechanism as [`MemoryObservabilityGateway`]) so that consumers receive
/// low-latency push events. Historical events are replayed from the database
/// before the live stream begins.
pub struct PostgresObservabilityGateway {
    pool: PgPool,
    event_tx: broadcast::Sender<AgentEvent>,
    event_channel_capacity: usize,
}

impl PostgresObservabilityGateway {
    pub fn new(pool: PgPool) -> Self {
        let (event_tx, _event_rx) = broadcast::channel(256);
        Self {
            pool,
            event_tx,
            event_channel_capacity: 256,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    /// Apply the observability schema. Safe to call repeatedly.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::raw_sql(OBSERVABILITY_DDL)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("observability schema init failed: {}", e)))?;
        Ok(())
    }

    /// Publish an event to all active subscribers and persist it to PostgreSQL.
    /// This is a synchronous method (matching the surface of
    /// [`MemoryObservabilityGateway::publish_event`]) so that it can be used
    /// as a drop-in replacement in the [`EventGatewaySink`] path. The PG write
    /// is performed in a spawned task so the caller never blocks.
    pub fn publish_event(&self, event: AgentEvent) {
        let _ = self.event_tx.send(event.clone());

        let pool = self.pool.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::persist_event(&pool, &event).await {
                tracing::warn!("Failed to persist observability event: {}", e);
            }
        });
    }

    /// Record a structured log entry for the given task.
    pub async fn record_log(&self, task_id: &str, entry: LogEntry) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_observability_logs (task_id, timestamp, level, source, message, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(task_id)
        .bind(entry.timestamp)
        .bind(&entry.level)
        .bind(&entry.source)
        .bind(&entry.message)
        .bind(&entry.metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("record_log failed: {}", e)))?;
        Ok(())
    }

    /// Record or update metrics for a task.
    pub async fn record_metrics(&self, metrics: &TaskMetrics) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_observability_metrics
                (task_id, total_tokens, prompt_tokens, completion_tokens, tool_calls, iterations, duration_ms, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (task_id) DO UPDATE SET
                total_tokens      = EXCLUDED.total_tokens,
                prompt_tokens     = EXCLUDED.prompt_tokens,
                completion_tokens = EXCLUDED.completion_tokens,
                tool_calls        = EXCLUDED.tool_calls,
                iterations        = EXCLUDED.iterations,
                duration_ms       = EXCLUDED.duration_ms,
                timestamp         = EXCLUDED.timestamp
            "#,
        )
        .bind(&metrics.task_id)
        .bind(metrics.total_tokens as i64)
        .bind(metrics.prompt_tokens as i64)
        .bind(metrics.completion_tokens as i64)
        .bind(metrics.tool_calls as i32)
        .bind(metrics.iterations as i32)
        .bind(metrics.duration_ms as i64)
        .bind(metrics.timestamp)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("record_metrics failed: {}", e)))?;
        Ok(())
    }

    /// Register a raw-log index entry.
    pub async fn register_raw_index(&self, entry: &RawLogIndex) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_observability_raw_index (stream, date, file_path, encoding, record_count, byte_size, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&entry.stream)
        .bind(entry.date)
        .bind(&entry.file_path)
        .bind(&entry.encoding)
        .bind(entry.record_count as i64)
        .bind(entry.byte_size as i64)
        .bind(entry.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("register_raw_index failed: {}", e)))?;
        Ok(())
    }

    /// Register a snapshot URL.
    pub async fn register_snapshot(&self, snapshot_id: &str, url: &str) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_observability_snapshots (snapshot_id, url)
            VALUES ($1, $2)
            ON CONFLICT (snapshot_id) DO UPDATE SET url = EXCLUDED.url
            "#,
        )
        .bind(snapshot_id)
        .bind(url)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("register_snapshot failed: {}", e)))?;
        Ok(())
    }

    /// Update squad state in PostgreSQL.
    pub async fn update_squad_state(&self, squad_id: &str, state: &SquadState) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_observability_squads (squad_id, task_id, status, agents, completion_pct, retry_count, snapshot_id, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (squad_id) DO UPDATE SET
                task_id        = EXCLUDED.task_id,
                status         = EXCLUDED.status,
                agents         = EXCLUDED.agents,
                completion_pct = EXCLUDED.completion_pct,
                retry_count    = EXCLUDED.retry_count,
                snapshot_id    = EXCLUDED.snapshot_id,
                updated_at     = EXCLUDED.updated_at
            "#,
        )
        .bind(squad_id)
        .bind(&state.task_id)
        .bind(squad_status_str(state.status))
        .bind(serde_json::to_value(&state.agents).map_err(SFError::Serialization)?)
        .bind(state.completion_pct)
        .bind(state.retry_count as i32)
        .bind(&state.snapshot_id)
        .bind(state.created_at)
        .bind(state.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("update_squad_state failed: {}", e)))?;
        Ok(())
    }

    // ─── Internal helpers ───

    async fn persist_event(pool: &PgPool, event: &AgentEvent) -> SFResult<()> {
        let (event_type, agent_id, task_id, crew_id, squad_id, timestamp) =
            extract_event_meta(event);
        let payload = serde_json::to_value(event).map_err(SFError::Serialization)?;

        sqlx::query(
            r#"
            INSERT INTO cog_observability_events (event_type, agent_id, task_id, crew_id, squad_id, payload, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(event_type)
        .bind(agent_id)
        .bind(task_id)
        .bind(crew_id)
        .bind(squad_id)
        .bind(payload)
        .bind(timestamp)
        .execute(pool)
        .await
        .map_err(|e| SFError::Database(format!("persist_event failed: {}", e)))?;
        Ok(())
    }

    async fn query_historical_events(&self, filter: &EventFilter) -> SFResult<Vec<AgentEvent>> {
        let since = filter
            .since
            .unwrap_or_else(|| Utc::now() - Duration::hours(24));

        let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
            "SELECT payload FROM cog_observability_events WHERE timestamp >= $1 ORDER BY timestamp ASC LIMIT 10000",
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("query historical events failed: {}", e)))?;

        let mut events = Vec::with_capacity(rows.len());
        for (payload,) in rows {
            match serde_json::from_value::<AgentEvent>(payload) {
                Ok(ev) => {
                    if crate::event_filter::event_matches(filter, &ev) {
                        events.push(ev);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize historical event: {}", e);
                }
            }
        }
        Ok(events)
    }
}

#[async_trait]
impl ObservabilityGateway for PostgresObservabilityGateway {
    async fn subscribe_events(
        &self,
        filter: EventFilter,
    ) -> SFResult<cog_core::observability::AgentEventStream> {
        let historical = self.query_historical_events(&filter).await?;
        let rx = self.event_tx.subscribe();

        let stream = futures::stream::iter(historical.into_iter().map(Ok)).chain(
            futures::stream::unfold((rx, filter), |(mut rx, filter)| async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if crate::event_filter::event_matches(&filter, &event) {
                                return Some((Ok(event), (rx, filter)));
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
            }),
        );

        Ok(Box::pin(stream))
    }

    async fn get_agent_state(&self, agent_id: &str) -> SFResult<AgentState> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state FROM cog_agent_states WHERE agent_id = $1")
                .bind(agent_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("get_agent_state failed: {}", e)))?;

        match row {
            Some((s,)) => {
                let state: AgentState = serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(state)
            }
            None => Err(SFError::Agent(format!("agent {} not found", agent_id))),
        }
    }

    async fn get_task_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let row: Option<(String, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT snapshot_id, event_offset, timestamp FROM cog_checkpoints WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("get_task_checkpoint failed: {}", e)))?;

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

    async fn get_task_metrics(&self, task_id: &str) -> SFResult<TaskMetrics> {
        let row: Option<(i64, i64, i64, i32, i32, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT total_tokens, prompt_tokens, completion_tokens, tool_calls, iterations, duration_ms, timestamp
             FROM cog_observability_metrics WHERE task_id = $1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("get_task_metrics failed: {}", e)))?;

        match row {
            Some((total, prompt, completion, tools, iters, duration, ts)) => Ok(TaskMetrics {
                task_id: task_id.into(),
                total_tokens: total as u64,
                prompt_tokens: prompt as u64,
                completion_tokens: completion as u64,
                tool_calls: tools as u32,
                iterations: iters as u32,
                duration_ms: duration as u64,
                timestamp: ts,
            }),
            None => Err(SFError::Agent(format!(
                "metrics not found for task {}",
                task_id
            ))),
        }
    }

    async fn get_task_logs(&self, task_id: &str, limit: usize) -> SFResult<Vec<LogEntry>> {
        let rows: Vec<(DateTime<Utc>, String, String, String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT timestamp, level, source, message, metadata
            FROM cog_observability_logs
            WHERE task_id = $1
            ORDER BY timestamp DESC
            LIMIT $2
            "#,
        )
        .bind(task_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("get_task_logs failed: {}", e)))?;

        let mut logs = Vec::with_capacity(rows.len());
        for (timestamp, level, source, message, metadata) in rows {
            logs.push(LogEntry {
                timestamp,
                level,
                source,
                message,
                metadata,
            });
        }
        logs.reverse();
        Ok(logs)
    }

    async fn get_snapshot_url(&self, snapshot_id: &str) -> SFResult<String> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT url FROM cog_observability_snapshots WHERE snapshot_id = $1")
                .bind(snapshot_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("get_snapshot_url failed: {}", e)))?;

        match row {
            Some((url,)) => Ok(url),
            None => Err(SFError::Agent(format!(
                "snapshot {} not found",
                snapshot_id
            ))),
        }
    }

    async fn get_raw_log_index(&self, stream: &str, date: NaiveDate) -> SFResult<Vec<RawLogIndex>> {
        let rows: Vec<(String, String, i64, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT file_path, encoding, record_count, byte_size, created_at
             FROM cog_observability_raw_index
             WHERE stream = $1 AND date = $2",
        )
        .bind(stream)
        .bind(date)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("get_raw_log_index failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for (file_path, encoding, record_count, byte_size, created_at) in rows {
            entries.push(RawLogIndex {
                stream: stream.into(),
                date,
                file_path,
                encoding,
                record_count: record_count as u64,
                byte_size: byte_size as u64,
                created_at,
            });
        }
        Ok(entries)
    }

    async fn get_cluster_overview(&self) -> SFResult<ClusterOverview> {
        let total_tasks: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cog_observability_metrics")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                SFError::Database(format!("get_cluster_overview metrics count failed: {}", e))
            })?;

        let active_tasks: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM cog_observability_metrics WHERE iterations > 0")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!("get_cluster_overview active tasks failed: {}", e))
                })?;

        let avg_duration: Option<(i64,)> =
            sqlx::query_as("SELECT AVG(duration_ms)::bigint FROM cog_observability_metrics")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!("get_cluster_overview avg duration failed: {}", e))
                })?;

        let total_squads: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cog_observability_squads")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                SFError::Database(format!("get_cluster_overview squads count failed: {}", e))
            })?;

        let active_squads: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM cog_observability_squads WHERE status = 'running'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            SFError::Database(format!("get_cluster_overview active squads failed: {}", e))
        })?;

        let total_agents: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cog_agent_states")
            .fetch_one(&self.pool)
            .await
            .unwrap_or((0,));

        let active_agents: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM cog_agent_states WHERE state = 'active'")
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));

        Ok(ClusterOverview {
            total_agents: total_agents.0 as usize,
            active_agents: active_agents.0 as usize,
            total_tasks: total_tasks.0 as usize,
            active_tasks: active_tasks.0 as usize,
            queued_tasks: 0,
            failed_tasks: 0,
            avg_task_duration_ms: avg_duration.map(|d| d.0 as u64).unwrap_or(0),
            cluster_health: "healthy".into(),
            timestamp: Utc::now(),
            total_squads: total_squads.0 as usize,
            active_squads: active_squads.0 as usize,
        })
    }

    async fn get_squad_state(&self, squad_id: &str) -> SFResult<SquadState> {
        let row: Option<(String, String, serde_json::Value, f32, i32, Option<String>, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT task_id, status, agents, completion_pct, retry_count, snapshot_id, created_at, updated_at
             FROM cog_observability_squads WHERE squad_id = $1",
        )
        .bind(squad_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("get_squad_state failed: {}", e)))?;

        match row {
            Some((
                task_id,
                status_str,
                agents_json,
                completion_pct,
                retry_count,
                snapshot_id,
                created_at,
                updated_at,
            )) => {
                let agents: Vec<cog_core::AgentSummary> =
                    serde_json::from_value(agents_json).map_err(SFError::Serialization)?;
                Ok(SquadState {
                    squad_id: squad_id.into(),
                    task_id,
                    status: parse_squad_status(&status_str)?,
                    agents,
                    completion_pct,
                    retry_count: retry_count as u32,
                    snapshot_id,
                    created_at,
                    updated_at,
                })
            }
            None => Err(SFError::Agent(format!("squad {} not found", squad_id))),
        }
    }

    fn publish_event(&self, event: AgentEvent) {
        let _ = self.event_tx.send(event);
    }
}

// ─── Event meta extraction ───

#[allow(clippy::type_complexity)]
fn extract_event_meta(
    event: &AgentEvent,
) -> (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    DateTime<Utc>,
) {
    let event_type = crate::event_filter::event_type_name(event).to_string();
    let timestamp = event_timestamp(event);

    let (agent_id, task_id, crew_id, squad_id) = match event {
        AgentEvent::AgentStart {
            agent_id,
            crew_id,
            squad_id,
            ..
        } => (
            Some(agent_id.clone()),
            None,
            crew_id.clone(),
            squad_id.clone(),
        ),
        AgentEvent::AgentEnd {
            agent_id,
            crew_id,
            squad_id,
            ..
        } => (
            Some(agent_id.clone()),
            None,
            crew_id.clone(),
            squad_id.clone(),
        ),
        AgentEvent::TurnStart { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::TurnEnd { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::MessageStart { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::MessageUpdate { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::MessageEnd { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::ToolExecutionStart { agent_id, .. } => {
            (Some(agent_id.clone()), None, None, None)
        }
        AgentEvent::ToolExecutionUpdate { agent_id, .. } => {
            (Some(agent_id.clone()), None, None, None)
        }
        AgentEvent::ToolExecutionEnd { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::StateChange {
            agent_id,
            crew_id,
            squad_id,
            ..
        } => (
            Some(agent_id.clone()),
            None,
            crew_id.clone(),
            squad_id.clone(),
        ),
        AgentEvent::TaskStatusChange {
            agent_id,
            task_id,
            crew_id,
            squad_id,
            ..
        } => (
            agent_id.clone(),
            Some(task_id.clone()),
            crew_id.clone(),
            squad_id.clone(),
        ),
        AgentEvent::SelfReview { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::ReActStepStart { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::ReActStepEnd { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::AgentError {
            agent_id,
            crew_id,
            squad_id,
            ..
        } => (
            Some(agent_id.clone()),
            None,
            crew_id.clone(),
            squad_id.clone(),
        ),
        AgentEvent::ResourceAlert {
            agent_id,
            crew_id,
            squad_id,
            ..
        } => (
            Some(agent_id.clone()),
            None,
            crew_id.clone(),
            squad_id.clone(),
        ),
        AgentEvent::Heartbeat { agent_id, .. } => (Some(agent_id.clone()), None, None, None),
        AgentEvent::CheckpointSaved {
            agent_id,
            task_id,
            crew_id,
            squad_id,
            ..
        } => (
            Some(agent_id.clone()),
            Some(task_id.clone()),
            crew_id.clone(),
            squad_id.clone(),
        ),
    };

    (event_type, agent_id, task_id, crew_id, squad_id, timestamp)
}

fn event_timestamp(event: &AgentEvent) -> DateTime<Utc> {
    match event {
        AgentEvent::AgentStart { timestamp, .. }
        | AgentEvent::AgentEnd { timestamp, .. }
        | AgentEvent::TurnStart { timestamp, .. }
        | AgentEvent::TurnEnd { timestamp, .. }
        | AgentEvent::MessageStart { timestamp, .. }
        | AgentEvent::MessageUpdate { timestamp, .. }
        | AgentEvent::MessageEnd { timestamp, .. }
        | AgentEvent::ToolExecutionStart { timestamp, .. }
        | AgentEvent::ToolExecutionUpdate { timestamp, .. }
        | AgentEvent::ToolExecutionEnd { timestamp, .. }
        | AgentEvent::StateChange { timestamp, .. }
        | AgentEvent::TaskStatusChange { timestamp, .. }
        | AgentEvent::SelfReview { timestamp, .. }
        | AgentEvent::ReActStepStart { timestamp, .. }
        | AgentEvent::ReActStepEnd { timestamp, .. }
        | AgentEvent::AgentError { timestamp, .. }
        | AgentEvent::ResourceAlert { timestamp, .. }
        | AgentEvent::Heartbeat { timestamp, .. }
        | AgentEvent::CheckpointSaved { timestamp, .. } => *timestamp,
    }
}

fn squad_status_str(status: SquadStatus) -> &'static str {
    match status {
        SquadStatus::Pending => "pending",
        SquadStatus::Running => "running",
        SquadStatus::Complete => "complete",
        SquadStatus::Failed => "failed",
        SquadStatus::Retrying => "retrying",
    }
}

fn parse_squad_status(s: &str) -> SFResult<SquadStatus> {
    match s {
        "pending" => Ok(SquadStatus::Pending),
        "running" => Ok(SquadStatus::Running),
        "complete" => Ok(SquadStatus::Complete),
        "failed" => Ok(SquadStatus::Failed),
        "retrying" => Ok(SquadStatus::Retrying),
        other => Err(SFError::Agent(format!("unknown squad status: {}", other))),
    }
}
