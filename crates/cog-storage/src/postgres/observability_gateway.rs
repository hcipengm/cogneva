use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;

use cog_core::{
    AgentEvent, AgentState, ClusterOverview, EventFilter, LogEntry, ObservabilityGateway,
    RawLogIndex, SFError, SFResult, SquadState, SquadStatus, TaskCheckpoint, TaskMetrics,
};

// ─── DDL ───

/// This store's schema, and the one a restore replays.
pub const OBSERVABILITY_DDL: &str = r#"
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
/// mechanism as [`crate::mem::MemoryObservabilityGateway`]) so that consumers receive
/// low-latency push events. Historical events are replayed from the database
/// before the live stream begins.
pub struct PostgresObservabilityGateway {
    pool: PgPool,
    event_tx: broadcast::Sender<AgentEvent>,
    event_channel_capacity: usize,
    /// The task-state store this process actually uses, so the overview's task
    /// counts come from the rows the DAG writes rather than from a second
    /// reading of the same database taken here. Absent, the counts stay
    /// `None` — the census lives on the task-state surface, and this gateway
    /// has no way to answer it alone.
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    /// 订阅流落后广播时，丢掉的条数写到哪里。见
    /// [`crate::consumer_lag::record_consumer_lag`]：缺席时那一格只剩订阅方看见的
    /// **少了的事件**，没有任何地方记着「它漏了几条」。
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    /// 已失败、还没落进计数器的条数。失败是**成批**来的（一次洪水里成千上万），
    /// 而每失败一次就 upsert 同一行，会把「记一次失败」变成第二个故障源：
    /// 2026-10-18 现场里这条写入和失败的插入抢同一张热表，失败越多越堵、
    /// 越堵越失败，池被它自己占满。所以失败只在这里加一，最多每
    /// [`FAILURE_FLUSH_SECS`] 秒落一次。
    failed_pending: Arc<std::sync::atomic::AtomicU64>,
    /// 上一次把待落量写进库的时刻。
    failed_last_flush: Arc<std::sync::Mutex<std::time::Instant>>,
}

/// 失败计数最多多久落一次库。选 5 秒是因为它要明显短于一条告警的判定窗
/// （分钟级），又足以把「每次失败一行」压成「每 5 秒一行」。
const FAILURE_FLUSH_SECS: u64 = 5;

impl PostgresObservabilityGateway {
    /// 接上指标面。缺席是正常的（嵌入式运行没有后端），不是错误。
    pub fn with_metrics(mut self, metrics: Option<Arc<dyn cog_core::MetricsBackend>>) -> Self {
        self.metrics = metrics;
        self
    }

    /// 在第一个事件到来之前，先把失败计数按 0 写一条。
    ///
    /// 失败计数是「一次落库失败加一」，不失败就一行都没有。那样「这一段没有
    /// 失败」与「落库这条通路根本没接上」在读数面上逐字同形，而两种情形要走的
    /// 下一步相反：前者去看上游，后者说明写者压根没装配。留下来自写者自己的
    /// 指标句柄，而不是由提这个问题的一方去读侧补一条 0——读的人记的 0 只能
    /// 证明读的人活着。
    ///
    /// 写 0 是增量而不是赋值，所以重启不会抹掉另一个进程已经累上去的总数。
    pub async fn seed_persist_failure_series(&self) {
        let Some(ref metrics) = self.metrics else {
            return;
        };
        if let Err(e) = metrics
            .record_counter(
                cog_core::metric_names::OBSERVABILITY_EVENT_PERSIST_FAILED_TOTAL,
                0.0,
                std::collections::HashMap::new(),
            )
            .await
        {
            tracing::warn!(
                "cannot seed the observability persist failure counter: {}",
                e
            );
        }
    }

    pub fn new(pool: PgPool) -> Self {
        let (event_tx, _event_rx) = broadcast::channel(256);
        Self {
            pool,
            event_tx,
            event_channel_capacity: 256,
            state_backend: None,
            metrics: None,
            failed_pending: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            failed_last_flush: Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    /// Hand over the task-state store so the overview can report task counts.
    pub fn with_state_backend(mut self, backend: Arc<dyn cog_core::StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// A count query whose failure is a missing reading, not a zero.
    ///
    /// Returning 0 for a failed query would report an empty cluster in exactly
    /// the shape of a real reading — the one thing a reader cannot tell apart.
    async fn count_or_none(&self, sql: &str) -> Option<usize> {
        match sqlx::query_as::<_, (i64,)>(sql).fetch_one(&self.pool).await {
            Ok((n,)) => Some(n.max(0) as usize),
            Err(e) => {
                tracing::warn!(error = %e, query = %sql, "cluster overview count unavailable");
                None
            }
        }
    }

    /// Apply the observability schema. Safe to call repeatedly.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::raw_sql(OBSERVABILITY_DDL)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("observability schema init failed: {}", e)))?;
        Ok(())
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

    /// Add one run's metrics to its task's row.
    ///
    /// A task id is not a run id: the pipeline hands one task id to its
    /// planner, generator and evaluator, and each of them is a separate run
    /// that reports its own usage once at its exit. Overwriting the row would
    /// leave the task's last run standing in for the task, and the book would
    /// then answer "what did the role that finished last spend" while its
    /// readers ask "what did this task cost" — the two differ by exactly the
    /// roles that came before. So the counters add and the timestamp is the
    /// last write, which is the only field where "latest" is the meaning.
    pub async fn record_metrics(&self, metrics: &TaskMetrics) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cog_observability_metrics
                (task_id, total_tokens, prompt_tokens, completion_tokens, tool_calls, iterations, duration_ms, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (task_id) DO UPDATE SET
                total_tokens      = cog_observability_metrics.total_tokens      + EXCLUDED.total_tokens,
                prompt_tokens     = cog_observability_metrics.prompt_tokens     + EXCLUDED.prompt_tokens,
                completion_tokens = cog_observability_metrics.completion_tokens + EXCLUDED.completion_tokens,
                tool_calls        = cog_observability_metrics.tool_calls        + EXCLUDED.tool_calls,
                iterations        = cog_observability_metrics.iterations        + EXCLUDED.iterations,
                duration_ms       = cog_observability_metrics.duration_ms       + EXCLUDED.duration_ms,
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
        // 随闭包一起走的一份句柄：返回的流是 `'static` 的，借不到 `self`。
        let lag_metrics = self.metrics.clone();

        let stream =
            futures::stream::iter(historical.into_iter().map(Ok)).chain(futures::stream::unfold(
                (rx, filter, lag_metrics),
                |(mut rx, filter, metrics)| async move {
                    loop {
                        match rx.recv().await {
                            Ok(event) => {
                                if crate::event_filter::event_matches(&filter, &event) {
                                    return Some((Ok(event), (rx, filter, metrics)));
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => return None,
                            // 落后不是坏掉：丢掉的几环补不回来，继续消费即可。但「漏了
                            // 几条」要有人记着——订阅方只看得见少掉的事件。
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                crate::consumer_lag::record_consumer_lag(&metrics, skipped).await;
                            }
                        }
                    }
                },
            ));

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

    async fn record_task_metrics(&self, metrics: TaskMetrics) -> SFResult<()> {
        self.record_metrics(&metrics).await
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
        // Task counts come from the task-state surface: the run census holds
        // only finished runs and carries no status, so it cannot say how many
        // tasks are queued or running — it can only say how many have run.
        let counts = match self.state_backend.as_ref() {
            Some(backend) => backend.task_status_counts().await?,
            None => None,
        };

        // Average over the runs that were accounted for. Only a finished run
        // has a duration, so the denominator is the census, not the task
        // inventory; NULL from AVG (no rows) stays None rather than 0.
        let avg_duration: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT AVG(duration_ms)::bigint FROM cog_observability_metrics")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!("get_cluster_overview avg duration failed: {}", e))
                })?;

        // No production writer exists for the squad table (only tests call
        // `update_squad_state`), so a count over it would be a zero nothing
        // produced. `None` says that; the day something writes squads, this
        // becomes a reading.
        let total_squads = None;
        let active_squads = None;

        let total_agents = self
            .count_or_none("SELECT COUNT(*) FROM cog_agent_states")
            .await;
        let active_agents = self
            .count_or_none("SELECT COUNT(*) FROM cog_agent_states WHERE state = 'active'")
            .await;

        Ok(ClusterOverview {
            total_agents,
            active_agents,
            total_tasks: counts.map(|c| c.total),
            active_tasks: counts.map(|c| c.active),
            queued_tasks: counts.map(|c| c.queued),
            failed_tasks: counts.map(|c| c.failed),
            avg_task_duration_ms: avg_duration
                .and_then(|row| row.0)
                .and_then(|d| u64::try_from(d).ok()),
            // The storage layer holds no alert evidence, so it cannot answer.
            // The surface that can — the process holding the alert source —
            // fills this in before delivering the overview.
            cluster_health: "unknown".into(),
            timestamp: Utc::now(),
            total_squads,
            active_squads,
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

    /// Publish to the in-process subscribers **and** persist the event, in one
    /// method.
    ///
    /// It used to be two: an inherent `publish_event` that did both and this
    /// trait arm that only sent to the broadcast. Production reaches the
    /// gateway as `Arc<dyn ObservabilityGateway>`, so it took the arm that does
    /// not write, and `cog_observability_events` stayed empty while nothing
    /// failed -- the send succeeded, no warning fired, and the table read as
    /// "no events happened". The two implementations had the same name, so
    /// every call site looked like it persisted.
    ///
    /// The write is spawned so the caller never blocks on PostgreSQL: this sits
    /// on the event-gateway-sink path, and a subscriber that blocks there slows
    /// the broadcast it is reading from.
    ///
    /// A failed write is counted, not only logged. The log line dies with the
    /// pod, and the reading that matters afterwards -- "the events were
    /// delivered and the table still did not grow" -- has to survive that, or
    /// the next reader repeats the search that found this bug. The count is
    /// what turns the consumer's `handoff_total{stage="attempted"}` into "how
    /// many reached the table": it is one per failed event, while the handoff
    /// is one per pass, so it gets its own name rather than a label on a
    /// counter with a different unit.
    fn publish_event(&self, event: AgentEvent) {
        let _ = self.event_tx.send(event.clone());

        let pool = self.pool.clone();
        let metrics = self.metrics.clone();
        let failed_pending = Arc::clone(&self.failed_pending);
        let failed_last_flush = Arc::clone(&self.failed_last_flush);
        tokio::spawn(async move {
            if let Err(e) = Self::persist_event(&pool, &event).await {
                tracing::warn!("Failed to persist observability event: {}", e);
                if let Some(metrics) = metrics {
                    note_persist_failure(&metrics, &failed_pending, &failed_last_flush).await;
                }
            }
        });
    }
}

/// 记一次落库失败：本地加一，最多每 [`FAILURE_FLUSH_SECS`] 秒落一次库。
///
/// 为什么不是「失败一次写一次」：这条计数和失败的插入写在同一张表的同一行上
/// （`cog_metric_counter_totals` 按 `(name, labels)` upsert），一次洪水里每失败
/// 一条就再排一次队，于是失败越多越堵、越堵越失败。2026-10-18 现场实测：15 条
/// 会话卡在同一个 upsert 上，池被占满，而失败在继续产生新的写。
///
/// 代价写在明处：进程在这 5 秒里死掉，待落的条数随之丢失；把这批数写回库的那次
/// 调用本身失败时也丢失（它要和失败写抢同一张表，这是无法回避的）。这两处都只影响
/// 「计数的精度」，不影响它回答的那个问题——这一段有没有失败。计数器是累加的
/// （`record_counter` 是加不是赋），所以分批落不会改变总量。
async fn note_persist_failure(
    metrics: &Arc<dyn cog_core::MetricsBackend>,
    pending: &std::sync::atomic::AtomicU64,
    last: &std::sync::Mutex<std::time::Instant>,
) {
    use std::sync::atomic::Ordering;

    pending.fetch_add(1, Ordering::Relaxed);
    {
        // 这段不 await：std 的锁跨 await 会同时挡住 tokio 的任务迁移和下一个失败。
        let mut last = match last.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if last.elapsed() < std::time::Duration::from_secs(FAILURE_FLUSH_SECS) {
            return;
        }
        *last = std::time::Instant::now();
    }
    let batch = pending.swap(0, Ordering::Relaxed);
    if batch == 0 {
        return;
    }
    let _ = metrics
        .record_counter(
            cog_core::metric_names::OBSERVABILITY_EVENT_PERSIST_FAILED_TOTAL,
            batch as f64,
            std::collections::HashMap::new(),
        )
        .await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::MetricsBackend;

    fn gateway_with(
        metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    ) -> PostgresObservabilityGateway {
        // No connection is made: the seed only touches the metrics backend, and
        // a test that needed a live server would not run where this question
        // gets asked.
        let pool =
            PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/nothing").expect("a lazy pool");
        PostgresObservabilityGateway::new(pool).with_metrics(metrics)
    }

    /// 失败按批落库：同一窗口里的第二次失败不再写库。
    ///
    /// 这条钉的是**写入口的分寸**，不是计数本身——一次洪水里每个失败都 upsert
    /// 同一行，会把记账变成第二个故障源。窗口内的失败只加本地计数，到点才落一次，
    /// 落下去的是这段时间的总数（计数器是加不是赋）。
    #[tokio::test]
    async fn failures_are_recorded_once_per_window_not_once_per_failure() {
        use std::sync::atomic::AtomicU64;
        use std::time::{Duration as StdDuration, Instant};

        let backend = Arc::new(crate::mem::MemoryMetricsBackend::new());
        let metrics: Arc<dyn cog_core::MetricsBackend> = backend.clone();
        let pending = AtomicU64::new(0);
        // 窗口已经到期，所以第一次失败就该落库。
        let last = std::sync::Mutex::new(Instant::now() - StdDuration::from_secs(60));
        let name = cog_core::metric_names::OBSERVABILITY_EVENT_PERSIST_FAILED_TOTAL.as_str();

        note_persist_failure(&metrics, &pending, &last).await;
        let totals = backend.query_counter_totals(name).await.unwrap();
        assert_eq!(totals.len(), 1, "第一次失败要落一条");
        assert_eq!(totals[0].value, 1.0, "落的是这一窗口里的失败条数");

        // 同一窗口里的第二次失败只加本地计数，不写库。
        note_persist_failure(&metrics, &pending, &last).await;
        let totals = backend.query_counter_totals(name).await.unwrap();
        assert_eq!(
            totals[0].value, 1.0,
            "窗口没到就再写一次库，等于把每个失败都变成一次热行写入"
        );

        // 窗口到期，攒下的两条一起落，总数是累加的。
        *last.lock().unwrap() = Instant::now() - StdDuration::from_secs(60);
        note_persist_failure(&metrics, &pending, &last).await;
        let totals = backend.query_counter_totals(name).await.unwrap();
        assert_eq!(
            totals[0].value, 3.0,
            "攒下的失败要一条不少地加上去：1 次已落 ＋ 1 次待落 ＋ 1 次本次"
        );
    }

    /// 装配完成的写者要在读数面上留下一条 0。没有这一条，一个从未失败的
    /// 存储与一个根本没接上的存储导出同一份空读数。
    #[tokio::test]
    async fn the_writer_declares_the_persist_failure_series_at_zero() {
        let backend = Arc::new(crate::mem::MemoryMetricsBackend::new());
        let metrics: Arc<dyn cog_core::MetricsBackend> = backend.clone();

        gateway_with(Some(metrics))
            .seed_persist_failure_series()
            .await;

        let totals = backend
            .query_counter_totals(
                cog_core::metric_names::OBSERVABILITY_EVENT_PERSIST_FAILED_TOTAL.as_str(),
            )
            .await
            .expect("the in-memory backend answers");
        assert_eq!(totals.len(), 1, "the seed must create exactly one series");
        assert_eq!(totals[0].value, 0.0, "the seed must not count a failure");
        assert!(
            totals[0].labels.is_empty(),
            "the seed's label set must be the one the failure arm writes, \
             otherwise it declares a different series"
        );
    }

    /// 没有指标后端的部署（嵌入式运行）既不写也不报错：能记的东西本来就不
    /// 存在，这不是故障，启动不该为它停下。
    #[tokio::test]
    async fn a_writer_without_a_backend_seeds_nothing_and_survives() {
        gateway_with(None).seed_persist_failure_series().await;
    }
}
