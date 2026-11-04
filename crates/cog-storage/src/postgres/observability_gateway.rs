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

use super::state_reads::{
    agent_state_from_json, checkpoint_from_row, AgentStateRow, CheckpointRow, AGENT_STATE_SQL,
    CHECKPOINT_SQL,
};

// ─── DDL ───

/// How many log lines one task keeps.
///
/// Sized by what the route is for: a per-task log answers "what happened at the
/// end of this run", and the run report is its unit. Two hundred is about a
/// year of a task that runs daily, and leaves a task that runs in a loop for an
/// hour with the last few hours of its own history. The bound is per task, so
/// the table's size is the number of live tasks times this — not the platform's
/// uptime times its run rate, which is what an unbounded append-only table is.
///
/// It is a cap, not a retention period: a quiet task keeps its lines however
/// old they are, because nothing here has an opinion about age. A deployment
/// that wants the other rule needs a sweep with a clock in it, which this is
/// deliberately not.
// cap: bound — from what a run report is for (a year of daily runs), not from a
// measurement on any host
pub const LOG_LINES_PER_TASK: i64 = 200;

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
    /// 正在排队的待落事件。见 [`EVENT_BATCH_MAX`]：这条通路每秒能产生几百条
    /// 事件，一条一次 INSERT 就是几百次提交，而提交要等 WAL 落盘。
    persist_tx: tokio::sync::mpsc::Sender<AgentEvent>,
    /// 队列的收端，由写者任务取走一次。放在锁里是因为写者要在第一次发布时才
    /// 起（[`PostgresObservabilityGateway::new`] 是同步函数，可能不在运行时里）。
    persist_rx: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<AgentEvent>>>>,
    /// 写者任务起来了没有。
    persist_writer_started: Arc<std::sync::Once>,
}

/// 一批最多写几条。
///
/// 这不是从容量导出的数：它由「一次提交的固定成本摊到多少条上才不亏」定。
/// 按本机实测的行宽估（2026-11-04，一条事件的行约 300 字节），64 条一次提交，
/// 一次提交的 WAL 页与提交记录摊到 64 条上就低到可以忽略；再往上加行，省下的
/// 提交成本已经小于一条提交持有连接、占着 WAL 的时间，而队列满时整批变慢的
/// 方差更大。
// cap: bound — 使一次提交的固定成本摊到 64 条上可忽略，不是从任何容量读数导出
const EVENT_BATCH_MAX: usize = 64;

/// 第一条到了之后最多再等多久凑批。
///
/// 事件是持续来的（实测约每秒 180 条），凑够一批通常不用等；这个等待只对
/// 安静时段起作用，让单条事件不因为「等下一批」而延迟上分钟。
// cap: bound — 与事件到达间隔（毫秒级）相比足够短，只用来收尾一批
const EVENT_BATCH_LINGER_MS: u64 = 50;

/// 队列能排多少条。满了不丢事件，改为立刻单独写一条（见
/// [`PostgresObservabilityGateway::publish_event`]）。
///
/// 按「数据库停住多久还不丢」定：每秒约 180 条时，四千条约是 22 秒的积压。
// cap: bound — 按事件速率（约 180/秒）乘一段可等待的数据库停顿估出
const EVENT_WRITE_QUEUE: usize = 4096;

/// 失败计数最多多久落一次库。选 5 秒是因为它要明显短于一条告警的判定窗
/// （分钟级），又足以把「每次失败一行」压成「每 5 秒一行」。
// cap: bound — 要明显短于一条告警的判定窗（分钟级），又够把「每次失败一行」压成「每 5 秒一行」
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
        let (persist_tx, persist_rx) = tokio::sync::mpsc::channel(EVENT_WRITE_QUEUE);
        Self {
            pool,
            event_tx,
            event_channel_capacity: 256,
            state_backend: None,
            metrics: None,
            failed_pending: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            failed_last_flush: Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
            persist_tx,
            persist_rx: Arc::new(std::sync::Mutex::new(Some(persist_rx))),
            persist_writer_started: Arc::new(std::sync::Once::new()),
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

    /// Apply the observability schema. Safe to call repeatedly.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::raw_sql(OBSERVABILITY_DDL)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("observability schema init failed: {}", e)))?;
        Ok(())
    }

    /// Record a structured log entry for the given task.
    ///
    /// The insert is followed by a trim that keeps the task's newest
    /// [`LOG_LINES_PER_TASK`] rows. Without it this table is append-only and
    /// its size is the platform's uptime times its run rate — the shape the
    /// event log already had to be bounded for, on a table a thousand times
    /// larger.
    ///
    /// Per task rather than per table because the reader asks in tasks: it
    /// names one task id and a count. A table-wide capacity would let one busy
    /// task push a quiet task's only line out, and that task's reader would
    /// then get an empty list — the exact false answer this table exists to
    /// stop giving.
    ///
    /// The trim is a separate statement from the insert because one statement
    /// cannot see its own CTE's rows: a `DELETE` beside the `INSERT` in the
    /// same statement counts the table as it was before the row landed and
    /// would therefore take one line too many. Two statements can leave the
    /// task one row over its bound between them, which is a row a reader can
    /// see and the next write removes; losing the newest line is not.
    ///
    /// A failed trim does not fail the write. The line is in; the bound is the
    /// part that did not hold, and saying so is more useful to the caller than
    /// telling it the write it can already read back did not happen.
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

        if let Err(e) = sqlx::query(
            r#"
            DELETE FROM cog_observability_logs
            WHERE task_id = $1
              AND id NOT IN (
                  SELECT id FROM cog_observability_logs
                  WHERE task_id = $1
                  ORDER BY id DESC
                  LIMIT $2
              )
            "#,
        )
        .bind(task_id)
        .bind(LOG_LINES_PER_TASK)
        .execute(&self.pool)
        .await
        {
            tracing::warn!(
                task_id,
                error = %e,
                kept = LOG_LINES_PER_TASK,
                "this task's log lines were written but not trimmed to the bound"
            );
        }
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

    /// 把一批事件写成**一次**提交。
    ///
    /// 一条一次 INSERT 与一次批量 INSERT 的差别不在语句，在提交：每次提交都要
    /// 等 WAL 落盘，而这条通路实测每秒约 180 条事件，就是一秒 180 次「等盘」。
    /// 宿主同时跑着构建和集群时，每次等盘的延迟被拉长到秒级，池里的连接全被
    /// 这些提交占着，读的人拿到连接要等十秒——2026-11-04 现场就是这样：慢的不
    /// 是这条插入的语句时间，而是它背后那次提交。
    ///
    /// 一批在同一个事务里，所以「整批都没写进去」与「整批都写进去了」只有两种
    /// 结局，没有写了一半的一批：失败按条数计（见调用方），不按批计。
    async fn persist_events(pool: &PgPool, events: &[AgentEvent]) -> SFResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        // 先序列化再建语句：`push_values` 的闭包不能返回错误，把可能失败的
        // 这一步放在外面，一条坏事件不会让半个批进入语句构造。
        let mut rows = Vec::with_capacity(events.len());
        for event in events {
            let (event_type, agent_id, task_id, crew_id, squad_id, timestamp) =
                extract_event_meta(event);
            let payload = serde_json::to_value(event).map_err(SFError::Serialization)?;
            rows.push((
                event_type, agent_id, task_id, crew_id, squad_id, payload, timestamp,
            ));
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("persist_events begin failed: {}", e)))?;
        let mut qb = sqlx::QueryBuilder::new(
            "INSERT INTO cog_observability_events \
             (event_type, agent_id, task_id, crew_id, squad_id, payload, timestamp) ",
        );
        qb.push_values(rows, |mut b, row| {
            b.push_bind(row.0)
                .push_bind(row.1)
                .push_bind(row.2)
                .push_bind(row.3)
                .push_bind(row.4)
                .push_bind(row.5)
                .push_bind(row.6);
        });
        qb.build()
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("persist_events insert failed: {}", e)))?;
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("persist_events commit failed: {}", e)))?;
        Ok(())
    }

    async fn query_historical_events(&self, filter: &EventFilter) -> SFResult<Vec<AgentEvent>> {
        // The default window is the same constant the capacity sweep may not
        // delete inside. Two spellings of one promise is how a sweep comes to
        // trim a row this query was still going to ask for, and the gap it
        // leaves is silent: an event that is gone reads exactly like an event
        // that never happened.
        let since = filter.since.unwrap_or_else(|| {
            Utc::now()
                - Duration::seconds(crate::observability_event_cap::DEFAULT_READ_WINDOW_SECS as i64)
        });

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
        let row: Option<AgentStateRow> = sqlx::query_as(AGENT_STATE_SQL)
            .bind(agent_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("get_agent_state failed: {}", e)))?;

        match row {
            Some((document,)) => agent_state_from_json(&document),
            None => Err(SFError::Agent(format!("agent {} not found", agent_id))),
        }
    }

    async fn get_task_checkpoint(&self, task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
        let row: Option<CheckpointRow> = sqlx::query_as(CHECKPOINT_SQL)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("get_task_checkpoint failed: {}", e)))?;

        row.map(|row| checkpoint_from_row(task_id, row)).transpose()
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

    async fn record_task_log(&self, task_id: &str, entry: LogEntry) -> SFResult<()> {
        self.record_log(task_id, entry).await
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

        // No production writer fills `cog_agent_states`: an agent announces
        // itself into the shared registry and beats there, and only the
        // supervisor's own heartbeat path ever called the state backend that
        // would write this table. A count over it is a zero nothing produced,
        // and printing it next to a roster of live workers says the fleet is
        // empty. `None` says "not measured here"; the surface that holds the
        // registry fills both cells from it before delivering the overview.
        //
        // The old `WHERE state = 'active'` was wrong twice over: the column
        // holds a serialized `AgentState`, so a bare string never matches even
        // once rows appear.
        let total_agents = None;
        let active_agents = None;

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
            cluster_health: cog_core::ClusterHealth::Unknown,
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
    /// The write never blocks the caller: this sits on the event-gateway-sink
    /// path, and a subscriber that blocks there slows the broadcast it is
    /// reading from. It does not write one row per call either — the event goes
    /// on a queue that a single writer drains in batches (see
    /// [`run_event_writer`]), because a commit per event turns the event rate
    /// into a commit rate and every commit waits on the WAL.
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

        self.start_event_writer();

        // 队列满时**不丢**，改为立刻单独写这一条：排队的代价由这一条自己承担，
        // 而不是把它从表里抹掉——表少一条与「这件事没发生」在读的人眼里同形。
        match self.persist_tx.try_send(event) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
                self.write_one_now(event, "the write queue is full");
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(event)) => {
                // 只会发生在写者任务已经退出之后（本进程不再收事件）；同样不丢。
                self.write_one_now(event, "the batch writer is gone");
            }
        }
    }
}

impl PostgresObservabilityGateway {
    /// 第一次发布时把写者任务起来。取收端这件事只做一次；`Once` 保证并发发布
    /// 不会起出第二个写者（两个写者对同一队列取事件会互相偷，批就永远凑不满）。
    fn start_event_writer(&self) {
        self.persist_writer_started.call_once(|| {
            let rx = match self.persist_rx.lock() {
                Ok(mut slot) => slot.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(rx) = rx {
                tokio::spawn(run_event_writer(
                    self.pool.clone(),
                    rx,
                    self.metrics.clone(),
                    Arc::clone(&self.failed_pending),
                    Arc::clone(&self.failed_last_flush),
                ));
            }
        });
    }

    /// 越过队列直接写一条，理由写进告警里：这条通路自己失败了，读的人要知道。
    fn write_one_now(&self, event: AgentEvent, why: &'static str) {
        tracing::warn!("writing one observability event outside the batch queue: {why}");
        let pool = self.pool.clone();
        let metrics = self.metrics.clone();
        let failed_pending = Arc::clone(&self.failed_pending);
        let failed_last_flush = Arc::clone(&self.failed_last_flush);
        tokio::spawn(async move {
            if let Err(e) = PostgresObservabilityGateway::persist_events(&pool, &[event]).await {
                tracing::warn!("Failed to persist observability event: {}", e);
                if let Some(metrics) = metrics {
                    note_persist_failure(&metrics, &failed_pending, &failed_last_flush).await;
                }
            }
        });
    }
}

/// 批量落库的写者：收够一批（或等够 [`EVENT_BATCH_LINGER_MS`]）就写一次。
///
/// 一开始一条就写一条、每条各起一个任务。事件是持续来的，那样等于把「每秒多少
/// 条事件」直接变成「每秒多少次提交」，而每次提交都要等 WAL 落盘；宿主被构建和
/// 集群同时占着的时候，连接就都被这些提交占住，读的人拿连接要等十秒。
///
/// 队列关了才退出：进程收尾时最后一批照样写出去。
async fn run_event_writer(
    pool: PgPool,
    mut rx: tokio::sync::mpsc::Receiver<AgentEvent>,
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    failed_pending: Arc<std::sync::atomic::AtomicU64>,
    failed_last_flush: Arc<std::sync::Mutex<std::time::Instant>>,
) {
    let linger = std::time::Duration::from_millis(EVENT_BATCH_LINGER_MS);
    let mut batch: Vec<AgentEvent> = Vec::with_capacity(EVENT_BATCH_MAX);
    loop {
        // 第一条阻塞等，之后最多再等一个 linger 把这一批凑满。
        let Some(first) = rx.recv().await else {
            return;
        };
        batch.clear();
        batch.push(first);
        let deadline = tokio::time::Instant::now() + linger;
        while batch.len() < EVENT_BATCH_MAX {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(event)) => batch.push(event),
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let written = batch.len() as u64;
        if let Err(e) = PostgresObservabilityGateway::persist_events(&pool, &batch).await {
            tracing::warn!(
                events = written,
                error = %e,
                "Failed to persist a batch of observability events"
            );
            if let Some(ref metrics) = metrics {
                for _ in 0..written {
                    note_persist_failure(metrics, &failed_pending, &failed_last_flush).await;
                }
            }
        }
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
        // 一次写没有执行者、没有任务：这一行记的是「哪个前缀下的东西被改了」。
        AgentEvent::ResourceChanged { .. } => (None, None, None, None),
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
        | AgentEvent::CheckpointSaved { timestamp, .. }
        | AgentEvent::ResourceChanged { timestamp, .. } => *timestamp,
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
