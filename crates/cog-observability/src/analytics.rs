/// Analytics backend for event tracking and OLAP queries.
/// - Phase 1: PostgreSQL(JSONB) fallback available
/// - Phase 2: ClickHouse for high-volume analytics (>100M events/month)
///   **Machine layer**: Structured event ingestion for downstream dashboards.
///   **Human layer**: Analytics API for user behavior and system metrics.
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{HttpClient, HttpRequest};

/// Analytics event record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AnalyticsEvent {
    pub event_id: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub properties: HashMap<String, Value>,
}

impl AnalyticsEvent {
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type: event_type.into(),
            timestamp: Utc::now(),
            user_id: None,
            session_id: None,
            task_id: None,
            agent_id: None,
            properties: HashMap::new(),
        }
    }

    pub fn user_id(mut self, id: impl Into<String>) -> Self {
        self.user_id = Some(id.into());
        self
    }

    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.session_id = Some(id.into());
        self
    }

    pub fn task_id(mut self, id: impl Into<String>) -> Self {
        self.task_id = Some(id.into());
        self
    }

    pub fn agent_id(mut self, id: impl Into<String>) -> Self {
        self.agent_id = Some(id.into());
        self
    }

    pub fn property(mut self, key: impl Into<String>, value: Value) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}

/// Analytics storage backend trait.
#[async_trait::async_trait]
pub trait AnalyticsBackend: Send + Sync {
    /// Insert a single analytics event.
    async fn insert_event(&self, event: AnalyticsEvent) -> anyhow::Result<()>;

    /// Insert a batch of analytics events.
    async fn insert_batch(&self, events: Vec<AnalyticsEvent>) -> anyhow::Result<()>;

    /// Query events with optional filters.
    async fn query_events(
        &self,
        event_type: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: usize,
    ) -> anyhow::Result<Vec<AnalyticsEvent>>;

    /// Aggregate count by event type in a time range.
    async fn aggregate_count(
        &self,
        event_type: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        granularity: Granularity,
    ) -> anyhow::Result<Vec<TimeBucket>>;

    /// Health check.
    async fn health_check(&self) -> bool;
}

/// Time granularity for aggregations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Granularity {
    Minute,
    Hour,
    Day,
}

impl Granularity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Granularity::Minute => "minute",
            Granularity::Hour => "hour",
            Granularity::Day => "day",
        }
    }

    pub fn clickhouse_format(&self) -> &'static str {
        match self {
            Granularity::Minute => "%Y-%m-%d %H:%M:00",
            Granularity::Hour => "%Y-%m-%d %H:00:00",
            Granularity::Day => "%Y-%m-%d 00:00:00",
        }
    }
}

/// A single time-bucketed aggregation result.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TimeBucket {
    pub bucket_time: DateTime<Utc>,
    pub count: u64,
}

// ─── ClickHouse Implementation ────────────────────────────────────

/// ClickHouse HTTP-based analytics backend.
/// Communicates via the ClickHouse HTTP interface (default port 8123).
/// Supports JSONEachRow format for inserts and JSON format for queries.
pub struct ClickHouseAnalyticsBackend {
    client: Option<Arc<dyn HttpClient>>,
    base_url: String,
    database: String,
    table: String,
    username: String,
    password: String,
}

impl ClickHouseAnalyticsBackend {
    pub fn new(base_url: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            client: None,
            base_url: base_url.into(),
            database: database.into(),
            table: "analytics_events".into(),
            username: "default".into(),
            password: String::new(),
        }
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = username.into();
        self.password = password.into();
        self
    }

    pub fn with_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn url(&self) -> String {
        format!("{}/?database={}", self.base_url, self.database)
    }

    fn auth_header(&self) -> Option<(String, String)> {
        if self.password.is_empty() {
            None
        } else {
            let creds = format!("{}:{}", self.username, self.password);
            let encoded = general_purpose::STANDARD.encode(creds);
            Some(("Authorization".into(), format!("Basic {}", encoded)))
        }
    }

    fn client(&self) -> anyhow::Result<&Arc<dyn HttpClient>> {
        self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("ClickHouseAnalyticsBackend has no HttpClient configured")
        })
    }

    /// Initialize the analytics table if it does not exist.
    pub async fn init_table(&self) -> anyhow::Result<()> {
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.{} (
                event_id String,
                event_type LowCardinality(String),
                timestamp DateTime64(3),
                user_id Nullable(String),
                session_id Nullable(String),
                task_id Nullable(String),
                agent_id Nullable(String),
                properties String
            ) ENGINE = MergeTree()
            ORDER BY (event_type, timestamp)
            PARTITION BY toYYYYMM(timestamp)
            TTL timestamp + INTERVAL 1 YEAR
            SETTINGS index_granularity = 8192
            "#,
            self.database, self.table
        );

        let mut req = HttpRequest::post(&self.base_url).body(ddl.into_bytes());
        if let Some((k, v)) = self.auth_header() {
            req = req.header(k, v);
        }

        let resp = self.client()?.execute(req).await?;
        if !resp.is_success() {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("ClickHouse init_table failed: {}", text);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl AnalyticsBackend for ClickHouseAnalyticsBackend {
    async fn insert_event(&self, event: AnalyticsEvent) -> anyhow::Result<()> {
        self.insert_batch(vec![event]).await
    }

    async fn insert_batch(&self, events: Vec<AnalyticsEvent>) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut rows = Vec::with_capacity(events.len());
        for ev in events {
            let properties = serde_json::to_string(&ev.properties)?;
            let row = serde_json::json!({
                "event_id": ev.event_id,
                "event_type": ev.event_type,
                "timestamp": ev.timestamp.timestamp_millis(),
                "user_id": ev.user_id,
                "session_id": ev.session_id,
                "task_id": ev.task_id,
                "agent_id": ev.agent_id,
                "properties": properties,
            });
            rows.push(row);
        }

        let body = rows
            .into_iter()
            .map(|r| serde_json::to_string(&r))
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");

        let url = format!(
            "{}/?query=INSERT+INTO+{}+FORMAT+JSONEachRow",
            self.url(),
            self.table
        );
        let mut req = HttpRequest::post(&url).body(body.into_bytes());
        if let Some((k, v)) = self.auth_header() {
            req = req.header(k, v);
        }

        let resp = self.client()?.execute(req).await?;
        if !resp.is_success() {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("ClickHouse insert_batch failed: {}", text);
        }

        Ok(())
    }

    async fn query_events(
        &self,
        event_type: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: usize,
    ) -> anyhow::Result<Vec<AnalyticsEvent>> {
        let mut conditions = Vec::new();
        if let Some(et) = event_type {
            conditions.push(format!("event_type = '{}'", escape_clickhouse_string(et)));
        }
        if let Some(s) = start {
            conditions.push(format!(
                "timestamp >= toDateTime64({}, 3)",
                s.timestamp_millis()
            ));
        }
        if let Some(e) = end {
            conditions.push(format!(
                "timestamp <= toDateTime64({}, 3)",
                e.timestamp_millis()
            ));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            SELECT event_id, event_type, timestamp, user_id, session_id, task_id, agent_id, properties
            FROM {} {}
            ORDER BY timestamp DESC
            LIMIT {}
            FORMAT JSON
            "#,
            self.table, where_clause, limit
        );

        let mut req = HttpRequest::post(self.url()).body(query.into_bytes());
        if let Some((k, v)) = self.auth_header() {
            req = req.header(k, v);
        }

        let resp = self.client()?.execute(req).await?;
        if !resp.is_success() {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("ClickHouse query_events failed: {}", text);
        }

        let json: Value = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        let data = json
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        let mut events = Vec::new();
        for row in data {
            let properties_str = row["properties"].as_str().unwrap_or("{}");
            let properties: HashMap<String, Value> =
                serde_json::from_str(properties_str).unwrap_or_default();

            events.push(AnalyticsEvent {
                event_id: row["event_id"].as_str().unwrap_or("").to_string(),
                event_type: row["event_type"].as_str().unwrap_or("").to_string(),
                timestamp: parse_clickhouse_datetime(&row["timestamp"]),
                user_id: row["user_id"].as_str().map(String::from),
                session_id: row["session_id"].as_str().map(String::from),
                task_id: row["task_id"].as_str().map(String::from),
                agent_id: row["agent_id"].as_str().map(String::from),
                properties,
            });
        }

        Ok(events)
    }

    async fn aggregate_count(
        &self,
        event_type: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        granularity: Granularity,
    ) -> anyhow::Result<Vec<TimeBucket>> {
        let fmt = granularity.clickhouse_format();
        let query = format!(
            r#"
            SELECT
                toDateTime(formatDateTime(timestamp, '{}')) AS bucket_time,
                count() AS cnt
            FROM {}
            WHERE event_type = '{}' AND timestamp >= toDateTime64({}, 3) AND timestamp <= toDateTime64({}, 3)
            GROUP BY bucket_time
            ORDER BY bucket_time ASC
            FORMAT JSON
            "#,
            fmt,
            self.table,
            escape_clickhouse_string(event_type),
            start.timestamp_millis(),
            end.timestamp_millis(),
        );

        let mut req = HttpRequest::post(self.url()).body(query.into_bytes());
        if let Some((k, v)) = self.auth_header() {
            req = req.header(k, v);
        }

        let resp = self.client()?.execute(req).await?;
        if !resp.is_success() {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("ClickHouse aggregate_count failed: {}", text);
        }

        let json: Value = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        let data = json
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        let mut buckets = Vec::new();
        for row in data {
            buckets.push(TimeBucket {
                bucket_time: parse_clickhouse_datetime(&row["bucket_time"]),
                count: row["cnt"].as_u64().unwrap_or(0),
            });
        }

        Ok(buckets)
    }

    async fn health_check(&self) -> bool {
        let mut req = HttpRequest::post(self.url()).body(b"SELECT 1 FORMAT JSON".to_vec());
        if let Some((k, v)) = self.auth_header() {
            req = req.header(k, v);
        }

        match self.client() {
            Ok(client) => match client.execute(req).await {
                Ok(resp) => resp.is_success(),
                Err(e) => {
                    tracing::warn!("ClickHouse health check failed: {}", e);
                    false
                }
            },
            Err(_) => {
                tracing::warn!("ClickHouse health check failed: no HttpClient configured");
                false
            }
        }
    }
}

fn escape_clickhouse_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

fn parse_clickhouse_datetime(value: &Value) -> DateTime<Utc> {
    value
        .as_str()
        .and_then(|s| {
            DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|d| d.with_timezone(&Utc))
        })
        .or_else(|| {
            value.as_str().and_then(|s| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .ok()
                    .map(|ndt| DateTime::from_naive_utc_and_offset(ndt, Utc))
            })
        })
        .unwrap_or_else(Utc::now)
}

// ─── PostgreSQL Fallback Implementation ───────────────────────────

/// PostgreSQL-backed analytics backend for Phase 1.
/// Stores events in a JSONB table, suitable for moderate volumes
/// (< 100M events/month).  The design doc recommends ClickHouse
/// for higher volumes.
pub struct PostgresAnalyticsBackend {
    pool: sqlx::PgPool,
    table: String,
}

impl PostgresAnalyticsBackend {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            table: "analytics_events".into(),
        }
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// Initialize the events table.
    pub async fn init_table(&self) -> anyhow::Result<()> {
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                event_id VARCHAR(64) NOT NULL UNIQUE,
                event_type VARCHAR(32) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                user_id VARCHAR(64),
                session_id VARCHAR(64),
                task_id VARCHAR(64),
                agent_id VARCHAR(64),
                properties JSONB NOT NULL DEFAULT '{{}}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_{}_type_time ON {}(event_type, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_{}_time ON {}(timestamp DESC);
            "#,
            self.table, self.table, self.table, self.table, self.table
        );
        sqlx::query(&ddl).execute(&self.pool).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AnalyticsBackend for PostgresAnalyticsBackend {
    async fn insert_event(&self, event: AnalyticsEvent) -> anyhow::Result<()> {
        self.insert_batch(vec![event]).await
    }

    async fn insert_batch(&self, events: Vec<AnalyticsEvent>) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        for ev in events {
            sqlx::query(&format!(
                r#"
                INSERT INTO {} (event_id, event_type, timestamp, user_id, session_id, task_id, agent_id, properties)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (event_id) DO NOTHING
                "#,
                self.table
            ))
            .bind(&ev.event_id)
            .bind(&ev.event_type)
            .bind(ev.timestamp)
            .bind(&ev.user_id)
            .bind(&ev.session_id)
            .bind(&ev.task_id)
            .bind(&ev.agent_id)
            .bind(serde_json::to_value(&ev.properties)?)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn query_events(
        &self,
        event_type: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: usize,
    ) -> anyhow::Result<Vec<AnalyticsEvent>> {
        let mut conditions = Vec::new();
        let mut params_count = 0usize;

        if event_type.is_some() {
            params_count += 1;
            conditions.push(format!("event_type = ${}", params_count));
        }
        if start.is_some() {
            params_count += 1;
            conditions.push(format!("timestamp >= ${}", params_count));
        }
        if end.is_some() {
            params_count += 1;
            conditions.push(format!("timestamp <= ${}", params_count));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            r#"
            SELECT event_id, event_type, timestamp, user_id, session_id, task_id, agent_id, properties
            FROM {}
            {}
            ORDER BY timestamp DESC
            LIMIT {}
            "#,
            self.table, where_clause, limit
        );

        let mut query = sqlx::query_as::<_, AnalyticsRow>(&sql);
        if let Some(et) = event_type {
            query = query.bind(et);
        }
        if let Some(s) = start {
            query = query.bind(s);
        }
        if let Some(e) = end {
            query = query.bind(e);
        }

        let rows: Vec<AnalyticsRow> = query.fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| r.into_event()).collect())
    }

    async fn aggregate_count(
        &self,
        event_type: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        granularity: Granularity,
    ) -> anyhow::Result<Vec<TimeBucket>> {
        let trunc = match granularity {
            Granularity::Minute => "minute",
            Granularity::Hour => "hour",
            Granularity::Day => "day",
        };

        let sql = format!(
            r#"
            SELECT date_trunc('{}', timestamp) AS bucket_time, COUNT(*) AS cnt
            FROM {}
            WHERE event_type = $1 AND timestamp >= $2 AND timestamp <= $3
            GROUP BY bucket_time
            ORDER BY bucket_time ASC
            "#,
            trunc, self.table
        );

        let rows: Vec<(chrono::NaiveDateTime, i64)> = sqlx::query_as(&sql)
            .bind(event_type)
            .bind(start)
            .bind(end)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(|(naive, count)| TimeBucket {
                bucket_time: DateTime::from_naive_utc_and_offset(naive, Utc),
                count: count as u64,
            })
            .collect())
    }

    async fn health_check(&self) -> bool {
        sqlx::query("SELECT 1").fetch_one(&self.pool).await.is_ok()
    }
}

#[derive(sqlx::FromRow)]
struct AnalyticsRow {
    event_id: String,
    event_type: String,
    timestamp: DateTime<Utc>,
    user_id: Option<String>,
    session_id: Option<String>,
    task_id: Option<String>,
    agent_id: Option<String>,
    properties: Value,
}

impl AnalyticsRow {
    fn into_event(self) -> AnalyticsEvent {
        AnalyticsEvent {
            event_id: self.event_id,
            event_type: self.event_type,
            timestamp: self.timestamp,
            user_id: self.user_id,
            session_id: self.session_id,
            task_id: self.task_id,
            agent_id: self.agent_id,
            properties: self
                .properties
                .as_object()
                .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
        }
    }
}

// ─── Background Event Buffer ───────────────────────────────────────

/// Background task that buffers analytics events and flushes them to
/// ClickHouse in batches.
/// Events are collected via an unbounded channel and flushed either when
/// the batch reaches `max_batch_size` or every `flush_interval`.
pub struct ClickHouseEventBuffer {
    tx: tokio::sync::mpsc::UnboundedSender<AnalyticsEvent>,
}

impl ClickHouseEventBuffer {
    pub fn new(
        backend: std::sync::Arc<ClickHouseAnalyticsBackend>,
        flush_interval: std::time::Duration,
        max_batch_size: usize,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AnalyticsEvent>();

        tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(max_batch_size);
            let mut interval = tokio::time::interval(flush_interval);

            loop {
                tokio::select! {
                    Some(event) = rx.recv() => {
                        buffer.push(event);
                        if buffer.len() >= max_batch_size {
                            let batch = std::mem::replace(
                                &mut buffer,
                                Vec::with_capacity(max_batch_size),
                            );
                            if let Err(e) = backend.insert_batch(batch).await {
                                tracing::warn!("ClickHouse background flush failed: {}", e);
                            }
                        }
                    }
                    _ = interval.tick() => {
                        if !buffer.is_empty() {
                            let batch = std::mem::replace(
                                &mut buffer,
                                Vec::with_capacity(max_batch_size),
                            );
                            if let Err(e) = backend.insert_batch(batch).await {
                                tracing::warn!("ClickHouse background flush failed: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub fn send(&self, event: AnalyticsEvent) {
        let _ = self.tx.send(event);
    }
}
