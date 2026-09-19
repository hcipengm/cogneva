use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{MetricSample, MetricType, MetricsBackend, SFError, SFResult};

/// PostgreSQL-backed metrics backend.
pub struct PostgresMetricsBackend {
    pool: PgPool,
}

impl PostgresMetricsBackend {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the required table and indexes if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_metrics_samples (
                id SERIAL PRIMARY KEY,
                metric_type TEXT NOT NULL,
                name TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_metrics_name_type ON cog_metrics_samples(name, metric_type)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_metrics_timestamp ON cog_metrics_samples(timestamp)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // Cumulative counter totals live in their own row per label set and are
        // incremented in place. Deriving them from the sample log instead would
        // mean an aggregate over every sample ever written -- hundreds of
        // megabytes and seconds of latency -- on a path a scrape endpoint hits
        // continuously. `labels` is JSONB, whose btree equality is canonical, so
        // the primary key does not depend on caller key ordering.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_metric_counter_totals (
                name TEXT NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                value DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (name, labels)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // The table starts empty on purpose. Seeding it from the counter samples
        // already logged would restore every series ever recorded, and the ones
        // keyed on an object id never recur — the exposition would carry
        // thousands of series that stopped moving long ago. A counter that
        // restarts at zero is an event Prometheus reads natively; resurrecting
        // dead series is not.
        Ok(())
    }

    async fn increment_counter_total(
        &self,
        name: &str,
        value: f64,
        labels: &HashMap<String, String>,
    ) -> SFResult<()> {
        let labels_json = serde_json::to_value(labels).map_err(SFError::Serialization)?;

        sqlx::query(
            r#"
            INSERT INTO cog_metric_counter_totals (name, labels, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (name, labels)
            DO UPDATE SET value = cog_metric_counter_totals.value + EXCLUDED.value,
                          updated_at = NOW()
            "#,
        )
        .bind(name)
        .bind(labels_json)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }

    async fn record(
        &self,
        metric_type: &str,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let labels_json = serde_json::to_value(labels).map_err(SFError::Serialization)?;

        sqlx::query(
            r#"
            INSERT INTO cog_metrics_samples (metric_type, name, value, labels, timestamp)
            VALUES ($1, $2, $3, $4, NOW())
            "#,
        )
        .bind(metric_type)
        .bind(name)
        .bind(value)
        .bind(labels_json)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }

    async fn query_range(
        &self,
        metric_type: &str,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let rows: Vec<(f64, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT value, labels, timestamp
            FROM cog_metrics_samples
            WHERE metric_type = $1 AND name = $2 AND timestamp >= $3 AND timestamp <= $4
            ORDER BY timestamp ASC
            "#,
        )
        .bind(metric_type)
        .bind(name)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut samples = Vec::with_capacity(rows.len());
        for (value, labels_json, timestamp) in rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            samples.push(MetricSample {
                timestamp,
                value,
                labels,
            });
        }
        Ok(samples)
    }
}

#[async_trait]
impl MetricsBackend for PostgresMetricsBackend {
    async fn record_gauge(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.record("gauge", name, value, labels).await
    }

    async fn record_counter(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        // Both the sample log and the running total are kept: the log answers
        // range queries, the total answers the scrape endpoint, and callers of
        // either must keep working.
        self.record("counter", name, value, labels.clone()).await?;
        self.increment_counter_total(name, value, &labels).await
    }

    async fn record_histogram(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.record("histogram", name, value, labels).await
    }

    async fn query_gauge_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.query_range("gauge", name, start, end).await
    }

    async fn query_counter_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.query_range("counter", name, start, end).await
    }

    async fn query_counter_totals(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        let rows: Vec<(serde_json::Value, f64, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT labels, value, updated_at
            FROM cog_metric_counter_totals
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut samples = Vec::with_capacity(rows.len());
        for (labels_json, value, timestamp) in rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            samples.push(MetricSample {
                timestamp,
                value,
                labels,
            });
        }
        Ok(samples)
    }

    async fn query_histogram_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.query_range("histogram", name, start, end).await
    }

    async fn list_metric_names(&self, metric_type: MetricType) -> SFResult<Vec<String>> {
        // Counters are read from their own table, so a counter name is one that
        // table holds -- listing them from the sample log instead would offer
        // names whose totals row has not been written yet, and the read that
        // follows would come back empty.
        let rows: Vec<(String,)> = match metric_type {
            MetricType::Counter => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metric_counter_totals ORDER BY name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?,
            _ => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metrics_samples WHERE metric_type = $1 ORDER BY name",
            )
            .bind(metric_type.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?,
        };
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    async fn health_check(&self) -> SFResult<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }
}
