use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{
    histogram_bucket_bounds, HistogramTotals, MetricSample, MetricType, MetricsBackend, SFError,
    SFResult,
};

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

        // Histogram buckets are accumulations for the same reason counters are,
        // and live one row per (series, bucket) rather than as an array column:
        // an array would have to be grown in place when an observation lands
        // above the top bound, and Postgres array element assignment is not
        // available in the upsert form this path needs. A row per bucket makes
        // the increment uniform and leaves no separate overflow column to keep
        // in step with the count it belongs to.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_metric_histogram_buckets (
                name TEXT NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                bucket INTEGER NOT NULL,
                observations BIGINT NOT NULL,
                PRIMARY KEY (name, labels, bucket)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // The sum of observations has no bucket to live in — it is not a count
        // and cannot be recovered from the bucket counts without the raw values.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_metric_histogram_sums (
                name TEXT NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                sum DOUBLE PRECISION NOT NULL,
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

    /// Add one observation to the bucket it falls in.
    ///
    /// A value above the top finite bound lands in bucket index `bounds.len()`,
    /// which is the `+Inf` bucket — the same index the read side treats as the
    /// overflow, so the two ends agree without a separate overflow column.
    async fn increment_histogram_bucket(
        &self,
        name: &str,
        value: f64,
        labels: &HashMap<String, String>,
    ) -> SFResult<()> {
        let bounds = histogram_bucket_bounds(name);
        let bucket = bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(bounds.len()) as i32;
        let labels_json = serde_json::to_value(labels).map_err(SFError::Serialization)?;

        sqlx::query(
            r#"
            INSERT INTO cog_metric_histogram_buckets (name, labels, bucket, observations)
            VALUES ($1, $2, $3, 1)
            ON CONFLICT (name, labels, bucket)
            DO UPDATE SET observations = cog_metric_histogram_buckets.observations + 1
            "#,
        )
        .bind(name)
        .bind(labels_json.clone())
        .bind(bucket)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO cog_metric_histogram_sums (name, labels, sum)
            VALUES ($1, $2, $3)
            ON CONFLICT (name, labels)
            DO UPDATE SET sum = cog_metric_histogram_sums.sum + EXCLUDED.sum,
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
        self.record("histogram", name, value, labels.clone())
            .await?;
        self.increment_histogram_bucket(name, value, &labels).await
    }

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        // `DISTINCT ON` with a matching leading sort key gives the newest row
        // per label set in one pass; ordering timestamps descending inside the
        // group is what makes the first row the current value.
        let rows: Vec<(f64, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (labels) value, labels, timestamp
            FROM cog_metrics_samples
            WHERE metric_type = 'gauge' AND name = $1
            ORDER BY labels, timestamp DESC
            "#,
        )
        .bind(name)
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

    async fn query_histogram_totals(&self, name: &str) -> SFResult<Vec<HistogramTotals>> {
        let bounds = histogram_bucket_bounds(name);

        let bucket_rows: Vec<(serde_json::Value, i32, i64)> = sqlx::query_as(
            r#"
            SELECT labels, bucket, observations
            FROM cog_metric_histogram_buckets
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let sum_rows: Vec<(serde_json::Value, f64, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT labels, sum, updated_at
            FROM cog_metric_histogram_sums
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // Keyed on the serialized labels rather than on a `HashMap` of them:
        // the JSONB text is what the primary key compares, so grouping on it
        // keeps two rows in one series exactly when the database considers them
        // one series.
        let mut series: HashMap<String, HistogramTotals> = HashMap::new();

        for (labels_json, sum, updated_at) in sum_rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            let key = serde_json::to_string(&labels).map_err(SFError::Serialization)?;
            series.insert(
                key,
                HistogramTotals {
                    labels,
                    buckets: bounds.iter().map(|b| (*b, 0)).collect(),
                    overflow: 0,
                    count: 0,
                    sum,
                    updated_at,
                },
            );
        }

        for (labels_json, bucket, observations) in bucket_rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            let key = serde_json::to_string(&labels).map_err(SFError::Serialization)?;
            let observations = observations.max(0) as u64;
            let totals = series.entry(key).or_insert_with(|| HistogramTotals {
                labels,
                buckets: bounds.iter().map(|b| (*b, 0)).collect(),
                overflow: 0,
                count: 0,
                sum: 0.0,
                // A bucket row without a sums row is not a series this backend
                // can time; the sums row is written by the same call, so the
                // only way to be here is a partially written observation.
                updated_at: Utc::now(),
            });

            if let Some(slot) = totals.buckets.get_mut(bucket.max(0) as usize) {
                slot.1 = observations;
            } else {
                totals.overflow = observations;
            }
            totals.count += observations;
        }

        Ok(series.into_values().collect())
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
        // A name is listed from wherever its value is read, so that listing and
        // reading cannot disagree. Counters and histograms keep their current
        // value in their own accumulation tables, and reading the name out of
        // the sample log instead answers a different question: it offers names
        // whose accumulation row does not exist yet (the read that follows comes
        // back empty), and it drops a name whose log rows the capacity sweep
        // pruned while its accumulation — the thing actually served — is still
        // there. Only gauges are read through the log, because for a gauge the
        // log is the value.
        let rows: Vec<(String,)> = match metric_type {
            MetricType::Counter => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metric_counter_totals ORDER BY name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?,
            MetricType::Histogram => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metric_histogram_sums ORDER BY name",
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
