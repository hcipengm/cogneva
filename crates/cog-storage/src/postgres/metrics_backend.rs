use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{
    histogram_bucket_bounds, HistogramTotals, MetricName, MetricSample, MetricType, MetricsBackend,
    SFError, SFResult,
};

/// The identity of one series, from the only thing that identifies it.
///
/// A row per series comes back from the store, but each row's labels are read
/// into their own `HashMap`, and a map's iteration order is drawn per instance
/// rather than from its contents. Keying on the map — or on serializing it —
/// therefore splits one series across as many keys as the map has orders, and
/// redraws the split on every scrape, because the maps are rebuilt each time.
/// The canonical label-set form is a function of the contents alone, and it is
/// the same one the render side groups by, so the two ends cannot come to
/// different conclusions about what one series is.
fn series_key(labels: &HashMap<String, String>) -> String {
    cog_core::observability_text::format_labels(labels)
}

/// The read a scrape makes of one gauge: its newest sample per label set.
///
/// Public because it is half of a pair with the index that carries it (see
/// `idx_cog_metrics_latest_series` in [`PostgresMetricsBackend::init_schema`]),
/// and the gate that holds the two together has to explain the statement the
/// backend actually runs rather than a copy of it. One hand-written copy of a
/// statement is how a gate comes to certify something else.
pub const GAUGE_LATEST_SQL: &str = r#"
            SELECT DISTINCT ON (labels) value, labels, timestamp
            FROM cog_metrics_samples
            WHERE metric_type = 'gauge' AND name = $1
            ORDER BY labels, timestamp DESC
            "#;

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

        // The other half of the counter-totals decision below: gauges do read
        // their current value out of this log, because a gauge's newest sample
        // *is* its value, so the per-series read is on the same path a scrape
        // endpoint hits continuously. `(name, metric_type)` alone cannot order
        // that read the way `DISTINCT ON` needs, so the planner sorted every
        // sample of the series -- measured at 124,631 rows returning 32, external
        // merge of 8 MB to a temporary file, 1.7 s -- to answer a question whose
        // answer is one row per series. With labels and the descending timestamp
        // after the equality columns the walk is in group order and the sort is
        // gone; `value` rides along so the walk never visits the heap at all.
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_metrics_latest_series
                ON cog_metrics_samples(name, metric_type, labels, timestamp DESC)
                INCLUDE (value)
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
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.record("gauge", name.as_str(), value, labels).await
    }

    async fn record_counter(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        // Both the sample log and the running total are kept: the log answers
        // range queries, the total answers the scrape endpoint, and callers of
        // either must keep working.
        let name = name.as_str();
        self.record("counter", name, value, labels.clone()).await?;
        self.increment_counter_total(name, value, &labels).await
    }

    async fn record_histogram(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let name = name.as_str();
        self.record("histogram", name, value, labels.clone())
            .await?;
        self.increment_histogram_bucket(name, value, &labels).await
    }

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        // `DISTINCT ON` with a matching leading sort key gives the newest row
        // per label set in one pass; ordering timestamps descending inside the
        // group is what makes the first row the current value. The index that
        // makes that a walk instead of a sort is created in `init_schema`, and
        // `GAUGE_LATEST_SQL` is the statement the two are held together by.
        let rows: Vec<(f64, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(GAUGE_LATEST_SQL)
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

        let mut series: HashMap<String, HistogramTotals> = HashMap::new();

        for (labels_json, sum, updated_at) in sum_rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            let key = series_key(&labels);
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
            let key = series_key(&labels);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个标签集的每一行都由它自己那份 map 承载，而迭代顺序是按实例取的，
    /// 与内容无关。键若取自 map 本身（或它的序列化文本），同一个序列就会按
    /// 排列数碎成多份，且每次抓取的碎法都不同——渲染端于是拿到半个序列的桶，
    /// 与另一个键下的 `_sum`，两边都对不上。内容相同必须得出同一个键。
    #[test]
    fn one_label_set_is_one_series_key_however_their_maps_iterate() {
        let mut keys = std::collections::HashSet::new();
        for _ in 0..32 {
            // 每轮重建一份 map：内容一样，迭代顺序各不同。
            let labels: HashMap<String, String> = [
                ("endpoint", "/api/v1/tasks"),
                ("method", "GET"),
                ("status", "200"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            keys.insert(series_key(&labels));
        }
        assert_eq!(keys.len(), 1, "一个标签集只能有一个键：{keys:?}")
    }

    /// 分键用的是渲染端分组用的那个形式：两端若各有一套身份，「一个序列」在
    /// 存储端和抓取端就会是不同的东西。
    #[test]
    fn the_series_key_is_the_form_the_render_side_groups_by() {
        let labels: HashMap<String, String> = [("b", "2"), ("a", "1")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(series_key(&labels), "a=\"1\",b=\"2\"");
    }
}
