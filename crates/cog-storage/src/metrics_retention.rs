//! Bounds the metrics sample log.
//!
//! `cog_metrics_samples` is append-only — one row per observation — and for a
//! long time nothing ever removed a row from it. The table grew with the
//! traffic rather than with what anyone wanted to look at: probes alone
//! accounted for most of the rows at one point, and a reconciliation loop that
//! re-read the same records kept adding rows while it did so.
//!
//! The rows are not free and they are not readable either. Every reader of
//! this table asks for a short window: the scrape reads histograms over the
//! last 300 s and gauges over 3600 s, the admin endpoint defaults to 60 s, and
//! counters are answered from `cog_metric_counter_totals` rather than from the
//! log at all. A row older than the widest of those windows cannot be returned
//! by any code path. Retaining it costs disk and buys nothing.
//!
//! The window is deliberately far wider than any reader needs, so that the
//! pruning never competes with a read. What it guarantees is the shape of the
//! table: bounded by the retention window rather than by uptime.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The append-only sample log written by [`crate::PostgresMetricsBackend`].
pub const SAMPLES_TABLE: &str = "cog_metrics_samples";

/// Rows removed per statement. A single unbounded `DELETE` over months of rows
/// holds its locks for the whole scan and hands PostgreSQL one enormous
/// transaction to clean up afterwards; going in batches keeps each statement
/// short and lets the space be reclaimed while the rest is still being deleted.
const DELETE_BATCH: i64 = 20_000;

/// Prunes rows from the metrics sample log once they fall outside the
/// retention window.
pub struct SampleRetention {
    pool: PgPool,
    table: String,
    retention: Duration,
    metrics: Option<Arc<dyn MetricsBackend>>,
}

impl SampleRetention {
    pub fn new(pool: PgPool, retention: Duration) -> Self {
        Self {
            pool,
            table: SAMPLES_TABLE.to_string(),
            retention,
            metrics: None,
        }
    }

    /// Prune a table other than the live one. Exists so a test can point the
    /// sweeper at a throwaway table.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// Attach a metrics backend so each sweep reports how many rows it removed
    /// and how much is left. Without it the sweeper still runs; the table
    /// simply has no size of its own to report.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Sweep once immediately, then every `interval_secs` until shutdown.
    pub async fn run(&self, interval_secs: u64, shutdown: ShutdownSignal) {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.sweep_once().await {
                        Ok(removed) => {
                            if removed > 0 {
                                info!(table = self.table, rows = removed, "Pruned aged metrics samples");
                            }
                            self.report().await;
                        }
                        Err(e) => warn!(table = self.table, error = %e, "Metrics sample retention sweep failed"),
                    }
                }
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Delete every row older than the window, in bounded batches. Returns how
    /// many rows were removed.
    pub async fn sweep_once(&self) -> SFResult<u64> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(self.retention)
                .map_err(|e| SFError::Config(format!("metrics retention out of range: {}", e)))?;

        let sql = format!(
            "DELETE FROM {table} WHERE id IN (
                 SELECT id FROM {table} WHERE timestamp < $1 ORDER BY timestamp LIMIT $2
             )",
            table = crate::partition_maintainer::quote_ident(&self.table),
        );

        let mut removed = 0u64;
        loop {
            let batch = sqlx::query(&sql)
                .bind(cutoff)
                .bind(DELETE_BATCH)
                .execute(&self.pool)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?
                .rows_affected();
            removed += batch;
            // A short batch means the aged rows are exhausted; a full one means
            // there may be more behind it.
            if batch < DELETE_BATCH as u64 {
                break;
            }
        }
        Ok(removed)
    }

    /// Publish how much the sample log holds and how far back it reaches, so
    /// the table's growth is visible from the scrape instead of only through a
    /// query against the database.
    async fn report(&self) {
        let Some(ref mb) = self.metrics else { return };
        let labels = std::collections::HashMap::new();

        match self.row_count().await {
            Ok(rows) => {
                if let Err(e) = mb
                    .record_gauge("metrics_samples_rows", rows as f64, labels.clone())
                    .await
                {
                    warn!(error = %e, "metrics_samples_rows emit failed");
                }
            }
            Err(e) => warn!(table = self.table, error = %e, "sample log size unavailable"),
        }

        if let Err(e) = mb
            .record_gauge(
                "metrics_samples_retention_seconds",
                self.retention.as_secs_f64(),
                labels,
            )
            .await
        {
            warn!(error = %e, "metrics_samples_retention_seconds emit failed");
        }
    }

    async fn row_count(&self) -> SFResult<i64> {
        let sql = format!(
            "SELECT count(*) FROM {}",
            crate::partition_maintainer::quote_ident(&self.table)
        );
        sqlx::query_scalar(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
    }
}
