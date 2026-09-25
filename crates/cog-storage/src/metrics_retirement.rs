//! Removes the rows of metric names nothing produces any more.
//!
//! A retired name is not a reading: no producer will write it again, so whatever
//! is stored under it will never be updated. Where that matters is every place a
//! series is stored, and the metric store has four:
//!
//! - `cog_metrics_samples`, the append-only log — for a gauge this *is* the
//!   value, read as the newest sample per label set.
//! - `cog_metric_counter_totals`, one row per label set, rewritten in place —
//!   the value of a counter.
//! - `cog_metric_histogram_buckets` and `cog_metric_histogram_sums`, the
//!   accumulation of a histogram.
//!
//! The sweep in [`crate::SampleLogCap`] cannot reach the last three: they have
//! no rows to rank and no overshoot to trim, because they hold current state
//! rather than history. An unreachable store is a series that is not merely held
//! but never deleted at all — a retired counter keeps being listed and served at
//! its frozen total, and a frozen total reads downstream as "no traffic" rather
//! than as "this metric is gone", which is the more misleading of the two.
//!
//! So the release is its own pass, not a variant of pruning. Pruning is driven
//! by how much is held and only runs when the store is over its budget; a
//! retirement is a fact about a name and has to take effect whether or not the
//! store happens to be full. Riding the capacity sweep would mean a quiet
//! deployment never releasing anything, and the store is at its quietest exactly
//! where a rename would otherwise sit unfixed.
//!
//! What it never does is decide that a name is dead on its own. That judgement
//! belongs where the names are known — [`cog_core::RETIRED_METRIC_NAMES`] — and
//! a guess made here from age or from rank would delete a live series' current
//! value whenever its producer was slow.

use std::collections::HashMap;

use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::{MetricsBackend, SFError, SFResult};

/// The accumulation tables, named for the kind of value each holds.
pub const COUNTER_TOTALS_TABLE: &str = "cog_metric_counter_totals";
pub const HISTOGRAM_BUCKETS_TABLE: &str = "cog_metric_histogram_buckets";
pub const HISTOGRAM_SUMS_TABLE: &str = "cog_metric_histogram_sums";

/// Rows one statement may delete from the sample log.
///
/// Not a policy and not a cadence: it bounds how long a single `DELETE` holds
/// locks, the same way the sweep's batch does. A retired name's history can be
/// the larger part of the log, and deleting it in one statement would block
/// every writer for as long as that takes.
const RELEASE_BATCH: i64 = 5_000;

/// Gauge reporting the rows this pass removed, per table.
///
/// One series per table rather than one total: a table that keeps reporting a
/// non-zero removal is the reading that says a release is not draining, and a
/// merged scalar would hide which of the four it is.
use cog_core::metric_names::METRICS_RETIRED_ROWS_REMOVED as RETIRED_ROWS_REMOVED_METRIC;

/// What one release pass removed, per table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetirementOutcome {
    pub samples: u64,
    pub counter_totals: u64,
    pub histogram_buckets: u64,
    pub histogram_sums: u64,
}

impl RetirementOutcome {
    pub fn total(&self) -> u64 {
        self.samples + self.counter_totals + self.histogram_buckets + self.histogram_sums
    }

    /// The per-table counts, named for the table each came from.
    pub fn per_table(&self) -> [(&'static str, u64); 4] {
        [
            ("samples", self.samples),
            ("counter_totals", self.counter_totals),
            ("histogram_buckets", self.histogram_buckets),
            ("histogram_sums", self.histogram_sums),
        ]
    }
}

/// Deletes the rows of retired metric names from every table that stores one.
pub struct MetricsRetirement {
    pool: PgPool,
    samples: String,
    counter_totals: String,
    histogram_buckets: String,
    histogram_sums: String,
    retired_names: Vec<&'static str>,
}

impl MetricsRetirement {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            samples: crate::metrics_sample_cap::SAMPLES_TABLE.to_string(),
            counter_totals: COUNTER_TOTALS_TABLE.to_string(),
            histogram_buckets: HISTOGRAM_BUCKETS_TABLE.to_string(),
            histogram_sums: HISTOGRAM_SUMS_TABLE.to_string(),
            retired_names: cog_core::RETIRED_METRIC_NAMES.to_vec(),
        }
    }

    /// Point the pass at other tables. Exists so a test can use throwaway
    /// tables instead of the live ones.
    pub fn with_tables(
        mut self,
        samples: impl Into<String>,
        counter_totals: impl Into<String>,
        histogram_buckets: impl Into<String>,
        histogram_sums: impl Into<String>,
    ) -> Self {
        self.samples = samples.into();
        self.counter_totals = counter_totals.into();
        self.histogram_buckets = histogram_buckets.into();
        self.histogram_sums = histogram_sums.into();
        self
    }

    /// Replace the retired-name set. Exists so a test can name a series of its
    /// own instead of depending on which names the codebase happens to have
    /// retired.
    pub fn with_retired_names(mut self, names: Vec<&'static str>) -> Self {
        self.retired_names = names;
        self
    }

    /// Run the pass until nothing is left to remove, and say what went.
    ///
    /// Idempotent: a second pass over the same names finds nothing. That is what
    /// lets any deployment holding the store run it on its own cadence without
    /// agreeing with anyone else about who may run it — every deployment ships
    /// the same list, and two of them deleting the same rows is two statements
    /// where the second finds none.
    pub async fn release(&self) -> SFResult<RetirementOutcome> {
        if self.retired_names.is_empty() {
            return Ok(RetirementOutcome::default());
        }

        // The accumulation tables hold one row per series rather than one per
        // observation, so their share of a retirement is bounded by how many
        // label sets the name ever carried and one statement is enough. The log
        // is the append-only one and gets the batched loop.
        let counter_totals = self.delete_by_name(&self.counter_totals).await?;
        let histogram_buckets = self.delete_by_name(&self.histogram_buckets).await?;
        let histogram_sums = self.delete_by_name(&self.histogram_sums).await?;

        let mut samples = 0u64;
        loop {
            let deleted = self.delete_log_batch().await?;
            samples += deleted;
            if deleted < RELEASE_BATCH as u64 {
                break;
            }
        }

        Ok(RetirementOutcome {
            samples,
            counter_totals,
            histogram_buckets,
            histogram_sums,
        })
    }

    /// `DELETE ... WHERE name = ANY($1)` against a table that holds one row per
    /// series.
    async fn delete_by_name(&self, table: &str) -> SFResult<u64> {
        let sql = format!(
            "DELETE FROM {} WHERE name = ANY($1)",
            crate::partition_maintainer::quote_ident(table)
        );
        Ok(sqlx::query(&sql)
            .bind(self.names())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected())
    }

    /// One batch of the sample log's retired rows, oldest first.
    async fn delete_log_batch(&self) -> SFResult<u64> {
        let sql = format!(
            "DELETE FROM {table} WHERE id IN (
                 SELECT id FROM {table}
                 WHERE name = ANY($1)
                 ORDER BY id
                 LIMIT $2
             )",
            table = crate::partition_maintainer::quote_ident(&self.samples),
        );
        Ok(sqlx::query(&sql)
            .bind(self.names())
            .bind(RELEASE_BATCH)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected())
    }

    fn names(&self) -> Vec<String> {
        self.retired_names
            .iter()
            .map(|n| (*n).to_string())
            .collect()
    }
}

/// One release pass, with whatever it is allowed to report through.
///
/// It is driven from the sweep's loop rather than given a timer of its own:
/// the sweep's period is derived from how fast the log fills and capped at
/// [`crate::metrics_sample_cap`]'s longest wait, which makes it the one
/// periodic visitor the metric store has. Hanging the release off it means a
/// retirement takes effect within one period without inventing a second
/// constant to be wrong about. What it must not inherit from the sweep is the
/// capacity gate: the release runs every cycle, whether or not the store is
/// over budget.
pub struct RetirementPass {
    retirement: std::sync::Arc<MetricsRetirement>,
    metrics: Option<std::sync::Arc<dyn MetricsBackend>>,
}

impl RetirementPass {
    pub fn new(retirement: std::sync::Arc<MetricsRetirement>) -> Self {
        Self {
            retirement,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// One pass. Failures are logged, not propagated: a store that cannot be
    /// reached must not stop the sweep that runs beside it.
    pub async fn run_once(&self) {
        match self.retirement.release().await {
            Ok(outcome) => {
                if outcome.total() > 0 {
                    let counts: HashMap<&'static str, u64> =
                        outcome.per_table().into_iter().collect();
                    info!(
                        removed = outcome.total(),
                        ?counts,
                        "Released retired metric rows"
                    );
                }
                self.report(&outcome).await;
            }
            Err(e) => warn!(error = %e, "Retired metric release failed"),
        }
    }

    /// Publish what the pass removed from each table, every cycle.
    ///
    /// Every cycle rather than only when it found something: a gauge that is
    /// written only on a non-zero removal holds its last non-zero value
    /// forever, and a frozen removal count is exactly as unreadable as the
    /// frozen total this pass exists to delete.
    async fn report(&self, outcome: &RetirementOutcome) {
        let Some(ref mb) = self.metrics else { return };
        for (table, removed) in outcome.per_table() {
            let labels = HashMap::from([("table".to_string(), table.to_string())]);
            if let Err(e) = mb
                .record_gauge(RETIRED_ROWS_REMOVED_METRIC, removed as f64, labels)
                .await
            {
                warn!(
                    error = %e,
                    metric = %RETIRED_ROWS_REMOVED_METRIC,
                    table,
                    "retirement removal gauge emit failed"
                );
            }
        }
    }
}
