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

use crate::metrics_sample_cap::DEPLOYMENT_LABEL;

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
/// One series per table and per deployment rather than one total: a table that
/// keeps reporting a non-zero removal is the reading that says a release is not
/// draining, and a merged scalar would hide which of the four it is — while a
/// series shared by two deployments hides which of *them* it is.
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
    /// The deployment this process is, for the reading below. `None` publishes
    /// unlabelled, which is what a process whose platform never told it which
    /// deployment it belongs to can honestly do.
    deployment: Option<String>,
}

impl RetirementPass {
    pub fn new(retirement: std::sync::Arc<MetricsRetirement>) -> Self {
        Self {
            retirement,
            metrics: None,
            deployment: None,
        }
    }

    pub fn with_metrics(mut self, metrics: std::sync::Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Name this deployment on the reading this pass publishes.
    ///
    /// Every deployment ships the release and runs it on its own cadence, and
    /// they all write into one shared store where a series *is* its label set.
    /// Without this name the passes of two deployments land in the same series,
    /// and the reader's `min_over_time` is then the minimum across both streams
    /// rather than over one deployment's passes. Those are not the same claim:
    /// the release clears a table in one pass however large its backlog, so of
    /// any two passes that straddle a deletion the later one finds the rows
    /// already gone and writes a zero. The merged minimum is therefore zero in
    /// every window holding such a pair, and "some deployment's passes keep
    /// finding rows" — the condition the rule exists for — becomes unreadable on
    /// a series that carries neither deployment's passes.
    ///
    /// The value has to be bounded: a deployment name, not a pod name. Every
    /// series' newest row is kept forever, so an identity that changes per
    /// rollout buys a permanent floor row per rollout.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
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
    /// Every cycle, cycles that removed nothing included, because one of this
    /// series' readers counts samples instead of reading the value: the rule
    /// that says a release is not draining is `min_over_time(...[1h]) > 0`,
    /// which is "every sample in the last hour was positive". One sample per
    /// pass is what makes that the same claim as "every pass in the last hour
    /// found rows". Gating the write would keep the value honest and break the
    /// claim — the gate that writes only on change, or only on a non-zero
    /// removal, leaves the hour holding change points rather than passes, and
    /// the rule fires the moment the first removal lands instead of after an
    /// hour of them.
    ///
    /// The worry a gate is reached for is real and is answered by this same
    /// write: a pass that removed nothing writes a zero, so a removal that
    /// stops reads as stopped rather than as its last non-zero value held
    /// forever. What that needs is the zero, not a row every pass — the row
    /// every pass is for the counting reader above.
    ///
    /// Whose passes they are has to be on the row as well, for the same reason
    /// the count is: this loop runs in every deployment, so a series identified
    /// only by its table carries the passes of both, and the minimum across two
    /// streams is not the minimum over either one.
    async fn report(&self, outcome: &RetirementOutcome) {
        let Some(ref mb) = self.metrics else { return };
        publish_removals(mb, outcome, self.deployment.as_deref()).await;
    }
}

/// One row per table, whatever the pass removed.
///
/// Split out of the pass so the shape the counting reader depends on can be
/// read and tested without a live store: the pass itself is welded to a pool,
/// and the claim under test is about rows, not about deletion.
///
/// The labels are the table and the deployment that ran the pass. The deployment
/// is not decoration on a reading the reader already scopes itself: it is what
/// makes the series *this deployment's* pass stream, and the reader asks a
/// question about one stream.
async fn publish_removals(
    metrics: &std::sync::Arc<dyn MetricsBackend>,
    outcome: &RetirementOutcome,
    deployment: Option<&str>,
) {
    for (table, removed) in outcome.per_table() {
        let mut labels = HashMap::from([("table".to_string(), table.to_string())]);
        if let Some(deployment) = deployment {
            labels.insert(DEPLOYMENT_LABEL.to_string(), deployment.to_string());
        }
        if let Err(e) = metrics
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryMetricsBackend;

    /// Every row this pass has published, oldest first. The reading under test
    /// is how many there are and what the last one says, so the rows are the
    /// reading and not the values.
    async fn published(metrics: &MemoryMetricsBackend) -> Vec<(String, f64)> {
        metrics
            .query_gauge_range(
                RETIRED_ROWS_REMOVED_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|sample| {
                (
                    sample.labels.get("table").cloned().unwrap_or_default(),
                    sample.value,
                )
            })
            .collect()
    }

    fn backend() -> (
        std::sync::Arc<MemoryMetricsBackend>,
        std::sync::Arc<dyn MetricsBackend>,
    ) {
        let concrete = std::sync::Arc::new(MemoryMetricsBackend::new());
        let erased: std::sync::Arc<dyn MetricsBackend> =
            std::sync::Arc::clone(&concrete) as std::sync::Arc<dyn MetricsBackend>;
        (concrete, erased)
    }

    /// The label set of every row this pass published, oldest first. Read beside
    /// [`published`], which drops the labels to read the values.
    async fn published_labels(metrics: &MemoryMetricsBackend) -> Vec<HashMap<String, String>> {
        metrics
            .query_gauge_range(
                RETIRED_ROWS_REMOVED_METRIC.as_str(),
                chrono::Utc::now() - chrono::Duration::minutes(5),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|sample| sample.labels)
            .collect()
    }

    /// Two passes that removed nothing leave two rows per table, and the newer
    /// of each pair says zero.
    ///
    /// The rule reading this series asks whether every sample in the last hour
    /// was positive, so a pass that writes nothing is a pass the rule cannot
    /// see: gated on the value, or on "only when it found something", the hour
    /// would hold change points instead of passes and the rule would fire on
    /// the first removal rather than on an hour of them. The second assertion
    /// is what keeps the gate from being reached for the other way round — a
    /// count that goes quiet has to read as quiet, which is the zero, not the
    /// absence of a row.
    #[tokio::test]
    async fn a_pass_that_removed_nothing_still_reports() {
        let (concrete, metrics) = backend();
        let nothing = RetirementOutcome::default();

        publish_removals(&metrics, &nothing, None).await;
        let after_first = published(&concrete).await;
        assert_eq!(
            after_first.len(),
            nothing.per_table().len(),
            "the first pass must report one row per table: {after_first:?}"
        );
        assert!(
            after_first.iter().all(|(_, value)| *value == 0.0),
            "a pass that removed nothing reports zero: {after_first:?}"
        );

        publish_removals(&metrics, &nothing, None).await;
        let after_second = published(&concrete).await;
        assert_eq!(
            after_second.len(),
            2 * nothing.per_table().len(),
            "a second pass that removed nothing was not reported: the reader that \
             counts samples per pass cannot see it: {after_second:?}"
        );
    }

    /// And a pass that did remove something moves the value, under the same
    /// label set as the zeros it replaces.
    ///
    /// Counting rows alone would pass an implementation that only ever wrote
    /// zeros, so the value face is judged beside the row face.
    #[tokio::test]
    async fn a_pass_that_removed_rows_reports_them_per_table() {
        let (concrete, metrics) = backend();
        publish_removals(&metrics, &RetirementOutcome::default(), None).await;

        let outcome = RetirementOutcome {
            samples: 7,
            counter_totals: 0,
            histogram_buckets: 3,
            histogram_sums: 0,
        };
        publish_removals(&metrics, &outcome, None).await;

        let rows = published(&concrete).await;
        let latest: HashMap<String, f64> = rows
            .iter()
            .rev()
            .take(outcome.per_table().len())
            .map(|(table, value)| (table.clone(), *value))
            .collect();
        assert_eq!(
            latest.get("samples").copied(),
            Some(7.0),
            "the table that lost rows must report how many: {rows:?}"
        );
        assert_eq!(
            latest.get("histogram_buckets").copied(),
            Some(3.0),
            "every table reports its own count rather than a shared one: {rows:?}"
        );
        assert_eq!(
            latest.get("counter_totals").copied(),
            Some(0.0),
            "a table that lost nothing is reported as zero, not omitted: {rows:?}"
        );
    }

    /// A reading that is one deployment's own pass stream says which deployment
    /// ran it.
    ///
    /// Every deployment ships this loop and they share one store, where a series
    /// *is* its label set. Two deployments' passes in one series make the
    /// reader's `min_over_time` a minimum across both streams, and since the
    /// release clears a table in one pass however large its backlog the second
    /// pass of any pair finds nothing and writes zero — so the merged minimum is
    /// zero exactly when a deletion happened, and the rule for "a release that
    /// is not draining" could never fire. The label is what keeps the question
    /// about one stream askable.
    ///
    /// The two assertions are the two ways to get it wrong: a missing name (the
    /// series is shared again) and a name that grows per restart (a pod name
    /// buys a permanent floor row per rollout, since every series' newest row is
    /// kept forever).
    #[tokio::test]
    async fn the_reading_names_the_deployment_that_ran_the_pass() {
        let (concrete, metrics) = backend();
        let outcome = RetirementOutcome {
            samples: 7,
            ..RetirementOutcome::default()
        };

        publish_removals(&metrics, &outcome, Some("probe-deployment")).await;

        for labels in published_labels(&concrete).await {
            assert_eq!(
                labels.get(DEPLOYMENT_LABEL).map(String::as_str),
                Some("probe-deployment"),
                "a reading that is one deployment's passes must name it: {labels:?}"
            );
            assert!(
                labels.contains_key("table"),
                "the table stays on the reading: a merged scalar would hide which \
                 of the four stores is not draining: {labels:?}"
            );
            assert_eq!(
                labels.len(),
                2,
                "the deployment and the table are the whole identity: {labels:?}"
            );
        }
    }

    /// And a process the platform never told which deployment it is publishes
    /// unlabelled rather than under an empty name.
    ///
    /// A blank label value is a name every such process would answer with, which
    /// is the same collision the label exists to end — under a value that reads
    /// like an answer.
    #[tokio::test]
    async fn a_pass_with_no_deployment_available_publishes_no_name() {
        let (concrete, metrics) = backend();

        publish_removals(&metrics, &RetirementOutcome::default(), None).await;
        let named = RetirementPass::new(std::sync::Arc::new(MetricsRetirement::new(
            PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/nothing").expect("a lazy pool"),
        )))
        .with_deployment("   ");
        assert_eq!(
            named.deployment, None,
            "a blank deployment name is no name, not an empty one"
        );

        for labels in published_labels(&concrete).await {
            assert!(
                !labels.contains_key(DEPLOYMENT_LABEL),
                "a deployment nobody named must publish no name at all: {labels:?}"
            );
        }
    }
}
