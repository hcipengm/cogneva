//! Retires per-call rows from the token ledger, but only where the fold already
//! answered for them.
//!
//! `gateway_llm_usage` is the row-per-call account of what the platform spent:
//! one row per upstream call, carrying its tokens, latency and the task it was
//! bought for. Nothing has ever removed a row from it. Measured on this
//! deployment on 2026-10-27 it held 312,803 rows over 32 days — 80 MB — and it
//! grows with call volume, which is the one quantity here that has no ceiling
//! of its own.
//!
//! Age alone is not a safe cutoff for this table, and the reason is the hourly
//! fold beside it. `cog_llm_usage_rollup` is a *derivation* of the ledger: every
//! window it holds was computed by summing ledger rows, and a window it does
//! not hold has never been summed. Deleting a row that no window covered does
//! not age it out, it deletes the only copy of that spend — the rollup would
//! report the hour as quiet forever, with nothing left to recompute it from and
//! no reading that could notice. So the sweep's question is two-part, and both
//! halves have to hold: the row is past the retention, *and* the fold reaches
//! back to it.
//!
//! The second half is the reason this loop publishes what it cannot delete. A
//! ledger that stops shrinking because the fold's older end sits above the
//! retention looks exactly like a retention that is working, from every reading
//! the deletion itself can produce. The uncovered rows and their age are
//! therefore published beside the size, and the rule that reads them fires on
//! the pair: rows exist *and* they are past the retention, which is the state
//! where the promise to the reader has stopped being kept.
//!
//! No lease guards the pass. The batches take rows in timestamp order, so two
//! replicas sweeping at once take them in the same order and cannot deadlock
//! against each other, and the set that gets deleted is defined by the clock
//! rather than by who is running.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, Row};
use tracing::{info, warn};

use cog_core::metric_names;
use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

use crate::usage_store::{LEDGER_TABLE, ROLLUP_TABLE};

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "observability_llm_usage_retention";

/// The label that names the deployment a reading came from: the ledger is
/// shared, and a reading without a name is served by whichever process the
/// scrape happens to reach.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// How long a per-call row is kept by default.
///
/// Thirty days, the same promise the prompt archive beside it makes. What
/// outlives it is the *aggregate*: the fold keeps the hourly sums indefinitely,
/// so a question about last quarter's spend is still answered after the rows
/// that bought it are gone. What ends at thirty days is the per-call detail —
/// which task bought which tokens — and that is a promise about the ledger
/// rather than a side effect of the table's size.
pub const DEFAULT_KEEP_DAYS: i64 = 30;

/// The shortest retention this loop will accept.
///
/// One day. Zero or negative days would erase the ledger every pass, which
/// reads as a working retention while destroying the rows the current hour's
/// fold is computed from — the value falls back rather than being honoured.
pub const MIN_KEEP_DAYS: i64 = 1;

/// The environment variable that carries the retention.
pub const KEEP_DAYS_ENV: &str = "COGNEVA_LLM_USAGE_KEEP_DAYS";

/// How often the loop looks for rows past the retention.
///
/// Five minutes, for the same reason the prompt archive uses it: the readings
/// this loop publishes are what the alert rules read, and Prometheus drops a
/// series it has not seen for five minutes.
pub const PERIOD: Duration = Duration::from_secs(300);

/// How many rows one statement may delete.
///
/// Two thousand rows is a few hundred kilobytes and a short lock. The pass
/// repeats until it finds nothing past the cutoff, so the number of batches
/// follows the backlog rather than the batch size.
const BATCH_ROWS: i64 = 2_000;

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionOutcome {
    /// Rows the ledger held after the pass.
    pub held: i64,
    /// Rows this pass retired.
    pub removed: u64,
    /// The age of the oldest surviving row, in seconds; zero when empty.
    pub oldest_age_secs: i64,
    /// The retention this pass enforced, in seconds.
    pub keep_secs: i64,
    /// Rows older than the fold's own floor, which this pass may not delete
    /// however old they are.
    pub uncovered: i64,
    /// The age of the oldest uncovered row, in seconds; zero when there is
    /// none.
    pub uncovered_oldest_age_secs: i64,
}

/// The ledger sweep: the table, the pool it lives on, and the age this
/// deployment promises its readers.
#[derive(Clone)]
pub struct UsageRetention {
    pool: PgPool,
    keep: Duration,
    metrics: Option<Arc<dyn MetricsBackend>>,
    deployment: String,
}

impl UsageRetention {
    pub fn new(pool: PgPool, keep: Duration) -> Self {
        Self {
            pool,
            keep,
            metrics: None,
            deployment: String::new(),
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Name the deployment these readings belong to.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        self.deployment = deployment.into();
        self
    }

    /// The retention this sweep enforces.
    pub fn keep(&self) -> Duration {
        self.keep
    }

    /// Spawn the sweep as a supervised loop under this file's own name.
    pub fn spawn(self: Arc<Self>, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            LOOP,
            cog_core::loop_health::Cadence::Periodic(PERIOD),
            shutdown.clone(),
            move |beat| {
                let sweep = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move { sweep.run(beat, shutdown).await }
            },
        )
    }

    /// Sweep once immediately, then every [`PERIOD`], until shutdown.
    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        let mut interval = tokio::time::interval(PERIOD);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            beat.beat();
            match self.retire_once(|| beat.beat()).await {
                Ok(outcome) => {
                    if outcome.removed > 0 {
                        info!(
                            rows = outcome.removed,
                            held = outcome.held,
                            uncovered = outcome.uncovered,
                            "Retired LLM usage rows past the retention"
                        );
                    }
                    self.report(&outcome).await;
                    self.report_pass_failed(false).await;
                }
                Err(e) => {
                    // A failed pass leaves every value at its last reading, so
                    // a ledger that cannot be swept and one that is not growing
                    // would otherwise be told apart by nothing.
                    self.report_pass_failed(true).await;
                    warn!(error = %e, "LLM usage retention pass failed");
                }
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Retire every row that is past the retention *and* inside what the fold
    /// covers, calling `progress` before each batch.
    ///
    /// The fold's older end is read once per pass rather than per batch: it
    /// moves only when the backfill lands a window, and a value that changed
    /// mid-pass would make two batches of one pass answer the same question
    /// differently.
    pub async fn retire_once(&self, progress: impl Fn()) -> SFResult<RetentionOutcome> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(self.keep).unwrap_or_else(|_| chrono::Duration::days(30));
        let floor = self.fold_floor().await?;
        let mut removed = 0u64;
        if let Some(floor) = floor {
            if cutoff > floor {
                loop {
                    progress();
                    let batch = sqlx::query(&format!(
                        "DELETE FROM {table} WHERE id IN (
                             SELECT id FROM {table}
                              WHERE ts < $2 AND ts >= $3
                              ORDER BY ts
                              LIMIT $1
                         )",
                        table = LEDGER_TABLE,
                    ))
                    .bind(BATCH_ROWS)
                    .bind(cutoff)
                    .bind(floor)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| SFError::Database(e.to_string()))?
                    .rows_affected();
                    removed += batch;
                    if batch == 0 {
                        break;
                    }
                }
            }
        }

        let held = self.row_count().await?;
        let oldest_age_secs = self.oldest_age_secs().await?;
        let (uncovered, uncovered_oldest_age_secs) = self.uncovered(floor).await?;
        Ok(RetentionOutcome {
            held,
            removed,
            oldest_age_secs,
            keep_secs: self.keep.as_secs() as i64,
            uncovered,
            uncovered_oldest_age_secs,
        })
    }

    /// The oldest window the fold holds, or `None` when it holds nothing: with
    /// no floor, every row is uncovered and none may be retired.
    async fn fold_floor(&self) -> SFResult<Option<chrono::DateTime<chrono::Utc>>> {
        sqlx::query(&format!(
            "SELECT MIN(window_start) AS floor FROM {ROLLUP_TABLE}"
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))
        .map(|row| row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("floor"))
    }

    /// Rows in the ledger, through the same statement the capacity sweeps use:
    /// this count and theirs are read side by side on one panel, so the two
    /// must be the same measurement rather than two that happen to agree today.
    async fn row_count(&self) -> SFResult<i64> {
        let sql = cog_core::sql::relation_rows_sql(LEDGER_TABLE);
        sqlx::query_scalar(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
    }

    /// The age of the oldest row in the ledger, in seconds.
    ///
    /// Zero for an empty ledger rather than an absent series: a table nobody
    /// has written to is not a retention that has fallen behind.
    async fn oldest_age_secs(&self) -> SFResult<i64> {
        sqlx::query(&cog_core::sql::relation_oldest_age_sql(LEDGER_TABLE, "ts"))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
            .map(|row| row.get::<Option<i64>, _>("age").unwrap_or(0))
    }

    /// Rows below the fold's floor, and the age of the oldest of them.
    ///
    /// These are the rows the sweep may not touch however far past the
    /// retention they are, and the pair is what the rule reads: a count alone
    /// cannot tell "the fold is two hours behind and catching up" from "the
    /// retention has been defeated for a month".
    async fn uncovered(
        &self,
        floor: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<(i64, i64)> {
        let row = sqlx::query(&format!(
            "SELECT count(*) AS n,
                    EXTRACT(EPOCH FROM (NOW() - MIN(ts)))::bigint AS age
               FROM {LEDGER_TABLE} WHERE $1::timestamptz IS NULL OR ts < $1"
        ))
        .bind(floor)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok((
            row.get::<i64, _>("n"),
            row.get::<Option<i64>, _>("age").unwrap_or(0),
        ))
    }

    /// Publish what the ledger holds, how far back it reaches, the retention
    /// this process enforces, and what it may not retire.
    async fn report(&self, outcome: &RetentionOutcome) {
        let Some(ref mb) = self.metrics else { return };
        let labels = self.labels();
        let pairs = [
            (metric_names::LLM_USAGE_ROWS, outcome.held as f64),
            (
                metric_names::LLM_USAGE_OLDEST_AGE_SECONDS,
                outcome.oldest_age_secs as f64,
            ),
            (
                metric_names::LLM_USAGE_KEEP_SECONDS,
                outcome.keep_secs as f64,
            ),
            (
                metric_names::LLM_USAGE_UNCOVERED_ROWS,
                outcome.uncovered as f64,
            ),
            (
                metric_names::LLM_USAGE_UNCOVERED_OLDEST_AGE_SECONDS,
                outcome.uncovered_oldest_age_secs as f64,
            ),
        ];
        for (name, value) in pairs {
            self.emit(mb, name, value, labels.clone()).await;
        }
    }

    async fn report_pass_failed(&self, failed: bool) {
        let Some(ref mb) = self.metrics else { return };
        self.emit(
            mb,
            metric_names::LLM_USAGE_SWEEP_FAILED,
            if failed { 1.0 } else { 0.0 },
            self.labels(),
        )
        .await;
    }

    fn labels(&self) -> HashMap<String, String> {
        if self.deployment.is_empty() {
            return HashMap::new();
        }
        HashMap::from([(DEPLOYMENT_LABEL.to_string(), self.deployment.clone())])
    }

    async fn emit(
        &self,
        mb: &Arc<dyn MetricsBackend>,
        name: cog_core::MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) {
        if let Err(e) = mb.record_gauge(name, value, labels).await {
            warn!(error = %e, metric = %name, "LLM usage retention gauge emit failed");
        }
    }
}

/// The retention this process was told, or the default.
///
/// A value that cannot be read, or that is below [`MIN_KEEP_DAYS`], falls back
/// rather than being honoured: shortening the ledger to nothing turns the table
/// into one that is trivially bounded and the promise to its readers into one
/// that was never kept.
pub fn keep_days_from_env() -> i64 {
    keep_days_from(std::env::var(KEEP_DAYS_ENV).ok().as_deref())
}

/// [`keep_days_from_env`] against an explicit value, so the fallback can be
/// tested without a process environment.
///
/// Zero falls back to the default: the ledger answers what was spent and by
/// which agent, and a ledger swept to nothing every pass reads as a deployment
/// that spent nothing rather than one that stopped keeping the books. Nothing
/// downstream can tell those apart, so `0` is treated as a misconfiguration.
pub fn keep_days_from(raw: Option<&str>) -> i64 {
    cog_core::retention::keep_days_or_default(raw, DEFAULT_KEEP_DAYS, MIN_KEEP_DAYS)
}

/// The deployment this process runs in, for the readings below.
pub fn deployment_name() -> String {
    std::env::var(cog_core::env::DEPLOYMENT_NAME_ENV).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retention_below_the_floor_falls_back_to_the_default() {
        assert_eq!(keep_days_from(None), DEFAULT_KEEP_DAYS);
        assert_eq!(keep_days_from(Some("0")), DEFAULT_KEEP_DAYS);
        assert_eq!(keep_days_from(Some("-7")), DEFAULT_KEEP_DAYS);
        assert_eq!(keep_days_from(Some("not a number")), DEFAULT_KEEP_DAYS);
    }

    #[test]
    fn a_declared_retention_is_honoured() {
        assert_eq!(keep_days_from(Some("7")), 7);
        assert_eq!(keep_days_from(Some(" 90 ")), 90);
    }

    /// The sweep's period has to be short enough that its own published
    /// readings do not go stale between scrapes.
    #[test]
    fn the_period_is_inside_the_scrape_gap() {
        assert!(PERIOD.as_secs() <= 300);
    }
}
