//! Bounds the metrics sample log by how much it holds, not by how old it is.
//!
//! `cog_metrics_samples` is append-only — one row per observation — and for a
//! long time the only thing keeping it from growing without bound was a
//! retention window: delete every row older than N seconds. That answers the
//! wrong question. Age is a property of the row, not of the log, and the two
//! things anyone actually cares about are "how much is in here" and "is any of
//! it still readable". A time window bounds neither: traffic can put an
//! unbounded number of rows inside any window, and the window still deletes
//! rows that are being read while keeping rows nobody ever asks for.
//!
//! So the knob is a capacity — a ceiling on rows held — and the sweep runs to
//! hold the log under it. Nothing in the config answers "how long do we keep
//! this"; an operator sets how big it may get.
//!
//! Rows rather than bytes because rows are what the producer makes. The
//! producer cannot choose how wide a row is, so a byte ceiling would be a
//! ceiling it can only honour by writing fewer observations — which it cannot
//! do without dropping facts. The footprint in bytes is measured and published
//! alongside, so the disk cost is visible, but it is a reading, not the knob.
//!
//! One row is exempt from eviction: the newest row of every gauge series. A
//! gauge's value *is* its newest row — the scrape asks for the latest sample per
//! label set — so a gauge series whose newest row went is a series that vanished
//! from `/metrics`, which reads as "this never existed" rather than as "this was
//! trimmed". That floor is what the capacity may never buy, and it is one row
//! per series rather than everything at the newest instant: a row is exempt for
//! being a series' newest, not for being as recent as its newest. If the floor
//! alone holds more rows than the capacity allows, the sweep stops at the floor
//! and says so: refusing to delete more is the correct outcome, and a silent
//! refusal would look exactly like the capacity being met.
//!
//! Counters and histograms are not floored, because the log is not where their
//! value is. Their current value lives in their own accumulation tables, one row
//! per label set, and the log holds the observations that fed it — history a
//! reader may look back over, not a reading that has to be there. Flooring them
//! would also make the capacity unbindable exactly where it matters most: a
//! counter keyed on an object id mints a series per object and never repeats
//! one, so the floor over those kinds grows without bound and the sweep would
//! report itself over capacity on every pass while unable to reach it.
//!
//! A name nothing writes any more needs no exemption here at all, and the
//! release is not a variant of this sweep — a retired name's rows have to go
//! whether or not the log is over budget, so it is its own pass (see
//! [`crate::MetricsRetirement`]) driven from this loop for its cadence. What the
//! sweep must not do is inherit the retirement's reach or the retirement the
//! sweep's capacity gate.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The append-only sample log written by [`crate::PostgresMetricsBackend`].
pub const SAMPLES_TABLE: &str = "cog_metrics_samples";

/// Longest the sweeper will wait between passes.
///
/// Not a policy: the sweep also republishes the log's size, and Prometheus
/// drops a series it has not seen for five minutes rather than holding the
/// last value. A period at or beyond that would make the size reading
/// disappear exactly when the log has gone quiet — the moment an operator is
/// most likely to look. Half the staleness window leaves room for a scrape to
/// land late.
const MAX_SWEEP_PERIOD: Duration = Duration::from_secs(120);

/// Shortest the sweeper will wait between passes. A guard against a control
/// loop that never sleeps, not a cadence anyone is choosing.
const MIN_SWEEP_PERIOD: Duration = Duration::from_secs(1);

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Rows held after the pass.
    pub held: i64,
    /// Rows deleted by the pass.
    pub removed: u64,
    /// The capacity the pass was aiming at; `None` when pruning is off.
    pub budget: Option<i64>,
    /// Set when the newest-per-series floor stopped the pass short of the
    /// capacity. The log is over budget and no amount of further sweeping will
    /// fix it.
    pub floor_held: bool,
}

/// Holds the metrics sample log under a row capacity.
pub struct SampleLogCap {
    pool: PgPool,
    table: String,
    budget: i64,
    metrics: Option<Arc<dyn MetricsBackend>>,
    retirement: Option<Arc<crate::metrics_retirement::RetirementPass>>,
}

/// What the previous pass saw and when it ran. The next period is derived from
/// this so it is a function of how fast the log is actually filling rather
/// than of a number someone picked.
#[derive(Debug, Clone, Copy)]
struct PreviousPass {
    at: Instant,
    held: i64,
}

impl SampleLogCap {
    /// `budget` is the row ceiling; 0 turns pruning off, which is also how a
    /// deployment opts out — only the deployment that means to hold the log
    /// down should be the one doing it, and every deployment sharing the
    /// database would otherwise sweep the same rows.
    pub fn new(pool: PgPool, budget: u64) -> Self {
        Self {
            pool,
            table: SAMPLES_TABLE.to_string(),
            budget: budget as i64,
            metrics: None,
            retirement: None,
        }
    }

    /// Prune a table other than the live one. Exists so a test can point the
    /// sweeper at a throwaway table.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// Run the retirement pass every cycle, whatever the budget says.
    ///
    /// The release is attached here rather than given a loop of its own because
    /// this loop is the only thing that visits the metric store on a period, and
    /// a second period would be a second constant to keep right. Being attached
    /// does not make it conditional on pruning: a budget of 0 means this
    /// deployment does not own the log, which is a statement about capacity and
    /// not about whether a retired name should still be served.
    pub fn with_retirement(
        mut self,
        retirement: Arc<crate::metrics_retirement::RetirementPass>,
    ) -> Self {
        self.retirement = Some(retirement);
        self
    }

    /// Attach a metrics backend so each pass reports how many rows it removed,
    /// how much is left, and how many bytes that costs. Without it the sweeper
    /// still runs; the log simply has no size of its own to report.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Sweep once immediately, then at whatever period the fill rate calls
    /// for, until shutdown.
    pub async fn run(&self, shutdown: ShutdownSignal) {
        let mut period = MIN_SWEEP_PERIOD;
        let mut previous: Option<PreviousPass> = None;
        loop {
            // Before the sweep, so a retired name's rows are gone when the sweep
            // ranks what is left rather than being counted as ordinary history
            // for one more cycle.
            if let Some(retirement) = self.retirement.clone() {
                retirement.run_once().await;
            }
            match self.sweep_once().await {
                Ok(outcome) => {
                    if outcome.removed > 0 {
                        info!(
                            table = self.table,
                            rows = outcome.removed,
                            held = outcome.held,
                            "Pruned metrics samples over capacity"
                        );
                    }
                    if outcome.floor_held {
                        warn!(
                            table = self.table,
                            held = outcome.held,
                            budget = outcome.budget,
                            "Metrics sample log is over capacity and cannot be pruned further \
                             without dropping a series' current value"
                        );
                    }
                    period = next_period(previous, &outcome);
                    previous = Some(PreviousPass {
                        at: Instant::now(),
                        held: outcome.held,
                    });
                    self.report(&outcome).await;
                }
                Err(e) => {
                    warn!(table = self.table, error = %e, "Metrics sample capacity sweep failed")
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(period) => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Delete oldest-first until the log is within capacity, or until the only
    /// rows left are the ones every series needs.
    pub async fn sweep_once(&self) -> SFResult<SweepOutcome> {
        let held = self.row_count().await?;
        let mut outcome = SweepOutcome {
            held,
            removed: 0,
            budget: self.budget_enabled().then_some(self.budget),
            floor_held: false,
        };

        let Some(budget) = outcome.budget else {
            return Ok(outcome);
        };
        if held <= budget {
            return Ok(outcome);
        }

        let mut remaining = held - budget;
        while remaining > 0 {
            let batch = self.delete_surplus(remaining).await?;
            outcome.removed += batch;
            remaining -= batch as i64;
            if batch == 0 {
                break;
            }
        }

        outcome.held = self.row_count().await?;
        outcome.floor_held = outcome.held > budget;
        Ok(outcome)
    }

    /// Delete up to `limit` rows, oldest first, keeping each gauge series'
    /// newest row. Returns how many went.
    ///
    /// The exemption is by rank, not by a timestamp cutoff. A cutoff at
    /// `min(per-series max timestamp)` reads well and is cheap, but it keeps
    /// every row that happens to share that instant, so a series written as one
    /// burst under a single timestamp could not be pruned at all — the sweep
    /// would report itself over capacity while fifty deletable rows sat there.
    /// Ranking asks the question actually meant — "is this the newest row of
    /// its series" — and answers it with one row per series, ties broken by
    /// id so the answer does not depend on which of two equal rows the planner
    /// happens to visit first.
    ///
    /// The predicate names the kind because the exemption is not about rows at
    /// all: it is about which kinds are read through the log. A gauge is, a
    /// counter and a histogram are read from their accumulation tables, so their
    /// log rows are history and fall to the sweep at their ordinary rank — the
    /// rank comparison is computed for them anyway, and every row of theirs is
    /// deletable.
    ///
    /// The statement takes exactly as much work as the overshoot it is
    /// correcting: the cap it is enforcing is the batch size, so there is no
    /// second number deciding how much a single statement may hold locks over.
    ///
    /// Every column the filter reads is projected by the ranking subquery, and
    /// that is load-bearing rather than tidy: a column the subquery leaves out
    /// does not fail to compile, it resolves against the delete's own table and
    /// turns the subquery into a correlated one — a pass over the whole table for
    /// each row of it.
    async fn delete_surplus(&self, limit: i64) -> SFResult<u64> {
        let sql = format!(
            "DELETE FROM {table} WHERE id IN (
                 SELECT id FROM (
                     SELECT id, metric_type, timestamp,
                            row_number() OVER (
                                PARTITION BY metric_type, name, labels
                                ORDER BY timestamp DESC, id DESC
                            ) AS newest_rank
                     FROM {table}
                 ) ranked
                 WHERE metric_type <> 'gauge' OR newest_rank > 1
                 ORDER BY timestamp, id
                 LIMIT $1
             )",
            table = crate::partition_maintainer::quote_ident(&self.table),
        );
        Ok(sqlx::query(&sql)
            .bind(limit.max(1))
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected())
    }

    fn budget_enabled(&self) -> bool {
        self.budget > 0
    }

    /// Publish what the log holds, what it is allowed to hold, and what it
    /// costs on disk, so the log's growth is visible from the scrape instead
    /// of only through a query against the database.
    async fn report(&self, outcome: &SweepOutcome) {
        let Some(ref mb) = self.metrics else { return };

        self.emit(mb, "metrics_samples_rows", outcome.held as f64)
            .await;
        if let Some(budget) = outcome.budget {
            self.emit(mb, "metrics_samples_budget_rows", budget as f64)
                .await;
        }
        self.emit(
            mb,
            "metrics_samples_over_capacity",
            outcome.floor_held as u8 as f64,
        )
        .await;

        match self.table_bytes().await {
            Ok(bytes) => self.emit(mb, "metrics_samples_bytes", bytes as f64).await,
            Err(e) => warn!(table = self.table, error = %e, "sample log size unavailable"),
        }
    }

    async fn emit(&self, mb: &Arc<dyn MetricsBackend>, name: &str, value: f64) {
        if let Err(e) = mb
            .record_gauge(name, value, std::collections::HashMap::new())
            .await
        {
            warn!(error = %e, metric = name, "metrics sample log gauge emit failed");
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

    /// On-disk footprint of the table, including its indexes and TOAST. This
    /// is what the log costs the volume; it lags the row count, because
    /// PostgreSQL does not return the space a `DELETE` frees until it
    /// vacuums, so it is published as a reading and never used to decide what
    /// to delete.
    async fn table_bytes(&self) -> SFResult<i64> {
        sqlx::query_scalar("SELECT pg_total_relation_size(to_regclass($1))")
            .bind(&self.table)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
    }
}

/// How long to wait before the next pass.
///
/// Derived from how fast the log is filling: wait about as long as it would
/// take to reach the capacity at the rate the previous pass observed, so a
/// busy log is checked often and a quiet one is left alone. There is no
/// configured sweep interval, for the same reason there is no configured
/// retention: the useful cadence is a consequence of the traffic, and one
/// constant would only be right for one deployment.
///
/// A log that cannot be brought under capacity waits the longest period even
/// though it is not quiet, because waiting is all that is left — the rows it
/// would need to delete are the ones the floor protects, and retrying sooner
/// cannot help.
fn next_period(previous: Option<PreviousPass>, outcome: &SweepOutcome) -> Duration {
    if outcome.floor_held {
        return MAX_SWEEP_PERIOD;
    }
    let Some(budget) = outcome.budget else {
        return MAX_SWEEP_PERIOD;
    };
    let Some(previous) = previous else {
        return MIN_SWEEP_PERIOD;
    };
    let elapsed = previous.at.elapsed().as_secs_f64();
    let added = outcome.held - previous.held;
    if elapsed <= 0.0 || added <= 0 {
        // Not filling. Nothing to prune until something is written, so the
        // longest period is the right one: it still republishes the size often
        // enough to outlive the scrape's staleness window.
        return MAX_SWEEP_PERIOD;
    }
    let per_second = added as f64 / elapsed;
    // The wait is until the first row that will need pruning, not until the
    // capacity: a log sitting exactly at its budget is one row away from work.
    let headroom = (budget - outcome.held + 1).max(1) as f64;
    let secs = headroom / per_second;
    if !secs.is_finite() {
        return MAX_SWEEP_PERIOD;
    }
    Duration::from_secs_f64(secs).clamp(MIN_SWEEP_PERIOD, MAX_SWEEP_PERIOD)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(held: i64, budget: Option<i64>, floor_held: bool) -> SweepOutcome {
        SweepOutcome {
            held,
            removed: 0,
            budget,
            floor_held,
        }
    }

    fn pass_ago(held: i64, secs: u64) -> Option<PreviousPass> {
        Some(PreviousPass {
            at: Instant::now() - Duration::from_secs(secs),
            held,
        })
    }

    /// A log filling at ten rows a second with nine hundred rows of headroom
    /// left is checked in about ninety seconds — the cadence follows the
    /// traffic rather than a constant.
    #[test]
    fn a_filling_log_is_checked_when_it_would_reach_capacity() {
        let period = next_period(pass_ago(1000, 10), &outcome(1100, Some(2000), false));
        assert!(
            period >= Duration::from_secs(89) && period <= Duration::from_secs(91),
            "expected about the time to fill 900 rows at 10 rows/s, got {period:?}"
        );
    }

    /// A log that is not growing waits the longest period rather than being
    /// polled.
    #[test]
    fn a_log_that_is_not_filling_waits_the_longest_period() {
        assert_eq!(
            next_period(pass_ago(1000, 10), &outcome(1000, Some(2000), false)),
            MAX_SWEEP_PERIOD
        );
    }

    /// A log that has just been pruned below its previous reading is shrinking,
    /// not filling, and must not be read as a negative rate.
    #[test]
    fn a_log_pruned_since_the_last_pass_waits_the_longest_period() {
        assert_eq!(
            next_period(pass_ago(9000, 10), &outcome(100, Some(2000), false)),
            MAX_SWEEP_PERIOD
        );
    }

    /// A log that cannot be brought under capacity is not retried on a short
    /// period: nothing it does next pass could change the outcome.
    #[test]
    fn a_log_the_floor_holds_wait_the_longest_period_however_fast_it_fills() {
        assert_eq!(
            next_period(pass_ago(100, 10), &outcome(900, Some(500), true)),
            MAX_SWEEP_PERIOD
        );
    }

    /// A log sitting exactly at its budget is one write away from needing a
    /// pass. The wait is until that write, and never shorter than the floor
    /// that keeps the loop from spinning.
    #[test]
    fn a_log_at_its_budget_waits_until_the_next_write() {
        // Filling at ten rows a second, the next write is a tenth of a second
        // away, which is inside the floor.
        let fast = next_period(pass_ago(1990, 1), &outcome(2000, Some(2000), false));
        assert_eq!(fast, MIN_SWEEP_PERIOD);
        // At a tenth of a row a second it is ten seconds away.
        let slow = next_period(pass_ago(1990, 100), &outcome(2000, Some(2000), false));
        assert!(
            slow >= Duration::from_secs(9) && slow <= Duration::from_secs(11),
            "expected about ten seconds, got {slow:?}"
        );
    }

    /// Pruning off is not a reason to poll.
    #[test]
    fn no_budget_waits_the_longest_period() {
        assert_eq!(
            next_period(pass_ago(10, 10), &outcome(2000, None, false)),
            MAX_SWEEP_PERIOD
        );
    }

    /// The very first pass has nothing to compare against, so it looks again
    /// promptly rather than assuming the log is quiet.
    #[test]
    fn the_first_pass_schedules_the_next_one_promptly() {
        assert_eq!(
            next_period(None, &outcome(10, Some(2000), false)),
            MIN_SWEEP_PERIOD
        );
    }
}
