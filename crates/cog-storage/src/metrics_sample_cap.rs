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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The append-only sample log written by [`crate::PostgresMetricsBackend`].
pub const SAMPLES_TABLE: &str = "cog_metrics_samples";

/// The role this loop takes a lease on, as a reader joins it to the work.
pub const ROLE: &str = "metrics_sample_cap";

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "storage_metrics_sample_cap";

/// The label that names the deployment a reading below came from.
///
/// The sample log is shared and a series is its label set, so a reading
/// published with an empty one belongs to every process at once: whichever
/// deployment wrote last is what every scrape serves, and a deployment that
/// deliberately publishes nothing still serves whoever did. That is the wrong
/// shape for a reading that is a deployment's own claim — how large it allows
/// this log to grow, and whether the pass *it* ran could free anything — and it
/// is invisible for a reading about the log itself, which every writer would
/// answer the same way.
///
/// The value has to be bounded: a deployment name, not a pod name. Every
/// series' newest row is kept forever, so an identity that changes per rollout
/// buys a permanent floor row per rollout — around seventy a day at this
/// repository's landing rate, which is the unbounded floor the log's own design
/// refuses to grow.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// The cadence this loop declares for itself: the longest it will wait between
/// two passes.
///
/// The wait it actually takes next is derived from how fast the log is filling
/// and is never longer than this, so a declared period of this length is the
/// one that makes "no pass for six of them" mean the loop stopped rather than
/// that the log went quiet.
pub const LOOP_PERIOD: Duration = MAX_SWEEP_PERIOD;

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
    /// The arbiter over which replica prunes, when the deployment has one.
    role: Option<Arc<dyn cog_core::OwnerLeaseBroker>>,
    /// The deployment this process is, for the readings below. `None` publishes
    /// them unlabelled, which is what a process whose platform never told it
    /// which deployment it belongs to can honestly do.
    deployment: Option<String>,
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
            role: None,
            deployment: None,
        }
    }

    /// Name this deployment on the readings this loop publishes.
    ///
    /// Every one of them is a claim made by one deployment — the capacity it
    /// allows this log, and whether the pass it ran could free anything — and
    /// they land in a log every deployment shares. Without a name they are
    /// served by whichever process a reader scrapes as though the value had
    /// come from there, which is how a deployment that publishes no budget at
    /// all comes to answer with the one the deployment beside it declared.
    ///
    /// A blank name is treated as none: an empty label value is a series of its
    /// own that says nothing, and it would split every reader's series set
    /// without answering the question the label was added for.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
        self
    }

    /// Lease the pruning half of this loop, so that replicas sharing one
    /// database do not each hold the log down towards the same capacity.
    ///
    /// The budget keeps its meaning — whether this deployment prunes at all —
    /// and the lease decides which replica does. Without an arbiter the loop
    /// prunes wherever the budget allows it, which is what every deployment did
    /// before there was one.
    pub fn with_role(mut self, broker: Arc<dyn cog_core::OwnerLeaseBroker>) -> Self {
        self.role = Some(broker);
        self
    }

    /// Spawn the sweeper as a supervised loop under this file's own name.
    ///
    /// Registered from here rather than by the caller so the name the reading
    /// reports and the name the loop is declared under cannot become two
    /// spellings of the same loop — the caller would have to reach across a
    /// crate boundary for the constant, and a call site is where a literal
    /// gets written.
    pub fn spawn(self: Arc<Self>, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            LOOP,
            cog_core::loop_health::Cadence::Periodic(LOOP_PERIOD),
            shutdown.clone(),
            move |beat| {
                let cap = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move { cap.run(beat, shutdown).await }
            },
        )
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
    ///
    /// `beat` is the loop's own handle, handed here because this is where the
    /// role is contended for: the same stamp that says the loop is cycling
    /// carries whether this process is the one pruning.
    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        // Held on the lease's own cadence rather than on this loop's, because a
        // pass over a log far over its budget can outlast a term: the loop's
        // period is inside the ask period (see the test that holds the two
        // together), but a pass does not have to be, and a term that ran out
        // mid-pass would hand the log to a second process sweeping it.
        //
        // Only a deployment that means to prune contends for it. A claim is not
        // free to the process that has no use for one: the holder renews on its
        // own cadence, so a deployment that never prunes keeps the deployment
        // that does out of the role for as long as it lives. Asking the budget
        // first is what makes the ordering below true instead of intended.
        let role = if self.budget_enabled() {
            Some(cog_core::RoleHold::start(
                self.role.clone(),
                ROLE,
                beat.clone(),
                &shutdown,
            ))
        } else {
            None
        };
        let mut period = MIN_SWEEP_PERIOD;
        let mut previous: Option<PreviousPass> = None;
        loop {
            beat.beat();
            // Before the sweep, so a retired name's rows are gone when the sweep
            // ranks what is left rather than being counted as ordinary history
            // for one more cycle.
            if let Some(retirement) = self.retirement.clone() {
                retirement.run_once().await;
            }
            // Two different questions decide the same pass, and they are asked
            // in this order on purpose: the budget says whether this deployment
            // prunes at all, and only a deployment that does asks who should be
            // doing it. A deployment with no role to ask is one whose budget
            // already answered no.
            //
            // A process that may not prune still measures and reports. That is
            // not the same reading as the one the pruner publishes — no budget,
            // no floor, no removals — and it is deliberately not: this process
            // cannot say whether the log is being held down, and the reading it
            // publishes must not claim it can. The role's own series are where
            // that distinction is legible.
            let prunes = match role.as_ref() {
                Some(hold) => hold.may_act().await,
                None => false,
            };
            let pass = if prunes {
                self.sweep_once().await
            } else {
                self.measure_once().await
            };
            match pass {
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

    /// Measure the log and prune nothing.
    ///
    /// What a process owes its readers when it may not prune: the size is a
    /// property of the table, not of who is allowed to delete from it, and the
    /// budgets a process that is not pruning reports as `None` are exactly the
    /// ones it is not enforcing. The alternative — publishing the configured
    /// capacity and a held count above it — reads as a log being let grow by the
    /// process that is not the one letting it.
    pub async fn measure_once(&self) -> SFResult<SweepOutcome> {
        Ok(SweepOutcome {
            held: self.row_count().await?,
            removed: 0,
            budget: None,
            floor_held: false,
        })
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
    /// The rank question — "is this the newest row of its series" — is asked of
    /// one candidate row at a time, with ties broken by id so the answer does
    /// not depend on which of two equal rows the planner happens to visit
    /// first.
    ///
    /// The predicate names the kind because the exemption is not about rows at
    /// all: it is about which kinds are read through the log. A gauge is, a
    /// counter and a histogram are read from their accumulation tables, so their
    /// log rows are history and fall to the sweep at their ordinary rank — every
    /// row of theirs is deletable, and the rank question is never asked about
    /// them.
    ///
    /// Asking it per row rather than ranking the table first is what keeps a
    /// pass' cost following the overshoot instead of the log. Computing
    /// `row_number() OVER (PARTITION BY metric_type, name, labels ...)` for the
    /// whole table reads every row of the log to delete a batch of it: measured
    /// at limit 50 over 200,200 rows, a `WindowAgg` over all 200,200 rows, an
    /// external merge sort of 15 MB, and 1.7 s. The correlated form walks rows
    /// in `(timestamp, id)` order, stops inside `LIMIT`, and answers each
    /// candidate with one probe of the newest-per-series index — measured at the
    /// same limit, 1.1 ms and 209 buffers over 200,200 rows against 1.3 ms and
    /// 156 buffers over 5,000, so the same batch costs the same either way. That
    /// index is why the per-row question is cheap, and the correlated subquery
    /// names `name`, `labels` and `timestamp` in the order the index holds them.
    ///
    /// The batch size is the only number bounding a single statement's work and
    /// the locks it holds: the `LIMIT` is the cap being enforced, not a second
    /// knob, so a pass' reach and its cost are the same number.
    async fn delete_surplus(&self, limit: i64) -> SFResult<u64> {
        let sql = delete_surplus_sql(&self.table);
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

        self.emit(
            mb,
            cog_core::metric_names::METRICS_SAMPLES_ROWS,
            outcome.held as f64,
        )
        .await;
        if let Some(budget) = outcome.budget {
            self.emit(
                mb,
                cog_core::metric_names::METRICS_SAMPLES_BUDGET_ROWS,
                budget as f64,
            )
            .await;
            // The floor verdict belongs to the pass that could have pruned, and
            // it is published under the same condition as the budget because it
            // is the same claim: a pass with no budget did not sweep, has no
            // floor to report, and a zero written from here would say "nothing
            // is being held down" on behalf of a process that never looked.
            // The deployment label is what keeps that silence legible: without
            // it a deployment that published nothing would still be served the
            // budget the deployment beside it declared.
            self.emit(
                mb,
                cog_core::metric_names::METRICS_SAMPLES_OVER_CAPACITY,
                outcome.floor_held as u8 as f64,
            )
            .await;
        }

        match self.table_bytes().await {
            Ok(bytes) => {
                self.emit(
                    mb,
                    cog_core::metric_names::METRICS_SAMPLES_BYTES,
                    bytes as f64,
                )
                .await
            }
            Err(e) => warn!(table = self.table, error = %e, "sample log size unavailable"),
        }
    }

    /// The labels every reading this loop publishes carries.
    ///
    /// One place, because the readings are read beside each other and a reader
    /// that has to tell "this deployment declared nothing" from "another
    /// deployment declared something" needs them to agree on who is speaking.
    fn labels(&self) -> HashMap<String, String> {
        match self.deployment {
            Some(ref deployment) => {
                HashMap::from([(DEPLOYMENT_LABEL.to_string(), deployment.clone())])
            }
            None => HashMap::new(),
        }
    }

    async fn emit(&self, mb: &Arc<dyn MetricsBackend>, name: cog_core::MetricName, value: f64) {
        if let Err(e) = mb.record_gauge(name, value, self.labels()).await {
            warn!(error = %e, metric = %name, "metrics sample log gauge emit failed");
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

/// The statement a pass runs, with the table already quoted.
///
/// Exported rather than inlined at the call site so what a test explains is the
/// text the sweep executes rather than a copy of it: the costs that matter here
/// are properties of the plan this text produces, and a copy goes on being
/// explained happily after the statement beside it has been rewritten.
///
/// `$1` is the batch size. Rows go oldest first, and a gauge row is skipped when
/// it is the newest row of its own series — the one row every reader of that
/// series reaches it through.
pub fn delete_surplus_sql(table: &str) -> String {
    format!(
        "DELETE FROM {table} WHERE id IN (
             SELECT c.id FROM {table} c
             WHERE c.metric_type <> 'gauge'
                OR c.id <> (SELECT g.id FROM {table} g
                            WHERE g.metric_type = 'gauge'
                              AND g.name = c.name
                              AND g.labels = c.labels
                            ORDER BY g.timestamp DESC, g.id DESC
                            LIMIT 1)
             ORDER BY c.timestamp, c.id
             LIMIT $1
         )",
        table = crate::partition_maintainer::quote_ident(table),
    )
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

    /// The batch has to be what bounds the statement's work, and the shape that
    /// broke that is a ranking over the whole table: `row_number() OVER (...)`
    /// reads every row of the log to delete a batch of it. Only a live plan can
    /// show the cost, but this pins the shape, so the rewrite cannot be undone
    /// and go on passing everything that never runs a plan.
    #[test]
    fn the_delete_asks_about_one_row_at_a_time() {
        let sql = delete_surplus_sql("cog_metrics_samples");
        assert!(
            !sql.contains("row_number()"),
            "the delete ranks the whole table again: {sql}"
        );
        assert!(
            sql.contains("c.id <> (SELECT g.id"),
            "the delete no longer answers the rank question per row: {sql}"
        );
        assert!(
            sql.contains("ORDER BY g.timestamp DESC, g.id DESC"),
            "the per-row answer is not the newest row of the series: {sql}"
        );
        assert!(
            sql.contains("LIMIT $1"),
            "the batch is not the bound the statement is given: {sql}"
        );
    }

    /// The table name is an identifier, so it is quoted everywhere it lands —
    /// the delete's own table and the two scans the subquery is built from.
    /// Forgetting one leaves the statement erroring out or, worse, resolving to
    /// a different table.
    #[test]
    fn every_place_the_table_is_named_is_quoted() {
        let sql = delete_surplus_sql(r#"odd"name"#);
        assert_eq!(
            sql.matches(r#""odd""name""#).count(),
            3,
            "the table is not quoted everywhere it is named: {sql}"
        );
    }

    /// The loop asks again as often as the lease contract assumes, which is what
    /// lets the rule that says a role is unowned treat a handover as bounded by
    /// the term plus one ask period. A loop whose work cadence was slower than
    /// that would have to ask on a timer of its own rather than declare a longer
    /// period here, because a holder that asks after its term has run out is
    /// contending with whoever took the role in the meantime.
    #[test]
    fn the_declared_period_renews_inside_the_lease_contract() {
        assert!(
            LOOP_PERIOD <= cog_core::owner_lease::ASK_PERIOD,
            "the loop declares a period of {LOOP_PERIOD:?}, longer than the ask period {:?} \
             the lease and its alerting are written against",
            cog_core::owner_lease::ASK_PERIOD
        );
    }
}
