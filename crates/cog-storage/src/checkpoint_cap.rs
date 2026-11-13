//! Bounds the checkpoint store by how much it holds — and never deletes a
//! snapshot a task board still points at.
//!
//! `cog_snapshots` holds one row per checkpoint: an agent's state and its
//! context window, written by the checkpoint round that runs while a task is in
//! flight. A round writes the new snapshot, records its id on the task's board,
//! and only then deletes the one it replaced. Nothing deletes the last snapshot
//! of a task that has finished, and nothing deletes a board, so every task the
//! platform has ever checkpointed leaves its final checkpoint behind for good.
//!
//! Measured on 2026-10-27: 54,452 rows over 924 MB of table and indexes, of
//! which 39,610 rows (about 580 MB, measured the same day) are referenced by no
//! board field at all.
//! Every one of those rows is in the nightly backup, and the backup drill
//! restores the whole dump to prove it — so the garbage is paid for every night
//! by a restore that has a per-pass budget to finish inside.
//!
//! One thing a sweep may never buy here: **a snapshot a board points at is the
//! resume point.** Losing it means a task cannot be resumed, and the board
//! would still point at the id — a silent gap in exactly the mechanism this
//! table exists for. The board is written before the old snapshot is deleted
//! for the same reason, so the pointer is the only authority there is.
//!
//! There is no age floor beside it, and none is needed: a row no board names
//! is a leftover by construction, whatever its age — the round that wrote it
//! has already pointed the board elsewhere, and no reader can reach it by task.
//! An age floor would have been a number protecting a view that does not read
//! this table by time, so it protected nothing.
//!
//! When the pointers hold more rows than the capacity allows, the sweep stops
//! and says so. That state is a policy question and not a fault: boards are
//! never retired, so the ceiling on what may be deleted is the number of
//! snapshots that have finished. Raising the capacity or retiring finished
//! tasks' boards are both answers, and neither is a code change.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::retention::{
    row_budget_against_volume, RetentionReadings, SOURCE_DERIVED, UNIT_ROWS,
};
use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The registry key this loop's target answers to.
pub const KNOB: &str = "checkpointStore.maxRows";

/// The table the checkpoint store writes.
pub const SNAPSHOTS_TABLE: &str = "cog_snapshots";

/// The board table whose field values name the snapshots a task would resume
/// from.
pub const BOARDS_TABLE: &str = "cog_context_boards";

/// The role this loop takes a lease on. Only the holder prunes, so replicas
/// sharing one database do not each hold the table down.
pub const ROLE: &str = "checkpoint_cap";

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "storage_checkpoint_cap";

/// The label that names the deployment a reading below came from. Same reason
/// as the other capacity loops': the table is shared and a reading without a
/// name is served by whichever process the scrape reaches.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// Rows the table may hold by default.
///
/// Measured on 2026-10-27, the rows a board still points at were 14,842 of
/// 54,452 — the rest was garbage. Forty thousand sits well above that live set
/// and well below the table, so the capacity binds on the garbage today while
/// leaving room for the live set to grow several times over before it becomes
/// the wall. It is a default for the deployment that does not say otherwise,
/// not a policy.
// cap: bound — 要给活集留出还能增长数倍的空间才算成为墙；这是不说自己容量的部署的默认值，不是策略
pub const DEFAULT_MAX_ROWS: u64 = 40_000;

/// The environment variable that carries the row ceiling.
pub const MAX_ROWS_ENV: &str = "COGNEVA_CHECKPOINT_MAX_ROWS";

/// Longest the sweeper will wait between passes.
///
/// The sweep also republishes the table's size, and Prometheus drops a series
/// it has not seen for five minutes rather than holding the last value.
// cap: bound — 清扫顺带重发这张表的大小，而 Prometheus 五分钟没见到序列就丢掉，不保留最后那个值
const MAX_SWEEP_PERIOD: Duration = Duration::from_secs(120);

/// Shortest the sweeper will wait between passes. A guard against a control
/// loop that never sleeps, not a cadence anyone is choosing.
// cap: bound — 防的是一个从不睡眠的控制循环，不是谁在挑节拍
const MIN_SWEEP_PERIOD: Duration = Duration::from_secs(1);

/// The cadence this loop declares for itself.
pub const LOOP_PERIOD: Duration = MAX_SWEEP_PERIOD;

/// How many rows one statement may delete. Bounds a single statement's work
/// and the locks it holds; the `LIMIT` is how the capacity is enforced, so a
/// pass' reach and its cost are the same number.
// cap: bound — LIMIT 就是容量的执行方式，所以一轮的覆盖范围与它的代价是同一个数；这个数界的是单句要持多久锁
const BATCH_ROWS: i64 = 5_000;

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSweepOutcome {
    /// Rows held after the pass.
    pub held: i64,
    /// Rows deleted by the pass.
    pub removed: u64,
    /// The capacity the pass was aiming at; `None` when pruning is off.
    pub budget: Option<i64>,
    /// The capacity this pass computed, whether or not it was allowed to prune
    /// against it. Zero when the deployment opted out of pruning.
    pub target: i64,
    /// Which input decided `target` this pass: the deployment's seed, or the
    /// measured cost of a row on the declared volume.
    pub target_source: &'static str,
    /// Set when the board pointers stopped the
    /// pass short of the capacity. The table is over budget and no amount of
    /// further sweeping may fix it.
    pub held_above: bool,
    /// The table's size on disk when the pass started, when it could be read.
    pub bytes: Option<u64>,
}

/// Holds the checkpoint store under a row capacity, outside the read window
/// and clear of every board pointer.
pub struct CheckpointCap {
    pool: PgPool,
    table: String,
    boards: String,
    /// The row ceiling this deployment declared. It is what a process with no
    /// measured cost keeps, and zero turns pruning off.
    seed_budget: i64,
    /// The declared size of the database volume; with it and the measured cost
    /// of a row, every pass recomputes the ceiling.
    volume_bytes: Option<u64>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    readings: Option<Arc<RetentionReadings>>,
    role: Option<Arc<dyn cog_core::OwnerLeaseBroker>>,
    deployment: Option<String>,
}

/// What the previous pass saw and when it ran, so the next period follows the
/// fill rate rather than a number someone picked.
#[derive(Debug, Clone, Copy)]
struct PreviousPass {
    at: Instant,
    held: i64,
}

impl CheckpointCap {
    /// `budget` is the row ceiling; 0 turns pruning off, which is also how a
    /// deployment opts out — every deployment sharing the database would
    /// otherwise sweep the same rows.
    pub fn new(pool: PgPool, budget: u64) -> Self {
        Self {
            pool,
            table: SNAPSHOTS_TABLE.to_string(),
            boards: BOARDS_TABLE.to_string(),
            seed_budget: budget as i64,
            volume_bytes: None,
            metrics: None,
            readings: None,
            role: None,
            deployment: None,
        }
    }

    /// The declared size of the volume this table lives on.
    ///
    /// With a volume and a measured cost per row, every pass recomputes the row
    /// ceiling; without one the deployment's seed stands. The volume itself
    /// changes only with a resize, so it is read once.
    pub fn with_database_volume(mut self, bytes: Option<u64>) -> Self {
        self.volume_bytes = bytes;
        self
    }

    /// Attach the reading face that carries this loop's target and beat count.
    pub fn with_readings(mut self, readings: Arc<RetentionReadings>) -> Self {
        self.readings = Some(readings);
        self
    }

    /// Point the sweeper at other tables. Exists so a test can use throwaway
    /// ones instead of the live checkpoint store.
    pub fn with_tables(mut self, snapshots: impl Into<String>, boards: impl Into<String>) -> Self {
        self.table = snapshots.into();
        self.boards = boards.into();
        self
    }

    /// Name this deployment on the readings this loop publishes.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
        self
    }

    /// Lease the pruning half of this loop.
    pub fn with_role(mut self, broker: Arc<dyn cog_core::OwnerLeaseBroker>) -> Self {
        self.role = Some(broker);
        self
    }

    /// Attach a metrics backend so each pass reports what the table holds and
    /// what that costs on disk.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Spawn the sweeper as a supervised loop under this file's own name.
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

    /// Sweep once immediately, then at whatever period the fill rate calls
    /// for, until shutdown.
    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        // Only a deployment that means to prune contends for the lease: a
        // deployment that never prunes would otherwise hold the role on its own
        // renewal cadence and keep the deployment that does out of it for as
        // long as it lives.
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
            let prunes = match role.as_ref() {
                Some(hold) => hold.may_act().await,
                None => false,
            };
            let pass = if prunes {
                self.sweep_once_beating(|| beat.beat()).await
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
                            "Pruned checkpoints over capacity"
                        );
                    }
                    if outcome.held_above {
                        warn!(
                            table = self.table,
                            held = outcome.held,
                            budget = outcome.budget,
                            "Checkpoint store is over capacity and cannot be pruned further: \
                             every remaining row is a board pointer"
                        );
                    }
                    period = next_period(previous, &outcome);
                    previous = Some(PreviousPass {
                        at: Instant::now(),
                        held: outcome.held,
                    });
                    self.report(&outcome).await;
                    self.report_pass_outcome(false).await;
                }
                Err(e) => {
                    // A failed pass leaves every value reading at its last
                    // value, so a sweep that cannot run and a store that is not
                    // growing would look the same. This reading says the pass
                    // itself did not answer.
                    self.report_pass_outcome(true).await;
                    warn!(table = self.table, error = %e, "Checkpoint capacity sweep failed")
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(period) => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Measure the table and prune nothing.
    ///
    /// The ceiling is still computed: this deployment is not the one allowed to
    /// prune, but the reading it publishes is the same function of the same
    /// measurements, so a reader does not have to ask which replica swept.
    pub async fn measure_once(&self) -> SFResult<CheckpointSweepOutcome> {
        let held = self.row_count().await?;
        let bytes = self.table_bytes().await;
        let (target, target_source) =
            row_budget_against_volume(self.seed_budget, self.volume_bytes, held, bytes);
        Ok(CheckpointSweepOutcome {
            held,
            removed: 0,
            budget: None,
            target,
            target_source,
            held_above: false,
            bytes,
        })
    }

    /// Delete oldest-first until the table is within capacity, or until every
    /// row left is a board pointer.
    pub async fn sweep_once(&self) -> SFResult<CheckpointSweepOutcome> {
        self.sweep_once_beating(|| {}).await
    }

    /// The same pass, calling `progress` once before every batch.
    ///
    /// The loop hands `beat.beat` in here, because the work between two beats
    /// is otherwise the whole pass. A first pass over a store far over its
    /// budget deletes in batches and can outlast the wall the loop declared for
    /// itself, and the sweep paying for the event log's first pass measured
    /// that the hard way: a pass that ran for about fifteen minutes against a
    /// wall of six two-minute periods raised `background_loop_stalled` while it
    /// was working exactly as designed, which leaves the rule silent about the
    /// case it exists for — one batch wedged on a lock. Beating per batch bounds
    /// the published age by the gap the loop can actually bound, the same reason
    /// a loop draining a queue declares the bound on one item instead of on the
    /// queue.
    pub async fn sweep_once_beating(
        &self,
        progress: impl Fn(),
    ) -> SFResult<CheckpointSweepOutcome> {
        let held = self.row_count().await?;
        let bytes = self.table_bytes().await;
        let (target, target_source) =
            row_budget_against_volume(self.seed_budget, self.volume_bytes, held, bytes);
        let mut outcome = CheckpointSweepOutcome {
            held,
            removed: 0,
            budget: self.budget_enabled().then_some(target),
            target,
            target_source,
            held_above: false,
            bytes,
        };

        let Some(budget) = outcome.budget else {
            return Ok(outcome);
        };
        if held <= budget {
            return Ok(outcome);
        }

        let mut remaining = held - budget;
        // The pass stops early only on a batch that took nothing. That is not
        // the same claim as the count read back afterwards still standing above
        // the budget: the table is written while a pass runs, so a sweep that
        // took every row it was asked to can recount a few rows higher and read
        // as one the floor stopped.
        let mut stalled = false;
        while remaining > 0 {
            progress();
            let batch = self.delete_surplus(BATCH_ROWS.min(remaining)).await?;
            outcome.removed += batch;
            remaining -= batch as i64;
            if batch == 0 {
                stalled = true;
                break;
            }
        }

        outcome.held = self.row_count().await?;
        outcome.held_above = stalled && outcome.held > budget;
        Ok(outcome)
    }

    /// Delete up to `limit` rows older than `cutoff` that no board points at,
    /// oldest first.
    async fn delete_surplus(&self, limit: i64) -> SFResult<u64> {
        let sql = delete_surplus_sql(&self.table, &self.boards);
        Ok(sqlx::query(&sql)
            .bind(limit.max(1))
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected())
    }

    fn budget_enabled(&self) -> bool {
        self.seed_budget > 0
    }

    /// What the table costs on disk, when it can be read.
    async fn table_bytes(&self) -> Option<u64> {
        match crate::relation_bytes::relation_bytes(&self.pool, &self.table).await {
            Ok(bytes) => Some(bytes.max(0) as u64),
            Err(e) => {
                warn!(table = self.table, error = %e, "checkpoint store size unavailable");
                None
            }
        }
    }

    /// Publish what the table holds, what it may hold, whether the floor is
    /// what stops it, and what it costs on disk.
    async fn report(&self, outcome: &CheckpointSweepOutcome) {
        // The beat goes out even when nothing else does: a deployment that
        // holds no lease still recomputes the ceiling, and a stopped loop and a
        // steady table would otherwise read the same.
        if self.budget_enabled() {
            if let Some(ref readings) = self.readings {
                readings.evaluate(
                    KNOB,
                    outcome.target as f64,
                    UNIT_ROWS,
                    outcome.target_source == SOURCE_DERIVED,
                );
            }
        }
        let Some(ref mb) = self.metrics else { return };

        self.emit(
            mb,
            cog_core::metric_names::CHECKPOINTS_ROWS,
            outcome.held as f64,
        )
        .await;
        if let Some(budget) = outcome.budget {
            self.emit(
                mb,
                cog_core::metric_names::CHECKPOINTS_BUDGET_ROWS,
                budget as f64,
            )
            .await;
            // The floor verdict is published under the same condition as the
            // budget because it is the same claim: a pass with no budget did
            // not sweep, and a zero from here would say "nothing is being held
            // down" on behalf of a process that never looked.
            self.emit(
                mb,
                cog_core::metric_names::CHECKPOINTS_OVER_CAPACITY,
                outcome.held_above as u8 as f64,
            )
            .await;
        }

        if let Some(bytes) = outcome.bytes {
            self.emit(mb, cog_core::metric_names::CHECKPOINTS_BYTES, bytes as f64)
                .await;
        }
    }

    async fn report_pass_outcome(&self, failed: bool) {
        let Some(ref mb) = self.metrics else { return };
        publish_pass_outcome(mb, &self.labels(), failed).await;
    }

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
            warn!(error = %e, metric = %name, "checkpoint capacity gauge emit failed");
        }
    }

    async fn row_count(&self) -> SFResult<i64> {
        crate::relation_rows::relation_rows(&self.pool, &self.table).await
    }
}

async fn publish_pass_outcome(
    metrics: &Arc<dyn MetricsBackend>,
    labels: &HashMap<String, String>,
    failed: bool,
) {
    let value = if failed { 1.0 } else { 0.0 };
    if let Err(e) = metrics
        .record_gauge(
            cog_core::metric_names::CHECKPOINT_SWEEP_FAILED,
            value,
            labels.clone(),
        )
        .await
    {
        warn!(error = %e, "checkpoint sweep outcome emit failed");
    }
}

/// The statement a pass runs, with both tables already quoted.
///
/// Exported rather than inlined so a test explains the text the sweep executes
/// rather than a copy of it.
///
/// `$1` is the batch size. Two things hold at once: nothing a board field names
/// goes (the resume point), and what does go is the oldest first, so the index
/// on `timestamp` answers the order in one scan.
pub fn delete_surplus_sql(table: &str, boards: &str) -> String {
    format!(
        "DELETE FROM {table} WHERE snapshot_id IN (
             SELECT s.snapshot_id FROM {table} s
             WHERE NOT EXISTS (
                   SELECT 1 FROM {boards} b WHERE b.field_value = s.snapshot_id
               )
             ORDER BY s.timestamp, s.snapshot_id
             LIMIT $1
         )",
        table = cog_core::sql::quote_relation(table),
        boards = cog_core::sql::quote_relation(boards),
    )
}

/// How long to wait before the next pass.
///
/// Derived from how fast the table is filling: wait about as long as it would
/// take to reach the capacity at the rate the previous pass observed. A store
/// the floor is holding over capacity waits the longest period even though it
/// is not quiet, because deleting sooner is not allowed.
fn next_period(previous: Option<PreviousPass>, outcome: &CheckpointSweepOutcome) -> Duration {
    if outcome.held_above {
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
        return MAX_SWEEP_PERIOD;
    }
    let per_second = added as f64 / elapsed;
    let headroom = (budget - outcome.held + 1).max(1) as f64;
    let secs = headroom / per_second;
    if !secs.is_finite() {
        return MAX_SWEEP_PERIOD;
    }
    Duration::from_secs_f64(secs).clamp(MIN_SWEEP_PERIOD, MAX_SWEEP_PERIOD)
}

/// The row ceiling this process was told, or the default.
///
/// A value that cannot be read falls back rather than stopping the sweep: an
/// unparsable bound must not turn into "keep everything", which is the failure
/// this loop exists to prevent.
pub fn max_rows_from_env() -> u64 {
    max_rows_from(std::env::var(MAX_ROWS_ENV).ok().as_deref())
}

/// See [`max_rows_from_env`].
pub fn max_rows_from(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_ROWS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use cog_core::retention::SOURCE_SEED;

    fn outcome(held: i64, budget: Option<i64>, held_above: bool) -> CheckpointSweepOutcome {
        CheckpointSweepOutcome {
            held,
            removed: 0,
            budget,
            target: budget.unwrap_or(0),
            target_source: SOURCE_SEED,
            held_above,
            bytes: None,
        }
    }

    fn pass_ago(held: i64, secs: u64) -> Option<PreviousPass> {
        Some(PreviousPass {
            at: Instant::now() - Duration::from_secs(secs),
            held,
        })
    }

    /// Both outcomes of a pass land: the reading separates "the sweep found
    /// nothing to do" from "the sweep could not run", which is exactly the pair
    /// a frozen store and a quiet one otherwise share.
    #[tokio::test]
    async fn a_pass_outcome_is_written_on_both_branches() {
        let concrete = std::sync::Arc::new(crate::MemoryMetricsBackend::new());
        let metrics: std::sync::Arc<dyn MetricsBackend> =
            std::sync::Arc::clone(&concrete) as std::sync::Arc<dyn MetricsBackend>;
        let labels = HashMap::from([(DEPLOYMENT_LABEL.to_string(), "cogneva-app".to_string())]);

        let now = Utc::now();
        publish_pass_outcome(&metrics, &labels, false).await;
        publish_pass_outcome(&metrics, &labels, true).await;

        let rows = concrete
            .query_gauge_range(
                cog_core::metric_names::CHECKPOINT_SWEEP_FAILED.as_str(),
                now - chrono::Duration::minutes(5),
                now + chrono::Duration::minutes(5),
            )
            .await
            .expect("gauge history");
        assert_eq!(rows.len(), 2, "both passes publish a row of their own");
        assert_eq!(rows[0].value, 0.0, "a returned pass writes a 0");
        assert_eq!(rows[1].value, 1.0, "a failed pass writes a 1, not nothing");
        assert_eq!(
            rows[0].labels.get(DEPLOYMENT_LABEL),
            Some(&"cogneva-app".to_string())
        );
    }

    /// The statement names the window as a cutoff the caller binds, excludes
    /// every row a board points at, orders by the index that answers both the
    /// order and the cutoff, and takes the batch size as a bound rather than a
    /// literal. A rewrite that dropped the board exclusion would still delete
    /// rows — and would delete the resume point of a running task.
    #[test]
    fn the_delete_statement_keeps_the_order_and_the_pointers() {
        let sql = delete_surplus_sql("cog_snapshots", "cog_context_boards");
        assert!(!sql.contains("s.timestamp < $"), "no age floor: {sql}");
        assert!(
            sql.contains("NOT EXISTS (\n                   SELECT 1 FROM \"cog_context_boards\" b WHERE b.field_value = s.snapshot_id"),
            "the board pointer has to be an exclusion in the statement itself: {sql}"
        );
        assert!(sql.contains("ORDER BY s.timestamp, s.snapshot_id"), "{sql}");
        assert!(sql.contains("LIMIT $1"), "{sql}");
    }

    /// A store the floor holds over capacity waits the longest period: retrying
    /// sooner cannot delete anything, and the reading it publishes is the one
    /// that matters.
    #[test]
    fn a_store_held_over_capacity_waits_the_longest_period() {
        let held = outcome(50_000, Some(40_000), true);
        assert_eq!(next_period(pass_ago(49_000, 60), &held), MAX_SWEEP_PERIOD);
    }

    /// The wait follows the fill rate rather than a chosen number: a store
    /// filling fast is swept sooner than one that is nearly still.
    ///
    /// Both cases are picked so that each lands strictly inside the clamp. The
    /// first version of this test gave both sides a ten-thousand-row headroom
    /// against fill rates that each needed longer than the longest period to
    /// close it, so both came back at the ceiling and the assertion compared
    /// 120 s against 120 s — a statement about the clamp, not about the rate.
    #[test]
    fn the_wait_follows_the_fill_rate() {
        // 101 rows of headroom: the wait is decided by the rate, and neither
        // the period's floor nor its ceiling can be what the comparison sees.
        let fast = outcome(30_000, Some(30_100), false);
        let slow = outcome(30_000, Some(30_100), false);
        let fast_period = next_period(pass_ago(29_000, 60), &fast);
        let slow_period = next_period(pass_ago(29_900, 60), &slow);
        assert!(
            fast_period > MIN_SWEEP_PERIOD && slow_period < MAX_SWEEP_PERIOD,
            "both have to land inside the clamp, or this compares two ceilings \
             again: {fast_period:?} and {slow_period:?}"
        );
        assert!(
            fast_period < slow_period,
            "1000 rows a minute has to be swept sooner than 100: {fast_period:?} vs {slow_period:?}"
        );
    }

    /// 目标不变的一拍仍然是一拍：读数上的求值计数必须往上走，来源标签必须
    /// 从 seed 变成 derived。没有这一条，「回路在算」就只剩启动时那一次。
    #[tokio::test]
    async fn a_beat_whose_target_does_not_move_still_counts() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://probe:probe@127.0.0.1:1/probe")
            .expect("lazy pool");
        let readings = Arc::new(RetentionReadings::new());
        let cap = CheckpointCap::new(pool, 40_000)
            .with_readings(Arc::clone(&readings));

        cap.report(&CheckpointSweepOutcome {
            held: 1_000,
            removed: 0,
            budget: Some(524_288),
            target: 524_288,
            target_source: SOURCE_DERIVED,
            held_above: false,
            bytes: Some(1024 * 1024),
        })
        .await;
        let first = readings.reading(KNOB).expect("first beat");
        assert_eq!(first.0, 524_288.0);
        assert_eq!(first.1, 1);
        assert!(first.2, "the target came from a measurement");

        // 同一个目标：值不变，计数必须变。
        cap.report(&CheckpointSweepOutcome {
            held: 1_000,
            removed: 0,
            budget: Some(524_288),
            target: 524_288,
            target_source: SOURCE_DERIVED,
            held_above: false,
            bytes: Some(1024 * 1024),
        })
        .await;
        let second = readings.reading(KNOB).expect("second beat");
        assert_eq!(second.0, 524_288.0);
        assert_eq!(second.1, 2, "a steady target is still a beat");
        assert_eq!(readings.evaluations(KNOB), 2);
    }

    /// A bound that cannot be read falls back instead of switching the sweep
    /// off; an explicit zero is how a deployment opts out.
    #[test]
    fn a_bound_that_cannot_be_read_falls_back_instead_of_switching_off() {
        assert_eq!(max_rows_from(Some("not a number")), DEFAULT_MAX_ROWS);
        assert_eq!(max_rows_from(None), DEFAULT_MAX_ROWS);
        assert_eq!(
            max_rows_from(Some("0")),
            0,
            "an explicit zero is how a deployment opts out"
        );
    }
}
