//! 观测事件日志的容量读数：**不删行**，超了就把它记下来。
//!
//! `cog_observability_events` 是事实面：一条事件一行。事实不能被「到点删」——
//! 删掉的字节再也拿不回来，而这条表的价值恰恰在于「之后还能翻回来」。所以这里
//! 没有删行动作：容量是一个**读数**（持有多少、占多少字节、每天涨多少、余量还剩
//! 多少），超过读数时这一趟只做一件事——把「超了、而且没有人搬」说出来。
//!
//! 2026-10-27 的历史读数（2.92M 行 / 1.6 GB、每天约 30 万行）当时导向的是「按容量
//! 删」。2026-11-18 实测：表里 19,366 行、一天的量、每行 152 字节，活数据约 3 MB，
//! 而表文件是 430 MB（同一天实测）——那是被删空的页。清扫一遍遍删行留下坑，文件不缩；
//! 也就是说那套容量护住的不是磁盘，是它自己留下的坑。一次 `VACUUM FULL`（实测
//! 2026-11-18：430 MB 收回成 5 MB）正好证明这一点。
//!
//! 超读数之后的正确动作是**搬**：把最老的分区/行归档到对象存储，校验摘要，再从库里
//! 删。在那条路存在之前，这一趟只报告，不删。
//!
//! The one thing a capacity may never buy here is the read window. Readers ask
//! this table for "the events since T", and the historical-events view asks for
//! the last day; a sweep that deleted rows inside that window would answer a
//! well-formed query with a silent gap, which reads exactly like "nothing
//! happened then". So rows newer than the declared window are not deletable, no
//! matter what the capacity says. When the window alone holds more rows than
//! the capacity allows, the sweep stops at the window and says so — refusing to
//! delete further is the correct outcome, and a silent refusal would look
//! exactly like the capacity being met.
//!
//! That is a deliberately different floor from the metrics sample log's. There
//! the exempt rows are each series' newest (a gauge's value *is* its newest
//! row); here the exempt rows are the ones inside a time window, because the
//! question this table answers is a time range. Same shape — the sweep stops
//! short and reports it — for two different reasons a reader must not conflate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::retention::{
    longest_read_span, read_window_secs, row_budget_against_volume, RetentionReadings,
    SOURCE_DERIVED, UNIT_ROWS, UNIT_SECONDS,
};

/// The registry key this loop's target answers to.
pub const KNOB: &str = "observability.eventLog.maxRows";

/// The registry key this loop's read window answers to.
pub const KNOB_READ_WINDOW: &str = cog_core::retention::OBSERVABILITY_EVENTS_READ_WINDOW_KNOB;

use cog_core::{MetricsBackend, SFResult, ShutdownSignal};

/// The append-only event log written by the observability gateway.
pub const EVENTS_TABLE: &str = "cog_observability_events";

/// The role this loop takes a lease on. Only the holder prunes, so that
/// replicas sharing one database do not each hold the table down.
pub const ROLE: &str = "observability_event_cap";

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "storage_observability_event_cap";

/// The label that names the deployment a reading below came from.
///
/// Same reason as the metrics sample log's: the table is shared, the readings
/// land in a store every deployment writes to, and a reading without a name is
/// served by whichever process the scrape reaches — so a deployment that
/// declares no capacity would answer with the one the deployment beside it
/// declared.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// Rows the table may hold by default.
///
/// One million rows is about three days of events at the ordinary rate and
/// under a day and a half at the worst rate seen so far, so the capacity binds
/// well outside the read window on a normal day and inside it during a storm —
/// which is what the window verdict is for. It is a default for the deployment
/// that does not say otherwise, not a policy: the deployment states its own.
// cap: fixed — 平常日子在读取窗之外、风暴期在窗之内，那正是窗口判据要回答的；这是不说自己容量的部署的默认值，不是策略
// keep: 删掉后不说自己容量的部署没有默认值，窗口判据没有可越的地板；由「不说自己容量的部署的默认值」兜住
pub const DEFAULT_MAX_ROWS: u64 = 1_000_000;

/// How far back readers are promised rows, in seconds.
///
/// Not a retention knob — it is the floor the capacity may not cross. It
/// matches the default window of the historical-events query, so a reader that
/// asks for what it asks for by default never sees a gap the sweep made.
// cap: fixed — 不是保留旋钮，是容量不许越过的地板：与历史事件查询的默认窗同值，按默认问的人不会看见清扫挖出的缺口
// keep: 删掉后清扫可以越过读者默认问的窗口，按默认问的人看见缺口；由「容量不许越过的地板」兜住
pub const DEFAULT_READ_WINDOW_SECS: u64 =
    cog_core::retention::OBSERVABILITY_EVENTS_READ_WINDOW_DEFAULT_SECS;

/// The environment variables that carry the two numbers.
///
/// Read from the environment rather than from [`cog_core::Config`] because
/// they are not platform policy — they are this loop's own bounds, and the
/// process that runs the loop is the one the deployment told.
pub const MAX_ROWS_ENV: &str = "COGNEVA_OBSERVABILITY_EVENT_MAX_ROWS";
/// See [`MAX_ROWS_ENV`].
pub const READ_WINDOW_ENV: &str = cog_core::retention::OBSERVABILITY_EVENTS_READ_WINDOW_ENV;

/// Longest the sweeper will wait between passes.
///
/// Not a policy: the sweep also republishes the table's size, and Prometheus
/// drops a series it has not seen for five minutes rather than holding the
/// last value. A period at or beyond that would make the size reading vanish
/// exactly when the table has gone quiet.
// cap: fixed — 到五分钟 Prometheus 会丢掉这条序列，表一安静大小读数就跟着消失
// keep: 删掉后清扫周期长到五分钟，表一安静这条序列就从 Prometheus 消失；由「5 分钟丢序列」兜住
const MAX_SWEEP_PERIOD: Duration = Duration::from_secs(120);

/// Shortest the sweeper will wait between passes. A guard against a control
/// loop that never sleeps, not a cadence anyone is choosing.
// cap: fixed — 防的是一个从不睡眠的控制循环，不是谁在挑节拍
// keep: 删掉后一个从不睡眠的清扫循环把库打满；由「防的是从不睡眠的控制循环」兜住
const MIN_SWEEP_PERIOD: Duration = Duration::from_secs(1);

/// The cadence this loop declares for itself: the longest it will wait
/// between two passes.
pub const LOOP_PERIOD: Duration = MAX_SWEEP_PERIOD;

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSweepOutcome {
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
    /// Set when the read window stopped the pass short of the capacity. The
    /// table is over budget and no amount of further sweeping may fix it.
    pub window_held: bool,
    /// The table's size on disk when the pass started, when it could be read.
    pub bytes: Option<u64>,
    /// The floor this pass enforced: the declaration, widened by the longest
    /// span a reader actually asked for.
    pub window_secs: u64,
    /// Whether `window_secs` was widened by a measurement rather than standing
    /// on the declaration.
    pub window_derived: bool,
}

/// Holds the observability event log under a row capacity, outside the read
/// window.
pub struct EventLogCap {
    pool: PgPool,
    table: String,
    /// The row ceiling this deployment declared. It is what a process with no
    /// measured cost keeps, and zero turns pruning off.
    seed_budget: i64,
    /// The declared size of the database volume; with it and the measured cost
    /// of a row, every pass recomputes the ceiling.
    volume_bytes: Option<u64>,
    read_window: Duration,
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

impl EventLogCap {
    /// `budget` is the row ceiling; 0 turns pruning off, which is also how a
    /// deployment opts out — every deployment sharing the database would
    /// otherwise sweep the same rows.
    pub fn new(pool: PgPool, budget: u64) -> Self {
        Self {
            pool,
            table: EVENTS_TABLE.to_string(),
            seed_budget: budget as i64,
            volume_bytes: None,
            read_window: Duration::from_secs(DEFAULT_READ_WINDOW_SECS),
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

    /// The floor this beat enforces, and whether a reader's question widened it.
    fn effective_read_window(&self) -> (u64, bool) {
        read_window_secs(
            self.read_window.as_secs(),
            longest_read_span(KNOB_READ_WINDOW),
        )
    }

    /// The window below which no row is deletable.
    pub fn with_read_window(mut self, window: Duration) -> Self {
        self.read_window = window;
        self
    }

    /// Prune a table other than the live one. Exists so a test can point the
    /// sweeper at a throwaway table.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
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

    /// Spawn the sweeper as a supervised loop under this file's own name, so
    /// the name the reading reports and the name the loop is declared under
    /// cannot become two spellings of the same loop.
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
                            "Pruned observability events over capacity"
                        );
                    }
                    if outcome.window_held {
                        warn!(
                            table = self.table,
                            held = outcome.held,
                            budget = outcome.budget,
                            window_secs = outcome.window_secs,
                            "Observability event log is over capacity and cannot be pruned \
                             further without deleting rows inside the read window"
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
                    // value, and the capacity rules keep trusting them while
                    // their companion clock is within its age bound — so a
                    // sweep that cannot run and a table that is not growing
                    // would look the same. This reading says the pass itself
                    // did not answer.
                    self.report_pass_outcome(true).await;
                    warn!(table = self.table, error = %e, "Observability event capacity sweep failed")
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
    /// What a process owes its readers when it may not prune: the size is a
    /// property of the table, not of who may delete from it, and the budget a
    /// process that is not pruning reports as `None` is exactly the one it is
    /// not enforcing.
    pub async fn measure_once(&self) -> SFResult<EventSweepOutcome> {
        let held = self.row_count().await?;
        let bytes = self.table_bytes().await;
        let (target, target_source) =
            row_budget_against_volume(self.seed_budget, self.volume_bytes, held, bytes);
        let (window_secs, window_derived) = self.effective_read_window();
        Ok(EventSweepOutcome {
            held,
            removed: 0,
            budget: None,
            target,
            target_source,
            window_held: false,
            bytes,
            window_secs,
            window_derived,
        })
    }

    /// Delete oldest-first until the table is within capacity, or until the
    /// only rows left are the ones inside the read window.
    pub async fn sweep_once(&self) -> SFResult<EventSweepOutcome> {
        self.sweep_once_beating(|| {}).await
    }

    /// The same pass, calling `progress` once before every batch.
    ///
    /// The loop hands `beat.beat` in here, because the work between two beats
    /// is otherwise the whole pass: measured on 2026-10-27, a first pass over a
    /// log 751,203 rows over its budget ran for about fifteen minutes against a
    /// declared period of two and a wall of six of them, so
    /// `background_loop_stalled` fired on a sweep that was working exactly as
    /// designed — and a rule that fires on the repair is silent about the case
    /// it exists for, one batch wedged on a lock. Beating per batch bounds the
    /// published age by the gap the loop can actually bound, which is the same
    /// reason a loop draining a queue declares the bound on one item instead of
    /// on the queue.
    pub async fn sweep_once_beating(&self, progress: impl Fn()) -> SFResult<EventSweepOutcome> {
        let held = self.row_count().await?;
        let bytes = self.table_bytes().await;
        let (target, target_source) =
            row_budget_against_volume(self.seed_budget, self.volume_bytes, held, bytes);
        let (window_secs, window_derived) = self.effective_read_window();
        let mut outcome = EventSweepOutcome {
            held,
            removed: 0,
            budget: self.budget_enabled().then_some(target),
            target,
            target_source,
            window_held: false,
            bytes,
            window_secs,
            window_derived,
        };

        let Some(budget) = outcome.budget else {
            return Ok(outcome);
        };
        if held <= budget {
            return Ok(outcome);
        }

        // 超过这一跑算出的容量时，**不删**。
        //
        // 「删」是丢数据；这条日志是事实面的一部分，删掉就再也拿不回来。容量这个词
        // 在这里的意思因此只有「读到这儿为止可读的那一段」，而它一旦被超过，正确的
        // 动作是**把它搬走**（归档到对象存储、校验摘要、再从库里删），不是就地丢。
        // 在那条路存在之前，这一趟只做一件事：把「超了、而且没人搬」记成读数。
        //
        // 2026-11-18 实测（这张表就是本机这 Ten Gigabyte 的卷上的那张）：19,366 行、
        // 2026-11-18 实测：每行 152 字节、一天的量；表文件却有 430 MB——那是被删空的页，清扫一遍遍删行
        // 留下坑，文件不缩。也就是说这套容量在这一天里护住的不是磁盘，是它自己的坑。
        progress();
        outcome.held = self.row_count().await?;
        outcome.window_held = outcome.held > budget;
        Ok(outcome)
    }

    fn budget_enabled(&self) -> bool {
        self.seed_budget > 0
    }

    /// What the table costs on disk, when it can be read.
    async fn table_bytes(&self) -> Option<u64> {
        match crate::relation_bytes::relation_bytes(&self.pool, &self.table).await {
            Ok(bytes) => Some(bytes.max(0) as u64),
            Err(e) => {
                warn!(table = self.table, error = %e, "event log size unavailable");
                None
            }
        }
    }

    /// Publish what the table holds, what it may hold, whether the window is
    /// what stops it, and what it costs on disk.
    async fn report(&self, outcome: &EventSweepOutcome) {
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
        // The floor is a promise to readers whether or not this deployment
        // prunes, and its beat is the same evidence as the ceiling's.
        if let Some(ref readings) = self.readings {
            readings.evaluate(
                KNOB_READ_WINDOW,
                outcome.window_secs as f64,
                UNIT_SECONDS,
                outcome.window_derived,
            );
        }
        let Some(ref mb) = self.metrics else { return };

        self.emit(
            mb,
            cog_core::metric_names::OBSERVABILITY_EVENTS_ROWS,
            outcome.held as f64,
        )
        .await;
        if let Some(budget) = outcome.budget {
            self.emit(
                mb,
                cog_core::metric_names::OBSERVABILITY_EVENTS_BUDGET_ROWS,
                budget as f64,
            )
            .await;
            // The window verdict is published under the same condition as the
            // budget because it is the same claim: a pass with no budget did
            // not sweep, and a zero from here would say "nothing is being held
            // down" on behalf of a process that never looked.
            self.emit(
                mb,
                cog_core::metric_names::OBSERVABILITY_EVENTS_OVER_CAPACITY,
                outcome.window_held as u8 as f64,
            )
            .await;
        }

        if let Some(bytes) = outcome.bytes {
            self.emit(
                mb,
                cog_core::metric_names::OBSERVABILITY_EVENTS_BYTES,
                bytes as f64,
            )
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
            warn!(error = %e, metric = %name, "observability event log gauge emit failed");
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
            cog_core::metric_names::OBSERVABILITY_EVENT_SWEEP_FAILED,
            value,
            labels.clone(),
        )
        .await
    {
        warn!(error = %e, "observability event sweep outcome emit failed");
    }
}

/// How long to wait before the next pass.
///
/// Derived from how fast the table is filling: wait about as long as it would
/// take to reach the capacity at the rate the previous pass observed. A table
/// the read window is holding over capacity waits the longest period even
/// though it is not quiet, because deleting sooner is not allowed and retrying
/// cannot help.
fn next_period(previous: Option<PreviousPass>, outcome: &EventSweepOutcome) -> Duration {
    if outcome.window_held {
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

/// The read window this process was promised, or the default.
pub fn read_window_from_env() -> Duration {
    read_window_from(std::env::var(READ_WINDOW_ENV).ok().as_deref())
}

/// See [`read_window_from_env`].
///
/// A window of zero would make every row deletable the moment it is written, so
/// a zero or unparsable value falls back to the default instead — the floor is
/// the promise to readers, and no configuration may remove it.
pub fn read_window_from(raw: Option<&str>) -> Duration {
    Duration::from_secs(cog_core::retention::read_window_seed(
        raw,
        DEFAULT_READ_WINDOW_SECS,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::retention::SOURCE_SEED;

    fn outcome(held: i64, budget: Option<i64>, window_held: bool) -> EventSweepOutcome {
        EventSweepOutcome {
            held,
            removed: 0,
            budget,
            target: budget.unwrap_or(0),
            target_source: SOURCE_SEED,
            window_held,
            bytes: None,
            window_secs: DEFAULT_READ_WINDOW_SECS,
            window_derived: false,
        }
    }

    // proves: observability.eventLog.maxRows
    /// 目标不变的一拍仍然是一拍：求值计数往上走，来源标签从 seed 变成
    /// derived。没有这一条，「回路在算」就只剩启动时那一次。
    #[tokio::test]
    async fn a_beat_whose_target_does_not_move_still_counts() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://probe:probe@127.0.0.1:1/probe")
            .expect("lazy pool");
        let readings = Arc::new(RetentionReadings::new());
        let cap = EventLogCap::new(pool, 100_000).with_readings(Arc::clone(&readings));

        for expected in 1..=2 {
            cap.report(&EventSweepOutcome {
                held: 80_000,
                removed: 0,
                budget: Some(120_000),
                target: 120_000,
                target_source: SOURCE_DERIVED,
                window_held: false,
                bytes: Some(64 * 1024 * 1024),
                window_secs: DEFAULT_READ_WINDOW_SECS,
                window_derived: false,
            })
            .await;
            let (target, beats, derived) = readings.reading(KNOB).expect("a beat");
            assert_eq!(target, 120_000.0);
            assert_eq!(beats, expected, "a steady target is still a beat");
            assert_eq!(readings.evaluations(KNOB), expected);
            // The read window is recomputed and recorded by the same pass, so
            // its count has to move with the ceiling's.
            assert_eq!(readings.evaluations(KNOB_READ_WINDOW), expected);
            assert!(derived);
        }
    }

    // proves: observability.eventLog.readWindowSeconds
    /// 地板跟着读者实际问过的跨度走：问得更宽，下一拍就变宽并标 derived；
    /// 问得比声明窄，声明仍是生效值。
    #[tokio::test]
    async fn the_read_window_follows_the_longest_question_asked() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://probe:probe@127.0.0.1:1/probe")
            .expect("lazy pool");
        let cap = EventLogCap::new(pool, 100_000);
        assert_eq!(
            cap.effective_read_window(),
            (DEFAULT_READ_WINDOW_SECS, false)
        );
        cog_core::retention::record_read_span(KNOB_READ_WINDOW, DEFAULT_READ_WINDOW_SECS / 2);
        assert_eq!(
            cap.effective_read_window(),
            (DEFAULT_READ_WINDOW_SECS, false),
            "a narrower question does not shrink the floor"
        );
        cog_core::retention::record_read_span(KNOB_READ_WINDOW, DEFAULT_READ_WINDOW_SECS * 7);
        assert_eq!(
            cap.effective_read_window(),
            (DEFAULT_READ_WINDOW_SECS * 7, true)
        );
    }

    fn pass_ago(held: i64, secs: u64) -> Option<PreviousPass> {
        Some(PreviousPass {
            at: Instant::now() - Duration::from_secs(secs),
            held,
        })
    }

    /// Both outcomes of a pass land: the reading separates "the sweep found
    /// nothing to do" from "the sweep could not run", which is exactly the pair
    /// a frozen table and a quiet one otherwise share.
    #[tokio::test]
    async fn a_pass_outcome_is_written_on_both_branches() {
        let concrete = std::sync::Arc::new(crate::MemoryMetricsBackend::new());
        let metrics: std::sync::Arc<dyn MetricsBackend> =
            std::sync::Arc::clone(&concrete) as std::sync::Arc<dyn MetricsBackend>;
        let labels = HashMap::from([(DEPLOYMENT_LABEL.to_string(), "cogneva-app".to_string())]);

        let now = chrono::Utc::now();
        publish_pass_outcome(&metrics, &labels, false).await;
        publish_pass_outcome(&metrics, &labels, true).await;

        let rows = concrete
            .query_gauge_range(
                cog_core::metric_names::OBSERVABILITY_EVENT_SWEEP_FAILED.as_str(),
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

    /// 同一批事件重新导出必须得到同一串字节：读回来逐字节比对那条校验全靠它。
    #[test]
    fn the_same_batch_serialises_to_the_same_bytes() {
        let events = vec![ArchivedEvent {
            id: 1,
            event_type: "MessageUpdate".into(),
            agent_id: Some("a".into()),
            task_id: None,
            crew_id: None,
            squad_id: None,
            payload: "{\"k\":1}".into(),
            timestamp: chrono::DateTime::<chrono::Utc>::MIN_UTC,
        }];
        let once = ndjson_of(&events).expect("serialises");
        let twice = ndjson_of(&events).expect("serialises");
        assert_eq!(once, twice);
        assert!(once.ends_with(b"\n"), "one line per event");
    }

    /// 键里带首尾 id 与摘要前八位：对象自己能对上「它是不是那份字节」。
    #[test]
    fn the_key_names_the_span_and_the_digest() {
        let key = archive_key("events/archive", 7, 42, "0123456789abcdef");
        assert_eq!(key, "events/archive/7-42-01234567.ndjson");
    }

    /// A table the window holds over capacity waits the longest period instead
    /// of retrying sooner: deleting sooner is not allowed, so a short period
    /// would only be a loop spinning on the same refusal.
    #[test]
    fn a_window_held_table_waits_the_longest_period() {
        let o = outcome(2_000_000, Some(1_000_000), true);
        assert_eq!(next_period(pass_ago(1_900_000, 10), &o), MAX_SWEEP_PERIOD);
    }

    /// A table at its budget is one row away from work, so the wait is until
    /// the first row that will need pruning rather than until the capacity.
    #[test]
    fn the_wait_follows_the_fill_rate() {
        let o = outcome(900_000, Some(1_000_000), false);
        // 100,000 rows in 100 s is 1,000 rows/s, so 100,001 rows of headroom is
        // about 100 s — well inside the clamp.
        let period = next_period(pass_ago(800_000, 100), &o);
        assert!(
            period >= Duration::from_secs(99) && period <= Duration::from_secs(101),
            "expected about 100 s, got {period:?}"
        );
        assert_eq!(next_period(None, &o), MIN_SWEEP_PERIOD);
        assert_eq!(next_period(pass_ago(900_000, 10), &o), MAX_SWEEP_PERIOD);
    }

    /// The two env readers fall back rather than disabling anything. An
    /// unparsable ceiling must not read as "no ceiling", and a zero window must
    /// not read as "delete everything at once" — both fall back to the
    /// promises this module makes by default.
    #[test]
    fn a_bound_that_cannot_be_read_falls_back_instead_of_switching_off() {
        for raw in [None, Some(""), Some("  "), Some("ten"), Some("-1")] {
            assert_eq!(max_rows_from(raw), DEFAULT_MAX_ROWS, "raw={raw:?}");
        }
        assert_eq!(max_rows_from(Some(" 5000 ")), 5_000);
        // Zero is the documented way to switch pruning off, and it is a value
        // the operator typed — not a value that failed to parse.
        assert_eq!(max_rows_from(Some("0")), 0);

        for raw in [None, Some(""), Some("0"), Some("ten")] {
            assert_eq!(
                read_window_from(raw),
                Duration::from_secs(DEFAULT_READ_WINDOW_SECS),
                "raw={raw:?}"
            );
        }
        assert_eq!(read_window_from(Some(" 3600 ")), Duration::from_secs(3_600));
    }
}

// ── 归档：唯一允许删的那条路 ────────────────────────────────────────────────
//
// 删一行的前提是「它已经在别处、而且那一份校验过」。所以这条路的顺序是死的：
// 读一批 → 写成一份对象 → **读回来逐字节比对** → 才删这一批的行。任何一步不成立，
// 那一批一行都不删；失败原因进返回值，调用方据此报读数，而不是安静地继续。

/// 一批老事件在对象存储里的样子：一行一条事件，字段顺序固定，字节可复现。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArchivedEvent {
    pub id: i32,
    pub event_type: String,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub crew_id: Option<String>,
    pub squad_id: Option<String>,
    pub payload: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// 把一批事件写成 NDJSON：一行一条，行内字段顺序由结构体固定。
///
/// 顺序固定是判据的一部分：同一批行重新导出必须得到同一串字节，否则「读回来逐字节
/// 相等」这条校验就永远只能靠运气。
pub fn ndjson_of(events: &[ArchivedEvent]) -> SFResult<Vec<u8>> {
    let mut out = Vec::new();
    for event in events {
        let line = serde_json::to_vec(event).map_err(cog_core::SFError::Serialization)?;
        out.extend_from_slice(&line);
        out.push(b'\n');
    }
    Ok(out)
}

/// 这一批在对象存储里的键：前缀 / 年月 / 表名 / 首尾 id / 摘要前 8 位。
///
/// 摘要进名字而不是只进元数据：拿回来的对象能自己对上「它是不是那份字节」，
/// 不必再问一次库。
pub fn archive_key(prefix: &str, first: i32, last: i32, digest: &str) -> String {
    let short = &digest[..8.min(digest.len())];
    format!("{prefix}/{first}-{last}-{short}.ndjson")
}

/// 归档一批老事件，校验通过后删掉它们。
///
/// `cutoff` 之外（更老）的行才可归档；批大小由调用方给，一次一批，删一行一批地推进。
/// 返回 `Ok(None)` 表示没有可归档的行。
pub async fn archive_batch(
    pool: &PgPool,
    table: &str,
    backend: &Arc<dyn cog_core::ObjectBackend>,
    prefix: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    batch: i64,
) -> SFResult<Option<ArchiveOutcome>> {
    let table = cog_core::sql::quote_relation(table);
    // 一行的原始列：id、种类、四个归属列、正文、时刻。宽是这张表的形状；拆成结构体
    // 只是把同一条 SELECT 再抄一遍，所以这里就按那八列写。
    #[allow(clippy::type_complexity)]
    type EventRow = (
        i32,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        chrono::DateTime<chrono::Utc>,
    );
    let rows: Vec<EventRow> = sqlx::query_as(&format!(
        "SELECT id, event_type, agent_id, task_id, crew_id, squad_id, payload, timestamp \
             FROM {table} WHERE timestamp < $1 ORDER BY timestamp, id LIMIT $2"
    ))
    .bind(cutoff)
    .bind(batch.max(1))
    .fetch_all(pool)
    .await
    .map_err(|e| cog_core::SFError::Database(e.to_string()))?;
    if rows.is_empty() {
        return Ok(None);
    }
    let events: Vec<ArchivedEvent> = rows
        .into_iter()
        .map(
            |(id, event_type, agent_id, task_id, crew_id, squad_id, payload, timestamp)| {
                ArchivedEvent {
                    id,
                    event_type,
                    agent_id,
                    task_id,
                    crew_id,
                    squad_id,
                    payload,
                    timestamp,
                }
            },
        )
        .collect();
    let bytes = ndjson_of(&events)?;
    let digest = blake3::hash(&bytes).to_hex().to_string();
    let key = archive_key(prefix, events[0].id, events[events.len() - 1].id, &digest);
    backend.put(&key, &bytes).await?;
    // 读回来逐字节比对：写成功不等于写得对，而这一批马上要被删掉。
    let back = backend
        .get(&key)
        .await?
        .ok_or_else(|| cog_core::SFError::IO(format!("{key} 写进去之后读不回来：这一批不删")))?;
    if back != bytes {
        return Err(cog_core::SFError::IO(format!(
            "{key} 读回来的字节与写进去的不一样（写了 {} 字节、读回 {} 字节）：这一批不删",
            bytes.len(),
            back.len()
        )));
    }
    let ids: Vec<i32> = events.iter().map(|event| event.id).collect();
    let deleted = sqlx::query(&format!("DELETE FROM {table} WHERE id = ANY($1)"))
        .bind(&ids)
        .execute(pool)
        .await
        .map_err(|e| cog_core::SFError::Database(e.to_string()))?
        .rows_affected();
    Ok(Some(ArchiveOutcome {
        key,
        digest,
        archived: events.len(),
        deleted,
    }))
}

/// 一批归档的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveOutcome {
    pub key: String,
    pub digest: String,
    pub archived: usize,
    pub deleted: u64,
}
