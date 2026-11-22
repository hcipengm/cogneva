//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//! 检查点仓库的规模是一条**读数**，不是一个到点执行的删除循环。
//!
//! `cog_snapshots` 每一行是一次检查点：一个 agent 的状态与上下文窗口。一次检查点回合写新行、
//! 把 id 记到任务板上、再删掉被替换的那一行；已经结束的任务最后一次检查点没人删、任务板也
//! 没人删，于是平台跑过的每个任务都在这里永久留下一行。
//!
//! 实测 2026-10-27：54,452 行、表与索引合计 924 MB，其中 39,610 行（约 580 MB）没有任何
//! 任务板字段指向它们。这些行每一行都进每晚的备份包，而备份演练要把整份 dump 恢复一遍才算
//! 证明——于是这些垃圾每晚都要付一次恢复的账。
//!
//! 从前这里的行为是：给行数一个上限，超了就从最老的开删，**但永不删任务板指向的**（那是
//! 续跑点，删掉等于这个任务再也起不来），删不动就说「触底」。这套东西治不了它要治的病：
//!
//! 1. 「哪些行是垃圾」本来就不是按年龄能回答的——一行是不是垃圾由任务板决定，而任务板永不
//!    退役，所以「已经结束」这件事在这张表里读不出来。
//! 2. 删除不可回读：删掉的字节没有任何地方留痕。事后问「某个任务那次检查点还在不在」，答案
//!    只能按当时的界去推断。
//! 3. 「触底」是一个判据顶着另一个判据：容量说该删，任务板说不能删，循环于是每轮自报超容量
//!    而一行不删——一个永远亮着的红，等于没有红。
//!
//! 所以这一族改成**只测量、不删除**：每拍把「现在有多少行、声明的容量是多少、按卷大小算出
//! 来能放多少行、字节多少、超没超」发出去。超了就是超了，让它在面板上红着，由人决定抬容量、
//! 退役已结束任务的板、还是承认它就该长。任务板指针那条不变式从「删除时的地板」变成
//! 「没有任何东西会删」——这是更强的那一版，而不是更弱的。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tracing::warn;

use cog_core::retention::{
    row_budget_against_volume, RetentionReadings, SOURCE_DERIVED, UNIT_ROWS,
};
use cog_core::{MetricsBackend, SFResult, ShutdownSignal};

/// The registry key this loop's target answers to.
pub const KNOB: &str = "checkpointStore.maxRows";

/// The table the checkpoint store writes.
pub const SNAPSHOTS_TABLE: &str = "cog_snapshots";

/// The board table whose field values name the snapshots a task would resume
/// from.
///
/// Nothing here reads it any more — the loop deletes nothing, so there is no
/// pointer to protect. It stays named because the *boards* are what would have
/// to change before this store could ever be trimmed, and a reader asking "who
/// decides what is garbage" needs the name in hand.
pub const BOARDS_TABLE: &str = "cog_context_boards";

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
/// and well below the table. It is the declaration a deployment makes when it
/// does not say otherwise, not a policy: nothing acts on it.
// cap: bound — 这是不说自己容量的部署的默认声明值，不是策略；它不再驱动任何删除
pub const DEFAULT_MAX_ROWS: u64 = 40_000;

/// The environment variable a deployment overrides the declaration with.
pub const MAX_ROWS_ENV: &str = "COGNEVA_CHECKPOINT_MAX_ROWS";

/// How often the store is measured.
///
/// 一分钟是五分钟这个**约定**的五分之一：Prometheus 的默认 lookback delta 是
/// 五分钟，超过它这一条序列就从抓取面上消失——不是变成旧值，是不存在。所以一个
/// 「现在有多大」的读数必须比那个窗口刷新得更勤。
// cap: fixed — Prometheus 的默认 lookback delta = 5 分钟，读数须在其内至少刷新一次；这里取 1/5
pub const LOOP_PERIOD: Duration = Duration::from_secs(60);

/// 读一次仓库得到的那几个数。**一个都不来自删除**。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReading {
    /// Rows the store holds.
    pub held: i64,
    /// The capacity this deployment declares, or the one its volume implies;
    /// `None` when it declares neither.
    pub target: Option<i64>,
    /// Which input decided [`Self::target`]: the deployment's own declaration,
    /// or the measured cost of a row against the declared volume.
    pub target_source: &'static str,
    /// `held > target`, decided only when there is a target to compare to.
    pub over_capacity: bool,
    /// The table's size on disk when it could be read.
    pub bytes: Option<u64>,
}

/// Measures the checkpoint store. Removes nothing from it.
pub struct CheckpointCap {
    pool: PgPool,
    table: String,
    /// The row ceiling this deployment declared. Zero means it declared none.
    seed_budget: i64,
    /// The declared size of the database volume; with it and the measured cost
    /// of a row, every pass computes what the volume would hold.
    volume_bytes: Option<u64>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    readings: Option<Arc<RetentionReadings>>,
    deployment: Option<String>,
}

impl CheckpointCap {
    /// `budget` is the declared row ceiling; `0` declares none.
    pub fn new(pool: PgPool, budget: u64) -> Self {
        Self {
            pool,
            table: SNAPSHOTS_TABLE.to_string(),
            seed_budget: budget as i64,
            volume_bytes: None,
            metrics: None,
            readings: None,
            deployment: None,
        }
    }

    /// The declared size of the volume this table lives on.
    ///
    /// With a volume and a measured cost per row, the reading carries what the
    /// volume would hold; without one the deployment's own declaration stands.
    /// The volume changes only with a resize, so it is read once.
    pub fn with_database_volume(mut self, bytes: Option<u64>) -> Self {
        self.volume_bytes = bytes;
        self
    }

    /// Attach the reading face that carries this loop's target and beat count.
    pub fn with_readings(mut self, readings: Arc<RetentionReadings>) -> Self {
        self.readings = Some(readings);
        self
    }

    /// Point the loop at another table. Exists so a test can use a throwaway
    /// one instead of the live checkpoint store.
    pub fn with_table(mut self, snapshots: impl Into<String>) -> Self {
        self.table = snapshots.into();
        self
    }

    /// Name this deployment on the readings this loop publishes.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
        self
    }

    /// Attach a metrics backend so each pass reports what the table holds and
    /// what that costs on disk.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Spawn the measuring loop as a supervised loop under this file's own name.
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

    /// Measure once immediately, then once per [`LOOP_PERIOD`], until shutdown.
    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        loop {
            beat.beat();
            match self.measure_once().await {
                Ok(reading) => {
                    if reading.over_capacity {
                        warn!(
                            table = self.table,
                            held = reading.held,
                            target = reading.target,
                            "Checkpoint store holds more rows than its capacity says; nothing \
                             deletes it, so this reading stays red until somebody decides what \
                             to do with it"
                        );
                    }
                    self.report(&reading).await;
                    self.report_pass_outcome(false).await;
                }
                Err(e) => {
                    // A failed pass leaves every value reading at its last
                    // value, so a loop that cannot run and a store that is not
                    // growing would look the same. This reading says the pass
                    // itself did not answer.
                    self.report_pass_outcome(true).await;
                    warn!(table = self.table, error = %e, "Checkpoint store measurement failed")
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(LOOP_PERIOD) => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Read the store: how many rows, what it costs, what the capacity says.
    pub async fn measure_once(&self) -> SFResult<CheckpointReading> {
        let held = self.row_count().await?;
        let bytes = self.table_bytes().await;
        let (target, target_source) =
            row_budget_against_volume(self.seed_budget, self.volume_bytes, held, bytes);
        let target = (self.seed_budget > 0 || self.volume_bytes.is_some()).then_some(target);
        Ok(CheckpointReading {
            held,
            target,
            target_source,
            over_capacity: target.is_some_and(|target| held > target),
            bytes,
        })
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

    /// Publish what the table holds, what its capacity says, and what it costs.
    ///
    /// The beat goes out even when nothing else does: a deployment that declares
    /// no capacity still recomputes the target, and a stopped loop and a steady
    /// table would otherwise read the same.
    async fn report(&self, reading: &CheckpointReading) {
        if self.seed_budget > 0 {
            if let Some(ref readings) = self.readings {
                let target = reading.target.unwrap_or(self.seed_budget);
                readings.evaluate(
                    KNOB,
                    target as f64,
                    UNIT_ROWS,
                    reading.target_source == SOURCE_DERIVED,
                );
            }
        }
        let Some(ref mb) = self.metrics else { return };

        self.emit(
            mb,
            cog_core::metric_names::CHECKPOINTS_ROWS,
            reading.held as f64,
        )
        .await;
        if let Some(target) = reading.target {
            self.emit(
                mb,
                cog_core::metric_names::CHECKPOINTS_BUDGET_ROWS,
                target as f64,
            )
            .await;
            // The verdict is published under the same condition as the capacity
            // because it is the same claim: a deployment that declares no
            // capacity has none to be over, and a zero written from here would
            // say "this store is fine" on behalf of a process that declared
            // nothing.
            self.emit(
                mb,
                cog_core::metric_names::CHECKPOINTS_OVER_CAPACITY,
                reading.over_capacity as u8 as f64,
            )
            .await;
        }
        if let Some(bytes) = reading.bytes {
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

/// The declared ceiling, from the environment, or the default.
pub fn max_rows_from_env() -> u64 {
    std::env::var(MAX_ROWS_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_ROWS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(held: i64, target: Option<i64>) -> CheckpointReading {
        CheckpointReading {
            held,
            target,
            target_source: "declared",
            over_capacity: target.is_some_and(|target| held > target),
            bytes: None,
        }
    }

    /// A store past its capacity reads as over it; one at the capacity does not,
    /// and the verdict does not care which input produced the capacity.
    ///
    /// The boundary is the whole reading: off by one here and a dashboard says
    /// "over" for a store that is exactly at its limit. Both sources are checked
    /// because a target derived from the volume is compared with the same `>` as
    /// a declared one, and a second comparison written for it would be the place
    /// the two drift apart.
    #[test]
    fn over_capacity_is_strictly_above_the_target_whichever_input_set_it() {
        for source in ["declared", "derived"] {
            let mut at = reading(10, Some(10));
            at.target_source = source;
            assert!(!at.over_capacity, "{source}: at the target is not over it");
            let mut above = reading(11, Some(10));
            above.target_source = source;
            assert!(above.over_capacity, "{source}: one row past is over");
        }
    }

    /// A deployment that declares neither a capacity nor a volume is not over
    /// one.
    #[test]
    fn no_declared_capacity_is_not_a_capacity_of_zero() {
        assert!(!reading(5, None).over_capacity);
    }

    // proves: checkpointStore.maxRows
    /// 目标不变的一拍仍然是一拍：读数上的求值计数必须往上走，来源标签必须
    /// 从 seed 变成 derived。没有这一条，「回路在算」就只剩启动时那一次。
    #[tokio::test]
    async fn a_beat_whose_target_does_not_move_still_counts() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://probe:probe@127.0.0.1:1/probe")
            .expect("lazy pool");
        let readings = Arc::new(RetentionReadings::new());
        let cap = CheckpointCap::new(pool, 40_000)
            .with_readings(Arc::clone(&readings))
            .with_database_volume(Some(8 * 1024 * 1024 * 1024));

        for expected in 1..=2 {
            cap.report(&CheckpointReading {
                held: 30_000,
                target: Some(120_000),
                target_source: SOURCE_DERIVED,
                over_capacity: false,
                bytes: Some(64 * 1024 * 1024),
            })
            .await;
            let (target, beats, derived) = readings.reading(KNOB).expect("a beat");
            assert_eq!(target, 120_000.0);
            assert_eq!(beats, expected, "a steady target is still a beat");
            assert_eq!(readings.evaluations(KNOB), expected);
            assert!(derived, "a volume-derived target says so on the reading");
        }
    }

    /// Nothing in this module deletes. The pointer invariant is stronger for
    /// it: a snapshot a board names cannot be lost by a loop that removes
    /// nothing at all.
    #[test]
    fn the_module_carries_no_delete_statement() {
        let source = include_str!("checkpoint_cap.rs");
        let forbidden = ["DE", "LETE FROM"].join("");
        assert!(
            !source.to_uppercase().contains(&forbidden),
            "a delete statement came back into the measuring loop"
        );
    }
}
