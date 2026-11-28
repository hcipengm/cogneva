//! Measures the per-task event archive, and retires nothing.
//!
//! `cog_events` is where a task's agent loop files each iteration so a run that
//! did not converge can be read back afterwards. It used to be retired whole
//! streams at a time once their newest event aged past a declared number of
//! days. That is gone: the archive is the only record of what a run saw, a
//! stream is read from offset zero by both readers (the handover readback and
//! the ralph loop), and a retired stream answers those reads with a gap that
//! looks exactly like "the run started there". Nothing in the reply would say
//! otherwise.
//!
//! What is left is the measurement, which is what an operator actually needs:
//! how many rows, how many streams, and how far back the oldest row reaches.
//! A table that grows is a reading, and the answer to growth is a decision made
//! against that reading — archive it somewhere addressable, or accept the cost —
//! not a clock that deletes the evidence.
//!
//! The loop keeps its name and its lease-free shape: with nothing to delete
//! there is nothing two replicas could deadlock over, and every replica may
//! publish.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, Row};
use tracing::warn;

use cog_core::metric_names;
use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The table this loop measures.
pub const TABLE: &str = "cog_events";

/// The table whose rows name the streams a task could resume from.
pub const CHECKPOINTS_TABLE: &str = "cog_checkpoints";

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "storage_task_event_retention";

/// The label that names the deployment a reading below came from: the table is
/// shared, and a reading without a name is served by whichever process the
/// scrape happens to reach.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// How often the loop measures the archive.
///
/// Two minutes, matched to the capacity sweeps beside it: it republishes the
/// table's size, and Prometheus drops a series it has not seen for five minutes
/// rather than holding the last value. The age it measures moves by days, so
/// nothing here needs to be faster.
// cap: fixed — 顺带重发表大小，五分钟不发 Prometheus 就丢序列；它量的「年龄」以天计
// keep: 删掉后五分钟不发读数，Prometheus 丢掉表大小序列；由「5 分钟丢序列」兜住
pub const PERIOD: Duration = Duration::from_secs(120);

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEventRetentionOutcome {
    /// Rows the archive held after the pass.
    pub held: i64,
    /// Distinct streams the archive held after the pass.
    pub tasks: i64,
    /// The age of the oldest row, in seconds; zero for an empty archive.
    pub oldest_age_secs: i64,
}

/// The retention sweep: the archive, the pool it lives on, and the age this
/// deployment promises its readers.
#[derive(Clone)]
pub struct TaskEventRetention {
    pool: PgPool,
    table: String,
    checkpoints: String,
    metrics: Option<Arc<dyn MetricsBackend>>,
    deployment: String,
}

impl TaskEventRetention {
    /// The measurement loop. It has nothing to configure: what it reports is
    /// the archive as it is.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            table: TABLE.to_string(),
            checkpoints: CHECKPOINTS_TABLE.to_string(),
            metrics: None,
            deployment: String::new(),
        }
    }

    /// Point the sweep at other tables. Only a test does this, and it is what
    /// lets the suite run against its own schema instead of the live archive.
    pub fn with_tables(
        mut self,
        events: impl Into<String>,
        checkpoints: impl Into<String>,
    ) -> Self {
        self.table = events.into();
        self.checkpoints = checkpoints.into();
        self
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
        // A pass that overran its period resumes from now rather than firing a
        // backlog of catch-ups: what it missed is found by the cutoff, not by
        // the timer.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            beat.beat();
            let pass = self.measure_once().await;
            match pass {
                Ok(outcome) => {
                    self.report(&outcome).await;
                    self.report_pass_failed(false).await;
                }
                Err(e) => {
                    // A failed pass leaves every value reading at its last
                    // value, and the rules keep trusting them — so an archive
                    // that cannot be swept and one that is not growing look the
                    // same. This cell says the pass itself did not answer.
                    self.report_pass_failed(true).await;
                    warn!(table = self.table, error = %e, "Task event retention pass failed");
                }
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Measure the archive and retire nothing.
    ///
    /// The replica that does not hold the lease publishes from here, and so does
    /// a deployment whose retention is zero. Both are answers a reader needs:
    /// a deployment that is not bounding the table is exactly the one whose
    /// operator should see how large it is.
    pub async fn measure_once(&self) -> SFResult<TaskEventRetentionOutcome> {
        Ok(TaskEventRetentionOutcome {
            held: self.row_count().await?,
            tasks: self.stream_count().await?,
            oldest_age_secs: self.oldest_age_secs().await?,
        })
    }

    async fn row_count(&self) -> SFResult<i64> {
        crate::relation_rows::relation_rows(&self.pool, &self.table).await
    }

    /// How many task streams the archive holds.
    ///
    /// The row count alone cannot tell a table holding one long run from one
    /// holding a thousand short ones, and retirement removes whole streams — so
    /// a pass that deleted many rows and few streams and one that deleted the
    /// same rows in one stream read differently here.
    async fn stream_count(&self) -> SFResult<i64> {
        sqlx::query(&format!(
            "SELECT count(DISTINCT task_id) AS n FROM {}",
            cog_core::sql::quote_relation(&self.table)
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))
        .map(|row| row.get::<i64, _>("n"))
    }

    /// The age of the oldest row in the archive, in seconds.
    ///
    /// Zero for an empty archive, and that is the reading rather than an
    /// absence: a table nobody has written to is not a retention that has
    /// fallen behind, and a series that disappeared would make the rule
    /// comparing it to the retention go silent exactly while the archive is
    /// clean.
    async fn oldest_age_secs(&self) -> SFResult<i64> {
        sqlx::query(&cog_core::sql::relation_oldest_age_sql(
            &self.table,
            "created_at",
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))
        .map(|row| row.get::<Option<i64>, _>("age").unwrap_or(0))
    }

    /// Publish what the archive holds and how far back it reaches.
    ///
    /// The retention is published only by a deployment that retires: a reading
    /// of zero would make every comparison against it a verdict about a policy
    /// this deployment does not have, and the rule that compares the two
    /// readings is written to stay silent when one of them is missing.
    async fn report(&self, outcome: &TaskEventRetentionOutcome) {
        let Some(ref mb) = self.metrics else { return };
        let labels = self.labels();
        let pairs = vec![
            (metric_names::TASK_EVENTS_ROWS, outcome.held as f64),
            (metric_names::TASK_EVENTS_TASKS, outcome.tasks as f64),
            (
                metric_names::TASK_EVENTS_OLDEST_AGE_SECONDS,
                outcome.oldest_age_secs as f64,
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
            metric_names::TASK_EVENTS_SWEEP_FAILED,
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
            warn!(error = %e, metric = %name, "Task event retention gauge emit failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 量的是这张表：名字与检查点表都在，且没有「保留期」这一格可读。
    #[tokio::test]
    async fn a_sweep_names_the_archive_it_measures() {
        let sweep = TaskEventRetention::new(
            PgPool::connect_lazy("postgres://example").expect("a lazy pool"),
        );
        assert_eq!(sweep.table, TABLE);
        assert_eq!(sweep.checkpoints, CHECKPOINTS_TABLE);
    }

    /// 这套代码里没有一条删除语句。
    ///
    /// 判的是**形状**而不是行为：一个再长的时间跨度都不会让它删任何东西，
    /// 因为它根本没有那条语句。行为那一半在真库用例里（`tests/task_event_retention.rs`）。
    #[test]
    fn the_module_carries_no_delete_statement() {
        // 整份文件都算：这条断言自己也不写那句删除语句，所以不需要切产出面。
        let source = include_str!("task_event_retention.rs");
        // 判的词在这里拼出来，不写字面量：写出来的话，这条断言会先命中它自己。
        let forbidden = ["DELETE", "FROM"].join(" ");
        assert!(
            !source.to_uppercase().contains(&forbidden),
            "档案退役的删除语句还在：这条界要拆的是「到点删」，不是换个阈值"
        );
    }
}
