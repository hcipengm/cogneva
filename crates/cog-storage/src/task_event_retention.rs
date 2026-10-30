//! Retires the per-task event archive a whole stream at a time.
//!
//! `cog_events` is where a task's agent loop files each iteration so a run that
//! did not converge can be read back afterwards. Nothing has ever removed a row
//! from it, so its size is the platform's uptime times the iteration rate.
//! Measured on 2026-10-27: 130,871 rows over 211 MB spanning 2026-09-23 to that
//! day, about 3,800 rows a day. Every one of those rows is in the nightly
//! backup, and the backup drill restores the whole dump to prove it — so the
//! archive is paid for every night by a restore that has to finish inside a
//! budget.
//!
//! Two things a sweep may never buy here, and both come from how the table is
//! read rather than from how it is written:
//!
//! - **A stream is not a prefix.** Both readers ask for a task's events from
//!   offset zero — the handover readback to find the last artifact a change
//!   produced, and the ralph loop to replay a run's iterations. A sweep that
//!   deleted the oldest rows of a stream would answer those reads with a gap
//!   that reads exactly like "the run started there", and nothing in the reply
//!   would say otherwise. So the unit of retirement is the whole stream: a task
//!   either still has all of its events or it has none of them, and the count
//!   of streams is a published reading so "none" is visible rather than
//!   inferred.
//! - **A stream a checkpoint names is the resume point.** The offset in
//!   `cog_checkpoints` is a position in this sequence. Deleting the sequence
//!   under a live checkpoint leaves the reader resuming at a position that no
//!   longer exists, and the sequence restarts at one for the next append — so
//!   the reader would skip every event after the reset without an error. The
//!   checkpoint is the only authority on which streams are resumable, and a
//!   stream it names is not deletable however old it is.
//!
//! What is left is an age policy: a stream whose newest event is older than the
//! retention is retired in full. That is deliberately *not* a row capacity. A
//! row capacity would have to cut inside the streams of recent tasks to bind,
//! which is the first rule above; and the readers' promise is about a run that
//! concluded, not about how many rows the platform filed last month. A task
//! that has been appending for longer than the retention keeps everything it
//! has until it goes quiet — the cost of the promise, and the age reading below
//! is what makes that cost visible instead of hidden inside the count.
//!
//! A lease decides which replica retires. The set being deleted is defined by
//! the clock, so the *result* does not depend on who runs it — but two replicas
//! running it at the same moment take locks on the same rows, and two statements
//! whose row sets overlap are free to reach them in different orders, which is a
//! deadlock the database resolves by aborting one and reporting the pass as
//! failed. The lease costs a renewal and removes that: the holder retires, and
//! every other replica measures and publishes. A deployment that declares a
//! shorter retention does shorten it for the whole database, which is why the
//! retention is published per deployment: the shortest declaration in force is
//! the one the readings show.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, Row};
use tracing::{info, warn};

use cog_core::metric_names;
use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The table this loop retires streams from.
pub const TABLE: &str = "cog_events";

/// The table whose rows name the streams a task could resume from.
pub const CHECKPOINTS_TABLE: &str = "cog_checkpoints";

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "storage_task_event_retention";

/// The role this loop takes a lease on. Only the holder retires, so replicas
/// sharing one database do not take the same row locks at the same moment.
pub const ROLE: &str = "task_event_retention";

/// The label that names the deployment a reading below came from: the table is
/// shared, and a reading without a name is served by whichever process the
/// scrape happens to reach.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// How long a stream is kept by default.
///
/// Thirty days. The archive exists so a run that did not converge can be read
/// back after the fact, and the question is asked days later, not months; a
/// stream is retired only once it has been quiet for that long, so a live run
/// is never cut. It is a default for the deployment that does not say
/// otherwise, not a policy.
// cap: bound — 一条流要安静这么久才被退役，正在跑的绝不会被切；这是不说自己保留期的部署的默认值，不是策略
pub const DEFAULT_KEEP_DAYS: i64 = 30;

/// The shortest retention this loop will accept.
///
/// A day. The readers replay a whole stream, so a retention shorter than the
/// time a run takes would retire streams that are still being read. A value
/// below this is a misconfiguration and falls back rather than being honoured.
// cap: bound — 读者要重放整条流，保留期短于一次运行就该跑的时长就会退掉还在被读的流；低于它的值按配置错误回退
pub const MIN_KEEP_DAYS: i64 = 1;

/// The environment variable that carries the retention.
pub const KEEP_DAYS_ENV: &str = "COGNEVA_TASK_EVENT_KEEP_DAYS";

/// How often the loop looks for streams past the retention.
///
/// Two minutes, matched to the two capacity sweeps beside it: it republishes the
/// table's size, and Prometheus drops a series it has not seen for five minutes
/// rather than holding the last value. The age it measures moves by days, so
/// nothing here needs to be faster.
// cap: bound — 清扫顺带重发表大小，五分钟不发 Prometheus 就丢序列；它量的「年龄」以天计，这里没东西需要更快
pub const PERIOD: Duration = Duration::from_secs(120);

/// How many streams one statement may retire.
///
/// The statement's work is bounded by the streams it names, not by the rows
/// they hold, so the bound is on candidate selection too: the pass re-selects
/// its candidates each batch, and a batch is what the loop publishes as its
/// bound between two beats. Five hundred streams is well under the smallest
/// batch that matters at the rate this table grows.
// cap: bound — 这一批就是两拍之间发布的界；五百条流远小于这张表增长速度下「算一批」的最小量
const BATCH_STREAMS: i64 = 500;

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEventRetentionOutcome {
    /// Rows the archive held after the pass.
    pub held: i64,
    /// Distinct streams the archive held after the pass.
    pub tasks: i64,
    /// Rows this pass retired.
    pub removed: u64,
    /// The age of the oldest surviving row, in seconds; zero for an empty
    /// archive.
    pub oldest_age_secs: i64,
    /// The retention this pass enforced, in seconds; `None` when this
    /// deployment measures the archive without retiring anything.
    pub keep_secs: Option<i64>,
}

/// The retention sweep: the archive, the pool it lives on, and the age this
/// deployment promises its readers.
#[derive(Clone)]
pub struct TaskEventRetention {
    pool: PgPool,
    keep: Option<Duration>,
    table: String,
    checkpoints: String,
    metrics: Option<Arc<dyn MetricsBackend>>,
    role: Option<Arc<dyn cog_core::OwnerLeaseBroker>>,
    deployment: String,
}

impl TaskEventRetention {
    /// `keep` is the retention; `None` turns retirement off, which is how a
    /// deployment says that another process sharing this database is the one
    /// that retires. It still measures — a deployment that does not prune is
    /// exactly the one whose operator needs to see the size it is not bounding.
    pub fn new(pool: PgPool, keep: Option<Duration>) -> Self {
        Self {
            pool,
            keep,
            table: TABLE.to_string(),
            checkpoints: CHECKPOINTS_TABLE.to_string(),
            metrics: None,
            role: None,
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

    /// Lease the retiring half of this loop.
    pub fn with_role(mut self, broker: Arc<dyn cog_core::OwnerLeaseBroker>) -> Self {
        self.role = Some(broker);
        self
    }

    /// The retention this sweep enforces.
    pub fn keep(&self) -> Option<Duration> {
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
        // Only a deployment that means to retire contends for the lease: one
        // that measures would otherwise hold the role and keep the deployment
        // that does out of it for as long as it lives.
        let role = if self.keep.is_some() {
            Some(cog_core::RoleHold::start(
                self.role.clone(),
                ROLE,
                beat.clone(),
                &shutdown,
            ))
        } else {
            None
        };
        let mut interval = tokio::time::interval(PERIOD);
        // A pass that overran its period resumes from now rather than firing a
        // backlog of catch-ups: what it missed is found by the cutoff, not by
        // the timer.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            beat.beat();
            let retires = match role.as_ref() {
                Some(hold) => hold.may_act().await,
                None => false,
            };
            let pass = if retires {
                self.retire_once(|| beat.beat()).await
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
                            "Retired task event streams past the retention"
                        );
                    }
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

    /// Retire every stream that has been quiet past the retention, calling
    /// `progress` before each batch.
    ///
    /// The loop hands `beat.beat` in here: the work between two beats would
    /// otherwise be the whole pass, and a first run against an archive that grew
    /// while nothing retired it deletes for as long as the backlog lasts. That
    /// is the shape that made a working sweep read as a stopped loop on the
    /// event log, so the bound published here is one batch.
    pub async fn retire_once(&self, progress: impl Fn()) -> SFResult<TaskEventRetentionOutcome> {
        let mut removed = 0u64;
        if let Some(keep) = self.keep {
            let cutoff = chrono::Utc::now()
                - chrono::Duration::from_std(keep)
                    .unwrap_or_else(|_| chrono::Duration::days(DEFAULT_KEEP_DAYS));
            loop {
                progress();
                let batch = sqlx::query(&format!(
                    "WITH quiet AS (
                         SELECT DISTINCT task_id FROM {table} WHERE created_at < $2
                         EXCEPT
                         SELECT DISTINCT task_id FROM {table} WHERE created_at >= $2
                     ), pin AS (
                         SELECT task_id FROM {checkpoints}
                          WHERE task_id IN (SELECT task_id FROM quiet)
                     ), batch AS (
                         SELECT task_id FROM quiet
                         EXCEPT
                         SELECT task_id FROM pin
                         LIMIT $1
                     )
                     DELETE FROM {table} WHERE task_id IN (SELECT task_id FROM batch)",
                    table = cog_core::sql::quote_relation(&self.table),
                    checkpoints = cog_core::sql::quote_relation(&self.checkpoints),
                ))
                .bind(BATCH_STREAMS)
                .bind(cutoff)
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

        let held = self.row_count().await?;
        let tasks = self.stream_count().await?;
        let oldest_age_secs = self.oldest_age_secs().await?;
        Ok(TaskEventRetentionOutcome {
            held,
            tasks,
            removed,
            oldest_age_secs,
            keep_secs: self.keep.map(|k| k.as_secs() as i64),
        })
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
            removed: 0,
            oldest_age_secs: self.oldest_age_secs().await?,
            keep_secs: self.keep.map(|k| k.as_secs() as i64),
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
        let mut pairs = vec![
            (metric_names::TASK_EVENTS_ROWS, outcome.held as f64),
            (metric_names::TASK_EVENTS_TASKS, outcome.tasks as f64),
            (
                metric_names::TASK_EVENTS_OLDEST_AGE_SECONDS,
                outcome.oldest_age_secs as f64,
            ),
        ];
        if let Some(keep_secs) = outcome.keep_secs {
            pairs.push((metric_names::TASK_EVENTS_KEEP_SECONDS, keep_secs as f64));
        }
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

/// The retention this process was told.
///
/// `Some(0)` is a deployment saying it does not retire — it measures and leaves
/// the deletion to a process whose declaration it agrees with. A value that
/// cannot be read, or that is negative, falls back to [`DEFAULT_KEEP_DAYS`]: a
/// retention of zero days is how the archive would end up empty by
/// construction, and an unreadable knob is no reason to forget everything.
pub fn keep_days_from_env() -> Option<i64> {
    keep_days_from(std::env::var(KEEP_DAYS_ENV).ok().as_deref())
}

/// See [`keep_days_from_env`].
///
/// Zero means **stop sweeping**, not "sweep everything": the readers replay a
/// task's events from offset zero, so a retired prefix is answered as a gap that
/// looks exactly like the start of the run. That is a state worth being able to
/// ask for on purpose, so it is the [`keep_days_or_disable`] reading rather than
/// the fall-back one.
///
/// [`keep_days_or_disable`]: cog_core::retention::keep_days_or_disable
pub fn keep_days_from(raw: Option<&str>) -> Option<i64> {
    cog_core::retention::keep_days_or_disable(raw, DEFAULT_KEEP_DAYS, MIN_KEEP_DAYS)
}

/// The deployment this process belongs to, as the platform names it.
///
/// The same production the capacity loops beside this one use, and read for the
/// same reason: it is the name the manifest already gives the workload, and a
/// pod name would be an identity that changes on every rollout.
pub fn deployment_name() -> String {
    std::env::var(cog_core::env::DEPLOYMENT_NAME_ENV).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retention_that_cannot_be_read_falls_back_and_zero_turns_it_off() {
        assert_eq!(keep_days_from(None), Some(DEFAULT_KEEP_DAYS));
        assert_eq!(keep_days_from(Some("nope")), Some(DEFAULT_KEEP_DAYS));
        assert_eq!(keep_days_from(Some("-3")), Some(DEFAULT_KEEP_DAYS));
        assert_eq!(
            keep_days_from(Some("0")),
            None,
            "zero says this deployment retires nothing, rather than erasing the archive"
        );
        assert_eq!(
            keep_days_from(Some("7")),
            Some(7),
            "a deployment that says seven days means seven"
        );
    }

    #[tokio::test]
    async fn a_sweep_without_a_retention_names_the_archive_and_no_policy() {
        let sweep = TaskEventRetention::new(
            PgPool::connect_lazy("postgres://example").expect("a lazy pool"),
            None,
        );
        assert_eq!(sweep.keep(), None);
        assert_eq!(sweep.table, TABLE);
        assert_eq!(sweep.checkpoints, CHECKPOINTS_TABLE);
    }
}
