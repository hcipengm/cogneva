//! What the release builds of changes cost the host, split by which change
//! they were for and how they ended.
//!
//! The deployer builds one release binary per change. Its duration was carried
//! back to the caller as a field on the artifact and nowhere else, so the
//! questions a host sharing its machine with the cluster has to answer had no
//! reading at all: which changes are worth their build time, and how the builds
//! that did not produce a binary failed.
//!
//! Three series, one row per change kind, and they are three because they are
//! three different questions:
//!
//! - `cogneva_evolution_build_seconds_total{intent,outcome}` — host wall time
//!   spent building. The label says which ending the time belonged to, because
//!   a budget kill and a build that ran to an end are not the same purchase:
//!   the first bought a budget's worth of host and no answer, the second bought
//!   an answer.
//! - `cogneva_evolution_build_last_seconds{intent}` — how long the last build
//!   that ended on its own took. This is the measurement of the work, and it is
//!   the one that moves before changes start being lost to the budget.
//! - `cogneva_evolution_build_outcomes_total{intent,outcome}` — how the attempts
//!   ended. This is the denominator every reading above is divided by, and it is
//!   where the attempts that never reached cargo are counted.
//!
//! Nothing is summed across the change kinds. A total would answer "builds cost
//! the host time", which nobody has to decide anything about; the decision is
//! always which kind of change to spend the next build on, and the label is the
//! only thing that distinguishes them.
//!
//! The change kind is the change's own entry point, the same identity the
//! landing funnel publishes, so a build cost can be read against the fate of the
//! changes it was spent on. A change whose entry point is not carried to the
//! deployer reads as `unattributed` rather than being dropped, for the same
//! reason the funnel keeps that value: an unnamed producer is a finding about
//! the producer, not a gap in the reading.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::types::task::EvolutionIntent;
use cog_core::SFResult;

/// Host wall time the builds of each kind spent, in seconds, per outcome.
pub const BUILD_SECONDS_TOTAL_METRIC: &str = "cogneva_evolution_build_seconds_total";

/// How long the last build of each kind that ended on its own took, in seconds.
/// Absent for a kind whose builds have never ended on their own.
pub const BUILD_LAST_SECONDS_METRIC: &str = "cogneva_evolution_build_last_seconds";

/// How many builds of each kind have ended each way since this process started.
pub const BUILD_OUTCOMES_TOTAL_METRIC: &str = "cogneva_evolution_build_outcomes_total";

/// How much resident memory the last build of each kind that could be measured
/// peaked at, in bytes.
///
/// The observed counterpart of the sizing constant the build gate divides its
/// memory dimension by. That constant is a frozen measurement and this is the
/// same quantity taken again, on the same host, from the same tree of
/// processes, so the two can be subtracted: a constant that has stopped
/// describing the builds is then a difference a reader can see rather than a
/// bound nobody can explain.
///
/// Absent — not zero — for a kind with no measured build yet, and for builds
/// whose process tree could not be read at all. The two absences are the same
/// row here and are told apart by the attempt counter next to it: a build that
/// ran and went unmeasured is visible as an attempt with no peak.
pub const BUILD_LAST_PEAK_RSS_BYTES_METRIC: &str = "cogneva_evolution_build_last_peak_rss_bytes";

/// The change-kind label: the same spelling the landing funnel uses.
pub const INTENT_LABEL: &str = "intent";

/// The outcome label.
pub const OUTCOME_LABEL: &str = crate::LABEL_OUTCOME;

/// The outcome a build that never reached cargo carries.
///
/// Named so the emitter can leave it out of the seconds series without a second
/// list of outcomes to keep in step: the one outcome with no time to report is
/// the one whose name is written down here.
pub const OUTCOME_UNSTARTED: &str = "unstarted";

/// Every value the outcome label can take. A producer that publishes one series
/// per class needs the classes it has none of as well, or an empty class is
/// indistinguishable from a class that was never wired up.
pub const BUILD_OUTCOMES: [&str; 4] = ["built", "failed", "timed_out", OUTCOME_UNSTARTED];

/// How one build ended, with the time it cost the host.
///
/// The duration is inside the ending that has one, so "a build that never ran
/// reports a duration" is not a state this code can express. The same shape
/// keeps the two readings apart: a build the budget killed spent real host time
/// and is not a measurement of the work, and the caller cannot record it as
/// one.
///
/// 可序列化是为了越过进程边界：构建搬进一个一过性 Job 之后，只有执行侧知道
/// cargo 是被预算杀的还是自己失败的，而记账要在常驻进程那一侧、记进同一个读数族。
/// 判定它是什么**只有一处**（部署器），过边界的只是这个结论本身。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BuildEnding {
    /// cargo finished and the binary was staged.
    Built(Duration),
    /// cargo ran to an end without producing a binary.
    Failed(Duration),
    /// The deployment budget killed cargo. The duration is the wall it was
    /// killed at, i.e. the budget: that is what the host was occupied for, and
    /// reporting it as the length of the work would say the work finishes
    /// exactly at the limit.
    TimedOut(Duration),
    /// cargo was never invoked — the slot gate refused the build, or the
    /// binary could not be spawned. Both mean no build happened, and neither
    /// leaves a duration to report.
    Unstarted,
}

impl BuildEnding {
    /// The label value.
    pub fn outcome(self) -> &'static str {
        match self {
            BuildEnding::Built(_) => "built",
            BuildEnding::Failed(_) => "failed",
            BuildEnding::TimedOut(_) => "timed_out",
            BuildEnding::Unstarted => OUTCOME_UNSTARTED,
        }
    }

    /// Seconds this build occupied the host, for the endings that occupied it.
    fn host_seconds(self) -> Option<u64> {
        match self {
            BuildEnding::Built(d) | BuildEnding::Failed(d) | BuildEnding::TimedOut(d) => {
                Some(d.as_secs())
            }
            BuildEnding::Unstarted => None,
        }
    }

    /// Whether this build ended on its own, i.e. whether its duration measures
    /// the work rather than the wall it was cut off at.
    fn measures_the_work(self) -> bool {
        matches!(self, BuildEnding::Built(_) | BuildEnding::Failed(_))
    }

    /// The wall this ending carries, in seconds, for the rows that carry one.
    /// A build that never reached cargo has none, and its row says so by leaving
    /// the field out rather than by reporting zero.
    fn elapsed_seconds(self) -> Option<f64> {
        match self {
            BuildEnding::Built(d) | BuildEnding::Failed(d) | BuildEnding::TimedOut(d) => {
                Some(d.as_secs_f64())
            }
            BuildEnding::Unstarted => None,
        }
    }
}

/// One change kind's readings.
#[derive(Debug, Default)]
struct IntentRow {
    /// Host seconds per outcome, for the outcomes whose build reached cargo.
    seconds: BTreeMap<&'static str, u64>,
    /// The last build that ended on its own; absent until one does.
    last_secs: Option<u64>,
    /// The peak resident memory of the last build that could be measured.
    ///
    /// Kept apart from `last_secs` because they answer different questions and
    /// stop being reportable for different reasons: a build the budget killed
    /// has a duration that measures the budget rather than the work, so it does
    /// not become `last_secs` — but what it took in memory before it was killed
    /// is a real measurement, and on a build killed for memory it is the one
    /// reading worth having.
    last_peak_bytes: Option<u64>,
    /// Attempts per outcome.
    counts: BTreeMap<&'static str, u64>,
}

/// How many attempts the in-process window keeps.
///
/// The window answers "what has this process been building since it started", not
/// "what has ever been built"; the latter needs a store, and the reader here is the
/// face someone looks at while deciding where to spend the next build. A depth that
/// outlived a restart would have to be written down somewhere, and an unwritten
/// window that silently drops the oldest row is the shape that lies least.
// cap: bound
pub const BUILD_RUN_WINDOW: usize = 64;

/// The builds this process ran, by change kind, plus the last few as rows.
#[derive(Debug, Default)]
pub struct EvolutionBuildReadings {
    rows: Mutex<BTreeMap<&'static str, IntentRow>>,
    /// 最近 [`BUILD_RUN_WINDOW`] 次尝试，新在前。与计数在同一个锁序里写，所以
    /// 行数与计数不会互相漂移——两处分别记就会有一个先写一个后写。
    runs: Mutex<VecDeque<cog_core::BuildRunInfo>>,
}

impl EvolutionBuildReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one build attempt and how it ended.
    ///
    /// `None` is a build whose change kind the call site does not carry. It is
    /// recorded under the `unattributed` value the enum already has for exactly
    /// this, so the time it spent is still on the reading and the producer that
    /// failed to name its change is visible.
    ///
    /// `peak_rss_bytes` is what the build's process tree was measured at while
    /// it ran; `None` is a build nothing could sample. It travels with the
    /// ending rather than in a second call, so "which build was the last one"
    /// cannot differ between the duration and the memory row.
    pub fn record(
        &self,
        change_id: &str,
        intent: Option<EvolutionIntent>,
        ending: BuildEnding,
        peak_rss_bytes: Option<u64>,
    ) {
        let key = intent.unwrap_or(EvolutionIntent::Unattributed).as_str();
        // A reading must not be the thing that breaks a build: a poisoned lock
        // means some other thread panicked while holding it, and the counter
        // this build would have moved is not worth failing the build for.
        let mut rows = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let row = rows.entry(key).or_default();
        *row.counts.entry(ending.outcome()).or_default() += 1;
        if let Some(secs) = ending.host_seconds() {
            *row.seconds.entry(ending.outcome()).or_default() += secs;
        }
        if ending.measures_the_work() {
            row.last_secs = ending.host_seconds();
        }
        if peak_rss_bytes.is_some() {
            row.last_peak_bytes = peak_rss_bytes;
        }
        drop(rows);

        // 同一处写两遍：行与计数出自同一次调用，就不会出现「计数涨了、行没涨」
        // 这种两个读者各说各话的状态。
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        runs.push_front(cog_core::BuildRunInfo {
            change_id: change_id.to_string(),
            intent: key.to_string(),
            outcome: ending.outcome().to_string(),
            seconds: ending.elapsed_seconds(),
            at: chrono::Utc::now(),
        });
        while runs.len() > BUILD_RUN_WINDOW {
            runs.pop_back();
        }
    }

    /// 最近 `limit` 次尝试（新在前），不超过窗口深度。
    pub fn recent_runs(&self, limit: usize) -> Vec<cog_core::BuildRunInfo> {
        let runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        runs.iter()
            .take(limit.min(BUILD_RUN_WINDOW))
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl Observable for EvolutionBuildReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let rows = self.rows.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::with_capacity(EvolutionIntent::ALL.len() * (BUILD_OUTCOMES.len() + 2));
        for intent in EvolutionIntent::ALL {
            let key = intent.as_str();
            let row = rows.get(key);
            for outcome in BUILD_OUTCOMES {
                let count = row
                    .and_then(|r| r.counts.get(outcome).copied())
                    .unwrap_or(0);
                out.push(
                    RawMetric::new(BUILD_OUTCOMES_TOTAL_METRIC, count as f64)
                        .with_label(INTENT_LABEL, key)
                        .with_label(OUTCOME_LABEL, outcome),
                );
                // A build that never reached cargo has no seconds to report.
                // Its row exists in the attempt counter and nowhere else,
                // rather than as a zero a reader could divide by.
                if outcome == OUTCOME_UNSTARTED {
                    continue;
                }
                let secs = row
                    .and_then(|r| r.seconds.get(outcome).copied())
                    .unwrap_or(0);
                out.push(
                    RawMetric::new(BUILD_SECONDS_TOTAL_METRIC, secs as f64)
                        .with_label(INTENT_LABEL, key)
                        .with_label(OUTCOME_LABEL, outcome),
                );
            }
            if let Some(last) = row.and_then(|r| r.last_secs) {
                out.push(
                    RawMetric::new(BUILD_LAST_SECONDS_METRIC, last as f64)
                        .with_label(INTENT_LABEL, key),
                );
            }
            // Absent rather than zero for a kind nothing has measured yet, the
            // same way the duration row is: a zero peak would read as a build
            // that used no memory, and would subtract from the sizing constant
            // as "the constant is too big".
            if let Some(bytes) = row.and_then(|r| r.last_peak_bytes) {
                out.push(
                    RawMetric::new(BUILD_LAST_PEAK_RSS_BYTES_METRIC, bytes as f64)
                        .with_label(INTENT_LABEL, key),
                );
            }
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn reading(readings: &EvolutionBuildReadings, metric: &str, intent: &str) -> Option<f64> {
        readings
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.name == metric && m.labels.get(INTENT_LABEL).map(String::as_str) == Some(intent)
            })
            .map(|m| m.value)
    }

    async fn outcome_reading(
        readings: &EvolutionBuildReadings,
        metric: &str,
        intent: &str,
        outcome: &str,
    ) -> Option<f64> {
        readings
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.name == metric
                    && m.labels.get(INTENT_LABEL).map(String::as_str) == Some(intent)
                    && m.labels.get(OUTCOME_LABEL).map(String::as_str) == Some(outcome)
            })
            .map(|m| m.value)
    }

    /// Every change kind and every outcome is on the scrape from the start: a
    /// class with no builds yet is a zero, not a missing row, or a reader
    /// cannot tell an idle kind from one that was never wired up.
    #[tokio::test]
    async fn every_kind_and_every_outcome_is_published_before_anything_runs() {
        let readings = EvolutionBuildReadings::new();
        for intent in EvolutionIntent::ALL {
            for outcome in BUILD_OUTCOMES {
                assert_eq!(
                    outcome_reading(
                        &readings,
                        BUILD_OUTCOMES_TOTAL_METRIC,
                        intent.as_str(),
                        outcome
                    )
                    .await,
                    Some(0.0),
                    "{}/{outcome} is not published",
                    intent.as_str()
                );
            }
            for outcome in BUILD_OUTCOMES.iter().filter(|o| **o != OUTCOME_UNSTARTED) {
                assert_eq!(
                    outcome_reading(
                        &readings,
                        BUILD_SECONDS_TOTAL_METRIC,
                        intent.as_str(),
                        outcome
                    )
                    .await,
                    Some(0.0),
                    "{}/{} seconds is not published",
                    intent.as_str(),
                    outcome
                );
            }
            // The one thing that is absent rather than zero: no build of this
            // kind has ended on its own, and a zero would read as "instant".
            assert_eq!(
                reading(&readings, BUILD_LAST_SECONDS_METRIC, intent.as_str()).await,
                None
            );
        }
    }

    /// The change kind is the label that separates the readings: one kind's
    /// build moves its own rows and no others.
    #[tokio::test]
    async fn a_build_moves_only_its_own_change_kind() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-ci",
            Some(EvolutionIntent::CiFix),
            BuildEnding::Built(Duration::from_secs(90)),
            None,
        );
        assert_eq!(
            outcome_reading(&readings, BUILD_OUTCOMES_TOTAL_METRIC, "ci_fix", "built").await,
            Some(1.0)
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_SECONDS_METRIC, "ci_fix").await,
            Some(90.0)
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_SECONDS_METRIC, "self_audit").await,
            None
        );
        assert_eq!(
            outcome_reading(
                &readings,
                BUILD_OUTCOMES_TOTAL_METRIC,
                "self_audit",
                "built"
            )
            .await,
            Some(0.0)
        );
    }

    /// A build the budget killed is on the host-time reading — it really held
    /// the machine — but it does not become "how long a build takes": that
    /// number is the budget, and a reader checking headroom would read it as
    /// headroom of zero every time.
    #[tokio::test]
    async fn a_killed_build_is_host_time_and_not_a_measurement_of_the_work() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-audit",
            Some(EvolutionIntent::SelfAudit),
            BuildEnding::TimedOut(Duration::from_secs(3600)),
            None,
        );
        assert_eq!(
            outcome_reading(
                &readings,
                BUILD_OUTCOMES_TOTAL_METRIC,
                "self_audit",
                "timed_out"
            )
            .await,
            Some(1.0)
        );
        assert_eq!(
            outcome_reading(
                &readings,
                BUILD_SECONDS_TOTAL_METRIC,
                "self_audit",
                "timed_out"
            )
            .await,
            Some(3600.0)
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_SECONDS_METRIC, "self_audit").await,
            None
        );
    }

    /// A build that never reached cargo is counted and has no seconds at all —
    /// not zero seconds, which a reader dividing by the attempt count would
    /// take as a build that cost nothing.
    #[tokio::test]
    async fn a_build_that_never_ran_has_no_seconds() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-fix",
            Some(EvolutionIntent::IssueFix),
            BuildEnding::Unstarted,
            None,
        );
        assert_eq!(
            outcome_reading(
                &readings,
                BUILD_OUTCOMES_TOTAL_METRIC,
                "issue_fix",
                OUTCOME_UNSTARTED
            )
            .await,
            Some(1.0)
        );
        assert_eq!(
            outcome_reading(
                &readings,
                BUILD_SECONDS_TOTAL_METRIC,
                "issue_fix",
                OUTCOME_UNSTARTED
            )
            .await,
            None
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_SECONDS_METRIC, "issue_fix").await,
            None
        );
    }

    /// A change whose kind the call site does not carry is recorded as
    /// unattributed rather than dropped: the host time it spent is still on the
    /// reading, and the value names the producer that did not carry it.
    #[tokio::test]
    async fn a_build_with_no_change_kind_lands_under_unattributed() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-anon",
            None,
            BuildEnding::Failed(Duration::from_secs(30)),
            None,
        );
        assert_eq!(
            outcome_reading(
                &readings,
                BUILD_OUTCOMES_TOTAL_METRIC,
                "unattributed",
                "failed"
            )
            .await,
            Some(1.0)
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_SECONDS_METRIC, "unattributed").await,
            Some(30.0)
        );
    }

    /// The emitted outcome strings are the list the emitter iterates, so a
    /// labelling that drifted from that list would leave a class a reader can
    /// never see. Every ending has to name one of the published values.
    #[tokio::test]
    async fn every_ending_names_a_published_outcome() {
        let readings = EvolutionBuildReadings::new();
        let endings = [
            BuildEnding::Built(Duration::from_secs(1)),
            BuildEnding::Failed(Duration::from_secs(1)),
            BuildEnding::TimedOut(Duration::from_secs(1)),
            BuildEnding::Unstarted,
        ];
        for ending in endings {
            assert!(
                BUILD_OUTCOMES.contains(&ending.outcome()),
                "{} is not in BUILD_OUTCOMES",
                ending.outcome()
            );
            readings.record(
                "change-evolve",
                Some(EvolutionIntent::SelfEvolution),
                ending,
                None,
            );
        }
        let mut named: Vec<&str> = endings.iter().map(|e| e.outcome()).collect();
        named.sort_unstable();
        let mut published = BUILD_OUTCOMES.to_vec();
        published.sort_unstable();
        assert_eq!(
            named, published,
            "an outcome is not reachable by any ending"
        );
        for outcome in BUILD_OUTCOMES {
            assert!(
                outcome_reading(
                    &readings,
                    BUILD_OUTCOMES_TOTAL_METRIC,
                    "self_evolution",
                    outcome
                )
                .await
                .unwrap_or(0.0)
                    > 0.0,
                "{outcome} was recorded but is not on the scrape"
            );
        }
    }

    /// 一条变更重试三次是三次尝试，不是一条变更：按变更去重会把重试藏起来，
    /// 而重试次数正是决定下一次把构建花在哪里的那个数。
    #[tokio::test]
    async fn a_retried_change_gets_a_row_per_attempt() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-1",
            Some(EvolutionIntent::IssueFix),
            BuildEnding::Failed(Duration::from_secs(12)),
            None,
        );
        readings.record(
            "change-1",
            Some(EvolutionIntent::IssueFix),
            BuildEnding::Built(Duration::from_secs(40)),
            None,
        );
        let runs = readings.recent_runs(10);
        assert_eq!(runs.len(), 2, "two attempts are two rows");
        assert!(runs.iter().all(|r| r.change_id == "change-1"));
        // 新在前：读的人先看到最近那次是否成了。
        assert_eq!(runs[0].outcome, "built");
        assert_eq!(runs[1].outcome, "failed");
        assert_eq!(runs[0].seconds, Some(40.0));
    }

    /// 没跑起来的构建在行里没有耗时一格，而不是 0——0 读起来像「跑了一瞬间」。
    #[tokio::test]
    async fn a_build_that_never_ran_has_no_seconds_in_its_row() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-2",
            Some(EvolutionIntent::CiFix),
            BuildEnding::Unstarted,
            None,
        );
        let runs = readings.recent_runs(10);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, OUTCOME_UNSTARTED);
        assert_eq!(runs[0].seconds, None);
    }

    /// 没有携带入口的构建在行里读作 unattributed，与计数同一个值：两个读者看
    /// 同一件事，不该各说各话。
    #[tokio::test]
    async fn a_row_without_a_change_kind_says_unattributed() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-3",
            None,
            BuildEnding::Failed(Duration::from_secs(5)),
            None,
        );
        let runs = readings.recent_runs(10);
        assert_eq!(runs[0].intent, EvolutionIntent::Unattributed.as_str());
        assert_eq!(runs[0].seconds, Some(5.0));
    }

    /// 窗口是有界的：超过深度就丢最老的，且「要多少给多少」不会被窗口外的
    /// 请求骗出一个更长的列表。
    #[tokio::test]
    async fn the_window_keeps_the_newest_rows_and_drops_the_rest() {
        let readings = EvolutionBuildReadings::new();
        for i in 0..(BUILD_RUN_WINDOW + 3) {
            readings.record(
                &format!("change-{i}"),
                Some(EvolutionIntent::SelfEvolution),
                BuildEnding::Built(Duration::from_secs(1)),
                None,
            );
        }
        let runs = readings.recent_runs(usize::MAX);
        assert_eq!(runs.len(), BUILD_RUN_WINDOW);
        assert_eq!(
            runs[0].change_id,
            format!("change-{}", BUILD_RUN_WINDOW + 2)
        );
        assert!(
            runs.iter().all(|r| r.change_id != "change-0"),
            "the oldest attempt is the one the window drops"
        );
        assert_eq!(readings.recent_runs(3).len(), 3);
    }

    /// The peak is published for a kind that has one, and absent — not zero —
    /// for a kind that has not been measured. A zero would be read as "this
    /// kind's builds use no memory", which is the one wrong answer a reader
    /// comparing it against the sizing constant would believe.
    #[tokio::test]
    async fn a_measured_peak_is_published_and_an_unmeasured_one_is_absent() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-m",
            Some(EvolutionIntent::CiFix),
            BuildEnding::Built(Duration::from_secs(60)),
            Some(5 * 1024 * 1024 * 1024),
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_PEAK_RSS_BYTES_METRIC, "ci_fix").await,
            Some(5.0 * 1024.0 * 1024.0 * 1024.0)
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_PEAK_RSS_BYTES_METRIC, "self_audit").await,
            None,
            "a kind nothing has built is absent, not zero"
        );

        readings.record(
            "change-n",
            Some(EvolutionIntent::IssueFix),
            BuildEnding::Built(Duration::from_secs(10)),
            None,
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_PEAK_RSS_BYTES_METRIC, "issue_fix").await,
            None,
            "a build nothing could sample leaves the row absent"
        );
    }

    /// A build the budget killed does not become "how long a build takes", but
    /// the memory it had reached when it was killed is a real measurement — and
    /// on a build killed for its memory it is the reading worth having.
    #[tokio::test]
    async fn a_killed_build_still_reports_what_it_took() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-k",
            Some(EvolutionIntent::SelfAudit),
            BuildEnding::TimedOut(Duration::from_secs(3600)),
            Some(7 * 1024 * 1024 * 1024),
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_SECONDS_METRIC, "self_audit").await,
            None,
            "a killed build is not a measurement of the work"
        );
        assert_eq!(
            reading(&readings, BUILD_LAST_PEAK_RSS_BYTES_METRIC, "self_audit").await,
            Some(7.0 * 1024.0 * 1024.0 * 1024.0)
        );
    }

    /// A build that never reached cargo reports no peak, and must not erase the
    /// peak of the last build that did: the row is "the last measured build",
    /// and an attempt that never ran is not a measurement of zero.
    #[tokio::test]
    async fn an_unstarted_attempt_does_not_erase_the_last_peak() {
        let readings = EvolutionBuildReadings::new();
        readings.record(
            "change-p",
            Some(EvolutionIntent::SelfEvolution),
            BuildEnding::Built(Duration::from_secs(60)),
            Some(3 * 1024 * 1024 * 1024),
        );
        readings.record(
            "change-q",
            Some(EvolutionIntent::SelfEvolution),
            BuildEnding::Unstarted,
            None,
        );
        assert_eq!(
            reading(
                &readings,
                BUILD_LAST_PEAK_RSS_BYTES_METRIC,
                "self_evolution"
            )
            .await,
            Some(3.0 * 1024.0 * 1024.0 * 1024.0)
        );
    }
}
