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

use std::collections::BTreeMap;
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

/// The change-kind label: the same spelling the landing funnel uses.
pub const INTENT_LABEL: &str = "intent";

/// The outcome label.
pub const OUTCOME_LABEL: &str = "outcome";

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

/// One change kind's readings.
#[derive(Debug, Default)]
struct IntentRow {
    /// Host seconds per outcome, for the outcomes whose build reached cargo.
    seconds: BTreeMap<&'static str, u64>,
    /// The last build that ended on its own; absent until one does.
    last_secs: Option<u64>,
    /// Attempts per outcome.
    counts: BTreeMap<&'static str, u64>,
}

/// The builds this process ran, by change kind.
#[derive(Debug, Default)]
pub struct EvolutionBuildReadings {
    rows: Mutex<BTreeMap<&'static str, IntentRow>>,
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
    pub fn record(&self, intent: Option<EvolutionIntent>, ending: BuildEnding) {
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
            Some(EvolutionIntent::CiFix),
            BuildEnding::Built(Duration::from_secs(90)),
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
            Some(EvolutionIntent::SelfAudit),
            BuildEnding::TimedOut(Duration::from_secs(3600)),
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
        readings.record(Some(EvolutionIntent::IssueFix), BuildEnding::Unstarted);
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
        readings.record(None, BuildEnding::Failed(Duration::from_secs(30)));
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
            readings.record(Some(EvolutionIntent::SelfEvolution), ending);
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
}
