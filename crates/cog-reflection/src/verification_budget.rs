//! What a self-evolution run cost, per kind, as an observation.
//!
//! This was once the two budgets that bounded a run — a pinned number per kind,
//! raised by a high-water mark taken from the runs that finished and from the
//! runs the budget killed. That loop is gone with the walls it served: a run is
//! no longer killed at a time, it is stopped when it stops moving
//! (`cog_core::progress`), and "how long may this take" is no longer a number
//! anybody writes down.
//!
//! What is still measured here is what the runs did: how long the last one of
//! each kind took, and how many were stopped by the judge. The first is the
//! reading that says the work got slower before anything has to be stopped; the
//! second is the count of attempts that were void and had to be redone.
//!
//! Nothing here is summed across the two kinds: the two jobs differ by an order
//! of magnitude, and one total would answer "something was slow" for a question
//! that is always "which one".

use std::sync::atomic::{AtomicU64, Ordering};

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// The two kinds of run this sink observes.
///
/// One label value each, and the label values are the whole domain — a reader
/// that sums them has thrown away the only thing that distinguishes the two
/// knobs. Published so a rule or a panel can be checked against the same list
/// this code emits instead of against a copy of it.
pub const BUDGET_KINDS: &[&str] = &["test", "build"];

/// The window a run of this kind is judged against, in seconds: how long it may
/// be silent before it is judged stopped. Zero until the deployment has
/// measured one, which is the honest reading — before that nothing is judged.
///
/// This is a measurement, not a bound: the number is the longest gap between
/// two steps of a run that was still working, so it grows with the heaviest
/// work this deployment has seen and never retires a run for taking long.
pub const WINDOW_SECONDS_METRIC: &str = "cogneva_verification_window_seconds";

/// How long the last run of each kind took, in seconds. Absent until one runs.
pub const LAST_RUN_SECONDS_METRIC: &str = "cogneva_verification_last_run_seconds";

/// Runs stopped because they stopped moving, per kind, since this process
/// started.
pub const STOPPED_TOTAL_METRIC: &str = "cogneva_verification_stopped_total";

/// The kind label, spelled once so the emitter and the probes that look for it
/// cannot drift.
pub const KIND_LABEL: &str = "kind";

/// The `test` kind, the name used at both the emitter and the recording sites.
pub const KIND_TEST: &str = "test";

/// The `build` kind.
pub const KIND_BUILD: &str = "build";

/// No run has been recorded yet.
///
/// Not zero: a run that took no time is not a thing cargo does, but a zero is
/// also what a real reading looks like, and an absent run reported as a zero
/// duration reads as "instant" — the opposite of "nothing has happened". The
/// value is `u64::MAX` because no run can have taken it.
const NO_RUN: u64 = u64::MAX;

/// What window each kind is judged against and what its runs have done.
///
/// One instance is shared by the verification side and the deployment side: they
/// are the two halves of the same process's budget, and a reader asking whether
/// the pipeline is being starved of time wants both in one scrape.
#[derive(Debug)]
pub struct VerificationBudget {
    test_stopped: AtomicU64,
    build_stopped: AtomicU64,
    last_test_secs: AtomicU64,
    last_build_secs: AtomicU64,
}

impl VerificationBudget {
    /// The observation sink. It takes no number: nothing here bounds a run.
    pub fn new() -> Self {
        Self {
            test_stopped: AtomicU64::new(0),
            build_stopped: AtomicU64::new(0),
            last_test_secs: AtomicU64::new(NO_RUN),
            last_build_secs: AtomicU64::new(NO_RUN),
        }
    }

    /// The window a run of this kind is judged against. A caller that needs
    /// the number reads [`crate::change_pipeline::progress_window_secs`]; this
    /// method stays so the published series keeps resolving a value instead of
    /// a missing series.
    pub fn timeout_secs(&self, _kind: &str) -> u64 {
        crate::change_pipeline::progress_window_secs()
    }

    /// Which side set the number [`Self::timeout_secs`] returns: always the
    /// measurement, because nothing is pinned any more.
    pub fn budget_source(&self, _kind: &str) -> &'static str {
        "measured"
    }

    /// Record a run that finished — passed or failed on its own — after
    /// `elapsed_secs`.
    pub fn record_run(&self, kind: &str, elapsed_secs: u64) {
        self.last_slot(kind).store(elapsed_secs, Ordering::Relaxed);
    }

    /// Record a run the judge stopped because it stopped moving.
    ///
    /// The elapsed reading is deliberately not written: the run was cut short,
    /// so its duration is how long it took to notice — not a measurement of the
    /// work — and publishing it as "how long the last run took" would report
    /// the watchdog interval as the cost of the job.
    pub fn record_stopped(&self, kind: &str) {
        self.stopped_counter(kind).fetch_add(1, Ordering::Relaxed);
    }

    fn last_slot(&self, kind: &str) -> &AtomicU64 {
        if kind == KIND_BUILD {
            &self.last_build_secs
        } else {
            &self.last_test_secs
        }
    }

    fn stopped_counter(&self, kind: &str) -> &AtomicU64 {
        if kind == KIND_BUILD {
            &self.build_stopped
        } else {
            &self.test_stopped
        }
    }
}

impl Default for VerificationBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl VerificationBudget {
    /// What the last run of this kind took, for a caller that has to carry the
    /// reading somewhere else. Without it the reading exists only as a metric
    /// emitted by this process, so a run that happened in another process --
    /// which is where execution goes when it moves off the long-running one --
    /// would be a run this family of readings never sees.
    ///
    /// `None` is not zero: a zero-second run is a real reading, and reporting an
    /// absent one as zero says "instant" instead of "nothing has happened".
    pub fn last_run_secs(&self, kind: &str) -> Option<u64> {
        match self.last_slot(kind).load(Ordering::Relaxed) {
            NO_RUN => None,
            secs => Some(secs),
        }
    }

    /// How many runs of this kind the budget has killed. A counter rather than a
    /// flag because the reader on the other side of a process boundary adds what
    /// it is told to its own, and "how many" is what can be added.
    pub fn stopped(&self, kind: &str) -> u64 {
        self.stopped_counter(kind).load(Ordering::Relaxed)
    }

    /// Take on readings produced elsewhere. Same quantities, same kinds, so the
    /// reader that was already asking this process about one of them keeps
    /// getting the same answer when the work moves.
    ///
    /// The elapsed time is written only when the other side had one: an absent
    /// reading must not overwrite a real one from an earlier run in this
    /// process, and a stopped run deliberately carries no elapsed time (its
    /// duration is how long it took to notice, not a measurement of the work).
    pub fn adopt_run(&self, kind: &str, elapsed_secs: Option<u64>, stopped: u64) {
        if let Some(secs) = elapsed_secs {
            self.record_run(kind, secs);
        }
        for _ in 0..stopped {
            self.record_stopped(kind);
        }
    }
}

#[async_trait::async_trait]
impl Observable for VerificationBudget {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::with_capacity(BUDGET_KINDS.len() * 3);
        for kind in BUDGET_KINDS {
            out.push(
                RawMetric::new(WINDOW_SECONDS_METRIC, self.timeout_secs(kind) as f64)
                    .with_label(KIND_LABEL, *kind)
                    .with_label(crate::LABEL_SOURCE, self.budget_source(kind)),
            );
            out.push(
                RawMetric::new(
                    STOPPED_TOTAL_METRIC,
                    self.stopped_counter(kind).load(Ordering::Relaxed) as f64,
                )
                .with_label(KIND_LABEL, *kind),
            );
            let elapsed = self.last_slot(kind).load(Ordering::Relaxed);
            if elapsed != NO_RUN {
                out.push(
                    RawMetric::new(LAST_RUN_SECONDS_METRIC, elapsed as f64)
                        .with_label(KIND_LABEL, *kind),
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

    async fn reading(budget: &VerificationBudget, metric: &str, kind: &str) -> Option<f64> {
        budget
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.name == metric && m.labels.get(KIND_LABEL).map(String::as_str) == Some(kind)
            })
            .map(|m| m.value)
    }

    /// Both kinds reach the scrape under their own kind, and both report the
    /// bound that now applies: the measured window. Neither is summed into the
    /// other, and neither is a pinned number any more.
    #[tokio::test]
    async fn each_kind_is_reported_under_its_own_kind() {
        let budget = VerificationBudget::new();
        let window = crate::change_pipeline::progress_window_secs() as f64;
        assert_eq!(
            reading(&budget, WINDOW_SECONDS_METRIC, KIND_TEST).await,
            Some(window)
        );
        assert_eq!(
            reading(&budget, WINDOW_SECONDS_METRIC, KIND_BUILD).await,
            Some(window)
        );
        assert_eq!(budget.budget_source(KIND_TEST), "measured");
    }

    /// A kind that has never run has no elapsed reading. Reporting zero would
    /// say the run was instantaneous, which is the opposite of "no run yet".
    #[tokio::test]
    async fn an_unrun_kind_has_no_elapsed_reading() {
        let budget = VerificationBudget::new();
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_TEST).await,
            None
        );
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_BUILD).await,
            None
        );
    }

    /// A stop moves the counter of its own kind and no other, and leaves the
    /// elapsed reading alone — a run cut short has no duration of its own to
    /// report, only the time it took to notice.
    #[tokio::test]
    async fn a_stop_moves_only_its_own_counter() {
        let budget = VerificationBudget::new();
        budget.record_stopped(KIND_TEST);
        assert_eq!(
            reading(&budget, STOPPED_TOTAL_METRIC, KIND_TEST).await,
            Some(1.0)
        );
        assert_eq!(
            reading(&budget, STOPPED_TOTAL_METRIC, KIND_BUILD).await,
            Some(0.0)
        );
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_TEST).await,
            None
        );
    }

    /// A run that finished on its own moves the elapsed reading and leaves the
    /// counter alone. Otherwise "the last run took this long" and "the last run
    /// was stopped" would be the same reading, and only one of them means
    /// changes are being lost.
    #[tokio::test]
    async fn a_completed_run_does_not_move_the_counter() {
        let budget = VerificationBudget::new();
        budget.record_run(KIND_TEST, 900);
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_TEST).await,
            Some(900.0)
        );
        assert_eq!(
            reading(&budget, STOPPED_TOTAL_METRIC, KIND_TEST).await,
            Some(0.0)
        );
        // The elapsed reading is what the last run did, not the slowest ever:
        // a fast run after a slow one is the reading, not a step backwards.
        budget.record_run(KIND_TEST, 100);
        assert_eq!(budget.last_run_secs(KIND_TEST), Some(100));
    }

    /// Readings taken in another process land here unchanged, stops included:
    /// the work moves off the long-running process, and the counters a reader
    /// adds have to keep adding up.
    #[tokio::test]
    async fn readings_from_another_process_are_adopted() {
        let budget = VerificationBudget::new();
        budget.adopt_run(KIND_BUILD, Some(700), 2);
        assert_eq!(budget.last_run_secs(KIND_BUILD), Some(700));
        assert_eq!(budget.stopped(KIND_BUILD), 2);
        assert_eq!(budget.stopped(KIND_TEST), 0);
    }
}
