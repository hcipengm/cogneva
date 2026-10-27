//! The two budgets that bound a self-evolution run, what they did, and what the
//! runs themselves say the budget has to be.
//!
//! `test_timeout_secs` and `build_timeout_secs` are the knobs the deployment
//! pins. They are read here as a **floor**, not as the answer: a budget that is
//! only ever the pinned number is a number no reading can move, so the same
//! wall clock that retires one change at 3600 retires the next one, and the only
//! thing that changes is how many were lost. The two facts this module measures
//! are what close that loop:
//!
//! - how long runs that finished on their own took, and
//! - whether any run was killed by the budget.
//!
//! The highest duration ever observed for a kind is a lower bound on what the
//! work needs; the budget is that bound with headroom over it, clamped between
//! the pinned value (a budget never shrinks below what the deployment asked
//! for, so this loop can only ever cause fewer timeouts) and a hard multiple of
//! it (a run cannot overshoot the budget it was killed at by an unbounded
//! amount, so one pathological measurement must not buy an arbitrarily loose
//! budget). A kill is evidence too: the run exceeded the budget it was held to,
//! so that budget becomes the observed lower bound and the next run gets the
//! headroom.
//!
//! Which side set the number is published with it. A pinned 3600 and a derived
//! 3600 are the same digit and completely different facts, and the reader who
//! has to decide whether to raise the pin needs to know which one it is looking
//! at.
//!
//! The elapsed reading is not decoration on the counter. A budget that is about
//! to bind is visible in the elapsed time of the runs that still pass, and the
//! counter only starts moving after changes have already been lost to it.
//!
//! Nothing here is summed across the two kinds. The budgets differ by the
//! factor the two jobs differ by, and one total would answer "something was
//! slow" for a question that is always "which one".

use std::sync::atomic::{AtomicU64, Ordering};

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// The two kinds of run a budget bounds.
///
/// One label value each, and the label values are the whole domain — a reader
/// that sums them has thrown away the only thing that distinguishes the two
/// knobs. Published so a rule or a panel can be checked against the same list
/// this code emits instead of against a copy of it.
pub const BUDGET_KINDS: &[&str] = &["test", "build"];

/// The effective budget, per kind, in seconds.
pub const BUDGET_SECONDS_METRIC: &str = "cogneva_verification_budget_seconds";

/// How long the last run of each kind took, in seconds. Absent until one runs.
pub const LAST_RUN_SECONDS_METRIC: &str = "cogneva_verification_last_run_seconds";

/// Runs killed by their budget, per kind, since this process started.
pub const TIMEOUTS_TOTAL_METRIC: &str = "cogneva_verification_timeouts_total";

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

/// Which side set the budget a run was held to, published under the crate's own
/// `source` label so a pinned number and a measured one never read the same.
///
/// The budget is the number the deployment pinned; no run has needed more.
pub const SOURCE_CONFIGURED: &str = "configured";

/// A run needed more than the pin and the budget is that measurement with
/// headroom over it.
pub const SOURCE_DERIVED: &str = "derived";

/// The measurement asked for more than the hard multiple of the pin, and the
/// budget stopped there. A run killed at a ceiling is the case where the loop
/// has run out of room and a human has to look.
pub const SOURCE_CEILING: &str = "ceiling";

/// Headroom over the slowest run of this kind that finished on its own, as the
/// fraction `NUM / DEN` of its duration. Three halves leaves a third of the
/// budget empty at the observed maximum: enough that a run merely a little
/// slower than the slowest one so far still finishes, small enough that a
/// budget which keeps getting killed still escapes in a few steps rather than
/// in one jump that makes the next failure take hours to surface.
const BUDGET_HEADROOM_NUM: u64 = 3;

/// The denominator of the headroom fraction.
const BUDGET_HEADROOM_DEN: u64 = 2;

/// The hardest the derived budget may go: this many times the pinned value.
///
/// A run cannot exceed the budget it was killed at, so each kill can only raise
/// the observed lower bound by the headroom factor; without a stop the budget
/// would climb by that factor per kill for as long as the host stayed slow. Four
/// is the point where one bad measurement can no longer cost the pipeline hours
/// of wall clock before the next change gets judged — past it, the honest answer
/// is that the pin is wrong, and a name on the reading says so.
const MAX_BUDGET_MULTIPLE: u64 = 4;

/// What the two budgets are set to and what runs have done against them.
///
/// One instance is shared by the verification side and the deployment side: they
/// are the two halves of the same process's budget, and a reader asking whether
/// the pipeline is being starved of time wants both in one scrape.
#[derive(Debug)]
pub struct VerificationBudget {
    configured_test: u64,
    configured_build: u64,
    /// The longest a run of this kind has been observed to take, starting at
    /// zero for "none yet": the derivation must not start from a mark it never
    /// measured.
    /// Monotone, because the question is what the work *needs*, and one fast
    /// run does not make the slow ones need less. A slow run is the only thing
    /// that raises it, and nothing lowers it — a budget that shrinks the moment
    /// the next run is quick would oscillate around the very durations that
    /// keep being killed.
    test_high_water: AtomicU64,
    build_high_water: AtomicU64,
    test_timeouts: AtomicU64,
    build_timeouts: AtomicU64,
    last_test_secs: AtomicU64,
    last_build_secs: AtomicU64,
}

impl VerificationBudget {
    pub fn new(test_timeout_secs: u64, build_timeout_secs: u64) -> Self {
        Self {
            configured_test: test_timeout_secs,
            configured_build: build_timeout_secs,
            test_high_water: AtomicU64::new(0),
            build_high_water: AtomicU64::new(0),
            test_timeouts: AtomicU64::new(0),
            build_timeouts: AtomicU64::new(0),
            last_test_secs: AtomicU64::new(NO_RUN),
            last_build_secs: AtomicU64::new(NO_RUN),
        }
    }

    /// The number the deployment pinned, the floor of the derivation.
    pub fn configured_secs(&self, kind: &str) -> u64 {
        match kind {
            KIND_BUILD => self.configured_build,
            _ => self.configured_test,
        }
    }

    /// The budget that kind is allowed, for the caller that is about to enforce
    /// it. Reading it from here rather than from the knob keeps one source for
    /// "what the run was actually held to", and reading it at the moment of the
    /// run rather than when the pipeline was built is what lets a measurement
    /// from an earlier run apply to this one.
    ///
    /// A pin of zero stays zero. Zero is not a budget the work can run inside —
    /// it would kill every run on the first poll — so it can only mean the
    /// deployment has not set one, and inventing a budget out of a measurement
    /// in that state would start enforcing a bound nobody asked for.
    pub fn timeout_secs(&self, kind: &str) -> u64 {
        let configured = self.configured_secs(kind);
        if configured == 0 {
            return 0;
        }
        let observed = self.high_water(kind);
        if observed == 0 {
            return configured;
        }
        let derived = observed
            .saturating_mul(BUDGET_HEADROOM_NUM)
            .saturating_add(BUDGET_HEADROOM_DEN - 1)
            / BUDGET_HEADROOM_DEN;
        derived
            .max(configured)
            .min(configured.saturating_mul(MAX_BUDGET_MULTIPLE))
    }

    /// Which side set the number [`Self::timeout_secs`] returns.
    pub fn budget_source(&self, kind: &str) -> &'static str {
        let configured = self.configured_secs(kind);
        let effective = self.timeout_secs(kind);
        if configured == 0 || effective <= configured {
            return SOURCE_CONFIGURED;
        }
        if effective >= configured.saturating_mul(MAX_BUDGET_MULTIPLE) {
            return SOURCE_CEILING;
        }
        SOURCE_DERIVED
    }

    /// The longest run of this kind seen so far, or `None` before the first.
    ///
    /// A run that measured zero and a run that never happened both read as
    /// `None`: neither can move a budget in any direction, since the derivation
    /// floors at the pinned value either way. The elapsed reading on the metric
    /// is where a real zero is still visible as a zero.
    pub fn high_water_secs(&self, kind: &str) -> Option<u64> {
        match self.high_water(kind) {
            0 => None,
            secs => Some(secs),
        }
    }

    /// Record a run that finished — passed or failed on its own — after
    /// `elapsed_secs`.
    pub fn record_run(&self, kind: &str, elapsed_secs: u64) {
        self.last_slot(kind).store(elapsed_secs, Ordering::Relaxed);
        raise(self.high_water_slot(kind), elapsed_secs);
    }

    /// Record a run the budget killed.
    ///
    /// The elapsed reading is deliberately not written. A killed run was cut
    /// short at the budget, so its duration is the budget and not a measurement
    /// of the work — publishing it as "how long the last run took" would say
    /// the work finishes exactly at the limit, and a reader checking whether the
    /// budget has headroom would take that as headroom of zero every time.
    ///
    /// The high-water mark is a different question and does take the kill: the
    /// run needed at least the budget it was killed at, so that budget is the
    /// lower bound the next one derives from. The elapsed reading stays absent
    /// because the run has no duration of its own to report.
    pub fn record_timeout(&self, kind: &str) {
        self.timeout_counter(kind).fetch_add(1, Ordering::Relaxed);
        raise(self.high_water_slot(kind), self.timeout_secs(kind));
    }

    fn last_slot(&self, kind: &str) -> &AtomicU64 {
        if kind == KIND_BUILD {
            &self.last_build_secs
        } else {
            &self.last_test_secs
        }
    }

    fn high_water_slot(&self, kind: &str) -> &AtomicU64 {
        if kind == KIND_BUILD {
            &self.build_high_water
        } else {
            &self.test_high_water
        }
    }

    fn high_water(&self, kind: &str) -> u64 {
        if kind == KIND_BUILD {
            self.build_high_water.load(Ordering::Relaxed)
        } else {
            self.test_high_water.load(Ordering::Relaxed)
        }
    }

    fn timeout_counter(&self, kind: &str) -> &AtomicU64 {
        if kind == KIND_BUILD {
            &self.build_timeouts
        } else {
            &self.test_timeouts
        }
    }
}

impl Default for VerificationBudget {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

/// Keep the larger of the slot's value and `value`.
///
/// `fetch_max` rather than a load-and-store: several runs of one kind can be
/// recording into the same slot from different tasks, and a compare-and-store
/// written by hand loses the larger reading when the two interleave.
fn raise(slot: &AtomicU64, value: u64) {
    slot.fetch_max(value, Ordering::Relaxed);
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
    pub fn timeouts(&self, kind: &str) -> u64 {
        self.timeout_counter(kind).load(Ordering::Relaxed)
    }

    /// Take on readings produced elsewhere. Same quantities, same kinds, so the
    /// reader that was already asking this process about one of them keeps
    /// getting the same answer when the work moves.
    ///
    /// The elapsed time is written only when the other side had one: an absent
    /// reading must not overwrite a real one from an earlier run in this
    /// process, and a killed run deliberately carries no elapsed time (its
    /// duration is the budget, not a measurement of the work).
    pub fn adopt_run(&self, kind: &str, elapsed_secs: Option<u64>, timeouts: u64) {
        if let Some(secs) = elapsed_secs {
            self.record_run(kind, secs);
        }
        for _ in 0..timeouts {
            self.record_timeout(kind);
        }
    }
}

#[async_trait::async_trait]
impl Observable for VerificationBudget {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::with_capacity(BUDGET_KINDS.len() * 3);
        for kind in BUDGET_KINDS {
            out.push(
                RawMetric::new(BUDGET_SECONDS_METRIC, self.timeout_secs(kind) as f64)
                    .with_label(KIND_LABEL, *kind)
                    .with_label(crate::LABEL_SOURCE, self.budget_source(kind)),
            );
            out.push(
                RawMetric::new(
                    TIMEOUTS_TOTAL_METRIC,
                    self.timeout_counter(kind).load(Ordering::Relaxed) as f64,
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

    /// The value of one label on the budget reading of one kind.
    async fn label(
        budget: &VerificationBudget,
        metric: &str,
        kind: &str,
        name: &str,
    ) -> Option<String> {
        budget
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.name == metric && m.labels.get(KIND_LABEL).map(String::as_str) == Some(kind)
            })
            .and_then(|m| m.labels.get(name).cloned())
    }

    /// Both budgets reach the scrape under their own kind, and neither is
    /// summed into the other: the two knobs are set independently and a reader
    /// has to be able to see which one it is looking at.
    #[tokio::test]
    async fn each_budget_is_reported_under_its_own_kind() {
        let budget = VerificationBudget::new(3600, 1800);
        assert_eq!(
            reading(&budget, BUDGET_SECONDS_METRIC, KIND_TEST).await,
            Some(3600.0)
        );
        assert_eq!(
            reading(&budget, BUDGET_SECONDS_METRIC, KIND_BUILD).await,
            Some(1800.0)
        );
    }

    /// A kind that has never run has no elapsed reading. Reporting zero would
    /// say the run was instantaneous, which is the opposite of "no run yet".
    #[tokio::test]
    async fn an_unrun_kind_has_no_elapsed_reading() {
        let budget = VerificationBudget::new(3600, 1800);
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_TEST).await,
            None
        );
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_BUILD).await,
            None
        );
    }

    /// A timeout moves the counter of its own kind and no other, and leaves the
    /// elapsed reading alone — a run cut short at the budget has no duration of
    /// its own to report.
    #[tokio::test]
    async fn a_timeout_moves_only_its_own_counter() {
        let budget = VerificationBudget::new(3600, 1800);
        budget.record_timeout(KIND_TEST);
        assert_eq!(
            reading(&budget, TIMEOUTS_TOTAL_METRIC, KIND_TEST).await,
            Some(1.0)
        );
        assert_eq!(
            reading(&budget, TIMEOUTS_TOTAL_METRIC, KIND_BUILD).await,
            Some(0.0)
        );
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_TEST).await,
            None
        );
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_BUILD).await,
            None
        );
    }

    /// A run that finished on its own moves the elapsed reading and leaves the
    /// counter alone. Otherwise "the last run took the whole budget" and "the
    /// budget killed the last run" would be the same reading, and only one of
    /// them means changes are being lost.
    #[tokio::test]
    async fn a_completed_run_does_not_move_the_counter() {
        let budget = VerificationBudget::new(3600, 1800);
        budget.record_run(KIND_TEST, 900);
        assert_eq!(
            reading(&budget, LAST_RUN_SECONDS_METRIC, KIND_TEST).await,
            Some(900.0)
        );
        assert_eq!(
            reading(&budget, TIMEOUTS_TOTAL_METRIC, KIND_TEST).await,
            Some(0.0)
        );
    }

    /// A run that took most of its budget moves the next budget above it. This
    /// is the whole point of the loop: the reading existed before, the number
    /// did not follow it, and the change that was 30% away from the wall was
    /// followed by another change at the same wall.
    #[tokio::test]
    async fn a_run_that_nearly_fills_the_budget_raises_it() {
        let budget = VerificationBudget::new(3600, 1800);
        budget.record_run(KIND_TEST, 3000);
        // Three halves of the observed maximum, still above the pin.
        assert_eq!(budget.timeout_secs(KIND_TEST), 4500);
        assert_eq!(budget.high_water_secs(KIND_TEST), Some(3000));
        // The build kind has its own scale and its own reading: a slow test run
        // says nothing about how long a release build needs.
        assert_eq!(budget.timeout_secs(KIND_BUILD), 1800);
    }

    /// A run well inside the pin leaves it exactly where the deployment put it.
    /// The loop widens a budget; it does not take a budget away.
    #[tokio::test]
    async fn a_run_inside_the_pin_leaves_it_alone() {
        let budget = VerificationBudget::new(3600, 1800);
        budget.record_run(KIND_TEST, 1200);
        assert_eq!(budget.timeout_secs(KIND_TEST), 3600);
        assert_eq!(budget.budget_source(KIND_TEST), SOURCE_CONFIGURED);
    }

    /// A kill is evidence: the run needed more than the budget it was held to,
    /// so that budget becomes the lower bound and the next run gets the
    /// headroom. Each further kill steps up again, and the last step stops at
    /// the hard multiple of the pin instead of climbing forever.
    #[tokio::test]
    async fn each_kill_steps_the_budget_up_to_the_cap() {
        let budget = VerificationBudget::new(3600, 1800);

        budget.record_timeout(KIND_TEST);
        assert_eq!(budget.timeout_secs(KIND_TEST), 5400);
        assert_eq!(budget.budget_source(KIND_TEST), SOURCE_DERIVED);

        budget.record_timeout(KIND_TEST);
        assert_eq!(budget.timeout_secs(KIND_TEST), 8100);

        budget.record_timeout(KIND_TEST);
        assert_eq!(budget.timeout_secs(KIND_TEST), 12150);

        budget.record_timeout(KIND_TEST);
        assert_eq!(budget.timeout_secs(KIND_TEST), 14400);
        assert_eq!(
            budget.budget_source(KIND_TEST),
            SOURCE_CEILING,
            "a budget held at the hard multiple has to say so: at that point the \
             pin is wrong and the reading is the only thing that can say it"
        );
        // The build kind is untouched by any of it.
        assert_eq!(budget.timeout_secs(KIND_BUILD), 1800);
        assert_eq!(budget.timeouts(KIND_TEST), 4);
    }

    /// A fast run after a slow one does not take back what the slow one proved.
    /// Otherwise the budget would oscillate around the durations that keep
    /// getting killed, spending most of its time on the wrong side of them.
    #[tokio::test]
    async fn a_fast_run_after_a_slow_one_does_not_lower_the_budget() {
        let budget = VerificationBudget::new(3600, 1800);
        budget.record_run(KIND_TEST, 3000);
        budget.record_run(KIND_TEST, 100);
        assert_eq!(budget.timeout_secs(KIND_TEST), 4500);
        assert_eq!(budget.high_water_secs(KIND_TEST), Some(3000));
        assert_eq!(budget.last_run_secs(KIND_TEST), Some(100));
    }

    /// A deployment that pinned no budget keeps none. Zero is not a budget the
    /// work fits inside, it is the absence of one, and a measurement must not
    /// quietly start enforcing a bound nobody asked for.
    #[tokio::test]
    async fn an_unset_pin_stays_unset() {
        let budget = VerificationBudget::new(0, 0);
        budget.record_run(KIND_TEST, 9000);
        assert_eq!(budget.timeout_secs(KIND_TEST), 0);
        assert_eq!(budget.budget_source(KIND_TEST), SOURCE_CONFIGURED);
    }

    /// The budget reading carries which side set it. A pinned 3600 and a
    /// derived 3600 are the same digit and different facts, and the reader that
    /// has to decide whether to raise the pin is the one that needs the
    /// difference.
    #[tokio::test]
    async fn the_budget_reading_says_which_side_set_it() {
        let budget = VerificationBudget::new(3600, 1800);
        assert_eq!(
            label(
                &budget,
                BUDGET_SECONDS_METRIC,
                KIND_TEST,
                crate::LABEL_SOURCE
            )
            .await,
            Some(SOURCE_CONFIGURED.to_string())
        );
        budget.record_run(KIND_TEST, 3000);
        assert_eq!(
            label(
                &budget,
                BUDGET_SECONDS_METRIC,
                KIND_TEST,
                crate::LABEL_SOURCE
            )
            .await,
            Some(SOURCE_DERIVED.to_string())
        );
        assert_eq!(
            label(
                &budget,
                BUDGET_SECONDS_METRIC,
                KIND_BUILD,
                crate::LABEL_SOURCE
            )
            .await,
            Some(SOURCE_CONFIGURED.to_string()),
            "the build kind did not move and must not claim a measurement it never saw"
        );
    }

    /// The kind domain is published, so a rule or a probe reading the label can
    /// be checked against the emitter's list instead of against its own copy.
    #[tokio::test]
    async fn every_published_kind_is_emitted() {
        let budget = VerificationBudget::new(3600, 1800);
        let metrics = budget.collect_metrics("").await.unwrap();
        for kind in BUDGET_KINDS {
            assert!(
                metrics.iter().any(|m| m.name == BUDGET_SECONDS_METRIC
                    && m.labels.get(KIND_LABEL).map(String::as_str) == Some(kind)),
                "{kind} is in BUDGET_KINDS but no budget reading carries it"
            );
        }
    }
}
