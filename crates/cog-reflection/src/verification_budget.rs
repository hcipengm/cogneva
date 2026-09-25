//! The two budgets that bound a self-evolution run, and what they did.
//!
//! `test_timeout_secs` and `build_timeout_secs` are the knobs that decide when a
//! verification or a deployment run is killed. Both were, until recently, fields
//! nothing read; both now wrap the child in a timeout. What stayed missing is
//! any reading of the two facts that matter to whoever has to set them:
//!
//! - what the effective budget is, and
//! - how close real runs come to it, and whether any has been killed by it.
//!
//! Without the second, the only way a run's budget shows up is as an error the
//! caller never sees: a bounded run that exceeds its budget is retired with a
//! reason, and the reason lives in one log line inside one pod's cycle. The
//! aggregate is what says whether the budget is the thing standing in the way —
//! a host that got slower turns every change into a timeout, and that looks
//! exactly like a pipeline with nothing to do.
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

/// What the two budgets are set to and what runs have done against them.
///
/// One instance is shared by the verification side and the deployment side: they
/// are the two halves of the same process's budget, and a reader asking whether
/// the pipeline is being starved of time wants both in one scrape.
#[derive(Debug)]
pub struct VerificationBudget {
    test_timeout_secs: u64,
    build_timeout_secs: u64,
    test_timeouts: AtomicU64,
    build_timeouts: AtomicU64,
    last_test_secs: AtomicU64,
    last_build_secs: AtomicU64,
}

impl VerificationBudget {
    pub fn new(test_timeout_secs: u64, build_timeout_secs: u64) -> Self {
        Self {
            test_timeout_secs,
            build_timeout_secs,
            test_timeouts: AtomicU64::new(0),
            build_timeouts: AtomicU64::new(0),
            last_test_secs: AtomicU64::new(NO_RUN),
            last_build_secs: AtomicU64::new(NO_RUN),
        }
    }

    /// The budget that kind is allowed, for the caller that is about to enforce
    /// it. Reading it from here rather than from the knob keeps one source for
    /// "what the run was actually held to".
    pub fn timeout_secs(&self, kind: &str) -> u64 {
        match kind {
            KIND_BUILD => self.build_timeout_secs,
            _ => self.test_timeout_secs,
        }
    }

    /// Record a run that finished — passed or failed on its own — after
    /// `elapsed_secs`.
    pub fn record_run(&self, kind: &str, elapsed_secs: u64) {
        self.last_slot(kind).store(elapsed_secs, Ordering::Relaxed);
    }

    /// Record a run the budget killed.
    ///
    /// The elapsed reading is deliberately not written. A killed run was cut
    /// short at the budget, so its duration is the budget and not a measurement
    /// of the work — publishing it as "how long the last run took" would say
    /// the work finishes exactly at the limit, and a reader checking whether the
    /// budget has headroom would take that as headroom of zero every time.
    pub fn record_timeout(&self, kind: &str) {
        self.timeout_counter(kind).fetch_add(1, Ordering::Relaxed);
    }

    fn last_slot(&self, kind: &str) -> &AtomicU64 {
        if kind == KIND_BUILD {
            &self.last_build_secs
        } else {
            &self.last_test_secs
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

#[async_trait::async_trait]
impl Observable for VerificationBudget {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::with_capacity(BUDGET_KINDS.len() * 3);
        for kind in BUDGET_KINDS {
            out.push(
                RawMetric::new(BUDGET_SECONDS_METRIC, self.timeout_secs(kind) as f64)
                    .with_label(KIND_LABEL, *kind),
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
