//! How each round's discovery probes ended, one series per exit.
//!
//! The loop already counts these: every round it sorts each probe on the books
//! into one of nine exits and prints the counts on a single line. A line is not
//! a reading. These counts are meant to be looked at *over time* -- the
//! re-measure recipe next to the deadline says which way to move it depending
//! on whether `still_open` keeps standing above zero and whether `row_missing`
//! ever has a value -- and nothing can do that to a log line: no trend, no
//! alert rule, and on a dashboard "this loop has not judged a single probe yet"
//! reads exactly like "this loop is not running".
//!
//! One name per exit, never one name for the sum. Folded into a total, "every
//! probe answered" and "every probe came back without an answer" print the same
//! number, and the second one is the finding: it means the discovery loop has
//! no judge of its own and is reading whatever the generic path happened to
//! return.
//!
//! Published through an `Observable` rather than the metric-name registry in
//! `cog-core`: raw metrics are collected by the reading process and vanish with
//! their producer, so they leave no rows and the registry -- whose job is
//! answering "which series can this build still produce?" against the store --
//! has nothing to say about them.

use std::sync::Mutex;

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// The exits, one name each. Spelled as constants rather than inline so a rule's
/// selector and the emitter cannot drift apart, and named after the exit rather
/// than the probe so a rule never has to guess which kind of ending it holds.
pub const VALIDATED_METRIC: &str = "cogneva_discovery_probe_validated";
pub const REJECTED_METRIC: &str = "cogneva_discovery_probe_rejected";
pub const INCONCLUSIVE_METRIC: &str = "cogneva_discovery_probe_inconclusive";
/// The outcome was read and held no verdict.
pub const UNANSWERED_METRIC: &str = "cogneva_discovery_probe_unanswered";
/// The task ended failed or cancelled, leaving no result to read.
pub const RUN_ENDED_WITHOUT_RESULT_METRIC: &str =
    "cogneva_discovery_probe_run_ended_without_result";
/// The task the book names can no longer be found this round.
pub const ROW_MISSING_METRIC: &str = "cogneva_discovery_probe_row_missing";
/// Waited past its lifetime; the probe is dropped rather than kept on the books.
pub const EXPIRED_METRIC: &str = "cogneva_discovery_probe_expired";
/// Not finished yet, still on the books.
pub const STILL_OPEN_METRIC: &str = "cogneva_discovery_probe_still_open";
/// The verdict was read but the recorder failed to record it.
pub const NOT_RECORDED_METRIC: &str = "cogneva_discovery_probe_not_recorded";

/// Which exit each probe on the books took this round, one count per exit.
///
/// Counted apart rather than summed: "0 judged" has two meanings that print the
/// same -- the book is empty, and everything on it came back without a verdict
/// -- and those two point at completely different next steps (one asks for
/// nothing, the other is "this loop has no judge" itself).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeTally {
    /// The outcome carried a verdict and it was recorded.
    pub validated: usize,
    pub rejected: usize,
    pub inconclusive: usize,
    /// The outcome was read, and held no verdict.
    pub unanswered: usize,
    /// The task ended failed or cancelled, with no result to read.
    pub run_ended_without_result: usize,
    /// The task the book names could not be found this round.
    pub row_missing: usize,
    /// Waited past its lifetime; this probe is retired and leaves the books.
    pub expired: usize,
    /// Not finished yet; it stays on the books.
    pub still_open: usize,
    /// A verdict was read and the recording of it failed.
    pub not_recorded: usize,
}

/// The last round's tally, in a shape a scrape can read.
///
/// Holds the *latest* round rather than a running total on purpose: every one of
/// these nine is a state (how many are on the books right now, how many the
/// recording of failed), and a state has to be written back to zero by the round
/// that empties it. Accumulating would make "the books are empty now" read as
/// "several never came back", forever.
#[derive(Debug, Default)]
pub struct DiscoveryOutcomeReadings {
    last: Mutex<OutcomeTally>,
}

impl DiscoveryOutcomeReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the round that just ended.
    ///
    /// Called by the function that produces the tally, at its own exit, rather
    /// than by whatever drives it: a call site is a position, and a position
    /// criterion cannot see a control path that returns early.
    pub fn record(&self, tally: &OutcomeTally) {
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = *tally;
    }

    /// The latest round's counts, for a reader inside this process.
    pub fn last(&self) -> OutcomeTally {
        *self.last.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait::async_trait]
impl Observable for DiscoveryOutcomeReadings {
    /// All nine series from the first scrape on, zeros included.
    ///
    /// A process that has not run a round yet publishes zeros rather than
    /// nothing, and here that is a statement about a book rather than a claim
    /// about the outside world. The nine counts describe probes currently on
    /// the books; a process whose loop has not run yet holds an empty book, so
    /// zero is what it reads, not an average over nothing. The question that
    /// would be dishonest to answer this way -- "is this loop running in this
    /// process at all" -- is not this family's to answer: the loop-health
    /// sentinel next to it already carries that, and it says "not running" by
    /// going stale rather than by reporting a number.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let t = self.last();
        Ok(vec![
            RawMetric::new(VALIDATED_METRIC, t.validated as f64),
            RawMetric::new(REJECTED_METRIC, t.rejected as f64),
            RawMetric::new(INCONCLUSIVE_METRIC, t.inconclusive as f64),
            RawMetric::new(UNANSWERED_METRIC, t.unanswered as f64),
            RawMetric::new(
                RUN_ENDED_WITHOUT_RESULT_METRIC,
                t.run_ended_without_result as f64,
            ),
            RawMetric::new(ROW_MISSING_METRIC, t.row_missing as f64),
            RawMetric::new(EXPIRED_METRIC, t.expired as f64),
            RawMetric::new(STILL_OPEN_METRIC, t.still_open as f64),
            RawMetric::new(NOT_RECORDED_METRIC, t.not_recorded as f64),
        ])
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// Empty: the counts do not vary by dimension, so the collector takes them
    /// once instead of once per dimension.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(metrics: &[RawMetric], name: &str) -> f64 {
        metrics
            .iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("no series named {name}"))
            .value
    }

    /// Every exit is its own series, and each one carries its own count.
    ///
    /// Nine distinct values, so a series wired to the wrong field reads back a
    /// number that belongs to a different exit -- the shape a copy-paste in the
    /// block above produces, and one that a total would hide.
    #[tokio::test]
    async fn each_exit_reaches_the_scrape_as_its_own_series() {
        let readings = DiscoveryOutcomeReadings::new();
        readings.record(&OutcomeTally {
            validated: 1,
            rejected: 2,
            inconclusive: 3,
            unanswered: 4,
            run_ended_without_result: 5,
            row_missing: 6,
            expired: 7,
            still_open: 8,
            not_recorded: 9,
        });

        let metrics = readings.collect_metrics("").await.expect("collect_metrics");
        assert_eq!(metrics.len(), 9, "one series per exit, no more no less");
        for (name, expected) in [
            (VALIDATED_METRIC, 1.0),
            (REJECTED_METRIC, 2.0),
            (INCONCLUSIVE_METRIC, 3.0),
            (UNANSWERED_METRIC, 4.0),
            (RUN_ENDED_WITHOUT_RESULT_METRIC, 5.0),
            (ROW_MISSING_METRIC, 6.0),
            (EXPIRED_METRIC, 7.0),
            (STILL_OPEN_METRIC, 8.0),
            (NOT_RECORDED_METRIC, 9.0),
        ] {
            assert_eq!(named(&metrics, name), expected, "{name}");
        }
    }

    /// A round that empties the books writes the counts back to zero.
    ///
    /// The failure this rules out is the one a running total produces: once a
    /// probe is counted as still open, the reading keeps saying so while the
    /// books are in fact empty, and the re-measure recipe next to the deadline
    /// ("`still_open` standing above zero means the margin is too small") then
    /// stands above zero for a reason that has nothing left to fix.
    #[tokio::test]
    async fn a_round_that_empties_the_books_writes_them_back_to_zero() {
        let readings = DiscoveryOutcomeReadings::new();
        readings.record(&OutcomeTally {
            still_open: 3,
            row_missing: 2,
            ..Default::default()
        });
        let before = readings.collect_metrics("").await.expect("collect_metrics");
        assert_eq!(named(&before, STILL_OPEN_METRIC), 3.0);

        readings.record(&OutcomeTally::default());
        let after = readings.collect_metrics("").await.expect("collect_metrics");
        assert_eq!(named(&after, STILL_OPEN_METRIC), 0.0, "still_open");
        assert_eq!(named(&after, ROW_MISSING_METRIC), 0.0, "row_missing");
    }

    /// A process that has not run a round publishes zeros, not nothing.
    ///
    /// Nine zeroes say the book is empty, which is true of a loop that has
    /// never round-tripped. Publishing an empty vector would say something
    /// else: that nothing here knows what a probe book is, which is the shape
    /// a process that does not carry this loop produces.
    #[tokio::test]
    async fn a_loop_that_has_not_run_a_round_publishes_zeros() {
        let metrics = DiscoveryOutcomeReadings::new()
            .collect_metrics("")
            .await
            .expect("collect_metrics");
        assert_eq!(metrics.len(), 9);
        assert!(metrics.iter().all(|m| m.value == 0.0), "{metrics:?}");
    }
}
