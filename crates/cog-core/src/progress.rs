//! Whether a running attempt is *moving*, as a fact its supervisor can read.
//!
//! Before this module the only question a supervisor could ask about a running
//! task was how long it had been running, and the answer it acted on was a
//! declared budget: the task was killed the moment elapsed time passed it. That
//! predicate measures the clock, not the work — a run that was streaming tokens
//! the whole way and one that wedged in its first second are the same reading to
//! it. The cost was measurable: on 2026-11-08 every failed task in three days
//! carried the same sentence, `degenerate_loop: task ran its whole Ns budget
//! without producing a result`, and the budget was the p99 of *completed*
//! durations — so the runs it killed never entered the sample it was computed
//! from, and the loss was invisible from both ends.
//!
//! The replacement asks the question that separates those two runs: when did
//! this attempt last do something? A step that produces output records a beat;
//! silence past a window derived from measured gaps is what stops an attempt.
//! Elapsed time is not part of the decision, so a slow run that keeps working is
//! never cut, and a run that stops moving is cut without waiting for a budget to
//! expire.
//!
//! Process-local on purpose. A beat is evidence that *this* process is driving
//! the attempt; a supervisor in another process reads the lease, which is the
//! fact that survives a process going away. Persisting beats would turn a
//! liveness check into write traffic proportional to tokens.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The quantile of measured inter-beat gaps that a silence window is taken at.
///
/// Chosen as a false-alarm budget rather than tuned: a window at this quantile
/// reclaims roughly one attempt in a thousand *while it is still working*, and
/// the price of a false reclaim is a whole attempt's spend. Lowering it buys
/// faster detection at a rate that grows with how many steps a day the fleet
/// takes, which is the wrong thing to hold fixed.
///
/// It is also what the sample floor is derived from (see [`SAMPLE_FLOOR`]), so
/// moving this one number moves both, and neither can drift from the other.
// cap: bound — 误判预算是拍定的：窗口取在这里，随手停摆回收约千分之一次仍在干活的尝试，这就是选它的全部理由；没有一条测量能把它调到别处
const SILENCE_QUANTILE: f64 = 0.999;

/// How many gap samples a window needs before it is a reading at all.
///
/// One sample per quantile point: an estimate of the q-quantile carries no
/// information about the tail until there is at least one observation beyond it.
/// Derived from [`SILENCE_QUANTILE`] rather than picked, so the two cannot
/// disagree — a floor that stayed at 100 while the quantile moved to 0.9999
/// would report a window from a sample that cannot support it.
// cap: bound — 1 / (1 - SILENCE_QUANTILE)：估 0.999 分位要至少有一个落在它之外的点
pub const SAMPLE_FLOOR: usize = 1000;

fn beacons() -> &'static Mutex<HashMap<String, Instant>> {
    static B: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    B.get_or_init(|| Mutex::new(HashMap::new()))
}

fn gaps() -> &'static Mutex<GapSamples> {
    static G: OnceLock<Mutex<GapSamples>> = OnceLock::new();
    G.get_or_init(|| Mutex::new(GapSamples::default()))
}

/// A bounded reservoir of measured inter-beat gaps.
///
/// Bounded because the window it feeds is a quantile of recent behaviour, not of
/// all behaviour ever: an unbounded vector would answer with the slowest month
/// this deployment ever had, and a window that only widens is the same defect as
/// a constant — it stops being derived from what is happening.
#[derive(Default)]
struct GapSamples {
    seen: u64,
    ring: Vec<Duration>,
    next: usize,
    /// The window as of the last sample. Cached because the readers are hot and
    /// the writer is not: every watchdog poll, every reclaim sweep and every
    /// scrape asks for it, while a sample arrives once per step. Re-sorting the
    /// reservoir on each read would put a 10 000-element sort on the read path
    /// for a number that only changes when a step completes.
    window: Option<Duration>,
}

impl GapSamples {
    /// Ten windows' worth is enough for a p99.9 without holding a day of samples:
    /// the reservoir is sized from the floor, which is itself derived from the
    /// quantile, so there is no second number to keep in step.
    fn capacity() -> usize {
        SAMPLE_FLOOR * 10
    }

    fn push(&mut self, gap: Duration) {
        self.seen = self.seen.saturating_add(1);
        if self.ring.len() < Self::capacity() {
            self.ring.push(gap);
        } else {
            self.ring[self.next] = gap;
        }
        self.next = (self.next + 1) % Self::capacity();
        self.window = self.compute_quantile(SILENCE_QUANTILE);
    }

    /// The q-quantile of what is held, or `None` when the sample cannot support
    /// it. Reporting a window from too few samples would put a number on a
    /// population nobody has looked at yet — the same mistake the wall made,
    /// wearing the clothes of a measurement.
    fn compute_quantile(&self, q: f64) -> Option<Duration> {
        if self.ring.len() < SAMPLE_FLOOR {
            return None;
        }
        let mut sorted = self.ring.clone();
        sorted.sort_unstable();
        // Nearest-rank: the first observation at or beyond the quantile. No
        // interpolation, because the quantity being read is "how long did a
        // real gap last", and an average of two neighbours is a duration no run
        // ever had.
        let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
        Some(sorted[rank - 1])
    }
}

/// Record that this attempt just did something.
///
/// Called from the places where work actually advances — a streamed event, a
/// completed tool call, an iteration boundary — not from a timer, because a
/// timer would report the process is alive (which the lease already says) rather
/// than that the attempt is progressing.
pub fn beat(task_id: &str) {
    if task_id.is_empty() {
        return;
    }
    let mut map = beacons().lock().unwrap_or_else(|p| p.into_inner());
    let now = Instant::now();
    if let Some(previous) = map.insert(task_id.to_string(), now) {
        let gap = now.saturating_duration_since(previous);
        drop(map);
        gaps().lock().unwrap_or_else(|p| p.into_inner()).push(gap);
    }
}

/// When this attempt last did something, or `None` when nothing ever recorded it.
///
/// `None` is a different fact from "a long time ago" and the caller must not
/// fold the two: an attempt whose executor emits no beats is unmeasurable, and
/// treating unmeasurable as silence would cut every task of that kind on its
/// first check.
pub fn last_beat(task_id: &str) -> Option<Instant> {
    beacons()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(task_id)
        .copied()
}

/// Drop an attempt's beacon. Called when the attempt ends, so a long-lived
/// process does not carry a map entry per task it has ever run.
pub fn forget(task_id: &str) {
    beacons()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(task_id);
}

/// The silence window, measured: the [`SILENCE_QUANTILE`] quantile of observed
/// inter-beat gaps.
///
/// `None` until [`SAMPLE_FLOOR`] gaps have been seen. A caller that gets `None`
/// has no window and must say so rather than substituting one; the reading is
/// "this deployment has not run enough steps to know", which is not the same as
/// "nothing is stuck" or "everything is stuck".
pub fn silence_window() -> Option<Duration> {
    gaps().lock().unwrap_or_else(|p| p.into_inner()).window
}

/// The stall verdict: how long this attempt has been quiet and the measured
/// window it exceeded, or `None` when there is no evidence that it stopped.
///
/// One definition, read by both things that can end a run — the orchestrator's
/// reclaim sweep and the executor router's watchdog. Two copies would be two
/// predicates, and the day they drift is the day one side stops a run while the
/// other keeps waiting for it, with no reading that says which is right.
///
/// `None` covers three different situations on purpose, because all three mean
/// the same thing to a caller: nobody measured a beat for this attempt, or the
/// deployment has not run enough steps to know what a long gap is, or the
/// attempt is inside the window. In each case "do not stop it" is the only
/// answer supported by evidence.
pub fn stall(task_id: &str) -> Option<(Duration, Duration)> {
    let last = last_beat(task_id)?;
    let window = silence_window()?;
    let silent = last.elapsed();
    (silent > window).then_some((silent, window))
}

/// How many gap samples have been observed, for a reader that has to say whether
/// the window is a measurement yet.
pub fn gap_sample_count() -> usize {
    gaps().lock().unwrap_or_else(|p| p.into_inner()).ring.len()
}

/// Test hook: plant `count` gaps of a known length, so a test can fix the
/// window instead of racing real timing. A window asserted from gaps the test
/// also produced is the only kind of window a test can compare against.
#[doc(hidden)]
pub fn seed_gaps_for_tests(gap: Duration, count: usize) {
    let mut g = gaps().lock().unwrap_or_else(|p| p.into_inner());
    for _ in 0..count {
        g.push(gap);
    }
}

/// The one lock the test hooks below hand out.
///
/// Tokio's rather than std's because some of the tests that need it are async:
/// a `std` guard held across an `await` is a deadlock waiting for the first
/// test that holds it while awaiting a task that wants it, and clippy refuses
/// the shape outright (`await_holding_lock`). One mutex with two doors, not two
/// mutexes — two would not exclude each other, and the races the lock exists to
/// prevent would come straight back.
fn serial() -> &'static tokio::sync::Mutex<()> {
    static L: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Test hook: hold the one lock that serialises tests sharing this state.
///
/// The beacons and the reservoir are one population per process, so two tests
/// that seed or clear them are two writers of the same measurement. Run
/// concurrently, one test's `reset` lands inside another's window and the
/// assertion then describes a population neither test set up — the failure
/// shows up as a flaky red that says nothing about the code under test.
///
/// Tests that only *beat* need not hold it: a handful of real gaps cannot reach
/// [`SAMPLE_FLOOR`], so they cannot move a window.
///
/// Blocks the thread. A test that is itself async takes it through
/// [`async_test_lock`] instead; calling this one from inside a runtime panics
/// by tokio's rule, which is the right failure — it names the wrong door rather
/// than hanging.
#[doc(hidden)]
pub fn test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    serial().blocking_lock()
}

/// Test hook: the same lock, for a test that is itself async.
#[doc(hidden)]
pub async fn async_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    serial().lock().await
}

/// Test hook: clear every beacon and every sample.
///
/// Exists because this state is process-global and the tests in different
/// modules would otherwise read each other's beacons — a specimen that shares
/// the population it measures is the failure mode this module's own tests must
/// not have.
#[doc(hidden)]
pub fn reset_for_tests() {
    beacons().lock().unwrap_or_else(|p| p.into_inner()).clear();
    *gaps().lock().unwrap_or_else(|p| p.into_inner()) = GapSamples::default();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sample floor is the number of quantile points, not a number anybody
    /// picked: `1 / (1 - q)`. A floor that stopped following the quantile would
    /// let the window be reported from a sample that cannot support it.
    #[test]
    fn the_sample_floor_follows_from_the_quantile() {
        let from_quantile = (1.0 / (1.0 - SILENCE_QUANTILE)).round() as usize;
        assert_eq!(
            SAMPLE_FLOOR, from_quantile,
            "the floor must be one sample per quantile point"
        );
    }

    #[test]
    fn a_window_is_not_reported_before_the_sample_supports_it() {
        let _serial = test_lock();
        reset_for_tests();
        for _ in 0..SAMPLE_FLOOR - 1 {
            gaps().lock().unwrap().push(Duration::from_millis(10));
        }
        assert_eq!(
            silence_window(),
            None,
            "a window from too few samples is a guess wearing a measurement's clothes"
        );
        gaps().lock().unwrap().push(Duration::from_millis(10));
        assert_eq!(silence_window(), Some(Duration::from_millis(10)));
    }

    /// The window moves with what the fleet actually does: faster steps shrink
    /// it, slower steps grow it. This is the property the wall never had — it
    /// was computed once from completed runs and then executed forever.
    #[test]
    fn the_window_follows_the_measured_gaps() {
        let _serial = test_lock();
        reset_for_tests();
        // Fill the reservoir so the ring is at capacity and the newest samples
        // replace the oldest, which is what makes it a reading of *now*.
        for _ in 0..GapSamples::capacity() {
            gaps().lock().unwrap().push(Duration::from_secs(10));
        }
        let slow = silence_window().expect("enough samples");
        for _ in 0..GapSamples::capacity() {
            gaps().lock().unwrap().push(Duration::from_millis(200));
        }
        let fast = silence_window().expect("enough samples");
        assert!(
            fast < slow,
            "the window must shrink when the fleet speeds up: slow={slow:?} fast={fast:?}"
        );
    }

    #[test]
    fn a_beat_is_readable_and_forgettable() {
        let _serial = test_lock();
        reset_for_tests();
        assert_eq!(last_beat("t1"), None, "no beat is not a very old beat");
        beat("t1");
        assert!(last_beat("t1").is_some());
        forget("t1");
        assert_eq!(last_beat("t1"), None);
    }

    /// Beats feed the window: two beats are one gap, and the gap between them is
    /// what the window is made of.
    #[test]
    fn beats_produce_gap_samples() {
        let _serial = test_lock();
        reset_for_tests();
        assert_eq!(gap_sample_count(), 0);
        beat("t1");
        assert_eq!(gap_sample_count(), 0, "the first beat has no predecessor");
        beat("t1");
        assert_eq!(gap_sample_count(), 1);
    }
}
