//! Periodically folds the LLM usage ledger into durable per-window rows.
//!
//! The realtime token series (`llm_tokens_total`) lives inside the gateway
//! process, is split per pod, and starts over at zero on every gateway rollout.
//! That makes it unusable as the answer to "where did the tokens go" across a
//! restart or across an outage: the series simply is not there. This loop runs
//! in the main application instead — a process that shares the database's
//! lifetime domain rather than the gateway's rollout cycle — and writes the
//! fold into [`crate::usage_store::ROLLUP_TABLE`], where it survives both a
//! gateway restart and a period with no traffic at all.
//!
//! No lease guards it. A lease answers "which replica may act", and here every
//! replica acting is harmless: the fold's rows are keyed on the window they
//! cover, so two processes rolling the same window upsert the same rows. They
//! agree on the values because the read and the write are not atomic but the
//! input is stable — a closed window's source rows no longer grow, since the
//! ledger stamps each row with the time it was accepted and offers no way to
//! backdate one. Two writers racing can only interleave their two statements;
//! neither can see a window that a third party is still appending to. Give the
//! ledger a backdating path and that argument lapses: the fold would then need
//! a lease, or a watermark the write path enforces. Correctness otherwise comes
//! from the work being recomputable from the ledger, not from a single writer —
//! so the loop does not need to know whether it is the only one running, and a
//! replica that was down during a window still catches that window up on its
//! next pass.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::capability::{judge_window_freshness, Capability, Verdict, WindowWatermark};
use crate::usage_store::{windows_before, windows_to_roll, LlmUsageStore};

/// The loop, as the liveness readings name it. A loop whose whole job is to run
/// on a period has no other way to say it is still doing so.
pub const LOOP: &str = "observability_llm_usage_rollup";

/// Accounting window length.
///
/// One hour: long enough that a quiet window is still a meaningful rate, short
/// enough that a shift in where the tokens go is visible the same day. This is
/// a property of the reading, not a knob — a deployment that wanted a different
/// window would want a different series, not a different number here.
// cap: bound — 是这条读数的属性不是旋钮：想要另一个窗的部署想要的是另一条序列，不是这里换个数字
pub const WINDOW_SECS: i64 = 3600;

/// How often the loop looks for windows that have closed.
///
/// Shorter than a window so a closed one lands well before the next boundary,
/// which keeps the newest folded window close to now without polling.
// cap: bound — 短于一个窗，关上的窗才会在下个边界之前落好，最新折好的窗贴近现在而不靠轮询
pub const PERIOD: Duration = Duration::from_secs(600);

/// How far back a first run reaches.
///
/// A first run has no `last_rolled_window_end` to resume from and must not
/// replay the whole ledger; this bounds the catch-up to the recent past, which
/// is where the holes worth filling are. Restarts after the first run resume
/// from the table itself and are not bounded by this — a gap longer than the
/// lookback because the fold never ran would otherwise be skipped, and a skipped
/// window is exactly the hole the fold exists to prevent.
// cap: bound — 折从来没跑过时留下的那段会因此被跳过，而被跳过的窗正是这个折要防的洞
pub const LOOKBACK_SECS: i64 = 7 * 24 * 3600;

/// How many windows one pass may add to the *older* end of the fold.
///
/// The lookback bounds a first run, which leaves the ledger's older history
/// unsummed by any window; the backfill walks down to it a batch at a time.
/// A day per pass keeps the loop's own work bounded while still converging on
/// an archive that grew for months before the fold existed — measured on this
/// deployment on 2026-10-27, the ledger reached back 32 days while the fold
/// covered 24 of them. The number is windows rather than rows because the
/// cost of a window is set by how many combinations it carries, which is a
/// property of the deployment rather than of the backlog.
// cap: bound — 一个窗的代价由它带多少种组合决定，那是部署的属性而不是积压的属性
pub const BACKFILL_WINDOWS_PER_PASS: usize = 24;

/// Fold the windows that have closed since the last fold, then carry the fold's
/// older end down toward the ledger's oldest row.
///
/// Returns the number of rows written, 0 when there was nothing to do.
///
/// Exposed so the loop's one decision — which windows to fold — can be driven
/// from a test without a running tokio task.
pub async fn run_once(store: &LlmUsageStore) -> anyhow::Result<u64> {
    let last = store.last_rolled_window_end().await?;
    let windows = windows_to_roll(last, Utc::now(), WINDOW_SECS, LOOKBACK_SECS);
    let mut written = store.rollup_windows(&windows).await?;
    written += backfill_once(store).await?;
    Ok(written)
}

/// Carry the fold one batch further back, toward the oldest row the ledger
/// still holds.
///
/// The fold's older end is itself the resume point: the backfill asks where the
/// fold starts and lands the windows immediately below it, so nothing is
/// remembered outside the rollup table and a restart resumes where the table
/// says. A window with no traffic still lands its zero rows for every
/// combination the ledger knows, which is what keeps the boundary moving
/// through a quiet stretch instead of stalling on it.
async fn backfill_once(store: &LlmUsageStore) -> anyhow::Result<u64> {
    let Some(first) = store.first_rolled_window_start().await? else {
        // Nothing folded yet: the head catch-up above is what creates the first
        // windows, and there is no boundary to walk down from.
        return Ok(0);
    };
    let Some(floor) = store.oldest_ledger_ts().await? else {
        return Ok(0);
    };
    let windows = windows_before(first, floor, WINDOW_SECS, BACKFILL_WINDOWS_PER_PASS);
    store.rollup_windows(&windows).await
}

/// Spawn the rollup as a supervised loop under this file's own name.
pub fn spawn(
    store: Arc<LlmUsageStore>,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    cog_core::loop_health::spawn(
        LOOP,
        cog_core::loop_health::Cadence::Periodic(PERIOD),
        shutdown.clone(),
        move |beat| {
            let store = store.clone();
            let shutdown = shutdown.clone();
            async move {
                let mut interval = tokio::time::interval(PERIOD);
                // A pass that overran its period should resume from now, not
                // fire a backlog of catch-ups: the windows it missed are found
                // by their own timestamps on the next pass, not by the timer.
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = interval.tick() => match run_once(&store).await {
                            Ok(0) => {}
                            Ok(rows) => {
                                info!(rows, "llm usage rollup folded windows");
                            }
                            Err(e) => {
                                warn!(error = %e, "llm usage rollup pass failed");
                            }
                        },
                        _ = shutdown.wait() => break,
                    }
                }
            }
        },
    )
}

/// The capability id this face reports under.
pub const FRESHNESS_CAPABILITY_ID: &str = "usage_rollup_current";

/// How far behind the clock the newest folded window may be before the fold
/// counts as stopped.
///
/// Two windows: the loop folds a window once it closes and looks for work every
/// `PERIOD`, so at steady state the newest folded window ended somewhere
/// between one window and one window plus one pass ago. One window would be red
/// on a healthy system every time a pass ran late, and a bound nobody can be
/// inside is the same as no bound.
// cap: bound — 稳态滞后在一个窗到「一个窗加一趟」之间；两倍窗是「漏了一整趟」的界
pub const FRESHNESS_MAX_LAG: Duration = Duration::from_secs(2 * WINDOW_SECS as u64);

#[async_trait::async_trait]
impl WindowWatermark for LlmUsageStore {
    async fn newest_window_end(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        self.last_rolled_window_end().await
    }
}

/// "The usage fold keeps up with the clock."
///
/// The loop's liveness reading says the loop is beating; it cannot say the fold
/// is landing. A pass that ticks and fails to write — the table refusing the
/// insert, the ledger query erroring — beats on time and leaves the fold where
/// it was, and a fold standing still answers "where did the tokens go" with a
/// history that stops at the last successful pass. The loop's own log line is
/// not a reading: nobody selects it, and it is gone with the pod.
///
/// The claim is declared whether or not this process built a store. A
/// deployment whose database was never configured has a fold that is not
/// current, and a claim that disappears with its input is the shape this layer
/// removes — `Unmeasured`, not an undeclared claim.
pub struct UsageRollupFreshness<W: WindowWatermark + 'static> {
    store: Option<Arc<W>>,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl<W: WindowWatermark + 'static> UsageRollupFreshness<W> {
    pub fn new(store: Option<Arc<W>>) -> Self {
        Self {
            store,
            now: Arc::new(Utc::now),
        }
    }

    /// Pin the clock. Used by tests, which cannot wait two windows to see the
    /// lag bound work.
    pub fn with_clock(
        store: Option<Arc<W>>,
        now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Self {
        Self { store, now }
    }

    /// Judge a watermark against a clock, without either the store or the loop.
    pub fn judge_watermark(watermark: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Verdict {
        judge_window_freshness(watermark, now, FRESHNESS_MAX_LAG)
    }
}

#[async_trait::async_trait]
impl<W: WindowWatermark + 'static> Capability for UsageRollupFreshness<W> {
    fn id(&self) -> &'static str {
        FRESHNESS_CAPABILITY_ID
    }

    fn cadence(&self) -> cog_core::loop_health::Cadence {
        // A new answer can only appear when a window closes, so asking once a
        // window is the fastest the judgement can change.
        cog_core::loop_health::Cadence::Periodic(Duration::from_secs(WINDOW_SECS as u64))
    }

    async fn judge(&self) -> Verdict {
        let now = (self.now)();
        let Some(store) = self.store.as_ref() else {
            return Verdict::Unmeasured(
                "this process built no usage ledger, so nothing here folds its windows".into(),
            );
        };
        match store.newest_window_end().await {
            Ok(watermark) => Self::judge_watermark(watermark, now),
            Err(e) => Verdict::Unmeasured(format!("the usage fold could not be read: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    /// A first run fills the lookback and stops at the newest closed window; it
    /// does not emit the still-open one.
    #[test]
    fn a_first_run_covers_the_lookback_up_to_the_newest_closed_window() {
        // 01:30 — the window [01:00, 02:00) is open, so the newest closed one is
        // [00:00, 01:00).
        let now = t(2026, 10, 10, 1, 30);
        let windows = windows_to_roll(None, now, 3600, 3 * 3600);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows.last().unwrap().end, t(2026, 10, 10, 1, 0));
        assert_eq!(windows.first().unwrap().start, t(2026, 10, 9, 22, 0));
    }

    /// Resuming from a folded window folds exactly the ones after it — the
    /// catch-up a restarted process owes.
    #[test]
    fn a_resume_folds_only_the_windows_after_the_last_one() {
        let now = t(2026, 10, 10, 3, 5);
        let last = Some(t(2026, 10, 10, 0, 0));
        let windows = windows_to_roll(last, now, 3600, 7 * 24 * 3600);
        let ends: Vec<_> = windows.iter().map(|w| w.end).collect();
        assert_eq!(
            ends,
            vec![
                t(2026, 10, 10, 1, 0),
                t(2026, 10, 10, 2, 0),
                t(2026, 10, 10, 3, 0),
            ]
        );
    }

    /// Already current: nothing closed since the last fold, so nothing to do —
    /// the loop must not rewrite the window that is still open.
    #[test]
    fn a_current_fold_has_no_work() {
        let now = t(2026, 10, 10, 3, 59);
        let last = Some(t(2026, 10, 10, 3, 0));
        assert!(windows_to_roll(last, now, 3600, 7 * 24 * 3600).is_empty());
    }

    /// A gap longer than the lookback is still fully filled when the table
    /// knows where to resume from: the lookback bounds only the first run, not
    /// a catch-up that has a starting point.
    #[test]
    fn a_long_gap_is_filled_past_the_lookback_when_resuming() {
        let now = t(2026, 10, 10, 2, 0);
        // Last folded ten days ago.
        let last = Some(t(2026, 9, 30, 0, 0));
        let windows = windows_to_roll(last, now, 3600, 7 * 24 * 3600);
        assert_eq!(
            windows.first().unwrap().start,
            t(2026, 9, 30, 0, 0),
            "the catch-up started at the first window after the last fold"
        );
        assert_eq!(windows.last().unwrap().end, t(2026, 10, 10, 2, 0));
        assert_eq!(windows.len(), 10 * 24 + 2);
    }

    /// Rerunning produces the same keys, which is what makes a second process
    /// or a restarted one land on the same rows instead of adjacent ones.
    #[test]
    fn the_window_boundaries_are_a_function_of_the_grid_not_of_now() {
        let a = windows_to_roll(
            Some(t(2026, 10, 10, 0, 0)),
            t(2026, 10, 10, 2, 1),
            3600,
            3600,
        );
        let b = windows_to_roll(
            Some(t(2026, 10, 10, 0, 0)),
            t(2026, 10, 10, 2, 59),
            3600,
            3600,
        );
        assert_eq!(a, b);
    }

    /// An off-grid resume point is snapped up to the next boundary rather than
    /// creating a window that shares time with an already-folded one.
    #[test]
    fn an_off_grid_resume_is_snapped_up() {
        let windows = windows_to_roll(
            Some(t(2026, 10, 10, 0, 30)),
            t(2026, 10, 10, 3, 0),
            3600,
            3600,
        );
        assert_eq!(windows.first().unwrap().start, t(2026, 10, 10, 1, 0));
    }

    /// The declared period is a cadence the liveness family can read: it is
    /// shorter than a window, so "no beat for several periods" means the loop
    /// stopped rather than that nothing needed folding.
    #[test]
    fn the_period_is_shorter_than_a_window() {
        assert!(PERIOD.as_secs() < WINDOW_SECS as u64);
    }

    /// A backfill lands the windows immediately below the fold and stops at the
    /// ledger's oldest row: below that there is nothing to sum, and the batch
    /// ends where the fold currently starts so the two meet without a hole.
    #[test]
    fn a_backfill_reaches_from_the_fold_down_to_the_ledger_floor() {
        let windows = windows_before(t(2026, 10, 10, 0, 0), t(2026, 10, 1, 0, 0), 3600, 10_000);
        assert_eq!(windows.len(), 9 * 24);
        assert_eq!(windows.first().unwrap().start, t(2026, 10, 1, 0, 0));
        assert_eq!(
            windows.last().unwrap().end,
            t(2026, 10, 10, 0, 0),
            "the last window ends where the fold starts, not after it"
        );
    }

    /// One pass moves the boundary by the batch size and no more: a backfill
    /// over a month of ledger is many passes, which is what keeps each pass
    /// readable as work rather than as a stopped loop.
    #[test]
    fn a_backfill_batch_moves_the_boundary_by_its_size_and_no_more() {
        let first = windows_before(t(2026, 10, 10, 0, 0), t(2026, 9, 1, 0, 0), 3600, 24);
        assert_eq!(first.len(), 24);
        assert_eq!(first.last().unwrap().end, t(2026, 10, 10, 0, 0));
        assert_eq!(first.first().unwrap().start, t(2026, 10, 9, 0, 0));

        // The next pass resumes from where this one stopped, and the two are
        // adjacent: no window folded twice, none skipped.
        let second = windows_before(first[0].start, t(2026, 9, 1, 0, 0), 3600, 24);
        assert_eq!(second.last().unwrap().end, first[0].start);
        assert_eq!(second.len(), 24);
    }

    /// The ledger's floor lands inside a window rather than on its boundary, and
    /// the window holding it is still folded: a row at the floor is a row the
    /// fold has to answer for, so stopping above it would leave exactly the
    /// oldest row unsummed and undeletable.
    #[test]
    fn the_window_holding_the_ledger_floor_is_included() {
        let windows = windows_before(t(2026, 10, 2, 0, 0), t(2026, 10, 1, 23, 30), 3600, 10_000);
        assert_eq!(windows.first().unwrap().start, t(2026, 10, 1, 23, 0));
        assert_eq!(windows.first().unwrap().end, t(2026, 10, 2, 0, 0));
    }

    /// Nothing below the fold: the head catch-up already starts where the
    /// ledger does, and a backfill that ran anyway would rewrite windows that
    /// are already summed.
    #[test]
    fn a_fold_that_already_reaches_the_floor_has_no_backfill() {
        assert!(windows_before(t(2026, 10, 1, 0, 0), t(2026, 10, 1, 0, 0), 3600, 24).is_empty());
        assert!(
            windows_before(t(2026, 10, 1, 0, 0), t(2026, 10, 2, 0, 0), 3600, 24).is_empty(),
            "a ledger floor above the fold means the fold is already older, \
             and walking up from it would fold the same hours twice"
        );
    }

    /// Repeated passes walk the boundary all the way down without leaving a gap
    /// between what one pass folded and what the next one takes.
    #[test]
    fn repeated_passes_reach_the_floor_without_a_gap() {
        let floor = t(2026, 9, 30, 6, 0);
        let mut cursor = t(2026, 10, 3, 0, 0);
        let mut seen_start = cursor;
        for _ in 0..20 {
            let batch = windows_before(cursor, floor, 3600, 24);
            if batch.is_empty() {
                break;
            }
            assert_eq!(
                batch.last().unwrap().end,
                cursor,
                "each batch ends where the previous one began"
            );
            cursor = batch[0].start;
            seen_start = cursor;
        }
        assert_eq!(
            seen_start,
            t(2026, 9, 30, 6, 0),
            "the walk stopped at the grid point holding the oldest row"
        );
    }
}
