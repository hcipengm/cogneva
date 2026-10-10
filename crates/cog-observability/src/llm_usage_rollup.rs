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
//! replica acting is harmless by construction: the fold's rows are keyed on the
//! window they cover, so two processes rolling the same window upsert the same
//! rows with the same values. Correctness comes from the work being recomputable
//! from the ledger, not from a single writer — so the loop does not need to know
//! whether it is the only one running, and a replica that was down during a
//! window still catches that window up on its next pass.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{info, warn};

use crate::usage_store::{windows_to_roll, LlmUsageStore};

/// The loop, as the liveness readings name it. A loop whose whole job is to run
/// on a period has no other way to say it is still doing so.
pub const LOOP: &str = "observability_llm_usage_rollup";

/// Accounting window length.
///
/// One hour: long enough that a quiet window is still a meaningful rate, short
/// enough that a shift in where the tokens go is visible the same day. This is
/// a property of the reading, not a knob — a deployment that wanted a different
/// window would want a different series, not a different number here.
pub const WINDOW_SECS: i64 = 3600;

/// How often the loop looks for windows that have closed.
///
/// Shorter than a window so a closed one lands well before the next boundary,
/// which keeps the newest folded window close to now without polling.
pub const PERIOD: Duration = Duration::from_secs(600);

/// How far back a first run reaches.
///
/// A first run has no `last_rolled_window_end` to resume from and must not
/// replay the whole ledger; this bounds the catch-up to the recent past, which
/// is where the holes worth filling are. Restarts after the first run resume
/// from the table itself and are not bounded by this — a gap longer than the
/// lookback because the fold never ran would otherwise be skipped, and a skipped
/// window is exactly the hole the fold exists to prevent.
pub const LOOKBACK_SECS: i64 = 7 * 24 * 3600;

/// Roll every window that has closed since the last one folded. Returns the
/// number of rows written, 0 when there was nothing to do.
///
/// Exposed so the loop's one decision — which windows to fold — can be driven
/// from a test without a running tokio task.
pub async fn run_once(store: &LlmUsageStore) -> anyhow::Result<u64> {
    let last = store.last_rolled_window_end().await?;
    let windows = windows_to_roll(last, Utc::now(), WINDOW_SECS, LOOKBACK_SECS);
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
}
