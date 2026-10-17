//! Pacing spend against the quota a window has left, at job boundaries.
//!
//! The quota is not published anywhere we can read it. What the ledger does
//! know is the **measured capacity** of a window: the tokens an upstream served
//! before it answered "this cycle's limit is reached". That sum is what the plan
//! actually allowed, measured rather than claimed, and it is the only quantity
//! here that has an observation behind it.
//!
//! Two rules follow from measurement, and the rest of this module is those two
//! rules turned into arithmetic:
//!
//! - The allowance at any instant is the capacity spread over the window's
//!   length: `capacity * elapsed / window`. Spending the whole capacity on the
//!   first day is not a faster way to finish the month, it is a way to spend
//!   most of it idle — the shape that made four days consume a thirty-day quota.
//! - A job is admitted as a whole or not at all. Cutting a running job off
//!   midway strands the tokens it already spent on unfinished work, moving waste
//!   from the tail to the middle; so a job may only be held before its first
//!   call, and only when the remaining allowance cannot cover a whole one.
//!
//! Nothing here invents a number: an upstream with no window length, or a
//! ledger with no measured capacity yet, produces no allowance and therefore no
//! pacing. A bound with no observation behind it would be exactly the kind of
//! invented limit this codebase forbids.

use cog_core::LlmWindowPace;
use serde::{Deserialize, Serialize};

/// One upstream's measured capacity, and when the window it was measured in
/// ran out.
///
/// `burned_at_unix` is both the end of the window that was spent and the start
/// of the next one: an upstream refuses with "this cycle's limit is reached"
/// only after the last token of that cycle is used, so the refusal instant is a
/// derivation of the boundary rather than a guess at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasuredWindow {
    /// Tokens served in the window that ended at `burned_at_unix`.
    pub capacity_tokens: u64,
    /// The upstream's own statement of how long its window is, in seconds.
    pub window_secs: u64,
    /// When the spent window ended, unix seconds.
    pub burned_at_unix: i64,
}

impl MeasuredWindow {
    /// Whether this measurement can pace anything at all.
    pub fn is_usable(&self) -> bool {
        self.capacity_tokens > 0 && self.window_secs > 0
    }

    /// Start of the window that contains `now_unix`.
    ///
    /// Windows repeat from the instant the previous one was spent, because that
    /// is the only boundary this measurement knows. A clock earlier than the
    /// burn reads the burn itself: the arithmetic must not wrap backwards into
    /// a window start in the future.
    pub fn window_start_at(&self, now_unix: i64) -> i64 {
        if !self.is_usable() {
            return self.burned_at_unix;
        }
        let elapsed = (now_unix - self.burned_at_unix).max(0) as u64;
        let windows = elapsed / self.window_secs;
        self.burned_at_unix + (windows * self.window_secs) as i64
    }

    /// Tokens this window has allowed by `now_unix`: the measured capacity
    /// spread linearly over the window's length.
    ///
    /// Linear rather than a daily budget because a window is not necessarily a
    /// day and a rate is the quantity both shapes share. The value never
    /// exceeds the capacity: past the window's end the next window's clock
    /// restarts, so a reader cannot see more allowance than was measured.
    pub fn allowance_at(&self, now_unix: i64) -> u64 {
        if !self.is_usable() {
            return 0;
        }
        let start = self.window_start_at(now_unix);
        let elapsed = (now_unix - start).max(0) as u64;
        let elapsed = elapsed.min(self.window_secs);
        ((self.capacity_tokens as u128 * elapsed as u128) / self.window_secs as u128) as u64
    }
}

/// Seconds until the allowance covers one more whole job, or `None` when the
/// pace is not what is holding work back.
///
/// Used for the refusal's `Retry-After`: an answer that names no instant leaves
/// the caller retrying into the same line.
pub fn wait_secs(pace: &LlmWindowPace, rate_per_sec: f64) -> Option<u64> {
    if !pace.blocked() || rate_per_sec <= 0.0 {
        return None;
    }
    let need = pace
        .spent_tokens
        .saturating_add(pace.reserve_tokens)
        .saturating_sub(pace.allowance_tokens);
    Some((need as f64 / rate_per_sec).ceil().max(1.0) as u64)
}

/// The rate a [`MeasuredWindow`] hands out allowance at, tokens per second.
pub fn allowance_rate(capacity_tokens: u64, window_secs: u64) -> f64 {
    if window_secs == 0 {
        0.0
    } else {
        capacity_tokens as f64 / window_secs as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400;

    fn month() -> MeasuredWindow {
        MeasuredWindow {
            capacity_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            burned_at_unix: 1_000_000,
        }
    }

    #[test]
    fn the_allowance_is_the_capacity_spread_over_the_window() {
        let m = month();
        assert_eq!(m.allowance_at(1_000_000), 0, "a window starts with nothing");
        assert_eq!(m.allowance_at(1_000_000 + 10 * DAY), 10_000_000);
        assert_eq!(m.allowance_at(1_000_000 + 20 * DAY), 20_000_000);
        // One second before the window ends, essentially the whole capacity is
        // available -- and never more: the last second is not rounded up.
        let last_second = (30 * DAY) as u64 - 1;
        assert_eq!(
            m.allowance_at(1_000_000 + 30 * DAY - 1),
            (30_000_000u64 * last_second) / (30 * DAY) as u64
        );
    }

    #[test]
    fn the_window_rolls_over_and_the_clock_restarts() {
        let m = month();
        let start = m.window_start_at(1_000_000 + 30 * DAY + 5);
        assert_eq!(start, 1_000_000 + 30 * DAY);
        // Five seconds into the second window is 5/30d of the capacity, which
        // rounds down to nothing -- the line restarts at zero and climbs.
        assert_eq!(
            m.allowance_at(1_000_000 + 30 * DAY + 5),
            (30_000_000u64 * 5) / 2_592_000
        );
        assert_eq!(m.allowance_at(1_000_000 + 45 * DAY), 15_000_000);
    }

    #[test]
    fn an_unmeasured_window_paces_nothing() {
        let mut m = month();
        m.window_secs = 0;
        assert_eq!(m.allowance_at(1_000_000 + DAY), 0);
        assert_eq!(m.window_start_at(1_000_000 + DAY), m.burned_at_unix);
        assert!(!m.is_usable());
    }

    #[test]
    fn a_job_is_admitted_as_a_whole_or_held() {
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 1_000_000,
            spent_tokens: 800_000,
            reserve_tokens: 300_000,
            window_secs: 30 * DAY as u64,
            window_start_unix: 0,
        };
        assert!(
            pace.blocked(),
            "200k of allowance cannot fit a 300k job, and admitting it strands half of it"
        );
        let ready = LlmWindowPace {
            allowance_tokens: 1_100_000,
            ..pace
        };
        assert!(!ready.blocked());
        // With no measured job size the line alone decides.
        let unreserved = LlmWindowPace {
            reserve_tokens: 0,
            ..pace
        };
        assert!(!unreserved.blocked());
    }

    #[test]
    fn an_unpaced_pool_never_blocks() {
        let pace = LlmWindowPace {
            spent_tokens: 10,
            allowance_tokens: 0,
            ..Default::default()
        };
        assert!(!pace.blocked(), "no measurement is not a zero allowance");
    }

    #[test]
    fn the_wait_covers_the_shortfall_at_the_window_rate() {
        let m = month();
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 10_000_000,
            spent_tokens: 10_200_000,
            reserve_tokens: 300_000,
            window_secs: m.window_secs,
            window_start_unix: m.burned_at_unix,
        };
        let rate = allowance_rate(m.capacity_tokens, m.window_secs);
        let wait = wait_secs(&pace, rate).expect("blocked, so it names a wait");
        // 500k short at 1M/day is about half a day.
        assert!((wait as i64 - DAY / 2).abs() <= 5, "waited {wait}s");
    }
}
