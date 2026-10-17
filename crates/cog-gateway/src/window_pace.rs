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

/// What the process knows about one upstream's last window.
#[derive(Debug, Clone, Copy, Default)]
pub struct WindowEvidence {
    /// When the upstream last answered "this cycle's limit is reached", as the
    /// ledger's own classification records it.
    pub last_quota_refusal_unix: Option<i64>,
    /// Whether the **live** health reading says the outstanding refusal is a
    /// spent cycle, and when the upstream said its quota returns.
    ///
    /// A refusal is an observation even when the ledger cannot name its cause:
    /// rows written before the cause column existed still mark the moment the
    /// allowance ran out, and the upstream names the reset in the refusal
    /// itself. This is the same observation as `last_quota_refusal_unix`, read
    /// from the live evidence rather than from a stored row, and it is what
    /// lets pacing start before the next window is spent instead of after.
    pub quota_cycle_observed: bool,
    /// When the upstream said its quota returns, when the refusal named it.
    ///
    /// This, not the refusal, is the window's end: an allowance can run out on
    /// the first day of a seven-day window, so the refusal marks the exhaustion
    /// and the reset marks the boundary.
    pub quota_reset_unix: Option<i64>,
    /// When the upstream last served a call. Recorded here so the rule below
    /// can be stated against it: this instant is **not** a window boundary.
    pub last_ok_unix: Option<i64>,
    /// Tokens served in the window that ended at `last_quota_refusal_unix`.
    /// Unused when the boundary comes from the live reading; the caller sums
    /// the named window instead.
    pub burned_window_tokens: u64,
    /// The upstream's own statement of its window length.
    pub window_secs: u64,
    /// The instant the decision is made at, used only to reject a named reset
    /// that cannot belong to the refusal in force.
    pub now_unix: i64,
}

/// When the window this upstream last spent ended, if anything observed it.
///
/// Two observations are equally good and neither is a guess:
/// - a ledger row the classification already names as a spent cycle, which is
///   durable and survives restarts;
/// - a live reading that says the outstanding refusal is a spent cycle and
///   names when the quota returns.
///
/// The newest successful call is not one of them. It marks where serving
/// stopped, which is equally true of an upstream that was throttled and of one
/// nobody called: measured on 2026-10-17, a window whose last success was six
/// days old reported that 84% of its capacity was still unspent a moment
/// before the quota really reset, and pacing against that reading would replay
/// the failure this module exists to prevent — spending a whole window in a
/// day.
pub fn window_end(evidence: WindowEvidence) -> Option<i64> {
    if let Some(wall) = evidence.last_quota_refusal_unix {
        // The refusal says the allowance ran out; it does not say the window
        // ended there. When the upstream also names the reset instant, that
        // instant is the boundary and the refusal is only where spending
        // stopped. A reset outside `(wall, wall + window)` cannot belong to
        // this refusal — it is a leftover from another window or a parse
        // accident — so it is not used.
        return Some(match evidence.quota_reset_unix {
            Some(reset) if reset > wall && (reset - wall) < evidence.window_secs as i64 => reset,
            _ => wall,
        });
    }
    if evidence.quota_cycle_observed {
        // Without a classified row, the live reading is the only witness to the
        // wall, and the reset it names is the boundary. The plausibility guard
        // is one window either side of now: a reset must be near enough to be
        // this cycle's, and a refusal that has already reset is still this
        // window's boundary (the phase rolls forward from it).
        let window_secs = evidence.window_secs as i64;
        if let Some(reset) = evidence.quota_reset_unix {
            if window_secs > 0
                && reset > evidence.now_unix - window_secs
                && reset <= evidence.now_unix + window_secs
            {
                return Some(reset);
            }
        }
    }
    None
}

/// The measurement that may pace this upstream, if any.
///
/// `capacity_tokens` must be what the ledger served in the window
/// [`window_end`] names: around the classified wall row when there is one
/// (that row is the only burn evidence the ledger itself carries), otherwise
/// summed between `end - window` and `end`. A fresh observation wins; without
/// one, the window measured earlier still stands; without either, this
/// upstream is left unpaced rather than paced against a window nobody
/// observed.
pub fn measured_window(
    evidence: WindowEvidence,
    capacity_tokens: u64,
    latched: Option<MeasuredWindow>,
) -> Option<MeasuredWindow> {
    let Some(end) = window_end(evidence) else {
        return latched;
    };
    let fresh = MeasuredWindow {
        capacity_tokens,
        window_secs: evidence.window_secs,
        burned_at_unix: end,
    };
    if fresh.is_usable() {
        Some(fresh)
    } else {
        latched
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
    fn a_window_opens_on_a_measured_quota_wall() {
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_ok_unix: Some(1_999_000),
            quota_reset_unix: None,
            burned_window_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        let measured =
            measured_window(evidence, 30_000_000, None).expect("a refusal is a measurement");
        assert_eq!(measured.burned_at_unix, 2_000_000);
        assert_eq!(measured.capacity_tokens, 30_000_000);
    }

    #[test]
    fn the_named_reset_is_the_boundary_the_refusal_is_not() {
        // 2026-10-17 live shape: an upstream exhausted its seven-day allowance
        // days before the cycle ended. Anchoring the window at the refusal
        // would report most of the next window already elapsed the moment the
        // quota returned -- an allowance nobody measured.
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_ok_unix: Some(1_999_000),
            quota_reset_unix: Some(2_000_000 + 3 * DAY),
            burned_window_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        let measured =
            measured_window(evidence, 30_000_000, None).expect("a refusal is a measurement");
        assert_eq!(
            measured.burned_at_unix,
            2_000_000 + 3 * DAY,
            "the window ends where the upstream said it does"
        );
        // At the reset the next window starts with nothing spent, not with the
        // three days between exhaustion and reset already banked.
        assert_eq!(measured.allowance_at(2_000_000 + 3 * DAY), 0);
        assert_eq!(
            measured.allowance_at(2_000_000 + 3 * DAY + 15 * DAY),
            15_000_000
        );
    }

    #[test]
    fn a_reset_that_cannot_belong_to_this_refusal_is_not_used() {
        let base = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_ok_unix: None,
            quota_reset_unix: Some(2_000_000 + 40 * DAY),
            burned_window_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        let measured = measured_window(base, 30_000_000, None).expect("a refusal is a measurement");
        assert_eq!(
            measured.burned_at_unix, 2_000_000,
            "a reset beyond the window it claims to end is left over from another one"
        );
    }

    #[test]
    fn the_last_successful_call_is_not_a_window_boundary() {
        // The shape that was live on 2026-10-17: the newest success was six
        // days old, so pacing from it reported 84% of the window still unspent
        // an instant before the quota actually reset. Nothing was measured, so
        // nothing may be paced.
        let evidence = WindowEvidence {
            last_quota_refusal_unix: None,
            last_ok_unix: Some(1_000_000),
            quota_reset_unix: None,
            burned_window_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        assert_eq!(measured_window(evidence, 30_000_000, None), None);
    }

    #[test]
    fn a_live_quota_cycle_reading_opens_the_window_at_the_reset_it_names() {
        // The shape the ledger cannot store yet: the refusal rows predate the
        // cause column, so the only witness to the wall is the health reading.
        // Ignoring it leaves the whole next window unpaced -- the burst the
        // pacer exists to prevent happens before the wall it would pace from.
        let now = 5_000_000;
        let evidence = WindowEvidence {
            last_quota_refusal_unix: None,
            last_ok_unix: None,
            quota_cycle_observed: true,
            quota_reset_unix: Some(now + DAY),
            burned_window_tokens: 0,
            window_secs: (7 * DAY) as u64,
            now_unix: now,
        };
        assert_eq!(window_end(evidence), Some(now + DAY));
        let measured = measured_window(evidence, 138_000_000, None).expect("live evidence counts");
        assert_eq!(measured.burned_at_unix, now + DAY);
        assert_eq!(measured.allowance_at(now + DAY), 0);
        assert_eq!(measured.allowance_at(now + DAY + 7 * DAY / 2), 69_000_000);
    }

    #[test]
    fn a_live_entitlement_refusal_opens_no_window() {
        // Entitlement never resets, so pacing it would spread quota that will
        // never arrive; a reset named beside it is not this window's end.
        let now = 5_000_000;
        let evidence = WindowEvidence {
            quota_cycle_observed: false,
            quota_reset_unix: Some(now + DAY),
            window_secs: (7 * DAY) as u64,
            now_unix: now,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), None);
    }

    #[test]
    fn a_reset_more_than_a_window_away_is_not_this_refusals_boundary() {
        let now = 5_000_000;
        let evidence = WindowEvidence {
            quota_cycle_observed: true,
            quota_reset_unix: Some(now + 3 * 7 * DAY),
            window_secs: (7 * DAY) as u64,
            now_unix: now,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), None);
    }

    #[test]
    fn the_window_measured_earlier_stands_until_a_fresh_refusal_arrives() {
        let earlier = month();
        let no_refusal = WindowEvidence {
            last_quota_refusal_unix: None,
            last_ok_unix: None,
            quota_reset_unix: None,
            burned_window_tokens: 0,
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        assert_eq!(measured_window(no_refusal, 0, Some(earlier)), Some(earlier));
        // A refusal whose burn could not be summed (the rows it needs were
        // pruned or never landed) is not a usable measurement either; the
        // earlier one is still the best observation available.
        let empty_burn = WindowEvidence {
            last_quota_refusal_unix: Some(3_000_000),
            ..no_refusal
        };
        assert_eq!(measured_window(empty_burn, 0, Some(earlier)), Some(earlier));
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
