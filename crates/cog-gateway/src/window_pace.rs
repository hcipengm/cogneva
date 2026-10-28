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
/// ended.
///
/// `burned_at_unix` is both the end of the window that was spent and the start
/// of the next one. It comes from an observation of the boundary — the instant
/// the upstream names, or the first call that succeeds after the wall — never
/// from the refusal itself: refusing says the allowance ran out, which can
/// happen on the window's first day.
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
    /// burn reads the burn itself: `elapsed` is clamped at zero rather than
    /// divided while still negative, which would put the start after `now_unix`
    /// -- an allowance for a window that has not begun.
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
    /// Latest instant serving resumed after a spent-cycle refusal.
    ///
    /// The observation for upstreams that name no reset instant: a call cannot
    /// succeed while the window is spent, so a resume bounds the instant the
    /// quota returned from above, and like every reset it is a window start —
    /// windows repeat from it. It is not the newest success, which may be days
    /// old and says only where serving stopped. It may precede the most recent
    /// refusal: the quota can be spent again without the window rolling, and
    /// the previous cycle's resume is then what anchors the phase.
    pub last_resume_unix: Option<i64>,
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
/// Three observations can name the boundary, in order of preference:
/// - the instant an upstream named in its refusal, which is exact;
/// - the latest successful call that resumed serving after a wall, which bounds
///   the instant that quota returned from above (a call cannot succeed while
///   the window is spent); the error is the retry delay, and the instant is a
///   window start, so the phase rolls forward from it even across later walls;
/// - a live reading that says the outstanding refusal is a spent cycle and
///   names when the quota returns.
///
/// The refusal instant is **not** one of them. It marks where the allowance ran
/// out, which for a seven-day window can be its first day: taking it as the
/// boundary banks the idle days before the real reset as elapsed window, and
/// then hands out that much allowance the moment the quota returns — measured
/// on 2026-10-19, 5.9 of 7 days were banked that way.
///
/// The newest successful call is not one of them either. It marks where serving
/// stopped, which is equally true of an upstream that was throttled and of one
/// nobody called: measured on 2026-10-17, a window whose last success was six
/// days old reported that 84% of its capacity was still unspent a moment
/// before the quota really reset, and pacing against that reading would replay
/// the failure this module exists to prevent — spending a whole window in a
/// day.
pub fn window_end(evidence: WindowEvidence) -> Option<i64> {
    window_boundary(evidence).map(|(end, _)| end)
}

/// Which observation named a window boundary.
///
/// Kept beside the instant because the three are not equally strong, and a
/// reader that sees only the instant cannot tell a phase anchored on the
/// upstream's own promise from one anchored on a resume that bounds it from
/// above by a retry delay. A stored measurement that does not say which is a
/// number with no provenance: it can be repeated, not audited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundarySource {
    /// The instant an upstream named in its refusal: exact, if the refusal is
    /// this cycle's.
    NamedReset,
    /// The first successful call after a wall: bounds the reset from above by
    /// the retry delay.
    LastResume,
    /// The live health reading said the outstanding refusal is a spent cycle
    /// and named when the quota returns.
    ObservedReset,
}

impl BoundarySource {
    /// The spelling stored in `gateway_llm_window.source`.
    pub fn as_str(self) -> &'static str {
        match self {
            BoundarySource::NamedReset => "named_reset",
            BoundarySource::LastResume => "last_resume",
            BoundarySource::ObservedReset => "observed_reset",
        }
    }
}

/// The boundary [`window_end`] names, with the observation it came from.
pub fn window_boundary(evidence: WindowEvidence) -> Option<(i64, BoundarySource)> {
    if let Some(wall) = evidence.last_quota_refusal_unix {
        // The refusal says the allowance ran out; it does not say the window
        // ended there. When the upstream also names the reset instant, that
        // instant is the boundary and the refusal is only where spending
        // stopped. A reset outside `(wall, wall + window)` cannot belong to
        // this refusal — it is a leftover from another window or a parse
        // accident — so it is not used.
        if let Some(reset) = evidence.quota_reset_unix {
            if reset > wall && (reset - wall) < evidence.window_secs as i64 {
                return Some((reset, BoundarySource::NamedReset));
            }
        }
        // No named instant: the latest resume is the observation that remains.
        // Serving resumes only once the quota is back, so a resume bounds the
        // reset from above, within one retry delay of it, and as a window start
        // it anchors the phase — windows repeat from it. It need not follow
        // *this* wall: the allowance can be spent again in a later window
        // without serving resuming since, and the previous cycle's resume is
        // then the only boundary ever observed. Anchoring late errs toward a
        // smaller allowance, which is the safe direction.
        //
        // Two cycles old is not an anchor any more: a local ruler (five hours)
        // whose phase is fixed by the first call after a reset would have been
        // re-anchored many times over, and keeping a stale instant would hand
        // out a ramp on a boundary nobody observed. Past that age the caller
        // keeps what it last measured instead.
        if let Some(resumed) = evidence.last_resume_unix {
            let window = evidence.window_secs as i64;
            if resumed <= evidence.now_unix
                && window > 0
                && evidence.now_unix - resumed < 2 * window
            {
                return Some((resumed, BoundarySource::LastResume));
            }
        }
        // Neither observed: the boundary is unknown, and saying otherwise is
        // what starts the next window at the instant the allowance ran out.
        // The caller keeps whatever measurement it already had.
        return None;
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
                return Some((reset, BoundarySource::ObservedReset));
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
    measured_window_with_boundary(evidence, capacity_tokens, latched).map(|(measured, _)| measured)
}

/// [`measured_window`], also reporting which observation the boundary rests on.
///
/// The source is `None` exactly when the fresh observation was not usable and
/// the earlier measurement is being carried: a carried window keeps the
/// provenance recorded when it was measured, so a caller must not overwrite
/// that row's source from this call.
pub fn measured_window_with_boundary(
    evidence: WindowEvidence,
    capacity_tokens: u64,
    latched: Option<MeasuredWindow>,
) -> Option<(MeasuredWindow, Option<BoundarySource>)> {
    let Some((end, source)) = window_boundary(evidence) else {
        return latched.map(|measured| (measured, None));
    };
    let fresh = MeasuredWindow {
        capacity_tokens,
        window_secs: evidence.window_secs,
        burned_at_unix: end,
    };
    if fresh.is_usable() {
        Some((fresh, Some(source)))
    } else {
        latched.map(|measured| (measured, None))
    }
}

/// Seconds until the allowance covers one more whole job, or `None` when the
/// pace is not what is holding work back.
///
/// Used for the refusal's `Retry-After`: an answer that names no instant leaves
/// the caller retrying into the same line.
///
/// The allowance is a **bounded** ramp, not a rate that keeps accruing from
/// wherever the shortfall is measured. The difference is invisible while the
/// window still has room and decisive exactly when it has none: with the whole
/// capacity spent, the shortfall against the live allowance keeps answering
/// "one job's worth of ramp", which is the steady-state admission interval and
/// never arrives, while the only instant that serves the caller is after the
/// window resets. A caller that honours the first answer comes back roughly
/// once per steady-state interval and is refused each time.
pub fn wait_secs(pace: &LlmWindowPace, rate_per_sec: f64, now_unix: i64) -> Option<u64> {
    if !pace.blocked() || rate_per_sec <= 0.0 {
        return None;
    }
    let need = pace.spent_tokens.saturating_add(pace.reserve_tokens);
    let window_secs = pace.window_secs as i64;
    let start = pace.window_start_unix;

    // The slope and span together are the ramp: their product is the capacity
    // it climbs to, which is the height the target has to fit under.
    let capacity = rate_per_sec * window_secs as f64;
    if start > 0 && window_secs > 0 && capacity > 0.0 {
        if need as f64 <= capacity {
            // One more whole job fits the window: solve the ramp for the
            // instant it covers `need`, not for a shortfall measured from now.
            //
            // A reach already behind `now` is not a defect: `blocked()` was
            // decided from a published pace snapshot, and time kept moving --
            // the ramp has since covered the need, so the truthful answer is
            // "now" (one second), not a wait invented to look deliberate. The
            // next decision re-reads the pace either way.
            let reach = start as f64 + window_secs as f64 * need as f64 / capacity;
            return Some((reach - now_unix as f64).ceil().max(1.0) as u64);
        }
        // The window cannot fit one more whole job before it resets, so no
        // instant inside it serves the caller. The next window starts at zero
        // and has to accrue the reserve before that job fits; the answer is
        // the boundary plus that ramp, not the shortfall against a ramp which
        // has already topped out.
        //
        // A start still in the future is itself the next boundary: the pace
        // anchors the current window at the named reset, so its clock has not
        // begun. Adding a whole window to that instant would name a boundary a
        // cycle too late.
        let reset = if start >= now_unix {
            start as f64
        } else {
            (start + window_secs) as f64
        };
        let resume = reset + pace.reserve_tokens as f64 / rate_per_sec;
        return Some((resume - now_unix as f64).ceil().max(1.0) as u64);
    }

    // No window start was published, so the ramp's origin is unknown and the
    // shortfall against the live allowance is the only reading left.
    let shortfall = need.saturating_sub(pace.allowance_tokens);
    Some((shortfall as f64 / rate_per_sec).ceil().max(1.0) as u64)
}

/// The rate a [`MeasuredWindow`] hands out allowance at, tokens per second.
pub fn allowance_rate(capacity_tokens: u64, window_secs: u64) -> f64 {
    if window_secs == 0 {
        0.0
    } else {
        capacity_tokens as f64 / window_secs as f64
    }
}

/// Whether one pool member's allowance went into the pool sum this beat, and if
/// not, which measurement was missing.
///
/// The pool gauges (`llm_pool_allowance_tokens`, `llm_pool_spent_tokens`,
/// `llm_pool_paced_upstreams`) add up the members that were **measured**, and
/// the request path's `LlmWindowPace::blocked` decides for the whole pool from
/// that sum. So a member with no ruler contributes zero, and on every one of
/// those gauges a contribution of zero is the same bytes as "this member has no
/// allowance left".
///
/// Those two call for opposite repairs. A spent window is waited out; a missing
/// ruler is given one, and no amount of waiting produces it. The per-member
/// pacing table already treats a member with no ruler as "not paced here",
/// which is the fail-open direction; the pool report is fail-closed on the same
/// absence, and until this verdict is published nothing on the reading side
/// says which members are on which side of that.
///
/// The closed set is about *which measurement is missing*, not about how bad
/// the state is: the three missing cases are repaired in three different
/// places, and a single "no ruler" would send every reader to the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ruler {
    /// Measured: this member's allowance and spend are in the pool sum.
    Measured,
    /// The upstream has never stated a quota window length, so there is
    /// nothing to spread a capacity over. Giving it a ruler means getting that
    /// length out of it, not waiting.
    NoWindowReported,
    /// The length is known but the capacity is not: the ledger holds no
    /// settled burn for this member and no live observation of a boundary
    /// pins one. Waiting produces the ledger row only if the upstream refuses.
    WindowUnmeasurable,
    /// The window is measured but this window's spend could not be read. The
    /// ruler exists; the ledger query is what has to come back.
    SpendUnreadable,
    /// There is no usage ledger to read at all, so nothing can be paced. Not a
    /// property of any member -- the whole pool is in this state together --
    /// but it is reported per member so that the count below has a population.
    NoUsageLedger,
}

impl Ruler {
    /// The value of the `reason` label. A closed set of five, one per variant.
    pub fn label(self) -> &'static str {
        match self {
            Ruler::Measured => "measured",
            Ruler::NoWindowReported => "no_window_reported",
            Ruler::WindowUnmeasurable => "window_unmeasurable",
            Ruler::SpendUnreadable => "spend_unreadable",
            Ruler::NoUsageLedger => "no_usage_ledger",
        }
    }

    /// Whether this member's allowance is in the pool sum.
    pub fn contributes(self) -> bool {
        matches!(self, Ruler::Measured)
    }
}

/// How many pool members contributed nothing to the pool sum this beat.
///
/// Published as its own gauge rather than left to `pool_size - paced`: the pool
/// size is not on the metric face, so the subtraction needs a number nothing
/// publishes. This is that number, and it is the one that says "the sum the
/// request path refuses on is smaller than the pool it stands for".
pub fn upstreams_without_ruler(rulers: impl IntoIterator<Item = Ruler>) -> usize {
    rulers
        .into_iter()
        .filter(|ruler| !ruler.contributes())
        .count()
}

/// The two series this verdict is published as are registered in
/// `cog_core::metric_names` (`LLM_POOL_UPSTREAM_WITH_RULER` and
/// `LLM_POOL_UPSTREAMS_WITHOUT_RULER`) rather than declared here: the gateway
/// records gauges through a registered name, because a series whose name lives
/// only at a call site is one nothing else in the workspace can find.
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
            reserve_population: 7,
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
            reserve_population: 7,
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
    fn a_refusal_alone_names_no_boundary() {
        // The wall says the allowance ran out, not that the cycle ended. With
        // no reset named and no success since, the boundary is unknown and the
        // caller keeps whatever it measured before.
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_ok_unix: Some(1_999_000),
            quota_reset_unix: None,
            burned_window_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), None);
        assert_eq!(measured_window(evidence, 30_000_000, None), None);
    }

    #[test]
    fn the_boundary_says_which_observation_named_it() {
        // The instant alone cannot tell an exact promise from one bounded from
        // above by a resume, and a stored row that reports only the instant is
        // a number nobody can audit.
        let named = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            quota_reset_unix: Some(2_000_000 + 3 * DAY),
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        assert_eq!(
            window_boundary(named),
            Some((2_000_000 + 3 * DAY, BoundarySource::NamedReset))
        );

        let resumed = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_resume_unix: Some(2_001_000),
            window_secs: (30 * DAY) as u64,
            now_unix: 2_002_000,
            ..Default::default()
        };
        assert_eq!(
            window_boundary(resumed),
            Some((2_001_000, BoundarySource::LastResume))
        );

        let live = WindowEvidence {
            quota_cycle_observed: true,
            quota_reset_unix: Some(5_000_000 + DAY),
            window_secs: (7 * DAY) as u64,
            now_unix: 5_000_000,
            ..Default::default()
        };
        assert_eq!(
            window_boundary(live),
            Some((5_000_000 + DAY, BoundarySource::ObservedReset))
        );
    }

    #[test]
    fn a_carried_window_keeps_the_provenance_it_was_measured_with() {
        // Nothing was observed this pass, so the earlier measurement stands and
        // no source may be written over the one it was stored with.
        let earlier = month();
        let nothing_observed = WindowEvidence {
            window_secs: (30 * DAY) as u64,
            ..Default::default()
        };
        let (measured, source) = measured_window_with_boundary(nothing_observed, 0, Some(earlier))
            .expect("the earlier measurement still paces");
        assert_eq!(measured, earlier);
        assert_eq!(source, None);
    }

    #[test]
    fn a_resume_bounds_the_reset_and_anchors_the_phase() {
        // Serving cannot resume while the window is spent, so the first success
        // after a wall is the only observation of the reset an upstream that
        // names no instant leaves behind. 2026-10-19 live shape: wall at
        // 10-19 11:06Z, real reset at 10-25 03:13Z.
        let wall = 2_000_000;
        let resumed = wall + 5 * DAY + 16 * 3_600;
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(wall),
            last_ok_unix: Some(resumed),
            quota_reset_unix: None,
            last_resume_unix: Some(resumed),
            burned_window_tokens: 17_284_171,
            window_secs: (7 * DAY) as u64,
            now_unix: resumed + 3 * DAY,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), Some(resumed));
        let measured = measured_window(evidence, 17_284_171, None).expect("a resume is evidence");
        assert_eq!(measured.burned_at_unix, resumed);
        // The new window starts empty, not with the days between exhaustion and
        // the reset banked as elapsed time.
        assert_eq!(measured.allowance_at(resumed), 0);
        assert_eq!(measured.allowance_at(resumed + 7 * DAY / 2), 17_284_171 / 2);
    }

    #[test]
    fn a_previous_cycles_resume_still_anchors_the_phase() {
        // The quota can be spent again in the window a resume opened, and then
        // the most recent wall has no success after it. Dropping the earlier
        // resume would leave the boundary unobserved for a whole week, and the
        // stored measurement would keep the wall as the window start -- the
        // very defect. Windows repeat from the resume, so it still anchors.
        let resumed = 2_000_000;
        let wall = resumed + DAY; // spent on the new window's first day
        let now = resumed + 2 * DAY;
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(wall),
            last_ok_unix: Some(wall - 3_600),
            quota_reset_unix: None,
            last_resume_unix: Some(resumed),
            burned_window_tokens: 17_284_171,
            window_secs: (7 * DAY) as u64,
            now_unix: now,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), Some(resumed));
        let measured = measured_window(evidence, 17_284_171, None).expect("a resume is evidence");
        assert_eq!(measured.burned_at_unix, resumed);
        assert_eq!(measured.window_start_at(now), resumed);
        // Two days into the window the allowance is two sevenths, not the
        // fifth the wall would have implied.
        assert_eq!(measured.allowance_at(now), 17_284_171 * 2 / 7);
        // And it rolls forward at the real reset rather than reopening there.
        assert_eq!(
            measured.window_start_at(resumed + 7 * DAY),
            resumed + 7 * DAY
        );
        assert_eq!(measured.allowance_at(resumed + 7 * DAY), 0);
    }

    #[test]
    fn a_resume_two_cycles_old_is_not_an_anchor() {
        // The five-hour ruler restarts with the first call after each reset, so
        // a resume from a month ago says nothing about where this window began;
        // a ramp computed from it would be an allowance nobody observed.
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_resume_unix: Some(2_000_000 - 30 * DAY),
            window_secs: (5 * 3_600) as u64,
            now_unix: 2_000_000,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), None);
    }

    #[test]
    fn a_resume_after_now_is_not_evidence() {
        // A resume is by construction in the past; a future one is a bad
        // reading, and anchoring the phase to it would hand out an allowance
        // that has not been earned.
        let evidence = WindowEvidence {
            last_quota_refusal_unix: Some(2_000_000),
            last_resume_unix: Some(2_000_000 + 3 * DAY),
            window_secs: (7 * DAY) as u64,
            now_unix: 2_000_000 + DAY,
            ..Default::default()
        };
        assert_eq!(window_end(evidence), None);
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
        assert_eq!(
            window_end(base),
            None,
            "a reset beyond the window it claims to end is left over from another one, and the wall is not a substitute"
        );
        assert_eq!(measured_window(base, 30_000_000, None), None);
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
            last_resume_unix: None,
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
            last_resume_unix: None,
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
            reserve_population: 7,
            window_secs: m.window_secs,
            window_start_unix: m.burned_at_unix,
        };
        let rate = allowance_rate(m.capacity_tokens, m.window_secs);
        let now = m.burned_at_unix + 10 * DAY;
        let wait = wait_secs(&pace, rate, now).expect("blocked, so it names a wait");
        // 500k short at 1M/day is about half a day.
        assert!((wait as i64 - DAY / 2).abs() <= 5, "waited {wait}s");
    }

    #[test]
    fn a_spent_window_waits_past_its_reset_and_the_next_ramp() {
        let m = month();
        // The whole capacity is spent: the shortfall against the live allowance
        // is one job's worth, which is also the steady-state admission interval
        // -- exactly the number that never becomes true.
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 30_000_000,
            spent_tokens: 30_000_000,
            reserve_tokens: 1_000_000,
            reserve_population: 7,
            window_secs: m.window_secs,
            window_start_unix: m.burned_at_unix,
        };
        let rate = allowance_rate(m.capacity_tokens, m.window_secs);
        let now = m.burned_at_unix + 10 * DAY;
        let wait = wait_secs(&pace, rate, now).expect("blocked, so it names a wait");
        // 20 days to the boundary, then one more day for the reserve to accrue.
        let expected = 21 * DAY;
        assert!(
            (wait as i64 - expected).abs() <= 5,
            "a spent window must wait for the reset plus the next ramp: waited {wait}s"
        );

        // The shortfall reading would have said one day, and a caller honouring
        // that comes back into the same wall ~21 times before it can serve.
        let shortfall =
            (pace.spent_tokens + pace.reserve_tokens - pace.allowance_tokens) as f64 / rate;
        assert!(
            shortfall.ceil() as i64 <= DAY,
            "the old reading was {shortfall}s"
        );
    }

    /// 2026-10-18 的现场数字，整组钉住：上一窗烧穿（138,762,822 / 138,762,822），
    /// 边界 1,792,339,200 在 11.5 小时后，reserve 570,831。
    ///
    /// 旧算式按「短差 ÷ 速率」报 2,488 秒（41 分钟，那正是稳态放行间隔），与
    /// 真实可入账时刻差 17 倍。新算式报「到边界的 41,549 秒 + 新窗斜坡补满一件
    /// 任务的 2,488 秒」= 44,037 秒。边界那一刻的 allowance 恰好是 0，提前一秒
    /// 回来照样被拒——答案是边界加斜坡，不是边界本身。
    #[test]
    fn the_live_burned_window_names_the_boundary_plus_the_ramp() {
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 0,
            spent_tokens: 0,
            reserve_tokens: 570_831,
            reserve_population: 7,
            window_secs: 604_800,
            window_start_unix: 1_792_339_200,
        };
        let rate = allowance_rate(138_762_822, 604_800);
        assert_eq!(wait_secs(&pace, rate, 1_792_297_651), Some(44_037));
    }

    #[test]
    fn a_window_that_still_fits_reports_the_ramp_and_not_the_reset() {
        let m = month();
        // Room for one more whole job, but not right now: the ramp reaches the
        // target in the middle of the window, so the answer is that instant --
        // not the boundary.
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 10_000_000,
            spent_tokens: 10_000_000,
            reserve_tokens: 1_000_000,
            reserve_population: 7,
            window_secs: m.window_secs,
            window_start_unix: m.burned_at_unix,
        };
        let rate = allowance_rate(m.capacity_tokens, m.window_secs);
        let now = m.burned_at_unix + 10 * DAY;
        let wait = wait_secs(&pace, rate, now).expect("blocked, so it names a wait");
        assert!(
            (wait as i64 - DAY).abs() <= 5,
            "one day of ramp, not the twenty left in the window: waited {wait}s"
        );
    }

    #[test]
    fn without_a_published_window_start_the_shortfall_is_the_only_reading() {
        let m = month();
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 10_000_000,
            spent_tokens: 10_200_000,
            reserve_tokens: 300_000,
            reserve_population: 7,
            window_secs: m.window_secs,
            window_start_unix: 0,
        };
        let rate = allowance_rate(m.capacity_tokens, m.window_secs);
        let wait = wait_secs(&pace, rate, m.burned_at_unix).expect("blocked, so it names a wait");
        assert!((wait as i64 - DAY / 2).abs() <= 5, "waited {wait}s");
    }

    /// 池和只加进量到窗口的成员，而请求路的 `blocked()` 拿这个和判整池：一个没
    /// 有尺子的成员在读数上与「额度为零」同形。这条守住「同形」被拆开——没进和
    /// 的成员必须被数出来，而进和的那一个不能被算成缺席。
    #[test]
    fn a_member_without_a_ruler_is_counted_and_the_measured_one_is_not() {
        let verdicts = [
            Ruler::Measured,
            Ruler::NoWindowReported,
            Ruler::WindowUnmeasurable,
            Ruler::SpendUnreadable,
        ];
        assert_eq!(upstreams_without_ruler(verdicts), 3);
        // 全都有尺子时是 0 而不是「没有这一族」：0 也是一个要被印出来的答案，
        // 而缺席只在生产者没跑时出现。
        assert_eq!(upstreams_without_ruler([Ruler::Measured]), 0);
        assert_eq!(upstreams_without_ruler([]), 0);
    }

    /// 五个值两张脸必须一一对上：`contributes()` 是 `1`／`0` 的取值，
    /// `label()` 是 `reason` 标签。两者错开一位，读数上就会出现一条「值为 0 而
    /// reason 说 measured」的序列，而那正是这一族要拆开的那个混淆。
    #[test]
    fn the_verdict_label_and_the_value_cannot_disagree() {
        for ruler in [
            Ruler::Measured,
            Ruler::NoWindowReported,
            Ruler::WindowUnmeasurable,
            Ruler::SpendUnreadable,
            Ruler::NoUsageLedger,
        ] {
            assert_eq!(
                ruler.label() == "measured",
                ruler.contributes(),
                "{ruler:?} 的标签与贡献方向对不上"
            );
        }
    }

    /// 没配用量账本时整池一起没有尺子，而原因与「成员没自报窗口」不同：前者的
    /// 修法是配账本，后者是去要窗口长度。合成一个值会把两种修法混成一种。
    #[test]
    fn no_ledger_is_its_own_reason_not_a_member_without_a_window() {
        assert_ne!(
            Ruler::NoUsageLedger.label(),
            Ruler::NoWindowReported.label()
        );
        assert!(!Ruler::NoUsageLedger.contributes());
    }
}
