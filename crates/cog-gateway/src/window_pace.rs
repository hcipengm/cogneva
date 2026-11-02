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

/// One of an upstream's rulers, weighed against what that ruler has already
/// served.
///
/// A member is not "a window" but "a window plus what went into it", because
/// the question the pool asks — may one more job start? — is answered by the
/// difference, and a ramp without its spend reads as a fresh window forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RulerMember {
    pub measured: MeasuredWindow,
    /// Tokens the upstream served inside this ruler's window.
    pub spent_tokens: u64,
    /// The upstream refused this window more recently than it served it.
    pub exhausted: bool,
}

impl RulerMember {
    /// What this ruler still allows by `now_unix`.
    ///
    /// A window the upstream has said is spent allows nothing, whatever the
    /// ramp still describes: the refusal is the later observation.
    pub fn remaining_tokens(&self, now_unix: i64) -> u64 {
        if self.exhausted {
            return 0;
        }
        self.measured
            .allowance_at(now_unix)
            .saturating_sub(self.spent_tokens)
    }

    /// Whether this measurement still speaks about the window in force.
    ///
    /// Two cycles is the same age limit `window_boundary` puts on a resume
    /// anchor, for the same reason: past it the phase has rolled at least twice
    /// with nobody watching, so the ramp describes windows that already ended.
    pub fn is_current(&self, now_unix: i64) -> bool {
        let window = self.measured.window_secs as i64;
        window > 0 && now_unix.saturating_sub(self.measured.burned_at_unix) < 2 * window
    }
}

/// Which of one upstream's rulers governs this beat.
///
/// An upstream can carry more than one quota window at once — a five-hour burst
/// limit nested inside a seven-day volume limit — and every one of them has to
/// hold. The pool's arithmetic is a sum over **members**, so a member may enter
/// it only once:
///
/// - adding every ruler credits one account several times, and the pool then
///   believes it may spend their sum;
/// - adding only the ruler the newest refusal named makes the bound depend on
///   which wall was hit most recently. Measured 2026-11-01: the seven-day ruler
///   first appeared in the ledger at the instant it was already spent, so a
///   whole day of burn had been paced against the five-hour ruler alone, whose
///   capacity is roughly four days' worth of the weekly quota per day.
///
/// The ruler that governs is the one with the least left after its own spend:
/// it is the one that would refuse first, so satisfying it satisfies the rest.
/// Ties keep the shortest window, which is the order the caller hands them in
/// and also the ruler that refills soonest.
///
/// `reserve_tokens` is what one whole job costs, and it takes a ruler out of
/// the running when that ruler's **capacity** is smaller than it. Such a window
/// can never pay for a job: its allowance ramps from zero to its capacity and
/// no further, so the admission subtraction is negative for the window's whole
/// life and the pool admits nothing until the measurement expires — a bound
/// that cannot be satisfied by waiting, reported in the same words as one that
/// can. Measured 2026-11-02: the five-hour ruler held 190,982 tokens while one
/// job drew 348,009 at the 75th percentile, so against a whole-job reserve that
/// ruler could only ever refuse. A window too small to hold one job is not a
/// budget this gate can honour at job granularity; it is a wall that will be
/// hit and re-measured, which is what the refusal path already does.
///
/// When *no* live ruler can hold a job, the largest one still governs rather
/// than none: with no member in the pool sum the request path refuses on
/// nothing at all, and an unthrottled burn is the failure this module exists to
/// prevent while a refusal that names the budget is at least the truth. Its
/// allowance is the honest best case, and the subtraction stays negative, so
/// the pool reports the budget as the reason instead of inventing headroom.
pub fn governing_ruler(
    members: &[RulerMember],
    now_unix: i64,
    reserve_tokens: u64,
) -> Option<usize> {
    let live = || {
        members
            .iter()
            .enumerate()
            .filter(|(_, member)| member.measured.is_usable() && member.is_current(now_unix))
    };
    let tightest = |(_, a): &(usize, &RulerMember), (_, b): &(usize, &RulerMember)| {
        a.remaining_tokens(now_unix)
            .cmp(&b.remaining_tokens(now_unix))
            .then(a.measured.window_secs.cmp(&b.measured.window_secs))
    };
    live()
        .filter(|(_, member)| member.measured.capacity_tokens >= reserve_tokens)
        .min_by(tightest)
        .or_else(|| live().max_by_key(|(_, member)| member.measured.capacity_tokens))
        .map(|(index, _)| index)
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

/// 一窗被上游当面收走之后，还要多久才可能再放行一件整任务。
///
/// 坡度在这个形状里不作数：它算的是"照已花的量，此刻本可以再花多少"，而上游刚
/// 说过这一窗没有那个量。能答的只有窗口边界——新的一窗从零起，攒够一件整任务的
/// 预留还要 `reserve / rate` 秒。起点还在未来时（复位时刻已经指名、这一窗还没
/// 开始）边界就是那个起点本身，再加一整窗会把复位推到下一个周期去。
///
/// 不这么答的代价不是一句保守的等待：按坡度答，`wait_secs` 会算出"此刻已经攒够"
/// 的一秒，调用方据此立刻重试，围成一个一秒一次的拒绝环。
pub fn wait_until_reopened(pace: &LlmWindowPace, rate_per_sec: f64, now_unix: i64) -> Option<u64> {
    if !pace.blocked() {
        return None;
    }
    let window_secs = pace.window_secs as i64;
    let start = pace.window_start_unix;
    if window_secs <= 0 || start <= 0 {
        // 没有窗长、也没有起点：没有边界可指，沿用坡度那一边的答复。
        return wait_secs(pace, rate_per_sec, now_unix);
    }
    let boundary = if start >= now_unix {
        start
    } else {
        start + window_secs
    };
    let accrue = if rate_per_sec > 0.0 {
        pace.reserve_tokens as f64 / rate_per_sec
    } else {
        0.0
    };
    Some((boundary as f64 + accrue - now_unix as f64).ceil().max(1.0) as u64)
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

/// Folds the measured members into the pool-level report.
///
/// The pool report is a **report**, not a window: each member paces against its
/// own window, and the amounts add up only because the question "how much may
/// this pool spend by now" is answered member by member and then summed. The
/// window, though, is not a sum of anything. Two members on a weekly and a
/// monthly cycle have two starts and two lengths, and a reading that took the
/// earliest start and the longest length would name a window that neither
/// member is in — measured on 2026-10-30: a 7-day member that resumed on
/// 10-26 paired with a 30-day member whose quota returns on 11-04 came out as
/// "one 30-day window starting 10-26", an end 21 days after the only date the
/// pool itself names for the quota's return.
///
/// So the window is carried only when every measured member shares one, and is
/// left unreported (zero) otherwise. The amounts stay: they are still what the
/// members allow between them, and dropping them would hide the pacing that is
/// actually in force. A reader that needs a start or an end then knows it has
/// to ask per member, which is where the answer exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolPace {
    measured_upstreams: usize,
    allowance_tokens: u64,
    spent_tokens: u64,
    /// The window every measured member is in, while they all share one.
    shared_window: Option<(u64, i64)>,
    /// A member contributed a different window than the one seen first.
    mixed_windows: bool,
}

impl PoolPace {
    /// Add one member's measurement and what it has spent in its window.
    pub fn add(&mut self, measured: &MeasuredWindow, spent_tokens: u64, now_unix: i64) {
        self.add_with_allowance(
            measured,
            spent_tokens,
            now_unix,
            measured.allowance_at(now_unix),
        );
    }

    /// Add one member the upstream has just refused for its quota.
    ///
    /// The measurement still says what the ramp *would* pay out, but the
    /// upstream has said in so many words that this window's allowance is gone:
    /// the refusal is the newer observation, so the member enters the sum with
    /// what it spent and **no** allowance.
    ///
    /// Crediting the ramp here is what makes a spent window read as an open
    /// one. The difference is not cosmetic: the pool's headroom
    /// (`allowance - spent - reserve`) is the operand every reader uses to
    /// separate "the budget closed admission" from "the channel did, and the
    /// budget is fine". Measured 2026-11-01: a member refused with
    /// `quota_cycle` while its stored ramp still published 753k tokens of
    /// allowance against 52k spent, so the subtraction came out positive and
    /// named the one repair that could not work.
    pub fn add_exhausted(&mut self, measured: &MeasuredWindow, spent_tokens: u64, now_unix: i64) {
        self.add_with_allowance(measured, spent_tokens, now_unix, 0);
    }

    fn add_with_allowance(
        &mut self,
        measured: &MeasuredWindow,
        spent_tokens: u64,
        now_unix: i64,
        allowance_tokens: u64,
    ) {
        self.measured_upstreams += 1;
        self.allowance_tokens = self.allowance_tokens.saturating_add(allowance_tokens);
        self.spent_tokens = self.spent_tokens.saturating_add(spent_tokens);
        let here = (measured.window_secs, measured.window_start_at(now_unix));
        match self.shared_window {
            None => self.shared_window = Some(here),
            Some(seen) if seen == here => {}
            // Compared as a pair on purpose: two members can start at the same
            // instant with different lengths, or share a length and start at
            // different instants. Either way they are not in one window.
            Some(_) => self.mixed_windows = true,
        }
    }

    /// The report, with the reserve that is shared across the whole pool (one
    /// job's cost does not depend on who serves it).
    pub fn finish(self, reserve_tokens: u64, reserve_population: u64) -> LlmWindowPace {
        let window = if self.mixed_windows {
            None
        } else {
            self.shared_window
        };
        LlmWindowPace {
            measured_upstreams: self.measured_upstreams,
            allowance_tokens: self.allowance_tokens,
            spent_tokens: self.spent_tokens,
            reserve_tokens,
            reserve_population,
            window_secs: window.map(|(secs, _)| secs).unwrap_or(0),
            window_start_unix: window.map(|(_, start)| start).unwrap_or(0),
        }
    }
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
    /// What one whole job costs at the reserve the gateway runs with, so the
    /// capacity filter below is exercised at a realistic size rather than at a
    /// number picked to make both sides of every assertion pass.
    const ONE_JOB: u64 = 1_100_000;

    fn month() -> MeasuredWindow {
        MeasuredWindow {
            capacity_tokens: 30_000_000,
            window_secs: (30 * DAY) as u64,
            burned_at_unix: 1_000_000,
        }
    }

    /// 同一条窗上的两家合成一个窗：起点与窗长都还在，读者算得出窗尾。
    #[test]
    fn members_in_one_window_are_reported_as_that_window() {
        let now = 1_000_000 + 10 * DAY;
        let mut pool = PoolPace::default();
        pool.add(&month(), 1_000_000, now);
        pool.add(&month(), 2_000_000, now);
        let report = pool.finish(500, 5);
        assert_eq!(report.measured_upstreams, 2);
        assert_eq!(report.allowance_tokens, 20_000_000, "两家各摊到一千万");
        assert_eq!(report.spent_tokens, 3_000_000);
        assert_eq!(report.window_secs, (30 * DAY) as u64);
        // 窗还没翻，起点就是上一次烧穿的时刻——两家看到的是同一个起点。
        assert_eq!(report.window_start_unix, 1_000_000);
        assert_eq!(report.reserve_tokens, 500, "整活成本是全池共用的一个量");
    }

    /// 上游当面拒了这一窗的额度之后，这一家进和时不该再带坡度给的允许量。
    ///
    /// 坡度是撞墙**之前**量到的，撞墙是更晚的那次观测；两者对着同一窗说得相反
    /// 时，后说的那个算数。2026-11-01 的现网形状：`allowance` 753k 对 `spent`
    /// 52k，余量为正，于是每一处读余量的地方都说"不是预算拦的"——而这一家刚刚
    /// 用 `quota_cycle` 说了这一窗没了。
    #[test]
    fn a_member_refused_for_quota_enters_the_sum_without_its_ramp() {
        let now = 1_000_000 + 10 * DAY;
        let ramp = MeasuredWindow {
            capacity_tokens: 4_006_951,
            window_secs: (5 * 3_600) as u64,
            burned_at_unix: 1_000_000 + 10 * DAY - 3_600,
        };
        let mut open = PoolPace::default();
        open.add(&ramp, 52_235, now);
        let open = open.finish(358_030, 533);
        assert!(
            open.allowance_tokens > open.spent_tokens + open.reserve_tokens,
            "没撞墙时坡度确实还装得下一件整任务，这一格是下面那条的对照"
        );

        let mut refused = PoolPace::default();
        refused.add_exhausted(&ramp, 52_235, now);
        let refused = refused.finish(358_030, 533);
        assert_eq!(refused.measured_upstreams, 1, "撞墙不等于没有尺子");
        assert_eq!(refused.spent_tokens, 52_235, "花掉多少照实报");
        assert_eq!(refused.allowance_tokens, 0, "这一窗的额度已经被收走");
        assert!(
            refused.allowance_tokens < refused.spent_tokens + refused.reserve_tokens,
            "余量为负正是「预算关的门」，读余量的判据据此才分得开两种停摆"
        );
    }

    /// 窗不一样长时，报表带额度、不带窗：起点与窗长分属两家，拼起来是第三条窗。
    ///
    /// 2026-10-30 的现网形状：一条 7 天窗从 10-26 起，一条 30 天窗的复位在 11-04。
    /// 取最早起点配最长窗长会印出「10-26 起的 30 天窗，窗尾 11-25」——比池子自己
    /// 说的复位晚二十一天，而两条上游都不在这个窗里。
    #[test]
    fn members_in_different_windows_leave_the_report_without_a_window() {
        let week = MeasuredWindow {
            capacity_tokens: 33_000_000,
            window_secs: (7 * DAY) as u64,
            burned_at_unix: 1_793_028_477,
        };
        let quarter = MeasuredWindow {
            capacity_tokens: 328_662_149,
            // 复位在未来的那一支：窗起点就是它自己，摊平量因此为零。
            window_secs: (30 * DAY) as u64,
            burned_at_unix: 1_793_807_999,
        };
        let now = 1_793_333_000;
        let mut pool = PoolPace::default();
        pool.add(&week, 0, now);
        pool.add(&quarter, 0, now);
        let report = pool.finish(358_943, 533);
        assert_eq!(report.measured_upstreams, 2);
        assert!(
            report.allowance_tokens > 0,
            "额度是各家摊平量之和，窗不同不改变这一半"
        );
        assert_eq!(report.window_secs, 0, "没有一条共同的窗，就不报窗长");
        assert_eq!(
            report.window_start_unix, 0,
            "最早起点配最长窗长会拼出两条都不在的窗"
        );
    }

    /// 同样的窗长、不同的起点也是两条窗：只比长度不够，要比起止这一对。
    #[test]
    fn a_shared_length_does_not_make_a_shared_window() {
        let now = 1_000_000 + 10 * DAY;
        let first = month();
        let second = MeasuredWindow {
            burned_at_unix: 1_000_000 + 15 * DAY,
            ..month()
        };
        let mut pool = PoolPace::default();
        pool.add(&first, 0, now);
        pool.add(&second, 0, now);
        let report = pool.finish(0, 0);
        assert_eq!(report.window_secs, 0);
        assert_eq!(report.window_start_unix, 0);
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

    /// 一窗被收走之后，答复落在窗口边界上，不落在坡度算出来的那一秒。
    ///
    /// 2026-11-01 的现网形状：5 小时窗从 03:27:59 起，容量 4,006,951，花了 52,235，
    /// 上游每一次都答"这一窗的额度用完了"。允许量按零发布之后，坡度那一侧仍然算得
    /// 出"此刻早就攒够了这件任务"——答复是一秒，调用方一秒一次地重试。边界才对。
    #[test]
    fn a_refused_window_answers_with_its_boundary_not_with_a_spent_ramp() {
        let start = 1_000_000;
        let window_secs = 5 * 3_600;
        let now = start + 3_600;
        let pace = LlmWindowPace {
            measured_upstreams: 1,
            allowance_tokens: 0,
            spent_tokens: 52_235,
            reserve_tokens: 358_030,
            reserve_population: 533,
            window_secs,
            window_start_unix: start,
        };
        let rate = allowance_rate(4_006_951, window_secs);
        assert!(pace.blocked());
        assert_eq!(
            wait_secs(&pace, rate, now),
            Some(1),
            "坡度把已经收走的额度当成还能攒出来，这一格是下面那条的对照"
        );
        let wait = wait_until_reopened(&pace, rate, now).expect("blocked, so it names a wait");
        let boundary = start + window_secs as i64;
        let expected = (boundary - now) as f64 + 358_030.0 / rate;
        assert!(
            (wait as f64 - expected).abs() <= 60.0,
            "答复是边界加新一窗攒够预留的那一段，实际等了 {wait}s，应约 {expected}s"
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

    /// 一家同时被 5 小时与 7 天两把尺子管着时，进池和的是**紧**的那把。
    ///
    /// 现网形状（2026-11-01）：5 小时那把按 4.0M 的容量摊，7 天那把按一周烧掉
    /// 的 23.19M 摊。同一时刻前者的余量是后者的四倍多，取后者才是上游真会先
    /// 拒的那个界。只按最新那次拒绝点名的窗长判，等于让「最近撞的是哪堵墙」
    /// 决定配额——那天一整天的消耗就是按 5 小时那把走的，7 天那把直到花光才
    /// 第一次出现在账上。
    #[test]
    fn the_tighter_of_one_upstreams_nested_rulers_governs() {
        let now = 1_000_000 + 10 * DAY;
        // 5 小时这把刚过半：摊到 2.0M，一分没花。
        let burst = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 4_000_000,
                window_secs: 18_000,
                burned_at_unix: now - 9_000,
            },
            spent_tokens: 0,
            exhausted: false,
        };
        // 7 天这把到了第六天：摊到 19.88M，已经花掉 19M，只剩八百多万分之一。
        let week = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 23_190_000,
                window_secs: (7 * DAY) as u64,
                burned_at_unix: now - 6 * DAY,
            },
            spent_tokens: 19_000_000,
            exhausted: false,
        };
        assert!(burst.remaining_tokens(now) > week.remaining_tokens(now));
        let members = [burst, week];
        assert_eq!(
            governing_ruler(&members, now, ONE_JOB),
            Some(1),
            "宽的那把先放行，紧的那把随后就会拒，配速照宽的走等于把拒绝留给上游"
        );

        // 两把都进和就是两份额度：池会以为自己能花 5 小时那把的 2.0M **再加上**
        // 7 天那把剩下的那一点，而这两笔是同一个账户的同一份额度。
        let mut both = PoolPace::default();
        both.add(&members[0].measured, members[0].spent_tokens, now);
        both.add(&members[1].measured, members[1].spent_tokens, now);
        let both = both.finish(0, 0);
        let mut one = PoolPace::default();
        let governing = members[governing_ruler(&members, now, ONE_JOB).unwrap()];
        one.add(&governing.measured, governing.spent_tokens, now);
        let one = one.finish(0, 0);
        assert_eq!(
            both.allowance_tokens - one.allowance_tokens,
            members[0].measured.allowance_at(now),
            "多出来的那份正是另一把尺子的允许量，凭空记进池里"
        );
    }

    /// 上游当面拒了其中一把尺子时，就是它说了算：余量为零，不是坡度说的那个数。
    #[test]
    fn an_exhausted_ruler_governs_even_when_its_ramp_still_pays_out() {
        let now = 1_000_000 + 10 * DAY;
        let open = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 30_000_000,
                window_secs: (30 * DAY) as u64,
                burned_at_unix: now - 10 * DAY,
            },
            spent_tokens: 1_000_000,
            exhausted: false,
        };
        let refused = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 4_000_000,
                window_secs: 18_000,
                burned_at_unix: now - 18_000,
            },
            spent_tokens: 52_235,
            exhausted: true,
        };
        assert_eq!(
            governing_ruler(&[open, refused], now, ONE_JOB),
            Some(1),
            "被拒的那把余量为零，坡度还在说的那份额度不能替它说话"
        );
        assert_eq!(refused.remaining_tokens(now), 0);
    }

    /// 超过两个周期没人看过的容量不再是尺子：相位已经滚过不止一遍。
    ///
    /// 现网形状：改名前的上游（`kimi-k3`）留下的一行 7 天窗，八天没再有过任何
    /// 调用，行的坡度却还在按周滚动地把一整周的额度算给池。
    #[test]
    fn a_ruler_nobody_observed_for_two_cycles_stops_governing() {
        let now = 1_000_000 + 30 * DAY;
        let stale = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 17_284_171,
                window_secs: (7 * DAY) as u64,
                burned_at_unix: now - 20 * DAY,
            },
            spent_tokens: 0,
            exhausted: false,
        };
        assert!(!stale.is_current(now), "二十天前的边界不止两个周期");
        assert_eq!(
            governing_ruler(&[stale], now, ONE_JOB),
            None,
            "没有一只还说得出此刻容量的尺子，就该报「量不出来」而不是报一份额度"
        );

        let fresh = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 17_284_171,
                window_secs: (7 * DAY) as u64,
                burned_at_unix: now - 8 * DAY,
            },
            ..stale
        };
        assert!(fresh.is_current(now), "一个周期多一点的边界仍在用");
        assert_eq!(governing_ruler(&[fresh], now, ONE_JOB), Some(0));
    }

    /// 一窗的容量装不下一件整活时，它不许当那把尺子。
    ///
    /// 现网形状（2026-11-02 的量）：五小时那把容量 190,982，七天那把
    /// 15,112,408，而一件活按 99 分位要 1,099,270。五小时那把的允许量从零爬
    /// 到 190,982 就到顶，永远追不上预留，于是只要它说了算，这一家在整个窗里
    /// 一件也放不进来——而读数面上它写的是「等额度」，等多久都不会变。让它退出
    /// 之后，七天那把按自己的坡度放行，五小时那把撞墙时仍然会被 `exhausted`
    /// 那一路按零计（拒绝是这一家的观测，不是某一把尺子的）。
    #[test]
    fn a_window_that_cannot_hold_one_job_never_governs() {
        let now = 1_000_000 + 10 * DAY;
        let five_hour = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 190_982,
                window_secs: 18_000,
                burned_at_unix: now - 9_000,
            },
            spent_tokens: 0,
            exhausted: false,
        };
        let week = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 15_112_408,
                window_secs: (7 * DAY) as u64,
                burned_at_unix: now - 3 * DAY,
            },
            spent_tokens: 1_000_000,
            exhausted: false,
        };
        // 没有预留时，五小时那把余量最小、照旧最紧——这条断言把「换掉答案的
        // 是预留，不是别的什么改动」钉在同一个夹具上。
        assert_eq!(governing_ruler(&[five_hour, week], now, 0), Some(0));
        assert_eq!(
            governing_ruler(&[five_hour, week], now, ONE_JOB),
            Some(1),
            "装不下一件整活的窗口当尺子，这一家就永远放不进一件活"
        );
    }

    /// 一把都装不下时，仍然由最大的那把报数，而不是谁都不报。
    ///
    /// 谁都不进池和，请求路就没有可拒的数——那是不限速地烧，比拒更坏；最大的
    /// 那把给出的是这一家此刻最好的那份允许量，减法照样是负的，于是池子说的是
    /// 「额度不够」，而不是凭空多出一份产能。
    #[test]
    fn the_largest_window_still_reports_when_none_can_hold_a_job() {
        let now = 1_000_000 + 10 * DAY;
        let small = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 190_982,
                window_secs: 18_000,
                burned_at_unix: now - 9_000,
            },
            spent_tokens: 0,
            exhausted: false,
        };
        let smaller = RulerMember {
            measured: MeasuredWindow {
                capacity_tokens: 120_000,
                window_secs: 3_600,
                burned_at_unix: now - 600,
            },
            spent_tokens: 0,
            exhausted: false,
        };
        assert_eq!(
            governing_ruler(&[smaller, small], now, ONE_JOB),
            Some(1),
            "两把都装不下一件活时，报最大的那份允许量"
        );
        assert!(
            small.remaining_tokens(now) < ONE_JOB,
            "这条判据的前提：放行的那份允许量仍然付不起一件整活"
        );
    }
}
