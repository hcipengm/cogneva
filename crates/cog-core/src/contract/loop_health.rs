//! The vocabulary a background loop declares itself with, and the names its
//! readings are published under.
//!
//! A loop that says nothing about its rhythm cannot be told apart from one that
//! stopped, and a rules file that invents the series name cannot be told apart
//! from one that reads a series nobody publishes. Both halves of that agreement
//! are here, because both are read by someone other than the publisher: the
//! declaring side names [`Cadence`], and the deployed rules name the constants
//! below.
//!
//! What is deliberately not here is anything that holds state or starts a task —
//! the registry, the beat handles, the restart supervision. A reader that only
//! needs to declare a cadence or name a series does not thereby take on the
//! publisher's lifetime.

use std::time::Duration;

/// Loops this process started, per loop; the census of what is being watched.
pub const LOOP_REGISTERED: &str = "cogneva_loop_registered";
/// The cadence a loop declared, in seconds; 0 means the loop has no cadence and
/// its age is not a judgement of anything.
pub const LOOP_PERIOD_SECONDS: &str = "cogneva_loop_period_seconds";
/// Seconds since a loop last beat, computed when the scrape arrives.
pub const LOOP_TICK_AGE_SECONDS: &str = "cogneva_loop_tick_age_seconds";
/// The longest a loop's cycle may spend working between two beats, in seconds.
///
/// The half of a beat-to-beat gap that [`LOOP_PERIOD_SECONDS`] does not cover.
/// Zero for a loop whose period bounds its whole cycle, which is every loop that
/// does not declare otherwise — so a rule sums this onto the period and reads
/// the same wall it always did for the loops that never needed the distinction.
///
/// It is published instead of omitted, and so is the zero: a rule has to do
/// arithmetic with it, and a series that is absent for some loops and present
/// for others would make the sum silently drop exactly the loops the whole
/// declaration exists for.
///
/// The bound belongs to the loop because only the loop knows what it does
/// between beats; it is expected to come from the object that enforces the work
/// (a subprocess timeout, a wait budget) rather than from a measurement of past
/// rounds, which would only ever widen and would credit a loop for the slowest
/// thing it ever did.
pub const LOOP_WORK_BUDGET_SECONDS: &str = "cogneva_loop_work_budget_seconds";
/// Times a loop ended while the process was still running, as a counter.
pub const LOOP_DEATHS_TOTAL: &str = "cogneva_loop_deaths_total";
/// Times a loop was restarted after panicking, as a counter.
///
/// Separate from the deaths, because the two answer different questions and are
/// repaired differently: a death is a loop that stayed dead, while a restart is
/// a loop that panicked and is running again. Counting restarts as deaths would
/// make the two indistinguishable exactly in the case a reader has to act on,
/// and counting them nowhere would let a loop panic every hour forever without
/// anything saying so — a repair that leaves no reading is a defect made quiet.
pub const LOOP_RESTARTS_TOTAL: &str = "cogneva_loop_restarts_total";
/// Label naming the loop. Its value set is bounded by configuration rather than
/// by traffic: one value per loop instance the process starts, so a site that
/// runs one instance per configured stream or workspace names it after that
/// configured value. A name built from message content would make the series
/// unbounded, and a name shared by two instances would let a dead one hide
/// behind the beats of its live sibling.
pub const LOOP_LABEL: &str = "loop";

/// The loops that take part in a single-writer role, as `1`.
///
/// Published by a process whose loop was given a role to contend for, and by no
/// one else — a loop that does no single-writer work has no ownership reading to
/// make and says nothing here. It exists for the rule that has to tell "no
/// process is holding this role" apart from "no process was ever asked to": the
/// holder gauge is silent in both, and a rule over the gauge alone would be
/// quiet through a deployment that lost its owner for good.
///
/// `0` is the third state, and it is not the same as silence: this loop has the
/// role and has stood down from it on purpose, so the role is free by design
/// rather than lost. A loop that stands down is exactly the case the rule above
/// must not treat as an incident — the seat is open because the work cannot be
/// done from here right now, which is when the role is meant to leave this
/// deployment — so it says "not in the running" instead of saying nothing.
pub const LOOP_ROLE_DECLARED: &str = "cogneva_loop_role_declared";

/// `1` on the process that holds the loop's role, `0` on one that asked and was
/// told another process holds it.
///
/// Absent when the process could not ask at all, which is why the two states are
/// not a single series with a third value: a zero claims a live holder exists
/// somewhere else, and a process that could not reach the arbiter has no
/// business making that claim. The failures it did have are counted next door.
pub const LOOP_OWNER_HELD: &str = "cogneva_loop_owner_held";

/// Times this process took the role, as a counter: the first term, and every
/// term it won back after losing one.
pub const LOOP_OWNER_ACQUISITIONS_TOTAL: &str = "cogneva_loop_owner_acquisitions_total";

/// Times this process held the role and then stopped, as a counter.
///
/// A loss is the takeover seen from the losing side, and it is not the same
/// reading as the winner's acquisition: a process that was the holder and no
/// longer is has stopped doing work it was doing, and everything it had
/// published up to that moment is now last-known rather than current. Counted
/// where it happened, because the process that lost the role may be the only
/// one that can say the work moved.
pub const LOOP_OWNER_LOSSES_TOTAL: &str = "cogneva_loop_owner_losses_total";

/// Cycles this loop's role could not be asked about at all, as a counter.
///
/// One per attempt, not one per outage: a loop asks once per cycle, so this
/// divided by the loop's own period is how much of the time the role was
/// unprovable. It is the reading that separates an unheld role from an
/// unreachable arbiter — both leave the holder gauge silent, and only one of
/// them is repaired by looking at the store.
pub const LOOP_OWNER_PROBE_FAILURES_TOTAL: &str = "cogneva_loop_owner_probe_failures_total";

/// The deployed rule that reads whether any process holds a declared role.
pub const OWNER_UNOWNED_RULE: &str = "background_loop_role_unowned";
/// The deployed rule that reads whether two processes hold one role at once.
pub const OWNER_SPLIT_RULE: &str = "background_loop_role_split";
/// The deployed rule that reads the cycles a role could not be asked about.
pub const OWNER_PROBE_RULE: &str = "background_loop_role_probe_failing";

/// The deployed rule that reads the age this module publishes.
pub const STALL_RULE: &str = "background_loop_stalled";
/// The deployed rule that reads the death counter this module publishes.
pub const DEATH_RULE: &str = "background_loop_died";
/// The deployed rule that reads the restart counter this module publishes.
pub const RESTART_RULE: &str = "background_loop_restarted";

/// How often a loop is expected to reach the top of its cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cadence {
    /// Every `Duration`, whether or not the cycle found work, and the work one
    /// cycle does between two beats is short enough that the period covers it.
    ///
    /// This is what makes a stalled loop distinguishable from an idle one. It is
    /// the wrong declaration for a loop whose cycle runs a build, a test or a
    /// queue wait: its period would then bound only the wait for the next tick,
    /// and every healthy cycle would age past any multiple of it.
    Periodic(Duration),
    /// Every `period`, and the work one cycle does between two beats takes up to
    /// `work`.
    ///
    /// For a loop whose cycle contains a step it does not control the duration
    /// of. Both numbers are parts of the loop's own design, and each has to come
    /// from what enforces it: `period` from the wait the loop itself sleeps,
    /// `work` from the timeout or budget on the step that runs inside the cycle.
    /// A reader adds them, so the wall a rule compares an age against is the
    /// loop's own numbers rather than a constant that goes stale when one of the
    /// two is configured differently.
    ///
    /// A loop that drains a queue declares the bound on **one item** and beats
    /// between items, because that is the gap it can actually bound — declaring
    /// the queue's total would be a claim about a length it does not know.
    PeriodicWithWork {
        /// How often the loop reaches the top of its cycle while it is idle.
        period: Duration,
        /// The longest the work between two beats may take.
        work: Duration,
    },
    /// Only when there is work to do. The age is still published — it is a
    /// reading — but nothing may conclude "stuck" from it.
    EventDriven,
}

impl Cadence {
    /// The declared period in whole seconds, or 0 for a loop with no cadence.
    pub fn period_secs(self) -> u64 {
        match self {
            Cadence::Periodic(d) => d.as_secs(),
            Cadence::PeriodicWithWork { period, .. } => period.as_secs(),
            Cadence::EventDriven => 0,
        }
    }

    /// The bound the loop declares on the work between two beats, in whole
    /// seconds; 0 when its period already covers its cycle.
    pub fn work_budget_secs(self) -> u64 {
        match self {
            Cadence::PeriodicWithWork { work, .. } => work.as_secs(),
            Cadence::Periodic(_) | Cadence::EventDriven => 0,
        }
    }
}

/// What this process allows a loop that panicked, before it is left dead.
///
/// Installed once at startup from the configuration document, because the budget
/// belongs to the process's loops rather than to any one of them: three loops
/// holding three different budgets would make the same defect read differently
/// depending on which loop happened to hit it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartSettings {
    /// How many times in a row a loop is run again after panicking.
    ///
    /// Three, because a fault that a restart repairs is repaired by the first
    /// one: the second and third exist for the faults that need the state they
    /// tripped over to be gone, and a loop that panics past all three is not
    /// having a bad cycle — it is a defect, and a fourth attempt would only
    /// postpone the reading that says so. Zero turns restarting off and leaves
    /// a panic as fatal as it was before this existed.
    pub max_consecutive: u32,
    /// The floor under a loop's first wait between attempts, in seconds.
    ///
    /// A loop with a cadence waits its own period; a loop that only wakes on work
    /// has no rhythm to wait, so it waits this. Five seconds is long enough that
    /// a loop which panics immediately does not fill the log with attempts (the
    /// waits double from here, so a spent budget is tens of seconds, not
    /// milliseconds) and short enough that an event-driven loop is back before
    /// the next burst of work.
    pub backoff_floor_secs: u64,
}

impl Default for RestartSettings {
    fn default() -> Self {
        Self {
            max_consecutive: 3,
            backoff_floor_secs: 5,
        }
    }
}

/// The restart budget one loop is judged by, derived from what it declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartPolicy {
    /// Restarts allowed back to back before the loop is left dead.
    pub max_consecutive: u32,
    /// The wait before the first attempt to run the loop again; it doubles from
    /// there, up to [`Self::backoff_max`].
    pub backoff: Duration,
    /// The longest wait, and the lifetime an attempt has to reach to count as
    /// having taken.
    pub backoff_max: Duration,
}

/// How many of a loop's own periods the longest restart wait spans.
///
/// Six is not a value chosen here: it is the multiple the deployed stall rule
/// already measures a loop against, and the rule adds the loop's declared work
/// budget on top of that multiple. Waiting longer than the multiple would keep a
/// loop from beating past the point where it is announced as stalled, so the two
/// readings would describe the same state in opposite directions. The work
/// budget is deliberately left out of this wait: it is time the loop spends
/// working rather than time between attempts, and a restart that waited it out
/// would delay the repair by the length of the thing that is not being repaired.
// cap: derived — 量的是「在不在干活」而不是两次尝试之间的时间；等满它的重启会把修复推迟整整一段没在被修的时间
const STALL_PERIODS: u32 = 6;

impl RestartPolicy {
    /// The budget for a loop with this cadence under these settings.
    pub fn for_cadence(cadence: Cadence, settings: RestartSettings) -> Self {
        let declared = match cadence {
            Cadence::Periodic(period) => period,
            Cadence::PeriodicWithWork { period, .. } => period,
            Cadence::EventDriven => Duration::ZERO,
        };
        let base = declared.max(Duration::from_secs(settings.backoff_floor_secs));
        Self {
            max_consecutive: settings.max_consecutive,
            backoff: base,
            backoff_max: base * STALL_PERIODS,
        }
    }

    /// How long to wait before the `run`th attempt in a row.
    pub(crate) fn wait_before(&self, run: u32) -> Duration {
        self.backoff
            .saturating_mul(1u32 << (run - 1).min(16))
            .min(self.backoff_max)
    }
}
