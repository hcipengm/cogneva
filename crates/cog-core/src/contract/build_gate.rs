//! The build gate's contract half: the names it publishes under, the identity
//! of the directory it bounds, and the one slot through which a process that
//! has installed a gate is reachable from this crate.
//!
//! The gate itself -- its slot files, the file locks they carry, the control
//! law that derives the bound, the host samples it derives it from -- lives in
//! the crate that runs builds (`cog-reflection`). What stays here is what the
//! rest of `cog-core` reads: the metric names a dashboard and a rule are
//! written against, `dir_identity`, and the registration point through which
//! `build_cache_readings` and the resource account reach whatever gate this
//! process installed.
//!
//! The split is not cosmetic. A process that runs no builds still has to
//! *say* that, and the reading that says it is published from beside the gate;
//! keeping the derivation here would make every crate that reads the series
//! depend on the machinery that produces them.
//!
//! Nothing in this file may take a file lock or read host process statistics: a
//! contract that reached for either would be the implementation under another
//! name. The mechanism's own tokens therefore appear nowhere in this file, and
//! a grep for them over it must stay empty -- that is the check this half is
//! held to.

use std::path::Path;
use std::sync::{Arc, OnceLock};

use crate::build_cache_readings::CacheSlot;
use crate::contract::resource_ledger::ResourceEvent;

/// Builds holding a slot in this process, as a gauge.
pub const BUILD_GATE_IN_FLIGHT: &str = "cogneva_build_gate_in_flight";
/// Slot files locked right now in the shared directory, as a gauge: every
/// builder on the host counted, this process and any other alike.
///
/// Published beside [`BUILD_GATE_IN_FLIGHT`] because the two answer different
/// questions, and only the difference between them is the one this series
/// exists for. The in-flight gauge counts what *this* process is running, so a
/// builder that is not this process is invisible in it; this one is read from
/// the locks themselves, which is the only reading that does not depend on the
/// other builder being cooperative. What it is for: the bound in
/// [`BUILD_GATE_SLOTS`] is a number of slot files, and the claim a bound is
/// actually holding is that at most that many builds run on this host at once.
/// Two series of build activity that never meet leave that claim unreadable --
/// one process admitting up to its bound while another builds beside it looks
/// exactly like one process admitting up to its bound, and the difference is
/// whether the host is carrying one build or two.
pub const BUILD_GATE_OCCUPIED: &str = "cogneva_build_gate_occupied";
/// Holders that are not builds and hold every slot to keep them out, as a gauge:
/// 1 while such a caller is excluding builds, 0 otherwise.
///
/// Its own series because it answers the question a refusal raises — "who took
/// the slot, a build or something else?" — and every other reading here answers
/// it wrongly: the in-flight gauge and the held readings count builds, so
/// folding a cache sweep or a registry restart into them would report it as a
/// build that occupied the host for as long as it ran, and would leave a refusal
/// it caused pointing at a build that does not exist. What the exclusion
/// *achieves* is counted by its own callers, at the work being deferred.
pub const BUILD_GATE_EXCLUDED: &str = "cogneva_build_gate_excluded";
/// Builds waiting for a slot in this process, as a gauge.
pub const BUILD_GATE_WAITING: &str = "cogneva_build_gate_waiting";
/// Slots this gate permits, as a gauge; 0 when the gate is not in force.
///
/// The bound in force right now, which on a gate that derives its bound is a
/// measurement rather than a setting: it moves with the host, and it is read
/// beside [`BUILD_GATE_CEILING`], which is the same quantity before the
/// control law has had its say.
pub const BUILD_GATE_SLOTS: &str = "cogneva_build_gate_slots";
/// The most builds the host could take right now, as a gauge, carrying the
/// dimension that is setting it.
///
/// Published apart from [`BUILD_GATE_SLOTS`] because the two answer different
/// questions and only their difference is actionable: the ceiling says what the
/// machine can absorb, the bound says what is being admitted, and a bound below
/// its ceiling is the control law holding back (which is what it is told to do
/// until the host has been calm for a while). The `dimension` label is which one
/// is tightest — a bound of one because the disk is saturated is a different
/// fact from a bound of one because the memory floor is in sight, or because the
/// container's own limit is, and the number alone cannot tell them apart.
pub const BUILD_GATE_CEILING: &str = "cogneva_build_gate_ceiling";
/// Builds that have taken a slot, as a counter.
pub const BUILD_GATE_ACQUIRED_TOTAL: &str = "cogneva_build_gate_acquired_total";
/// Builds that were refused a slot, as a counter, by `reason`: the wall that
/// turned the build away.
///
/// The walls do not all share an owner — a build that found every slot taken
/// (`busy`, `waited_out`) is a capacity reading, a sweep or registry restart
/// holding the field (`maintenance_hold`) is maintenance standing the builds
/// down on purpose, and another work's reservation (`queue_ahead`) is the queue
/// doing its job — and one number over all four cannot say which of them is
/// happening. Summed over `reason` this is the number the counter carried
/// before the label existed, so a reader that wants the old total keeps it.
///
/// It carries `asker` for the same reason read the other way: the walls do not
/// fall on the works alike, so a refusal count says which wall and not who paid
/// for it, and a work refused every round is the one a reader has to find. See
/// `BUILD_GATE_ASKERS` (declared beside the gate, where the refusals are
/// recorded) for the closed set and for why the caller's own words
/// are not the label. Summed over `asker` this is likewise the number the
/// counter carried before that label existed.
pub const BUILD_GATE_REFUSED_TOTAL: &str = "cogneva_build_gate_refused_total";
/// Time builds have spent waiting for a slot, in milliseconds, summed over the
/// builds that went on to take one.
///
/// Divided by [`BUILD_GATE_ACQUIRED_TOTAL`] this is the queue delay of a build
/// that ran; it is not divided here because the division belongs to whoever
/// picks the window, and a mean published by the producer would be a mean over
/// the process's whole life, which is history rather than a reading.
pub const BUILD_GATE_WAIT_MS_TOTAL: &str = "cogneva_build_gate_wait_ms_total";
/// The longest a single build has waited before taking a slot, in milliseconds.
///
/// A mean cannot show whether a bound is mildly tight or whether one build
/// waited out the whole budget while others walked straight in, and the second
/// is the case that sizes the bound. A high-water mark of this process: after a
/// restart it reads lower than the host has seen, so it is a floor.
pub const BUILD_GATE_WAIT_MS_MAX: &str = "cogneva_build_gate_wait_ms_max";
/// Time builds have spent holding slots, in milliseconds, summed over the same
/// builds as [`BUILD_GATE_WAIT_MS_TOTAL`].
///
/// This is what a build occupies the host for, and the budget on it is the wait
/// budget: a builder that holds a slot for longer than everyone is willing to
/// wait is a bound that refuses everyone else.
pub const BUILD_GATE_HELD_MS_TOTAL: &str = "cogneva_build_gate_held_ms_total";
/// The longest single slot hold, in milliseconds. A high-water mark.
pub const BUILD_GATE_HELD_MS_MAX: &str = "cogneva_build_gate_held_ms_max";
/// How long the work holding each slot has held it, in seconds, as a gauge,
/// carrying the holder's own `what` and the slot it holds.
///
/// The refusal report already names the holder and its tenure, but that is an
/// event: it exists at the moment somebody was turned away, and the starved
/// case is the one where that moment never comes again — the waiter gives up,
/// stops asking, and the hold that caused it leaves no trace anyone can read
/// later. This series is the same fact as a value, read from the lock at scrape
/// time, so a hold that nobody is currently asking about is still visible.
///
/// `what` is the holder's own words rather than a label from a closed set: the
/// set of things that take a slot is open (a build, a change verification, a
/// cache sweep), and an unknown holder folded into an `other` bucket would be
/// exactly the caller a reader needs to name. A row exists only while the lock
/// is held; `BUILD_GATE_OCCUPIED` already says how many are, so free slots need
/// no row of their own.
pub const BUILD_GATE_HOLDER_SECONDS: &str = "cogneva_build_gate_holder_seconds";
/// How long a build is willing to wait for a slot, in milliseconds; 0 when no
/// gate is in force.
///
/// The wall the wait is measured against: a wait that reached this value either
/// took the slot at the buzzer or was refused, and without the wall on the same
/// axis a wait of 25s reads as small or as enormous depending on the deployment
/// it is not shown beside.
pub const BUILD_GATE_WAIT_BUDGET_MS: &str = "cogneva_build_gate_wait_budget_ms";
/// Name of the rule that reads the refusal counter, so the pairing can be asserted.
pub const BUILD_GATE_RULE: &str = "build_gate_refused";
/// Name of the rule that reads the slot count of a process that runs builds, so
/// the pairing can be asserted.
pub const BUILD_GATE_IN_FORCE_RULE: &str = "build_gate_not_in_force";

/// Label value for a process that runs builds and therefore took the gate.
pub const BUILD_GATE_ROLE_BUILDER: &str = "builder";
/// Label value for a process that runs no builds and publishes zero rows.
pub const BUILD_GATE_ROLE_CONTROL_PLANE: &str = "control-plane";

/// `dev:ino` of a directory, read from the filesystem rather than remembered, so
/// the reading follows the directory the gate actually bounds.
///
/// Public because it is the identity of a directory rather than of the gate: any
/// reading that has to tell "the same path" from "the same directory" publishes
/// this value, and a reader can then join those readings to one directory. Two
/// workloads mount different volumes at the same path, and a label carrying the
/// path would merge their readings into one number.
#[cfg(unix)]
pub fn dir_identity(dir: &Path) -> String {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(dir) {
        Ok(md) => format!("{}:{}", md.dev(), md.ino()),
        Err(_) => "absent".to_string(),
    }
}

#[cfg(not(unix))]
pub fn dir_identity(_dir: &Path) -> String {
    "unsupported".to_string()
}

/// What a process that installed a build gate can be asked, from a crate that
/// does not hold the gate.
///
/// Four questions, and each has exactly one asker that is not the gate's own
/// crate: the cache pass needs the gate as a slot, the resource account needs
/// the events the gate queued while it moved the bound or refused a build, and
/// a producer elsewhere in the process needs somewhere to put an event it did
/// not decide about the host itself. All come through this trait because the
/// gate's own type cannot be named here, and `GateSlot` is what turns the first
/// answer into the slot the pass takes.
#[async_trait::async_trait]
pub trait ProcessGate: Send + Sync {
    /// Whether anything bounds builds in this process at all.
    fn in_force(&self) -> bool;

    /// The exclusion the cache pass holds while it deletes: `None` while a
    /// build has the field.
    async fn take_for_reclaim(&self) -> Option<Box<dyn Send + Sync>>;

    /// Hands this process's queued resource events to the account's consumer.
    fn drain_events(&self) -> Vec<ResourceEvent>;

    /// Queues one event for that same consumer.
    ///
    /// The ring belongs to the process, not to the bound: its first producer is
    /// the control law, but a wall the cluster raised against this process's
    /// work is an event of the same account and there is only one consumer to
    /// hand it to. Carrying it here keeps a second queue from being written.
    fn record_event(&self, event: ResourceEvent);

    /// Events a full ring pushed out before a consumer took them.
    fn events_dropped(&self) -> u64;
}

/// The registered gate, read as the cache pass's slot.
///
/// A wrapper rather than an `impl CacheSlot for dyn ProcessGate`, because the
/// trait is not object-safe in a way that would let one be handed to the pass:
/// the pass wants an owned trait object it can hold for the length of a pass,
/// and `take` is the only method on it that matters here.
pub struct GateSlot(Arc<dyn ProcessGate>);

#[async_trait::async_trait]
impl CacheSlot for GateSlot {
    fn in_force(&self) -> bool {
        self.0.in_force()
    }

    async fn take(&self) -> Option<Box<dyn Send + Sync>> {
        self.0.take_for_reclaim().await
    }
}

/// The slot to hand the cache pass, when a gate has been installed.
pub fn cache_slot() -> Option<GateSlot> {
    global().map(GateSlot)
}

static GLOBAL: OnceLock<Arc<dyn ProcessGate>> = OnceLock::new();

/// Registers the process-wide gate.
///
/// The first call wins, as it does for the gate itself: the bound is a property
/// of the host, and two components of one process disagreeing about it would
/// each enforce their own. A later call is a no-op rather than an error -- the
/// install path is idempotent by design, and the caller cannot do anything
/// about a second one.
pub fn install_global(gate: Arc<dyn ProcessGate>) {
    let _ = GLOBAL.set(gate);
}

/// The process-wide gate, when one has been installed.
pub fn global() -> Option<Arc<dyn ProcessGate>> {
    GLOBAL.get().cloned()
}

/// Hands this process's queued resource events to the account's consumer.
///
/// Empty when no gate is in force: a process that bounds no builds makes no
/// decisions about the host, so it has no events about the bound to report.
pub fn drain_resource_events() -> Vec<ResourceEvent> {
    global().map(|gate| gate.drain_events()).unwrap_or_default()
}

/// Queues one event from anywhere in this process for that same consumer.
///
/// Answers whether it was queued. A process with no gate in force has no ring
/// and no consumer either, so the answer is `false` rather than an error: the
/// producer's decision still happened, but this process was never one that
/// keeps the account, and the caller should say so in its own log instead of
/// believing it was written down.
pub fn record_resource_event(event: ResourceEvent) -> bool {
    match global() {
        Some(gate) => {
            gate.record_event(event);
            true
        }
        None => false,
    }
}

/// How many events a full ring pushed out before a consumer took them.
pub fn dropped_resource_events() -> u64 {
    global().map(|gate| gate.events_dropped()).unwrap_or(0)
}
