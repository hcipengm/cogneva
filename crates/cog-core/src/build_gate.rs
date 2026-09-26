//! A host-wide gate on how many builds run at once.
//!
//! Builds and the cluster share one host, and a `cargo build --release` plus a
//! `cargo test --workspace` in parallel is enough to take that host to its
//! knees: the machine pages instead of refusing, so every workload on it
//! degrades at once, including the event loops that would have reported the
//! problem. The count of concurrent builds is the thing to bound, and it is a
//! **per-host** quantity, not a per-process one — the deployer, the change
//! verification, the baseline porter and the image builder all run on the same
//! machine, in different processes, and each of them spawning a build whenever
//! it has one to run is how the sum gets there.
//!
//! The gate is `flock` on a slot file per permitted build, so the bound lives in
//! the kernel rather than in a pid file this process has to keep honest:
//!
//! - A crashed or killed builder releases its slot when its file descriptors
//!   close. A pid file has to be aged out by a heuristic afterwards, and until
//!   that heuristic fires the host stays one build short of the bound it thinks
//!   it has.
//! - A second *process* is excluded as surely as a second task here. An
//!   in-process semaphore would leave the other builder processes unbounded,
//!   which is the case that matters — the sum is what takes the host down.
//!
//! Two ways to ask, because the callers disagree about what a busy gate means:
//!
//! - [`BuildGate::try_acquire`] refuses immediately. A polling loop (the
//!   mainline deployer's cycle) is better off skipping this tick than waiting
//!   inside it, since it comes back on its own.
//! - [`BuildGate::acquire`] waits up to the configured budget and then refuses.
//!   A one-shot build has nowhere to come back to, so a refusal there is a real
//!   outcome, and it must not be reported as the change having failed.
//!
//! What the gate cannot do is tell whether the builders it excludes are on this
//! host or not — that depends on the slot directory being one directory for
//! every process on the machine. If two deployments point at different
//! directories, each gets its own bound and nothing in any log would say so, so
//! the directory's identity is published alongside the readings: two processes
//! that print the same `dev:ino` are gating each other, and two that do not are
//! not.
//!
//! Only the process that runs builds installs a gate. A process that runs none
//! would take a lock on its own copy of the directory — the same path in a
//! different container is a different directory — and publish a bound that
//! excludes nobody, which is the one reading an operator cannot afford to
//! misread. Such a process still publishes the series, at zero: a series that is
//! simply absent and a gate that bounds nothing read identically, and the whole
//! point of the readings is to tell those apart. Every series carries the role
//! it was published by, so a rule can ask about builders alone.
//!
//! What no reading here can show is a builder that never asked: the token only
//! bounds the build paths that call it, and a path nobody wired up produces no
//! series at all rather than a zero.
//!
//! How long the bound costs is measured over the builds that took a slot, and
//! over those alone: how long they queued, and how long they then held the host.
//! A wait that ended in a refusal is deliberately outside both — it is not a
//! build that ran, and its length is not a reading at all: a refusal has either
//! waited the whole budget or, for a caller that will come back later, not
//! waited at all, so its duration is fixed by which of the two it was, which
//! [`BUILD_GATE_REFUSED_TOTAL`] already says. Folding those waits into the mean
//! would drag it towards zero every time a polling caller asked and was turned
//! away, which is the one number a reader would then use to size the bound.
//! Counting them into the maximum instead would compare two different events
//! under one name.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::config::BuildGateConfig;
use crate::contract::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use crate::{SFError, SFResult};

/// Builds holding a slot in this process, as a gauge.
pub const BUILD_GATE_IN_FLIGHT: &str = "cogneva_build_gate_in_flight";
/// Builds waiting for a slot in this process, as a gauge.
pub const BUILD_GATE_WAITING: &str = "cogneva_build_gate_waiting";
/// Slots this gate permits, as a gauge; 0 when the gate is not in force.
pub const BUILD_GATE_SLOTS: &str = "cogneva_build_gate_slots";
/// Builds that have taken a slot, as a counter.
pub const BUILD_GATE_ACQUIRED_TOTAL: &str = "cogneva_build_gate_acquired_total";
/// Builds that were refused a slot, as a counter.
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

/// How often a waiter looks again. Builds run for minutes, so this only decides
/// how promptly a freed slot is picked up.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

static GLOBAL: OnceLock<Arc<BuildGate>> = OnceLock::new();

/// Installs the process-wide gate from configuration.
///
/// The first call wins and later ones are ignored: the bound is a property of
/// the host, and two components of one process disagreeing about it would each
/// enforce their own. A gate that cannot be installed is reported and left
/// uninstalled rather than silently binding to nothing — the readings say
/// whether it is in force.
pub fn install(cfg: &BuildGateConfig) -> Arc<BuildGate> {
    GLOBAL
        .get_or_init(|| {
            let gate = Arc::new(BuildGate::new(cfg));
            if gate.in_force() {
                tracing::info!(
                    slots = gate.slots,
                    wait_secs = gate.wait.as_secs(),
                    dir = %gate.dir.display(),
                    identity = %gate.identity(),
                    "host build gate installed"
                );
            } else {
                tracing::warn!(
                    enabled = cfg.enabled,
                    dir = %cfg.lock_dir,
                    "host build gate is not in force; concurrent builds are unbounded"
                );
            }
            gate
        })
        .clone()
}

/// The process-wide gate, when one has been installed.
pub fn global() -> Option<Arc<BuildGate>> {
    GLOBAL.get().cloned()
}

/// What a process should publish about the build bound, given whether it runs
/// builds at all.
///
/// Two processes on one host sharing a slot directory bound each other; a
/// process that runs no builds has nothing to bound and must not take a lock
/// that looks like one, so it publishes zeros instead. Both publish the same
/// series names, so a reader can compare a bound that is not in force against a
/// bound that was never asked for.
pub fn install_for(cfg: &BuildGateConfig, runs_builds: bool) -> Arc<dyn Observable> {
    if runs_builds {
        install(cfg)
    } else {
        tracing::info!(
            dir = %cfg.lock_dir,
            "this process runs no builds; publishing the build gate readings as not in force"
        );
        Arc::new(InactiveBuildGate {
            dir: cfg.lock_dir.clone(),
        })
    }
}

/// The build gate readings of a process that runs no builds.
///
/// Not a gate: [`acquire`] in this process finds nothing installed and lets the
/// caller build ungated, and this type exists so that state is a reading rather
/// than an absence.
struct InactiveBuildGate {
    dir: String,
}

#[async_trait]
impl Observable for InactiveBuildGate {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(readings(
            0,
            Counters::default(),
            &dir_identity(Path::new(&self.dir)),
            BUILD_GATE_ROLE_CONTROL_PLANE,
            0,
        ))
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Everything a gate counts, gathered in one value so the series set has one
/// source. A gate that is not in force publishes [`Counters::default`] — the
/// zero of every counter is also the reading of a process that runs nothing.
#[derive(Default)]
struct Counters {
    in_flight: u64,
    waiting: u64,
    acquired: u64,
    refused: u64,
    wait_ms_total: u64,
    wait_ms_max: u64,
    held_ms_total: u64,
    held_ms_max: u64,
}

/// The whole series set, in one place: a reader that has to tell "no bound here"
/// from "this deployment never reported" cannot do it if the two sources of the
/// set can drift.
fn readings(
    slots: usize,
    c: Counters,
    dir: &str,
    role: &str,
    wait_budget_ms: u64,
) -> Vec<RawMetric> {
    let label = |m: RawMetric| {
        m.with_label("dir", dir.to_string())
            .with_label("role", role.to_string())
    };
    vec![
        label(RawMetric::new(BUILD_GATE_SLOTS, slots as f64)),
        label(RawMetric::new(BUILD_GATE_IN_FLIGHT, c.in_flight as f64)),
        label(RawMetric::new(BUILD_GATE_WAITING, c.waiting as f64)),
        label(RawMetric::new(BUILD_GATE_ACQUIRED_TOTAL, c.acquired as f64)),
        label(RawMetric::new(BUILD_GATE_REFUSED_TOTAL, c.refused as f64)),
        label(RawMetric::new(
            BUILD_GATE_WAIT_MS_TOTAL,
            c.wait_ms_total as f64,
        )),
        label(RawMetric::new(BUILD_GATE_WAIT_MS_MAX, c.wait_ms_max as f64)),
        label(RawMetric::new(
            BUILD_GATE_HELD_MS_TOTAL,
            c.held_ms_total as f64,
        )),
        label(RawMetric::new(BUILD_GATE_HELD_MS_MAX, c.held_ms_max as f64)),
        label(RawMetric::new(
            BUILD_GATE_WAIT_BUDGET_MS,
            wait_budget_ms as f64,
        )),
    ]
}

/// Takes a slot for a build that has nowhere to come back to, waiting within the
/// configured budget.
///
/// `Ok(None)` means no gate is in force in this process (unconfigured, disabled,
/// or a platform without `flock`), and the caller should build ungated. `Err`
/// means a slot was refused, and the build must not run: reported as anything
/// else, a gate that refuses would show up as a build that failed.
pub async fn acquire(what: &str) -> SFResult<Option<BuildPermit>> {
    match global() {
        Some(gate) => gate.acquire(what).await.map(Some),
        None => Ok(None),
    }
}

/// Takes a slot for a build that will be retried later, refusing at once when
/// none is free.
pub async fn try_acquire(what: &str) -> SFResult<Option<BuildPermit>> {
    match global() {
        Some(gate) => gate.try_acquire(what).await.map(Some),
        None => Ok(None),
    }
}

/// How a slot request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The caller asked not to wait, and no slot was free.
    Busy,
    /// The wait budget ran out with every slot still taken.
    Waited,
}

impl Refusal {
    fn reason(self, slots: usize, wait: Duration) -> String {
        match self {
            Refusal::Busy => format!("all {slots} build slots are taken"),
            Refusal::Waited => format!(
                "all {slots} build slots were taken for the whole {}s wait budget",
                wait.as_secs()
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Refusal::Busy => "busy",
            Refusal::Waited => "waited_out",
        }
    }
}

/// The host-wide build bound.
pub struct BuildGate {
    dir: PathBuf,
    /// Slots actually enforceable: 0 when the gate is not in force.
    slots: usize,
    wait: Duration,
    installed: bool,
    in_flight: AtomicU64,
    waiting: AtomicU64,
    acquired: AtomicU64,
    refused: AtomicU64,
    wait_ms_total: AtomicU64,
    wait_ms_max: AtomicU64,
    held_ms_total: AtomicU64,
    held_ms_max: AtomicU64,
}

impl std::fmt::Debug for BuildGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildGate")
            .field("dir", &self.dir)
            .field("slots", &self.slots)
            .field("wait", &self.wait)
            .field("installed", &self.installed)
            .finish()
    }
}

impl BuildGate {
    /// Builds the gate from configuration, creating the slot directory.
    ///
    /// A gate that cannot create its directory is not in force: binding to a
    /// directory that does not exist would refuse every build, and refusing
    /// every build is a worse failure than running them unbounded.
    pub fn new(cfg: &BuildGateConfig) -> Self {
        let dir = PathBuf::from(&cfg.lock_dir);
        let mut installed = false;
        if cfg.enabled && cfg.max_concurrent > 0 {
            installed = create_slot_dir(&dir).is_ok();
        }
        Self {
            slots: if installed { cfg.max_concurrent } else { 0 },
            wait: Duration::from_secs(cfg.wait_secs),
            installed,
            dir,
            in_flight: AtomicU64::new(0),
            waiting: AtomicU64::new(0),
            acquired: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            wait_ms_total: AtomicU64::new(0),
            wait_ms_max: AtomicU64::new(0),
            held_ms_total: AtomicU64::new(0),
            held_ms_max: AtomicU64::new(0),
        }
    }

    /// Whether this gate is excluding anything at all.
    pub fn in_force(&self) -> bool {
        self.installed && self.slots > 0
    }

    /// The counters as they stand, for the reading.
    fn counters(&self) -> Counters {
        Counters {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            waiting: self.waiting.load(Ordering::Relaxed),
            acquired: self.acquired.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
            wait_ms_total: self.wait_ms_total.load(Ordering::Relaxed),
            wait_ms_max: self.wait_ms_max.load(Ordering::Relaxed),
            held_ms_total: self.held_ms_total.load(Ordering::Relaxed),
            held_ms_max: self.held_ms_max.load(Ordering::Relaxed),
        }
    }

    /// The budget a waiter is refused at, in milliseconds; 0 when no gate is in
    /// force, since nothing is being waited for.
    fn wait_budget_ms(&self) -> u64 {
        if self.in_force() {
            self.wait.as_millis() as u64
        } else {
            0
        }
    }

    /// Records a wait that ended in taking a slot, and hands back when the slot
    /// was taken so the hold can be measured when it is released.
    fn slot_taken(&self, waited: Duration) -> Instant {
        let waited_ms = waited.as_millis() as u64;
        self.wait_ms_total.fetch_add(waited_ms, Ordering::Relaxed);
        self.wait_ms_max.fetch_max(waited_ms, Ordering::Relaxed);
        Instant::now()
    }

    /// Records a slot being released after `held`.
    fn slot_released(&self, held: Duration) {
        let held_ms = held.as_millis() as u64;
        self.held_ms_total.fetch_add(held_ms, Ordering::Relaxed);
        self.held_ms_max.fetch_max(held_ms, Ordering::Relaxed);
    }

    /// Where the slot files live.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `dev:ino` of the slot directory, or `absent` when it cannot be read.
    ///
    /// Two processes on one host that gate each other print the same value;
    /// two that do not, do not. Without it, a deployment whose builders each
    /// bound themselves separately looks exactly like one that bounds the host.
    pub fn identity(&self) -> String {
        dir_identity(&self.dir)
    }

    /// Takes a slot, waiting for one up to the configured budget.
    pub async fn acquire(self: &Arc<Self>, what: &str) -> SFResult<BuildPermit> {
        self.acquire_within(what, self.wait).await
    }

    /// Takes a slot only if one is free right now.
    pub async fn try_acquire(self: &Arc<Self>, what: &str) -> SFResult<BuildPermit> {
        self.acquire_within(what, Duration::ZERO).await
    }

    async fn acquire_within(
        self: &Arc<Self>,
        what: &str,
        budget: Duration,
    ) -> SFResult<BuildPermit> {
        if !self.in_force() {
            // No bound to enforce: hand back a permit that holds nothing, so the
            // caller's shape does not depend on whether the gate is installed.
            return Ok(BuildPermit {
                gate: Arc::clone(self),
                held: None,
                taken_at: None,
                what: what.to_string(),
            });
        }
        let waiting = self.waiting.fetch_add(1, Ordering::Relaxed) + 1;
        if waiting > 1 {
            tracing::info!(what, waiting, "build waiting for a slot");
        }
        let started = Instant::now();
        let deadline = started + budget;
        // The first pass is unconditional, so a zero budget still means "look
        // once" rather than "never look".
        loop {
            for slot in 0..self.slots {
                if let Some(file) = lock_slot(&self.dir, slot) {
                    self.waiting.fetch_sub(1, Ordering::Relaxed);
                    self.in_flight.fetch_add(1, Ordering::Relaxed);
                    let total = self.acquired.fetch_add(1, Ordering::Relaxed) + 1;
                    let waited = started.elapsed();
                    let taken_at = self.slot_taken(waited);
                    if waited.as_secs() > 0 {
                        tracing::info!(
                            what,
                            slot,
                            waited_secs = waited.as_secs(),
                            "build took a slot after waiting"
                        );
                    }
                    tracing::debug!(what, slot, total, "build took a slot");
                    return Ok(BuildPermit {
                        gate: Arc::clone(self),
                        held: Some(file),
                        taken_at: Some(taken_at),
                        what: what.to_string(),
                    });
                }
            }
            if Instant::now() >= deadline {
                self.waiting.fetch_sub(1, Ordering::Relaxed);
                let total = self.refused.fetch_add(1, Ordering::Relaxed) + 1;
                let refusal = if budget.is_zero() {
                    Refusal::Busy
                } else {
                    Refusal::Waited
                };
                let reason = refusal.reason(self.slots, budget);
                tracing::warn!(
                    what,
                    refusal = refusal.as_str(),
                    slots = self.slots,
                    dir = %self.dir.display(),
                    identity = %self.identity(),
                    total,
                    "{reason}"
                );
                return Err(SFError::ResourceExhausted(format!(
                    "build gate refused a slot for {what}: {reason}"
                )));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// A held slot. Dropping it releases the slot to the next builder.
#[derive(Debug)]
pub struct BuildPermit {
    gate: Arc<BuildGate>,
    /// The locked file, when a slot was actually taken. Closing it -- which drop
    /// does -- is what releases the lock.
    held: Option<std::fs::File>,
    /// When the slot was taken, set with `held` and read only where `held` is
    /// set: a permit that holds nothing was never counted as a build that ran,
    /// so it contributes no hold to the readings either.
    taken_at: Option<Instant>,
    what: String,
}

impl BuildPermit {
    /// Whether this permit holds a slot.
    pub fn held(&self) -> bool {
        self.held.is_some()
    }
}

impl Drop for BuildPermit {
    fn drop(&mut self) {
        if let (Some(_), Some(taken_at)) = (&self.held, self.taken_at) {
            self.gate.in_flight.fetch_sub(1, Ordering::Relaxed);
            self.gate.slot_released(taken_at.elapsed());
            tracing::debug!(what = %self.what, "build released its slot");
        }
    }
}

/// Creates the slot directory, reporting why when it cannot.
fn create_slot_dir(dir: &Path) -> SFResult<()> {
    std::fs::create_dir_all(dir)
        .map_err(|e| SFError::IO(format!("create build gate dir {}: {e}", dir.display())))
}

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

/// Takes the exclusive lock on one slot file, or reports that someone else has it.
#[cfg(unix)]
fn lock_slot(dir: &Path, slot: usize) -> Option<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(format!("slot-{slot}.lock")))
        .ok()?;
    use std::os::unix::io::AsRawFd;
    // SAFETY: flock only touches the descriptor table of this process for a
    // descriptor this scope owns; EWOULDBLOCK is the expected "someone else has
    // it" answer and carries no memory obligations.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Some(file)
    } else {
        None
    }
}

/// Without `flock` there is no cross-process bound to enforce, so the gate
/// refuses to pretend it has one.
#[cfg(not(unix))]
fn lock_slot(_dir: &Path, _slot: usize) -> Option<std::fs::File> {
    None
}

/// Publishes the gate's readings.
#[async_trait]
impl Observable for BuildGate {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let slots = if self.in_force() { self.slots } else { 0 };
        Ok(readings(
            slots,
            self.counters(),
            &self.identity(),
            BUILD_GATE_ROLE_BUILDER,
            self.wait_budget_ms(),
        ))
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The readings are the same for every dimension: they count builds on this
    /// host, and which task asked does not change that.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn gate(dir: &Path, slots: usize, wait_secs: u64) -> Arc<BuildGate> {
        Arc::new(BuildGate::new(&BuildGateConfig {
            enabled: true,
            max_concurrent: slots,
            wait_secs,
            lock_dir: dir.to_string_lossy().into_owned(),
        }))
    }

    /// A build has to be runnable from a spawned task, or the call sites cannot
    /// hold their slot across an await point.
    #[test]
    fn a_permit_can_be_held_across_an_await() {
        fn assert_send<T: Send>() {}
        assert_send::<BuildPermit>();
        assert_send::<Arc<BuildGate>>();
    }

    #[tokio::test]
    async fn a_gate_admits_only_as_many_builds_as_it_has_slots() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);

        let first = gate.try_acquire("first").await.unwrap();
        assert!(first.held());
        assert!(
            gate.try_acquire("second").await.is_err(),
            "a second build must not start while the only slot is taken"
        );

        drop(first);
        let third = gate.try_acquire("third").await.unwrap();
        assert!(third.held(), "dropping a permit frees its slot");
    }

    /// The bound is a property of the directory, not of the gate instance: two
    /// builders in different processes share nothing but the slot files, so if
    /// the bound were per instance every process would enforce its own and the
    /// host would see the sum.
    #[tokio::test]
    async fn two_gates_on_one_directory_bound_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let one = gate(dir.path(), 1, 0);
        let other = gate(dir.path(), 1, 0);

        let held = one.try_acquire("in this gate").await.unwrap();
        assert!(
            other.try_acquire("in the other gate").await.is_err(),
            "the other builder sees the slot as taken"
        );

        drop(held);
        assert!(
            other.try_acquire("now free").await.is_ok(),
            "the slot is free for the other builder once it is released"
        );
    }

    /// A slot comes back when the last copy of its open file description goes, not
    /// when the holder drops its handle.
    ///
    /// `flock` is owned by the open file description, so a second handle onto the same
    /// description is another reference that keeps the slot taken after `BuildPermit`
    /// is dropped. A `dup` is one way to get such a handle; the copy a fork leaves in
    /// a child that has not exec'd yet is another, and that is the window a caller can
    /// be refused in even though it just released its own permit — the gate reads what
    /// the kernel reports, and while a copy is open the kernel is right to say taken.
    ///
    /// The copy below is a `dup` rather than a forked child, because a fork copies the
    /// whole fd table: forked before it narrows that table, the child also holds what
    /// every other test in this binary has open, and each of those is a reference on
    /// somebody else's lock. This binary starts no processes, so the `dup` here is the
    /// only second reference that exists while the test runs.
    #[tokio::test]
    async fn a_slot_comes_back_when_the_last_copy_of_the_lock_file_goes() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);

        let permit = gate.try_acquire("the holding build").await.unwrap();
        let held = permit
            .held
            .as_ref()
            .expect("a taken slot holds its lock file");
        // SAFETY: `held` is open for as long as `permit`, so its descriptor is valid,
        // and the returned descriptor is this test's to close exactly once.
        let duplicate = unsafe { libc::dup(held.as_raw_fd()) };
        assert!(duplicate >= 0, "dup failed");
        // SAFETY: `duplicate` is a fresh descriptor owned by nobody else.
        let copy = unsafe { OwnedFd::from_raw_fd(duplicate) };

        drop(permit);
        assert!(
            gate.try_acquire("the next build").await.is_err(),
            "a second handle on the lock file keeps the slot taken"
        );

        drop(copy);
        assert!(
            gate.try_acquire("after the copy is gone").await.is_ok(),
            "the slot comes back with the last copy of the description"
        );
    }

    /// A waiting build is a reading of its own: a gate at its bound with nothing
    /// queued and a gate whose queue is stuck look the same from the outside
    /// unless the waiters are counted.
    #[tokio::test]
    async fn a_waiting_build_is_visible_while_it_waits() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 30);

        let held = gate.try_acquire("holder").await.unwrap();
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.acquire("waiter").await })
        };
        // Let the waiter reach the poll loop.
        for _ in 0..50 {
            if gate.waiting.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            gate.waiting.load(Ordering::Relaxed),
            1,
            "the queued build is counted while it waits"
        );
        assert!(!waiter.is_finished(), "it is still waiting for the slot");

        drop(held);
        let permit = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter takes the freed slot")
            .unwrap()
            .unwrap();
        assert!(permit.held());
        assert_eq!(gate.waiting.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_gate_at_its_bound_refuses_when_the_wait_budget_runs_out() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 1);

        let _held = gate.try_acquire("holder").await.unwrap();
        let started = Instant::now();
        let refused = gate.acquire("one-shot build").await;
        assert!(refused.is_err(), "a full gate must not hand out a slot");
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "it waited the configured budget before refusing"
        );
        // Classified as environmental: the same build succeeds when the host
        // frees up, so a caller that reads this as the work failing would
        // retire a change because the machine was busy.
        assert!(matches!(
            refused.unwrap_err(),
            SFError::ResourceExhausted(_)
        ));
        assert!(refused_is_environmental());
        assert_eq!(gate.refused.load(Ordering::Relaxed), 1);
    }

    fn refused_is_environmental() -> bool {
        SFError::ResourceExhausted("busy".into()).is_environment_failure()
    }

    /// How long the bound costs is the reason to look at it at all: a gate at
    /// its bound with a build queued behind it reads the same as a gate nobody
    /// is waiting on unless the queue time is measured, and the wait alone says
    /// nothing about whether the slot comes back — a build holding a slot for
    /// longer than anyone waits starves everyone else.
    #[tokio::test]
    async fn a_build_that_waited_reports_how_long_it_queued_and_how_long_it_held() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 30);

        let holder = gate.try_acquire("holder").await.unwrap();
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.acquire("waiter").await })
        };
        tokio::time::sleep(Duration::from_millis(600)).await;
        drop(holder);
        let permit = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter takes the freed slot")
            .unwrap()
            .unwrap();

        let worst_wait = reading(&gate, BUILD_GATE_WAIT_MS_MAX).await;
        assert!(
            worst_wait >= 400.0,
            "the wait in the queue is measured, got {worst_wait}ms"
        );
        assert!(
            reading(&gate, BUILD_GATE_WAIT_MS_TOTAL).await >= worst_wait,
            "the wait is summed over the builds that took a slot as well"
        );

        // The holder is still holding when the waiter is let in, so by the time
        // both are released the gate has seen two holds: the totals cannot come
        // out below the worst of them.
        tokio::time::sleep(Duration::from_millis(600)).await;
        drop(permit);
        let worst_hold = reading(&gate, BUILD_GATE_HELD_MS_MAX).await;
        assert!(
            worst_hold >= 400.0,
            "the hold on the slot is measured, got {worst_hold}ms"
        );
        let held_total = reading(&gate, BUILD_GATE_HELD_MS_TOTAL).await;
        assert_eq!(
            reading(&gate, BUILD_GATE_ACQUIRED_TOTAL).await,
            2.0,
            "both builds took a slot, so the denominator of the means is 2"
        );
        assert!(
            held_total >= 800.0 && held_total > worst_hold,
            "both holds are summed, got {held_total}ms against a worst of {worst_hold}ms"
        );
    }

    /// A gate that refuses a build is not a gate that made it queue: the wait
    /// that ended in a refusal is either the whole budget or, for a caller that
    /// comes back later, no wait at all, so its length is already said by which
    /// of the two it was. Summing it into the queue delay would let a polling
    /// caller that is turned away every tick drag the mean towards zero — the
    /// one number a reader would size the bound with.
    #[tokio::test]
    async fn a_wait_that_ended_in_a_refusal_is_not_a_queue_delay() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 1);

        let _held = gate.try_acquire("holder").await.unwrap();
        let waited_before = reading(&gate, BUILD_GATE_WAIT_MS_TOTAL).await;
        let worst_before = reading(&gate, BUILD_GATE_WAIT_MS_MAX).await;

        let started = Instant::now();
        assert!(
            gate.acquire("one-shot build").await.is_err(),
            "the gate is full"
        );
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "the refused caller really did wait out the budget"
        );

        assert_eq!(
            reading(&gate, BUILD_GATE_WAIT_MS_TOTAL).await,
            waited_before,
            "the refused wait was added to the queue delay"
        );
        assert_eq!(
            reading(&gate, BUILD_GATE_WAIT_MS_MAX).await,
            worst_before,
            "the refused wait was counted as the worst wait of a build that ran"
        );
    }

    /// Whether a wait of 25s is short or long depends on the budget it was
    /// allowed to use, and that number is configuration: a reader comparing a
    /// wait against a value they had to go and look up elsewhere is not reading
    /// a panel.
    #[tokio::test]
    async fn the_wait_budget_is_published_as_the_wall_a_wait_is_refused_at() {
        let dir = tempfile::tempdir().unwrap();
        let budgeted = gate(dir.path(), 1, 30);
        assert_eq!(
            reading(&budgeted, BUILD_GATE_WAIT_BUDGET_MS).await,
            30_000.0
        );

        let off = Arc::new(BuildGate::new(&BuildGateConfig {
            enabled: false,
            max_concurrent: 4,
            wait_secs: 30,
            lock_dir: dir.path().to_string_lossy().into_owned(),
        }));
        assert_eq!(
            reading(&off, BUILD_GATE_WAIT_BUDGET_MS).await,
            0.0,
            "no gate in force means no wall, not a budget nobody enforces"
        );
    }

    /// A gate that is not in force must not silently look like one that is: the
    /// slot reading is 0, and the permit says it holds nothing.
    #[tokio::test]
    async fn a_gate_out_of_force_hands_back_an_empty_permit() {
        let dir = tempfile::tempdir().unwrap();
        let disabled = Arc::new(BuildGate::new(&BuildGateConfig {
            enabled: false,
            max_concurrent: 4,
            wait_secs: 0,
            lock_dir: dir.path().to_string_lossy().into_owned(),
        }));
        assert!(!disabled.in_force());

        let permit = disabled.try_acquire("ungated build").await.unwrap();
        assert!(!permit.held());
        drop(permit);
        assert_eq!(disabled.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(disabled.refused.load(Ordering::Relaxed), 0);
        assert_eq!(
            reading(&disabled, BUILD_GATE_HELD_MS_TOTAL).await,
            0.0,
            "a build that was never gated never held a slot to report"
        );
        let slots = reading(&disabled, BUILD_GATE_SLOTS).await;
        assert_eq!(slots, 0.0, "an out-of-force gate publishes no slots");
    }

    /// A directory that cannot be created leaves the gate out of force rather
    /// than binding it to something no other builder will ever lock.
    #[tokio::test]
    async fn a_gate_that_cannot_create_its_directory_is_not_in_force() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let g = gate(&file, 1, 0);
        assert!(!g.in_force());
        assert!(!g.try_acquire("x").await.unwrap().held());
    }

    /// Whether two builders exclude each other is decided by the directory they
    /// point at, and nothing else in any reading says so. Publishing the
    /// directory's `dev:ino` is what makes "both are gating, separately" a
    /// readable state instead of an invisible one.
    #[tokio::test]
    async fn the_slot_directory_identity_is_published() {
        let dir = tempfile::tempdir().unwrap();
        let g = gate(dir.path(), 2, 0);
        let metrics = g.collect_metrics("D5").await.unwrap();
        let slots = metrics
            .iter()
            .find(|m| m.name == BUILD_GATE_SLOTS)
            .expect("the slots reading is published");
        assert_eq!(slots.value, 2.0);
        assert_eq!(
            slots.labels.get("dir").map(String::as_str),
            Some(g.identity().as_str())
        );
        assert_ne!(
            g.identity(),
            "absent",
            "the directory the gate created can be identified"
        );
        let elsewhere = tempfile::tempdir().unwrap();
        assert_ne!(
            g.identity(),
            gate(elsewhere.path(), 1, 0).identity(),
            "two directories are told apart"
        );
    }

    async fn reading(gate: &Arc<BuildGate>, name: &str) -> f64 {
        gate.collect_metrics("D5")
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("{name} is published"))
            .value
    }

    fn inactive(dir: &Path) -> Arc<dyn Observable> {
        install_for(
            &BuildGateConfig {
                enabled: true,
                max_concurrent: 4,
                wait_secs: 0,
                lock_dir: dir.to_string_lossy().into_owned(),
            },
            false,
        )
    }

    /// A process that runs no builds still has to be readable as such: the series
    /// is published at zero rather than left out, because "this deployment never
    /// reported" and "this deployment bounds nothing" are the two states an
    /// operator has to tell apart, and an absent series says neither.
    #[tokio::test]
    async fn a_process_that_runs_no_builds_publishes_the_series_at_zero() {
        let dir = tempfile::tempdir().unwrap();
        let published = inactive(dir.path()).collect_metrics("D5").await.unwrap();
        let names: Vec<&str> = published.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                BUILD_GATE_SLOTS,
                BUILD_GATE_IN_FLIGHT,
                BUILD_GATE_WAITING,
                BUILD_GATE_ACQUIRED_TOTAL,
                BUILD_GATE_REFUSED_TOTAL,
                BUILD_GATE_WAIT_MS_TOTAL,
                BUILD_GATE_WAIT_MS_MAX,
                BUILD_GATE_HELD_MS_TOTAL,
                BUILD_GATE_HELD_MS_MAX,
                BUILD_GATE_WAIT_BUDGET_MS,
            ],
            "the series set does not depend on whether this process builds"
        );
        for metric in &published {
            assert_eq!(
                metric.value, 0.0,
                "{} must read zero where no build can be admitted",
                metric.name
            );
        }
    }

    /// The count a rule reads is per-host, and a host has one builder process
    /// plus processes that run none. Both carry the directory they are talking
    /// about, so the zero rows are attributable instead of anonymous.
    #[tokio::test]
    async fn every_reading_says_which_directory_it_is_about() {
        let dir = tempfile::tempdir().unwrap();
        let builder = gate(dir.path(), 1, 0);
        for metric in builder.collect_metrics("D5").await.unwrap() {
            assert_eq!(
                metric.labels.get("dir").map(String::as_str),
                Some(builder.identity().as_str()),
                "{} is not attributed to a directory",
                metric.name
            );
            assert_eq!(
                metric.labels.get("role").map(String::as_str),
                Some(BUILD_GATE_ROLE_BUILDER),
                "{} is not attributed to the role that published it",
                metric.name
            );
        }
        let no_builder = inactive(dir.path());
        for metric in no_builder.collect_metrics("D5").await.unwrap() {
            assert!(
                metric.labels.contains_key("dir"),
                "{} carries no directory",
                metric.name
            );
            assert_eq!(
                metric.labels.get("role").map(String::as_str),
                Some(BUILD_GATE_ROLE_CONTROL_PLANE),
                "{} must not read as a builder's row",
                metric.name
            );
        }
    }

    /// The two rules, each asserted against the series this module publishes:
    /// renaming either side leaves both ends self-consistent and the signal gone.
    #[test]
    fn deployed_rules_query_the_metrics_this_module_publishes() {
        let rules = chart_rules();
        let find = |name: &str| -> String {
            rules
                .iter()
                .find(|(rule, _)| rule == name)
                .map(|(_, promql)| promql.clone())
                .unwrap_or_else(|| panic!("rule {name} missing"))
        };

        let refused = find(BUILD_GATE_RULE);
        assert!(
            refused.contains(BUILD_GATE_REFUSED_TOTAL),
            "rule {BUILD_GATE_RULE} must query {BUILD_GATE_REFUSED_TOTAL}, got: {refused}"
        );

        // A builder whose gate is not in force is the state the refusal counter
        // cannot show: nothing is being refused, because nothing is being
        // bounded. The rule has to select builders by role, or the zero rows the
        // control plane publishes would keep it firing everywhere.
        let not_in_force = find(BUILD_GATE_IN_FORCE_RULE);
        assert!(
            not_in_force.contains(BUILD_GATE_SLOTS),
            "rule {BUILD_GATE_IN_FORCE_RULE} must query {BUILD_GATE_SLOTS}, got: {not_in_force}"
        );
        assert!(
            not_in_force.contains(&format!("\"{BUILD_GATE_ROLE_BUILDER}\"")),
            "rule {BUILD_GATE_IN_FORCE_RULE} must select builders by role, got: {not_in_force}"
        );
    }

    fn chart_rules() -> Vec<(String, String)> {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        root.pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present")
            .iter()
            .map(|r| {
                (
                    r["name"].as_str().expect("rule has a name").to_string(),
                    r["promql"].as_str().expect("rule has a promql").to_string(),
                )
            })
            .collect()
    }
}
