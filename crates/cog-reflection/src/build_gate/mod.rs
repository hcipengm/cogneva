//! A host-wide gate on how many builds run at once.
//!
//! The gate lives here, beside the builds it bounds, and not in `cog-core`:
//! what it does is take locks and sample the host, which is the business of the
//! crate that runs builds. The names it publishes under, the identity of the
//! slot directory and the registration point through which the rest of the
//! workspace reaches the installed gate are in `cog_core::contract::build_gate`
//! -- a crate that only reads these series must not have to depend on the
//! machinery that produces them.
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
//! How many builds that bound admits is **derived from what the machine the
//! builds run on is measured doing**, not configured: cpu, memory and the disk
//! each say how many more builds they can absorb right now, the container's own
//! memory limit says the same about the cgroup the builds run in, and the
//! tightest of them sets the ceiling. Configuration keeps an override for the
//! operator who knows the machine better than the derivation does; it is not how
//! the number is arrived at. The derivation is a reading of the dimension that
//! is binding, so a bound that is at one because the disk is saturated reads
//! differently from one that is at one because memory is, and the same host
//! gives a different answer when it is idle. Nothing here is a constant about
//! which machine this is.
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
//! And two kinds of holder, which want different things from the gate. A build
//! wants one slot: it is one of the things the bound bounds. The two callers
//! that are not builds — restarting the registry a build is pushing to, and
//! removing cache files from under a build that would then fail for a reason
//! that has nothing to do with it — want *every* build out, and a slot cannot
//! promise that: a slot is a share of the bound, so a holder of one keeps the
//! others out only while the bound happens to be one, and a bound that moves
//! with the host would arm and disarm them without moving a single reading.
//! Exclusions therefore take a lock of their own, which every build holds for as
//! long as it runs: the exclusion is a property of the file rather than of how
//! many slots there are, and it says the same thing at any bound.
//!
//! That lock is also what makes an exclusion *reachable*. Asking for every slot
//! meant winning a race against the builders, and on a host where builds run
//! back to back with each other the sweep never won: it lost its turn to each
//! build that arrived while it was asking, which is every build, so the cache it
//! was there to trim grew without bound while its refusals all read as "the host
//! is busy building". A single lock can be asked for and *waited* on instead of
//! raced for, and the kernel hands it to the waiter as soon as the running build
//! releases it — ahead of builds that ask later. [`BuildGate::try_acquire_exclusive`]
//! and [`BuildGate::acquire_exclusive`] are those two asks, and what such a hold
//! counts as is its own reading rather than a build's.
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

pub mod arming;

pub use arming::{effective_config, effective_gate_for};

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use cog_core::admission::{sample, Pressure};
use cog_core::build_cache_readings::CacheSlot;
use cog_core::config::BuildGateConfig;
use cog_core::contract::build_gate::*;
use cog_core::contract::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::contract::resource_ledger::{ResourceEvent, ResourceEventKind, ResourceEventOutcome};
use cog_core::{SFError, SFResult};

/// How much of one cpu a build of this workspace draws on the host, over its
/// life.
///
/// Measured by walking the build's whole process tree — the `cargo` root and
/// every process whose parent chain reaches it, which is where the parallelism
/// lives — and dividing the sum of its `utime+stime` deltas by the wall clock
/// between samples. One release build of this workspace (`cargo build --release
/// --bin cogneva`, the build a mainline advance runs), sampled every five
/// seconds for fourteen minutes, 31 samples: median 4.1 cores, peak 4.7. The
/// median and not the peak, because the ceiling asks how many builds fit
/// *alongside* each other, and sizing the bound on the burst would refuse builds
/// the host can in fact run.
///
/// This is what one of *these* builds costs the host, which is the unit the
/// bound is denominated in. The build runs in a container with a four-core cpu
/// quota, so `cargo` sizes its job count to that quota and the tree fills it
/// rather than the whole host; the number is a property of this deployment, and
/// one that gives its builder a larger share will measure a larger number.
///
/// Only this kind of build was sampled. The dev-profile builds (clippy, test)
/// drive fewer and smaller `rustc` processes over the same tree and no series
/// for one exists yet, so the first dev-profile series should confirm or correct
/// this rather than the number standing for all three kinds on faith.
const BUILD_DEMAND_CORES: f64 = 4.1;
/// Peak memory one build of this workspace holds, measured the same way: the
/// high-water mark of summed `VmRSS` over the build's process tree.
///
/// The tree and not the `cargo` process: the memory is in the `rustc` children
/// and in the test binaries, and a build that pages the host is never the
/// parent. That series peaked at 4481 MiB with a median of 3616 MiB, and the
/// peak is what is used here because memory is the one dimension where the
/// failure is not a slowdown: a container that runs out kills the build.
///
/// Compared against `MemAvailable` rather than `MemFree`, because the page cache
/// this build is filling counts as available.
const BUILD_PEAK_RSS_BYTES: u64 = 4481 * 1024 * 1024;
/// Memory the rest of the host must keep, measured as the lowest `MemAvailable`
/// seen over seven days of samples: the floor is what the machine has actually
/// been down to with the cluster and the sessions on it, and a bound that spends
/// below it is spending memory something else is using.
///
/// The window's minimum was 36.5 GiB and its 5th percentile 47.2 GiB, so this
/// line sits at the observed trough: it is crossed only when the host is back
/// where its worst moment was, not when it is merely busier than usual.
const MEM_FLOOR_BYTES: u64 = 36 * 1024 * 1024 * 1024;
/// Cpu stall, as a percentage, below which the host counts as having room.
///
/// `cpu some avg60` over the same seven days had a median of 1.5% and a maximum
/// of 20%: 5% is a line the host crosses rarely and that a build's own demand
/// does not sit above, so it does not make the bound a function of itself.
const CPU_CALM_PCT: f64 = 5.0;
/// Disk stall, as a percentage, the builds together may cost.
///
/// This is the dimension that binds on a host where builds and containers share
/// one device. What one build costs it depends on what the build is doing: over
/// a release build of an already-built tree the device read `io some avg10` of
/// 0.00-0.01%, a build's cold compile phase has been measured driving the same
/// reading to 30-58%, and the worst episode over seven days reached 87%. The
/// budget sits above the top of what one build has been seen to cost, so a build
/// cannot pin the bound by its own noise, and below what two of them have been
/// seen to cost together. Dividing it by the stall measured now gives how many
/// builds the device is absorbing.
const IO_STALL_BUDGET_PCT: f64 = 70.0;
/// Disk stall below which the device counts as quiet, so the bound may rise.
/// Over seven days `io some` had a median of 0.1% and the host was quiet for
/// most of the window: rising is slow and only on a quiet device, because the
/// cost of being wrong is asymmetric.
const IO_CALM_PCT: f64 = 1.0;
/// Memory stall below which the host counts as having room, on the same seven
/// days (median 0%, worst episode 16%).
const MEM_CALM_PCT: f64 = 1.0;
/// How long the control law waits between moves of the persisted bound.
///
/// The readings are taken continuously but the decision is not: a bound that
/// chased every sample would move every time a build's own compile phase pushed
/// the device, which is the chase the `avg60` window and this one exist to
/// damp. A build is a tens-of-minutes event, so a minute of lag costs nothing
/// next to the stability.
const BOUND_WINDOW: Duration = Duration::from_secs(60);
/// How long the cpu is sampled to read what it is doing: two reads of the
/// counters, this far apart. Load average would be the wrong number here — a
/// host with processes blocked on the disk shows a load of twenty with idle
/// cores, and this dimension is about cores.
const CPU_SAMPLE_GAP: Duration = Duration::from_millis(200);

/// Where the derived bound is persisted, so every process on the host enforces
/// the same number and a process that just started does not spend its first
/// window building against a bound it has not derived yet.
const BOUND_FILE: &str = "buildgate.bound";
/// The file whose lock elects the one process that moves the bound.
///
/// One writer because the derivation is a reading of the host and the host is
/// one machine: two processes each writing their own sample would leave two
/// builders enforcing two different numbers.
const CONTROLLER_FILE: &str = "buildgate.controller";
/// The file every build holds shared and every exclusion holds alone.
///
/// Its own file rather than the slot files, so an exclusion is a promise the
/// caller can wait on rather than a race it has to win against whoever asks for
/// a slot next.
const CLAIM_FILE: &str = "builds.claim";
/// The file a work that keeps being turned away leaves to claim the next build.
///
/// A caller that comes back on its own — the deployer's cycle, the cache sweep's
/// walk — takes its turn by asking again, and on a host where builds run back to
/// back the answer is taken every time by whoever asks a moment earlier. The
/// work that is *waiting* is the one that never wins that race, and it is the one
/// with the most standing to run: it has been waiting longest. So a refusal is
/// not the end of the ask — the refused work says here that it is still waiting,
/// builds that find this fresh stand down rather than start, and the running
/// build finishing frees the field for the work that was waiting instead of for
/// the next arrival.
///
/// Freshness is the whole liveness rule, and it is a good one here: what this
/// file claims is that its author is still asking, and an author that stops
/// asking stops refreshing it, so the claim lapses on its own. The cost is
/// bounded by that window — a work that goes away mid-claim holds builds out for
/// no more than the window — and it is paid only while some build is turned away
/// by it, which is the state this exists to end.
const RESERVE_FILE: &str = "builds.reserve";
/// How long a claim on the next build is honored after the claim was last
/// refreshed. Longer than the cycles that refresh it (the deployer's is ten
/// minutes) so a live waiter does not lose its turn between two asks, and short
/// enough that a waiter that stops asking gives the field back within the same
/// order of time.
const RESERVE_FRESH: Duration = Duration::from_secs(900);

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
            // The same object, reachable from `cog-core`: the cache pass reads
            // it as its slot and the resource account drains the events it
            // queued, and neither of them can name this crate.
            install_global(Arc::new(RegisteredGate(Arc::clone(&gate))));
            if gate.in_force() {
                tracing::info!(
                    configured = cfg.max_concurrent,
                    derived = gate.fixed.is_none(),
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

/// The host-wide build gate, handed to the cache pass as its exclusion.
///
/// The wrapper rather than a bare `Arc<BuildGate>` because `CacheSlot` is
/// `cog-core`'s trait and neither it nor `Arc` is this crate's to implement
/// against: the hold, the lock it takes and the refusal it reports are the
/// gate's own, and this only names them.
pub struct Exclusion(pub Arc<BuildGate>);

/// The host-wide build gate *is* the slot for the process that runs a
/// deployment's own builds: it is what bounds how many of them run at once,
/// and a pass excludes itself from all of them rather than from one, because
/// the files it deletes are ones any build could be reading.
#[async_trait]
impl CacheSlot for Exclusion {
    fn in_force(&self) -> bool {
        self.0.in_force()
    }

    async fn take(&self) -> Option<Box<dyn Send + Sync>> {
        match self.0.try_acquire_exclusive("build-cache-reclaim").await {
            Ok(permit) => Some(Box::new(permit)),
            Err(_) => None,
        }
    }
}

struct RegisteredGate(Arc<BuildGate>);

#[async_trait]
impl ProcessGate for RegisteredGate {
    fn in_force(&self) -> bool {
        self.0.in_force()
    }

    /// The host-wide build gate *is* the slot for the process that runs a
    /// deployment's own builds: it is what bounds how many of them run at once,
    /// and a pass excludes itself from all of them rather than from one,
    /// because the files it deletes are ones any build could be reading.
    async fn take_for_reclaim(&self) -> Option<Box<dyn Send + Sync>> {
        match self.0.try_acquire_exclusive("build-cache-reclaim").await {
            Ok(permit) => Some(Box::new(permit)),
            Err(_) => None,
        }
    }

    fn drain_events(&self) -> Vec<ResourceEvent> {
        BuildGate::drain_events(&self.0)
    }

    fn record_event(&self, event: ResourceEvent) {
        BuildGate::record_event(&self.0, event)
    }

    fn events_dropped(&self) -> u64 {
        BuildGate::events_dropped(&self.0)
    }
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
        // No slots held either: a process that runs no builds takes no lock, so
        // the count of locks is not its to report -- and it counts locks in a
        // directory it does not mount.
        Ok(readings(
            0,
            0,
            0,
            DIM_NONE,
            Counters::default(),
            Origin {
                dir: &dir_identity(Path::new(&self.dir)),
                role: BUILD_GATE_ROLE_CONTROL_PLANE,
            },
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
    excluded: u64,
    waiting: u64,
    acquired: u64,
    /// One count per ask and per wall, indexed by [`asker_index`] and
    /// [`RefusedBy::index`], so the refusal counter publishes a fixed series set
    /// rather than one that grows with the walls a process happens to have hit
    /// or the works it happens to have refused.
    refused: [[u64; RefusedBy::ALL.len()]; ASKER_CELLS],
    wait_ms_total: u64,
    wait_ms_max: u64,
    held_ms_total: u64,
    held_ms_max: u64,
}

/// What the host is doing, as the dimensions the bound is derived from.
struct HostLoad {
    pressure: Pressure,
    /// Cores the host was busy with over the sample, counting neither a core that
    /// was idle nor one that was waiting on the disk: a core blocked on IO is a
    /// core a build can have, and folding it in would read a disk problem as a
    /// cpu one and hold the bound down for the wrong reason.
    busy_cores: f64,
    /// Cores this host has, counted from the same file the busy time came from.
    cores: usize,
    /// The ceiling of the cgroup the builds run in, when they run in one.
    ///
    /// None for a process whose cgroup has no limit of its own, which is the
    /// case on a host shell and the case the host's own dimensions already
    /// describe.
    budget: Option<BuildBudget>,
}

/// The ceiling of the cgroup this process's builds run in.
///
/// Only the memory limit is read, and that is a decision rather than a
/// simplification. The two cgroup ceilings fail differently: a cpu quota is a
/// throttle, so a build that has to share it runs slower and still finishes,
/// which is the queue's business and shows up in the wait readings; a memory
/// limit is a wall, so a build over it is killed, which is a change that failed
/// for a reason that has nothing to do with the change. A bound set from the
/// host alone would not see that wall: measured on this deployment's evolution
/// cgroup — 4 cores and, when this was measured, 8 GiB — one build of the
/// workspace peaks at 4.7 GiB of summed `VmRSS` with 1.7 GiB of anonymous
/// memory already resident, which put a
/// second concurrent build past that limit while the host had tens of gigabytes
/// free. The peak is the wall this dimension is sized against, and it does not
/// move when the limit does: what the limit decides is how many builds fit under
/// it. The manifest's own number is therefore the whole of the fix here — this
/// dimension reports the wall, it cannot open it.
struct BuildBudget {
    /// Bytes the cgroup may use in total, and bytes charged to it now.
    mem_max: u64,
    mem_current: u64,
    /// How much of `mem_current` the kernel can take back on demand: the page
    /// cache the cgroup holds as a result of having read files, which is what a
    /// cgroup that has built before is mostly holding.
    mem_reclaimable: u64,
}

/// Bytes of a cgroup's charge the kernel can give back on demand, read out of
/// the cgroup's own `memory.stat`.
///
/// The whole `file` line, not the `inactive_file` part of it. Which pages sit on
/// the kernel's active list says what reclaim would cost — a page read recently
/// enough to be kept is one that has to be read again — and not whether the
/// memory is available: under pressure the kernel demotes the active list and
/// gives those pages back the same way. Reading the narrow line as the free part
/// is what pinned this dimension at one on this deployment: measured
/// 2026-10-13 on the evolution cgroup, a 16 GiB limit held 15.5 GiB charged, of
/// which 14.0 GiB was file pages — 3.5 inactive and 10.5 active — so the
/// arithmetic left 0.93 of a build. One build of the workspace peaks at 4481
/// MiB, the cgroup had never reached its limit once (`memory.events max 0`), and
/// the bound was one against a wall no reading showed.
///
/// Zero when the file does not name it, which counts the whole charge as
/// unreclaimable — the reading this dimension used before it knew the
/// difference, so nothing loses a bound it had.
fn reclaimable_file_bytes(stat: &str) -> u64 {
    stat.lines()
        .find_map(|line| line.strip_prefix("file "))
        .and_then(|bytes| bytes.trim().parse().ok())
        .unwrap_or(0)
}

/// The mount the cgroup filesystem is read from on this host.
///
/// A cgroup namespace makes the process's own cgroup be `/` inside this mount, so
/// joining the path out of `/proc/self/cgroup` onto it is right whether the
/// container has a namespace of its own or not.
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

impl BuildBudget {
    /// Reads this process's cgroup, if it has one and it is bounded.
    ///
    /// The builds are this process's children and inherit its cgroup, which is
    /// what makes this the limit that kills them. That inheritance is the whole
    /// of the reason the two are the same, so a builder that moved its children
    /// into a cgroup of their own — a scope per build, a sidecar — would leave
    /// this reading a limit nothing runs under, and the label would go on saying
    /// `cgroup_memory` about a number that had stopped describing the builds.
    fn read() -> Option<Self> {
        // Either a full path (`/kubepods.slice/...`) or `/` when the container
        // has its own cgroup namespace.
        let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = text
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .unwrap_or("/");
        Self::read_at(Path::new(CGROUP_ROOT), path.trim())
    }

    /// Reads one cgroup out of a given filesystem root.
    ///
    /// The root is an argument rather than the constant itself so a test can aim
    /// it at a fixture and move the limit between two reads. Nothing is
    /// remembered between calls: the limit is re-derived from the files every
    /// time, which is what lets a container whose memory limit is raised
    /// mid-life stop being bound by the number it started with.
    fn read_at(root: &Path, cgroup_path: &str) -> Option<Self> {
        let base = root.join(cgroup_path.trim_start_matches('/'));
        let mem_max = read_u64_file(&base.join("memory.max"))?;
        let mem_current = read_u64_file(&base.join("memory.current"))?;
        // A cgroup whose accounting cannot be read is charged in full, which is
        // the reading this dimension had before it could tell cache apart.
        let mem_reclaimable = std::fs::read_to_string(base.join("memory.stat"))
            .map(|stat| reclaimable_file_bytes(&stat))
            .unwrap_or(0);
        Some(Self {
            mem_max,
            mem_current,
            mem_reclaimable,
        })
    }

    /// How many more builds the cgroup's limit can hold.
    ///
    /// What is left of the limit less what the kernel cannot take back, rather
    /// than what is left of the limit less everything charged to it: on a cgroup
    /// that has built before, most of `memory.current` is the page cache of the
    /// files the build itself read back — read against a 16 GiB limit, 14 GiB of
    /// it charged and 12 GiB of that page cache, so the charge the kernel cannot
    /// hand back is 2 GiB and what is left is room for three builds. The same
    /// reading taken against everything charged leaves 0.45 of a build, which is
    /// room for none of one.
    ///
    /// A dimension pinned at one is not a reading: it would name itself the wall
    /// on every settled cgroup and hide the dimensions that do move, and
    /// reclaiming that cache is IO, which the io dimension is the one charged
    /// for — spending only one budget for it is what keeps that charge from
    /// being paid twice.
    fn ceiling(&self) -> f64 {
        self.mem_max
            .saturating_sub(self.mem_current.saturating_sub(self.mem_reclaimable)) as f64
            / BUILD_PEAK_RSS_BYTES as f64
    }
}

/// A `u64` out of a one-value cgroup file, without a value it does not hold:
/// `max` means the cgroup is unbounded, which is not a ceiling of zero.
fn read_u64_file(path: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    text.trim().parse().ok()
}

impl HostLoad {
    /// Reads the host, or reports that it could not be read.
    ///
    /// There is no default here. A dimension that cannot be read is
    /// indistinguishable from one that is idle, and a bound derived from a
    /// stand-in would be exactly the kind of number this derivation exists to
    /// stop shipping, so the caller keeps the bound it already has instead.
    async fn measure(dir: &Path) -> Option<Self> {
        let pressure = sample(dir)?;
        let (cores, busy, total) = cpu_times()?;
        // Two reads, one gap: the counters are cumulative since boot, so the
        // question "is the cpu busy" is only answered by a difference.
        tokio::time::sleep(CPU_SAMPLE_GAP).await;
        let (_, busy_after, total_after) = cpu_times()?;
        let ticks = total_after.saturating_sub(total);
        if ticks == 0 {
            return None;
        }
        let busy_cores = busy_after.saturating_sub(busy) as f64 / ticks as f64 * cores as f64;
        Some(Self {
            pressure,
            busy_cores,
            cores,
            budget: BuildBudget::read(),
        })
    }

    /// The most builds this machine could take right now, and the dimension that
    /// is setting it.
    ///
    /// Each dimension answers "how many builds fit in what is left of me", and
    /// the smallest answer is what the machine can absorb. They are floored at
    /// one rather than at zero: a machine with no room runs one build and queues
    /// the rest, because a bound of zero refuses every build forever, and a
    /// machine that never does the work it has is a worse failure than a busy
    /// one. None of them is allowed to count above the number of cores, which is
    /// where builds stop adding throughput and start taking it from each other.
    ///
    /// The machine is the host for a deployment that builds on the host, and the
    /// narrower of the host and the cgroup for one that builds inside a
    /// container: the host's readings are the right answer to "paging the host",
    /// and the cgroup's limit is the right answer to "killing the build", and a
    /// bound has to answer both.
    fn ceiling(&self) -> (usize, &'static str) {
        let cores = self.cores.max(1) as f64;
        let cpu = ((cores - self.busy_cores) / BUILD_DEMAND_CORES).clamp(1.0, cores);
        let memory = (self
            .pressure
            .mem_available_bytes
            .saturating_sub(MEM_FLOOR_BYTES) as f64
            / BUILD_PEAK_RSS_BYTES as f64)
            .clamp(1.0, cores);
        // The disk is charged against the stall it is showing now: the builds it
        // is absorbing are the budget divided by that, so the same device reads
        // as room for several builds when it is quiet and for one when a build
        // is already keeping it busy.
        let io = (IO_STALL_BUDGET_PCT / self.pressure.io_avg60.max(0.01)).clamp(1.0, cores);
        let cgroup_memory = self
            .budget
            .as_ref()
            .map(|b| b.ceiling().clamp(1.0, cores))
            .unwrap_or(cores);

        let mut tightest = cpu;
        let mut dimension = DIM_CPU;
        for (factor, name) in [
            (memory, DIM_MEMORY),
            (io, DIM_IO),
            (cgroup_memory, DIM_CGROUP_MEMORY),
        ] {
            if factor < tightest {
                tightest = factor;
                dimension = name;
            }
        }
        (tightest.floor().max(1.0) as usize, dimension)
    }

    /// Whether the host has been quiet enough for the bound to be allowed to
    /// rise.
    ///
    /// Rising is the direction that has to be earned: the cost of a bound that is
    /// one too high is the whole host degrading at once, and the cost of one that
    /// is one too low is a build waiting, so the bound comes down to the ceiling
    /// the moment the host is busy and goes back up only once it has been quiet
    /// on every dimension that can be watched.
    ///
    /// The container's own memory limit is not among those dimensions, and does
    /// not need to be: it is a cap on the ceiling rather than a sign of how busy
    /// the machine is, and it is already folded into the ceiling this decides
    /// whether to rise to. A ceiling the container has brought down to one stays
    /// one whichever way this goes, so consulting it here would only be a second
    /// place for the same number to be read from.
    fn is_calm(&self) -> bool {
        self.pressure.cpu_avg60 < CPU_CALM_PCT
            && self.pressure.io_avg60 < IO_CALM_PCT
            && self.pressure.mem_avg60 < MEM_CALM_PCT
            && self.pressure.mem_available_bytes
                > MEM_FLOOR_BYTES.saturating_add(BUILD_PEAK_RSS_BYTES)
    }
}

/// The dimension a ceiling was last set by, as the `dimension` label.
const DIM_CPU: &str = "cpu";
const DIM_MEMORY: &str = "memory";
const DIM_IO: &str = "io";
/// The memory limit of the cgroup the builds run in. Its own dimension rather
/// than another `memory`: a bound of one because the host is short of memory and
/// one because the container's own limit is in sight are fixed by different
/// things — the first by whoever else is on the host, the second by the limit in
/// the deployment's own manifest — and the number alone cannot tell them apart.
const DIM_CGROUP_MEMORY: &str = "cgroup_memory";
/// Not a dimension of the host: a bound the operator set, which is the one bound
/// that is not derived from a measurement.
const DIM_OVERRIDE: &str = "override";
/// The label of a gate that has to measure the host before it can say what the
/// host can take. Different from a ceiling of zero, which is a measurement.
const DIM_UNMEASURED: &str = "unmeasured";
/// The label for a process that derives nothing because it bounds nothing.
const DIM_NONE: &str = "none";

/// The bound to enforce next, from the bound in force and what the host is
/// showing.
///
/// Falling is immediate and rising is not: a bound above the ceiling comes down
/// to it at once, and a bound at or below it only rises on a quiet host. That
/// asymmetry is the whole control law; everything it moves on is the ceiling's
/// own reading, taken again each window, so there is no second opinion about the
/// host to disagree with the first.
fn next_bound(bound: usize, ceiling: usize, calm: bool) -> usize {
    if calm { ceiling } else { bound.min(ceiling) }.max(1)
}

/// Cores this host has, and its cpu time so far as `(busy, total)`.
///
/// `busy` excludes idle and IO wait, since a host whose cores are idle while
/// processes queue on the disk has spare cpu whatever its load average says.
fn cpu_times() -> Option<(usize, u64, u64)> {
    let text = std::fs::read_to_string("/proc/stat").ok()?;
    let mut cores = 0usize;
    let mut busy = 0u64;
    let mut total = 0u64;
    for line in text.lines() {
        let Some(label) = line.split_whitespace().next() else {
            continue;
        };
        let times: Vec<u64> = line
            .split_whitespace()
            .skip(1)
            .filter_map(|f| f.parse().ok())
            .collect();
        if times.len() < 4 {
            continue;
        }
        if label == "cpu" {
            let idle = times[3];
            let iowait = times.get(4).copied().unwrap_or(0);
            // The first eight fields only. The line continues with `guest` and
            // `guest_nice`, which are time already counted inside `user` and
            // `nice`: adding them again would inflate the denominator and read a
            // busy host as a quieter one, on exactly the hosts where the number
            // matters most.
            total = times.iter().take(8).sum();
            busy = total.saturating_sub(idle).saturating_sub(iowait);
        } else if label.len() > 3 && label.starts_with("cpu") {
            cores += 1;
        }
    }
    if cores == 0 || total == 0 {
        return None;
    }
    Some((cores, busy, total))
}

/// Where a set of readings came from: the directory the gate bounds and the side
/// of the gate this process is on.
///
/// One value rather than two, because the two label every metric in the set and
/// neither means anything without the other: a `role` with no `dir` names a side
/// of no particular gate, and two gates on one host would merge into one series.
#[derive(Clone, Copy)]
struct Origin<'a> {
    dir: &'a str,
    role: &'a str,
}

/// The whole series set, in one place: a reader that has to tell "no bound here"
/// from "this deployment never reported" cannot do it if the two sources of the
/// set can drift.
fn readings(
    slots: usize,
    occupied: usize,
    ceiling: usize,
    dimension: &str,
    c: Counters,
    origin: Origin<'_>,
    wait_budget_ms: u64,
) -> Vec<RawMetric> {
    let label = |m: RawMetric| {
        m.with_label("dir", origin.dir.to_string())
            .with_label("role", origin.role.to_string())
    };
    let mut out = vec![
        label(RawMetric::new(BUILD_GATE_SLOTS, slots as f64)),
        label(RawMetric::new(BUILD_GATE_CEILING, ceiling as f64))
            .with_label("dimension", dimension.to_string()),
        label(RawMetric::new(BUILD_GATE_OCCUPIED, occupied as f64)),
        label(RawMetric::new(BUILD_GATE_IN_FLIGHT, c.in_flight as f64)),
        label(RawMetric::new(BUILD_GATE_EXCLUDED, c.excluded as f64)),
        label(RawMetric::new(BUILD_GATE_WAITING, c.waiting as f64)),
        label(RawMetric::new(BUILD_GATE_ACQUIRED_TOTAL, c.acquired as f64)),
    ];
    // One row per wall and per ask, all of them always, so a reader summing the
    // counter over either label gets the same total the one-number version
    // published and the series set depends on neither the walls this process has
    // hit nor the works it has refused. The ask is the outer loop because the
    // wall is the older axis and its rows keep their order.
    for wall in RefusedBy::ALL {
        for asker in 0..ASKER_CELLS {
            out.push(
                label(RawMetric::new(
                    BUILD_GATE_REFUSED_TOTAL,
                    c.refused[asker][wall.index()] as f64,
                ))
                .with_label("reason", wall.as_str().to_string())
                .with_label("asker", asker_label(asker).to_string()),
            );
        }
    }
    out.extend([
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
    ]);
    out
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

/// Takes every slot for work that must not overlap a build, refusing at once
/// when any slot is taken.
///
/// For a caller that is not a build and whose exclusion is the point of the
/// work: while the permit is held no build can start, and the ones already
/// running have finished. `Ok(None)` means no gate is in force here, so there
/// is nothing to exclude builds from — the caller proceeds, as it does for
/// [`try_acquire`], since a deployment that bounds no builds has no bound to
/// take a turn on either.
pub async fn try_acquire_exclusive(what: &str) -> SFResult<Option<BuildPermit>> {
    match global() {
        Some(gate) => gate.try_acquire_exclusive(what).await.map(Some),
        None => Ok(None),
    }
}

/// Takes every slot for work that must not overlap a build, waiting for a
/// running build to finish.
///
/// The waiting form of [`try_acquire_exclusive`], for work whose turn cannot be
/// lost: a caller that is retried by something else can refuse and come back, a
/// process switch cannot -- its staged binary stays staged until some later
/// change asks for another switch.
pub async fn await_exclusive(what: &str, budget: Duration) -> SFResult<Option<BuildPermit>> {
    match global() {
        Some(gate) => gate.await_exclusive(what, budget).await.map(Some),
        None => Ok(None),
    }
}

/// Which wall turned a build away, as the values the refusal counter carries.
///
/// The counter used to be one number, and the walls behind it do not have the
/// same owner: a host whose slots are all busy is a capacity reading, a sweep or
/// a registry restart holding the field is maintenance standing the builds down
/// on purpose, and another work's reservation is the queue working as it should.
/// One number over all four says "a build was refused" and cannot say which —
/// the same shape the CI reader's no-verdict counter carries its reasons to
/// avoid. The set is closed at four so the series set stays fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefusedBy {
    /// The caller asked not to wait, and no slot was free.
    Busy,
    /// The wait budget ran out with every slot still taken.
    WaitedOut,
    /// A build-cache sweep or a registry restart was holding the field.
    Excluded,
    /// Another work had been turned away and claimed the next build.
    Reserved,
}

impl RefusedBy {
    /// Every wall, in the order the counter publishes them.
    const ALL: [RefusedBy; 4] = [Self::Busy, Self::WaitedOut, Self::Excluded, Self::Reserved];

    /// This wall's own words, said the same way every time it causes a refusal.
    ///
    /// `slots` and `wait` are the two walls that are a matter of counts; the
    /// other two are walls in name only and say their sentence regardless.
    fn reason(self, slots: usize, wait: Duration) -> String {
        match self {
            RefusedBy::Busy => format!("all {slots} build slots are taken"),
            RefusedBy::WaitedOut => format!(
                "all {slots} build slots were taken for the whole {}s wait budget",
                wait.as_secs()
            ),
            RefusedBy::Excluded => {
                "a build-cache sweep or a registry restart is holding the field".to_string()
            }
            RefusedBy::Reserved => "another work claimed the next build".to_string(),
        }
    }

    /// The value a reader sees, which is the whole of what an alert says about
    /// the cause: a bare token with no sentence around it.
    ///
    /// Named for what happened rather than for the variant, because the two
    /// walls that are not slot counts say nothing on their own.
    /// `maintenance_hold` rather than `excluded` because the
    /// `cogneva_build_gate_excluded` gauge is a different subject: it counts
    /// builds held out of the gate at this instant, where this label says why
    /// one build was turned away. A refused build found the field held, it was
    /// not itself held out of the gate, and one word for the two would say they
    /// are the same thing.
    fn as_str(self) -> &'static str {
        match self {
            RefusedBy::Busy => "busy",
            RefusedBy::WaitedOut => "waited_out",
            RefusedBy::Excluded => "maintenance_hold",
            RefusedBy::Reserved => "queue_ahead",
        }
    }

    fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|wall| *wall == self)
            .expect("every wall is in ALL")
    }
}

/// The works that ask this gate for a slot, and the value each is counted
/// under.
///
/// The `reason` label says which wall turned an ask away and cannot say who was
/// asking, and the walls do not fall on the works alike. On a host with one
/// slot, a single long `change verification` hold refuses `mainline advance`
/// every time it comes back: the rollout is held for as long as that hold lasts
/// and the counter cannot tell that from a build that merely arrived early.
/// Read the other way it is the same gap — refusals under `maintenance_hold`
/// say the sweeps and the builds are racing, but not which of the builds paid
/// for it, and a work starved every round is exactly the one a reader has to
/// find.
///
/// The set is closed, which is what makes the label admissible where the
/// caller's own words are not. `what` is free text — it is also the name a
/// holder writes into its slot file and the name in the refusal's warn line —
/// and a free text as a label value would grow the series set with every new
/// call site, which is the growth the closed set exists to bound. A `what` that
/// is not one of these words is counted under [`BUILD_GATE_ASKER_UNNAMED`]
/// rather than dropped: summed over `asker` the counter is then the number it
/// carried before this label existed, and a call site asking under words that
/// are not here shows up as an unnamed row with a number in it instead of
/// vanishing into the whole.
///
/// Each pair is `(the words the call site asks with, the value the reading
/// carries)`. It is a table rather than a type because `what` is a `&str` at
/// every call site and most of them are tests asking as themselves — a test
/// that asks as `"a build in the first slot"` is counted under the unnamed
/// cell, which is what it is. The table is the whole of the vocabulary, so a
/// caller that starts asking under new words is a reader's problem and not a
/// silent one: the unnamed cell moves.
pub const BUILD_GATE_ASKERS: [(&str, &str); 9] = [
    ("evolution build", "evolution_build"),
    ("baseline evaluation", "baseline_evaluation"),
    ("change verification", "change_verification"),
    ("change lint verification", "change_lint_verification"),
    ("mainline advance", "mainline_advance"),
    ("registry gc reissue", "registry_gc_reissue"),
    ("registry maintenance", "registry_maintenance"),
    ("buildah store reclaim", "buildah_store_reclaim"),
    ("build-cache-reclaim", "build_cache_reclaim"),
];

/// The value an ask carries when it asks under words
/// [`BUILD_GATE_ASKERS`] does not list.
pub const BUILD_GATE_ASKER_UNNAMED: &str = "unnamed";

/// How many asks the refusal counter publishes per wall: the named works and
/// the unnamed one.
const ASKER_CELLS: usize = BUILD_GATE_ASKERS.len() + 1;

/// The cell an ask is counted in, in the order [`BUILD_GATE_ASKERS`] lists the
/// works; anything else is the last cell.
fn asker_index(what: &str) -> usize {
    BUILD_GATE_ASKERS
        .iter()
        .position(|(words, _)| *words == what)
        .unwrap_or(BUILD_GATE_ASKERS.len())
}

/// The value the ask in cell `index` is published under.
fn asker_label(index: usize) -> &'static str {
    match BUILD_GATE_ASKERS.get(index) {
        Some((_, label)) => label,
        None => BUILD_GATE_ASKER_UNNAMED,
    }
}

/// Says that this process's work is waiting for the next build.
///
/// The free-function half of [`BuildGate::reserve_build_field`], for a caller
/// that reaches the gate through [`global`] rather than by holding it.
pub fn reserve_build_field(what: &str) {
    if let Some(gate) = global() {
        gate.reserve_build_field(what);
    }
}

/// How many events a gate holds before it starts dropping the oldest.
///
/// Sized for a consumer that ticks every few minutes: a refusal burst is a few
/// dozen asks, and a bound moves at most once per measurement window. The number
/// is not a policy about how many events matter -- anything this large means the
/// consumer is not running, which is what the dropped count is for.
const EVENT_RING_CAPACITY: usize = 256;

/// The process that made a decision about the host, as the account records it.
///
/// In a pod `HOSTNAME` is the pod name, which is the identity a reader can find
/// in the cluster. Without one this falls back to the host's own name, and only
/// then to a word that says the identity is unknown rather than inventing one.
pub(crate) fn gate_actor() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// The account's event for a bound that moved, or `None` when it did not.
///
/// Stated apart from the measurement so the shape can be judged without a host:
/// the five things the account promises a reader -- who, when, on which reading,
/// from what to what, and how it ended -- are all here, and none of them can be
/// read off the log line this replaces after that line rotates away.
#[allow(clippy::too_many_arguments)]
fn bound_move_event(
    current: usize,
    next: usize,
    ceiling: usize,
    dimension: &'static str,
    calm: bool,
    dir: &Path,
    cores: usize,
    busy_cores: f64,
) -> Option<ResourceEvent> {
    if next == current {
        // A decision not to move is not an event: the account records what
        // happened, and a row per quiet window would bury the rows that matter
        // under the ones that say nothing did.
        return None;
    }
    Some(ResourceEvent {
        kind: ResourceEventKind::BoundChange,
        occurred_at: chrono::Utc::now(),
        subject: "build_gate.bound".to_string(),
        actor: gate_actor(),
        reading: BUILD_GATE_CEILING.to_string(),
        reading_value: Some(ceiling as f64),
        from_value: Some(current as i64),
        to_value: Some(next as i64),
        outcome: ResourceEventOutcome::Applied,
        detail: serde_json::json!({
            "dimension": dimension,
            "calm": calm,
            "dir": dir.display().to_string(),
            "cores": cores,
            "busy_cores": busy_cores,
        }),
        count: 1,
    })
}

/// The host-wide build bound.
pub struct BuildGate {
    dir: PathBuf,
    /// The bound the operator set, or `None` to derive one.
    ///
    /// `Some` is a fixed bound and not a cap on the derivation: an operator who
    /// sets this is saying what this machine is, which is knowledge the
    /// derivation does not have, and a cap would leave them arguing with a
    /// measurement that cannot see the deployment's other users.
    fixed: Option<usize>,
    wait: Duration,
    installed: bool,
    /// The bound this process derived last, as the number it is enforcing; 0
    /// before any measurement.
    derived: AtomicU64,
    /// The ceiling of that measurement: what the host could take if it stayed
    /// quiet, before the control law's say. 0 before any measurement.
    measured_ceiling: AtomicU64,
    /// The dimension the ceiling came from.
    dimension: Mutex<&'static str>,
    /// When the bound was last moved. A build is a tens-of-minutes event, so the
    /// window only has to damp the derivation against its own samples.
    window: Mutex<Option<Instant>>,
    /// The lock that elects the one process allowed to move the bound, taken
    /// once and held for the life of the process.
    controller: OnceLock<bool>,
    in_flight: AtomicU64,
    excluded: AtomicU64,
    waiting: AtomicU64,
    acquired: AtomicU64,
    /// One counter per ask and per wall, indexed by [`asker_index`] and
    /// [`RefusedBy::index`].
    refused: [[AtomicU64; RefusedBy::ALL.len()]; ASKER_CELLS],
    wait_ms_total: AtomicU64,
    wait_ms_max: AtomicU64,
    held_ms_total: AtomicU64,
    held_ms_max: AtomicU64,
    /// Events this process produced and has not handed to the account's
    /// consumer yet.
    ///
    /// In this process's memory rather than written to the database here: the
    /// gate is core, it has no store, and a producer that reached into storage
    /// would make a bound's move fail on a database that is down -- a decision
    /// about the host cannot wait on a database. The consumer drains this and
    /// appends; an event therefore survives until the process that saw it ends,
    /// which is longer than the counter it replaces survives (a counter loses
    /// its whole history at restart, this loses only what it has not yet
    /// handed over).
    events: Mutex<VecDeque<ResourceEvent>>,
    /// Events pushed out of the ring before a consumer took them. Published,
    /// not hidden: a bounded queue that overflows silently turns "the account
    /// is complete" into a claim nobody can check.
    events_dropped: AtomicU64,
}

impl std::fmt::Debug for BuildGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildGate")
            .field("dir", &self.dir)
            .field("fixed", &self.fixed)
            .field("wait", &self.wait)
            .field("installed", &self.installed)
            .field("derived", &self.derived.load(Ordering::Relaxed))
            .finish()
    }
}

impl BuildGate {
    /// Builds the gate from configuration, creating the slot directory.
    ///
    /// A gate that cannot create its directory is not in force: binding to a
    /// directory that does not exist would refuse every build, and refusing
    /// every build is a worse failure than running them unbounded. A configured
    /// `max_concurrent` of zero is not "off" but "work it out": a bound of zero
    /// is the one bound that refuses every build, so it cannot be what a field
    /// named "how many builds at once" means.
    pub fn new(cfg: &BuildGateConfig) -> Self {
        let dir = PathBuf::from(&cfg.lock_dir);
        let mut installed = false;
        if cfg.enabled {
            installed = create_slot_dir(&dir).is_ok();
        }
        Self {
            fixed: (cfg.max_concurrent > 0).then_some(cfg.max_concurrent),
            wait: Duration::from_secs(cfg.wait_secs),
            installed,
            dir,
            derived: AtomicU64::new(0),
            measured_ceiling: AtomicU64::new(0),
            dimension: Mutex::new(DIM_UNMEASURED),
            window: Mutex::new(None),
            controller: OnceLock::new(),
            in_flight: AtomicU64::new(0),
            excluded: AtomicU64::new(0),
            waiting: AtomicU64::new(0),
            acquired: AtomicU64::new(0),
            refused: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            wait_ms_total: AtomicU64::new(0),
            wait_ms_max: AtomicU64::new(0),
            held_ms_total: AtomicU64::new(0),
            held_ms_max: AtomicU64::new(0),
            events: Mutex::new(VecDeque::new()),
            events_dropped: AtomicU64::new(0),
        }
    }

    /// Queues one event for the account's consumer.
    ///
    /// The ring is bounded, and what it drops is counted rather than silently
    /// lost. A consumer that is not running is the case where the ring fills;
    /// the count is what lets a reading say "events are being produced and
    /// nothing is consuming them" instead of the account simply appearing
    /// smaller than the world.
    fn record_event(&self, event: ResourceEvent) {
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());
        if events.len() >= EVENT_RING_CAPACITY {
            events.pop_front();
            self.events_dropped.fetch_add(1, Ordering::Relaxed);
        }
        events.push_back(event);
    }

    /// Hands over everything queued so far, oldest first.
    ///
    /// Draining is the consumer's act, in one step: an event is either still
    /// here for the next consumer or already handed to this one. Two consumers
    /// cannot both take it, because the queue is taken rather than copied.
    pub fn drain_events(&self) -> Vec<ResourceEvent> {
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());
        events.drain(..).collect()
    }

    /// How many events were lost to a full ring since this process started.
    pub fn events_dropped(&self) -> u64 {
        self.events_dropped.load(Ordering::Relaxed)
    }

    /// Whether this gate is excluding anything at all.
    pub fn in_force(&self) -> bool {
        self.installed
    }

    /// The bound in force for the next acquisition.
    ///
    /// Read fresh every time rather than remembered: the bound moves with the
    /// host, and a build admitted against the number this process worked out
    /// before the last storm would be admitted against a reading nobody is
    /// standing behind. The operator's override is the one bound not re-read,
    /// because it is not derived: it says what the machine is, and only the
    /// operator knows when that changed.
    ///
    /// The value enforced is never above what this process itself measured: a
    /// process that is not the controller reads the controller's bound from the
    /// file, and one whose own reading of the host is worse than that file goes
    /// with its own, since holding a build back is always safe and admitting one
    /// on a stale number is the failure this is here to prevent.
    fn bound(&self) -> usize {
        if let Some(fixed) = self.fixed {
            return fixed;
        }
        let persisted = self.persisted_bound().unwrap_or(0);
        let derived = self.derived.load(Ordering::Relaxed) as usize;
        match (persisted, derived) {
            (0, 0) => 1,
            (0, d) | (d, 0) => d.max(1),
            (p, d) => p.min(d).max(1),
        }
    }

    /// The ceiling to publish: what the host can take, and what set it.
    fn ceiling(&self) -> (usize, &'static str) {
        if let Some(fixed) = self.fixed {
            return (fixed, DIM_OVERRIDE);
        }
        if !self.installed {
            return (0, DIM_NONE);
        }
        let ceiling = self.measured_ceiling.load(Ordering::Relaxed) as usize;
        let dimension = *self.dimension.lock().unwrap_or_else(|e| e.into_inner());
        (ceiling, dimension)
    }

    fn bound_path(&self) -> PathBuf {
        self.dir.join(BOUND_FILE)
    }

    fn persisted_bound(&self) -> Option<usize> {
        read_gate_file(&self.bound_path())?.trim().parse().ok()
    }

    /// Writes the derived bound where every process on the host reads it.
    ///
    /// A write that fails leaves the file at its previous value, which is a
    /// bound this process still enforces: the logger says so rather than the
    /// gate pretending the host was measured into a number nobody can read.
    fn persist_bound(&self, bound: usize) {
        let path = self.bound_path();
        if let Err(e) = write_gate_file(&path, &format!("{bound}\n")) {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                bound,
                "could not persist the build bound; other processes on this host keep the bound they read"
            );
            return;
        }
        // The bound is what the gate admits; the files below it are what it hands
        // out. A bound that fell leaves the room above it on disk, where a reader
        // that counts the directory -- a person looking at the gate, or the
        // occupied reading -- takes it for room the gate does not have, and the
        // host is read as being allowed to build beside itself.
        lay_out_slots(&self.dir, bound);
    }

    /// Lays the slot directory out on a round where the bound did not move.
    ///
    /// The round that moves the bound lays the directory out beside the number
    /// it writes above; a round that does not still has a name to take back. A
    /// slot above the bound that was held when the bound last fell is skipped at
    /// that moment -- removing a held name is what puts two builds in one slot
    /// -- and nothing else comes back for it once its holder leaves, so the name
    /// outlives the bound that disowned it and reads to anyone counting the
    /// directory as room the host does not have. Only the controller runs this:
    /// the bound is one process's decision for the host, and so are the names
    /// under it.
    fn reconcile_slots(&self, bound: usize) {
        lay_out_slots(&self.dir, bound);
    }

    /// Whether this process is the one that moves the bound.
    ///
    /// The lock is taken once and kept for the life of the process, so the
    /// election cannot flap: the first builder to start is the controller until
    /// it exits. Anyone else enforces what the controller decided, and holds
    /// back below it when its own reading says the host is worse.
    fn is_controller(&self) -> bool {
        *self.controller.get_or_init(|| self.take_controller_lock())
    }

    #[cfg(unix)]
    fn take_controller_lock(&self) -> bool {
        use std::os::unix::io::AsRawFd;
        let Some(file) = open_gate_file(&self.dir.join(CONTROLLER_FILE), true) else {
            return false;
        };
        // SAFETY: as in `lock_slot` — flock touches only this process's
        // descriptor table for a descriptor this scope owns, and EWOULDBLOCK is
        // the expected answer when another process got here first.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            // Held for the process, not for this call: the lock *is* the
            // election, and releasing it here would let a second process take
            // the field and start writing a second number for the same host.
            std::mem::forget(file);
            true
        } else {
            false
        }
    }

    #[cfg(not(unix))]
    fn take_controller_lock(&self) -> bool {
        false
    }

    /// Re-derives the bound, at most once per window, and persists it where this
    /// process is the one elected to.
    ///
    /// Called where the bound is about to be used and where it is published, so
    /// the number a build is admitted against is never older than one window
    /// while the host is being watched. It runs on no timer of its own: a
    /// process that is neither asking for slots nor being scraped has no reader
    /// for a moved bound, and a task that polls the host for a reader that is not
    /// there is a cost this design has no reason to pay.
    async fn step(&self) {
        if self.fixed.is_some() || !self.installed {
            return;
        }
        {
            let window = self.window.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(last) = *window {
                if last.elapsed() < BOUND_WINDOW {
                    return;
                }
            }
        }
        let Some(load) = HostLoad::measure(&self.dir).await else {
            // No move rather than a move to a default: a host this process
            // cannot read is not a host it may size a bound for.
            tracing::debug!(
                dir = %self.dir.display(),
                "host load unreadable; the build bound stays where it was"
            );
            return;
        };
        let (ceiling, dimension) = load.ceiling();
        let calm = load.is_calm();
        let persisted = self.persisted_bound();
        let current = match (persisted, self.derived.load(Ordering::Relaxed) as usize) {
            (Some(p), d) if d > 0 => p.min(d).max(1),
            (Some(p), _) => p.max(1),
            (None, d) if d > 0 => d,
            (None, _) => ceiling,
        };
        let next = next_bound(current, ceiling, calm);
        // A process that is not the controller may hold back below what the
        // controller decided and may not go above it: the bound rising is a
        // decision about the host, and one process makes it.
        let enforce = if self.is_controller() {
            next
        } else {
            next.min(persisted.unwrap_or(next)).max(1)
        };
        *self.dimension.lock().unwrap_or_else(|e| e.into_inner()) = dimension;
        self.measured_ceiling
            .store(ceiling as u64, Ordering::Relaxed);
        self.derived.store(enforce as u64, Ordering::Relaxed);
        *self.window.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        let controller = self.is_controller();
        // Written whenever the file disagrees with what is being enforced, not
        // only when the number moved: the first derivation of a directory is
        // exactly the case where the file is absent and the two look equal to
        // this process, and it is the case the file exists for — the next
        // process to start reads what the host was measured at instead of
        // deriving it again from a host it has not watched.
        if controller && persisted != Some(enforce) {
            self.persist_bound(enforce);
        } else if controller {
            self.reconcile_slots(enforce);
        }
        if enforce != current {
            tracing::info!(
                from = current,
                to = enforce,
                ceiling,
                dimension,
                calm,
                controller,
                busy_cores = load.busy_cores,
                cores = load.cores,
                cpu_stall_avg60 = load.pressure.cpu_avg60,
                io_stall_avg60 = load.pressure.io_avg60,
                mem_stall_avg60 = load.pressure.mem_avg60,
                mem_available_mb = load.pressure.mem_available_bytes / (1024 * 1024),
                builds_running = load.pressure.builds_in_flight,
                dir = %self.dir.display(),
                "host build bound moved"
            );
            // The same move, into the account: a bound that moved at runtime
            // left no commit behind, and a log line has rotated away by the
            // time anyone asks why the bound is what it is. Only the controller
            // records it -- one process makes the decision, and a process that
            // merely held back below it did not make one.
            if controller {
                if let Some(event) = bound_move_event(
                    current,
                    enforce,
                    ceiling,
                    dimension,
                    calm,
                    &self.dir,
                    load.cores,
                    load.busy_cores,
                ) {
                    self.record_event(event);
                }
            }
        }
    }

    /// Says that this work is waiting for the next build, so builds that arrive
    /// before it stand down.
    ///
    /// Called by a work that has just been refused and comes back on its own: a
    /// one-shot build waits in [`BuildGate::acquire`] instead and does not need
    /// this. The waiters are taken in the order they ask, each holding a claim of
    /// its own until its ask is answered or the claim lapses. A claim has one
    /// thing to accomplish — that the field stops being taken by arrivals — and a
    /// second claim does not add to it: it only takes the turn from the work that
    /// asked first, whose next ask then comes round on a field someone else is
    /// holding, so its claim never survives to the ask it was made for. A holder
    /// refreshing its own claim is what keeps a work that keeps losing its place.
    pub fn reserve_build_field(&self, what: &str) {
        if !self.installed {
            return;
        }
        let since = unix_now();
        let path = self.dir.join(RESERVE_FILE);
        let previous = self.reservation().map(|(holder, _)| holder);
        // A claim that is already here and fresh belongs to the work that asked
        // first, and it is left to it. Arrivals stand down on the claim itself
        // rather than on who left it, so this ask has nothing to gain by writing
        // over one, and the other work has its turn to lose.
        if let Some(holder) = previous.as_deref() {
            if holder != what {
                tracing::info!(
                    what,
                    held_by = holder,
                    dir = %self.dir.display(),
                    "the next build is already claimed; leaving that turn to the work that asked first"
                );
                return;
            }
        }
        if let Err(e) = write_gate_file(&path, &format!("{what}\t{since}\n")) {
            tracing::warn!(
                what,
                path = %path.display(),
                error = %e,
                "could not claim the next build; this work will be refused again until the host is free"
            );
            return;
        }
        tracing::info!(
            what,
            previous = previous.as_deref().unwrap_or("none"),
            fresh_secs = RESERVE_FRESH.as_secs(),
            dir = %self.dir.display(),
            "claiming the next build: builds stand down until this work gets one"
        );
    }

    /// The claim on the next build, when there is a fresh one.
    fn reservation(&self) -> Option<(String, u64)> {
        let text = read_gate_file(&self.dir.join(RESERVE_FILE))?;
        let mut fields = text.trim().splitn(2, '\t');
        let what = fields.next()?.to_string();
        let since: u64 = fields.next()?.parse().ok()?;
        if what.is_empty() {
            return None;
        }
        let age = unix_now().saturating_sub(since);
        if age > RESERVE_FRESH.as_secs() {
            // A claim that is not being refreshed is a work that stopped asking:
            // honoring it would hold builds out for a waiter that is gone.
            return None;
        }
        Some((what, since))
    }

    /// Releases a claim held by `what`, which is done once its ask is answered.
    ///
    /// Someone else's claim is left alone, whether this is a build that took a
    /// slot or an exclusion that took the field: they are waiting too, and this
    /// ask being answered is not their turn. Taking it would be losing a turn
    /// rather than spending one — the other work asks again on its own cycle and
    /// finds the field free of its claim and busy with whoever came next, so a
    /// work on a longer cycle than the field's traffic never gets a turn at all.
    /// Its own holder releases it when its ask is answered, and a claim nobody
    /// refreshes lapses at [`RESERVE_FRESH`].
    fn release_reservation(&self, what: &str) {
        if !self.installed {
            return;
        }
        match self.reservation() {
            Some((holder, _)) if holder == what => {}
            _ => return,
        }
        let path = self.dir.join(RESERVE_FILE);
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "could not release the claim on the next build; it lapses when it goes stale"
                );
            }
        }
    }

    /// The counters as they stand, for the reading.
    fn counters(&self) -> Counters {
        Counters {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            excluded: self.excluded.load(Ordering::Relaxed),
            waiting: self.waiting.load(Ordering::Relaxed),
            acquired: self.acquired.load(Ordering::Relaxed),
            refused: std::array::from_fn(|asker| {
                std::array::from_fn(|wall| self.refused[asker][wall].load(Ordering::Relaxed))
            }),
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

    /// Takes the build field alone, or none of it, refusing at once when a build
    /// holds it.
    ///
    /// The field rather than every slot, because a slot is a share of the bound
    /// and not an exclusion from it: with several builds permitted, holding one
    /// slot keeps the others out only while the bound happens to be one, and a
    /// bound that moves with the host would arm and disarm such a holder without
    /// moving a single reading. The claim says "no build is running, and none
    /// starts while I work" at any bound.
    ///
    /// A refusal takes nothing at all. There is nothing to take back here — the
    /// claim is one file — and that is the point of it: an ask that cannot
    /// succeed is not a partial hold that turns builds away while the work it was
    /// taken for does not run.
    pub async fn try_acquire_exclusive(self: &Arc<Self>, what: &str) -> SFResult<BuildPermit> {
        if !self.in_force() {
            // Nothing bounds builds here, so nothing can be excluded from them.
            return Ok(self.permit(what, Vec::new(), HoldKind::Excluding));
        }
        match lock_claim_exclusive(&self.dir) {
            Some(file) => {
                record_holder(&file, what);
                // The field is this caller's now, so its own claim on the next
                // build has been answered and is released. Another work's is not:
                // this pass took the field without answering that work's ask, and
                // a claim released here would be gone before its holder's next
                // ask comes round.
                self.release_reservation(what);
                self.excluded.fetch_add(1, Ordering::Relaxed);
                Ok(self.permit(what, vec![file], HoldKind::Excluding))
            }
            None => {
                let held_by = name_holder(&self.dir, CLAIM_FILE);
                let reason =
                    format!("a build is running, so builds cannot be excluded ({held_by})");
                // An exclusion is retried by a caller on its own cycle, so a
                // refusal here is not a failure but the time between two walks —
                // and with builds arriving back to back, that cycle can be too
                // short ever to come round to an empty field: the walk after this
                // one finds the next build already started. The claim says so
                // instead, and the builds in the way stand down on their own, so
                // the next walk is not a race it loses.
                //
                // Only when a *build* is the obstacle. While another exclusion
                // holds the claim, builds are already kept out, so claiming the
                // next build would hold them out again after that exclusion ends
                // without anything to reclaim for. A shared lock is how the two
                // are told apart: it is compatible with the shared locks builds
                // hold and refused by the exclusive lock an exclusion holds.
                if lock_claim_shared(&self.dir).is_some() {
                    self.reserve_build_field(what);
                }
                tracing::warn!(
                    what,
                    holder = %held_by,
                    dir = %self.dir.display(),
                    identity = %self.identity(),
                    "{reason}"
                );
                // Deliberately not counted as a refusal: the refusal counter is
                // read as "a build was refused a slot", and nothing was refused
                // here. What this call achieved is counted by its caller, at the
                // work that did not run.
                Err(SFError::ResourceExhausted(format!(
                    "build gate could not exclude builds for {what}: {reason}"
                )))
            }
        }
    }

    /// Takes the build field, waiting for a running build to finish.
    ///
    /// The waiting form of [`BuildGate::try_acquire_exclusive`], for a caller
    /// whose work has no next round to come back on. Refusing is the right
    /// answer for a sweep, which runs again on its own cycle; it is the wrong
    /// one for a process switch, which would leave the staged binary unswitched
    /// until some later change happens to ask for another switch.
    ///
    /// While it waits it claims the next build, so an arrival between two looks
    /// stands down instead of slipping into the gap a running build leaves. The
    /// claim lapses on its own if this process dies before its ask is answered,
    /// and it is released the moment the field is taken.
    ///
    /// `budget` is the caller's, not the gate's: only the caller knows how long
    /// its work can wait. A build on this host takes minutes, so a budget in the
    /// same order as the build timeout is the shape a caller wants; a budget
    /// short of one build turns every collision into a lost turn.
    pub async fn await_exclusive(
        self: &Arc<Self>,
        what: &str,
        budget: Duration,
    ) -> SFResult<BuildPermit> {
        if !self.in_force() {
            return Ok(self.permit(what, Vec::new(), HoldKind::Excluding));
        }
        let started = Instant::now();
        loop {
            let refusal = match self.try_acquire_exclusive(what).await {
                Ok(permit) => return Ok(permit),
                Err(error) => error,
            };
            if started.elapsed() >= budget {
                return Err(refusal);
            }
            // Asked for after the refusal, not before: the claim is what keeps
            // the field from being taken while this work is between looks, and a
            // claim made before the first look would stand down the very build
            // it is waiting for.
            self.reserve_build_field(what);
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// A permit with the accounting of one kind of hold.
    fn permit(self: &Arc<Self>, what: &str, held: Vec<GateLock>, kind: HoldKind) -> BuildPermit {
        BuildPermit {
            gate: Arc::clone(self),
            held,
            taken_at: None,
            what: what.to_string(),
            kind,
        }
    }

    async fn acquire_within(
        self: &Arc<Self>,
        what: &str,
        budget: Duration,
    ) -> SFResult<BuildPermit> {
        if !self.in_force() {
            // No bound to enforce: hand back a permit that holds nothing, so the
            // caller's shape does not depend on whether the gate is installed.
            return Ok(self.permit(what, Vec::new(), HoldKind::Building));
        }
        self.step().await;
        let waiting = self.waiting.fetch_add(1, Ordering::Relaxed) + 1;
        if waiting > 1 {
            tracing::info!(what, waiting, "build waiting for a slot");
        }
        let started = Instant::now();
        let deadline = started + budget;
        // The first pass is unconditional, so a zero budget still means "look
        // once" rather than "never look".
        loop {
            let bound = self.bound();
            // Why this pass got nowhere, when it was not the slot count that
            // stopped it: which wall, and who was holding what put it there.
            let mut blocked_by: Option<(RefusedBy, String)> = None;
            // A slot whose lock call failed for a reason that is not a holder.
            // Kept apart from `blocked_by` because the two say different things:
            // one is somebody working, the other is the gate not being able to
            // ask.
            let mut unavailable: Option<std::io::Error> = None;
            for slot in 0..bound {
                let file = match lock_slot(&self.dir, slot) {
                    Ok(file) => file,
                    Err(SlotLock::Held) => continue,
                    Err(SlotLock::Unavailable(error)) => {
                        unavailable = Some(error);
                        continue;
                    }
                };
                // The claim is what says a build is *running* as opposed to
                // having taken a slot: an exclusion holds it alone, and while it
                // does, no build starts. Given back at once when it is held, so
                // that a build never sits on a slot while a sweep finishes.
                let Some(claim) = lock_claim_shared(&self.dir) else {
                    drop(file);
                    blocked_by = Some((RefusedBy::Excluded, name_holder(&self.dir, CLAIM_FILE)));
                    break;
                };
                // A work that was turned away and said it is still waiting gets
                // the next build, and this arrival is not it.
                if let Some((claimant, _)) = self.reservation() {
                    if claimant != what {
                        drop(claim);
                        drop(file);
                        blocked_by = Some((RefusedBy::Reserved, format!("\"{claimant}\"")));
                        break;
                    }
                }
                record_holder(&file, what);
                record_holder(&claim, what);
                self.release_reservation(what);
                self.waiting.fetch_sub(1, Ordering::Relaxed);
                self.in_flight.fetch_add(1, Ordering::Relaxed);
                let total = self.acquired.fetch_add(1, Ordering::Relaxed) + 1;
                let waited = started.elapsed();
                let taken_at = self.slot_taken(waited);
                if waited.as_secs() > 0 {
                    tracing::info!(
                        what,
                        slot,
                        bound,
                        waited_secs = waited.as_secs(),
                        "build took a slot after waiting"
                    );
                }
                tracing::debug!(what, slot, bound, total, "build took a slot");
                return Ok(BuildPermit {
                    gate: Arc::clone(self),
                    held: vec![claim, file],
                    taken_at: Some(taken_at),
                    what: what.to_string(),
                    kind: HoldKind::Building,
                });
            }
            if Instant::now() >= deadline {
                self.waiting.fetch_sub(1, Ordering::Relaxed);
                let by = match blocked_by.as_ref() {
                    Some((wall, _)) => *wall,
                    None if budget.is_zero() => RefusedBy::Busy,
                    None => RefusedBy::WaitedOut,
                };
                // Counted against the ask as well as the wall: which work was
                // turned away is the half of a refusal a reader cannot get back
                // from the wall, and it is only known here, while the caller's
                // own words are still in hand.
                let asker = asker_index(what);
                let total = self.refused[asker][by.index()].fetch_add(1, Ordering::Relaxed) + 1;
                // Who is holding, read from the holders themselves. A refusal's
                // own duration is not a reading -- it is fixed by whether the
                // caller waited -- but the holder's is: it is the length of one
                // piece of work, and it is what tells a caller that came back
                // once too often from one that is queueing behind a single
                // unbroken hold. When the slots are not what stopped the ask, the
                // slots' holders are not who stopped it either, so the reason
                // carries what did instead.
                let held = match blocked_by.as_ref() {
                    Some((_, holder)) => holder.clone(),
                    // Neither a holder nor a wait: the kernel would not answer
                    // whether the slot is free. Saying so turns a refusal that
                    // would otherwise name the last work to have written its
                    // line into a refusal that names its own cause.
                    None => match &unavailable {
                        Some(error) => format!("the slot could not be locked: {error}"),
                        None => holder_report(&self.dir, bound),
                    },
                };
                // The wall's own sentence, whenever the wall is what stopped the
                // ask: four walls, four sentences, so the same wall reads the
                // same way in every refusal it causes. A build kept out by
                // something other than the slot count says so as well, because
                // the slots' holders are then not who stopped it.
                let opening = match blocked_by.as_ref() {
                    Some(_) => "the gate is not open to this build: ",
                    None => "",
                };
                let reason = format!("{opening}{} ({held})", by.reason(bound, budget));
                // The refusal, into the account. The counter beside this says
                // how many; it cannot say when, or which bound was in force, or
                // who was holding -- and it dies with this process. The subject
                // carries the wall as well as the ask, because the same work
                // turned away by two different walls is two facts, not one
                // counted twice.
                self.record_event(ResourceEvent {
                    kind: ResourceEventKind::AdmissionRefusal,
                    occurred_at: chrono::Utc::now(),
                    subject: format!("{what}:{}", by.as_str()),
                    actor: gate_actor(),
                    reading: BUILD_GATE_SLOTS.to_string(),
                    reading_value: Some(bound as f64),
                    from_value: None,
                    to_value: None,
                    outcome: ResourceEventOutcome::Refused,
                    detail: serde_json::json!({
                        "wall": by.as_str(),
                        "what": what,
                        "holder": held,
                        "bound": bound,
                        "ceiling": self.measured_ceiling.load(Ordering::Relaxed),
                        "dir": self.dir.display().to_string(),
                    }),
                    count: 1,
                });
                tracing::warn!(
                    what,
                    asker = asker_label(asker),
                    refusal = by.as_str(),
                    slots = bound,
                    holder = %held,
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

/// What a permit's holder is doing with the host, which decides what it counts
/// as.
///
/// The two are accounted separately because they answer different questions: a
/// build's hold is a reading of how long the host is busy building, and an
/// exclusion is a reading of why a build that asked just then got nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldKind {
    /// A build: holds one slot, and counts in every build reading.
    Building,
    /// Work that is not a build and holds every slot. Counts in
    /// [`BUILD_GATE_EXCLUDED`] alone, because it is not a build that ran and it
    /// did not occupy the host for as long as a build does.
    Excluding,
}

/// A held slot, or every slot. Dropping it releases them to the next builder.
#[derive(Debug)]
pub struct BuildPermit {
    gate: Arc<BuildGate>,
    /// The locked files, when slots were actually taken. Their locks go when
    /// the permit does. Empty for one of two reasons, told apart by `taken_at`:
    /// the gate is not in force, or the permit is an exclusion over a gate that
    /// bounds nothing.
    held: Vec<GateLock>,
    /// When the slot was taken, set for a build that took one and read only
    /// there: a permit that holds nothing was never counted as a build that ran,
    /// so it contributes no hold to the readings either.
    taken_at: Option<Instant>,
    what: String,
    kind: HoldKind,
}

impl BuildPermit {
    /// Whether this permit holds anything.
    pub fn held(&self) -> bool {
        !self.held.is_empty()
    }
}

impl Drop for BuildPermit {
    fn drop(&mut self) {
        if self.held.is_empty() {
            return;
        }
        // The locks go with the values in `held`, by [`GateLock`]'s drop rather
        // than by these handles closing: see there for why the distinction is
        // the difference between a release and a wait.
        match self.kind {
            HoldKind::Building => {
                self.gate.in_flight.fetch_sub(1, Ordering::Relaxed);
                if let Some(taken_at) = self.taken_at {
                    self.gate.slot_released(taken_at.elapsed());
                }
            }
            HoldKind::Excluding => {
                self.gate.excluded.fetch_sub(1, Ordering::Relaxed);
            }
        }
        tracing::debug!(what = %self.what, kind = ?self.kind, "held slots released");
    }
}

/// Creates the slot directory, reporting why when it cannot.
/// Who is holding a slot, as the holder itself wrote it down.
///
/// The permission carries no identity: `flock` answers "someone has it" and
/// nothing else, so a caller refused a slot could only be told that the host was
/// busy, and learning *which* work held it took a `ps` inside the container —
/// the reading existed, but only on a machine you had to already be on. The
/// holder writes one line into the slot file it locked instead, and the refusing
/// side reads it, so the refusal says what was asked for and what is holding it.
///
/// A refusal raises exactly this question, and the refusal counter answers it
/// wrongly: that counter is per requester, so sixty refusals of one work read
/// the same whether they were sixty different holders or one work that never let
/// go. The holder's own `what` is what tells those apart, which is why it is
/// carried on the refusal; [`BUILD_GATE_HOLDER_SECONDS`] publishes the same
/// holder as a value, because a refusal only exists for an ask that was made
/// while the hold was in force, and the starved ask is the one that stopped
/// being made.
struct Holder {
    what: String,
    pid: u32,
}

/// Writes `holder` into a slot file this process has just locked.
///
/// Written after the lock, never before: a reader that finds a line here can
/// rely on the kernel to say whether its writer still lives, because a holder
/// that dies leaves the line behind and the lock released. Written at all only
/// because the lock cannot say it.
#[cfg(unix)]
fn record_holder(file: &std::fs::File, what: &str) {
    use std::io::{Seek, SeekFrom, Write};
    let since = unix_now();
    let line = format!("{}\t{since}\t{what}\n", std::process::id());
    let mut handle = file;
    if handle.set_len(0).is_ok() {
        let _ = handle
            .seek(SeekFrom::Start(0))
            .and_then(|_| handle.write_all(line.as_bytes()));
    }
}

/// Seconds since the epoch, as the clock both sides of a record agree on.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(not(unix))]
fn record_holder(_file: &std::fs::File, _what: &str) {}

/// Where the record above is read back from, plus the one thing the record
/// cannot say about itself: whether it is current.
///
/// The lock is asked, not the clock. A slot whose lock can be taken over holds
/// nobody, so whatever is written in it was left by a holder that is gone and
/// must not be named in a refusal — otherwise the report would outlive its
/// subject and the next starved caller would blame work that finished long ago.
#[cfg(unix)]
fn read_holder(dir: &Path, slot: usize) -> Option<(Holder, std::time::Duration)> {
    read_holder_at(&dir.join(format!("slot-{slot}.lock")))
}

#[cfg(not(unix))]
fn read_holder(_dir: &Path, _slot: usize) -> Option<(Holder, std::time::Duration)> {
    None
}

/// The record in one file, when the file is still held.
#[cfg(unix)]
fn read_holder_at(path: &Path) -> Option<(Holder, std::time::Duration)> {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;

    let file = open_gate_file(path, false)?;
    loop {
        // SAFETY: as in `lock_slot` — flock touches only this process's
        // descriptor table for a descriptor this scope owns, and EWOULDBLOCK is
        // the expected answer.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            // Nobody holds it, so the line below is a leftover.
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
            return None;
        }
        // A signal is not a holder. Asked again rather than read as one: the
        // record below is only the truth about a file that is actually held, and
        // naming it otherwise blames whoever wrote the line last.
        if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break;
    }
    let mut text = String::new();
    (&file).read_to_string(&mut text).ok()?;
    let line = text.lines().next()?;
    let mut fields = line.splitn(3, '\t');
    let pid: u32 = fields.next()?.parse().ok()?;
    let since_unix: u64 = fields.next()?.parse().ok()?;
    let what = fields.next()?.to_string();
    if what.is_empty() {
        return None;
    }
    let held = std::time::Duration::from_secs(unix_now().saturating_sub(since_unix));
    Some((Holder { what, pid }, held))
}

#[cfg(not(unix))]
fn read_holder_at(_path: &Path) -> Option<(Holder, std::time::Duration)> {
    None
}

/// What to say about the holder of one named file, for the refusals whose cause
/// is a file rather than a slot count.
///
/// A file with no record is told apart from a free file on purpose: the caller
/// asking has already found the lock held, and "someone holds this and wrote
/// nothing down" is a different fact from "nobody holds this".
fn name_holder(dir: &Path, name: &str) -> String {
    match read_holder_at(&dir.join(name)) {
        Some((h, held)) => format!("\"{}\" (pid {}) for {}s", h.what, h.pid, held.as_secs()),
        None => "a holder that left no record".to_string(),
    }
}

/// What a refusal can say about what is holding the slots it could not take.
///
/// Told apart from "no holder" on purpose: a slot taken by a holder that wrote
/// nothing — an older build of this gate, or a record the filesystem refused —
/// is not the same fact as a free slot, and a reader deciding whether the host
/// is busy or the gate is broken needs to see which one it has.
fn holder_report(dir: &Path, slots: usize) -> String {
    let held: Vec<String> = (0..slots)
        .filter_map(|slot| read_holder(dir, slot))
        .map(|(h, held)| format!("\"{}\" (pid {}) for {}s", h.what, h.pid, held.as_secs()))
        .collect();
    if held.is_empty() {
        format!("{} of {slots} slots taken, none of them named", slots)
    } else if held.len() == slots {
        format!("held by {}", held.join(", "))
    } else {
        format!("{} of {slots} slots named: {}", held.len(), held.join(", "))
    }
}

/// The same holders as [`holder_report`], as a series rather than a sentence.
///
/// Read at scrape time and not written by the holders: a holder that only
/// published at acquisition would leave its row behind after it died, and a
/// holder that publishes on a timer is a writer that has to be added to every
/// caller. The lock already says whether the record below it still has a
/// subject, so the reading is taken from the same place the refusal takes its
/// answer from.
///
/// Rows exist only for slots that are held: a free slot has no `what` to carry,
/// and `BUILD_GATE_OCCUPIED` already counts how many are taken.
fn holder_metrics(dir: &Path, slots: usize, origin: &Origin<'_>) -> Vec<RawMetric> {
    (0..slots)
        .filter_map(|slot| read_holder(dir, slot).map(|(h, held)| (slot, h, held)))
        .map(|(slot, holder, held)| {
            RawMetric::new(BUILD_GATE_HOLDER_SECONDS, held.as_secs_f64())
                .with_label("dir", origin.dir.to_string())
                .with_label("role", origin.role.to_string())
                .with_label("slot", slot.to_string())
                .with_label("what", holder.what)
        })
        .collect()
}

fn create_slot_dir(dir: &Path) -> SFResult<()> {
    std::fs::create_dir_all(dir)
        .map_err(|e| SFError::IO(format!("create build gate dir {}: {e}", dir.display())))?;
    open_gate_dir(dir);
    Ok(())
}

/// Makes the slot files in the directory match the bound it publishes.
///
/// Which slots exist and which slots the gate admits are two facts that have to
/// agree wherever both can be read, because the directory outlives the bound
/// that made it: a bound that fell leaves the files it used to admit behind, and
/// the next reader of the directory -- a person, or a reading that counts held
/// slots -- sees a gate that hands out more room than it does.
///
/// The files below the bound are made here rather than left to the first build
/// that needs one, because not every builder may create a file: the host's own
/// client takes a slot by opening a file that is already there and will not
/// invent one, so a bound that rose over a directory without its files would be
/// room the host can never take. Writing the bound and making the room go
/// together, on the one side that publishes either.
///
/// The rule that a file someone holds is never unlinked is kept exactly: a slot
/// file's meaning is its lock, so a name removed from under a holder would let
/// the next arrival create a second file under it and two builds would sit in
/// one slot. The lock is taken first and the name is removed only while it is
/// held, so the file that goes is one no build is in. Best-effort throughout: a
/// name this process may not create or unlink is left as it is.
#[cfg(unix)]
fn lay_out_slots(dir: &Path, bound: usize) {
    for slot in 0..bound {
        // Opening for writing creates the file under the same mode a builder
        // would find it in, so nothing here is a slot only this user can use.
        let _ = open_gate_file(&dir.join(format!("slot-{slot}.lock")), true);
    }
    use std::os::unix::io::AsRawFd;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(slot) = name
            .to_string_lossy()
            .strip_prefix("slot-")
            .and_then(|rest| rest.strip_suffix(".lock"))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            continue;
        };
        if slot < bound {
            continue;
        }
        let path = entry.path();
        let Some(file) = open_gate_file(&path, false) else {
            continue;
        };
        // SAFETY: as in `lock_slot` -- flock touches only this process's
        // descriptor table for a descriptor this scope owns, and EWOULDBLOCK is
        // the expected "a build is in this slot" answer.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            continue;
        }
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(not(unix))]
fn lay_out_slots(_dir: &Path, _bound: usize) {}

/// Modes that make the gate directory usable by every builder on the host.
///
/// World-writable and sticky for the directory, world-writable for the files in
/// it. Nothing here is a secret -- a holder's name, a claim, a count -- so the
/// mode is not about who may read it; it is about who may *write* it, and the
/// answer has to be everyone who builds.
const GATE_DIR_MODE: u32 = 0o1777;
const GATE_FILE_MODE: u32 = 0o666;

/// Opens the gate directory to every builder on this host, whichever user it
/// runs as.
///
/// The gate is a rendezvous between builders that are neither the same program
/// nor the same user: a shell on the host and the container both take slots
/// here. Each of them has to be able to create a lock file and to write its own
/// name into one, so the directory has to permit both -- a builder that cannot
/// create the file cannot ask for a slot at all, and the gate then bounds only
/// the builder that happened to create the directory. Sticky so that one
/// builder cannot unlink a file another is holding, which for a lock file is the
/// same as releasing it.
///
/// Set on the way in rather than left to creation, because the directory
/// outlives the process that made it: one created before this ran is owned by
/// whoever ran first with whatever mode the umask gave, and leaving it that way
/// keeps every other builder out while the gate goes on reading as installed.
/// The same argument applies to the files, and is made where they are opened.
///
/// Failing is not fatal and must not be silent: this may be a directory this
/// process does not own, in which case it can still gate itself, and the warning
/// says which builder was left out rather than the gate quietly bounding one of
/// several.
fn open_gate_dir(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(GATE_DIR_MODE))
        {
            tracing::warn!(
                dir = %dir.display(),
                error = %e,
                "could not open the build gate directory to every builder; a builder that runs as another user cannot take a slot here and nothing in the readings says so"
            );
        }
    }
}

/// Takes the exclusive lock on one slot file, or reports that someone else has it.
#[cfg(unix)]
fn lock_slot(dir: &Path, slot: usize) -> Result<GateLock, SlotLock> {
    file_lock(
        &dir.join(format!("slot-{slot}.lock")),
        libc::LOCK_EX | libc::LOCK_NB,
    )
}

/// Without `flock` there is no cross-process bound to enforce, so the gate
/// refuses to pretend it has one.
#[cfg(not(unix))]
fn lock_slot(_dir: &Path, _slot: usize) -> Result<GateLock, SlotLock> {
    Err(SlotLock::Held)
}

/// Takes the claim a build holds while it runs, shared with the other builds.
#[cfg(unix)]
fn lock_claim_shared(dir: &Path) -> Option<GateLock> {
    file_lock(&dir.join(CLAIM_FILE), libc::LOCK_SH | libc::LOCK_NB).ok()
}

#[cfg(not(unix))]
fn lock_claim_shared(_dir: &Path) -> Option<GateLock> {
    None
}

/// Takes the claim alone: while this is held no build can start.
#[cfg(unix)]
fn lock_claim_exclusive(dir: &Path) -> Option<GateLock> {
    file_lock(&dir.join(CLAIM_FILE), libc::LOCK_EX | libc::LOCK_NB).ok()
}

#[cfg(not(unix))]
fn lock_claim_exclusive(_dir: &Path) -> Option<GateLock> {
    None
}

/// Locks a file in the slot directory, creating it if it is not there yet.
///
/// Opens one of the gate's own files in the shared slot directory, refusing to
/// follow a symbolic link and refusing anything that is not a regular file.
///
/// The directory is writable by *every* builder, and the builders are not all the
/// same user: one of them runs as an unprivileged user while the process that
/// writes the bound runs as root. A name in here is therefore something that side
/// can plant. A link at one of these names would send the next writer's bytes
/// wherever it pointed — and for the bound that writer is the privileged side — while
/// a fifo at a name that is read would make the reader wait forever. `O_NOFOLLOW`
/// refuses the link outright and the metadata check refuses everything that is not
/// a file, so neither shape is reachable rather than merely survived.
#[cfg(unix)]
fn open_gate_file(path: &Path, write: bool) -> Option<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(!write)
        .write(write)
        .create(write)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .ok()?;
    if write {
        // The files carry what a *holder* wrote: its name, its claim on the next
        // build. The holder is not always the process that created the file --
        // a slot the container made is written by the host's shell the next
        // time round -- so the mode has to be set rather than inherited from
        // whichever umask created it. Set here rather than at creation for the
        // same reason as the directory: an install that predates this keeps
        // files only one builder can write, and reopening one is the moment that
        // can be corrected without asking an operator to chmod a host path.
        // Best-effort: a file this process does not own refuses, and the holder
        // note it then cannot write is a reading it loses, not a lock it loses.
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(GATE_FILE_MODE));
    }
    file.metadata().ok()?.is_file().then_some(file)
}

/// The contents of one of the gate's files, or nothing when it is absent, is not
/// a regular file, or is a link.
#[cfg(unix)]
fn read_gate_file(path: &Path) -> Option<String> {
    use std::io::Read;
    let mut file = open_gate_file(path, false)?;
    let mut text = String::new();
    file.read_to_string(&mut text).ok()?;
    Some(text)
}

/// Replaces one of the gate's files by writing a sibling and renaming it over the
/// name.
///
/// The rename is what closes the last way a planted name could matter: a rename
/// replaces whatever sits at the destination instead of writing through it, so a
/// link left at the name is replaced by a regular file rather than followed. The
/// sibling carries this process's id because two works waiting on the next build
/// claim it at once and neither may read the other's half-written line; it is
/// hidden and pid-suffixed rather than shared so that the rename is always the
/// only thing that decides the contents at the name.
#[cfg(unix)]
fn write_gate_file(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(".{name}.{}", std::process::id()));
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&tmp)?;
        // The temporary file is made under this process's umask and then
        // renamed onto the gate's name, so without this the file the rename
        // lands carries a mode only this process can write -- and the rename is
        // what makes it the gate's, not who performed it. That is the same
        // state `open_gate_file` corrects on open, and it has to be corrected
        // here too: the two writers of these files are different programs, so
        // setting it on one path leaves the other path's files unshareable, and
        // the reading that says so is a builder refused the right to claim its
        // own turn. Set before the write so the name never exists in the wrong
        // mode even for an instant.
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(GATE_FILE_MODE));
        file.write_all(text.as_bytes())?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(not(unix))]
fn read_gate_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

#[cfg(not(unix))]
fn write_gate_file(path: &Path, text: &str) -> std::io::Result<()> {
    std::fs::write(path, text)
}

/// Creating is how a bound that rose takes effect: the slots above the bound that
/// existed when the directory was made are files nobody has written yet, and the
/// first build to reach one makes it. Lowering the bound never removes a file
/// that is held -- a slot file's meaning is its lock, so unlinking one another
/// process holds would hand the next arrival a second lock on the same slot. The
/// files the bound no longer admits are removed by [`lay_out_slots`], which
/// takes that lock before it removes anything, so the names go while each one is
/// provably empty.
#[cfg(unix)]
fn file_lock(path: &Path, how: libc::c_int) -> Result<GateLock, SlotLock> {
    let file = open_gate_file(path, true).ok_or(SlotLock::Held)?;
    use std::os::unix::io::AsRawFd;
    loop {
        // SAFETY: flock only touches the descriptor table of this process for a
        // descriptor this scope owns; EWOULDBLOCK is the expected "someone else
        // has it" answer and carries no memory obligations.
        let rc = unsafe { libc::flock(file.as_raw_fd(), how) };
        if rc == 0 {
            return Ok(GateLock(file));
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            // Somebody else has it: the one answer that may be read as "busy".
            Some(code) if code == libc::EWOULDBLOCK => return Err(SlotLock::Held),
            // A signal arrived before the call could decide. This is a retry,
            // not a refusal: reading it as "taken" turned the gate's own next
            // ask into a refusal of a slot that was free, and the refusal named
            // whoever had written its line there last.
            Some(code) if code == libc::EINTR => continue,
            // Anything else -- a lock table with no room, a descriptor the
            // kernel will not act on -- is neither a holder nor a wait, and is
            // kept as itself so a refusal can say what it actually met.
            _ => return Err(SlotLock::Unavailable(error)),
        }
    }
}

/// Why a slot could not be taken.
///
/// The distinction is the whole point: [`Held`](Self::Held) is another work
/// doing something, and every reading built on a refusal means "the host is
/// busy" only for that one. A call the kernel refused is the gate failing to
/// ask, and folding it into "taken" produces a refusal that names the last work
/// to have written its line into the slot as though it were the cause.
#[derive(Debug)]
enum SlotLock {
    /// Somebody else holds it.
    Held,
    /// The kernel refused the call itself.
    Unavailable(std::io::Error),
}

/// One of the gate's locks, released when the value goes.
///
/// `flock` belongs to the open file description, and closing a handle is not the
/// same as releasing the lock: a child forked while it is held carries a copy of
/// the description into its own table until it reaches `exec` (close-on-exec
/// fires there, not at the fork), and a `dup` leaves a second reference for as
/// long as its owner keeps it. A lock released by closing therefore stays taken
/// until the last copy goes -- so the gate's own next ask, microseconds later,
/// is refused by the copy of a lock the gate has already given up. That is not a
/// theory: on a host with one slot a single leaked child pinned it indefinitely,
/// and the tests that drop a permit and immediately ask again failed in CI.
/// Unlocking here reaches the description itself, so the lock ends with the
/// value that took it.
#[derive(Debug)]
struct GateLock(std::fs::File);

impl std::ops::Deref for GateLock {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for GateLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            // SAFETY: `flock` touches only this process's descriptor table for
            // a descriptor this value owns, and an unlock carries no memory
            // obligations.
            unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

/// Publishes the gate's readings.
#[async_trait]
impl Observable for BuildGate {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        // The scrape is one of the two places the bound is re-derived: a reader
        // is looking at the host exactly then, and a bound published from a
        // reading nobody has refreshed is the reading this module is for.
        self.step().await;
        let slots = if self.in_force() { self.bound() } else { 0 };
        let (ceiling, dimension) = self.ceiling();
        // Read from the directory rather than counted here, because the question
        // is what the host is carrying and not what this process is doing with
        // it: a builder that is not this process does not report to this
        // process, and its hold is visible exactly where it put it.
        let identity = self.identity();
        let origin = Origin {
            dir: &identity,
            role: BUILD_GATE_ROLE_BUILDER,
        };
        let mut metrics = readings(
            slots,
            cog_core::admission::build_slots_held(&self.dir),
            ceiling,
            dimension,
            self.counters(),
            origin,
            self.wait_budget_ms(),
        );
        // The holds are read from the same directory the occupancy count comes
        // from, and for the same reason: what matters is who is on the host, not
        // who asked this process. Published as a value beside the refusal that
        // names the same holder, because the refusal only exists for an ask that
        // happened to be made while the hold was in force.
        metrics.extend(holder_metrics(&self.dir, slots, &origin));
        Ok(metrics)
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

    /// The gate bounds every builder on the host, not only the one that made the
    /// directory: a second builder that cannot create a lock file cannot ask for
    /// a slot at all.
    #[test]
    fn the_gate_directory_is_opened_to_every_builder() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gate");

        // The case that matters is the one on a host that has already run a
        // build: the directory exists, owned by whoever ran first, and the mode
        // is whatever the umask gave. Installing the gate has to leave it usable
        // by the next builder, whatever user that is.
        fs::create_dir_all(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

        let _ = gate(&path, 1, 0);

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            mode, GATE_DIR_MODE,
            "a directory that predates the gate keeps its old mode, and only the builder that created it can take a slot"
        );
    }

    /// The holder's name and the claim on the next build are written by whoever
    /// holds the file, which is not always whoever created it.
    #[test]
    fn a_gate_file_is_writable_by_the_builder_that_opens_it() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CLAIM_FILE);

        let held = open_gate_file(&path, true).unwrap();

        let mode = held.metadata().unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            mode, GATE_FILE_MODE,
            "the file a build holds is one only its creator can write, so the next builder on the host cannot record that it is holding it"
        );
    }

    /// The other writer of these files is the one that writes by *path* -- the
    /// bound, and the claim on the next build -- and it goes through a temporary
    /// file and a rename. That path has to land the same mode, and it did not:
    /// the file was made under the process's umask, so the cluster's copy was
    /// readable by the host and not writable by it, and a host build could read
    /// the gate and never claim a turn on it.
    #[test]
    fn a_gate_file_written_by_path_lands_the_shared_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(RESERVE_FILE);

        write_gate_file(&path, "host build\t1791783276\n").unwrap();

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            mode, GATE_FILE_MODE,
            "the claim on the next build is rewritten by every later asker, so a mode from the umask leaves all but the first unable to ask"
        );
        // The temporary file must not survive the rename: one left beside the
        // gate's files is neither a slot nor a claim, and the reader that walks
        // the directory would find a name it has no meaning for.
        let leftovers: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with('.'))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary files left behind: {leftovers:?}"
        );
    }

    /// The reading the bound's claim is checked against counts every builder,
    /// and it is read from the locks rather than from what the builders report.
    #[tokio::test]
    async fn the_occupied_count_sees_a_lock_this_process_did_not_take() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);

        assert_eq!(occupied(&gate).await, 0, "nothing is running yet");

        // A slot held by anyone at all, taken the way another builder on the
        // host would take it: by locking the file, without this process knowing.
        let slot = file_lock(&dir.path().join("slot-0.lock"), libc::LOCK_EX).unwrap();
        assert_eq!(
            occupied(&gate).await,
            1,
            "a build this process never admitted is a build the host is carrying"
        );

        drop(slot);
        assert_eq!(occupied(&gate).await, 0);
    }

    async fn occupied(gate: &Arc<BuildGate>) -> usize {
        gate.collect_metrics("D5")
            .await
            .unwrap()
            .iter()
            .find(|m| m.name == BUILD_GATE_OCCUPIED)
            .map(|m| m.value as usize)
            .expect("the gate publishes the occupied count on every scrape")
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

    /// The self-replacement path has no next round to come back on: it stages a
    /// binary and then execs the process image away. A refusal there leaves the
    /// staged binary unswitched until some unrelated change asks again, so it
    /// waits instead -- and it has to claim the next build while it waits, or a
    /// build arriving in the gap the running one leaves takes the field first
    /// and this wait never ends.
    #[tokio::test]
    async fn an_exclusive_wait_takes_the_field_once_the_build_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let running = gate.try_acquire("a change verification").await.unwrap();

        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.await_exclusive("self-exec switch", Duration::from_secs(30))
                    .await
            })
        };
        tokio::time::sleep(POLL_INTERVAL * 2).await;
        assert!(
            !waiter.is_finished(),
            "a build holds the field, so the switch cannot have taken it"
        );
        assert_eq!(
            gate.reservation().map(|(what, _)| what).as_deref(),
            Some("self-exec switch"),
            "a wait that cannot take the field claims the next build"
        );
        assert!(
            gate.try_acquire("an arrival").await.is_err(),
            "an arrival must not take the build a waiting switch claimed"
        );

        drop(running);
        let field = waiter
            .await
            .unwrap()
            .expect("the wait ends when the build it was waiting for leaves");
        assert!(field.held(), "the switch holds the field after the wait");
        assert!(
            gate.try_acquire("an arrival").await.is_err(),
            "builds stay out for as long as the switch holds the field"
        );

        drop(field);
        assert!(
            gate.try_acquire("an arrival").await.is_ok(),
            "releasing the switch releases the field"
        );
    }

    /// The budget belongs to the caller: only it knows how long its work can
    /// wait. Running out is a refusal, not a silent permit -- a switch that
    /// believed it was alone and was not is the failure this guards.
    #[tokio::test]
    async fn an_exclusive_wait_gives_up_when_its_budget_runs_out() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let running = gate.try_acquire("a change verification").await.unwrap();

        let refused = gate
            .await_exclusive("self-exec switch", Duration::ZERO)
            .await
            .expect_err("the field is held, and the budget has no room to wait");
        let text = refused.to_string();
        assert!(
            text.contains("self-exec switch") && text.contains("\"a change verification\""),
            "a refusal has to name the switch that asked and the build that held it, got: {text}"
        );
        drop(running);
    }

    /// Every site in this crate that replaces the process image first takes
    /// the build field.
    ///
    /// The hazard is an *order*, not a function: the switcher lives in a lower
    /// crate that cannot reach this gate, so the order can only be enforced
    /// where the switch is called. A third call site that forgets the wait
    /// reintroduces the truncation in a place no other test looks, and it will
    /// look correct until the one round where a build is in flight.
    #[test]
    fn every_switch_in_this_crate_waits_for_the_build_field() {
        // Built in two pieces so this scan does not find its own needle.
        let needle = concat!("switch_and_", "restart()");
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sites = 0;
        for path in rust_files(&root) {
            let text = fs::read_to_string(&path).unwrap();
            for (at, _) in text.match_indices(needle) {
                // Only a call: `foo.switch_and_restart()` ends with a dot or a
                // receiver, a declaration has none and the needle would not
                // contain `()` there anyway.
                let before = &text[..at];
                let window: Vec<&str> = before.lines().rev().take(80).collect();
                assert!(
                    window.iter().any(|line| line.contains("await_exclusive")),
                    "{}: this switch does not wait for the build field -- an exec \
                     here kills whatever build is in flight and leaves its lock behind",
                    path.display()
                );
                sites += 1;
            }
        }
        assert!(
            sites >= 2,
            "the scan found {sites} switch sites in this crate; it is meant to see \
             the auto-deploy and the admin paths, so it is looking in the wrong place"
        );
    }

    fn rust_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }
        out
    }

    /// A refusal used to name only the caller that was turned away, which says
    /// nothing about why: the permission carries no identity, so answering "who
    /// has it" meant a `ps` inside the container. The holder writes its own name
    /// into the slot it locked, and the refusal reads it back.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_refusal_names_the_work_holding_the_slot() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);

        let held = gate.try_acquire("change verification").await.unwrap();
        let refused = gate
            .try_acquire("mainline advance")
            .await
            .expect_err("the only slot is taken");
        let text = refused.to_string();
        assert!(
            text.contains("mainline advance") && text.contains("\"change verification\""),
            "a refusal has to name the requester and the holder, got: {text}"
        );

        drop(held);
    }

    /// The hold as a value, not only as a sentence inside a refusal: a refusal
    /// exists only for an ask that was made while the hold was in force, and the
    /// starved case is the one where the waiter stopped asking. Published at
    /// scrape time so it can be read long after the last refusal.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_held_slot_publishes_its_holder_and_its_tenure() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);

        let held = gate.try_acquire("change verification").await.unwrap();
        let metrics = gate.collect_metrics("D5").await.unwrap();
        let row = metrics
            .iter()
            .find(|m| m.name == BUILD_GATE_HOLDER_SECONDS)
            .expect("a taken slot is a hold, and a hold names its holder");
        assert_eq!(
            row.labels.get("what").map(String::as_str),
            Some("change verification"),
            "the holder's own words, not a label from a closed set"
        );
        assert_eq!(
            row.labels.get("slot").map(String::as_str),
            Some("0"),
            "which slot the hold is on"
        );
        assert!(row.value >= 0.0, "tenure is seconds since the claim");

        drop(held);
        let after = gate.collect_metrics("D5").await.unwrap();
        assert!(
            !after.iter().any(|m| m.name == BUILD_GATE_HOLDER_SECONDS),
            "a free slot has no holder to publish"
        );
    }

    /// The record outlives the holder: a process that dies leaves the line it
    /// wrote while the kernel releases the lock. Reporting that line would make
    /// every later refusal blame work that finished long ago, so the lock is
    /// asked whether the record still has a subject.
    #[cfg(unix)]
    #[test]
    fn a_record_left_by_a_departed_holder_is_not_named() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("slot-0.lock"),
            b"4242\t1700000000\tchange verification\n",
        )
        .unwrap();

        let report = holder_report(dir.path(), 1);
        assert!(
            !report.contains("change verification"),
            "a lock nobody holds has no holder to name, got: {report}"
        );
        assert!(report.contains("none of them named"), "got: {report}");
    }

    /// An exclusion has to exclude *builds*, and one slot does not do that: the
    /// gate admits as many builds as it has slots, so a holder of one keeps the
    /// others out only while that count happens to be one. This is the promise
    /// the two callers that take the gate to keep builds away were relying on
    /// without saying so, and it is why it takes every slot.
    #[tokio::test]
    async fn an_exclusion_keeps_every_build_out_even_where_several_may_run() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 3, 0);

        let excluding = gate
            .try_acquire_exclusive("sweeping the build cache")
            .await
            .unwrap();
        assert!(excluding.held(), "an exclusion holds its slots");
        for name in ["a build that starts now", "and another"] {
            assert!(
                gate.try_acquire(name).await.is_err(),
                "{name} must not start while builds are excluded"
            );
        }
        assert_eq!(
            reading(&gate, BUILD_GATE_EXCLUDED).await,
            1.0,
            "the exclusion is readable while it lasts"
        );

        drop(excluding);
        assert_eq!(
            reading(&gate, BUILD_GATE_EXCLUDED).await,
            0.0,
            "it stops being an exclusion when it is released"
        );
        assert!(
            gate.try_acquire("after the sweep").await.is_ok(),
            "every slot comes back"
        );
    }

    /// An exclusion is not a build, so it must not be counted as one. The
    /// in-flight gauge and the held readings answer "how busy is this host
    /// building", and a sweep or a registry restart folded into them would read
    /// as a build that occupied the host for as long as it ran — which is the
    /// reading the bound itself is sized from.
    #[tokio::test]
    async fn an_exclusion_is_not_counted_as_a_build_that_ran() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 2, 0);

        let excluding = gate
            .try_acquire_exclusive("restarting the registry")
            .await
            .unwrap();
        assert_eq!(
            reading(&gate, BUILD_GATE_IN_FLIGHT).await,
            0.0,
            "no build is running while a sweep holds every slot"
        );
        drop(excluding);
        assert_eq!(
            reading(&gate, BUILD_GATE_ACQUIRED_TOTAL).await,
            0.0,
            "the exclusion did not take a slot as a build"
        );
        assert_eq!(
            reading(&gate, BUILD_GATE_HELD_MS_TOTAL).await,
            0.0,
            "and it did not hold the host the way a build does"
        );
    }

    /// A refusal has to take nothing. The exclusion asks for one file and either
    /// has it or does not, so there is no partial hold to leave behind — and the
    /// slot a build is holding is not what decides it, because the exclusion is
    /// not asking for slots.
    #[tokio::test]
    async fn an_exclusion_that_cannot_take_the_field_takes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 3, 0);
        // A build running: it holds the field, and its slot is not the point.
        let running = gate.try_acquire("a build that is running").await.unwrap();
        assert!(running.held());

        let refused = gate.try_acquire_exclusive("sweeping the build cache").await;
        assert!(
            refused.is_err(),
            "a build holds the field, so builds cannot be excluded"
        );
        assert_eq!(
            reading(&gate, BUILD_GATE_EXCLUDED).await,
            0.0,
            "an exclusion that was refused is not an exclusion that is in force"
        );
        assert_eq!(
            refused_reading(&gate).await,
            0.0,
            "no build was refused a slot here: an exclusion that could not be taken is not a build turned away"
        );
        drop(running);
        assert!(
            gate.try_acquire_exclusive("sweeping the build cache")
                .await
                .is_ok(),
            "once the build is out, the field is the sweep's"
        );
    }

    /// The exclusion is a promise about *builds*, and it has to hold at every
    /// bound. As a take of every slot it held only while the bound was one, and
    /// the bound is now something the host decides, so the promise cannot be a
    /// property of how many slots there are.
    #[tokio::test]
    async fn an_exclusion_holds_the_field_whatever_the_bound_is() {
        let dir = tempfile::tempdir().unwrap();
        // A bound the host has no say in, well above one.
        let gate = Arc::new(BuildGate::new(&BuildGateConfig {
            enabled: true,
            max_concurrent: 8,
            wait_secs: 0,
            lock_dir: dir.path().to_string_lossy().into_owned(),
        }));
        let excluding = gate
            .try_acquire_exclusive("restarting the registry")
            .await
            .unwrap();
        for name in ["a build that starts now", "and another"] {
            assert!(
                gate.try_acquire(name).await.is_err(),
                "{name} must not start while builds are excluded"
            );
        }
        assert!(excluding.held());
    }

    /// Every dimension is a count of builds: what is left of it, divided by what
    /// one build demands. The tightest is the answer, and which one it was is
    /// part of the reading — a bound of one on a saturated disk is a different
    /// fact from a bound of one on a full host.
    #[test]
    fn the_ceiling_is_the_tightest_dimension() {
        let load = |cpu: f64, io60: f64, mem_available: u64| HostLoad {
            pressure: Pressure {
                cpu_avg10: 0.0,
                cpu_avg60: cpu,
                io_avg10: 0.0,
                io_avg60: io60,
                mem_avg10: 0.0,
                mem_avg60: 0.0,
                mem_available_bytes: mem_available,
                builds_in_flight: 0,
            },
            busy_cores: 0.0,
            cores: 24,
            budget: None,
        };
        let plenty = 64 * 1024 * 1024 * 1024;
        // Derived rather than written out: this is the cpu dimension's own
        // arithmetic, and a second copy of the answer here would only be a place
        // for it to go stale the next time the per-build demand is re-measured.
        let cpu_bound = (24.0 / BUILD_DEMAND_CORES).floor() as usize;

        assert_eq!(
            load(0.0, 0.1, plenty).ceiling(),
            (cpu_bound, DIM_CPU),
            "a quiet host is bounded by what its cores can run: 24 cores over {BUILD_DEMAND_CORES} each"
        );
        assert_eq!(
            load(0.0, IO_STALL_BUDGET_PCT, plenty).ceiling(),
            (1, DIM_IO),
            "a device showing the whole stall budget absorbs one build"
        );
        assert_eq!(
            load(0.0, 0.1, MEM_FLOOR_BYTES).ceiling(),
            (1, DIM_MEMORY),
            "memory down to the floor leaves room for one build, not zero"
        );
        assert_eq!(
            load(0.0, IO_STALL_BUDGET_PCT * 3.0, MEM_FLOOR_BYTES).ceiling(),
            (1, DIM_MEMORY),
            "the floor is a report about memory, not about the disk: whichever is tighter is named"
        );
        assert_eq!(
            load(0.0, IO_STALL_BUDGET_PCT / 2.0, plenty).ceiling(),
            (2, DIM_IO),
            "half the stall budget spent means the device is absorbing two builds"
        );
    }

    /// The cgroup the builds run in is a dimension of its own, and it is the one
    /// whose ceiling is a wall: a build over it is killed, so it is read whatever
    /// the host has free, and it is named apart from the host's own memory.
    #[test]
    fn the_containers_memory_limit_caps_the_ceiling() {
        let gib = 1024 * 1024 * 1024;
        let load = |mem_max: u64, mem_current: u64, mem_reclaimable: u64| HostLoad {
            pressure: Pressure {
                cpu_avg10: 0.0,
                cpu_avg60: 0.0,
                io_avg10: 0.0,
                io_avg60: 0.1,
                mem_avg10: 0.0,
                mem_avg60: 0.0,
                mem_available_bytes: 64 * gib,
                builds_in_flight: 0,
            },
            busy_cores: 0.0,
            cores: 24,
            budget: Some(BuildBudget {
                mem_max,
                mem_current,
                mem_reclaimable,
            }),
        };
        // One build per core the host could run, and then some: a limit this
        // roomy is above the cpu dimension's own answer, so the container is not
        // what is deciding and the reading has to say `cpu`.
        let cpu_bound = (24.0 / BUILD_DEMAND_CORES).floor() as usize;
        let roomy = (cpu_bound as u64 + 1) * BUILD_PEAK_RSS_BYTES;

        assert_eq!(
            load(roomy, 0, 0).ceiling(),
            (cpu_bound, DIM_CPU),
            "a limit with room for one build per core and then some leaves the host deciding"
        );
        assert_eq!(
            load(8 * gib, 4 * gib, 0).ceiling(),
            (1, DIM_CGROUP_MEMORY),
            "8 GiB with half of it charged and none of it cache leaves room for one build of {BUILD_PEAK_RSS_BYTES} bytes, not two"
        );
        assert_eq!(
            load(20 * gib, 8 * gib, 0).ceiling(),
            (2, DIM_CGROUP_MEMORY),
            "and the count rises with the limit, which is what makes it a reading rather than a setting"
        );
        assert_eq!(
            load(gib, gib, 0).ceiling(),
            (1, DIM_CGROUP_MEMORY),
            "a cgroup with nothing left is still one build: a bound of zero refuses every build forever"
        );
        // The shape a cgroup that has built before is in: charged close to its
        // limit, and most of the charge the pages of the build's own reads. The
        // two answers below are the same cgroup, told apart only by whether the
        // cache in it is counted as spent.
        assert_eq!(
            load(16 * gib, 14 * gib, 0).ceiling(),
            (1, DIM_CGROUP_MEMORY),
            "counting the page cache as spent pins this dimension at one, which is what it did"
        );
        assert_eq!(
            load(16 * gib, 14 * gib, 12 * gib).ceiling(),
            (3, DIM_CGROUP_MEMORY),
            "12 of those 14 GiB are cache the kernel gives back, so the limit has room for three builds"
        );
        // The same pair read off this deployment on 2026-10-13, where the
        // cgroup held 14.0 GiB of file pages and only 3.5 GiB of them inactive:
        // the inactive line is the reading that said one, and the file page
        // line -- the memory the kernel demotes and reclaims either way -- says
        // three.
        assert_eq!(
            load(16 * gib, 16_595_374_080, 3_779_870_720).ceiling(),
            (1, DIM_CGROUP_MEMORY),
            "the inactive part alone leaves under one build and floors this dimension at one"
        );
        assert_eq!(
            load(16 * gib, 16_595_374_080, 15_053_443_072).ceiling(),
            (3, DIM_CGROUP_MEMORY),
            "the file pages are room for three, on a cgroup that never reached its limit"
        );
    }

    /// What the cgroup is charged is not what a build can spend, and the part
    /// that is taken back has to be read out of the cgroup's own accounting: a
    /// number read off the wrong line would give back memory that is not there.
    #[test]
    fn the_reclaimable_part_is_the_file_pages_and_all_of_them() {
        // The shape read off this deployment's evolution cgroup on 2026-10-13:
        // 14.0 GiB of file pages against a 16 GiB limit, 3.5 GiB of them
        // inactive and 10.5 GiB active.
        let stat = "\
anon 854978560
file 15053443072
kernel 686620672
inactive_file 3779870720
active_file 11273572352
unevictable 28532736
";
        assert_eq!(
            reclaimable_file_bytes(stat),
            15053443072,
            "the file pages are what the kernel gives back, active list or not"
        );
        assert!(
            reclaimable_file_bytes(stat) > 3779870720,
            "reading only the inactive ones is what pinned this dimension at one: \
             the other 10.5 GiB is cache the kernel demotes and reclaims like any other"
        );
        assert_eq!(
            reclaimable_file_bytes("anon 4096\n"),
            0,
            "a cgroup that names no file pages gets nothing given back, not everything"
        );
        assert_eq!(
            reclaimable_file_bytes("file_mapped 4096\nfile_dirty 2048\n"),
            0,
            "`file` followed by a letter is a different line and not this one"
        );
        assert_eq!(
            reclaimable_file_bytes(""),
            0,
            "and neither does a cgroup whose accounting cannot be parsed at all"
        );
    }

    /// 同一个夹具，把 `memory.max` 抬上去之后再读一次，ceiling 必须跟着抬。
    ///
    /// 这是这个模块被欠下的那笔账：容量是**每次读现算**的，不是开机记住的——容器
    /// 中生命期被抬高内存限额（改 manifest、换一台机器）之后，它不该继续被启动那一刻
    /// 的数箍着。**射程说清**：这里钉的是「读这件事本身」每次重新从文件推，它钉不住
    /// 「以后有人在它上面加一层一次性缓存」——两者在读数上同形，隔一层只能靠读代码。
    /// 另外，`step()` 那条路上真值每 `BOUND_WINDOW` 才动一次，所以中生命期的调整是
    /// **一个窗口之内**被看见、不是立刻；那个窗口是有意留的，不在这里判。
    #[test]
    fn a_raised_limit_is_seen_by_the_next_read() {
        let dir = tempfile::tempdir().unwrap();
        let cg = dir.path().join("kubepods.slice/cogneva");
        fs::create_dir_all(&cg).unwrap();
        fs::write(cg.join("memory.current"), "1000000000").unwrap();
        fs::write(cg.join("memory.stat"), "anon 1000000000\nfile 0\n").unwrap();
        fs::write(cg.join("memory.max"), "4000000000").unwrap();

        let before = BuildBudget::read_at(dir.path(), "/kubepods.slice/cogneva")
            .unwrap()
            .ceiling();

        // 只改限额，别的一个字节不动。
        fs::write(cg.join("memory.max"), "16000000000").unwrap();
        let after = BuildBudget::read_at(dir.path(), "/kubepods.slice/cogneva")
            .unwrap()
            .ceiling();

        assert!(
            after > before,
            "限额从 4 GB 抬到 16 GB，ceiling 却从 {before} 变成 {after}：读到的是记住的那个数"
        );
        // 抬上来的应当是**整个差**，不是在某个被挑中的数附近晃：高处那 12 GB 全进 ceiling。
        assert!(
            (after - before - 12_000_000_000f64 / BUILD_PEAK_RSS_BYTES as f64).abs() < 1e-9,
            "抬高的 12 GB 没有整个进 ceiling：{before} -> {after}"
        );
    }

    /// The bound comes down at once and goes up only on a quiet host. Both
    /// directions matter: coming down late is the host degrading, going up early
    /// is the host degrading on the way back.
    #[test]
    fn the_bound_falls_at_once_and_rises_only_on_a_calm_host() {
        assert_eq!(
            next_bound(4, 1, false),
            1,
            "a bound above what the host is showing comes down to it"
        );
        assert_eq!(
            next_bound(1, 4, false),
            1,
            "a bound below the ceiling does not rise while the host is busy"
        );
        assert_eq!(
            next_bound(1, 4, true),
            4,
            "a quiet host gets the whole ceiling back"
        );
        assert_eq!(
            next_bound(0, 0, false),
            1,
            "there is no bound of zero: a host with no room runs one build"
        );
    }

    /// A host that cannot be read is not a host to size a bound for: the
    /// dimension that failed to read would take the place of one that is idle.
    #[test]
    fn a_host_that_is_quiet_on_every_dimension_is_calm() {
        let quiet = HostLoad {
            pressure: Pressure {
                cpu_avg10: 0.0,
                cpu_avg60: CPU_CALM_PCT - 0.1,
                io_avg10: 0.0,
                io_avg60: IO_CALM_PCT - 0.1,
                mem_avg10: 0.0,
                mem_avg60: MEM_CALM_PCT - 0.1,
                mem_available_bytes: 64 * 1024 * 1024 * 1024,
                builds_in_flight: 0,
            },
            busy_cores: 1.0,
            cores: 24,
            budget: None,
        };
        assert!(quiet.is_calm(), "a quiet host is calm");

        let busy_disk = HostLoad {
            pressure: Pressure {
                io_avg60: IO_CALM_PCT + 0.1,
                ..quiet.pressure
            },
            ..quiet
        };
        assert!(
            !busy_disk.is_calm(),
            "one dimension over its line is enough: the disk is what a bound of one is usually about"
        );
    }

    /// The bound is a fact about the host that every process on it shares, so it
    /// is persisted rather than derived per process: a builder that has just
    /// started enforces what the host was measured at, not a guess of its own.
    #[tokio::test]
    async fn a_derived_bound_is_read_back_from_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let gate = derived_gate(dir.path(), 0);

        assert_eq!(
            gate.bound(),
            1,
            "nothing measured and nothing persisted is one build, not none"
        );

        std::fs::write(dir.path().join(BOUND_FILE), b"3\n").unwrap();
        assert_eq!(
            gate.bound(),
            3,
            "the number on disk is the bound a process enforces"
        );

        std::fs::write(dir.path().join(BOUND_FILE), b"not a number\n").unwrap();
        assert_eq!(
            gate.bound(),
            1,
            "a bound that cannot be read is not a bound to raise the host to"
        );
    }

    /// Deriving moves the fenced number: the reading is taken again when the
    /// bound is asked for and when it is published, and what it derived lands in
    /// the directory for the other processes on the host.
    #[tokio::test]
    async fn a_deriving_gate_measures_the_host_and_persists_what_it_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let gate = derived_gate(dir.path(), 0);

        let _permit = gate
            .try_acquire("a build on a machine that can be read")
            .await;
        let metrics = gate.collect_metrics("D5").await.unwrap();
        let value = |name: &str| {
            metrics
                .iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("{name} is published"))
        };
        let slots = value(BUILD_GATE_SLOTS).value as usize;
        let ceiling = value(BUILD_GATE_CEILING).value as usize;
        let dimension = value(BUILD_GATE_CEILING)
            .labels
            .get("dimension")
            .cloned()
            .unwrap_or_default();

        assert!(
            matches!(dimension.as_str(), DIM_CPU | DIM_MEMORY | DIM_IO),
            "the ceiling has to name the dimension it came from, got {dimension}"
        );
        assert!(
            slots >= 1 && slots <= ceiling.max(1),
            "the bound is between one and the ceiling it was derived from, got {slots} of {ceiling}"
        );
        let persisted: usize = std::fs::read_to_string(dir.path().join(BOUND_FILE))
            .expect("a deriving gate persists its bound")
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            persisted, slots,
            "what is enforced is what is written down for the rest of the host"
        );
    }

    /// The directory is writable by every builder, and not all of them are the
    /// same user, so a name in it is something the other side can plant. A link
    /// at the name the bound is written to would have the privileged writer put
    /// the bound wherever the link pointed; the write replaces the name instead
    /// of following it.
    #[cfg(unix)]
    #[test]
    fn a_link_at_the_bounds_name_is_replaced_not_written_through() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"untouched\n").unwrap();
        let bound_path = dir.path().join(BOUND_FILE);
        std::os::unix::fs::symlink(&victim, &bound_path).unwrap();

        write_gate_file(&bound_path, "3\n").unwrap();

        assert_eq!(
            std::fs::read_to_string(&victim).unwrap(),
            "untouched\n",
            "the file the link pointed at is not where the bound goes"
        );
        assert_eq!(
            read_gate_file(&bound_path).as_deref(),
            Some("3\n"),
            "the bound is at its own name after the write"
        );
        assert!(
            !std::fs::symlink_metadata(&bound_path)
                .unwrap()
                .file_type()
                .is_symlink(),
            "the name holds the bound itself, not a link to it"
        );
    }

    /// A link is not followed on the way in either: read as its target, a link
    /// would let the unprivileged side choose the bound the privileged side
    /// enforces — a file it can write is a file it can point the bound at.
    #[cfg(unix)]
    #[test]
    fn a_link_where_the_bound_is_read_is_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"99\n").unwrap();
        std::os::unix::fs::symlink(&victim, dir.path().join(BOUND_FILE)).unwrap();

        assert!(
            read_gate_file(&dir.path().join(BOUND_FILE)).is_none(),
            "a link is not a bound"
        );
        let gate = derived_gate(dir.path(), 0);
        assert_eq!(
            gate.bound(),
            1,
            "a link at the bound's name leaves the host unmeasured, not bounded at the target's number"
        );
    }

    /// The bound says what the gate admits and the directory says what exists.
    /// A bound that fell leaves the difference on disk, where anyone counting
    /// files reads a gate that hands out more room than it does.
    #[cfg(unix)]
    #[test]
    fn a_bound_that_fell_takes_its_slot_files_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 2, 0);
        let stale = dir.path().join("slot-1.lock");
        std::fs::write(&stale, b"").unwrap();

        gate.persist_bound(1);

        assert!(
            !stale.exists(),
            "the slot the bound no longer admits is not left on the host"
        );
        assert!(
            dir.path().join("slot-0.lock").exists(),
            "the slot the bound does admit is there for a builder that cannot make it"
        );
    }

    /// A slot file's meaning is its lock, so the one file a bound must not take
    /// with it is the one a build is in: removing the name would let the next
    /// arrival create a second file under it and two builds would sit in one slot.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_slot_a_build_is_in_survives_a_bound_that_fell() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 2, 0);
        let first = gate.try_acquire("a build in the first slot").await.unwrap();
        let second = gate
            .try_acquire("a build in the second slot")
            .await
            .unwrap();
        let in_use = dir.path().join("slot-1.lock");
        assert!(in_use.exists(), "the second build is in the second slot");

        gate.persist_bound(1);

        assert!(
            in_use.exists(),
            "a slot a build is in is not removed from under it"
        );
        drop(second);
        drop(first);
    }

    /// The slot a bound could not take while a build was in it is taken back on
    /// a later round, once that build is gone.
    ///
    /// What a bound falls past is not always free when it falls: the name it
    /// leaves is the one a build is in, and the pass that moves the bound skips
    /// it. Nothing at that moment records an obligation to come back, so if only
    /// a moving bound laid the directory out the name would stay for as long as
    /// the directory does, read by anyone counting files as room the host does
    /// not have. This drives the same reconcile a round with no bound movement
    /// runs, so the name comes back on the round after the build leaves.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_round_that_moves_no_bound_takes_back_the_room_above_it() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 2, 0);
        let first = gate.try_acquire("a build in the first slot").await.unwrap();
        let second = gate
            .try_acquire("a build in the second slot")
            .await
            .unwrap();
        gate.persist_bound(1);
        let stale = dir.path().join("slot-1.lock");
        assert!(stale.exists(), "the held slot survives the bound that fell");

        drop(second);
        drop(first);
        gate.reconcile_slots(1);

        assert!(
            !stale.exists(),
            "a bound that stood still still takes back the room above it once it is free"
        );
        assert!(dir.path().join("slot-0.lock").exists());
    }

    /// A work that keeps being turned away claims the next build, and a build
    /// that arrives before it stands down: the ask that lost the race every time
    /// is the one that has been waiting longest, and without this it loses it
    /// again.
    #[tokio::test]
    async fn a_claim_on_the_next_build_holds_the_arrivals_out() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let running = gate.try_acquire("a build that is running").await.unwrap();

        // The work that keeps coming back is refused, so it claims the next one.
        assert!(gate.try_acquire("mainline advance").await.is_err());
        gate.reserve_build_field("mainline advance");

        drop(running);
        let arrived = gate.try_acquire("a change verification");
        assert!(
            arrived.await.is_err(),
            "an arrival must not take the build the waiting work claimed"
        );

        let waiting = gate.try_acquire("mainline advance").await.unwrap();
        assert!(waiting.held(), "the claim is answered by its own work");
        assert!(
            gate.reservation().is_none(),
            "and it is released once the ask is answered"
        );
        drop(waiting);
        assert!(
            gate.try_acquire("a change verification").await.is_ok(),
            "the field is open to arrivals again"
        );
    }

    /// An exclusion that is turned away by a *build* claims the next one without
    /// its caller having to say so: the sweep's cycle is long and the builds it
    /// is losing to arrive back to back, so the walk after a refusal would find
    /// the next build already started. This is the wiring, not the claim itself
    /// — a caller that never asks for the claim is the whole failure mode.
    #[tokio::test]
    async fn an_exclusion_refused_by_a_build_claims_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let running = gate.try_acquire("a build that is running").await.unwrap();

        assert!(
            gate.try_acquire_exclusive("a build-cache sweep")
                .await
                .is_err(),
            "a build is holding the field, so nothing can be excluded from it"
        );
        assert_eq!(
            gate.reservation().map(|(what, _)| what).as_deref(),
            Some("a build-cache sweep"),
            "the refused exclusion must have claimed the next build on its own"
        );

        drop(running);
        assert!(
            gate.try_acquire("a change verification").await.is_err(),
            "the claimed build must not go to an arrival"
        );
        let sweep = gate.try_acquire_exclusive("a build-cache sweep").await;
        assert!(
            sweep.is_ok(),
            "the claim is answered by the work that made it"
        );
        assert!(
            gate.reservation().is_none(),
            "and the exclusion taking the field releases it"
        );
    }

    /// An exclusion takes the field whenever it is free, and taking it is not the
    /// same as answering the work that claimed the next build: that work's ask is
    /// still outstanding, so its claim has to outlive the pass. Released here it
    /// is gone before its holder asks again, which for a work on a longer cycle
    /// than the field's traffic is every turn.
    #[tokio::test]
    async fn an_exclusion_leaves_another_works_claim_standing() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let running = gate.try_acquire("a build that is running").await.unwrap();
        assert!(gate.try_acquire("mainline advance").await.is_err());
        gate.reserve_build_field("mainline advance");

        drop(running);
        let sweep = gate
            .try_acquire_exclusive("a build-cache sweep")
            .await
            .unwrap();
        assert_eq!(
            gate.reservation().map(|(what, _)| what).as_deref(),
            Some("mainline advance"),
            "the sweep's pass does not answer the advance's ask"
        );
        drop(sweep);
        assert!(
            gate.try_acquire("a change verification").await.is_err(),
            "and arrivals are still held out until that ask is answered"
        );
        let waiting = gate.try_acquire("mainline advance").await.unwrap();
        drop(waiting);
        assert!(
            gate.reservation().is_none(),
            "answered by its own work, and released then"
        );
    }

    /// A refusal is not a licence to take the turn another work is holding —
    /// arrivals stand down on the claim itself, whichever work left it. A second
    /// claim adds nothing to that and costs the work that asked first its place,
    /// so the first claim stands until its holder is answered or it lapses.
    #[tokio::test]
    async fn a_claim_already_held_is_left_to_its_holder() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let running = gate.try_acquire("a build that is running").await.unwrap();
        gate.reserve_build_field("a build-cache sweep");
        gate.reserve_build_field("mainline advance");
        assert_eq!(
            gate.reservation().map(|(what, _)| what).as_deref(),
            Some("a build-cache sweep"),
            "the work that asked first keeps the turn it claimed"
        );

        drop(running);
        assert!(
            gate.try_acquire("a change verification").await.is_err(),
            "arrivals stand down on that claim just the same"
        );
        let sweep = gate.try_acquire_exclusive("a build-cache sweep").await;
        assert!(sweep.is_ok(), "and it is answered by its own work");
        drop(sweep);
        assert!(gate.reservation().is_none());
    }

    /// The other half of that wiring: while a peer exclusion holds the field,
    /// builds are kept out already, so a second exclusion must not claim the
    /// next one — it would hold them out again after the field is free, with
    /// nothing left that it was claimed for. A shared lock is what tells the
    /// two obstacles apart, and this is where it has to.
    #[tokio::test]
    async fn an_exclusion_refused_by_a_peer_exclusion_claims_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let sweep = gate
            .try_acquire_exclusive("a build-cache sweep")
            .await
            .unwrap();

        assert!(
            gate.try_acquire_exclusive("a registry restart")
                .await
                .is_err(),
            "one exclusion at a time"
        );
        assert!(
            gate.reservation().is_none(),
            "a peer exclusion is not a build to be claimed past"
        );

        drop(sweep);
        assert!(
            gate.try_acquire("a change verification").await.is_ok(),
            "builds were never held out by the second exclusion"
        );
    }

    /// A claim that nobody refreshes is a work that stopped asking, and honoring
    /// it would hold builds out for a waiter that is gone. The window is the
    /// whole liveness rule, so it is the thing to test.
    #[tokio::test]
    async fn a_stale_claim_is_not_honored() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let stale = unix_now() - RESERVE_FRESH.as_secs() - 60;
        std::fs::write(
            dir.path().join(RESERVE_FILE),
            format!("mainline advance\t{stale}\n"),
        )
        .unwrap();

        assert!(
            gate.reservation().is_none(),
            "a claim past its window is not a claim"
        );
        assert!(
            gate.try_acquire("a change verification").await.is_ok(),
            "builds are not held out by a waiter that stopped asking"
        );
    }

    /// A fixed bound is the one bound that is not a reading: nothing about the
    /// machine can move it, and the reading has to say so rather than name a
    /// dimension it never looked at.
    #[tokio::test]
    async fn a_configured_bound_is_not_derived_and_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 2, 0);
        assert_eq!(gate.bound(), 2);
        assert_eq!(gate.ceiling(), (2, DIM_OVERRIDE));
        assert!(
            !dir.path().join(BOUND_FILE).exists(),
            "a gate that derives nothing has nothing to write down for the host"
        );
    }

    fn derived_gate(dir: &Path, wait_secs: u64) -> Arc<BuildGate> {
        Arc::new(BuildGate::new(&BuildGateConfig {
            enabled: true,
            max_concurrent: 0,
            wait_secs,
            lock_dir: dir.to_string_lossy().into_owned(),
        }))
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

    /// A slot comes back with the permit that took it, not with the last copy of
    /// its open file description.
    ///
    /// `flock` is owned by the open file description, and this process is not the
    /// only holder of that description: a child forked while the slot is held
    /// carries a copy until it reaches `exec`, and a `dup` leaves a second
    /// reference for as long as its owner keeps it. Letting the slot come back
    /// only when the last copy closes meant a caller arriving inside that window
    /// -- microseconds after the holder dropped its permit -- was told the slot
    /// was still taken, and on a host with one slot a stray child could pin it
    /// long after the work that took it had finished. The permit releases the
    /// lock on the description before its handles close, so the copy cannot
    /// outlive the permit's decision.
    ///
    /// The copy below is a `dup` rather than a forked child, because a fork copies
    /// the whole fd table: forked before it narrows that table, the child also holds
    /// what every other test in this binary has open, and each of those is a
    /// reference on somebody else's lock. The `dup` shares the description exactly
    /// as a forked child does, so it keeps the slot taken for as long as a child
    /// would have.
    ///
    /// Both files a permit holds are checked, because they are held for different
    /// readers: the slot is what the next build waits for, and the claim is what a
    /// cache sweep waits for.
    #[tokio::test]
    async fn a_slot_comes_back_with_the_permit_though_a_copy_of_the_lock_file_survives() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        fn duplicate(file: &std::fs::File) -> OwnedFd {
            // SAFETY: `file` is open for as long as the caller keeps it, so its
            // descriptor is valid, and the returned descriptor is this test's to
            // close exactly once.
            let fd = unsafe { libc::dup(file.as_raw_fd()) };
            assert!(fd >= 0, "dup failed");
            // `dup` copies the description but not the close-on-exec flag, and the
            // file it came from was opened with that flag set. Left clear, the copy
            // would ride into any child another test in this binary spawns during
            // the window and keep the lock held past the exec -- a reference this
            // test cannot see and does not drop. Setting the flag back keeps the
            // only references the ones this test holds.
            // SAFETY: `fd` is open and this thread owns it.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(flags >= 0, "fcntl F_GETFD failed");
            // SAFETY: as above; `F_SETFD` with a value built from the flags just read.
            let set = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
            assert!(set >= 0, "fcntl F_SETFD failed");
            // SAFETY: `fd` is a fresh descriptor owned by nobody else.
            unsafe { OwnedFd::from_raw_fd(fd) }
        }

        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);

        let permit = gate.try_acquire("the holding build").await.unwrap();
        // A build holds its claim and then its slot, in that order.
        let claim_copy = duplicate(&permit.held[0]);
        let slot_copy = duplicate(&permit.held[1]);

        drop(permit);
        assert!(
            gate.try_acquire_exclusive("sweeping the build cache")
                .await
                .is_ok(),
            "the field comes back with the permit, not with a copy of its claim"
        );
        let next = gate
            .try_acquire("the next build")
            .await
            .expect("the slot comes back with the permit, not with a copy of its descriptor");
        assert!(next.held());

        drop(next);
        drop(claim_copy);
        drop(slot_copy);
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
        assert_eq!(refused_total(&gate), 1);
        assert_eq!(
            refused_on(&gate, RefusedBy::WaitedOut),
            1,
            "the wall it hit was the wait budget, not a sweep or a reservation"
        );
    }

    fn refused_is_environmental() -> bool {
        SFError::ResourceExhausted("busy".into()).is_environment_failure()
    }

    /// Refusals this gate counted on one wall, whoever asked.
    fn refused_on(gate: &BuildGate, wall: RefusedBy) -> u64 {
        gate.refused
            .iter()
            .map(|asks| asks[wall.index()].load(Ordering::Relaxed))
            .sum()
    }

    /// Refusals this gate counted for one asker, on whatever wall.
    fn refused_by_asker(gate: &BuildGate, what: &str) -> u64 {
        let asker = asker_index(what);
        gate.refused[asker]
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum()
    }

    /// Refusals this gate counted over every wall and every asker.
    fn refused_total(gate: &BuildGate) -> u64 {
        gate.refused
            .iter()
            .flatten()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum()
    }

    /// A build turned away because the field is being swept is not a host that is
    /// simply busy, and the counter has to say which: one is maintenance standing
    /// the builds down on purpose, the other is capacity. Counted together they
    /// read as the same alert and send an operator to the wrong place.
    #[tokio::test]
    async fn a_build_refused_by_a_sweep_lands_on_its_own_wall() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 2, 0);
        let sweep = gate
            .try_acquire_exclusive("sweeping the build cache")
            .await
            .unwrap();
        assert!(sweep.held(), "the exclusion holds the field");
        let refused = gate.acquire("a one-shot build").await;
        assert!(refused.is_err(), "an exclusion admits no build at all");
        assert_eq!(
            refused_on(&gate, RefusedBy::Excluded),
            1,
            "the sweep is the wall this build hit"
        );
        assert_eq!(
            refused_on(&gate, RefusedBy::Busy),
            0,
            "and it is not the capacity reading, which would send the reader to the bound"
        );
        drop(sweep);
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
        assert_eq!(refused_total(&disabled), 0);
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

    /// The refusal counter summed over every wall, from the published rows: the
    /// one number the counter carried before it was split.
    async fn refused_reading(gate: &Arc<BuildGate>) -> f64 {
        gate.collect_metrics("D5")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == BUILD_GATE_REFUSED_TOTAL)
            .map(|m| m.value)
            .sum()
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
        let mut expected = vec![
            BUILD_GATE_SLOTS,
            BUILD_GATE_CEILING,
            BUILD_GATE_OCCUPIED,
            BUILD_GATE_IN_FLIGHT,
            BUILD_GATE_EXCLUDED,
            BUILD_GATE_WAITING,
            BUILD_GATE_ACQUIRED_TOTAL,
        ];
        expected.extend(std::iter::repeat_n(
            BUILD_GATE_REFUSED_TOTAL,
            ASKER_CELLS * RefusedBy::ALL.len(),
        ));
        expected.extend([
            BUILD_GATE_WAIT_MS_TOTAL,
            BUILD_GATE_WAIT_MS_MAX,
            BUILD_GATE_HELD_MS_TOTAL,
            BUILD_GATE_HELD_MS_MAX,
            BUILD_GATE_WAIT_BUDGET_MS,
        ]);
        assert_eq!(
            names, expected,
            "the series set does not depend on whether this process builds"
        );
        for metric in &published {
            assert_eq!(
                metric.value, 0.0,
                "{} must read zero where no build can be admitted",
                metric.name
            );
        }
        // One row per wall and per ask, all of them, even where none has been
        // hit: a reader summing the counter over either label has to get the
        // whole number, not the walls this process happens to have reached nor
        // the works it happens to have refused.
        let grid: Vec<(&str, &str)> = published
            .iter()
            .filter(|m| m.name == BUILD_GATE_REFUSED_TOTAL)
            .filter_map(|m| {
                let reason = m.labels.get("reason")?.as_str();
                let asker = m.labels.get("asker")?.as_str();
                Some((reason, asker))
            })
            .collect();
        let mut expected_grid: Vec<(&str, &str)> = Vec::new();
        for wall in RefusedBy::ALL {
            for asker in 0..ASKER_CELLS {
                expected_grid.push((wall.as_str(), asker_label(asker)));
            }
        }
        assert_eq!(
            grid, expected_grid,
            "every wall and every ask is published, so a refusal can be attributed to both"
        );
        assert_eq!(
            expected_grid
                .iter()
                .filter(|(_, asker)| *asker == BUILD_GATE_ASKER_UNNAMED)
                .count(),
            RefusedBy::ALL.len(),
            "the unnamed ask is a cell of its own, once per wall"
        );
    }

    /// A refusal has to say which work paid for it and not only which wall
    /// stopped it, because the wall is where the reading is and the work is
    /// where the remedy is: a host with one slot refuses whichever work comes
    /// back, and the work starved every round is the one nobody can see.
    ///
    /// The words the works ask under are a closed set, so an ask that is not on
    /// it is counted rather than lost: the total over the asks is still the
    /// total the counter published before this label existed.
    #[tokio::test]
    async fn a_refusal_says_which_work_paid_for_it() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 0);
        let _held = gate.try_acquire("change verification").await.unwrap();

        for _ in 0..2 {
            assert!(
                gate.try_acquire("mainline advance").await.is_err(),
                "the one slot is held, so the rollout is refused"
            );
        }
        assert!(
            gate.try_acquire("a build in the first slot").await.is_err(),
            "an ask the table does not name is refused like any other"
        );

        assert_eq!(refused_by_asker(&gate, "mainline advance"), 2);
        assert_eq!(refused_by_asker(&gate, "change verification"), 0);
        assert_eq!(
            refused_by_asker(&gate, "a build in the first slot"),
            1,
            "an ask under words the gate does not list is counted, not dropped"
        );
        assert_eq!(refused_total(&gate), 3);

        let published = gate.collect_metrics("D5").await.unwrap();
        let row = |asker: &str| -> Option<f64> {
            published
                .iter()
                .find(|m| {
                    m.name == BUILD_GATE_REFUSED_TOTAL
                        && m.labels.get("asker").map(String::as_str) == Some(asker)
                })
                .map(|m| m.value)
        };
        assert_eq!(row("mainline_advance"), Some(2.0));
        assert_eq!(row("change_verification"), Some(0.0));
        assert_eq!(row(BUILD_GATE_ASKER_UNNAMED), Some(1.0));
        assert_eq!(
            published
                .iter()
                .filter(|m| m.name == BUILD_GATE_REFUSED_TOTAL)
                .map(|m| m.value)
                .sum::<f64>(),
            3.0,
            "summed over the asks the counter is the number it carried before the label"
        );
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

    /// A refusal has to reach the account with the four things a counter cannot
    /// carry: when it happened, which wall, which bound was in force, and which
    /// work paid for it. The counter beside it survives only as long as the
    /// process does.
    #[tokio::test]
    async fn a_refusal_is_also_an_event_with_its_wall_and_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 1);

        let _held = gate.try_acquire("holder").await.unwrap();
        assert!(gate.acquire("one-shot build").await.is_err());

        let events = gate.drain_events();
        assert_eq!(events.len(), 1, "one refusal, one event: {events:?}");
        let event = &events[0];
        assert_eq!(event.kind, ResourceEventKind::AdmissionRefusal);
        assert_eq!(event.outcome, ResourceEventOutcome::Refused);
        assert!(
            event.subject.starts_with("one-shot build:"),
            "the subject names the work that was turned away: {}",
            event.subject
        );
        assert!(
            event.subject.ends_with(RefusedBy::WaitedOut.as_str()),
            "and the wall that turned it away: {}",
            event.subject
        );
        assert_eq!(event.reading, BUILD_GATE_SLOTS);
        assert_eq!(
            event.reading_value,
            Some(1.0),
            "the bound in force at the refusal is part of the row"
        );
        assert_eq!(event.count, 1);
        assert!(
            gate.drain_events().is_empty(),
            "draining hands the events over once; a second consumer must not see the same refusal"
        );
    }

    /// 第二类生产者走同一条环：拒绝这次滚动 Job 的是集群的配额准入，不是构建槽
    /// 的控制律，但账上的消费者只有一个。环属于**进程**，不属于界——两本账会让
    /// 「有一条消费没在跑」这类读数分叉。
    #[tokio::test]
    async fn an_event_from_another_producer_reaches_the_same_consumer() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 1);
        let registered = RegisteredGate(Arc::clone(&gate));
        let event = ResourceEvent {
            kind: ResourceEventKind::AdmissionRefusal,
            occurred_at: chrono::Utc::now(),
            subject: "mainline advance:quota".to_string(),
            actor: "cogneva-evolution-abc".to_string(),
            reading: "kubectl:job-failed-condition".to_string(),
            reading_value: None,
            from_value: None,
            to_value: None,
            outcome: ResourceEventOutcome::Refused,
            detail: serde_json::json!({"wall": "quota"}),
            count: 1,
        };
        ProcessGate::record_event(&registered, event.clone());
        assert_eq!(
            gate.drain_events(),
            vec![event],
            "an event handed in through the process-facing gate is the one the consumer takes"
        );
        assert!(gate.drain_events().is_empty());
    }

    /// A bound that moved carries both ends and the reading it moved on; a bound
    /// that did not move is not an event at all, or every quiet window would add
    /// a row saying nothing happened.
    #[test]
    fn a_bound_that_moved_is_an_event_with_both_ends() {
        let dir = Path::new("/tmp/gate-dir");
        let moved = bound_move_event(1, 3, 3, DIM_IO, true, dir, 24, 20.0)
            .expect("a move from 1 to 3 is an event");
        assert_eq!(moved.kind, ResourceEventKind::BoundChange);
        assert_eq!(moved.outcome, ResourceEventOutcome::Applied);
        assert_eq!(moved.subject, "build_gate.bound");
        assert_eq!(moved.reading, BUILD_GATE_CEILING);
        assert_eq!(moved.reading_value, Some(3.0));
        assert_eq!(moved.from_value, Some(1));
        assert_eq!(moved.to_value, Some(3));
        assert_eq!(moved.detail["dimension"], DIM_IO);
        assert_eq!(moved.detail["calm"], true);
        assert!(
            bound_move_event(2, 2, 2, DIM_IO, false, dir, 24, 24.0).is_none(),
            "a bound that stayed where it was is not a move"
        );
    }

    /// The ring is bounded, and what it pushes out is counted: a consumer that
    /// is not running must not make the account look like a world in which
    /// nothing happened.
    #[tokio::test]
    async fn a_full_ring_drops_the_oldest_and_counts_it() {
        let dir = tempfile::tempdir().unwrap();
        let gate = gate(dir.path(), 1, 1);
        let event = |n: i64| ResourceEvent {
            kind: ResourceEventKind::AdmissionRefusal,
            occurred_at: chrono::Utc::now(),
            subject: format!("asker-{n}"),
            actor: "test".to_string(),
            reading: BUILD_GATE_SLOTS.to_string(),
            reading_value: Some(1.0),
            from_value: None,
            to_value: None,
            outcome: ResourceEventOutcome::Refused,
            detail: serde_json::json!({}),
            count: 1,
        };
        for n in 0..(EVENT_RING_CAPACITY as i64 + 10) {
            gate.record_event(event(n));
        }
        assert_eq!(gate.events_dropped(), 10);
        let drained = gate.drain_events();
        assert_eq!(drained.len(), EVENT_RING_CAPACITY);
        assert_eq!(
            drained[0].subject, "asker-10",
            "the oldest are the ones pushed out, and the newest are what a consumer gets"
        );
    }
}
