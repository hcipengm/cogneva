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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::admission::{sample, Pressure};
use crate::config::BuildGateConfig;
use crate::contract::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use crate::{SFError, SFResult};

/// Builds holding a slot in this process, as a gauge.
pub const BUILD_GATE_IN_FLIGHT: &str = "cogneva_build_gate_in_flight";
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
            0,
            DIM_NONE,
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
    excluded: u64,
    waiting: u64,
    acquired: u64,
    refused: u64,
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
    /// Bytes the cgroup may use in total, and bytes it is using now.
    mem_max: u64,
    mem_current: u64,
}

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
        // has its own cgroup namespace; joining it onto the mount that the
        // process can see is right either way, because a cgroup namespace makes
        // `/` be the cgroup the process is in.
        let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = text
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .unwrap_or("/")
            .trim();
        let base = Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
        let mem_max = read_u64_file(&base.join("memory.max"))?;
        let mem_current = read_u64_file(&base.join("memory.current"))?;
        Some(Self {
            mem_max,
            mem_current,
        })
    }

    /// How many more builds the cgroup's limit can hold.
    ///
    /// What is left of the limit rather than what is left of its *unreclaimable*
    /// part: the page cache charged to the cgroup is reclaimable, but reclaiming
    /// it is IO, and a bound that spends that room is admitting builds into a
    /// budget that will be paid for by the disk the io dimension is already
    /// watching.
    fn ceiling(&self) -> f64 {
        self.mem_max.saturating_sub(self.mem_current) as f64 / BUILD_PEAK_RSS_BYTES as f64
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

/// The whole series set, in one place: a reader that has to tell "no bound here"
/// from "this deployment never reported" cannot do it if the two sources of the
/// set can drift.
fn readings(
    slots: usize,
    ceiling: usize,
    dimension: &str,
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
        label(RawMetric::new(BUILD_GATE_CEILING, ceiling as f64))
            .with_label("dimension", dimension.to_string()),
        label(RawMetric::new(BUILD_GATE_IN_FLIGHT, c.in_flight as f64)),
        label(RawMetric::new(BUILD_GATE_EXCLUDED, c.excluded as f64)),
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

/// Says that this process's work is waiting for the next build.
///
/// The free-function half of [`BuildGate::reserve_build_field`], for a caller
/// that reaches the gate through [`global`] rather than by holding it.
pub fn reserve_build_field(what: &str) {
    if let Some(gate) = global() {
        gate.reserve_build_field(what);
    }
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
            refused: AtomicU64::new(0),
            wait_ms_total: AtomicU64::new(0),
            wait_ms_max: AtomicU64::new(0),
            held_ms_total: AtomicU64::new(0),
            held_ms_max: AtomicU64::new(0),
        }
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
        }
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
        self.measured_ceiling.store(ceiling as u64, Ordering::Relaxed);
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
        }
    }

    /// Says that this work is waiting for the next build, so builds that arrive
    /// before it stand down.
    ///
    /// Called by a work that has just been refused and comes back on its own: a
    /// one-shot build waits in [`BuildGate::acquire`] instead and does not need
    /// this. It is a request rather than a queue — two works waiting overwrite
    /// each other's claim and the later one wins — because what it has to
    /// accomplish is only that the field stops being taken by arrivals, and
    /// after that the waiters are again taken in the order they ask, each having
    /// a claim of its own.
    pub fn reserve_build_field(&self, what: &str) {
        if !self.installed {
            return;
        }
        let since = unix_now();
        let path = self.dir.join(RESERVE_FILE);
        let previous = self.reservation().map(|(holder, _)| holder);
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
    fn release_reservation(&self, what: &str) {
        // Someone else's claim is left alone: they are waiting too, and this ask
        // being answered is not their turn.
        if let Some((holder, _)) = self.reservation() {
            if holder == what {
                self.clear_reservation();
            }
        }
    }

    /// Releases the claim on the next build, whoever holds it.
    ///
    /// For the exclusion that has just taken the field: the claim was a way
    /// through to the field, and it has been answered. Left behind, it would keep
    /// builds out of a field that is free again the moment the exclusion ends.
    /// A claim that is not refreshed comes back by itself at the next walk of
    /// whoever wanted it.
    fn clear_reservation(&self) {
        if !self.installed {
            return;
        }
        if self.reservation().is_none() {
            return;
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
                // The field is this caller's now, so the claim on the next build
                // has been answered: leaving it would hold the builds that come
                // after this pass out of a field that is free again.
                self.clear_reservation();
                self.excluded.fetch_add(1, Ordering::Relaxed);
                Ok(self.permit(what, vec![file], HoldKind::Excluding))
            }
            None => {
                let held_by = name_holder(&self.dir, CLAIM_FILE);
                let reason = format!("a build is running, so builds cannot be excluded ({held_by})");
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

    /// A permit with the accounting of one kind of hold.
    fn permit(
        self: &Arc<Self>,
        what: &str,
        held: Vec<std::fs::File>,
        kind: HoldKind,
    ) -> BuildPermit {
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
            let mut blocked_by: Option<String> = None;
            for slot in 0..bound {
                let Some(file) = lock_slot(&self.dir, slot) else {
                    continue;
                };
                // The claim is what says a build is *running* as opposed to
                // having taken a slot: an exclusion holds it alone, and while it
                // does, no build starts. Given back at once when it is held, so
                // that a build never sits on a slot while a sweep finishes.
                let Some(claim) = lock_claim_shared(&self.dir) else {
                    drop(file);
                    blocked_by = Some(format!(
                        "a build-cache sweep or a registry restart is holding the field ({})",
                        name_holder(&self.dir, CLAIM_FILE)
                    ));
                    break;
                };
                // A work that was turned away and said it is still waiting gets
                // the next build, and this arrival is not it.
                if let Some((claimant, _)) = self.reservation() {
                    if claimant != what {
                        drop(claim);
                        drop(file);
                        blocked_by = Some(format!(
                            "\"{claimant}\" was turned away and has claimed the next build"
                        ));
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
                let total = self.refused.fetch_add(1, Ordering::Relaxed) + 1;
                let refusal = if budget.is_zero() {
                    Refusal::Busy
                } else {
                    Refusal::Waited
                };
                // Who is holding, read from the holders themselves. A refusal's
                // own duration is not a reading -- it is fixed by whether the
                // caller waited -- but the holder's is: it is the length of one
                // piece of work, and it is what tells a caller that came back
                // once too often from one that is queueing behind a single
                // unbroken hold. When the slots are not what stopped the ask, the
                // slots' holders are not who stopped it either, so the reason
                // carries what did instead.
                let held = match blocked_by.as_deref() {
                    Some(why) => why.to_string(),
                    None => holder_report(&self.dir, bound),
                };
                let reason = match blocked_by.as_deref() {
                    Some(why) => format!("the gate is not open to this build: {why}"),
                    None => format!("{} ({held})", refusal.reason(bound, budget)),
                };
                tracing::warn!(
                    what,
                    refusal = refusal.as_str(),
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
    /// The locked files, when slots were actually taken. Closing them -- which
    /// drop does -- is what releases the locks. Empty for one of two reasons,
    /// told apart by `taken_at`: the gate is not in force, or the permit is an
    /// exclusion over a gate that bounds nothing.
    held: Vec<std::fs::File>,
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
/// carried on the refusal rather than published as a series of its own with no
/// rule to read it.
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
    // SAFETY: as in `lock_slot` — flock touches only this process's descriptor
    // table for a descriptor this scope owns, and EWOULDBLOCK is the expected
    // answer.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        // Nobody holds it, so the line below is a leftover.
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        return None;
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
    file_lock(&dir.join(format!("slot-{slot}.lock")), libc::LOCK_EX | libc::LOCK_NB)
}

/// Without `flock` there is no cross-process bound to enforce, so the gate
/// refuses to pretend it has one.
#[cfg(not(unix))]
fn lock_slot(_dir: &Path, _slot: usize) -> Option<std::fs::File> {
    None
}

/// Takes the claim a build holds while it runs, shared with the other builds.
#[cfg(unix)]
fn lock_claim_shared(dir: &Path) -> Option<std::fs::File> {
    file_lock(&dir.join(CLAIM_FILE), libc::LOCK_SH | libc::LOCK_NB)
}

#[cfg(not(unix))]
fn lock_claim_shared(_dir: &Path) -> Option<std::fs::File> {
    None
}

/// Takes the claim alone: while this is held no build can start.
#[cfg(unix)]
fn lock_claim_exclusive(dir: &Path) -> Option<std::fs::File> {
    file_lock(&dir.join(CLAIM_FILE), libc::LOCK_EX | libc::LOCK_NB)
}

#[cfg(not(unix))]
fn lock_claim_exclusive(_dir: &Path) -> Option<std::fs::File> {
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
/// first build to reach one makes it. Lowering the bound never removes a file —
/// a slot file's meaning is its lock, so unlinking one another process holds
/// would hand the next arrival a second lock on the same slot.
#[cfg(unix)]
fn file_lock(path: &Path, how: libc::c_int) -> Option<std::fs::File> {
    let file = open_gate_file(path, true)?;
    use std::os::unix::io::AsRawFd;
    // SAFETY: flock only touches the descriptor table of this process for a
    // descriptor this scope owns; EWOULDBLOCK is the expected "someone else has
    // it" answer and carries no memory obligations.
    let rc = unsafe { libc::flock(file.as_raw_fd(), how) };
    if rc == 0 {
        Some(file)
    } else {
        None
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
        Ok(readings(
            slots,
            ceiling,
            dimension,
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
            reading(&gate, BUILD_GATE_REFUSED_TOTAL).await,
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
        let load = |mem_max: u64, mem_current: u64| HostLoad {
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
            }),
        };
        // One build per core the host could run, and then some: a limit this
        // roomy is above the cpu dimension's own answer, so the container is not
        // what is deciding and the reading has to say `cpu`.
        let cpu_bound = (24.0 / BUILD_DEMAND_CORES).floor() as usize;
        let roomy = (cpu_bound as u64 + 1) * BUILD_PEAK_RSS_BYTES;

        assert_eq!(
            load(roomy, 0).ceiling(),
            (cpu_bound, DIM_CPU),
            "a limit with room for one build per core and then some leaves the host deciding"
        );
        assert_eq!(
            load(8 * gib, 4 * gib).ceiling(),
            (1, DIM_CGROUP_MEMORY),
            "8 GiB with half of it already charged leaves room for one build of {BUILD_PEAK_RSS_BYTES} bytes, not two"
        );
        assert_eq!(
            load(20 * gib, 8 * gib).ceiling(),
            (2, DIM_CGROUP_MEMORY),
            "and the count rises with the limit, which is what makes it a reading rather than a setting"
        );
        assert_eq!(
            load(1 * gib, 1 * gib).ceiling(),
            (1, DIM_CGROUP_MEMORY),
            "a cgroup with nothing left is still one build: a bound of zero refuses every build forever"
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

        let _permit = gate.try_acquire("a build on a machine that can be read").await;
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
            gate.try_acquire_exclusive("a build-cache sweep").await.is_err(),
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
        assert!(sweep.is_ok(), "the claim is answered by the work that made it");
        assert!(
            gate.reservation().is_none(),
            "and the exclusion taking the field releases it"
        );
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
        let sweep = gate.try_acquire_exclusive("a build-cache sweep").await.unwrap();

        assert!(
            gate.try_acquire_exclusive("a registry restart").await.is_err(),
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
    ///
    /// Both files a permit holds are checked, because they are held for different
    /// readers: the slot is what the next build waits for, and the claim is what a
    /// cache sweep waits for, so a copy of either keeps *its* readers out.
    #[tokio::test]
    async fn a_slot_comes_back_when_the_last_copy_of_the_lock_file_goes() {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        fn duplicate(file: &std::fs::File) -> OwnedFd {
            // SAFETY: `file` is open for as long as the caller keeps it, so its
            // descriptor is valid, and the returned descriptor is this test's to
            // close exactly once.
            let fd = unsafe { libc::dup(file.as_raw_fd()) };
            assert!(fd >= 0, "dup failed");
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
            gate.try_acquire("the next build").await.is_err(),
            "a second handle on the slot file keeps the slot taken"
        );
        drop(slot_copy);
        let next = gate.try_acquire("the next build").await.unwrap();
        assert!(
            gate.try_acquire_exclusive("sweeping the build cache")
                .await
                .is_err(),
            "a second handle on the claim file keeps the field taken from a sweep"
        );
        drop(claim_copy);
        drop(next);
        assert!(
            gate.try_acquire_exclusive("sweeping the build cache")
                .await
                .is_ok(),
            "the field comes back with the last copy of the claim"
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
                BUILD_GATE_CEILING,
                BUILD_GATE_IN_FLIGHT,
                BUILD_GATE_EXCLUDED,
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
