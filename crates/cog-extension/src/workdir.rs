//! Per-task worktree routing for the standalone sandbox executor.
//!
//! Each DAG task id maps to a fixed-path git worktree cut lazily from the
//! executor's own bare repository: agents of the same task collaborate in one
//! tree while different tasks can never trample each other's files, and the
//! self-invented shared `/tmp/cogneva-repo` path disappears structurally.
//!
//! Lifecycle uses only evidence, no task-end signals: a sidecar next to each
//! tree records `last_used_unix`; a periodic GC reaps trees idle past the TTL
//! and enforces an LRU cap. Trees and the bare repo live on a PVC, so a
//! process restart reconciles state from `git worktree list` plus the sidecar
//! directory instead of trusting an in-memory index.
//!
//! One fact the volume cannot hold, and so the one thing that lives in this
//! process: a command running in a task's tree claims it ([`WorktreeUse`]) for
//! as long as the child may live. Ranking by `last_used_unix` alone cannot tell
//! a tree with a command running in it from an idle one — the sidecar stops
//! advancing the moment the command starts — so the LRU cap could delete the
//! tree out from under a build that then fails on files disappearing and gets
//! the blame for it. The claim is deliberately process-local: the child dies
//! with this process, so a restart cannot leave a stale claim behind, and
//! everything a restart must reconcile is still on the volume.
//!
//! The bare-repo + worktree + out-of-tree sidecar + external target dir +
//! stale-lock self-heal pattern is deliberately copied from the evolution
//! WorkspaceManager: cog-extension must not depend on upper crates, and only
//! about a hundred lines of that semantics apply here.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cog_core::claim_footprint::{ClaimFootprint, ClaimMount};
use cog_core::{SFError, SFResult};
use prometheus::{Counter, CounterVec, Encoder, Gauge, GaugeVec, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

use cog_core::build_cache_readings::{BuildCacheReadings, CacheSlot};

use crate::build_cache::{BuildCacheConfig, CommandSlot, SANDBOX_BUILD_CACHE_LOOP};

/// Loop name reported through the background-loop liveness family.
pub const WORKTREE_GC_LOOP: &str = "extension_worktree_gc";

/// Loop name of the offline mirror refresh.
pub const WORKTREE_FETCH_LOOP: &str = "extension_worktree_fetch";

/// Loop name of the claim-footprint walk over this executor's own volumes.
pub const VOLUME_FOOTPRINT_LOOP: &str = "extension_volume_footprint";

// cap: bound — seed; applies only until two trees exist to give an arrival interval
const DEFAULT_TTL_SECS: u64 = 21600;
// cap: bound — seed; applies only until a reclamation pass has been timed, then the interval follows the pass
const DEFAULT_GC_INTERVAL_SECS: u64 = 600;
// cap: bound — seed; applies only until the first observed move of main, then the fetch period follows that
const DEFAULT_FETCH_INTERVAL_SECS: u64 = 300;
// cap: bound — policy ceiling when the deployment names none; the disk measurement can still lower it
const DEFAULT_MAX_WORKSPACES: usize = 8;

/// Trees kept cut and ready under `workspaces/.warm`, so a new task's first
/// command does not pay for a checkout.
///
/// Measured in this pod on 2026-11-07: `git worktree add --detach main` took
/// 3403 ms (measured) for a 49 MB tree, and that cost lands on the first tool
/// call of every new conversation -- a `printf` that took 6.7 s end to end
/// (measured) spent 6.5 s of it waiting for its tree. Claude Code pays nothing
/// there because its tools run in a directory that already exists; this is the
/// platform's equivalent of that, and the number is small because a warm tree
/// is only worth holding
/// until the next arrival: arrivals measured ~1.1 a minute on this host, and
/// the refill runs off the request path.
// cap: bound — seed; a deployment that names none gets two, the pool never holds more than half the task ceiling
const DEFAULT_WARM_TREES: usize = 2;

/// Where warm trees wait between arrivals. Dot-prefixed because every listing
/// of task trees in this file filters on a non-dot name, and a pool tree that
/// started counting against the task ceiling would evict the task it is meant
/// to spare.
const WARM_DIR_NAME: &str = ".warm";

const META_DIR_NAME: &str = ".meta";

/// The entry a bare repository writes into every worktree it owns, and the one
/// thing that tells a real tree apart from a directory that merely has the
/// right name. Named once because two readers ask the same question -- the
/// pool deciding whether a slot can be handed out, and the health check
/// deciding whether a task's tree is still the one git registered.
const WORKTREE_LINK: &str = ".git";

/// Maximum task id length, keeps the on-disk directory name clear of
/// `ENAMETOOLONG` while accommodating generated ids such as
/// `<owner-task>-ralph-roundtable-i12-<8 hex>`.
// cap: bound — 目录名要躲开 ENAMETOOLONG，又要装得下生成的 id
const MAX_TASK_ID_LEN: usize = 128;

/// The shortest time between two reclamation passes, and the pace the loop
/// wakes at whatever the derived interval turns out to be -- waking is cheap,
/// a pass is not.
// cap: bound — 两次回收之间的最短间隔，也是循环无条件醒来的节奏
const MIN_GC_INTERVAL_SECS: u64 = 30;

/// How many times a pass's own duration the interval must cover, so maintenance
/// costs at most one this-many-th of the process's time. A ratio rather than a
/// period: the pass gets slower as the volume fills, and the interval has to
/// follow it or the upkeep becomes the load. Shared by reclamation and the
/// build-cache walk -- one control law, one number.
// cap: bound — 间隔要覆盖一次回收自身耗时的这么多倍，维护至多占 1/20
const MAINTENANCE_OVERHEAD_MULTIPLE: u64 = 20;

/// The published name of the interval the reclamation loop is keeping.
const GC_INTERVAL_METRIC: &str = "sandbox_workspace_gc_interval_seconds";

/// The shortest wait between two upstream fetches. Below this the loop would be
/// asking the mirror for the same commit faster than a push can arrive, and the
/// cost of the git round trip stops being paid for by anything. A floor rather
/// than a period: the period itself follows how often `main` is observed to
/// move.
// cap: bound — 比这更密就是在向镜像要同一个 commit，比一次推送还快
const MIN_FETCH_INTERVAL_SECS: u64 = 30;

/// The published name of the wait the fetch loop is currently keeping, and the
/// label saying whether that number came from a measurement or from the
/// configuration. Without it the configured number is the only one anyone can
/// see, and the loop silently stopping to use it would be invisible.
const FETCH_INTERVAL_METRIC: &str = "sandbox_repo_fetch_interval_seconds";

/// The label saying which input set a published period -- configuration, floor
/// or measurement. Without it the configured number is the only one anyone can
/// see, and the loop silently stopping to use it would be invisible.
const SOURCE_LABEL: &str = "source";

/// Every value [`SOURCE_LABEL`] may take. Published together, exactly one of
/// them at 1; see [`PeriodGauge`] for why the state cannot be a label on the
/// number itself.
const SOURCES: [&str; 3] = ["configured", "floor", "measured"];

/// The published name of the idle TTL the reclamation pass is using.
const TTL_METRIC: &str = "sandbox_workspace_ttl_seconds";

/// The published name of the wait the volume walk is currently keeping.
const VOLUME_WALK_INTERVAL_METRIC: &str = "sandbox_volume_walk_interval_seconds";

/// How long the fetch loop should wait, given how long `main` was still before
/// it last moved.
///
/// Half the observed stillness, so the worktrees are at most half a push
/// behind: an upstream that moves every ten minutes is polled every five, and
/// one that moves hourly is polled every half hour instead of every five
/// minutes for nothing. `observed` is zero until a move has been seen -- then
/// there is nothing to follow and the configured period stands, which is the
/// only role that number keeps.
fn fetch_interval_from(observed_stillness_secs: u64, configured: Duration) -> Duration {
    if observed_stillness_secs == 0 {
        return configured;
    }
    Duration::from_secs((observed_stillness_secs / 2).max(MIN_FETCH_INTERVAL_SECS))
}

/// What the filesystem under the workspaces root reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiskReading {
    total_bytes: u64,
    avail_bytes: u64,
}

/// Read `df -PB1 <dir>`: the header, then one row per filesystem in blocks of
/// one byte, so the size and available columns are already bytes.
fn parse_df(stdout: &str) -> Option<DiskReading> {
    let line = stdout.lines().nth(1)?;
    let cols: Vec<&str> = line.split_whitespace().collect();
    Some(DiskReading {
        total_bytes: cols.get(1)?.parse().ok()?,
        avail_bytes: cols.get(3)?.parse().ok()?,
    })
}

/// How many task trees the volume can hold: the ones already there, plus the
/// ones its remaining room buys at the size a tree actually measures.
///
/// Both inputs are readings, which is the point. A repository that grows makes
/// every tree larger, so the cap falls on the next pass; so does anything else
/// filling the volume. `None` when no tree has been measured yet: there is no
/// per-tree figure to divide by, and a cap invented without one would be the
/// written number wearing a measurement's clothes.
fn workspace_cap(disk: DiskReading, trees_bytes: u64, trees: usize) -> Option<usize> {
    if trees == 0 || trees_bytes == 0 {
        return None;
    }
    let per_tree = trees_bytes / trees as u64;
    if per_tree == 0 {
        return None;
    }
    // The cap is reached by cutting trees, so what has to stay free is the room
    // to cut and write one more: a tree's size as measured here. A fraction of
    // the volume is a guess at that same size, and guesses it both ways -- a
    // fifteenth of a large volume is many trees' worth left unused, and a
    // fifteenth of a nearly full one can be less than a single tree, which is
    // the failure the reserve exists to prevent: git fails to write while the
    // cap is being reached, and that failure is blamed on whatever ran there.
    let room = disk.avail_bytes.saturating_sub(per_tree);
    Some(trees + (room / per_tree) as usize)
}

/// How long an idle tree is worth holding, from the two readings that decide
/// it: how many trees the volume holds and how fast new ones arrive.
///
/// The reuse history cannot set this. A tree idle longer than the TTL has
/// already been reaped, so every gap anyone can observe is shorter than the
/// number it would be feeding -- a rule built on them doubles the TTL over and
/// over and never settles, all while looking like a closed loop. What does not
/// depend on the TTL is the arrival rate and the capacity: holding a tree for
/// as long as it takes new trees to refill half the volume is the point past
/// which holding costs more than rebuilding.
fn ttl_from(capacity: usize, trees: usize, span_secs: u64) -> Option<Duration> {
    if capacity == 0 || trees < 2 || span_secs == 0 {
        return None;
    }
    let between_arrivals = span_secs / trees as u64;
    if between_arrivals == 0 {
        return None;
    }
    let held = (capacity as u64).saturating_mul(between_arrivals) / 2;
    Some(Duration::from_secs(held.max(MIN_TTL_SECS)))
}

/// The shortest an idle tree is held, whatever the arithmetic says: below this
/// the executor rebuilds trees it has just finished with.
// cap: bound — 空闲树最少留一小时，再短执行器就在重建刚用完的树
const MIN_TTL_SECS: u64 = 3600;

/// Which of the three inputs set the period in force.
///
/// A rule reading the published interval has to be able to tell "the loop is
/// keeping the number the chart wrote" from "the loop is running at a floor it
/// may not go below" from "the loop slowed to a share of what it costs" -- the
/// three say different things about the machine, and one `measured` value would
/// let the first two read the same.
fn period_source(pass_millis: u64, derived: Duration, floor: Duration) -> &'static str {
    if pass_millis == 0 {
        "configured"
    } else if derived < floor {
        "floor"
    } else {
        "measured"
    }
}

/// A period this process keeps, published as the number in force plus a one-hot
/// family naming the input set that chose it.
///
/// The state cannot be a label on the number itself: a Prometheus client keeps
/// every label set it has been handed for the life of the process, so the first
/// pass writing `source="configured"` and the next writing `source="measured"`
/// leaves both published forever -- two series, each claiming to be the period
/// in force, and a rule selecting either one reads a wait the loop stopped
/// keeping. Measured on the cluster: `sandbox_volume_walk_interval_seconds`
/// carried `source="configured"` and `source="floor"` at 300 side by side, with
/// nothing saying which one was current. Publishing all three sources with
/// exactly one at 1 says the same thing and leaves nothing behind.
#[derive(Clone)]
struct PeriodGauge {
    seconds: Gauge,
    source: GaugeVec,
}

impl PeriodGauge {
    /// Register the number at the configured period, which is what is in force
    /// before the first timed pass -- a zero there would read as a loop about
    /// to spin, not as a period that has not been derived yet.
    fn new(
        registry: &Registry,
        name: &str,
        help: &str,
        configured: Duration,
    ) -> Result<Self, prometheus::Error> {
        let seconds = Gauge::new(name, help)?;
        // Named off the number it belongs to, so the pair cannot be registered
        // under names that drift apart.
        let source = GaugeVec::new(
            Opts::new(
                format!("{}_source", name.strip_suffix("_seconds").unwrap_or(name)),
                "Which input set chose the period in force: 1 for that one, 0 for the others",
            ),
            &[SOURCE_LABEL],
        )?;
        registry.register(Box::new(seconds.clone()))?;
        registry.register(Box::new(source.clone()))?;
        let published = Self { seconds, source };
        published.set(configured.as_secs_f64(), "configured");
        Ok(published)
    }

    /// Publish `seconds` as the period in force, chosen by `source`.
    fn set(&self, seconds: f64, source: &str) {
        self.seconds.set(seconds);
        for candidate in SOURCES {
            self.source
                .with_label_values(&[candidate])
                .set(if candidate == source { 1.0 } else { 0.0 });
        }
    }
}

/// How often to measure the volumes this executor writes.
///
/// The configured interval is the floor here rather than a seed: walking a
/// claim costs I/O on the same disk the tasks are building on, so a walk is
/// never worth running more often than the chart said, and is worth running
/// less often once the tree it walks takes a real share of that interval.
fn volume_walk_interval_from(pass_millis: u64, configured: Duration) -> Duration {
    paced_interval(
        pass_millis,
        configured,
        configured.as_secs(),
        MAINTENANCE_OVERHEAD_MULTIPLE,
    )
}

/// How often to reclaim, from how long a pass costs.
///
/// The loop's own work is the thing that must stay cheap, so the interval is a
/// multiple of the measured pass duration rather than a period chosen once. A
/// pass that took half a second with a 600s interval was 0.08% of the process;
/// one that takes 40s on a full volume would be 6.7% of it, and the fixed
/// interval would keep paying that. Before any pass has been timed the
/// configured interval stands: there is nothing to scale from.
fn gc_interval_from(pass_millis: u64, configured: Duration) -> Duration {
    paced_interval(
        pass_millis,
        configured,
        MIN_GC_INTERVAL_SECS,
        MAINTENANCE_OVERHEAD_MULTIPLE,
    )
}

/// How often to run something whose cost grows with what it walks, from how
/// long the last run took.
///
/// Both loops here -- reclaiming the task worktrees and re-walking the build
/// cache -- have the same control law: their own work may take one
/// this-many-th of the process, so the period is a multiple of the work, never
/// below a floor. A fixed period silently spends a growing share as the volume
/// fills, which is the failure that looks like "the executor got slow" rather
/// than "the executor is reclaiming".
fn paced_interval(
    pass_millis: u64,
    configured: Duration,
    floor_secs: u64,
    multiple: u64,
) -> Duration {
    if pass_millis == 0 {
        return configured;
    }
    scaled(pass_millis, multiple).max(Duration::from_secs(floor_secs))
}

/// The period a pass of `pass_millis` buys at a given share, before any floor.
///
/// Split out because the published reading has to say which input set the
/// number in force, and that answer is "the measurement" only when this figure
/// is the one above the floor.
fn scaled(pass_millis: u64, multiple: u64) -> Duration {
    Duration::from_secs((pass_millis / 1000).saturating_mul(multiple))
}

#[derive(Debug, Clone)]
pub struct WorkdirConfig {
    pub workspaces_root: PathBuf,
    pub bare_repo: PathBuf,
    pub target_dir: PathBuf,
    pub seed_urls: Vec<String>,
    pub ttl: Duration,
    pub gc_interval: Duration,
    pub fetch_interval: Duration,
    pub max_workspaces: usize,
    /// How many trees to keep cut and ready under [`WARM_DIR_NAME`]. Never
    /// more than half the task ceiling: the pool exists to make arrivals
    /// cheap, and spending the ceiling on trees no task has asked for would
    /// make the tasks themselves wait.
    pub warm_trees: usize,
    /// Claim-backed volumes this process measures the footprint of. Which claim
    /// backs which directory is a fact only the deployment knows, so it comes
    /// from there; what lies below a mount is not declared, because this process
    /// reads its own mount table for that.
    pub data_volumes: Vec<ClaimMount>,
    /// Why the declared volume list could not be read, when it could not. Carried
    /// rather than logged here so the router can report it on the surface that
    /// outlives a log line.
    pub data_volume_declaration_error: Option<String>,
    /// How often each declared volume is re-walked.
    pub data_volume_interval: Duration,
    /// What bounds the build cache the commands of this executor write into.
    pub build_cache: BuildCacheConfig,
}

impl WorkdirConfig {
    pub fn from_env() -> Self {
        let path = |key: &str, default: PathBuf| {
            std::env::var(key)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or(default)
        };
        let secs = |key: &str, default: u64, min: u64| {
            let v = std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(default)
                .max(min);
            Duration::from_secs(v)
        };
        let max = std::env::var("SANDBOX_MAX_TASK_WORKSPACES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_WORKSPACES)
            .max(1);
        let warm = std::env::var("SANDBOX_WARM_TREES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_WARM_TREES)
            .min(max / 2);
        let (data_volumes, data_volume_declaration_error) =
            match std::env::var(cog_core::claim_footprint::MOUNTS_ENV) {
                Ok(raw) => match cog_core::claim_footprint::parse_claim_paths(&raw) {
                    Ok(mounts) => (mounts, None),
                    Err(e) => (Vec::new(), Some(e)),
                },
                Err(_) => (Vec::new(), None),
            };
        // The other variable of that pair names the claim behind the application
        // data directory, which this process does not have: its directories are
        // the executor's own. A deployment that sets it here gets no reading at
        // all, and a switch that reads as set while measuring nothing is the
        // failure this whole surface exists to end -- so it is refused out loud
        // rather than ignored.
        if std::env::var(cog_core::claim_footprint::CLAIM_ENV)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
        {
            error!(
                "{} is set on the sandbox executor, which has no application data \
                 directory; declare this pod's volumes through {} instead",
                cog_core::claim_footprint::CLAIM_ENV,
                cog_core::claim_footprint::MOUNTS_ENV
            );
        }
        Self {
            workspaces_root: path(
                "SANDBOX_WORKSPACES_ROOT",
                cog_core::paths::app_path("sandbox/workspaces"),
            ),
            bare_repo: path(
                "SANDBOX_BARE_REPO",
                cog_core::paths::app_path("sandbox/repo.git"),
            ),
            target_dir: path(
                "CARGO_TARGET_DIR",
                cog_core::paths::app_path("sandbox/src/target"),
            ),
            // Ordered mirrors, not one URL: reachability is per host and
            // uneven — measured from this pod, one mirror answered in under a
            // second while the other stalled to a 20s timeout — so a single
            // upstream leaves the worktrees on stale refs for as long as that
            // host is unreachable. A leftover `__PLACEHOLDER__` from an
            // unrendered static manifest is never a usable URL.
            seed_urls: std::env::var("SANDBOX_REPO_SEED_URLS")
                .ok()
                .map(|v| split_seed_urls(&v))
                .unwrap_or_default(),

            ttl: secs("SANDBOX_WORKSPACE_TTL_SECS", DEFAULT_TTL_SECS, 1),
            gc_interval: secs(
                "SANDBOX_WORKSPACE_GC_INTERVAL_SECS",
                DEFAULT_GC_INTERVAL_SECS,
                10,
            ),
            fetch_interval: secs(
                "SANDBOX_REPO_FETCH_INTERVAL_SECS",
                DEFAULT_FETCH_INTERVAL_SECS,
                30,
            ),
            max_workspaces: max,
            warm_trees: warm,
            data_volumes,
            data_volume_declaration_error,
            data_volume_interval: secs(
                cog_core::claim_footprint::INTERVAL_ENV,
                cog_core::claim_footprint::DEFAULT_SCAN_INTERVAL_SECS,
                cog_core::claim_footprint::MIN_SCAN_INTERVAL_SECS,
            ),
            build_cache: BuildCacheConfig::from_env(),
        }
    }
}

/// A URL is usable when it is non-empty and does not contain an unresolved
/// `__PLACEHOLDER__` marker.
fn is_usable_url(url: &str) -> bool {
    let u = url.trim();
    !u.is_empty() && !u.contains("__")
}

/// Split a comma-separated mirror list into usable URLs, preserving order:
/// the first entry is the preferred upstream and the rest are fallbacks.
fn split_seed_urls(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|u| is_usable_url(u))
        .map(str::to_string)
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TaskMeta {
    task_id: String,
    created_at_unix: i64,
    last_used_unix: i64,
}

#[derive(Clone)]
pub struct WorkdirMetrics {
    registry: Registry,
    workspaces: Gauge,
    gc_reclaimed: Counter,
    /// Cap passes that could not be met without deleting a tree a command is
    /// running in. A pass that defers is otherwise invisible: the cap holding
    /// and the cap being unreachable both read as "nothing happened", and only
    /// this series says which trees are keeping the count above the cap.
    cap_deferred: Counter,
    fetch_failures: Counter,
    unscoped_requests: Counter,
    errors: CounterVec,
    disk_total_bytes: Gauge,
    disk_avail_bytes: Gauge,
    /// Bytes held by each claim-backed volume of this pod. The same series a
    /// deployment-wide reading publishes: one volume has one footprint, and a
    /// second name for it would leave the rule that compares it against the
    /// declared size reading only the half that happens to be spelled its way.
    data_volume_used: GaugeVec,
    /// The fetch wait in force, and the input set that chose it. A derived
    /// number that is not published is indistinguishable from a configured
    /// one, and the loop quietly going back to the configured value would
    /// read as if nothing had changed.
    fetch_interval: PeriodGauge,
    /// The idle TTL in force, published the same way.
    ttl_seconds: PeriodGauge,
    /// The reclamation interval in force, published the same way.
    gc_interval_seconds: PeriodGauge,
    /// The volume walk's interval in force, published the same way.
    volume_walk_interval_seconds: PeriodGauge,
}

impl WorkdirMetrics {
    /// Built from the configuration because the periods in force are its
    /// numbers until a pass has been timed.
    fn new(cfg: &WorkdirConfig) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let workspaces = Gauge::new(
            "sandbox_workspaces",
            "Current number of per-task git worktrees on the executor volume",
        )?;
        let gc_reclaimed = Counter::new(
            "sandbox_workspace_gc_reclaimed_total",
            "Task worktrees removed by TTL/LRU garbage collection",
        )?;
        let cap_deferred = Counter::new(
            "sandbox_workspace_cap_deferred_total",
            "Task worktrees kept above the LRU cap because a command was running in them",
        )?;
        let fetch_failures = Counter::new(
            "sandbox_workspace_fetch_failures_total",
            "Failures of the background bare-repo upstream fetch",
        )?;
        let unscoped_requests = Counter::new(
            "sandbox_workspace_unscoped_requests_total",
            "Executor requests that carried no task id and ran outside a task worktree",
        )?;
        let errors = CounterVec::new(
            Opts::new(
                "sandbox_workspace_errors_total",
                "Task worktree routing errors by kind",
            ),
            &["kind"],
        )?;
        let disk_total_bytes = Gauge::new(
            "sandbox_workspace_disk_total_bytes",
            "Total bytes on the filesystem backing the executor workspaces",
        )?;
        let disk_avail_bytes = Gauge::new(
            "sandbox_workspace_disk_avail_bytes",
            "Available bytes on the filesystem backing the executor workspaces",
        )?;
        // Registered childless, and children are only ever created from a
        // successful walk: a series that exists before anything was measured
        // would read as a volume holding zero bytes, which is a claim about the
        // volume rather than an absence of evidence.
        let data_volume_used = GaugeVec::new(
            Opts::new(
                cog_core::claim_footprint::USED_METRIC,
                "Measured bytes held by the claim-backed volumes of this workload",
            ),
            &[cog_core::claim_footprint::CLAIM_LABEL],
        )?;
        registry.register(Box::new(workspaces.clone()))?;
        registry.register(Box::new(gc_reclaimed.clone()))?;
        registry.register(Box::new(cap_deferred.clone()))?;
        registry.register(Box::new(fetch_failures.clone()))?;
        registry.register(Box::new(unscoped_requests.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(disk_total_bytes.clone()))?;
        registry.register(Box::new(disk_avail_bytes.clone()))?;
        registry.register(Box::new(data_volume_used.clone()))?;
        let fetch_interval = PeriodGauge::new(
            &registry,
            FETCH_INTERVAL_METRIC,
            "Seconds the background fetch loop is currently waiting between upstream fetches",
            cfg.fetch_interval,
        )?;
        let ttl_seconds = PeriodGauge::new(
            &registry,
            TTL_METRIC,
            "Seconds an idle task worktree is held before reclamation",
            cfg.ttl,
        )?;
        let gc_interval_seconds = PeriodGauge::new(
            &registry,
            GC_INTERVAL_METRIC,
            "Seconds between reclamation passes of the task worktrees",
            cfg.gc_interval,
        )?;
        let volume_walk_interval_seconds = PeriodGauge::new(
            &registry,
            VOLUME_WALK_INTERVAL_METRIC,
            "Seconds between walks of the volumes this executor writes",
            cfg.data_volume_interval,
        )?;
        Ok(Self {
            registry,
            workspaces,
            gc_reclaimed,
            cap_deferred,
            fetch_failures,
            unscoped_requests,
            errors,
            disk_total_bytes,
            disk_avail_bytes,
            data_volume_used,
            fetch_interval,
            ttl_seconds,
            gc_interval_seconds,
            volume_walk_interval_seconds,
        })
    }

    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .is_ok()
        {
            String::from_utf8_lossy(&buf).into_owned()
        } else {
            String::new()
        }
    }

    pub(crate) fn inc_error(&self, kind: &str) {
        self.errors.with_label_values(&[kind]).inc();
    }

    pub fn inc_unscoped(&self) {
        self.unscoped_requests.inc();
    }
}

/// A command's claim on a task worktree.
///
/// Held for as long as the child process may live, not for as long as the
/// caller's response stream: a command keeps running after its client
/// disconnects, so a claim that ended with the stream would reopen exactly the
/// window it exists to close. The executor hands the claim to the task that
/// drives the child.
///
/// Nothing about it is written down, and that is the point: the child dies with
/// this process, so a restart cannot leave a claim standing, while everything a
/// restart must reconcile — the tree, its sidecar — is still on the volume.
pub struct WorktreeUse {
    router: Arc<WorkdirRouter>,
    task_id: String,
}

impl Drop for WorktreeUse {
    fn drop(&mut self) {
        self.router.release(self.task_id.as_str());
        // The sidecar is what the next cap pass ranks this tree by, and the end
        // of a claim is the moment the tree was last used. Without this refresh
        // a build that just ran for ten minutes reads as the oldest tree on the
        // volume the moment it finishes — exactly when its artifacts are about
        // to be read — and the next pass deletes it.
        //
        // No runtime here means the process is shutting down; the sidecar then
        // keeps its older stamp, which is what the next process adopts.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let router = Arc::clone(&self.router);
            let task_id = self.task_id.clone();
            handle.spawn(async move { router.touch(&task_id).await });
        }
    }
}

/// What a command holds for as long as its child may live.
///
/// Two facts, one lifetime: the claim that keeps its worktree from being
/// reclaimed, and the place in this process's command slot that keeps the build
/// cache from being emptied underneath it. Both have to outlast the response
/// stream rather than end with it -- a client that disconnects mid-command
/// leaves the child running.
pub struct CommandHold {
    _claim: Option<WorktreeUse>,
    _slot: tokio::sync::OwnedRwLockReadGuard<()>,
}

/// A panic elsewhere while the claim map was held must not turn every later
/// reclamation into a panic of its own: the map holds counts of running
/// commands, not data that can be left half-written, so a poisoned lock is read
/// through rather than refused.
fn lock_claims(
    claims: &std::sync::Mutex<std::collections::HashMap<String, usize>>,
) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, usize>> {
    claims.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct WorkdirRouter {
    cfg: WorkdirConfig,
    meta_dir: PathBuf,
    /// Serializes tree creation, cap eviction and GC so two first-seen tasks
    /// never run `worktree add` / `worktree remove` against the same state.
    /// The hot path for an existing tree takes no lock.
    create_lock: Mutex<()>,
    /// Keeps the warm pool's own refill out of [`Self::create_lock`]. A refill
    /// is a checkout, and an arrival that had to queue behind one would be
    /// waiting for a tree meant for somebody who has not arrived yet -- the
    /// cost the pool exists to remove, reintroduced for the second caller.
    warm_lock: Mutex<()>,
    /// Task ids with a command running in their tree right now, and how many.
    /// Read by the reclamation rules, which must not delete a tree in use; see
    /// [`WorktreeUse`] for why this is process state rather than a sidecar.
    in_use: std::sync::Mutex<std::collections::HashMap<String, usize>>,
    metrics: WorkdirMetrics,
    /// One handle per declared claim-backed volume. Built here so the claim and
    /// the directory it is measured against are fixed together for the life of
    /// the process.
    footprints: Vec<Arc<ClaimFootprint>>,
    /// The build cache every command of this process writes into, measured and
    /// bounded on a timer. Here rather than in a module of its own because the
    /// fact that bounds it is this router's own: the commands it is running.
    build_cache: Arc<BuildCacheReadings>,
    /// What says whether a command is running: every command holds a read guard
    /// for as long as its child may live, and a reclaim pass takes the write
    /// guard. Held across a command's whole life on purpose -- a client that
    /// disconnects mid-command leaves it running.
    command_lock: Arc<RwLock<()>>,
    /// The upstream head this process last saw, and when it moved before that.
    /// The fetch period is a reading of how often the repository actually
    /// changes: polling a mirror every fixed number of seconds either watches
    /// a head that has not moved or trails one that moved several times.
    last_head: std::sync::Mutex<Option<String>>,
    last_move_unix: AtomicI64,
    stillness_secs: AtomicU64,
    last_fetch_unix: AtomicI64,
    /// The longest git command that has completed here, in milliseconds. What
    /// the timeout is a multiple of; see [`cog_core::git_budget`].
    git_slowest_ms: AtomicU64,
    /// The last reclamation pass's duration, in milliseconds, and when it
    /// started. What the interval is a multiple of.
    gc_pass_millis: AtomicU64,
    last_gc_unix: AtomicI64,
    /// The same two for the build-cache walk.
    cache_pass_millis: AtomicU64,
    last_cache_walk_unix: AtomicI64,
    /// The same two for the walk over the volumes this executor writes.
    volume_pass_millis: AtomicU64,
    last_volume_walk_unix: AtomicI64,
}

impl WorkdirRouter {
    pub fn new(cfg: WorkdirConfig) -> SFResult<Arc<Self>> {
        let meta_dir = cfg.workspaces_root.join(META_DIR_NAME);
        let metrics = WorkdirMetrics::new(&cfg)
            .map_err(|e| SFError::IO(format!("workdir metrics init: {e}")))?;
        if let Some(reason) = &cfg.data_volume_declaration_error {
            // Nothing below this point can measure those volumes, so the loss is
            // reported where it happens rather than left to be inferred from a
            // series that never appears.
            error!(
                error = %reason,
                variable = cog_core::claim_footprint::MOUNTS_ENV,
                "declared volume footprints cannot be read; no volume of this executor is measured"
            );
        }
        let footprints = cfg
            .data_volumes
            .iter()
            .map(|mount| {
                Arc::new(ClaimFootprint::for_mounted_volume(
                    mount.claim.clone(),
                    PathBuf::from(&mount.path),
                ))
            })
            .collect();
        let build_cache = Arc::new(cfg.build_cache.readings(cfg.target_dir.clone()));
        Ok(Arc::new(Self {
            cfg,
            meta_dir,
            create_lock: Mutex::new(()),
            warm_lock: Mutex::new(()),
            in_use: std::sync::Mutex::new(std::collections::HashMap::new()),
            metrics,
            footprints,
            build_cache,
            command_lock: Arc::new(RwLock::new(())),
            last_head: std::sync::Mutex::new(None),
            last_move_unix: AtomicI64::new(0),
            stillness_secs: AtomicU64::new(0),
            last_fetch_unix: AtomicI64::new(0),
            git_slowest_ms: AtomicU64::new(0),
            gc_pass_millis: AtomicU64::new(0),
            last_gc_unix: AtomicI64::new(0),
            cache_pass_millis: AtomicU64::new(0),
            last_cache_walk_unix: AtomicI64::new(0),
            volume_pass_millis: AtomicU64::new(0),
            last_volume_walk_unix: AtomicI64::new(0),
        }))
    }

    /// Build the router from `SANDBOX_*` env only when the seeded bare repo is
    /// present. An absent bare repo means the executor runs without a
    /// provisioned volume (local/embedded/test usage); callers then keep the
    /// legacy process-cwd behaviour instead of inventing a shared directory.
    pub async fn from_env() -> Option<Arc<Self>> {
        let cfg = WorkdirConfig::from_env();
        match tokio::fs::try_exists(cfg.bare_repo.join("HEAD")).await {
            Ok(true) => match Self::new(cfg) {
                Ok(router) => Some(router),
                Err(e) => {
                    warn!(error = %e, "per-task workdir router disabled");
                    None
                }
            },
            Ok(false) => {
                info!(bare = %cfg.bare_repo.display(),
                    "per-task workdir router disabled: bare repo not seeded");
                None
            }
            Err(e) => {
                warn!(error = %e, "per-task workdir router disabled");
                None
            }
        }
    }

    pub fn config(&self) -> &WorkdirConfig {
        &self.cfg
    }

    pub fn target_dir(&self) -> &Path {
        &self.cfg.target_dir
    }

    pub fn metrics(&self) -> &WorkdirMetrics {
        &self.metrics
    }

    /// The build cache this process's commands write into, as measured.
    pub fn build_cache(&self) -> &Arc<BuildCacheReadings> {
        &self.build_cache
    }

    /// The fact a build-cache reclamation pass needs: free only while no command
    /// of this process is running. See [`crate::build_cache`] for why this
    /// executor's own commands are what answers that question here.
    pub fn build_cache_slot(&self) -> Arc<dyn CacheSlot> {
        Arc::new(CommandSlot::new(Arc::clone(&self.command_lock)))
    }

    /// Register a command that is about to run, holding its claim on a task
    /// worktree (when it has one) for as long as its child may live.
    ///
    /// Awaited rather than tried: a command that arrives while a reclamation
    /// pass is deleting from the cache waits for the pass instead of building
    /// against a half-emptied one. The wait is bounded by the pass, and the pass
    /// is bounded by the size of the plan it is carrying out.
    pub async fn command_hold(&self, claim: Option<WorktreeUse>) -> CommandHold {
        CommandHold {
            _claim: claim,
            _slot: self.command_lock.clone().read_owned().await,
        }
    }

    /// Claim a tree for a command about to run in it. Counted, because two
    /// commands of one task may overlap and the tree stays in use until the last
    /// of them is gone.
    fn claim(self: &Arc<Self>, task_id: &str) -> WorktreeUse {
        {
            let mut claims = lock_claims(&self.in_use);
            *claims.entry(task_id.to_string()).or_insert(0) += 1;
        }
        WorktreeUse {
            router: Arc::clone(self),
            task_id: task_id.to_string(),
        }
    }

    fn release(&self, task_id: &str) {
        let mut claims = lock_claims(&self.in_use);
        if let Some(n) = claims.get_mut(task_id) {
            *n -= 1;
            if *n == 0 {
                claims.remove(task_id);
            }
        }
    }

    /// Commands running in this tree right now. `own_claim` excludes the claim
    /// the asking caller just took itself, which is not evidence of another
    /// command and must not make its own rebuild impossible.
    fn commands_running(&self, task_id: &str, own_claim: bool) -> usize {
        let claims = lock_claims(&self.in_use);
        let n = claims.get(task_id).copied().unwrap_or(0);
        if own_claim {
            n.saturating_sub(1)
        } else {
            n
        }
    }

    /// Resolve the working directory for a task id and claim it for the command
    /// about to run there, lazily creating the tree on first sight.
    ///
    /// The claim is taken before the first filesystem call: a reclamation pass
    /// that has already listed this tree must still see it in use when it
    /// reaches its own decision, and the only moment a claim can protect a tree
    /// is before anything starts using it. Invalid ids and add failures surface
    /// as errors instead of silently collapsing onto a shared directory.
    pub async fn route(self: &Arc<Self>, task_id: &str) -> SFResult<(PathBuf, WorktreeUse)> {
        validate_task_id(task_id)?;
        let use_guard = self.claim(task_id);
        let (path, took_warm) = self.route_validated(task_id, true).await?;
        // Refill after the answer, never before it: the caller is waiting for
        // a directory to run in, and the tree that replaces the one just taken
        // is for a task that has not arrived yet.
        if took_warm {
            let router = Arc::clone(self);
            tokio::spawn(async move { router.warm_pool_once().await });
        }
        Ok((path, use_guard))
    }

    /// The pool directory. Sits inside the workspaces root so it lands on the
    /// same volume as the trees it holds -- a pool on another filesystem is a
    /// pool whose hand-out is the copy it was meant to avoid.
    fn warm_root(&self) -> PathBuf {
        self.cfg.workspaces_root.join(WARM_DIR_NAME)
    }

    /// The file that says a slot finished being cut. `git worktree add` writes
    /// the `.git` link before it checks anything out, so a slot caught halfway
    /// through looks exactly like a finished one -- and being handed a
    /// half-checked-out tree reads as a repository whose files are missing,
    /// which is the kind of failure that gets blamed on the task.
    fn ready_marker(slot: &Path) -> PathBuf {
        let mut name = slot.file_name().unwrap_or_default().to_os_string();
        name.push(".ready");
        slot.with_file_name(name)
    }

    /// The trees waiting in the pool right now, oldest name first. A directory
    /// without the `.git` link a healthy worktree carries is skipped: `git
    /// worktree move` would refuse it, and a slot that cannot be handed out
    /// reads as a pool that is full.
    async fn warm_slots(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(mut rd) = tokio::fs::read_dir(self.warm_root()).await else {
            return out;
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();
            if !tokio::fs::try_exists(path.join(WORKTREE_LINK))
                .await
                .unwrap_or(false)
            {
                continue;
            }
            if !tokio::fs::try_exists(Self::ready_marker(&path))
                .await
                .unwrap_or(false)
            {
                continue;
            }
            out.push(path);
        }
        out.sort();
        out
    }

    /// Move a waiting tree onto `path` instead of cutting a new one there.
    /// Returns whether one was taken; a pool that is empty, disabled, or whose
    /// move failed falls through to the ordinary checkout.
    async fn take_warm(&self, task_id: &str, path: &Path) -> bool {
        if self.cfg.warm_trees == 0 {
            return false;
        }
        let slots = self.warm_slots().await;
        let Some(slot) = slots.into_iter().next() else {
            return false;
        };
        // Sidecar before the tree, same as `add_worktree`: a crash between the
        // two leaves a reclaimable record rather than an untracked tree.
        let now = unix_now();
        let meta = TaskMeta {
            task_id: task_id.to_string(),
            created_at_unix: now,
            last_used_unix: now,
        };
        if let Err(e) = self.write_meta(&meta).await {
            warn!(task_id = %task_id, error = %e, "warm tree not taken: could not write its sidecar");
            return false;
        }
        if let Err(e) = self
            .git_bare(&[
                "worktree",
                "move",
                slot.to_str().unwrap_or(""),
                path.to_str().unwrap_or(""),
            ])
            .await
        {
            self.remove_meta(task_id);
            warn!(
                task_id = %task_id,
                from = %slot.display(),
                error = %e,
                "warm tree not taken: moving it onto the task path failed"
            );
            return false;
        }
        let _ = tokio::fs::remove_file(Self::ready_marker(&slot)).await;
        self.register_safe_directory(path).await;
        // The pool is filled ahead of demand, so the tree may have been cut
        // before `main` moved. Re-pointing it is what keeps a warm tree from
        // being a stale one: a task that gets a tree three commits behind would
        // read the platform's own source wrong, and nothing on the surface
        // would say why.
        self.reset_to_main(path).await;
        info!(
            task_id = %task_id,
            from = %slot.display(),
            "handed a warm worktree to a new task instead of cutting one"
        );
        true
    }

    /// Point a task tree at `main`, and drop anything the previous user left in
    /// it. Best effort: the tree is usable either way, and a failure here is
    /// the next command's own to report.
    async fn reset_to_main(&self, path: &Path) {
        let Ok(head) = self.run_git(&["rev-parse", "HEAD"], Some(path)).await else {
            return;
        };
        let Ok(main) = self.git_bare(&["rev-parse", "main"]).await else {
            return;
        };
        if head.trim() == main.trim() {
            // Nothing to move and nothing to drop: the tree was cut from the
            // commit it is about to be handed out at.
            return;
        }
        let _ = self.run_git(&["reset", "--hard", "main"], Some(path)).await;
    }

    /// Cut trees until the pool holds as many as the deployment asked for.
    ///
    /// Best effort and never blocking an arrival: the lock is taken only if it
    /// is free, because a pool refill that queues behind a task's own creation
    /// makes the pool the reason the task waited.
    async fn warm_pool_once(&self) {
        let want = self.cfg.warm_trees;
        if want == 0 {
            return;
        }
        let Ok(_g) = self.warm_lock.try_lock() else {
            return;
        };
        let mut held = self.warm_slots().await.len();
        if held >= want {
            return;
        }
        // The pool does not compete with the tasks it serves: when the ceiling
        // is already spoken for, cutting another tree for a task that has not
        // arrived is a checkout paid against the task that has.
        let trees = self.list_task_trees().await;
        if trees.len() + want > self.cap_for(&trees).await {
            return;
        }
        if let Err(e) = tokio::fs::create_dir_all(self.warm_root()).await {
            warn!(error = %e, "warm worktree pool: cannot create its directory");
            return;
        }
        // Slots are named, not counted: the trees taken out are not necessarily
        // the highest-numbered ones, so walking `held..want` would ask for a
        // name that is still occupied and leave the pool short by that many.
        let mut name = 0usize;
        while held < want && name < want * 4 {
            let slot = self.warm_root().join(format!("slot-{name}"));
            name += 1;
            if tokio::fs::try_exists(&slot).await.unwrap_or(false) {
                // A directory without its ready marker is a cut that was
                // interrupted (this process died between the checkout and the
                // marker). It holds the name and no task can use it, so left
                // alone it is a pool that reads full and is not.
                if !tokio::fs::try_exists(Self::ready_marker(&slot))
                    .await
                    .unwrap_or(false)
                {
                    self.force_remove(&slot).await;
                } else {
                    continue;
                }
            }
            if let Err(e) = self.cut_worktree(&slot).await {
                warn!(error = %e, slot = %slot.display(), "warm worktree pool: cutting a tree failed");
                return;
            }
            self.register_safe_directory(&slot).await;
            if let Err(e) = tokio::fs::write(Self::ready_marker(&slot), b"").await {
                warn!(error = %e, slot = %slot.display(), "warm worktree pool: recording a cut tree as ready failed; the slot will be cut again");
                let _ = self.force_remove(&slot).await;
                return;
            }
            held += 1;
        }
        info!(want, held, "warm worktree pool refilled");
    }

    /// The tree on its own, without a claim: a caller that will run something in
    /// it must go through [`Self::route`], which is the only thing that keeps a
    /// reclamation pass from deleting it while the command runs.
    async fn route_validated(&self, task_id: &str, own_claim: bool) -> SFResult<(PathBuf, bool)> {
        let path = self.cfg.workspaces_root.join(task_id);
        if self.is_healthy(&path).await {
            self.touch(task_id).await;
            return Ok((path, false));
        }
        let _g = self.create_lock.lock().await;
        if self.is_healthy(&path).await {
            self.touch(task_id).await;
            return Ok((path, false));
        }
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            if self.commands_running(task_id, own_claim) > 0 {
                // Rebuilding means removing first, and a command is running in
                // this tree: the caller can come back, the command cannot.
                return Err(SFError::IO(format!(
                    "task worktree for {task_id} is in use by a running command and is not a \
                     healthy worktree; refusing to rebuild it from under that command"
                )));
            }
            warn!(task_id = %task_id, path = %path.display(),
                "task path exists but is not a healthy worktree; rebuilding");
            self.force_remove(&path).await;
        }
        self.enforce_cap(1).await;
        if self.take_warm(task_id, &path).await {
            self.refresh_count_metric().await;
            return Ok((path, true));
        }
        if let Err(e) = self.add_worktree(task_id, &path).await {
            self.metrics.inc_error("worktree_add");
            return Err(e);
        }
        self.refresh_count_metric().await;
        Ok((path, false))
    }

    async fn add_worktree(&self, task_id: &str, path: &Path) -> SFResult<()> {
        let now = unix_now();
        let meta = TaskMeta {
            task_id: task_id.to_string(),
            created_at_unix: now,
            last_used_unix: now,
        };
        // Sidecar lands before the tree: a crash between the two leaves a
        // reclaimable record; the reverse order leaves an untracked tree.
        self.write_meta(&meta).await?;
        if let Err(e) = self.cut_worktree(path).await {
            self.remove_meta(task_id);
            return Err(e);
        }
        info!(task_id = %task_id, path = %path.display(), "created per-task worktree");
        self.register_safe_directory(path).await;
        Ok(())
    }

    /// Check out `main` into `path` as a detached worktree of the bare repo.
    /// Shared by the task path and the warm pool, which want the same tree and
    /// differ only in what they record about it.
    async fn cut_worktree(&self, path: &Path) -> SFResult<()> {
        if let Some(root) = path.parent() {
            tokio::fs::create_dir_all(root).await.map_err(|e| {
                SFError::IO(format!("create worktree root {}: {e}", root.display()))
            })?;
        }
        self.git_bare(&[
            "worktree",
            "add",
            "--detach",
            path.to_str().unwrap_or(""),
            "main",
        ])
        .await
        .map(|_| ())
    }

    /// Healthy = worktree dir present with the `.git` link the bare repo
    /// manages. Authoritative registration reconciliation happens at boot and
    /// GC via `git worktree list`.
    async fn is_healthy(&self, path: &Path) -> bool {
        tokio::fs::try_exists(path.join(WORKTREE_LINK))
            .await
            .unwrap_or(false)
            && self.read_meta_of(path).is_some()
    }

    /// Boot-time reconciliation: prune stale registrations, adopt registered
    /// trees that lost their sidecar, drop sidecars whose tree is gone, and
    /// make sure every tree path is a git safe.directory.
    pub async fn recover(&self) -> SFResult<()> {
        tokio::fs::create_dir_all(&self.meta_dir)
            .await
            .map_err(|e| {
                SFError::IO(format!("create meta dir {}: {e}", self.meta_dir.display()))
            })?;
        tokio::fs::create_dir_all(&self.cfg.target_dir).await.ok();
        let _ = self.git_bare(&["worktree", "prune"]).await;
        // `prune` leaves locked registrations alone, so one whose tree is
        // already gone survives every boot: the cap counts it, the volume does
        // not hold it, and no reclaim can ever clear it. Unlock the ones the
        // volume cannot produce and prune again, so the count this process
        // starts with is the count of trees that exist.
        let mut dropped = false;
        for (id, path) in self.list_task_trees().await {
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                continue;
            }
            if self.clear_admin_lock(&path).await {
                warn!(task_id = %id, "dropping a locked registration whose tree is gone");
                dropped = true;
            }
        }
        if dropped {
            let _ = self.git_bare(&["worktree", "prune"]).await;
        }

        let trees = self.list_task_trees().await;
        for (id, path) in &trees {
            if self.read_meta(id).is_none() {
                warn!(task_id = %id, "adopting worktree with missing sidecar");
                let now = unix_now();
                self.write_meta(&TaskMeta {
                    task_id: id.clone(),
                    created_at_unix: now,
                    last_used_unix: now,
                })
                .await?;
            }
            self.register_safe_directory(path).await;
        }
        self.prune_orphan_metas(&trees).await;
        self.refresh_count_metric().await;
        // A pool that only refilled on demand would be empty on the first
        // arrival after every restart, which is exactly when the checkout it
        // saves is most visible.
        self.warm_pool_once().await;
        Ok(())
    }

    /// Reap trees idle past the TTL, then enforce the LRU cap. Returns the
    /// number of reclaimed trees.
    ///
    /// Times the pass on the way out: how long reclamation costs is what the
    /// interval follows, and a failed pass cost what it cost.
    pub async fn gc_once(&self) -> SFResult<u64> {
        let started = std::time::Instant::now();
        let reaped = self.gc_pass().await;
        self.gc_pass_millis
            .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        self.last_gc_unix.store(unix_now(), Ordering::Relaxed);
        // After the pass, not inside it: the pass holds the creation lock, and
        // the refill takes it only when nothing else wants it.
        self.warm_pool_once().await;
        reaped
    }

    async fn gc_pass(&self) -> SFResult<u64> {
        let _g = self.create_lock.lock().await;
        let now = unix_now();
        let trees = self.list_task_trees().await;
        let capacity = self.cap_for(&trees).await;
        // Oldest arrival to now, so the span covers exactly the trees being
        // counted. Without a second tree there is no arrival interval to read,
        // and the configured TTL stands.
        //
        // Measured over the task lane only, because that is the lane the TTL
        // governs: a standing shell is never reaped for idleness and is held
        // for as long as its agent exists, so counting it would stretch the
        // span by however old that conversation is and report a hold time
        // derived from a tree the rule does not apply to.
        let task_trees = trees.iter().filter(|(id, _)| !is_standing_lane(id)).count();
        let oldest = trees
            .iter()
            .filter(|(id, _)| !is_standing_lane(id))
            .filter_map(|(id, _)| self.read_meta(id))
            .map(|meta| meta.created_at_unix)
            .min();
        let derived = oldest.and_then(|first| {
            ttl_from(
                capacity,
                task_trees,
                now.saturating_sub(first).max(0) as u64,
            )
        });
        if let Some(d) = derived {
            self.metrics.ttl_seconds.set(d.as_secs() as f64, "measured");
        } else {
            self.metrics
                .ttl_seconds
                .set(self.cfg.ttl.as_secs() as f64, "configured");
        }
        let ttl = derived.unwrap_or(self.cfg.ttl).as_secs() as i64;
        let mut reclaimed = 0u64;
        for (id, path) in trees.iter() {
            // Asked per tree at the decision rather than from a snapshot taken
            // when the pass started: a command that started while this pass was
            // walking must not lose the tree it is running in.
            if self.commands_running(id, false) > 0 {
                continue;
            }
            // An idle hour is not evidence that a standing shell is done with:
            // its id comes back on the next turn, so the TTL would delete the
            // user's files between two conversations rather than after one.
            // The cap still reclaims it when a slot is needed -- see
            // [`is_standing_lane`].
            let reap = match self.read_meta(id) {
                Some(meta) => {
                    !is_standing_lane(id) && now.saturating_sub(meta.last_used_unix) >= ttl
                }
                None => {
                    // Registered tree without sidecar: adopt rather than reap,
                    // a restart may have raced its first write.
                    let now = unix_now();
                    let _ = self
                        .write_meta(&TaskMeta {
                            task_id: id.clone(),
                            created_at_unix: now,
                            last_used_unix: now,
                        })
                        .await;
                    false
                }
            };
            if reap {
                info!(task_id = %id, "reclaiming idle task worktree (TTL expired)");
                self.force_remove(path).await;
                self.remove_meta(id);
                self.metrics.gc_reclaimed.inc();
                reclaimed += 1;
            }
        }
        self.enforce_cap(0).await;
        let _ = self.git_bare(&["worktree", "prune"]).await;
        self.prune_orphan_metas(&self.list_task_trees().await).await;
        self.refresh_count_metric().await;
        Ok(reclaimed)
    }

    /// Remove least-recently-used trees until at most `max - incoming` trees
    /// remain. Trees with a command running in them are left alone, so the cap
    /// is a target this pass may not reach; what it could not reach is counted
    /// rather than silently dropped. Caller must hold [`Self::create_lock`]
    /// semantics (route and gc_once take it; this fn assumes exclusion).
    async fn enforce_cap(&self, incoming: usize) {
        let mut trees = self.list_task_trees().await;
        // Two different numbers, and the lower one wins: the configured value is
        // a policy ceiling (how many trees this executor is meant to serve at
        // once), while the measured one is what the volume can actually hold at
        // the size these trees are. A policy ceiling on its own cannot stop the
        // disk filling up -- the trees grow with the repository -- and a disk
        // reading on its own cannot stop inotify watches and git processes
        // multiplying. Which one applied is logged whenever the measurement is
        // the tighter of the two, because a cap that quietly stopped being the
        // configured one reads exactly like a cap that was never reached.
        let max = self.cap_for(&trees).await;
        if trees.len() + incoming <= max {
            return;
        }
        // Two lanes, and the eviction order is per lane: a task tree pays
        // before a standing shell does. See [`is_standing_lane`] for why the
        // LRU premise does not hold for the second one.
        trees.sort_by_key(|(id, _)| {
            (
                is_standing_lane(id),
                self.read_meta(id.as_str())
                    .map(|m| m.last_used_unix)
                    .unwrap_or(i64::MIN),
            )
        });
        let mut remaining = trees.len() + incoming;
        let mut deferred = 0u64;
        for (id, path) in trees {
            if remaining <= max {
                break;
            }
            if self.commands_running(&id, false) > 0 {
                // Deleting this would take the tree out from under a command
                // that keeps running with its files vanishing beneath it, and
                // the failure would be read as the command's own. Defer
                // instead — and say so, because a cap that is quietly not met
                // reads exactly like a cap that was never reached.
                deferred += 1;
                continue;
            }
            warn!(task_id = %id, cap = max, "reclaiming task worktree (LRU cap)");
            self.force_remove(&path).await;
            self.remove_meta(&id);
            self.metrics.gc_reclaimed.inc();
            remaining -= 1;
        }
        if deferred > 0 {
            self.metrics.cap_deferred.inc_by(deferred as f64);
            warn!(
                deferred,
                cap = max,
                "task worktree cap not met: the trees left to reclaim have commands running in them"
            );
        }
    }

    /// Fetch upstream refs/tags into the bare repo. Never fatal: offline
    /// executors keep serving the refs present at seed.
    ///
    /// Every configured mirror is tried in order. Reachability is per host and
    /// uneven — measured from this pod, one mirror stalled to a 20s timeout
    /// while the other answered in 0.86s, and 19 of 43 consecutive attempts
    /// against the single configured host failed outright — so stopping at the
    /// first failure left the worktrees on a stale baseline with nothing but a
    /// warning to show for it.
    /// Whether enough time has passed since the last fetch for another one to
    /// be worth its round trip.
    fn fetch_due(&self) -> bool {
        let stillness = self.stillness_secs.load(Ordering::Relaxed);
        let wait = fetch_interval_from(stillness, self.cfg.fetch_interval);
        let source = period_source(
            stillness,
            Duration::from_secs(stillness / 2),
            Duration::from_secs(MIN_FETCH_INTERVAL_SECS),
        );
        self.metrics
            .fetch_interval
            .set(wait.as_secs() as f64, source);
        let last = self.last_fetch_unix.load(Ordering::Relaxed);
        if last == 0 {
            return true;
        }
        unix_now() - last >= wait.as_secs() as i64
    }

    /// How often to reclaim: a multiple of the last pass's own duration, so
    /// the loop's work stays a fixed share of the process however big the
    /// volume it walks gets. Recomputed per call, for the same reason the git
    /// timeout is.
    fn gc_interval(&self) -> Duration {
        gc_interval_from(
            self.gc_pass_millis.load(Ordering::Relaxed),
            self.cfg.gc_interval,
        )
    }

    /// Whether the interval since the last pass has elapsed.
    fn gc_due(&self) -> bool {
        let pass_millis = self.gc_pass_millis.load(Ordering::Relaxed);
        let wait = self.gc_interval();
        // Before the first timed pass there is nothing to scale from, and the
        // configured interval is what stands. When the twenty-times figure is
        // under the floor it is the floor that is in force, and saying
        // "measured" there would credit a measurement that decided nothing.
        let source = period_source(
            pass_millis,
            scaled(pass_millis, MAINTENANCE_OVERHEAD_MULTIPLE),
            Duration::from_secs(MIN_GC_INTERVAL_SECS),
        );
        self.metrics
            .gc_interval_seconds
            .set(wait.as_secs_f64(), source);
        let last = self.last_gc_unix.load(Ordering::Relaxed);
        if last == 0 {
            return true;
        }
        unix_now() - last >= wait.as_secs() as i64
    }

    /// Whether the build-cache walk is due, and if so, the interval it is
    /// keeping.
    ///
    /// The reading this process publishes is written here rather than left at
    /// the configured seed: the number in force is the one a rule comparing a
    /// pass against its cadence has to see, and before the first timed pass the
    /// configured interval is what is in force.
    fn cache_walk_due(&self, cache: &cog_core::build_cache_readings::BuildCacheReadings) -> bool {
        let pass_millis = self.cache_pass_millis.load(Ordering::Relaxed);
        let wait = paced_interval(
            pass_millis,
            self.cfg.build_cache.scan_interval,
            cog_core::build_cache::MIN_SCAN_INTERVAL_SECS,
            MAINTENANCE_OVERHEAD_MULTIPLE,
        );
        cache.set_scan_interval_secs(wait.as_secs());
        let last = self.last_cache_walk_unix.load(Ordering::Relaxed);
        if last == 0 {
            return true;
        }
        unix_now() - last >= wait.as_secs() as i64
    }

    /// Whether the volume walk is due, and if so, the interval it is keeping.
    ///
    /// Walks of a claim read the same disk the commands build on, so the
    /// configured number is a floor: the walk never runs more often than that,
    /// and runs less often once a walk costs a real share of the interval it is
    /// scheduled at. Published with the label saying which input set it.
    fn volume_walk_due(&self) -> bool {
        let pass_millis = self.volume_pass_millis.load(Ordering::Relaxed);
        let wait = volume_walk_interval_from(pass_millis, self.cfg.data_volume_interval);
        let source = period_source(
            pass_millis,
            scaled(pass_millis, MAINTENANCE_OVERHEAD_MULTIPLE),
            self.cfg.data_volume_interval,
        );
        self.metrics
            .volume_walk_interval_seconds
            .set(wait.as_secs_f64(), source);
        let last = self.last_volume_walk_unix.load(Ordering::Relaxed);
        if last == 0 {
            return true;
        }
        unix_now() - last >= wait.as_secs() as i64
    }

    fn note_volume_walk(&self, pass_millis: u64) {
        self.volume_pass_millis
            .store(pass_millis, Ordering::Relaxed);
        self.last_volume_walk_unix
            .store(unix_now(), Ordering::Relaxed);
    }

    fn note_cache_walk(&self, pass_millis: u64) {
        self.cache_pass_millis.store(pass_millis, Ordering::Relaxed);
        self.last_cache_walk_unix
            .store(unix_now(), Ordering::Relaxed);
    }

    /// Take this fetch's view of `main` and, when it differs from the previous
    /// one, record how long the head had been still before it moved. That
    /// stillness is the only measurement anyone here has of how often the
    /// repository changes, and it is what the fetch period follows.
    async fn note_head_move(&self) {
        let head = match self.git_bare(&["rev-parse", "main"]).await {
            Ok(out) => out.trim().to_string(),
            Err(_) => return,
        };
        let now = unix_now();
        let mut last = match self.last_head.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match last.as_deref() {
            Some(previous) if previous != head => {
                let still_since = self.last_move_unix.swap(now, Ordering::Relaxed);
                if still_since > 0 {
                    self.stillness_secs
                        .store((now - still_since).max(1) as u64, Ordering::Relaxed);
                }
            }
            Some(_) => {}
            None => {
                self.last_move_unix.store(now, Ordering::Relaxed);
            }
        }
        *last = Some(head);
    }

    pub async fn fetch_once(&self) {
        self.last_fetch_unix.store(unix_now(), Ordering::Relaxed);
        if self
            .git_bare(&["remote", "get-url", "upstream"])
            .await
            .is_err()
        {
            let origin = self
                .git_bare(&["remote", "get-url", "origin"])
                .await
                .ok()
                .filter(|u| is_usable_url(u));
            let url = self.cfg.seed_urls.first().cloned().or(origin);
            let Some(url) = url else {
                warn!("no usable upstream remote for sandbox bare repo; skipping fetch");
                self.metrics.fetch_failures.inc();
                return;
            };
            if let Err(e) = self.git_bare(&["remote", "add", "upstream", &url]).await {
                warn!(error = %e, "could not add upstream remote");
                self.metrics.fetch_failures.inc();
                return;
            }
        }

        let mut current = self
            .git_bare(&["remote", "get-url", "upstream"])
            .await
            .unwrap_or_default()
            .trim()
            .to_string();

        // Whatever the remote already points at is tried last, so a bare repo
        // seeded without a configured mirror list still refreshes itself.
        let mut mirrors = self.cfg.seed_urls.clone();
        if !current.is_empty() && !mirrors.contains(&current) {
            mirrors.push(current.clone());
        }

        let mut failure: Option<(String, SFError)> = None;
        for mirror in &mirrors {
            if &current != mirror {
                if let Err(e) = self
                    .git_bare(&["remote", "set-url", "upstream", mirror])
                    .await
                {
                    warn!(error = %e, mirror = %mirror, "could not repoint upstream remote");
                    continue;
                }
                current = mirror.clone();
            }
            // A bare `fetch upstream` only writes refs/remotes/upstream/* and
            // never advances the local `main` that new worktrees are cut from;
            // the `+main:main` refspec fast-forces the local branch to upstream
            // so the 300s background fetch actually freshens the baseline.
            // Existing detached task trees keep pointing at their own commit.
            match self
                .git_bare(&["fetch", "upstream", "--tags", "--force", "+main:main"])
                .await
            {
                Ok(_) => {
                    info!(mirror = %mirror, "sandbox bare repo fetched from upstream");
                    self.note_head_move().await;
                    return;
                }
                Err(e) => {
                    warn!(error = %e, mirror = %mirror, "sandbox mirror unreachable; trying the next");
                    failure = Some((mirror.clone(), e));
                }
            }
        }

        match failure {
            Some((mirror, e)) => {
                warn!(
                    error = %e,
                    mirror = %mirror,
                    tried = mirrors.len(),
                    "every sandbox upstream mirror failed; serving seeded refs"
                );
                self.metrics.fetch_failures.inc();
            }
            None => {
                warn!("no usable upstream remote for sandbox bare repo; skipping fetch");
                self.metrics.fetch_failures.inc();
            }
        }
    }

    /// Walk every declared volume once and publish what each holds.
    ///
    /// A walk that fails leaves the previous reading in place. The alternative —
    /// publishing the partial total, or a zero — is a number smaller than the
    /// truth that can only keep the comparison against the declared size quiet,
    /// which is the state this reading exists to make visible.
    pub async fn measure_volumes_once(&self) {
        for footprint in &self.footprints {
            match footprint.measure_blocking().await {
                Ok(()) => {
                    if let Some(bytes) = footprint.value() {
                        self.metrics
                            .data_volume_used
                            .with_label_values(&[footprint.claim()])
                            .set(bytes as f64);
                    }
                }
                Err(e) => warn!(
                    claim = %footprint.claim(),
                    dir = %footprint.dir().display(),
                    error = %e,
                    "volume footprint scan failed; keeping the last measurement"
                ),
            }
        }
    }

    /// Spawn the GC, footprint and fetch background loops. Call once after
    /// [`Self::recover`].
    pub fn spawn_maintenance(self: &Arc<Self>) {
        let gc = Arc::clone(self);
        // The interval a pass may run at follows the pass; the pace the loop
        // wakes at does not, because a wake with nothing due costs nothing and
        // a declared cadence has to be the period the loop really wakes on.
        let gc_wake = Duration::from_secs(MIN_GC_INTERVAL_SECS);
        // No stop signal and no exit of its own: ending means worktrees stop
        // being reclaimed, which fills the volume the tasks run on.
        drop(cog_core::loop_health::spawn_unstoppable(
            WORKTREE_GC_LOOP,
            cog_core::loop_health::Cadence::Periodic(gc_wake),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let gc = Arc::clone(&gc);
                async move {
                    let mut ticker = tokio::time::interval(gc_wake);
                    ticker.tick().await;
                    loop {
                        beat.beat();
                        ticker.tick().await;
                        if !gc.gc_due() {
                            continue;
                        }
                        if let Err(e) = gc.gc_once().await {
                            warn!(error = %e, "task worktree GC failed");
                        }
                    }
                }
            },
        ));
        // A pod with no declared volumes gets no such loop: a walk registered to
        // measure nothing publishes only its own liveness, and its name in the
        // census would read as a reading being taken.
        if !self.footprints.is_empty() {
            let walker = Arc::clone(self);
            let interval = walker.cfg.data_volume_interval;
            // No stop signal and no exit of its own: ending means the volume
            // stops being measured, and the deployment's comparison against its
            // declared size goes back to having no reading behind it.
            drop(cog_core::loop_health::spawn_unstoppable(
                VOLUME_FOOTPRINT_LOOP,
                cog_core::loop_health::Cadence::Periodic(interval),
                // Rebuilt per attempt, so everything the body consumes is cloned here.
                move |beat| {
                    let walker = Arc::clone(&walker);
                    async move {
                        let mut ticker = tokio::time::interval(interval);
                        loop {
                            // The first tick of a fresh interval completes at
                            // once, so the deployment has a reading from the
                            // start rather than one scan interval later.
                            beat.beat();
                            ticker.tick().await;
                            if !walker.volume_walk_due() {
                                continue;
                            }
                            let started = std::time::Instant::now();
                            walker.measure_volumes_once().await;
                            // The walk alone is timed: the waiting it is
                            // scheduled by is not part of what the next period
                            // has to cover, so there is no path from the
                            // interval back into the measurement that sets it.
                            walker.note_volume_walk(started.elapsed().as_millis() as u64);
                        }
                    }
                },
            ));
        }
        let fetch = Arc::clone(self);
        // The loop wakes on a floor, and each waking decides whether a fetch is
        // due. The declared cadence is that floor, which the loop really does
        // keep; the interval between fetches is a separate, measured number, so
        // a repository that moves hourly is not polled every five minutes and
        // one that moves every minute is not left an hour behind.
        let floor = Duration::from_secs(MIN_FETCH_INTERVAL_SECS);
        drop(cog_core::loop_health::spawn_unstoppable(
            WORKTREE_FETCH_LOOP,
            cog_core::loop_health::Cadence::Periodic(floor),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let fetch = Arc::clone(&fetch);
                async move {
                    // Refresh immediately rather than on the startup critical
                    // path: a hung offline fetch must never delay the HTTP
                    // listener past the liveness grace window (which would
                    // crash-loop the pod). Failure only records a metric and
                    // retries when the next fetch is due.
                    beat.beat();
                    fetch.fetch_once().await;
                    let mut ticker = tokio::time::interval(floor);
                    ticker.tick().await;
                    loop {
                        ticker.tick().await;
                        // A beat per waking, not per fetch: this loop's health
                        // is about whether it is running, and it is running
                        // whether or not upstream had anything for it.
                        beat.beat();
                        if fetch.fetch_due() {
                            fetch.fetch_once().await;
                        }
                    }
                }
            },
        ));
        // The cache every command here builds into. Spawned whether or not a cap
        // is configured: measuring it is what makes the cap a decision somebody
        // can make, and the size is the reading that says when to make it.
        let cache = Arc::clone(&self.build_cache);
        let slot = self.build_cache_slot();
        // Waking is cheap and a pass is not: the loop wakes on the shared floor
        // and beats on every waking, and the interval it actually walks a pass
        // at follows what the pass costs. The cadence declared here is the one
        // the loop really wakes on, so a longer interval is not read as a stall.
        let cache_wake = Duration::from_secs(cog_core::build_cache::MIN_SCAN_INTERVAL_SECS);
        let cache_router = Arc::clone(self);
        // The claim this cache shares, and the walker already measuring it: the
        // ceiling is that claim's declared size minus what its co-tenants hold,
        // and both halves come from readings this process takes anyway. Nothing
        // here computes a number once and keeps it -- the co-tenants grow, and
        // the pass that notices is this one.
        let ceiling_cfg = self.cfg.build_cache.clone();
        let ceiling_footprint = ceiling_cfg.volume_claim.as_ref().and_then(|(claim, _)| {
            self.footprints
                .iter()
                .find(|f| f.claim() == claim.as_str())
                .cloned()
        });
        // No stop signal and no exit of its own: ending means the cache stops
        // being measured, and a cache nobody measures reads exactly like one
        // that is not growing.
        drop(cog_core::loop_health::spawn_unstoppable(
            SANDBOX_BUILD_CACHE_LOOP,
            cog_core::loop_health::Cadence::Periodic(cache_wake),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let cache = Arc::clone(&cache);
                let slot = Arc::clone(&slot);
                let router = Arc::clone(&cache_router);
                let ceiling_cfg = ceiling_cfg.clone();
                let ceiling_footprint = ceiling_footprint.clone();
                async move {
                    let mut ticker = tokio::time::interval(cache_wake);
                    loop {
                        // The first tick of a fresh interval completes at once, so
                        // the deployment has a size reading from the start rather
                        // than one interval later.
                        ticker.tick().await;
                        beat.beat();
                        if !router.cache_walk_due(&cache) {
                            continue;
                        }
                        cache.set_cap_ceiling(
                            ceiling_cfg.measured_ceiling(ceiling_footprint.as_deref(), &cache),
                        );
                        let started = std::time::Instant::now();
                        cache.measure_and_enforce(Some(slot.as_ref())).await;
                        router.note_cache_walk(started.elapsed().as_millis() as u64);
                    }
                }
            },
        ));
    }

    // -- internals -------------------------------------------------------------

    fn meta_path(&self, task_id: &str) -> PathBuf {
        self.meta_dir.join(format!("{task_id}.json"))
    }

    /// Publish one tree's sidecar in a single step.
    ///
    /// The write goes to a sibling file first and is then renamed into place.
    /// A plain write truncates before it fills: between those two moments the
    /// sidecar is a zero-length or half-written file, and every reader of it is
    /// lock-free — a reclaim pass, a cap ranking, another writer's
    /// read-modify-write. A reader that catches that window parses nothing and
    /// concludes the tree has no sidecar, and the reclamation rule for a tree
    /// without one is to *adopt* it rather than reap it, so a live fault reads
    /// as a stale tree kept alive. Measured on 2026-11-03: a tree was routed,
    /// the claim on it was released (which re-stamps `last_used` from a task of
    /// its own), and the very next `gc_once` counted zero reaps where one tree
    /// was idle — the test holding that claim failed on roughly a quarter of
    /// its runs.
    ///
    /// Rename within one directory is atomic, so a reader sees either the whole
    /// old file or the whole new one. The temporary name cannot collide with a
    /// sidecar (`meta_dir` readers only ever accept `*.json`) nor with a
    /// concurrent write of the same tree (the id is per-process and per-call).
    async fn write_meta(&self, meta: &TaskMeta) -> SFResult<()> {
        let text = serde_json::to_string_pretty(meta)?;
        // A sidecar is recomputable from the tree it describes, so a reader on
        // the same kernel seeing the whole file is the whole requirement.
        cog_core::atomic_write::write_bytes_async(
            &self.meta_path(&meta.task_id),
            text.as_bytes(),
            cog_core::atomic_write::Durability::SameKernel,
        )
        .await
    }

    fn read_meta(&self, task_id: &str) -> Option<TaskMeta> {
        std::fs::read_to_string(self.meta_path(task_id))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
    }

    fn read_meta_of(&self, path: &Path) -> Option<TaskMeta> {
        let id = path.file_name()?.to_str()?;
        self.read_meta(id)
    }

    fn remove_meta(&self, task_id: &str) {
        let _ = std::fs::remove_file(self.meta_path(task_id));
    }

    async fn touch(&self, task_id: &str) {
        if let Some(mut meta) = self.read_meta(task_id) {
            meta.last_used_unix = unix_now();
            if self.write_meta(&meta).await.is_err() {
                warn!(task_id = %task_id, "failed to refresh task worktree last_used");
            }
        }
    }

    async fn prune_orphan_metas(&self, trees: &[(String, PathBuf)]) {
        let live: std::collections::HashSet<String> =
            trees.iter().map(|(id, _)| id.clone()).collect();
        let Ok(mut entries) = tokio::fs::read_dir(&self.meta_dir).await else {
            return;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Some(id) = name.strip_suffix(".json") else {
                continue;
            };
            if !live.contains(id) {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }
    }

    /// Registered worktrees under the workspaces root, as `(task_id, path)`.
    async fn list_task_trees(&self) -> Vec<(String, PathBuf)> {
        let out = match self.git_bare(&["worktree", "list", "--porcelain"]).await {
            Ok(out) => out,
            Err(e) => {
                warn!(error = %e, "git worktree list failed");
                return Vec::new();
            }
        };
        parse_worktree_list(&out)
            .into_iter()
            .filter(|e| !e.bare && e.path.parent() == Some(self.cfg.workspaces_root.as_path()))
            .filter_map(|e| {
                let id = e.path.file_name()?.to_str()?.to_string();
                Some((id, e.path))
            })
            .collect()
    }

    async fn force_remove(&self, path: &Path) {
        // An admin lock makes the tree invisible to both halves below:
        // `worktree remove --force` refuses a locked tree, so the directory is
        // taken by `remove_dir_all` while the registration stays, and
        // `worktree prune` skips locked entries. What is left is a tree that
        // is gone from the volume and still counted by the cap — on
        // 2026-11-06 the live volume carried 21 such registrations against 7
        // real trees, so the cap of 8 could never be met: every creation paid
        // a reclaim pass, that pass logged the same unreachable ids as
        // reclaimed, and it evicted the tree a conversation had just used.
        self.clear_admin_lock(path).await;
        let _ = self
            .git_bare(&["worktree", "remove", "--force", path.to_str().unwrap_or("")])
            .await;
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
        self.forget_safe_directory(path).await;
        let _ = self.git_bare(&["worktree", "prune"]).await;
    }

    /// Drop the `locked` marker a killed `worktree add` left on the tree at
    /// `path`. Returns whether one was found.
    ///
    /// `git worktree unlock` names the tree by path and so cannot reach one
    /// whose directory is already gone — which is the shape this leak takes —
    /// so the registration is located by the `gitdir` file git writes for it.
    /// Nothing in this process locks a tree, so a lock met on the way to a
    /// removal is a corpse: deleting the marker is the only way back to an
    /// entry `worktree remove` and `worktree prune` will both act on.
    async fn clear_admin_lock(&self, path: &Path) -> bool {
        let Ok(mut entries) = tokio::fs::read_dir(self.cfg.bare_repo.join("worktrees")).await
        else {
            return false;
        };
        let mut cleared = false;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Ok(target) = tokio::fs::read_to_string(entry.path().join("gitdir")).await else {
                continue;
            };
            if Path::new(target.trim()).parent() != Some(path) {
                continue;
            }
            let locked = entry.path().join("locked");
            if !tokio::fs::try_exists(&locked).await.unwrap_or(false) {
                continue;
            }
            match tokio::fs::remove_file(&locked).await {
                Ok(()) => {
                    let id = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    warn!(
                        task_id = %id,
                        "cleared a worktree admin lock left by a killed process"
                    );
                    cleared = true;
                }
                Err(e) => warn!(
                    error = %e,
                    path = %locked.display(),
                    "could not clear a worktree admin lock"
                ),
            }
        }
        cleared
    }

    /// Mark a tree safe for git, once.
    ///
    /// `--add` appends unconditionally and nothing removed the entry when the
    /// tree went away, so the shared global config grew with every task: by
    /// 2026-10-20 it held 30k dead paths and 1.5 MB that every git invocation
    /// on the host parsed. The ownership check reads global config only, so
    /// this file is where the entry must live; the bound is "add when absent"
    /// plus the removal in teardown.
    async fn register_safe_directory(&self, path: &Path) {
        let path = path.to_str().unwrap_or("");
        if path.is_empty() {
            return;
        }
        let listed = self
            .run_git(&["config", "--global", "--get-all", "safe.directory"], None)
            .await
            .map(|out| out.lines().any(|line| line.trim() == path))
            .unwrap_or(false);
        if listed {
            return;
        }
        let _ = self
            .run_git(
                &["config", "--global", "--add", "safe.directory", path],
                None,
            )
            .await;
    }

    /// Drop the tree's entry when the tree goes away. `--fixed-value` because
    /// git reads the value as a regex otherwise.
    async fn forget_safe_directory(&self, path: &Path) {
        let path = path.to_str().unwrap_or("");
        if path.is_empty() {
            return;
        }
        let _ = self
            .run_git(
                &[
                    "config",
                    "--global",
                    "--fixed-value",
                    "--unset-all",
                    "safe.directory",
                    path,
                ],
                None,
            )
            .await;
    }

    async fn refresh_count_metric(&self) {
        self.metrics
            .workspaces
            .set(self.list_task_trees().await.len() as f64);
        self.refresh_disk_metric().await;
    }

    /// Filesystem capacity via `df` (the runtime image carries coreutils).
    async fn refresh_disk_metric(&self) {
        if let Some(disk) = self.read_disk().await {
            self.metrics.disk_total_bytes.set(disk.total_bytes as f64);
            self.metrics.disk_avail_bytes.set(disk.avail_bytes as f64);
        }
    }

    /// What the filesystem under the workspaces root reports. One reading feeds
    /// both the panel and the cap, so the number the cap follows is the number
    /// an operator can see.
    async fn read_disk(&self) -> Option<DiskReading> {
        let out = tokio::process::Command::new("df")
            .arg("-PB1")
            .arg(&self.cfg.workspaces_root)
            .output()
            .await
            .ok()?;
        if !out.status.success() {
            return None;
        }
        parse_df(&String::from_utf8_lossy(&out.stdout))
    }

    /// The cap this pass runs under. Two different numbers, and the lower one
    /// wins: the configured value is a policy ceiling (how many trees this
    /// executor is meant to serve at once), while the measured one is what the
    /// volume can actually hold at the size these trees are. A policy ceiling
    /// on its own cannot stop the disk filling up -- the trees grow with the
    /// repository -- and a disk reading on its own cannot stop inotify watches
    /// and git processes multiplying.
    async fn cap_for(&self, trees: &[(String, PathBuf)]) -> usize {
        let declared = self.cfg.max_workspaces;
        match self.measured_cap(trees).await {
            Some(measured) => {
                if measured < declared {
                    info!(
                        declared,
                        measured,
                        trees = trees.len(),
                        "task worktree cap lowered to what the volume holds"
                    );
                }
                measured.min(declared)
            }
            None => declared,
        }
    }

    /// What the volume and the trees say it can hold. `None` when neither has
    /// been measured yet: the caller then has only the configured ceiling to go
    /// on, and says so by not lowering it.
    async fn measured_cap(&self, trees: &[(String, PathBuf)]) -> Option<usize> {
        if trees.is_empty() {
            return None;
        }
        let disk = self.read_disk().await?;
        let root = self.cfg.workspaces_root.clone();
        let bytes =
            tokio::task::spawn_blocking(move || cog_core::fs_size::dir_size_bytes(&root, &[]).ok())
                .await
                .ok()??;
        workspace_cap(disk, bytes, trees.len())
    }

    async fn run_git(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
        match self.run_git_once(args, workdir).await {
            Ok(out) => Ok(out),
            Err(e) => {
                let cleared = match stale_lock_candidate(&e.to_string()) {
                    Some(lock) => {
                        let gone = clear_stale_git_lock(&lock, self.stale_lock_age()).await;
                        if gone {
                            warn!(lock = %lock.display(),
                                "removed stale git lock left by a killed process; retrying");
                        }
                        gone
                    }
                    None => false,
                };
                if cleared {
                    self.run_git_once(args, workdir).await
                } else {
                    Err(e)
                }
            }
        }
    }

    /// How long a git command may run here: a multiple of the slowest one that
    /// has completed on this volume, floored. Recomputed per call so a volume
    /// that got slower (a bigger repository, a busier disk) is allowed the time
    /// it now needs, rather than being killed at a number written elsewhere.
    fn git_timeout(&self) -> Duration {
        cog_core::git_budget::git_timeout_from(self.git_slowest_ms.load(Ordering::Relaxed))
    }

    /// How old a git lock has to be before it can only be a corpse.
    ///
    /// Killed git processes (OOM, node restart) leave `.lock` files behind, and
    /// every git write here is bounded by [`Self::git_timeout`] and
    /// kill-on-drop. So a lock older than twice that timeout cannot belong to a
    /// live command: the command that made it would have been killed first. Twice
    /// rather than once, because a command may have taken the lock just as this
    /// pass looked at the clock.
    fn stale_lock_age(&self) -> Duration {
        self.git_timeout() * 2
    }

    async fn run_git_once(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
        let mut cmd = tokio::process::Command::new("git");
        cmd.args(args).kill_on_drop(true);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let started = std::time::Instant::now();
        let out = tokio::time::timeout(self.git_timeout(), cmd.output())
            .await
            .map_err(|_| SFError::IO(format!("git {} timed out", args.join(" "))))?
            .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?;
        // Only completed commands are recorded, whatever their exit code: a
        // command that failed fast is still evidence of how fast git is here,
        // and one that never returns must not raise the bar for the next.
        self.git_slowest_ms
            .fetch_max(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        if !out.status.success() {
            return Err(SFError::IO(format!(
                "git {} failed: {}{}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    async fn git_bare(&self, args: &[&str]) -> SFResult<String> {
        let mut full: Vec<&str> = vec!["--git-dir", self.cfg.bare_repo.to_str().unwrap_or("")];
        full.extend_from_slice(args);
        self.run_git(&full, None).await
    }
}

/// Strict task id validation: the id is a client-supplied filesystem name, so
/// only whitelisted characters pass and any traversal or reserved form is an
/// error (never a silent rename into a shared bucket).
pub(crate) fn validate_task_id(id: &str) -> SFResult<()> {
    let valid = !id.is_empty()
        && id.len() <= MAX_TASK_ID_LEN
        && !id.starts_with('.')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(SFError::Agent(format!(
            "invalid task id for workspace routing: {id:?}"
        )))
    }
}

/// The prefix `cog-agent` gives the task id of a call that carries no task
/// scope: one workspace per calling agent, named after that agent. Kept in
/// step with `sandbox_task_id` in `crates/cog-agent/src/tools.rs`; the two
/// crates do not depend on each other, so the spelling is shared by this
/// comment rather than by an import.
const STANDING_SHELL_PREFIX: &str = "unscoped-shell-";

/// Whether a task id names a standing agent's shell rather than one round of
/// task work.
///
/// The cap evicts by least-recently-used, and that rule rests on an assumption:
/// a tree nobody touched recently is a tree nobody will ask for again. It holds
/// for a task tree, whose id is spent by the round that created it, and it
/// fails for a conversation shell, whose id is the calling agent's own name and
/// is handed back verbatim on the next turn. Measured on the deployed chat lane
/// (2026-11-06): a squad pipeline creating a tree roughly every minute churned
/// an eight-slot pool, `unscoped-shell-vera-82191e4a` was reclaimed 144 s after
/// the turn that used it, and the files written in that turn went with it --
/// the next turn was routed to a fresh checkout and could not see its own work.
fn is_standing_lane(task_id: &str) -> bool {
    task_id.starts_with(STANDING_SHELL_PREFIX)
}

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Extract the lock path from git's fixed wording:
/// `fatal: Unable to create '<path>/index.lock': File exists.`
/// Only `.lock` paths are accepted.
fn stale_lock_candidate(error_text: &str) -> Option<PathBuf> {
    const PREFIX: &str = "Unable to create '";
    const SUFFIX: &str = "': File exists";
    let start = error_text.find(PREFIX)? + PREFIX.len();
    let end = error_text[start..].find(SUFFIX)? + start;
    let path = PathBuf::from(&error_text[start..end]);
    if path.extension().and_then(|s| s.to_str()) == Some("lock") {
        Some(path)
    } else {
        None
    }
}

async fn clear_stale_git_lock(lock: &Path, max_age: Duration) -> bool {
    let Ok(meta) = tokio::fs::metadata(lock).await else {
        return false;
    };
    let Ok(mtime) = meta.modified() else {
        return false;
    };
    if mtime.elapsed().unwrap_or(Duration::ZERO) < max_age {
        return false;
    }
    tokio::fs::remove_file(lock).await.is_ok()
}

#[derive(Debug, Clone)]
struct WorktreeEntry {
    path: PathBuf,
    bare: bool,
}

fn parse_worktree_list(text: &str) -> Vec<WorktreeEntry> {
    let mut out = Vec::new();
    let mut cur: Option<WorktreeEntry> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("worktree ") {
            if let Some(entry) = cur.take() {
                out.push(entry);
            }
            cur = Some(WorktreeEntry {
                path: PathBuf::from(rest),
                bare: false,
            });
        } else if let Some(entry) = cur.as_mut() {
            if line == "bare" {
                entry.bare = true;
            }
        }
    }
    if let Some(entry) = cur {
        out.push(entry);
    }
    out
}

/// Anchor a payload path against a task worktree: relative paths resolve inside
/// the tree, absolute paths keep their meaning. No filesystem fence is added
/// here — the pod boundary remains the security boundary for now.
pub fn anchor_path(base: Option<&Path>, path: &str) -> PathBuf {
    let p = Path::new(path);
    match base {
        Some(dir) if p.is_relative() => dir.join(p),
        _ => p.to_path_buf(),
    }
}

/// Visible to the crate because the executor server has to drive a real router:
/// whether a running command claims its tree is decided there, and a claim that
/// is never taken looks exactly like one that is.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn config_parses_env_with_safe_floors() {
        std::env::set_var("SANDBOX_WORKSPACE_TTL_SECS", "1");
        std::env::set_var("SANDBOX_WORKSPACE_GC_INTERVAL_SECS", "1");
        std::env::set_var("SANDBOX_REPO_FETCH_INTERVAL_SECS", "1");
        std::env::set_var("SANDBOX_MAX_TASK_WORKSPACES", "0");
        std::env::set_var("COGNEVA_DATA_VOLUME_INTERVAL_SECS", "1");
        std::env::set_var(
            "SANDBOX_REPO_SEED_URLS",
            "__GIT_SEED_URL__, https://gitee.com/o/r.git",
        );
        let cfg = WorkdirConfig::from_env();
        assert!(cfg.ttl >= Duration::from_secs(1));
        assert!(cfg.gc_interval >= Duration::from_secs(10));
        assert!(cfg.fetch_interval >= Duration::from_secs(30));
        assert_eq!(cfg.max_workspaces, 1);
        assert_eq!(
            cfg.data_volume_interval,
            Duration::from_secs(cog_core::claim_footprint::MIN_SCAN_INTERVAL_SECS),
            "a scan faster than the scrape interval only costs walk time"
        );
        assert_eq!(
            cfg.seed_urls,
            vec!["https://gitee.com/o/r.git".to_string()],
            "an unresolved placeholder is dropped, the real mirror survives"
        );
        for key in [
            "SANDBOX_WORKSPACE_TTL_SECS",
            "SANDBOX_WORKSPACE_GC_INTERVAL_SECS",
            "SANDBOX_REPO_FETCH_INTERVAL_SECS",
            "SANDBOX_MAX_TASK_WORKSPACES",
            "SANDBOX_REPO_SEED_URLS",
            "COGNEVA_DATA_VOLUME_INTERVAL_SECS",
        ] {
            std::env::remove_var(key);
        }
    }

    /// The declared volumes are the deployment's statement about this pod, so a
    /// list it cannot read has to be visible rather than quietly measuring
    /// nothing: a volume nobody measures looks exactly like a volume that is
    /// small enough.
    #[test]
    fn config_carries_a_broken_volume_declaration_instead_of_dropping_it() {
        std::env::set_var(
            "COGNEVA_DATA_VOLUME_MOUNTS",
            "cogneva-sandbox-pvc=/opt/cogneva/sandbox\n/opt/cogneva/other\n",
        );
        let cfg = WorkdirConfig::from_env();
        assert!(cfg.data_volumes.is_empty());
        assert!(cfg.data_volume_declaration_error.is_some());

        std::env::set_var(
            "COGNEVA_DATA_VOLUME_MOUNTS",
            "cogneva-sandbox-pvc = /opt/cogneva/sandbox/\n",
        );
        let cfg = WorkdirConfig::from_env();
        assert!(cfg.data_volume_declaration_error.is_none());
        assert_eq!(cfg.data_volumes.len(), 1);
        assert_eq!(cfg.data_volumes[0].claim, "cogneva-sandbox-pvc");
        assert_eq!(cfg.data_volumes[0].path, "/opt/cogneva/sandbox");
        std::env::remove_var("COGNEVA_DATA_VOLUME_MOUNTS");
    }

    /// Build a router over `root` whose only declared volume is `claim` on
    /// `dir`, so the footprint rules can be driven without a workload around
    /// them.
    fn router_watching(root: &Path, bare: &Path, claim: &str, dir: &Path) -> Arc<WorkdirRouter> {
        let cfg = WorkdirConfig {
            workspaces_root: root.join("workspaces"),
            bare_repo: bare.to_path_buf(),
            target_dir: root.join("src").join("target"),
            seed_urls: Vec::new(),
            ttl: DEFAULT_TTL_SECS_FALLBACK,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: 8,
            warm_trees: 0,
            data_volumes: vec![ClaimMount {
                claim: claim.to_string(),
                path: dir.display().to_string(),
            }],
            data_volume_declaration_error: None,
            data_volume_interval: Duration::from_secs(30),
            build_cache: BuildCacheConfig::default(),
        };
        WorkdirRouter::new(cfg).unwrap()
    }

    fn series_of(render: &str, claim: &str) -> Option<String> {
        let wanted = format!(
            "{}{{{}=\"{claim}\"}}",
            cog_core::claim_footprint::USED_METRIC,
            cog_core::claim_footprint::CLAIM_LABEL
        );
        render
            .lines()
            .find(|line| line.starts_with(&wanted))
            .map(str::to_string)
    }

    /// The sandbox executor is the process that writes this volume, and it is
    /// the only one that can measure what the volume holds: the kubelet's
    /// per-volume numbers for a directory-backed volume describe the node's
    /// filesystem, not the claim. Before the first successful walk nothing is
    /// published, because a zero would be a claim that the volume is empty.
    #[tokio::test]
    async fn declared_volume_is_measured_and_published_under_its_claim() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let volume = tmp.path().join("sandbox-volume");
        std::fs::create_dir_all(volume.join("workspaces")).unwrap();
        std::fs::write(volume.join("f.bin"), vec![0u8; 4096]).unwrap();
        std::fs::write(volume.join("workspaces/g.bin"), vec![0u8; 100]).unwrap();

        let r = router_watching(tmp.path(), &bare, "cogneva-sandbox-pvc", &volume);
        assert!(
            series_of(&r.metrics().render(), "cogneva-sandbox-pvc").is_none(),
            "a series before the first walk claims the volume is empty"
        );

        r.measure_volumes_once().await;
        assert_eq!(
            series_of(&r.metrics().render(), "cogneva-sandbox-pvc"),
            Some(format!(
                "{}{{{}=\"cogneva-sandbox-pvc\"}} 4196",
                cog_core::claim_footprint::USED_METRIC,
                cog_core::claim_footprint::CLAIM_LABEL
            ))
        );

        // A walk that cannot read the volume keeps the last reading: a smaller
        // number can only silence the comparison against the declared size.
        std::fs::remove_dir_all(&volume).unwrap();
        r.measure_volumes_once().await;
        assert!(r.metrics().render().contains(
            "cogneva_data_volume_used_bytes{persistentvolumeclaim=\"cogneva-sandbox-pvc\"} 4196"
        ));
    }

    /// The cache this executor's commands build into is bounded here, by the
    /// one fact this process has: whether it is running a command. With one
    /// running the pass deletes nothing and is counted as deferred; with none,
    /// the same pass brings the cache under the cap.
    ///
    /// The pairing is the point. A pass that only measured, or one that deleted
    /// while a command was reading the cache, would both read as "the bound is
    /// being enforced" from the byte total alone.
    #[tokio::test]
    async fn the_build_cache_is_reclaimed_only_while_no_command_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let target = tmp.path().join("src").join("target");
        for name in ["one", "two", "three"] {
            let path = target.join("release/incremental").join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, vec![b'x'; 1000]).unwrap();
        }
        std::fs::create_dir_all(target.join("release/deps")).unwrap();
        std::fs::write(target.join("release/deps/lib.rlib"), vec![b'x'; 1000]).unwrap();

        let cfg = WorkdirConfig {
            workspaces_root: tmp.path().join("workspaces"),
            bare_repo: bare.clone(),
            target_dir: target.clone(),
            seed_urls: Vec::new(),
            ttl: DEFAULT_TTL_SECS_FALLBACK,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: 8,
            warm_trees: 0,
            data_volumes: Vec::new(),
            data_volume_declaration_error: None,
            data_volume_interval: Duration::from_secs(30),
            build_cache: BuildCacheConfig {
                cap: cog_core::config::CacheCapSetting::Fixed(3000),
                scan_interval: Duration::from_secs(300),
                volume_claim: None,
            },
        };
        let router = WorkdirRouter::new(cfg).unwrap();
        let slot = router.build_cache_slot();
        let on_disk = || cog_core::fs_size::dir_size_bytes(&target, &[]).unwrap();

        let running = router.command_hold(None).await;
        router
            .build_cache()
            .measure_and_enforce(Some(slot.as_ref()))
            .await;
        assert_eq!(on_disk(), 4000, "a running command freezes the cache");

        drop(running);
        router
            .build_cache()
            .measure_and_enforce(Some(slot.as_ref()))
            .await;
        let after = on_disk();
        assert!(after <= 3000, "the pass left {after} bytes over a 3000 cap");

        // Both ends are readable on the surface the deployment scrapes, so
        // "nothing was removed" and "nothing is running" can be told apart.
        let text = crate::build_cache::render(router.build_cache()).await;
        assert!(text.contains("outcome=\"busy\"} 1"), "{text}");
        assert!(text.contains("outcome=\"reclaimed\"} 1"), "{text}");
    }

    /// Two volumes are two readings, each joined against its own declaration:
    /// one number covering both would hide an overrun on the volume that
    /// overran behind the one that did not.
    #[tokio::test]
    async fn each_declared_volume_keeps_its_own_series() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let first = tmp.path().join("vol-a");
        let second = tmp.path().join("vol-b");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        std::fs::write(first.join("a.bin"), vec![0u8; 1000]).unwrap();
        std::fs::write(second.join("b.bin"), vec![0u8; 2000]).unwrap();

        let cfg = WorkdirConfig {
            workspaces_root: tmp.path().join("workspaces"),
            bare_repo: bare.clone(),
            target_dir: tmp.path().join("src").join("target"),
            seed_urls: Vec::new(),
            ttl: DEFAULT_TTL_SECS_FALLBACK,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: 8,
            warm_trees: 0,
            data_volumes: vec![
                ClaimMount {
                    claim: "vol-a-pvc".into(),
                    path: first.display().to_string(),
                },
                ClaimMount {
                    claim: "vol-b-pvc".into(),
                    path: second.display().to_string(),
                },
            ],
            data_volume_declaration_error: None,
            data_volume_interval: Duration::from_secs(30),
            build_cache: BuildCacheConfig::default(),
        };
        let r = WorkdirRouter::new(cfg).unwrap();
        r.measure_volumes_once().await;

        let render = r.metrics().render();
        assert!(series_of(&render, "vol-a-pvc").unwrap().ends_with(" 1000"));
        assert!(series_of(&render, "vol-b-pvc").unwrap().ends_with(" 2000"));
    }

    /// Every loop this executor starts has to be in the census, or its exit is
    /// silent: each of them keeps its last reading when it stops, which is what a
    /// loop that is running and has nothing to do looks like too.
    #[tokio::test]
    async fn every_started_loop_is_registered_in_the_census() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let volume = tmp.path().join("vol");
        std::fs::create_dir_all(&volume).unwrap();

        // The registry is process-wide, so the negative half is asserted first,
        // before anything in this test registers the volume walk.
        router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK).spawn_maintenance();
        assert!(
            !cog_core::loop_health::registry()
                .names()
                .iter()
                .any(|n| n == VOLUME_FOOTPRINT_LOOP),
            "a pod with no declared volume must not carry a walk that measures nothing"
        );

        router_watching(tmp.path(), &bare, "cogneva-sandbox-pvc", &volume).spawn_maintenance();

        let names = cog_core::loop_health::registry().names();
        for expected in [
            WORKTREE_GC_LOOP,
            WORKTREE_FETCH_LOOP,
            VOLUME_FOOTPRINT_LOOP,
            SANDBOX_BUILD_CACHE_LOOP,
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "{expected} is not in the census: {names:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_task_ids() {
        assert!(validate_task_id("").is_err());
        assert!(validate_task_id("../escape").is_err());
        assert!(validate_task_id("..").is_err());
        assert!(validate_task_id(".").is_err());
        assert!(validate_task_id(".meta").is_err());
        assert!(validate_task_id("a/b").is_err());
        assert!(validate_task_id("a b").is_err());
        assert!(validate_task_id("task$x").is_err());
        assert!(validate_task_id(&"x".repeat(MAX_TASK_ID_LEN + 1)).is_err());
        validate_task_id("ralph-roundtable-7bb0d434-3a3e-4c9f-b1a0-deadbeefcafe").unwrap();
        validate_task_id("squad_task.1-x").unwrap();
    }

    #[test]
    fn placeholder_url_is_not_usable() {
        assert!(is_usable_url("https://github.com/o/r.git"));
        assert!(!is_usable_url("__GIT_SEED_URL__"));
        assert!(!is_usable_url("  "));
    }

    #[test]
    fn seed_urls_preserve_mirror_order() {
        assert_eq!(
            split_seed_urls("https://github.com/o/r.git,https://gitee.com/o/r.git"),
            vec![
                "https://github.com/o/r.git".to_string(),
                "https://gitee.com/o/r.git".to_string()
            ],
            "the first entry stays the preferred upstream, the rest are fallbacks"
        );
        assert!(split_seed_urls("__GIT_SEED_URL__").is_empty());
        assert!(split_seed_urls("").is_empty());
        assert_eq!(
            split_seed_urls("  https://gitee.com/o/r.git  ,  "),
            vec!["https://gitee.com/o/r.git".to_string()]
        );
    }

    #[test]
    fn anchors_relative_paths_under_base_keeps_absolute() {
        let base = Path::new("/ws/task-1");
        assert_eq!(
            anchor_path(Some(base), "a/b.txt"),
            Path::new("/ws/task-1/a/b.txt")
        );
        assert_eq!(
            anchor_path(Some(base), "/etc/hostname"),
            Path::new("/etc/hostname")
        );
        assert_eq!(anchor_path(None, "a/b.txt"), Path::new("a/b.txt"));
    }

    #[test]
    fn parses_porcelain_worktree_list() {
        let text =
            "worktree /a/bare.git\nHEAD abc\nbare\n\nworktree /a/ws/task-1\nHEAD def\ndetached\n\n";
        let entries = parse_worktree_list(text);
        assert_eq!(entries.len(), 2);
        assert!(entries[0].bare);
        assert!(!entries[1].bare);
        assert_eq!(entries[1].path, PathBuf::from("/a/ws/task-1"));
    }

    #[test]
    fn stale_lock_candidate_parses_git_wording() {
        let err = "fatal: Unable to create '/repo/worktrees/x/index.lock': File exists.";
        assert_eq!(
            stale_lock_candidate(err),
            Some(PathBuf::from("/repo/worktrees/x/index.lock"))
        );
        assert!(stale_lock_candidate("Unable to create '/x/HEAD': File exists.").is_none());
        assert!(stale_lock_candidate("nothing here").is_none());
    }

    fn git(args: &[&str], dir: &Path) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn git_out(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .expect("run git");
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    pub(crate) fn seed_bare(root: &Path) -> PathBuf {
        let work = root.join("seed");
        std::fs::create_dir_all(&work).unwrap();
        git(&["init", "-q", "-b", "main"], &work);
        std::fs::write(work.join("README"), "base\n").unwrap();
        git(&["add", "-A"], &work);
        git(&["commit", "-qm", "base"], &work);
        let bare = root.join("repo.git");
        let out = std::process::Command::new("git")
            .args(["clone", "-q", "--bare"])
            .arg(&work)
            .arg(&bare)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(out.status.success(), "bare clone failed");
        bare
    }

    pub(crate) fn router(
        root: &Path,
        bare: &Path,
        max: usize,
        ttl: Duration,
    ) -> Arc<WorkdirRouter> {
        router_with_mirrors(root, bare, max, ttl, Vec::new())
    }

    fn router_with_mirrors(
        root: &Path,
        bare: &Path,
        max: usize,
        ttl: Duration,
        seed_urls: Vec<String>,
    ) -> Arc<WorkdirRouter> {
        let cfg = WorkdirConfig {
            workspaces_root: root.join("workspaces"),
            bare_repo: bare.to_path_buf(),
            target_dir: root.join("src").join("target"),
            seed_urls,
            ttl,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: max,
            warm_trees: 0,
            data_volumes: Vec::new(),
            data_volume_declaration_error: None,
            data_volume_interval: Duration::from_secs(30),
            build_cache: BuildCacheConfig::default(),
        };
        WorkdirRouter::new(cfg).unwrap()
    }

    /// Same router, with a warm pool of `warm` trees.
    fn router_with_warm(
        root: &Path,
        bare: &Path,
        max: usize,
        ttl: Duration,
        warm: usize,
    ) -> Arc<WorkdirRouter> {
        let cfg = WorkdirConfig {
            workspaces_root: root.join("workspaces"),
            bare_repo: bare.to_path_buf(),
            target_dir: root.join("src").join("target"),
            seed_urls: Vec::new(),
            ttl,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: max,
            warm_trees: warm,
            data_volumes: Vec::new(),
            data_volume_declaration_error: None,
            data_volume_interval: Duration::from_secs(30),
            build_cache: BuildCacheConfig::default(),
        };
        WorkdirRouter::new(cfg).unwrap()
    }

    /// Clone `bare`, add one commit on `main`, push it back, return the new head.
    fn advance_bare(bare: &Path, work: &Path) -> String {
        std::fs::create_dir_all(work).unwrap();
        let out = std::process::Command::new("git")
            .args(["clone", "-q"])
            .arg(bare)
            .arg(work)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(out.status.success(), "clone from {bare:?} failed");
        std::fs::write(work.join("ADVANCE"), "1\n").unwrap();
        git(&["add", "-A"], work);
        git(&["commit", "-qm", "advance"], work);
        git(&["push", "-q", "origin", "main"], work);
        git_out(work, &["rev-parse", "HEAD"])
    }

    #[tokio::test]
    async fn fetch_once_fails_over_to_the_next_mirror() {
        let tmp = tempfile::tempdir().unwrap();

        let mirror_root = tmp.path().join("mirror-b");
        let mirror = seed_bare(&mirror_root);
        let want = advance_bare(&mirror, &tmp.path().join("mirror-b-work"));

        let router_root = tmp.path().join("router");
        let bare = seed_bare(&router_root);
        let before = git_out(&bare, &["rev-parse", "main"]);

        let r = router_with_mirrors(
            &router_root,
            &bare,
            8,
            DEFAULT_TTL_SECS_FALLBACK,
            vec![
                tmp.path().join("missing-mirror.git").display().to_string(),
                mirror.display().to_string(),
            ],
        );

        r.fetch_once().await;

        assert_eq!(
            git_out(&bare, &["rev-parse", "main"]),
            want,
            "the reachable mirror must still advance the baseline"
        );
        assert_ne!(before, want);
        assert_eq!(
            r.metrics.fetch_failures.get(),
            0.0,
            "a mirror that answered is not a fetch failure"
        );
        assert_eq!(
            git_out(&bare, &["remote", "get-url", "upstream"]),
            mirror.display().to_string(),
            "the working mirror is kept for subsequent fetches"
        );
    }

    #[tokio::test]
    async fn route_creates_and_reuses_task_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);

        let (path, _) = r.route("task-1").await.unwrap();
        assert_eq!(path, tmp.path().join("workspaces").join("task-1"));
        assert!(path.join("README").exists());
        assert!(tmp.path().join("workspaces/.meta/task-1.json").exists());

        let (again, _) = r.route("task-1").await.unwrap();
        assert_eq!(again, path);
        assert_eq!(r.list_task_trees().await.len(), 1);
    }

    /// A new task takes a tree that was cut before it arrived. Without the
    /// pool this is a checkout of the whole repository on the first tool call
    /// of every conversation -- measured at 3.4 s for 49 MB in the executor.
    #[tokio::test]
    async fn a_new_task_is_handed_a_warm_tree_instead_of_cutting_one() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK, 2);

        r.warm_pool_once().await;
        let slots = r.warm_slots().await;
        assert_eq!(slots.len(), 2, "the pool fills ahead of demand");
        let taken = slots[0].clone();

        let (path, _) = r.route("warm-task").await.unwrap();
        assert!(
            path.join("README").exists(),
            "the handed tree is a real checkout"
        );
        assert_eq!(
            r.warm_slots().await.len(),
            1,
            "the tree came out of the pool instead of being cut beside it"
        );
        assert!(
            !taken.exists(),
            "the pool slot moved to the task path; a copy left behind is a checkout, not a hand-out"
        );
        assert!(
            r.list_task_trees()
                .await
                .iter()
                .any(|(id, p)| id == "warm-task" && p == &path),
            "the tree the task got is registered as its worktree"
        );
    }

    /// A tree that waited in the pool while `main` moved is re-pointed at
    /// hand-out: a task given a tree three commits behind would read the
    /// platform's own source wrong, and nothing on the surface would say why.
    #[tokio::test]
    async fn a_warm_tree_cut_before_main_moved_is_repointed_when_handed_out() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK, 1);

        r.warm_pool_once().await;
        assert_eq!(r.warm_slots().await.len(), 1);
        let moved_to = advance_bare(&bare, &tmp.path().join("advance-work"));

        let (path, _) = r.route("late-task").await.unwrap();
        assert_eq!(
            git_out(&path, &["rev-parse", "HEAD"]),
            moved_to,
            "the warm tree must be pointed at the main it is handed out under"
        );
        assert!(
            path.join("ADVANCE").exists(),
            "the commit main moved to is checked out, not just recorded as HEAD"
        );
    }

    /// A slot caught halfway through its checkout must not be handed out.
    /// `git worktree add` links `.git` before it checks anything out, so a
    /// tree with a `.git` and no checkout is the one shape that looks ready
    /// and is not: a task given it reads a repository whose files are all
    /// missing, and that reads as a broken platform rather than a race.
    #[tokio::test]
    async fn a_warm_slot_still_being_cut_is_not_handed_out() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK, 1);

        r.warm_pool_once().await;
        let slot = r.warm_slots().await.pop().expect("the pool filled");
        tokio::fs::remove_file(WorkdirRouter::ready_marker(&slot))
            .await
            .unwrap();
        assert!(
            slot.join(WORKTREE_LINK).exists(),
            "the precondition is a slot that still carries its git link"
        );
        assert!(
            r.warm_slots().await.is_empty(),
            "a slot with no finished checkout is not a tree to hand out"
        );

        let (path, _) = r.route("racing-task").await.unwrap();
        assert!(path.join("README").exists(), "the task got a whole tree");
        assert!(
            slot.exists(),
            "the unfinished slot was left where it was, not moved onto the task"
        );
    }

    /// 测试里的默认 TTL：从生产那个数算出来，别再写一遍——写两遍的六小时会漂。
    pub(crate) const DEFAULT_TTL_SECS_FALLBACK: Duration =
        Duration::from_secs(super::DEFAULT_TTL_SECS);

    #[tokio::test]
    async fn tasks_are_isolated_and_keep_their_own_files() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);

        let (a, _) = r.route("task-a").await.unwrap();
        let (b, _) = r.route("task-b").await.unwrap();
        std::fs::write(a.join("out.txt"), "from-a").unwrap();
        std::fs::write(b.join("out.txt"), "from-b").unwrap();

        assert_eq!(
            std::fs::read_to_string(a.join("out.txt")).unwrap(),
            "from-a"
        );
        assert_eq!(
            std::fs::read_to_string(b.join("out.txt")).unwrap(),
            "from-b"
        );
        assert_eq!(r.list_task_trees().await.len(), 2);
    }

    #[tokio::test]
    async fn invalid_id_is_error_not_shared_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        assert!(r.route("../evil").await.is_err());
        assert!(r.route(".meta").await.is_err());
        assert_eq!(r.list_task_trees().await.len(), 0);
    }

    #[tokio::test]
    async fn gc_reaps_idle_ttl_trees_and_sidecars() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, Duration::ZERO);
        // No command is running, so the claim `route` returns is dropped at
        // once — an idle tree, which is the only kind this reaps.
        let (path, _) = r.route("stale-task").await.unwrap();
        let meta = tmp.path().join("workspaces/.meta/stale-task.json");
        assert!(meta.exists());

        let n = r.gc_once().await.unwrap();
        assert_eq!(n, 1);
        assert!(!path.exists());
        assert!(!meta.exists());
        assert_eq!(r.list_task_trees().await.len(), 0);
    }

    #[tokio::test]
    async fn lru_cap_evicts_oldest_only() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK);
        r.route("old").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("mid").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("new").await.unwrap();

        let trees = r.list_task_trees().await;
        let ids: Vec<String> = trees.into_iter().map(|(id, _)| id).collect();
        assert!(ids.contains(&"mid".to_string()), "{ids:?}");
        assert!(ids.contains(&"new".to_string()), "{ids:?}");
        assert!(!ids.contains(&"old".to_string()));
    }

    /// A tree with a command running in it is in use, and the cap's own policy
    /// is least-recently-*used*: the oldest tree on the volume is not a
    /// candidate while its command runs. Deferring is counted, because a cap
    /// that could not be met and a cap that was never reached read the same.
    #[tokio::test]
    async fn lru_cap_defers_instead_of_deleting_a_tree_in_use() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK);
        let (running, in_use) = r.route("running").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("idle").await.unwrap();

        // The oldest tree is the one with the command in it, so the cap takes
        // the next oldest instead of the tree under the command.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("newest").await.unwrap();

        let ids: Vec<String> = r
            .list_task_trees()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(running.exists(), "the tree a command runs in must survive");
        assert!(ids.contains(&"running".to_string()), "{ids:?}");
        assert!(!ids.contains(&"idle".to_string()), "{ids:?}");
        assert_eq!(
            r.metrics.cap_deferred.get(),
            1.0,
            "a cap that could not be met has to be readable: {ids:?}"
        );

        // Once the command is over the tree is an ordinary tree again.
        drop(in_use);
        tokio::time::sleep(Duration::from_millis(300)).await;
        r.route("later").await.unwrap();
        assert_eq!(r.list_task_trees().await.len(), 2);
        assert_eq!(
            r.metrics.cap_deferred.get(),
            1.0,
            "a finished command leaves nothing to defer"
        );
    }

    /// A conversation shell is named after the agent and comes back on the next
    /// turn; a task tree is named after one round and never comes back. Under a
    /// plain LRU the older shell loses every time a pipeline round needs a
    /// slot, and the user's files lose with it.
    #[tokio::test]
    async fn a_standing_shell_outlives_the_task_trees_that_share_its_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK);
        // The shell is the oldest tree on the volume, so plain LRU takes it.
        r.route("unscoped-shell-vera-82191e4a").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("round-a").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("round-b").await.unwrap();

        let ids: Vec<String> = r
            .list_task_trees()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(
            ids.contains(&"unscoped-shell-vera-82191e4a".to_string()),
            "a conversation must keep the tree its files are in: {ids:?}"
        );
        assert!(ids.contains(&"round-b".to_string()), "{ids:?}");
        assert!(!ids.contains(&"round-a".to_string()), "{ids:?}");

        // The standing lane is a priority, not an exemption: with nothing else
        // left to reclaim, the cap still holds.
        let tmp2 = tempfile::tempdir().unwrap();
        let bare2 = seed_bare(tmp2.path());
        let r = router(tmp2.path(), &bare2, 1, DEFAULT_TTL_SECS_FALLBACK);
        r.route("unscoped-shell-old").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route("unscoped-shell-new").await.unwrap();
        let ids: Vec<String> = r
            .list_task_trees()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec!["unscoped-shell-new".to_string()]);
        assert_eq!(r.metrics.cap_deferred.get(), 0.0);
    }

    /// The idle TTL is a rule about finished work: a task tree's id is spent by
    /// its round, so an idle one is a tree nobody will ask for. A standing shell
    /// comes back by name, so the same idle hour says nothing — reaping it would
    /// delete the user's files between two conversations rather than after one.
    #[tokio::test]
    async fn the_idle_ttl_leaves_a_standing_shell_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, Duration::ZERO);
        let (shell, _) = r.route("unscoped-shell-vera-82191e4a").await.unwrap();
        let (round, _) = r.route("round-a").await.unwrap();
        // Stamped together so the arrival span is zero and the configured TTL
        // stands; the derived one is floored at an hour and would hide the rule.
        let together = unix_now();
        for id in ["unscoped-shell-vera-82191e4a", "round-a"] {
            let mut meta = r.read_meta(id).expect("a routed tree has a sidecar");
            meta.created_at_unix = together;
            meta.last_used_unix = together;
            r.write_meta(&meta).await.unwrap();
        }

        assert_eq!(
            r.gc_once().await.unwrap(),
            1,
            "only the spent round is idle work"
        );
        assert!(
            shell.exists(),
            "a conversation keeps the files it wrote last time"
        );
        assert!(!round.exists(), "a finished round's tree is still reaped");
        assert_eq!(r.list_task_trees().await.len(), 1);
    }

    /// The idle rule reaps task trees, so the hold time it derives has to come
    /// from their arrivals. A conversation that has been open for a week would
    /// otherwise stretch the span by that week and publish a hold time the rule
    /// never applied — a number that reads as "idle rounds are kept for weeks".
    #[tokio::test]
    async fn the_derived_ttl_is_measured_over_the_task_lane() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        r.route("unscoped-shell-vera-82191e4a").await.unwrap();
        for id in ["round-a", "round-b", "round-c", "round-d"] {
            r.route(id).await.unwrap();
        }
        let week_ago = unix_now() - 7 * 86_400;
        let hour_ago = unix_now() - 3_600;
        for id in [
            "unscoped-shell-vera-82191e4a",
            "round-a",
            "round-b",
            "round-c",
            "round-d",
        ] {
            let mut meta = r.read_meta(id).expect("a routed tree has a sidecar");
            meta.created_at_unix = if is_standing(id) { week_ago } else { hour_ago };
            meta.last_used_unix = meta.created_at_unix;
            r.write_meta(&meta).await.unwrap();
        }
        r.gc_once().await.unwrap();

        // Four rounds arriving within an hour into a pool of eight: half a
        // pool of arrivals is 8 * (3600 / 4) / 2 = 3600 s. Counting the week-old
        // conversation among them would derive 15 days instead.
        let published = r.metrics.ttl_seconds.seconds.get();
        assert!(
            published < 2.0 * 86_400.0,
            "the hold time must come from the rounds, not the conversation: {published}"
        );
        assert!(
            r.list_task_trees()
                .await
                .iter()
                .all(|(id, _)| is_standing(id)),
            "an hour of idleness is past a hold time derived from hourly arrivals"
        );

        fn is_standing(id: &str) -> bool {
            id.starts_with("unscoped-shell-")
        }
    }

    /// A registration a killed add left locked is invisible to both halves of
    /// removal: `worktree remove --force` refuses it, so the directory goes by
    /// `remove_dir_all` while the registration stays, and `worktree prune`
    /// skips it. The cap goes on counting a tree that is not on the volume, so
    /// it can never be met — the live volume on 2026-11-06 held 21 of these
    /// against 7 trees, and the pass that could not converge is what evicted
    /// the tree a conversation had just used.
    #[tokio::test]
    async fn the_cap_removes_a_locked_tree_instead_of_counting_it_forever() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 1, DEFAULT_TTL_SECS_FALLBACK);
        let (old, claim) = r.route("locked-old").await.unwrap();
        drop(claim);
        git(
            &[
                "worktree",
                "lock",
                "--reason",
                "initializing",
                old.to_str().unwrap(),
            ],
            &bare,
        );
        tokio::time::sleep(Duration::from_millis(1100)).await;

        r.route("newest").await.unwrap();

        assert!(!old.exists(), "the cap was asked to make room for one tree");
        let ids: Vec<String> = r
            .list_task_trees()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(
            !ids.contains(&"locked-old".to_string()),
            "a removed tree must leave the count with it: {ids:?}"
        );
        assert_eq!(r.metrics.cap_deferred.get(), 0.0, "{ids:?}");
    }

    /// The shape the leak actually takes: the process that created the tree
    /// died after the directory was deleted and before the registration went,
    /// so `git worktree unlock` — which names the tree by path — cannot reach
    /// it any more. Boot has to drop it, or every later pass counts it.
    #[tokio::test]
    async fn boot_drops_a_locked_registration_whose_tree_is_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (path, claim) = r.route("killed-add").await.unwrap();
        drop(claim);
        git(
            &[
                "worktree",
                "lock",
                "--reason",
                "initializing",
                path.to_str().unwrap(),
            ],
            &bare,
        );
        std::fs::remove_dir_all(&path).unwrap();
        assert!(
            r.list_task_trees()
                .await
                .iter()
                .any(|(id, _)| id == "killed-add"),
            "the registration is what the leak leaves behind"
        );

        r.recover().await.unwrap();

        let ids: Vec<String> = r
            .list_task_trees()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(
            !ids.contains(&"killed-add".to_string()),
            "a tree the volume does not hold must not count against the cap: {ids:?}"
        );
    }

    /// The TTL rule asks the same question as the cap: a tree idle past the TTL
    /// is not idle if a command is running in it.
    ///
    /// What this pins is the in-use exception, not how the TTL is derived. The
    /// pass prefers a TTL derived from how fast trees arrive over the one
    /// configured, and arrival is stamped in whole seconds: two trees whose
    /// creation straddles a second boundary give a span of two, which derives
    /// `MIN_TTL_SECS` and reaps nothing at all. That made this assertion fail
    /// on CI on 2026-11-03 (reaped 0 where it asserts 1) on a runner slow
    /// enough for the two routes below to land in different seconds. So the
    /// premise this test needs — "the configured TTL is the one in force" — is
    /// written down instead of hoped for: both trees are stamped as arriving at
    /// the same instant, which is what makes the derivation decline to answer.
    #[tokio::test]
    async fn ttl_reaps_idle_trees_but_not_one_in_use() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, Duration::ZERO);
        let (running, _in_use) = r.route("running").await.unwrap();
        let (idle, _) = r.route("idle").await.unwrap();
        let arriving_together = unix_now();
        for id in ["running", "idle"] {
            let mut meta = r.read_meta(id).expect("a routed tree has a sidecar");
            meta.created_at_unix = arriving_together;
            meta.last_used_unix = arriving_together;
            r.write_meta(&meta).await.unwrap();
        }

        assert_eq!(
            r.gc_once().await.unwrap(),
            1,
            "the idle tree is the one the TTL rule reaps"
        );
        assert!(
            running.exists(),
            "a tree with a command in it is not idle, whatever its stamp says"
        );
        assert!(!idle.exists(), "an idle tree past the TTL is reaped");
    }

    /// The sidecar is published whole, so a reader never catches it empty.
    ///
    /// Nothing synchronises a reader of a sidecar with its writer: a reclaim
    /// pass, a cap ranking and another writer's read-modify-write all read it
    /// with no lock. A write that empties the file first leaves a window in
    /// which the parse fails and the tree reads as having no sidecar at all —
    /// and the reclaim rule for that tree is to adopt it, so the fault reads as
    /// a stale tree kept alive. This holds a writer and a reader against one
    /// tree and fails on the first read that comes back empty.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sidecar_is_replaced_whole_so_a_concurrent_reader_never_sees_none() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        r.route("reader").await.unwrap();

        let writer_router = Arc::clone(&r);
        let writer = tokio::spawn(async move {
            for stamp in 1..=200i64 {
                writer_router
                    .write_meta(&TaskMeta {
                        task_id: "reader".to_string(),
                        created_at_unix: 1,
                        last_used_unix: stamp,
                    })
                    .await
                    .unwrap();
            }
        });

        let mut reads = 0u64;
        loop {
            let seen = r.read_meta("reader");
            reads += 1;
            assert!(
                seen.is_some(),
                "read {reads} caught the sidecar mid-publish: the file a reader sees must always \
                 be one whole version, never a truncated one"
            );
            if writer.is_finished() {
                break;
            }
        }
        writer.await.unwrap();
        assert!(reads > 1, "the writer finished before the reader ran once");
    }

    /// A command ending is the tree's last use, and the sidecar is what the next
    /// cap pass ranks it by. Without that stamp a build that just ran for ten
    /// minutes reads as the oldest tree on the volume the moment it finishes —
    /// exactly when its artifacts are about to be read.
    #[tokio::test]
    async fn a_command_ending_stamps_the_tree_as_last_used() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (_, in_use) = r.route("built").await.unwrap();
        let before = r.read_meta("built").unwrap().last_used_unix;

        // The stamp has second granularity, so a command has to outlive the
        // second it started in for the difference to exist at all.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        drop(in_use);

        // The stamp is written by a task of its own, so it is waited for rather
        // than assumed: the claim is that it happens, not that it happens by
        // some particular instant.
        let mut after = before;
        for _ in 0..40 {
            after = r.read_meta("built").unwrap().last_used_unix;
            if after > before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            after > before,
            "the end of a command must be stamped on the sidecar: {before} -> {after}"
        );
    }

    /// Rebuilding a tree means removing it first. A tree that stopped looking
    /// healthy *and* has a command running in it is the one case where the
    /// caller has to come back: the command cannot.
    #[tokio::test]
    async fn a_tree_in_use_is_not_rebuilt_under_its_command() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (path, _in_use) = r.route("busy").await.unwrap();

        // The tree stops looking like a worktree while a command runs in it.
        std::fs::remove_file(path.join(WORKTREE_LINK)).unwrap();

        let err = match r.route("busy").await {
            Ok(_) => panic!("rebuilding a tree in use must be refused"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("in use"), "{err}");
        assert!(
            path.exists(),
            "the tree a command runs in is not removed and rebuilt"
        );
    }

    #[tokio::test]
    async fn restart_reconciles_trees_and_sidecars() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r1 = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (path, _) = r1.route("task-survive").await.unwrap();

        // Simulate a lost sidecar (tree still registered): recover adopts it.
        std::fs::remove_file(tmp.path().join("workspaces/.meta/task-survive.json")).unwrap();
        r1.recover().await.unwrap();
        assert!(tmp
            .path()
            .join("workspaces/.meta/task-survive.json")
            .exists());

        // A brand-new router process over the same PVC routes to the same tree.
        let r2 = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        r2.recover().await.unwrap();
        assert_eq!(r2.route("task-survive").await.unwrap().0, path);

        // Orphan sidecar without a tree is dropped during recovery.
        std::fs::write(tmp.path().join("workspaces/.meta/ghost.json"), "{}").unwrap();
        r2.recover().await.unwrap();
        assert!(!tmp.path().join("workspaces/.meta/ghost.json").exists());
    }

    #[tokio::test]
    async fn stale_git_lock_is_cleared_and_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (path, _) = r.route("lock-task").await.unwrap();
        let gitdir = PathBuf::from(git_out(&path, &["rev-parse", "--absolute-git-dir"]));
        let lock = gitdir.join("index.lock");
        std::fs::write(&lock, "").unwrap();
        // Backdate mtime past the 600s stale threshold.
        let old = std::time::SystemTime::now() - Duration::from_secs(900);
        let times = std::fs::FileTimes::new()
            .set_accessed(old)
            .set_modified(old);
        std::fs::File::open(&lock)
            .unwrap()
            .set_times(times)
            .unwrap();

        // run_git self-heals the stale lock and the retry succeeds.
        r.run_git(&["reset", "--hard", "HEAD"], Some(&path))
            .await
            .unwrap();
        assert!(!lock.exists());
    }

    /// The cap is a reading, not a setting: it says what this volume holds at
    /// the size these trees measure, and it moves when either input moves.
    #[test]
    fn the_worktree_cap_follows_the_volume_and_the_measured_tree_size() {
        const MIB: u64 = 1024 * 1024;
        let disk_with = |avail_mib: u64| DiskReading {
            total_bytes: 5 * 1024 * MIB,
            avail_bytes: avail_mib * MIB,
        };
        // Fixtures picked for the test, not a machine: e.g. four trees of
        // e.g. 100 MiB each on a volume with e.g. 4 GiB free. One tree's worth
        // is reserved so the next one can still be cut; what is left buys the
        // rest of the trees at that size. A share of the volume instead --
        // e.g. a fifteenth -- would hold back a figure nobody measured a tree
        // for, and answer one less.
        assert_eq!(workspace_cap(disk_with(4096), 4 * 100 * MIB, 4), Some(43));
        // The same four trees, with the volume nearly full: the cap is smaller
        // than the number of trees that already exist, so the next pass
        // reclaims rather than grows.
        assert_eq!(workspace_cap(disk_with(300), 4 * 100 * MIB, 4), Some(6));
        // Big trees on a nearly full volume: a fifteenth of what is free is
        // less than one tree, so it would let one more be cut into room that a
        // single tree then fills.
        assert_eq!(workspace_cap(disk_with(500), 4 * 400 * MIB, 4), Some(4));
        // Trees that grew (the repository added a large file) shrink it too.
        assert_eq!(workspace_cap(disk_with(4096), 4 * 400 * MIB, 4), Some(13));
        // Nothing measured yet: no per-tree figure, so no invented cap.
        assert_eq!(workspace_cap(disk_with(4096), 0, 0), None);
        assert_eq!(workspace_cap(disk_with(4096), 0, 4), None);
    }

    /// Polling is paid for by how often the thing being polled changes.
    #[test]
    fn the_fetch_period_follows_how_long_main_was_still() {
        let configured = Duration::from_secs(300);
        // Nothing observed yet: the configured period is all there is.
        assert_eq!(fetch_interval_from(0, configured), configured);
        // A head that moves every 30 minutes is polled every 15, not every 5.
        assert_eq!(
            fetch_interval_from(1800, configured),
            Duration::from_secs(900)
        );
        // A busy repository is polled no faster than a push can arrive.
        assert_eq!(
            fetch_interval_from(10, configured),
            Duration::from_secs(MIN_FETCH_INTERVAL_SECS)
        );
    }

    /// The same law paces the cache walk, at its own floor: a walk that costs
    /// N seconds buys twenty times that period, never less than the shared
    /// floor. Both loops take their multiple from one constant, so tuning the
    /// share tunes both rather than letting one drift.
    #[test]
    fn the_cache_walk_interval_follows_the_slowest_pass() {
        let configured = Duration::from_secs(300);
        let walk = |pass_millis: u64| {
            paced_interval(
                pass_millis,
                configured,
                cog_core::build_cache::MIN_SCAN_INTERVAL_SECS,
                MAINTENANCE_OVERHEAD_MULTIPLE,
            )
        };
        // Before any walk has been timed the configured interval stands.
        assert_eq!(walk(0), configured);
        // A ten-second walk over a large cache buys 200s, under the 300s seed:
        // the seed was chosen for a cache that walked faster than this.
        assert_eq!(walk(10_000), Duration::from_secs(200));
        // A walk faster than the floor warrants is served at the floor.
        assert_eq!(
            walk(500),
            Duration::from_secs(cog_core::build_cache::MIN_SCAN_INTERVAL_SECS)
        );
        // The share is shared with reclamation, not restated for this loop.
        assert_eq!(
            gc_interval_from(10_000, configured),
            paced_interval(
                10_000,
                configured,
                MIN_GC_INTERVAL_SECS,
                MAINTENANCE_OVERHEAD_MULTIPLE
            )
        );
    }

    /// The walk over the executor's own volumes costs I/O on the same disk the
    /// tasks build on, so its period is the configured interval as a floor and
    /// a share of the walk above it -- and the published reading says which of
    /// the three inputs set the number in force.
    #[test]
    fn the_volume_walk_never_runs_faster_than_the_chart_and_slows_with_its_cost() {
        let configured = Duration::from_secs(300);
        // Nothing walked yet: the configured interval is all there is.
        assert_eq!(volume_walk_interval_from(0, configured), configured);
        // A one-second walk buys twenty seconds, under the floor: the floor is
        // what is in force, and `period_source` says so rather than crediting
        // the measurement.
        assert_eq!(volume_walk_interval_from(1_000, configured), configured);
        assert_eq!(
            period_source(
                1_000,
                scaled(1_000, MAINTENANCE_OVERHEAD_MULTIPLE),
                configured
            ),
            "floor"
        );
        // A twenty-second walk buys 400s, above the floor: now the measurement
        // is what set the period.
        assert_eq!(
            volume_walk_interval_from(20_000, configured),
            Duration::from_secs(400)
        );
        assert_eq!(
            period_source(
                20_000,
                scaled(20_000, MAINTENANCE_OVERHEAD_MULTIPLE),
                configured
            ),
            "measured"
        );
        // Nothing timed: configuration.
        assert_eq!(period_source(0, Duration::ZERO, configured), "configured");
        // Ten times the walk, ten times the period: the share is held constant.
        assert_eq!(
            volume_walk_interval_from(200_000, configured),
            Duration::from_secs(4000)
        );
    }

    /// Every published series of one metric in a rendered scrape, as
    /// `(the series' `source` label or the empty string, sample value)`.
    fn published(rendered: &str, metric: &str) -> Vec<(String, f64)> {
        let labelled = format!("{metric}{{");
        let bare = format!("{metric} ");
        let mut out = Vec::new();
        for line in rendered.lines() {
            let (source, value) = if let Some(rest) = line.strip_prefix(&labelled) {
                let source = rest.split('"').nth(1).unwrap_or_default().to_string();
                (source, rest)
            } else if let Some(rest) = line.strip_prefix(&bare) {
                (String::new(), rest)
            } else {
                continue;
            };
            let value: f64 = value
                .rsplit(' ')
                .next()
                .unwrap_or_default()
                .parse()
                .unwrap_or(f64::NAN);
            out.push((source, value));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The source family a period's name derives to, by the same rule
    /// [`PeriodGauge::new`] registers it under.
    fn source_metric(metric: &str) -> String {
        format!(
            "{}_source",
            metric.strip_suffix("_seconds").unwrap_or(metric)
        )
    }

    /// The loop itself uses that law and publishes the interval it is keeping:
    /// a walk that just ran is not due again, and the reading moves from the
    /// configured number to the measured one.
    #[test]
    fn the_volume_walk_publishes_the_interval_it_keeps() {
        let tmp = tempfile::tempdir().unwrap();
        let r = router(
            tmp.path(),
            &tmp.path().join("repo.git"),
            8,
            Duration::from_secs(3600),
        );
        // Nothing walked yet: due at once, and the reading says why.
        assert!(r.volume_walk_due());
        let first = r.metrics().render();
        assert_eq!(
            published(&first, VOLUME_WALK_INTERVAL_METRIC),
            vec![(String::new(), 30.0)],
            "{first}"
        );
        assert_eq!(
            published(&first, &source_metric(VOLUME_WALK_INTERVAL_METRIC)),
            vec![
                ("configured".to_string(), 1.0),
                ("floor".to_string(), 0.0),
                ("measured".to_string(), 0.0),
            ],
            "{first}"
        );

        // A walk that took twenty seconds buys 400s at the configured 30s
        // floor, so the next one is not due and the reading says measured.
        r.note_volume_walk(20_000);
        assert!(
            !r.volume_walk_due(),
            "a walk that just finished read as due again"
        );
        let after = r.metrics().render();
        assert_eq!(
            published(&after, VOLUME_WALK_INTERVAL_METRIC),
            vec![(String::new(), 400.0)],
            "{after}"
        );
        assert_eq!(
            published(&after, &source_metric(VOLUME_WALK_INTERVAL_METRIC)),
            vec![
                ("configured".to_string(), 0.0),
                ("floor".to_string(), 0.0),
                ("measured".to_string(), 1.0),
            ],
            "{after}"
        );
    }

    /// A period whose source moved must not leave the old one published. When
    /// the state rode on the number as a label, every source the loop had ever
    /// used stayed in the scrape at its last value -- measured on the cluster
    /// as `sandbox_volume_walk_interval_seconds{source="configured"} 300` next
    /// to `{source="floor"} 300`, two numbers both claiming to be the wait in
    /// force and nothing saying which was current. One number in force, one
    /// one-hot family, nothing left behind.
    #[test]
    fn a_period_that_moved_leaves_one_number_and_no_stale_state() {
        let tmp = tempfile::tempdir().unwrap();
        let r = router(
            tmp.path(),
            &tmp.path().join("repo.git"),
            8,
            Duration::from_secs(3600),
        );
        let sources = source_metric(VOLUME_WALK_INTERVAL_METRIC);
        // Configured, then floor, then measured: all three states in turn, so a
        // stale series would have had two chances to appear. Each call publishes
        // before it decides, so a pass that is not due still sets the reading.
        assert!(r.volume_walk_due());
        // A 1-second walk buys 20s, under the configured 30s floor.
        r.note_volume_walk(1_000);
        assert!(
            !r.volume_walk_due(),
            "a walk that just ran came due at once"
        );
        // A 20-second walk buys 400s, above it.
        r.note_volume_walk(20_000);
        assert!(!r.volume_walk_due(), "a 400s walk came due after 30s");

        let rendered = r.metrics().render();
        let numbers = published(&rendered, VOLUME_WALK_INTERVAL_METRIC);
        assert_eq!(
            numbers.len(),
            1,
            "one period is in force at a time, so one series has to publish it: {rendered}"
        );
        assert_eq!(numbers[0].1, 400.0, "{rendered}");

        let states = published(&rendered, &sources);
        assert_eq!(
            states.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>(),
            vec!["configured", "floor", "measured"],
            "all three sources are published on every scrape, so a reader does not \
             have to guess a missing one: {rendered}"
        );
        assert_eq!(
            states.iter().filter(|(_, v)| *v == 1.0).count(),
            1,
            "exactly the source that chose the period in force is set: {rendered}"
        );
        assert_eq!(
            states
                .iter()
                .find(|(s, _)| s == "measured")
                .map(|(_, v)| *v),
            Some(1.0),
            "{rendered}"
        );
    }

    /// Reclamation's own cost is what the interval covers, so a volume that
    /// fills and makes the pass slower moves the interval with it instead of
    /// letting the loop's share of the process grow. The pass is timed as the
    /// work alone -- the waiting it is scheduled by is not part of it -- so
    /// there is no path from the interval back into the measurement that sets
    /// it.
    #[test]
    fn the_gc_interval_follows_the_slowest_pass_not_a_written_period() {
        let configured = Duration::from_secs(600);
        // No pass timed yet: the configured interval is all there is.
        assert_eq!(gc_interval_from(0, configured), configured);
        // A pass that took one second gets a twenty-second interval, which the
        // floor raises to the pace the loop wakes at.
        assert_eq!(
            gc_interval_from(1_000, configured),
            Duration::from_secs(MIN_GC_INTERVAL_SECS)
        );
        // A pass that took forty seconds -- a full volume, many trees -- gets
        // twenty times that, so reclamation stays at one twentieth of the time.
        assert_eq!(
            gc_interval_from(40_000, configured),
            Duration::from_secs(800)
        );
        // Ten times the pass, ten times the interval: the share is what is
        // held constant, not the period.
        assert_eq!(
            gc_interval_from(400_000, configured),
            Duration::from_secs(8000)
        );
    }

    /// The TTL is set by how fast trees arrive and how many fit -- never by how
    /// long trees have sat idle, which is the one input that cannot be observed
    /// independently of the TTL itself.
    #[test]
    fn the_idle_ttl_follows_arrivals_and_capacity_not_reuse_gaps() {
        // Ten trees over ten hours is one an hour; a volume holding 20 takes
        // twenty hours to refill, and half of that is the hold.
        assert_eq!(
            ttl_from(20, 10, 10 * 3600),
            Some(Duration::from_secs(10 * 3600))
        );
        // Twice the arrivals in the same span: the same volume holds half as
        // much time.
        assert_eq!(
            ttl_from(20, 20, 10 * 3600),
            Some(Duration::from_secs(5 * 3600))
        );
        // A bigger volume holds longer.
        assert!(ttl_from(40, 10, 10 * 3600) > ttl_from(20, 10, 10 * 3600));
        // Never below the floor, and never invented from one tree.
        assert_eq!(
            ttl_from(1, 10, 10 * 3600),
            Some(Duration::from_secs(MIN_TTL_SECS))
        );
        assert_eq!(ttl_from(20, 1, 10 * 3600), None);
        assert_eq!(ttl_from(20, 10, 0), None);
    }

    #[test]
    fn the_df_row_is_read_as_bytes() {
        let out = "Filesystem     1B-blocks        Used   Available Capacity Mounted on\n\
                   /dev/sda1    5368709120  1073741824  4294967296      20% /data\n";
        let disk = parse_df(out).unwrap();
        assert_eq!(disk.total_bytes, 5_368_709_120);
        assert_eq!(disk.avail_bytes, 4_294_967_296);
        // A failed `df` prints a diagnostic instead of a row.
        assert_eq!(parse_df("df: /nope: No such file or directory\n"), None);
    }
}
