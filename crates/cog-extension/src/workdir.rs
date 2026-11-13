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

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
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
// cap: bound — seed when the deployment names none; the volume's own reading replaces it as soon as one tree exists
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
// cap: bound — seed; applies until an arrival rate and a checkout cost have both been measured
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

/// Maximum lane length, keeps the on-disk directory name clear of
/// `ENAMETOOLONG` while accommodating generated ids such as
/// `<owner-task>-ralph-roundtable-i12-<8 hex>`.
///
/// Taken from `cog-core` rather than chosen here: the lane name is derived
/// there and validated here, and a second, tighter ceiling would reject exactly
/// the long ids that derivation promised a checkout -- with no error, since an
/// invalid lane reads as a caller that named none.
const MAX_TASK_ID_LEN: usize = cog_core::MAX_CHECKOUT_NAME_CHARS;

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

/// How many task trees the volume is measured to hold, and the input set that
/// chose it. The deployment's number is only the seed: before a tree exists
/// there is no per-tree figure to divide the volume by, and after one exists
/// the volume's own reading is the cap.
const WORKSPACE_CAP_METRIC: &str = "sandbox_workspace_cap";

/// How many times the cap has been recomputed. The number itself only moves
/// when the volume or the trees do; this shows the pass that reads them is
/// running.
const WORKSPACE_CAP_EVALUATIONS_METRIC: &str = "sandbox_workspace_cap_evaluations_total";

/// The input sets [`WORKSPACE_CAP_METRIC`] can name: the deployment's seed
/// before any tree has been measured, the volume's reading after.
const CAP_SOURCES: [&str; 2] = ["seed", "measured"];

/// The number of trees the pool is keeping ready, and the input set that chose
/// it. The seed is what the deployment names; the measurement is an arrival
/// rate times a checkout cost. Without this series the seed is the only number
/// anyone can see, and a pool that never left it looks the same as a pool whose
/// law happens to agree with it.
const WARM_TREES_METRIC: &str = "sandbox_workspace_warm_trees";

/// How many times the pool's target has been recomputed. A value that only
/// changes when its inputs do cannot show that the loop is running; this can.
const WARM_TREES_EVALUATIONS_METRIC: &str = "sandbox_workspace_warm_trees_evaluations_total";

/// The input sets [`WARM_TREES_METRIC`] can name: the deployment's seed before
/// both measurements exist, the measurement after.
const WARM_TREES_SOURCES: [&str; 2] = ["seed", "measured"];

/// The window arrival samples are kept over. Longer than any working day, so a
/// burst is not averaged away by the quiet hours around it.
const ARRIVAL_WINDOW_SECS: i64 = 7 * 24 * 3600;

/// How many arrival timestamps to keep. A cap rather than a promise: the window
/// above already bounds the queue for realistic rates, and this bounds it for
/// a pathological one.
const MAX_ARRIVAL_SAMPLES: usize = 1024;

/// How many checkout durations to keep for the median. Small on purpose: the
/// cost follows the repository's size, which changes over days, not seconds.
const CHECKOUT_SAMPLES: usize = 32;

/// How many ready trees are worth holding: Little's law over the two costs the
/// pool trades against each other.
///
/// An arrival every `span / arrivals` seconds, each costing `checkout_secs` to
/// serve, means that many arrivals are in flight while one tree is being cut.
/// Holding exactly that many removes the checkout from the request path without
/// spending the volume on trees no one has asked for. The ceiling is half the
/// task capacity, because the pool exists to make arrivals cheap and spending
/// the whole volume on trees no task has asked for would make the tasks
/// themselves wait.
///
/// `None` until both a rate and a cost have been measured: a number invented
/// from one measurement is the configured value wearing a measurement's
/// clothes.
fn warm_trees_from(
    arrivals: usize,
    span_secs: u64,
    checkout_secs: f64,
    ceiling: usize,
) -> Option<usize> {
    if arrivals < 2 || span_secs == 0 || checkout_secs <= 0.0 {
        return None;
    }
    if ceiling == 0 {
        return Some(0);
    }
    let per_second = arrivals as f64 / span_secs as f64;
    let in_flight = (per_second * checkout_secs).ceil() as usize;
    Some(in_flight.clamp(1, ceiling))
}

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
///
/// The arithmetic is `cog_core::pacing`, the one copy every self-paced loop in
/// the workspace uses; this wrapper only adapts milliseconds and a whole-number
/// share to it.
fn paced_interval(
    pass_millis: u64,
    configured: Duration,
    floor_secs: u64,
    multiple: u64,
) -> Duration {
    cog_core::pacing::paced_interval(
        Duration::from_millis(pass_millis),
        configured,
        Duration::from_secs(floor_secs),
        multiple as f64,
    )
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
    /// The cap's seed: how many trees this executor is meant to serve before
    /// the volume has been measured. Once a tree exists its own size and the
    /// volume's free room set the cap, and this stops applying.
    pub max_workspaces: usize,
    /// A fixture volume of a known capacity, for tests that need a ceiling
    /// smaller than a test host's disk. Production never sets it: the cap is
    /// what the volume measures.
    #[cfg(test)]
    pub cap_override: Option<usize>,
    /// How many trees to keep cut and ready under [`WARM_DIR_NAME`], before an
    /// arrival rate and a checkout cost have both been measured. The measured
    /// target is clamped to half the task capacity: the pool exists to make
    /// arrivals cheap, and spending the capacity on trees no task has asked
    /// for would make the tasks themselves wait.
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
            .unwrap_or(DEFAULT_WARM_TREES);
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
            #[cfg(test)]
            cap_override: None,
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

/// What a tree's sidecar records about it.
///
/// `task_id` holds the lane the tree belongs to. The field kept its name so the
/// sidecars on a running volume still read back: a rename would make every
/// existing tree look like a registration without a record, and the boot pass
/// would adopt each one as if its sidecar had been lost.
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
    /// The task-tree cap in force, and the input set that chose it.
    cap: Gauge,
    cap_source: GaugeVec,
    cap_evaluations: Counter,
    /// The warm pool's target in force, and the input set that chose it.
    warm_trees: Gauge,
    warm_trees_source: GaugeVec,
    warm_trees_evaluations: Counter,
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
        let cap = Gauge::new(
            WORKSPACE_CAP_METRIC,
            "Task worktrees the volume is measured to hold at the size these trees are",
        )?;
        let cap_source = GaugeVec::new(
            Opts::new(
                format!("{WORKSPACE_CAP_METRIC}_source"),
                "Which input set chose the task-tree cap: 1 for that one, 0 for the other",
            ),
            &[SOURCE_LABEL],
        )?;
        let cap_evaluations = Counter::new(
            WORKSPACE_CAP_EVALUATIONS_METRIC,
            "Recomputations of the task-tree cap from the volume's reading",
        )?;
        registry.register(Box::new(cap.clone()))?;
        registry.register(Box::new(cap_source.clone()))?;
        registry.register(Box::new(cap_evaluations.clone()))?;
        cap.set(cfg.max_workspaces as f64);
        for candidate in CAP_SOURCES {
            cap_source
                .with_label_values(&[candidate])
                .set(if candidate == "seed" { 1.0 } else { 0.0 });
        }
        let warm_trees = Gauge::new(
            WARM_TREES_METRIC,
            "Trees the warm pool is keeping cut and ready, chosen from an arrival rate and a checkout cost",
        )?;
        let warm_trees_source = GaugeVec::new(
            Opts::new(
                format!("{WARM_TREES_METRIC}_source"),
                "Which input set chose the pool target: 1 for that one, 0 for the other",
            ),
            &[SOURCE_LABEL],
        )?;
        let warm_trees_evaluations = Counter::new(
            WARM_TREES_EVALUATIONS_METRIC,
            "Recomputations of the warm pool target by the reclamation pass",
        )?;
        registry.register(Box::new(warm_trees.clone()))?;
        registry.register(Box::new(warm_trees_source.clone()))?;
        registry.register(Box::new(warm_trees_evaluations.clone()))?;
        // Registered at the seed rather than at zero: zero would read as a
        // pool that has been turned off, which is a decision the measurement
        // has not made yet.
        for candidate in WARM_TREES_SOURCES {
            warm_trees_source
                .with_label_values(&[candidate])
                .set(if candidate == "seed" { 1.0 } else { 0.0 });
        }
        warm_trees.set(cfg.warm_trees as f64);
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
            cap,
            cap_source,
            cap_evaluations,
            warm_trees,
            warm_trees_source,
            warm_trees_evaluations,
        })
    }

    /// Publish the task-tree cap in force and the one input set that chose it.
    fn set_cap(&self, trees: usize, source: &str) {
        self.cap.set(trees as f64);
        for candidate in CAP_SOURCES {
            self.cap_source
                .with_label_values(&[candidate])
                .set(if candidate == source { 1.0 } else { 0.0 });
        }
    }

    /// Publish the pool target in force and the one input set that chose it.
    fn set_warm_trees(&self, trees: usize, source: &str) {
        self.warm_trees.set(trees as f64);
        for candidate in WARM_TREES_SOURCES {
            self.warm_trees_source
                .with_label_values(&[candidate])
                .set(if candidate == source { 1.0 } else { 0.0 });
        }
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
    /// How many ready trees the pool should hold right now. Seeded from the
    /// deployment, then recomputed by every reclamation pass from the arrival
    /// rate and the checkout cost it measures.
    warm_target: AtomicUsize,
    /// When lanes last arrived, most recent last. The rate is counted over the
    /// span these cover, so a burst is visible while it happens rather than
    /// averaged away against the process's whole life.
    arrivals: std::sync::Mutex<VecDeque<i64>>,
    /// The last few successful checkout durations, in milliseconds. A checkout
    /// is the cost the pool removes from the request path, and it is what the
    /// rate is multiplied by.
    checkout_millis: std::sync::Mutex<VecDeque<u64>>,
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
        let warm_seed = cfg.warm_trees;
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
            gc_pass_millis: AtomicU64::new(0),
            last_gc_unix: AtomicI64::new(0),
            cache_pass_millis: AtomicU64::new(0),
            last_cache_walk_unix: AtomicI64::new(0),
            volume_pass_millis: AtomicU64::new(0),
            last_volume_walk_unix: AtomicI64::new(0),
            warm_target: AtomicUsize::new(warm_seed),
            arrivals: std::sync::Mutex::new(VecDeque::new()),
            checkout_millis: std::sync::Mutex::new(VecDeque::new()),
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

    /// Resolve the working directory for a lane and claim it for the command
    /// about to run there, lazily creating the tree on first sight.
    ///
    /// A lane is an employee's checkout (see [`worktree_for_agent`]); a bare
    /// task id still routes, for a caller that named no employee, so an old
    /// entry point keeps landing somewhere rather than nowhere.
    ///
    /// The claim is taken before the first filesystem call: a reclamation pass
    /// that has already listed this tree must still see it in use when it
    /// reaches its own decision, and the only moment a claim can protect a tree
    /// is before anything starts using it. Invalid lanes and add failures
    /// surface as errors instead of silently collapsing onto a shared directory.
    pub async fn route(self: &Arc<Self>, lane: &str) -> SFResult<(PathBuf, WorktreeUse)> {
        validate_task_id(lane)?;
        let use_guard = self.claim(lane);
        let (path, took_warm) = self.route_validated(lane, true).await?;
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

    /// Record that a lane just got a tree, so the pool's target follows how
    /// fast lanes actually arrive. Counted for a warm hand-out too: the pool
    /// still has to replace the tree that was handed over, and the rate that
    /// matters is arrivals, not checkouts.
    fn note_arrival(&self) {
        let now = unix_now();
        let mut arrivals = self
            .arrivals
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        arrivals.push_back(now);
        while arrivals
            .front()
            .is_some_and(|first| now.saturating_sub(*first) > ARRIVAL_WINDOW_SECS)
        {
            arrivals.pop_front();
        }
        while arrivals.len() > MAX_ARRIVAL_SAMPLES {
            arrivals.pop_front();
        }
    }

    /// Record how long a successful checkout took. The median of the recent
    /// samples is the cost the pool removes; the median rather than the mean so
    /// one stalled `git worktree add` does not set the pool's size for as long
    /// as it stays in the window.
    fn note_checkout(&self, millis: u64) {
        let mut samples = self
            .checkout_millis
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        samples.push_back(millis);
        while samples.len() > CHECKOUT_SAMPLES {
            samples.pop_front();
        }
    }

    /// How fast lanes arrive: how many got a tree, over the span the samples
    /// cover. `None` until two arrivals are far enough apart in time to define
    /// an interval.
    fn arrival_rate(&self) -> Option<(usize, u64)> {
        let arrivals = self
            .arrivals
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let first = *arrivals.front()?;
        let span = unix_now().saturating_sub(first);
        (arrivals.len() >= 2 && span > 0).then_some((arrivals.len(), span as u64))
    }

    /// The checkout cost in force, as seconds.
    fn checkout_secs(&self) -> Option<f64> {
        let mut samples: Vec<u64> = self
            .checkout_millis
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .copied()
            .collect();
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let median = samples[samples.len() / 2];
        Some(median as f64 / 1000.0)
    }

    /// Recompute the pool's target from this pass's capacity, the arrival rate
    /// and the checkout cost. `seed` stands until all three exist.
    fn derive_warm_target(&self, capacity: usize) {
        self.metrics.warm_trees_evaluations.inc();
        let seed = self.cfg.warm_trees;
        if seed == 0 {
            self.warm_target.store(0, Ordering::Relaxed);
            self.metrics.set_warm_trees(0, "seed");
            return;
        }
        let ceiling = capacity / 2;
        let measured = self.arrival_rate().and_then(|(arrivals, span)| {
            warm_trees_from(arrivals, span, self.checkout_secs()?, ceiling)
        });
        match measured {
            Some(target) => {
                self.warm_target.store(target, Ordering::Relaxed);
                self.metrics.set_warm_trees(target, "measured");
            }
            None => {
                let target = seed.min(ceiling);
                self.warm_target.store(target, Ordering::Relaxed);
                self.metrics.set_warm_trees(target, "seed");
            }
        }
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
    async fn take_warm(&self, lane: &str, path: &Path) -> bool {
        if self.warm_target.load(Ordering::Relaxed) == 0 {
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
            task_id: lane.to_string(),
            created_at_unix: now,
            last_used_unix: now,
        };
        if let Err(e) = self.write_meta(&meta).await {
            warn!(lane = %lane, error = %e, "warm tree not taken: could not write its sidecar");
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
            self.remove_meta(lane);
            warn!(
                lane = %lane,
                from = %slot.display(),
                error = %e,
                "warm tree not taken: moving it onto the task path failed"
            );
            return false;
        }
        let _ = tokio::fs::remove_file(Self::ready_marker(&slot)).await;
        self.register_safe_directory(path).await;
        // The slot was cut detached and the pool is filled ahead of demand, so
        // two things have to happen at hand-out: the tree gets its owner's
        // branch, and it is re-pointed at `main` in case the branch of that
        // name was cut before `main` moved. `checkout -B` does both at once --
        // a tree handed out three commits behind would read the platform's own
        // source wrong, and nothing on the surface would say why.
        //
        // Untracked files need no cleanup here: a slot is cut and handed out
        // without ever being run in, so there is nothing but `main` in it.
        self.point_at_branch(path, lane).await;
        info!(
            lane = %lane,
            from = %slot.display(),
            "handed a warm worktree to an employee instead of cutting one"
        );
        true
    }

    /// Put `path` on the branch its lane owns, starting from the current
    /// `main`. Best effort: the tree is usable either way, and a failure here
    /// is the next command's own to report.
    async fn point_at_branch(&self, path: &Path, lane: &str) {
        let branch = lane_branch(lane);
        if let Err(e) = self
            .run_git(&["checkout", "-B", &branch, "main"], Some(path))
            .await
        {
            warn!(
                lane = %lane,
                branch = %branch,
                error = %e,
                "warm tree handed out but its lane branch could not be checked out; \
                 the tree is usable and its commits would go to a detached head"
            );
        }
    }

    /// Cut trees until the pool holds as many as the deployment asked for.
    ///
    /// Best effort and never blocking an arrival: both locks are taken only if
    /// they are free, because a pool refill that queues behind a task's own
    /// creation makes the pool the reason the task waited.
    ///
    /// The pool lives *inside* the ceiling rather than on top of it: its slots
    /// are counted with the task trees (see [`Self::enforce_cap`]), so a refill
    /// at a full ceiling reclaims the least recently used task tree instead of
    /// standing down. Standing down was the older rule, and it made the pool
    /// useless exactly when it was needed -- measured on 2026-11-07, with the
    /// ceiling at 32 and the volume holding 32, the pool was drained by two
    /// warm hand-outs and never refilled, so every arrival after that paid the
    /// checkout the pool exists to remove. It looked healthy in the log, which
    /// recorded the one refill and the two hand-outs and said nothing about the
    /// pool that stayed empty.
    async fn warm_pool_once(&self) {
        let want = self.warm_target.load(Ordering::Relaxed);
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
        // Cutting a tree is `worktree add` against the same state a creation
        // evicts from, so the two cannot run against it at once. Free is the
        // common case -- the refill is spawned after the arrival it serves has
        // its directory -- and a creation in flight means this pass is skipped
        // rather than queued; the next hand-out or GC pass comes back.
        let Ok(_c) = self.create_lock.try_lock() else {
            return;
        };
        if let Err(e) = tokio::fs::create_dir_all(self.warm_root()).await {
            warn!(error = %e, "warm worktree pool: cannot create its directory");
            return;
        }
        // Room is made once for the whole refill, before the first cut: the
        // ceiling counts the slots this pass is about to add.
        self.enforce_cap(want - held).await;
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
            if let Err(e) = self.cut_worktree(&slot, None).await {
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
    async fn route_validated(&self, lane: &str, own_claim: bool) -> SFResult<(PathBuf, bool)> {
        let path = self.cfg.workspaces_root.join(lane);
        if self.is_healthy(&path).await {
            self.touch(lane).await;
            return Ok((path, false));
        }
        let _g = self.create_lock.lock().await;
        if self.is_healthy(&path).await {
            self.touch(lane).await;
            return Ok((path, false));
        }
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            if self.commands_running(lane, own_claim) > 0 {
                // Rebuilding means removing first, and a command is running in
                // this tree: the caller can come back, the command cannot.
                return Err(SFError::IO(format!(
                    "worktree for {lane} is in use by a running command and is not a \
                     healthy worktree; refusing to rebuild it from under that command"
                )));
            }
            warn!(lane = %lane, path = %path.display(),
                "lane path exists but is not a healthy worktree; rebuilding");
            self.force_remove(&path).await;
        }
        // The hand-out first, and the cap after it: a task that takes a warm
        // tree adds nothing to the volume, so asking the cap to make room for
        // it would reclaim a tree nobody needed to lose.
        if self.take_warm(lane, &path).await {
            self.note_arrival();
            self.refresh_count_metric().await;
            return Ok((path, true));
        }
        self.enforce_cap(1).await;
        if let Err(e) = self.add_worktree(lane, &path).await {
            self.metrics.inc_error("worktree_add");
            return Err(e);
        }
        self.note_arrival();
        self.refresh_count_metric().await;
        Ok((path, false))
    }

    async fn add_worktree(&self, lane: &str, path: &Path) -> SFResult<()> {
        let now = unix_now();
        let meta = TaskMeta {
            task_id: lane.to_string(),
            created_at_unix: now,
            last_used_unix: now,
        };
        // Sidecar lands before the tree: a crash between the two leaves a
        // reclaimable record; the reverse order leaves an untracked tree.
        self.write_meta(&meta).await?;
        if let Err(e) = self.cut_worktree(path, Some(lane)).await {
            self.remove_meta(lane);
            return Err(e);
        }
        info!(lane = %lane, path = %path.display(), "created the employee's worktree");
        self.register_safe_directory(path).await;
        Ok(())
    }

    /// Cut `main` into `path` as a worktree of the bare repo.
    ///
    /// With a lane, the tree is checked out on that lane's branch rather than
    /// detached: a detached checkout takes commits into no branch at all, so a
    /// reader could not tell which tree a commit landed on, and the employee's
    /// next turn would find nothing it had committed. Without a lane -- the warm
    /// pool, whose slot has no owner yet -- the tree is cut detached and the
    /// branch is attached when the slot is handed out (`take_warm`).
    ///
    /// `-B` rather than `-b`: a lane's branch outlives its tree, so a re-cut
    /// finds the name taken. Moving it back to `main` is the point -- a tree
    /// starts from the platform's current code, and the work of a tree that has
    /// already been reclaimed belongs to the change it became, not to this
    /// checkout.
    async fn cut_worktree(&self, path: &Path, lane: Option<&str>) -> SFResult<()> {
        if let Some(root) = path.parent() {
            tokio::fs::create_dir_all(root).await.map_err(|e| {
                SFError::IO(format!("create worktree root {}: {e}", root.display()))
            })?;
        }
        let branch = lane.map(lane_branch);
        let mut args = vec!["worktree", "add"];
        match branch.as_deref() {
            Some(branch) => {
                args.push("-B");
                args.push(branch);
            }
            None => args.push("--detach"),
        }
        args.push(path.to_str().unwrap_or(""));
        args.push("main");
        let started = std::time::Instant::now();
        let out = self.git_bare(&args).await;
        if out.is_ok() {
            self.note_checkout(started.elapsed().as_millis() as u64);
        }
        out.map(|_| ())
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
        self.derive_warm_target(capacity);
        // Oldest arrival to now, so the span covers exactly the trees being
        // counted. Without a second tree there is no arrival interval to read,
        // and the configured TTL stands.
        //
        // Measured over the task-kind trees only, because those are the trees
        // the TTL governs: an employee's checkout is never reaped for idleness
        // and is held for as long as its employee is registered, so counting it
        // would stretch the span by however old that conversation is and report
        // a hold time derived from a tree the rule does not apply to.
        let task_trees = trees.iter().filter(|(id, _)| !is_employee_lane(id)).count();
        let oldest = trees
            .iter()
            .filter(|(id, _)| !is_employee_lane(id))
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
            // An idle hour is not evidence that an employee is done with its
            // checkout: the lane comes back on the next turn, so the TTL would
            // delete the user's files between two conversations rather than
            // after one. The cap still reclaims it when a slot is needed -- see
            // [`is_employee_lane`].
            let reap = match self.read_meta(id) {
                Some(meta) => {
                    !is_employee_lane(id) && now.saturating_sub(meta.last_used_unix) >= ttl
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
    /// semantics (route, gc_once and the warm refill take it; this fn assumes
    /// exclusion).
    ///
    /// The trees waiting in the warm pool are counted here, because they are
    /// trees on the same volume and the ceiling is about the volume: a pool
    /// slot held outside the count is room the cap thinks it still has, and
    /// the next arrivals would spend it twice. That is also what gives the pool
    /// its reservation -- a task creation at the ceiling reclaims enough to
    /// leave the pool's slots standing.
    async fn enforce_cap(&self, incoming: usize) {
        let mut trees = self.list_task_trees().await;
        let warm = self.warm_slots().await.len();
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
        if trees.len() + warm + incoming <= max {
            return;
        }
        // Two kinds of tree, and the eviction order is per kind: one round of
        // task work pays before an employee's checkout does. See
        // [`is_employee_lane`] for why the LRU premise does not hold for the
        // second one.
        trees.sort_by_key(|(id, _)| {
            (
                is_employee_lane(id),
                self.read_meta(id.as_str())
                    .map(|m| m.last_used_unix)
                    .unwrap_or(i64::MIN),
            )
        });
        // The pool's slots are part of what has to end up under `max`, and they
        // are not in `trees`, so they enter the count here and never leave it
        // through the loop below: a pool tree is never the one reclaimed.
        let mut remaining = trees.len() + warm + incoming;
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

    /// The cap this pass runs under: what the volume is measured to hold at
    /// the size these trees are. The deployment's number is the seed for the
    /// passes before any tree exists -- there is no per-tree figure to divide
    /// the volume by yet -- and stops applying as soon as one does. A policy
    /// ceiling that kept applying would be a number about a volume somebody
    /// measured once, held against a volume that has changed since: trees grow
    /// with the repository, and co-tenants fill the same disk.
    async fn cap_for(&self, trees: &[(String, PathBuf)]) -> usize {
        self.metrics.cap_evaluations.inc();
        #[cfg(test)]
        if let Some(cap) = self.cfg.cap_override {
            // A fixture volume of a known size: the cap tests need a ceiling
            // the workspace root of a test host does not naturally provide.
            self.metrics.set_cap(cap, "measured");
            return cap;
        }
        match self.measured_cap(trees).await {
            Some(measured) => {
                self.metrics.set_cap(measured, "measured");
                measured
            }
            None => {
                let seed = self.cfg.max_workspaces;
                self.metrics.set_cap(seed, "seed");
                seed
            }
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

    /// How old a git lock has to be before it can only be a corpse; see
    /// [`cog_core::git_budget`]. Absent until this deployment has measured how
    /// quiet a live command gets, and then no lock is cleared.
    fn stale_lock_age(&self) -> Duration {
        cog_core::git_budget::stale_lock_age()
    }

    async fn run_git_once(&self, args: &[&str], workdir: Option<&Path>) -> SFResult<String> {
        let line = args.join(" ");
        let mut cmd = tokio::process::Command::new("git");
        cmd.args(args).kill_on_drop(true);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let out = match cog_core::progress::run_child_watched(cmd, &format!("git {line}"))
            .await
            .map_err(|e| SFError::IO(format!("failed to run git: {e}")))?
        {
            cog_core::progress::ChildOutcome::Exited(out) => out,
            cog_core::progress::ChildOutcome::Stalled { silent, window } => {
                return Err(SFError::IO(format!(
                    "git {line} stopped moving: no output and no CPU time for {silent:?}, \
                     longer than any gap this deployment has ever seen between steps of a \
                     command that was still working ({window:?}); it was killed"
                )))
            }
        };
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

/// Whether a lane belongs to an employee rather than to one round of task work.
///
/// The prefix is `cog-core`'s, because the run names its own checkout from the
/// same spelling this router reads: two spellings leave the model told about one
/// tree and the executor routing to another.
///
/// The distinction is what the cap and the TTL are told apart by. The cap evicts
/// by least-recently-used, and that rule rests on an assumption: a tree nobody
/// touched recently is a tree nobody will ask for again. It holds for a task's
/// tree, whose id is spent by the round that created it, and it fails for an
/// employee's, whose lane is handed back verbatim on the next turn. Measured on
/// the deployed chat lane (2026-11-06): a squad pipeline creating a tree roughly
/// every minute churned an eight-slot pool, `unscoped-shell-vera-82191e4a` was
/// reclaimed 144 s after the turn that used it, and the files written in that
/// turn went with it -- the next turn was routed to a fresh checkout and could
/// not see its own work.
fn is_employee_lane(lane: &str) -> bool {
    lane.starts_with(cog_core::WORKTREE_LANE_PREFIX)
}

/// The branch a lane's checkout is on.
///
/// Under `work/` so a reader listing the bare repo's branches can see whose
/// work is where; the lane name is unique per employee, and a branch name may
/// carry `/` even where a directory name may not.
fn lane_branch(lane: &str) -> String {
    format!("work/{lane}")
}

/// The checkout an employee works in, from the employee's own id.
///
/// The one answer to "which tree is this request's", for a request that named
/// its caller. `None` when the request named nobody: a caller with no identity
/// is routed by the task id it carried, which is where such a call landed
/// before lanes existed.
pub fn worktree_for_agent(agent_id: &str) -> Option<String> {
    if agent_id.trim().is_empty() {
        return None;
    }
    let lane = cog_core::worktree_lane(agent_id);
    validate_task_id(&lane).ok().map(|_| lane)
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

    /// The lane an employee id routes to. The tests say `lane("vera-…")` rather
    /// than spelling the name so that a change to the derivation is felt here
    /// too: a literal would keep passing while the run named a different tree.
    fn lane(agent_id: &str) -> String {
        worktree_for_agent(agent_id).expect("a named agent has a lane")
    }

    /// 一辆树的车道上挂着自己的分支：树被回收之后，那份活落在哪仍然读得出来。
    ///
    /// 以前切出来的是游离头（`--detach`），在上面提交等于把提交放进没有名字的
    /// 地方——读的人说不出它落在哪棵树上，而这一位下一回合回来也找不到自己刚提
    /// 过的那个提交。
    #[tokio::test]
    async fn a_lane_checkout_is_on_a_branch_named_after_its_lane() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (path, _) = r.route(&lane("squad:DSC-1A2B-generator")).await.unwrap();

        let head = tokio::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(&path)
            .output()
            .await
            .unwrap();
        let branch = String::from_utf8_lossy(&head.stdout).trim().to_string();
        assert_eq!(
            branch,
            lane_branch(&lane("squad:DSC-1A2B-generator")),
            "车道上的树要站在自己的分支上，否则提交没有落点"
        );
        assert_ne!(branch, "HEAD", "游离头上的提交不属于任何一棵树");
    }

    /// 暖池交出去的树同样站在车道的分支上：池子里的槽是游离切出来的，分支在
    /// 交出去的那一刻才挂上。
    #[tokio::test]
    async fn a_warm_tree_is_handed_out_on_its_lane_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK, 1);
        r.warm_pool_once().await;
        assert!(
            !r.warm_slots().await.is_empty(),
            "the pool is meant to be cut ahead of demand"
        );

        let (path, took_warm) = r
            .route_validated(&lane("squad:DSC-1A2B-planner"), true)
            .await
            .unwrap();
        assert!(took_warm, "the slot was there and had to be handed out");
        let head = tokio::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(&path)
            .output()
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&head.stdout).trim(),
            lane_branch(&lane("squad:DSC-1A2B-planner")),
            "暖池交出去的树也必须站在车道的分支上"
        );
    }

    /// 请求里的执行者决定树，请求里的任务号只做归属。
    ///
    /// 同一个任务里的几位以前共用一棵树：规划器读的文件、生成器写的文件、评审
    /// 跑的命令都在同一个工作区里，而任务号做完就废，树也跟着废。这里钉住的是
    /// 「两个执行者、同一个任务号 → 两棵树」。
    #[tokio::test]
    async fn two_employees_on_one_task_do_not_share_a_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let generator = lane("squad:DSC-1A2B-generator");
        let planner = lane("squad:DSC-1A2B-planner");
        let (g, _) = r.route(&generator).await.unwrap();
        let (p, _) = r.route(&planner).await.unwrap();
        assert_ne!(g, p, "同一个任务里的两个执行者挤进了一棵树");
        assert!(g.ends_with(&generator) && p.ends_with(&planner));
        assert!(
            worktree_for_agent("").is_none(),
            "没报名号的请求没有车道，只能按任务号退回去"
        );
    }

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
            cap_override: Some(8),
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
            cap_override: Some(8),
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
            cap_override: Some(8),
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
            cap_override: Some(max),
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
            cap_override: Some(max),
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

    /// The ceiling is the volume's, not the task lane's: when the task trees
    /// fill it and the pool is empty, the refill reclaims one instead of
    /// standing down.
    ///
    /// Standing down left the pool permanently empty under load -- measured in
    /// production on 2026-11-07 with the ceiling at 32 and the volume holding
    /// 32: the pool was drained by two hand-outs at 09:04 and 09:13 and never
    /// refilled, so every arrival after that paid the checkout the pool exists
    /// to remove. The log looked healthy, because the refill that did happen and
    /// the hand-outs were each recorded and the standing down was not.
    #[tokio::test]
    async fn a_pool_drained_at_a_full_ceiling_refills_by_reclaiming_a_task_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK, 1);

        // Two tasks and an empty pool, so the ceiling is full with task trees
        // and no claim is holding either of them.
        for id in ["first", "second"] {
            let (_path, guard) = r.route(id).await.unwrap();
            drop(guard);
        }
        assert_eq!(
            r.list_task_trees().await.len(),
            2,
            "the task trees fill the ceiling"
        );
        assert!(r.warm_slots().await.is_empty());

        r.warm_pool_once().await;

        assert_eq!(
            r.warm_slots().await.len(),
            1,
            "the pool refills at a full ceiling"
        );
        assert_eq!(
            r.list_task_trees().await.len(),
            1,
            "the slot came out of the ceiling, so one task tree was reclaimed for it"
        );
    }

    /// The other half of counting the pool: its slots are trees on the volume,
    /// so a task tree that would be the third on a volume that holds two makes
    /// room for them -- and the pool's own tree is never the one that goes.
    #[tokio::test]
    async fn the_ceiling_counts_the_pool_s_trees_against_a_new_task_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK, 1);

        // The task first, while the pool is still empty, so this tree is cut
        // rather than handed out and no refill is queued behind it.
        let (_path, guard) = r.route("a").await.unwrap();
        drop(guard);
        r.warm_pool_once().await;
        assert_eq!(r.warm_slots().await.len(), 1);
        assert_eq!(r.list_task_trees().await.len(), 1);

        // A third tree would not fit. Room is made by the task lane, because
        // the pool slot is part of what has to fit.
        r.enforce_cap(1).await;

        assert!(
            r.list_task_trees().await.is_empty(),
            "the pool's slot was counted, so the task tree made room for it"
        );
        assert_eq!(
            r.warm_slots().await.len(),
            1,
            "the pool's own tree is never the one reclaimed"
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

    /// An employee's checkout is named after the employee and comes back on the
    /// next turn; a task's tree is named after one round and never comes back.
    /// Under a plain LRU the older checkout loses every time a pipeline round
    /// needs a slot, and the user's files lose with it.
    #[tokio::test]
    async fn an_employee_checkout_outlives_the_task_trees_that_share_its_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 2, DEFAULT_TTL_SECS_FALLBACK);
        // The shell is the oldest tree on the volume, so plain LRU takes it.
        r.route(&lane("vera-82191e4a")).await.unwrap();
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
            ids.contains(&lane("vera-82191e4a")),
            "a conversation must keep the tree its files are in: {ids:?}"
        );
        assert!(ids.contains(&"round-b".to_string()), "{ids:?}");
        assert!(!ids.contains(&"round-a".to_string()), "{ids:?}");

        // The standing lane is a priority, not an exemption: with nothing else
        // left to reclaim, the cap still holds.
        let tmp2 = tempfile::tempdir().unwrap();
        let bare2 = seed_bare(tmp2.path());
        let r = router(tmp2.path(), &bare2, 1, DEFAULT_TTL_SECS_FALLBACK);
        r.route(&lane("old")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.route(&lane("new")).await.unwrap();
        let ids: Vec<String> = r
            .list_task_trees()
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec![lane("new")]);
        assert_eq!(r.metrics.cap_deferred.get(), 0.0);
    }

    /// The idle TTL is a rule about finished work: a task's tree is spent by
    /// its round, so an idle one is a tree nobody will ask for. An employee's
    /// checkout comes back by name, so the same idle hour says nothing —
    /// reaping it would delete the user's files between two conversations
    /// rather than after one.
    #[tokio::test]
    async fn the_idle_ttl_leaves_an_employee_checkout_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, Duration::ZERO);
        let (shell, _) = r.route(&lane("vera-82191e4a")).await.unwrap();
        let (round, _) = r.route("round-a").await.unwrap();
        // Stamped together so the arrival span is zero and the configured TTL
        // stands; the derived one is floored at an hour and would hide the rule.
        let together = unix_now();
        for id in [lane("vera-82191e4a").as_str(), "round-a"] {
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
    /// from their arrivals. A checkout that has been open for a week would
    /// otherwise stretch the span by that week and publish a hold time the rule
    /// never applied — a number that reads as "idle rounds are kept for weeks".
    #[tokio::test]
    async fn the_derived_ttl_is_measured_over_the_task_lane() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        r.route(&lane("vera-82191e4a")).await.unwrap();
        for id in ["round-a", "round-b", "round-c", "round-d"] {
            r.route(id).await.unwrap();
        }
        let week_ago = unix_now() - 7 * 86_400;
        let hour_ago = unix_now() - 3_600;
        for id in [
            lane("vera-82191e4a").as_str(),
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
            is_employee_lane(id)
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
    /// enough for the two routes below to land in different seconds, and again
    /// on 2026-11-12 in a full-suite run, where the same two routes sat more
    /// than a second apart in wall-clock time.
    ///
    /// So this test no longer asks which of the two TTLs is in force. It stamps
    /// both trees **older than either one** — the configured `ZERO` and the
    /// largest value the derivation can return, `MIN_TTL_SECS` — and then the
    /// only thing left deciding the outcome is the in-use exception, which is
    /// the premise the test exists for. The arrivals are still stamped at one
    /// instant, so a reader that wonders which branch ran is not answered by a
    /// race either.
    #[tokio::test]
    async fn ttl_reaps_idle_trees_but_not_one_in_use() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, Duration::ZERO);
        let (running, _in_use) = r.route("running").await.unwrap();
        let (idle, _) = r.route("idle").await.unwrap();
        let arriving_together = unix_now();
        // Older than every TTL a pass can pick, so neither branch can spare an
        // idle tree and the assertion measures the exception rather than the
        // derivation.
        let past_every_ttl = arriving_together - MIN_TTL_SECS as i64 - 1;
        for id in ["running", "idle"] {
            let mut meta = r.read_meta(id).expect("a routed tree has a sidecar");
            meta.created_at_unix = arriving_together;
            meta.last_used_unix = past_every_ttl;
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
        // How old a lock has to be before it can only be a corpse is the
        // deployment's own measured window doubled, not a number chosen here:
        // until something has been measured no lock may be cleared, because a
        // corpse cannot be told from a command still working. So the window is
        // seeded first and the lock is backdated past twice it.
        let _serial = cog_core::progress::async_test_lock().await;
        cog_core::progress::reset_for_tests();
        cog_core::progress::seed_gaps_for_tests(
            Duration::from_secs(300),
            cog_core::progress::SAMPLE_FLOOR,
        );

        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK);
        let (path, _) = r.route("lock-task").await.unwrap();
        let gitdir = PathBuf::from(git_out(&path, &["rev-parse", "--absolute-git-dir"]));
        let lock = gitdir.join("index.lock");
        std::fs::write(&lock, "").unwrap();
        // Backdate mtime past twice the 300s window seeded above.
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
        cog_core::progress::reset_for_tests();
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

    /// The cap is not a setting: a pass reads the volume, and when the
    /// footprint these trees occupy changes, the very next pass answers a
    /// different cap. No restart, no configuration.
    #[tokio::test]
    async fn the_worktree_cap_is_recomputed_from_the_volume_on_the_next_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let cfg = WorkdirConfig {
            workspaces_root: tmp.path().join("workspaces"),
            bare_repo: bare.clone(),
            target_dir: tmp.path().join("src").join("target"),
            seed_urls: Vec::new(),
            ttl: DEFAULT_TTL_SECS_FALLBACK,
            gc_interval: Duration::from_secs(600),
            fetch_interval: Duration::from_secs(300),
            max_workspaces: 32,
            cap_override: None,
            warm_trees: 0,
            data_volumes: Vec::new(),
            data_volume_declaration_error: None,
            data_volume_interval: Duration::from_secs(30),
            build_cache: BuildCacheConfig::default(),
        };
        let r = WorkdirRouter::new(cfg).unwrap();
        r.route("one").await.unwrap();
        r.route("two").await.unwrap();
        r.gc_once().await.unwrap();
        let before = r.metrics().cap.get();
        assert!(
            before > 32.0,
            "a volume this empty holds more than the seed: {before}"
        );
        assert_eq!(
            r.metrics()
                .cap_source
                .with_label_values(&["measured"])
                .get(),
            1.0
        );

        // The same trees, a footprint eight mebibytes larger: the next pass
        // can only answer a smaller cap. This is the input the law divides by.
        std::fs::write(
            tmp.path().join("workspaces").join("grown.bin"),
            vec![0u8; 8 * 1024 * 1024],
        )
        .unwrap();
        r.gc_once().await.unwrap();
        let after = r.metrics().cap.get();
        assert!(
            after < before,
            "the cap did not follow the measured footprint: {before} -> {after}"
        );
        assert!(r.metrics().cap_evaluations.get() >= 2.0);
    }

    /// The pool's size is a function of two measurements: how often lanes
    /// arrive and what one checkout costs. A different input answers
    /// differently, which is the whole difference between this and a number
    /// someone chose.
    #[test]
    fn the_warm_pool_target_follows_arrivals_and_checkout_cost() {
        // Two arrivals in a minute at a 3.4 s checkout: less than one tree in
        // flight, so the pool holds one.
        assert_eq!(warm_trees_from(2, 60, 3.4, 16), Some(1));
        // Thirty a minute at the same cost: two are in flight while one is cut.
        assert_eq!(warm_trees_from(30, 60, 3.4, 16), Some(2));
        // The same arrivals against a cheaper checkout need one.
        assert_eq!(warm_trees_from(30, 60, 1.0, 16), Some(1));
        // Never more than half the capacity, however busy the volume is.
        assert_eq!(warm_trees_from(6000, 60, 3.4, 4), Some(4));
        // Nothing measured: no target, so the seed stands.
        assert_eq!(warm_trees_from(1, 60, 3.4, 16), None);
        assert_eq!(warm_trees_from(2, 0, 3.4, 16), None);
        assert_eq!(warm_trees_from(2, 60, 0.0, 16), None);
        assert_eq!(warm_trees_from(2, 60, 3.4, 0), Some(0));
    }

    /// The target is recomputed by every reclamation pass and published with
    /// the input set that chose it. The evaluation counter is what shows the
    /// beat is running when the number happens not to move.
    #[tokio::test]
    async fn the_warm_pool_target_is_recomputed_on_the_next_reclamation_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let bare = seed_bare(tmp.path());
        let r = router_with_warm(tmp.path(), &bare, 8, DEFAULT_TTL_SECS_FALLBACK, 1);
        r.gc_once().await.unwrap();
        assert_eq!(r.metrics().warm_trees.get(), 1.0);
        assert_eq!(
            r.metrics()
                .warm_trees_source
                .with_label_values(&["seed"])
                .get(),
            1.0
        );
        let beats = r.metrics().warm_trees_evaluations.get();

        // Two arrivals a second apart and a checkout measured at 3.4 s: seven
        // would be in flight, clamped to half the eight-tree capacity.
        r.note_checkout(3400);
        r.note_arrival();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        r.note_arrival();
        r.gc_once().await.unwrap();
        assert_eq!(r.metrics().warm_trees.get(), 4.0);
        assert_eq!(
            r.metrics()
                .warm_trees_source
                .with_label_values(&["measured"])
                .get(),
            1.0
        );
        assert!(r.metrics().warm_trees_evaluations.get() > beats);
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
