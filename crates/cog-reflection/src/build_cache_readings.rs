//! How many bytes the shared build cache holds, layer by layer.
//!
//! The cache is what makes a build affordable on a host that shares its machine
//! with the cluster, and it only ever grows: every change built adds to it, and
//! nothing measured it, capped it or removed anything from it. The one number
//! anybody had came from running `du` by hand, so the question a cache that
//! fills its volume raises -- which part of it to drop -- had no reading at all,
//! and the answer after a build starts failing on a full disk is "all of it",
//! which costs a full cold rebuild of every workspace.
//!
//! Two series, because they answer two different questions:
//!
//! - `cogneva_build_target_bytes{layer}` -- what the cache holds, divided by
//!   layer. The layer is a path inside the cache two levels down, which is the
//!   grain that separates the things a reader can decide differently about: the
//!   `deps` and `.fingerprint` halves are the compiled results (dropping them is
//!   the cold rebuild), while `incremental` and `tmp` are speed and scratch
//!   space whose cost to lose is a slower next build. A file that cargo hardlinks
//!   under two names — which is what it does with everything it lifts out of
//!   `deps` — is counted once, under the first of its names in path order, and
//!   its pass frees its bytes once, because that is how many times they are on
//!   the volume.
//! - `cogneva_build_target_bytes_scan_age_seconds` -- how long ago the cache was
//!   measured. The sizes keep their last reading when a walk fails, which is the
//!   right thing to publish and is also invisible: a scan loop that died leaves
//!   behind a cache that reads as unchanging rather than as unmeasured.
//!
//! A cap can be held against that total, and when one is configured the same
//! walk that produces the reading also enforces it: the module that removes
//! bytes (`build_cache_reclaim`) is driven from here so that the number a cap is
//! checked against and the plan that acts on it come from one snapshot. The
//! decision of *what* to drop is that module's; this one owns the timer, the
//! measurement and the state a reader sees afterwards.
//!
//! The cap family, published only when a cap is configured:
//!
//! - `cogneva_build_target_bytes_cap{dir}` -- the cap itself, so a panel can
//!   draw the wall the other lines are measured against.
//! - `cogneva_build_target_over_limit_bytes{dir}` -- how far above it the cache
//!   is, as of the last walk. The earlier of the two numbers that say the cap is
//!   not holding.
//! - `cogneva_build_target_unmet_bytes{dir}` -- how far above it the cache
//!   stayed after a pass ran. Not the same reading: the first says the cache is
//!   over its cap, this one says removing what may be removed did not fix it,
//!   and they call for different actions (wait for the next pass; or change the
//!   cap or the permissions).
//! - `cogneva_build_target_over_cap_total{outcome}` -- walks that found the cache
//!   over its cap, by what happened next: `reclaimed` (a pass brought it under),
//!   `unmet` (a pass ran and could not), `busy` (a build held the slot, so no
//!   pass ran) or `ungated` (no build gate is in force, so nothing may be
//!   removed). A growing `busy` or `ungated` is a cap that is not being enforced
//!   at all rather than one that is failing.
//! - `cogneva_build_target_reclaimed_bytes_total{dir}` -- bytes removed so far.
//! - `cogneva_build_target_last_reclaim_seconds{dir}` -- when a pass last ran,
//!   and the process start when none has. A pass that never runs has to age
//!   somewhere, or a cache over its cap reads the same as one being fixed.
//! - `cogneva_build_target_scan_interval_seconds{dir}` -- the configured
//!   interval, so a rule can say "no pass in six intervals" without a constant
//!   that goes stale when the interval is configured differently.
//!
//! Only the process that owns the directory publishes it, which is the process
//! that runs builds: a deployment with no builder has no cache to report rather
//! than a cache of zero bytes. Which processes those are is readable from the
//! build gate's `role` label, so "no series" can be traced to its cause.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cog_core::build_gate::{dir_identity, BuildGate};
use cog_core::fs_size::{self, FileEntry};
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};
use tracing::{info, warn};

/// Apparent bytes the cache holds, per layer.
pub const BUILD_TARGET_BYTES_METRIC: &str = "cogneva_build_target_bytes";

/// Seconds since the cache was last measured.
pub const BUILD_TARGET_SCAN_AGE_METRIC: &str = "cogneva_build_target_bytes_scan_age_seconds";

/// The cap the cache is held to, in bytes. Published only when one is set.
pub const BUILD_TARGET_CAP_METRIC: &str = "cogneva_build_target_bytes_cap";

/// Bytes the cache is above its cap, as of the last walk.
pub const BUILD_TARGET_OVER_LIMIT_METRIC: &str = "cogneva_build_target_over_limit_bytes";

/// Bytes still above the cap after the last pass that ran.
pub const BUILD_TARGET_UNMET_METRIC: &str = "cogneva_build_target_unmet_bytes";

/// Walks that found the cache over its cap, by what happened next.
pub const BUILD_TARGET_OVER_CAP_METRIC: &str = "cogneva_build_target_over_cap_total";

/// The label naming what a pass did, or why it did not run.
pub const OUTCOME_LABEL: &str = "outcome";

/// A pass ran and brought the cache under its cap.
pub const OUTCOME_RECLAIMED: &str = "reclaimed";

/// A pass ran and the cache stayed above its cap.
pub const OUTCOME_UNMET: &str = "unmet";

/// A build held the only build slot, so no pass ran.
pub const OUTCOME_BUSY: &str = "busy";

/// No build gate is in force, so nothing may be removed.
pub const OUTCOME_UNGATED: &str = "ungated";

/// This loop's name in the liveness census.
pub const BUILD_CACHE_WATCH_LOOP: &str = "build_cache_watch";

/// Every value the outcome label takes, so a reader can see the whole domain
/// with zeros rather than inferring it from whichever values happened to occur.
pub const RECLAIM_OUTCOMES: &[&str] = &[
    OUTCOME_RECLAIMED,
    OUTCOME_UNMET,
    OUTCOME_BUSY,
    OUTCOME_UNGATED,
];

/// Bytes removed from the cache by this process so far.
pub const BUILD_TARGET_RECLAIMED_METRIC: &str = "cogneva_build_target_reclaimed_bytes_total";

/// When a reclamation pass last ran, in unix seconds.
pub const BUILD_TARGET_LAST_RECLAIM_METRIC: &str = "cogneva_build_target_last_reclaim_seconds";

/// The configured scan interval, so a rule can say how long is too long without
/// carrying a copy of the interval that goes stale when it is configured.
pub const BUILD_TARGET_SCAN_INTERVAL_METRIC: &str = "cogneva_build_target_scan_interval_seconds";

/// The layer label.
pub const LAYER_LABEL: &str = "layer";

/// The label naming the directory the reading belongs to.
///
/// `dev:ino` rather than the path, for the same reason the build gate publishes
/// it: the same path is mounted from different volumes in different workloads,
/// and two caches behind one path would otherwise be summed into one number with
/// nothing to say they are two.
pub const DIR_LABEL: &str = "dir";

/// The layer carrying everything past [`MAX_PUBLISHED_LAYERS`].
pub const OTHER_LAYER: &str = "other";

/// How deep into the cache a layer is taken from.
///
/// Two: one level is a cargo profile (`debug`, `release`), which says nothing
/// about what can be dropped, and cargo keeps its own division one level below
/// that (`deps`, `incremental`, `build`, `.fingerprint`, `examples`).
pub const CACHE_LAYER_DEPTH: usize = 2;

/// How many layers get a series of their own.
///
/// The layer name comes from a directory in the cache, and a directory name is
/// data that builds write, so the domain is not closed by construction. The cap
/// is on the published side and the fold is by size: the layers a reader would
/// act on keep their names, the rest sum into [`OTHER_LAYER`] so the total stays
/// exact, and a reader sees the fold as that series growing. Cargo's own layout
/// is well under this, so in practice nothing is folded.
pub const MAX_PUBLISHED_LAYERS: usize = 12;

/// Fastest the cache may be re-walked at. The walk is metadata-only, but it
/// walks a tree with hundreds of thousands of entries on a host that is also
/// building, so it holds no value being fresher than minutes.
pub const MIN_SCAN_INTERVAL_SECS: u64 = 60;

/// What the last pass left behind.
#[derive(Debug, Default, Clone, Copy)]
struct ReclaimState {
    /// Bytes above the cap as of the last walk; `None` before one has been made
    /// with a cap configured.
    over_limit: Option<u64>,
    /// Bytes above the cap after the last pass that ran; `None` before one has.
    unmet: Option<u64>,
}

/// The cache this process owns, as measured by the last completed walk.
///
/// The measurement is written by the scan loop and read by the metrics pull,
/// which are different tasks, hence the lock and the atomic rather than a
/// single handoff.
pub struct BuildCacheReadings {
    dir: PathBuf,
    layers: Mutex<BTreeMap<String, u64>>,
    /// Unix seconds of the last completed walk; 0 before one has happened.
    scanned_at: AtomicU64,
    /// Bytes the cache may hold, or 0 when it is measured but not bounded.
    cap_bytes: u64,
    scan_interval_secs: u64,
    reclaim: Mutex<ReclaimState>,
    reclaimed_bytes: AtomicU64,
    /// Passes that found the cache over its cap, by outcome.
    over_cap: Mutex<BTreeMap<String, u64>>,
    /// Unix seconds of the last pass that ran. The process start until one does,
    /// so a pass that never runs ages against a clock a rule can read: were this
    /// absent until the first pass, "no pass has ever run" would be the one state
    /// with no reading at all.
    last_pass_at: AtomicU64,
}

impl BuildCacheReadings {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            layers: Mutex::new(BTreeMap::new()),
            scanned_at: AtomicU64::new(0),
            cap_bytes: 0,
            scan_interval_secs: 0,
            reclaim: Mutex::new(ReclaimState::default()),
            reclaimed_bytes: AtomicU64::new(0),
            over_cap: Mutex::new(BTreeMap::new()),
            last_pass_at: AtomicU64::new(unix_now()),
        }
    }

    /// Bound the cache to `max_bytes`, re-walked every `scan_interval_secs`.
    ///
    /// `max_bytes = 0` leaves the cache measured and unbounded, which is the
    /// default: what the number should be is a deployment's own decision —
    /// derived from the volume behind the directory — and a default that started
    /// removing bytes on upgrade would be making that decision silently.
    pub fn with_cap(mut self, max_bytes: u64, scan_interval_secs: u64) -> Self {
        self.cap_bytes = max_bytes;
        self.scan_interval_secs = scan_interval_secs;
        self
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The cap, or `None` when this cache is not bounded.
    pub fn cap_bytes(&self) -> Option<u64> {
        (self.cap_bytes > 0).then_some(self.cap_bytes)
    }

    /// How long between walks.
    pub fn scan_interval_secs(&self) -> u64 {
        self.scan_interval_secs.max(MIN_SCAN_INTERVAL_SECS)
    }

    /// Record one completed walk.
    pub fn set_layers(&self, layers: BTreeMap<String, u64>, scanned_at_unix_secs: u64) {
        let mut held = self.layers.lock().unwrap_or_else(|e| e.into_inner());
        *held = layers;
        self.scanned_at
            .store(scanned_at_unix_secs, Ordering::Release);
    }

    /// The last measurement, or `None` before one has happened.
    ///
    /// Not an empty map: a cache that reads as zero bytes before anything was
    /// walked is a claim about the cache rather than an absence of evidence,
    /// and it is the reading a cap would be checked against.
    pub fn measured(&self) -> Option<BTreeMap<String, u64>> {
        let scanned_at = self.scanned_at.load(Ordering::Acquire);
        if scanned_at == 0 {
            return None;
        }
        let held = self.layers.lock().unwrap_or_else(|e| e.into_inner());
        Some(held.clone())
    }

    /// Seconds since the last completed walk, or `None` before one has happened.
    pub fn scan_age_secs(&self, now_unix_secs: u64) -> Option<u64> {
        let scanned_at = self.scanned_at.load(Ordering::Acquire);
        (scanned_at != 0).then(|| now_unix_secs.saturating_sub(scanned_at))
    }

    /// The layers to publish, largest first, with everything past the cap folded
    /// into [`OTHER_LAYER`].
    fn published_layers(&self, layers: &BTreeMap<String, u64>) -> Vec<(String, u64)> {
        let mut ordered: Vec<(&String, u64)> = layers.iter().map(|(k, v)| (k, *v)).collect();
        // Largest first, then by name, so two layers of equal size do not swap
        // series between scrapes.
        ordered.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

        let mut out = Vec::with_capacity(ordered.len().min(MAX_PUBLISHED_LAYERS) + 1);
        let mut folded = 0u64;
        for (index, (layer, bytes)) in ordered.into_iter().enumerate() {
            if index < MAX_PUBLISHED_LAYERS {
                out.push((layer.clone(), bytes));
            } else {
                folded = folded.saturating_add(bytes);
            }
        }
        // Always published, zero included: a reader has to be able to tell "no
        // layer was folded" from "this series is not wired up".
        out.push((OTHER_LAYER.to_string(), folded));
        out
    }

    /// Bring the cache back under its cap, if it is over one and a build slot
    /// can be taken.
    ///
    /// `files` are the entries of the walk that produced the measurement being
    /// published, so the total the cap is judged against and the plan that acts
    /// on it come from one snapshot. Nothing is removed without holding the
    /// build slot: a file deleted under a running build fails that build for a
    /// reason that has nothing to do with it, and that failure would be recorded
    /// against a change rather than against the host.
    pub async fn enforce_cap(&self, files: &[FileEntry], gate: Option<&Arc<BuildGate>>) {
        let Some(cap) = self.cap_bytes() else {
            return;
        };
        let total = fs_size::counted_bytes(files);
        let excess = total.saturating_sub(cap);
        {
            let mut state = self.reclaim.lock().unwrap_or_else(|e| e.into_inner());
            state.over_limit = Some(excess);
        }
        if excess == 0 {
            return;
        }

        // The gate is what makes "no build is running" a fact rather than a hope.
        // Where it is not in force, the cap is reported as unenforceable instead
        // of being enforced against a guess.
        let Some(gate) = gate.filter(|g| g.in_force()) else {
            self.count_outcome(OUTCOME_UNGATED);
            warn!(
                dir = %self.dir.display(),
                over_limit_bytes = excess,
                "build cache is over its cap and no build gate is in force, so nothing was removed"
            );
            return;
        };
        let permit = match gate.try_acquire("build-cache-reclaim").await {
            Ok(permit) => permit,
            Err(_) => {
                // A build holds the slot. Not a failure: the cache is over its
                // cap while the host is busy building into it, and the next walk
                // will try again. It is counted separately because a cache that
                // is *never* reclaimed and one that cannot be reclaimed call for
                // different things.
                self.count_outcome(OUTCOME_BUSY);
                info!(
                    dir = %self.dir.display(),
                    over_limit_bytes = excess,
                    "build cache is over its cap; a build holds the slot, so the pass waits for the next walk"
                );
                return;
            }
        };

        let plan = crate::build_cache_reclaim::plan_reclaim(files, total, cap);
        let outcome = crate::build_cache_reclaim::apply_reclaim(&self.dir, &plan);
        // Held for the whole deletion: the slot is what keeps a build from
        // reading a file this pass is about to remove.
        drop(permit);

        let freed = outcome.freed_bytes;
        self.reclaimed_bytes.fetch_add(freed, Ordering::Relaxed);
        let remaining = excess.saturating_sub(freed);
        {
            let mut state = self.reclaim.lock().unwrap_or_else(|e| e.into_inner());
            state.unmet = Some(remaining);
        }
        self.last_pass_at.store(unix_now(), Ordering::Release);
        self.count_outcome(if remaining == 0 {
            OUTCOME_RECLAIMED
        } else {
            OUTCOME_UNMET
        });

        let failures: Vec<String> = outcome
            .failures
            .iter()
            .take(5)
            .map(|(path, why)| format!("{}: {why}", path.display()))
            .collect();
        if remaining == 0 && outcome.failures.is_empty() {
            info!(
                dir = %self.dir.display(),
                deleted_names = outcome.deleted_names,
                freed_files = outcome.freed_files,
                freed_bytes = freed,
                cap_bytes = cap,
                "build cache reclaimed down to its cap"
            );
        } else {
            warn!(
                dir = %self.dir.display(),
                deleted_names = outcome.deleted_names,
                freed_files = outcome.freed_files,
                freed_bytes = freed,
                cap_bytes = cap,
                unmet_bytes = remaining,
                failures = outcome.failures.len(),
                first_failures = ?failures,
                "build cache could not be reclaimed down to its cap"
            );
        }
    }

    /// Record one walk that found the cache over its cap.
    fn count_outcome(&self, outcome: &str) {
        let mut counts = self.over_cap.lock().unwrap_or_else(|e| e.into_inner());
        let count = counts.entry(outcome.to_string()).or_default();
        *count = count.saturating_add(1);
    }

    /// The cap family, or nothing when this cache is not bounded.
    fn cap_metrics(&self, dir: &str) -> Vec<RawMetric> {
        let Some(cap) = self.cap_bytes() else {
            return Vec::new();
        };
        let state = *self.reclaim.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = vec![
            RawMetric::new(BUILD_TARGET_CAP_METRIC, cap as f64).with_label(DIR_LABEL, dir),
            RawMetric::new(
                BUILD_TARGET_SCAN_INTERVAL_METRIC,
                self.scan_interval_secs() as f64,
            )
            .with_label(DIR_LABEL, dir),
            RawMetric::new(
                BUILD_TARGET_RECLAIMED_METRIC,
                self.reclaimed_bytes.load(Ordering::Relaxed) as f64,
            )
            .with_label(DIR_LABEL, dir),
            RawMetric::new(
                BUILD_TARGET_LAST_RECLAIM_METRIC,
                self.last_pass_at.load(Ordering::Acquire) as f64,
            )
            .with_label(DIR_LABEL, dir),
        ];
        if let Some(over) = state.over_limit {
            out.push(
                RawMetric::new(BUILD_TARGET_OVER_LIMIT_METRIC, over as f64)
                    .with_label(DIR_LABEL, dir),
            );
        }
        if let Some(unmet) = state.unmet {
            out.push(
                RawMetric::new(BUILD_TARGET_UNMET_METRIC, unmet as f64).with_label(DIR_LABEL, dir),
            );
        }
        let counts = self.over_cap.lock().unwrap_or_else(|e| e.into_inner());
        for outcome in RECLAIM_OUTCOMES {
            out.push(
                RawMetric::new(
                    BUILD_TARGET_OVER_CAP_METRIC,
                    counts.get(*outcome).copied().unwrap_or(0) as f64,
                )
                .with_label(DIR_LABEL, dir)
                .with_label(OUTCOME_LABEL, *outcome),
            );
        }
        out
    }
}

/// Unix seconds now, saturating at the epoch.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[async_trait]
impl Observable for BuildCacheReadings {
    /// Nothing at all before the first walk completes, so the series a cap is
    /// checked against does not exist yet rather than existing with a value
    /// nothing measured.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let dir = dir_identity(&self.dir);
        // The cap family comes first and does not depend on a walk having
        // succeeded: a cap is a configuration and "nothing reclaimed yet" is a
        // count of zero, not a claim about the cache. A cache whose walk keeps
        // failing is exactly when a reader most needs to see that it is supposed
        // to be bounded.
        let mut out: Vec<RawMetric> = self.cap_metrics(&dir);
        let Some(layers) = self.measured() else {
            return Ok(out);
        };
        out.extend(
            self.published_layers(&layers)
                .into_iter()
                .map(|(layer, bytes)| {
                    RawMetric::new(BUILD_TARGET_BYTES_METRIC, bytes as f64)
                        .with_label(DIR_LABEL, dir.clone())
                        .with_label(LAYER_LABEL, layer)
                }),
        );
        if let Some(age) = self.scan_age_secs(unix_now()) {
            out.push(
                RawMetric::new(BUILD_TARGET_SCAN_AGE_METRIC, age as f64)
                    .with_label(DIR_LABEL, dir.clone()),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The cache size is not a per-dimension metric: every dimension is built
    /// from the same tree, so declaring no dimension is what tells the collector
    /// to pull this observable once rather than once per question.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Re-measure the cache on a timer, publish the result on `readings`, and
/// enforce its cap.
pub async fn run_build_cache_watch(readings: Arc<BuildCacheReadings>, shutdown: ShutdownSignal) {
    let dir = readings.dir().to_path_buf();
    let interval = Duration::from_secs(readings.scan_interval_secs());
    // Every series this watcher publishes is its own measurement — the size, the
    // cap it is held to, what a pass reclaimed. If the task died, all of them
    // stop being written, and a cache nobody is measuring reads exactly like a
    // cache that is small. Its liveness therefore cannot come from itself.
    let beat = cog_core::loop_health::register(
        BUILD_CACHE_WATCH_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
    );
    let _mortality = beat.watch_death(shutdown.clone());
    match readings.cap_bytes() {
        Some(cap) => info!(
            dir = %dir.display(),
            interval_secs = interval.as_secs(),
            depth = CACHE_LAYER_DEPTH,
            cap_bytes = cap,
            metric = BUILD_TARGET_BYTES_METRIC,
            "build cache watcher started; the cache is capped"
        ),
        // Said out loud, because the same watcher with no cap looks the same in
        // the readings as a cap that is never reached.
        None => info!(
            dir = %dir.display(),
            interval_secs = interval.as_secs(),
            depth = CACHE_LAYER_DEPTH,
            metric = BUILD_TARGET_BYTES_METRIC,
            "build cache watcher started; no cap is configured, so the cache is measured and not bounded"
        ),
    }

    let mut ticker = tokio::time::interval(interval);
    loop {
        // One stamp per pass, whatever the pass measured: a cache that did not
        // grow is not a watcher that stopped.
        beat.beat();
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                let path = dir.clone();
                // Off the runtime: the walk is metadata-only but it is a walk of
                // a large tree, and it must not hold up the cycles that build
                // into this cache.
                //
                // One walk, not two: the files are what the cap is enforced
                // against and the layers are what is published, and a plan built
                // from a different snapshot than the published total would be
                // enforcing a figure nobody can see.
                let walked = tokio::task::spawn_blocking(move || {
                    fs_size::dir_files(&path, CACHE_LAYER_DEPTH, &[])
                })
                .await;
                match walked {
                    Ok(Ok(files)) => {
                        readings.set_layers(fs_size::layer_totals(&files), unix_now());
                        readings
                            .enforce_cap(&files, cog_core::build_gate::global().as_ref())
                            .await;
                    }
                    // A failed walk yields a total that is too small, which can
                    // only silence a cap. Keep the last measurement rather than
                    // publish a fictional small one, and say so.
                    Ok(Err(e)) => warn!(
                        error = %e,
                        dir = %dir.display(),
                        "build cache scan failed; keeping the last measurement"
                    ),
                    Err(e) => warn!(error = %e, "build cache scan task panicked"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readings_with(layers: &[(&str, u64)]) -> BuildCacheReadings {
        let readings = BuildCacheReadings::new("/nonexistent-cache");
        let map: BTreeMap<String, u64> = layers
            .iter()
            .map(|(name, bytes)| (name.to_string(), *bytes))
            .collect();
        readings.set_layers(map, unix_now());
        readings
    }

    async fn published(readings: &BuildCacheReadings) -> Vec<(String, String, u64)> {
        let metrics = readings.collect_metrics("").await.unwrap();
        metrics
            .into_iter()
            .filter(|m| m.name == BUILD_TARGET_BYTES_METRIC)
            .map(|m| {
                (
                    m.labels.get(LAYER_LABEL).cloned().unwrap_or_default(),
                    m.labels.get(DIR_LABEL).cloned().unwrap_or_default(),
                    m.value as u64,
                )
            })
            .collect()
    }

    /// Before the first walk there is no series at all: a zero here would read
    /// as a cache that is empty, and a cap checked against it would never fire.
    #[tokio::test]
    async fn nothing_is_published_before_a_walk_completes() {
        let readings = BuildCacheReadings::new("/nonexistent-cache");
        assert!(readings.collect_metrics("").await.unwrap().is_empty());
        assert!(readings.measured().is_none());
        assert!(readings.scan_age_secs(unix_now()).is_none());
    }

    /// Every layer keeps its own series, and `other` is published as a zero so
    /// "nothing was folded" is a reading rather than a missing series.
    #[tokio::test]
    async fn each_layer_is_published_and_nothing_is_folded_under_the_cap() {
        let readings = readings_with(&[("debug/deps", 100), ("debug/incremental", 30), ("tmp", 1)]);
        let mut layers: Vec<(String, u64)> = published(&readings)
            .await
            .into_iter()
            .map(|(layer, _, bytes)| (layer, bytes))
            .collect();
        layers.sort();
        assert_eq!(
            layers,
            vec![
                ("debug/deps".to_string(), 100),
                ("debug/incremental".to_string(), 30),
                ("other".to_string(), 0),
                ("tmp".to_string(), 1),
            ]
        );
    }

    /// Past the cap the smallest layers fold into `other` and the total is
    /// unchanged: bounding the series count must not lose bytes, because the
    /// total is what a cap is checked against.
    #[tokio::test]
    async fn layers_past_the_cap_fold_into_other_without_losing_bytes() {
        let many: Vec<(String, u64)> = (0..MAX_PUBLISHED_LAYERS + 3)
            .map(|i| (format!("layer-{i:02}"), (i as u64 + 1) * 10))
            .collect();
        let readings = BuildCacheReadings::new("/nonexistent-cache");
        readings.set_layers(many.iter().cloned().collect(), unix_now());

        let published = published(&readings).await;
        assert_eq!(published.len(), MAX_PUBLISHED_LAYERS + 1);
        let total: u64 = published.iter().map(|(_, _, bytes)| bytes).sum();
        assert_eq!(total, many.iter().map(|(_, bytes)| bytes).sum::<u64>());
        // The three smallest are the ones folded.
        let folded = published
            .iter()
            .find(|(layer, _, _)| layer == OTHER_LAYER)
            .map(|(_, _, bytes)| *bytes)
            .unwrap();
        assert_eq!(folded, 10 + 20 + 30);
        for i in 0..3 {
            assert!(
                !published
                    .iter()
                    .any(|(layer, _, _)| *layer == format!("layer-{i:02}")),
                "layer-{i:02} was among the smallest and should be folded"
            );
        }
    }

    /// A walk that fails keeps the last measurement instead of publishing a
    /// smaller one, and the age is what says the number is not fresh.
    #[tokio::test]
    async fn the_last_measurement_survives_a_failed_walk_and_its_age_is_readable() {
        let readings = BuildCacheReadings::new("/nonexistent-cache");
        let scanned_at = unix_now().saturating_sub(120);
        readings.set_layers(
            [("debug/deps".to_string(), 7)].into_iter().collect(),
            scanned_at,
        );

        assert_eq!(
            readings.measured().unwrap().get("debug/deps").copied(),
            Some(7)
        );
        assert_eq!(readings.scan_age_secs(scanned_at + 5), Some(5));
    }

    /// The directory label is the one the build gate publishes, so a cache
    /// reading and a slot reading can be joined to the same directory.
    #[tokio::test]
    async fn the_directory_label_names_the_directory_and_not_the_path() {
        let dir = std::env::temp_dir();
        let readings = BuildCacheReadings::new(&dir);
        readings.set_layers([(".".to_string(), 1)].into_iter().collect(), unix_now());
        let published = published(&readings).await;
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].1, dir_identity(&dir));
        assert_ne!(published[0].1, dir.display().to_string());
    }

    /// A gate in force, over its own slot directory.
    fn gate(lock_dir: &Path, slots: usize, enabled: bool) -> Arc<BuildGate> {
        Arc::new(BuildGate::new(&cog_core::config::BuildGateConfig {
            enabled,
            max_concurrent: slots,
            wait_secs: 0,
            lock_dir: lock_dir.display().to_string(),
        }))
    }

    /// A cache on disk: `(relative path, bytes)` under a fresh directory.
    fn cache(files: &[(&str, usize)]) -> (tempfile::TempDir, Vec<FileEntry>) {
        let root = tempfile::tempdir().unwrap();
        for (rel, bytes) in files {
            let path = root.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, vec![b'x'; *bytes]).unwrap();
        }
        let entries = fs_size::dir_files(root.path(), CACHE_LAYER_DEPTH, &[]).unwrap();
        (root, entries)
    }

    fn on_disk(root: &Path) -> u64 {
        fs_size::dir_size_bytes(root, &[]).unwrap()
    }

    /// Wait until the gate itself reports the slot as free.
    ///
    /// Dropping a permit closes this process's handle, but a `flock` is held by the
    /// open file description rather than by the handle: any child another test in
    /// this binary forked while the permit was open carries a copy of that
    /// description until it execs, so for that window the gate keeps refusing a slot
    /// that no build holds. The refusal path is asserted where it belongs — the
    /// first half of this test and the gate's own tests — while this test is about
    /// what a free gate does, so it waits for the gate to say free instead of
    /// trusting that releasing the handle released the lock.
    async fn wait_until_the_slot_is_free(gate: &Arc<BuildGate>) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(permit) = gate.try_acquire("waiting for the slot to come back").await {
                drop(permit);
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the slot did not come back after the permit was dropped"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    async fn reading(
        readings: &BuildCacheReadings,
        metric: &str,
        outcome: Option<&str>,
    ) -> Option<f64> {
        readings
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.name == metric
                    && outcome
                        .is_none_or(|o| m.labels.get(OUTCOME_LABEL).map(String::as_str) == Some(o))
            })
            .map(|m| m.value)
    }

    /// With a cap set and the slot free, the cache is brought under it: the
    /// bytes actually leave the disk, and the pass is readable as having done
    /// it.
    #[tokio::test]
    async fn a_cache_over_its_cap_is_brought_under_it() {
        let lock = tempfile::tempdir().unwrap();
        let (root, files) = cache(&[
            ("release/incremental/one", 1000),
            ("release/incremental/two", 1000),
            ("release/incremental/three", 1000),
            ("release/deps/lib.rlib", 1000),
        ]);
        let readings = BuildCacheReadings::new(root.path()).with_cap(3000, 300);

        readings
            .enforce_cap(&files, Some(&gate(lock.path(), 1, true)))
            .await;

        assert_eq!(on_disk(root.path()), 3000, "the excess must leave the disk");
        assert_eq!(
            reading(&readings, BUILD_TARGET_OVER_LIMIT_METRIC, None).await,
            Some(1000.0)
        );
        assert_eq!(
            reading(&readings, BUILD_TARGET_UNMET_METRIC, None).await,
            Some(0.0),
            "a pass that reached the cap has nothing left unmet"
        );
        assert_eq!(
            reading(
                &readings,
                BUILD_TARGET_OVER_CAP_METRIC,
                Some(OUTCOME_RECLAIMED)
            )
            .await,
            Some(1.0)
        );
        assert_eq!(
            reading(&readings, BUILD_TARGET_RECLAIMED_METRIC, None).await,
            Some(1000.0)
        );
        // The compiled result was not touched while the speed layer could pay.
        assert!(root.path().join("release/deps/lib.rlib").exists());
    }

    /// A build holding the slot is not an error and not a deletion: the pass
    /// waits for the next walk, and says which of the two it was.
    #[tokio::test]
    async fn a_build_in_flight_defers_the_pass_rather_than_deleting() {
        let lock = tempfile::tempdir().unwrap();
        let gate = gate(lock.path(), 1, true);
        let held = gate.try_acquire("a build").await.unwrap();
        let (root, files) = cache(&[
            ("release/incremental/one", 1000),
            ("release/incremental/two", 1000),
            ("release/incremental/three", 1000),
        ]);
        let readings = BuildCacheReadings::new(root.path()).with_cap(2000, 300);

        readings.enforce_cap(&files, Some(&gate)).await;

        assert_eq!(on_disk(root.path()), 3000, "nothing may be removed");
        assert_eq!(
            reading(&readings, BUILD_TARGET_OVER_CAP_METRIC, Some(OUTCOME_BUSY)).await,
            Some(1.0)
        );
        assert_eq!(
            reading(&readings, BUILD_TARGET_UNMET_METRIC, None).await,
            None,
            "no pass ran, so there is no result to report"
        );
        drop(held);
        wait_until_the_slot_is_free(&gate).await;

        readings.enforce_cap(&files, Some(&gate)).await;
        assert_eq!(on_disk(root.path()), 2000, "the next free pass does it");
        assert_eq!(
            reading(
                &readings,
                BUILD_TARGET_OVER_CAP_METRIC,
                Some(OUTCOME_RECLAIMED)
            )
            .await,
            Some(1.0)
        );
    }

    /// No gate is no permission to delete: without the slot there is no fact
    /// saying a build is not reading this cache, and a file removed under a
    /// build fails that build for a reason that is not its own.
    #[tokio::test]
    async fn nothing_is_removed_without_a_gate_in_force() {
        let lock = tempfile::tempdir().unwrap();
        let (root, files) = cache(&[("release/incremental/one", 1000)]);
        let readings = BuildCacheReadings::new(root.path()).with_cap(1, 300);

        readings.enforce_cap(&files, None).await;
        readings
            .enforce_cap(&files, Some(&gate(lock.path(), 1, false)))
            .await;

        assert_eq!(on_disk(root.path()), 1000);
        assert_eq!(
            reading(
                &readings,
                BUILD_TARGET_OVER_CAP_METRIC,
                Some(OUTCOME_UNGATED)
            )
            .await,
            Some(2.0)
        );
        assert_eq!(
            reading(&readings, BUILD_TARGET_RECLAIMED_METRIC, None).await,
            Some(0.0)
        );
    }

    /// A cap the cache cannot be brought under — one below what the per-layer
    /// floor keeps — is reported as unmet rather than retried forever in
    /// silence, and the floor's file is still on disk.
    #[tokio::test]
    async fn a_cap_under_the_floor_is_reported_as_unmet() {
        let lock = tempfile::tempdir().unwrap();
        let (root, files) = cache(&[("release/incremental/one", 1000)]);
        let readings = BuildCacheReadings::new(root.path()).with_cap(1, 300);

        readings
            .enforce_cap(&files, Some(&gate(lock.path(), 1, true)))
            .await;

        assert_eq!(on_disk(root.path()), 1000, "the floor holds the only file");
        assert_eq!(
            reading(&readings, BUILD_TARGET_UNMET_METRIC, None).await,
            Some(999.0),
            "the 1 byte of the cap is reachable; the floor file's other 999 are not"
        );
        assert_eq!(
            reading(&readings, BUILD_TARGET_OVER_CAP_METRIC, Some(OUTCOME_UNMET)).await,
            Some(1.0)
        );
    }

    /// With no cap configured nothing is ever removed, and the cap family is not
    /// published at all: a cache with no cap must not read as a cache of zero
    /// bytes that is inside it.
    #[tokio::test]
    async fn an_uncapped_cache_is_measured_and_left_alone() {
        let lock = tempfile::tempdir().unwrap();
        let (root, files) = cache(&[("release/incremental/one", 1000)]);
        let readings = BuildCacheReadings::new(root.path());

        readings
            .enforce_cap(&files, Some(&gate(lock.path(), 1, true)))
            .await;

        assert_eq!(on_disk(root.path()), 1000);
        let metrics = readings.collect_metrics("").await.unwrap();
        assert!(
            !metrics
                .iter()
                .any(|m| m.name.contains("_cap") || m.name.contains("reclaim")),
            "{:?}",
            metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
        );
    }

    /// The whole family is published with a cap configured, including the
    /// outcomes that have not happened, so a reader sees the domain with zeros
    /// rather than inferring it from what occurred.
    #[tokio::test]
    async fn the_cap_family_is_published_with_its_zeros() {
        let root = tempfile::tempdir().unwrap();
        let readings = BuildCacheReadings::new(root.path()).with_cap(4096, 300);

        let metrics = readings.collect_metrics("").await.unwrap();
        let names: Vec<&str> = metrics.iter().map(|m| m.name.as_str()).collect();
        for expected in [
            BUILD_TARGET_CAP_METRIC,
            BUILD_TARGET_SCAN_INTERVAL_METRIC,
            BUILD_TARGET_RECLAIMED_METRIC,
            BUILD_TARGET_LAST_RECLAIM_METRIC,
        ] {
            assert!(
                names.contains(&expected),
                "{expected} missing from {names:?}"
            );
        }
        // No walk has happened, so nothing claims to have measured the cache.
        assert!(!names.contains(&BUILD_TARGET_OVER_LIMIT_METRIC));
        assert!(!names.contains(&BUILD_TARGET_BYTES_METRIC));
        let outcomes: Vec<&str> = metrics
            .iter()
            .filter(|m| m.name == BUILD_TARGET_OVER_CAP_METRIC)
            .map(|m| {
                m.labels
                    .get(OUTCOME_LABEL)
                    .map(String::as_str)
                    .unwrap_or("")
            })
            .collect();
        assert_eq!(outcomes.len(), RECLAIM_OUTCOMES.len(), "{outcomes:?}");
        for value in RECLAIM_OUTCOMES {
            assert!(
                outcomes.contains(value),
                "{value} missing from {outcomes:?}"
            );
        }
    }
}
