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
//! - `cogneva_build_target_over_cap_seconds{dir}` -- how long the cache has been
//!   over its cap, as a run of walks that each found it over; 0 while it is under.
//!   The duration a rule needs, and not the same series as the age of the last
//!   pass: those two are equal only while the cache is over its cap without
//!   interruption, and reading one for the other turns "no pass in six intervals"
//!   -- which a cache that was under its cap for most of them earns by being
//!   fine -- into a claim that the cache has been over its cap for six intervals.
//!   A run bounded this way is also what a restart can honestly report: the first
//!   walk of a process that finds the cache over starts a run at that moment.
//! - `cogneva_build_target_reclaimed_bytes_total{dir}` -- bytes removed so far.
//! - `cogneva_build_target_last_reclaim_seconds{dir}` -- when a pass last ran,
//!   and 0 when none has. A pass that never runs has to age somewhere, or a
//!   cache over its cap reads the same as one being fixed; the epoch ages it
//!   from something true, where the process start would age it from something
//!   that has not happened. How long ago that was is *not* how long the cache
//!   has been over its cap -- a cache that spent most of that time under its cap
//!   earns the same age by being fine -- and the series that answers that
//!   question is `over_cap_seconds` above.
//! - `cogneva_build_target_scan_interval_seconds{dir}` -- the configured
//!   interval, so a rule can say "no pass in six intervals" without a constant
//!   that goes stale when the interval is configured differently.
//!
//! Only the process that owns the directory publishes it, which is the process
//! that runs builds: a deployment with no builder has no cache to report rather
//! than a cache of zero bytes, and the series carry the directory's identity
//! rather than its path, so nothing here can be read as a sum over two caches
//! that happen to share a path.
//!
//! **Two processes here run builds, and they share no lock.** The one that runs
//! a deployment's own builds has the host-wide build gate; the sandbox executor
//! builds into a target directory on a volume of its own, holds no slot in that
//! gate and cannot take one -- it is the pod with no mounted secrets and no
//! host path to the slot directory. What the pass needs from a process's own
//! account of "a build is running" is the same in both cases, and that is what
//! [`CacheSlot`] abstracts: whether such an account is in force, and something
//! to hold for the duration of a deletion. The readings and the pass are here
//! rather than beside either process because the *meaning* of these series has
//! one author: a second implementation of "what `unmet` means" would be a second
//! reading of the same name.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tracing::{info, warn};

use crate::build_gate::{dir_identity, BuildGate};
use crate::fs_size::{self, FileEntry};
use crate::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use crate::{SFResult, ShutdownSignal};

// The names, labels and depths this family travels under live beside the
// readings, so both publishers of it read one copy: see `crate::build_cache`.
pub use crate::build_cache::*;

/// The fact that says whether anything is currently building into a cache.
///
/// A passing deletion needs more than a measurement of the cache: it needs
/// something it can *hold* for the whole of the deletion, or the deletion it
/// authorises can begin a moment before a build that is about to read the same
/// files. What that fact is differs per process (see the module doc), and what
/// the pass needs from it does not.
#[async_trait]
pub trait CacheSlot: Send + Sync {
    /// Whether anything bounds the builds into this cache at all.
    ///
    /// False means nothing may be removed: a cap with no such fact behind it is
    /// reported as [`OUTCOME_UNGATED`] rather than enforced against a guess.
    fn in_force(&self) -> bool;

    /// Take the slot for one whole pass, or `None` while a build holds it.
    ///
    /// The guard is held across the deletion and released by dropping it. An
    /// implementation that cannot tell whether a build is running returns `None`
    /// forever rather than handing one out -- refusing to delete is the only
    /// safe answer to "I do not know", and it is counted as [`OUTCOME_BUSY`].
    async fn take(&self) -> Option<Box<dyn Send + Sync>>;
}

/// The host-wide build gate *is* the slot for the process that runs a
/// deployment's own builds: it is what bounds how many of them run at once, and
/// a pass excludes itself from all of them rather than from one, because the
/// files it deletes are ones any build could be reading.
#[async_trait]
impl CacheSlot for Arc<BuildGate> {
    fn in_force(&self) -> bool {
        self.as_ref().in_force()
    }

    async fn take(&self) -> Option<Box<dyn Send + Sync>> {
        match self.try_acquire_exclusive("build-cache-reclaim").await {
            Ok(permit) => Some(Box::new(permit)),
            Err(_) => None,
        }
    }
}

/// What the last pass left behind.
#[derive(Debug, Default, Clone, Copy)]
struct ReclaimState {
    /// Bytes above the cap as of the last walk; `None` before one has been made
    /// with a cap configured.
    over_limit: Option<u64>,
    /// Bytes above the cap after the last pass that ran; `None` before one has.
    unmet: Option<u64>,
    /// When the run of walks that found the cache over its cap began; `None`
    /// when the last walk found it under, and before a first walk that did not.
    ///
    /// A run of walks rather than of passes: a pass that freed bytes and left the
    /// cache over does not restart it, because the question this answers is how
    /// long the cap has been unmet, and a pass that did not meet it did not
    /// change that answer.
    over_since: Option<u64>,
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
    /// Unix seconds of the last pass that ran, and 0 until one does.
    ///
    /// A pass that never runs still has to age against a clock a rule can read --
    /// were this absent until the first pass, "no pass has ever run" would be the
    /// one state with no reading at all -- and the epoch is the one instant a pass
    /// cannot have run at. Seeding it with the process start instead publishes a
    /// pass that did not happen: a fresh process reads as "reclaimed 0s ago" while
    /// `reclaimed_bytes_total` is still 0, and the stalled rule stays quiet for
    /// the first six intervals of every process, which is exactly the window a
    /// process that never finds a free slot spends over its cap.
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
            last_pass_at: AtomicU64::new(0),
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

    /// One pass: walk the cache, publish what the walk found, and bring it under
    /// the cap.
    ///
    /// One walk, not two: the entries are what the cap is enforced against and
    /// the layers are what is published, and a plan built from a different
    /// snapshot than the published total would be enforcing a figure nobody can
    /// see. The walk is off the runtime -- it is metadata-only, but it is a walk
    /// of a large tree and it must not hold up the cycles that build into it.
    ///
    /// A walk that fails keeps the last measurement rather than publishing a
    /// smaller one, and says so: a total that is too small can only silence a
    /// cap, and the age of the reading is what tells a stale number from a fresh
    /// one.
    pub async fn measure_and_enforce(&self, slot: Option<&dyn CacheSlot>) {
        let path = self.dir.clone();
        let walked =
            tokio::task::spawn_blocking(move || fs_size::dir_files(&path, CACHE_LAYER_DEPTH, &[]))
                .await;
        match walked {
            Ok(Ok(files)) => {
                self.set_layers(fs_size::layer_totals(&files), unix_now());
                self.enforce_cap(&files, slot).await;
            }
            Ok(Err(e)) => warn!(
                error = %e,
                dir = %self.dir.display(),
                "build cache scan failed; keeping the last measurement"
            ),
            Err(e) => warn!(error = %e, "build cache scan task panicked"),
        }
    }

    /// Bring the cache back under its cap, if it is over one and a slot can be
    /// taken.
    ///
    /// `files` are the entries of the walk that produced the measurement being
    /// published, so the total the cap is judged against and the plan that acts
    /// on it come from one snapshot. Nothing is removed without holding `slot`:
    /// a file deleted under a running build fails that build for a reason that
    /// has nothing to do with it, and that failure would be recorded against a
    /// change rather than against the host.
    pub async fn enforce_cap(&self, files: &[FileEntry], slot: Option<&dyn CacheSlot>) {
        let Some(cap) = self.cap_bytes() else {
            return;
        };
        let total = fs_size::counted_bytes(files);
        let excess = total.saturating_sub(cap);
        {
            let mut state = self.reclaim.lock().unwrap_or_else(|e| e.into_inner());
            state.over_limit = Some(excess);
            // What ends the run is a walk that finds the cache under its cap, and
            // only that: an over-cap walk leaves the beginning where it was
            // whether or not a pass ran during it, because a pass that freed
            // bytes and stayed over has not ended anything a reader asked about.
            if excess == 0 {
                state.over_since = None;
            } else if state.over_since.is_none() {
                state.over_since = Some(unix_now());
            }
        }
        if excess == 0 {
            return;
        }

        // The slot is what makes "no build is running" a fact rather than a hope.
        // Where nothing says so, the cap is reported as unenforceable instead of
        // being enforced against a guess.
        let Some(slot) = slot.filter(|slot| slot.in_force()) else {
            self.count_outcome(OUTCOME_UNGATED);
            warn!(
                dir = %self.dir.display(),
                over_limit_bytes = excess,
                "build cache is over its cap and nothing says whether a build is running, so nothing was removed"
            );
            return;
        };
        let Some(permit) = slot.take().await else {
            // A build holds the slot. Not a failure: the cache is over its cap
            // while the host is busy building into it, and the next walk will
            // try again. It is counted separately because a cache that is
            // *never* reclaimed and one that cannot be reclaimed call for
            // different things.
            self.count_outcome(OUTCOME_BUSY);
            info!(
                dir = %self.dir.display(),
                over_limit_bytes = excess,
                "build cache is over its cap; a build holds the slot, so the pass waits for the next walk"
            );
            return;
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
        // Published whether or not the cache is over, so that a rule reading it
        // has a number in both states rather than a series that appears when the
        // cache goes over -- which a rule would read as the same thing as a walk
        // that never happened. Zero rather than absent is also what makes "under
        // its cap" sayable: `over_limit_bytes` publishes 0 for that state too,
        // and a reader comparing the two would otherwise have to treat a missing
        // series as a satisfied cap.
        out.push(
            RawMetric::new(
                BUILD_TARGET_OVER_CAP_SECS_METRIC,
                state
                    .over_since
                    .map(|since| unix_now().saturating_sub(since))
                    .unwrap_or(0) as f64,
            )
            .with_label(DIR_LABEL, dir),
        );
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
            crate::build_cache::published_layers(&layers)
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
pub fn spawn_build_cache_watch(
    readings: Arc<BuildCacheReadings>,
    shutdown: ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let dir = readings.dir().to_path_buf();
    let interval = Duration::from_secs(readings.scan_interval_secs());
    // Every series this watcher publishes is its own measurement — the size, the
    // cap it is held to, what a pass reclaimed. If the task died, all of them
    // stop being written, and a cache nobody is measuring reads exactly like a
    // cache that is small. Its liveness therefore cannot come from itself.
    crate::loop_health::spawn(
        BUILD_CACHE_WATCH_LOOP,
        crate::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let readings = Arc::clone(&readings);
            let dir = dir.clone();
            let shutdown = shutdown.clone();
            async move {
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
                            // Read per pass rather than remembered: the gate is
                            // installed during startup and the readings object is
                            // built beside it, so a slot captured once could be
                            // the empty one and the cap would then be reported as
                            // unenforceable for the life of the process.
                            let slot = crate::build_gate::global();
                            readings
                                .measure_and_enforce(slot.as_ref().map(|g| g as &dyn CacheSlot))
                                .await;
                        }
                    }
                }
            }
        },
    )
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
        Arc::new(BuildGate::new(&crate::config::BuildGateConfig {
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

    /// Run a pass on a free slot, retrying while the gate refuses one.
    ///
    /// Dropping a permit closes this process's handle, but a `flock` is held by
    /// the open file description rather than by the handle: a child another test
    /// in this binary forked while the permit was open carries a copy of that
    /// description until it execs, so the gate keeps refusing a slot that no
    /// build holds for as long as that child is around. A slot is therefore free
    /// at an instant, and the only thing that can read the instant it needs is
    /// the pass itself -- waiting for a free slot and then asking for one leaves
    /// the window the wait opened between the two. Retrying is also what a
    /// caller in a deployment does: a pass that finds the gate busy defers to
    /// the next walk. A gate that refuses for ten seconds fails here by name and
    /// count rather than as a byte total that did not move.
    async fn run_a_pass_when_the_slot_is_free(
        readings: &BuildCacheReadings,
        files: &[FileEntry],
        gate: &Arc<BuildGate>,
    ) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let refused_before =
                reading(readings, BUILD_TARGET_OVER_CAP_METRIC, Some(OUTCOME_BUSY)).await;
            readings.enforce_cap(files, Some(gate)).await;
            let refused_after =
                reading(readings, BUILD_TARGET_OVER_CAP_METRIC, Some(OUTCOME_BUSY)).await;
            if refused_after == refused_before {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the gate refused every pass for ten seconds: busy {refused_before:?} -> {refused_after:?}"
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
        run_a_pass_when_the_slot_is_free(&readings, &files, &gate).await;
        // Named in the message because it is the other half of the reading: a
        // pass that ran and freed nothing looks from the byte total alone like
        // a pass that never ran.
        let unmet = reading(&readings, BUILD_TARGET_UNMET_METRIC, None).await;
        assert_eq!(
            on_disk(root.path()),
            2000,
            "the next free pass does it (bytes over the cap after it: {unmet:?})"
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
    }

    /// No slot is no permission to delete: with nothing saying whether a build
    /// is reading this cache -- no slot at all, or one that is not in force --
    /// a file removed under a build fails that build for a reason that is not
    /// its own.
    #[tokio::test]
    async fn nothing_is_removed_without_a_slot_in_force() {
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

    /// How long the cache has been over its cap is a run of walks, and what
    /// ends it is a walk that found the cache under. A pass does not: a cache
    /// that has been over its cap for an hour reads that way whether or not a
    /// pass paid part of it down during the hour, and the rule that fires on
    /// this reads the walk's own count of it rather than the age of the last
    /// pass, which a cache that spent the hour under its cap has too.
    #[tokio::test]
    async fn the_over_cap_run_is_ended_by_a_walk_under_the_cap_and_not_by_a_pass() {
        let lock = tempfile::tempdir().unwrap();
        let gate = gate(lock.path(), 1, true);
        let (root, over) = cache(&[("release/incremental/one", 4000)]);
        let readings = BuildCacheReadings::new(root.path()).with_cap(1000, 300);

        // The reading moves with the clock, so a second can pass between the
        // walk and the read; what is under test is which moment the run is
        // measured from, not the wall clock's resolution.
        let secs_over = |value: Option<f64>| value.expect("published with a cap configured");

        run_a_pass_when_the_slot_is_free(&readings, &over, &gate).await;
        assert!(
            secs_over(reading(&readings, BUILD_TARGET_OVER_CAP_SECS_METRIC, None).await) < 5.0,
            "the run begins at the walk that found the cache over"
        );

        // What a run that has been going a while reads as, without waiting an
        // hour for it: the beginning is the only state this carries, so moving
        // the beginning moves the reading by the same amount.
        readings.reclaim.lock().unwrap().over_since = Some(unix_now().saturating_sub(3600));
        assert!(
            secs_over(reading(&readings, BUILD_TARGET_OVER_CAP_SECS_METRIC, None).await) >= 3600.0
        );

        // The same over-cap cache, walked again with the slot free. Whatever the
        // pass freed, the cache is still over, and the run is where it was: a
        // pass is not a walk that found the cache under its cap.
        run_a_pass_when_the_slot_is_free(&readings, &over, &gate).await;
        assert!(
            secs_over(reading(&readings, BUILD_TARGET_OVER_CAP_SECS_METRIC, None).await) >= 3600.0,
            "a pass that ran is not a walk that found the cache under"
        );

        // A walk that finds nothing in the cache ends the run, and says so with a
        // zero rather than by going absent.
        readings.enforce_cap(&[], Some(&gate)).await;
        assert_eq!(
            reading(&readings, BUILD_TARGET_OVER_CAP_SECS_METRIC, None).await,
            Some(0.0),
            "under its cap is a run of length zero"
        );

        // And the next walk that finds it over starts a new run rather than
        // resuming the one that ended.
        run_a_pass_when_the_slot_is_free(&readings, &over, &gate).await;
        assert!(
            secs_over(reading(&readings, BUILD_TARGET_OVER_CAP_SECS_METRIC, None).await) < 5.0,
            "a new run begins at the walk that found it over again"
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
            BUILD_TARGET_OVER_CAP_SECS_METRIC,
        ] {
            assert!(
                names.contains(&expected),
                "{expected} missing from {names:?}"
            );
        }
        // No walk has happened, so nothing claims to have measured the cache.
        assert!(!names.contains(&BUILD_TARGET_OVER_LIMIT_METRIC));
        assert!(!names.contains(&BUILD_TARGET_BYTES_METRIC));
        // No pass has happened either, and this series says when one last did.
        // The epoch is the answer to "never"; the process start is not an answer
        // to that question at all, and it is the plausible one -- a reader (or the
        // stalled rule) takes a start-seeded value for a pass that just paid.
        assert_eq!(
            reading(&readings, BUILD_TARGET_LAST_RECLAIM_METRIC, None).await,
            Some(0.0)
        );
        // Present before a walk and reading zero, so that a rule has a number in
        // both states rather than a series that appears when the cache goes over
        // -- which would be the same shape as a cache nothing ever walked. Zero
        // here means "no walk has found it over", and only a walk can start a
        // run, because whether the cache is over its cap is not a thing this
        // process knows between walks.
        assert_eq!(
            reading(&readings, BUILD_TARGET_OVER_CAP_SECS_METRIC, None).await,
            Some(0.0)
        );
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
