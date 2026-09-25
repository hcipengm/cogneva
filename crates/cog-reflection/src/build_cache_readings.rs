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
//!   space whose cost to lose is a slower next build.
//! - `cogneva_build_target_bytes_scan_age_seconds` -- how long ago the cache was
//!   measured. The sizes keep their last reading when a walk fails, which is the
//!   right thing to publish and is also invisible: a scan loop that died leaves
//!   behind a cache that reads as unchanging rather than as unmeasured.
//!
//! Nothing is capped here yet -- this is the reading a cap will be held against,
//! and the cap belongs with the code that removes bytes, not with the reading.
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
use cog_core::build_gate::dir_identity;
use cog_core::fs_size;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};
use tracing::{info, warn};

/// Apparent bytes the cache holds, per layer.
pub const BUILD_TARGET_BYTES_METRIC: &str = "cogneva_build_target_bytes";

/// Seconds since the cache was last measured.
pub const BUILD_TARGET_SCAN_AGE_METRIC: &str = "cogneva_build_target_bytes_scan_age_seconds";

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
}

impl BuildCacheReadings {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            layers: Mutex::new(BTreeMap::new()),
            scanned_at: AtomicU64::new(0),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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
        let Some(layers) = self.measured() else {
            return Ok(Vec::new());
        };
        let dir = dir_identity(&self.dir);
        let mut out: Vec<RawMetric> = self
            .published_layers(&layers)
            .into_iter()
            .map(|(layer, bytes)| {
                RawMetric::new(BUILD_TARGET_BYTES_METRIC, bytes as f64)
                    .with_label(DIR_LABEL, dir.clone())
                    .with_label(LAYER_LABEL, layer)
            })
            .collect();
        if let Some(age) = self.scan_age_secs(unix_now()) {
            out.push(
                RawMetric::new(BUILD_TARGET_SCAN_AGE_METRIC, age as f64).with_label(DIR_LABEL, dir),
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

/// Re-measure the cache on a timer and publish the result on `readings`.
pub async fn run_build_cache_watch(
    readings: Arc<BuildCacheReadings>,
    interval_secs: u64,
    shutdown: ShutdownSignal,
) {
    let dir = readings.dir().to_path_buf();
    let interval = Duration::from_secs(interval_secs.max(MIN_SCAN_INTERVAL_SECS));
    info!(
        dir = %dir.display(),
        interval_secs = interval.as_secs(),
        depth = CACHE_LAYER_DEPTH,
        metric = BUILD_TARGET_BYTES_METRIC,
        "build cache watcher started"
    );

    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                let path = dir.clone();
                // Off the runtime: the walk is metadata-only but it is a walk of
                // a large tree, and it must not hold up the cycles that build
                // into this cache.
                let walked = tokio::task::spawn_blocking(move || {
                    fs_size::dir_layers(&path, CACHE_LAYER_DEPTH, &[])
                })
                .await;
                match walked {
                    Ok(Ok(layers)) => readings.set_layers(layers, unix_now()),
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
}
