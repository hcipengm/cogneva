//! The node's container image store, measured by walking it once per cadence.
//!
//! This is the cell of the resource account that no other reading reaches. The
//! kubelet reports per-volume usage for the claims it mounts, but the layers a
//! container image expands into are not a volume: containerd keeps them under its
//! own root, and every byte of them is spent on the node's filesystem. Before
//! this, the account could say how full the disk was and how much of it each
//! claim held, and the difference between those two numbers had no name.
//!
//! The walk is the only honest reading here. `crictl` and the containerd API
//! report a per-image *compressed* size, which is not what the disk carries after
//! unpacking, and they report nothing for layers shared between images. What the
//! disk actually holds is the sum of the files under the store's own directories,
//! so that is what is measured.
//!
//! Every top-level entry of the store is published as its own `part` rather than
//! as one total: content and snapshots are separately reclaimable, and a store
//! whose total is flat while its parts move is a different story from one that is
//! simply growing. A part that appears after an upgrade shows up as a new series
//! without this code changing.
//!
//! A part that cannot be walked keeps its last measurement and is counted as not
//! readable, the same as a claim directory that cannot be walked: the number is
//! published with its age beside it, so a stale measurement is visible as stale
//! rather than as a store that stopped growing.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cog_core::claim_footprint::ClaimFootprint;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};

/// Bytes one part of the store holds.
pub const BYTES_METRIC: &str = "cogneva_image_store_bytes";

/// Label naming which part a sample is about.
pub const PART_LABEL: &str = "part";

/// How many parts the last scan found.
pub const PARTS_METRIC: &str = "cogneva_image_store_parts";

/// How many of them were walked successfully.
pub const READABLE_METRIC: &str = "cogneva_image_store_readable";

/// Seconds since the last completed scan, or since process start before one.
pub const AGE_METRIC: &str = "cogneva_image_store_age_seconds";

/// Label carrying the node, as on every other node-scoped reading.
pub const NODE_LABEL: &str = "node";

/// Root the store is expected at inside the container.
pub const DEFAULT_ROOT: &str = "/host/containerd";

/// Environment variable naming the store root.
pub const ROOT_ENV: &str = "COGNEVA_IMAGE_STORE_ROOT";

/// Environment variable naming this node.
pub const NODE_ENV: &str = "COGNEVA_NODE_NAME";

/// Environment variable naming the re-walk cadence.
pub const INTERVAL_ENV: &str = "COGNEVA_IMAGE_SCAN_INTERVAL_SECS";

/// Cadence a walk runs at unless the deployment says otherwise.
///
/// Slower than the claim scan because this tree is much larger and changes on the
/// scale of image pulls, not of a workload writing a file.
pub const DEFAULT_INTERVAL_SECS: u64 = 900;

/// Fastest cadence a walk may run at.
pub const MIN_INTERVAL_SECS: u64 = 60;

/// Prefix of this scanner's name in the loop liveness census.
pub const SCAN_LOOP: &str = "image_store_scan";

/// What one walk found, so the caller can log the same numbers it publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanReport {
    /// Top-level entries the store holds.
    pub parts: usize,
    /// How many of them the walk succeeded on.
    pub readable: usize,
    /// Entries that are not directories, measured by size instead.
    pub files: usize,
}

/// Whether the store root itself could be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    Completed(ScanReport),
    RootUnreadable,
}

struct Store {
    /// One footprint per top-level entry, keyed by the entry's name.
    ///
    /// Kept across scans for the same reason as a claim footprint: a failed walk
    /// must leave the last measurement in place rather than publish a zero.
    parts: BTreeMap<String, Arc<ClaimFootprint>>,
    readable: usize,
    last_scan: Instant,
}

/// Walks a node's container image store and publishes its size per part.
pub struct ImageStoreObservable {
    root: PathBuf,
    node: String,
    store: Mutex<Store>,
    scans: AtomicU64,
}

impl ImageStoreObservable {
    pub fn new(root: impl Into<PathBuf>, node: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            node: node.into(),
            store: Mutex::new(Store {
                parts: BTreeMap::new(),
                readable: 0,
                last_scan: Instant::now(),
            }),
            scans: AtomicU64::new(0),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// How many walks have completed since this process started.
    pub fn scans(&self) -> u64 {
        self.scans.load(Ordering::Relaxed)
    }

    /// Seconds since the last completed walk, or since start before one.
    pub fn scan_age(&self) -> Duration {
        self.store.lock().unwrap().last_scan.elapsed()
    }

    /// Walk the store root once.
    ///
    /// Blocking, and called from the blocking pool: the walk costs what the store
    /// holds, so it must never run on a runtime thread a scrape is waiting on.
    pub fn scan(&self) -> Scan {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    root = %self.root.display(),
                    error = %e,
                    "image store root unreadable; keeping the last walk and letting its age grow"
                );
                return Scan::RootUnreadable;
            }
        };

        // The mount table of *this* process: a directory inside the store that is
        // itself a mount belongs to whatever is mounted there, and counting it
        // here would report bytes that are not the store's.
        let mounts = cog_core::claim_footprint::current_mounts();

        let mut found: BTreeMap<String, PathBuf> = BTreeMap::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                tracing::debug!("store entry with a non-UTF-8 name; it has no series");
                continue;
            };
            found.insert(name.to_string(), entry.path());
        }

        let mut store = self.store.lock().unwrap();
        // Retire what the root no longer holds before walking what it does: a
        // part deleted from disk must not be read back out of this process.
        store.parts.retain(|key, _| found.contains_key(key));

        let mut readable = 0usize;
        let mut files = 0usize;
        for (name, path) in &found {
            let footprint = store
                .parts
                .entry(name.clone())
                .or_insert_with(|| {
                    Arc::new(ClaimFootprint::new(
                        name.clone(),
                        path.clone(),
                        cog_core::claim_footprint::nested_mounts_under(path, &mounts),
                    ))
                })
                .clone();
            // A plain file at the top of the store is measured as itself: walking
            // it as a directory would fail and leave the part permanently
            // unreadable, which is a false claim about the store.
            if path.is_file() {
                files += 1;
                match std::fs::metadata(path) {
                    Ok(meta) => {
                        footprint.set_used_bytes(meta.len());
                        readable += 1;
                    }
                    Err(e) => tracing::warn!(
                        part = %name,
                        path = %path.display(),
                        error = %e,
                        "store file not statable; keeping the last measurement"
                    ),
                }
                continue;
            }
            match footprint.measure() {
                Ok(()) => readable += 1,
                Err(e) => tracing::warn!(
                    part = %name,
                    path = %path.display(),
                    error = %e,
                    "store part not walkable; keeping the last measurement"
                ),
            }
        }

        store.readable = readable;
        store.last_scan = Instant::now();
        drop(store);
        self.scans.fetch_add(1, Ordering::Relaxed);

        Scan::Completed(ScanReport {
            parts: found.len(),
            readable,
            files,
        })
    }

    /// Walk on the runtime's blocking pool.
    pub async fn scan_blocking(self: &Arc<Self>) -> Scan {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.scan())
            .await
            .unwrap_or(Scan::RootUnreadable)
    }
}

#[async_trait]
impl Observable for ImageStoreObservable {
    /// One series per part the last walk completed, plus the coverage of that
    /// walk and its age.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let store = self.store.lock().unwrap();
        let mut out = Vec::new();
        for (part, footprint) in &store.parts {
            // No measurement before the first successful walk: a zero would say
            // the part is empty, which is the claim this reading exists to
            // replace with evidence.
            let Some(bytes) = footprint.value() else {
                continue;
            };
            out.push(
                RawMetric::new(BYTES_METRIC, bytes as f64)
                    .with_label(PART_LABEL, part.clone())
                    .with_label(NODE_LABEL, self.node.clone()),
            );
        }
        let parts = store.parts.len() as f64;
        let readable = store.readable as f64;
        let age = store.last_scan.elapsed().as_secs_f64();
        drop(store);

        let node = self.node.as_str();
        out.push(RawMetric::new(PARTS_METRIC, parts).with_label(NODE_LABEL, node));
        out.push(RawMetric::new(READABLE_METRIC, readable).with_label(NODE_LABEL, node));
        out.push(RawMetric::new(AGE_METRIC, age).with_label(NODE_LABEL, node));
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Re-walk on a cadence, with the loop's own liveness reported beside it.
pub async fn run_image_store_scan(
    observable: Arc<ImageStoreObservable>,
    interval_secs: u64,
    shutdown: ShutdownSignal,
) {
    let interval = Duration::from_secs(interval_secs.max(MIN_INTERVAL_SECS));
    let loop_name = format!("{SCAN_LOOP}:{}", observable.node());
    let _ = cog_core::loop_health::spawn(
        loop_name,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        move |beat| {
            let observable = Arc::clone(&observable);
            let shutdown = shutdown.clone();
            async move {
                tracing::info!(
                    root = %observable.root().display(),
                    node = observable.node(),
                    interval_secs = interval.as_secs(),
                    "image store scan started"
                );
                let mut ticker = tokio::time::interval(interval);
                loop {
                    // Stamped whether or not the walk found anything: the age is
                    // a reading of the loop, and a loop that only stamped when it
                    // did something would report the state of the root as its
                    // own liveness.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            match observable.scan_blocking().await {
                                Scan::Completed(report) => tracing::debug!(
                                    parts = report.parts,
                                    readable = report.readable,
                                    files = report.files,
                                    "image store walked"
                                ),
                                Scan::RootUnreadable => {}
                            }
                        }
                    }
                }
            }
        },
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const NODE: &str = "node-a";

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cog-image-store-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(path: &Path, bytes: usize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    async fn readings(observable: &ImageStoreObservable) -> Vec<RawMetric> {
        observable.collect_metrics("").await.unwrap()
    }

    fn value_of(readings: &[RawMetric], name: &str, part: Option<&str>) -> Option<f64> {
        readings
            .iter()
            .find(|m| {
                m.name == name
                    && part.is_none_or(|want| {
                        m.labels.get(PART_LABEL).map(String::as_str) == Some(want)
                    })
            })
            .map(|m| m.value)
    }

    /// Each top-level entry of the store is its own reading, and the walk's
    /// coverage is published beside the sizes so a part it could not walk is
    /// visible as unreadable rather than as empty.
    #[tokio::test]
    async fn every_part_of_the_store_is_reported_with_the_coverage_of_the_walk() {
        let root = scratch("parts");
        write(
            &root.join("io.containerd.content.v1.content").join("blob"),
            4096,
        );
        write(
            &root
                .join("io.containerd.snapshotter.v1.overlayfs")
                .join("snap")
                .join("f"),
            8192,
        );

        let observable = ImageStoreObservable::new(&root, NODE);
        assert_eq!(
            observable.scan(),
            Scan::Completed(ScanReport {
                parts: 2,
                readable: 2,
                files: 0
            })
        );

        let out = readings(&observable).await;
        assert_eq!(
            value_of(&out, BYTES_METRIC, Some("io.containerd.content.v1.content")),
            Some(4096.0)
        );
        assert_eq!(
            value_of(
                &out,
                BYTES_METRIC,
                Some("io.containerd.snapshotter.v1.overlayfs")
            ),
            Some(8192.0)
        );
        assert_eq!(value_of(&out, PARTS_METRIC, None), Some(2.0));
        assert_eq!(value_of(&out, READABLE_METRIC, None), Some(2.0));
        assert!(value_of(&out, AGE_METRIC, None).is_some());
    }

    /// A part that is deleted from disk stops being reported: a series that
    /// outlives its directory would read as a store that still holds the bytes.
    #[tokio::test]
    async fn a_retired_part_stops_being_reported() {
        let root = scratch("retire");
        write(&root.join("content").join("blob"), 1024);
        let observable = ImageStoreObservable::new(&root, NODE);
        observable.scan();
        assert_eq!(
            value_of(&readings(&observable).await, BYTES_METRIC, Some("content")),
            Some(1024.0)
        );

        fs::remove_dir_all(root.join("content")).unwrap();
        observable.scan();
        let out = readings(&observable).await;
        assert!(value_of(&out, BYTES_METRIC, Some("content")).is_none());
        assert_eq!(value_of(&out, PARTS_METRIC, None), Some(0.0));
    }

    /// A store root that cannot be read is not a store of size zero: no part is
    /// published, the coverage says nothing was seen, and the age keeps counting
    /// from process start so a scanner that never walked is on the stale rule.
    #[tokio::test]
    async fn an_unreadable_root_publishes_coverage_and_no_size() {
        let observable = ImageStoreObservable::new(scratch("missing").join("nope"), NODE);
        assert_eq!(observable.scan(), Scan::RootUnreadable);
        let out = readings(&observable).await;
        assert!(value_of(&out, BYTES_METRIC, None).is_none());
        assert_eq!(value_of(&out, PARTS_METRIC, None), Some(0.0));
        assert_eq!(value_of(&out, READABLE_METRIC, None), Some(0.0));
        assert!(observable.scans() == 0);
    }

    fn chart_rule(name: &str) -> serde_json::Value {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        root.pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present")
            .iter()
            .find(|r| r["name"] == name)
            .unwrap_or_else(|| panic!("rule {name} missing"))
            .clone()
    }

    /// A reading only becomes an alert if a rule queries its exact name; a
    /// rename on either side leaves both halves internally consistent and the
    /// signal silently absent. Each series is pinned against its reader, and the
    /// file this module publishes from is where the contract test looks for it.
    #[test]
    fn every_reading_this_module_publishes_has_the_rule_that_reads_it() {
        for (metric, rule_name) in [
            (BYTES_METRIC, "image_store_bytes_high"),
            (PARTS_METRIC, "image_store_incomplete"),
            (READABLE_METRIC, "image_store_incomplete"),
            (AGE_METRIC, "image_store_scan_stale"),
        ] {
            let promql = chart_rule(rule_name)["promql"]
                .as_str()
                .expect("promql is a string")
                .to_string();
            assert!(
                promql.contains(metric),
                "rule {rule_name} must read {metric}, got: {promql}"
            );
        }
    }

    /// The byte bound has to sit below the disk-wide alert and above what a
    /// healthy store reaches, or it either duplicates a rule that already fires
    /// on the same disk or pages on every node the moment it is deployed.
    #[test]
    fn the_size_bound_sits_between_a_healthy_store_and_the_disk_wide_alert() {
        let rule = chart_rule("image_store_bytes_high");
        let threshold = rule["condition"]["greater_than"]
            .as_f64()
            .expect("greater_than is a number");
        let disk_high = chart_rule("node_disk_usage_high")["condition"]["greater_than"]
            .as_f64()
            .expect("greater_than is a number");
        assert!(
            (0.05..disk_high / 100.0).contains(&threshold),
            "size bound {threshold} is outside the band a measured store leaves"
        );
    }

    /// A plain file at the top of the store has a size like any other part;
    /// walking it as a directory would leave it permanently unreadable.
    #[tokio::test]
    async fn a_plain_file_at_the_top_is_measured_by_its_own_size() {
        let root = scratch("file");
        write(&root.join("io.containerd.metadata.v1.bolt"), 2048);
        let observable = ImageStoreObservable::new(&root, NODE);
        assert_eq!(
            observable.scan(),
            Scan::Completed(ScanReport {
                parts: 1,
                readable: 1,
                files: 1
            })
        );
        assert_eq!(
            value_of(
                &readings(&observable).await,
                BYTES_METRIC,
                Some("io.containerd.metadata.v1.bolt")
            ),
            Some(2048.0)
        );
    }
}
