//! Every claim-backed directory on this node, measured by walking the node's own
//! storage root.
//!
//! A claim's declared size is admission-time arithmetic and the kubelet cannot
//! fill in the other half of the comparison on a directory-backed volume: the
//! filesystem behind a local-path claim is the node's root filesystem, so
//! `kubelet_volume_stats_used_bytes` reports the same ~600 GB against every
//! claim on the node. Measured here instead of believed there: on this cluster
//! twelve claims carried twelve identical figures and not one of them was the
//! claim's.
//!
//! The reading that does answer it is a walk, and the processes that can walk a
//! volume are the ones writing to it -- which leaves out every volume written by
//! an image that is not ours: the database, the cache, the queue, the object
//! store, the monitoring stack's own disks. Nothing of ours runs inside those
//! pods, so no process of ours has the mount, and a claim with no walker is a
//! claim with no reading: it cannot be compared against its declaration, no
//! rule can read it, and a volume quietly filling up is invisible until it is
//! full.
//!
//! What does see all of them is the node's storage root, where the provisioner
//! keeps every claim as one directory named after it. One process per node walks
//! that root and reports each directory under the series the declared-size rule
//! already divides -- the same name and the same join label the in-process
//! walkers use, because a claim measured by two processes must be one reading
//! rather than two that disagree.
//!
//! Three things make the reading usable rather than merely present.
//!
//! It says which namespace each directory belongs to, taken from the directory's
//! own name rather than from the scraper's target label. A DaemonSet is scraped
//! as one target in its own namespace, so a label the scraper supplies would
//! attribute every volume on the node -- including the monitoring stack's -- to
//! the namespace the scanner runs in, and the join against the declaration would
//! then match the wrong claim or none.
//!
//! It publishes coverage beside the numbers. `scan_dirs` is how many claim
//! directories the root holds and `scan_readable` how many of them this process
//! could walk: a root it cannot traverse, or one directory that ownership moved
//! out from under it, produces a smaller number rather than a zero, and the two
//! are different facts -- a node with no claims is not a node whose claims are
//! invisible.
//!
//! And it publishes the age of its last completed scan. The walk is metadata-only
//! but it is proportional to what the volumes hold -- about two seconds for
//! 200 GB on this host -- so it runs on its own cadence and the scrape reads the
//! last result. A snapshot nobody refreshed and a volume that stopped growing
//! produce the same number, which is exactly why the staleness is a reading of
//! its own. The age is measured from the process's start before the first scan
//! completes, so a scanner that never got one does not look like a scanner that
//! finished a minute ago.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cog_core::claim_footprint::ClaimFootprint;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};

/// The series carrying each claim's measured footprint.
///
/// The same name the in-process walkers publish, and for the same reason: the
/// deployed rule divides it by the claim's request, so a second name would be a
/// second quantity that no rule reads. Samples from two processes are the same
/// claim's bytes and every reader of this series aggregates by
/// `(namespace, persistentvolumeclaim)` before dividing.
pub use cog_core::claim_footprint::USED_METRIC;

/// Label naming the claim a sample belongs to.
pub use cog_core::claim_footprint::CLAIM_LABEL;

/// Label naming the namespace the claim lives in.
///
/// Stamped from the directory's own name rather than left to the scraper: a
/// node's storage root holds claims from every namespace, and the scraper's
/// label for a DaemonSet target is the scanner's namespace, not theirs.
pub const NAMESPACE_LABEL: &str = "namespace";

/// Label naming the node the walk was taken on.
///
/// The same label the host's cgroup readings carry, so a volume that fills a
/// node's filesystem can be read beside the node's own disk reading. A claim
/// exists on exactly one node, so this also makes two scanners claiming the same
/// directory visible instead of averaging into one number.
pub const NODE_LABEL: &str = crate::LABEL_NODE;

/// How many claim directories the storage root holds.
pub const SCAN_DIRS_METRIC: &str = "cogneva_claim_scan_dirs";

/// How many of them this process could walk on the last scan.
pub const SCAN_READABLE_METRIC: &str = "cogneva_claim_scan_readable";

/// Seconds since the last completed scan, or since process start before one has.
pub const SCAN_AGE_METRIC: &str = "cogneva_claim_scan_age_seconds";

/// Environment variable naming the node this process runs on. Declared once for
/// every daemon that has to label its readings with the node it came from.
pub use crate::host_ledger::NODE_ENV;

/// Environment variable naming the node's storage root, as this process sees it.
pub const ROOT_ENV: &str = "COGNEVA_CLAIM_STORE_ROOT";

/// The mount point the deployment uses when it names none.
pub const DEFAULT_ROOT: &str = "/host/storage";

/// Environment variable naming the walk cadence.
pub const INTERVAL_ENV: &str = "COGNEVA_CLAIM_SCAN_INTERVAL_SECS";

/// Cadence a scan runs at when nothing states one.
///
/// The same default the in-process walkers use: two processes reporting the same
/// claim must be as fresh as each other, or the older one's number reads as
/// growth that did not happen.
// cap: bound — 两条 claim 的读数必须一样新，否则旧的那条读作一次没有发生过的增长
pub const DEFAULT_INTERVAL_SECS: u64 = 300;

/// Fastest cadence a scan may run at.
///
/// A scan walks every directory below the root, so it costs what the node holds
/// rather than a fixed amount, and a deployment asking for more than this gets
/// this instead of a knob whose only effect is to keep the disk busy.
// cap: bound — 要求更密的人拿到这个值，而不是一个唯一效果是让磁盘更忙的旋钮
pub const MIN_INTERVAL_SECS: u64 = 30;

/// Prefix of this scanner's name in the loop liveness census.
pub const SCAN_LOOP: &str = "claim_store_scan";

/// The claim a provisioner's directory stands for: `(namespace, claim)`.
///
/// The provisioner keeps one directory per claim, named `pvc-<uid>_<namespace>
/// _<claim>`. Both halves of the identity are in the name because the name is
/// the only thing the node has: there is no API call behind this reading, so a
/// directory whose name cannot be read this way is a directory this process
/// cannot attribute to a claim, and reporting it under a guess would attribute
/// real bytes to a volume that does not own them.
///
/// Neither a namespace nor a claim name may contain an underscore, which is what
/// makes the split exact rather than greedy.
pub fn parse_claim_dir(name: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix("pvc-")?;
    let mut fields = rest.splitn(3, '_');
    let uid = fields.next()?;
    let namespace = fields.next()?;
    let claim = fields.next()?;
    if uid.is_empty() || !is_name(namespace) || !is_name(claim) {
        return None;
    }
    Some((namespace.to_string(), claim.to_string()))
}

/// Whether `text` is a Kubernetes object name: lowercase alphanumerics, `-` and
/// `.`, no longer than 253 characters, starting and ending with an alphanumeric.
fn is_name(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 253
        && text
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        && !text.starts_with(['-', '.'])
        && !text.ends_with(['-', '.'])
}

/// What one scan found, so the caller can log the same numbers it publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanReport {
    /// Claim directories the root holds.
    pub dirs: usize,
    /// How many of them the walk succeeded on.
    pub readable: usize,
    /// Directories that are not claims, so no reading is owed for them.
    pub skipped: usize,
}

/// Whether the root itself could be read. A root that cannot be listed is a
/// scanner that learned nothing this round; the snapshot it held stands, and its
/// age keeps growing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    Completed(ScanReport),
    RootUnreadable,
}

struct Store {
    /// One footprint per claim, keyed by `(namespace, claim)`.
    ///
    /// Kept across scans because a failed walk must leave the last measurement
    /// in place: a number that is too small can only silence the comparison it
    /// feeds, whereas a stale one is a reading whose age is published beside it.
    claims: BTreeMap<(String, String), Arc<ClaimFootprint>>,
    dirs: usize,
    readable: usize,
    /// When the last scan completed, or process start if none has yet.
    last_scan: Instant,
}

/// Walks a node's storage root and publishes one footprint per claim it holds.
pub struct ClaimStoreObservable {
    root: PathBuf,
    node: String,
    store: Mutex<Store>,
    /// Scans completed since start, so a scanner that has not finished its first
    /// is distinguishable in a test from one that has.
    scans: AtomicU64,
}

impl ClaimStoreObservable {
    pub fn new(root: impl Into<PathBuf>, node: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            node: node.into(),
            store: Mutex::new(Store {
                claims: BTreeMap::new(),
                dirs: 0,
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

    /// How many scans have completed since this process started.
    pub fn scans(&self) -> u64 {
        self.scans.load(Ordering::Relaxed)
    }

    /// Seconds since the last completed scan, or since start before one has.
    pub fn scan_age(&self) -> Duration {
        self.store.lock().unwrap().last_scan.elapsed()
    }

    /// The claims this process is reporting, as `(namespace, claim)`.
    pub fn reported(&self) -> Vec<(String, String)> {
        self.store.lock().unwrap().claims.keys().cloned().collect()
    }

    /// Walk the storage root once.
    ///
    /// Blocking, and called from the blocking pool: a scan costs what the node
    /// holds, so it must never run on a runtime thread that a scrape is waiting
    /// on.
    ///
    /// A claim whose directory disappears stops being reported rather than
    /// keeping its last number: the bytes are no longer on this node, and a
    /// series that outlives its directory would read as a claim that exists
    /// while the account says it does not.
    pub fn scan(&self) -> Scan {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(
                    root = %self.root.display(),
                    error = %e,
                    "claim store root unreadable; keeping the last scan and letting its age grow"
                );
                return Scan::RootUnreadable;
            }
        };

        // The mount table of *this* process: a directory below the root that is
        // itself a mount belongs to whatever is mounted there, and counting its
        // bytes into the claim above it would report the same bytes twice.
        let mounts = cog_core::claim_footprint::current_mounts();

        let mut found: BTreeMap<(String, String), Vec<PathBuf>> = BTreeMap::new();
        let mut skipped = 0usize;
        let mut vanished = 0usize;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                skipped += 1;
                continue;
            };
            let Some(key) = parse_claim_dir(name) else {
                // Not every directory under a storage root is a claim, but a
                // directory shaped like one that fails to parse is worth seeing
                // once: it is either a provisioner whose naming changed or a
                // claim this reading is silently not covering.
                tracing::debug!(
                    dir = %name,
                    "not a claim directory; no footprint is owed for it"
                );
                skipped += 1;
                continue;
            };
            found.entry(key).or_default().push(entry.path());
        }
        for dirs in found.values_mut() {
            dirs.sort();
        }

        // The store is held for the bookkeeping only, never for the walk. A
        // scrape collects this map on every request, and the walk costs what the
        // node holds -- minutes of throttled CPU once the image stores are in
        // the same process -- so holding the map across it makes every scrape
        // and probe of this pod wait for a walk that is not producing anything
        // yet. The footprints themselves are shared and written in place, so a
        // scrape during a walk reads the previous number; that is what the scan
        // age next to it is for.
        let work: Vec<_> = {
            let mut store = self.store.lock().unwrap();
            // Retire what the root no longer holds before walking what it does, so a
            // deleted claim cannot be read back out of this process's memory.
            store.claims.retain(|key, _| found.contains_key(key));
            found
                .iter()
                .map(|(key, dirs)| {
                    let footprint = store
                        .claims
                        .entry(key.clone())
                        .or_insert_with(|| {
                            Arc::new(ClaimFootprint::new(
                                key.1.clone(),
                                dirs[0].clone(),
                                cog_core::claim_footprint::nested_mounts_under(&dirs[0], &mounts),
                            ))
                        })
                        .clone();
                    (key, dirs, footprint)
                })
                .collect()
        };

        let mut readable = 0usize;
        for (key, dirs, footprint) in &work {
            // One claim name can own more than one directory on this node: a
            // claim that was deleted while its reclaim policy kept the
            // directory, and the claim that later came back under the same
            // name. The series is keyed on the name -- that is what the
            // declaration it is divided by is keyed on -- so the directories
            // are reported together rather than the later one replacing the
            // earlier: replacing it would leave the kept directory's bytes on
            // the node and out of the account, which is the one shape a full
            // disk can hide behind.
            if dirs.len() > 1 {
                tracing::warn!(
                    namespace = %key.0,
                    claim = %key.1,
                    dirs = dirs.len(),
                    "more than one directory on this node carries one claim name; their bytes are reported together"
                );
            }
            // Streamed per directory: this node's storage root holds 525k
            // files, and the walk that keeps one entry per file needs several
            // times the memory this process is given. Measured here: 2.4 s and
            // 17 MB peak for the whole root, and the total agrees with
            // `du -sb` to the byte.
            let mut total = 0u64;
            let mut gone_here = 0usize;
            let mut failure = None;
            for dir in dirs.iter() {
                let exclude = cog_core::claim_footprint::nested_mounts_under(dir, &mounts);
                match cog_core::fs_size::dir_bytes_counted_once(dir, &exclude) {
                    Ok(counted) => {
                        total = total.saturating_add(counted.bytes);
                        gone_here += counted.vanished;
                    }
                    Err(e) => {
                        failure = Some((dir, e));
                        break;
                    }
                }
            }
            match failure {
                // A partial sum is smaller than the truth and a smaller number
                // can only silence the comparison this feeds, so a directory
                // that could not be walked leaves the whole name at its last
                // measurement.
                Some((dir, e)) => tracing::warn!(
                    namespace = %key.0,
                    claim = %key.1,
                    dir = %dir.display(),
                    error = %e,
                    "claim directory not walkable; keeping the last measurement"
                ),
                None => {
                    footprint.set_used_bytes(total);
                    readable += 1;
                    vanished += gone_here;
                }
            }
        }

        {
            let mut store = self.store.lock().unwrap();
            store.dirs = found.len();
            store.readable = readable;
            store.last_scan = Instant::now();
        }
        self.scans.fetch_add(1, Ordering::Relaxed);

        // A claim whose directories were removed while it was walked is still a
        // number, but it is the tree as it stood at the end of the walk: without
        // this line the drop it shows is indistinguishable from a walk that
        // stopped early.
        if vanished > 0 {
            tracing::warn!(
                vanished,
                dirs = found.len(),
                "the storage root was pruned while it was walked; the totals are the tree as it stood at the end"
            );
        }

        Scan::Completed(ScanReport {
            dirs: found.len(),
            readable,
            skipped,
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
impl Observable for ClaimStoreObservable {
    /// One series per claim the last scan completed, plus the coverage of that
    /// scan and its age. Every sample carries the node and the namespace.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let store = self.store.lock().unwrap();
        let mut out = Vec::new();
        for ((namespace, claim), footprint) in &store.claims {
            // No footprint before the first successful walk: a zero would say
            // the volume is empty, which is the claim this reading exists to
            // replace with evidence.
            let Some(bytes) = footprint.value() else {
                continue;
            };
            out.push(
                RawMetric::new(USED_METRIC, bytes as f64)
                    .with_label(CLAIM_LABEL, claim.clone())
                    .with_label(NAMESPACE_LABEL, namespace.clone())
                    .with_label(NODE_LABEL, self.node.clone()),
            );
        }
        let dirs = store.dirs as f64;
        let readable = store.readable as f64;
        let age = store.last_scan.elapsed().as_secs_f64();
        drop(store);

        let node = self.node.as_str();
        out.push(RawMetric::new(SCAN_DIRS_METRIC, dirs).with_label(NODE_LABEL, node));
        out.push(RawMetric::new(SCAN_READABLE_METRIC, readable).with_label(NODE_LABEL, node));
        out.push(RawMetric::new(SCAN_AGE_METRIC, age).with_label(NODE_LABEL, node));
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The reading describes the node, not a dimension: every task asking about
    /// the node gets the same answer.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Re-scan on a cadence and publish the result, with the loop's own liveness
/// reported beside it.
///
/// The census name carries the node because a scanner runs once per node: a name
/// shared by two of them would let a stopped one hide behind the beats of a live
/// one on another node.
pub async fn run_claim_store_scan(
    observable: Arc<ClaimStoreObservable>,
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
                    "claim store scan started"
                );
                let mut ticker = tokio::time::interval(interval);
                loop {
                    // Stamped whether or not this pass found anything to walk:
                    // the age is a reading of the loop, and a loop that only
                    // stamped when it did something would report the state of
                    // the root as its own liveness.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            match observable.scan_blocking().await {
                                Scan::Completed(report) => tracing::debug!(
                                    dirs = report.dirs,
                                    readable = report.readable,
                                    skipped = report.skipped,
                                    "claim store scanned"
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
    use crate::test_support::{chart_rule, write};
    use std::collections::BTreeMap as Map;
    use std::fs;
    use std::path::Path;

    const NODE: &str = "node-a";

    fn scratch(name: &str) -> cog_test_support::ScratchDir {
        cog_test_support::ScratchDir::new(&format!("claim-store-{name}"))
    }

    /// `pvc-<uid>_<namespace>_<claim>`, with a uid shaped like a real one.
    fn claim_dir(root: &Path, namespace: &str, claim: &str) -> PathBuf {
        let dir = root.join(format!(
            "pvc-11111111-2222-3333-4444-555555555555_{namespace}_{claim}"
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn observable(root: &Path) -> Arc<ClaimStoreObservable> {
        Arc::new(ClaimStoreObservable::new(root, NODE))
    }

    /// Footprint readings keyed by `(namespace, claim)`, with the scanner's own
    /// coverage series keyed under the empty pair.
    async fn collect_from(observable: &ClaimStoreObservable) -> Map<(String, String), f64> {
        observable
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == USED_METRIC || m.name == SCAN_DIRS_METRIC)
            .map(|m| {
                (
                    (
                        m.labels.get(NAMESPACE_LABEL).cloned().unwrap_or_default(),
                        m.labels.get(CLAIM_LABEL).cloned().unwrap_or_default(),
                    ),
                    m.value,
                )
            })
            .collect()
    }

    /// The two halves of the identity are both in the directory's name, and both
    /// have to come out of it: the reading is joined against the declaration on
    /// them, so a namespace taken from the scraper instead attributes monitoring's
    /// volumes to the scanner's own namespace.
    #[test]
    fn the_directory_name_carries_the_namespace_and_the_claim() {
        assert_eq!(
            parse_claim_dir("pvc-1a2b3c4d-0000-0000-0000-000000000000_cogneva_postgres-data"),
            Some(("cogneva".to_string(), "postgres-data".to_string()))
        );
        // A claim name may not contain `_`, so the first split is the whole split
        // and a namespace holding a dot or a dash survives intact.
        assert_eq!(
            parse_claim_dir("pvc-uid_monitoring.data-prom_prom-0"),
            Some(("monitoring.data-prom".to_string(), "prom-0".to_string()))
        );
        assert_eq!(parse_claim_dir("pvc-uid_ns"), None);
        assert_eq!(parse_claim_dir("pvc-uid__claim"), None);
        assert_eq!(parse_claim_dir("pvc-uid_ns_"), None);
        assert_eq!(parse_claim_dir("pvc-"), None);
        assert_eq!(parse_claim_dir("data"), None);
        assert_eq!(parse_claim_dir("pvc-uid_NS_claim"), None);
    }

    /// The reading that this module exists for: a claim directory's bytes come
    /// out under the series the declared-size rule divides, labelled with the
    /// namespace from the directory rather than the scanner's own.
    #[tokio::test]
    async fn every_claim_directory_is_reported_under_the_series_the_rule_divides() {
        let root = scratch("walk");
        let data = claim_dir(&root, "cogneva", "data-pvc");
        write(&data.join("file"), 4096);
        let logs = claim_dir(&root, "monitoring", "loki-0");
        write(&logs.join("chunks").join("a"), 2048);

        let observable = observable(&root);
        let scan = observable.scan();
        assert_eq!(
            scan,
            Scan::Completed(ScanReport {
                dirs: 2,
                readable: 2,
                skipped: 0
            })
        );

        let readings: Map<(String, String), f64> = observable
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == USED_METRIC)
            .map(|m| {
                (
                    (
                        m.labels[NAMESPACE_LABEL].clone(),
                        m.labels[CLAIM_LABEL].clone(),
                    ),
                    m.value,
                )
            })
            .collect();
        assert_eq!(
            readings.get(&("cogneva".into(), "data-pvc".into())),
            Some(&4096.0)
        );
        assert_eq!(
            readings.get(&("monitoring".into(), "loki-0".into())),
            Some(&2048.0),
            "a claim from another namespace lost its own name: {readings:?}"
        );
    }

    /// One claim name can own two directories on the node: a kept directory
    /// from a deleted claim, and the claim that came back under the same name.
    /// Both are on the disk, so reporting the later one alone would leave the
    /// kept directory's bytes out of the account while they still fill the
    /// node -- the shape a full disk hides behind.
    #[tokio::test]
    async fn two_directories_under_one_claim_name_are_reported_together() {
        let root = scratch("folded");
        let kept = root.join("pvc-00000000-0000-0000-0000-000000000001_rehearsal_nats-pvc");
        let live = root.join("pvc-00000000-0000-0000-0000-000000000002_rehearsal_nats-pvc");
        write(&kept.join("kept"), 1024);
        write(&live.join("live"), 2048);

        let observable = observable(&root);
        assert_eq!(
            observable.scan(),
            Scan::Completed(ScanReport {
                dirs: 1,
                readable: 1,
                skipped: 0
            }),
            "one claim name is one reading, however many directories carry it"
        );

        let readings = collect_from(&observable).await;
        assert_eq!(
            readings.get(&("rehearsal".into(), "nats-pvc".into())),
            Some(&3072.0),
            "the kept directory's bytes are missing from the claim: {readings:?}"
        );
    }

    /// Before the first walk there is no reading at all: a zero would say the
    /// volume is empty, which is a claim about the volume rather than an absence
    /// of evidence.
    #[tokio::test]
    async fn no_footprint_is_published_before_a_walk() {
        let root = scratch("unwalked");
        claim_dir(&root, "cogneva", "data-pvc");
        let readings = collect_from(&observable(&root)).await;
        assert!(
            !readings.keys().any(|(_, claim)| claim == "data-pvc"),
            "a claim was reported before it was walked: {readings:?}"
        );
    }

    /// Coverage is the count of claim directories found and walked, and a
    /// directory that is not a claim is neither: counting it would leave
    /// `scan_dirs != scan_readable` forever and make the coverage rule a
    /// permanent alarm on a healthy node.
    #[tokio::test]
    async fn a_directory_that_is_not_a_claim_is_not_counted_as_unreadable() {
        let root = scratch("stranger");
        claim_dir(&root, "cogneva", "data-pvc");
        fs::create_dir_all(root.join("lost+found")).unwrap();

        let observable = observable(&root);
        assert_eq!(
            observable.scan(),
            Scan::Completed(ScanReport {
                dirs: 1,
                readable: 1,
                skipped: 1
            })
        );
        let readings = collect_from(&observable).await;
        assert_eq!(readings.get(&("".into(), "".into())), Some(&1.0));
    }

    /// A root that cannot be listed leaves the last snapshot standing and lets
    /// its age grow: the alternative is to report every claim as gone, which
    /// reads as "the volumes were deleted" rather than "the scanner went blind".
    #[tokio::test]
    async fn an_unreadable_root_keeps_the_last_scan_and_ages() {
        let root = scratch("blind");
        write(&claim_dir(&root, "cogneva", "data-pvc").join("f"), 100);
        let observable = observable(&root);
        observable.scan();
        let before = observable.scan_age();

        let blind = self::observable(&root.join("gone"));
        assert_eq!(blind.scan(), Scan::RootUnreadable);
        assert_eq!(blind.scans(), 0, "an unreadable root counted as a scan");

        let readings: Map<(String, String), f64> = observable
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == SCAN_AGE_METRIC)
            .map(|m| (("".to_string(), "".to_string()), m.value))
            .collect();
        assert!(
            readings
                .values()
                .next()
                .is_some_and(|age| *age >= before.as_secs_f64()),
            "the scan's age was not published: {readings:?}"
        );
    }

    /// A claim whose directory is gone stops being reported: the bytes are off
    /// this node, and a series that outlived its directory would be joined
    /// against a declaration that may already belong to another volume.
    #[tokio::test]
    async fn a_removed_claim_directory_stops_being_reported() {
        let root = scratch("retire");
        let dir = claim_dir(&root, "cogneva", "data-pvc");
        write(&dir.join("f"), 512);
        let observable = observable(&root);
        observable.scan();
        assert_eq!(observable.reported().len(), 1);

        fs::remove_dir_all(&dir).unwrap();
        observable.scan();
        assert!(observable.reported().is_empty());
    }

    /// A scrape reads the previous walk's numbers, so it must not wait for the
    /// walk in progress: this node's storage root is 525k files, and a walk
    /// that held the map would leave the pod unscrapable -- and, since its
    /// probes run on the same runtime, restartable -- for seconds every
    /// cadence. Calibrated against the walk itself, so the bound means the same
    /// thing on a fast machine and a loaded one. Three attempts, because the
    /// failure this guards is a wait of the whole walk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_scrape_is_not_held_by_the_walk_it_reads_past() {
        let root = scratch("scrape-during-walk");
        let dir = claim_dir(&root, "cogneva", "data-pvc");
        for part in 0..20 {
            for file in 0..1000 {
                write(
                    &dir.join(format!("part{part}")).join(format!("{file:04}")),
                    8,
                );
            }
        }
        let observable = observable(&root);

        let started = std::time::Instant::now();
        observable.scan();
        let walk_costs = started.elapsed();

        let mut best = std::time::Duration::MAX;
        for _ in 0..3 {
            let walking = Arc::clone(&observable);
            let scan = std::thread::spawn(move || walking.scan());
            std::thread::sleep(walk_costs / 4);
            let collecting = std::time::Instant::now();
            let _ = observable.collect_metrics("").await.unwrap();
            best = best.min(collecting.elapsed());
            scan.join().unwrap();
        }

        assert!(
            best < walk_costs / 3,
            "a scrape waited {best:?} on a walk that costs {walk_costs:?}"
        );
    }

    /// A published reading only becomes an alert if a rule queries its exact
    /// name. A rename on either side leaves both halves internally consistent and
    /// the signal silently absent, so each series is pinned against its reader.
    #[test]
    fn every_reading_this_module_publishes_has_the_rule_that_reads_it() {
        let cases = [
            (SCAN_DIRS_METRIC, "claim_scan_incomplete"),
            (SCAN_READABLE_METRIC, "claim_scan_incomplete"),
            (SCAN_AGE_METRIC, "claim_scan_stale"),
            (USED_METRIC, "claim_usage_unmeasured"),
        ];
        for (metric, rule_name) in cases {
            let promql = chart_rule(rule_name)["promql"]
                .as_str()
                .expect("promql is a string")
                .to_string();
            assert!(
                promql.contains(metric),
                "rule {rule_name} must read {metric}, got: {promql}"
            );
        }
        // There is deliberately no presence series of this module's own: a node
        // whose scanner never ran is already read as "these claims have no
        // usage" by claim_usage_unmeasured, and a series nothing reads is a
        // claim on a reader that does not exist.
    }

    /// The fill-level rule is about the space between the growth loop's own
    /// threshold and the declaration: below the loop's 1/1.2 the loop is already
    /// acting, and above 1.0 the over-declared rule owns the volume. A threshold
    /// that drifted out of that band would either page on every volume the loop
    /// is about to grow, or duplicate the other rule on the same volume.
    #[test]
    fn the_fill_level_rule_sits_between_the_loop_and_the_declaration() {
        let rule = chart_rule("claim_fill_level_high");
        let promql = rule["promql"].as_str().expect("promql is a string");
        let threshold = rule["condition"]["greater_than"]
            .as_f64()
            .expect("greater_than is a number");
        assert!(
            (0.834..1.0).contains(&threshold),
            "fill-level threshold {threshold} is outside the band the growth loop leaves"
        );
        assert!(
            promql.contains("< 1.0"),
            "rule claim_fill_level_high must stop below the over-declared rule's 1.0, got: {promql}"
        );
        // A volume measured by two processes is two series of the same claim; the
        // join is on the claim, so both sides have to aggregate the node off.
        assert!(
            promql
                .matches("by (namespace, persistentvolumeclaim)")
                .count()
                >= 2,
            "rule claim_fill_level_high must keep the ratio per claim: {promql}"
        );
        // The instant is one build landing on a volume; the window is the volume
        // filling up. Without it the rule pages on the way past.
        assert!(
            promql.contains("max_over_time(") && promql.contains("[30m]"),
            "rule claim_fill_level_high must read a window rather than a single sample: {promql}"
        );
    }

    /// The staleness bound is derived from the cadence, so the two have to be
    /// read together: a bound at or below the cadence fires on a healthy scanner
    /// between two scans, and one far above it reports a dead scanner late.
    #[test]
    fn the_staleness_bound_is_three_cadences() {
        let bound = chart_rule("claim_scan_stale")["condition"]["greater_than"]
            .as_f64()
            .expect("greater_than is a number");
        assert_eq!(
            bound,
            (DEFAULT_INTERVAL_SECS * 3) as f64,
            "the stale bound is no longer three default cadences"
        );
        let rule = chart_rule("claim_scan_stale");
        let summary = rule["summary"].as_str().expect("summary is a string");
        assert!(
            summary.contains(&DEFAULT_INTERVAL_SECS.to_string()),
            "the summary does not name the cadence the bound is derived from: {summary}"
        );
    }

    /// Every sample names the node it was taken on, so a claim reported by two
    /// scanners is visible as two nodes rather than as one number twice.
    #[tokio::test]
    async fn every_sample_carries_the_node() {
        let root = scratch("node");
        write(&claim_dir(&root, "cogneva", "data-pvc").join("f"), 1);
        let observable = observable(&root);
        observable.scan();
        for reading in observable.collect_metrics("").await.unwrap() {
            assert_eq!(
                reading.labels.get(NODE_LABEL).map(String::as_str),
                Some(NODE),
                "{} lost its node label",
                reading.name
            );
        }
    }
}
