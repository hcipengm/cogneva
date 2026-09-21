//! Reports how many bytes the application data directory actually occupies.
//!
//! A persistent volume's declared size is a claim, not a measurement. Nothing
//! reconciles the two: the request is admission-time arithmetic, and for a
//! directory-backed volume the filesystem underneath is the whole node, so the
//! per-volume capacity the kubelet reports is not the volume's. The only party
//! that can measure what a volume holds is the process writing to it, so it
//! publishes the number and the deployment compares it against the declaration.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};
use tracing::{info, warn};

/// Gauge carrying the measured footprint, labelled with the claim backing the
/// directory so it can be joined against the claim's declared size.
pub const DATA_VOLUME_USED_METRIC: &str = "cogneva_data_volume_used_bytes";

/// Name of the rule that consumes the gauge, so the pairing can be asserted.
pub const DATA_VOLUME_USED_METRIC_RULE: &str = "data_volume_over_declared_size";

/// Fastest cadence the directory may be re-walked at. The walk is metadata-only
/// and cheap, but it holds no value being fresher than the scrape interval.
pub const MIN_SCAN_INTERVAL_SECS: u64 = 30;

/// One directory to measure: the claim that backs it, the directory itself,
/// and any nested directories below it that belong to a different claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedTarget {
    pub claim: String,
    pub dir: PathBuf,
    pub exclude: Vec<PathBuf>,
}

/// Parse the deployment's `claim=path` list, one entry per line.
///
/// A malformed entry is returned as a reason, never skipped: a mount the
/// operator declared and the process silently dropped looks exactly like a
/// volume that is small, which is the blindness this exists to end.
pub fn parse_mounts(raw: &str) -> Result<Vec<crate::config::WatchedVolumeConfig>, String> {
    let mut out = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((claim, path)) = line.split_once('=') else {
            return Err(format!("entry `{line}` is not `claim=path`"));
        };
        let claim = claim.trim();
        let path = path.trim();
        if claim.is_empty() || path.is_empty() {
            return Err(format!("entry `{line}` leaves the claim or the path empty"));
        }
        out.push(crate::config::WatchedVolumeConfig {
            claim: claim.to_string(),
            path: path.to_string(),
        });
    }
    Ok(out)
}

/// The directories a deployment should measure, and why any listed mount
/// could not become one.
///
/// The application data directory is the one every workload has, and it is
/// measured only when a claim names it: the claim is what the reading is
/// joined against, so without one the number has nothing to be compared to.
/// Explicitly listed mounts follow, each carrying its own claim. Every claim
/// is measured against its own directory, because a reading attributed to the
/// wrong claim invents an overrun on one volume and hides the one on another.
pub fn watched_targets(
    cfg: &crate::config::DataVolumeWatchConfig,
    app_data_dir: &Path,
) -> (Vec<WatchedTarget>, Vec<String>) {
    let mut targets = Vec::new();
    let mut rejected = Vec::new();

    if !cfg.claim.trim().is_empty() {
        targets.push(WatchedTarget {
            claim: cfg.claim.trim().to_string(),
            dir: app_data_dir.to_path_buf(),
            exclude: Vec::new(),
        });
    }

    for volume in &cfg.volumes {
        if volume.claim.trim().is_empty() {
            rejected.push(format!(
                "watched volume {} names no claim; it cannot be joined against a declaration",
                volume.path
            ));
            continue;
        }
        if volume.path.trim().is_empty() {
            rejected.push(format!(
                "watched volume {} names no path; there is nothing to measure",
                volume.claim
            ));
            continue;
        }
        targets.push(WatchedTarget {
            claim: volume.claim.trim().to_string(),
            dir: PathBuf::from(volume.path.trim().trim_end_matches('/')),
            exclude: Vec::new(),
        });
    }

    (targets, rejected)
}

/// Give every target the nested mounts its own walk must leave out.
///
/// A directory below a mount point holds bytes of another volume only because
/// something is mounted there, so the process's own mount table is the
/// authoritative list of what to leave out. Reading it beats restating the
/// list in configuration: a hand-kept list cannot tell when a mount has moved,
/// and a stale entry fails silently in both directions — one that names a
/// mount which no longer exists inflates the parent, one that omits a mount
/// that now exists hands the child's bytes to the parent.
pub fn derive_exclusions(targets: &mut [WatchedTarget], mounts: &[PathBuf]) {
    for target in targets.iter_mut() {
        target.exclude = mounts
            .iter()
            .filter(|mount| mount.as_path() != target.dir && mount.starts_with(&target.dir))
            .cloned()
            .collect();
    }
}

/// Mount points of this process's own mount namespace, unreadable ones
/// omitted.
///
/// Empty is a real answer — a container with no submounts under its volumes,
/// or a platform where the table cannot be read. It degrades towards
/// over-counting rather than blindness: a parent that absorbs a nested volume
/// reports a number that is too large, which is visible, whereas leaving out a
/// mount that should have been counted would report a volume as smaller than it
/// is and silence its alert.
fn current_mounts() -> Vec<PathBuf> {
    let Ok(text) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            // `<id> <parent> <maj:min> <root> <mount point> <opts> [more] - <fstype> <src> <super opts>`
            let head = line.split(" - ").next()?;
            let field = head.split_whitespace().nth(4)?;
            Some(PathBuf::from(unescape_mount_field(field)))
        })
        .collect()
}

/// Undo the octal escaping the kernel applies to spaces, tabs, newlines and
/// backslashes in a mount point.
fn unescape_mount_field(field: &str) -> String {
    let raw = field.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\\' && i + 3 < raw.len() {
            let digits = std::str::from_utf8(&raw[i + 1..i + 4]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(digits, 8) {
                out.push(byte);
                i += 4;
                continue;
            }
        }
        out.push(raw[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Resolve the configured targets against this process's own mount table.
///
/// Separate from [`watched_targets`] so the pairing rules stay a pure function;
/// everything the mount table adds is applied on top of them.
pub fn resolve_targets(
    cfg: &crate::config::DataVolumeWatchConfig,
    app_data_dir: &Path,
) -> (Vec<WatchedTarget>, Vec<String>) {
    let (mut targets, rejected) = watched_targets(cfg, app_data_dir);
    let mounts = current_mounts();
    if mounts.is_empty() {
        warn!("mount table unreadable; a nested mount will be counted into its parent");
    }
    derive_exclusions(&mut targets, &mounts);
    (targets, rejected)
}

/// Footprint gauge for one claim-backed directory.
///
/// The measurement is written by the scan loop and read by the metrics pull,
/// which are different tasks, hence the atomics rather than a lock.
pub struct DataVolumeObservable {
    claim: String,
    used_bytes: AtomicU64,
    measured: AtomicBool,
}

impl DataVolumeObservable {
    pub fn new(claim: impl Into<String>) -> Self {
        Self {
            claim: claim.into(),
            used_bytes: AtomicU64::new(0),
            measured: AtomicBool::new(false),
        }
    }

    pub fn claim(&self) -> &str {
        &self.claim
    }

    pub fn set_used_bytes(&self, bytes: u64) {
        self.used_bytes.store(bytes, Ordering::Relaxed);
        self.measured.store(true, Ordering::Release);
    }

    /// The last successful measurement, or `None` before one has happened.
    ///
    /// Not zero: a gauge that reports before anything was walked reads as "the
    /// volume is empty", which is a claim about the volume rather than an
    /// absence of evidence, and it is the exact confusion this gauge exists to
    /// end.
    pub fn used_bytes(&self) -> Option<u64> {
        self.measured
            .load(Ordering::Acquire)
            .then(|| self.used_bytes.load(Ordering::Relaxed))
    }
}

#[async_trait]
impl Observable for DataVolumeObservable {
    /// Nothing at all until the first walk completes, so the series the
    /// deployment joins against the declared size does not exist yet rather
    /// than existing with a fabricated value.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(self
            .used_bytes()
            .map(|bytes| {
                vec![RawMetric::new(DATA_VOLUME_USED_METRIC, bytes as f64)
                    .with_label("persistentvolumeclaim", &self.claim)]
            })
            .unwrap_or_default())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The footprint is not a per-dimension metric: every dimension consumes
    /// the same volume, and the metrics endpoint asks each observable for one
    /// dimension, so answering only that one dimension would hide the gauge.
    fn available_dimensions(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Walk `dir` and total the size of every regular file below it, skipping any
/// entry whose path is listed in `exclude`.
///
/// Apparent size (file length), not allocated blocks: it is the amount of data
/// that was written, which is the quantity a declared size is about. Block
/// accounting adds per-file allocation slack that a directory with many small
/// files inflates without anything having grown.
///
/// An excluded path is neither counted nor descended into, and the exclusion is
/// on the top-level directory alone rather than on its contents: a mount inside
/// this volume is the work of a different claim, and its bytes would otherwise
/// be reported against both.
///
/// Symlinks are neither counted nor descended into: their targets may live
/// outside the volume, and following them could revisit a directory forever.
fn dir_size_bytes(dir: &Path, exclude: &[PathBuf]) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let path = entry.path();
            if exclude.contains(&path) {
                continue;
            }
            let meta = entry.metadata()?;
            if meta.is_dir() {
                pending.push(path);
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Ok(total)
}

/// Re-measure the target's directory on a timer and publish the result on
/// `observable`.
pub async fn run_data_volume_watch(
    target: WatchedTarget,
    observable: Arc<DataVolumeObservable>,
    interval_secs: u64,
    shutdown: ShutdownSignal,
) {
    let WatchedTarget { dir, exclude, .. } = target;
    let interval = Duration::from_secs(interval_secs.max(MIN_SCAN_INTERVAL_SECS));
    info!(
        dir = %dir.display(),
        claim = %observable.claim(),
        excluded = exclude.len(),
        interval_secs = interval.as_secs(),
        metric = DATA_VOLUME_USED_METRIC,
        "data volume footprint watcher started"
    );

    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            _ = ticker.tick() => {
                let path = dir.clone();
                let excluded = exclude.clone();
                match tokio::task::spawn_blocking(move || dir_size_bytes(&path, &excluded)).await {
                    Ok(Ok(bytes)) => observable.set_used_bytes(bytes),
                    // A failed walk yields a number that is too small, which can
                    // only silence the alert. Keep the last measurement rather
                    // than publish a fictional low one, and say so.
                    Ok(Err(e)) => warn!(
                        error = %e,
                        dir = %dir.display(),
                        "data volume scan failed; keeping the last measurement"
                    ),
                    Err(e) => warn!(error = %e, "data volume scan task panicked"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cog-datavol-{}-{}", name, std::process::id()));
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn sums_nested_regular_files() {
        let root = scratch("sum");
        fs::create_dir_all(root.join("a/b")).unwrap();
        fs::write(root.join("top.bin"), vec![0u8; 1000]).unwrap();
        fs::write(root.join("a/mid.bin"), vec![0u8; 2000]).unwrap();
        fs::write(root.join("a/b/deep.bin"), vec![0u8; 3000]).unwrap();
        assert_eq!(dir_size_bytes(&root, &[]).unwrap(), 6000);
        fs::remove_dir_all(&root).ok();
    }

    /// A mount nested inside this volume belongs to a different claim, so its
    /// bytes must leave this reading entirely: counting them here reports the
    /// same data against two claims, which invents an overrun on one volume and
    /// hides a real one on the other.
    #[test]
    fn excluded_subdirectory_is_neither_counted_nor_descended_into() {
        let root = scratch("exclude");
        let nested = root.join("src");
        fs::create_dir_all(nested.join("deep")).unwrap();
        fs::write(root.join("own.bin"), vec![0u8; 1000]).unwrap();
        fs::write(nested.join("other.bin"), vec![0u8; 7000]).unwrap();
        fs::write(nested.join("deep/x.bin"), vec![0u8; 9000]).unwrap();

        assert_eq!(dir_size_bytes(&root, &[]).unwrap(), 17_000);
        assert_eq!(
            dir_size_bytes(&root, std::slice::from_ref(&nested)).unwrap(),
            1000
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn empty_directory_is_zero_and_missing_directory_is_an_error() {
        let root = scratch("empty");
        assert_eq!(dir_size_bytes(&root, &[]).unwrap(), 0);
        fs::remove_dir_all(&root).ok();
        // 空目录给 0，读不到目录给错误：两者不能都塌成 0，否则"没量到"会被
        // 发布成"用量为零"。
        assert!(dir_size_bytes(&root, &[]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_is_not_followed() {
        let root = scratch("symlink");
        let outside = scratch("symlink-target");
        fs::write(outside.join("huge.bin"), vec![0u8; 5000]).unwrap();
        fs::write(root.join("inside.bin"), vec![0u8; 7]).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        // 链到卷外的东西不算这个卷的占用，否则链接一次就把别人的字节记进来。
        assert_eq!(dir_size_bytes(&root, &[]).unwrap(), 7);
        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&outside).ok();
    }

    fn watch_cfg(
        claim: &str,
        volumes: Vec<crate::config::WatchedVolumeConfig>,
    ) -> crate::config::DataVolumeWatchConfig {
        crate::config::DataVolumeWatchConfig {
            claim: claim.to_string(),
            interval_secs: 300,
            volumes,
        }
    }

    /// The application data directory is measured only when a claim names it:
    /// the claim is the join key, so a reading without one has nothing to be
    /// compared against. Listed mounts are unaffected by that absence.
    #[test]
    fn app_data_dir_is_measured_only_under_a_named_claim() {
        let app = PathBuf::from("/var/lib/cogneva-data");

        let (targets, rejected) = watched_targets(&watch_cfg("", Vec::new()), &app);
        assert!(targets.is_empty());
        assert!(rejected.is_empty());

        let (targets, rejected) = watched_targets(&watch_cfg("app-pvc", Vec::new()), &app);
        assert_eq!(
            targets,
            vec![WatchedTarget {
                claim: "app-pvc".into(),
                dir: app.clone(),
                exclude: Vec::new(),
            }]
        );
        assert!(rejected.is_empty());
    }

    /// Every claim gets its own reading. Collapsing them into one number, or
    /// dropping the ones that arrive without a claim, is exactly the blindness
    /// that let the largest volume overrun with no observation face.
    #[test]
    fn each_listed_volume_becomes_its_own_target() {
        let app = PathBuf::from("/var/lib/cogneva-data");
        let cfg = watch_cfg(
            "app-pvc",
            vec![
                crate::config::WatchedVolumeConfig {
                    claim: "sandbox-data".into(),
                    path: "/opt/cogneva/sandbox/".into(),
                },
                crate::config::WatchedVolumeConfig {
                    claim: "sandbox-src".into(),
                    path: "/opt/cogneva/sandbox/src".into(),
                },
            ],
        );

        let (targets, rejected) = watched_targets(&cfg, &app);
        assert!(rejected.is_empty());
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].claim, "app-pvc");
        assert_eq!(targets[0].dir, app);
        assert_eq!(targets[1].claim, "sandbox-data");
        // 尾斜杠必须在解析时就抹掉：`/a/b/` 与 `/a/b` 在路径比较里是两个值，
        // 排除项与目标路径差一个斜杠就会让包含关系判不出来。
        assert_eq!(targets[1].dir, PathBuf::from("/opt/cogneva/sandbox"));
        assert_eq!(targets[2].dir, PathBuf::from("/opt/cogneva/sandbox/src"));
    }

    /// A listed mount missing either half of its identity is dropped with a
    /// reason rather than measured. Silently measuring it under a fabricated
    /// claim would attribute one volume's bytes to another; silently dropping
    /// it would hide the volume, which is the defect being fixed.
    #[test]
    fn incomplete_listed_volumes_are_rejected_with_a_reason() {
        let app = PathBuf::from("/var/lib/cogneva-data");
        let cfg = watch_cfg(
            "",
            vec![
                crate::config::WatchedVolumeConfig {
                    claim: "  ".into(),
                    path: "/opt/cogneva/sandbox".into(),
                },
                crate::config::WatchedVolumeConfig {
                    claim: "sandbox-src".into(),
                    path: String::new(),
                },
                crate::config::WatchedVolumeConfig {
                    claim: " ok ".into(),
                    path: " /data ".into(),
                },
            ],
        );

        let (targets, rejected) = watched_targets(&cfg, &app);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].claim, "ok");
        assert_eq!(targets[0].dir, PathBuf::from("/data"));
        assert_eq!(rejected.len(), 2);
        assert!(rejected[0].contains("names no claim"), "{}", rejected[0]);
        assert!(rejected[1].contains("names no path"), "{}", rejected[1]);
    }

    #[test]
    fn mount_list_parses_lines_and_refuses_the_rest() {
        let parsed = parse_mounts(
            "cogneva-evolution-pvc = /opt/cogneva/sandbox\n\
             \n\
             cogneva-evolution-source-pvc=/opt/cogneva/sandbox/src\n",
        )
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].claim, "cogneva-evolution-pvc");
        assert_eq!(parsed[0].path, "/opt/cogneva/sandbox");
        assert_eq!(parsed[1].path, "/opt/cogneva/sandbox/src");

        // 半条声明一律报错而不是跳过：被静默丢掉的那个卷看起来就像"它很小"。
        assert!(parse_mounts("/opt/cogneva/sandbox").is_err());
        assert!(parse_mounts("= /opt/cogneva/sandbox").is_err());
        assert!(parse_mounts("cogneva-pvc=").is_err());
        assert!(parse_mounts("").unwrap().is_empty());
    }

    /// A volume nested inside another is a different claim's data. Leaving it
    /// in the parent's walk reports the same bytes twice: an overrun on the
    /// parent that is really the child's, and both readings then hide the one
    /// ratio that actually crossed the declaration.
    #[test]
    fn nested_mounts_are_excluded_from_their_parent_only() {
        let app = PathBuf::from("/var/lib/cogneva-data");
        let cfg = watch_cfg(
            "",
            vec![
                crate::config::WatchedVolumeConfig {
                    claim: "sandbox-data".into(),
                    path: "/opt/cogneva/sandbox".into(),
                },
                crate::config::WatchedVolumeConfig {
                    claim: "sandbox-src".into(),
                    path: "/opt/cogneva/sandbox/src".into(),
                },
            ],
        );
        let mounts = vec![
            PathBuf::from("/"),
            PathBuf::from("/etc/hosts"),
            PathBuf::from("/opt/cogneva/sandbox"),
            PathBuf::from("/opt/cogneva/sandbox/src"),
            // 同前缀但不同目录：组件级比较必须不把它当成子目录。
            PathBuf::from("/opt/cogneva/sandbox2"),
        ];

        let (mut targets, _) = watched_targets(&cfg, &app);
        derive_exclusions(&mut targets, &mounts);
        assert_eq!(
            targets[0].exclude,
            vec![PathBuf::from("/opt/cogneva/sandbox/src")]
        );
        // 自己不是自己的排除项：把自己排掉等于这个卷恒报 0。
        assert_eq!(targets[1].exclude, Vec::<PathBuf>::new());
    }

    #[test]
    fn mount_field_escaping_is_undone() {
        assert_eq!(unescape_mount_field("/a\\040b"), "/a b");
        assert_eq!(unescape_mount_field("/a\\134b"), "/a\\b");
        assert_eq!(unescape_mount_field("/plain"), "/plain");
        // 非转义的孤立反斜杠照原样保留，不能被当成转义前缀吃掉后一个字符。
        assert_eq!(unescape_mount_field("/a\\b"), "/a\\b");
    }

    /// The two facts the exclusion needs — that a mount exists and where —
    /// come from the kernel, and the parser has to read the real table rather
    /// than a shape invented for the test.
    #[test]
    fn current_mounts_reads_this_processes_own_table() {
        let mounts = current_mounts();
        assert!(
            mounts.iter().any(|m| m == Path::new("/")),
            "the root mount is always present, got {mounts:?}"
        );
        assert!(
            !mounts.iter().any(|m| m.as_os_str().is_empty()),
            "an unparsed line yields an empty path that would match nothing"
        );
    }

    #[tokio::test]
    async fn gauge_carries_the_claim_regardless_of_dimension() {
        let obs = DataVolumeObservable::new("cogneva-data-pvc");
        // 还没量过就不该有这条序列：先报一个 0 等于替卷宣称"它是空的"。
        assert!(obs.collect_metrics("D8").await.unwrap().is_empty());
        obs.set_used_bytes(18_000_000_000);
        // 指标端点对每个 observable 只问一个维度；任何维度都要答，否则
        // 这个 gauge 在 Prometheus 里根本不存在。
        for dim in ["D5", "D8", "anything"] {
            let metrics = obs.collect_metrics(dim).await.unwrap();
            assert_eq!(metrics.len(), 1);
            assert_eq!(metrics[0].name, DATA_VOLUME_USED_METRIC);
            assert_eq!(metrics[0].value, 18_000_000_000.0);
            assert_eq!(
                metrics[0]
                    .labels
                    .get("persistentvolumeclaim")
                    .map(String::as_str),
                Some("cogneva-data-pvc")
            );
        }
    }

    /// The gauge only becomes an alert if a rule queries its exact name. A
    /// rename on either side leaves both halves internally consistent and the
    /// signal silently absent, so the two are pinned against each other here.
    #[test]
    fn deployed_rule_queries_the_metric_this_module_publishes() {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        let rules = root
            .pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present");
        let rule = rules
            .iter()
            .find(|r| r["name"] == DATA_VOLUME_USED_METRIC_RULE)
            .unwrap_or_else(|| panic!("rule {DATA_VOLUME_USED_METRIC_RULE} missing"));
        let promql = rule["promql"].as_str().expect("promql is a string");
        assert!(
            promql.contains(DATA_VOLUME_USED_METRIC),
            "rule {DATA_VOLUME_USED_METRIC_RULE} must query {DATA_VOLUME_USED_METRIC}, got: {promql}"
        );
        // 分子是量出来的占用，分母必须是集群那份声明量，否则比的是自己的配置。
        assert!(
            promql.contains("kube_persistentvolumeclaim_resource_requests_storage_bytes"),
            "the divisor must be the cluster's declared size, got: {promql}"
        );
    }

    /// Every pod of a deployment publishes this gauge, so a rollout leaves two
    /// pods reporting the same claim until the old one exits. A bare numerator
    /// then has two series per join key and the whole expression fails to
    /// evaluate ("many-to-one matching must be explicit"), which blinds the
    /// rule for the length of every rollout. Aggregating the numerator by the
    /// join keys is what keeps the two readings collapsing into one.
    #[test]
    fn rollout_overlap_cannot_break_the_volume_rule() {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = fs::read_to_string(&chart).expect("chart config readable");
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        let rule = root
            .pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .and_then(|rules| {
                rules
                    .iter()
                    .find(|r| r["name"] == DATA_VOLUME_USED_METRIC_RULE)
            })
            .unwrap_or_else(|| panic!("rule {DATA_VOLUME_USED_METRIC_RULE} missing"));
        let promql = rule["promql"].as_str().expect("promql is a string");

        let join_keys = "namespace, persistentvolumeclaim";
        let aggregated = format!("by ({join_keys}) ({DATA_VOLUME_USED_METRIC})");
        let at = promql.find(&aggregated).unwrap_or_else(|| {
            panic!("the numerator must be aggregated by {join_keys} before the division, got: {promql}")
        });
        assert!(
            promql.contains(&format!("on({join_keys})")),
            "the join must be on the claim identity, got: {promql}"
        );
        // The overlapping readings measure the same directory, so the
        // aggregation may collapse them but must not add them up.
        let operator = promql[..at].split_whitespace().last().unwrap_or("");
        assert!(
            !matches!(operator, "sum" | "count"),
            "`{operator}` would inflate the numerator instead of collapsing the overlap, got: {promql}"
        );
    }

    #[tokio::test]
    async fn scan_publishes_the_measured_bytes() {
        let root = scratch("loop");
        fs::write(root.join("f.bin"), vec![0u8; 4096]).unwrap();
        let obs = Arc::new(DataVolumeObservable::new("claim"));
        assert_eq!(obs.used_bytes(), None);

        let shutdown = ShutdownSignal::new();
        let handle = {
            let obs = obs.clone();
            let target = WatchedTarget {
                claim: "claim".into(),
                dir: root.clone(),
                exclude: Vec::new(),
            };
            tokio::spawn(async move {
                run_data_volume_watch(target, obs, 1, shutdown.clone()).await;
            })
        };

        // 第一次 tick 立即触发，等一下让扫描落位。
        for _ in 0..50 {
            if obs.used_bytes() == Some(4096) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(obs.used_bytes(), Some(4096));
        handle.abort();
        fs::remove_dir_all(&root).ok();
    }
}
