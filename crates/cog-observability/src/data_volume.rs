//! Reports how many bytes the claim-backed directories of this deployment occupy.
//!
//! A persistent volume's declared size is a claim, not a measurement. Nothing
//! reconciles the two: the request is admission-time arithmetic, and for a
//! directory-backed volume the filesystem underneath is the whole node, so the
//! per-volume capacity the kubelet reports is not the volume's. The only party
//! that can measure what a volume holds is the process writing to it, so it
//! publishes the number and the deployment compares it against the declaration.
//!
//! Which directories those are is decided here; the measurement itself and the
//! series it is published under belong to
//! [`cog_core::claim_footprint`], because a second process measures a volume of
//! its own and the two cannot disagree about what a reading means.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFResult, ShutdownSignal};
use tracing::{info, warn};

/// Gauge carrying the measured footprint, labelled with the claim backing the
/// directory so it can be joined against the claim's declared size.
pub use cog_core::claim_footprint::USED_METRIC as DATA_VOLUME_USED_METRIC;

/// Name of the rule that consumes the gauge, so the pairing can be asserted.
pub const DATA_VOLUME_USED_METRIC_RULE: &str = "data_volume_over_declared_size";

/// Fastest cadence the directory may be re-walked at. The walk is metadata-only
/// and cheap, but it holds no value being fresher than the scrape interval.
pub use cog_core::claim_footprint::MIN_SCAN_INTERVAL_SECS;

/// This loop's name in the liveness census.
pub const DATA_VOLUME_WATCH_LOOP: &str = "data_volume_watch";

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
/// The format is shared with the other process that measures a volume of its own
/// — the sandbox executor states its pairing through the same variable — so the
/// grammar and its error messages live with the footprint reading itself. A
/// second parser would accept a slightly different subset, and the two would
/// disagree about a declaration nobody wrote down twice.
pub fn parse_mounts(raw: &str) -> Result<Vec<crate::config::WatchedVolumeConfig>, String> {
    Ok(cog_core::claim_footprint::parse_claim_paths(raw)?
        .into_iter()
        .map(|mount| crate::config::WatchedVolumeConfig {
            claim: mount.claim,
            path: mount.path,
        })
        .collect())
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
/// What counts as a nested mount is the footprint reading's own rule, since the
/// other process measuring a volume of its own applies the same one.
pub fn derive_exclusions(targets: &mut [WatchedTarget], mounts: &[PathBuf]) {
    for target in targets.iter_mut() {
        target.exclude = cog_core::claim_footprint::nested_mounts_under(&target.dir, mounts);
    }
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
    let mounts = cog_core::claim_footprint::current_mounts();
    if mounts.is_empty() {
        warn!("mount table unreadable; a nested mount will be counted into its parent");
    }
    derive_exclusions(&mut targets, &mounts);
    (targets, rejected)
}

/// Footprint gauge for one claim-backed directory.
///
/// The measurement itself is [`cog_core::claim_footprint::ClaimFootprint`], which
/// the other process measuring a volume of its own also holds: the number, the
/// label it is published under and the meaning of "not measured yet" have to be
/// the same in both, or the same volume would be described two ways.
pub struct DataVolumeObservable {
    footprint: Arc<cog_core::claim_footprint::ClaimFootprint>,
}

impl DataVolumeObservable {
    pub fn new(target: WatchedTarget) -> Self {
        Self {
            footprint: Arc::new(cog_core::claim_footprint::ClaimFootprint::new(
                target.claim,
                target.dir,
                target.exclude,
            )),
        }
    }

    pub fn claim(&self) -> &str {
        self.footprint.claim()
    }

    pub fn dir(&self) -> &Path {
        self.footprint.dir()
    }

    pub fn set_used_bytes(&self, bytes: u64) {
        self.footprint.set_used_bytes(bytes);
    }

    /// Walk the directory on the blocking pool and record what it holds.
    pub async fn measure_blocking(self: &Arc<Self>) -> std::io::Result<()> {
        self.footprint.measure_blocking().await
    }

    /// The last successful measurement, or `None` before one has happened.
    ///
    /// Not zero: a gauge that reports before anything was walked reads as "the
    /// volume is empty", which is a claim about the volume rather than an
    /// absence of evidence, and it is the exact confusion this gauge exists to
    /// end.
    pub fn used_bytes(&self) -> Option<u64> {
        self.footprint.value()
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
                    .with_label(cog_core::claim_footprint::CLAIM_LABEL, self.claim())]
            })
            .unwrap_or_default())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The footprint is not a per-dimension metric: every dimension consumes
    /// the same volume, so declaring no dimension is what tells the collector
    /// to pull this observable once rather than once per question.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Re-measure the observable's directory on a timer and publish the result.
///
/// The directory to walk is the observable's own: the number and the claim it is
/// attributed to have to travel together, so the loop cannot be handed a
/// directory belonging to a different identity than the series it feeds.
pub async fn run_data_volume_watch(
    observable: Arc<DataVolumeObservable>,
    interval_secs: u64,
    shutdown: ShutdownSignal,
) {
    let interval = Duration::from_secs(interval_secs.max(MIN_SCAN_INTERVAL_SECS));
    // The footprint gauge is this loop's own reading, so the loop being gone and
    // the walk having nothing to say would read the same: the liveness of the
    // watcher has to come from a face that outlives it -- and a panic is run
    // again in place, so the reading is not the only thing that survives it.
    let _ = cog_core::loop_health::spawn(
        DATA_VOLUME_WATCH_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let observable = Arc::clone(&observable);
            let shutdown = shutdown.clone();
            async move {
                info!(
                    dir = %observable.dir().display(),
                    claim = %observable.claim(),
                    interval_secs = interval.as_secs(),
                    metric = DATA_VOLUME_USED_METRIC,
                    "data volume footprint watcher started"
                );

                let mut ticker = tokio::time::interval(interval);
                loop {
                    // Stamped whether or not this pass found anything to measure: the age is
                    // a reading of the loop, and a loop that only stamps when it did
                    // something reports the state of the directory as its own liveness.
                    beat.beat();
                    tokio::select! {
                        biased;
                        _ = shutdown.wait() => break,
                        _ = ticker.tick() => {
                            // A failed walk yields a number that is too small, which can
                            // only silence the alert. The footprint keeps the last
                            // measurement rather than publish a fictional low one, and
                            // the failure is still said out loud here.
                            if let Err(e) = observable.measure_blocking().await {
                                warn!(
                                    error = %e,
                                    dir = %observable.dir().display(),
                                    "data volume scan failed; keeping the last measurement"
                                );
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

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cog-datavol-{}-{}", name, std::process::id()));
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();
        dir
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

    /// A target whose directory is never walked: only the publication rules are
    /// under test.
    fn unmeasured(claim: &str) -> DataVolumeObservable {
        DataVolumeObservable::new(WatchedTarget {
            claim: claim.to_string(),
            dir: PathBuf::from("/var/lib/cogneva-data"),
            exclude: Vec::new(),
        })
    }

    #[tokio::test]
    async fn gauge_carries_the_claim_regardless_of_dimension() {
        let obs = unmeasured("cogneva-data-pvc");
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
        let obs = Arc::new(DataVolumeObservable::new(WatchedTarget {
            claim: "claim".into(),
            dir: root.clone(),
            exclude: Vec::new(),
        }));
        assert_eq!(obs.used_bytes(), None);

        let shutdown = ShutdownSignal::new();
        let handle = {
            let obs = obs.clone();
            tokio::spawn(async move {
                run_data_volume_watch(obs, 1, shutdown.clone()).await;
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
