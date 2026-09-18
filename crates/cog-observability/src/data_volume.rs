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

/// Walk `dir` and total the size of every regular file below it.
///
/// Apparent size (file length), not allocated blocks: it is the amount of data
/// that was written, which is the quantity a declared size is about. Block
/// accounting adds per-file allocation slack that a directory with many small
/// files inflates without anything having grown.
///
/// Symlinks are neither counted nor descended into: their targets may live
/// outside the volume, and following them could revisit a directory forever.
fn dir_size_bytes(dir: &Path) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                pending.push(entry.path());
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Ok(total)
}

/// Re-measure `dir` on a timer and publish the result on `observable`.
pub async fn run_data_volume_watch(
    dir: PathBuf,
    observable: Arc<DataVolumeObservable>,
    interval_secs: u64,
    shutdown: ShutdownSignal,
) {
    let interval = Duration::from_secs(interval_secs.max(MIN_SCAN_INTERVAL_SECS));
    info!(
        dir = %dir.display(),
        claim = %observable.claim(),
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
                match tokio::task::spawn_blocking(move || dir_size_bytes(&path)).await {
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
        assert_eq!(dir_size_bytes(&root).unwrap(), 6000);
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn empty_directory_is_zero_and_missing_directory_is_an_error() {
        let root = scratch("empty");
        assert_eq!(dir_size_bytes(&root).unwrap(), 0);
        fs::remove_dir_all(&root).ok();
        // 空目录给 0，读不到目录给错误：两者不能都塌成 0，否则"没量到"会被
        // 发布成"用量为零"。
        assert!(dir_size_bytes(&root).is_err());
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
        assert_eq!(dir_size_bytes(&root).unwrap(), 7);
        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&outside).ok();
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
            let dir = root.clone();
            tokio::spawn(async move {
                run_data_volume_watch(dir, obs, 1, shutdown.clone()).await;
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
