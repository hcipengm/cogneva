//! Observability exporters 配置——cog-observability 自有配置段（core
//! config.rs 不聚合单 crate 配置）。自读 cogneva.json
//! `observability` 段并叠加 `COGNEVA_LOKI_*` / `COGNEVA_JAEGER_*` /
//! `COGNEVA_CLICKHOUSE_*` / `COGNEVA_ALERTMANAGER_*` env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

/// The document this revision declares, compiled into the binary.
///
/// The chart's copy is the single source both delivery paths render from — the
/// helm ConfigMap takes the file as it is, and the k3s carrier is held
/// field-for-field equal to it by the deploy parity gate — so one embed covers
/// both. Compiled in rather than read from disk: this is the reference side of
/// a judgement about what *was* delivered, and a reference the deployment could
/// rewrite would answer nothing.
pub const DECLARED_CONFIG_JSON: &str =
    include_str!("../../../deploy/helm/cogneva/files/cogneva.json");

/// Where this process reads its configuration document.
///
/// One resolution for the loader and for the delivery check: two copies of the
/// path would let a check judge a file the process never reads.
pub fn config_path() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var("COGNEVA_CONFIG_PATH").unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into()),
    )
}

/// Observability exporters configuration (Loki / Jaeger / ClickHouse / Alertmanager).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityExportersConfig {
    pub loki: LokiConfig,
    pub jaeger: JaegerConfig,
    pub clickhouse: ClickHouseConfig,
    pub alertmanager: AlertmanagerConfig,
    pub elasticsearch: ElasticsearchConfig,
    pub infra_watch: InfraWatchConfig,
    pub trace_collector: TraceCollectorConfig,
    pub data_volume_watch: DataVolumeWatchConfig,
    pub config_declaration: ConfigDeclarationConfig,
}

/// The delivered document against the one this revision declares.
///
/// The check runs because the delivered document is where the alert rules live,
/// so a document that lags its revision cannot report its own lag: every rule
/// that would say "these rules are not in force" is in the document that did
/// not arrive. Only the reading cadence is configurable — there is deliberately
/// no switch, because the one check whose subject is "the configuration did not
/// reach me" must not be something the configuration can turn off.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigDeclarationConfig {
    /// How long to wait before the second read that separates a deployment
    /// still rolling from a document that is genuinely behind. A pod can
    /// legitimately start on the previous document while the ConfigMap is
    /// applied around it; when the file has moved on by the second read, the
    /// process is on its way to being replaced rather than stuck.
    pub settle_secs: u64,
}

impl Default for ConfigDeclarationConfig {
    fn default() -> Self {
        Self { settle_secs: 60 }
    }
}

impl ConfigDeclarationConfig {
    /// Floor for the settle window. The mount is refreshed by kubelet on its
    /// own sync period, so a window shorter than that would read the same stale
    /// document twice and call a rollout a permanent gap.
    pub const MIN_SETTLE_SECS: u64 = 10;
}

/// Persistent-volume footprint watcher: measures the application data
/// directory and publishes it as a gauge so the deployment can compare it
/// against the volume's declared size.
///
/// The claim name is the one fact the process cannot derive — it knows the
/// directory it writes, not which claim backs it — so the deployment supplies
/// it, per workload, because two workloads mount two different claims at the
/// same path. A claim is also the switch: no claim means the directory is not
/// claim-backed here, and the watcher stays off. There is deliberately no
/// separate enable flag, so the watcher cannot be turned on without naming
/// what it measures.
///
/// `claim` covers the application data directory alone, which is all a
/// single-volume workload has. `volumes` adds further claim-backed mounts;
/// a workload whose largest volume is not the data directory would otherwise
/// have no reading for the volume most likely to overrun.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DataVolumeWatchConfig {
    /// Name of the persistent volume claim backing the application data
    /// directory. Empty disables the watcher.
    pub claim: String,
    /// How often every watched directory is re-measured. Clamped up to
    /// [`crate::data_volume::MIN_SCAN_INTERVAL_SECS`].
    pub interval_secs: u64,
    /// Additional claim-backed mounts to measure, each independently joined
    /// against its own declaration. Normally supplied by the deployment
    /// through `COGNEVA_DATA_VOLUME_MOUNTS`, because the pairing is a fact
    /// about that workload's own pods.
    pub volumes: Vec<WatchedVolumeConfig>,
}

/// One claim-backed mount to measure beyond the application data directory.
///
/// Nested mounts are deliberately absent: the process derives from its own
/// mount table which directories below `path` are separate volumes, so a
/// deployment states only the pairing it alone knows.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WatchedVolumeConfig {
    /// Claim backing `path`; the label the reading carries and the key the
    /// declaration is joined on.
    pub claim: String,
    /// Mount point to walk.
    pub path: String,
}

/// Trace collector in-memory buffering policy. Squad agents can run for
/// hours; without a byte budget the per-agent event buffer grows without
/// bound and OOM-kills the host process (observed: one planner trace
/// reached 80 MiB before persistence). The budget is deployment policy,
/// not a code constant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TraceCollectorConfig {
    /// Per-agent in-memory event buffer budget in bytes. On overflow the
    /// buffered events are flushed as a partial trace chunk and buffering
    /// resumes, so a long run yields several bounded chunks instead of one
    /// unbounded trace. 0 disables chunking (not recommended).
    pub buffer_max_bytes: usize,
}

impl Default for TraceCollectorConfig {
    fn default() -> Self {
        Self {
            buffer_max_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Infrastructure alert watcher: evaluates PromQL rules against a
/// Prometheus-compatible endpoint and drives results into the persistent
/// alert state machine, so infrastructure faults (node disk pressure, pod
/// crash loops) become persisted alerts that self-discovery can consume.
///
/// Rules come from configuration, not code: thresholds and even the set of
/// watched signals are deployment policy, and hardcoding them would force a
/// rebuild for every tuning change.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InfraWatchConfig {
    pub enabled: bool,
    /// Prometheus-compatible base URL; empty disables the watcher.
    pub prometheus_url: String,
    pub poll_interval_secs: u64,
    /// Consecutive evaluation failures of one rule before the watcher raises
    /// its own alert about the rule being broken. A failed query only logs
    /// otherwise, so a dead Prometheus or a blocked network path would leave
    /// the whole self-discovery channel dark without anyone noticing. 0
    /// disables this self-alert.
    pub eval_failure_alert_after: u32,
    pub rules: Vec<InfraRule>,
}

impl Default for InfraWatchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prometheus_url: String::new(),
            poll_interval_secs: 0,
            eval_failure_alert_after: 3,
            rules: Vec::new(),
        }
    }
}

/// One PromQL-backed alert rule. Every series the query returns is evaluated
/// against `condition`; each matching series is one alert instance keyed by
/// rule name plus its identity labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraRule {
    pub name: String,
    pub promql: String,
    pub condition: cog_core::AlertCondition,
    pub severity: cog_core::AlertSeverity,
    /// Human-readable summary; `{value}` is substituted with the last
    /// observed sample value.
    pub summary: String,
}

impl InfraWatchConfig {
    /// Minimum poll cadence: faster polling only burns Prometheus CPU without
    /// improving detection, since scrape intervals dominate freshness.
    pub const MIN_POLL_INTERVAL_SECS: u64 = 30;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LokiConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub flush_interval_sec: u64,
    pub max_batch_size: usize,
}

impl Default for LokiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:3100".into(),
            max_retries: 3,
            timeout_secs: 10,
            flush_interval_sec: 5,
            max_batch_size: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JaegerConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub service_name: String,
}

impl Default for JaegerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:14268/api/traces".into(),
            service_name: "cogneva".into(),
        }
    }
}

/// `password` 在 Debug 输出中脱敏。
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClickHouseConfig {
    pub enabled: bool,
    pub base_url: String,
    pub database: String,
    pub table: String,
    pub username: String,
    pub password: String,
    pub flush_interval_sec: u64,
    pub max_batch_size: usize,
}

impl std::fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("enabled", &self.enabled)
            .field("base_url", &self.base_url)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("username", &self.username)
            .field("password", &cog_core::config::redacted(&self.password))
            .field("flush_interval_sec", &self.flush_interval_sec)
            .field("max_batch_size", &self.max_batch_size)
            .finish()
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: "http://localhost:8123".into(),
            database: "cogneva".into(),
            table: "events".into(),
            username: "default".into(),
            password: "".into(),
            flush_interval_sec: 10,
            max_batch_size: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertmanagerConfig {
    pub enabled: bool,
    pub webhook_url: String,
    pub timeout_secs: u64,
}

impl Default for AlertmanagerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            webhook_url: "http://localhost:9093/api/v1/alerts".into(),
            timeout_secs: 10,
        }
    }
}

/// `password` / `api_key` 在 Debug 输出中脱敏。
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ElasticsearchConfig {
    pub enabled: bool,
    pub base_url: String,
    pub username: String,
    pub password: String,
    pub api_key: String,
}

impl std::fmt::Debug for ElasticsearchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchConfig")
            .field("enabled", &self.enabled)
            .field("base_url", &self.base_url)
            .field("username", &self.username)
            .field("password", &cog_core::config::redacted(&self.password))
            .field("api_key", &cog_core::config::redacted(&self.api_key))
            .finish()
    }
}

impl Default for ElasticsearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: "http://localhost:9200".into(),
            username: "".into(),
            password: "".into(),
            api_key: "".into(),
        }
    }
}

const OBS_ENV: &[(&str, &str)] = &[
    ("COGNEVA_LOKI_ENABLED", "loki.enabled"),
    ("COGNEVA_LOKI_ENDPOINT", "loki.endpoint"),
    ("COGNEVA_JAEGER_ENABLED", "jaeger.enabled"),
    ("COGNEVA_JAEGER_ENDPOINT", "jaeger.endpoint"),
    ("COGNEVA_CLICKHOUSE_ENABLED", "clickhouse.enabled"),
    ("COGNEVA_CLICKHOUSE_BASE_URL", "clickhouse.base_url"),
    ("COGNEVA_CLICKHOUSE_DATABASE", "clickhouse.database"),
    ("COGNEVA_CLICKHOUSE_TABLE", "clickhouse.table"),
    ("COGNEVA_CLICKHOUSE_USERNAME", "clickhouse.username"),
    ("COGNEVA_CLICKHOUSE_PASSWORD", "clickhouse.password"),
    ("COGNEVA_ALERTMANAGER_ENABLED", "alertmanager.enabled"),
    (
        "COGNEVA_ALERTMANAGER_WEBHOOK_URL",
        "alertmanager.webhook_url",
    ),
    ("COGNEVA_INFRA_WATCH_ENABLED", "infra_watch.enabled"),
    ("COGNEVA_INFRA_WATCH_URL", "infra_watch.prometheus_url"),
    (
        "COGNEVA_INFRA_WATCH_POLL_SECS",
        "infra_watch.poll_interval_secs",
    ),
    (
        "COGNEVA_TRACE_BUFFER_MAX_BYTES",
        "trace_collector.buffer_max_bytes",
    ),
    ("COGNEVA_DATA_VOLUME_CLAIM", "data_volume_watch.claim"),
    (
        "COGNEVA_DATA_VOLUME_INTERVAL_SECS",
        "data_volume_watch.interval_secs",
    ),
    (
        "COGNEVA_CONFIG_DECLARATION_SETTLE_SECS",
        "config_declaration.settle_secs",
    ),
];

impl ObservabilityExportersConfig {
    /// 自读 cogneva.json `observability` 段 + env 覆盖；文件/段缺失回退
    /// 默认，段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        Self::load_from(&config_path())
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        let mut section = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                root.pointer("/observability")
                    .cloned()
                    .unwrap_or(serde_json::json!({}))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cog_core::config::apply_env_paths(&mut section, OBS_ENV);
        // 挂载清单是一个列表，而上面的标量映射会把每个 env 值都变成字符串，
        // 所以它单走一条路：解析失败响亮报错，不能让 Pod 起来却什么都不量。
        if let Ok(raw) = std::env::var("COGNEVA_DATA_VOLUME_MOUNTS") {
            apply_mount_list(&mut section, &raw)?;
        }
        serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} observability: {e}", path.display())))
    }
}

/// Overwrite the watched-mount list from the `claim=path` env value.
fn apply_mount_list(section: &mut serde_json::Value, raw: &str) -> SFResult<()> {
    let volumes = crate::data_volume::parse_mounts(raw)
        .map_err(|e| SFError::Config(format!("COGNEVA_DATA_VOLUME_MOUNTS: {e}")))?;
    if volumes.is_empty() {
        return Ok(());
    }
    let watch = section
        .as_object_mut()
        .map(|root| {
            root.entry("data_volume_watch")
                .or_insert_with(|| serde_json::json!({}))
        })
        .ok_or_else(|| SFError::Config("observability section is not an object".into()))?;
    let watch = watch
        .as_object_mut()
        .ok_or_else(|| SFError::Config("data_volume_watch is not an object".into()))?;
    watch.insert(
        "volumes".into(),
        serde_json::to_value(volumes)
            .map_err(|e| SFError::Config(format!("watched mounts: {e}")))?,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_returns_default() {
        let cfg =
            ObservabilityExportersConfig::load_from(std::path::Path::new("/nonexistent/x.json"))
                .unwrap();
        assert!(!cfg.loki.enabled);
    }

    #[test]
    fn reads_section() {
        let dir = std::env::temp_dir().join(format!("cog-obs-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"observability": {"loki": {"enabled": true, "endpoint": "http://loki:3100"},
                "clickhouse": {"password": "p@ss"}}}"#,
        )
        .unwrap();
        let cfg = ObservabilityExportersConfig::load_from(&path).unwrap();
        assert!(cfg.loki.enabled);
        assert_eq!(cfg.loki.endpoint, "http://loki:3100");
        let dbg = format!("{:?}", cfg.clickhouse);
        assert!(!dbg.contains("p@ss"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn data_volume_watch_is_off_until_a_claim_is_named() {
        // 缺省即关闭：卷名是部署侧才知道的事实，代码不能替它猜一张卷。
        let cfg = ObservabilityExportersConfig::default();
        assert!(cfg.data_volume_watch.claim.is_empty());
        assert_eq!(cfg.data_volume_watch.interval_secs, 0);
        assert!(cfg.data_volume_watch.volumes.is_empty());

        let dir = std::env::temp_dir().join(format!("cog-obs-vol-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"observability": {"data_volume_watch": {"claim": "cogneva-data-pvc", "interval_secs": 300}}}"#,
        )
        .unwrap();
        let cfg = ObservabilityExportersConfig::load_from(&path).unwrap();
        assert_eq!(cfg.data_volume_watch.claim, "cogneva-data-pvc");
        assert_eq!(cfg.data_volume_watch.interval_secs, 300);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A workload's largest volume need not be its data directory. Listing
    /// further mounts is how the deployments whose overrun risk lives elsewhere
    /// get a reading at all.
    #[test]
    fn reads_additional_watched_volumes() {
        let dir = std::env::temp_dir().join(format!("cog-obs-vols-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"observability": {"data_volume_watch": {
                "claim": "",
                "interval_secs": 600,
                "volumes": [
                    {"claim": "sandbox-data", "path": "/opt/cogneva/sandbox"},
                    {"claim": "sandbox-src", "path": "/opt/cogneva/sandbox/src"}
                ]}}}"#,
        )
        .unwrap();
        let cfg = ObservabilityExportersConfig::load_from(&path).unwrap();
        let volumes = &cfg.data_volume_watch.volumes;
        assert_eq!(volumes.len(), 2);
        assert_eq!(volumes[0].claim, "sandbox-data");
        assert_eq!(volumes[0].path, "/opt/cogneva/sandbox");
        assert_eq!(volumes[1].path, "/opt/cogneva/sandbox/src");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The list cannot go through the scalar env mapping, so it has its own
    /// path. It has to land in a section the file may not even have declared,
    /// and a malformed value must fail the load rather than start a pod that
    /// quietly measures nothing.
    #[test]
    fn mount_list_lands_in_the_section_and_malformed_input_is_loud() {
        let mut section = serde_json::json!({"loki": {"enabled": false}});
        apply_mount_list(
            &mut section,
            "cogneva-evolution-pvc=/opt/cogneva/sandbox\n\
             cogneva-evolution-source-pvc=/opt/cogneva/sandbox/src",
        )
        .unwrap();
        let cfg: ObservabilityExportersConfig = serde_json::from_value(section).unwrap();
        assert!(!cfg.loki.enabled);
        assert_eq!(cfg.data_volume_watch.volumes.len(), 2);
        assert_eq!(
            cfg.data_volume_watch.volumes[1].claim,
            "cogneva-evolution-source-pvc"
        );

        let mut section = serde_json::json!({});
        assert!(apply_mount_list(&mut section, "not-a-pair").is_err());

        // 空值等于没配，不能让一个空 env 把文件里写的清单清掉。
        let mut section = serde_json::json!({
            "data_volume_watch": {"volumes": [{"claim": "from-file", "path": "/x"}]}
        });
        apply_mount_list(&mut section, "   \n  ").unwrap();
        let cfg: ObservabilityExportersConfig = serde_json::from_value(section).unwrap();
        assert_eq!(cfg.data_volume_watch.volumes.len(), 1);
        assert_eq!(cfg.data_volume_watch.volumes[0].claim, "from-file");
    }
}
