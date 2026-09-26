//! Observability exporters 配置——cog-observability 自有配置段（core
//! config.rs 不聚合单 crate 配置）。自读 cogneva.json
//! `observability` 段并叠加 `COGNEVA_LOKI_*` / `COGNEVA_JAEGER_*` /
//! `COGNEVA_CLICKHOUSE_*` / `COGNEVA_ALERTMANAGER_*` env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::alerts::{AlertChannel, SmtpConfig};
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

/// Where an alert goes once one fires.
///
/// There is deliberately no enable flag: the address is the switch, the same
/// rule the notification outlets follow. A flag on top of three addresses is
/// four states for three facts, and the state it adds — addresses filled in
/// while delivery is off — reads at runtime exactly like "nobody configured an
/// alert address", which is the failure this section exists to avoid.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertmanagerConfig {
    /// Alertmanager-**receiver** URL: the dispatcher posts the receiver
    /// payload shape (`{"version":"1","alerts":[…]}`), which Alertmanager's own
    /// submission API does not accept. Empty means no such outlet.
    pub webhook_url: String,
    pub timeout_secs: u64,
    pub email: AlertEmailConfig,
    pub slack: AlertSlackConfig,
}

impl Default for AlertmanagerConfig {
    fn default() -> Self {
        Self {
            webhook_url: String::new(),
            timeout_secs: 10,
            email: AlertEmailConfig::default(),
            slack: AlertSlackConfig::default(),
        }
    }
}

/// Email outlet. Addresses are the switches — see [`crate::config::AlertmanagerConfig`].
///
/// `recipients` is the string form of what the contract type carries as a list:
/// the configuration surface is env-driven and an env value is a scalar, so a
/// comma-separated field is the only shape a deployment can write. It is split
/// at channel construction, where the contract type is built.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertEmailConfig {
    /// Comma-separated recipients. Empty means no email outlet.
    pub recipients: String,
    /// SMTP relay host. Empty means no email outlet: a recipient list with
    /// nowhere to hand it to is not an outlet, and treating it as one would
    /// announce a receiver that cannot exist.
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    #[serde(default)]
    pub smtp_password: String,
    pub from_address: String,
    pub use_tls: bool,
    pub subject_template: String,
}

impl Default for AlertEmailConfig {
    fn default() -> Self {
        Self {
            recipients: String::new(),
            smtp_host: String::new(),
            smtp_port: 587,
            smtp_username: String::new(),
            smtp_password: String::new(),
            from_address: "alerts@cogneva.local".into(),
            use_tls: true,
            subject_template: String::new(),
        }
    }
}

impl AlertEmailConfig {
    /// Recipients as the contract's list form, empty entries dropped.
    pub fn recipient_list(&self) -> Vec<String> {
        self.recipients
            .split(',')
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .map(str::to_string)
            .collect()
    }

    /// Whether this is an outlet: recipients to send to and a relay to send
    /// through. Both halves are needed, so the predicate names both.
    pub fn is_outlet(&self) -> bool {
        !self.recipients.trim().is_empty() && !self.smtp_host.trim().is_empty()
    }
}

/// Slack outlet: an incoming-webhook URL and the channel to post in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertSlackConfig {
    /// Incoming-webhook URL. Empty means no Slack outlet.
    pub webhook_url: String,
    /// Channel override carried in the payload; empty lets the webhook's own
    /// default apply.
    pub channel: String,
}

impl AlertSlackConfig {
    pub fn is_outlet(&self) -> bool {
        !self.webhook_url.trim().is_empty()
    }
}

/// The config path each alert channel reads its switch from, and the name it
/// reports under.
///
/// This is the producer's own claim about where its addresses live, and it is
/// the only place that claim exists: a deployment writes these paths and finds
/// out nothing when one of them is wrong. A channel whose path no deployment
/// can write is a dispatcher present in code that can never be built, and at
/// runtime that is indistinguishable from "no address configured" — a
/// legitimate state. Paths are relative to the `observability` section, which
/// is where the environment overrides are applied.
///
/// A channel may need more than one path (email needs a relay *and*
/// recipients); every path it cannot work without is listed.
pub const ALERT_CHANNEL_CONFIG_PATHS: [(&str, &str); 4] = [
    (ALERT_CHANNEL_WEBHOOK, "alertmanager.webhook_url"),
    (ALERT_CHANNEL_EMAIL, "alertmanager.email.recipients"),
    (ALERT_CHANNEL_EMAIL, "alertmanager.email.smtp_host"),
    (ALERT_CHANNEL_SLACK, "alertmanager.slack.webhook_url"),
];

/// The one path whose value is a credential.
///
/// It is listed apart from the rest so the deploy-side gate can require a
/// different delivery for it: everything else may be a plain value, this one
/// must arrive through a Secret, and a values key or a ConfigMap entry for it
/// would be a credential written into the repository's manifests.
pub const ALERT_CHANNEL_CREDENTIAL_PATH: &str = "alertmanager.email.smtp_password";

/// Channel names, in the order they are registered and announced.
pub const ALERT_CHANNEL_WEBHOOK: &str = "webhook";
pub const ALERT_CHANNEL_EMAIL: &str = "email";
pub const ALERT_CHANNEL_SLACK: &str = "slack";

/// Every channel name the dispatcher implements, in registration order.
pub fn alert_channel_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = ALERT_CHANNEL_CONFIG_PATHS.iter().map(|(n, _)| *n).collect();
    names.dedup();
    names
}

impl AlertmanagerConfig {
    /// The channels this configuration enables, in registration order.
    ///
    /// The one derivation of what leaves this process: the registration, the
    /// announcement and the reachability test all read it, because two lists
    /// of channels drift the moment one is added and then the process
    /// announces an outlet it never registered.
    pub fn channels(&self) -> Vec<AlertChannel> {
        let mut channels = Vec::new();

        if !self.webhook_url.trim().is_empty() {
            channels.push(AlertChannel::Webhook {
                url: self.webhook_url.trim().to_string(),
                headers: std::collections::HashMap::new(),
            });
        }

        if self.email.is_outlet() {
            let optional = |value: &str| {
                let value = value.trim();
                (!value.is_empty()).then(|| value.to_string())
            };
            channels.push(AlertChannel::Email {
                smtp_config: SmtpConfig {
                    host: self.email.smtp_host.trim().to_string(),
                    port: self.email.smtp_port,
                    username: optional(&self.email.smtp_username),
                    password: optional(&self.email.smtp_password),
                    from_address: self.email.from_address.trim().to_string(),
                    use_tls: self.email.use_tls,
                },
                to: self.email.recipient_list(),
                subject_template: self.email.subject_template.clone(),
            });
        }

        if self.slack.is_outlet() {
            channels.push(AlertChannel::Slack {
                webhook_url: self.slack.webhook_url.trim().to_string(),
                channel: self.slack.channel.trim().to_string(),
            });
        }

        channels
    }

    /// Names of the enabled channels, read off the channels themselves rather
    /// than re-tested: the announcement and the registration cannot disagree
    /// when one is the other's shadow.
    pub fn channel_names(&self) -> Vec<&'static str> {
        self.channels()
            .iter()
            .map(|channel| match channel {
                AlertChannel::Webhook { .. } => ALERT_CHANNEL_WEBHOOK,
                AlertChannel::Email { .. } => ALERT_CHANNEL_EMAIL,
                AlertChannel::Slack { .. } => ALERT_CHANNEL_SLACK,
            })
            .collect()
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

/// The environment variable behind each setting of this section.
///
/// This table is the producer's claim about where its settings are written from,
/// and the deployside reachability gate reads it as such: a key nothing renders
/// is a knob the deployment cannot turn, and at runtime such a knob is
/// indistinguishable from one nobody set.
pub const OBS_ENV: &[(&str, &str)] = &[
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
    (
        "COGNEVA_ALERTMANAGER_WEBHOOK_URL",
        "alertmanager.webhook_url",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_RECIPIENTS",
        "alertmanager.email.recipients",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_SMTP_HOST",
        "alertmanager.email.smtp_host",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_SMTP_PORT",
        "alertmanager.email.smtp_port",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_SMTP_USERNAME",
        "alertmanager.email.smtp_username",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_SMTP_PASSWORD",
        "alertmanager.email.smtp_password",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_FROM",
        "alertmanager.email.from_address",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_USE_TLS",
        "alertmanager.email.use_tls",
    ),
    (
        "COGNEVA_ALERTMANAGER_EMAIL_SUBJECT_TEMPLATE",
        "alertmanager.email.subject_template",
    ),
    (
        "COGNEVA_ALERTMANAGER_SLACK_WEBHOOK_URL",
        "alertmanager.slack.webhook_url",
    ),
    (
        "COGNEVA_ALERTMANAGER_SLACK_CHANNEL",
        "alertmanager.slack.channel",
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

    /// Nothing configured means nothing leaves the process — and that is what
    /// the section's own default has to be, because the default document is
    /// what every deployment that says nothing gets.
    #[test]
    fn the_default_document_builds_no_alert_channel() {
        let cfg = AlertmanagerConfig::default();
        assert!(cfg.channels().is_empty());
        assert!(cfg.channel_names().is_empty());
    }

    /// An address rendered as an empty string is an absent address: manifests
    /// render unset keys as `""`, and treating that as an outlet would claim a
    /// receiver nobody configured.
    #[test]
    fn blank_addresses_are_not_channels() {
        let mut cfg = AlertmanagerConfig {
            webhook_url: "  ".into(),
            ..AlertmanagerConfig::default()
        };
        cfg.slack.webhook_url = "".into();
        cfg.email.recipients = "ops@example.invalid".into();
        cfg.email.smtp_host = " ".into();
        assert!(cfg.channels().is_empty(), "{:?}", cfg.channels());
    }

    /// A recipient list with no relay is not an email outlet: it cannot send
    /// anywhere, and announcing it would name a receiver that does not exist.
    #[test]
    fn email_needs_both_a_relay_and_recipients() {
        let mut cfg = AlertmanagerConfig::default();
        cfg.email.smtp_host = "smtp.example.invalid".into();
        assert!(cfg.channels().is_empty());

        cfg.email.recipients = "ops@example.invalid, oncall@example.invalid".into();
        assert_eq!(cfg.channel_names(), vec![ALERT_CHANNEL_EMAIL]);
        match &cfg.channels()[0] {
            AlertChannel::Email {
                smtp_config, to, ..
            } => {
                assert_eq!(smtp_config.host, "smtp.example.invalid");
                assert_eq!(to.len(), 2, "recipients split on commas: {to:?}");
                assert_eq!(to[0], "ops@example.invalid");
            }
            other => panic!("expected an email channel, got {other:?}"),
        }
    }

    /// The names are the channels, not a second list: each channel that gets
    /// built is announced once, in registration order.
    #[test]
    fn the_announced_names_are_the_built_channels() {
        let mut cfg = AlertmanagerConfig {
            webhook_url: "https://receiver.example.invalid/alerts".into(),
            ..AlertmanagerConfig::default()
        };
        cfg.slack.webhook_url = "https://hooks.example.invalid/T1".into();
        cfg.email.recipients = "ops@example.invalid".into();
        cfg.email.smtp_host = "smtp.example.invalid".into();
        assert_eq!(
            cfg.channel_names(),
            vec![
                ALERT_CHANNEL_WEBHOOK,
                ALERT_CHANNEL_EMAIL,
                ALERT_CHANNEL_SLACK
            ]
        );
        assert_eq!(cfg.channels().len(), cfg.channel_names().len());
    }

    /// Every path in the producer's table, written through the loader's own
    /// setter, has to enable the channel it is declared for. This is the tie
    /// the table needs: the path a deployment's env map writes and the field
    /// the predicate reads are two names for one thing, and a table checked
    /// only against itself would keep agreeing with itself while the
    /// deployment wrote somewhere nobody reads.
    ///
    /// Paths are section-relative because that is where the overrides land, so
    /// the write goes through the same `apply_env_paths` shape the process uses.
    #[test]
    fn each_declared_path_enables_its_channel() {
        for channel in alert_channel_names() {
            let paths: Vec<&str> = ALERT_CHANNEL_CONFIG_PATHS
                .iter()
                .filter(|(name, _)| *name == channel)
                .map(|(_, path)| *path)
                .collect();
            assert!(
                !paths.is_empty(),
                "{channel} 在表里没有任何配置路径，可达性检查会空转"
            );

            let mut value = serde_json::to_value(AlertmanagerConfig::default())
                .expect("alert channels serialize");
            for path in paths {
                let relative = path
                    .strip_prefix("alertmanager.")
                    .unwrap_or_else(|| panic!("{path} 不在 alertmanager 段下"));
                let sample = if relative.ends_with("webhook_url") {
                    "https://hook.example.invalid/x"
                } else {
                    "ops@example.invalid"
                };
                cog_core::config::set_json_path(&mut value, relative, sample);
            }
            let cfg: AlertmanagerConfig =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("{channel}: {e}"));
            assert!(
                cfg.channel_names().contains(&channel),
                "写入 {channel} 声明的路径没有建出这个出口：{:?}",
                cfg.channel_names()
            );
        }
    }
}
