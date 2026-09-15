//! Observability exporters 配置——cog-observability 自有配置段（core
//! config.rs 不聚合单 crate 配置）。自读 cogneva.json
//! `observability` 段并叠加 `COGNEVA_LOKI_*` / `COGNEVA_JAEGER_*` /
//! `COGNEVA_CLICKHOUSE_*` / `COGNEVA_ALERTMANAGER_*` env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

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
];

impl ObservabilityExportersConfig {
    /// 自读 cogneva.json `observability` 段 + env 覆盖；文件/段缺失回退
    /// 默认，段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
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
        serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} observability: {e}", path.display())))
    }
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
}
