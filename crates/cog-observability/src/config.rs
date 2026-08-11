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
    ("COGNEVA_ALERTMANAGER_ENABLED", "alertmanager.enabled"),
    (
        "COGNEVA_ALERTMANAGER_WEBHOOK_URL",
        "alertmanager.webhook_url",
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
