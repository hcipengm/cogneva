pub mod alerts;
pub mod analytics;
pub mod explainability;
pub mod explainability_pg;
pub mod jaeger;
pub mod logs;
pub mod metrics;
pub mod observable;
pub mod plugin;
pub mod probes;
pub mod raw_stream;
pub mod search_index;
pub mod snapshot;
pub mod traces;

pub use logs::{init_subscriber, LogFilterHandle};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Unified observability configuration.
/// observability: { prometheus, jaeger, log_level, snapshot, raw_streams }
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObservabilityConfig {
    pub prometheus: PrometheusConfig,
    pub jaeger: JaegerConfig,
    pub log_level: String,
    pub log_format: LogFormat,
    pub snapshot: SnapshotConfig,
    pub raw_streams: RawStreamConfig,
    pub explainability: ExplainabilityConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            prometheus: PrometheusConfig::default(),
            jaeger: JaegerConfig::default(),
            log_level: "info".into(),
            log_format: LogFormat::Pretty,
            snapshot: SnapshotConfig::default(),
            raw_streams: RawStreamConfig::default(),
            explainability: ExplainabilityConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub enum LogFormat {
    Json,
    #[default]
    Pretty,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PrometheusConfig {
    pub enabled: bool,
    pub port: u16,
    pub registry_prefix: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            registry_prefix: "cogneva".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotConfig {
    pub enabled: bool,
    pub storage_directory: PathBuf,
    pub hot_retention_days: u32,
    pub warm_retention_days: u32,
    pub compression_hot: i32,
    pub compression_warm: i32,
    pub compression_cold: i32,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            storage_directory: PathBuf::from("/var/lib/cogneva-data/snapshots"),
            hot_retention_days: 7,
            warm_retention_days: 90,
            compression_hot: 0,
            compression_warm: 3,
            compression_cold: 9,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RawStreamConfig {
    pub enabled: bool,
    pub hot_dir: PathBuf,
    pub warm_dir: PathBuf,
    pub cold_dir: PathBuf,
    pub max_hot_file_size_mb: u64,
    pub flush_interval_sec: u64,
}

impl Default for RawStreamConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            hot_dir: PathBuf::from("/var/lib/cogneva-data/raw-streams/hot"),
            warm_dir: PathBuf::from("/var/lib/cogneva-data/raw-streams/warm"),
            cold_dir: PathBuf::from("/var/lib/cogneva-data/raw-streams/cold"),
            max_hot_file_size_mb: 256,
            flush_interval_sec: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExplainabilityConfig {
    pub enabled: bool,
    pub max_records_memory: usize,
    pub persist_interval_sec: u64,
}

impl Default for ExplainabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_records_memory: 10_000,
            persist_interval_sec: 60,
        }
    }
}

/// Re-export the legacy MetricsExporter for backward compatibility.
pub use metrics::MetricsExporter;
pub use observable::ObservabilityObservable;

/// Unified observability initializer.
/// Call once at application startup (cogneva/src/main.rs).
/// Initializes all three consumer layers:
/// - Human: Prometheus metrics, structured logs, Jaeger traces
/// - Agent/Developer: Snapshot manager
/// - Machine: Raw stream writer
///   **Important**: This function installs a global `tracing_subscriber`.
///   It must be called *before* any `tracing` macros are used.
pub fn init(config: &ObservabilityConfig) -> ObservabilityInitResult {
    let mut result = ObservabilityInitResult::default();

    if config.prometheus.enabled {
        result.metrics_exporter = Some(metrics::MetricsExporter::new());
        result.metrics_backend = Some(Arc::new(metrics::PrometheusMetricsBackend::new(
            &config.prometheus.registry_prefix,
        )));
    }

    if config.jaeger.enabled {
        let exporter = jaeger::init_jaeger_subscriber(
            &config.jaeger.endpoint,
            &config.jaeger.service_name,
            &config.log_level,
            config.log_format.clone(),
            None,
        );
        result.jaeger_exporter = Some(exporter);
    } else {
        logs::init_subscriber(&config.log_level, config.log_format.clone());
    }

    if config.raw_streams.enabled {
        result.raw_stream_writer = Some(Arc::new(raw_stream::RawStreamWriter::new(
            config.raw_streams.hot_dir.clone(),
            config.raw_streams.warm_dir.clone(),
            config.raw_streams.cold_dir.clone(),
            config.raw_streams.max_hot_file_size_mb,
            config.raw_streams.flush_interval_sec,
        )));
    }

    if config.explainability.enabled {
        result.explainability_store = Some(Arc::new(explainability::ExplainabilityStore::new(
            config.explainability.max_records_memory,
        )));
    }

    result
}

/// Result of observability initialization.
#[derive(Default)]
pub struct ObservabilityInitResult {
    pub metrics_exporter: Option<metrics::MetricsExporter>,
    pub metrics_backend: Option<Arc<metrics::PrometheusMetricsBackend>>,
    pub jaeger_exporter: Option<Arc<jaeger::JaegerExporter>>,
    pub raw_stream_writer: Option<Arc<raw_stream::RawStreamWriter>>,
    pub explainability_store: Option<Arc<explainability::ExplainabilityStore>>,
}

/// A single explainability record for AI decision tracking.
/// Maps to design doc ch3 data type 15 (explainability).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExplainabilityRecord {
    pub record_id: String,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub decision_type: String,
    pub confidence: f64,
    pub reasoning_chain: Vec<String>,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub metadata: HashMap<String, serde_json::Value>,
    pub timestamp: DateTime<Utc>,
}

/// Protobuf raw envelope header for all 7 streams.
#[derive(Debug, Clone, PartialEq)]
pub struct RawEnvelope {
    pub meta: RawMeta,
    pub context: RawContext,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawMeta {
    pub stream_name: String,
    pub event_id: String,
    pub timestamp_unix_ms: i64,
    pub trace_id: String,
    pub span_id: String,
    pub source_crate: String,
    pub source_version: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RawContext {
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub team_id: Option<String>,
    pub labels: HashMap<String, String>,
}

/// Enum of the 7 Protobuf raw streams.
/// session_raw, task_raw, agent_raw, llm_raw, tool_raw, system_raw, transport_raw
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RawStreamName {
    Session,
    Task,
    Agent,
    Llm,
    Tool,
    System,
    Transport,
}

impl RawStreamName {
    pub fn as_str(&self) -> &'static str {
        match self {
            RawStreamName::Session => "session_raw",
            RawStreamName::Task => "task_raw",
            RawStreamName::Agent => "agent_raw",
            RawStreamName::Llm => "llm_raw",
            RawStreamName::Tool => "tool_raw",
            RawStreamName::System => "system_raw",
            RawStreamName::Transport => "transport_raw",
        }
    }

    pub fn file_name(&self, date: &chrono::NaiveDate) -> String {
        format!("{}_{}.raw", self.as_str(), date.format("%Y%m%d"))
    }
}

/// Unified observability handle for the three consumer layers.
/// This is the primary interface used by cogneva and cog-gateway.
pub struct ObservabilityHandle {
    pub metrics_backend: Option<Arc<dyn cog_core::MetricsBackend>>,
    pub raw_stream_writer: Option<Arc<raw_stream::RawStreamWriter>>,
    pub explainability_store: Option<Arc<explainability::ExplainabilityStore>>,
}

impl ObservabilityHandle {
    pub fn new(init: &ObservabilityInitResult) -> Self {
        Self {
            metrics_backend: init
                .metrics_backend
                .clone()
                .map(|b| b as Arc<dyn cog_core::MetricsBackend>),
            raw_stream_writer: init.raw_stream_writer.clone(),
            explainability_store: init.explainability_store.clone(),
        }
    }
}
