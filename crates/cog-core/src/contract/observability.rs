use crate::{AgentEvent, SFResult};
use chrono::{DateTime, NaiveDate, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

// Re-export futures::Stream so consumers can work with AgentEventStream without
// adding a separate futures dependency.

/// A boxed stream of [`AgentEvent`]s yielded by [`ObservabilityGateway::subscribe_events`].
pub type AgentEventStream = Pin<Box<dyn Stream<Item = SFResult<AgentEvent>> + Send>>;

/// Filter criteria for event subscription.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EventFilter {
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub squad_id: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub since: Option<DateTime<Utc>>,
}

/// Metrics snapshot for a single task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskMetrics {
    pub task_id: String,
    pub total_tokens: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub tool_calls: u32,
    pub iterations: u32,
    pub duration_ms: u64,
    pub timestamp: DateTime<Utc>,
}

/// A single structured log entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub source: String,
    pub message: String,
    pub metadata: serde_json::Value,
}

/// Index entry for a raw Protobuf log segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawLogIndex {
    pub stream: String,
    pub date: NaiveDate,
    pub file_path: String,
    pub encoding: String,
    pub record_count: u64,
    pub byte_size: u64,
    pub created_at: DateTime<Utc>,
}

/// Cluster-wide observability overview.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterOverview {
    pub total_agents: usize,
    pub active_agents: usize,
    pub total_tasks: usize,
    pub active_tasks: usize,
    pub queued_tasks: usize,
    pub failed_tasks: usize,
    pub avg_task_duration_ms: u64,
    pub cluster_health: String,
    pub timestamp: DateTime<Utc>,
    pub total_squads: usize,
    pub active_squads: usize,
}

/// Squad lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SquadStatus {
    Pending,
    Running,
    Complete,
    Failed,
    Retrying,
}

/// Summarized view of an agent within a squad.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentSummary {
    pub agent_id: String,
    pub state: crate::AgentState,
    pub task_id: Option<String>,
    pub last_heartbeat: DateTime<Utc>,
}

/// Summarized view of a squad within a crew.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SquadSummary {
    pub squad_id: String,
    pub status: SquadStatus,
    pub agent_count: usize,
    pub completed_agent_count: usize,
}

/// Full state of a squad.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SquadState {
    pub squad_id: String,
    pub task_id: String,
    pub status: SquadStatus,
    pub agents: Vec<AgentSummary>,
    pub completion_pct: f32,
    pub retry_count: u32,
    pub snapshot_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Active trace context for a single request/task.
/// Carried through the async call stack and injected into:
/// - AgentEvent metadata
/// - RawEnvelope meta.trace_id / meta.span_id
/// - HTTP headers (x-trace-id)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub sampled: bool,
}

impl TraceContext {
    pub fn new(trace_id: impl Into<String>, span_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            parent_span_id: None,
            sampled: true,
        }
    }

    pub fn generate() -> Self {
        Self {
            trace_id: uuid::Uuid::new_v4().to_string().replace("-", ""),
            span_id: uuid::Uuid::new_v4().to_string().replace("-", ""),
            parent_span_id: None,
            sampled: true,
        }
    }

    pub fn with_parent(mut self, parent_span_id: impl Into<String>) -> Self {
        self.parent_span_id = Some(parent_span_id.into());
        self
    }

    pub fn to_headers(&self) -> HashMap<String, String> {
        let mut h = HashMap::new();
        h.insert("x-trace-id".into(), self.trace_id.clone());
        h.insert("x-span-id".into(), self.span_id.clone());
        if let Some(ref p) = self.parent_span_id {
            h.insert("x-parent-span-id".into(), p.clone());
        }
        h
    }

    pub fn from_headers(headers: &HashMap<String, String>) -> Option<Self> {
        let trace_id = headers.get("x-trace-id")?;
        let span_id = headers.get("x-span-id")?;
        Some(Self {
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            parent_span_id: headers.get("x-parent-span-id").cloned(),
            sampled: true,
        })
    }
}

/// Prometheus-compatible metrics exporter.
pub trait MetricsExporter: Send + Sync {
    /// Encode metrics into Prometheus text format.
    fn encode(&self) -> crate::SFResult<Vec<u8>>;
}

// ─── Search Backend ────────────────────────────────────────────────────────

/// Single search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub doc_id: String,
    pub index: String,
    pub score: f64,
    pub highlights: Vec<String>,
    pub source: serde_json::Value,
}

/// Search backend trait — abstracts Elasticsearch / OpenSearch implementations.
#[async_trait::async_trait]
pub trait SearchBackend: Send + Sync {
    /// Search across one or more indices.
    async fn search(
        &self,
        indices: &[String],
        query: &str,
        limit: usize,
    ) -> crate::SFResult<Vec<SearchResult>>;
}

// ─── Replay Engine ─────────────────────────────────────────────────────────

/// Replay engine trait — deterministic re-execution from a persisted trace.
#[async_trait::async_trait]
pub trait ReplayEngine: Send + Sync {
    /// Load a trace by id and replay its events.
    /// Returns the number of events replayed.
    async fn replay(
        &self,
        trace_id: &str,
        event_handler: Box<dyn FnMut(crate::AgentEvent) -> crate::SFResult<()> + Send>,
    ) -> crate::SFResult<u64>;
}

// ─── Observable trait + helpers (merged from observable.rs) ────────────────

/// 原始指标数据点 —— 各业务 crate 暴露原始数据，cog-eval 负责计算最终指标值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawMetric {
    pub name: String,
    pub value: f64,
    pub timestamp_ms: u64,
    pub labels: HashMap<String, String>,
}

impl RawMetric {
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            name: name.into(),
            value,
            timestamp_ms,
            labels: HashMap::new(),
        }
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

/// 工具调用快照 —— 用于 TraceFragment 中记录一次工具调用。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallSnapshot {
    pub tool_name: String,
    pub params: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// 执行轨迹片段 —— 记录单步的完整上下文，用于 D2/D3/D4/D8 等维度的回放与评估。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceFragment {
    pub step_index: usize,
    pub action_type: String,
    pub action_params: serde_json::Value,
    pub thought: Option<String>,
    pub screenshot_hash: Option<String>,
    pub ui_state: Option<serde_json::Value>,
    pub tool_calls: Vec<ToolCallSnapshot>,
    pub duration_ms: u64,
    pub success: bool,
    pub error: Option<String>,
}

/// 可观测性 trait —— 各业务 crate 实现此 trait 暴露原始数据。
#[async_trait::async_trait]
pub trait Observable: Send + Sync {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>>;
    async fn collect_trace(&self, task_id: &str) -> SFResult<Vec<TraceFragment>>;
    fn available_dimensions(&self) -> Vec<String>;
}

/// 从多个 Observable 聚合指定维度的指标（便捷函数）。
pub async fn collect_all_metrics(
    observables: &[Arc<dyn Observable>],
    dimension: &str,
) -> Vec<RawMetric> {
    let mut all = Vec::new();
    for observable in observables {
        match observable.collect_metrics(dimension).await {
            Ok(mut metrics) => all.append(&mut metrics),
            Err(e) => {
                tracing::warn!(dimension = %dimension, error = %e, "Observable::collect_metrics failed")
            }
        }
    }
    all
}

/// 按每个 observable 实际分维度的情况采集：声明了维度的按每个维度采一次，
/// `available_dimensions()` 为空的只采一次。
///
/// 空声明是"这个量不随维度变"的意思——卷占用、trace 分层积压这类 gauge 对每个
/// 维度都是同一个数。按维度逐个采它，会让同一组序列在一个抓取体里重复出现；
/// 值一样时 Prometheus 当作重复样本丢掉，值在两采之间变一次就会变成同一时间戳
/// 冲突的样本而被拒，那一条序列在该次抓取里就没有读数。
pub async fn collect_metrics_for_dimensions(
    observables: &[Arc<dyn Observable>],
    dimensions: &[String],
) -> Vec<RawMetric> {
    let mut all = Vec::new();
    for observable in observables {
        if observable.available_dimensions().is_empty() {
            match observable.collect_metrics("").await {
                Ok(mut metrics) => all.append(&mut metrics),
                Err(e) => tracing::warn!(error = %e, "Observable::collect_metrics failed"),
            }
            continue;
        }
        for dimension in dimensions {
            match observable.collect_metrics(dimension).await {
                Ok(mut metrics) => all.append(&mut metrics),
                Err(e) => {
                    tracing::warn!(dimension = %dimension, error = %e, "Observable::collect_metrics failed")
                }
            }
        }
    }
    all
}

/// 从多个 Observable 聚合指定任务的轨迹片段（便捷函数）。
pub async fn collect_all_traces(
    observables: &[Arc<dyn Observable>],
    task_id: &str,
) -> Vec<TraceFragment> {
    let mut all = Vec::new();
    for observable in observables {
        match observable.collect_trace(task_id).await {
            Ok(traces) => all.extend(traces),
            Err(e) => {
                tracing::warn!(task_id = %task_id, error = %e, "Observable::collect_trace failed")
            }
        }
    }
    all
}

// ─── ObservabilityGateway (merged from storage/observability.rs) ───────────

/// Unified observability gateway for the Supervisor layer.
/// Aggregates queries across all 15 data types so that Supervisor only
/// depends on this single interface rather than individual storage clients.
#[async_trait::async_trait]
pub trait ObservabilityGateway: Send + Sync {
    async fn subscribe_events(&self, filter: EventFilter) -> SFResult<AgentEventStream>;

    async fn get_agent_state(&self, agent_id: &str) -> SFResult<crate::AgentState>;

    async fn get_task_checkpoint(&self, task_id: &str) -> SFResult<Option<crate::TaskCheckpoint>>;

    async fn get_task_metrics(&self, task_id: &str) -> SFResult<TaskMetrics>;

    async fn get_task_logs(&self, task_id: &str, limit: usize) -> SFResult<Vec<LogEntry>>;

    async fn get_snapshot_url(&self, snapshot_id: &str) -> SFResult<String>;

    async fn get_raw_log_index(
        &self,
        stream: &str,
        date: chrono::NaiveDate,
    ) -> SFResult<Vec<RawLogIndex>>;

    async fn get_cluster_overview(&self) -> SFResult<ClusterOverview>;

    async fn get_squad_state(&self, squad_id: &str) -> SFResult<SquadState>;

    fn publish_event(&self, event: AgentEvent);
}

// ─── Self-Evolution Metrics ────────────────────────────────────────────────

/// Counter-style metrics for the self-evolution pipeline.
/// Implemented by `cog-observability` and consumed by `cog-reflection`.
#[async_trait::async_trait]
pub trait EvolutionMetrics: Send + Sync {
    async fn record_event(&self, failed: bool);
    async fn record_change_applied(&self);
    async fn record_change_failed(&self);
}

// ─── Infrastructure vs business traffic ────────────────────────────────────

/// Whether an `endpoint` label names an infrastructure route rather than
/// business work.
///
/// Liveness probes (`/health`, `/health/live`, `/health/ready`) and the
/// scraper's own poll of `/metrics` run on a fixed timer, always answer 2xx,
/// and carry no user intent. Folded into the same counter as business
/// requests they pad the denominator of every ratio computed over it: probes
/// tick several times a minute while real traffic can fall to zero, so the
/// ratio ends up dominated by requests that cannot fail and a regression
/// affecting only business endpoints hides inside it.
///
/// The producer keeps these routes out of the business series; consumers
/// filter them out of any body that still carries them, because a scrape can
/// reach a replica running an older revision, or read counter totals written
/// before the producer stopped emitting them. Liveness stays observed: a
/// failing probe surfaces as the pod's readiness state and restart count.
pub fn is_infra_endpoint(endpoint: &str) -> bool {
    endpoint == "/health"
        || endpoint.starts_with("/health/")
        || endpoint == "/metrics"
        || endpoint.starts_with("/metrics/")
}

/// The `endpoint` label of a rendered Prometheus series line, if it has one.
/// `None` for lines without the label: an unclassified series must not be
/// mistaken for infrastructure, since dropping it would lose real traffic.
pub fn series_endpoint(line: &str) -> Option<&str> {
    let rest = line.split_once("endpoint=\"")?.1;
    rest.split_once('"').map(|(value, _)| value)
}

#[cfg(test)]
mod infra_endpoint_tests {
    use super::{
        collect_metrics_for_dimensions, is_infra_endpoint, series_endpoint, Observable, RawMetric,
        SFResult, TraceFragment,
    };
    use std::sync::Arc;

    #[test]
    fn classified_as_infra() {
        for endpoint in [
            "/health",
            "/health/live",
            "/health/ready",
            "/metrics",
            "/metrics/",
        ] {
            assert!(is_infra_endpoint(endpoint), "{endpoint} must be infra");
        }
    }

    #[test]
    fn business_routes_are_not_infra() {
        // `/healthz` and `/metrics-report` only share a prefix with the probe
        // routes; treating them as infra would drop real traffic.
        for endpoint in [
            "/healthz",
            "/metrics-report",
            "/api/v1/tasks",
            "/",
            "unmatched",
        ] {
            assert!(!is_infra_endpoint(endpoint), "{endpoint} must not be infra");
        }
    }

    #[test]
    fn reads_endpoint_label() {
        let line =
            "http_requests_total{method=\"GET\",endpoint=\"/health/ready\",status=\"200\"} 42";
        assert_eq!(series_endpoint(line), Some("/health/ready"));
    }

    #[test]
    fn absent_label_is_not_infra() {
        // A series without the label carries no classification; it must fall
        // through to the caller's default rather than be dropped.
        let line = "http_requests_total{method=\"GET\",status=\"200\"} 42";
        assert_eq!(series_endpoint(line), None);
        assert!(!series_endpoint(line).is_some_and(is_infra_endpoint));
    }

    /// Counts how many times the endpoint asked it, per dimension.
    struct CountingObservable {
        dimensions: Vec<String>,
        asked: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Observable for CountingObservable {
        async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
            self.asked.lock().unwrap().push(dimension.to_string());
            Ok(vec![RawMetric::new("a_gauge", 1.0)])
        }
        async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
            Ok(Vec::new())
        }
        fn available_dimensions(&self) -> Vec<String> {
            self.dimensions.clone()
        }
    }

    /// An observable that ignores the dimension is pulled once, not once per
    /// configured dimension. Pulling it per dimension repeats every series in
    /// the scrape body: identical copies collapse, but a value that moves
    /// between two pulls lands as a second sample for the same timestamp and
    /// the series loses its reading for that scrape.
    #[tokio::test]
    async fn a_dimensionless_observable_is_pulled_once() {
        let dimensions = vec!["D4".to_string(), "D5".to_string(), "D8".to_string()];

        let flat = Arc::new(CountingObservable {
            dimensions: Vec::new(),
            asked: Default::default(),
        });
        let flat_dyn: Arc<dyn Observable> = flat.clone();
        let metrics = collect_metrics_for_dimensions(&[flat_dyn], &dimensions).await;
        assert_eq!(metrics.len(), 1);
        assert_eq!(flat.asked.lock().unwrap().len(), 1);

        // And the other direction: an observable that does branch still gets
        // every configured dimension, so narrowing this cannot silently drop
        // a dimension's metrics.
        let branched = Arc::new(CountingObservable {
            dimensions: vec!["D4".into(), "D5".into()],
            asked: Default::default(),
        });
        let branched_dyn: Arc<dyn Observable> = branched.clone();
        let metrics = collect_metrics_for_dimensions(&[branched_dyn], &dimensions).await;
        assert_eq!(metrics.len(), 3);
        assert_eq!(
            *branched.asked.lock().unwrap(),
            vec!["D4".to_string(), "D5".to_string(), "D8".to_string()]
        );
    }
}
