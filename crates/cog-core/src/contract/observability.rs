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
