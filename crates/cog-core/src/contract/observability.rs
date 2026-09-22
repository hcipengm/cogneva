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

/// 一个 observable 会按它分支的维度，以及它在该维度上的序列基数是否有界。
///
/// `bounded` 是给**消费侧**看的：只有有界的维度才允许进周期抓取。逐对象取键的
/// 维度（按 `task_id` 逐 step、按会话 id 逐消息……）在长跑里只会增长，抓它等于把
/// 无界基数引进抓取体，时间一长既压垮抓取也压垮存储。
///
/// 有界性挂在**这个 observable 的这个维度**上，不是挂在维度名上：同一个 "D1" 在
/// 编排面上是累计计数器（有界），在 agent 面上是逐 step 记录（无界），按名字一刀
/// 切会把有界的那半也挡在外面。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionSpec {
    pub name: String,
    pub bounded: bool,
}

impl DimensionSpec {
    /// 该维度的序列基数有界，可以进周期抓取。
    pub fn bounded(name: &str) -> Self {
        Self {
            name: name.into(),
            bounded: true,
        }
    }

    /// 该维度逐对象取键，基数无界，不得进周期抓取；仍可由明确知道自己在干什么的
    /// 消费者按需单采。
    pub fn unbounded(name: &str) -> Self {
        Self {
            name: name.into(),
            bounded: false,
        }
    }
}

/// 可观测性 trait —— 各业务 crate 实现此 trait 暴露原始数据。
#[async_trait::async_trait]
pub trait Observable: Send + Sync {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>>;
    async fn collect_trace(&self, task_id: &str) -> SFResult<Vec<TraceFragment>>;
    /// 这个 observable 会按哪些维度分支，以及每个维度的基数是否有界。
    ///
    /// 返回空集是"读数不随维度变"的意思：卷占用、trace 分层积压这类 gauge 对每个
    /// 维度都是同一个数，采集侧只采一次。
    fn available_dimensions(&self) -> Vec<DimensionSpec>;
}

/// 按每个 observable 自己声明的维度采集：只问它声明过、且声明为有界、且没被
/// `allow` 挡在外面的那些维度；一个可采的维度都没有的 observable 只采一次。
///
/// 问谁、问哪些，判据都在产出侧——`available_dimensions()` 是产出侧对自己行为的
/// 陈述，消费侧照它来。手写一张"抓哪些维度"的清单当权威，新声明的有界维度会静默
/// 不进抓取（产出侧加了个维度，什么都不报错，就是读数永远不出现），而没声明过的
/// 维度会被问一遍：那些返回空还好，返回"与维度无关的那部分读数"的就会在同一个抓取
/// 体里重复出现——值一样时 Prometheus 当作重复样本丢掉，值在两采之间变一次就变成
/// 同一时间戳冲突的样本而被拒，那一条序列在该次抓取里没有读数。
///
/// `allow` 是叠加在声明之上的**收窄**，不是替代：空 = 不限额，按声明的有界维度全采；
/// 非空 = 只采其中被点名的。无界的维度无论在不在 `allow` 里都不采——它是否该进抓取
/// 由产出侧的有界性声明决定，不由部署方的一个字符串决定。
pub async fn collect_metrics_for_dimensions(
    observables: &[Arc<dyn Observable>],
    allow: &[String],
) -> Vec<RawMetric> {
    let unrestricted = allow.is_empty();
    let mut all = Vec::new();
    for observable in observables {
        let wanted: Vec<String> = observable
            .available_dimensions()
            .into_iter()
            .filter(|spec| {
                spec.bounded && (unrestricted || allow.iter().any(|name| name == &spec.name))
            })
            .map(|spec| spec.name)
            .collect();

        if wanted.is_empty() {
            match observable.collect_metrics("").await {
                Ok(mut metrics) => all.append(&mut metrics),
                Err(e) => tracing::warn!(error = %e, "Observable::collect_metrics failed"),
            }
            continue;
        }
        for dimension in wanted {
            match observable.collect_metrics(&dimension).await {
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

/// Metric names this codebase used to publish and no longer does.
///
/// A retired name is not a reading. Nothing will write it again, so its newest
/// sample is the newest it will ever have — and the sample log's rule that each
/// series keeps its newest row would hold that row forever, which is how a
/// renamed metric becomes a series `/metrics` serves and nothing ever updates.
/// Naming the retirement here is what lets the last row age out with the rest.
///
/// The list lives in core rather than beside the exporter because the sweep
/// needs it too, and two copies would drift — the drift being silent, a name
/// retired in one copy and not the other leaves exactly the zombie this list
/// exists to prevent.
///
/// Removing a name from the code that recorded it means adding it here. That is
/// the one maintenance step, and forgetting it reproduces today's behaviour
/// rather than deleting something a live series needed: the list only ever
/// widens what the sweep may delete.
pub const RETIRED_METRIC_NAMES: &[&str] = &["metrics_samples_retention_seconds"];

/// Whether `name` is a retired metric. See [`RETIRED_METRIC_NAMES`].
pub fn is_retired_metric(name: &str) -> bool {
    RETIRED_METRIC_NAMES.contains(&name)
}

#[cfg(test)]
mod infra_endpoint_tests {
    use super::{
        collect_metrics_for_dimensions, is_infra_endpoint, is_retired_metric, series_endpoint,
        DimensionSpec, Observable, RawMetric, SFResult, TraceFragment,
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
        dimensions: Vec<DimensionSpec>,
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
        fn available_dimensions(&self) -> Vec<DimensionSpec> {
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
    }

    /// An observable is asked for the dimensions it declares, and for nothing
    /// else. Asking a branched observable for a dimension it does not answer
    /// is not harmless: an implementation that also returns a
    /// dimension-independent core emits that core again for every extra
    /// question, and the second copy of a counter that moved between the two
    /// calls is a duplicate sample for one timestamp, which loses the series
    /// for that scrape.
    #[tokio::test]
    async fn a_branched_observable_is_asked_only_what_it_declares() {
        let dimensions = vec!["D4".to_string(), "D5".to_string(), "D8".to_string()];

        let branched = Arc::new(CountingObservable {
            dimensions: vec![DimensionSpec::bounded("D4"), DimensionSpec::bounded("D5")],
            asked: Default::default(),
        });
        let branched_dyn: Arc<dyn Observable> = branched.clone();
        collect_metrics_for_dimensions(&[branched_dyn], &dimensions).await;
        assert_eq!(
            *branched.asked.lock().unwrap(),
            vec!["D4".to_string(), "D5".to_string()]
        );
    }

    /// A dimension declared unbounded stays out of the scrape even when the
    /// allowance names it, and even when nothing else is left to ask — the
    /// observable is then pulled once for whatever it has that does not vary
    /// by dimension, which is exactly the readings that are safe to take.
    #[tokio::test]
    async fn an_unbounded_dimension_is_not_scraped_even_when_named() {
        let unbounded_only = Arc::new(CountingObservable {
            dimensions: vec![
                DimensionSpec::unbounded("D1"),
                DimensionSpec::unbounded("D2"),
            ],
            asked: Default::default(),
        });
        let unbounded_dyn: Arc<dyn Observable> = unbounded_only.clone();

        // No allowance: the declaration is the whole rule.
        collect_metrics_for_dimensions(std::slice::from_ref(&unbounded_dyn), &[]).await;
        assert_eq!(*unbounded_only.asked.lock().unwrap(), vec!["".to_string()]);

        // Naming it explicitly must not open it: whether it may be scraped is
        // the producer's boundedness claim, not a deployment's string.
        unbounded_only.asked.lock().unwrap().clear();
        collect_metrics_for_dimensions(&[unbounded_dyn], &["D1".to_string()]).await;
        assert_eq!(*unbounded_only.asked.lock().unwrap(), vec!["".to_string()]);
    }

    /// An allowance narrows what is declared; it does not replace it. A
    /// dimension nobody declares is asked of nobody, so listing it changes
    /// nothing — and an empty allowance means "no narrowing", not "ask
    /// nothing", which would silently blank the whole endpoint.
    #[tokio::test]
    async fn the_allowance_only_narrows() {
        let declared = Arc::new(CountingObservable {
            dimensions: vec![DimensionSpec::bounded("D4"), DimensionSpec::bounded("D6")],
            asked: Default::default(),
        });
        let declared_dyn: Arc<dyn Observable> = declared.clone();

        collect_metrics_for_dimensions(std::slice::from_ref(&declared_dyn), &[]).await;
        assert_eq!(
            *declared.asked.lock().unwrap(),
            vec!["D4".to_string(), "D6".to_string()]
        );

        declared.asked.lock().unwrap().clear();
        collect_metrics_for_dimensions(&[declared_dyn], &["D4".to_string(), "D9".to_string()])
            .await;
        assert_eq!(*declared.asked.lock().unwrap(), vec!["D4".to_string()]);
    }

    /// The one name currently retired has to be one, because the sweep's
    /// exemption is now keyed on this list: a list that matched nothing would
    /// leave the zombie series in place while looking like it was handled.
    #[test]
    fn the_retired_name_is_matched_by_the_lookup() {
        assert!(
            is_retired_metric("metrics_samples_retention_seconds"),
            "the retired series the sweep has to release is not matched"
        );
    }

    /// And it has to match exactly one name — a predicate that matched
    /// everything would strip the newest-row floor from every live series.
    #[test]
    fn the_lookup_matches_nothing_else() {
        for name in [
            "metrics_samples_rows",
            "memory_operations_total",
            "memory_unextracted_raw",
        ] {
            assert!(!is_retired_metric(name), "{name} is a live series");
        }
    }
}
