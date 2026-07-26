use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::{MetricSample, MetricsBackend, SFError, SFResult};
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Registry,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Prometheus metrics exporter.
/// Wraps a `prometheus::Registry` and provides encoding for the
/// `/metrics` HTTP endpoint.  This is the **human-facing** metrics
/// component (design doc 16 DevOps components #1-10).
pub struct MetricsExporter {
    registry: Arc<Registry>,
}

impl MetricsExporter {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Registry::new()),
        }
    }

    pub fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&self.gather(), &mut buffer)?;
        Ok(buffer)
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }
}

impl Default for MetricsExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl cog_core::MetricsExporter for MetricsExporter {
    fn encode(&self) -> cog_core::SFResult<Vec<u8>> {
        self.encode()
            .map_err(|e| cog_core::SFError::Internal(e.to_string()))
    }
}

/// Prometheus-backed `MetricsBackend` implementation.
/// Bridges cog-core's `MetricsBackend` trait with the prometheus crate,
/// so that metrics recorded via `record_counter` / `record_gauge` /
/// `record_histogram` are exposed on the `/metrics` endpoint in
/// standard Prometheus text format.
/// **Human consumer layer** — used by Grafana dashboards and Alertmanager.
pub struct PrometheusMetricsBackend {
    registry: Registry,
    prefix: String,
    counters: std::sync::Mutex<HashMap<String, CounterVec>>,
    gauges: std::sync::Mutex<HashMap<String, GaugeVec>>,
    histograms: std::sync::Mutex<HashMap<String, HistogramVec>>,
}

impl PrometheusMetricsBackend {
    pub fn new(prefix: &str) -> Self {
        Self {
            registry: Registry::new(),
            prefix: prefix.to_string(),
            counters: std::sync::Mutex::new(HashMap::new()),
            gauges: std::sync::Mutex::new(HashMap::new()),
            histograms: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&self.registry.gather(), &mut buffer)?;
        Ok(buffer)
    }

    fn full_name(&self, name: &str) -> String {
        if self.prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}_{}", self.prefix, name)
        }
    }

    fn sorted_label_keys(labels: &HashMap<String, String>) -> Vec<String> {
        let mut keys: Vec<String> = labels.keys().cloned().collect();
        keys.sort();
        keys
    }

    fn get_or_create_counter(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> SFResult<Counter> {
        let mut store = self
            .counters
            .lock()
            .map_err(|_| SFError::Agent("counter lock poisoned".into()))?;
        let sorted_keys = Self::sorted_label_keys(labels);
        let key = format!("{}:{:?}", name, sorted_keys);

        if !store.contains_key(&key) {
            let counter_vec = CounterVec::new(
                prometheus::Opts::new(self.full_name(name), format!("Counter for {}", name)),
                &sorted_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )
            .map_err(|e| SFError::Agent(format!("prometheus counter init: {}", e)))?;
            self.registry.register(Box::new(counter_vec.clone())).ok();
            store.insert(key.clone(), counter_vec);
        }

        let label_values: Vec<&str> = sorted_keys
            .iter()
            .map(|k| labels.get(k).map(|s| s.as_str()).unwrap_or(""))
            .collect();

        store[&key]
            .get_metric_with_label_values(&label_values)
            .map_err(|e| SFError::Agent(format!("prometheus label lookup: {}", e)))
    }

    fn get_or_create_gauge(&self, name: &str, labels: &HashMap<String, String>) -> SFResult<Gauge> {
        let mut store = self
            .gauges
            .lock()
            .map_err(|_| SFError::Agent("gauge lock poisoned".into()))?;
        let sorted_keys = Self::sorted_label_keys(labels);
        let key = format!("{}:{:?}", name, sorted_keys);

        if !store.contains_key(&key) {
            let gauge_vec = GaugeVec::new(
                prometheus::Opts::new(self.full_name(name), format!("Gauge for {}", name)),
                &sorted_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )
            .map_err(|e| SFError::Agent(format!("prometheus gauge init: {}", e)))?;
            self.registry.register(Box::new(gauge_vec.clone())).ok();
            store.insert(key.clone(), gauge_vec);
        }

        let label_values: Vec<&str> = sorted_keys
            .iter()
            .map(|k| labels.get(k).map(|s| s.as_str()).unwrap_or(""))
            .collect();

        store[&key]
            .get_metric_with_label_values(&label_values)
            .map_err(|e| SFError::Agent(format!("prometheus label lookup: {}", e)))
    }

    fn get_or_create_histogram(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> SFResult<Histogram> {
        let mut store = self
            .histograms
            .lock()
            .map_err(|_| SFError::Agent("histogram lock poisoned".into()))?;
        let sorted_keys = Self::sorted_label_keys(labels);
        let key = format!("{}:{:?}", name, sorted_keys);

        if !store.contains_key(&key) {
            let hist_vec = HistogramVec::new(
                HistogramOpts::new(self.full_name(name), format!("Histogram for {}", name))
                    .buckets(prometheus::exponential_buckets(0.001, 2.0, 15).unwrap_or_default()),
                &sorted_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )
            .map_err(|e| SFError::Agent(format!("prometheus histogram init: {}", e)))?;
            self.registry.register(Box::new(hist_vec.clone())).ok();
            store.insert(key.clone(), hist_vec);
        }

        let label_values: Vec<&str> = sorted_keys
            .iter()
            .map(|k| labels.get(k).map(|s| s.as_str()).unwrap_or(""))
            .collect();

        store[&key]
            .get_metric_with_label_values(&label_values)
            .map_err(|e| SFError::Agent(format!("prometheus label lookup: {}", e)))
    }
}

#[async_trait]
impl MetricsBackend for PrometheusMetricsBackend {
    async fn record_gauge(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let gauge = self.get_or_create_gauge(name, &labels)?;
        gauge.set(value);
        Ok(())
    }

    async fn record_counter(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let counter = self.get_or_create_counter(name, &labels)?;
        counter.inc_by(value);
        Ok(())
    }

    async fn record_histogram(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let hist = self.get_or_create_histogram(name, &labels)?;
        hist.observe(value);
        Ok(())
    }

    async fn query_gauge_range(
        &self,
        _name: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        // Prometheus does not support ad-hoc range queries on raw samples
        // in the client library. For production, use PromQL via HTTP API.
        Ok(Vec::new())
    }

    async fn query_counter_range(
        &self,
        _name: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        Ok(Vec::new())
    }

    async fn query_histogram_range(
        &self,
        _name: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        Ok(Vec::new())
    }

    async fn health_check(&self) -> SFResult<()> {
        let _ = self.registry.gather();
        Ok(())
    }
}

/// Convenience helper for recording task-level metrics.
pub struct TaskMetricsRecorder {
    backend: Arc<dyn MetricsBackend>,
    task_id: String,
}

impl TaskMetricsRecorder {
    pub fn new(backend: Arc<dyn MetricsBackend>, task_id: impl Into<String>) -> Self {
        Self {
            backend,
            task_id: task_id.into(),
        }
    }

    pub async fn record_llm_call(
        &self,
        model: &str,
        latency_ms: f64,
        tokens_in: u64,
        tokens_out: u64,
    ) {
        let mut labels = HashMap::new();
        labels.insert("task_id".into(), self.task_id.clone());
        labels.insert("model".into(), model.into());

        let _ = self
            .backend
            .record_histogram("llm_call_latency_ms", latency_ms, labels.clone())
            .await;
        let _ = self
            .backend
            .record_counter("llm_tokens_total", (tokens_in + tokens_out) as f64, labels)
            .await;
    }

    pub async fn record_tool_call(&self, tool_name: &str, latency_ms: f64, success: bool) {
        let mut labels = HashMap::new();
        labels.insert("task_id".into(), self.task_id.clone());
        labels.insert("tool_name".into(), tool_name.into());
        labels.insert(
            "status".into(),
            if success {
                "success".into()
            } else {
                "failure".into()
            },
        );

        let _ = self
            .backend
            .record_histogram("tool_call_latency_ms", latency_ms, labels.clone())
            .await;
        let _ = self
            .backend
            .record_counter("tool_calls_total", 1.0, labels)
            .await;
    }

    pub async fn record_step(&self, iteration: u32) {
        let mut labels = HashMap::new();
        labels.insert("task_id".into(), self.task_id.clone());
        labels.insert("iteration".into(), iteration.to_string());
        let _ = self
            .backend
            .record_counter("agent_steps_total", 1.0, labels)
            .await;
    }
}
