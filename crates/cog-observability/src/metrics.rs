use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::{MetricSample, MetricsBackend, SFError, SFResult};
use prometheus::core::Collector;
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

    fn histogram_buckets(name: &str) -> Vec<f64> {
        // The unit has to match the observations. A seconds-scale scheme fed
        // millisecond values puts every observation in the top bucket, and
        // `histogram_quantile` then reports that bucket's boundary as if it
        // were the measured latency — a plausible-looking number that is not a
        // measurement. The `_ms` suffix is the series' own unit declaration, so
        // it selects the scheme; a name that declares no unit keeps the
        // seconds-scale default.
        if name.ends_with("_ms") {
            // 1 ms doubling up to ~65 s: covers a fast local call and a slow
            // model turn without spending buckets below a millisecond.
            prometheus::exponential_buckets(1.0, 2.0, 17).unwrap_or_default()
        } else {
            prometheus::exponential_buckets(0.001, 2.0, 15).unwrap_or_default()
        }
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
                    .buckets(Self::histogram_buckets(name)),
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

    /// The registry holds real cumulative counters, so the total of every label
    /// set is readable directly from the `CounterVec` children.
    async fn query_counter_totals(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        let full = self.full_name(name);
        let store = self
            .counters
            .lock()
            .map_err(|_| SFError::Agent("counter lock poisoned".into()))?;
        let mut samples = Vec::new();
        for vec in store.values() {
            for family in vec.collect() {
                if family.get_name() != full {
                    continue;
                }
                for metric in family.get_metric() {
                    let labels: HashMap<String, String> = metric
                        .get_label()
                        .iter()
                        .map(|l| (l.get_name().to_string(), l.get_value().to_string()))
                        .collect();
                    samples.push(MetricSample {
                        timestamp: Utc::now(),
                        value: metric.get_counter().get_value(),
                        labels,
                    });
                }
            }
        }
        Ok(samples)
    }

    async fn query_histogram_range(
        &self,
        _name: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        Ok(Vec::new())
    }

    async fn list_metric_names(&self, metric_type: cog_core::MetricType) -> SFResult<Vec<String>> {
        // The per-kind maps are keyed by `name:label-keys`, so one recorded name
        // appears once per label set it has seen; the name is the part before
        // the first colon, and the caller wants each name once.
        fn names_of<V>(store: &std::sync::Mutex<HashMap<String, V>>) -> SFResult<Vec<String>> {
            let store = store
                .lock()
                .map_err(|_| SFError::Agent("metrics lock poisoned".into()))?;
            let mut names: Vec<String> = store
                .keys()
                .filter_map(|key| key.split(':').next())
                .map(str::to_string)
                .collect();
            names.sort_unstable();
            names.dedup();
            Ok(names)
        }

        match metric_type {
            cog_core::MetricType::Gauge => names_of(&self.gauges),
            cog_core::MetricType::Counter => names_of(&self.counters),
            cog_core::MetricType::Histogram => names_of(&self.histograms),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 毫秒量级的观测必须落在多个桶里。秒量级的桶界配毫秒值会把每一条观测都塞进
    /// 最上面的桶，`histogram_quantile` 于是把桶上界当作实测延迟报出来——一个看起来
    /// 合理、却不是测量的数。这条断言钉住"单位对上"，而不是钉住具体桶界。
    #[test]
    fn millisecond_series_get_a_bucket_scheme_that_resolves_milliseconds() {
        let buckets = PrometheusMetricsBackend::histogram_buckets("llm_call_latency_ms");
        let finite: Vec<f64> = buckets.iter().copied().filter(|b| b.is_finite()).collect();

        assert!(
            finite.first().copied().unwrap_or(f64::MAX) <= 1.0,
            "最下面的桶界要在 1ms 以内，否则快调用分不出来: {finite:?}"
        );
        assert!(
            finite.last().copied().unwrap_or(0.0) >= 30_000.0,
            "最上面的桶界要盖过慢模型的一分钟级延迟: {finite:?}"
        );
        // 300ms 这种典型观测不该和最上面的桶界贴在同一个桶里。
        assert!(
            finite.iter().any(|b| *b >= 256.0 && *b < 512.0),
            "300ms 量级的观测要有自己的桶界，否则 P95 被粗化成桶上界: {finite:?}"
        );
    }

    /// 不带 `_ms` 的名字保留秒量级默认，免得给一个按秒记的序列套上毫秒桶界。
    #[test]
    fn a_series_that_declares_no_unit_keeps_the_seconds_scale_default() {
        let buckets = PrometheusMetricsBackend::histogram_buckets("some_future_duration");
        let finite: Vec<f64> = buckets.iter().copied().filter(|b| b.is_finite()).collect();

        assert!(
            finite.first().copied().unwrap_or(f64::MAX) <= 0.01,
            "单位未声明时保持秒量级默认: {finite:?}"
        );
    }
}
