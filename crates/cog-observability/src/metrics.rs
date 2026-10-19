use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::{HistogramTotals, MetricName, MetricSample, MetricsBackend, SFError, SFResult};
use prometheus::core::Collector;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Registry,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Turn a registry's cumulative bucket counts into the per-bucket counts
/// [`HistogramTotals`] carries.
///
/// The registry reports each bucket as everything at or below its bound,
/// because that is what an exposition needs. The shared type stores buckets as
/// they are accumulated, one observation in exactly one bucket, so the
/// cumulative series has to be differenced back.
fn differential_buckets(cumulative: Vec<(f64, u64)>) -> Vec<(f64, u64)> {
    let mut out = Vec::with_capacity(cumulative.len());
    let mut previous = 0u64;
    for (bound, count) in cumulative {
        out.push((bound, count.saturating_sub(previous)));
        previous = count;
    }
    out
}

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
                prometheus::Opts::new(
                    self.full_name(name),
                    // The wording comes from the contract crate's registry, not
                    // from this file: the same series is served on another
                    // exposition too, and a `Counter for <name>` line here would
                    // describe one series two ways — leaving the reader of this
                    // body, which is the one a scrape actually reaches, with the
                    // version that says nothing about the measurement.
                    cog_core::metric_help_text(cog_core::MetricType::Counter, name),
                ),
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
                prometheus::Opts::new(
                    self.full_name(name),
                    cog_core::metric_help_text(cog_core::MetricType::Gauge, name),
                ),
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
        // The scheme is shared with the backends that accumulate bucket counts
        // by hand, so that a metric name describes the same boundaries
        // whichever backend is behind it.
        cog_core::histogram_bucket_bounds(name)
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
                HistogramOpts::new(
                    self.full_name(name),
                    cog_core::metric_help_text(cog_core::MetricType::Histogram, name),
                )
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
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let gauge = self.get_or_create_gauge(name.as_str(), &labels)?;
        gauge.set(value);
        Ok(())
    }

    async fn record_counter(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let counter = self.get_or_create_counter(name.as_str(), &labels)?;
        counter.inc_by(value);
        Ok(())
    }

    async fn record_histogram(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let hist = self.get_or_create_histogram(name.as_str(), &labels)?;
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

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        // A registry gauge holds one value per label set, already the newest by
        // construction: `set` replaces rather than appends.
        let full = self.full_name(name);
        let store = self
            .gauges
            .lock()
            .map_err(|_| SFError::Agent("gauge lock poisoned".into()))?;
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
                        value: metric.get_gauge().get_value(),
                        labels,
                    });
                }
            }
        }
        Ok(samples)
    }

    /// The registry's histograms already carry cumulative buckets, so they are
    /// readable directly rather than re-derived from the observations.
    async fn query_histogram_totals(&self, name: &str) -> SFResult<Vec<HistogramTotals>> {
        let full = self.full_name(name);
        let store = self
            .histograms
            .lock()
            .map_err(|_| SFError::Agent("histogram lock poisoned".into()))?;
        let mut totals = Vec::new();
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

                    // The boundaries come from the registry rather than from
                    // the shared scheme: a histogram created before the scheme
                    // last changed still reports the buckets it was actually
                    // observed into.
                    let histogram = metric.get_histogram();
                    let mut cumulative: Vec<(f64, u64)> = Vec::new();
                    let mut overflow = 0u64;
                    for bucket in histogram.get_bucket() {
                        let bound = bucket.get_upper_bound();
                        if bound.is_finite() {
                            cumulative.push((bound, bucket.get_cumulative_count()));
                        } else {
                            overflow = bucket.get_cumulative_count();
                        }
                    }
                    cumulative
                        .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

                    totals.push(HistogramTotals {
                        labels,
                        buckets: differential_buckets(cumulative),
                        overflow,
                        count: histogram.get_sample_count(),
                        sum: histogram.get_sample_sum(),
                        // The registry is the live accumulator itself, read here
                        // rather than a stored copy of it: everything recorded
                        // before this instant is already in the counts, so now
                        // is the only answer available and the right one. There
                        // is no stale reading to warn about — a registry that
                        // can be read is a producer that is still running.
                        updated_at: Utc::now(),
                    });
                }
            }
        }
        Ok(totals)
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

#[cfg(test)]
mod tests {
    //! principle: feedback_a_series_name_that_is_only_a_prefix_of_its_literal_escapes_the_census
    //!
    //! 这个模块带着从当前树、单一修订回答该原则的门：抓取面普查的反向扫描只
    //! 认「整条字面量就是序列名」的出现，嵌在 `# HELP` 更长字面量里的不算。
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

    /// 被抓取的那个面上，说明文本必须就是登记过的那句。
    ///
    /// 这条断的是**承载序列的那个载体**：池指标只出现在这条进程内导出面上，
    /// 描述表若只喂给另一条按存储渲染的面，读者拿到的就是一句
    /// `Gauge for llm_upstream_healthy`——名字重复一遍，不含任何信息，而
    /// "序列缺席 = 从没被观测过" 这个语义恰好是读者最需要被告知的一条。
    #[tokio::test]
    async fn a_described_series_is_served_with_the_registered_wording() {
        let backend = PrometheusMetricsBackend::new("");
        backend
            .record_gauge(
                MetricName::for_tests_only(cog_core::metric_names::LLM_UPSTREAM_HEALTHY.as_str()),
                0.0,
                HashMap::new(),
            )
            .await
            .unwrap();

        let body = String::from_utf8(backend.encode().unwrap()).unwrap();
        let registered = cog_core::metric_description(
            cog_core::MetricType::Gauge,
            cog_core::metric_names::LLM_UPSTREAM_HEALTHY.as_str(),
        )
        .expect("这条序列在登记表里");
        assert!(
            body.contains(registered),
            "导出面必须带登记过的那句说明，而不是 `Gauge for <name>`: {body}"
        );
    }

    /// 没登记的名字仍要带一句能指出缺口的说明，而不是被丢掉：丢一条序列是
    /// 拿掉一个测量，留下名字重复一遍只是少一句话。
    #[tokio::test]
    async fn an_undescribed_series_is_still_served_with_a_placeholder() {
        let backend = PrometheusMetricsBackend::new("");
        backend
            .record_gauge(
                MetricName::for_tests_only("some_future_gauge"),
                7.0,
                HashMap::new(),
            )
            .await
            .unwrap();

        let body = String::from_utf8(backend.encode().unwrap()).unwrap();
        assert!(body.contains("some_future_gauge 7"), "{body}");
        assert!(
            body.contains("Undocumented metric some_future_gauge"),
            "无描述的序列要指出缺的是哪一步: {body}"
        );
    }

    /// A counter's zero increment still creates the series, and that is the
    /// whole reason a publisher can seed a domain it has no events for: the
    /// scrape then carries the class at zero instead of omitting it, and
    /// "nothing has happened yet" stops reading like "never wired up".
    #[tokio::test]
    async fn a_zero_increment_puts_a_counter_on_the_scrape() {
        let backend = PrometheusMetricsBackend::new("");
        backend
            .record_counter(
                MetricName::for_tests_only("some_future_total"),
                0.0,
                HashMap::from([("fate".to_string(), "landed".to_string())]),
            )
            .await
            .unwrap();

        let body = String::from_utf8(backend.encode().unwrap()).unwrap();
        assert!(
            body.contains("some_future_total{fate=\"landed\"} 0"),
            "a zero increment must still put the series on the scrape: {body}"
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

    /// 抓取面普查的反向扫描：一条序列名只有作为整条字符串字面量出现——名字
    /// 之后紧跟闭合引号——时才算被读到；嵌在 `# HELP <name> ...` 这类更长
    /// 字面量里的名字只是前缀，必须逃出普查。
    fn census_reads_series(source: &str, name: &str) -> bool {
        let bytes = source.as_bytes();
        let wanted = name.as_bytes();
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            if bytes[cursor] != b'"' {
                cursor += 1;
                continue;
            }
            let content_start = cursor + 1;
            let mut content_end = content_start;
            while content_end < bytes.len() && bytes[content_end] != b'"' {
                if bytes[content_end] == b'\\' && content_end + 1 < bytes.len() {
                    content_end += 2;
                } else {
                    content_end += 1;
                }
            }
            if bytes.get(content_start..content_end) == Some(wanted) {
                return true;
            }
            cursor = content_end + 1;
        }
        false
    }

    /// 普查的反向扫描要求「名字之后紧跟一个引号」：整条字面量就是这个名字。
    /// 渲染代码把名字写在更长的 `# HELP <name> ...` 字面量里，那种出现不
    /// 能把序列数成在场；只有真正读这个序列的人写出的整条名字面量才算。
    #[tokio::test]
    async fn a_series_name_that_is_only_a_prefix_of_its_literal_escapes_the_census() {
        let name = "principle_prefix_census_probe";
        let backend = PrometheusMetricsBackend::new("");
        backend
            .record_gauge(MetricName::for_tests_only(name), 3.0, HashMap::new())
            .await
            .unwrap();
        let body = String::from_utf8(backend.encode().unwrap()).unwrap();

        // 一次渲染同时带着两种出现：HELP 行把名字包进更长的文本，序列行上
        // 名字是自己的记号。
        assert!(
            body.contains(format!("# HELP {name} ").as_str()),
            "渲染必须带着把名字包进更长文本的 HELP 行: {body}"
        );
        assert!(
            body.contains(format!("{name} 3").as_str()),
            "序列必须以自己的记号出现在抓取面上: {body}"
        );

        let help_line = body
            .lines()
            .find(|line| line.starts_with("# HELP "))
            .expect("rendered exposition carries a HELP line");

        // 只有更长的 HELP 字面量时，名字后面是空格而不是引号，普查数不到。
        let longer_literal = format!("\"{help_line}\"");
        assert!(
            !census_reads_series(&longer_literal, name),
            "只作为更长字面量前缀的名字必须逃出普查: {longer_literal}"
        );
        // 名字后面拖一截更长的尾巴，整条字面量仍不是这个名字。
        let stretched = format!("\"{name}_and_then_some\"");
        assert!(
            !census_reads_series(&stretched, name),
            "名字是字面量前缀而非整条时不算读到: {stretched}"
        );

        // 整条字面量就是名字时闭合引号紧跟其后，这才是普查认的那种出现。
        let with_reader = format!("\"{name}\"\n{longer_literal}");
        assert!(
            census_reads_series(&with_reader, name),
            "整条字面量就是名字时普查必须数到: {with_reader}"
        );
    }
}
