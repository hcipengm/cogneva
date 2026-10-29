//! 测试面的内存 metrics 替身。
//!
//! 这个 crate 不拥有 Prometheus 注册表，也不拥有任何落盘后端——注册表是实现，
//! 跟着拥有导出格式的 crate 走；网关只持有 [`cog_core::MetricsBackend`] 与
//! [`cog_core::ScrapableMetrics`] 两个契约。但网关自己的测试要问两件事：这一拍
//! 写了哪条、值是多少（抓取文本），以及某个系列现在有几个样本（按名字取总数）。
//! 替身两样都答，测试就不必为了一个内存后端去依赖存储 crate。
//!
//! 它不判定抓取文本的语法正确性——那条由导出 crate 自己的测试负责。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use cog_core::{MetricName, MetricSample, MetricType, MetricsBackend, SFResult};

/// 一个系列的键：指标名 + 按名字排好的标签。
type SeriesKey = (String, Vec<(String, String)>);

fn series_key(name: MetricName, labels: &HashMap<String, String>) -> SeriesKey {
    let mut pairs: Vec<(String, String)> =
        labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    pairs.sort();
    (name.as_str().to_string(), pairs)
}

fn render_key(key: &SeriesKey) -> String {
    if key.1.is_empty() {
        return key.0.clone();
    }
    let rendered: Vec<String> = key.1.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
    format!("{}{{{}}}", key.0, rendered.join(","))
}

fn render_value(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

fn sample(key: &SeriesKey, value: f64) -> MetricSample {
    MetricSample {
        timestamp: chrono::Utc::now(),
        value,
        labels: key.1.iter().cloned().collect(),
    }
}

/// 内存里的 metrics 后端：gauge 覆盖写、counter 累加、直方图记次数与和。
#[derive(Default)]
pub struct TestMetrics {
    series: Mutex<BTreeMap<SeriesKey, f64>>,
    /// 每个系列是哪种东西。抓取面按类型枚举名字，所以替身也得记得住——否则
    /// 「后端持有什么」这一问在它身上永远是空集。
    kinds: Mutex<BTreeMap<String, MetricType>>,
    /// (观测次数, 观测和)。桶的划分是真实编码器的事。
    histograms: Mutex<BTreeMap<SeriesKey, (u64, f64)>>,
}

impl TestMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn already_of(&self, name: MetricName, kind: MetricType) {
        self.kinds
            .lock()
            .unwrap()
            .entry(name.as_str().to_string())
            .or_insert(kind);
    }

    fn matching(&self, name: &str, kind: MetricType) -> Vec<MetricSample> {
        if self.kinds.lock().unwrap().get(name) != Some(&kind) {
            return Vec::new();
        }
        self.series
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.0 == name)
            .map(|(key, value)| sample(key, *value))
            .collect()
    }
}

#[async_trait::async_trait]
impl MetricsBackend for TestMetrics {
    async fn record_gauge(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.already_of(name, MetricType::Gauge);
        self.series
            .lock()
            .unwrap()
            .insert(series_key(name, &labels), value);
        Ok(())
    }

    async fn record_counter(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.already_of(name, MetricType::Counter);
        *self
            .series
            .lock()
            .unwrap()
            .entry(series_key(name, &labels))
            .or_insert(0.0) += value;
        Ok(())
    }

    async fn record_histogram(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.already_of(name, MetricType::Histogram);
        let mut held = self.histograms.lock().unwrap();
        let slot = held.entry(series_key(name, &labels)).or_insert((0, 0.0));
        slot.0 += 1;
        slot.1 += value;
        Ok(())
    }

    async fn query_gauge_range(
        &self,
        _: &str,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        Ok(Vec::new())
    }

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        Ok(self.matching(name, MetricType::Gauge))
    }

    async fn query_counter_range(
        &self,
        _: &str,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        Ok(Vec::new())
    }

    async fn query_counter_totals(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        Ok(self.matching(name, MetricType::Counter))
    }

    async fn query_histogram_totals(&self, name: &str) -> SFResult<Vec<cog_core::HistogramTotals>> {
        Ok(self
            .histograms
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.0 == name)
            .map(|(key, (count, sum))| cog_core::HistogramTotals {
                labels: key.1.iter().cloned().collect(),
                // 这个替身不划桶：每条观测都在顶上那一档，所以整批都记成溢出。
                // 调用方问的是"量了几次、合计多少"，这两格是真的。
                buckets: Vec::new(),
                overflow: *count,
                count: *count,
                sum: *sum,
                updated_at: chrono::Utc::now(),
            })
            .collect())
    }

    async fn query_histogram_range(
        &self,
        _: &str,
        _: chrono::DateTime<chrono::Utc>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        Ok(Vec::new())
    }

    async fn list_metric_names(&self, kind: MetricType) -> SFResult<Vec<String>> {
        Ok(self
            .kinds
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, held)| **held == kind)
            .map(|(name, _)| name.clone())
            .collect())
    }

    async fn health_check(&self) -> SFResult<()> {
        Ok(())
    }
}

impl cog_core::ScrapableMetrics for TestMetrics {
    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = String::new();
        for (key, value) in self.series.lock().unwrap().iter() {
            out.push_str(&render_key(key));
            out.push(' ');
            out.push_str(&render_value(*value));
            out.push('\n');
        }
        for (key, (count, sum)) in self.histograms.lock().unwrap().iter() {
            out.push_str(&format!("{}_count {count}\n", render_key(key)));
            out.push_str(&format!("{}_sum {}\n", render_key(key), render_value(*sum)));
        }
        Ok(out.into_bytes())
    }
}

/// 一份可共享的替身，按网关状态里那一格的类型给出。
pub fn test_metrics() -> Arc<dyn cog_core::ScrapableMetrics> {
    Arc::new(TestMetrics::new())
}
