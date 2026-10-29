//! 一个内存 `MetricsBackend`：让用例能读回「这一拍写没写、写了多少」。
//!
//! 和 [`crate::MemoryStateBackend`] 同一个理由：被测 crate 要的是一面能记数的板子，
//! 不是存储 crate 的那块。真实后端的判题规则（去重、分桶、上限）属于它自己的测试。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::{
    HistogramTotals, MetricName, MetricSample, MetricType, MetricsBackend, SFError, SFResult,
};
use std::collections::HashMap;
use std::sync::RwLock;

// ─── In-memory implementation ───

#[derive(Debug, Default)]
pub struct MemoryMetricsBackend {
    gauges: RwLock<HashMap<String, Vec<MetricSample>>>,
    counters: RwLock<HashMap<String, Vec<MetricSample>>>,
    histograms: RwLock<HashMap<String, Vec<MetricSample>>>,
}

impl MemoryMetricsBackend {
    pub fn new() -> Self {
        Self {
            gauges: RwLock::new(HashMap::new()),
            counters: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    fn push_sample(
        store: &mut HashMap<String, Vec<MetricSample>>,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) {
        store.entry(name.into()).or_default().push(MetricSample {
            timestamp: Utc::now(),
            value,
            labels,
        });
    }

    /// Group key for a label set. Derived from label names so that two samples
    /// carrying the same labels never land in different groups just because
    /// their maps happen to iterate differently.
    fn label_key(labels: &HashMap<String, String>) -> String {
        let mut pairs: Vec<(&String, &String)> = labels.iter().collect();
        pairs.sort_unstable();
        pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The newest sample of each label set, in no particular order.
    fn latest_per_label_set(samples: &[MetricSample]) -> Vec<MetricSample> {
        let mut latest: HashMap<String, MetricSample> = HashMap::new();
        for sample in samples {
            let key = Self::label_key(&sample.labels);
            match latest.get(&key) {
                Some(existing) if existing.timestamp >= sample.timestamp => {}
                _ => {
                    latest.insert(key, sample.clone());
                }
            }
        }
        latest.into_values().collect()
    }

    fn query_range(
        store: &HashMap<String, Vec<MetricSample>>,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<MetricSample> {
        store
            .get(name)
            .map(|samples| {
                samples
                    .iter()
                    .filter(|s| s.timestamp >= start && s.timestamp <= end)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl MetricsBackend for MemoryMetricsBackend {
    async fn record_gauge(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let mut store = self
            .gauges
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Self::push_sample(&mut store, name.as_str(), value, labels);
        Ok(())
    }

    async fn record_counter(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let mut store = self
            .counters
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Self::push_sample(&mut store, name.as_str(), value, labels);
        Ok(())
    }

    async fn record_histogram(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        let mut store = self
            .histograms
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Self::push_sample(&mut store, name.as_str(), value, labels);
        Ok(())
    }

    async fn query_gauge_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let store = self
            .gauges
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::query_range(&store, name, start, end))
    }

    async fn query_counter_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let store = self
            .counters
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::query_range(&store, name, start, end))
    }

    async fn query_counter_totals(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        let store = self
            .counters
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut totals: HashMap<String, MetricSample> = HashMap::new();
        for sample in store.get(name).map(Vec::as_slice).unwrap_or_default() {
            let key = Self::label_key(&sample.labels);
            match totals.get_mut(&key) {
                Some(existing) => existing.value += sample.value,
                None => {
                    totals.insert(key, sample.clone());
                }
            }
        }
        Ok(totals.into_values().collect())
    }

    async fn query_histogram_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let store = self
            .histograms
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::query_range(&store, name, start, end))
    }

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        let store = self
            .gauges
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(Self::latest_per_label_set(
            store.get(name).map(Vec::as_slice).unwrap_or_default(),
        ))
    }

    async fn query_histogram_totals(&self, name: &str) -> SFResult<Vec<HistogramTotals>> {
        let store = self
            .histograms
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let bounds = cog_core::histogram_bucket_bounds(name);

        let mut series: HashMap<String, HistogramTotals> = HashMap::new();
        for sample in store.get(name).map(Vec::as_slice).unwrap_or_default() {
            let key = Self::label_key(&sample.labels);
            let totals = series.entry(key).or_insert_with(|| HistogramTotals {
                labels: sample.labels.clone(),
                buckets: bounds.iter().map(|b| (*b, 0)).collect(),
                overflow: 0,
                count: 0,
                sum: 0.0,
                updated_at: sample.timestamp,
            });

            match bounds.iter().position(|bound| sample.value <= *bound) {
                Some(index) => totals.buckets[index].1 += 1,
                None => totals.overflow += 1,
            }
            totals.count += 1;
            totals.sum += sample.value;
            totals.updated_at = totals.updated_at.max(sample.timestamp);
        }

        Ok(series.into_values().collect())
    }

    async fn list_metric_names(&self, metric_type: MetricType) -> SFResult<Vec<String>> {
        let store = match metric_type {
            MetricType::Gauge => &self.gauges,
            MetricType::Counter => &self.counters,
            MetricType::Histogram => &self.histograms,
        }
        .read()
        .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut names: Vec<String> = store.keys().cloned().collect();
        names.sort_unstable();
        Ok(names)
    }

    async fn health_check(&self) -> SFResult<()> {
        let _guard = self
            .gauges
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(())
    }
}
