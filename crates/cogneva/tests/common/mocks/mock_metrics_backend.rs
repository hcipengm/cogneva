use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::{MetricSample, MetricsBackend, SFResult};
use std::collections::HashMap;
use std::sync::Mutex;

/// A mock [`MetricsBackend`] for testing that records all metric calls in a Vec.
#[derive(Debug)]
pub struct MockMetricsBackend {
    records: Mutex<Vec<MetricRecord>>,
}

#[derive(Debug, Clone)]
pub struct MetricRecord {
    pub kind: MetricKind,
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Gauge,
    Counter,
    Histogram,
}

#[allow(dead_code)]
impl MockMetricsBackend {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    /// Return all recorded metric calls.
    pub fn recorded_calls(&self) -> Vec<MetricRecord> {
        self.records.lock().unwrap().clone()
    }

    /// Return calls filtered by metric name.
    pub fn calls_for(&self, name: &str) -> Vec<MetricRecord> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.name == name)
            .cloned()
            .collect()
    }

    /// Return the total number of recorded calls.
    pub fn call_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    fn push(&self, kind: MetricKind, name: &str, value: f64, labels: HashMap<String, String>) {
        self.records.lock().unwrap().push(MetricRecord {
            kind,
            name: name.into(),
            value,
            labels,
            timestamp: Utc::now(),
        });
    }
}

impl Default for MockMetricsBackend {
    fn default() -> Self {
        Self::new()
    }
}

/// Group key for a label set, derived from label names so that two samples
/// carrying the same labels never land in different groups just because their
/// maps happen to iterate differently.
fn label_key(labels: &HashMap<String, String>) -> String {
    let mut pairs: Vec<(&String, &String)> = labels.iter().collect();
    pairs.sort_unstable();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[async_trait]
impl MetricsBackend for MockMetricsBackend {
    async fn record_gauge(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.push(MetricKind::Gauge, name, value, labels);
        Ok(())
    }

    async fn record_counter(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.push(MetricKind::Counter, name, value, labels);
        Ok(())
    }

    async fn record_histogram(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.push(MetricKind::Histogram, name, value, labels);
        Ok(())
    }

    async fn query_gauge_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let records = self.records.lock().unwrap();
        let samples: Vec<MetricSample> = records
            .iter()
            .filter(|r| {
                r.kind == MetricKind::Gauge
                    && r.name == name
                    && r.timestamp >= start
                    && r.timestamp <= end
            })
            .map(|r| MetricSample {
                timestamp: r.timestamp,
                value: r.value,
                labels: r.labels.clone(),
            })
            .collect();
        Ok(samples)
    }

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        let records = self.records.lock().unwrap();
        let mut latest: HashMap<String, MetricSample> = HashMap::new();
        for r in records
            .iter()
            .filter(|r| r.kind == MetricKind::Gauge && r.name == name)
        {
            let key = label_key(&r.labels);
            match latest.get(&key) {
                Some(existing) if existing.timestamp >= r.timestamp => {}
                _ => {
                    latest.insert(
                        key,
                        MetricSample {
                            timestamp: r.timestamp,
                            value: r.value,
                            labels: r.labels.clone(),
                        },
                    );
                }
            }
        }
        Ok(latest.into_values().collect())
    }

    async fn query_counter_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let records = self.records.lock().unwrap();
        let samples: Vec<MetricSample> = records
            .iter()
            .filter(|r| {
                r.kind == MetricKind::Counter
                    && r.name == name
                    && r.timestamp >= start
                    && r.timestamp <= end
            })
            .map(|r| MetricSample {
                timestamp: r.timestamp,
                value: r.value,
                labels: r.labels.clone(),
            })
            .collect();
        Ok(samples)
    }

    async fn query_counter_totals(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        let records = self.records.lock().unwrap();
        let mut totals: std::collections::HashMap<String, MetricSample> =
            std::collections::HashMap::new();
        for r in records
            .iter()
            .filter(|r| r.kind == MetricKind::Counter && r.name == name)
        {
            let key = label_key(&r.labels);
            match totals.get_mut(&key) {
                Some(existing) => existing.value += r.value,
                None => {
                    totals.insert(
                        key,
                        MetricSample {
                            timestamp: r.timestamp,
                            value: r.value,
                            labels: r.labels.clone(),
                        },
                    );
                }
            }
        }
        Ok(totals.into_values().collect())
    }

    async fn query_histogram_totals(&self, name: &str) -> SFResult<Vec<cog_core::HistogramTotals>> {
        let bounds = cog_core::histogram_bucket_bounds(name);
        let records = self.records.lock().unwrap();
        let mut series: HashMap<String, cog_core::HistogramTotals> = HashMap::new();
        for r in records
            .iter()
            .filter(|r| r.kind == MetricKind::Histogram && r.name == name)
        {
            let key = label_key(&r.labels);
            let totals = series
                .entry(key)
                .or_insert_with(|| cog_core::HistogramTotals {
                    labels: r.labels.clone(),
                    buckets: bounds.iter().map(|b| (*b, 0)).collect(),
                    overflow: 0,
                    count: 0,
                    sum: 0.0,
                });
            match bounds.iter().position(|bound| r.value <= *bound) {
                Some(index) => totals.buckets[index].1 += 1,
                None => totals.overflow += 1,
            }
            totals.count += 1;
            totals.sum += r.value;
        }
        Ok(series.into_values().collect())
    }

    async fn query_histogram_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        let records = self.records.lock().unwrap();
        let samples: Vec<MetricSample> = records
            .iter()
            .filter(|r| {
                r.kind == MetricKind::Histogram
                    && r.name == name
                    && r.timestamp >= start
                    && r.timestamp <= end
            })
            .map(|r| MetricSample {
                timestamp: r.timestamp,
                value: r.value,
                labels: r.labels.clone(),
            })
            .collect();
        Ok(samples)
    }

    async fn list_metric_names(&self, metric_type: cog_core::MetricType) -> SFResult<Vec<String>> {
        let kind = match metric_type {
            cog_core::MetricType::Gauge => MetricKind::Gauge,
            cog_core::MetricType::Counter => MetricKind::Counter,
            cog_core::MetricType::Histogram => MetricKind::Histogram,
        };
        let records = self.records.lock().unwrap();
        let mut names: Vec<String> = records
            .iter()
            .filter(|r| r.kind == kind)
            .map(|r| r.name.clone())
            .collect();
        names.sort_unstable();
        names.dedup();
        Ok(names)
    }

    async fn health_check(&self) -> SFResult<()> {
        Ok(())
    }
}
