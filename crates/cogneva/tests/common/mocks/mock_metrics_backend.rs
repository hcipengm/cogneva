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

    async fn health_check(&self) -> SFResult<()> {
        Ok(())
    }
}
