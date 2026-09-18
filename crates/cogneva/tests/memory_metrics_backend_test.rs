use chrono::{Duration, Utc};
use cog_core::MetricsBackend;
use cog_storage::MemoryMetricsBackend;
use std::collections::HashMap;

fn labels() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("agent_id".into(), "a-1".into());
    m
}

#[tokio::test]
async fn test_memory_record_and_query_gauge() {
    let backend = MemoryMetricsBackend::new();
    let before = Utc::now() - Duration::seconds(1);

    backend
        .record_gauge("cpu_usage", 42.0, labels())
        .await
        .unwrap();

    let after = Utc::now() + Duration::seconds(1);
    let samples = backend
        .query_gauge_range("cpu_usage", before, after)
        .await
        .unwrap();

    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].value, 42.0);
    assert_eq!(samples[0].labels.get("agent_id"), Some(&"a-1".into()));
}

#[tokio::test]
async fn test_memory_record_and_query_counter() {
    let backend = MemoryMetricsBackend::new();
    let before = Utc::now() - Duration::seconds(1);

    backend
        .record_counter("requests", 1.0, labels())
        .await
        .unwrap();
    backend
        .record_counter("requests", 2.0, labels())
        .await
        .unwrap();

    let after = Utc::now() + Duration::seconds(1);
    let samples = backend
        .query_counter_range("requests", before, after)
        .await
        .unwrap();

    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].value, 1.0);
    assert_eq!(samples[1].value, 2.0);
}

/// A counter's total is cumulative over everything recorded, not over a
/// window: a series declared `counter` must never decrease, otherwise
/// `rate()` over successive scrapes reports nonsense.
#[tokio::test]
async fn counter_total_is_cumulative_per_label_set() {
    let backend = MemoryMetricsBackend::new();
    let mut other = HashMap::new();
    other.insert("agent_id".to_string(), "a-2".to_string());

    backend
        .record_counter("requests", 1.0, labels())
        .await
        .unwrap();
    backend
        .record_counter("requests", 2.0, labels())
        .await
        .unwrap();
    backend
        .record_counter("requests", 5.0, other)
        .await
        .unwrap();

    let totals = backend.query_counter_totals("requests").await.unwrap();
    assert_eq!(totals.len(), 2, "one total per label set: {totals:?}");

    let a1 = totals
        .iter()
        .find(|s| s.labels.get("agent_id").map(String::as_str) == Some("a-1"))
        .expect("a-1 total");
    let a2 = totals
        .iter()
        .find(|s| s.labels.get("agent_id").map(String::as_str) == Some("a-2"))
        .expect("a-2 total");
    assert_eq!(a1.value, 3.0);
    assert_eq!(a2.value, 5.0);
}

#[tokio::test]
async fn test_memory_record_and_query_histogram() {
    let backend = MemoryMetricsBackend::new();
    let before = Utc::now() - Duration::seconds(1);

    backend
        .record_histogram("latency_ms", 150.0, labels())
        .await
        .unwrap();
    backend
        .record_histogram("latency_ms", 200.0, labels())
        .await
        .unwrap();

    let after = Utc::now() + Duration::seconds(1);
    let samples = backend
        .query_histogram_range("latency_ms", before, after)
        .await
        .unwrap();

    assert_eq!(samples.len(), 2);
}

#[tokio::test]
async fn test_memory_query_range_filters_by_time() {
    let backend = MemoryMetricsBackend::new();

    let old = Utc::now() - Duration::hours(1);
    let recent = Utc::now();

    backend.record_gauge("temp", 100.0, labels()).await.unwrap();

    // Query old range should return nothing
    let old_samples = backend
        .query_gauge_range("temp", old - Duration::seconds(10), old)
        .await
        .unwrap();
    assert!(old_samples.is_empty());

    // Query recent range should return the sample
    let recent_samples = backend
        .query_gauge_range(
            "temp",
            recent - Duration::seconds(10),
            recent + Duration::seconds(10),
        )
        .await
        .unwrap();
    assert_eq!(recent_samples.len(), 1);
}

#[tokio::test]
async fn test_memory_query_missing_metric() {
    let backend = MemoryMetricsBackend::new();
    let now = Utc::now();

    let samples = backend
        .query_gauge_range("nonexistent", now - Duration::seconds(10), now)
        .await
        .unwrap();
    assert!(samples.is_empty());
}

#[tokio::test]
async fn test_memory_multiple_metrics_isolated() {
    let backend = MemoryMetricsBackend::new();
    let before = Utc::now() - Duration::seconds(1);

    backend.record_gauge("cpu", 10.0, labels()).await.unwrap();
    backend
        .record_gauge("memory", 50.0, labels())
        .await
        .unwrap();

    let after = Utc::now() + Duration::seconds(1);
    let cpu = backend
        .query_gauge_range("cpu", before, after)
        .await
        .unwrap();
    let memory = backend
        .query_gauge_range("memory", before, after)
        .await
        .unwrap();

    assert_eq!(cpu.len(), 1);
    assert_eq!(cpu[0].value, 10.0);
    assert_eq!(memory.len(), 1);
    assert_eq!(memory[0].value, 50.0);
}
