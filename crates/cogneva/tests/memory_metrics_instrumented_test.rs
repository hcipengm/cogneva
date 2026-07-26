use cog_core::{
    MemoryBackend, MetricsBackend, RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry,
};
use cog_memory::*;
use cog_storage::MemoryMetricsBackend;
use std::sync::Arc;

fn make_raw(id: &str, text: &str) -> RawSource {
    RawSource::new(id, "default", "text/plain", text.as_bytes().to_vec())
}

fn make_source_ref(raw_id: &str) -> SourceRef {
    SourceRef::new(format!("memory://{}", raw_id), "test/v1")
}

#[tokio::test]
async fn test_instrumented_backend_records_counters() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    backend.archive_raw(&make_raw("r1", "hello")).await.unwrap();
    backend.get_raw("default", "r1").await.unwrap();
    backend.list_raw("default", None).await.unwrap();

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);

    let counters = metrics
        .query_counter_range("memory_operations_total", start, end)
        .await
        .unwrap();
    assert_eq!(counters.len(), 3, "expected 3 operation counters");

    let archive_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"archive_raw".into()))
        .count();
    assert_eq!(archive_count, 1);

    let get_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"get_raw".into()))
        .count();
    assert_eq!(get_count, 1);

    let list_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"list_raw".into()))
        .count();
    assert_eq!(list_count, 1);
}

#[tokio::test]
async fn test_instrumented_backend_records_histograms() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    backend.archive_raw(&make_raw("r1", "hello")).await.unwrap();

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);

    let hists = metrics
        .query_histogram_range("memory_operation_latency_ms", start, end)
        .await
        .unwrap();
    assert!(!hists.is_empty(), "expected latency histograms");
    assert!(hists[0].value >= 0.0, "latency should be non-negative");
}

#[tokio::test]
async fn test_instrumented_backend_delegates_schema_ops() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    let entry = SchemaEntry::new(
        "s1",
        "default",
        SchemaKind::Entity,
        "Redis",
        "redis",
        make_source_ref("r1"),
    );

    backend.store_schema("default", &entry).await.unwrap();
    let found = backend.get_schema("default", "s1").await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "Redis");

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);
    let counters = metrics
        .query_counter_range("memory_operations_total", start, end)
        .await
        .unwrap();
    let schema_ops = counters
        .iter()
        .filter(|c| {
            matches!(
                c.labels.get("operation").map(|s| s.as_str()),
                Some("store_schema") | Some("get_schema")
            )
        })
        .count();
    assert_eq!(schema_ops, 2);
}

#[tokio::test]
async fn test_instrumented_backend_delegates_summary_ops() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    let emb = vec![0.1f32; 4];
    let entry = SummaryEntry::new(
        "sum1",
        "default",
        "text",
        emb,
        "test",
        make_source_ref("r1"),
    );

    backend.store_summary("default", &entry).await.unwrap();
    let found = backend.get_summary("default", "sum1").await.unwrap();
    assert!(found.is_some());

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);
    let counters = metrics
        .query_counter_range("memory_operations_total", start, end)
        .await
        .unwrap();
    let summary_ops = counters
        .iter()
        .filter(|c| {
            matches!(
                c.labels.get("operation").map(|s| s.as_str()),
                Some("store_summary") | Some("get_summary")
            )
        })
        .count();
    assert_eq!(summary_ops, 2);
}

#[tokio::test]
async fn test_instrumented_backend_passes_through_metrics() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner.clone(), metrics);

    backend.archive_raw(&make_raw("m1", "A")).await.unwrap();

    assert_eq!(backend.metrics().raw_archived, 1);
    assert_eq!(inner.metrics().raw_archived, 1);
}

#[tokio::test]
async fn test_instrumented_backend_records_search_ops() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s1",
                "default",
                SchemaKind::Entity,
                "X",
                "x",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    backend.search_schema("default", "X", 10).await.unwrap();

    let emb = vec![0.1f32; 4];
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum1",
                "default",
                "text",
                emb,
                "test",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    backend
        .search_summary("default", &[0.1f32; 4], 5, None)
        .await
        .unwrap();

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);
    let counters = metrics
        .query_counter_range("memory_operations_total", start, end)
        .await
        .unwrap();

    let search_schema_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"search_schema".into()))
        .count();
    let search_summary_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"search_summary".into()))
        .count();

    assert_eq!(search_schema_count, 1);
    assert_eq!(search_summary_count, 1);
}

#[tokio::test]
async fn test_instrumented_backend_records_list_ops() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s1",
                "default",
                SchemaKind::Entity,
                "X",
                "x",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    backend.list_schema("default").await.unwrap();

    let emb = vec![0.1f32; 4];
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum1",
                "default",
                "text",
                emb,
                "test",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    backend.list_summary("default").await.unwrap();

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);
    let counters = metrics
        .query_counter_range("memory_operations_total", start, end)
        .await
        .unwrap();

    let list_schema_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"list_schema".into()))
        .count();
    let list_summary_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"list_summary".into()))
        .count();

    assert_eq!(list_schema_count, 1);
    assert_eq!(list_summary_count, 1);
}

#[tokio::test]
async fn test_instrumented_backend_records_delete_ops() {
    let inner = Arc::new(MemoryMemoryBackend::new());
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let backend = MetricsInstrumentedMemoryBackend::new(inner, metrics.clone());

    backend.archive_raw(&make_raw("r1", "hello")).await.unwrap();
    backend.delete_raw("default", "r1").await.unwrap();

    let start = chrono::Utc::now() - chrono::Duration::seconds(5);
    let end = chrono::Utc::now() + chrono::Duration::seconds(5);
    let counters = metrics
        .query_counter_range("memory_operations_total", start, end)
        .await
        .unwrap();

    let delete_raw_count = counters
        .iter()
        .filter(|c| c.labels.get("operation") == Some(&"delete_raw".into()))
        .count();
    assert_eq!(delete_raw_count, 1);
}
