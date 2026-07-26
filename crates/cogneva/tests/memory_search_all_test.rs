use cog_core::{
    MemoryBackend, SchemaEntry, SchemaKind, SourceRef, SummaryEntry, UnifiedSearchResult,
};
use cog_memory::*;
use cog_storage::FileObjectBackend;
use std::sync::Arc;

fn make_source_ref(raw_id: &str) -> SourceRef {
    SourceRef::new(format!("memory://{}", raw_id), "test/v1")
}

#[tokio::test]
async fn test_search_all_schema_only() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "schema-1",
                "default",
                SchemaKind::Entity,
                "PostgreSQL",
                "postgresql",
                make_source_ref("raw-1"),
            )
            .with_properties(serde_json::json!({"category": "database"})),
        )
        .await
        .unwrap();

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "schema-2",
                "default",
                SchemaKind::Entity,
                "Redis",
                "redis",
                make_source_ref("raw-2"),
            )
            .with_properties(serde_json::json!({"category": "cache"})),
        )
        .await
        .unwrap();

    let results = backend
        .search_all("default", "PostgreSQL", None, 10, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    match &results[0] {
        UnifiedSearchResult::Schema(s) => assert_eq!(s.entry.name, "PostgreSQL"),
        _ => panic!("Expected schema result"),
    }
}

#[tokio::test]
async fn test_search_all_schema_and_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "schema-1",
                "default",
                SchemaKind::Entity,
                "PostgreSQL",
                "postgresql",
                make_source_ref("raw-1"),
            ),
        )
        .await
        .unwrap();

    let mut emb = vec![0.0f32; 4];
    emb[0] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum-1",
                "default",
                "PostgreSQL performance tuning",
                emb.clone(),
                "test",
                make_source_ref("raw-1"),
            ),
        )
        .await
        .unwrap();

    let query_emb = vec![1.0f32, 0.0, 0.0, 0.0];
    let results = backend
        .search_all("default", "PostgreSQL", Some(&query_emb), 10, None)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    let schema_count = results
        .iter()
        .filter(|r| matches!(r, UnifiedSearchResult::Schema(_)))
        .count();
    let summary_count = results
        .iter()
        .filter(|r| matches!(r, UnifiedSearchResult::Summary(_)))
        .count();
    assert_eq!(schema_count, 1);
    assert_eq!(summary_count, 1);
}

#[tokio::test]
async fn test_search_all_time_range_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    let mut emb = vec![0.0f32; 4];
    emb[0] = 1.0;
    let mut old_entry = SummaryEntry::new(
        "sum-1",
        "default",
        "Old decision",
        emb.clone(),
        "test",
        make_source_ref("raw-1"),
    );
    old_entry.generated_at = chrono::Utc::now() - chrono::Duration::days(10);
    backend.store_summary("default", &old_entry).await.unwrap();

    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum-2",
                "default",
                "Recent decision",
                emb.clone(),
                "test",
                make_source_ref("raw-2"),
            ),
        )
        .await
        .unwrap();

    let start = chrono::Utc::now() - chrono::Duration::days(5);
    let end = chrono::Utc::now() + chrono::Duration::days(1);
    let results = backend
        .search_all("default", "decision", Some(&emb), 10, Some((start, end)))
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        UnifiedSearchResult::Summary(s) => assert_eq!(s.entry.id, "sum-2"),
        _ => panic!("Expected summary result"),
    }
}

#[tokio::test]
async fn test_search_all_empty_query() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let results = backend
        .search_all("default", "", None, 10, None)
        .await
        .unwrap();
    assert!(results.is_empty());
}
