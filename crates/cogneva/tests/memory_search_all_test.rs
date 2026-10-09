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

/// `search_all` answers with two ranked groups — the schema hits lead and the
/// summary hits follow — and each group's order is total. Every candidate here
/// is a substring hit at the same importance, so the scores *and* the
/// similarities all tie and only the id can order them (descending, like every
/// other key in the ranking). The corpus is big enough that a group left in the
/// store's own order would have to land on the expected permutation by chance.
///
/// The two groups share the budget rather than getting one each: ten slots for
/// twelve hits means the schema group leads *and* consumes six of them.
#[tokio::test]
async fn test_search_all_orders_the_groups_and_breaks_ties_on_the_id() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    // Stored out of order, so that the order they come back in is the ranking's
    // doing and not the order they went in.
    for id in ["s3", "s1", "s6", "s4", "s2", "s5"] {
        backend
            .store_schema(
                "default",
                &SchemaEntry::new(
                    id,
                    "default",
                    SchemaKind::Entity,
                    "PostgreSQL cluster",
                    "postgres",
                    make_source_ref(id),
                )
                .with_importance(0.5),
            )
            .await
            .unwrap();
    }
    for id in ["m3", "m1", "m6", "m4", "m2", "m5"] {
        backend
            .store_summary(
                "default",
                &SummaryEntry::new(
                    id,
                    "default",
                    "PostgreSQL tuning",
                    vec![1.0, 0.0],
                    "test",
                    make_source_ref(id),
                )
                .with_importance(0.5),
            )
            .await
            .unwrap();
    }

    let ids = |results: Vec<UnifiedSearchResult>| {
        results
            .into_iter()
            .map(|r| match r {
                UnifiedSearchResult::Schema(s) => s.entry.id,
                UnifiedSearchResult::Summary(s) => s.entry.id,
            })
            .collect::<Vec<_>>()
    };

    let first = ids(backend
        .search_all("default", "PostgreSQL", None, 10, None)
        .await
        .unwrap());
    assert_eq!(
        first,
        vec!["s6", "s5", "s4", "s3", "s2", "s1", "m6", "m5", "m4", "m3"],
        "the schema group leads, a tie inside a group breaks on the id, and the two groups share the budget"
    );
    let second = ids(backend
        .search_all("default", "PostgreSQL", None, 10, None)
        .await
        .unwrap());
    assert_eq!(first, second, "a ranking must not vary between calls");
}
