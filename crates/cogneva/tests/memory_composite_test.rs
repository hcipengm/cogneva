use cog_core::{MemoryBackend, RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry};
use cog_memory::*;
use cog_storage::FileObjectBackend;
use std::sync::Arc;

fn make_raw(id: &str, text: &str) -> RawSource {
    RawSource::new(
        id,
        "default",
        "conversation/transcript",
        text.as_bytes().to_vec(),
    )
}

fn make_source_ref(raw_id: &str) -> SourceRef {
    SourceRef::new(format!("memory://{}", raw_id), "test/v1")
}

#[tokio::test]
async fn test_composite_memory_raw_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let source = make_raw("raw-1", "Hello composite world");
    let uri = backend.archive_raw(&source).await.unwrap();
    assert!(uri.starts_with("file://"));

    let retrieved = backend.get_raw("default", "raw-1").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, "raw-1");
    assert_eq!(
        String::from_utf8_lossy(&retrieved.payload),
        "Hello composite world"
    );
}

#[tokio::test]
async fn test_composite_memory_schema_crud() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let entry = SchemaEntry::new(
        "schema-1",
        "default",
        SchemaKind::Entity,
        "PostgreSQL",
        "postgresql",
        make_source_ref("raw-1"),
    )
    .with_properties(serde_json::json!({"category": "database"}));

    backend.store_schema("default", &entry).await.unwrap();

    let retrieved = backend.get_schema("default", "schema-1").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "PostgreSQL");
}

#[tokio::test]
async fn test_composite_memory_summary_search() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    let mut emb_a = vec![0.0f32; 4];
    emb_a[0] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sa",
                "default",
                "Decision A",
                emb_a,
                "test",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();

    let mut emb_b = vec![0.0f32; 4];
    emb_b[1] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sb",
                "default",
                "Decision B",
                emb_b,
                "test",
                make_source_ref("r2"),
            ),
        )
        .await
        .unwrap();

    let query = vec![1.0f32, 0.0, 0.0, 0.0];
    let results = backend
        .search_summary("default", &query, 2, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].entry.id, "sa");
}

#[tokio::test]
async fn test_composite_memory_list_raw() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    backend.archive_raw(&make_raw("a", "A")).await.unwrap();
    backend.archive_raw(&make_raw("b", "B")).await.unwrap();

    let ids = backend.list_raw("default", None).await.unwrap();
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn test_composite_memory_schema_for_raw() {
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
                "s1",
                "default",
                SchemaKind::Entity,
                "X",
                "x",
                make_source_ref("raw-x"),
            ),
        )
        .await
        .unwrap();

    let for_x = backend.schema_for_raw("default", "raw-x").await.unwrap();
    assert_eq!(for_x.len(), 1);
    assert_eq!(for_x[0].name, "X");
}

#[tokio::test]
async fn test_composite_memory_summary_for_raw() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "s1",
                "default",
                "Summary X",
                vec![0.1; 4],
                "test",
                make_source_ref("raw-x"),
            ),
        )
        .await
        .unwrap();

    let for_x = backend.summary_for_raw("default", "raw-x").await.unwrap();
    assert_eq!(for_x.len(), 1);
    assert_eq!(for_x[0].text, "Summary X");
}

#[tokio::test]
async fn test_composite_memory_list_schema() {
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
                "s1",
                "default",
                SchemaKind::Entity,
                "Alpha",
                "alpha",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s2",
                "default",
                SchemaKind::Entity,
                "Beta",
                "beta",
                make_source_ref("r2"),
            ),
        )
        .await
        .unwrap();

    let all = backend.list_schema("default").await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn test_composite_memory_list_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum1",
                "default",
                "Text A",
                vec![0.1; 4],
                "test",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum2",
                "default",
                "Text B",
                vec![0.2; 4],
                "test",
                make_source_ref("r2"),
            ),
        )
        .await
        .unwrap();

    let all = backend.list_summary("default").await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn test_composite_memory_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    assert_eq!(backend.metrics().raw_archived, 0);

    backend.archive_raw(&make_raw("m1", "A")).await.unwrap();
    assert_eq!(backend.metrics().raw_archived, 1);

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s1",
                "default",
                SchemaKind::Entity,
                "A",
                "a",
                make_source_ref("m1"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(backend.metrics().schema_stored, 1);

    let mut emb = vec![0.0f32; 4];
    emb[0] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum1",
                "default",
                "text",
                emb,
                "test",
                make_source_ref("m1"),
            ),
        )
        .await
        .unwrap();

    let query = vec![1.0f32, 0.0, 0.0, 0.0];
    backend
        .search_summary("default", &query, 1, None)
        .await
        .unwrap();
    assert_eq!(backend.metrics().summary_searched, 1);
}

#[tokio::test]
async fn test_composite_memory_delete_raw() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    backend
        .archive_raw(&make_raw("del-1", "content"))
        .await
        .unwrap();
    let ids = backend.list_raw("default", None).await.unwrap();
    assert_eq!(ids.len(), 1);

    backend.delete_raw("default", "del-1").await.unwrap();
    let ids = backend.list_raw("default", None).await.unwrap();
    assert_eq!(ids.len(), 0);
    let raw = backend.get_raw("default", "del-1").await.unwrap();
    assert!(raw.is_none());
}

#[tokio::test]
async fn test_composite_memory_delete_schema() {
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
                "sd1",
                "default",
                SchemaKind::Entity,
                "X",
                "x",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(backend.list_schema("default").await.unwrap().len(), 1);

    backend.delete_schema("default", "sd1").await.unwrap();
    assert_eq!(backend.list_schema("default").await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_composite_memory_delete_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sumd1",
                "default",
                "Text",
                vec![0.1; 4],
                "test",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(backend.list_summary("default").await.unwrap().len(), 1);

    backend.delete_summary("default", "sumd1").await.unwrap();
    assert_eq!(backend.list_summary("default").await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_composite_memory_health_check() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    backend.health_check().await.unwrap();
}

#[tokio::test]
async fn test_composite_memory_update_schema_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let entry = SchemaEntry::new(
        "schema-1",
        "default",
        SchemaKind::Entity,
        "PostgreSQL",
        "postgresql",
        make_source_ref("raw-1"),
    )
    .with_properties(serde_json::json!({"category": "database", "version": "14"}));

    backend.store_schema("default", &entry).await.unwrap();

    let update = SchemaEntry::new(
        "schema-1",
        "default",
        SchemaKind::Entity,
        "PostgreSQL",
        "postgresql",
        make_source_ref("raw-2"),
    )
    .with_properties(serde_json::json!({"version": "15", "license": "PostgreSQL"}));

    backend.update_schema("default", &update).await.unwrap();

    let retrieved = backend.get_schema("default", "schema-1").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.properties["category"], "database");
    assert_eq!(retrieved.properties["version"], "15");
    assert_eq!(retrieved.properties["license"], "PostgreSQL");
    assert_eq!(retrieved.source_ref.raw_uri, "memory://raw-2");
}

#[tokio::test]
async fn test_composite_memory_update_summary_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend =
        CompositeMemoryBackend::new(object, Arc::new(cog_storage::MemoryVectorBackend::new()), 4);

    let mut emb = vec![0.0f32; 4];
    emb[0] = 1.0;
    let entry = SummaryEntry::new(
        "sum-1",
        "default",
        "Original text",
        emb.clone(),
        "test",
        make_source_ref("raw-1"),
    );
    backend.store_summary("default", &entry).await.unwrap();

    let mut emb2 = vec![0.0f32; 4];
    emb2[1] = 1.0;
    let update = SummaryEntry::new(
        "sum-1",
        "default",
        "Updated text",
        emb2.clone(),
        "test",
        make_source_ref("raw-2"),
    );
    backend.update_summary("default", &update).await.unwrap();

    let retrieved = backend.get_summary("default", "sum-1").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.text, "Updated text");
    assert_eq!(retrieved.embedding, emb2);
    assert_eq!(retrieved.source_ref.raw_uri, "memory://raw-2");

    // Verify re-indexed embedding is searchable
    let query = vec![0.0f32, 1.0, 0.0, 0.0];
    let results = backend
        .search_summary("default", &query, 1, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry.id, "sum-1");
}

#[tokio::test]
async fn test_composite_memory_persistence_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let mut backend = CompositeMemoryBackend::new(
        object.clone(),
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        4,
    );
    backend.set_persist_dir(tmp.path());

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s1",
                "default",
                SchemaKind::Entity,
                "Redis",
                "redis",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();

    let emb = vec![0.1f32; 4];
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "sum1",
                "default",
                "Summary",
                emb,
                "test",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();

    let mut backend2 = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );
    backend2.set_persist_dir(tmp.path());
    backend2.load().await.unwrap();

    let s = backend2.get_schema("default", "s1").await.unwrap();
    assert!(s.is_some());
    assert_eq!(s.unwrap().name, "Redis");

    let su = backend2.get_summary("default", "sum1").await.unwrap();
    assert!(su.is_some());
    assert_eq!(su.unwrap().text, "Summary");

    let results = backend2
        .search_summary("default", &[0.1f32; 4], 5, None)
        .await
        .unwrap();
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_explicit_ingest_and_forget() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    backend
        .ingest_explicit("default", "Explicit memory text", 0.8, vec!["tag1".into()])
        .await
        .unwrap();

    let all = backend.list_summary("default").await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].text, "Explicit memory text");
    assert_eq!(all[0].importance, 0.8);

    let raw_id = all[0].id.clone();
    backend.forget("default", &raw_id).await.unwrap();

    let after = backend.list_summary("default").await.unwrap();
    assert_eq!(after.len(), 0);
}

#[tokio::test]
async fn test_time_range_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let now = chrono::Utc::now();
    let old_time = now - chrono::Duration::hours(24);
    let recent_time = now - chrono::Duration::hours(1);

    let mut old_entry = SummaryEntry::new(
        "old",
        "default",
        "Old summary",
        vec![1.0f32, 0.0, 0.0, 0.0],
        "test",
        make_source_ref("r1"),
    );
    old_entry.generated_at = old_time;
    backend.store_summary("default", &old_entry).await.unwrap();

    let mut recent_entry = SummaryEntry::new(
        "recent",
        "default",
        "Recent summary",
        vec![0.0f32, 1.0, 0.0, 0.0],
        "test",
        make_source_ref("r2"),
    );
    recent_entry.generated_at = recent_time;
    backend
        .store_summary("default", &recent_entry)
        .await
        .unwrap();

    let query = vec![1.0f32, 0.0, 0.0, 0.0];

    let all_results = backend
        .search_summary("default", &query, 10, None)
        .await
        .unwrap();
    assert_eq!(all_results.len(), 2);

    let range = (
        now - chrono::Duration::hours(12),
        now + chrono::Duration::hours(1),
    );
    let filtered = backend
        .search_summary("default", &query, 10, Some(range))
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].entry.id, "recent");
}

#[tokio::test]
async fn test_decay_quantizes_embeddings() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let now = chrono::Utc::now();
    let old_time = now - chrono::Duration::hours(48);

    let mut entry = SummaryEntry::new(
        "decay1",
        "default",
        "Decay test",
        vec![0.12345f32, 0.98765f32, 0.11111f32, 0.99999f32],
        "test",
        make_source_ref("r1"),
    )
    .with_importance(0.1);
    entry.generated_at = old_time;
    backend.store_summary("default", &entry).await.unwrap();

    let report = backend.decay("default", 3600, 0.5).await.unwrap();
    assert_eq!(report.entries_decayed, 1);

    let retrieved = backend
        .get_summary("default", "decay1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.embedding, vec![0.12f32, 0.99f32, 0.11f32, 1.0f32]);
}

#[tokio::test]
async fn test_composite_memory_query_relations_outgoing() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    // Store entity
    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "e1",
                "default",
                SchemaKind::Entity,
                "Alice",
                "alice",
                make_source_ref("r1"),
            ),
        )
        .await
        .unwrap();

    // Store outgoing relation from Alice to Bob
    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "rel1",
                "default",
                SchemaKind::Relation,
                "Alice -> Bob",
                "alice_to_bob",
                make_source_ref("r1"),
            )
            .with_properties(
                serde_json::json!({"from": "Alice", "to": "Bob", "relation_type": "manages"}),
            ),
        )
        .await
        .unwrap();

    let results = backend
        .query_relations("default", "Alice", cog_core::RelationDirection::From, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "rel1");
}

#[tokio::test]
async fn test_composite_memory_query_relations_incoming() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    // Store outgoing relation from Alice to Bob
    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "rel1",
                "default",
                SchemaKind::Relation,
                "Alice -> Bob",
                "alice_to_bob",
                make_source_ref("r1"),
            )
            .with_properties(
                serde_json::json!({"from": "Alice", "to": "Bob", "relation_type": "manages"}),
            ),
        )
        .await
        .unwrap();

    // Query incoming for Bob
    let results = backend
        .query_relations("default", "Bob", cog_core::RelationDirection::To, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "rel1");
}

#[tokio::test]
async fn test_composite_memory_query_relations_filtered() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    // Store two relations from Alice
    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "rel1",
                "default",
                SchemaKind::Relation,
                "Alice -> Bob",
                "alice_to_bob",
                make_source_ref("r1"),
            )
            .with_properties(
                serde_json::json!({"from": "Alice", "to": "Bob", "relation_type": "manages"}),
            ),
        )
        .await
        .unwrap();

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "rel2",
                "default",
                SchemaKind::Relation,
                "Alice -> Carol",
                "alice_to_carol",
                make_source_ref("r1"),
            )
            .with_properties(
                serde_json::json!({"from": "Alice", "to": "Carol", "relation_type": "reports_to"}),
            ),
        )
        .await
        .unwrap();

    // Filter by relation_type "manages"
    let results = backend
        .query_relations(
            "default",
            "Alice",
            cog_core::RelationDirection::From,
            Some("manages"),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "rel1");

    // Filter by relation_type "reports_to"
    let results = backend
        .query_relations(
            "default",
            "Alice",
            cog_core::RelationDirection::From,
            Some("reports_to"),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "rel2");

    // No filter should return both
    let results = backend
        .query_relations("default", "Alice", cog_core::RelationDirection::From, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
}
