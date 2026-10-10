use cog_core::{
    MemoryBackend, MetricsBackend, RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry,
};
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
async fn test_decay_demotes_aged_low_value_summary() {
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
    // 0.1 * 0.5 = 0.05, still above the archive floor, so the entry is demoted
    // in place, not removed.
    assert_eq!(report.entries_decayed, 1);
    assert_eq!(report.entries_archived, 0);

    let retrieved = backend
        .get_summary("default", "decay1")
        .await
        .unwrap()
        .unwrap();
    assert!(
        (retrieved.importance - 0.05).abs() < 1e-6,
        "importance should be halved, got {}",
        retrieved.importance
    );
    // The embedding is untouched: decay changes how the entry is valued, not the
    // vector it is stored with.
    assert_eq!(
        retrieved.embedding,
        vec![0.12345f32, 0.98765f32, 0.11111f32, 0.99999f32]
    );
}

#[tokio::test]
async fn test_decay_archives_summary_that_falls_to_the_floor() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let old_time = chrono::Utc::now() - chrono::Duration::hours(48);
    let mut entry = SummaryEntry::new(
        "decay-archive",
        "default",
        "Already near the floor",
        vec![0.5f32, 0.5f32],
        "test",
        make_source_ref("r1"),
    )
    .with_importance(0.03);
    entry.generated_at = old_time;
    backend.store_summary("default", &entry).await.unwrap();

    // 0.03 * 0.5 = 0.015, at or below the floor, so this pass archives it.
    let report = backend.decay("default", 3600, 0.5).await.unwrap();
    assert_eq!(report.entries_decayed, 0);
    assert_eq!(report.entries_archived, 1);
    assert!(
        backend
            .get_summary("default", "decay-archive")
            .await
            .unwrap()
            .is_none(),
        "an archived entry must leave the searchable summary layer"
    );
}

#[tokio::test]
async fn test_decay_leaves_fresh_and_high_value_summaries_alone() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    let now = chrono::Utc::now();
    // Fresh (well within the age threshold) though low value.
    let mut fresh = SummaryEntry::new(
        "fresh",
        "default",
        "Just stored",
        vec![1.0f32, 0.0f32],
        "test",
        make_source_ref("r1"),
    )
    .with_importance(0.01);
    fresh.generated_at = now;
    backend.store_summary("default", &fresh).await.unwrap();

    // Old but high value.
    let mut valuable = SummaryEntry::new(
        "valuable",
        "default",
        "Worth keeping",
        vec![0.0f32, 1.0f32],
        "test",
        make_source_ref("r2"),
    )
    .with_importance(0.9);
    valuable.generated_at = now - chrono::Duration::hours(48);
    backend.store_summary("default", &valuable).await.unwrap();

    let report = backend.decay("default", 3600, 0.5).await.unwrap();
    assert_eq!(report.entries_decayed, 0);
    assert_eq!(report.entries_archived, 0);
    assert!(backend
        .get_summary("default", "fresh")
        .await
        .unwrap()
        .is_some());
    assert!(backend
        .get_summary("default", "valuable")
        .await
        .unwrap()
        .is_some());
}

/// Drive the real maintenance loop rather than calling `decay` directly: the
/// defect this exists for is that the documented automatic decay had no caller,
/// so a test that called `decay` itself would pass against the broken state.
#[tokio::test]
async fn test_decay_maintenance_loop_drives_a_real_pass() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = Arc::new(CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    ));
    let mut entry = SummaryEntry::new(
        "loop-decay",
        "default",
        "aged low value",
        vec![0.2f32, 0.2f32],
        "test",
        make_source_ref("r1"),
    )
    .with_importance(0.4);
    entry.generated_at = chrono::Utc::now() - chrono::Duration::hours(48);
    backend.store_summary("default", &entry).await.unwrap();

    let metrics = Arc::new(cog_observability::metrics::PrometheusMetricsBackend::new(
        "",
    ));
    // The smallest interval the config allows is one second; the loop's first
    // sweep lands one period after start, so poll rather than race the timer.
    cog_memory::maintenance::spawn_decay_loop(
        backend.clone() as Arc<dyn MemoryBackend>,
        metrics.clone() as Arc<dyn MetricsBackend>,
        cog_memory::MaintenanceConfig {
            decay_interval_secs: 1,
            decay_age_threshold_secs: 3600,
            decay_importance_threshold: 0.5,
            decay_namespaces: vec!["default".into()],
        },
    );

    let mut saw_decayed = false;
    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let totals = metrics
            .query_counter_totals(cog_core::metric_names::MEMORY_DECAY_TOTAL.as_str())
            .await
            .unwrap();
        if totals
            .iter()
            .any(|s| s.labels.get("outcome").map(String::as_str) == Some("decayed"))
        {
            saw_decayed = true;
            break;
        }
    }
    assert!(saw_decayed, "the loop never recorded a decayed pass");

    let after = backend
        .get_summary("default", "loop-decay")
        .await
        .unwrap()
        .unwrap();
    assert!(
        (after.importance - 0.2).abs() < 1e-6,
        "the loop's pass must have demoted the entry, got {}",
        after.importance
    );
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

/// Dense-only embedder: every text maps to the same non-zero vector, which is
/// enough to distinguish "embedded" from the all-zero fallback.
struct StubEmbedder {
    dim: usize,
}

#[async_trait::async_trait]
impl cog_core::EmbeddingProvider for StubEmbedder {
    async fn embed(&self, texts: Vec<String>) -> cog_core::SFResult<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|_| vec![0.25f32; self.dim]).collect())
    }

    async fn embed_sparse(
        &self,
        texts: Vec<String>,
    ) -> cog_core::SFResult<Vec<cog_core::SparseEmbedding>> {
        Ok(texts
            .iter()
            .map(|_| cog_core::SparseEmbedding::new(vec![1], vec![1.0]))
            .collect())
    }

    fn dimension(&self) -> usize {
        self.dim
    }
}

/// The Raw layer reads its index from the object store rather than an
/// in-process map, so a fresh backend over the same store still lists what an
/// earlier one archived. A restart is modelled by building the backend twice.
#[tokio::test]
async fn test_list_raw_survives_backend_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));

    let first = CompositeMemoryBackend::new(
        object.clone(),
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        1024,
    );
    first.archive_raw(&make_raw("r1", "one")).await.unwrap();
    first.archive_raw(&make_raw("r2", "two")).await.unwrap();

    let second = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        1024,
    );
    let ids = second.list_raw("default", None).await.unwrap();
    assert_eq!(ids, vec!["r1".to_string(), "r2".to_string()]);
}

/// Archived raw sources stay self-describing: content type and tags come back
/// as stored instead of being guessed back as application/octet-stream.
#[tokio::test]
async fn test_raw_roundtrip_preserves_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        1024,
    );

    let source = make_raw("m1", "payload").with_tags(vec!["alpha".into(), "beta".into()]);
    backend.archive_raw(&source).await.unwrap();

    let back = backend.get_raw("default", "m1").await.unwrap().unwrap();
    assert_eq!(back.content_type, "conversation/transcript");
    assert_eq!(back.tags, vec!["alpha".to_string(), "beta".to_string()]);
    assert_eq!(String::from_utf8_lossy(&back.payload), "payload");
}

#[tokio::test]
async fn test_list_raw_filters_by_content_type() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        1024,
    );

    backend.archive_raw(&make_raw("a", "A")).await.unwrap();
    backend
        .archive_raw(&RawSource::new(
            "b",
            "default",
            "memory/explicit",
            b"B".to_vec(),
        ))
        .await
        .unwrap();

    let all = backend.list_raw("default", None).await.unwrap();
    assert_eq!(all.len(), 2);

    let only_conversation = backend
        .list_raw("default", Some("conversation/"))
        .await
        .unwrap();
    assert_eq!(only_conversation, vec!["a".to_string()]);
}

/// An explicit ingest embeds with the injected provider; a zero vector would
/// be unsearchable, which is the state this guards against.
#[tokio::test]
async fn test_ingest_explicit_embeds_with_provider() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        1024,
    )
    .with_embedder(Arc::new(StubEmbedder { dim: 1024 }));

    backend
        .ingest_explicit("default", "remember this", 0.9, Vec::new())
        .await
        .unwrap();

    let entries = backend.list_summary("default").await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].embedding.len(), 1024);
    assert!(
        entries[0].embedding.iter().any(|v| *v != 0.0),
        "explicit ingest must not store a zero vector when an embedder is available"
    );
}

/// Without an embedder there is no vector to store. The configured dimension
/// describes the shape a real embedder would produce, not a filler for the
/// entries that never had one: a run of that many zeros scores 0.0 against
/// every query, so a search would return arbitrary ties instead of nothing.
#[tokio::test]
async fn test_ingest_explicit_without_embedder_stores_no_vector() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(FileObjectBackend::new(tmp.path()));
    let backend = CompositeMemoryBackend::new(
        object,
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        1024,
    );

    backend
        .ingest_explicit("default", "no embedder here", 0.5, Vec::new())
        .await
        .unwrap();

    let entries = backend.list_summary("default").await.unwrap();
    assert!(entries[0].embedding.is_empty());
    assert_eq!(
        entries[0].embedding_model,
        cog_core::NO_EMBEDDING_MODEL,
        "an entry with no vector must not name an embedding model"
    );
}

/// An object store that counts its reads, so a test can assert how many the
/// code under test spent rather than only what it returned.
struct CountingObjectBackend {
    inner: FileObjectBackend,
    gets: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl cog_core::ObjectBackend for CountingObjectBackend {
    async fn put(&self, key: &str, data: &[u8]) -> cog_core::SFResult<String> {
        self.inner.put(key, data).await
    }

    async fn get(&self, key: &str) -> cog_core::SFResult<Option<Vec<u8>>> {
        self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(key).await
    }

    async fn delete(&self, key: &str) -> cog_core::SFResult<()> {
        self.inner.delete(key).await
    }

    async fn presign_url(&self, key: &str, expiry_secs: u64) -> cog_core::SFResult<String> {
        self.inner.presign_url(key, expiry_secs).await
    }

    async fn exists(&self, key: &str) -> cog_core::SFResult<bool> {
        self.inner.exists(key).await
    }

    async fn list(&self, prefix: Option<&str>) -> cog_core::SFResult<Vec<String>> {
        self.inner.list(prefix).await
    }
}

/// A listing that carries metadata pays one store read per item returned,
/// because the content type and length live inside each stored envelope. So the
/// `limit` has to bound the *reads*, not just the page: a listing that read the
/// whole namespace before trimming would spend a round trip per id to answer a
/// request for a handful, which on a busy namespace is the difference between a
/// fast response and one that times out. This pins the trim before the reads.
#[tokio::test]
async fn a_bounded_listing_reads_only_the_items_it_returns() {
    let tmp = tempfile::tempdir().unwrap();
    let object = Arc::new(CountingObjectBackend {
        inner: FileObjectBackend::new(tmp.path()),
        gets: std::sync::atomic::AtomicUsize::new(0),
    });
    let backend = CompositeMemoryBackend::new(
        object.clone(),
        Arc::new(cog_storage::MemoryVectorBackend::new()),
        128,
    );

    for id in ["raw-1", "raw-2", "raw-3", "raw-4", "raw-5"] {
        backend.archive_raw(&make_raw(id, "payload")).await.unwrap();
    }
    let before = object.gets.load(std::sync::atomic::Ordering::SeqCst);

    let listing = backend.list_raw_detailed("default", None, 2).await.unwrap();

    let reads = object.gets.load(std::sync::atomic::Ordering::SeqCst) - before;
    assert_eq!(
        listing.items.len(),
        2,
        "the page must hold at most `limit` items"
    );
    assert_eq!(
        listing.total, 5,
        "total must count the whole namespace, not the page"
    );
    assert_eq!(
        reads, 2,
        "only the returned items may be read back from the store"
    );
}
