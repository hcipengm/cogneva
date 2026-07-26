use cog_core::{
    MemoryBackend, MemoryExtractor, RawSource, SchemaEntry, SchemaKind, SourceRef, SummaryEntry,
};
use cog_memory::*;

use chrono::{Duration, Utc};

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
async fn test_memory_backend_raw_crud() {
    let backend = MemoryMemoryBackend::new();

    let source = make_raw("raw-1", "Hello world");
    let uri = backend.archive_raw(&source).await.unwrap();
    assert_eq!(uri, "memory://raw-1");

    let retrieved = backend.get_raw("default", "raw-1").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id, "raw-1");
    assert_eq!(retrieved.content_type, "conversation/transcript");
    assert_eq!(String::from_utf8_lossy(&retrieved.payload), "Hello world");

    let missing = backend.get_raw("default", "nonexistent").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_memory_backend_list_raw() {
    let backend = MemoryMemoryBackend::new();

    backend.archive_raw(&make_raw("a-1", "A")).await.unwrap();
    backend
        .archive_raw(&RawSource::new(
            "b-1",
            "default",
            "document/markdown",
            vec![1, 2, 3],
        ))
        .await
        .unwrap();
    backend.archive_raw(&make_raw("a-2", "B")).await.unwrap();

    let all = backend.list_raw("default", None).await.unwrap();
    assert_eq!(all.len(), 3);

    let conv = backend
        .list_raw("default", Some("conversation/"))
        .await
        .unwrap();
    assert_eq!(conv.len(), 2);
}

#[tokio::test]
async fn test_memory_backend_schema_crud() {
    let backend = MemoryMemoryBackend::new();

    let entry = SchemaEntry::new(
        "schema-1",
        "default",
        SchemaKind::Entity,
        "Redis",
        "redis",
        make_source_ref("raw-1"),
    )
    .with_properties(serde_json::json!({"category": "database"}))
    .with_confidence(0.95);

    backend.store_schema("default", &entry).await.unwrap();

    let retrieved = backend.get_schema("default", "schema-1").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.name, "Redis");
    assert_eq!(retrieved.key, "redis");
    assert_eq!(retrieved.confidence, 0.95);
    assert_eq!(
        retrieved.properties,
        serde_json::json!({"category": "database"})
    );

    let missing = backend.get_schema("default", "nonexistent").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_memory_backend_schema_search() {
    let backend = MemoryMemoryBackend::new();

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s1",
                "default",
                SchemaKind::Entity,
                "PostgreSQL",
                "postgresql",
                make_source_ref("raw-1"),
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
                "Redis",
                "redis",
                make_source_ref("raw-2"),
            ),
        )
        .await
        .unwrap();

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s3",
                "default",
                SchemaKind::Relation,
                "depends_on",
                "depends_on",
                make_source_ref("raw-3"),
            ),
        )
        .await
        .unwrap();

    let results = backend.search_schema("default", "post", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry.name, "PostgreSQL");

    let results = backend.search_schema("default", "redis", 10).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry.name, "Redis");

    let limited = backend.search_schema("default", "", 2).await.unwrap();
    assert_eq!(limited.len(), 2);
}

#[tokio::test]
async fn test_memory_backend_schema_for_raw() {
    let backend = MemoryMemoryBackend::new();

    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s1",
                "default",
                SchemaKind::Entity,
                "A",
                "a",
                make_source_ref("raw-x"),
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
                "B",
                "b",
                make_source_ref("raw-x"),
            ),
        )
        .await
        .unwrap();
    backend
        .store_schema(
            "default",
            &SchemaEntry::new(
                "s3",
                "default",
                SchemaKind::Entity,
                "C",
                "c",
                make_source_ref("raw-y"),
            ),
        )
        .await
        .unwrap();

    let for_x = backend.schema_for_raw("default", "raw-x").await.unwrap();
    assert_eq!(for_x.len(), 2);

    let for_y = backend.schema_for_raw("default", "raw-y").await.unwrap();
    assert_eq!(for_y.len(), 1);

    let for_z = backend.schema_for_raw("default", "raw-z").await.unwrap();
    assert!(for_z.is_empty());
}

#[tokio::test]
async fn test_memory_backend_summary_crud() {
    let backend = MemoryMemoryBackend::new();

    let entry = SummaryEntry::new(
        "sum-1",
        "default",
        "Key decision: use Rust for the backend.",
        vec![0.1f32; 128],
        "test-model/v1",
        make_source_ref("raw-1"),
    )
    .with_confidence(0.88);

    backend.store_summary("default", &entry).await.unwrap();

    let retrieved = backend.get_summary("default", "sum-1").await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.text, "Key decision: use Rust for the backend.");
    assert_eq!(retrieved.confidence, 0.88);
    assert_eq!(retrieved.embedding.len(), 128);
}

#[tokio::test]
async fn test_memory_backend_summary_search() {
    let backend = MemoryMemoryBackend::new();

    let query = vec![1.0f32, 0.0f32, 0.0f32];

    let mut entry_a = vec![0.0f32; 128];
    entry_a[0] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new("sa", "default", "A", entry_a, "m", make_source_ref("r1"))
                .with_related_schema_ids(vec!["s1".into()]),
        )
        .await
        .unwrap();

    let mut entry_b = vec![0.0f32; 128];
    entry_b[1] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new("sb", "default", "B", entry_b, "m", make_source_ref("r2")),
        )
        .await
        .unwrap();

    let mut entry_c = vec![0.0f32; 128];
    entry_c[2] = 1.0;
    backend
        .store_summary(
            "default",
            &SummaryEntry::new("sc", "default", "C", entry_c, "m", make_source_ref("r3")),
        )
        .await
        .unwrap();

    let results = backend
        .search_summary("default", &query, 2, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].entry.id, "sa");
}

#[tokio::test]
async fn test_memory_backend_summary_for_raw() {
    let backend = MemoryMemoryBackend::new();

    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "s1",
                "default",
                "A",
                vec![0.0; 4],
                "m",
                make_source_ref("raw-x"),
            ),
        )
        .await
        .unwrap();
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "s2",
                "default",
                "B",
                vec![0.0; 4],
                "m",
                make_source_ref("raw-x"),
            ),
        )
        .await
        .unwrap();
    backend
        .store_summary(
            "default",
            &SummaryEntry::new(
                "s3",
                "default",
                "C",
                vec![0.0; 4],
                "m",
                make_source_ref("raw-y"),
            ),
        )
        .await
        .unwrap();

    let for_x = backend.summary_for_raw("default", "raw-x").await.unwrap();
    assert_eq!(for_x.len(), 2);
}

#[tokio::test]
async fn test_rule_based_extractor_entities() {
    let extractor = RuleBasedExtractor::new();
    let source = make_raw(
        "raw-1",
        "Meeting notes:\n@entity:Alice\n@entity:Bob\n@relation:Alice->Bob",
    );

    let schema = extractor.extract_schema(&source).await.unwrap();
    assert_eq!(schema.len(), 3);

    let entities: Vec<_> = schema
        .iter()
        .filter(|e| e.kind == SchemaKind::Entity)
        .collect();
    assert_eq!(entities.len(), 2);
    assert_eq!(entities[0].name, "Alice");
    assert_eq!(entities[1].name, "Bob");

    let relations: Vec<_> = schema
        .iter()
        .filter(|e| e.kind == SchemaKind::Relation)
        .collect();
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].name, "Alice -> Bob");
    assert_eq!(
        relations[0].properties,
        serde_json::json!({"from": "Alice", "to": "Bob"})
    );
}

#[tokio::test]
async fn test_rule_based_extractor_summary() {
    let extractor = RuleBasedExtractor::new();
    let source = make_raw("raw-1", "This is a long conversation about architecture.");

    let summary = extractor.generate_summary(&source).await.unwrap();
    assert_eq!(summary.id, "summary-raw-1");
    assert_eq!(
        summary.text,
        "This is a long conversation about architecture."
    );
    assert_eq!(summary.embedding.len(), 128);
    assert!(summary.embedding.iter().all(|v| *v == 0.0));
}

#[tokio::test]
async fn test_ingestion_pipeline() {
    let extractor = RuleBasedExtractor::new();
    let pipeline = IngestionPipeline::new(extractor);

    let source = make_raw("raw-1", "Project kickoff:\n@entity:Cogneva\n@entity:Rust");

    let (schema, summary) = pipeline.ingest(&source).await.unwrap();
    assert_eq!(schema.len(), 2);
    assert_eq!(summary.id, "summary-raw-1");
    assert_eq!(summary.source_ref.raw_uri, "memory://raw-1");
}

#[test]
fn test_memory_consolidator_keep_newer() {
    let a = SchemaEntry::new(
        "s1",
        "default",
        SchemaKind::Entity,
        "A",
        "a",
        make_source_ref("raw-1"),
    );
    let mut b = a.clone();
    b.extracted_at = Utc::now() + Duration::seconds(10);

    let consolidator = MemoryConsolidator::new(ConsolidationStrategy::KeepNewer);
    let result = consolidator.merge_schema(&a, &b).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().extracted_at, b.extracted_at);
}

#[test]
fn test_memory_consolidator_keep_confidence() {
    let a = SchemaEntry::new(
        "s1",
        "default",
        SchemaKind::Entity,
        "A",
        "a",
        make_source_ref("raw-1"),
    )
    .with_confidence(0.5);
    let b = SchemaEntry::new(
        "s2",
        "default",
        SchemaKind::Entity,
        "A",
        "a",
        make_source_ref("raw-2"),
    )
    .with_confidence(0.9);

    let consolidator = MemoryConsolidator::new(ConsolidationStrategy::KeepHigherConfidence);
    let result = consolidator.merge_schema(&a, &b).unwrap();
    assert_eq!(result.unwrap().confidence, 0.9);
}

#[test]
fn test_memory_consolidator_preserve_both() {
    let a = SchemaEntry::new(
        "s1",
        "default",
        SchemaKind::Entity,
        "A",
        "a",
        make_source_ref("raw-1"),
    );
    let b = SchemaEntry::new(
        "s2",
        "default",
        SchemaKind::Entity,
        "A",
        "a",
        make_source_ref("raw-2"),
    );

    let consolidator = MemoryConsolidator::new(ConsolidationStrategy::PreserveBoth);
    let result = consolidator.merge_schema(&a, &b).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_memory_consolidator_deduplicate() {
    let entries = vec![
        SchemaEntry::new(
            "s1",
            "default",
            SchemaKind::Entity,
            "Redis",
            "redis",
            make_source_ref("r1"),
        )
        .with_confidence(0.5),
        SchemaEntry::new(
            "s2",
            "default",
            SchemaKind::Entity,
            "Redis",
            "redis",
            make_source_ref("r2"),
        )
        .with_confidence(0.9),
        SchemaEntry::new(
            "s3",
            "default",
            SchemaKind::Entity,
            "Postgres",
            "postgres",
            make_source_ref("r3"),
        ),
    ];

    let consolidator = MemoryConsolidator::new(ConsolidationStrategy::KeepHigherConfidence);
    let deduped = consolidator.deduplicate_schema(entries).unwrap();
    assert_eq!(deduped.len(), 2);

    let redis = deduped.iter().find(|e| e.key == "redis").unwrap();
    assert_eq!(redis.confidence, 0.9);
}

#[test]
fn test_source_ref_builder() {
    let sr = SourceRef::new("uri", "v1").with_range("0-100");
    assert_eq!(sr.raw_uri, "uri");
    assert_eq!(sr.extractor_version, "v1");
    assert_eq!(sr.range, Some("0-100".into()));
}

#[test]
fn test_raw_source_builder() {
    let now = chrono::Utc::now();
    let rs = RawSource::new("id", "default", "type", vec![1, 2, 3])
        .with_tags(vec!["a".into(), "b".into()])
        .with_created_at(now);
    assert_eq!(rs.tags, vec!["a", "b"]);
    assert_eq!(rs.created_at, now);
}

#[test]
fn test_schema_entry_builder() {
    let se = SchemaEntry::new(
        "id",
        "default",
        SchemaKind::Entity,
        "Name",
        "name",
        make_source_ref("raw"),
    )
    .with_confidence(0.75)
    .with_properties(serde_json::json!({"x": 1}));
    assert_eq!(se.confidence, 0.75);
    assert_eq!(se.properties, serde_json::json!({"x": 1}));
}

#[test]
fn test_summary_entry_builder() {
    let se = SummaryEntry::new(
        "id",
        "default",
        "text",
        vec![0.1; 4],
        "model",
        make_source_ref("raw"),
    )
    .with_confidence(0.6)
    .with_related_schema_ids(vec!["s1".into()]);
    assert_eq!(se.confidence, 0.6);
    assert_eq!(se.related_schema_ids, vec!["s1"]);
}
