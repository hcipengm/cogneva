//! 擦除记录读得回来：谁、何时、因为什么、哪一段。默认忽略，需要测试库。

mod common;

use cog_test_support::{database_url, own_schema, schema_pool};

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_redaction_reads_back_by_its_body_hash() {
    let url = database_url();
    let schema = own_schema(&url, "redaction_read").await;
    let pool = schema_pool(&url, &schema).await;
    common::apply_migration_files(
        &pool,
        &[
            "117_event_stream.up.sql",
            "119_event_payload_is_json.up.sql",
            "129_event_envelope_position.up.sql",
            "130_event_session_id.up.sql",
        ],
    )
    .await;

    let stream: std::sync::Arc<dyn cog_core::EventStore> = std::sync::Arc::new(
        cog_storage::postgres::event_log::EventLogStore::new(pool.clone(), "probe"),
    );
    let mut event = cog_core::FactEvent::new(cog_core::EventPayload::Redacted {
        artifact: None,
        oid: Some(7),
        sha256: "abc".into(),
        bytes: 10,
        reason: "owner asked".into(),
        actor: "owner".into(),
    });
    event.event_id = Some("R0000000000000000000000001".into());
    stream
        .append(event)
        .await
        .expect("the redaction is written");

    let found = cog_storage::redactions_for(&pool, "abc")
        .await
        .expect("the redaction reads back");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].actor, "owner");
    assert_eq!(found[0].body_oid, 7);
    assert_eq!(found[0].reason, "owner asked");
    assert!(cog_storage::redactions_for(&pool, "nope")
        .await
        .unwrap()
        .is_empty());
}
