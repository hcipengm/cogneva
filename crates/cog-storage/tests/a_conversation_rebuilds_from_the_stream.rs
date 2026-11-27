//! 一段会话能从流里读回来：属主那一句在 `intent_received` 上，助手的回答在
//! `message_sent(kind=answer)` 上，两条都带 `session`。会话表不参与。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

mod common;

use cog_test_support::{database_url, own_schema, schema_pool};

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_session_reads_back_from_its_facts() {
    let url = database_url();
    let schema = own_schema(&url, "dialogue_rebuild").await;
    let pool = schema_pool(&url, &schema).await;
    common::apply_migration_files(
        &pool,
        &[
            "117_event_stream.up.sql",
            "130_event_session_id.up.sql",
            "119_event_payload_is_json.up.sql",
        ],
    )
    .await;

    for (i, (kind, attributes, at)) in [
        (
            "intent_received",
            serde_json::json!({
                "source": "chat", "said_by": "alice", "addressed_to": "employee:7",
                "text": "把那一处改掉", "session": "s-1",
            }),
            "2026-11-24T01:00:00Z",
        ),
        (
            "message_sent",
            serde_json::json!({
                "from": "employee:7", "to": "alice", "kind": "answer",
                "text": "改好了", "session": "s-1",
            }),
            "2026-11-24T01:00:05Z",
        ),
        (
            "intent_received",
            serde_json::json!({
                "source": "chat", "said_by": "bob", "addressed_to": "employee:7",
                "text": "别人的一句", "session": "s-2",
            }),
            "2026-11-24T01:00:06Z",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query(
            "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
                step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
                session_id, attributes, at) \
             VALUES ($1, $2, 'chat', $3, $3, 'ok', 'ok', 1, $4, 'a', 'probe', 'chat', 1, $5, \
                     $6, $7::timestamptz)",
        )
        .bind(format!("C{i:0>25}"))
        .bind(kind)
        .bind(format!("s-1:{i}"))
        .bind(format!("s-1:{i}"))
        .bind("s-1")
        .bind(attributes.to_string())
        .bind(at)
        .execute(&pool)
        .await
        .expect("the fact is written");
    }

    let lines = cog_storage::dialogue_for_session(&pool, "s-1")
        .await
        .expect("the session reads back");
    assert_eq!(lines.len(), 2, "另一段会话的那一句不该混进来");
    assert_eq!(lines[0].said_by, "alice");
    assert_eq!(lines[0].text, "把那一处改掉");
    assert_eq!(lines[1].said_by, "employee:7");
    assert_eq!(lines[1].text, "改好了");
}
