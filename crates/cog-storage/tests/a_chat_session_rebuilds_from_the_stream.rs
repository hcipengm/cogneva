//! 一段会话能从流重建回 `chat_turns`：轮的根是意图事件，回答挂在它的因果上。
//!
//! 这一条只判「分组与两行的内容」：附件与工具调用的正文要靠对象存储/`artifact://`
//! 取，本文件用两个说「取不到」的端口，于是它们不进这一轮的断言。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

mod common;

use async_trait::async_trait;
use cog_core::{ArchivedBodyReader, AttachmentBytes, ChatTurnStore, SFError, SFResult};
use cog_storage::PostgresChatTurnStore;
use cog_test_support::{database_url, own_schema, schema_pool};

struct NoAttachments;
#[async_trait]
impl AttachmentBytes for NoAttachments {
    async fn get(&self, key: &str) -> SFResult<Vec<u8>> {
        Err(SFError::Database(format!(
            "no object store in this test: {key}"
        )))
    }
}

struct NoArtifacts;
#[async_trait]
impl ArchivedBodyReader for NoArtifacts {
    async fn read(&self, uri: &str) -> SFResult<String> {
        Err(SFError::Database(format!(
            "no artifact store in this test: {uri}"
        )))
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_session_rebuilds_into_the_turn_table() {
    let url = database_url();
    let schema = own_schema(&url, "chat_rebuild").await;
    let pool = schema_pool(&url, &schema).await;
    common::apply_migration_files(
        &pool,
        &[
            "081_chat_turns.up.sql",
            "083_chat_turn_failure.up.sql",
            "089_chat_turn_goal.up.sql",
            "101_chat_turn_attachments.up.sql",
            "117_event_stream.up.sql",
            "119_event_payload_is_json.up.sql",
            "130_event_session_id.up.sql",
        ],
    )
    .await;

    let session = "s-rebuild";
    let identity = cog_core::chat_intent_identity(session, "arr-1");
    let intent_event = cog_core::intent_event_id(&identity);
    let intent_attributes = serde_json::json!({
        "source": "chat",
        "said_by": "alice",
        "addressed_to": "employee:7",
        "text": "把那一处改掉",
        "session": session,
    })
    .to_string();
    sqlx::query(
        "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
            step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
            session_id, attributes, at) \
         VALUES ($1, 'intent_received', 'intent', $2, $2, 'received', 'ok', 1, $2, 'alice', \
                 'probe', 'intent', 1, $3, $4, '2026-11-24T01:00:00Z')",
    )
    .bind(&intent_event)
    .bind(&identity)
    .bind(session)
    .bind(intent_attributes)
    .execute(&pool)
    .await
    .expect("the intent is written");

    let answer_attributes = serde_json::json!({
        "from": "employee:7",
        "to": "alice",
        "kind": "answer",
        "text": "改好了",
        "session": session,
    })
    .to_string();
    sqlx::query(
        "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
            step, outcome, attempt, idempotency_key, causation_id, actor, writer, scope, \
            payload_version, session_id, attributes, at) \
         VALUES ('A0000000000000000000000001', 'message_sent', 'message', 'employee:7', \
                 'alice', 'sent', 'ok', 1, 'a-1', $1, 'employee:7', 'probe', 'message', 1, $2, $3, \
                 '2026-11-24T01:00:05Z')",
    )
    .bind(&intent_event)
    .bind(session)
    .bind(answer_attributes)
    .execute(&pool)
    .await
    .expect("the answer is written");

    let written = cog_storage::rebuild_chat_turns(&pool, session, &NoAttachments, &NoArtifacts)
        .await
        .expect("the stream rebuilds the session");
    assert_eq!(written, 1, "一轮问答应当写回一对行");

    let store = PostgresChatTurnStore::new(pool.clone());
    let history = store
        .history(session, 10)
        .await
        .expect("the rows read back");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].content, "把那一处改掉");
    assert_eq!(history[1].content, "改好了");
}
