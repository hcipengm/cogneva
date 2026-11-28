//! 变更账是 `change_judged` 的投影：删掉表，从流上折回每行最后量成的样子。
//! 默认忽略，需要测试库。

mod common;

use cog_test_support::{database_url, own_schema, schema_pool};
use serde_json::json;

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_change_account_rebuilds_from_its_judgements() {
    let url = database_url();
    let schema = own_schema(&url, "change_rebuild").await;
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
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS change_ledger (
            kind TEXT NOT NULL,
            scope TEXT NOT NULL,
            literal TEXT NOT NULL,
            state TEXT NOT NULL,
            first_seen_at TIMESTAMPTZ NOT NULL,
            measured_at TIMESTAMPTZ,
            measured_rev TEXT,
            retire_reason TEXT,
            unsettled_reason TEXT,
            parent_observation TEXT,
            PRIMARY KEY (kind, scope, literal)
        )",
    )
    .execute(&pool)
    .await
    .expect("the account table exists");

    let write = |i: i32, state: &'static str, at: &'static str| {
        let pool = pool.clone();
        async move {
            let attributes = json!({
                "kind": "source", "scope": "crates/x", "literal": "a rule",
                "state": state, "measured_rev": "rev-1",
                "retire_reason": null, "unsettled_reason": null,
                "parent_observation": null,
            })
            .to_string();
            sqlx::query(
                "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, \
                    step, outcome, attempt, idempotency_key, actor, writer, scope, payload_version, \
                    attributes, at) \
                 VALUES ($1, 'change_judged', 'change', $2, $2, 'judged', 'ok', 1, $3, \
                         'change-ledger', 'probe', 'change', 1, $4, $5::timestamptz)",
            )
            .bind(format!("J{i:0>25}"))
            .bind("source|crates/x|a rule")
            .bind(format!("judged-{i}"))
            .bind(attributes)
            .bind(at)
            .execute(&pool)
            .await
            .expect("the judgement is written");
        }
    };

    write(0, "pending", "2026-11-24T01:00:00Z").await;
    write(1, "landed", "2026-11-24T02:00:00Z").await;
    let written = cog_storage::rebuild_change_ledger(&pool)
        .await
        .expect("the stream rebuilds the account");
    assert_eq!(written, 1, "一个身份一行");
    let state: (String,) = sqlx::query_as(
        "SELECT state FROM change_ledger WHERE kind = 'source' AND scope = 'crates/x' AND literal = 'a rule'",
    )
    .fetch_one(&pool)
    .await
    .expect("the row reads back");
    assert_eq!(state.0, "landed", "取的是最后一条判词");
}
