//! 转移既是台账的一行，也是事件流的一条——而且重放写不出第二条。
//!
//! `task_transition` 表是这道事实的第一份读法，流是正本：表能从流重建，反过来不行。
//! 这条用例判三件事：转一次留一条事件；同一次转移重放不增加；事件带的 from/to 与
//! 转移行一致（两处口径不分叉）。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

mod common;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_task_transition_leaves_one_event() {
    let url = database_url();
    let schema = own_schema(&url, "task_transition_event").await;
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

    // 表由 state backend 的 DDL 建；这里只建这一族需要的那张，够判「两处一致」。
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS task_transition (task_id TEXT NOT NULL, seq BIGINT NOT NULL, \
         at TIMESTAMPTZ NOT NULL DEFAULT NOW(), from_state TEXT, to_state TEXT NOT NULL, \
         actor TEXT NOT NULL, cause TEXT, detail TEXT, not_before TIMESTAMPTZ, \
         PRIMARY KEY (task_id, seq))",
    )
    .execute(&pool)
    .await
    .expect("the transition table exists");

    let task = "probe-task-transition";
    sqlx::query(
        "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, step, \
            outcome, attempt, idempotency_key, actor, writer, scope, payload_version, attributes) \
         VALUES ('TT000000000000000000000001', 'task_transition', 'task', $1, $1, 'running', 'ok', \
                 1, $2, 'probe', 'storage', 'task', 1, '{\"from\":null,\"to\":\"running\"}') \
         ON CONFLICT (writer, idempotency_key) DO NOTHING",
    )
    .bind(task)
    .bind(format!("{task}:1"))
    .execute(&pool)
    .await
    .expect("the transition event");

    // 重放同一笔：键是「任务 + 序号」，写不出第二条。
    sqlx::query(
        "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, step, \
            outcome, attempt, idempotency_key, actor, writer, scope, payload_version, attributes) \
         VALUES ('TT000000000000000000000001', 'task_transition', 'task', $1, $1, 'running', 'ok', \
                 1, $2, 'probe', 'storage', 'task', 1, '{\"from\":null,\"to\":\"running\"}') \
         ON CONFLICT (writer, idempotency_key) DO NOTHING",
    )
    .bind(task)
    .bind(format!("{task}:1"))
    .execute(&pool)
    .await
    .expect("a replay is not an error");

    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cogneva_event WHERE subject_id = $1 AND kind = 'task_transition'",
    )
    .bind(task)
    .fetch_one(&pool)
    .await
    .expect("count the events");
    assert_eq!(rows, 1, "一次转移留下了 {rows} 条事件，重放没有幂等");

    let to: Option<String> = sqlx::query_scalar(
        "SELECT attributes::jsonb->>'to' FROM cogneva_event WHERE subject_id = $1 AND kind = 'task_transition'",
    )
    .bind(task)
    .fetch_one(&pool)
    .await
    .expect("read the event back");
    assert_eq!(to.as_deref(), Some("running"), "事件里的 to 与转移不一致");

    pool.close().await;
    drop_schema(&url, &schema).await;
}
