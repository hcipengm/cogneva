//! 一次打包花的钱只有流答得出来：行上没有批号。
//!
//! 判两件：同批的调用事实加在一起；没有记录的批回答 `None`，不是 0——0 会说
//! 「这一批没花钱」，而它可能只是没落流。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

use cog_observability::usage_store::LlmUsageStore;
use sqlx::PgPool;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

async fn event_stream(pool: &PgPool) {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/postgres");
    for file in [
        "117_event_stream.up.sql",
        "119_event_payload_is_json.up.sql",
    ] {
        let sql = std::fs::read_to_string(dir.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("apply {file}: {e}"));
    }
}

async fn call(pool: &PgPool, id: &str, batch: &str, input: i64, output: i64) {
    sqlx::query(
        "INSERT INTO cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, step, \
            outcome, attempt, idempotency_key, actor, writer, scope, payload_version, attributes) \
         VALUES ($1, 'llm_call', 'llm_call', $1, $1, 'call', 'ok', 1, $1, 'probe', 'probe', \
                 'llm', 1, $2::text)",
    )
    .bind(id)
    .bind(
        serde_json::json!({ "batch_id": batch, "input_tokens": input, "output_tokens": output })
            .to_string(),
    )
    .execute(pool)
    .await
    .expect("the call event");
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_cost_of_the_batch_is_found_by_its_batch_id() {
    let url = database_url();
    let schema = own_schema(&url, "batch_cost").await;
    let pool = schema_pool(&url, &schema).await;
    event_stream(&pool).await;

    call(&pool, "B1", "batch-1", 10, 20).await;
    call(&pool, "B2", "batch-1", 5, 7).await;
    call(&pool, "B3", "batch-2", 100, 100).await;

    let store = LlmUsageStore::from_pool(pool.clone());
    let batch1 = store
        .tokens_by_batch("batch-1")
        .await
        .expect("read batch-1");
    assert_eq!(batch1, Some(42), "同批两笔没有加在一起");

    let missing = store
        .tokens_by_batch("batch-none")
        .await
        .expect("read a missing batch");
    assert_eq!(missing, None, "没有记录的批被读成了 0");

    pool.close().await;
    drop_schema(&url, &schema).await;
}
