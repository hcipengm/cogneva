//! 汇总表不再被写：读的一切都从事件流来。
//!
//! 判三件（S20.1 / S21 / S32 的共同底层）：
//! - 调一次 `record()` 之后，`gateway_llm_usage` 里**没有新行**——那一侧停了；
//! - 同一批事实在流里，按人的聚合等于逐笔求和；
//! - 两条读路径（行/流）在同一窗口逐笔相等这件事，现在只剩流一条，所以「双写差值」
//!   恒为零——不是因为两边算得一样，是因为只剩一份。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

use cog_core::{LlmCall, LlmCallEvents};
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore};
use cog_storage::LlmCallEventStore;
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

fn row(actor: &str, input: u64, output: u64) -> LlmUsageRecord {
    LlmUsageRecord {
        refused_scope: None,
        upstream: "gone-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "gone-model".to_string(),
        result: "ok".to_string(),
        actor: actor.to_string(),
        origin: "gateway_pool".to_string(),
        tokens_input: input,
        tokens_output: output,
        tokens_cached: 0,
        tokens_cache_created: None,
        tokens_reasoning: None,
        estimated_input: None,
        cost_micros: None,
        price_book: None,
        refusal_kind: None,
        retry_after_secs: None,
        task: None,
        owner: None,
        session: None,
        latency_ms: 1,
        pace: None,
        usage_outcome: None,
    }
}

fn call(id: &str, actor: &str, input: u64, output: u64) -> LlmCall {
    LlmCall {
        call_id: id.to_string(),
        upstream: "gone-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "gone-model".to_string(),
        result: "ok".to_string(),
        input_tokens: input,
        output_tokens: output,
        cached_tokens: 0,
        origin: "gateway_pool".to_string(),
        owner: None,
        latency_ms: Some(1),
        task_id: None,
        actor: actor.to_string(),
        cost_micros: None,
        price_book: None,
        session: None,
        pace: None,
        at: chrono::Utc::now(),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_rollup_row_is_gone_and_the_stream_answers_the_same_numbers() {
    let url = database_url();
    let schema = own_schema(&url, "rollup_gone").await;
    let pool = schema_pool(&url, &schema).await;
    event_stream(&pool).await;

    let usage = LlmUsageStore::from_pool(pool.clone());
    usage
        .init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the usage tables exist");
    let events = LlmCallEventStore::new(pool.clone(), "rollup-gone-writer");

    for n in 0..3u64 {
        usage
            .record(&row("gone-a", 10, 20))
            .await
            .expect("record returns");
        events
            .append(&call(&format!("g{n}"), "gone-a", 10, 20))
            .await
            .expect("the event");
    }

    // 一：汇总表里没有新行——那一侧停了。
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM gateway_llm_usage")
        .fetch_one(&pool)
        .await
        .expect("count the rollup");
    assert_eq!(rows, 0, "汇总表还在被写：停写没做到");

    // 二：流答出同样的数——按人聚合等于逐笔求和。
    let since = chrono::Utc::now().timestamp() - 60;
    let (total, by_actor) = usage
        .spend_by_actor_since(since)
        .await
        .expect("read by actor");
    assert_eq!(total, 90, "三笔各 30 token，合计不对");
    assert_eq!(by_actor.get("gone-a"), Some(&90));

    // 三：双写差值恒为零——不是两边算得一样，是只剩一份。
    assert_eq!(rows, 0, "同一件事只剩流这一份");

    pool.close().await;
    drop_schema(&url, &schema).await;
}
