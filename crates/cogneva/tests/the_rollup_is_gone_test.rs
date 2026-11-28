//! 同一笔调用，行与事件两处都落，两个数必须相等。
//!
//! 这是 S39 ②「读切换与停写汇总表同一笔」的**过渡态**判据：行还在被写，读者里已经
//! 有一半切到流上，两边同时活着。判三件：
//! - 一次 `record()` 加一次 `append()` 之后，行与事件各多一条（少一条就是有一侧停了）；
//! - 两条路对同一批调用给出的 token 合计相等（`spend_by_actor_since` 走流，
//!   `usage_by_actor` 走行）；
//! - 按人聚合等于逐笔求和。
//!
//! 停写那一侧要等 16 个读者全部切完才能落——只停写不切读者，配速与会话花费会一起
//! 读成 0，而没有任何一条现有测试会红（2026-11-22 复核发现的形状）。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

use cog_core::LlmCall;
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore};
use cog_storage::LlmCallEventStore;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

fn row(actor: &str, input: u64, output: u64) -> LlmUsageRecord {
    LlmUsageRecord {
        finish_reason: None,
        ttft_ms: None,
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
        prompt_manifest: None,
        prompt_dropped: None,
        cost_micros: None,
        price_book: None,
        refusal_kind: None,
        retry_after_secs: None,
        // 花了 token 的行必须点名任务（S12 的库约束）；这一条测试判的是两条路
        // 对同一批调用给出同一个数，不是归因，所以给个固定名字就够。
        task: Some("rollup-gone-task".to_string()),
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
        cache_created_tokens: None,
        origin: "gateway_pool".to_string(),
        owner: None,
        latency_ms: Some(1),
        task_id: None,
        actor: actor.to_string(),
        cost_micros: None,
        price_book: None,
        batch_id: None,
        session: None,
        pace: None,
        tokens_reasoning: None,
        refusal_kind: None,
        retry_after_secs: None,
        usage_outcome: None,
        estimated_input: None,
        refused_scope: None,
        finish_reason: None,
        ttft_ms: None,
        prompt_manifest: None,
        prompt_dropped: None,
        at: chrono::Utc::now(),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_row_and_the_event_carry_the_same_numbers() {
    let url = database_url();
    let schema = own_schema(&url, "rollup_gone").await;
    let pool = schema_pool(&url, &schema).await;
    cog_test_support::event_stream(&pool).await;

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

    // 一：两侧各多三条——少一条就是有一侧停了。
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM gateway_llm_usage")
        .fetch_one(&pool)
        .await
        .expect("count the rollup");
    assert_eq!(
        rows, 3,
        "三次调用应当落下三行：停写与读切换必须同一笔，只做一半就是两侧各说各的"
    );
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cogneva_event WHERE kind = 'llm_call'")
            .fetch_one(&pool)
            .await
            .expect("count the stream");
    assert_eq!(events, 3, "三次调用应当落下三条事件");

    // 二：两条路对同一批调用给出同一个合计；按人聚合等于逐笔求和。
    let since = chrono::Utc::now().timestamp() - 60;
    let (total, by_actor) = usage
        .spend_by_actor_since(since)
        .await
        .expect("read by actor (stream)");
    assert_eq!(total, 90, "三笔各 30 token，流上的合计不对");
    assert_eq!(by_actor.get("gone-a"), Some(&90));

    let row_tokens: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(tokens_input + tokens_output), 0)::BIGINT FROM gateway_llm_usage",
    )
    .fetch_one(&pool)
    .await
    .expect("sum the rows");
    assert_eq!(
        row_tokens.max(0) as u64,
        total,
        "行与流对同一批调用给出了不同的合计：行 {row_tokens}，流 {total}"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}
