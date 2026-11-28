//! 一次打包花的钱只有流答得出来：行上没有批号。
//!
//! 判两件：同批的调用事实加在一起；没有记录的批回答 `None`，不是 0——0 会说
//! 「这一批没花钱」，而它可能只是没落流。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

use cog_core::LlmCall;
use cog_observability::usage_store::LlmUsageStore;
use cog_storage::LlmCallEventStore;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

async fn call(events: &LlmCallEventStore, id: &str, batch: &str, input: u64, output: u64) {
    // 走**写入口**而不是手写一条 INSERT：批号要落在信封那一列上，手写语句最容易
    // 落进 `attributes`，而读的人按列取——两边各读一半，恒答「这一批没有记录」。
    events
        .append(&LlmCall {
            call_id: id.to_string(),
            upstream: "batch-upstream".to_string(),
            api_style: "openai".to_string(),
            model: "batch-model".to_string(),
            result: "ok".to_string(),
            input_tokens: input,
            output_tokens: output,
            cached_tokens: 0,
            cache_created_tokens: None,
            origin: "gateway_pool".to_string(),
            owner: None,
            latency_ms: Some(1),
            task_id: None,
            actor: "probe".to_string(),
            cost_micros: None,
            price_book: None,
            session: None,
            pace: None,
            batch_id: Some(batch.to_string()),
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
        })
        .await
        .expect("the call event");
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_cost_of_the_batch_is_found_by_its_batch_id() {
    let url = database_url();
    let schema = own_schema(&url, "batch_cost").await;
    let pool = schema_pool(&url, &schema).await;
    cog_test_support::event_stream(&pool).await;

    let events = LlmCallEventStore::new(pool.clone(), "batch-cost-writer");
    call(&events, "B1", "batch-1", 10, 20).await;
    call(&events, "B2", "batch-1", 5, 7).await;
    call(&events, "B3", "batch-2", 100, 100).await;

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
