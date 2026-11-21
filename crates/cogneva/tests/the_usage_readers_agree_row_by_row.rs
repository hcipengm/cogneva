//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//!
//! 用量有两条读数：**行**（`gateway_llm_usage`，今天的正本）与**流**（`cogneva_event`
//! 里 `kind='llm_call'`，明天唯一的正本）。同一段时间上它们必须逐行相等——差一行就说
//! 明两边口径已经分叉，而那时停掉写行的一侧，等于把一个已经错开的数变成唯一答案。
//!
//! 这一条必须让**真写入口**写事件：`LlmCallEventStore`（`cog-storage`）与
//! `LlmUsageStore`（`cog-observability`）分住两个 crate，只有这里两边都够得着。手搓
//! SQL 插事件只能证明读者认得手搓的形状，那时它验的是夹具而不是写入口。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`（一次性库）：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cogneva --test the_usage_readers_agree_row_by_row -- --ignored
//! ```

use chrono::Utc;
use cog_core::{LlmCall, LlmCallEvents};
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore};
use cog_storage::LlmCallEventStore;
use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

/// 事件流那张表属于迁移，不属于任何一个 store：跑真身的迁移，而不是自己写一份形状。
async fn apply_event_stream_migration(pool: &sqlx::PgPool) {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../cog-storage/migrations/postgres");
    for file in [
        "117_event_stream.up.sql",
        "119_event_payload_is_json.up.sql",
        // 流上的读者按生成列取 token 与花费；缺这一条，列名根本不存在。
        "124_event_payload_query_columns.up.sql",
    ] {
        let sql = std::fs::read_to_string(dir.join(file))
            .unwrap_or_else(|e| panic!("read the migration {file}: {e}"));
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("apply the migration {file}: {e}"));
    }
}

fn usage_row(actor: &str, input: u64, output: u64, marker: &str) -> LlmUsageRecord {
    LlmUsageRecord {
        refused_scope: None,
        upstream: "agreement-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "agreement-model".to_string(),
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
        task: Some(marker.to_string()),
        owner: None,
        session: None,
        latency_ms: 12,
        pace: None,
        usage_outcome: None,
    }
}

fn call(call_id: &str, actor: &str, input: u64, output: u64) -> LlmCall {
    LlmCall {
        call_id: call_id.to_string(),
        upstream: "agreement-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "agreement-model".to_string(),
        result: "ok".to_string(),
        input_tokens: input,
        output_tokens: output,
        cached_tokens: 0,
        origin: "gateway_pool".to_string(),
        owner: None,
        latency_ms: Some(12),
        task_id: None,
        actor: actor.to_string(),
        cost_micros: None,
        price_book: None,
        batch_id: None,
        session: None,
        pace: None,
        at: Utc::now(),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_stream_reader_and_the_row_reader_agree_row_by_row() {
    let url = database_url();
    let schema = own_schema(&url, "usage_readers_agree").await;
    let pool = schema_pool(&url, &schema).await;

    apply_event_stream_migration(&pool).await;
    // 借这一只已经带着 search_path 的池：另开一只池会连到 public，于是写进去的行
    // 与读它们的那几格不在同一个 schema 上——两边都「成功」，比的是两份不同的库。
    let usage = LlmUsageStore::from_pool(pool.clone());
    usage
        .init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the usage tables exist");

    let since = Utc::now().timestamp() - 60;
    let marker = format!("usage-readers-agree:{}", uuid::Uuid::new_v4());
    let events = LlmCallEventStore::new(pool.clone(), format!("{marker}-writer"));

    for (n, (actor, input, output)) in [("eq-a", 10u64, 20u64), ("eq-b", 5, 7), ("eq-a", 1, 2)]
        .into_iter()
        .enumerate()
    {
        usage
            .record(&usage_row(actor, input, output, &marker))
            .await
            .expect("the row");
        // 事件 id 取调用身份的前 26 个字母数字：三笔的调用身份要在**前 26 个字符里**
        // 就分得开，否则它们在主键上撞成同一条事件（第一笔赢，后两笔报重复键）。
        events
            .append(&call(&format!("c{n}{marker}"), actor, input, output))
            .await
            .expect("the event");
    }

    // 行这一侧直接由 SQL 求和——`spend_by_actor_since` 自己已经切到流上了，
    // 拿它当「行的读数」是同一个查询和自己比，永远相等、也永远判不出东西。
    let row_rows = sqlx::query(
        "SELECT actor, SUM(tokens_input + tokens_output)::BIGINT AS tokens \
           FROM gateway_llm_usage GROUP BY actor",
    )
    .fetch_all(&pool)
    .await
    .expect("read the rows");
    let mut row_by_actor: std::collections::BTreeMap<String, u64> = Default::default();
    for r in &row_rows {
        let actor: String = sqlx::Row::get(r, "actor");
        let tokens: i64 = sqlx::Row::get(r, "tokens");
        row_by_actor.insert(actor, tokens.max(0) as u64);
    }
    let row_total: u64 = row_by_actor.values().sum();

    let (stream_total, stream_by_actor) = usage
        .spend_by_actor_since_from_stream(since)
        .await
        .expect("stream read");
    assert_eq!(
        stream_by_actor, row_by_actor,
        "按人的两条读数分叉了：停掉写行之前必须相等"
    );
    assert_eq!(stream_total, row_total);
    assert_eq!(row_total, 45, "三笔之和：30 + 12 + 3");

    pool.close().await;
    drop_schema(&url, &schema).await;
}
