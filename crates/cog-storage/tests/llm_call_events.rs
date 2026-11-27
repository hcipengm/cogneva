//! The `llm_call` write path against a live PostgreSQL.
//!
//! Two claims, and both are the server's answer rather than the caller's:
//!
//! - one call writes one row, with the envelope the stream's readers select by
//!   (kind, subject, writer, idempotency key) and the payload parseable as JSON
//!   in `attributes`;
//! - replaying the same call writes nothing the second time, because the
//!   identity is the idempotency key -- a retried call is the same spend, not a
//!   second one.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL`. The case owns a schema
//! of its own and drops it, so the live stream is never touched:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test llm_call_events -- --ignored
//! ```

use chrono::Utc;
use sqlx::PgPool;

use cog_core::LlmCall;
use cog_storage::LlmCallEventStore;

/// A schema carrying this run's event table, dropped when the case ends.
struct Scratch {
    pool: PgPool,
    schema: String,
}

impl Scratch {
    async fn new(case: &str) -> Self {
        let schema = cog_test_support::own_schema(
            &cog_test_support::database_url(),
            &format!("probe_llm_call_{case}"),
        )
        .await;
        let pool =
            cog_test_support::schema_pool_with(&cog_test_support::database_url(), &schema, 2).await;
        // 与真表同形状的最小列集：写入口点的是这些列，少一列就不是同一个问题。
        sqlx::query(
            "CREATE TABLE cogneva_event (
                 event_id        TEXT PRIMARY KEY,
                 at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 kind            TEXT NOT NULL,
                 subject_kind    TEXT NOT NULL,
                 subject_id      TEXT NOT NULL,
                 unit_of_work    TEXT NOT NULL,
                 step            TEXT NOT NULL,
                 outcome         TEXT NOT NULL,
                 attempt         INTEGER NOT NULL DEFAULT 1,
                 idempotency_key TEXT NOT NULL,
                 causation_id    TEXT,
                 batch_id        TEXT,
                 actor           TEXT NOT NULL,
                 writer          TEXT NOT NULL,
                 scope           TEXT NOT NULL,
                 payload_version INTEGER NOT NULL DEFAULT 1,
                 attributes      TEXT NOT NULL,
                 UNIQUE (writer, idempotency_key)
             )",
        )
        .execute(&pool)
        .await
        .expect("the event table must be creatable");
        // 回填读的是账本那张表：它要与真表同形状的那些列。
        sqlx::query(
            "CREATE TABLE gateway_llm_usage (
                 id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                 ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 actor          TEXT NOT NULL DEFAULT 'unknown',
                 upstream       TEXT NOT NULL,
                 api_style      TEXT NOT NULL,
                 model          TEXT NOT NULL,
                 result         TEXT NOT NULL,
                 origin         TEXT NOT NULL DEFAULT 'unknown',
                 tokens_input   BIGINT NOT NULL DEFAULT 0,
                 tokens_output  BIGINT NOT NULL DEFAULT 0,
                 tokens_cached  BIGINT NOT NULL DEFAULT 0,
                 task_id        TEXT,
                 owner_id       TEXT
             )",
        )
        .execute(&pool)
        .await
        .expect("the usage table must be creatable");
        Self { pool, schema }
    }

    async fn rows(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM cogneva_event")
            .fetch_one(&self.pool)
            .await
            .expect("count the events")
    }

    async fn done(self) {
        cog_test_support::drop_schema(&cog_test_support::database_url(), &self.schema).await;
    }
}

fn call(id: &str) -> LlmCall {
    LlmCall {
        call_id: id.to_string(),
        upstream: "probe".into(),
        api_style: "openai".into(),
        model: "probe-model".into(),
        result: "ok".into(),
        input_tokens: 10,
        output_tokens: 20,
        cached_tokens: 0,
        cache_created_tokens: None,
        origin: "gateway_pool".into(),
        owner: None,
        latency_ms: Some(30),
        task_id: Some("task-1".into()),
        actor: "probe-actor".into(),
        cost_micros: None,
        price_book: None,
        session: None,
        batch_id: None,
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
        at: Utc::now(),
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn one_call_writes_one_row_with_the_envelope_readers_select_by() {
    let scratch = Scratch::new("envelope").await;
    let store = LlmCallEventStore::new(scratch.pool.clone(), "probe-deployment");
    store.append(&call("call-1")).await.expect("append");

    let row = sqlx::query(
        "SELECT kind, subject_id, writer, idempotency_key, attributes
           FROM cogneva_event WHERE subject_id = 'call-1'",
    )
    .fetch_one(&scratch.pool)
    .await
    .expect("the row is there");
    use sqlx::Row;
    assert_eq!(row.get::<String, _>("kind"), "llm_call");
    assert_eq!(row.get::<String, _>("subject_id"), "call-1");
    assert_eq!(row.get::<String, _>("writer"), "probe-deployment");
    assert_eq!(
        row.get::<String, _>("idempotency_key"),
        "call-1",
        "the idempotency key is the call identity"
    );
    let payload: serde_json::Value = serde_json::from_str(&row.get::<String, _>("attributes"))
        .expect("attributes has to parse as an object, not as a string holding JSON");
    assert_eq!(payload["model"], "probe-model");
    assert_eq!(payload["input_tokens"], 10);
    assert_eq!(payload["task_id"], "task-1");
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn replaying_the_same_call_writes_nothing_the_second_time() {
    let scratch = Scratch::new("idempotent").await;
    let store = LlmCallEventStore::new(scratch.pool.clone(), "probe-deployment");
    store.append(&call("call-1")).await.expect("first");
    store.append(&call("call-1")).await.expect("replay");
    assert_eq!(
        scratch.rows().await,
        1,
        "a retried call is the same spend, not a second one"
    );
    scratch.done().await;
}

/// 回填是幂等的：同一批行跑两遍，流里仍然只有那一批。
///
/// 这条判据保护的是「回填可以中断、可以从头再来」：不幂等的话，跑到一半断了就得先
/// 记住跑到哪儿，而「记住跑到哪儿」正是回填要避免的那份额外状态。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn backfilling_the_same_rows_twice_writes_each_call_once() {
    let scratch = Scratch::new("backfill").await;
    sqlx::query(
        "INSERT INTO gateway_llm_usage (id, ts, actor, upstream, api_style, model, result, origin,
                                        tokens_input, tokens_output, tokens_cached)
         VALUES (gen_random_uuid(), NOW() - interval '1 day', 'a', 'u', 'openai', 'm', 'ok',
                 'gateway_pool', 10, 20, 0),
                (gen_random_uuid(), NOW() - interval '2 day', 'b', 'u', 'openai', 'm', 'ok',
                 'gateway_pool', 1, 2, 0)",
    )
    .execute(&scratch.pool)
    .await
    .expect("seed two rows");

    let (first, _) =
        cog_storage::postgres::usage_backfill::backfill_page(&scratch.pool, "probe", None, 100)
            .await
            .expect("first page");
    assert_eq!(first, 2);
    let (second, _) =
        cog_storage::postgres::usage_backfill::backfill_page(&scratch.pool, "probe", None, 100)
            .await
            .expect("second page");
    assert_eq!(second, 2, "第二遍照样读得到两行");
    assert_eq!(
        scratch.rows().await,
        2,
        "两遍回填之后流里仍然是两条：键由行 id 派生，重跑是同一次"
    );
    scratch.done().await;
}
