//! 归因缺口数的口径，against a live PostgreSQL.
//!
//! What this checks is what the database decides: whether a ledger row that
//! names *something* counts as attributed. Until 2026-10-26 every caller
//! stamped a run id (`ralph-pipeline-<uuid>`) rather than the task the run
//! worked for, so 144,819 rows carrying 1.17e8 tokens named identities that
//! appear nowhere in the task graph. Counting only rows with a null identity
//! made those 27% of the ledger's tokens read as neither a project's cost nor
//! as a gap — they vanished from both numbers, which is the shape of "cost
//! looks lower than it is, so cost performance looks better". They are the
//! second-largest nameless bucket: the rows naming nothing at all carry 3.10e8
//! of the ledger's 4.27e8.
//!
//! It cannot be checked without a server: the answer is a `NOT EXISTS` join
//! against the graph, and a DB-free test would only be restating the query.
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test usage_unattributed -- --ignored
//! ```
//!
//! The rows it writes live in 2001 so no concurrent run can land in the window
//! it reads, and are deleted at the end.

use chrono::{TimeZone, Utc};
use cog_observability::usage_store::LlmUsageStore;
use sqlx::PgPool;

use cog_test_support::database_url;

/// 一行账。身份由调用方给：`None` 是没声明，`Some` 是声明了。
async fn ledger_row(pool: &PgPool, task: Option<&str>, tokens: i64) {
    sqlx::query(
        "INSERT INTO gateway_llm_usage
             (id, ts, upstream, api_style, model, result, tokens_input, tokens_output, task_id)
         VALUES ($1, $2, 'test-upstream', 'openai', 'test-model', 'ok', $3, 0, $4)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(Utc.with_ymd_and_hms(2001, 1, 1, 0, 0, 0).unwrap())
    .bind(tokens)
    .bind(task)
    .execute(pool)
    .await
    .expect("write a ledger row");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_naming_a_run_is_no_more_traceable_than_a_row_naming_nothing() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    store
        .init_schema(&schema_runner)
        .await
        .expect("create the ledger schema");
    // 这个方法问的正是「账上这个 id 是不是一件活」，所以图的那张表必须在，
    // 而且只需这两列：它被当成名单读，不当成任务读。
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS cog_dag_tasks (
             workspace_id TEXT  NOT NULL,
             task_id      TEXT  NOT NULL,
             task         JSONB NOT NULL DEFAULT '{}'::jsonb,
             status       TEXT,
             updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
             PRIMARY KEY (workspace_id, task_id))",
    )
    .execute(&pool)
    .await
    .expect("create the task graph table");
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
         VALUES ('ws-gap', 'traced-task', '{}'::jsonb, 'pending')
         ON CONFLICT DO NOTHING",
    )
    .execute(&pool)
    .await
    .expect("write a task");

    // 三行：没身份的 111、写了运行 id 的 222、写了一件真任务的 444。
    ledger_row(&pool, None, 111).await;
    ledger_row(&pool, Some("ralph-pipeline-7bb0d434"), 222).await;
    ledger_row(&pool, Some("traced-task"), 444).await;

    let since = Utc.with_ymd_and_hms(2001, 1, 1, 0, 0, 0).unwrap();
    let until = Utc.with_ymd_and_hms(2001, 1, 2, 0, 0, 0).unwrap();
    let gap = store
        .unattributed_between(since, until)
        .await
        .expect("read the gap");

    assert_eq!(
        gap, 333,
        "写了一行 id 不等于归到了哪件活：任务图里没有这个 id，它就是没有出处；\
         而指向真任务的那 444 不算缺口"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE ts >= $1 AND ts < $2")
        .bind(since)
        .bind(until)
        .execute(&pool)
        .await
        .expect("leave the ledger as it was found");
    sqlx::query("DELETE FROM cog_dag_tasks WHERE task_id = 'traced-task'")
        .execute(&pool)
        .await
        .expect("leave the graph as it was found");
}
