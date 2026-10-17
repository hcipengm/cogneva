//! What a second run of the same task does to that task's census row, against
//! a live PostgreSQL.
//!
//! One task id is not one run: the pipeline hands the same id to its planner,
//! generator and evaluator, and each of them reports its own usage once when it
//! exits. Whether the per-task book then answers "what did this task cost" or
//! only "what did the last role spend" is decided entirely by the upsert, and
//! nothing in the call sites shows it: an implementation that replaced instead
//! of adding would keep every writer unchanged, every row plausible, and the
//! book short by every role but the last. The reading that catches it needs a
//! server, so this file is ignored by default and needs
//! `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test observability_task_census -- --ignored
//! ```
//!
//! The table is the backend's own (`init_schema`), because the upsert under
//! test is the one production runs. The rows carry a per-run marker and are
//! deleted at the end, so the database is left as it was found.

use chrono::{Duration, Utc};
use cog_core::{ObservabilityGateway, TaskMetrics};
use cog_storage::PostgresObservabilityGateway;
use sqlx::{PgPool, Row};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

fn metrics(task_id: &str, tokens: u64, calls: u32, iterations: u32, ms: i64) -> TaskMetrics {
    TaskMetrics {
        task_id: task_id.to_string(),
        total_tokens: tokens,
        prompt_tokens: tokens * 2 / 5,
        completion_tokens: tokens * 3 / 5,
        tool_calls: calls,
        iterations,
        duration_ms: ms as u64,
        timestamp: Utc::now() + Duration::milliseconds(ms),
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn two_runs_of_one_task_add_into_its_row_and_a_second_task_keeps_its_own() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let gateway = PostgresObservabilityGateway::new(pool.clone());
    gateway.init_schema().await.expect("init the schema");

    let marker = format!(
        "census-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let shared = format!("{marker}:shared");
    let other = format!("{marker}:other");

    // Two roles of one task, each reporting the usage of its own run.
    gateway
        .record_task_metrics(metrics(&shared, 100, 2, 3, 500))
        .await
        .expect("record the first run");
    let second = metrics(&shared, 200, 5, 4, 700);
    gateway
        .record_task_metrics(second.clone())
        .await
        .expect("record the second run");

    let row = gateway
        .get_task_metrics(&shared)
        .await
        .expect("read the task's row");
    assert_eq!(row.total_tokens, 300, "the second run replaced the first");
    assert_eq!(row.prompt_tokens, 120);
    assert_eq!(row.completion_tokens, 180);
    assert_eq!(row.tool_calls, 7);
    assert_eq!(row.iterations, 7);
    assert_eq!(row.duration_ms, 1200);
    assert_eq!(
        row.timestamp, second.timestamp,
        "timestamp is the one field where the latest write is the meaning"
    );

    // A row that is one run is still one run: the rule adds, it does not
    // require a second writer to say what the first one meant.
    gateway
        .record_task_metrics(metrics(&other, 50, 1, 1, 100))
        .await
        .expect("record another task");
    let other_row = gateway
        .get_task_metrics(&other)
        .await
        .expect("read the other task's row");
    assert_eq!(other_row.total_tokens, 50);

    let count: i64 =
        sqlx::query("SELECT COUNT(*) FROM cog_observability_metrics WHERE task_id = $1")
            .bind(&shared)
            .fetch_one(&pool)
            .await
            .expect("count the task's rows")
            .get(0);
    assert_eq!(count, 1, "one task id is one row");

    sqlx::query("DELETE FROM cog_observability_metrics WHERE task_id = ANY($1)")
        .bind(vec![shared.clone(), other.clone()])
        .execute(&pool)
        .await
        .expect("remove the probe rows");
}
