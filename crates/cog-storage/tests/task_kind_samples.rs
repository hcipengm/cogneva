//! The work duration a history-based baseline is measured from, against a live
//! PostgreSQL.
//!
//! What these tests check is what the database decides: which two timestamps a
//! finished row's duration is taken from, and whether a row whose work time was
//! never measured counts as a sample. Neither can be seen by a DB-free test --
//! the percentile and the grouping are the server's, and the two candidate
//! timestamps live in the row's JSON.
//!
//! The distinction has a price on this platform and not a small one. Five
//! finished `change_ledger` rows (2026-10-26) were created on 10-18 and only
//! picked up on 10-25: measured from creation they ran 7.7 days, measured from
//! the start they ran 7 minutes. A baseline built on the first ruler draws a
//! week-long bar for every one of the 2354 waiting tasks; on the second, a
//! seven-minute one. Queue time is not work.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test task_kind_samples -- --ignored
//! ```
//!
//! It clears the task table first, so the database is left as it was found.

use chrono::{Duration, Utc};
use cog_core::StateBackend;
use cog_storage::{PostgresObservabilityGateway, PostgresStateBackend};
use sqlx::PgPool;

use cog_test_support::database_url;

/// A finished row in one workspace, with the queue time and the work time kept
/// apart: it waited `queued_days` for a worker and then ran `worked_secs`.
///
/// `started` writes the start stamp. A row without one is a finished task whose
/// work time this platform never measured -- the state every row was in before
/// dispatch started stamping it.
async fn finished(
    pool: &PgPool,
    workspace: &str,
    id: &str,
    kind: &str,
    queued_days: i64,
    worked_secs: i64,
    started: bool,
) {
    let now = Utc::now();
    let mut task = serde_json::json!({
        "id": id,
        "task_type": { "custom": kind },
        "created_at": (now - Duration::days(queued_days)).to_rfc3339(),
        "updated_at": now.to_rfc3339(),
    });
    if started {
        task["started_at"] = serde_json::json!((now - Duration::seconds(worked_secs)).to_rfc3339());
    }
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status, updated_at)
         VALUES ($1, $2, $3, 'completed', NOW())
         ON CONFLICT (workspace_id, task_id)
         DO UPDATE SET task = EXCLUDED.task, status = EXCLUDED.status, updated_at = NOW()",
    )
    .bind(workspace)
    .bind(id)
    .bind(&task)
    .execute(pool)
    .await
    .expect("write a finished row");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_duration_a_baseline_inherits_is_the_time_the_task_worked() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    // The token axis joins the metrics table, so it has to exist even when this
    // test inserts no metrics row: a missing relation fails the whole query, and
    // the sample read would read as "this backend has no samples" instead.
    PostgresObservabilityGateway::new(pool.clone())
        .init_schema()
        .await
        .expect("create the observability schema");
    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("clear the task table");

    // Five rows that each waited a week and each ran ten minutes.
    for i in 0..5 {
        finished(
            &pool,
            "ws-samples",
            &format!("waited-{i}"),
            "waited",
            7,
            600,
            true,
        )
        .await;
    }
    // Five rows of another kind that finished but never stamped a start.
    for i in 0..5 {
        finished(
            &pool,
            "ws-samples",
            &format!("unmeasured-{i}"),
            "unmeasured",
            7,
            600,
            false,
        )
        .await;
    }

    let records = backend
        .task_kind_records("ws-samples", 5)
        .await
        .expect("read the samples");

    let waited = records
        .iter()
        .find(|record| record.kind == "waited")
        .expect("a kind with five measured work spans gets a record");
    assert_eq!(
        waited.median_duration_secs,
        600,
        "工期要从开工量起；按创建量会得到一周（{} 秒），那是排队时间",
        7 * 24 * 3600
    );
    assert_eq!(waited.samples, 5, "五条量过工时的样本");

    assert!(
        records.iter().all(|record| record.kind != "unmeasured"),
        "一条都没量过工时的种类不给记录：拿到一行时长中位 0 的读数，读者读成「量过了，是 0」"
    );

    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("leave the task table as it was found");
    sqlx::query("DELETE FROM cog_observability_metrics")
        .execute(&pool)
        .await
        .expect("leave the census as it was found");
}

/// A per-task cumulative census row: what this task has spent so far.
async fn spent(pool: &PgPool, id: &str, total_tokens: i64) {
    sqlx::query(
        "INSERT INTO cog_observability_metrics
             (task_id, total_tokens, prompt_tokens, completion_tokens,
              tool_calls, iterations, duration_ms, timestamp)
         VALUES ($1, $2, $2, 0, 0, 1, 0, NOW())
         ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(total_tokens)
    .execute(pool)
    .await
    .expect("write a census row");
}

/// The token axis counts its own samples; the duration axis counts its own.
///
/// A finished task with no census row is not a task that spent zero -- it is a
/// task whose spend was recorded under an identity the graph never saw (the run
/// id, before callers started stamping the owning task). Folding those into the
/// token median as zeros would drag every kind's budget down, and taking the
/// median over whatever few rows happen to carry a cost would set a whole
/// kind's budget from one task. Both are the wrong number for a gate that
/// refuses dispatch.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_budget_needs_its_own_samples_not_the_ones_that_finished() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    PostgresObservabilityGateway::new(pool.clone())
        .init_schema()
        .await
        .expect("create the observability schema");
    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("clear the task table");
    sqlx::query("DELETE FROM cog_observability_metrics")
        .execute(&pool)
        .await
        .expect("clear the census");

    // Five finished tasks of one kind, only one of which carries a cost.
    for i in 0..5 {
        finished(
            &pool,
            "ws-samples",
            &format!("partly-costed-{i}"),
            "partly_costed",
            0,
            600,
            true,
        )
        .await;
    }
    spent(&pool, "partly-costed-0", 900_000).await;

    // Five finished tasks of another kind, all costed.
    for (i, tokens) in [100, 200, 300, 400, 500].into_iter().enumerate() {
        let id = format!("costed-{i}");
        finished(&pool, "ws-samples", &id, "costed", 0, 600, true).await;
        spent(&pool, &id, tokens).await;
    }

    let records = backend
        .task_kind_records("ws-samples", 5)
        .await
        .expect("read the samples");

    let partly = records
        .iter()
        .find(|record| record.kind == "partly_costed")
        .expect("五条量过工时的任务仍然给这一行：没有花销样本不等于没有工期");
    assert_eq!(partly.samples, 5);
    assert_eq!(
        partly.median_tokens, 0,
        "一条带账的样本不足以给整类定预算：预算是拿一件活的账当一整类的计划"
    );

    let costed = records
        .iter()
        .find(|record| record.kind == "costed")
        .expect("五条带账的样本");
    assert_eq!(
        costed.median_tokens, 300,
        "五条带账样本取中位，不是把没有账的那些折成 0 再取"
    );

    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("leave the task table as it was found");
    sqlx::query("DELETE FROM cog_observability_metrics")
        .execute(&pool)
        .await
        .expect("leave the census as it was found");
}
