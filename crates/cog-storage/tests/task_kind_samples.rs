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

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

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
        .expect("leave the table as it was found");
}
