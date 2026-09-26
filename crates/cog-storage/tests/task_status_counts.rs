//! Task counting by state, against a live PostgreSQL.
//!
//! What this checks is what the database decides: that one grouped query over
//! the task table returns the buckets the overview reports, across every
//! workspace, and that a status string this build cannot name is counted in
//! the total only instead of being filed into a bucket nobody can justify.
//! A DB-free test cannot see any of it — the grouping and the unknowns are
//! decisions the server makes.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test task_status_counts -- --ignored
//! ```
//!
//! It clears the task table first, so the database is left as it was found.

use cog_core::{StateBackend, TaskStatus, TaskStatusCounts, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

fn task(id: &str, status: TaskStatus) -> cog_core::Task {
    let mut task = cog_core::Task::new(
        id.to_string(),
        TaskType::Custom("counts".into()),
        serde_json::json!({}),
    );
    task.status = status;
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_census_counts_every_workspace_and_leaves_unknown_statuses_in_the_total() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("clear the task table");

    for (workspace, id, status) in [
        ("ws-1", "t-1", TaskStatus::Running),
        ("ws-1", "t-2", TaskStatus::Pending),
        ("ws-1", "t-3", TaskStatus::Failed),
        ("ws-1", "t-4", TaskStatus::Completed),
        // A second workspace: the census is a cluster reading, so a count that
        // only saw the first one would be a number for a smaller fleet.
        ("ws-2", "t-5", TaskStatus::Scheduled),
        ("ws-2", "t-6", TaskStatus::Running),
    ] {
        backend
            .dag_set_task(workspace, id, &task(id, status))
            .await
            .expect("write a task");
    }

    let counts = backend
        .task_status_counts()
        .await
        .expect("count")
        .expect("this backend holds an enumerable task inventory");
    assert_eq!(
        counts,
        TaskStatusCounts {
            total: 6,
            active: 2,
            queued: 2,
            failed: 1,
        }
    );

    // A row written by a producer whose vocabulary this build does not share:
    // it exists, so it belongs in the total, and it must not be guessed into a
    // bucket where it would be counted as something it never claimed to be.
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status)
         VALUES ('ws-2', 't-7', '{}'::jsonb, 'suspended')",
    )
    .execute(&pool)
    .await
    .expect("write a row with an unknown status");

    let counts = backend
        .task_status_counts()
        .await
        .expect("count")
        .expect("inventory");
    assert_eq!(counts.total, 7, "the unknown row still exists");
    assert_eq!(counts.active, 2);
    assert_eq!(counts.queued, 2);
    assert_eq!(counts.failed, 1);

    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("leave the table as it was found");
}
