//! Task counting by state, against a live PostgreSQL.
//!
//! What this checks is what the database decides: that one grouped query over
//! the state ledger returns the buckets the overview reports, across every
//! workspace, and that the ledger refuses a state word this build cannot name.
//!
//! The refusal is half the reading. Before the ledger, a producer with another
//! vocabulary could park a row in a state nothing could dispatch, and the
//! census's only defence was to count it in the total and leave it out of every
//! bucket. The ledger closes that at the write: `task_transition.to_state` has a
//! check against the six states, so such a row cannot exist to be counted. A
//! DB-free test cannot see either -- the grouping and the constraint are both
//! the server's.
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

use cog_test_support::database_url;

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
    cog_test_support::clear_task_ledger(&pool)
        .await
        .expect("clear the task ledger");

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

    // A state word this build cannot name is refused at the ledger, so the
    // census can never be handed one. That is the stronger guarantee: a task
    // parked in an undispatchable state is one nothing will ever move, and the
    // only trustworthy count of it is the one that cannot happen.
    let refused = sqlx::query(
        "INSERT INTO task_transition (task_id, seq, to_state) \
         VALUES ('t-6', 2, 'suspended')",
    )
    .execute(&pool)
    .await;
    assert!(
        refused.is_err(),
        "账本必须拒绝它不认识的状态词，否则无人能派发的任务会停在表里"
    );

    cog_test_support::clear_task_ledger(&pool)
        .await
        .expect("leave the ledger as it was found");
}
