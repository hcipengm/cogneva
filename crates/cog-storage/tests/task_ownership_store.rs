//! The owner guard on a task transition, against a live PostgreSQL.
//!
//! Ownership is only as good as the statement that enforces it. A transition
//! that checked the status alone would let a unit that merely read the row move
//! work it does not own, and a payload that carried another owner would let one
//! unit hand a task to a second without a handover ever being written. Both are
//! invisible in the call sites -- the caller is unchanged and the write
//! succeeds -- so the check has to run against the server, in the same
//! statement production runs.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_ownership_store -- --ignored
//! ```
//!
//! The table is the backend's own (`init_schema`), and every task id carries a
//! per-run marker, so the database is left as it was found.

use chrono::Utc;
use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

fn task(id: &str, owner: Option<&str>, status: TaskStatus) -> Task {
    let mut task = Task::new(id, TaskType::DagNode, serde_json::json!({}));
    task.owner_unit = owner.map(str::to_string);
    task.status = status;
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn only_the_owner_named_by_the_row_can_move_it() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let marker = format!(
        "ownership-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let workspace = format!("{marker}:ws");
    let id = format!("{marker}:owned");
    backend
        .dag_set_task(&workspace, &id, &task(&id, Some("8b"), TaskStatus::Pending))
        .await
        .expect("seed the task");

    let mut moved = task(&id, Some("8b"), TaskStatus::Scheduled);
    moved.updated_at = Utc::now();
    let refused = backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Pending], &moved, "6e")
        .await;
    assert!(
        matches!(refused, Err(cog_core::SFError::Conflict(_))),
        "a unit that does not own the row must not move it: {refused:?}"
    );
    assert_eq!(
        backend
            .dag_get_task(&workspace, &id)
            .await
            .expect("read back")
            .expect("the row is still there")
            .status,
        TaskStatus::Pending,
        "a refused transition must not have written anything"
    );

    backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Pending], &moved, "8b")
        .await
        .expect("the owner's transition lands");
    assert_eq!(
        backend
            .dag_get_task(&workspace, &id)
            .await
            .expect("read back")
            .expect("the row is still there")
            .status,
        TaskStatus::Scheduled
    );

    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(&workspace)
        .execute(&pool)
        .await
        .expect("clean up");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_transition_cannot_hand_the_task_to_another_owner() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let marker = format!(
        "ownership-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let workspace = format!("{marker}:ws");
    let id = format!("{marker}:handover");
    backend
        .dag_set_task(&workspace, &id, &task(&id, Some("8b"), TaskStatus::Pending))
        .await
        .expect("seed the task");

    let mut stolen = task(&id, Some("6e"), TaskStatus::Running);
    stolen.updated_at = Utc::now();
    let refused = backend
        .dag_transition_owned_task(&workspace, &id, &[TaskStatus::Pending], &stolen, "8b")
        .await;
    assert!(
        matches!(refused, Err(cog_core::SFError::Conflict(_))),
        "changing the owner is the handover's decision, not a state transition's: {refused:?}"
    );
    let row = backend
        .dag_get_task(&workspace, &id)
        .await
        .expect("read back")
        .expect("the row is still there");
    assert_eq!(row.owner_unit.as_deref(), Some("8b"));
    assert_eq!(row.status, TaskStatus::Pending);

    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(&workspace)
        .execute(&pool)
        .await
        .expect("clean up");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_stale_read_is_a_conflict_and_an_unowned_row_is_not_claimable() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let marker = format!(
        "ownership-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let workspace = format!("{marker}:ws");

    let stale_id = format!("{marker}:stale");
    backend
        .dag_set_task(
            &workspace,
            &stale_id,
            &task(&stale_id, Some("8b"), TaskStatus::Running),
        )
        .await
        .expect("seed the running task");
    let mut moved = task(&stale_id, Some("8b"), TaskStatus::Scheduled);
    moved.updated_at = Utc::now();
    let refused = backend
        .dag_transition_owned_task(&workspace, &stale_id, &[TaskStatus::Pending], &moved, "8b")
        .await;
    assert!(
        matches!(refused, Err(cog_core::SFError::Conflict(_))),
        "a row that moved on is a conflict, not a failed task: {refused:?}"
    );

    // The shared queue's tasks name no owner; the owner-guarded path must not
    // be a way to claim one, or a unit could take work the graph never gave it.
    let unowned_id = format!("{marker}:unowned");
    backend
        .dag_set_task(
            &workspace,
            &unowned_id,
            &task(&unowned_id, None, TaskStatus::Pending),
        )
        .await
        .expect("seed the unowned task");
    let mut claimed = task(&unowned_id, Some("8b"), TaskStatus::Running);
    claimed.updated_at = Utc::now();
    let refused = backend
        .dag_transition_owned_task(
            &workspace,
            &unowned_id,
            &[TaskStatus::Pending],
            &claimed,
            "8b",
        )
        .await;
    assert!(
        matches!(refused, Err(cog_core::SFError::Conflict(_))),
        "an unowned task is not this unit's to take: {refused:?}"
    );

    let missing = backend
        .dag_transition_owned_task(
            &workspace,
            &format!("{marker}:never-existed"),
            &[TaskStatus::Pending],
            &moved,
            "8b",
        )
        .await;
    assert!(
        matches!(missing, Err(cog_core::SFError::TaskFailed { .. })),
        "a task that is not there is not a conflict: {missing:?}"
    );

    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(&workspace)
        .execute(&pool)
        .await
        .expect("clean up");
}
