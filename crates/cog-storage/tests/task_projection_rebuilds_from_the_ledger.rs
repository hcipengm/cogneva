//! 投影是可以丢的：它上面每一行都能从账本重新算出来。
//!
//! `task_current` 是一张**只放还动得了的任务**的小表：锁 workspace 的那条语句在
//! 它上面 `FOR UPDATE`，租约的在场也写在它上面，治理面按它筛「这件任务还开着」。
//! 它不存任何「只在这里说过一次」的东西：状态是迁移表里最后一条，尝试号与开始
//! 时刻是尝试表里最新那一行，退避记在那条迁移上。唯一不在账本里的是租约，而租约
//! 是**在场**不是历史——重建把它写成 NULL，读作「没有证据说它还在跑」，这正是
//! 回收路径本来就处理的那一档。
//!
//! 反过来说，读状态的那几个入口（`dag_get_task`、`dag_get_live_tasks`）走的是
//! 账本，不是投影：投影丢了，它们答得照样对。所以这里判三件事，都要一个活库：
//!
//! - 把整张投影删掉，状态读回来仍然对：它来自迁移，不是来自投影；
//! - 重建把还动得了的任务放回去、终态的不放，租约清空、退避原样；
//! - 一条终态任务被某个旧写者留在了投影里，重建会把它撤掉——两半合起来才是
//!   「投影恰好是账本上还活着的那批任务」。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_projection_rebuilds_from_the_ledger -- --ignored
//! ```

use chrono::{Duration, Utc};
use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

mod common;

use cog_test_support::database_url;

/// 本用例自己的工作区，收尾只清这一片。
const WORKSPACE: &str = "ws-rebuild";

fn task(id: &str, status: TaskStatus) -> Task {
    let mut task = Task::new(
        id,
        TaskType::DagNode,
        serde_json::json!({ "goal": "rebuild probe" }),
    );
    task.status = status;
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_lost_projection_is_recomputed_from_history() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("clear what an earlier round left");

    let expired = Utc::now() + Duration::minutes(30);
    for (id, status) in [
        ("t-pending", TaskStatus::Pending),
        ("t-scheduled", TaskStatus::Scheduled),
        ("t-running", TaskStatus::Running),
        ("t-done", TaskStatus::Completed),
        ("t-failed", TaskStatus::Failed),
    ] {
        let mut row = task(id, status);
        if id == "t-scheduled" {
            row.retry_not_before = Some(expired);
        }
        if id == "t-running" {
            row.lease_owner = Some("pod-that-wrote-it".into());
            row.lease_expires_at = Some(Utc::now() + Duration::minutes(5));
        }
        backend
            .dag_set_task(WORKSPACE, id, &row)
            .await
            .expect("write a task");
    }

    // 写完之后投影恰好是还动得了的那三条，终态的两条已经不在表里。
    assert_eq!(
        common::task_projection_ids(&pool, WORKSPACE).await,
        vec!["t-pending", "t-running", "t-scheduled"],
        "the projection holds the tasks that can still move"
    );
    let done = backend
        .dag_get_task(WORKSPACE, "t-done")
        .await
        .expect("read the finished task")
        .expect("a terminal task is still readable from the ledger");
    assert_eq!(done.status, TaskStatus::Completed);

    // 投影整个丢了——一次误删、一次恢复出来的旧备份、一次没跑完的重建，都是这个形状。
    sqlx::query("DELETE FROM task_current WHERE workspace_id = $1")
        .bind(WORKSPACE)
        .execute(&pool)
        .await
        .expect("lose the projection");
    assert!(
        common::task_projection_ids(&pool, WORKSPACE)
            .await
            .is_empty(),
        "the projection is exactly its rows: gone means gone"
    );
    assert_eq!(
        backend
            .dag_get_task(WORKSPACE, "t-running")
            .await
            .expect("read the running task")
            .expect("the task is still there")
            .status,
        TaskStatus::Running,
        "state is the last transition; a lost projection does not lose it"
    );

    // 重建。返回的是整张投影的行数（不是本工作区的），所以这里只判它个下限。
    let restored = backend
        .rebuild_task_projection(None)
        .await
        .expect("rebuild the projection");
    assert!(
        restored >= 3,
        "the rebuild reports what the projection holds afterwards: {restored}"
    );
    assert_eq!(
        common::task_projection_ids(&pool, WORKSPACE).await,
        vec!["t-pending", "t-running", "t-scheduled"],
        "the rebuild puts back exactly the tasks that can still move"
    );
    let running = backend
        .dag_get_task(WORKSPACE, "t-running")
        .await
        .expect("read the rebuilt running task")
        .expect("the running task is still there");
    assert_eq!(
        running.lease_owner, None,
        "a lease is presence, not history: a rebuild has no evidence it is still held"
    );
    assert_eq!(
        running.status,
        TaskStatus::Running,
        "the run is still running -- what the rebuild cannot restore is the lease, not the state"
    );
    let scheduled = backend
        .dag_get_task(WORKSPACE, "t-scheduled")
        .await
        .expect("read the rebuilt scheduled task")
        .expect("the scheduled task is still there");
    assert_eq!(
        scheduled.retry_not_before.map(|at| at.timestamp()),
        Some(expired.timestamp()),
        "the backoff is a decision recorded on the transition, so it survives the rebuild"
    );

    // 一个旧写者把终态任务留在了投影里：它不在账本的「还能动」里，重建之后要走。
    sqlx::query(
        "INSERT INTO task_current
             (workspace_id, task_id, state, as_of_seq, def_rev, updated_at)
         VALUES ($1, 't-done', 'pending', 1, 1, NOW())",
    )
    .bind(WORKSPACE)
    .execute(&pool)
    .await
    .expect("leave a stale row where an old writer would have left it");
    backend
        .rebuild_task_projection(Some("t-done"))
        .await
        .expect("rebuild only that task");
    assert_eq!(
        common::task_projection_ids(&pool, WORKSPACE).await,
        vec!["t-pending", "t-running", "t-scheduled"],
        "a finished task left in the projection is retired by the rebuild"
    );

    // 一件一件重建也是同一条语句：只碰点名的那一条。
    sqlx::query("DELETE FROM task_current WHERE workspace_id = $1")
        .bind(WORKSPACE)
        .execute(&pool)
        .await
        .expect("lose the projection again");
    backend
        .rebuild_task_projection(Some("t-pending"))
        .await
        .expect("rebuild one task");
    assert_eq!(
        common::task_projection_ids(&pool, WORKSPACE).await,
        vec!["t-pending"],
        "a rebuild narrowed to one task writes only that task"
    );

    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("leave the ledger as it was found");
}
