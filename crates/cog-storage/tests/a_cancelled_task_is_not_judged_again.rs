//! 一条已经被取消的任务，不会被随后交回来的失败再判一次——against a live
//! PostgreSQL.
//!
//! 取消是别的执行体（部署器按「这一版已经被后一版走过」、上游级联）对这一行做出
//! 的决定，写进 `status` 与 `error` 那一格；而失败是**取消之后**执行侧交回来的
//! 结局。两者先后到达时，先发生的那个是决定，后发生的那个只是结局——结局不能把
//! 决定改写回去。
//!
//! 2026-11-06 实测：13 轮 `ci-red-*-fix` 全部以 `terminal_env_failure: … no handle
//! …` 结束，而它们其实都是被部署器主动取消的。取消先写 `Cancelled` 与理由，
//! `dag_fail_task` 随后无条件把 `status` 改成 `Pending`（重试预算未用完）或
//! `Failed`（用完），并把 `error` 换成执行侧那句「说不清为什么结束」。于是每一次
//! 有意取消都被记成一次环境故障，还被 `ci_hold_attempts` 当成「试过了没成」抄进
//! 下一轮的题面——修复回路自己指着假证据干活。
//!
//! 这一格只有真库能判：守卫在 `dag_fail_task` 里，而它要读的是**库里那一行的
//! 当前状态**，不是调用方手里的快照。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL` 指向一张一次性的库：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test a_cancelled_task_is_not_judged_again -- --ignored
//! ```
//!
//! 它先清空自己那个工作区，跑完把库留在原样。

use std::time::Duration;

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

const WORKSPACE: &str = "ws-cancel-guard";
const TASK: &str = "fix-round-13";

/// 取消这一行的那个理由。判据认它原文：它一旦被换掉，读的人就再也看不出这一行
/// 是被有意停掉的还是自己失败的。
const REASON: &str = "the revision this fix was for has been walked past; the fix is stale";

/// 取消之后执行侧交回来的话。
const LATE_FAILURE: &str = "Task ended without answering, and no handle was left to say why";

fn task(id: &str) -> Task {
    let mut task = Task::new(id.to_string(), TaskType::DagNode, serde_json::json!({}));
    task.status = TaskStatus::Running;
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_late_failure_leaves_a_cancelled_row_cancelled() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("clear this test's workspace");

    backend
        .dag_set_task(WORKSPACE, TASK, &task(TASK))
        .await
        .expect("write the task");
    backend
        .dag_cancel_task(WORKSPACE, TASK, REASON.to_string())
        .await
        .expect("cancel the task");

    // 预算还有三次没用完：这一路本来会把这一行打回 `Pending` 等下一次。
    let (retried, cancelled) = backend
        .dag_fail_task(
            WORKSPACE,
            TASK,
            LATE_FAILURE.to_string(),
            None,
            3,
            Duration::ZERO,
        )
        .await
        .expect("report the late failure");

    assert!(
        !retried,
        "一条已经取消的行被记成了「回队列重试」：取消是决定，不是一次可以再试的失败"
    );
    assert!(
        cancelled.is_empty(),
        "一条已经取消的行被判死又级联取消了别人：{cancelled:?}"
    );

    let after = backend
        .dag_get_task(WORKSPACE, TASK)
        .await
        .expect("read the row back")
        .expect("the row is still there");
    assert_eq!(
        after.status,
        TaskStatus::Cancelled,
        "取消被执行侧交回来的结局改写成了 {:?}",
        after.status
    );
    assert_eq!(
        after.error.as_deref(),
        Some(REASON),
        "取消的理由被执行的那句话覆盖了——读的人从此只知道「没留下 handle」，\
         不知道这一行是被有意停掉的"
    );
    assert_eq!(after.retry_count, 0, "一次有意取消不该花掉重试预算");
}
