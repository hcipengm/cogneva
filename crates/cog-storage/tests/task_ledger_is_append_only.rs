//! 账本的历史只许追加，这条不变式由数据库判，不靠每个写者记得。
//!
//! 定义修订与状态迁移是「发生过什么」的记录。改写它们只有一种用法：在别处再存
//! 一份可以就地改的真值，然后两头互相打补丁——而这份账本存在的理由，正是把
//! 「状态存在几处、哪一处算数」收敛成一处。所以拒绝改写的是数据库自己：任何写者、
//! 任何连接、包括人在 psql 里敲的那一条，都撞同一个触发器。
//!
//! 反过来，这条用例也判「拒绝没有宽到把正常的路挡掉」：
//!
//! - 尝试可以就地关掉（`ended_at` 就是「这次跑完了」这个事实），这是写者每次
//!   结束一次执行都要做的事；
//! - 删除仍然是显式的管理动作（撤掉一件任务、清一个工作区），触发器只拦改写。
//!
//! 只在数据库里能判：触发器在不在、拒绝不拒绝，是服务端的行为。默认忽略，需要
//! `COGNEVA_TEST_DATABASE_URL` 指向一个可丢的库：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_ledger_is_append_only -- --ignored
//! ```

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

/// 一个属于本用例的工作区。
const WORKSPACE: &str = "ws-append-only";

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn history_rows_refuse_a_rewrite_while_attempts_and_removal_still_work() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("clear what an earlier round left");

    let mut task = Task::new(
        "t-history",
        TaskType::DagNode,
        serde_json::json!({ "goal": "记一笔历史" }),
    );
    task.status = TaskStatus::Running;
    backend
        .dag_set_task(WORKSPACE, "t-history", &task)
        .await
        .expect("write a task");

    // 两行历史都在：定义一版、迁移一条（`None → Running`）。
    for (statement, what) in [
        (
            "UPDATE task_defined SET venue = 'rewritten' WHERE task_id = 't-history'",
            "定义修订",
        ),
        (
            "UPDATE task_transition SET detail = 'rewritten' WHERE task_id = 't-history'",
            "状态迁移",
        ),
    ] {
        let error = sqlx::query(statement)
            .execute(&pool)
            .await
            .expect_err(&format!("{what}是历史，改写必须被数据库拒绝"));
        let text = error.to_string();
        assert!(
            text.contains("append-only"),
            "拒绝要说出是哪一个不变式而不是一句笼统的权限错：{text}"
        );
    }

    // 尝试是记录里的**当前状态**：一次执行跑完了，写者就地把它关掉。
    sqlx::query("UPDATE task_attempt SET ended_at = NOW() WHERE task_id = 't-history'")
        .execute(&pool)
        .await
        .expect("closing an attempt is a write the ledger makes on every completion");

    // 撤掉一件任务是显式的管理动作，照旧可做；删完之后读不到。
    backend
        .dag_remove_task(WORKSPACE, "t-history")
        .await
        .expect("removing a task is an explicit administrative act");
    assert!(
        backend
            .dag_get_task(WORKSPACE, "t-history")
            .await
            .expect("read the removed task")
            .is_none(),
        "a removed task must not read back"
    );

    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("leave the ledger as it was found");
}
