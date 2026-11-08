//! 卸载一次再装回来，账本要能自己走回去。
//!
//! 迁移的回滚不是「删掉就完了」：这一版的回滚要把旧文档表**从账本重建**出来，
//! 而升级路径又是从那张表导入新的账本。这两份代码互为对方的逆运算，没有一条用例
//! 同时跑过它们——单看哪一份都是自洽的，拼起来才知道漏了什么。
//!
//! 这里判四件事，全都要一个活库：
//!
//! - 回滚之后四张账本表与追加保护的函数都不在了：**函数不是表的子物**，删表不会
//!   删掉它调用的那个函数，留下的是一个没人调用的残留；
//! - 旧文档表被重建出来，里面的任务还在（说明账本确实持有原来文档的全部字段）；
//! - 再升级一次，任务回到账本里，状态还是原来那一条（回绕没有丢东西）；
//! - 追加保护跟着一起回来：再装一次之后历史照样拒绝改写。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL` 指向一个**可丢的**库——这条用例会
//! 真的把 `task_defined` 等四张表删掉再建回来：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test the_ledger_can_be_uninstalled_and_reinstalled -- --ignored
//! ```

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

const UP: &str = include_str!("../migrations/postgres/095_task_ledger.up.sql");
const DOWN: &str = include_str!("../migrations/postgres/095_task_ledger.down.sql");

/// 本用例自己的工作区与任务。
const WORKSPACE: &str = "ws-reinstall";
const TASK: &str = "t-survives-uninstall";

/// 表在不在。
async fn exists(pool: &PgPool, name: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("ask the catalog")
}

/// 追加保护的函数在不在。`to_regprocedure` 收的是带参数列表的名字——
/// `to_regproc('f()')` 在 PostgreSQL 里恒为 NULL，会把这个判断变成永远「不在」。
async fn guard_function(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT to_regprocedure('task_ledger_history_is_append_only()') IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .expect("ask the catalog for the guard function")
}

/// 历史表上的拒绝改写触发器在不在。
async fn guard_trigger(pool: &PgPool, table: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                         WHERE c.relname = $1 AND t.tgname = $2 AND NOT t.tgisinternal)",
    )
    .bind(table)
    .bind(format!("{table}_is_append_only"))
    .fetch_one(pool)
    .await
    .expect("ask the catalog for the guard trigger")
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn uninstalling_and_reinstalling_the_ledger_round_trips_a_task() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("clear what an earlier round left");

    let mut task = Task::new(
        TASK,
        TaskType::DagNode,
        serde_json::json!({ "goal": "活着穿过一次卸载" }),
    );
    task.status = TaskStatus::Running;
    backend
        .dag_set_task(WORKSPACE, TASK, &task)
        .await
        .expect("write a task");

    // 回滚。
    sqlx::raw_sql(DOWN)
        .execute(&pool)
        .await
        .expect("the down migration must run on a database that has the ledger");

    for table in [
        "task_defined",
        "task_transition",
        "task_attempt",
        "task_current",
    ] {
        assert!(
            !exists(&pool, table).await,
            "{table} must be gone after uninstalling"
        );
    }
    assert!(
        !guard_function(&pool).await,
        "a dropped table drops its triggers, not the function they call: \
         the guard function is a residue unless the down migration drops it too"
    );

    // 旧文档表被重建出来，任务在里面。
    assert!(
        exists(&pool, "cog_dag_tasks").await,
        "the rollback rebuilds the document table from the ledger"
    );
    let written_back: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id = $2",
    )
    .bind(WORKSPACE)
    .bind(TASK)
    .fetch_one(&pool)
    .await
    .expect("read the rebuilt document");
    assert_eq!(
        written_back, 1,
        "a task that was in the ledger must be in the document the rollback writes"
    );

    // 再装回来。
    sqlx::raw_sql(UP)
        .execute(&pool)
        .await
        .expect("the up migration must run on a database holding the old document table");

    assert!(
        !exists(&pool, "cog_dag_tasks").await,
        "the upgrade imports the document and drops it; a leftover table is a second truth"
    );
    let read_back = backend
        .dag_get_task(WORKSPACE, TASK)
        .await
        .expect("read the reinstalled task")
        .expect("the task must survive the round trip");
    assert_eq!(
        read_back.status,
        TaskStatus::Running,
        "the state the task had before the uninstall is the state it comes back with"
    );

    // 追加保护跟着回来，并且真的在拦。
    assert!(
        guard_function(&pool).await,
        "the reinstall must put the guard function back"
    );
    for table in ["task_defined", "task_transition"] {
        assert!(
            guard_trigger(&pool, table).await,
            "the reinstall must put the append-only trigger back on {table}"
        );
    }
    let refused = sqlx::query("UPDATE task_defined SET venue = 'rewritten' WHERE task_id = $1")
        .bind(TASK)
        .execute(&pool)
        .await
        .expect_err("the reinstalled guard must still refuse a rewrite");
    assert!(
        refused.to_string().contains("append-only"),
        "the refusal must name the invariant rather than a generic error: {refused}"
    );

    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("leave the ledger as it was found");
}
