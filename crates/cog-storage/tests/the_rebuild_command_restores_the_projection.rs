//! `cog-migrate --rebuild-task-projection` 是投影出事之后人手里的那条命令。
//!
//! 它与写者跑的是同一条语句（`PROJECT_SQL` / `UNPROJECT_SQL`），所以这里判的不
//! 是「算法对不对」，而是「这条命令真的接上了那个算法」：一个人敲下它，投影里
//! 该回来的回来、该走的走，而且它说的行数是真的。
//!
//! 走的是真二进制，不是库里那个函数：从参数到连接串到输出全都是这条命令的一部分，
//! 少接一环，人在凌晨三点敲下去的时候才会发现。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test the_rebuild_command_restores_the_projection -- --ignored
//! ```

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

mod common;

use cog_test_support::database_url;

const WORKSPACE: &str = "ws-rebuild-cli";

fn task(id: &str, status: TaskStatus) -> Task {
    let mut task = Task::new(
        id,
        TaskType::DagNode,
        serde_json::json!({ "goal": "cli probe" }),
    );
    task.status = status;
    task
}

/// 跑一次命令，返回它印在 stdout 上的那几行。
fn run_rebuild(args: &[&str]) -> String {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cog-migrate"))
        .arg("--database-url")
        .arg(database_url())
        .arg("--migrations-dir")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        // 迁移先干跑一遍：这条命令关心的是投影，迁移的应用过程不是它要判的东西，
        // 而干跑不连库、也不改任何东西。
        .arg("--dry-run")
        .arg("--rebuild-task-projection")
        .args(args)
        .output()
        .expect("run cog-migrate");
    assert!(
        output.status.success(),
        "the command must succeed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_command_restores_the_projection_and_reports_its_size() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("clear what an earlier round left");

    for (id, status) in [
        ("t-cli-one", TaskStatus::Pending),
        ("t-cli-two", TaskStatus::Running),
        ("t-cli-done", TaskStatus::Completed),
    ] {
        backend
            .dag_set_task(WORKSPACE, id, &task(id, status))
            .await
            .expect("write a task");
    }
    sqlx::query("DELETE FROM task_current WHERE workspace_id = $1")
        .bind(WORKSPACE)
        .execute(&pool)
        .await
        .expect("lose the projection");
    assert!(common::task_projection_ids(&pool, WORKSPACE)
        .await
        .is_empty());

    // 只重建一件：另一件仍然是缺席的。`--task-id` 窄的是范围，不是判据。
    let out = run_rebuild(&["--task-id", "t-cli-one"]);
    assert!(
        out.contains("task_current rows after rebuild:"),
        "the command must say what the projection now holds: {out}"
    );
    assert_eq!(
        common::task_projection_ids(&pool, WORKSPACE).await,
        vec!["t-cli-one"],
        "a rebuild for one task writes only that task"
    );

    // 不带 `--task-id`：整个投影从账本重建，终态的那一件不进投影。
    let out = run_rebuild(&[]);
    let reported: u64 = out
        .lines()
        .find_map(|line| line.strip_prefix("task_current rows after rebuild: "))
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or_else(|| panic!("the printed count must be a number: {out}"));
    let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM task_current")
        .fetch_one(&pool)
        .await
        .expect("count the projection");
    assert_eq!(
        reported as i64, actual,
        "the number the command prints must be the table's size: {out}"
    );
    assert_eq!(
        common::task_projection_ids(&pool, WORKSPACE).await,
        vec!["t-cli-one", "t-cli-two"],
        "every task that can still move is back, and the finished one stays out"
    );

    cog_test_support::clear_task_ledger_for(&pool, WORKSPACE)
        .await
        .expect("leave the ledger as it was found");
}
