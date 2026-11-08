//! What workspace a stored task says it belongs to, against a live PostgreSQL.
//!
//! A task used to name its workspace twice: in the `workspace_id` column and in
//! the `workspace_id` field of the JSON payload. The column decided which rows a
//! read returned; the field decided what a reader that trusted the payload
//! thought it had. Rows written before the field was filled carried the column
//! and not the field, and on 2026-11-06 the default workspace held 9,920 rows of
//! which 9,878 had an empty field -- so an authenticated reader was told the
//! platform had 42 tasks while the same query without a declared workspace
//! returned 9,920. Both numbers called themselves "the task total".
//!
//! The ledger answers it once. A task's workspace is a column on its definition
//! revision, and every read builds the task from that column -- there is no
//! second copy that can disagree, and no legacy population that can be missing
//! it.
//!
//! What is left to check is that the single answer is the one that was asked
//! for, on every read path, and that a write cannot put a task in one workspace
//! and have it read back from another. A DB-free test can see neither: the
//! answer is the query's.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test a_task_read_back_knows_its_workspace -- --ignored
//! ```
//!
//! It clears its own workspaces first, so the database is left as it was found.

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

const WORKSPACES: [&str; 3] = ["ws-legacy", "ws-fresh", "ws-declared"];

fn task(id: &str, venue: Option<&str>) -> Task {
    let mut task = Task::new(id.to_string(), TaskType::DagNode, serde_json::json!({}));
    task.venue = venue.map(str::to_string);
    task.status = TaskStatus::Pending;
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_task_knows_its_workspace_on_the_way_out_and_on_the_way_in() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    for workspace in WORKSPACES {
        cog_test_support::clear_task_ledger_for(&pool, workspace)
            .await
            .expect("clear this test's workspaces");
    }

    for (id, venue) in [("plain", None), ("venue", Some("memory"))] {
        backend
            .dag_set_task("ws-legacy", id, &task(id, venue))
            .await
            .expect("write a task");
    }

    let read = backend
        .dag_get_task("ws-legacy", "plain")
        .await
        .expect("read the task")
        .expect("the task is there");
    assert_eq!(
        read.workspace_id.as_deref(),
        Some("ws-legacy"),
        "一条写在 ws-legacy 下的任务，读回来必须自称属于 ws-legacy"
    );

    let all = backend
        .dag_get_all_tasks("ws-legacy")
        .await
        .expect("read every task");
    assert_eq!(all.len(), 2, "两件任务都在这个工作区里，两个都该读回来");
    assert!(
        all.iter()
            .all(|t| t.workspace_id.as_deref() == Some("ws-legacy")),
        "整批读回来时每一行都要带上它的工作区，不能只有单行读带：{:?}",
        all.iter().map(|t| &t.workspace_id).collect::<Vec<_>>()
    );

    let venue = backend
        .dag_get_venue_tasks("ws-legacy")
        .await
        .expect("read the venue rows");
    assert!(
        venue
            .iter()
            .all(|t| t.workspace_id.as_deref() == Some("ws-legacy")),
        "场地那一读也是同一个后端，工作区要从同一列来"
    );

    // 一件自称属于另一个工作区的任务，写进去之后属于**写进来的那个**工作区。
    // 这不是「覆盖」一格的改写，而是账本里工作区只有一处：写的时候说了放哪，
    // 它就是哪儿的。之前正文与列各回答一次，两处不一致时读的人各看到一边。
    let mut declared = task("declared", None);
    declared.workspace_id = Some("ws-declared".to_string());
    backend
        .dag_set_task("ws-fresh", "declared", &declared)
        .await
        .expect("write a task that names another workspace");

    let read = backend
        .dag_get_task("ws-fresh", "declared")
        .await
        .expect("read the task")
        .expect("the task is there");
    assert_eq!(
        read.workspace_id.as_deref(),
        Some("ws-fresh"),
        "工作区只有一处答案：写进来时说的那个"
    );
    assert!(
        backend
            .dag_get_task("ws-declared", "declared")
            .await
            .expect("read the other workspace")
            .is_none(),
        "正文里那一格不是第二份真值：按它去读必须是空的"
    );

    for workspace in WORKSPACES {
        cog_test_support::clear_task_ledger_for(&pool, workspace)
            .await
            .expect("leave the ledger as it was found");
    }
}
