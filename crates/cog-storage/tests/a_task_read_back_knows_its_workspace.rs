//! What workspace a stored task says it belongs to, against a live PostgreSQL.
//!
//! A task row names its workspace twice: once in the `workspace_id` column and
//! once in the `workspace_id` field of the JSON payload. The column decides
//! which rows a read returns; the field decides what a reader that trusts the
//! payload thinks it has.
//!
//! Rows written before the field was filled carry the column and not the field.
//! On 2026-11-06 the default workspace held 9,920 rows and 9,878 of them had an
//! empty field, and the gateway's visibility check reads the field -- so an
//! authenticated reader was told the platform had 42 tasks while the very same
//! query without a declared workspace returned 9,920. Both numbers called
//! themselves "the task total".
//!
//! Two halves have to hold and a DB-free test can see neither. A read must hand
//! back the workspace the row was stored under even when the payload predates
//! the field; and a write must fill the field, so no new row joins the legacy
//! population. The second half cannot be checked through the backend's own
//! reader -- the reader now fills the field, which would hide a write that does
//! not -- so it reads the stored JSON directly.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test a_task_read_back_knows_its_workspace -- --ignored
//! ```
//!
//! It clears the task table first, so the database is left as it was found.

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

fn task(id: &str, venue: Option<&str>) -> Task {
    let mut task = Task::new(id.to_string(), TaskType::DagNode, serde_json::json!({}));
    task.venue = venue.map(str::to_string);
    task.status = TaskStatus::Pending;
    task
}

/// 直接把一行按 2026-11-06 之前的写法塞进库里：列上有工作区，正文里没有那一格。
async fn insert_legacy_row(pool: &PgPool, workspace_id: &str, id: &str, venue: Option<&str>) {
    let value = serde_json::to_value(task(id, venue)).expect("serialise the task");
    assert!(
        !value.to_string().contains("workspace_id"),
        "the seeded row must not carry the field at all"
    );
    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status) VALUES ($1, $2, $3, $4)",
    )
    .bind(workspace_id)
    .bind(id)
    .bind(value)
    .bind("pending")
    .execute(pool)
    .await
    .expect("write a legacy row");
}

/// 一行在库里正文那一格的原文，绕开后端自己的读取。
async fn stored_field(pool: &PgPool, workspace_id: &str, id: &str) -> Option<String> {
    let (value,): (serde_json::Value,) =
        sqlx::query_as("SELECT task FROM cog_dag_tasks WHERE workspace_id = $1 AND task_id = $2")
            .bind(workspace_id)
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("read the stored row");
    value
        .get("workspace_id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// 两半写在一个用例里，因为它们共用一张表：两个用例会同时建表、同时清表，
/// 建表会撞 `pg_type` 的唯一键，清表会把对方刚写进去的行删掉。这两件事上一次
/// 都是真的，而报出来的是「schema 建不出来」，与要验的事毫无关系。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_task_knows_its_workspace_on_the_way_out_and_on_the_way_in() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("clear the task table");

    insert_legacy_row(&pool, "ws-legacy", "legacy-plain", None).await;
    insert_legacy_row(&pool, "ws-legacy", "legacy-venue", Some("memory")).await;

    let read = backend
        .dag_get_task("ws-legacy", "legacy-plain")
        .await
        .expect("read the legacy row")
        .expect("the row is there");
    assert_eq!(
        read.workspace_id.as_deref(),
        Some("ws-legacy"),
        "一条列上写着 ws-legacy 的行，读回来必须自称属于 ws-legacy；\
         正文里那一格空着时若不补，按正文判可见性的读者会把它当成别人的行"
    );

    let all = backend
        .dag_get_all_tasks("ws-legacy")
        .await
        .expect("read every task");
    assert_eq!(all.len(), 2, "两行都在这个工作区里，两个都该读回来");
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
        "场地那一读也是同一个后端，走的补法要一样"
    );

    backend
        .dag_set_task("ws-fresh", "fresh", &task("fresh", None))
        .await
        .expect("write a task");

    assert_eq!(
        stored_field(&pool, "ws-fresh", "fresh").await.as_deref(),
        Some("ws-fresh"),
        "写进来的行必须在正文里就带着工作区：只写列的话，下一次读它的人按正文判，\
         这一行就会被算成别人的——这一半用后端自己的读验证不出来，读的那一侧已经会补"
    );

    // 已经自己声明了工作区的行，写的时候不许被改写成列上的那个：补的是空格，不是覆盖。
    let mut declared = task("declared", None);
    declared.workspace_id = Some("ws-declared".to_string());
    backend
        .dag_set_task("ws-fresh", "declared", &declared)
        .await
        .expect("write a task that names its own workspace");
    assert_eq!(
        stored_field(&pool, "ws-fresh", "declared").await.as_deref(),
        Some("ws-declared"),
        "正文里已经写着的工作区不许被列上的值盖掉"
    );
}
