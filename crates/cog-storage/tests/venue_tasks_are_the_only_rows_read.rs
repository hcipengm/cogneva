//! Which rows the venue board is handed, against a live PostgreSQL.
//!
//! The board folds tasks by their venue label, and this backend no longer hands
//! it the whole graph: `dag_get_venue_tasks` excludes the unlabeled rows in the
//! query. Two things can go wrong that no reading of the board would show.
//!
//! The first is emptiness. Most of a long-running graph carries no label -- on
//! 2026-11-06, 9,987 of 9,991 rows -- so the filter is doing nearly all of the
//! work, and a predicate that is off by a trim (`venue` written as `"  "`)
//! quietly adds a venue with no name to the board, or drops one that has one.
//!
//! The second is the terminal rows. The board's "latest verdict" and "written
//! back" columns are read from tasks that are already completed, failed or
//! cancelled. A reader that reused the dispatch exclusion
//! (`TaskStatus::affects_dispatch`, which drops failed and cancelled) would
//! return rows and both columns would still be empty -- and an empty column
//! reads exactly like a venue where nothing has finished yet.
//!
//! A DB-free test cannot see either: the trim is the database's `btrim` and the
//! row set is the query's. It is ignored by default and needs
//! `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test venue_tasks_are_the_only_rows_read -- --ignored
//! ```
//!
//! It clears the task table first, so the database is left as it was found.

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::PostgresStateBackend;
use sqlx::PgPool;

use cog_test_support::database_url;

fn task(id: &str, venue: Option<&str>, status: TaskStatus) -> Task {
    let mut task = Task::new(id.to_string(), TaskType::DagNode, serde_json::json!({}));
    task.venue = venue.map(str::to_string);
    task.status = status;
    task
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_board_is_handed_the_labeled_rows_and_every_one_of_them() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    sqlx::query("DELETE FROM cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("clear the task table");

    let seeded = [
        ("v-running", Some("memory"), TaskStatus::Running),
        // 已经做完的行：看板的「最近一条判据」与「写回量」两列只从这里来。
        ("v-done", Some("memory"), TaskStatus::Completed),
        ("v-failed", Some("wiki"), TaskStatus::Failed),
        // 空格串不是场地：它若被算成一栏，看板上就多一栏没有名字的。
        ("v-blank", Some("   "), TaskStatus::Pending),
        ("v-none", None, TaskStatus::Completed),
        ("v-untagged-running", None, TaskStatus::Running),
    ];
    for (id, venue, status) in seeded {
        backend
            .dag_set_task("ws-venue", id, &task(id, venue, status))
            .await
            .expect("write a task");
    }
    // 另一个 workspace：这一读按 workspace 取行，跨 workspace 的行不该出现。
    backend
        .dag_set_task(
            "ws-other",
            "v-elsewhere",
            &task("v-elsewhere", Some("eval"), TaskStatus::Running),
        )
        .await
        .expect("write a task in the other workspace");

    let mut read: Vec<(String, String)> = backend
        .dag_get_venue_tasks("ws-venue")
        .await
        .expect("read the venue rows")
        .into_iter()
        .map(|task| {
            let venue = task.venue_label().unwrap_or("").to_string();
            (task.id, venue)
        })
        .collect();
    read.sort();

    let expected = vec![
        ("v-done".to_string(), "memory".to_string()),
        ("v-failed".to_string(), "wiki".to_string()),
        ("v-running".to_string(), "memory".to_string()),
    ];
    assert_eq!(
        read, expected,
        "带标签的行要一条不少（含 completed / failed），不带标签的和别的 workspace 的一条不多"
    );

    // 与「取全图再筛」逐行相同。查询换了位置，答案不能换：两边不一致的那天，
    // 表现为看板某一栏的条数与任务页数出来的不一样，而两边各自都说得通。
    let mut from_the_whole_graph: Vec<(String, String)> = backend
        .dag_get_all_tasks("ws-venue")
        .await
        .expect("read the whole graph")
        .into_iter()
        .filter(|task| task.venue_label().is_some())
        .map(|task| {
            let venue = task.venue_label().unwrap_or("").to_string();
            (task.id, venue)
        })
        .collect();
    from_the_whole_graph.sort();
    assert_eq!(read, from_the_whole_graph);
}
