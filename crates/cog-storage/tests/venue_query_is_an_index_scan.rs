//! Whether the venue board's read is a lookup, and whether the migration that
//! adds it can run on a database this store never built.
//!
//! Two things can be wrong here without any reading of the board changing, so
//! both are checked against a live PostgreSQL.
//!
//! The first is the estimate. The venue label lives in the task's JSON, so the
//! index over it is an expression index and `ANALYZE` is the only source of
//! statistics the planner has for it. A *partial* index -- one that covers only
//! the labeled rows, which is the natural way to keep it small -- has its
//! statistics gathered from the rows it covers, so its null fraction is zero
//! and the planner reads that as "nearly every row has a label". Its estimate
//! on a table shaped like the live one was 9,891 of 9,991 rows, and against
//! that estimate the index loses to scanning the table every time; the measured
//! plan was a sequential scan of 106 ms to return four rows. Dropping the
//! predicate fixed the estimate and the same statement became an index scan of
//! 0.13 ms. Nothing about the board's answer changes between the two, which is
//! why only the plan can tell them apart.
//!
//! The second is the fresh install. `cog_dag_tasks` is created by this store's
//! own schema, which runs *after* the migrations, so on a fresh install the
//! migration finds no table to index. An unguarded `CREATE INDEX` there fails,
//! and the runner does not skip a failed migration and carry on: it stops, so
//! every migration after it would be blocked as well. The migration is
//! therefore guarded on the table's existence, and the index is declared beside
//! the table's own `CREATE` so a fresh install still gets it.
//!
//! Ignored by default, and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test venue_query_is_an_index_scan -- --ignored
//! ```

use serde_json::Value;
use sqlx::{Connection, Executor, PgConnection, PgPool};

use cog_core::{StateBackend, Task, TaskStatus, TaskType};
use cog_storage::{PostgresStateBackend, VENUE_TASKS_SQL};

use cog_test_support::database_url;

/// The rows this binary writes. One workspace of its own, so a neighbour
/// running the other storage suites against the same database cannot move the
/// statistics this reading is taken from.
const PROBE_WORKSPACE: &str = "venue-index-probe";

/// Unlabeled rows around the three that carry a venue. The live table is
/// 9,987 unlabeled to 4 labeled (measured 2026-11-06); the ratio is what makes
/// the estimate wrong, and a handful of rows has no estimate worth reading.
const UNLABELED: i64 = 1_997;

const UPS: &str = include_str!("../migrations/postgres/093_cog_dag_tasks_venue_index.up.sql");

/// The plan of the statement the store reads venues with, built from the
/// store's own text: a copy written here would go on passing after the store's
/// stopped being indexable.
async fn plan_of_the_venue_read() -> Value {
    // A prepared statement does not outlive its session, and a session of its
    // own keeps the pool's connections out of this.
    let mut conn = PgConnection::connect(&database_url()).await.unwrap();
    let prepared = format!("PREPARE probe_venue(text) AS {VENUE_TASKS_SQL}");
    conn.execute(prepared.as_str()).await.unwrap();
    let explained = format!("EXPLAIN (FORMAT JSON) EXECUTE probe_venue('{PROBE_WORKSPACE}')");
    sqlx::query_scalar::<_, Value>(explained.as_str())
        .fetch_one(&mut conn)
        .await
        .unwrap()
}

/// Every node of a plan, subplans included.
fn nodes(plan: &Value) -> Vec<&Value> {
    let mut found = Vec::new();
    fn walk<'a>(node: &'a Value, out: &mut Vec<&'a Value>) {
        out.push(node);
        if let Some(children) = node.get("Plans").and_then(Value::as_array) {
            for child in children {
                walk(child, out);
            }
        }
    }
    walk(plan, &mut found);
    found
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_venue_read_is_a_lookup_and_returns_the_labeled_rows() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.expect("create the task schema");
    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(PROBE_WORKSPACE)
        .execute(&pool)
        .await
        .expect("clear this probe's rows");

    sqlx::query(
        "INSERT INTO cog_dag_tasks (workspace_id, task_id, task, status) \
         SELECT $1, 'p' || g, \
                jsonb_build_object('id', 'p' || g, 'task_type', 'DagNode', \
                                   'status', 'Pending', \
                                   'payload', jsonb_build_object('body', repeat('x', 4000))), \
                'Pending' \
         FROM generate_series(1, $2) g",
    )
    .bind(PROBE_WORKSPACE)
    .bind(UNLABELED)
    .execute(&pool)
    .await
    .expect("write the unlabeled rows");
    // The labeled rows go in through the store, not as hand-built JSON: these
    // are the rows the read has to deserialize, and a field missing from a
    // literal here would be this test's bug rather than the store's.
    for (id, venue, status) in [
        ("v-done", "memory", TaskStatus::Completed),
        ("v-running", "memory", TaskStatus::Running),
        ("v-failed", "wiki", TaskStatus::Failed),
    ] {
        let mut task = Task::new(id, TaskType::DagNode, serde_json::json!({}));
        task.venue = Some(venue.to_string());
        task.status = status;
        backend
            .dag_set_task(PROBE_WORKSPACE, id, &task)
            .await
            .expect("write a labeled row");
    }
    sqlx::query("ANALYZE cog_dag_tasks")
        .execute(&pool)
        .await
        .expect("refresh the statistics the plan is chosen from");

    let plan = plan_of_the_venue_read().await;
    let plan = &plan[0]["Plan"];
    let kinds: Vec<&str> = nodes(plan)
        .iter()
        .filter_map(|n| n.get("Node Type").and_then(Value::as_str))
        .collect();
    assert!(
        kinds
            .iter()
            .any(|k| *k == "Index Scan" || *k == "Bitmap Index Scan"),
        "带标签的行必须靠索引取，不能扫整表：{kinds:?}"
    );
    assert!(
        nodes(plan).iter().any(|n| {
            n.get("Index Name").and_then(Value::as_str) == Some("idx_cog_dag_tasks_venue")
        }),
        "取行用的必须是这条语句对应的那个索引：{kinds:?}"
    );

    let mut read: Vec<String> = backend
        .dag_get_venue_tasks(PROBE_WORKSPACE)
        .await
        .expect("read the venue rows")
        .into_iter()
        .map(|task| task.id)
        .collect();
    read.sort();
    assert_eq!(read, vec!["v-done", "v-failed", "v-running"]);

    sqlx::query("DELETE FROM cog_dag_tasks WHERE workspace_id = $1")
        .bind(PROBE_WORKSPACE)
        .execute(&pool)
        .await
        .expect("leave the database as it was found");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_index_migration_survives_a_database_that_has_no_task_table_yet() {
    // A schema of its own, because the question is what the migration does when
    // `cog_dag_tasks` is not on its search path -- running it against the real
    // database would only ever exercise the branch where the table is there.
    let mut conn = PgConnection::connect(&database_url())
        .await
        .expect("connect to the test database");
    conn.execute("DROP SCHEMA IF EXISTS venue_mig_probe CASCADE")
        .await
        .expect("clear the probe schema");
    conn.execute("CREATE SCHEMA venue_mig_probe")
        .await
        .expect("create the probe schema");
    conn.execute("SET search_path TO venue_mig_probe")
        .await
        .expect("aim the session at the probe schema");

    // The fresh-install path: no table, so nothing to index -- and no error,
    // because a failed migration stops the runner and everything after it.
    conn.execute(UPS)
        .await
        .expect("the migration must not fail when the table it indexes is not there yet");
    let index: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('idx_cog_dag_tasks_venue')::text")
            .fetch_one(&mut conn)
            .await
            .expect("ask whether the index exists");
    assert_eq!(index, None, "没有表就不该凭空建出索引");

    // The upgrade path: the table is there, so the index lands on it.
    conn.execute("CREATE TABLE cog_dag_tasks (workspace_id TEXT, task JSONB)")
        .await
        .expect("create a table for the migration to index");
    conn.execute(UPS)
        .await
        .expect("the migration applies once the table exists");
    let index: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('idx_cog_dag_tasks_venue')::text")
            .fetch_one(&mut conn)
            .await
            .expect("ask whether the index exists");
    assert_eq!(
        index.as_deref(),
        Some("idx_cog_dag_tasks_venue"),
        "表在的时候迁移必须把索引建出来"
    );

    conn.execute("DROP SCHEMA venue_mig_probe CASCADE")
        .await
        .expect("leave nothing behind");
}
