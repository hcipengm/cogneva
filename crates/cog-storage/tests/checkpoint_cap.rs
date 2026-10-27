//! What the checkpoint sweep deletes, and the two things it may never delete.
//!
//! `cog_snapshots` grows because nothing removes the final checkpoint of a
//! finished task and nothing retires the board that pointed at it. The sweep
//! answers that, and the claims worth a live server are the ones a reading of
//! the count cannot check:
//!
//! - past the capacity, the oldest unreferenced rows go first;
//! - a row a board field names stays, however old and however far over the
//!   capacity it is — it is the resume point, and deleting it leaves the board
//!   pointing at an id that no longer exists;
//! - a row younger than the read window stays, because the snapshots view
//!   promises that window and "no snapshots" reads exactly like "there never
//!   were any";
//! - when the pointers and the window together hold more than the capacity
//!   allows, the pass says so rather than reporting the capacity met.
//!
//! All of it needs a server, so all of it is ignored by default:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test checkpoint_cap -- --ignored
//! ```
//!
//! Each case builds its own schema and drops it at the end. The tables keep
//! their real names and are reached through the connection's search path, so
//! the sweep is never pointed at a renamed table and the shared database's own
//! `cog_snapshots` is never in reach.

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use cog_storage::CheckpointCap;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// One case's own schema, holding the two tables the sweep reads.
struct Scratch {
    schema: String,
}

impl Scratch {
    async fn new(case: &str) -> Self {
        let schema = format!("probe_checkpoint_cap_{case}");
        let setup = PgPool::connect(&database_url())
            .await
            .expect("connect to the test database");
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&setup)
            .await
            .expect("clear a schema left by an earlier run");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&setup)
            .await
            .expect("create the case's schema");
        setup.close().await;
        Self { schema }
    }

    /// A pool whose search path is this case's schema alone, so the store's
    /// unqualified table names land there and nowhere else.
    fn pool(&self) -> PgPool {
        let options = PgConnectOptions::from_str(&database_url())
            .expect("COGNEVA_TEST_DATABASE_URL is not a PostgreSQL URL")
            .options([("search_path", self.schema.as_str())]);
        PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy_with(options)
    }

    /// Put one snapshot `age` before now.
    async fn snapshot(&self, pool: &PgPool, id: &str, age: ChronoDuration) {
        sqlx::query(
            "INSERT INTO cog_snapshots (snapshot_id, task_id, timestamp) VALUES ($1, $2, $3)",
        )
        .bind(id)
        .bind(format!("task-{id}"))
        .bind(Utc::now() - age)
        .execute(pool)
        .await
        .expect("insert a scratch snapshot");
    }

    /// Point a task's board at a snapshot the way the checkpoint round does.
    async fn point_at(&self, pool: &PgPool, task: &str, snapshot_id: &str) {
        sqlx::query(
            "INSERT INTO cog_context_boards (task_id, field_name, field_value) \
             VALUES ($1, $2, $3)",
        )
        .bind(task)
        .bind("checkpoint_snapshot")
        .bind(snapshot_id)
        .execute(pool)
        .await
        .expect("insert a scratch board");
    }

    async fn ids(&self, pool: &PgPool) -> Vec<String> {
        sqlx::query("SELECT snapshot_id FROM cog_snapshots ORDER BY snapshot_id")
            .fetch_all(pool)
            .await
            .expect("list the scratch snapshots")
            .into_iter()
            .map(|row| row.get::<String, _>("snapshot_id"))
            .collect()
    }

    fn cap(&self, pool: &PgPool, budget: u64, window_secs: u64) -> CheckpointCap {
        CheckpointCap::new(pool.clone(), budget).with_read_window(Duration::from_secs(window_secs))
    }

    async fn done(&self) {
        let pool = PgPool::connect(&database_url())
            .await
            .expect("connect to the test database");
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .execute(&pool)
            .await
            .expect("drop the case's schema");
        pool.close().await;
    }
}

/// The two tables the sweep reads, in the case's schema, shaped like the real
/// ones in the columns it touches.
async fn scratch(case: &str) -> (Scratch, PgPool) {
    let scratch = Scratch::new(case).await;
    let pool = scratch.pool();
    // Two statements rather than one string: sqlx prepares what it is given,
    // and a prepared statement carries exactly one command.
    for ddl in [
        "CREATE TABLE cog_snapshots (
             snapshot_id TEXT PRIMARY KEY,
             task_id     TEXT NOT NULL,
             timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW()
         )",
        "CREATE TABLE cog_context_boards (
             task_id     TEXT NOT NULL,
             field_name  TEXT NOT NULL,
             field_value TEXT NOT NULL,
             PRIMARY KEY (task_id, field_name)
         )",
    ] {
        sqlx::query(ddl)
            .execute(&pool)
            .await
            .expect("create the case's tables");
    }
    (scratch, pool)
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn unreferenced_rows_past_the_window_go_oldest_first() {
    let (scratch, pool) = scratch("oldest_first").await;
    for n in 0..5 {
        scratch
            .snapshot(&pool, &format!("old-{n}"), ChronoDuration::hours(3 + n))
            .await;
    }
    scratch
        .snapshot(&pool, "recent", ChronoDuration::minutes(30))
        .await;

    let outcome = scratch
        .cap(&pool, 2, 3_600)
        .sweep_once()
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 4, "four rows have to go to reach two");
    assert_eq!(outcome.held, 2);
    assert!(!outcome.held_above, "the capacity was reachable");
    // Four of the five old rows go, oldest first: what survives is the
    // youngest of them, plus the row inside the window that was never a
    // candidate.
    assert_eq!(
        scratch.ids(&pool).await,
        vec!["old-0".to_string(), "recent".to_string()],
        "the oldest go first, and the row inside the window is not a candidate at all"
    );

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_snapshot_a_board_names_survives_at_any_age() {
    let (scratch, pool) = scratch("board_pointer").await;
    // The oldest row of all is a board pointer: age does not protect it, and
    // age does not condemn it.
    scratch
        .snapshot(&pool, "resume", ChronoDuration::days(90))
        .await;
    for n in 0..4 {
        scratch
            .snapshot(&pool, &format!("garbage-{n}"), ChronoDuration::days(10 + n))
            .await;
    }
    scratch.point_at(&pool, "task-resume", "resume").await;

    let outcome = scratch
        .cap(&pool, 1, 3_600)
        .sweep_once()
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 4,
        "only the unreferenced rows are deletable"
    );
    assert_eq!(outcome.held, 1);
    assert!(
        !outcome.held_above,
        "one row at the capacity is not a table held over it"
    );
    assert_eq!(
        scratch.ids(&pool).await,
        vec!["resume".to_string()],
        "the resume point must survive the oldest-first order"
    );

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_snapshot_inside_the_window_survives_even_unreferenced() {
    let (scratch, pool) = scratch("window").await;
    scratch
        .snapshot(&pool, "just-outside", ChronoDuration::seconds(3_700))
        .await;
    scratch
        .snapshot(&pool, "just-inside", ChronoDuration::seconds(3_500))
        .await;

    let outcome = scratch
        .cap(&pool, 1, 3_600)
        .sweep_once()
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 1, "only the row outside the window goes");
    assert_eq!(
        scratch.ids(&pool).await,
        vec!["just-inside".to_string()],
        "the window is the floor: not one row inside it may go"
    );

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_store_the_pointers_and_the_window_hold_over_capacity_says_so() {
    let (scratch, pool) = scratch("held_above").await;
    // Five unreferenced rows inside the window, all of them over a capacity of
    // one. Reaching the capacity would mean deleting rows a reader may still
    // ask for, so the pass has to stop and say so.
    for n in 0..5 {
        scratch
            .snapshot(
                &pool,
                &format!("recent-{n}"),
                ChronoDuration::minutes(10 + n),
            )
            .await;
    }

    let outcome = scratch
        .cap(&pool, 1, 3_600)
        .sweep_once()
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 0, "nothing inside the window is deletable");
    assert_eq!(outcome.held, 5);
    assert!(
        outcome.held_above,
        "a table the floor holds over budget must say so, not report the capacity met"
    );
    assert_eq!(scratch.ids(&pool).await.len(), 5);

    scratch.done().await;
}

/// A pass that needs more than one batch reports progress between them.
///
/// The loop stamps its own beat from that callback, so a pass that is working
/// keeps its published age at one batch while a batch wedged on a lock still
/// shows. Without a beat between batches the age of a pass over a store far
/// over its capacity is the length of the whole pass, and that length is
/// whatever the surplus is — the number of batches grows with it, so the loop
/// can never declare a wall that covers it.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_pass_that_needs_more_than_one_batch_reports_progress_between_them() {
    let (scratch, pool) = scratch("progress").await;
    // 12,001 rows against a capacity of one, all of them outside the read
    // window: 12,000 deletions in batches of 5,000 is three batches.
    sqlx::query(
        "INSERT INTO cog_snapshots (snapshot_id, task_id, timestamp) \
         SELECT 'row-' || g, 'task-' || g, NOW() - (g || ' minutes')::interval \
         FROM generate_series(1, 12001) AS g",
    )
    .execute(&pool)
    .await
    .expect("fill the scratch store");

    let beats = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&beats);
    let outcome = scratch
        .cap(&pool, 1, 1)
        .sweep_once_beating(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 12_000, "the whole surplus goes");
    assert_eq!(
        beats.load(Ordering::Relaxed),
        3,
        "12,000 rows in batches of 5,000 is three batches, and every one of them \
         has to report progress: the loop beats its heartbeat from this callback"
    );

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_pass_with_pruning_off_deletes_nothing() {
    let (scratch, pool) = scratch("pruning_off").await;
    for n in 0..3 {
        scratch
            .snapshot(&pool, &format!("old-{n}"), ChronoDuration::days(30 + n))
            .await;
    }

    let outcome = scratch
        .cap(&pool, 0, 3_600)
        .sweep_once()
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "a zero capacity is how a deployment opts out"
    );
    assert_eq!(
        outcome.budget, None,
        "and the reading must not claim a budget it was never given"
    );
    assert!(!outcome.held_above);
    assert_eq!(scratch.ids(&pool).await.len(), 3);

    scratch.done().await;
}

/// The startup schema has to survive the rows the boards already hold.
///
/// A field value is an opaque string, and real boards carry long ones; a btree
/// index keeps the value itself in every entry and refuses anything past a
/// third of a page. When that refusal lands in `init_schema` the whole gateway
/// refuses to start, and the rollout of that revision fails and rolls back --
/// measured 2026-10-27 on a live table whose longest board value was 11,456
/// bytes against a 2,704-byte ceiling.
///
/// The sweep only ever asks `field_value = <snapshot id>`, so the index that
/// question needs is a hash index: it stores the hash rather than the value and
/// has no length ceiling.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_startup_schema_holds_board_values_past_a_btree_entry() {
    let scratch = Scratch::new("long_board_value").await;
    let pool = scratch.pool();
    for ddl in [
        "CREATE TABLE cog_snapshots (
             snapshot_id TEXT PRIMARY KEY,
             task_id     TEXT NOT NULL,
             timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW()
         )",
        "CREATE TABLE cog_context_boards (
             task_id     TEXT NOT NULL,
             field_name  TEXT NOT NULL,
             field_value TEXT NOT NULL,
             updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
             PRIMARY KEY (task_id, field_name)
         )",
    ] {
        sqlx::query(ddl)
            .execute(&pool)
            .await
            .expect("scratch table");
    }
    // A value longer than a btree entry may be, already in the table before the
    // startup schema runs -- which is the state a rollout finds itself in.
    let long = "v".repeat(11_456);
    sqlx::query(
        "INSERT INTO cog_context_boards (task_id, field_name, field_value) \
         VALUES ($1, $2, $3)",
    )
    .bind("task-long")
    .bind("blob")
    .bind(&long)
    .execute(&pool)
    .await
    .expect("insert a long board value");

    sqlx::raw_sql(cog_storage::postgres::state_backend::DDL)
        .execute(&pool)
        .await
        .expect("the startup schema must apply over rows a board already holds");

    // And the index has to be one the equality question can use.
    let method: String = sqlx::query(
        "SELECT am.amname FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam \
         WHERE c.relname = 'idx_cog_context_boards_field_value'",
    )
    .fetch_one(&pool)
    .await
    .expect("the sweep's index exists after the schema runs")
    .get("amname");
    assert_eq!(
        method, "hash",
        "the sweep asks field_value = <id>; a btree here refuses the long values"
    );

    scratch.done().await;
}
