//! The checkpoint store is measured, never trimmed, against a live PostgreSQL.
//!
//! `cog_snapshots` grows because nothing removes the final checkpoint of a
//! finished task and nothing retires the board that pointed at it. What used to
//! answer that was a sweep: past a row capacity, delete the oldest rows no board
//! names. What answers it now is a reading, and the claims worth a live server
//! are the ones a counting query alone cannot check:
//!
//! - the row count published is the table's own, boards and all;
//! - past the declared capacity the reading says so and **removes nothing** --
//!   not the oldest unreferenced row either;
//! - a row a board field names is still there afterwards, and now for a stronger
//!   reason than the old floor: nothing in this loop deletes at all;
//! - a deployment that declares neither a capacity nor a volume has a target of
//!   none rather than a target of zero.
//!
//! All of it needs a server, so all of it is ignored by default:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test checkpoint_cap -- --ignored
//! ```
//!
//! Each case builds its own schema and drops it at the end. The tables keep
//! their real names and are reached through the connection's search path, so the
//! loop is never pointed at a renamed table and the shared database's own
//! `cog_snapshots` is never in reach.

use chrono::{Duration as ChronoDuration, Utc};
use cog_storage::CheckpointCap;
use sqlx::{PgPool, Row};

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool_with};

/// One case's own schema, holding the two tables the loop reads.
struct Scratch {
    schema: String,
}

impl Scratch {
    async fn new(case: &str) -> Self {
        let schema = own_schema(&database_url(), &format!("probe_checkpoint_cap_{case}")).await;
        Self { schema }
    }

    /// A pool whose search path is this case's schema alone, so the store's
    /// unqualified table names land there and nowhere else.
    async fn pool(&self) -> PgPool {
        schema_pool_with(&database_url(), &self.schema, 2).await
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

    fn cap(&self, pool: &PgPool, budget: u64) -> CheckpointCap {
        CheckpointCap::new(pool.clone(), budget)
    }

    async fn done(&self) {
        drop_schema(&database_url(), &self.schema).await;
    }
}

/// The two tables the sweep reads, in the case's schema, shaped like the real
/// ones in the columns it touches.
async fn scratch(case: &str) -> (Scratch, PgPool) {
    let scratch = Scratch::new(case).await;
    let pool = scratch.pool().await;
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
async fn the_reading_is_the_row_count_the_table_has_now() {
    let (scratch, pool) = scratch("reads_rows").await;
    for n in 0..5 {
        scratch
            .snapshot(&pool, &format!("snap-{n}"), ChronoDuration::hours(3 + n))
            .await;
    }

    let reading = scratch
        .cap(&pool, 40_000)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.held, 5, "the reading is the row count: {reading:?}");
    assert_eq!(reading.target, Some(40_000));
    assert!(!reading.over_capacity);
    assert!(
        reading.bytes.is_some_and(|bytes| bytes > 0),
        "the size on disk is a reading and this table has rows: {reading:?}"
    );

    scratch.done().await;
}

/// Past the declared capacity the reading says so — and nothing is removed.
///
/// The oldest rows here are exactly the ones the old sweep would have taken
/// first: unreferenced, well past the capacity, old enough that no read window
/// would have protected them either.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_store_past_its_capacity_reads_over_and_keeps_every_row() {
    let (scratch, pool) = scratch("over_capacity").await;
    for n in 0..6 {
        scratch
            .snapshot(&pool, &format!("old-{n}"), ChronoDuration::days(30 + n))
            .await;
    }

    let reading = scratch.cap(&pool, 2).measure_once().await.expect("measure");
    assert_eq!(reading.held, 6);
    assert_eq!(reading.target, Some(2));
    assert!(
        reading.over_capacity,
        "6 rows against a declared 2: {reading:?}"
    );
    assert_eq!(
        scratch.ids(&pool).await.len(),
        6,
        "the pass removed rows it was only supposed to count"
    );

    scratch.done().await;
}

/// A row a board points at survives, and now nothing could remove it.
///
/// The old sweep kept such a row with a floor: a `NOT EXISTS` clause in the
/// delete. The invariant is stronger now in the only way that matters — there is
/// no delete to carry a clause.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_snapshot_a_board_names_survives_at_any_age() {
    let (scratch, pool) = scratch("board_pointer").await;
    scratch
        .snapshot(&pool, "resume", ChronoDuration::days(90))
        .await;
    scratch.point_at(&pool, "task-resume", "resume").await;
    for n in 0..3 {
        scratch
            .snapshot(&pool, &format!("loose-{n}"), ChronoDuration::hours(n + 1))
            .await;
    }

    let reading = scratch.cap(&pool, 1).measure_once().await.expect("measure");
    assert!(reading.over_capacity, "four rows against a declared one");
    assert_eq!(
        scratch.ids(&pool).await,
        vec![
            "loose-0".to_string(),
            "loose-1".to_string(),
            "loose-2".to_string(),
            "resume".to_string()
        ],
        "the resume point is still there, and so is everything else"
    );

    scratch.done().await;
}

/// A deployment that declares neither a capacity nor a volume has no target.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn no_declaration_is_no_target_rather_than_a_target_of_zero() {
    let (scratch, pool) = scratch("no_declaration").await;
    scratch
        .snapshot(&pool, "only", ChronoDuration::minutes(5))
        .await;

    let reading = scratch.cap(&pool, 0).measure_once().await.expect("measure");
    assert_eq!(reading.target, None, "{reading:?}");
    assert!(!reading.over_capacity);

    scratch.done().await;
}

/// A declared volume turns the target into a derived one, and says so.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_declared_volume_makes_the_target_a_derived_reading() {
    let (scratch, pool) = scratch("volume").await;
    for n in 0..4 {
        scratch
            .snapshot(&pool, &format!("snap-{n}"), ChronoDuration::hours(n + 1))
            .await;
    }

    let reading = scratch
        .cap(&pool, 40_000)
        .with_database_volume(Some(8 * 1024 * 1024 * 1024))
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(
        reading.target_source,
        cog_core::retention::SOURCE_DERIVED,
        "a volume and a measured cost per row is a derived target: {reading:?}"
    );
    assert!(reading.target.is_some());

    scratch.done().await;
}
