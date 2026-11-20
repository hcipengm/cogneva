//! What the task-event archive reports, and that it retires nothing.
//!
//! `cog_events` is the only record of what a run saw. Both readers replay a
//! task's events from offset zero, so a retired stream answers a well-formed
//! read with a gap that looks exactly like the start of the run — and nothing
//! in the reply would say otherwise. The loop therefore measures: rows, streams,
//! and the age of the oldest row. What to do about growth is a decision made
//! against those readings, not a clock.
//!
//! The claim worth a live server is the one a reading of the count cannot
//! check: after a pass over an archive whose oldest rows are far past any
//! retention this platform ever declared, every row is still there.
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test task_event_retention -- --ignored
//! ```
//!
//! Each case builds its own schema and drops it at the end. The tables keep
//! their real names and are reached through the connection's search path, so
//! the sweep is never pointed at a renamed table and the shared database's own
//! `cog_events` is never in reach.

use chrono::{Duration as ChronoDuration, Utc};
use cog_storage::TaskEventRetention;
use sqlx::{PgPool, Row};

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool_with};

/// One case's own schema, holding the two tables the sweep reads.
struct Scratch {
    schema: String,
}

impl Scratch {
    async fn new(case: &str) -> Self {
        let schema = own_schema(&database_url(), &format!("probe_task_events_{case}")).await;
        Self { schema }
    }

    /// A pool whose search path is this case's schema alone, so the sweep's
    /// unqualified table names land there and nowhere else.
    async fn pool(&self) -> PgPool {
        schema_pool_with(&database_url(), &self.schema, 2).await
    }

    /// Append one event to a task's stream, `age` before now.
    async fn event(&self, pool: &PgPool, task: &str, offset: i64, age: ChronoDuration) {
        sqlx::query(
            "INSERT INTO cog_events (task_id, event_type, payload, offset_num, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(task)
        .bind("ralph_iteration")
        .bind(serde_json::json!({ "iteration": offset }))
        .bind(offset)
        .bind(Utc::now() - age)
        .execute(pool)
        .await
        .expect("insert a scratch event");
    }

    /// Record a resume point the way a checkpoint round does.
    async fn checkpoint(&self, pool: &PgPool, task: &str, event_offset: i64) {
        sqlx::query(
            "INSERT INTO cog_checkpoints (task_id, snapshot_id, event_offset) \
             VALUES ($1, $2, $3)",
        )
        .bind(task)
        .bind(format!("snap-{task}"))
        .bind(event_offset)
        .execute(pool)
        .await
        .expect("insert a scratch checkpoint");
    }

    async fn tasks(&self, pool: &PgPool) -> Vec<String> {
        sqlx::query("SELECT DISTINCT task_id FROM cog_events ORDER BY task_id")
            .fetch_all(pool)
            .await
            .expect("list the scratch streams")
            .into_iter()
            .map(|row| row.get::<String, _>("task_id"))
            .collect()
    }

    async fn rows_of(&self, pool: &PgPool, task: &str) -> i64 {
        sqlx::query("SELECT count(*) AS n FROM cog_events WHERE task_id = $1")
            .bind(task)
            .fetch_one(pool)
            .await
            .expect("count a scratch stream")
            .get::<i64, _>("n")
    }

    fn sweep(&self, pool: &PgPool) -> TaskEventRetention {
        TaskEventRetention::new(pool.clone())
    }

    async fn done(&self) {
        drop_schema(&database_url(), &self.schema).await;
    }
}

/// The store's schema, applied to a schema of the case's own.
///
/// The same bytes the store runs at startup rather than a hand-written subset:
/// a subset is the fixture answering its own question, and the one claim that
/// depends on it -- that the candidate query has an index to read -- would then
/// be a statement about this file.
async fn scratch(case: &str) -> (Scratch, PgPool) {
    let scratch = Scratch::new(case).await;
    let pool = scratch.pool().await;
    // The store's own schema, not a copy of it: a hand-written pair of tables
    // would let this suite pass against an index production never declares, and
    // the sweep's candidate query is only cheap because that index exists.
    sqlx::raw_sql(&cog_storage::postgres::state_backend::ddl())
        .execute(&pool)
        .await
        .expect("apply the store's schema to the case's schema");
    (scratch, pool)
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn an_aged_archive_is_measured_and_retires_nothing() {
    let (scratch, pool) = scratch("nothing_retired").await;

    // 一条六十天前就停了的流、一条最近还在写的流，以及一条被检查点指着的流。
    for offset in 0..3 {
        scratch
            .event(&pool, "quiet", offset, ChronoDuration::days(60))
            .await;
    }
    scratch
        .event(&pool, "live", 10, ChronoDuration::days(1))
        .await;
    scratch
        .event(&pool, "pinned", 20, ChronoDuration::days(90))
        .await;
    scratch.checkpoint(&pool, "pinned", 20).await;

    let outcome = scratch
        .sweep(&pool)
        .measure_once()
        .await
        .expect("measure the scratch archive");

    assert_eq!(outcome.held, 5, "五条事件都还在：这条循环不删任何东西");
    assert_eq!(outcome.tasks, 3, "三条流都还在");
    assert!(
        outcome.oldest_age_secs >= 89 * 86_400,
        "最老一条的年龄要报出来，否则「涨得慢」与「根本不涨」在读数上同形"
    );
    for task in scratch.tasks(&pool).await {
        let rows = scratch.rows_of(&pool, &task).await;
        let expected = if task == "quiet" { 3 } else { 1 };
        assert_eq!(rows, expected, "流 {task} 的行被删了：这条循环不许删行");
    }

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn an_empty_archive_reports_zero_rather_than_an_absence() {
    let (scratch, pool) = scratch("empty_archive").await;
    let outcome = scratch
        .sweep(&pool)
        .measure_once()
        .await
        .expect("measure an empty archive");
    assert_eq!(outcome.held, 0);
    assert_eq!(outcome.tasks, 0);
    assert_eq!(
        outcome.oldest_age_secs, 0,
        "空档案的年龄是零，不是「读不到」：两条读数在这里要分得开"
    );
    scratch.done().await;
}
