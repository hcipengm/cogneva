//! What the task-event retention deletes, and the two things it may never do.
//!
//! `cog_events` grows because nothing has ever removed a row from it, and the
//! claims worth a live server are the ones a reading of the count cannot check:
//!
//! - a stream quiet past the retention goes in full, so no reader ever sees the
//!   tail of a run without its head;
//! - a stream that has an event newer than the retention stays in full, even the
//!   events inside it that are older than the retention — the alternative is a
//!   prefix deletion, which both readers would read as the start of the run;
//! - a stream a checkpoint names stays, however old it is: the checkpoint is the
//!   resume point, and a sequence whose offsets restarted while the checkpoint
//!   still points past the reset loses events without an error;
//! - the candidate query is answered from the timestamp index, because a sweep
//!   that scans the whole archive every two minutes is the cost this retention
//!   exists to stop paying.
//!
//! All of it needs a server, so all of it is ignored by default:
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

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use cog_storage::TaskEventRetention;
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
        // The schema name carries the process and the case, so two runs of this
        // suite do not drop each other's tables mid-test.
        let schema = format!("probe_task_events_{case}_{}", std::process::id());
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

    /// A pool whose search path is this case's schema alone, so the sweep's
    /// unqualified table names land there and nowhere else.
    fn pool(&self) -> PgPool {
        let options = PgConnectOptions::from_str(&database_url())
            .expect("COGNEVA_TEST_DATABASE_URL is not a PostgreSQL URL")
            .options([("search_path", self.schema.as_str())]);
        PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy_with(options)
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

    fn sweep(&self, pool: &PgPool, keep: Option<Duration>) -> TaskEventRetention {
        TaskEventRetention::new(pool.clone(), keep)
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

/// The store's schema, applied to a schema of the case's own.
///
/// The same bytes the store runs at startup rather than a hand-written subset:
/// a subset is the fixture answering its own question, and the one claim that
/// depends on it -- that the candidate query has an index to read -- would then
/// be a statement about this file.
async fn scratch(case: &str) -> (Scratch, PgPool) {
    let scratch = Scratch::new(case).await;
    let pool = scratch.pool();
    // The store's own schema, not a copy of it: a hand-written pair of tables
    // would let this suite pass against an index production never declares, and
    // the sweep's candidate query is only cheap because that index exists.
    sqlx::raw_sql(cog_storage::postgres::state_backend::DDL)
        .execute(&pool)
        .await
        .expect("apply the store's schema to the case's schema");
    (scratch, pool)
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_stream_quiet_past_the_retention_is_retired_whole() {
    let (scratch, pool) = scratch("whole").await;
    // A run that concluded 40 days ago: three iterations, all of them old.
    for offset in 1..=3 {
        scratch
            .event(&pool, "finished", offset, ChronoDuration::days(40 + offset))
            .await;
    }
    // A run that started 40 days ago and appended again a minute ago: its old
    // events are older than the retention and still must survive, because a
    // reader replaying it from zero would otherwise begin mid-run.
    scratch
        .event(&pool, "running", 1, ChronoDuration::days(41))
        .await;
    scratch
        .event(&pool, "running", 2, ChronoDuration::minutes(1))
        .await;

    let outcome = scratch
        .sweep(&pool, Some(Duration::from_secs(30 * 86_400)))
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 3, "the quiet stream goes in full");
    assert_eq!(scratch.tasks(&pool).await, vec!["running".to_string()]);
    assert_eq!(
        scratch.rows_of(&pool, "running").await,
        2,
        "the live stream keeps the event that is older than the retention: \
         retirement works on streams, not on rows"
    );
    assert_eq!(outcome.held, 2);
    assert_eq!(outcome.tasks, 1);
    assert_eq!(outcome.keep_secs, Some(30 * 86_400));
    assert!(
        outcome.oldest_age_secs >= 40 * 86_400,
        "the surviving stream still reaches back past the retention, and the \
         reading says so instead of hiding it: {}",
        outcome.oldest_age_secs
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_stream_a_checkpoint_names_is_not_retired() {
    let (scratch, pool) = scratch("checkpoint").await;
    scratch
        .event(&pool, "resumable", 1, ChronoDuration::days(60))
        .await;
    scratch
        .event(&pool, "forgotten", 1, ChronoDuration::days(60))
        .await;
    scratch.checkpoint(&pool, "resumable", 1).await;

    let outcome = scratch
        .sweep(&pool, Some(Duration::from_secs(30 * 86_400)))
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 1);
    assert_eq!(
        scratch.tasks(&pool).await,
        vec!["resumable".to_string()],
        "the checkpoint is the resume point, so the stream it names is not the \
         sweep's to delete however old it is"
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_second_pass_finds_nothing_and_an_empty_archive_answers_zero() {
    let (scratch, pool) = scratch("second").await;
    scratch
        .event(&pool, "old", 1, ChronoDuration::days(45))
        .await;

    let keep = Some(Duration::from_secs(30 * 86_400));
    let first = scratch
        .sweep(&pool, keep)
        .retire_once(|| {})
        .await
        .expect("first");
    assert_eq!(first.removed, 1);
    let second = scratch
        .sweep(&pool, keep)
        .retire_once(|| {})
        .await
        .expect("second");
    assert_eq!(second.removed, 0);
    assert_eq!(
        (second.held, second.tasks, second.oldest_age_secs),
        (0, 0, 0),
        "an empty archive answers zero rather than leaving the series absent: \
         a gap in the reading and a clean archive would look the same"
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_pass_beats_between_batches() {
    let (scratch, pool) = scratch("batches").await;
    // One more stream than a batch holds, so the pass needs a second look to
    // find the last one and a third to find nothing.
    for n in 0..=500 {
        scratch
            .event(&pool, &format!("task-{n:04}"), 1, ChronoDuration::days(40))
            .await;
    }
    let calls = AtomicUsize::new(0);
    let outcome = scratch
        .sweep(&pool, Some(Duration::from_secs(30 * 86_400)))
        .retire_once(|| {
            calls.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 501);
    assert_eq!(outcome.held, 0);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "progress fires before every batch, so a first pass over a grown backlog \
         reads as work on the event log rather than as a stopped loop"
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_deployment_that_retires_nothing_still_publishes_the_size() {
    let (scratch, pool) = scratch("measure").await;
    scratch
        .event(&pool, "old", 1, ChronoDuration::days(45))
        .await;

    let outcome = scratch
        .sweep(&pool, None)
        .retire_once(|| {})
        .await
        .expect("pass");

    assert_eq!(outcome.removed, 0);
    assert_eq!((outcome.held, outcome.tasks), (1, 1));
    assert_eq!(
        outcome.keep_secs, None,
        "no retention is published, so the age comparison has nothing to be \
         measured against and stays silent"
    );
    assert!(
        outcome.oldest_age_secs >= 44 * 86_400,
        "the age is still published: this is the deployment whose operator needs \
         to see the size it is not bounding"
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_candidate_query_is_answered_from_the_timestamp_index() {
    let (scratch, pool) = scratch("plan").await;
    for n in 0..50 {
        scratch
            .event(&pool, &format!("task-{n:04}"), 1, ChronoDuration::days(40))
            .await;
    }
    // Another fifty streams inside the retention, so the query cannot be
    // answered by concluding the whole table is old.
    for n in 0..50 {
        scratch
            .event(
                &pool,
                &format!("live-{n:04}"),
                1,
                ChronoDuration::minutes(5),
            )
            .await;
    }
    sqlx::query("ANALYZE cog_events")
        .execute(&pool)
        .await
        .expect("analyze");

    // What the store declares: an index on the pair, in that order, so both
    // halves of the candidate question -- the old rows and the new ones -- are
    // ranges over it and the task ids come out of the index rather than out of
    // the heap.
    // Scoped to this case's schema: the catalogue is shared, and an index of the
    // same name left by another schema would answer for this one.
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 \
         WHERE c.relname = 'idx_cog_events_created_at_task' \
           AND n.nspname = current_schema() \
         ORDER BY a.attnum",
    )
    .fetch_all(&pool)
    .await
    .expect("the sweep's index is one the store declares");
    assert_eq!(
        columns,
        vec!["created_at".to_string(), "task_id".to_string()],
        "the sweep's candidate query is a range over the timestamps answered \
         with task ids; any other column order answers neither half"
    );

    // And the planner can answer the question from it. Sequential scans are
    // turned off for this one statement only: at a hundred rows a scan is
    // genuinely cheaper, so the question here is whether the index can answer
    // the predicate at all, and the sizing that decides preference belongs to
    // the archive rather than to a fixture.
    // One connection, because the setting is per session: a pool would be free
    // to answer the EXPLAIN on the connection that never saw the SET.
    let mut conn = pool.acquire().await.expect("a connection of its own");
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await
        .expect("discourage the scan for this session");
    let plan = sqlx::query(
        "EXPLAIN (FORMAT JSON) SELECT DISTINCT task_id FROM cog_events WHERE created_at < $1",
    )
    .bind(Utc::now() - ChronoDuration::days(30))
    .fetch_all(&mut *conn)
    .await
    .expect("explain the candidate selection")
    .into_iter()
    .map(|row| row.get::<serde_json::Value, _>("QUERY PLAN"))
    .collect::<Vec<_>>();
    drop(conn);
    let rendered = serde_json::to_string(&plan).expect("render the plan");
    assert!(
        rendered.contains("idx_cog_events_created_at_task"),
        "the candidate selection has to be answerable from the timestamp and \
         task id alone rather than from a walk over the archive: {rendered}"
    );
    scratch.done().await;
}
