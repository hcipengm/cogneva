//! What the token ledger's retention deletes, and the one thing it may never do.
//!
//! The ledger is the source the hourly fold is a derivation of, so the claims
//! worth a live server are the ones a row count cannot check:
//!
//! - a row past the retention whose window the fold already summed goes;
//! - a row below the fold's floor does not, however old it is -- deleting it
//!   would destroy the only copy of spend that no window ever counted, and the
//!   fold would report the hour as quiet forever;
//! - with no fold at all nothing is deletable, and every row is reported as
//!   uncovered, because a sweep that answered "nothing to do" there would be
//!   indistinguishable from a clean ledger;
//! - the sweep follows the fold down: once the backfill lands the window a row
//!   sits in, the same pass retires it without anything else being told.
//!
//! Each case builds its own schema and drops it. The tables keep their real
//! names and are reached through the connection's search path, so the sweep runs
//! the statements it runs in production and the shared database's own ledger is
//! never in reach.
//!
//! All of it needs a server, so all of it is ignored by default:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-observability --test usage_retention -- --ignored
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use cog_observability::usage_retention::UsageRetention;
use sqlx::{PgPool, Row};

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool_with};

/// One case's own schema, holding the ledger and the fold.
struct Scratch {
    schema: String,
}

impl Scratch {
    async fn new(case: &str) -> Self {
        let schema = own_schema(&database_url(), &format!("probe_usage_{case}")).await;
        Self { schema }
    }

    /// A pool whose search path is this case's schema alone, so the sweep's
    /// unqualified table names land there and nowhere else.
    async fn pool(&self) -> PgPool {
        schema_pool_with(&database_url(), &self.schema, 2).await
    }

    /// One call in the ledger, `age` before now.
    async fn row(&self, pool: &PgPool, age: ChronoDuration) {
        sqlx::query(
            "INSERT INTO gateway_llm_usage (id, ts, upstream, api_style, model, result) \
             VALUES (gen_random_uuid(), $1, 'probe', 'openai', 'probe-model', 'ok')",
        )
        .bind(Utc::now() - age)
        .execute(pool)
        .await
        .expect("insert a scratch ledger row");
    }

    /// `n` calls in one statement, `age` before now. One round trip per row
    /// would make the batch-boundary case the slowest test in the suite for a
    /// reason that has nothing to do with what it checks.
    async fn rows_many(&self, pool: &PgPool, n: i64, age: ChronoDuration) {
        sqlx::query(
            "INSERT INTO gateway_llm_usage (id, ts, upstream, api_style, model, result)
             SELECT gen_random_uuid(), $1, 'probe', 'openai', 'probe-model', 'ok'
               FROM generate_series(1, $2)",
        )
        .bind(Utc::now() - age)
        .bind(n)
        .execute(pool)
        .await
        .expect("insert scratch ledger rows");
    }

    /// A folded window starting `age` before now. One row is enough to say the
    /// fold reached that hour: the sweep asks where the fold starts, not what
    /// each window summed.
    async fn folded_window(&self, pool: &PgPool, age: ChronoDuration) {
        sqlx::query(
            "INSERT INTO cog_llm_usage_rollup
                 (window_start, window_end, actor, origin, upstream, api_style)
             VALUES ($1, $2, 'probe', 'unknown', 'probe', 'openai')",
        )
        .bind(Utc::now() - age)
        .bind(Utc::now() - age + ChronoDuration::hours(1))
        .execute(pool)
        .await
        .expect("insert a scratch folded window");
    }

    async fn rows(&self, pool: &PgPool) -> i64 {
        sqlx::query("SELECT count(*) AS n FROM gateway_llm_usage")
            .fetch_one(pool)
            .await
            .expect("count the scratch ledger")
            .get::<i64, _>("n")
    }

    fn sweep(&self, pool: &PgPool, keep: Duration) -> UsageRetention {
        UsageRetention::new(pool.clone(), keep)
    }

    async fn done(&self) {
        drop_schema(&database_url(), &self.schema).await;
    }
}

/// The store's schema, applied to a schema of the case's own.
///
/// The same bytes the store runs at startup rather than a hand-written pair of
/// tables: a subset is the fixture answering its own question, and the sweep's
/// statements name columns a subset would not have.
async fn scratch(case: &str) -> (Scratch, PgPool) {
    let scratch = Scratch::new(case).await;
    let pool = scratch.pool().await;
    sqlx::raw_sql(&cog_observability::usage_store::ddl())
        .execute(&pool)
        .await
        .expect("apply the store's schema to the case's schema");
    (scratch, pool)
}

const KEEP: Duration = Duration::from_secs(30 * 86_400);

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_past_the_retention_inside_the_fold_is_retired() {
    let (scratch, pool) = scratch("frees").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;
    scratch.row(&pool, ChronoDuration::days(1)).await;
    // The fold reaches back past both rows.
    scratch.folded_window(&pool, ChronoDuration::days(45)).await;
    scratch.folded_window(&pool, ChronoDuration::days(1)).await;

    let outcome = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 1, "the row past the retention goes");
    assert_eq!(outcome.held, 1);
    assert_eq!(
        outcome.uncovered, 0,
        "both rows were inside what the fold answered for"
    );
    assert_eq!(outcome.keep_secs, 30 * 86_400);
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_below_the_fold_floor_is_never_retired() {
    let (scratch, pool) = scratch("floor").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;
    // The fold only starts ten days back, so the row above is the only copy of
    // the spend it carries.
    scratch.folded_window(&pool, ChronoDuration::days(10)).await;

    let outcome = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "a row no window summed is not the sweep's to delete, however old"
    );
    assert_eq!(scratch.rows(&pool).await, 1);
    assert_eq!(
        outcome.uncovered, 1,
        "and the pass has to say so: a deletion that reported nothing to do \
         would read the same as a clean ledger"
    );
    assert!(
        outcome.uncovered_oldest_age_secs >= 40 * 86_400,
        "the age of what cannot be retired is what tells a fold catching up by \
         an hour from a retention that has stopped: {}",
        outcome.uncovered_oldest_age_secs
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn with_no_fold_at_all_nothing_is_deletable() {
    let (scratch, pool) = scratch("nofold").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;

    let outcome = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 0);
    assert_eq!(outcome.uncovered, 1);
    assert_eq!(scratch.rows(&pool).await, 1);
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_sweep_follows_the_fold_down_without_being_told() {
    let (scratch, pool) = scratch("follow").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;
    scratch.folded_window(&pool, ChronoDuration::days(10)).await;

    let first = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("first");
    assert_eq!((first.removed, first.uncovered), (0, 1));

    // The backfill lands the window the row sits in. Nothing tells the sweep;
    // it reads the fold's own floor every pass, which is what makes the two
    // halves one loop rather than two things that have to be kept in step.
    scratch.folded_window(&pool, ChronoDuration::days(41)).await;

    let second = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("second");
    assert_eq!(second.removed, 1);
    assert_eq!(second.uncovered, 0);
    assert_eq!(scratch.rows(&pool).await, 0);
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_second_pass_finds_nothing_and_an_empty_ledger_answers_zero() {
    let (scratch, pool) = scratch("second").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;
    scratch.folded_window(&pool, ChronoDuration::days(45)).await;

    let first = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("first");
    assert_eq!(first.removed, 1);
    let second = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {})
        .await
        .expect("second");
    assert_eq!(second.removed, 0);
    assert_eq!(
        (second.held, second.oldest_age_secs, second.uncovered),
        (0, 0, 0),
        "an empty ledger answers zero rather than leaving the series absent: a \
         gap in the reading and a clean ledger would look the same"
    );
    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_pass_beats_between_batches() {
    let (scratch, pool) = scratch("batches").await;
    scratch.folded_window(&pool, ChronoDuration::days(45)).await;
    // More than one batch's worth past the retention, so the pass needs a
    // second statement to finish and a third to find nothing.
    scratch
        .rows_many(&pool, 2500, ChronoDuration::days(40))
        .await;
    let calls = AtomicUsize::new(0);
    let outcome = scratch
        .sweep(&pool, KEEP)
        .retire_once(|| {
            calls.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("sweep");

    assert_eq!(outcome.removed, 2500);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "progress fires before every batch, so a first pass over a grown backlog \
         reads as work on the event log rather than as a stopped loop"
    );
    scratch.done().await;
}
