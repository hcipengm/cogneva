//! The token ledger is measured, never trimmed, against a live PostgreSQL.
//!
//! The ledger is the source the hourly fold is a derivation of, so the claims
//! worth a live server are the ones a row count alone cannot check:
//!
//! - the reading is the ledger's own row count and the age of its oldest row,
//!   however far past the declared window that age is;
//! - a row past the window is still there after a pass -- nothing deletes, and
//!   that is the point: the only safe cutoff was "past the retention **and**
//!   inside what the fold already summed", which is a judgement about another
//!   table rather than a retention;
//! - rows below the fold's floor are reported as uncovered, with the age of the
//!   oldest: that pair says which spans a spend question can only be answered
//!   from the detail rows;
//! - with no fold at all every row is uncovered and the reading says so, because
//!   a pass that answered "nothing to report" there would be indistinguishable
//!   from a clean ledger.
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
async fn a_row_past_the_window_is_measured_and_kept() {
    let (scratch, pool) = scratch("keeps").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;
    scratch.row(&pool, ChronoDuration::days(1)).await;
    scratch.folded_window(&pool, ChronoDuration::days(45)).await;

    let reading = scratch
        .sweep(&pool, KEEP)
        .measure_once()
        .await
        .expect("measure");

    assert_eq!(reading.held, 2, "the reading is the row count: {reading:?}");
    assert!(
        reading.oldest_age_secs >= 40 * 86_400 - 60,
        "the reading says how far back the ledger reaches: {reading:?}"
    );
    assert_eq!(
        scratch.rows(&pool).await,
        2,
        "a row past the window was removed; nothing here deletes"
    );
    scratch.done().await;
}
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn rows_below_the_folds_floor_are_reported_as_uncovered() {
    let (scratch, pool) = scratch("uncovered").await;
    scratch.row(&pool, ChronoDuration::days(40)).await;
    scratch.row(&pool, ChronoDuration::days(2)).await;
    // The fold only reaches back two days, so both rows sit below its floor.
    scratch.folded_window(&pool, ChronoDuration::days(2)).await;

    let reading = scratch
        .sweep(&pool, KEEP)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.uncovered, 2, "{reading:?}");
    assert!(
        reading.uncovered_oldest_age_secs >= 40 * 86_400 - 60,
        "the uncovered pair has to carry the oldest one's age: {reading:?}"
    );
    assert_eq!(scratch.rows(&pool).await, 2);
    scratch.done().await;
}
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn with_no_fold_every_row_is_uncovered_and_none_is_removed() {
    let (scratch, pool) = scratch("no_fold").await;
    scratch.rows_many(&pool, 5, ChronoDuration::days(10)).await;

    let reading = scratch
        .sweep(&pool, KEEP)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(
        reading.uncovered, 5,
        "no folded window, so every row is: {reading:?}"
    );
    assert_eq!(scratch.rows(&pool).await, 5);
    scratch.done().await;
}
/// The declared window is recomputed from the measured daily write, and the
/// reading keeps saying which input decided it.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_declared_window_follows_the_measured_write_volume() {
    let (scratch, pool) = scratch("volume").await;
    scratch.rows_many(&pool, 20, ChronoDuration::days(10)).await;
    scratch.folded_window(&pool, ChronoDuration::days(10)).await;

    let sized = scratch
        .sweep(&pool, KEEP)
        .with_database_volume(Some(10 * 1024u64.pow(3)));
    let reading = sized.measure_once().await.expect("measure");
    assert!(reading.keep_secs > 0, "{reading:?}");
    assert!(
        reading.keep_derived,
        "a declared volume and a measured write rate is a derived window: {reading:?}"
    );
    assert_eq!(scratch.rows(&pool).await, 20);
    scratch.done().await;
}
