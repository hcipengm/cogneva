//! The prompt-and-response archive's retention, against a live PostgreSQL.
//!
//! Three claims, and all three are the server's answer rather than the caller's:
//!
//! - the cutoff is the clock, so a pass retires every row older than the
//!   retention and leaves the rest alone -- the count it deletes is decided by
//!   `ts < cutoff`, not by anything the loop passes down;
//! - a second pass over the same archive finds nothing, because correctness
//!   comes from the deletion being a set defined by the clock rather than from a
//!   single writer -- two replicas sweeping the same cutoff take the same rows;
//! - an archive with nothing in it answers zero for its oldest age rather than
//!   an error. A series that disappeared would make the age rule go silent
//!   exactly while the archive is clean, so the empty case is a reading.
//!
//! The pass reports progress before each batch, so a first run against an
//! archive that grew while nothing retired it does not leave the loop looking
//! stopped for as long as the backlog lasts. The counts below are what says the
//! callback fires per batch rather than once per pass.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database. The test owns a schema of its own and drops it, so the live
//! archive is never touched:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test exchange_retention -- --ignored
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use cog_observability::exchange_retention::ExchangeRetention;

/// A schema carrying this run's table, dropped when the test ends.
struct Scratch {
    pool: PgPool,
    schema: String,
}

impl Scratch {
    async fn new(pool: PgPool) -> Self {
        let schema = format!("exchange_retention_test_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
            .execute(&pool)
            .await
            .expect("a leftover schema from an earlier run must be removable");
        sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
            .execute(&pool)
            .await
            .expect("the scratch schema must be creatable");
        sqlx::query(&format!(
            "CREATE TABLE \"{schema}\".gateway_llm_exchange (
                 id BIGSERIAL PRIMARY KEY,
                 ts TIMESTAMPTZ NOT NULL
             )"
        ))
        .execute(&pool)
        .await
        .expect("the archive table must be creatable");
        Self { pool, schema }
    }

    fn table(&self) -> String {
        format!("{}.gateway_llm_exchange", self.schema)
    }

    fn cap(&self, keep: Duration) -> ExchangeRetention {
        ExchangeRetention::new(self.pool.clone(), keep).with_table(self.table())
    }

    async fn done(self) {
        sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            self.schema
        ))
        .execute(&self.pool)
        .await
        .expect("the scratch schema must be droppable");
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_pass_retires_every_row_past_the_retention_and_only_those() {
    let url = std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running this test with --ignored",
    );
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let table = format!("\"{}\".gateway_llm_exchange", scratch.schema);

    // 3,001 rows past a one-day retention and 2,001 inside it. The extra row on
    // each side is what makes the batch arithmetic below say "one more than the
    // batch size" rather than a round number that a single statement would
    // satisfy either way.
    sqlx::query(&format!(
        "INSERT INTO {table} (ts)
         SELECT NOW() - interval '30 days' - (g || ' seconds')::interval
         FROM generate_series(1, 3001) AS g"
    ))
    .execute(&pool)
    .await
    .expect("fill the expired half");
    sqlx::query(&format!(
        "INSERT INTO {table} (ts)
         SELECT NOW() - (g || ' seconds')::interval
         FROM generate_series(1, 2001) AS g"
    ))
    .execute(&pool)
    .await
    .expect("fill the live half");

    let beats = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&beats);
    let outcome = scratch
        .cap(Duration::from_secs(86_400))
        .retire_once(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 3_001,
        "every expired row goes, and no other"
    );
    assert_eq!(outcome.held, 2_001, "the rows inside the retention stay");
    assert_eq!(
        outcome.keep_secs, 86_400,
        "the pass reports what it enforced"
    );
    assert!(
        outcome.oldest_age_secs < 3_600,
        "the oldest surviving row has to be one of the ones inside the \
         retention, not the month-old half: {}",
        outcome.oldest_age_secs
    );
    assert_eq!(
        beats.load(Ordering::Relaxed),
        3,
        "two full batches and the empty one that ends the pass: the callback \
         fires per batch, not once per pass"
    );

    let again = scratch
        .cap(Duration::from_secs(86_400))
        .retire_once(|| {})
        .await
        .expect("second sweep");
    assert_eq!(
        again.removed, 0,
        "a second pass over the same cutoff finds nothing"
    );
    assert_eq!(again.held, 2_001);

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_empty_archive_answers_zero_rather_than_nothing() {
    let url = std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running this test with --ignored",
    );
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool).await;

    let outcome = scratch
        .cap(Duration::from_secs(86_400))
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(outcome.held, 0);
    assert_eq!(outcome.removed, 0);
    assert_eq!(
        outcome.oldest_age_secs, 0,
        "an empty archive is a clean one: the age rule compares this to the \
         retention and zero is what has to arrive"
    );

    scratch.done().await;
}
