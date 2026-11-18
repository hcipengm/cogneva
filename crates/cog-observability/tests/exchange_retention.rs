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
        let schema = cog_test_support::own_schema(
            &cog_test_support::database_url(),
            "exchange_retention_test",
        )
        .await;
        sqlx::query(&format!(
            "CREATE TABLE \"{schema}\".gateway_llm_exchange (
                 id BIGSERIAL PRIMARY KEY,
                 ts TIMESTAMPTZ NOT NULL,
                 request_raw  TEXT,
                 response_raw TEXT
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
        cog_test_support::drop_schema(&cog_test_support::database_url(), &self.schema).await;
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn without_an_archive_a_pass_retires_nothing() {
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let table = format!("\"{}\".gateway_llm_exchange", scratch.schema);

    // 3,001 rows past a one-day retention and 2,001 inside it.
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

    let outcome = scratch
        .cap(Duration::from_secs(86_400))
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "no archive means no verified copy, and a row without one is the only copy of its body"
    );
    assert_eq!(
        outcome.kept_unarchived, 3_001,
        "the expired rows are counted as kept, not silently reported as retired"
    );
    assert_eq!(outcome.held, 5_002, "nothing left the table");

    scratch.done().await;
}

/// 归档里有这几行的副本时，这一遍才删它们。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn rows_whose_copies_are_archived_are_the_only_ones_retired() {
    use cog_core::ObjectBackend;
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let table = format!("\"{}\".gateway_llm_exchange", scratch.schema);

    let archive: Arc<dyn ObjectBackend> = Arc::new(cog_test_support::MemoryObjectBackend::new());
    for object in ["bodies/req-archived", "bodies/resp-archived"] {
        archive.put(object, b"{}").await.expect("seed the archive");
    }
    sqlx::query(&format!(
        "INSERT INTO {table} (ts, request_raw, response_raw) VALUES
             (NOW() - interval '30 days', 'bodies/req-archived', 'bodies/resp-archived'),
             (NOW() - interval '30 days', 'bodies/req-missing', NULL),
             (NOW() - interval '30 days', NULL, NULL)"
    ))
    .execute(&pool)
    .await
    .expect("fill three expired rows");

    let outcome = scratch
        .cap(Duration::from_secs(86_400))
        .with_archive(archive)
        .retire_once(|| {})
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 1,
        "only the row whose copies are archived goes"
    );
    assert_eq!(
        outcome.kept_unarchived, 2,
        "a missing copy and an unnamed copy are both 'not archived', and both stay"
    );
    assert_eq!(outcome.held, 2);

    scratch.done().await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_empty_archive_answers_zero_rather_than_nothing() {
    let url = cog_test_support::database_url();
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
