//! The prompt-and-response archive is measured, never trimmed, against a live
//! PostgreSQL.
//!
//! What used to answer the archive's growth was a retention: past N days, delete
//! the rows whose bodies read back from the archive. That condition was right
//! about the risk and wrong about the mechanism -- it made a minute loop ask
//! "would deleting this lose the only copy", a question about a deletion that
//! should not be happening at all. The claims worth a live server now:
//!
//! - the reading is the table's own row count and the age of its oldest row,
//!   however far past the declared window that age is;
//! - a row past the window is still there after a pass, with or without a copy
//!   in the archive -- nothing deletes;
//! - an archive with nothing in it answers zero for its oldest age rather than
//!   an error. A series that disappeared would make the age rule go silent
//!   exactly while the archive is clean, so the empty case is a reading.
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
            // 与真表同形状：写入口的 INSERT 点的是这些列，少一列就不是同一个问题。
            "CREATE TABLE \"{schema}\".gateway_llm_exchange (
                 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                 ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 upstream TEXT NOT NULL DEFAULT 'test-upstream',
                 api_style TEXT NOT NULL DEFAULT 'openai',
                 model TEXT NOT NULL DEFAULT 'm',
                 actor TEXT NOT NULL DEFAULT 'a',
                 origin TEXT NOT NULL DEFAULT 'gateway_pool',
                 task_id TEXT,
                 result TEXT NOT NULL DEFAULT 'ok',
                 latency_ms BIGINT NOT NULL DEFAULT 0,
                 request_sha256 TEXT NOT NULL DEFAULT '',
                 response_sha256 TEXT,
                 request_bytes BIGINT NOT NULL DEFAULT 0,
                 response_seen_bytes BIGINT NOT NULL DEFAULT 0,
                 response_bytes BIGINT,
                 request_raw  TEXT,
                 response_raw TEXT,
                 request_body TEXT,
                 response_body TEXT
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
async fn rows_past_the_window_are_measured_and_kept() {
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let table = scratch.table();
    sqlx::query(&format!(
        "INSERT INTO {table} (ts) VALUES
             (NOW() - interval '30 days'), (NOW() - interval '1 day')"
    ))
    .execute(&pool)
    .await
    .expect("fill two rows, one past the window");

    let reading = scratch
        .cap(Duration::from_secs(86_400))
        .measure_once()
        .await
        .expect("measure");

    assert_eq!(reading.held, 2, "the reading is the row count: {reading:?}");
    assert!(
        reading.oldest_age_secs >= 30 * 86_400 - 60,
        "the reading says how far back the archive reaches: {reading:?}"
    );
    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&pool)
        .await
        .expect("count the rows");
    assert_eq!(
        left, 2,
        "a row past the window was removed; nothing here deletes"
    );
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_copy_in_the_archive_does_not_change_what_is_kept() {
    use cog_core::ObjectBackend;
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let table = scratch.table();
    let archive: Arc<dyn ObjectBackend> = Arc::new(cog_test_support::MemoryObjectBackend::new());
    archive.put("bodies/req", b"{}").await.expect("seed");
    sqlx::query(&format!(
        "INSERT INTO {table} (ts, request_raw) VALUES (NOW() - interval '30 days', 'bodies/req')"
    ))
    .execute(&pool)
    .await
    .expect("fill one expired row");

    let reading = scratch
        .cap(Duration::from_secs(86_400))
        .with_archive(archive)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.held, 1);
    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&pool)
        .await
        .expect("count the rows");
    assert_eq!(
        left, 1,
        "an archived copy used to be the licence to delete; it is now only a fact"
    );
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_empty_archive_answers_zero_rather_than_nothing() {
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let reading = scratch
        .cap(Duration::from_secs(86_400))
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.held, 0);
    assert_eq!(
        reading.oldest_age_secs, 0,
        "an empty archive is a reading, not an error"
    );
}
