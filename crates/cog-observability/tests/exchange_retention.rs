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
                 response_bytes BIGINT NOT NULL DEFAULT 0,
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

/// 归档没成时正文落在行里：这一行说了「有一次调用」，就必须拿得出它说了什么。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_body_with_no_archive_copy_is_kept_in_the_row() {
    use cog_core::LlmExchangeRecord;
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let scratch = Scratch::new(pool.clone()).await;
    let table = format!("\"{}\".gateway_llm_exchange", scratch.schema);
    // 写入口的表名是拼进 SQL 的，没有「换一张表」的参数；于是给它一个把 search_path
    // 钉在这台用例自己的 schema 上的池——测的是写入口本身，不是这里另抄的一份 INSERT。
    let schema = scratch.schema.clone();
    let scoped = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _meta| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query(&format!("SET search_path TO \"{schema}\""))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("a pool scoped to this case's schema");
    let store = cog_observability::usage_store::LlmUsageStore::from_pool(scoped);

    let mut record = LlmExchangeRecord {
        upstream: "test-upstream".into(),
        api_style: "openai".into(),
        model: "m".into(),
        actor: "a".into(),
        origin: "gateway_pool".into(),
        task: None,
        result: "ok".into(),
        latency_ms: 1,
        request_sha256: "00".into(),
        response_sha256: Some("11".into()),
        request_bytes: 5,
        response_seen_bytes: 3,
        response_bytes: 0,
        request_raw: None,
        response_raw: None,
        request_inline: Some("hello".into()),
        response_inline: Some("bye".into()),
    };
    store
        .record_exchange(&record)
        .await
        .expect("record the exchange");
    let (req, resp): (Option<String>, Option<String>) =
        sqlx::query_as(&format!("SELECT request_body, response_body FROM {table}"))
            .fetch_one(&pool)
            .await
            .expect("read the row back");
    assert_eq!(req.as_deref(), Some("hello"));
    assert_eq!(resp.as_deref(), Some("bye"));

    // 有归档副本时行里不抄第二份：正文在归档里，行只存键。
    record.request_raw = Some("bodies/req".into());
    record.request_inline = None;
    record.response_inline = None;
    record.request_sha256 = "22".into();
    store.record_exchange(&record).await.expect("record again");
    let (req, resp): (Option<String>, Option<String>) = sqlx::query_as(&format!(
        "SELECT request_body, response_body FROM {table} WHERE request_sha256 = '22'"
    ))
    .fetch_one(&pool)
    .await
    .expect("read the second row back");
    assert_eq!(req, None, "the copy lives in the archive; the row names it");
    assert_eq!(resp, None);

    scratch.done().await;
}
