//! The durable per-call token ledger, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that a recorded call
//! round-trips the dimensions a reader selects by -- the cache split, and the
//! estimate that has to sit beside the upstream's own count -- and that the
//! additive column migration lands on a table that already holds rows without
//! rewriting them, including the one column whose absence is a value.
//! Neither can be checked without a server -- the INSERT is a statement only
//! PostgreSQL parses, and "old rows survive and read a default" is a fact about
//! the server's ALTER, not about our code. A statement that can never run keeps
//! every DB-free test green, which is why this file is ignored by default and
//! needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-observability --test usage_store_ledger -- --ignored
//! ```
//!
//! It is one case rather than several because the two faces are ordered: the
//! migration face drops the column, and a second case running beside it would
//! be judging a schema the first one is mid-way through changing. The rows it
//! writes carry a per-run marker and are deleted at the end, so the database is
//! left as it was found.

use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore};
use sqlx::{PgPool, Row};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_metered_dimensions_reach_the_ledger_and_the_column_migration_spares_existing_rows() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    // Three markers, because the migration face leaves a row behind, the row
    // written afterwards has to be told apart from it, and a row that carries no
    // estimate is a third shape that must survive as its own.
    let before = format!("usage-ledger-test-before:{}", uuid::Uuid::new_v4());
    let after = format!("usage-ledger-test-after:{}", uuid::Uuid::new_v4());
    let unpriced = format!("usage-ledger-test-unpriced:{}", uuid::Uuid::new_v4());

    let record = |actor: String, cached: u64, estimated: Option<u64>| LlmUsageRecord {
        upstream: "ledger-test-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "ledger-test-model".to_string(),
        result: "ok".to_string(),
        actor,
        origin: "sandbox".to_string(),
        tokens_input: 1000,
        tokens_output: 50,
        tokens_cached: cached,
        estimated_input: estimated,
        latency_ms: 12,
    };

    store.init_schema().await.expect("init the schema");

    // First face: a call that the upstream served partly from cache reaches the
    // ledger with the cache split intact. Folding it into `tokens_input` would
    // pass every other assertion in this file and still lose the one number the
    // ledger exists to carry.
    store
        .record(&record(before.clone(), 800, Some(900)))
        .await
        .expect("record a cached call");
    let row = sqlx::query(
        "SELECT tokens_input, tokens_output, tokens_cached, estimated_input \
         FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&before)
    .fetch_one(&pool)
    .await
    .expect("read the recorded call back");
    assert_eq!(row.get::<i64, _>("tokens_input"), 1000);
    assert_eq!(row.get::<i64, _>("tokens_output"), 50);
    assert_eq!(
        row.get::<i64, _>("tokens_cached"),
        800,
        "the cache split must survive the write, or the hit rate is unrecoverable"
    );
    assert_eq!(
        row.get::<Option<i64>, _>("estimated_input"),
        Some(900),
        "the estimate has to land on the same row as the upstream's count, or the two \
         rulers can only be read one call at a time"
    );

    // Second face: the table as it exists in production -- populated, and
    // without the columns. Dropping them here is what the real upgrade path does
    // by simply not having them yet, so re-running init_schema is exercising the
    // ALTER branch rather than the CREATE branch. Both dimensions are dropped
    // together because their migrations differ in the one way a reader depends
    // on: the cache column comes back with a default, the estimate column must
    // not, since "this row was never priced" is not "this row was priced at 0".
    sqlx::query(
        "ALTER TABLE gateway_llm_usage DROP COLUMN tokens_cached, DROP COLUMN estimated_input",
    )
    .execute(&pool)
    .await
    .expect("drop the columns to reproduce the pre-migration table");
    store
        .init_schema()
        .await
        .expect("re-init against a table without the columns");

    let row = sqlx::query(
        "SELECT tokens_input, tokens_cached, estimated_input FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&before)
    .fetch_one(&pool)
    .await
    .expect("the row must survive the migration");
    assert_eq!(
        row.get::<i64, _>("tokens_input"),
        1000,
        "an additive migration must not rewrite the rows it lands beside"
    );
    assert_eq!(
        row.get::<i64, _>("tokens_cached"),
        0,
        "a row written before the column existed reads the default, not NULL"
    );
    assert_eq!(
        row.get::<Option<i64>, _>("estimated_input"),
        None,
        "a row written before the estimate column existed reads NULL, not 0 -- a zero \
         would be read as a request whose prompt was empty"
    );

    let shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'tokens_cached'",
    )
    .fetch_one(&pool)
    .await
    .expect("the column must exist after the migration");
    assert_eq!(shape.get::<String, _>("is_nullable"), "NO");
    assert!(
        shape
            .get::<Option<String>, _>("column_default")
            .is_some_and(|d| d.contains('0')),
        "the migrated column needs a default, or every later row has to carry a value it does not know"
    );
    let estimate_shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'estimated_input'",
    )
    .fetch_one(&pool)
    .await
    .expect("the estimate column must exist after the migration");
    assert_eq!(
        estimate_shape.get::<String, _>("is_nullable"),
        "YES",
        "the estimate column is the nullable one on purpose: it has an absence to carry"
    );
    assert!(
        estimate_shape
            .get::<Option<String>, _>("column_default")
            .is_none(),
        "a default here would erase the difference between unpriced and priced at it"
    );

    // And the columns the migration added are ones the writer actually fills --
    // including the estimate's own absence, which has to come back as an absence
    // rather than as the zero the reader would otherwise have to guess about.
    store
        .record(&record(after.clone(), 640, Some(512)))
        .await
        .expect("record after the migration");
    store
        .record(&record(unpriced.clone(), 0, None))
        .await
        .expect("record a call whose path prices nothing");
    let row = sqlx::query(
        "SELECT tokens_cached, estimated_input FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&after)
    .fetch_one(&pool)
    .await
    .expect("read the post-migration call back");
    assert_eq!(row.get::<i64, _>("tokens_cached"), 640);
    assert_eq!(row.get::<Option<i64>, _>("estimated_input"), Some(512));
    let row = sqlx::query("SELECT estimated_input FROM gateway_llm_usage WHERE actor = $1")
        .bind(&unpriced)
        .fetch_one(&pool)
        .await
        .expect("read the unpriced call back");
    assert_eq!(
        row.get::<Option<i64>, _>("estimated_input"),
        None,
        "a call the writer did not price must read as absent, not as a priced zero"
    );

    for actor in [&before, &after, &unpriced] {
        sqlx::query("DELETE FROM gateway_llm_usage WHERE actor = $1")
            .bind(actor)
            .execute(&pool)
            .await
            .expect("clean up the probe rows");
    }
}
