//! Sample-log pruning against a live PostgreSQL.
//!
//! What these tests check is the part the database decides: that a row past
//! the window leaves and a row inside it stays, and that the batched delete
//! really drains a table larger than one batch rather than stopping at the
//! first statement. None of that can be checked without a server, so the tests
//! are ignored by default and need `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test metrics_retention -- --ignored
//! ```
//!
//! The probe tables are created and dropped by the tests themselves, so the
//! database is left as it was found. Each test gets its own table because the
//! tests share one database and run in parallel.

use std::time::Duration;

use sqlx::PgPool;

use cog_storage::SampleRetention;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

async fn fresh_probe(pool: &PgPool, table: &str) {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {table} (
             id SERIAL PRIMARY KEY,
             metric_type TEXT NOT NULL,
             name TEXT NOT NULL,
             value DOUBLE PRECISION NOT NULL,
             labels JSONB NOT NULL DEFAULT '{{}}',
             timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
         )"
    ))
    .execute(pool)
    .await
    .unwrap();
}

async fn drop_probe(pool: &PgPool, table: &str) {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(pool)
        .await
        .unwrap();
}

/// Insert `rows` rows, all `age` old.
async fn insert_aged(pool: &PgPool, table: &str, rows: i64, age: &str) {
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, timestamp)
         SELECT 'counter', 'probe_total', 1.0, NOW() - INTERVAL '{age}'
         FROM generate_series(1, $1)"
    ))
    .bind(rows)
    .execute(pool)
    .await
    .unwrap();
}

async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

fn retention(pool: PgPool, table: &str, secs: u64) -> SampleRetention {
    SampleRetention::new(pool, Duration::from_secs(secs)).with_table(table)
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn aged_rows_leave_and_recent_rows_stay() {
    let table = "metrics_retention_probe_aged";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 40, "2 days").await;
    insert_aged(&pool, table, 12, "1 minute").await;
    assert_eq!(count(&pool, table).await, 52);

    let removed = retention(pool.clone(), table, 3600)
        .sweep_once()
        .await
        .unwrap();

    assert_eq!(removed, 40);
    assert_eq!(count(&pool, table).await, 12);
    drop_probe(&pool, table).await;
}

/// A table holding more than one batch has to be drained by the loop, not left
/// partly pruned by the first statement.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_table_larger_than_one_batch_is_drained_completely() {
    let table = "metrics_retention_probe_batched";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    let rows = 45_000;
    insert_aged(&pool, table, rows, "2 days").await;
    insert_aged(&pool, table, 5, "1 minute").await;

    let removed = retention(pool.clone(), table, 3600)
        .sweep_once()
        .await
        .unwrap();

    assert_eq!(removed, rows as u64);
    assert_eq!(count(&pool, table).await, 5);
    drop_probe(&pool, table).await;
}

/// With nothing past the window the sweep has to be a no-op rather than a
/// delete of whatever it finds.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_sweep_inside_the_window_removes_nothing() {
    let table = "metrics_retention_probe_inside";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 7, "1 minute").await;

    let removed = retention(pool.clone(), table, 3600)
        .sweep_once()
        .await
        .unwrap();

    assert_eq!(removed, 0);
    assert_eq!(count(&pool, table).await, 7);
    drop_probe(&pool, table).await;
}
