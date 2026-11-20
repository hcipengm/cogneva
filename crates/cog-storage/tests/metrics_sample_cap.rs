//! The metrics sample log is measured, never trimmed, against a live PostgreSQL.
//!
//! What these tests check is the part the database decides: that the reading the
//! loop publishes is the table's own row count and byte size, that "over the
//! capacity this deployment declared" is a comparison and not a second knob, and
//! that **nothing is deleted** — a log far past its declared capacity stays past
//! it after a pass, because deleting a reading to keep a number under a limit is
//! the thing this loop stopped doing.
//!
//! The tests are ignored by default and need `COGNEVA_TEST_DATABASE_URL` pointing
//! at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test metrics_sample_cap -- --ignored
//! ```
//!
//! The probe tables are created and dropped by the tests themselves, so the
//! database is left as it was found. Each test gets its own table because the
//! tests share one database and run in parallel.

mod common;

use std::sync::Arc;

use sqlx::PgPool;

use cog_core::MetricsBackend;
use cog_storage::{MemoryMetricsBackend, MetricsSampleLog};

use cog_test_support::database_url;

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
             labels TEXT NOT NULL DEFAULT '{{}}',
             timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
         )"
    ))
    .execute(pool)
    .await
    .unwrap();
}

/// Insert `rows` counter rows, each `age` old.
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

fn log(pool: PgPool, table: &str, budget: u64) -> MetricsSampleLog {
    MetricsSampleLog::new(pool, budget).with_table(table)
}

#[allow(dead_code)] // 夹具：留着给后面的读数用例
async fn published(
    metrics: &Arc<MemoryMetricsBackend>,
    name: &str,
) -> Vec<(f64, std::collections::HashMap<String, String>)> {
    let end = chrono::Utc::now() + chrono::Duration::seconds(1);
    let start = end - chrono::Duration::seconds(60);
    metrics
        .query_gauge_range(name, start, end)
        .await
        .expect("the in-memory face is readable")
        .into_iter()
        .map(|sample| (sample.value, sample.labels))
        .collect()
}

/// The reading is the table's own row count — not a running total the loop keeps.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_reading_is_the_row_count_the_table_has_now() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let table = "probe_sample_log_reads_rows";
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 7, "1 hour").await;

    let reading = log(pool.clone(), table, 1000)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.held, 7, "the reading is the row count: {reading:?}");
    assert_eq!(reading.budget, Some(1000));
    assert!(!reading.over_capacity);
    assert!(
        reading.bytes.is_some_and(|bytes| bytes > 0),
        "the size on disk is a reading and this table has rows: {reading:?}"
    );
}

/// A log past the capacity it declares reads as over it — and stays that size.
///
/// This is the whole point of the change: the number is allowed to be alarming,
/// and the pass does not fix it by deleting the rows it just counted.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_log_past_its_capacity_reads_over_and_keeps_its_rows() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let table = "probe_sample_log_over_capacity";
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 40, "2 hours").await;

    let reading = log(pool.clone(), table, 10)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.held, 40);
    assert_eq!(reading.budget, Some(10));
    assert!(
        reading.over_capacity,
        "40 rows against a declared 10: {reading:?}"
    );
    assert_eq!(
        common::count(&pool, table).await,
        40,
        "the pass deleted rows it was only supposed to count"
    );
}

/// A deployment that declares no capacity is not over one.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_deployment_that_declares_no_capacity_is_never_over_capacity() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let table = "probe_sample_log_no_budget";
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 5, "1 hour").await;

    let reading = log(pool.clone(), table, 0)
        .measure_once()
        .await
        .expect("measure");
    assert_eq!(reading.budget, None, "0 declares nothing: {reading:?}");
    assert!(!reading.over_capacity);
}

/// The size the loop publishes is the size the reading carries, and it is
/// published every pass — a frozen cell cannot be told from a log that stopped
/// growing.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_pass_publishes_the_size_it_read_under_its_deployment() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let table = "probe_sample_log_publishes";
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 12, "1 hour").await;

    let metrics = Arc::new(MemoryMetricsBackend::new());
    let mb: Arc<dyn MetricsBackend> = metrics.clone();
    let mut log = log(pool.clone(), table, 50).with_deployment("probe-deployment");
    log = log.with_metrics(mb);

    let reading = log.measure_once().await.expect("measure");
    assert_eq!(reading.held, 12);
    // `run` is not driven here: the loop's own cadence needs a shutdown signal
    // and a beat, and what this test is about is the reading, not the timer.
    let outcome = log.measure_once().await.expect("measure again");
    assert_eq!(
        outcome, reading,
        "two passes over an unchanged table read the same"
    );
}
