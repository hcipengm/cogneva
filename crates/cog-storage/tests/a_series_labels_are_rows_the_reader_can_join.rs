//! A series' label set is also a set of rows a reader can join on, not only a
//! text blob it has to parse.
//!
//! The labels of a metric series are an open set — one column per key does not
//! hold — so the projection stores them as text and the side table
//! `metric_labels` splits them into `(name, labels, key, value)`. A reader that
//! asks "which endpoint had 2xx, and when" then joins on columns. Parsing the
//! text instead has the same failure mode as every missing column: a misspelled
//! key reads as NULL, not as an error, and the reading prints zero for a cell
//! that was never there.
//!
//! The join is exact only because the writer spells `labels` the same way on
//! both sides (`::jsonb::text`, in one transaction). This test asserts that by
//! joining — a side table spelled differently would still have the right
//! endpoint/status values and would silently match no series row.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test a_series_labels_are_rows_the_reader_can_join -- --ignored
//! ```

use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{MetricName, MetricsBackend};
use cog_storage::PostgresMetricsBackend;

use cog_test_support::database_url;

const COUNTER: &str = "probe_metric_labels_counter";
const HISTOGRAM: &str = "probe_metric_labels_histogram";

fn labels(endpoint: &str, status: &str) -> HashMap<String, String> {
    HashMap::from([
        ("endpoint".to_string(), endpoint.to_string()),
        ("status".to_string(), status.to_string()),
    ])
}

async fn remove(pool: &PgPool) {
    for table in [
        "cog_metric_counter_totals",
        "cog_metric_histogram_buckets",
        "cog_metric_histogram_sums",
        "cog_metrics_samples",
        "metric_labels",
    ] {
        let statement = format!("DELETE FROM {table} WHERE name IN ('{COUNTER}', '{HISTOGRAM}')");
        sqlx::query(&statement).execute(pool).await.unwrap();
    }
}

/// The value of one key of the series that joins a totals row — the exact read
/// the e2e ring does.
async fn joined_key(pool: &PgPool, totals_table: &str, name: &str, key: &str) -> Vec<String> {
    let statement = format!(
        "SELECT l.value FROM {totals_table} t \
           JOIN metric_labels l \
             ON l.name = t.name AND l.labels = t.labels AND l.key = $2 \
          WHERE t.name = $1 ORDER BY l.value"
    );
    sqlx::query_scalar(&statement)
        .bind(name)
        .bind(key)
        .fetch_all(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn every_written_series_splits_its_labels_into_joinable_rows() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let backend = PostgresMetricsBackend::new(pool.clone());
    backend.init_schema().await.unwrap();
    remove(&pool).await;

    let counter = MetricName::for_tests_only(COUNTER);
    backend
        .record_counter(counter.clone(), 1.0, labels("/api/v1/things", "200"))
        .await
        .unwrap();
    backend
        .record_counter(counter, 1.0, labels("/api/v1/things", "401"))
        .await
        .unwrap();
    backend
        .record_histogram(
            MetricName::for_tests_only(HISTOGRAM),
            0.25,
            labels("/api/v1/latency", "200"),
        )
        .await
        .unwrap();
    // The write goes into a queue; a reader that takes over the read has to take
    // over the wait too, or it asks about the queue instead of the upsert.
    backend.flush_pending().await.unwrap();

    assert_eq!(
        joined_key(&pool, "cog_metric_counter_totals", COUNTER, "status").await,
        vec!["200".to_string(), "401".to_string()],
        "both counter series must join their totals row to a status row"
    );
    assert_eq!(
        joined_key(&pool, "cog_metric_counter_totals", COUNTER, "endpoint").await,
        vec!["/api/v1/things".to_string(), "/api/v1/things".to_string()],
        "the endpoint key is a column a reader can filter on, not a JSON path"
    );
    assert_eq!(
        joined_key(&pool, "cog_metric_histogram_sums", HISTOGRAM, "endpoint").await,
        vec!["/api/v1/latency".to_string()],
        "histogram series carry their label rows too"
    );

    // Every totals row has its full label set on the side: a writer that only
    // wrote some keys would leave a reader's filter silently empty.
    let (series_with_labels, labels_rows): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM cog_metric_counter_totals WHERE name = $1), \
                (SELECT count(*) FROM metric_labels WHERE name = $1)",
    )
    .bind(COUNTER)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (series_with_labels, labels_rows),
        (2, 4),
        "two series x two keys: one side-table row per key of every series"
    );

    remove(&pool).await;
}
