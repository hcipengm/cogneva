//! What a counter write does to a total that is already there, against a live
//! PostgreSQL.
//!
//! A `_total` series is read by differencing two scrapes, so the whole family
//! of rates and panels rests on the stored total being accumulated. Nothing in
//! the call sites says so: an implementation that assigned instead of adding
//! would leave every writer unchanged and every series plausibly small, and the
//! loss — history cut at each restart — is only visible to a reader who
//! happened to compare two scrapes across one.
//!
//! One caller depends on the sharp edge of it. Seeding a closed set writes
//! every cell as a zero increment, so that "this boot consulted nothing" reads
//! as a zero rather than as the same absence a build without the reading shows.
//! That works only if a zero write creates a series and leaves an existing
//! total alone; if it assigned, every restart would clear the family. Both
//! halves are asserted here, and the second one needs the first one's writer
//! present, so they run against one series in one case.
//!
//! Neither can be checked without a server, so the test is ignored by default
//! and needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test metrics_counter_accumulation -- --ignored
//! ```
//!
//! The tables are the backend's own ([`PostgresMetricsBackend::init_schema`]),
//! because the upsert under test is the one production runs: a table with a
//! hand-written constraint would go on passing after `init_schema` stopped
//! creating that one. The probe writes under a name no build can produce
//! ([`MetricName::for_tests_only`]) and removes its rows afterwards, so a
//! throwaway database is left as it was found.

use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{MetricName, MetricsBackend};
use cog_storage::PostgresMetricsBackend;

const METRIC: &str = "probe_counter_accumulation";

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

fn labels(series: &str) -> HashMap<String, String> {
    HashMap::from([("series".to_string(), series.to_string())])
}

/// The stored total for one series, or `None` when the series is not there.
///
/// The distinction is the point of half of this test: a zero and a missing row
/// are the same number to a reader that only sums, and the seed exists to tell
/// them apart.
async fn total(pool: &PgPool, series: &str) -> Option<f64> {
    sqlx::query_scalar(
        "SELECT value FROM cog_metric_counter_totals WHERE name = $1 AND labels = $2",
    )
    .bind(METRIC)
    .bind(serde_json::json!({ "series": series }))
    .fetch_optional(pool)
    .await
    .unwrap()
}

/// Both faces a counter write leaves a row on.
async fn remove(pool: &PgPool) {
    for table in ["cog_metric_counter_totals", "cog_metrics_samples"] {
        let statement = format!("DELETE FROM {table} WHERE name = $1");
        sqlx::query(&statement)
            .bind(METRIC)
            .execute(pool)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_counter_write_adds_to_the_total_and_a_zero_write_only_creates_it() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let backend = PostgresMetricsBackend::new(pool.clone());
    backend.init_schema().await.unwrap();
    // An earlier run's rows would answer "is the series there" with a yes this
    // run did not produce.
    remove(&pool).await;

    let name = MetricName::for_tests_only(METRIC);

    // The seed's write, on a series nothing has counted: it has to create the
    // row at zero. A backend that only wrote totals it was given a value for
    // would leave nothing here, and the cell would be missing from the scrape
    // of a boot that ran no retrieval -- the absence the seed exists to remove.
    backend
        .record_counter(name, 0.0, labels("seed-only"))
        .await
        .unwrap();
    assert_eq!(
        total(&pool, "seed-only").await,
        Some(0.0),
        "a zero increment did not create the series: a boot with nothing to \
         consult would publish nothing at all"
    );

    // A series with real counts behind it.
    backend
        .record_counter(name, 5.0, labels("counted"))
        .await
        .unwrap();
    assert_eq!(
        total(&pool, "counted").await,
        Some(5.0),
        "the first write did not land its value in the total"
    );

    // The seed a later boot writes over it: the total has to survive. This is
    // the assertion an assigning backend fails, and it fails silently -- the
    // call site is identical either way.
    backend
        .record_counter(name, 0.0, labels("counted"))
        .await
        .unwrap();
    assert_eq!(
        total(&pool, "counted").await,
        Some(5.0),
        "a zero increment cleared a total another process had counted up: every \
         restart would reset the family"
    );

    // ...and the counting path proper, so an implementation that ignored all
    // but the first write is not left passing.
    backend
        .record_counter(name, 2.0, labels("counted"))
        .await
        .unwrap();
    assert_eq!(
        total(&pool, "counted").await,
        Some(7.0),
        "a second write did not add to the total"
    );

    remove(&pool).await;
}
