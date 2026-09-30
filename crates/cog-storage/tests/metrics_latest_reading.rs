//! What a scrape pays for a gauge's current value, against a live PostgreSQL.
//!
//! A gauge's value is its newest sample per label set, so `/metrics` answers a
//! gauge by reading the sample log — the counter-totals table next to it exists
//! precisely so that a counter's current value is *not* read that way. The read
//! is therefore on a path a scrape endpoint hits continuously, and what it may
//! cost is bounded by the answer (one row per series) rather than by how much
//! history the log is holding.
//!
//! The bound is asserted as rows: no node in the plan may see more rows than
//! there are series to answer with. Node types alone are not enough to hold
//! this, and the live log showed why. The same statement and the same index
//! that walk the series in group order on a table built by this test — 120,000
//! rows, index chosen, no sort — were answered by the planner with a bitmap
//! scan plus a sort on the production table: 120,457 rows into a 9,344 kB
//! temporary file, 848 ms, index present and unused. The difference is physical,
//! not textual: half a million sample deletions (retention trims this log
//! continuously) had left the live index carrying about twice the pages per
//! entry, which put the estimated cost of the ordered walk above the estimated
//! cost of reading the rows out of the heap and sorting them. A test that
//! asserts the plan's shape can be green here while production sorts, because
//! the planner's choice moves with a property of the table that a fresh fixture
//! cannot have. A bound on the rows the plan sees does not move with it.
//!
//! Both halves are checked here because both are the same claim: the answers
//! are the newest row of each series, and the plan that produced them saw no
//! more rows than that claim needs. Neither can be checked without a server, so
//! the test is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing
//! at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test metrics_latest_reading -- --ignored
//! ```
//!
//! The table is the backend's own ([`PostgresMetricsBackend::init_schema`]),
//! because the index under test is the one production creates: a probe table
//! with a hand-made index would go on passing after `init_schema` stopped
//! creating it. The test therefore drops and recreates that table, and drops it
//! again at the end, so the database is left as it was found.

use sqlx::{Executor, PgPool};
use std::collections::HashMap;

use cog_core::MetricsBackend;
use cog_storage::{PostgresMetricsBackend, GAUGE_LATEST_SQL};

const METRIC: &str = "probe_gauge_latest";
/// Label sets, and rows each — enough history that a sort of it cannot stay in
/// the server's 4 MB `work_mem` and has to spill, which is what the old plan
/// did on every scrape.
const SERIES: i64 = 8;
const ROWS: i64 = 15_000;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// Seed `SERIES` label sets of one gauge, oldest first, with the row's index as
/// its value. The newest row of series `s` is then the one whose value is `s`,
/// so "did the read return the newest sample" is a question about the value.
async fn seed(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO cog_metrics_samples (metric_type, name, value, labels, timestamp)
         SELECT 'gauge', $1, g::float8, jsonb_build_object('series', (g % $2)::text),
                NOW() - make_interval(secs => g::float8)
         FROM generate_series(0, $3 - 1) AS g",
    )
    .bind(METRIC)
    .bind(SERIES)
    .bind(SERIES * ROWS)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE cog_metrics_samples")
        .execute(pool)
        .await
        .unwrap();
}

/// Every `"Node Type"` the plan holds, outermost first.
fn node_types(plan: &serde_json::Value) -> Vec<String> {
    let mut found = Vec::new();
    collect_nodes(plan, &mut found);
    found
}

fn collect_nodes(node: &serde_json::Value, found: &mut Vec<String>) {
    if let Some(kind) = node.get("Node Type").and_then(|v| v.as_str()) {
        found.push(kind.to_string());
    }
    if let Some(children) = node.get("Plans").and_then(|v| v.as_array()) {
        for child in children {
            collect_nodes(child, found);
        }
    }
}

/// The most rows any one node of the plan saw.
fn max_rows_seen(plan: &serde_json::Value) -> u64 {
    let here = plan
        .get("Actual Rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let below = plan
        .get("Plans")
        .and_then(|v| v.as_array())
        .map(|children| children.iter().map(max_rows_seen).max().unwrap_or(0))
        .unwrap_or(0);
    here.max(below)
}

/// The blocks written to a temporary file anywhere in the plan.
fn temp_written_blocks(plan: &serde_json::Value) -> u64 {
    let here = plan
        .get("Temp Written Blocks")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let below = plan
        .get("Plans")
        .and_then(|v| v.as_array())
        .map(|children| children.iter().map(temp_written_blocks).sum())
        .unwrap_or(0);
    here + below
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_newest_sample_of_each_series_is_read_without_sorting_the_log() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    // Dropped first so the index under test is one `init_schema` creates now
    // rather than one an earlier run left behind.
    sqlx::query("DROP TABLE IF EXISTS cog_metrics_samples")
        .execute(&pool)
        .await
        .unwrap();
    let backend = PostgresMetricsBackend::new(pool.clone());
    backend.init_schema().await.unwrap();
    seed(&pool).await;

    // The answers: one row per label set, and it is that series' newest sample.
    let samples = backend.query_gauge_latest(METRIC).await.unwrap();
    assert_eq!(
        samples.len(),
        SERIES as usize,
        "a series came back more than once, or not at all"
    );
    let mut newest: HashMap<i64, f64> = HashMap::new();
    for sample in &samples {
        let series: i64 = sample.labels["series"].parse().unwrap();
        newest.insert(series, sample.value);
    }
    for series in 0..SERIES {
        assert_eq!(
            newest.get(&series).copied(),
            Some(series as f64),
            "series {series} did not answer with its newest sample"
        );
    }

    // The plan those answers came from: the label sets walked in the index's
    // order, one newest-sample probe each, and nothing along the way looking at
    // the series' history. `EXPLAIN EXECUTE` runs the statement the backend
    // runs, on one connection, because a prepared statement does not outlive
    // its session.
    let mut conn = pool.acquire().await.unwrap();
    let prepare = format!("PREPARE probe_latest(text) AS {GAUGE_LATEST_SQL}");
    conn.execute(prepare.as_str()).await.unwrap();
    // `EXPLAIN EXECUTE` takes the arguments as literals rather than as binds;
    // the statement being explained is still the backend's own text.
    let explain =
        format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE probe_latest('{METRIC}')");
    let plan: serde_json::Value = sqlx::query_scalar(explain.as_str())
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let plan = &plan[0]["Plan"];

    let kinds = node_types(plan);
    assert!(
        !kinds.iter().any(|kind| kind == "Sort"),
        "the latest read sorted the series' history: {kinds:?}"
    );
    assert_eq!(
        temp_written_blocks(plan),
        0,
        "the latest read spilled the series' history to a temporary file: {kinds:?}"
    );
    assert!(
        kinds.iter().any(|kind| kind.contains("Index")),
        "the latest read did not go through an index at all: {kinds:?}"
    );
    // The bound that no plan shape can be read for: one row per series, plus the
    // row that ends the walk. A plan that reads the history to answer shows up
    // here however it is arranged, sorted or not.
    let series_walk_rows = SERIES as u64 + 1;
    let seen = max_rows_seen(plan);
    assert!(
        seen <= series_walk_rows,
        "the latest read saw {seen} rows for {SERIES} series ({kinds:?}): what it reads is \
         supposed to be bounded by the answer, not by the history behind it"
    );

    sqlx::query("DROP TABLE IF EXISTS cog_metrics_samples")
        .execute(&pool)
        .await
        .unwrap();
    drop(conn);
    drop(pool);
}
