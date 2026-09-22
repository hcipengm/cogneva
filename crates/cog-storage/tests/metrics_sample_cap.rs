//! Sample-log pruning by capacity, against a live PostgreSQL.
//!
//! What these tests check is the part the database decides: that a log over
//! its row budget is brought back under it oldest-first, that a log under
//! budget is left alone, that a log larger than one statement is drained all
//! the way rather than partly, and that no gauge series is left without the one
//! row every reader reaches it through. None of that can be checked without a
//! server, so the tests are ignored by default and need
//! `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-storage --test metrics_sample_cap -- --ignored
//! ```
//!
//! The probe tables are created and dropped by the tests themselves, so the
//! database is left as it was found. Each test gets its own table because the
//! tests share one database and run in parallel.

use sqlx::PgPool;

use cog_storage::SampleLogCap;

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

/// Insert `rows` rows of one series, each `age` old.
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

/// Insert `rows` rows spread across `series` distinct label sets, oldest
/// first, so every series' newest row is the last one written.
async fn insert_series(pool: &PgPool, table: &str, series: i64, rows: i64) {
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'gauge', 'probe_gauge', 1.0,
                jsonb_build_object('series', g % $1),
                NOW() - make_interval(secs => g)
         FROM generate_series(0, $2 - 1) AS g"
    ))
    .bind(series)
    .bind(rows)
    .execute(pool)
    .await
    .unwrap();
}

/// Insert `rows` counter rows spread across `series` distinct label sets — the
/// shape of a counter keyed on an object id, where no label set ever recurs.
async fn insert_counter_series(pool: &PgPool, table: &str, series: i64, rows: i64) {
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'counter', 'probe_total', 1.0,
                jsonb_build_object('object', g % $1),
                NOW() - make_interval(secs => g)
         FROM generate_series(0, $2 - 1) AS g"
    ))
    .bind(series)
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

async fn count_named(pool: &PgPool, table: &str, name: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE name = $1"))
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn cap(pool: PgPool, table: &str, budget: u64) -> SampleLogCap {
    SampleLogCap::new(pool, budget).with_table(table)
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_log_over_budget_is_brought_back_to_it_oldest_first() {
    let table = "metrics_sample_cap_probe_over";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 52, "2 days").await;

    let outcome = cap(pool.clone(), table, 12).sweep_once().await.unwrap();

    assert_eq!(outcome.removed, 40);
    assert_eq!(outcome.held, 12);
    assert_eq!(outcome.budget, Some(12));
    assert!(!outcome.floor_held);
    assert_eq!(count(&pool, table).await, 12);
    drop_probe(&pool, table).await;
}

/// A log already inside its budget is left alone: the sweep deletes the
/// overshoot, not whatever it finds.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_log_inside_budget_removes_nothing() {
    let table = "metrics_sample_cap_probe_inside";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 7, "1 minute").await;

    let outcome = cap(pool.clone(), table, 100).sweep_once().await.unwrap();

    assert_eq!(outcome.removed, 0);
    assert_eq!(outcome.held, 7);
    assert!(!outcome.floor_held);
    assert_eq!(count(&pool, table).await, 7);
    drop_probe(&pool, table).await;
}

/// An overshoot larger than one statement has to be drained by the loop, not
/// left partly pruned by the first one.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_overshoot_larger_than_one_statement_is_drained_completely() {
    let table = "metrics_sample_cap_probe_bulk";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 45_000, "2 days").await;

    let outcome = cap(pool.clone(), table, 5).sweep_once().await.unwrap();

    assert_eq!(outcome.removed, 44_995);
    assert_eq!(outcome.held, 5);
    assert_eq!(count(&pool, table).await, 5);
    drop_probe(&pool, table).await;
}

/// A zero budget turns pruning off rather than deleting everything, which is
/// what a deployment that does not own the log sets.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_zero_budget_prunes_nothing() {
    let table = "metrics_sample_cap_probe_disabled";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 9, "2 days").await;

    let outcome = cap(pool.clone(), table, 0).sweep_once().await.unwrap();

    assert_eq!(outcome.removed, 0);
    assert_eq!(outcome.budget, None);
    assert_eq!(count(&pool, table).await, 9);
    drop_probe(&pool, table).await;
}

/// The floor is the point of the whole exercise: a budget tight enough to run
/// past the heads must stop at the heads and say so, not take the rows the
/// scrape needs to see the series at all.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn every_series_keeps_its_newest_row_even_when_the_budget_is_below_that() {
    let table = "metrics_sample_cap_probe_floor";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    let series = 20;
    insert_series(&pool, table, series, 200).await;

    // A budget that cannot be met: the floor sits at the oldest of the twenty
    // newest-per-series rows, so only the 180 rows below it can go and twenty
    // remain whatever the budget says.
    let outcome = cap(pool.clone(), table, 3).sweep_once().await.unwrap();

    assert_eq!(outcome.removed, 180);
    assert_eq!(outcome.held, series);
    assert!(
        outcome.floor_held,
        "the sweep must report that it stopped short"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM (
                 SELECT DISTINCT ON (labels) id FROM {table} ORDER BY labels, timestamp DESC
             ) heads"
        ))
        .fetch_one(&pool)
        .await
        .unwrap(),
        series,
        "every series must still be readable through its newest row"
    );
    drop_probe(&pool, table).await;
}

/// The floor is about which kinds are read through the log, not about being a
/// head. A gauge's value is its newest row, so that row stays; a counter's and a
/// histogram's value is in their accumulation tables, so every row of theirs is
/// history the sweep may take.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn only_a_gauge_series_keeps_a_row() {
    let table = "metrics_sample_cap_probe_kinds";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 4, "1 day").await;
    insert_series(&pool, table, 1, 4).await;

    let outcome = cap(pool.clone(), table, 1).sweep_once().await.unwrap();

    assert_eq!(
        count_named(&pool, table, "probe_total").await,
        0,
        "a counter's log rows are history and must all be deletable"
    );
    assert_eq!(
        count_named(&pool, table, "probe_gauge").await,
        1,
        "the gauge's newest row is the value the scrape reads"
    );
    assert_eq!(outcome.removed, 7);
    assert_eq!(outcome.held, 1);
    assert!(!outcome.floor_held);
    drop_probe(&pool, table).await;
}

/// A counter keyed on an object id mints a label set per object and never
/// repeats one. Were the floor to cover counters, this log could never be
/// brought under budget: every pass would report itself over capacity with no
/// row it was allowed to take, which makes the capacity knob inoperative
/// exactly where an unbounded series count lives.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_counter_whose_series_do_not_repeat_can_be_brought_under_budget() {
    let table = "metrics_sample_cap_probe_cardinality";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_counter_series(&pool, table, 30, 300).await;

    let outcome = cap(pool.clone(), table, 5).sweep_once().await.unwrap();

    assert_eq!(outcome.held, 5);
    assert_eq!(outcome.removed, 295);
    assert!(
        !outcome.floor_held,
        "nothing in a counter's log is a value any reader reaches it through"
    );
    drop_probe(&pool, table).await;
}

/// The same floor must not block a sweep that the budget can actually meet.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_floor_does_not_stop_a_sweep_the_budget_can_meet() {
    let table = "metrics_sample_cap_probe_floor_pass";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_series(&pool, table, 4, 250).await;

    // Four series over 250 rows: the heads are the four newest rows, so 246
    // are below the floor and the budget is well within reach.
    let outcome = cap(pool.clone(), table, 40).sweep_once().await.unwrap();

    assert_eq!(outcome.held, 40);
    assert_eq!(outcome.removed, 210);
    assert!(!outcome.floor_held);
    drop_probe(&pool, table).await;
}
