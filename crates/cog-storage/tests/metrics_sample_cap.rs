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

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;

use cog_core::loop_health::{
    Cadence, LoopHealth, LOOP_LABEL, LOOP_OWNER_ACQUISITIONS_TOTAL, LOOP_OWNER_HELD,
    LOOP_REGISTERED,
};
use cog_core::{MetricsBackend, Observable, OwnerLeaseBroker, ShutdownSignal};
use cog_storage::metrics_sample_cap::{DEPLOYMENT_LABEL, LOOP, LOOP_PERIOD, ROLE};
use cog_storage::{MemoryMetricsBackend, PgOwnerLeaseBroker, SampleLogCap, LEASE_TABLE};

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

/// Wait for the loop to publish `want` as its reading of the role, and return
/// the last one seen.
///
/// The waits in the role test below are for a pass to have happened, and a pass
/// takes as long as its queries take — a fixed sleep would be a number that has
/// to be re-guessed every time the host or the database changes.
async fn wait_for_held(health: &LoopHealth, want: f64, limit: Duration) -> Option<f64> {
    let started = Instant::now();
    loop {
        let seen = owner_held(health).await;
        if seen == Some(want) || started.elapsed() >= limit {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Wait for the log to come down to `want` rows, and return the last count
/// taken.
async fn wait_for_rows(pool: &PgPool, table: &str, want: i64, limit: Duration) -> i64 {
    let started = Instant::now();
    loop {
        let held = count(pool, table).await;
        if held <= want || started.elapsed() >= limit {
            return held;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// What the loop published about who holds its role, as its own reading rather
/// than as an inference from what it did.
async fn owner_held(health: &LoopHealth) -> Option<f64> {
    loop_metric(health, LOOP_OWNER_HELD).await
}

/// One series this loop publishes about itself, or nothing when it publishes no
/// such series at all. The two are different answers for the role readings: a
/// loop that never contended for the role has no held and no acquisitions
/// reading, and only the absence says so.
async fn loop_metric(health: &LoopHealth, name: &str) -> Option<f64> {
    health
        .collect_metrics("")
        .await
        .unwrap()
        .into_iter()
        .find(|m| m.name == name && m.labels.get(LOOP_LABEL).map(String::as_str) == Some(LOOP))
        .map(|m| m.value)
}

/// Clear whatever term is written for the role these tests contend over.
async fn clear_role(pool: &PgPool) {
    sqlx::query(&format!("DELETE FROM {LEASE_TABLE} WHERE role = $1"))
        .bind(ROLE)
        .execute(pool)
        .await
        .unwrap();
}

/// End the term written for the role, which is the state the row is in once its
/// holder has stopped renewing it.
async fn expire_role(pool: &PgPool) {
    sqlx::query(&format!(
        "UPDATE {LEASE_TABLE} SET expires_at = now() - interval '1 second' WHERE role = $1"
    ))
    .bind(ROLE)
    .execute(pool)
    .await
    .unwrap();
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

/// A sweep that reaches the budget is not a sweep the floor stopped, even when
/// the count taken afterwards stands above the budget.
///
/// The log is written while a pass runs, so the two claims come apart by
/// construction: removing the whole overshoot and recounting a few rows higher
/// is an ordinary pass, and reporting it as the floor produces the one verdict
/// that tells an operator to move the budget. The writes are made to arrive
/// during the pass the only way a test can make deterministic — a statement
/// trigger on the probe table that puts one row back for every batch the sweep
/// takes, standing in for the traffic that keeps the log moving.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_sweep_that_reaches_the_budget_is_not_a_sweep_the_floor_stopped() {
    let table = "metrics_sample_cap_probe_moving";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 300, "2 days").await;

    sqlx::query(&format!(
        "CREATE OR REPLACE FUNCTION {table}_writer() RETURNS TRIGGER AS $$
         BEGIN
             INSERT INTO {table} (metric_type, name, value, timestamp)
             VALUES ('counter', 'probe_total', 1.0, NOW());
             RETURN NULL;
         END; $$ LANGUAGE plpgsql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER {table}_writer AFTER DELETE ON {table}
         EXECUTE FUNCTION {table}_writer()"
    ))
    .execute(&pool)
    .await
    .unwrap();

    // Every row here is a counter's, so the deletion order can take all 300
    // and the budget of 100 is well within reach.
    let outcome = cap(pool.clone(), table, 100).sweep_once().await.unwrap();

    assert_eq!(
        outcome.removed, 200,
        "the pass must take its whole overshoot"
    );
    assert_eq!(
        outcome.held, 101,
        "the row the trigger put back is what pushes the count over"
    );
    assert!(
        !outcome.floor_held,
        "a pass that took every row it was asked to did not stop at the floor: \
         the count above the budget is the writes, not the heads"
    );
    drop_probe(&pool, table).await;
    // The trigger outlives the table it was attached to, so the function is
    // dropped only once nothing depends on it.
    sqlx::query(&format!("DROP FUNCTION IF EXISTS {table}_writer()"))
        .execute(&pool)
        .await
        .unwrap();
}

/// The role gate, read at both ends and at the switch that decides whether the
/// loop contends for it at all.
///
/// One loop over one table, in three runs — one role is one row, so the whole
/// story of who may hold it belongs in one test rather than in two that would
/// have to take turns.
///
/// A deployment with a budget of zero must not take it even with a broker
/// standing ready: the holder renews on the lease's own cadence, so a claim a
/// process has no use for keeps the process that does prune out of the role for
/// as long as the claimant lives. What such a loop publishes is nothing — a
/// held reading or an acquisitions count would both be claims about a role it
/// never asked for. While another process holds the role the loop cycles and
/// reports and keeps every row; once that holder's term has run out this
/// process takes the role and the same loop brings the log back under its
/// capacity.
///
/// What joins the runs is the loop's own reading of the role, so a loop that
/// failed to prune for any other reason — a query that errored — cannot pass
/// this.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_role_is_taken_only_by_the_deployments_that_prune() {
    let table = "metrics_sample_cap_probe_role";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 20, "2 days").await;

    let own: Arc<dyn OwnerLeaseBroker> = Arc::new(
        PgOwnerLeaseBroker::with_holder(pool.clone(), "probe-own")
            .await
            .unwrap(),
    );
    clear_role(&pool).await;

    // A budget of zero. The role is free and this loop has a broker to take it
    // with, which is what makes declining a decision rather than an inability.
    // The ask that must not happen happens at start-up, before the first pass:
    // the first ask is immediate and the next is an ask period away. So this is
    // a bounded chance to be wrong about a process that has already had its
    // chance, not a guess at how long a pass takes.
    let idle = Arc::new(cap(pool.clone(), table, 0).with_role(Arc::clone(&own)));
    let health = LoopHealth::new();
    let beat = health.register(LOOP, Cadence::Periodic(LOOP_PERIOD));
    let shutdown = ShutdownSignal::default();
    let running = tokio::spawn({
        let idle = Arc::clone(&idle);
        let shutdown = shutdown.clone();
        async move { idle.run(beat, shutdown).await }
    });
    let limit = Instant::now() + Duration::from_secs(2);
    while Instant::now() < limit {
        assert_eq!(
            owner_held(&health).await,
            None,
            "a deployment with no budget to prune against must not take the role"
        );
        assert_eq!(
            loop_metric(&health, LOOP_OWNER_ACQUISITIONS_TOTAL).await,
            None,
            "a deployment that contends for no role must never have asked for one"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        loop_metric(&health, LOOP_REGISTERED).await,
        Some(1.0),
        "the absence above must be a decision about the role, not a loop that never ran"
    );
    shutdown.trigger();
    running.await.unwrap();
    assert_eq!(
        count(&pool, table).await,
        20,
        "a zero budget must leave every row whether or not a role was available"
    );

    let other = PgOwnerLeaseBroker::with_holder(pool.clone(), "probe-other")
        .await
        .unwrap();
    assert!(
        other
            .lease(ROLE, Duration::from_secs(60))
            .try_hold()
            .await
            .unwrap(),
        "the fixture is another process holding the role"
    );

    let pruner = Arc::new(cap(pool.clone(), table, 5).with_role(Arc::clone(&own)));

    // Someone else's role. The loop still cycles and says what it sees; what it
    // must not do is prune.
    let health = LoopHealth::new();
    let beat = health.register(LOOP, Cadence::Periodic(LOOP_PERIOD));
    let shutdown = ShutdownSignal::default();
    let running = tokio::spawn({
        let pruner = Arc::clone(&pruner);
        let shutdown = shutdown.clone();
        async move { pruner.run(beat, shutdown).await }
    });
    let seen = wait_for_held(&health, 0.0, Duration::from_secs(30)).await;
    assert_eq!(
        seen,
        Some(0.0),
        "the loop must publish that the role is held elsewhere"
    );
    shutdown.trigger();
    running.await.unwrap();
    assert_eq!(
        count(&pool, table).await,
        20,
        "a process that does not hold the role must not prune"
    );

    // The holder stopped renewing. The loop starts again — as it would after a
    // restart — takes the role, and holds the log down.
    expire_role(&pool).await;
    let health = LoopHealth::new();
    let beat = health.register(LOOP, Cadence::Periodic(LOOP_PERIOD));
    let shutdown = ShutdownSignal::default();
    let running = tokio::spawn({
        let pruner = Arc::clone(&pruner);
        let shutdown = shutdown.clone();
        async move { pruner.run(beat, shutdown).await }
    });
    let held = wait_for_rows(&pool, table, 5, Duration::from_secs(30)).await;
    assert_eq!(
        held, 5,
        "the loop that holds the role must bring the log under capacity"
    );
    assert_eq!(
        owner_held(&health).await,
        Some(1.0),
        "the loop that pruned must be the one that took the role"
    );
    shutdown.trigger();
    running.await.unwrap();

    clear_role(&pool).await;
    drop_probe(&pool, table).await;
}

/// What the loop publishes says which deployment published it.
///
/// The sample log is shared and a series is its label set, so two deployments
/// that read the log differently write one series and the store answers with
/// whoever wrote last. The deployment label is what makes "this deployment
/// declared nothing" a readable statement rather than the deployment next door's
/// budget — and it has to be the deployment, not the pod, because every series'
/// newest row is kept forever and a name that changes per rollout leaves a
/// permanent row behind each time.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_readings_name_the_deployment_that_published_them() {
    let table = "metrics_sample_cap_probe_deployment";
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, table).await;
    insert_aged(&pool, table, 5, "2 days").await;

    let metrics = Arc::new(MemoryMetricsBackend::new());
    let named = Arc::new(
        cap(pool.clone(), table, 1000)
            .with_metrics(Arc::clone(&metrics) as Arc<dyn MetricsBackend>)
            .with_deployment("probe-deployment"),
    );
    let shutdown = ShutdownSignal::default();
    let handle = named.spawn(shutdown.clone());

    let mut labels = None;
    for _ in 0..200 {
        let series = metrics
            .query_gauge_latest(&cog_core::metric_names::METRICS_SAMPLES_ROWS)
            .await
            .unwrap();
        if let Some(sample) = series.into_iter().next() {
            labels = Some(sample.labels);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.trigger();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let labels = labels.expect("the loop must publish what the log holds");
    assert_eq!(
        labels.get(DEPLOYMENT_LABEL).map(String::as_str),
        Some("probe-deployment"),
        "a reading that is one deployment's own claim must name it: {labels:?}"
    );
    assert_eq!(
        labels.len(),
        1,
        "the deployment must be the only label these readings carry: {labels:?}"
    );

    // A process whose platform never named it publishes the same readings
    // unlabelled rather than under an empty name, which would be a series of
    // its own that says nothing and splits every reader's set in two.
    let metrics = Arc::new(MemoryMetricsBackend::new());
    let unnamed = Arc::new(
        cap(pool.clone(), table, 1000)
            .with_metrics(Arc::clone(&metrics) as Arc<dyn MetricsBackend>)
            .with_deployment("   "),
    );
    let shutdown = ShutdownSignal::default();
    let handle = unnamed.spawn(shutdown.clone());
    let mut labels = None;
    for _ in 0..200 {
        let series = metrics
            .query_gauge_latest(&cog_core::metric_names::METRICS_SAMPLES_ROWS)
            .await
            .unwrap();
        if let Some(sample) = series.into_iter().next() {
            labels = Some(sample.labels);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    shutdown.trigger();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    assert_eq!(
        labels.expect("the loop must publish what the log holds"),
        std::collections::HashMap::new(),
        "a blank deployment name is no name, not an empty one"
    );

    drop_probe(&pool, table).await;
}
