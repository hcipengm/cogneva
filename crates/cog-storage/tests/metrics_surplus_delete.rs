//! What one pruning statement costs, and which rows it takes, against a live
//! PostgreSQL.
//!
//! The sweep deletes in batches, so the batch is the only bound on a single
//! statement's work — and whether that bound holds is a property of the plan,
//! which is not visible without a server. Ranking every row of the log first,
//! `row_number() OVER (PARTITION BY metric_type, name, labels ...)` over the
//! unfiltered table, asks the per-series question for rows the pass is not
//! going to touch: measured over 200,100 rows it windowed all of them, spilled
//! 15 MB to a temporary file, and took 1.7 s to delete 50 rows. The correlated
//! form asks the same question of one candidate row at a time, inside the
//! `LIMIT`.
//!
//! Two things are checked here. The pass takes exactly the rows the ranked
//! statement took — cheaper and different would not be a rewrite — and what it
//! costs follows the batch rather than the amount of history behind it.
//!
//! The probe table is a clone of the backend's own schema, taken with `CREATE
//! TABLE ... (LIKE cog_metrics_samples INCLUDING ALL)` after
//! [`PostgresMetricsBackend::init_schema`], because the index the correlated
//! form leans on is the one production creates: a probe table with a hand-made
//! index would go on passing after `init_schema` stopped creating it. It is a
//! table of its own because the sample log's other live test drops and
//! recreates the real one. Dropped at the end, so the database is left as it
//! was found.
//!
//! Ignored by default, and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test metrics_surplus_delete -- --ignored
//! ```

use std::collections::HashSet;

use serde_json::Value;
use sqlx::{Connection, Executor, PgConnection, PgPool};

use cog_storage::metrics_sample_cap::SAMPLES_TABLE;
use cog_storage::{delete_surplus_sql, PostgresMetricsBackend, SampleLogCap};

/// Rows in the large probe table. Large enough that a batch is a rounding error
/// on the log, which is what makes the size question answerable: this many rows
/// behind a 50-row batch.
const BIG_ROWS: i64 = 200_000;

/// The oldest rows of the large fixture, copied verbatim into the small probe
/// table so both tables answer the same batch on the same rows and only the
/// amount of history behind them differs.
const SMALL_ROWS: i64 = 5_000;

/// The batch the cost question is asked with.
const BATCH: i64 = 50;

/// Rows one pass takes in the row-set question: more than the fixture's oldest
/// group, so the batch is not all one kind.
const TAKEN: i64 = 2_500;

/// The table the row-set question uses, and the answer the cost question is
/// asked of. Named separately so both tests can run at once, which they do.
const ROWS_PROBE: &str = "cap_probe_surplus_rows";
const BIG_PROBE: &str = "cap_probe_surplus_big";
const SMALL_PROBE: &str = "cap_probe_surplus_small";

/// `init_schema` creates the log with `IF NOT EXISTS`, and two sessions running
/// that for the same table at once is a race the server settles by failing one
/// of them. Both tests need the schema to clone, so they take turns.
static SCHEMA_SETUP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// Create `table` with the backend's own columns and indexes.
async fn fresh_probe(pool: &PgPool, table: &str) {
    let _setup = SCHEMA_SETUP.lock().await;
    PostgresMetricsBackend::new(pool.clone())
        .init_schema()
        .await
        .unwrap();
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {table} (LIKE {SAMPLES_TABLE} INCLUDING ALL)"
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

/// The shapes the live log holds, in the order a sweep meets them: a histogram
/// and a counter series keyed on an object id (every row of theirs is
/// deletable), a gauge series that stopped being written so its newest row is
/// protected at the old end of the table, one name published by two kinds at
/// once, and the long gauge series that hold most of the rows.
async fn seed(pool: &PgPool, table: &str, rows: i64) {
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'histogram', 'probe_operation_ms', g::float8,
                jsonb_build_object('bucket', (g % 20)::text),
                NOW() - make_interval(secs => 600000 + g::float8)
         FROM generate_series(0, 299) AS g"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'counter', 'probe_writes_total', g::float8,
                jsonb_build_object('object', (g % 50)::text),
                NOW() - make_interval(secs => 550000 + g::float8)
         FROM generate_series(0, 1999) AS g"
    ))
    .execute(pool)
    .await
    .unwrap();
    // A counter under a name a gauge also publishes: the kind decides which rows
    // are history, not the name.
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'counter', 'probe_shared', g::float8, '{{}}'::jsonb,
                NOW() - make_interval(secs => 500000 + g::float8)
         FROM generate_series(0, 4) AS g"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'gauge', 'probe_shared', 1.0, '{{}}'::jsonb,
                NOW() - make_interval(secs => 490000)
         FROM generate_series(0, 0) AS g"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'gauge', 'probe_stopped', g::float8,
                jsonb_build_object('series', (g % 3)::text),
                NOW() - make_interval(secs => 400000 + g::float8)
         FROM generate_series(0, 59) AS g"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {table} (metric_type, name, value, labels, timestamp)
         SELECT 'gauge', 'probe_busy', g::float8,
                jsonb_build_object('series', (g % 40)::text),
                NOW() - make_interval(secs => ($1 - g)::float8 / 1.7)
         FROM generate_series(0, $1 - 1) AS g"
    ))
    .bind(rows)
    .execute(pool)
    .await
    .unwrap();
}

/// The statement a pass ran before this one: rank every row of the table, then
/// keep the first rank of every gauge series. It is the reference the rewrite
/// has to agree with, so it is spelled out here rather than left to git.
fn ranked_form(table: &str, limit: i64) -> String {
    format!(
        "SELECT id FROM (
             SELECT id, metric_type, timestamp,
                    row_number() OVER (
                        PARTITION BY metric_type, name, labels
                        ORDER BY timestamp DESC, id DESC
                    ) AS newest_rank
             FROM {table}
         ) ranked
         WHERE metric_type <> 'gauge' OR newest_rank > 1
         ORDER BY timestamp, id
         LIMIT {limit}"
    )
}

async fn ids(pool: &PgPool, statement: &str) -> Vec<i32> {
    sqlx::query_scalar(statement).fetch_all(pool).await.unwrap()
}

async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

/// The plan of the statement the sweep runs, built from the producer's own text
/// on a session of its own: a prepared statement does not outlive its session,
/// and a fresh one keeps the pool's connections out of it.
async fn explain_pass(table: &str) -> Value {
    let mut conn = PgConnection::connect(&database_url()).await.unwrap();
    let prepared = format!("PREPARE probe_surplus AS {}", delete_surplus_sql(table));
    conn.execute(prepared.as_str()).await.unwrap();
    // `EXPLAIN EXECUTE` takes the batch as a literal; the statement being
    // explained is still the one the sweep runs.
    let explained =
        format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE probe_surplus({BATCH})");
    let plan: Value = sqlx::query_scalar(explained.as_str())
        .fetch_one(&mut conn)
        .await
        .unwrap();
    plan[0]["Plan"].clone()
}

fn walk<'a>(node: &'a Value, out: &mut Vec<&'a Value>) {
    out.push(node);
    if let Some(children) = node.get("Plans").and_then(|v| v.as_array()) {
        for child in children {
            walk(child, out);
        }
    }
}

fn nodes(plan: &Value) -> Vec<&Value> {
    let mut found = Vec::new();
    walk(plan, &mut found);
    found
}

fn node_kinds(plan: &Value) -> Vec<String> {
    nodes(plan)
        .iter()
        .filter_map(|n| n.get("Node Type").and_then(|v| v.as_str()))
        .map(str::to_string)
        .collect()
}

/// Blocks read or hit by the whole plan. The outermost node's reading is the
/// plan's own subtree; a subplan reports separately and is not part of it, so
/// the subplans are added rather than left out.
fn shared_blocks(plan: &Value) -> u64 {
    nodes(plan)
        .iter()
        .filter(|n| {
            matches!(
                n.get("Parent Relationship").and_then(|v| v.as_str()),
                Some("InitPlan") | Some("SubPlan")
            )
        })
        .map(|n| blocks(n))
        .sum::<u64>()
        + blocks(plan)
}

/// Whether a node hangs off the plan the delete walks rather than off a
/// subplan: the correlated lookup has a `Limit 1` of its own, and asking which
/// of the two is the batch means reading this.
fn hangs_off_the_delete(node: &Value) -> bool {
    !matches!(
        node.get("Parent Relationship").and_then(|v| v.as_str()),
        Some("InitPlan") | Some("SubPlan")
    )
}

fn blocks(node: &Value) -> u64 {
    ["Shared Hit Blocks", "Shared Read Blocks"]
        .iter()
        .filter_map(|key| node.get(key).and_then(|v| v.as_u64()))
        .sum()
}

/// Blocks written to a temporary file anywhere in the plan. Every node's own
/// count is taken, because a spill inside a subplan is reported there and not on
/// the node the subplan hangs from.
fn temp_written(plan: &Value) -> u64 {
    nodes(plan)
        .iter()
        .filter_map(|n| n.get("Temp Written Blocks").and_then(|v| v.as_u64()))
        .sum()
}

/// The most rows any single node read.
fn widest_read(plan: &Value) -> u64 {
    nodes(plan)
        .iter()
        .filter_map(|n| {
            n.get("Actual Rows").and_then(|v| v.as_u64()).map(|rows| {
                rows.saturating_mul(n.get("Actual Loops").and_then(|v| v.as_u64()).unwrap_or(1))
            })
        })
        .max()
        .unwrap_or(0)
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_pass_takes_the_same_rows_the_ranked_statement_took() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, ROWS_PROBE).await;
    seed(&pool, ROWS_PROBE, 2_000).await;

    let held = count(&pool, ROWS_PROBE).await;
    assert!(
        held > TAKEN + 500,
        "the fixture has no surplus to prune: {held} rows held, {TAKEN} asked for"
    );

    // The reference, taken before anything is deleted: the oldest rows of the
    // log that are not a gauge series' newest, ranking the whole table first.
    let mut expected = ids(&pool, &ranked_form(ROWS_PROBE, TAKEN)).await;
    assert_eq!(
        expected.len() as i64,
        TAKEN,
        "the ranked statement did not find a full batch to compare against"
    );

    // What the batch is made of, so agreement on a one-sided or empty set
    // cannot pass as agreement: the fixture puts a gauge row in it (the
    // protected ones were skipped, not the gauge kind), a counter row, and a
    // histogram row.
    let kinds: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT DISTINCT metric_type FROM {ROWS_PROBE} WHERE id = ANY($1)"
    ))
    .bind(&expected)
    .fetch_all(&pool)
    .await
    .unwrap();
    for kind in ["gauge", "counter", "histogram"] {
        assert!(
            kinds.iter().any(|k| k == kind),
            "the fixture does not ask the question for {kind}: {kinds:?}"
        );
    }

    let before: HashSet<i32> = ids(&pool, &format!("SELECT id FROM {ROWS_PROBE}"))
        .await
        .into_iter()
        .collect();
    let outcome = SampleLogCap::new(pool.clone(), (held - TAKEN) as u64)
        .with_table(ROWS_PROBE)
        .sweep_once()
        .await
        .unwrap();
    assert_eq!(
        outcome.removed as i64, TAKEN,
        "the pass took a different number of rows than the batch it was asked for"
    );
    let after: HashSet<i32> = ids(&pool, &format!("SELECT id FROM {ROWS_PROBE}"))
        .await
        .into_iter()
        .collect();
    let mut taken: Vec<i32> = before.difference(&after).copied().collect();
    taken.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        taken.len() as i64,
        TAKEN,
        "the pass removed rows but not the batch it reported"
    );
    assert_eq!(
        taken, expected,
        "the pass did not take the rows the ranked statement took"
    );

    drop_probe(&pool, ROWS_PROBE).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_batch_costs_the_batch_and_not_the_log_it_is_cut_from() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    fresh_probe(&pool, BIG_PROBE).await;
    fresh_probe(&pool, SMALL_PROBE).await;
    seed(&pool, BIG_PROBE, BIG_ROWS).await;
    sqlx::query(&format!(
        "INSERT INTO {SMALL_PROBE} (id, metric_type, name, value, labels, timestamp)
         SELECT id, metric_type, name, value, labels, timestamp
         FROM {BIG_PROBE} ORDER BY timestamp, id LIMIT $1"
    ))
    .bind(SMALL_ROWS)
    .execute(&pool)
    .await
    .unwrap();
    for table in [BIG_PROBE, SMALL_PROBE] {
        sqlx::query(&format!("ANALYZE {table}"))
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(
        count(&pool, BIG_PROBE).await,
        BIG_ROWS + 2_366,
        "the large fixture is not the size the cost question is asked of"
    );

    let big = explain_pass(BIG_PROBE).await;
    let small = explain_pass(SMALL_PROBE).await;

    let kinds = node_kinds(&big);
    assert!(
        !kinds.iter().any(|kind| kind == "WindowAgg"),
        "the pass ranked the whole log to delete a batch of it: {kinds:?}"
    );
    assert_eq!(
        temp_written(&big),
        0,
        "the pass spilled the log to a temporary file to delete a batch of it: {kinds:?}"
    );
    // The batch is the batch: one `Limit` outside the subplans, answering with
    // exactly as many rows as it was asked for. (The correlated lookup has a
    // `Limit 1` of its own, which reports as a subplan.)
    let batch_limits: Vec<&Value> = nodes(&big)
        .into_iter()
        .filter(|n| {
            n.get("Node Type").and_then(|v| v.as_str()) == Some("Limit") && hangs_off_the_delete(n)
        })
        .collect();
    assert_eq!(batch_limits.len(), 1, "the pass has no single batch bound");
    assert_eq!(
        batch_limits[0].get("Actual Rows").and_then(|v| v.as_u64()),
        Some(BATCH as u64),
        "the pass did not answer with the batch it was asked for"
    );
    // Nothing in the plan may read the log, whatever shape it takes: 100 rows
    // per row deleted is far above the measured 51 rows walked for a batch of
    // 50, and far below the 200,200 the ranked statement's scan read.
    let widest = widest_read(&big);
    assert!(
        widest <= BATCH as u64 * 100,
        "a node in the pass read {widest} rows to delete {BATCH}: {kinds:?}"
    );
    // The per-candidate question is answered by an index on the series, which is
    // what keeps it one probe rather than a scan of the log per row.
    assert!(
        nodes(&big).iter().any(|n| {
            let cond = n.get("Index Cond").and_then(|v| v.as_str()).unwrap_or("");
            cond.contains("name = ") && cond.contains("labels = ") && cond.contains("metric_type")
        }),
        "the per-row question was not answered by the newest-per-series index: {kinds:?}"
    );

    // Cost against size: the same batch on the same oldest rows, with 40x the
    // history behind them. Measured 470 blocks over 200,200 rows against 269
    // over 5,000 -- 1.75x for 40x the log -- so three times is the ceiling here:
    // room above the measurement for a different plan, and far below what
    // tracking the log looks like (the ranked statement's 2,546 blocks plus
    // 15 MB of spill).
    let big_blocks = shared_blocks(&big);
    let small_blocks = shared_blocks(&small);
    assert!(
        big_blocks <= small_blocks * 3,
        "a batch over {BIG_ROWS} rows cost {big_blocks} blocks against {small_blocks} \
         over {SMALL_ROWS}, so its cost follows the log rather than the batch"
    );

    drop_probe(&pool, BIG_PROBE).await;
    drop_probe(&pool, SMALL_PROBE).await;
}
