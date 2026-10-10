//! The per-window fold of the LLM usage ledger, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that a window's token
//! composition is the ledger's own sum over that window, that rerunning a window
//! lands the same rows with the same values (the fold's idempotence rests on the
//! primary key, a fact about the server, not about our code), and that a
//! combination the ledger knows but the window did not use still lands a zero
//! row — the row that tells "no traffic" apart from "the fold did not run".
//!
//! None of it can be checked without a server, so this file is ignored by
//! default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-observability --test usage_rollup -- --ignored
//! ```
//!
//! It cleans up after itself by actor marker, so the database is left as found.

use chrono::{DateTime, Duration, Utc};
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore, RollupWindow};
use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// Create the shared ledger and rollup tables under a session advisory lock.
///
/// The tests in this file run in parallel (the default) and each calls
/// `init_schema`, whose `CREATE TABLE IF NOT EXISTS` statements race on a fresh
/// database — the loser hits `pg_type_typname_nsp_index`. The lock makes the
/// DDL mutually exclusive: the second test waits for the first instead of
/// colliding with it.
async fn init_schema_once(store: &LlmUsageStore, pool: &PgPool) {
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(90_565_001i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    store.init_schema().await.expect("init the schema");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(90_565_001i64)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

/// Start of the hour at or before `now`.
fn hour_floor(now: DateTime<Utc>) -> DateTime<Utc> {
    let w = 3600;
    DateTime::from_timestamp(now.timestamp() - now.timestamp().rem_euclid(w), 0).unwrap()
}

/// The folded row for one `(window, actor, upstream)`, as a tuple.
async fn folded(
    pool: &PgPool,
    upstream: &str,
    window_start: DateTime<Utc>,
    actor: &str,
) -> Vec<(i64, i64, i64, i64, i64)> {
    sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT calls, failed_calls, tokens_input, tokens_output, tokens_cached \
         FROM cog_llm_usage_rollup \
         WHERE window_start = $1 AND actor = $2 AND upstream = $3",
    )
    .bind(window_start)
    .bind(actor)
    .bind(upstream)
    .fetch_all(pool)
    .await
    .expect("read the folded rows")
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_window_folds_the_ledger_and_reruns_in_place_with_zeros_for_the_idle() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    init_schema_once(&store, &pool).await;

    let run = uuid::Uuid::new_v4();
    let busy = format!("rollup-test-busy:{run}");
    let idle = format!("rollup-test-idle:{run}");
    let upstream = format!("rollup-test-upstream:{run}");
    let api_style = "openai".to_string();

    // A closed window, two hours back, so nothing the test writes can fall into
    // a window that is still open.
    let hour = hour_floor(Utc::now());
    let w = RollupWindow {
        start: hour - Duration::hours(2),
        end: hour - Duration::hours(1),
    };

    let record = |actor: &str, result: &str, input: u64, output: u64, cached: u64| LlmUsageRecord {
        upstream: upstream.clone(),
        api_style: api_style.clone(),
        model: "rollup-test-model".to_string(),
        result: result.to_string(),
        actor: actor.to_string(),
        tokens_input: input,
        tokens_output: output,
        tokens_cached: cached,
        latency_ms: 10,
    };

    // Two successful calls and one failed one inside the window.
    for r in [
        record(&busy, "ok", 100, 10, 40),
        record(&busy, "ok", 200, 20, 0),
        record(&busy, "error", 0, 0, 0),
    ] {
        store.record(&r).await.expect("record a windowed call");
    }
    // One call by a second actor, well outside the window: the ledger knows it,
    // so the window owes it a zero row.
    store
        .record(&record(&idle, "ok", 500, 5, 0))
        .await
        .expect("record the out-of-window call");

    // `record` stamps the row with the server's now, so the clock is moved to
    // the window afterwards rather than written around. The write path stays the
    // one under test; only the timestamp is posed.
    sqlx::query("UPDATE gateway_llm_usage SET ts = $1 WHERE actor = $2")
        .bind(w.start + Duration::minutes(10))
        .bind(&busy)
        .execute(&pool)
        .await
        .expect("place the busy actor's calls in the window");
    sqlx::query("UPDATE gateway_llm_usage SET ts = $1 WHERE actor = $2")
        .bind(w.start - Duration::hours(1))
        .bind(&idle)
        .execute(&pool)
        .await
        .expect("place the idle actor's call outside the window");

    // First fold.
    let first = store.rollup_windows(&[w]).await.expect("fold the window");

    let busy_rows = folded(&pool, &upstream, w.start, &busy).await;
    assert_eq!(busy_rows.len(), 1, "the used combination folds to one row");
    assert_eq!(
        busy_rows[0],
        (3, 1, 300, 30, 40),
        "the window's totals are the ledger's sum: three calls, one failed, \
         with the cache split kept beside the input"
    );

    let idle_rows = folded(&pool, &upstream, w.start, &idle).await;
    assert_eq!(
        idle_rows.len(),
        1,
        "a known combination the window did not use still lands a row"
    );
    assert_eq!(
        idle_rows[0],
        (0, 0, 0, 0, 0),
        "and that row is zero, which is what makes an outage legible"
    );

    // Second fold: same window, same rows, same values.
    let count_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cog_llm_usage_rollup WHERE window_start = $1 AND upstream = $2",
    )
    .bind(w.start)
    .bind(&upstream)
    .fetch_one(&pool)
    .await
    .expect("count before the rerun");

    let second = store
        .rollup_windows(&[w])
        .await
        .expect("fold the window again");

    let count_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cog_llm_usage_rollup WHERE window_start = $1 AND upstream = $2",
    )
    .bind(w.start)
    .bind(&upstream)
    .fetch_one(&pool)
    .await
    .expect("count after the rerun");

    assert_eq!(
        (first, count_before),
        (second, count_after),
        "a rerun writes the same number of rows and leaves the table the same size"
    );
    assert_eq!(
        folded(&pool, &upstream, w.start, &busy).await[0],
        (3, 1, 300, 30, 40),
        "and the same values, because the fold is a function of the ledger"
    );

    // The reader A1 promises returns the same composition the fold landed.
    let spend = store
        .usage_by_actor(w.start, w.end)
        .await
        .expect("read a window");
    let busy_line = spend
        .iter()
        .find(|s| s.actor == busy)
        .expect("the busy actor is in the window's composition");
    assert_eq!(busy_line.calls, 3);
    assert_eq!(busy_line.failed_calls, 1);
    assert_eq!(busy_line.tokens_input, 300);
    assert_eq!(busy_line.tokens_output, 30);
    assert_eq!(busy_line.tokens_cached, 40);

    for actor in [&busy, &idle] {
        sqlx::query("DELETE FROM gateway_llm_usage WHERE actor = $1")
            .bind(actor)
            .execute(&pool)
            .await
            .expect("clean up the ledger probe rows");
    }
    sqlx::query("DELETE FROM cog_llm_usage_rollup WHERE upstream = $1")
        .bind(&upstream)
        .execute(&pool)
        .await
        .expect("clean up the folded probe rows");
}

/// The rolled read face of A5: it answers from the fold, sums the windows that
/// fall inside the range, and leaves out a window the range does not cover —
/// so a spend question does not scan the per-call ledger and a partial window
/// is never reported as a short one.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_rolled_reader_sums_the_fold_over_the_windows_in_range() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    init_schema_once(&store, &pool).await;

    let run = uuid::Uuid::new_v4();
    let actor = format!("rolled-read-test:{run}");
    let upstream = format!("rolled-read-upstream:{run}");
    let api_style = "openai".to_string();

    let hour = hour_floor(Utc::now());
    let old = RollupWindow {
        start: hour - Duration::hours(4),
        end: hour - Duration::hours(3),
    };
    let w1 = RollupWindow {
        start: hour - Duration::hours(3),
        end: hour - Duration::hours(2),
    };
    let w2 = RollupWindow {
        start: hour - Duration::hours(2),
        end: hour - Duration::hours(1),
    };

    let record = |input: u64, output: u64, latency: u64| LlmUsageRecord {
        upstream: upstream.clone(),
        api_style: api_style.clone(),
        model: "rolled-read-model".to_string(),
        result: "ok".to_string(),
        actor: actor.clone(),
        tokens_input: input,
        tokens_output: output,
        tokens_cached: 0,
        latency_ms: latency,
    };

    // Two calls in the first in-range window, one in the second, and one in a
    // window the range excludes. Each is given a distinct input count so the
    // rows can be placed one by one.
    for r in [
        record(100, 10, 100),
        record(200, 20, 100),
        record(400, 40, 400),
        record(800, 80, 999),
    ] {
        store.record(&r).await.expect("record a call");
    }

    // `record` stamps the server's now; move the clock afterwards rather than
    // writing around it, so the write path stays the one under test.
    for (window, input) in [(old, 800i64), (w1, 100), (w1, 200), (w2, 400)] {
        sqlx::query("UPDATE gateway_llm_usage SET ts = $1 WHERE actor = $2 AND tokens_input = $3")
            .bind(window.start + Duration::minutes(10))
            .bind(&actor)
            .bind(input)
            .execute(&pool)
            .await
            .expect("place a call in its window");
    }

    store
        .rollup_windows(&[old, w1, w2])
        .await
        .expect("fold the windows");

    // The range covers the two newer windows but not the older one.
    let rolled = store
        .usage_by_actor_rolled(w1.start, w2.end)
        .await
        .expect("read the rolled spend");
    let line = rolled
        .iter()
        .find(|s| s.actor == actor)
        .expect("the actor is present in the range");
    assert_eq!(
        line.calls, 3,
        "two calls in the first window and one in the second"
    );
    assert_eq!(
        line.tokens_input, 700,
        "100 + 200 + 400, the older window left out"
    );
    assert_eq!(line.tokens_output, 70, "10 + 20 + 40");
    assert_eq!(
        line.avg_latency_ms, 200,
        "the mean is over the calls, (100+100+400)/3, not the mean of the \
         per-window means, (100+400)/2"
    );

    // A range that stops before the second window leaves it out.
    let first_only = store
        .usage_by_actor_rolled(w1.start, w1.end)
        .await
        .expect("read the first window only");
    let line1 = first_only
        .iter()
        .find(|s| s.actor == actor)
        .expect("the actor is present");
    assert_eq!(line1.calls, 2, "only the first window is in range");
    assert_eq!(line1.tokens_input, 300, "100 + 200");

    sqlx::query("DELETE FROM gateway_llm_usage WHERE actor = $1")
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("clean up the ledger probe rows");
    sqlx::query("DELETE FROM cog_llm_usage_rollup WHERE upstream = $1")
        .bind(&upstream)
        .execute(&pool)
        .await
        .expect("clean up the folded probe rows");
}
