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
    store.init_schema().await.expect("init the schema");

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
