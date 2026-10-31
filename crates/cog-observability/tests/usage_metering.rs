//! What reading the response found, against a live PostgreSQL.
//!
//! A successful call the upstream answered without a usage frame lands on the
//! ledger as `0 / 0 / 0` -- the same row a call that truly served nothing
//! produces. Summed into a window the two are one number, so the word saying
//! which happened has to survive the round trip on the row itself, and the read
//! has to hand it back per cell.
//!
//! None of that can be checked without a server: the column is added to a table
//! that already holds rows, and "the rows written before it existed read as no
//! reading" is a fact about PostgreSQL's ALTER with no default, not about our
//! code. A statement that can never run keeps every DB-free test green, which is
//! why this file is ignored by default and needs `COGNEVA_TEST_DATABASE_URL`
//! pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-observability --test usage_metering -- --ignored
//! ```
//!
//! The rows it writes carry a per-run marker and are deleted at the end, so the
//! database is left as it was found.

use cog_core::usage_ledger::MeteringOutcome;
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore};
use sqlx::PgPool;

use cog_test_support::database_url;

fn row(actor: &str, result: &str, outcome: Option<MeteringOutcome>) -> LlmUsageRecord {
    LlmUsageRecord {
        upstream: "metering-test-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "metering-test-model".to_string(),
        result: result.to_string(),
        actor: actor.to_string(),
        origin: "gateway_pool".to_string(),
        tokens_input: 0,
        tokens_output: 0,
        tokens_cached: 0,
        tokens_cache_created: 0,
        usage_outcome: outcome,
        estimated_input: None,
        refusal_kind: None,
        retry_after_secs: None,
        task: None,
        owner: None,
        latency_ms: 1,
        pace: None,
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_reading_a_successful_call_carried_reaches_the_ledger_and_comes_back_per_cell() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    let run = uuid::Uuid::new_v4();
    let spoke = format!("metering-test-read:{run}");
    let silent = format!("metering-test-absent:{run}");
    let legacy = format!("metering-test-legacy:{run}");
    let failed = format!("metering-test-error:{run}");

    let runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    store.init_schema(&runner).await.expect("init the schema");

    store
        .record(&row(&spoke, "ok", Some(MeteringOutcome::Read)))
        .await
        .expect("write a call whose upstream spoke");
    store
        .record(&row(&silent, "ok", Some(MeteringOutcome::Absent)))
        .await
        .expect("write a call whose upstream said nothing");
    // A row written before the column existed, which is the shape the ALTER
    // leaves behind: no reading, and not a reading of nothing.
    store
        .record(&row(&legacy, "ok", None))
        .await
        .expect("write a row with no reading");
    // A failed attempt never got a body, so it is excluded from the read rather
    // than counted as unmetered: it spends nothing rather than an unknown.
    store
        .record(&row(&failed, "error", None))
        .await
        .expect("write a failed attempt");

    let until = chrono::Utc::now() + chrono::Duration::hours(1);
    let since = until - chrono::Duration::hours(2);
    let cells = store
        .metering_between(since, until)
        .await
        .expect("read the metering cells");
    let mine: Vec<(String, i64)> = cells.into_iter().map(|c| (c.outcome, c.calls)).collect();
    let count = |word: &str| {
        mine.iter()
            .filter(|(outcome, _)| outcome == word)
            .map(|(_, calls)| *calls)
            .sum::<i64>()
    };
    // The whole table's cells are read, not just this run's, so each cell is
    // checked against the row this run wrote rather than against a total.
    assert!(
        count("read") >= 1,
        "a call whose frame arrived is counted as read: {mine:?}"
    );
    assert!(
        count("absent") >= 1,
        "a call whose upstream said nothing is counted as absent, not dropped: {mine:?}"
    );
    assert!(
        count("") >= 1,
        "a row with no reading is its own line rather than being folded into read: {mine:?}"
    );

    // The failed attempt is not on this face at all: its actor appears on no
    // cell, which is how "never got a body" stays out of the denominator.
    let failed_cells = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM gateway_llm_usage WHERE actor = $1 AND result = 'ok'",
    )
    .bind(&failed)
    .fetch_one(&pool)
    .await
    .expect("count the failed attempt's ok rows");
    assert_eq!(failed_cells, 0, "the failed row is not an ok row");

    // And the three readings survive the round trip as three distinct values.
    let stored = sqlx::query_scalar::<_, Option<String>>(
        "SELECT usage_outcome FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&silent)
    .fetch_one(&pool)
    .await
    .expect("read back the silent call");
    assert_eq!(
        stored.as_deref(),
        Some("absent"),
        "the word travels on the row, so a later reader can tell this zero from a measured one"
    );
    let missing = sqlx::query_scalar::<_, Option<String>>(
        "SELECT usage_outcome FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&legacy)
    .fetch_one(&pool)
    .await
    .expect("read back the row written without a reading");
    assert_eq!(missing, None, "no reading is NULL, not a guessed cell");

    for actor in [&spoke, &silent, &legacy, &failed] {
        sqlx::query("DELETE FROM gateway_llm_usage WHERE actor = $1")
            .bind(actor)
            .execute(&pool)
            .await
            .expect("clean up the probe rows");
    }
}
