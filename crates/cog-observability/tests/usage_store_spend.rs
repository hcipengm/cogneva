//! The money read: what a range of the ledger cost, against a live PostgreSQL.
//!
//! The projection in `cog-gateway` is tested without a database, which leaves
//! the one thing that can actually be wrong untested there: the statement. A
//! `GROUP BY` that forgets `price_book` returns a plausible total no price list
//! produced; a `SUM` wrapped in `COALESCE` reports an unpriced range as free.
//! Neither shows up as a type error, and both read correctly on a page.
//!
//! Ignored by default for the same reason the ledger round-trip beside it is:
//! it needs a throwaway database.
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-observability --test usage_store_spend -- --ignored
//! ```

use chrono::{Duration, Utc};
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore};
use sqlx::PgPool;

use cog_test_support::database_url;

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_money_read_splits_by_price_book_and_keeps_unpriced_calls_countable() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    // A per-run upstream keeps these rows apart from anything else in the
    // database, so the assertions hold on a table that is not empty.
    let upstream = format!("spend-test:{}", uuid::Uuid::new_v4());

    let record = |book: Option<&str>, micros: Option<i64>, cached: u64| LlmUsageRecord {
        refused_scope: None,
        upstream: upstream.clone(),
        api_style: "openai".to_string(),
        model: "spend-test-model".to_string(),
        result: "ok".to_string(),
        actor: "agent:test".to_string(),
        origin: "sandbox".to_string(),
        tokens_input: 1_000,
        tokens_output: 100,
        tokens_cached: cached,
        tokens_cache_created: None,
        tokens_reasoning: None,
        estimated_input: None,
        usage_outcome: None,
        cost_micros: micros,
        price_book: book.map(str::to_string),
        refusal_kind: None,
        retry_after_secs: None,
        task: None,
        owner: None,
        latency_ms: 1,
        pace: None,
    };

    // Three rows, two books: an old one and a new one, plus a row nobody
    // priced. A sum over all four that mixed the books would answer with a
    // number neither one produced.
    for row in [
        record(Some("CNY@2026-10-01"), Some(1_000), 10_000),
        record(Some("CNY@2026-11-05"), Some(2_000), 10_000),
        record(None, None, 0),
    ] {
        store.record(&row).await.expect("record the call");
    }

    let since = Utc::now() - Duration::minutes(5);
    let until = Utc::now() + Duration::minutes(5);
    let spend = store
        .priced_spend_between(since, until)
        .await
        .expect("read the range");

    let mine: Vec<_> = spend
        .rows
        .iter()
        .filter(|r| r.upstream == upstream)
        .collect();
    assert_eq!(mine.len(), 3, "两版价目各一组，没价的一组单独一组");

    let priced: i64 = mine.iter().filter_map(|r| r.cost_micros).sum();
    assert_eq!(priced, 3_000, "两版价目各自成行，钱是二者之和");
    assert_eq!(
        mine.iter().map(|r| r.priced_calls).sum::<i64>(),
        2,
        "只有两行带着价"
    );

    let unpriced = mine
        .iter()
        .find(|r| r.price_book.is_none())
        .expect("没价的那一行必须在结果里，而不是消失");
    assert_eq!(unpriced.cost_micros, None, "没价是 NULL，不是 0");
    assert_eq!(unpriced.priced_calls, 0);
    assert_eq!(unpriced.calls, 1);

    // Coverage is the rows' own span, and it must sit inside the requested one.
    let covered_since = spend.covered_since.expect("this range has rows");
    assert!(covered_since >= since);
    assert!(covered_since <= Utc::now());

    // A range with no rows reports no coverage rather than a zero-length span
    // at the requested endpoints.
    let empty = store
        .priced_spend_between(
            Utc::now() - Duration::days(2),
            Utc::now() - Duration::days(1) - Duration::minutes(1),
        )
        .await
        .expect("read an empty range");
    assert!(empty.covered_since.is_none());
    assert!(empty.covered_until.is_none());

    sqlx::query("DELETE FROM gateway_llm_usage WHERE upstream = $1")
        .bind(&upstream)
        .execute(&pool)
        .await
        .expect("clean up the rows this test wrote");
}
