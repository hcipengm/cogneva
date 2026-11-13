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
        session: None,
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

/// 一段对话花了多少：账本按 `session_id` 这一列折叠，不另记一本账。
///
/// 这条判的是**语句**，和上面那条同一个理由：`WHERE session_id = $1` 少写一个
/// 条件、或者把 NULL 当成「未归属的那一段」，读出来的都会是一个很像真数的合计
/// ——两段会话的钱并在一起，屏幕上每一段都变贵一倍，而没有任何类型错误。
///
/// 三件必须分开的事：别的会话的行不进这一段、没有会话归属的行（后台工作）不进
/// 任何一段、问一段账上没有的会话得到 `None` 而不是一份零。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_session_read_folds_only_that_conversation() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    let upstream = format!("session-spend-test:{}", uuid::Uuid::new_v4());
    let this = format!("session:{}", uuid::Uuid::new_v4());
    let other = format!("session:{}", uuid::Uuid::new_v4());

    let record = |session: Option<&str>, book: Option<&str>, micros: Option<i64>, cached: u64| {
        LlmUsageRecord {
            refused_scope: None,
            upstream: upstream.clone(),
            api_style: "openai".to_string(),
            model: "session-spend-test-model".to_string(),
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
            session: session.map(str::to_string),
            latency_ms: 1,
            pace: None,
        }
    };

    for row in [
        // 这一段：两次有价的调用（两版价目）+ 一次没价的 + 一次有价没钱币的。
        // 最后那一行是这台机器真实会写出来的形状之一（算出了钱却没记价目本）：
        // 它进不了任何币种的合计，因此必须落进「没定价」，不能凭空消失。
        record(Some(&this), Some("CNY@2026-10-01"), Some(1_000), 10),
        record(Some(&this), Some("CNY@2026-11-05"), Some(2_000), 20),
        record(Some(&this), None, None, 0),
        record(Some(&this), None, Some(500), 0),
        // 另一段：不许被算进来。
        record(Some(&other), Some("CNY@2026-11-05"), Some(99_000), 30),
        // 没有会话归属的后台工作：也不属于任何一段。
        record(None, Some("CNY@2026-11-05"), Some(50_000), 40),
    ] {
        store.record(&row).await.expect("record the call");
    }

    let spend = store
        .spend_for_session(&this)
        .await
        .expect("read the conversation")
        .expect("这一段账上有行");
    assert_eq!(spend.calls, 4, "只有这一段的四行：{}", spend.calls);
    assert_eq!(spend.tokens_input, 4_000);
    assert_eq!(spend.tokens_output, 400);
    assert_eq!(spend.tokens_cached, 30, "缓存读的是这一段的行");
    let money: i64 = spend.money.iter().map(|line| line.cost_micros).sum();
    assert_eq!(money, 3_000, "另一段与未归属的那些钱不许折进来");
    assert_eq!(
        spend
            .money
            .iter()
            .map(|line| line.priced_calls)
            .sum::<u64>(),
        2
    );
    assert_eq!(
        spend.unpriced_calls, 2,
        "没价的那一次、与有价却没记价目本的那一次，都要数得出来：漏掉后者会让\n         合计看起来是测量，其实只是两个数相加时少了一项"
    );

    let missing = store
        .spend_for_session("session:none-was-ever-here")
        .await
        .expect("read a conversation the ledger has no rows for");
    assert!(
        missing.is_none(),
        "账上没有这一段：答「没有」，不是一份零——零会被读成「这一段一分没花」"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE upstream = $1")
        .bind(&upstream)
        .execute(&pool)
        .await
        .expect("clean up the rows this test wrote");
}
