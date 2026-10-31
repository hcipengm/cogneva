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

use cog_test_support::database_url;

/// Create the shared ledger and rollup tables under a session advisory lock.
///
/// The tests in this file run in parallel (the default) and each calls
/// `init_schema`, whose `CREATE TABLE IF NOT EXISTS` statements race on a fresh
/// database — the loser hits `pg_type_typname_nsp_index`. The lock makes the
/// DDL mutually exclusive: the second test waits for the first instead of
/// colliding with it.
async fn init_schema_once(store: &LlmUsageStore, pool: &PgPool) {
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(90_565_001i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    store
        .init_schema(&schema_runner)
        .await
        .expect("init the schema");
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
        origin: "gateway_pool".to_string(),
        tokens_input: input,
        tokens_output: output,
        tokens_cached: cached,
        tokens_cache_created: 0,
        estimated_input: None,
        refusal_kind: None,
        retry_after_secs: None,
        task: None,
        owner: None,
        latency_ms: 10,
        pace: None,
        usage_outcome: None,
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
        count_before, count_after,
        "a rerun leaves this window's rows for the probe's upstream at the same count"
    );
    // What the rerun writes is not comparable between the two folds. The count
    // `rollup_windows` reports is one row for every combination the *whole*
    // ledger knows — the zero rows for idle combinations are most of it — so it
    // reads the vocabulary every case in this file contributes to, and the
    // second case here records its own combination and deletes it again when it
    // finishes. The number can therefore move in either direction across the
    // rerun without the fold having done anything different. All that is
    // assertable from here is the floor: this probe put two combinations in the
    // ledger before either fold, so each fold reports at least those two.
    for (written, name) in [(first, "the first fold"), (second, "the rerun")] {
        assert!(
            written >= 2,
            "{name} reports {written} rows written, fewer than the two \
             combinations this probe recorded, and the fold writes a row for \
             every combination the ledger knows"
        );
    }
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
        origin: "gateway_pool".to_string(),
        tokens_input: input,
        tokens_output: output,
        tokens_cached: 0,
        tokens_cache_created: 0,
        estimated_input: None,
        refusal_kind: None,
        retry_after_secs: None,
        task: None,
        owner: None,
        latency_ms: latency,
        pace: None,
        usage_outcome: None,
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
        .rows
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
    // The range reported is the one the rows were taken from — the window
    // edges, not the requested instants, which fall inside the first and last
    // windows and would name a range no window boundary matches.
    assert_eq!(rolled.covered_since, Some(w1.start));
    assert_eq!(rolled.covered_until, Some(w2.end));
    // And a window really was summed at each reported end: the oldest window
    // start and the newest window end under the reported range are the reported
    // ends themselves. A range that merely brackets the rows would pass the two
    // assertions above only by coincidence.
    let (lo, hi) = sqlx::query_as::<_, (Option<DateTime<Utc>>, Option<DateTime<Utc>>)>(
        "SELECT MIN(window_start), MAX(window_end) FROM cog_llm_usage_rollup \
         WHERE window_start >= $1 AND window_end <= $2",
    )
    .bind(rolled.covered_since.expect("a covered start"))
    .bind(rolled.covered_until.expect("a covered end"))
    .fetch_one(&pool)
    .await
    .expect("read the window edges under the reported range");
    assert_eq!(
        (lo, hi),
        (Some(w1.start), Some(w2.end)),
        "the reported endpoints are window boundaries the fold landed"
    );

    // A range that stops before the second window leaves it out.
    let first_only = store
        .usage_by_actor_rolled(w1.start, w1.end)
        .await
        .expect("read the first window only");
    let line1 = first_only
        .rows
        .iter()
        .find(|s| s.actor == actor)
        .expect("the actor is present");
    assert_eq!(line1.calls, 2, "only the first window is in range");
    assert_eq!(line1.tokens_input, 300, "100 + 200");
    assert_eq!(first_only.covered_since, Some(w1.start));
    assert_eq!(first_only.covered_until, Some(w1.end));

    // The trailing hour is the one a caller reaches for first and the one the
    // fold cannot have landed: it is the window that starts where the newest
    // folded window ends. Nothing is reported covered, which is what tells this
    // answer apart from an hour with no spend in it -- without the endpoints the
    // two are the same empty list.
    let unwrapped = store
        .usage_by_actor_rolled(w2.end, w2.end + Duration::hours(1))
        .await
        .expect("read a window the fold has not reached");
    assert!(
        unwrapped.covered_since.is_none() && unwrapped.covered_until.is_none(),
        "an unfolded window is not a covered one, got {:?}..{:?}",
        unwrapped.covered_since,
        unwrapped.covered_until
    );
    assert!(
        unwrapped.rows.is_empty(),
        "nothing was folded in that window, so no row may be summed from it"
    );

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

/// 一段区间里一次都没调用的组合，不是这段区间的一行。
///
/// 折叠表给每个见过的组合在每个窗口都落一行（零调用时是零行），好让「折过、
/// 但这一窗没人用」与「没折到这里」分得开——那是**一个窗口**的事实。把若干窗
/// 口加起来之后，这些零行变成「历史上跑过的全部调用方」，其中绝大多数在这段
/// 区间里什么都没干；按行数人头的人会把它们算成「在场」。两者必须分开，而分
/// 开的地方只能是求和这一面：逐窗口那一面还要靠零行画「量过但是零」的柱。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_summed_read_leaves_out_a_combination_the_range_never_used() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    init_schema_once(&store, &pool).await;

    let run = uuid::Uuid::new_v4();
    let actor = format!("idle-combo-test:{run}");
    let upstream = format!("idle-combo-upstream:{run}");

    let hour = hour_floor(Utc::now());
    let w0 = RollupWindow {
        start: hour - Duration::hours(3),
        end: hour - Duration::hours(2),
    };
    let w1 = RollupWindow {
        start: hour - Duration::hours(2),
        end: hour - Duration::hours(1),
    };

    store
        .record(&LlmUsageRecord {
            upstream: upstream.clone(),
            api_style: "openai".to_string(),
            model: "idle-combo-model".to_string(),
            result: "ok".to_string(),
            actor: actor.clone(),
            origin: "gateway_pool".to_string(),
            tokens_input: 10,
            tokens_output: 1,
            tokens_cached: 0,
            tokens_cache_created: 0,
            estimated_input: None,
            refusal_kind: None,
            retry_after_secs: None,
            task: None,
            owner: None,
            latency_ms: 1,
            pace: None,
            usage_outcome: None,
        })
        .await
        .expect("record a call");

    sqlx::query("UPDATE gateway_llm_usage SET ts = $1 WHERE actor = $2")
        .bind(w0.start + Duration::minutes(10))
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("place the call in the first window");

    store
        .rollup_windows(&[w0, w1])
        .await
        .expect("fold both windows");

    // 前置事实：第二个窗口里这个人有一行，而且是零行。没有这一行，「这段没有他」
    // 与「这段根本没折过」就分不开了，而下面那条断言也就成了空台词。
    let idle = folded(&pool, &upstream, w1.start, &actor).await;
    assert_eq!(
        idle,
        vec![(0, 0, 0, 0, 0)],
        "the fold lands a zero row for a combination the window did not use"
    );

    // 只读第二个窗口：他在这段里一次都没调用，所以这里没有他的行。
    let idle_range = store
        .usage_by_actor_rolled(w1.start, w1.end)
        .await
        .expect("read the window the combination sat out");
    assert!(
        idle_range.rows.iter().all(|s| s.actor != actor),
        "a combination that did nothing in the range is not a row of it, got {:?}",
        idle_range
            .rows
            .iter()
            .filter(|s| s.actor == actor)
            .collect::<Vec<_>>()
    );
    assert!(
        idle_range.covered_since == Some(w1.start) && idle_range.covered_until == Some(w1.end),
        "the range is still reported as covered: dropping the idle row is not the same answer \
         as having folded nothing"
    );

    // 两窗一起读：他有一行，数的是他真调用的那一次。
    let both = store
        .usage_by_actor_rolled(w0.start, w1.end)
        .await
        .expect("read both windows");
    let line = both
        .rows
        .iter()
        .find(|s| s.actor == actor)
        .expect("a combination that did call is a row");
    assert_eq!(
        line.calls, 1,
        "one call in the first window, none in the second"
    );
    assert_eq!(line.tokens_input, 10);

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

/// 一段没有花销的区间，究竟是「没人尝试」还是「每次都被挡回来」。
///
/// 花销面答不出这个：被熔断挡回来的调用没有发出去、没有 token，和一段没有任何
/// 调用的区间一样是零。区别只在结局那一列上——它记着每次尝试是怎么结束的。这条
/// 测试要求分成因计数：把 `refusal_kind` 并从 `result` 里，`quota_cycle` 与
/// 「没有更细成因」就会合成一行，而这两种要的修法完全不同。
///
/// 用的时间窗离现在足够远，别的测试不会往那里落行，所以断言可以做全集比较。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_outcome_breakdown_keeps_the_verdict_that_explains_a_failed_range() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    init_schema_once(&store, &pool).await;

    let run = uuid::Uuid::new_v4();
    let actor = format!("outcome-test:{run}");
    let upstream = format!("outcome-upstream:{run}");
    let start = hour_floor(Utc::now()) - Duration::hours(1_000);

    let record = |result: &str, kind: Option<&str>, tokens: u64| {
        let store = store.clone();
        let actor = actor.clone();
        let upstream = upstream.clone();
        let result = result.to_string();
        let kind = kind.map(|k| k.to_string());
        async move {
            store
                .record(&LlmUsageRecord {
                    upstream,
                    api_style: "openai".to_string(),
                    model: "outcome-model".to_string(),
                    result,
                    actor,
                    origin: "gateway_pool".to_string(),
                    tokens_input: tokens,
                    tokens_output: 0,
                    tokens_cached: 0,
                    tokens_cache_created: 0,
                    estimated_input: None,
                    refusal_kind: kind,
                    retry_after_secs: None,
                    task: None,
                    owner: None,
                    latency_ms: 1,
                    pace: None,
                    usage_outcome: None,
                })
                .await
                .expect("record a call");
        }
    };
    record("ok", None, 42).await;
    record("refused_by_circuit", None, 0).await;
    record("refused_by_circuit", None, 0).await;
    record("error", Some("quota_cycle"), 0).await;

    sqlx::query("UPDATE gateway_llm_usage SET ts = $1 WHERE actor = $2")
        .bind(start + Duration::minutes(5))
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("place the calls in the probe window");

    let outcomes = store
        .outcomes_between(start, start + Duration::hours(1))
        .await
        .expect("read the outcome breakdown");
    let lines: Vec<(String, String, i64)> = outcomes
        .iter()
        .map(|o| (o.result.clone(), o.refusal_kind.clone(), o.calls))
        .collect();
    assert_eq!(
        lines,
        vec![
            ("refused_by_circuit".to_string(), String::new(), 2),
            ("error".to_string(), "quota_cycle".to_string(), 1),
            ("ok".to_string(), String::new(), 1),
        ],
        "the breakdown keeps the kind beside the result, biggest line first"
    );

    // 同一段区间在花销面上只有一行、而且 token 只有那一次成功的：正因为如此，
    // 「零 token」才需要上面这张表来解释。
    let spend = store
        .usage_by_actor_rolled(start, start + Duration::hours(1))
        .await
        .expect("read the same range as spend");
    assert_eq!(
        spend.rows.len(),
        0,
        "nothing was folded for this probe window; the outcome read does not depend on the fold"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE actor = $1")
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("clean up the ledger probe rows");
}

/// 平台自己那本账：省下了什么、每个窗口被判成什么。两本账都在写、都没有读面，
/// 所以这条测的正是「读回来」这件事本身：花销为零的那一屏要靠它才说得出是
/// 「没花」还是「决定不花」。
///
/// 同时判两件容易被合并的事：
/// - 区间里没有记录时是**空**，不是零行或一行零——空才是「这本账这次没说什么」；
/// - 判定按 `(actor, verdict, reason)` 分格，`unexpected_absent` 与
///   `documented_skip` 不许被求和成同一行。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_account_read_returns_the_savings_and_the_judgements_it_was_given() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    init_schema_once(&store, &pool).await;
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    let skip_store = cog_observability::skip_store::LlmSkipStore::connect(&url)
        .await
        .expect("connect the saved-call store");
    skip_store
        .init_schema(&schema_runner)
        .await
        .expect("init the saved-call schema");
    sqlx::raw_sql(cog_observability::spend_verdict::DDL)
        .execute(&pool)
        .await
        .expect("init the verdict schema");

    let run = uuid::Uuid::new_v4();
    let stage = format!("account-stage:{run}");
    let actor = format!("account-actor:{run}");
    let start = hour_floor(Utc::now()) - Duration::hours(1_100);
    let end = start + Duration::hours(1);

    // 区间里什么也没有：两半都必须是空的，而且不是「一行零」。
    let empty_skips = store
        .skips_between(start, end)
        .await
        .expect("read an empty range's savings");
    let empty_verdicts = store
        .verdicts_between(start, end)
        .await
        .expect("read an empty range's judgements");
    assert!(
        empty_skips.iter().all(|s| s.stage != stage),
        "no saving was recorded for this probe stage"
    );
    assert!(
        empty_verdicts.iter().all(|v| v.actor != actor),
        "no window was judged for this probe site"
    );

    skip_store
        .record(&cog_core::LlmSkipRecord {
            stage: stage.clone(),
            reason: "upstream_unavailable",
        })
        .await
        .expect("record a saving");
    skip_store
        .record(&cog_core::LlmSkipRecord {
            stage: stage.clone(),
            reason: "upstream_unavailable",
        })
        .await
        .expect("record the same saving again");
    sqlx::query("UPDATE cog_llm_skip SET ts = $1 WHERE stage = $2")
        .bind(start + Duration::minutes(5))
        .bind(&stage)
        .execute(&pool)
        .await
        .expect("place the savings in the probe window");

    for (verdict, reason, windows) in [
        ("documented_skip", "upstream_unavailable", 2i64),
        ("unexpected_absent", "", 1i64),
    ] {
        for i in 0..windows {
            let window_start = start + Duration::hours(i);
            sqlx::query(
                "INSERT INTO cog_llm_spend_verdict \
                 (window_start, window_end, actor, verdict, skip_reason, skips, calls) \
                 VALUES ($1, $2, $3, $4, $5, 0, 0)",
            )
            .bind(window_start)
            .bind(window_start + Duration::hours(1))
            .bind(&actor)
            .bind(verdict)
            .bind(reason)
            .execute(&pool)
            .await
            .expect("plant a judged window");
        }
    }

    let skips = store
        .skips_between(start, end + Duration::hours(1))
        .await
        .expect("read the savings back");
    let mine: Vec<(String, String, i64)> = skips
        .into_iter()
        .filter(|s| s.stage == stage)
        .map(|s| (s.stage, s.reason, s.skips))
        .collect();
    assert_eq!(
        mine,
        vec![(stage.clone(), "upstream_unavailable".to_string(), 2)],
        "two recorded savings read back as one line of two"
    );

    let verdicts = store
        .verdicts_between(start, end + Duration::hours(1))
        .await
        .expect("read the judgements back");
    let mine: Vec<(String, String, i64)> = verdicts
        .into_iter()
        .filter(|v| v.actor == actor)
        .map(|v| (v.verdict, v.skip_reason, v.windows))
        .collect();
    assert_eq!(
        mine,
        vec![
            ("documented_skip".to_string(), "upstream_unavailable".to_string(), 2),
            ("unexpected_absent".to_string(), String::new(), 1),
        ],
        "two verdicts stay two lines: the one that needs a repair is not summed into the documented one"
    );

    sqlx::query("DELETE FROM cog_llm_skip WHERE stage = $1")
        .bind(&stage)
        .execute(&pool)
        .await
        .expect("clean up the saving probe rows");
    sqlx::query("DELETE FROM cog_llm_spend_verdict WHERE actor = $1")
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("clean up the verdict probe rows");
}
