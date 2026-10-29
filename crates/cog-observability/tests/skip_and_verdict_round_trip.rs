//! The two ledgers of the token account, against a live PostgreSQL: what is
//! written is what is read back, and the verdict a window gets is the one its
//! readings imply.
//!
//! Everything above this line was checked against constructed facts — the rule
//! in `cog_observability::spend_verdict` takes maps and returns words, and its
//! unit tests pass with no server at all. What that cannot check is the other
//! half of the claim: that the rows the rule is fed are the rows the writers
//! leave behind. A column bound in the wrong order, a `GROUP BY` that folds two
//! stages into one, an upsert whose key is not the key the reader reads with —
//! each of those passes every unit test and lands a verdict about a spend that
//! was never measured.
//!
//! So this runs the real path end to end: a saving is recorded through
//! [`cog_core::LlmSkipSink`] (the trait the call sites hold, not the store's own
//! method), spend rows are written into the usage ledger, the verdict fold runs
//! over the windows, and the cells are read back grouped. Three directions are
//! asserted — a window that spent, a window whose silence is on the record, and
//! a window whose silence is not — because a face that reports only one of them
//! is the failure mode this whole thing exists to prevent.
//!
//! Needs a server, so it is ignored by default and takes
//! `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-observability --test skip_and_verdict_round_trip -- --ignored
//! ```
//!
//! Each test creates a schema of its own and drops it at the end, so the
//! database is left as it was found and the tests do not have to run one at a
//! time. It must still never be pointed at anything but a throwaway: `CREATE
//! SCHEMA` is the one statement here that a live database would notice.

use chrono::{DateTime, Duration, TimeZone, Utc};
use sqlx::PgPool;

use cog_core::{LlmSkipRecord, LlmSkipSink};
use cog_observability::skip_store::{LlmSkipStore, SKIP_TABLE};
use cog_observability::spend_verdict::SpendVerdictStore;
use cog_observability::usage_store::{LlmUsageRecord, LlmUsageStore, LEDGER_TABLE};

/// The model every row this probe writes carries, so clearing up after itself
/// can never touch a row it did not write.
const PROBE_MODEL: &str = "verdict-probe-model";

use cog_test_support::{database_url, drop_schema, own_schema};

/// A schema of this test's own, reached by putting `search_path` in the URL.
///
/// Every table these stores touch is named unqualified, so the schema an
/// unqualified name resolves in is the schema this test dropped and created for
/// itself — which is what makes the file safe to point at a database that also
/// holds a deployment's tables. Dropping `cog_llm_skip` "to start from a clean
/// slate" against the public schema would drop the live saved-call ledger, and
/// this way the idea cannot even be expressed: there is no way for this test to
/// name a table outside its own schema. It also lets two tests in the file run
/// at the same time, which they do by default.
struct Probe {
    pool: PgPool,
    url: String,
    schema: String,
}

impl Probe {
    /// Connect to a schema of this run's own, with every store URL pointed at it.
    async fn open(label: &str) -> Self {
        let base = database_url();
        let schema = own_schema(&base, label).await;
        let sep = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{sep}options=-csearch_path%3D{schema}");
        // One connection proves the option was taken: a server that ignored it
        // would leave the test writing into the public schema, which is the one
        // thing this harness exists to prevent.
        let pool = PgPool::connect(&url).await.expect("connecting");
        let current: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .expect("asking which schema this connection is in");
        assert_eq!(
            current, schema,
            "the search_path in the URL was not taken, so these unqualified names \
             would resolve in {current} rather than in the probe schema"
        );
        Self {
            pool,
            url,
            schema: schema.to_string(),
        }
    }

    /// Leave nothing behind: the schema goes with everything in it.
    async fn close(self) {
        drop_schema(&database_url(), &self.schema).await;
    }
}

/// A whole hour, so the fold's grid and the fixture's rows line up.
fn hour(y: i32, mo: u32, d: u32, h: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, 0, 0).unwrap()
}

/// One call in the usage ledger. The row's time is the database's own default,
/// which is why every batch of spend is followed by [`place`].
async fn spend(store: &LlmUsageStore, actor: &str, tokens_input: u64, tokens_output: u64) {
    store
        .record(&LlmUsageRecord {
            upstream: "probe-upstream".into(),
            api_style: "openai".into(),
            model: PROBE_MODEL.into(),
            result: "ok".into(),
            actor: actor.into(),
            origin: "gateway_pool".into(),
            tokens_input,
            tokens_output,
            tokens_cached: 0,
            estimated_input: None,
            refusal_kind: None,
            retry_after_secs: None,
            task: None,
            latency_ms: 1,
            pace: None,
        })
        .await
        .expect("writing a usage row");
}

/// Move the rows just written into the window the fixture means.
///
/// Called immediately after each batch, so "just written" is unambiguous: the
/// ledger's writer stamps `NOW()` and a window filter reads the row's own time,
/// and a fixture that could not set it would be asserting against whatever hour
/// the test happened to run in.
async fn place(pool: &PgPool, ts: DateTime<Utc>) {
    sqlx::query(&format!(
        "UPDATE {LEDGER_TABLE} SET ts = $1 WHERE model = $2 AND ts >= NOW() - interval '1 minute'"
    ))
    .bind(ts)
    .bind(PROBE_MODEL)
    .execute(pool)
    .await
    .expect("placing the spend in the window it belongs to");
}

/// Every row this probe writes is named, so that clearing up after itself (and
/// moving its own rows) can never touch a row it did not write. The ledger is a
/// production table wherever it exists, and a test that deletes by window would
/// delete the account.
async fn clear_probe_rows(pool: &PgPool) {
    sqlx::query(&format!("DELETE FROM {LEDGER_TABLE} WHERE model = $1"))
        .bind(PROBE_MODEL)
        .execute(pool)
        .await
        .expect("clearing the probe's own rows");
}

/// The verdicts of one window, as `(actor, verdict, skip_reason)`.
async fn cells_in(
    store: &SpendVerdictStore,
    window: DateTime<Utc>,
) -> Vec<(String, String, String)> {
    store
        .verdicts_between(window, window + Duration::hours(1))
        .await
        .expect("reading the verdicts of a window")
        .into_iter()
        .map(|line| (line.actor, line.verdict, line.skip_reason))
        .collect()
}

/// A saving written through the contract the call sites hold is read back under
/// the stage it named, with the reason kept apart from the count.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_saving_written_through_the_sink_is_read_back_by_stage_and_reason() {
    let probe = Probe::open("probe_skip_round_trip").await;
    let store = LlmSkipStore::connect(&probe.url).await.expect("connecting");
    let schema_runner = cog_test_support::SchemaTextRunner::new(probe.pool.clone());
    store
        .init_schema(&schema_runner)
        .await
        .expect("creating the schema");

    // Two stages, two reasons, and one stage used twice — so a read that folds
    // the stage away, or the reason away, shows up as a different number rather
    // than as an empty answer.
    let sink: &dyn LlmSkipSink = &store;
    for (stage, reason) in [
        ("planner", "disabled"),
        ("planner", "disabled"),
        ("planner", "self_evolution"),
        ("moderator", "self_evolution"),
    ] {
        sink.record_skip(&LlmSkipRecord {
            stage: stage.to_string(),
            reason,
        })
        .await
        .expect("recording a saving");
    }

    let rows = store
        .skips_between(
            Utc::now() - Duration::minutes(5),
            Utc::now() + Duration::minutes(5),
        )
        .await
        .expect("reading the savings");
    let lines: Vec<(String, String, i64)> = rows
        .into_iter()
        .map(|row| (row.stage, row.reason, row.skips))
        .collect();
    assert_eq!(
        lines,
        vec![
            ("moderator".to_string(), "self_evolution".to_string(), 1),
            // Two decisions under `disabled` and one under `self_evolution`:
            // the count follows the (stage, reason) cell, not the stage.
            ("planner".to_string(), "disabled".to_string(), 2),
            ("planner".to_string(), "self_evolution".to_string(), 1),
        ]
    );

    // A window that ends before the savings were recorded reads none of them,
    // so a fold that ran over the wrong window cannot pass by reading today's.
    let past = store
        .skips_between(
            Utc::now() - Duration::hours(4),
            Utc::now() - Duration::hours(3),
        )
        .await
        .expect("reading an empty window");
    assert!(past.is_empty(), "{past:?}");

    probe.close().await;
}

/// The fold, over spend the fixture placed: a window that spent reads
/// `necessary`, the next one — silent with nothing on the record — reads
/// `unexpected_absent`, and the same silence with a saving beside it reads
/// `documented_skip`.
///
/// The three are checked in one run because the difference between them *is*
/// the claim: a face where one of the three is unreachable passes any single
/// assertion by answering the same word every time.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_fold_gives_each_window_the_verdict_its_readings_imply() {
    let probe = Probe::open("probe_verdict_fold").await;
    let url = probe.url.clone();
    let pool = probe.pool.clone();

    let ledger = LlmUsageStore::connect(&url).await.expect("connecting");
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    ledger
        .init_schema(&schema_runner)
        .await
        .expect("creating the ledger");
    // The fixture is the probe's own; the production table is created by the
    // gateway and this test must not depend on a deployment having run.
    clear_probe_rows(&pool).await;

    let skips = LlmSkipStore::connect(&url).await.expect("connecting");
    skips
        .init_schema(&schema_runner)
        .await
        .expect("creating the skip schema");
    let store = SpendVerdictStore::connect(&url, ledger.clone(), Some(skips.clone()))
        .await
        .expect("connecting");
    store
        .init_schema(&schema_runner)
        .await
        .expect("creating the verdict schema");

    // Two consecutive hours in the past, so both are closed windows with spend
    // the test placed rather than whatever the machine is doing.
    let spent = hour(2026, 1, 2, 3);
    let stopped = hour(2026, 1, 2, 4);
    spend(&ledger, "agent:planner", 1_000, 100).await;
    spend(&ledger, "agent:moderator", 500, 50).await;
    place(&pool, spent).await;
    // The moderator keeps spending; the planner does not.
    spend(&ledger, "agent:moderator", 400, 40).await;
    place(&pool, stopped).await;
    // And the planner's silence in the second hour has a reason on the record.
    skips
        .record(&LlmSkipRecord {
            stage: "planner".to_string(),
            reason: "self_evolution",
        })
        .await
        .expect("recording a saving");

    // The saving is recorded with the database's `now()`, so it lands in the
    // hour the test is running in, not in the fixture's window. Move it: the
    // row's own time is what the window filter reads, and a fixture that could
    // not place it would be asserting a join it never made.
    sqlx::query(&format!(
        "UPDATE {SKIP_TABLE} SET ts = $1 WHERE stage = 'planner'"
    ))
    .bind(stopped + Duration::minutes(10))
    .execute(&pool)
    .await
    .expect("placing the saving in the window it belongs to");

    let windows = cog_observability::usage_store::windows_to_roll(
        Some(spent),
        stopped + Duration::hours(1),
        3600,
        3600,
    );
    assert_eq!(windows.len(), 2, "the fixture expects exactly two windows");
    let written = store.judge_windows(&windows).await.expect("judging");
    assert!(
        written >= 4,
        "expected a cell per registered site: {written}"
    );

    let first = cells_in(&store, spent).await;
    // The planner and the moderator spent; every other registered site had not
    // been spending either, so it gets no cell rather than a verdict.
    assert!(
        first.contains(&("agent:planner".into(), "necessary".into(), String::new())),
        "{first:?}"
    );
    assert!(
        !first
            .iter()
            .any(|(_, verdict, _)| verdict == "unexpected_absent"),
        "a window with a live run and a spend judges nobody absent: {first:?}"
    );

    let second = cells_in(&store, stopped).await;
    assert!(
        second.contains(&("agent:moderator".into(), "necessary".into(), String::new())),
        "{second:?}"
    );
    assert!(
        second.contains(&(
            "self_review:planner".into(),
            "documented_skip".into(),
            "self_evolution".into()
        )),
        "the recorded saving is what makes this window's silence readable: {second:?}"
    );
    assert!(
        second.contains(&(
            "agent:planner".into(),
            "unexpected_absent".into(),
            String::new()
        )),
        "the planner stopped with nothing on the record: {second:?}"
    );

    // Rerunning the same windows is idempotent by primary key, which is what
    // lets two processes fold the same hour without doubling the account.
    let again = store.judge_windows(&windows).await.expect("judging again");
    assert_eq!(again, written);
    assert_eq!(cells_in(&store, stopped).await, second);

    // A saving face that cannot be read is the third value, and this is the
    // path that produces it in a real deployment: the skip store absent while
    // the ledger is there. The same window must not come back as an absence.
    let blind = SpendVerdictStore::connect(&url, ledger.clone(), None)
        .await
        .expect("connecting without the saving face");
    blind.judge_windows(&windows).await.expect("judging blind");
    let blind_cells = cells_in(&blind, stopped).await;
    assert!(
        blind_cells.contains(&(
            "self_review:planner".into(),
            "unsettled".into(),
            String::new()
        )),
        "an unreadable saving face has to say so rather than call it an absence: {blind_cells:?}"
    );
    assert!(
        !blind_cells
            .iter()
            .any(|(actor, verdict, _)| actor == "self_review:planner"
                && verdict == "unexpected_absent"),
        "{blind_cells:?}"
    );

    clear_probe_rows(&pool).await;
    probe.close().await;
}
