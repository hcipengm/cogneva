//! principle: feedback_a_paired_reading_lands_where_both_rulers_are_visible_and_is_measured_by_the_sender
//!
//! The durable per-call token ledger, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that a recorded call
//! round-trips the dimensions a reader selects by -- the cache split, and the
//! estimate that has to sit beside the upstream's own count -- and that the
//! additive column migration lands on a table that already holds rows without
//! rewriting them, including the one column whose absence is a value.
//! Neither can be checked without a server -- the INSERT is a statement only
//! PostgreSQL parses, and "old rows survive and read a default" is a fact about
//! the server's ALTER, not about our code. A statement that can never run keeps
//! every DB-free test green, which is why this file is ignored by default and
//! needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/postgres \
//!   cargo test -p cog-observability --test usage_store_ledger -- --ignored
//! ```
//!
//! It is one case rather than several because the two faces are ordered: the
//! migration face drops the column, and a second case running beside it would
//! be judging a schema the first one is mid-way through changing. The rows it
//! writes carry a per-run marker and are deleted at the end, so the database is
//! left as it was found.

use cog_observability::usage_store::{
    LlmUsageRecord, LlmUsageStore, PaceEvidence, PaceUpstreamEvidence, StoredWindow,
};
use sqlx::{PgPool, Row};

use cog_test_support::database_url;

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_metered_dimensions_reach_the_ledger_and_the_column_migration_spares_existing_rows() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let store = LlmUsageStore::connect(&url)
        .await
        .expect("connect the store");
    // Three markers, because the migration face leaves a row behind, the row
    // written afterwards has to be told apart from it, and a row that carries no
    // estimate is a third shape that must survive as its own.
    let before = format!("usage-ledger-test-before:{}", uuid::Uuid::new_v4());
    let after = format!("usage-ledger-test-after:{}", uuid::Uuid::new_v4());
    let unpriced = format!("usage-ledger-test-unpriced:{}", uuid::Uuid::new_v4());
    let task = format!("usage-ledger-test-task:{}", uuid::Uuid::new_v4());
    let refusal = format!("usage-ledger-test-refusal:{}", uuid::Uuid::new_v4());

    let record = |actor: String,
                  cached: u64,
                  estimated: Option<u64>,
                  task: Option<&str>,
                  refusal_kind: Option<&str>| LlmUsageRecord {
        upstream: "ledger-test-upstream".to_string(),
        api_style: "openai".to_string(),
        model: "ledger-test-model".to_string(),
        result: "ok".to_string(),
        actor,
        origin: "sandbox".to_string(),
        tokens_input: 1000,
        tokens_output: 50,
        tokens_cached: cached,
        estimated_input: estimated,
        refusal_kind: refusal_kind.map(str::to_string),
        retry_after_secs: None,
        task: task.map(str::to_string),
        latency_ms: 12,
        pace: None,
    };

    store.init_schema().await.expect("init the schema");

    // First face: a call that the upstream served partly from cache reaches the
    // ledger with the cache split intact. Folding it into `tokens_input` would
    // pass every other assertion in this file and still lose the one number the
    // ledger exists to carry.
    store
        .record(&record(before.clone(), 800, Some(900), Some(&task), None))
        .await
        .expect("record a cached call");
    let row = sqlx::query(
        "SELECT tokens_input, tokens_output, tokens_cached, estimated_input, task_id \
         FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&before)
    .fetch_one(&pool)
    .await
    .expect("read the recorded call back");
    assert_eq!(row.get::<i64, _>("tokens_input"), 1000);
    assert_eq!(row.get::<i64, _>("tokens_output"), 50);
    assert_eq!(
        row.get::<i64, _>("tokens_cached"),
        800,
        "the cache split must survive the write, or the hit rate is unrecoverable"
    );
    assert_eq!(
        row.get::<Option<i64>, _>("estimated_input"),
        Some(900),
        "the estimate has to land on the same row as the upstream's count, or the two \
         rulers can only be read one call at a time"
    );
    assert_eq!(
        row.get::<Option<String>, _>("task_id"),
        Some(task.clone()),
        "the job identity has to survive the write under the exact string the caller \
         used, or it cannot be joined to that run's own totals"
    );

    // Second face: the table as it exists in production -- populated, and
    // without the columns. Dropping them here is what the real upgrade path does
    // by simply not having them yet, so re-running init_schema is exercising the
    // ALTER branch rather than the CREATE branch. Both dimensions are dropped
    // together because their migrations differ in the one way a reader depends
    // on: the cache column comes back with a default, the estimate column must
    // not, since "this row was never priced" is not "this row was priced at 0".
    sqlx::query(
        "ALTER TABLE gateway_llm_usage DROP COLUMN tokens_cached, DROP COLUMN estimated_input, \
         DROP COLUMN task_id, DROP COLUMN refusal_kind, DROP COLUMN retry_after_secs",
    )
    .execute(&pool)
    .await
    .expect("drop the columns to reproduce the pre-migration table");
    store
        .init_schema()
        .await
        .expect("re-init against a table without the columns");

    let row = sqlx::query(
        "SELECT tokens_input, tokens_cached, estimated_input, task_id, refusal_kind, \
                retry_after_secs \
         FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&before)
    .fetch_one(&pool)
    .await
    .expect("the row must survive the migration");
    assert_eq!(
        row.get::<i64, _>("tokens_input"),
        1000,
        "an additive migration must not rewrite the rows it lands beside"
    );
    assert_eq!(
        row.get::<i64, _>("tokens_cached"),
        0,
        "a row written before the column existed reads the default, not NULL"
    );
    assert_eq!(
        row.get::<Option<i64>, _>("estimated_input"),
        None,
        "a row written before the estimate column existed reads NULL, not 0 -- a zero \
         would be read as a request whose prompt was empty"
    );
    assert_eq!(
        row.get::<Option<String>, _>("task_id"),
        None,
        "a row that predates the job column is unattributed, and a placeholder would \
         gather every such row into one job that never existed"
    );
    assert_eq!(
        row.get::<Option<String>, _>("refusal_kind"),
        None,
        "a row written before the refusal column existed has no cause to report, and \
         inventing one would make a past window read as a refusal nobody classified"
    );
    assert_eq!(
        row.get::<Option<i64>, _>("retry_after_secs"),
        None,
        "a row that predates the retry hint was never a circuit refusal, and a zero \
         would read as 'retry now' -- the opposite of what the column says"
    );

    let shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'tokens_cached'",
    )
    .fetch_one(&pool)
    .await
    .expect("the column must exist after the migration");
    assert_eq!(shape.get::<String, _>("is_nullable"), "NO");
    assert!(
        shape
            .get::<Option<String>, _>("column_default")
            .is_some_and(|d| d.contains('0')),
        "the migrated column needs a default, or every later row has to carry a value it does not know"
    );
    let estimate_shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'estimated_input'",
    )
    .fetch_one(&pool)
    .await
    .expect("the estimate column must exist after the migration");
    assert_eq!(
        estimate_shape.get::<String, _>("is_nullable"),
        "YES",
        "the estimate column is the nullable one on purpose: it has an absence to carry"
    );
    assert!(
        estimate_shape
            .get::<Option<String>, _>("column_default")
            .is_none(),
        "a default here would erase the difference between unpriced and priced at it"
    );
    let retry_shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'retry_after_secs'",
    )
    .fetch_one(&pool)
    .await
    .expect("the retry hint column must exist after the migration");
    assert_eq!(
        retry_shape.get::<String, _>("is_nullable"),
        "YES",
        "most rows are not circuit refusals; they need an absence to carry"
    );
    assert!(
        retry_shape
            .get::<Option<String>, _>("column_default")
            .is_none(),
        "a default would let every served call claim a retry time it never had"
    );

    // And the columns the migration added are ones the writer actually fills --
    // including the estimate's own absence, which has to come back as an absence
    // rather than as the zero the reader would otherwise have to guess about.
    let mut after_migration = record(after.clone(), 640, Some(512), Some(&task), None);
    // Only the circuit-refusal path sets the retry hint; what this case can check
    // is that a value set there survives the writer and the migrated table as the
    // number the circuit stated, not as a presence flag.
    after_migration.retry_after_secs = Some(4_312);
    store
        .record(&after_migration)
        .await
        .expect("record after the migration");
    store
        .record(&record(unpriced.clone(), 0, None, None, None))
        .await
        .expect("record a call whose path prices nothing");
    let row = sqlx::query(
        "SELECT tokens_cached, estimated_input, task_id, retry_after_secs \
         FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&after)
    .fetch_one(&pool)
    .await
    .expect("read the post-migration call back");
    assert_eq!(row.get::<i64, _>("tokens_cached"), 640);
    assert_eq!(row.get::<Option<i64>, _>("estimated_input"), Some(512));
    assert_eq!(row.get::<Option<String>, _>("task_id"), Some(task.clone()));
    assert_eq!(
        row.get::<Option<i64>, _>("retry_after_secs"),
        Some(4_312),
        "the retry hint must reach the ledger as the number the circuit stated"
    );
    let row =
        sqlx::query("SELECT estimated_input, task_id FROM gateway_llm_usage WHERE actor = $1")
            .bind(&unpriced)
            .fetch_one(&pool)
            .await
            .expect("read the unpriced call back");
    assert_eq!(
        row.get::<Option<i64>, _>("estimated_input"),
        None,
        "a call the writer did not price must read as absent, not as a priced zero"
    );
    assert_eq!(
        row.get::<Option<String>, _>("task_id"),
        None,
        "a call from a path with no job identity stays unattributed; it must not borrow \
         a neighbour's task or invent one"
    );

    // The refusal cause is a column of its own because the two values that reach
    // it call for opposite actions: a spent window comes back, an unpurchased
    // model never does. Folding them into `result = error` leaves the ledger able
    // to say an attempt failed but unable to say which of the two it was.
    store
        .record(&record(refusal.clone(), 0, None, None, Some("quota_cycle")))
        .await
        .expect("record a refused attempt");
    let row = sqlx::query("SELECT refusal_kind FROM gateway_llm_usage WHERE actor = $1")
        .bind(&refusal)
        .fetch_one(&pool)
        .await
        .expect("read the refusal back");
    assert_eq!(
        row.get::<Option<String>, _>("refusal_kind"),
        Some("quota_cycle".to_string()),
        "the cause has to survive the write under the same name the classifier emits, \
         or a reader grouping by it joins nothing"
    );
    let refusal_shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'refusal_kind'",
    )
    .fetch_one(&pool)
    .await
    .expect("the refusal column must exist after the migration");
    assert_eq!(
        refusal_shape.get::<String, _>("is_nullable"),
        "YES",
        "most rows are not refusals; a NOT NULL column would have to carry a value for them"
    );
    assert!(
        refusal_shape
            .get::<Option<String>, _>("column_default")
            .is_none(),
        "a default would make every non-refusal row name a cause it never had"
    );

    // The pace refusal carries the reading it was decided on, on its own row and
    // in its own columns. Reusing `estimated_input` would say the request was
    // priced, reusing `refusal_kind` would name an upstream as the refuser, and
    // reusing `upstream` would put a set of identities in a column that answers
    // "which single upstream is this row about". The gap a reader recomputes
    // (`spent + reserve - allowance`) is only true of these columns.
    let paced_evidence = format!("ledger-test-pace-evidence:{}", uuid::Uuid::new_v4());
    let mut paced_row = record(paced_evidence.clone(), 0, None, None, None);
    paced_row.result = "refused_by_pace".to_string();
    paced_row.pace = Some(PaceEvidence {
        measured_upstreams: 2,
        allowance_tokens: 1_000,
        spent_tokens: 1_200,
        reserve_tokens: 300,
        window_start_unix: 1_700_000_000,
        upstreams: vec![PaceUpstreamEvidence {
            upstream: "https://a.example.com|m1".to_string(),
            allowance_tokens: 400,
            spent_tokens: 900,
            reserve_tokens: 300,
            window_start_unix: 1_700_000_000,
            wait_secs: 120,
        }],
    });
    store
        .record(&paced_row)
        .await
        .expect("record a paced refusal with its evidence");
    let row = sqlx::query(
        "SELECT pace_measured_upstreams, pace_allowance_tokens, pace_spent_tokens, \
                pace_reserve_tokens, pace_window_start_unix, pace_upstreams \
           FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&paced_evidence)
    .fetch_one(&pool)
    .await
    .expect("read the paced refusal back");
    assert_eq!(
        row.get::<Option<i16>, _>("pace_measured_upstreams"),
        Some(2)
    );
    assert_eq!(
        row.get::<Option<i64>, _>("pace_allowance_tokens"),
        Some(1_000)
    );
    assert_eq!(row.get::<Option<i64>, _>("pace_spent_tokens"), Some(1_200));
    assert_eq!(row.get::<Option<i64>, _>("pace_reserve_tokens"), Some(300));
    assert_eq!(
        row.get::<Option<i64>, _>("pace_window_start_unix"),
        Some(1_700_000_000),
        "the boundary has to land with the other three numbers: allowance alone cannot \
         tell a window that has not opened yet from one burned down to nothing"
    );
    let upstreams = row
        .get::<Option<serde_json::Value>, _>("pace_upstreams")
        .expect("a paced refusal names the candidates it was refused on");
    let first = &upstreams[0];
    assert_eq!(first["upstream"], "https://a.example.com|m1");
    assert_eq!(first["wait_secs"], 120);

    // A row that was not paced has no reading to carry, and `NULL` is that
    // absence. A zero here would recompute as "the gap was closed exactly",
    // which is the one thing an unpaced row never said.
    let row = sqlx::query(
        "SELECT pace_measured_upstreams, pace_allowance_tokens, pace_spent_tokens, \
                pace_reserve_tokens, pace_window_start_unix, pace_upstreams \
           FROM gateway_llm_usage WHERE actor = $1",
    )
    .bind(&unpriced)
    .fetch_one(&pool)
    .await
    .expect("read the unpaced row back");
    assert_eq!(row.get::<Option<i16>, _>("pace_measured_upstreams"), None);
    assert_eq!(row.get::<Option<i64>, _>("pace_allowance_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("pace_spent_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("pace_reserve_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("pace_window_start_unix"), None);
    assert_eq!(
        row.get::<Option<serde_json::Value>, _>("pace_upstreams"),
        None
    );
    let pace_shape = sqlx::query(
        "SELECT is_nullable, column_default FROM information_schema.columns \
         WHERE table_name = 'gateway_llm_usage' AND column_name = 'pace_allowance_tokens'",
    )
    .fetch_one(&pool)
    .await
    .expect("the pace column must exist after the migration");
    assert_eq!(pace_shape.get::<String, _>("is_nullable"), "YES");
    assert!(
        pace_shape
            .get::<Option<String>, _>("column_default")
            .is_none(),
        "a default would give every unpaced row a reading it never had"
    );

    // The window pacer reads the ledger three ways, and each one is a statement
    // only PostgreSQL can run. They are exercised here because a query that can
    // never run keeps every DB-free test green: the pacer would simply read
    // nothing and pace nothing, which is indistinguishable from a pool that was
    // never over its line.
    let upstream = format!("ledger-test-window:{}", uuid::Uuid::new_v4());
    let paced_task = format!("ledger-test-paced-task:{}", uuid::Uuid::new_v4());
    let mut served = record(paced_task.clone(), 0, None, Some(paced_task.as_str()), None);
    served.upstream = upstream.clone();
    served.result = "ok".to_string();
    store.record(&served).await.expect("record a served call");
    // A job that was held at its boundary names the job but sent nothing; it
    // must not read as the job having started.
    let mut held = record(paced_task.clone(), 0, None, Some(paced_task.as_str()), None);
    held.upstream = upstream.clone();
    held.result = "refused_by_pace".to_string();
    store.record(&held).await.expect("record a paced refusal");
    // Put the served call in the second half of its second, ten seconds back so
    // it still precedes the wall below. The instant a reading hands back has to
    // contain the row it names: a cast that rounds to the nearest second puts
    // this row *after* its own instant, and a reader summing from there loses
    // it. Placing it deliberately makes that failure red on every run instead
    // of on whichever runs happen to land past the half-second.
    sqlx::query(
        "UPDATE gateway_llm_usage \
            SET ts = date_trunc('second', NOW()) - interval '10 seconds' + interval '0.9 seconds' \
          WHERE actor = $1 AND result = 'ok'",
    )
    .bind(&paced_task)
    .execute(&pool)
    .await
    .expect("place the served call in the second half of a second");
    let mut wall = record(
        "unrelated-actor".to_string(),
        0,
        None,
        None,
        Some("quota_cycle"),
    );
    wall.upstream = upstream.clone();
    wall.result = "error".to_string();
    store.record(&wall).await.expect("record the wall");

    let window = store
        .upstream_window(&upstream, 2_592_000)
        .await
        .expect("read the window");
    assert!(
        window.last_quota_refusal_unix.is_some(),
        "the classified refusal is the window's boundary; without it the pacer \
         would measure the wrong window as soon as traffic resumes"
    );
    assert_eq!(
        window.burned_window_tokens, 1_050,
        "the capacity is what the window served, counted from the successful call only"
    );
    let spent = store
        .spent_since(&upstream, window.last_ok_unix.expect("a served call"))
        .await
        .expect("read the spend");
    assert_eq!(spent, 1_050, "a paced refusal spends nothing");
    // A served call with no classified refusal beside it is not a window: the
    // newest success says where serving stopped, not where the allowance ran
    // out, so there is no capacity to report. Answering with a sum around it
    // would hand the pacer a number nobody measured.
    let unwalled = format!("ledger-test-unwalled:{}", uuid::Uuid::new_v4());
    let mut unwalled_call = record("unrelated-actor".to_string(), 0, None, None, None);
    unwalled_call.upstream = unwalled.clone();
    store
        .record(&unwalled_call)
        .await
        .expect("record a served call with no wall");
    let no_window = store
        .upstream_window(&unwalled, 2_592_000)
        .await
        .expect("read a window with no wall");
    assert!(
        no_window.last_quota_refusal_unix.is_none(),
        "without a refusal there is no boundary to pace from"
    );
    assert_eq!(
        no_window.burned_window_tokens, 0,
        "no wall means no measured capacity, not a sum around the last success"
    );
    assert!(
        store
            .task_has_started(&paced_task)
            .await
            .expect("ask whether the job started"),
        "a served call means the job is on the wire, so later calls must not be held"
    );
    assert!(
        !store
            .task_has_started(&format!("never-sent:{}", uuid::Uuid::new_v4()))
            .await
            .expect("ask about a job that never ran"),
        "a job with no rows has not started"
    );
    assert!(
        store
            .task_cost_quantile(30, 0.75)
            .await
            .expect("measure a job's cost")
            .is_some(),
        "with a completed job on the ledger the reserve has a reading"
    );

    // A window whose end the upstream named can be measured before the ledger
    // carries a labelled refusal: the sum runs against two fixed instants, so
    // it counts the window that ended at the reset and not the one now in
    // progress. Rows outside `(since, until]` are the ones that decide whether
    // the bound is honored at all.
    let bounded = format!("ledger-test-bounded:{}", uuid::Uuid::new_v4());
    let bounded_actors = [
        format!("bounded-old:{}", uuid::Uuid::new_v4()),
        format!("bounded-in:{}", uuid::Uuid::new_v4()),
        format!("bounded-new:{}", uuid::Uuid::new_v4()),
    ];
    for (actor, age_days) in bounded_actors.iter().zip([10_i64, 2, 0]) {
        let mut call = record(actor.clone(), 0, None, None, None);
        call.upstream = bounded.clone();
        store.record(&call).await.expect("record a bounded call");
        sqlx::query(&format!(
            "UPDATE gateway_llm_usage SET ts = NOW() - make_interval(days => {age_days}) \
             WHERE actor = $1"
        ))
        .bind(actor)
        .execute(&pool)
        .await
        .expect("place the call on the window's timeline");
    }
    let now = chrono::Utc::now().timestamp();
    let bounded_sum = store
        .burned_between(&bounded, now - 7 * 86_400, now - 86_400)
        .await
        .expect("sum the window between two instants");
    assert_eq!(
        bounded_sum, 1_050,
        "only the call inside the window counts; the older one is before its \
         start and the newest one after its end"
    );

    // The measurement itself needs a home that outlives the process: if it dies
    // with the gateway, the next rollout silently drops the bound and nothing in
    // the readings says it went away. One row per upstream and window length,
    // overwritten when a newer window of the same length is measured.
    let stored = StoredWindow {
        upstream: bounded.clone(),
        capacity_tokens: 1_050,
        window_secs: 604_800,
        boundary_unix: now - 86_400,
        source: "named_reset".to_string(),
        declared_reset_unix: Some(now - 86_400),
    };
    store
        .record_window(&stored)
        .await
        .expect("store the window measurement");
    assert_eq!(
        store
            .stored_window(&bounded, 604_800)
            .await
            .expect("read the measurement back"),
        Some(stored.clone()),
        "the measurement has to survive a restart under the same boundary"
    );
    let newer = StoredWindow {
        boundary_unix: now + 6 * 86_400,
        capacity_tokens: 2_000,
        ..stored.clone()
    };
    store
        .record_window(&newer)
        .await
        .expect("a newer window supersedes the older one");
    assert_eq!(
        store
            .stored_window(&bounded, 604_800)
            .await
            .expect("read the superseding measurement"),
        Some(newer.clone()),
        "the newest boundary is the one in force"
    );
    // A second window length is a second ruler, not an overwrite: the same
    // upstream can enforce a five-hour and a weekly limit at once, and both
    // measurements are true at the same time.
    let hourly = StoredWindow {
        upstream: bounded.clone(),
        capacity_tokens: 2_230_180,
        window_secs: 18_000,
        boundary_unix: now - 3_600,
        // The upstream named no instant; the boundary is the resume after its
        // wall, and the declared column stays NULL rather than copying it.
        source: "last_resume".to_string(),
        declared_reset_unix: None,
    };
    store
        .record_window(&hourly)
        .await
        .expect("store the nested window measurement");
    assert_eq!(
        store
            .stored_window(&bounded, 18_000)
            .await
            .expect("read the nested measurement back"),
        Some(hourly),
        "a five-hour ruler lives beside the weekly one"
    );
    assert_eq!(
        store
            .stored_window(&bounded, 604_800)
            .await
            .expect("the weekly ruler must survive the nested write"),
        Some(newer),
        "measuring one window length must not overwrite the other"
    );
    let rows = sqlx::query("SELECT COUNT(*) AS n FROM gateway_llm_window WHERE upstream = $1")
        .bind(&bounded)
        .fetch_one(&pool)
        .await
        .expect("count the stored windows");
    assert_eq!(
        rows.get::<i64, _>("n"),
        2,
        "one row per upstream and window length: the two rulers coexist, but a newer measurement of the same length supersedes"
    );

    sqlx::query("DELETE FROM gateway_llm_usage WHERE upstream = $1")
        .bind(&bounded)
        .execute(&pool)
        .await
        .expect("clean up the bounded window rows");
    sqlx::query("DELETE FROM gateway_llm_window WHERE upstream = $1")
        .bind(&bounded)
        .execute(&pool)
        .await
        .expect("clean up the stored window");
    sqlx::query("DELETE FROM gateway_llm_usage WHERE upstream = $1")
        .bind(&upstream)
        .execute(&pool)
        .await
        .expect("clean up the window probe rows");

    for actor in [&before, &after, &unpriced, &refusal] {
        sqlx::query("DELETE FROM gateway_llm_usage WHERE actor = $1")
            .bind(actor)
            .execute(&pool)
            .await
            .expect("clean up the probe rows");
    }
}
