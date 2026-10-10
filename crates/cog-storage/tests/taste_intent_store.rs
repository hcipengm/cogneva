//! What a submission's row does between the moment it is accepted and the
//! moment it becomes work, against a live PostgreSQL.
//!
//! A submission has two jobs at once — it is evidence of what somebody asked
//! for, and it is a row exactly one claimer may take — and both live in the
//! same row's status. A store that got the claim wrong would still answer
//! every call site correctly in isolation: an implementation that let a second
//! `dispose` overwrite the first only shows up as a submission whose recorded
//! fate disagrees with the work that was actually filed for it, which is
//! exactly the pair a later reader compares. The same is true of a re-send:
//! replacing the stored words on a duplicate id would leave the submitter
//! answered, the row present, and the evidence no longer what was read when the
//! decision was taken.
//!
//! None of that is decidable without a server, so these tests are ignored by
//! default and need `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test taste_intent_store -- --ignored
//! ```
//!
//! The table is the store's own ([`PostgresTasteIntentStore::init_schema`]): a
//! hand-written schema would go on passing after the production one changed
//! shape. The cases share one server and run in parallel, so each builds that
//! table in a schema of its own and drops it again — one table between them is
//! a race on two counts, over the rows in it and over the catalogue entries for
//! it, and a database is left as it was found either way.

use std::str::FromStr;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

use cog_core::{
    DirectionPreferenceIntent, EvaluatorVerdict, EvaluatorVerdictIntent, TasteDisposition,
    TasteIntent, TasteIntentPayload, TasteIntentSink, TasteIntentSource,
};
use cog_storage::PostgresTasteIntentStore;

/// Stamped on every row this test writes, so a row left behind by an
/// interrupted run is recognisable as one of these tests' rather than a
/// deployment's.
const PROBE_SUBMITTER: &str = "probe-taste-intent-store";

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

fn preference_says_prefer(preferred: &str) -> TasteIntentPayload {
    TasteIntentPayload::DirectionPreference(DirectionPreferenceIntent {
        preferred: preferred.to_string(),
        alternative: "the other way".to_string(),
        rationale: "one of them costs less to read back".to_string(),
    })
}

/// The instant as the column holds it: microseconds, not the clock's
/// nanoseconds.
///
/// The store writes `submitted_at` into a `timestamptz`, which keeps
/// microseconds, so an instant handed over as it was read comes back differing
/// in its last three digits. Comparing those two is a statement about the clock
/// rather than about the store; handing over the instant the column can hold is
/// what makes the equality below about what was stored and read back.
fn as_the_column_holds_it(at: DateTime<Utc>) -> DateTime<Utc> {
    at.trunc_subsecs(6)
}

fn submission(subject: &str, at: DateTime<Utc>) -> TasteIntent {
    TasteIntent {
        id: Uuid::new_v4(),
        subject: subject.to_string(),
        submitted_by: PROBE_SUBMITTER.to_string(),
        submitted_at: as_the_column_holds_it(at),
        payload: preference_says_prefer("the cheap one"),
        evidence_refs: Vec::new(),
    }
}

/// Where one case's copy of the table lives. Naming it per case is what makes
/// each reading here about this case's own rows: pointed at one table the cases
/// are one another's writers, and a sibling's cleanup mid-assertion deletes
/// submissions this case has just written.
fn schema_of(case: &str) -> String {
    format!("probe_taste_{case}")
}

/// An empty schema holding the store's own table, and the store over it.
///
/// `CREATE TABLE IF NOT EXISTS` is a check followed by a create, so two cases
/// creating the same table at the same instant can both pass the check and one
/// of them then loses on the catalogue's unique index — that is the failure
/// this suite reported, and it is not one a case can retry its way out of
/// without reading rows it did not write. Separate schemas remove the check
/// both ways: the rows and the catalogue entries are this case's alone.
async fn probe(case: &str) -> (PgPool, PostgresTasteIntentStore) {
    let schema = schema_of(case);
    // The schema has to exist before a connection can name it as its search
    // path, and the store creates its table in whatever schema the path
    // selects — a path naming nothing would leave it with nowhere to build.
    let setup = PgPool::connect(&database_url()).await.unwrap();
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&setup)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&setup)
        .await
        .unwrap();
    setup.close().await;

    let options = PgConnectOptions::from_str(&database_url())
        .expect("COGNEVA_TEST_DATABASE_URL is not a PostgreSQL URL")
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new().connect_with(options).await.unwrap();
    let store = PostgresTasteIntentStore::new(pool.clone());
    store.init_schema().await.unwrap();
    (pool, store)
}

/// Hand the schema back with everything in it, so a live database is left as it
/// was found.
async fn done(case: &str) {
    let schema = schema_of(case);
    let pool = PgPool::connect(&database_url()).await.unwrap();
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

/// A submission is readable while nothing has claimed it, and reading it back
/// returns what was submitted rather than a shape that only looks the same.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_submission_is_pending_until_something_claims_it() {
    let case = "pending_until_claimed";
    let (_pool, store) = probe(case).await;

    let submitted = submission("which retry policy", Utc::now());
    store.submit(&submitted).await.unwrap();

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent, submitted);
    assert_eq!(
        pending[0].task_id, None,
        "nothing has been filed for it, so there is no task to name"
    );
    assert_eq!(pending[0].intent.payload.kind(), submitted.payload.kind());

    done(case).await;
}

/// Filing is what takes a submission out of the queue, and the task it names
/// comes back with it — that pair is the join the rest of the system follows
/// from what was asked to what was done.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_filed_submission_leaves_the_queue_and_carries_its_task() {
    let case = "filed_leaves_queue";
    let (_pool, store) = probe(case).await;

    let submitted = submission("which retry policy", Utc::now());
    store.submit(&submitted).await.unwrap();
    store
        .dispose(submitted.id, TasteDisposition::Filed, Some("task-42"))
        .await
        .unwrap();

    assert!(
        store.pending_intents(10).await.unwrap().is_empty(),
        "a claimed submission must not be handed out again"
    );
    let filed = store
        .filed_intents(Utc::now() - Duration::hours(1), 10)
        .await
        .unwrap();
    assert_eq!(filed.len(), 1);
    assert_eq!(filed[0].intent.id, submitted.id);
    assert_eq!(filed[0].task_id.as_deref(), Some("task-42"));

    done(case).await;
}

/// The first decision about a submission is the one that stands. A claimer
/// that crashed after filing and came back with a different ending must not be
/// able to overwrite the ending the work was actually filed under.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_second_disposition_does_not_overwrite_the_first() {
    let case = "second_disposition";
    let (_pool, store) = probe(case).await;

    let submitted = submission("which retry policy", Utc::now());
    store.submit(&submitted).await.unwrap();
    store
        .dispose(submitted.id, TasteDisposition::Filed, Some("task-42"))
        .await
        .unwrap();
    store
        .dispose(submitted.id, TasteDisposition::Superseded, None)
        .await
        .unwrap();

    let filed = store
        .filed_intents(Utc::now() - Duration::hours(1), 10)
        .await
        .unwrap();
    assert_eq!(
        filed.len(),
        1,
        "the submission is still filed, not superseded"
    );
    assert_eq!(filed[0].task_id.as_deref(), Some("task-42"));

    done(case).await;
}

/// Disposing an id nobody submitted is not an error and writes nothing: the
/// only rows this can ever touch are ones the caller read first.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn disposing_an_unknown_id_writes_nothing() {
    let case = "unknown_id";
    let (_pool, store) = probe(case).await;

    store
        .dispose(Uuid::new_v4(), TasteDisposition::Filed, Some("task-42"))
        .await
        .unwrap();
    assert!(store.pending_intents(10).await.unwrap().is_empty());
    assert!(store
        .filed_intents(Utc::now() - Duration::hours(1), 10)
        .await
        .unwrap()
        .is_empty());

    done(case).await;
}

/// The filed window is what keeps a reader from re-reading the whole history
/// every round, so it has to be a window and not a synonym for "everything" --
/// and it has to be the window that says what it means: how long ago the *work*
/// was handed over, not how long ago the submission was made.
///
/// The two are the same instant for a submission filed the round it arrives, so
/// only the case where they come apart shows which one is in use: a submission
/// that sat unclaimed through an outage and was filed afterwards is recent work
/// by every clock the reader drives it with, and a window measured from its
/// submission would drop it on the round its failure would first have been seen
/// -- the failure of a submission whose work had only just begun.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_filed_window_follows_when_the_work_was_handed_over() {
    let case = "filed_window";
    let (pool, store) = probe(case).await;

    let now = Utc::now();
    // Submitted long before the window opens, filed inside it.
    let late_filed = submission("submitted before the window", now - Duration::hours(6));
    // Filed inside the window in wall-clock terms, then backdated: what an
    // older filing looks like once time has passed.
    let old_filed = submission("filed long ago", now - Duration::minutes(10));
    for submitted in [&late_filed, &old_filed] {
        store.submit(submitted).await.unwrap();
        store
            .dispose(submitted.id, TasteDisposition::Filed, Some("task-42"))
            .await
            .unwrap();
    }
    sqlx::query("UPDATE cog_taste_intents SET disposed_at = $2, submitted_at = $2 WHERE id = $1")
        .bind(old_filed.id)
        .bind(now - Duration::hours(2))
        .execute(&pool)
        .await
        .unwrap();

    let recent = store
        .filed_intents(now - Duration::hours(1), 10)
        .await
        .unwrap();
    assert_eq!(
        recent.iter().map(|row| row.intent.id).collect::<Vec<_>>(),
        vec![late_filed.id],
        "窗口要跟着「活交出去的时刻」，不是「提交的时刻」"
    );

    done(case).await;
}

/// The filed read is oldest filing first, so the reader's order is the order the
/// work was handed over rather than the order the submissions were made.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn filed_submissions_come_back_in_the_order_they_were_handed_over() {
    let case = "filed_order";
    let (_pool, store) = probe(case).await;

    let now = Utc::now();
    // Submitted newest-first, so an implementation ordering by submission would
    // answer differently from one ordering by filing.
    let filed_first = submission("filed first", now - Duration::minutes(1));
    let filed_second = submission("filed second", now);
    store.submit(&filed_first).await.unwrap();
    store
        .dispose(filed_first.id, TasteDisposition::Filed, Some("task-42"))
        .await
        .unwrap();
    store.submit(&filed_second).await.unwrap();
    store
        .dispose(filed_second.id, TasteDisposition::Filed, Some("task-42"))
        .await
        .unwrap();

    let filed = store
        .filed_intents(now - Duration::hours(1), 10)
        .await
        .unwrap();
    let subjects: Vec<&str> = filed
        .iter()
        .map(|row| row.intent.subject.as_str())
        .collect();
    assert_eq!(subjects, vec!["filed first", "filed second"]);

    done(case).await;
}

/// A re-sent submission keeps the first one's words. The submitter is answered
/// either way; what must not change is the text a later reader compares the
/// produced work against.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn re_submitting_the_same_id_keeps_the_first_words() {
    let case = "resend";
    let (_pool, store) = probe(case).await;

    let first = submission("which retry policy", Utc::now());
    let mut second = first.clone();
    second.payload = preference_says_prefer("a different answer");
    store.submit(&first).await.unwrap();
    store.submit(&second).await.unwrap();

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 1, "one id is one submission");
    assert_eq!(pending[0].intent.payload, first.payload);

    done(case).await;
}

/// Oldest first, and the cap counts the rows handed over rather than the rows
/// that were there: a caller that asked for one must get one.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn pending_submissions_come_back_oldest_first_within_the_cap() {
    let case = "pending_cap";
    let (_pool, store) = probe(case).await;

    let now = Utc::now();
    // Submitted out of order, so an implementation ordering by insertion would
    // answer differently from one ordering by the time submitted.
    let middle = submission("middle", now - Duration::minutes(30));
    let oldest = submission("oldest", now - Duration::hours(1));
    let newest = submission("newest", now);
    for submitted in [&middle, &oldest, &newest] {
        store.submit(submitted).await.unwrap();
    }

    let all = store.pending_intents(10).await.unwrap();
    let subjects: Vec<&str> = all.iter().map(|row| row.intent.subject.as_str()).collect();
    assert_eq!(subjects, vec!["oldest", "middle", "newest"]);

    let capped = store.pending_intents(1).await.unwrap();
    assert_eq!(capped.len(), 1);
    assert_eq!(capped[0].intent.subject, "oldest");

    done(case).await;
}

/// A payload that no longer parses is one unreadable row, not a failed batch:
/// the submissions beside it are still work, and dropping them would file none
/// of them because of a row nobody can use anyway.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_unreadable_payload_does_not_take_the_rest_of_the_batch_with_it() {
    let case = "unreadable_payload";
    let (pool, store) = probe(case).await;

    let readable = submission("readable", Utc::now());
    store.submit(&readable).await.unwrap();
    // A kind this build cannot spell. Nothing in the system writes one; a row
    // from an older build's vocabulary is the case being covered.
    sqlx::query(
        "INSERT INTO cog_taste_intents \
             (id, subject, submitted_by, submitted_at, payload, status) \
         VALUES ($1, $2, $3, $4, $5, 'pending')",
    )
    .bind(Uuid::new_v4())
    .bind("unreadable")
    .bind(PROBE_SUBMITTER)
    .bind(Utc::now())
    .bind(serde_json::json!({"kind": "a_kind_no_build_spells"}))
    .execute(&pool)
    .await
    .unwrap();

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent.id, readable.id);

    done(case).await;
}

/// The verdict vocabulary crosses the storage boundary as its own tag, not as
/// the shape of the neighbouring arm: a reader that got the wrong variant back
/// for a judgement would file work against the wrong question.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn each_kind_comes_back_as_the_kind_it_was_submitted_as() {
    let case = "kind_roundtrip";
    let (_pool, store) = probe(case).await;

    let now = Utc::now();
    let verdict = TasteIntent {
        id: Uuid::new_v4(),
        subject: "the evaluator that keeps failing".to_string(),
        submitted_by: PROBE_SUBMITTER.to_string(),
        submitted_at: now,
        payload: TasteIntentPayload::EvaluatorVerdict(EvaluatorVerdictIntent {
            verdict: EvaluatorVerdict::Confirmed,
            rationale: "the failures come from the workload, not the check".to_string(),
        }),
        evidence_refs: Vec::new(),
    };
    let preference = submission("which retry policy", now + Duration::seconds(1));
    for submitted in [&verdict, &preference] {
        store.submit(submitted).await.unwrap();
    }

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].intent.payload, verdict.payload);
    assert_eq!(pending[0].intent.payload.kind(), "evaluator_verdict");
    assert_eq!(pending[1].intent.payload, preference.payload);
    assert_eq!(pending[1].intent.payload.kind(), "direction_preference");

    done(case).await;
}

/// The material a judgement was made against survives the round trip, on the
/// kind that changes a standard as much as on the kind that judges a result.
///
/// This is the edge a later reader follows from "this judgement was wrong" back
/// to what it was wrong about; a store that dropped it would leave the subject
/// — the submitter's own words — as the only thing pointing anywhere, and words
/// are not a key.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_submission_keeps_the_material_it_named() {
    let case = "material_roundtrip";
    let (_pool, store) = probe(case).await;

    let mut submitted = submission("which retry policy", Utc::now());
    submitted.evidence_refs = vec![
        "tool-output-41-0002-0000".to_string(),
        "https://example.invalid/incident/41".to_string(),
    ];
    store.submit(&submitted).await.unwrap();

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].intent.evidence_refs, submitted.evidence_refs,
        "the material named at submission did not come back with the row"
    );
    assert_eq!(pending[0].intent, submitted);

    done(case).await;
}

/// A table that predates the column is brought forward by the same call that
/// creates it, and the rows already in it stay readable.
///
/// This is the deployment path, not a hypothetical: a live database holds rows
/// written before the column existed, and the reader runs against it the moment
/// the new build rolls out. A migration that only ran on a fresh table would
/// leave every existing deployment rejecting its own submissions until somebody
/// dropped the table by hand.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_row_from_before_the_column_reads_back_without_materials() {
    let case = "pre_column_row";
    let (pool, store) = probe(case).await;

    // Take the table back to the shape it had before the column, then let the
    // store bring it forward the way a rollout would.
    sqlx::query("ALTER TABLE cog_taste_intents DROP COLUMN evidence_refs")
        .execute(&pool)
        .await
        .unwrap();
    store.init_schema().await.unwrap();

    // A row written the way the old build wrote it: the column is left to its
    // default, exactly as an insert that never named it would.
    let old_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO cog_taste_intents \
             (id, subject, submitted_by, submitted_at, payload, status) \
         VALUES ($1, $2, $3, $4, $5, 'pending')",
    )
    .bind(old_id)
    .bind("an older judgement")
    .bind(PROBE_SUBMITTER)
    .bind(as_the_column_holds_it(Utc::now()))
    .bind(serde_json::to_value(preference_says_prefer("the cheap one")).unwrap())
    .execute(&pool)
    .await
    .unwrap();

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 1, "the pre-column row was not readable");
    assert_eq!(pending[0].intent.id, old_id);
    assert_eq!(
        pending[0].intent.evidence_refs,
        Vec::<String>::new(),
        "a row that named no material must read as having named none"
    );

    done(case).await;
}
