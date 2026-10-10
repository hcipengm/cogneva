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
//! shape. Every row written here carries [`PROBE_SUBMITTER`] and is removed
//! afterwards, so a throwaway database is left as it was found.

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use cog_core::{
    DirectionPreferenceIntent, EvaluatorVerdict, EvaluatorVerdictIntent, TasteDisposition,
    TasteIntent, TasteIntentPayload, TasteIntentSink, TasteIntentSource,
};
use cog_storage::PostgresTasteIntentStore;

/// Stamped on every row this test writes, so cleanup touches nothing else.
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

fn submission(subject: &str, at: DateTime<Utc>) -> TasteIntent {
    TasteIntent {
        id: Uuid::new_v4(),
        subject: subject.to_string(),
        submitted_by: PROBE_SUBMITTER.to_string(),
        submitted_at: at,
        payload: preference_says_prefer("the cheap one"),
    }
}

/// Both faces: one caller may hold only the sink, so the store is used the way
/// its two consumer kinds use it rather than as one object with two methods.
async fn store(pool: &PgPool) -> PostgresTasteIntentStore {
    let store = PostgresTasteIntentStore::new(pool.clone());
    store.init_schema().await.unwrap();
    store
}

async fn remove(pool: &PgPool) {
    sqlx::query("DELETE FROM cog_taste_intents WHERE submitted_by = $1")
        .bind(PROBE_SUBMITTER)
        .execute(pool)
        .await
        .unwrap();
}

/// A submission is readable while nothing has claimed it, and reading it back
/// returns what was submitted rather than a shape that only looks the same.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_submission_is_pending_until_something_claims_it() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// Filing is what takes a submission out of the queue, and the task it names
/// comes back with it — that pair is the join the rest of the system follows
/// from what was asked to what was done.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_filed_submission_leaves_the_queue_and_carries_its_task() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// The first decision about a submission is the one that stands. A claimer
/// that crashed after filing and came back with a different ending must not be
/// able to overwrite the ending the work was actually filed under.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_second_disposition_does_not_overwrite_the_first() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// Disposing an id nobody submitted is not an error and writes nothing: the
/// only rows this can ever touch are ones the caller read first.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn disposing_an_unknown_id_writes_nothing() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
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
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// The filed read is oldest filing first, so the reader's order is the order the
/// work was handed over rather than the order the submissions were made.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn filed_submissions_come_back_in_the_order_they_were_handed_over() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// A re-sent submission keeps the first one's words. The submitter is answered
/// either way; what must not change is the text a later reader compares the
/// produced work against.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn re_submitting_the_same_id_keeps_the_first_words() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

    let first = submission("which retry policy", Utc::now());
    let mut second = first.clone();
    second.payload = preference_says_prefer("a different answer");
    store.submit(&first).await.unwrap();
    store.submit(&second).await.unwrap();

    let pending = store.pending_intents(10).await.unwrap();
    assert_eq!(pending.len(), 1, "one id is one submission");
    assert_eq!(pending[0].intent.payload, first.payload);

    remove(&pool).await;
}

/// Oldest first, and the cap counts the rows handed over rather than the rows
/// that were there: a caller that asked for one must get one.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn pending_submissions_come_back_oldest_first_within_the_cap() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// A payload that no longer parses is one unreadable row, not a failed batch:
/// the submissions beside it are still work, and dropping them would file none
/// of them because of a row nobody can use anyway.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn an_unreadable_payload_does_not_take_the_rest_of_the_batch_with_it() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}

/// The verdict vocabulary crosses the storage boundary as its own tag, not as
/// the shape of the neighbouring arm: a reader that got the wrong variant back
/// for a judgement would file work against the wrong question.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn each_kind_comes_back_as_the_kind_it_was_submitted_as() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let store = store(&pool).await;
    remove(&pool).await;

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

    remove(&pool).await;
}
