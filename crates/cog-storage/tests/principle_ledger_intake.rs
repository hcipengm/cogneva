//! The principle ledger's intake, against a live PostgreSQL.
//!
//! What these tests check is what the statement decides, and it is the one thing
//! a reading of the account cannot show: which re-pushes write a row. The intake
//! is keyed on `(slug, source)`, so every later reading of the same principle
//! from the same source lands on the row already there, and whether that row is
//! rewritten or left alone is a property of the conflict clause -- not of the
//! pusher, not of the fold. The shape it used to have dropped every one of them.
//!
//! Two failure directions are checked, and each is invisible from the other side:
//!
//! - A reading that says something the queued one did not -- a tier declared
//!   where the first reading could not see the column -- has to re-queue, or the
//!   account keeps the older reading for good while the pass reports success.
//! - A reading that says nothing new has to write nothing, and a field it
//!   carries nothing for may not clear what is stored. Both are what keeps a
//!   full-corpus re-push (which is what a push that failed part way is retried
//!   with) from re-queueing the whole backlog and resetting the fold's own marks
//!   on rows whose values did not move.
//! - A reading that declares no tier has to clear the tier stored, which is the
//!   opposite sign of the bullet above and a deliberate exception to it: the
//!   tier's `NULL` is the account's word for "nothing is declared here", so a
//!   merge that kept the stored tier would make that word unsayable and leave a
//!   tier that could be declared and never taken back. One statement has to get
//!   both directions right, which is why both are checked.
//!
//! Neither can be settled without a server: which branch of an `ON CONFLICT`
//! runs, and whether `rows_affected` counts a row, are facts about the server.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test principle_ledger_intake -- --ignored
//! ```
//!
//! The throwaway database starts empty, so the migrations are applied first, and
//! every row carries a per-run marker in its slug and is deleted at the end.

use sqlx::PgPool;
use tokio::sync::Mutex;

use cog_core::{PrincipleCheckKind, PrincipleLedgerStore, PrincipleReading};
use cog_storage::PostgresPrincipleLedgerStore;

/// The fold is one consumer of one queue.
///
/// It takes the oldest unfolded readings in the database and offers no way to
/// ask for a probe's own, so two cases running at once fold each other's rows:
/// one would read its own mark off a queue the other had already moved. Each
/// case here asserts a sequence of pushes and folds, which is a story about the
/// queue rather than about a row, so they are serialized instead of merged into
/// one -- the shared object is the queue, and it is a singleton.
static QUEUE: Mutex<()> = Mutex::const_new(());

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

async fn store() -> (PostgresPrincipleLedgerStore, PgPool, String) {
    let url = database_url();
    migrate_once(&url).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    let store = PostgresPrincipleLedgerStore::new(pool.clone());
    (
        store,
        pool,
        format!("probe-principle-intake:{}", uuid::Uuid::new_v4()),
    )
}

/// Apply the migrations under a session advisory lock.
///
/// The tests in this file run in parallel and each calls this, whose
/// `CREATE TABLE IF NOT EXISTS` statements race on a fresh database -- the loser
/// hits `pg_type_typname_nsp_index`. The lock makes the DDL mutually exclusive:
/// the second caller waits for the first and then finds every version already
/// applied. The lock is held on a connection separate from the migrator's own
/// pool, so it stays held while the migrator runs.
async fn migrate_once(url: &str) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to take the lock");
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(97_002_i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    cog_storage::migrate::Migrator::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .run(url)
        .await
        .expect("apply the migrations to the throwaway database");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(97_002_i64)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

/// The reading one source pushes. Everything a later case does not change is
/// filled in, so a field left at `None` in a case reads as "this reading says
/// nothing about it" rather than as an omission.
fn reading(slug: &str, source: &str) -> PrincipleReading {
    PrincipleReading {
        slug: slug.to_string(),
        source: source.to_string(),
        lead: Some("a rule the probe names".into()),
        carrier_path: Some("memory/probe-intake.md".into()),
        carrier_sha256: None,
        host_gate: None,
        check_kind: None,
    }
}

/// Whether the queue still holds something to fold for this principle: the mark
/// the fold writes is the whole question, and it is asked of the row rather than
/// of a global pending count, which the tests in this file share.
async fn queued(pool: &PgPool, slug: &str) -> bool {
    sqlx::query_scalar("SELECT folded_at IS NULL FROM principle_ledger_declared WHERE slug = $1")
        .bind(slug)
        .fetch_one(pool)
        .await
        .expect("read the queue row")
}

async fn cleanup(pool: &PgPool, marker: &str) {
    for table in ["principle_ledger", "principle_ledger_declared"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE slug LIKE $1"))
            .bind(format!("{marker}/%"))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("clean up {table}: {e}"));
    }
}

/// A reading that carries something the queued one did not re-queues, and the
/// next fold moves the account onto it.
///
/// This is the regression the intake is checked for. A reading taken before a
/// column existed, or by a reader that could not yet see a field, files `NULL`
/// there; the corrected reading arrives later under the same `(slug, source)`,
/// and a conflict clause that leaves the existing row alone drops it while
/// counting the pass as done. Nothing downstream can say otherwise -- the row is
/// only ever re-read from what the queue holds, so a reading the queue refused
/// is a reading the account never gets.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_later_reading_that_declares_a_tier_re_queues_and_the_fold_carries_it_through() {
    let _serialized = QUEUE.lock().await;
    let (store, pool, marker) = store().await;
    let slug = format!("{marker}/tier");
    let source = "probe-intake";

    assert_eq!(
        store.declare(&[reading(&slug, source)]).await.unwrap(),
        1,
        "the first reading queues"
    );
    store
        .fold_declarations(100)
        .await
        .expect("fold the first reading");
    assert!(!queued(&pool, &slug).await, "the fold marked it");
    assert_eq!(
        store
            .get(&slug)
            .await
            .unwrap()
            .expect("the account holds it")
            .check_kind,
        None,
        "the first reading declared no tier, so the row carries none"
    );

    let mut corrected = reading(&slug, source);
    corrected.check_kind = Some(PrincipleCheckKind::Tree);
    assert_eq!(
        store.declare(&[corrected]).await.unwrap(),
        1,
        "a reading that says what the queued one did not has to re-queue; \
         writing no row here is the reading being dropped"
    );
    assert!(
        queued(&pool, &slug).await,
        "the re-queued reading has to be there for the next fold to take"
    );

    store
        .fold_declarations(100)
        .await
        .expect("fold the corrected reading");
    assert_eq!(
        store
            .get(&slug)
            .await
            .unwrap()
            .expect("the account holds it")
            .check_kind,
        Some(PrincipleCheckKind::Tree),
        "the account has to end on the reading that was pushed last"
    );

    cleanup(&pool, &marker).await;
}

/// A re-push that says nothing new writes no row, before and after a fold.
///
/// Both halves matter and the second is the one that bites: the first keeps a
/// batch that is re-sent because the pusher could not tell whether it landed
/// from doubling the queue, and the second keeps a full-corpus re-push from
/// resurrecting every row the fold has already marked -- which, with a fold that
/// takes a bounded batch per tick, is a queue that never empties.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_reading_that_says_nothing_new_writes_no_row_and_leaves_the_mark_alone() {
    let _serialized = QUEUE.lock().await;
    let (store, pool, marker) = store().await;
    let slug = format!("{marker}/repeat");
    let source = "probe-intake";
    let mut first = reading(&slug, source);
    first.check_kind = Some(PrincipleCheckKind::Shape);
    first.host_gate = Some("principle-gate.sh".into());

    assert_eq!(
        store.declare(&[first.clone()]).await.unwrap(),
        1,
        "the first reading queues"
    );
    assert_eq!(
        store.declare(&[first.clone()]).await.unwrap(),
        0,
        "the same reading twice is one row: the second writes nothing"
    );

    store.fold_declarations(100).await.expect("fold it");
    assert_eq!(
        store.declare(&[first]).await.unwrap(),
        0,
        "a re-push of what the folded row already says must not re-queue it"
    );
    assert!(
        !queued(&pool, &slug).await,
        "the fold's mark has to survive a repeat of the reading it folded"
    );

    cleanup(&pool, &marker).await;
}

/// A reading that declares no tier takes back a tier that was declared earlier,
/// in the queue and in the account.
///
/// `check_kind` is the one column where `NULL` is not "this reading says nothing"
/// but the account's own word for "no tier has been declared here", and a merge
/// that keeps the stored tier makes that word unwritable: a tier declared by
/// mistake -- or by a reading whose generator has since been fixed -- could never
/// be taken back, and every later fold would carry it forward. The direction
/// matters as much as the value: this is the opposite sign of the case below,
/// and one statement has to get both of them right.
///
/// The three steps are the whole story. The withdrawal is a change (it re-queues),
/// the fold carries it through, and a re-push of the same withdrawal is not a
/// change again -- without that last one, a corpus where a tier was withdrawn
/// would re-queue that row on every pass forever.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_reading_that_declares_no_tier_takes_back_the_one_the_account_holds() {
    let _serialized = QUEUE.lock().await;
    let (store, pool, marker) = store().await;
    let slug = format!("{marker}/withdrawn");
    let source = "probe-intake";
    let mut declared = reading(&slug, source);
    declared.check_kind = Some(PrincipleCheckKind::Shape);
    assert_eq!(
        store.declare(&[declared]).await.unwrap(),
        1,
        "the tier queues"
    );
    store.fold_declarations(100).await.expect("fold it");
    assert_eq!(
        store
            .get(&slug)
            .await
            .unwrap()
            .expect("it is there")
            .check_kind,
        Some(PrincipleCheckKind::Shape),
        "the account holds the declared tier"
    );

    // The same reading with no tier in it: nobody declares this one any more.
    let withdrawn = reading(&slug, source);
    assert_eq!(
        store
            .declare(std::slice::from_ref(&withdrawn))
            .await
            .unwrap(),
        1,
        "a withdrawn tier is a change to the row, so it has to re-queue"
    );
    store
        .fold_declarations(100)
        .await
        .expect("fold the withdrawal");
    assert_eq!(
        store
            .get(&slug)
            .await
            .unwrap()
            .expect("it is there")
            .check_kind,
        None,
        "the account has to end with no tier declared, not with the tier it had"
    );
    assert_eq!(
        store.declare(&[withdrawn]).await.unwrap(),
        0,
        "a withdrawal that is already the row's value is not a change again"
    );
    assert!(
        !queued(&pool, &slug).await,
        "and it may not leave the row re-queued for the next fold"
    );

    cleanup(&pool, &marker).await;
}

/// A reading that carries nothing for a field neither clears it nor counts as a
/// change to it.
///
/// `NULL` is not "this field is empty" here -- it is "this reading says nothing
/// about it" -- and the account already reads it that way. Counting the incoming
/// value instead of the value the write would produce would make every reading
/// that knows less than the stored row a change, re-queue it, write the same row,
/// and do it again on the next pass: a corpus-sized livelock that looks like
/// activity.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_reading_that_carries_nothing_for_a_field_neither_clears_it_nor_re_queues() {
    let _serialized = QUEUE.lock().await;
    let (store, pool, marker) = store().await;
    let slug = format!("{marker}/partial");
    let source = "probe-intake";
    let mut first = reading(&slug, source);
    first.check_kind = Some(PrincipleCheckKind::Shape);
    assert_eq!(
        store.declare(&[first]).await.unwrap(),
        1,
        "the first queues"
    );
    store.fold_declarations(100).await.expect("fold it");

    // The same principles, read by a source that knows only the tier.
    let mut silent = reading(&slug, source);
    silent.lead = None;
    silent.carrier_path = None;
    silent.check_kind = Some(PrincipleCheckKind::Shape);
    assert_eq!(
        store.declare(&[silent]).await.unwrap(),
        0,
        "a reading that carries nothing for a field is not a change to it"
    );

    // And when it does say something new, the fields it stays silent about
    // survive the write rather than being cleared by their `NULL`.
    let mut moved = reading(&slug, source);
    moved.lead = None;
    moved.carrier_path = None;
    moved.check_kind = Some(PrincipleCheckKind::Live);
    assert_eq!(store.declare(&[moved]).await.unwrap(), 1, "the tier moved");
    store.fold_declarations(100).await.expect("fold the move");

    let row = store
        .get(&slug)
        .await
        .unwrap()
        .expect("the account holds it");
    assert_eq!(row.check_kind, Some(PrincipleCheckKind::Live));
    assert_eq!(
        row.lead.as_deref(),
        Some("a rule the probe names"),
        "a reading that says nothing about the sentence may not clear it"
    );
    assert_eq!(row.carrier_path.as_deref(), Some("memory/probe-intake.md"));

    cleanup(&pool, &marker).await;
}
