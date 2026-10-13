//! The change ledger's storage, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that the reason a row is
//! `undecided` reaches its own column and reads back, that a later measurement of
//! another state clears it the way `landed` clears `landed_at`, that a reason
//! outside the closed set is refused rather than stored, that the revision a row
//! entered at is written once and never moved, and that a retirement queued on
//! its own intake is folded into the account by the ledger's writer and is
//! idempotent to re-fold. None of it can be checked without a server -- the
//! coupling between the reason and the state lives in the statement and in a CHECK
//! the server enforces, "a reason outside the set is refused" is a fact about an
//! ENUM or a CHECK, and the set-once rule of the entry revision is a fact about
//! which branch of an `ON CONFLICT` writes a column.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test change_ledger_store -- --ignored
//! ```
//!
//! The throwaway database starts empty, so the migrations are applied first: the
//! table's layout is owned by the migrations, and a column added in one place and
//! not the other fails only at runtime. Every row carries a per-run marker in its
//! scope and is deleted at the end, so the database is left as it was found.

use sqlx::PgPool;

use cog_core::{
    ChangeIdentity, ChangeLedgerStore, ChangeMeasurement, ChangeReading, ChangeSource, ChangeState,
    RetireReason,
};
use cog_storage::PostgresChangeLedgerStore;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

async fn store() -> (PostgresChangeLedgerStore, PgPool, String) {
    let url = database_url();
    migrate_once(&url).await;
    let pool = PgPool::connect(&url).await.expect("connect to the database");
    let store = PostgresChangeLedgerStore::new(pool.clone());
    (store, pool, format!("probe-change-ledger:{}", uuid::Uuid::new_v4()))
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
    let pool = PgPool::connect(url).await.expect("connect to take the lock");
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

fn identity(marker: &str, suffix: &str) -> ChangeIdentity {
    ChangeIdentity::new("grep", format!("{marker}/{suffix}"), "fn probe")
}

/// A reason reaches its own column, and a later measurement of another state
/// clears it -- the same set-and-clear rule `landed_at` lives by.
///
/// The clearing is the half a reason column can get wrong quietly: a reason left
/// beside a state it no longer describes reads as one, and the row would look
/// stuck for a reason nothing measured. It is checked by measuring the row again
/// in a settled state and reading the column back, not by trusting the update
/// clause's text.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn an_undecided_reason_reaches_its_column_and_clears_when_the_state_leaves_undecided() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "stuck");

    store
        .apply_measurement(
            &ChangeMeasurement::undecided(id.clone(), "scope_outside_tree")
                .at_revision("probe-rev-1"),
        )
        .await
        .expect("write an undecided measurement");

    let row = store.get(&id).await.expect("read the row").expect("the row exists");
    assert_eq!(row.state, ChangeState::Undecided);
    assert_eq!(
        row.unsettled_reason.as_deref(),
        Some("scope_outside_tree"),
        "the reason a row is undecided must survive the write, or no reader can ask which rows are stuck for which reason"
    );
    assert_eq!(row.measured_rev.as_deref(), Some("probe-rev-1"));
    assert_eq!(row.retire_reason, None, "an undecided row is not retired");

    // A settled measurement of the same identity moves the row out of undecided
    // and must leave no reason behind.
    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("probe-rev-2"),
        )
        .await
        .expect("write a landed measurement");

    let row = store.get(&id).await.expect("read the row back").expect("the row exists");
    assert_eq!(row.state, ChangeState::Landed);
    assert_eq!(
        row.unsettled_reason, None,
        "a reason left beside a state it does not describe would read as one"
    );

    cleanup(&pool, &marker).await;
}

/// A plain measurement of an outstanding state carries no reason, and a reason
/// outside the closed set is refused rather than stored.
///
/// The refusal is what makes the column a closed set rather than free text: a
/// name the judge never emits would count as a reason in a `GROUP BY` and name a
/// bucket no code can act on.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_plain_row_carries_no_reason_and_a_reason_outside_the_set_is_refused() {
    let (store, pool, marker) = store().await;

    let pending = identity(&marker, "pending");
    store
        .apply_measurement(&ChangeMeasurement::new(pending.clone(), ChangeState::Pending))
        .await
        .expect("write a pending measurement");
    let row = store.get(&pending).await.expect("read").expect("the row exists");
    assert_eq!(row.unsettled_reason, None, "a pending row carries no unsettled reason");

    // The store's coupling accepts this -- undecided with a reason -- so the
    // refusal has to come from the column's own closed set, which is the fact
    // under test.
    let bogus = identity(&marker, "bogus");
    let result = store
        .apply_measurement(&ChangeMeasurement::undecided(bogus.clone(), "not_a_real_reason"))
        .await;
    assert!(
        result.is_err(),
        "a reason outside the closed set must be refused by the column, not stored"
    );
    assert!(
        store.get(&bogus).await.expect("read").is_none(),
        "the refused row must not have been written"
    );

    cleanup(&pool, &marker).await;
}

/// A row's entry revision is written once and never moved: not by a second
/// document citing the same change, and not by a measurement.
///
/// The set-once rule is the whole point of the column -- it records which tree
/// the account was reading when the row entered, so anything that moves it later
/// turns "the tree it entered at" into "some tree it was touched by".
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_entry_revision_is_written_once_and_never_moved() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "entered");
    let reading = |doc: &str| ChangeReading {
        identity: id.clone(),
        title: None,
        assertion: None,
        source: ChangeSource {
            doc: doc.to_string(),
            verdict: None,
        },
    };

    // The row enters at one revision...
    store
        .record_readings(&[reading("doc-a")], Some("rev-one"))
        .await
        .expect("record the reading");
    assert_eq!(entry_rev(&pool, &id).await.as_deref(), Some("rev-one"));

    // ...a second document citing the same change does not move it...
    store
        .record_readings(&[reading("doc-b")], Some("rev-two"))
        .await
        .expect("record a second reading");
    assert_eq!(
        entry_rev(&pool, &id).await.as_deref(),
        Some("rev-one"),
        "a second reading must not move the revision the row entered at"
    );

    // ...and neither does a measurement.
    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("rev-three"),
        )
        .await
        .expect("measure the row");
    assert_eq!(
        entry_rev(&pool, &id).await.as_deref(),
        Some("rev-one"),
        "a measurement must not move the revision the row entered at"
    );

    cleanup(&pool, &marker).await;
}

/// A row created by a measurement entered by being measured, so its entry
/// revision is that measurement's -- the one path that sets the column without a
/// reading.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_created_by_a_measurement_enters_at_that_measurements_revision() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "measured-in");

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("rev-born"),
        )
        .await
        .expect("create the row by measuring it");
    assert_eq!(entry_rev(&pool, &id).await.as_deref(), Some("rev-born"));

    cleanup(&pool, &marker).await;
}

/// A queued retirement, folded by the account's own writer, moves the row to
/// `retired` with its reason and marks the intake row folded -- and applying it
/// twice is applying it once.
///
/// The retirement is not a reading and cannot ride the declaration intake, so it
/// is queued on its own table and folded here. What is under test is the coupling
/// the fold is responsible for: that the reason the classifier gave reaches the
/// ledger row rather than being dropped, that the intake row is marked folded so
/// the backlog returns to zero, and that a re-fold of the same identity changes
/// nothing -- the idempotence that lets an interrupted fold simply be repeated.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_queued_retirement_folds_into_the_account_and_is_idempotent() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "fixture");

    // The row exists in the account as outstanding before it is retired: a
    // retirement moves a row's state, it does not merely annotate an absent one.
    store
        .apply_measurement(&ChangeMeasurement::undecided(id.clone(), "scope_outside_tree"))
        .await
        .expect("write the row to be retired");
    assert_eq!(store.pending_retirements().await.expect("read the backlog"), 0);

    queue_retirement(&pool, &id, "fixture", "classifier:probe", &marker).await;
    assert_eq!(
        store.pending_retirements().await.expect("read the backlog"),
        1,
        "a queued retirement is the backlog until it is folded"
    );

    let folded = store.fold_retirements(10).await.expect("fold the retirement");
    assert_eq!(folded, 1, "the one queued retirement must be folded");

    let row = store.get(&id).await.expect("read").expect("the row exists");
    assert_eq!(row.state, ChangeState::Retired);
    assert_eq!(
        row.retire_reason,
        Some(RetireReason::Fixture),
        "the reason the classifier gave must reach the row, or a retired row cannot say why"
    );
    assert_eq!(
        row.unsettled_reason, None,
        "a reason beside a state it no longer describes would read as one"
    );
    assert_eq!(
        store.pending_retirements().await.expect("read the backlog"),
        0,
        "the intake row must be marked folded, or the backlog never clears"
    );

    // Applying the same retirement again is applying it once: the marker is
    // already down, so nothing is taken.
    let again = store.fold_retirements(10).await.expect("fold again");
    assert_eq!(again, 0, "a folded retirement is not folded twice");

    cleanup(&pool, &marker).await;
}

/// A retirement is re-examinable: a later measurement of another state clears it
/// the way a later measurement clears `landed`.
///
/// The retirement records what the classifier found, not a verdict that is true
/// for good. The check is the same rule `retired_at` and `retire_reason` follow
/// in the write statement -- a row re-measured out of `retired` carries no reason
/// beside a state that is no longer retired.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_later_measurement_clears_a_retirement() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "re-examined");

    queue_retirement(&pool, &id, "fixture", "classifier:probe", &marker).await;
    store.fold_retirements(10).await.expect("fold the retirement");
    assert_eq!(
        store.get(&id).await.expect("read").expect("the row exists").state,
        ChangeState::Retired
    );

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("probe-rev"),
        )
        .await
        .expect("measure the row again");

    let row = store.get(&id).await.expect("read back").expect("the row exists");
    assert_eq!(row.state, ChangeState::Landed);
    assert_eq!(
        row.retire_reason, None,
        "a reason left beside a state that is no longer retired would read as one"
    );

    cleanup(&pool, &marker).await;
}

/// Queue one retirement the way the classifier does: a plain write to the intake
/// table, not through the store -- the store is the account's writer, and the
/// whole point of the intake is that a classifier is not.
async fn queue_retirement(
    pool: &PgPool,
    id: &ChangeIdentity,
    reason: &str,
    source: &str,
    marker: &str,
) {
    sqlx::query(
        "INSERT INTO change_ledger_retired (kind, scope, literal, reason, source) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (kind, scope, literal) DO NOTHING",
    )
    .bind(&id.kind)
    .bind(&id.scope)
    .bind(&id.literal)
    .bind(reason)
    .bind(source)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("queue the retirement for {marker}: {e}"));
}

async fn entry_rev(pool: &PgPool, id: &ChangeIdentity) -> Option<String> {
    sqlx::query_scalar(
        "SELECT first_seen_rev FROM change_ledger \
         WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&id.kind)
    .bind(&id.scope)
    .bind(&id.literal)
    .fetch_one(pool)
    .await
    .expect("read the entry revision")
}

async fn cleanup(pool: &PgPool, marker: &str) {
    sqlx::query("DELETE FROM change_ledger WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(pool)
        .await
        .expect("clean up the probe rows");
    // The intake is left as it was too: a queued retirement names the same
    // marker, and a row left behind would be folded by a later run's pass.
    sqlx::query("DELETE FROM change_ledger_retired WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(pool)
        .await
        .expect("clean up the probe retirements");
}
