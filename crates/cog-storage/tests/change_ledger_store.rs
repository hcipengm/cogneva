//! The change ledger's storage, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that the reason a row is
//! `undecided` reaches its own column and reads back, that a later measurement of
//! another state clears it the way `landed` clears `landed_at`, that a reason
//! outside the closed set is refused rather than stored, that the revision a row
//! entered at is written once and never moved, and that a retirement queued on
//! its own intake is folded into the account by the ledger's writer and is
//! idempotent to re-fold, and that a document that corrects what it declares
//! reaches both the queue and the account's citation of it. None of it can be
//! checked without a server -- the
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
    ChangeIdentity, ChangeLedgerStore, ChangeMeasurement, ChangeReading, ChangeReopen,
    ChangeSource, ChangeState, RetireReason,
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
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    let store = PostgresChangeLedgerStore::new(pool.clone());
    (
        store,
        pool,
        format!("probe-change-ledger:{}", uuid::Uuid::new_v4()),
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

    let row = store
        .get(&id)
        .await
        .expect("read the row")
        .expect("the row exists");
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

    let row = store
        .get(&id)
        .await
        .expect("read the row back")
        .expect("the row exists");
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
        .apply_measurement(&ChangeMeasurement::new(
            pending.clone(),
            ChangeState::Pending,
        ))
        .await
        .expect("write a pending measurement");
    let row = store
        .get(&pending)
        .await
        .expect("read")
        .expect("the row exists");
    assert_eq!(
        row.unsettled_reason, None,
        "a pending row carries no unsettled reason"
    );

    // The store's coupling accepts this -- undecided with a reason -- so the
    // refusal has to come from the column's own closed set, which is the fact
    // under test.
    let bogus = identity(&marker, "bogus");
    let result = store
        .apply_measurement(&ChangeMeasurement::undecided(
            bogus.clone(),
            "not_a_real_reason",
        ))
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
        .apply_measurement(&ChangeMeasurement::undecided(
            id.clone(),
            "scope_outside_tree",
        ))
        .await
        .expect("write the row to be retired");
    assert_eq!(
        store.pending_retirements().await.expect("read the backlog"),
        0
    );

    queue_retirement(&pool, &id, "fixture", "classifier:probe", &marker).await;
    assert_eq!(
        store.pending_retirements().await.expect("read the backlog"),
        1,
        "a queued retirement is the backlog until it is folded"
    );

    let folded = store
        .fold_retirements(10)
        .await
        .expect("fold the retirement");
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
    store
        .fold_retirements(10)
        .await
        .expect("fold the retirement");
    assert_eq!(
        store
            .get(&id)
            .await
            .expect("read")
            .expect("the row exists")
            .state,
        ChangeState::Retired
    );

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("probe-rev"),
        )
        .await
        .expect("measure the row again");

    let row = store
        .get(&id)
        .await
        .expect("read back")
        .expect("the row exists");
    assert_eq!(row.state, ChangeState::Landed);
    assert_eq!(
        row.retire_reason, None,
        "a reason left beside a state that is no longer retired would read as one"
    );

    cleanup(&pool, &marker).await;
}

/// A corrected declaration reaches the account's citation of the document.
///
/// The correction is made the way the producer's own verb makes it -- the
/// declaration is withdrawn from the intake and declared again -- and what this
/// checks is the half that used to be missing: the corrected reading is recorded,
/// and the row went on citing the verdict the document made first, so the
/// disagreement between a document's claim and a measurement was reported against
/// a claim the document no longer made. It is not visible without a server: what
/// `jsonb` ends up holding after an `ON CONFLICT` update is a fact about the
/// server, and so is whether the second declaration reached the fold at all.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_corrected_declaration_reaches_the_accounts_citation_of_its_document() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "corrected");
    let doc = format!("docs/{marker}.md");
    let declaration = |verdict: &str| ChangeReading {
        identity: id.clone(),
        title: Some("a change the probe declares".into()),
        assertion: Some("the probe's assertion".into()),
        source: ChangeSource {
            doc: doc.clone(),
            verdict: Some(verdict.to_string()),
        },
    };

    assert_eq!(
        store.declare(&[declaration("undecided")]).await.unwrap(),
        1,
        "the first declaration queues"
    );
    assert_eq!(
        store.declare(&[declaration("undecided")]).await.unwrap(),
        0,
        "re-sending what is already queued is not a correction"
    );
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold the declaration");
    let cited = store
        .get(&id)
        .await
        .expect("read the row")
        .expect("the row exists")
        .sources;
    assert_eq!(cited.len(), 1, "one document, one citation: {cited:?}");
    assert_eq!(cited[0].verdict.as_deref(), Some("undecided"));

    // The producer's correction verb: take the declaration back off the intake
    // by the same four columns it was declared with, then declare it again.
    withdraw_declaration(&pool, &id, &doc).await;
    assert_eq!(
        store.declare(&[declaration("landed")]).await.unwrap(),
        1,
        "the corrected declaration is a fresh row, so it queues"
    );
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold the correction");
    let cited = store
        .get(&id)
        .await
        .expect("read the row")
        .expect("the row exists")
        .sources;
    assert_eq!(
        cited.len(),
        1,
        "the correction replaces the citation rather than adding one: {cited:?}"
    );
    assert_eq!(
        cited[0].verdict.as_deref(),
        Some("landed"),
        "the account cites what the document says now, not what it said first"
    );

    cleanup(&pool, &marker).await;
}

/// A declaration nobody has measured enters as work still owed, not as a stuck
/// row.
///
/// `undecided` is not "nobody has looked yet": it is what a measurement writes
/// when a check cannot be settled for a reason that is a property of the row, and
/// the store couples it to that reason -- there is no way to build a reason-less
/// `undecided` measurement, and a row carrying one reads as a stuck row whose
/// reason was lost. A freshly folded declaration is neither, so it enters
/// `pending` with no `measured_at`; that absence is what a reader uses to tell a
/// row nothing has looked at from one measured a while ago. Entering as
/// `undecided` would state a verdict no look reached and hide the row among the
/// ones that predate the reason column.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_freshly_folded_declaration_enters_pending_and_unmeasured() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "fresh");
    store
        .declare(&[ChangeReading {
            identity: id.clone(),
            title: Some("a change the probe declares".into()),
            assertion: Some("the probe's assertion".into()),
            source: ChangeSource {
                doc: format!("docs/{marker}.md"),
                verdict: Some("pending".into()),
            },
        }])
        .await
        .expect("queue the declaration");
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold the declaration");

    let row = store
        .get(&id)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(
        row.state,
        ChangeState::Pending,
        "a declaration nothing has measured is outstanding work, not a row stuck on a reason"
    );
    assert_eq!(
        row.unsettled_reason, None,
        "a state that is not undecided carries no reason"
    );
    assert!(
        row.measured_at.is_none(),
        "nothing has measured it, and the row must not read as measured"
    );

    cleanup(&pool, &marker).await;
}

/// The landed set is the outstanding set's complement, read oldest-looked-at
/// first -- and the ordering is the half a query like this gets wrong, because a
/// caller takes a slice of it and a slice off the wrong end re-asks the same rows
/// forever.
///
/// Both orders are asserted against the same three rows, since the reading that
/// matters is that the two reads partition the state: a row in the landed set is
/// absent from the outstanding one, and the other way round.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn landed_rows_are_the_other_half_of_outstanding_read_oldest_first() {
    let (store, pool, marker) = store().await;
    let owing = identity(&marker, "owing");
    let fresh = identity(&marker, "fresh");
    let stale = identity(&marker, "stale");

    for (id, state) in [
        (&owing, ChangeState::Pending),
        (&fresh, ChangeState::Landed),
        (&stale, ChangeState::Landed),
    ] {
        store
            .apply_measurement(
                &ChangeMeasurement::new(id.clone(), state).at_revision("probe-rev-1"),
            )
            .await
            .expect("measure a probe row");
    }
    // Two measurements in one run can share a timestamp, and the order under test
    // is the one the column decides -- so one of the two is aged by hand rather
    // than by sleeping, which would put the test's runtime in its verdict.
    sqlx::query(
        "UPDATE change_ledger SET measured_at = now() - interval '2 hours' \
                 WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&stale.kind)
    .bind(&stale.scope)
    .bind(&stale.literal)
    .execute(&pool)
    .await
    .expect("age one landed row");

    let due: Vec<ChangeIdentity> = store
        .landed_due(10)
        .await
        .expect("read the landed set")
        .into_iter()
        .map(|row| row.identity)
        .filter(|id| id.scope.starts_with(&marker))
        .collect();
    assert_eq!(
        due,
        vec![stale.clone(), fresh.clone()],
        "the landed set is both landed rows, the one nothing has looked at in \
         two hours first"
    );

    let outstanding: Vec<ChangeIdentity> = store
        .outstanding(50)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.identity)
        .collect();
    assert!(
        outstanding.contains(&owing)
            && !outstanding.contains(&fresh)
            && !outstanding.contains(&stale),
        "a landed row is not work, and the row that is must still be in the other read"
    );

    // Re-asking a row that still holds writes the same state back, and that write
    // is what moves the row off the head of this order. A caller takes a slice,
    // so a write-back that did not move it would re-ask the same rows every pass
    // and never reach the rest of the set -- the batch would be a fixture.
    store
        .apply_measurement(
            &ChangeMeasurement::new(stale.clone(), ChangeState::Landed).at_revision("probe-rev-2"),
        )
        .await
        .expect("re-measure a row that still holds");
    let after: Vec<ChangeIdentity> = store
        .landed_due(10)
        .await
        .expect("read the landed set again")
        .into_iter()
        .map(|row| row.identity)
        .filter(|id| id.scope.starts_with(&marker))
        .collect();
    assert_eq!(
        after,
        vec![fresh.clone(), stale.clone()],
        "the row re-measured must now be the last one this order reaches, not the first"
    );
    let kept = store
        .get(&stale)
        .await
        .expect("read the row back")
        .expect("the row is there")
        .landed_at;
    assert!(
        kept.is_some(),
        "re-measuring a row that still holds must not take its landed time away"
    );

    cleanup(&pool, &marker).await;
}

/// The reopen record keeps the landed time the account is about to clear.
///
/// The clearing is the whole reason the record exists: `landed_at` goes NULL the
/// moment the state leaves `landed`, so "the ledger had this as done" and
/// "nothing ever measured this" are one row apart afterwards. Checked by reading
/// the column back after the second measurement, not by trusting the update
/// clause's text -- and by comparing the recorded time against the one the store
/// itself had stamped, so a record that carried a time of its own making would
/// fail here.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_reopen_keeps_the_landed_time_the_next_measurement_clears() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "was-done");

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("probe-rev-1"),
        )
        .await
        .expect("measure the row as landed");
    let landed_at = store
        .get(&id)
        .await
        .expect("read the row back")
        .expect("the row is there")
        .landed_at
        .expect("a landed row carries the time it landed");

    store
        .record_reopen(&ChangeReopen {
            identity: id.clone(),
            was_landed_at: Some(landed_at),
            measured_rev: "probe-rev-2".to_string(),
        })
        .await
        .expect("record the reopen");
    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Pending).at_revision("probe-rev-2"),
        )
        .await
        .expect("measure the row as no longer holding");

    let row = store
        .get(&id)
        .await
        .expect("read the row back")
        .expect("the row is there");
    assert_eq!(row.state, ChangeState::Pending);
    assert!(
        row.landed_at.is_none(),
        "the account cleared the landed time, which is why the record below is the only copy of it"
    );

    let (recorded_at, recorded_rev): (Option<chrono::DateTime<chrono::Utc>>, String) =
        sqlx::query_as(
            "SELECT was_landed_at, measured_rev FROM change_reopen \
         WHERE kind = $1 AND scope = $2 AND literal = $3",
        )
        .bind(&id.kind)
        .bind(&id.scope)
        .bind(&id.literal)
        .fetch_one(&pool)
        .await
        .expect("read the reopen back");
    assert_eq!(
        recorded_at,
        Some(landed_at),
        "the record carries the time the account had stamped, not one of its own"
    );
    assert_eq!(recorded_rev, "probe-rev-2", "the revision the pass read");

    cleanup(&pool, &marker).await;
}

/// Take one declaration back off the intake, the way the producer's withdrawal
/// verb does: a plain delete on the four columns the intake is keyed on, not
/// through the store -- the store has no withdrawal, and the whole point of the
/// verb is that it is the declarer's to run.
async fn withdraw_declaration(pool: &PgPool, id: &ChangeIdentity, doc: &str) {
    let removed = sqlx::query(
        "DELETE FROM change_ledger_declared \
         WHERE (kind, scope, literal, doc) = ($1, $2, $3, $4)",
    )
    .bind(&id.kind)
    .bind(&id.scope)
    .bind(&id.literal)
    .bind(doc)
    .execute(pool)
    .await
    .expect("withdraw the declaration")
    .rows_affected();
    assert_eq!(removed, 1, "the declaration was there to withdraw");
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
    // And the reopen series, which nothing else ever deletes: its rows name the
    // same marker and outlive the account rows they were written beside.
    sqlx::query("DELETE FROM change_reopen WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(pool)
        .await
        .expect("clean up the probe reopens");
}
