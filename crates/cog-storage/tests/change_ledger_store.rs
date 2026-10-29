//! principle: feedback_when_the_behaviour_under_test_is_global_serialize_the_tests_do_not_scope_them
//! principle: feedback_showing_how_big_the_starved_set_is_does_not_stop_it_starving
//! principle: feedback_a_fold_that_takes_then_writes_then_marks_lets_the_slow_folder_land_the_old_reading
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

mod common;

use sqlx::{Connection, PgPool};

use cog_core::{
    ChangeBlock, ChangeIdentity, ChangeLedgerStore, ChangeMeasurement, ChangePriority,
    ChangeReading, ChangeReopen, ChangeSource, ChangeState, RetireReason,
};
use cog_storage::PostgresChangeLedgerStore;

use cog_test_support::database_url;

async fn store() -> (PostgresChangeLedgerStore, PgPool, String) {
    let url = database_url();
    common::migrate_once(&url, 97_002).await;
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

/// Hold the folder seat to one test at a time.
///
/// Both folds work on the queue itself rather than on the caller's rows: they
/// take whatever is queued, write the account, and mark afterwards. One folder
/// is what the product assumes -- the resident loop sits behind an owner lease,
/// so a second one only exists when the lease has moved -- and the tests run in
/// parallel, which is several folders at once. Two of them can then select the
/// same queued row and write it in the other order: the slower fold marks the
/// older reading after the newer one has already been written, and because the
/// row is now folded, nothing goes back for it. The assertions still read as
/// global facts; they are just no longer about the row under test.
///
/// This session lock (a key of its own, apart from the schema lock) makes the
/// folding tests exclusive, which is the one-folder rule this suite is missing.
/// Closing the connection releases it, including when a test panics.
async fn folder_lock() -> sqlx::PgConnection {
    let url = database_url();
    let mut conn = sqlx::PgConnection::connect(&url)
        .await
        .expect("connect to take the folder lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(97_003_i64)
        .execute(&mut conn)
        .await
        .expect("take the folder lock");
    conn
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
            priority: ChangePriority::Unstated,
            block: ChangeBlock::NONE,
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

/// A request to measure a row again is queued once however often it is asked,
/// and a later correction of the same rule is a new question rather than a
/// duplicate the intake swallows.
///
/// The second half is the one a `DO NOTHING` conflict clause would get wrong,
/// and it is not hypothetical: the fixture recogniser on this platform was
/// corrected once already, and a rule that was wrong once can be wrong again --
/// each correction has to reach the row, or the second one is silently absorbed
/// and the account keeps a state nobody asked it about.
///
/// Nothing here judges the row. A request says the rule behind its state was
/// corrected; only the writer's measurement says what the row is, which is why
/// this intake carries no state and no reason.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_remeasurement_request_is_queued_once_and_revived_by_a_later_correction() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "remeasure");
    store
        .apply_measurement(&ChangeMeasurement::retired(
            id.clone(),
            RetireReason::Fixture,
        ))
        .await
        .expect("write the retired row");

    // The same correction declared twice is one request.
    store
        .request_remeasurement(&id, "probe:first-correction")
        .await
        .expect("queue the request");
    store
        .request_remeasurement(&id, "probe:first-correction")
        .await
        .expect("queue the same request again");
    let queued = store
        .pending_remeasurements(500)
        .await
        .expect("read the queue");
    assert_eq!(
        queued.iter().filter(|queued| **queued == id).count(),
        1,
        "one row has one request outstanding, however many times it is asked"
    );

    store.mark_remeasured(&id).await.expect("fold the request");
    let queued = store
        .pending_remeasurements(500)
        .await
        .expect("read the queue");
    assert!(
        !queued.contains(&id),
        "a folded request is not handed out again"
    );

    // The rule is corrected a second time. That is a new question about the same
    // row, and it has to reach the fold.
    store
        .request_remeasurement(&id, "probe:second-correction")
        .await
        .expect("queue the later request");
    let queued = store
        .pending_remeasurements(500)
        .await
        .expect("read the queue");
    assert!(
        queued.contains(&id),
        "a correction after a fold is a new question, not a duplicate"
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
    let _folder_lock = folder_lock().await;
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
    let _folder_lock = folder_lock().await;
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
    let _folder_lock = folder_lock().await;
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
            priority: ChangePriority::Unstated,
            block: ChangeBlock::NONE,
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

/// A statement about urgency rides the declaration that made it, and the row's
/// own band is the most urgent of the ones still standing.
///
/// What makes this a storage fact rather than a formatting one: the band is
/// written into the same JSONB entry as the verdict, so a reader of a citation
/// gets the two together, and there is no column on the row that could hold a
/// second answer. The two halves below are the two ways that can go wrong --
/// a band averaged or overwritten by the last writer (both would read as
/// agreement between documents that disagree) and a band that can only rise,
/// which is what turns a priority into a label.
///
/// `P0` is the most urgent, so "the highest the sources stated" means `max` over
/// the band's own order and not over its spelling. A row that took `P3` over
/// `P1` -- the larger name -- would be reading the band backwards, and it would
/// do it silently, which is why the first assertion here is the whole range
/// rather than one adjacent pair.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_declarations_band_rides_the_citation_and_the_row_takes_the_highest() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "band");
    let urgent = format!("docs/{marker}-urgent.md");
    let quiet = format!("docs/{marker}-quiet.md");
    let silent = format!("docs/{marker}-silent.md");
    let declare = |doc: &str, band: ChangePriority| ChangeReading {
        identity: id.clone(),
        title: Some("a change the probe declares".into()),
        assertion: None,
        source: ChangeSource {
            doc: doc.to_string(),
            verdict: Some("unlanded".into()),
            priority: band,
            block: ChangeBlock::NONE,
        },
    };

    // One document asks at P1, another at P3, a third states nothing at all.
    assert_eq!(
        store
            .declare(&[
                declare(&urgent, ChangePriority::P1),
                declare(&quiet, ChangePriority::P3),
                declare(&silent, ChangePriority::Unstated),
            ])
            .await
            .unwrap(),
        3,
        "three documents, three declarations"
    );
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold the declarations");
    let row = store
        .get(&id)
        .await
        .expect("read the row")
        .expect("the row exists");
    let cited: Vec<(String, ChangePriority)> = row
        .sources
        .iter()
        .map(|s| (s.doc.clone(), s.priority))
        .collect();
    assert!(
        cited.contains(&(urgent.clone(), ChangePriority::P1))
            && cited.contains(&(quiet.clone(), ChangePriority::P3))
            && cited.contains(&(silent.clone(), ChangePriority::Unstated)),
        "each citation carries the band its own document stated, including the \
         one that stated none: {cited:?}"
    );
    assert_eq!(
        row.priority(),
        ChangePriority::P1,
        "the row's band is the most urgent its sources stated: P1 over P3, and \
         a document that stated nothing does not outrank either -- the band as \
         written, not the band as spelled"
    );

    // The band has to be able to fall, or `P1` is a label rather than a reading:
    // the urgent document takes its declaration back, and the row is left with
    // what the other two said.
    withdraw_declaration(&pool, &id, &urgent).await;
    assert_eq!(
        store
            .declare(&[declare(&urgent, ChangePriority::Unstated)])
            .await
            .unwrap(),
        1,
        "re-declaring is a fresh row, which is how a band is corrected"
    );
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold the corrected band");
    let row = store
        .get(&id)
        .await
        .expect("read the row back")
        .expect("the row is there");
    assert_eq!(
        row.sources
            .iter()
            .find(|s| s.doc == urgent)
            .map(|s| s.priority),
        Some(ChangePriority::Unstated),
        "the corrected declaration replaces the citation rather than adding one: {:?}",
        row.sources
    );
    assert_eq!(
        row.priority(),
        ChangePriority::P3,
        "the row's band fell to the most urgent that is still stated"
    );

    withdraw_declaration(&pool, &id, &urgent).await;
    withdraw_declaration(&pool, &id, &quiet).await;
    withdraw_declaration(&pool, &id, &silent).await;
    cleanup(&pool, &marker).await;
}

/// The outstanding read hands the urgent rows out first, and the order follows
/// the citations rather than a stamp taken when the row entered.
///
/// This is the half that a column cannot fake. The three rows below are declared
/// in the order unstated, P3, P0, so a read ordered by anything about arrival --
/// insertion, `first_seen_at`, the staleness key -- answers in exactly the
/// reverse of what it should. What makes the assertion mean something is that
/// all three are unmeasured, so the staleness key has no measured row to prefer
/// and the band is the only thing that can produce this order.
///
/// The second half is the one a stored band would fail: the urgent document
/// takes its declaration back and re-declares at `unstated`, and the row has to
/// fall behind the P3 one on the next read. A copy that was written once would
/// still say P0 here, and that is precisely the drift the contract refuses.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_outstanding_read_hands_out_the_most_urgent_band_first() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let unstated = identity(&marker, "unstated");
    let quiet = identity(&marker, "quiet");
    let urgent = identity(&marker, "urgent");
    let declare = |id: &ChangeIdentity, doc: &str, band: ChangePriority| ChangeReading {
        identity: id.clone(),
        title: Some("a change the order probe declares".into()),
        assertion: None,
        source: ChangeSource {
            doc: doc.to_string(),
            verdict: Some("unlanded".into()),
            priority: band,
            block: ChangeBlock::NONE,
        },
    };
    let doc = |id: &ChangeIdentity| format!("docs/{}.md", id.scope.replace('/', "-"));

    store
        .declare(&[
            declare(&unstated, &doc(&unstated), ChangePriority::Unstated),
            declare(&quiet, &doc(&quiet), ChangePriority::P3),
            declare(&urgent, &doc(&urgent), ChangePriority::P0),
        ])
        .await
        .expect("queue three declarations");
    // The fold takes the account's oldest unfolded declarations up to its
    // bound, so a bound sized to the three rows above is a bound on the queue
    // rather than on this test: another binary holding older unfolded rows
    // leaves this test's rows further down the queue, unfolded, and a row that
    // was never folded is not in the read at all.
    store
        .fold_declarations(10_000, Some("probe-rev"))
        .await
        .expect("fold the declarations");

    let order = |rows: Vec<cog_core::ChangeLedgerRow>| -> Vec<String> {
        rows.into_iter()
            .filter(|row| row.identity.scope.starts_with(&marker))
            .map(|row| row.identity.scope)
            .collect()
    };
    assert_eq!(
        order(store.outstanding(10_000).await.expect("read the work")),
        vec![
            urgent.scope.clone(),
            quiet.scope.clone(),
            unstated.scope.clone()
        ],
        "P0 before P3 before unstated, though that is the reverse of the order \
         the three entered in"
    );

    // The correction a document makes has to reach the order, or the column is a
    // label: withdraw the P0 declaration and re-declare the same document at
    // `unstated`, and the row falls behind the P3 one.
    withdraw_declaration(&pool, &urgent, &doc(&urgent)).await;
    store
        .declare(&[declare(&urgent, &doc(&urgent), ChangePriority::Unstated)])
        .await
        .expect("re-declare at the lower band");
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold the correction");
    assert_eq!(
        order(
            store
                .outstanding(10_000)
                .await
                .expect("read the work again")
        ),
        vec![
            quiet.scope.clone(),
            unstated.scope.clone(),
            urgent.scope.clone()
        ],
        "the row whose band was taken back now sits behind every row that still \
         holds one, and among the rows nobody prioritised the order is arrival \
         -- which is why the unstated row declared first comes before it"
    );

    // The count the reading face publishes partitions the same set the reads
    // slice: five cells, every band present, summing to the outstanding census.
    // A band dropped by the group-by -- or a rank the store did not fold back
    // into a band -- shows up here as a sum that does not close.
    let owed = store.owed_by_band().await.expect("count the owed set");
    assert_eq!(
        owed.iter().map(|(band, _)| *band).collect::<Vec<_>>(),
        ChangePriority::ALL.to_vec(),
        "every band is a cell, in the contract's own order"
    );
    let census: usize = store
        .census()
        .await
        .expect("read the census")
        .into_iter()
        .filter(|(state, _)| state.is_outstanding())
        .map(|(_, rows)| usize::try_from(rows).unwrap_or(usize::MAX))
        .sum();
    assert_eq!(
        owed.iter().map(|(_, rows)| rows).sum::<usize>(),
        census,
        "the bands partition the outstanding set"
    );

    for id in [&unstated, &quiet, &urgent] {
        withdraw_declaration(&pool, id, &doc(id)).await;
    }
    cleanup(&pool, &marker).await;
}

/// Rows that tie on every band key come back in one order, and it is the
/// identity's rather than whichever one the scan produced.
///
/// The band order runs out of keys before it runs out of ties: a fold writes
/// everything it takes inside one transaction, and `now()` is the transaction's
/// start, so a batch of declarations shares a single `first_seen_at`; two rows
/// in the same band then agree on the band, on the absence of a measurement and
/// on the arrival stamp. What decided the rest was the plan, which is a ranking
/// that cannot be reproduced -- measured 2026-10-28, the same three rows came
/// back in two different orders minutes apart and reded a test on two
/// revisions that changed nothing here.
///
/// The rows are declared in the order that opposes their identities: an order
/// that follows the arrival stamp, or the heap that follows the insert, answers
/// the other way round.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn rows_that_tie_on_every_band_key_come_back_in_identity_order() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let early = identity(&marker, "aaa-declared-second");
    let late = identity(&marker, "zzz-declared-first");
    let doc = |id: &ChangeIdentity| format!("docs/{}.md", id.scope.replace('/', "-"));
    let reading = |id: &ChangeIdentity| ChangeReading {
        identity: id.clone(),
        title: Some("a tie probe declares".into()),
        assertion: None,
        source: ChangeSource {
            doc: doc(id),
            verdict: Some("unlanded".into()),
            priority: ChangePriority::Unstated,
            block: ChangeBlock::NONE,
        },
    };

    // The identity that sorts first goes in last, so arriving first and coming
    // back first are two different answers.
    store
        .declare(&[reading(&late), reading(&early)])
        .await
        .expect("queue the two declarations");
    store
        .fold_declarations(10_000, Some("probe-rev"))
        .await
        .expect("fold them in one transaction");

    let order = |rows: Vec<cog_core::ChangeLedgerRow>| -> Vec<String> {
        rows.into_iter()
            .filter(|row| row.identity.scope.starts_with(&marker))
            .map(|row| row.identity.scope)
            .collect()
    };
    let expected = vec![early.scope.clone(), late.scope.clone()];
    let first = order(store.outstanding(10_000).await.expect("read the work"));
    assert_eq!(
        first, expected,
        "the tie is broken by the identity, not by the order the two entered in"
    );
    let second = order(store.outstanding(10_000).await.expect("read it again"));
    assert_eq!(
        second, first,
        "two reads of an unchanged set answer in one order; an order the plan \
         decides is not a ranking, and a caller paging through the queue sees \
         work handed out twice or skipped"
    );

    for id in [&early, &late] {
        withdraw_declaration(&pool, id, &doc(id)).await;
    }
    cleanup(&pool, &marker).await;
}

/// A batch the bands fill completely still reaches the rows behind them.
///
/// The band order is a prefix, so on its own it rotates only inside the rows that
/// hold a band: once the banded set is at least as large as the batch, every slot
/// is taken on every pass and a row nobody prioritised is never asked at all.
/// That is not hypothetical -- it is how the live ledger came to hold rows that
/// no pass had ever measured while the band counts were showing the cause. The
/// read keeps the band order where it leads and reserves part of its bound for
/// the stalest rows the set holds, whatever bands they carry. Probed by making
/// the banded set larger than the batch and asking whether the unprioritised
/// rows, which are the oldest in the set, are in the slice.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_batch_the_bands_fill_completely_still_asks_the_rows_behind_them() {
    let _folder_lock = folder_lock().await;
    const BATCH: usize = 40;
    const BANDED: usize = 60;
    const BEHIND: usize = 6;
    let (store, pool, marker) = store().await;
    let banded: Vec<ChangeIdentity> = (0..BANDED)
        .map(|i| identity(&marker, &format!("banded-{i}")))
        .collect();
    let behind: Vec<ChangeIdentity> = (0..BEHIND)
        .map(|i| identity(&marker, &format!("behind-{i}")))
        .collect();
    let reading = |id: &ChangeIdentity, band: ChangePriority| ChangeReading {
        identity: id.clone(),
        title: Some("a change the batch probe declares".into()),
        assertion: None,
        source: ChangeSource {
            doc: format!("docs/{}.md", id.scope.replace('/', "-")),
            verdict: Some("unlanded".into()),
            priority: band,
            block: ChangeBlock::NONE,
        },
    };
    // The banded rows carry a band; the ones behind carry none, which is the
    // whole point of the probe.
    let mut readings: Vec<ChangeReading> = banded
        .iter()
        .map(|id| reading(id, ChangePriority::P2))
        .collect();
    readings.extend(
        behind
            .iter()
            .map(|id| reading(id, ChangePriority::Unstated)),
    );
    store
        .declare(&readings)
        .await
        .expect("queue the probe declarations");
    store
        .fold_declarations(1_000, Some("probe-rev"))
        .await
        .expect("fold the declarations");
    // The banded rows are measured, so the rows behind them are strictly the
    // oldest in the set rather than tied with them on "nothing has looked". The
    // rows behind are then aged by hand: this file's other tests write to the
    // same database at the same time, and a row nothing has measured sorts ahead
    // of every measured one whatever its age, so the tie among the unmeasured
    // rows is what decides which of them a bounded read reaches. Ten days puts
    // these six at the head of that tie without sleeping.
    for id in &banded {
        store
            .apply_measurement(
                &ChangeMeasurement::new(id.clone(), ChangeState::Pending).at_revision("probe-rev"),
            )
            .await
            .expect("measure a banded row");
    }
    sqlx::query(
        "UPDATE change_ledger SET first_seen_at = now() - interval '10 days' \
         WHERE scope LIKE $1",
    )
    .bind(format!("{marker}/behind-%"))
    .execute(&pool)
    .await
    .expect("age the rows behind the bands");
    let behind_scopes: Vec<&str> = behind.iter().map(|id| id.scope.as_str()).collect();
    let in_slice = |rows: &[cog_core::ChangeLedgerRow]| -> Vec<String> {
        rows.iter()
            .filter(|row| row.identity.scope.starts_with(marker.as_str()))
            .map(|row| row.identity.scope.clone())
            .collect()
    };

    let batch = store.outstanding(BATCH).await.expect("read the batch");
    assert_eq!(
        batch.len(),
        BATCH,
        "the bound is still the bound: the second half is taken from what the \
         first did not take, so splitting the batch cannot return more or fewer \
         rows than the caller asked for"
    );
    let slice = in_slice(&batch);
    let reached = behind_scopes
        .iter()
        .filter(|scope| slice.iter().any(|seen| seen == *scope))
        .count();
    assert_eq!(
        reached, BEHIND,
        "a banded set larger than the batch leaves the band order no slot for \
         the rows behind it; the read has to reserve one, or those rows are \
         never asked -- the slice was {slice:?}"
    );

    // The slice itself is shared with every other test in this file, so which of
    // its slots are this probe's depends on what else is running. The order
    // inside it does not: among the rows this probe owns, the ones holding a band
    // come before the ones nobody prioritised, in every read.
    let whole = in_slice(&store.outstanding(10_000).await.expect("read the set"));
    assert_eq!(
        whole.len(),
        BANDED + BEHIND,
        "a caller that asks for the whole set gets the whole set, whatever legs \
         it was taken from"
    );
    assert!(
        banded.iter().any(|id| id.scope == whole[0]),
        "and in the band order the hand-over reads: the banded rows come before \
         the ones behind them, got {}",
        whole[0]
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
    let _folder_lock = folder_lock().await;
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
                priority: ChangePriority::Unstated,
                block: ChangeBlock::NONE,
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

    // The whole set, not a batch: which rows a bounded read takes is a question
    // about the order and the bound, and this test is about which states are in
    // it at all. A batch would also make the verdict depend on what the other
    // tests in this file are writing at the same moment.
    let outstanding: Vec<ChangeIdentity> = store
        .outstanding(10_000)
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

/// The landing revision is the one the row landed at, and a re-check moves the
/// row's own revision past it.
///
/// The two are the same column in the account and two different facts, and the
/// reader that compares a landing against the image the workloads run is asking
/// about the first. Measured on the live account the day this read existed: every
/// landed row the window sweep had touched since the image was built carried the
/// sweep's revision, so the whole swept set read as landed and not running while
/// the commits that had actually carried those changes were in the image.
///
/// The mechanism is asserted right above -- re-measuring a landed row writes a
/// new `measured_rev` and keeps its state -- and the event table's own migration
/// says the re-check writes no event. What this test pins is the consequence:
/// the landing comes out of the log, and a row with no landing in the log answers
/// `None` rather than the revision of a check it was not landed at.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_landing_revision_survives_a_re_check_and_is_absent_where_none_was_logged() {
    let (store, pool, marker) = store().await;
    let rechecked = identity(&marker, "rechecked");
    let unlogged = identity(&marker, "unlogged");

    store
        .apply_measurement(
            &ChangeMeasurement::new(rechecked.clone(), ChangeState::Landed)
                .at_revision("probe-rev-1"),
        )
        .await
        .expect("land the row at the first revision");
    store
        .apply_measurement(
            &ChangeMeasurement::new(rechecked.clone(), ChangeState::Landed)
                .at_revision("probe-rev-2"),
        )
        .await
        .expect("re-check the row at a later revision");

    // A landed row with no landing in the log: the state is written directly,
    // which is the only way to reach it without a landing event -- the same shape
    // every row that landed before the log existed has, and the reason the read
    // answers `None` instead of falling back to `measured_rev`.
    store
        .apply_measurement(
            &ChangeMeasurement::new(unlogged.clone(), ChangeState::Pending)
                .at_revision("probe-rev-3"),
        )
        .await
        .expect("enter a row that has not landed");
    sqlx::query(
        "UPDATE change_ledger SET state = 'landed', landed_at = now() \
         WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&unlogged.kind)
    .bind(&unlogged.scope)
    .bind(&unlogged.literal)
    .execute(&pool)
    .await
    .expect("write a landing the log will not know about");

    let rows = store
        .landed_at_revision(10_000)
        .await
        .expect("read the landed set with the revisions it landed at");
    let landing_of = |id: &ChangeIdentity| {
        rows.iter()
            .find(|(row, _)| row == id)
            .map(|(_, revision)| revision.clone())
    };
    assert_eq!(
        landing_of(&rechecked),
        Some(Some("probe-rev-1".to_string())),
        "the row's own revision moved to the re-check, so the landing has to come from the log"
    );
    assert_eq!(
        landing_of(&unlogged),
        Some(None),
        "a landing nobody logged has no revision to answer with, and the row's own is not one"
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
            scope_present: Some(false),
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

    let (recorded_at, recorded_rev, scope_present): (
        Option<chrono::DateTime<chrono::Utc>>,
        String,
        Option<bool>,
    ) = sqlx::query_as(
        "SELECT was_landed_at, measured_rev, scope_present FROM change_reopen \
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
    assert_eq!(
        scope_present,
        Some(false),
        "the answer the pass got -- the surface had left -- survives the write; \
         folded into a default it would read as a criterion that stopped holding"
    );

    cleanup(&pool, &marker).await;
}

/// A reopen whose pass could not ask the tree keeps that fact as an absence.
///
/// The column has three readings and the third one is the reason it is nullable:
/// `false` says the surface had left, and a git that failed says nothing about
/// the surface at all. Stored under either answer, a failed look would be
/// reported as a removal, and the count of claims that stopped holding inside a
/// surface still there would be short by however many looks happened to fail.
/// Checked by writing the reopen with `None` and reading NULL back -- not by
/// reading the column's definition, which would pass for a default of `false`.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_reopen_that_could_not_ask_the_tree_stores_no_answer() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "unanswered");

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("probe-rev-1"),
        )
        .await
        .expect("measure the row as landed");
    store
        .record_reopen(&ChangeReopen {
            identity: id.clone(),
            was_landed_at: None,
            measured_rev: "probe-rev-2".to_string(),
            scope_present: None,
        })
        .await
        .expect("record the reopen");

    let scope_present: Option<bool> = sqlx::query_scalar(
        "SELECT scope_present FROM change_reopen \
         WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&id.kind)
    .bind(&id.scope)
    .bind(&id.literal)
    .fetch_one(&pool)
    .await
    .expect("read the reopen back");
    assert_eq!(
        scope_present, None,
        "a look that failed is not the same answer as a surface that had left"
    );

    cleanup(&pool, &marker).await;
}

/// Take one declaration back off the intake, the way the producer's withdrawal
/// verb does: a plain delete on the four columns the intake is keyed on, not
/// through the store -- the store has no withdrawal, and the whole point of the
/// verb is that it is the declarer's to run.
/// The lock a probe holds while it compares two readings of the closure.
///
/// The closure counts every declaration in the database, not the caller's, so a
/// probe that reads it, does one thing, and reads it again is comparing two
/// whole-account numbers. That only holds still if nothing removes rows in
/// between -- and something does: every probe here deletes its own rows at the
/// end, so a neighbour finishing inside another probe's window moves the count
/// for a reason that probe never did. This is not hypothetical. The batch probe
/// declares 66 rows and cleans them up; when that cleanup landed between the
/// closure probe's two reads, `owed` fell 67 -> 2 while the probe had only added
/// a declaration, and the reading blamed the store for it. So every delete in
/// this file takes this lock, and a probe that compares closure readings holds
/// it across them: the account's population may move between comparisons, never
/// under one.
const CLOSURE_PROBE_LOCK: i64 = 5_110_202_610;

async fn take_closure_lock(tx: &mut sqlx::PgTransaction<'_>) {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(CLOSURE_PROBE_LOCK)
        .execute(&mut **tx)
        .await
        .expect("take the closure probe lock");
}

async fn withdraw_declaration(pool: &PgPool, id: &ChangeIdentity, doc: &str) {
    let mut tx = pool.begin().await.expect("open the withdrawal transaction");
    take_closure_lock(&mut tx).await;
    let removed = sqlx::query(
        "DELETE FROM change_ledger_declared \
         WHERE (kind, scope, literal, doc) = ($1, $2, $3, $4)",
    )
    .bind(&id.kind)
    .bind(&id.scope)
    .bind(&id.literal)
    .bind(doc)
    .execute(&mut *tx)
    .await
    .expect("withdraw the declaration")
    .rows_affected();
    tx.commit().await.expect("commit the withdrawal");
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

/// The events recorded for this run, oldest first: `(from_state, to_state,
/// measured_rev)`.
async fn events(pool: &PgPool, marker: &str) -> Vec<(Option<String>, String, Option<String>)> {
    sqlx::query_as(
        "SELECT from_state, to_state, measured_rev FROM change_ledger_event \
          WHERE scope LIKE $1 ORDER BY id",
    )
    .bind(format!("{marker}/%"))
    .fetch_all(pool)
    .await
    .expect("read the probe events")
}

/// The reasons each event carried, oldest first: `(to_state, retire_reason,
/// unsettled_reason)`.
async fn event_reasons(
    pool: &PgPool,
    marker: &str,
) -> Vec<(String, Option<String>, Option<String>)> {
    sqlx::query_as(
        "SELECT to_state, retire_reason, unsettled_reason FROM change_ledger_event \
          WHERE scope LIKE $1 ORDER BY id",
    )
    .bind(format!("{marker}/%"))
    .fetch_all(pool)
    .await
    .expect("read the probe event reasons")
}

/// What the account overwrote, the event half keeps.
///
/// The account is read in the same test on purpose: the point of the event is
/// not that the row exists but that it survives the statement which replaces
/// what it describes. A row that landed at `rev-c` says nowhere that it was
/// pending at `rev-a` -- the aim here is that this table is the only place that
/// reading can come from.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn entering_and_moving_are_events_while_a_repeat_measurement_is_not() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "moves");

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Pending).at_revision("rev-a"),
        )
        .await
        .expect("create the row by measuring it");
    assert_eq!(
        events(&pool, &marker).await,
        vec![(None, "pending".to_string(), Some("rev-a".to_string()))],
        "a row entering the account is one event, with no prior state"
    );

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Pending).at_revision("rev-b"),
        )
        .await
        .expect("measure the row again in the same state");
    assert_eq!(
        events(&pool, &marker).await.len(),
        1,
        "a re-measurement that finds the same state is not a transition"
    );

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("rev-c"),
        )
        .await
        .expect("land the row");
    assert_eq!(
        events(&pool, &marker).await,
        vec![
            (None, "pending".to_string(), Some("rev-a".to_string())),
            (
                Some("pending".to_string()),
                "landed".to_string(),
                Some("rev-c".to_string())
            ),
        ],
        "both ends of the move survive the measurement that replaced them"
    );

    cleanup(&pool, &marker).await;
}

/// One row's history reads back through the store, newest first, and stays on
/// its own identity.
///
/// The account says where a change is now; it does not say how it got there, and
/// once a later measurement has replaced the earlier one the steps exist only in
/// the event table. Two ways this read can be quietly wrong: it can return
/// another identity's moves (the identity is keyed by all three parts, and two
/// rows in one scope differ only in the literal), and it can hand back the
/// oldest steps when the reader asked for the newest.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_history_reads_back_through_the_store_newest_first_and_on_its_own_identity() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "history");
    // Same kind, same scope, one character different in the literal: the pair
    // that a read keyed by anything less than all three parts would merge.
    let sibling = ChangeIdentity::new("grep", id.scope.clone(), "fn probe, renamed");

    for (state, rev) in [
        (ChangeState::Pending, "rev-a"),
        (ChangeState::Partial, "rev-b"),
        (ChangeState::Landed, "rev-c"),
    ] {
        store
            .apply_measurement(&ChangeMeasurement::new(id.clone(), state).at_revision(rev))
            .await
            .unwrap_or_else(|e| panic!("measure {rev}: {e}"));
    }
    store
        .apply_measurement(
            &ChangeMeasurement::new(sibling.clone(), ChangeState::Pending).at_revision("rev-x"),
        )
        .await
        .expect("measure the sibling row");

    let history = store.events(&id, 50).await.expect("read one row's history");
    assert_eq!(history.len(), 3, "the sibling's move is not this row's");
    assert_eq!(history[0].to_state, ChangeState::Landed);
    assert_eq!(history[0].from_state, Some(ChangeState::Partial));
    assert_eq!(history[0].measured_rev.as_deref(), Some("rev-c"));
    assert_eq!(
        history[2].from_state, None,
        "entering the account is the one move with nothing before it"
    );
    assert_eq!(history[2].to_state, ChangeState::Pending);
    assert_eq!(history[2].measured_rev.as_deref(), Some("rev-a"));

    let newest = store.events(&id, 1).await.expect("read the newest move");
    assert_eq!(newest.len(), 1);
    assert_eq!(
        newest[0].to_state,
        ChangeState::Landed,
        "the bound has to drop the oldest steps, not keep them"
    );

    cleanup(&pool, &marker).await;
}

/// The reason a state carried travels with the transition, not only with the
/// row.
///
/// A later measurement clears `unsettled_reason` from the account, so without
/// this row the account has no record that the state ever had one. The second
/// event is checked too: a reason that stayed attached to a move it does not
/// describe would read as one.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_reason_of_a_transition_is_recorded_with_it() {
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "reason");

    store
        .apply_measurement(
            &ChangeMeasurement::undecided(id.clone(), "scope_outside_tree").at_revision("rev-a"),
        )
        .await
        .expect("create the row as undecided");
    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed).at_revision("rev-b"),
        )
        .await
        .expect("land the row");

    assert_eq!(
        event_reasons(&pool, &marker).await,
        vec![
            (
                "undecided".to_string(),
                None,
                Some("scope_outside_tree".to_string())
            ),
            ("landed".to_string(), None, None),
        ],
        "the reason rides the move it belongs to and not the one after it"
    );

    cleanup(&pool, &marker).await;
}

/// A demand is answered only by a landing, and a declaration whose change was
/// never measured counts as owed rather than vanishing.
///
/// This is the writeback the loop was missing: the intake is where a document
/// says what it wants, and until the closure was read the account had no way to
/// say which of those demands the tree ended up holding. The assertions are
/// stated as "at least" on the global counts because the other probes in this
/// file share the database and add declarations of their own -- a strict
/// equality would be a test that fails when a neighbour runs, which teaches a
/// reader to rerun rather than to look. "At least" is only sound if the account
/// cannot shrink under the probe, and neighbours do delete their rows on the way
/// out, so the three reads are taken while holding `CLOSURE_PROBE_LOCK` and no
/// neighbour's cleanup can land between them.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_demand_is_satisfied_by_a_landing_and_owed_until_one_is_measured() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    // An empty transaction, held open for one thing only: the advisory lock is
    // transaction-scoped, so dropping it (including on a panic) releases the
    // lock rather than leaking it back into the pool with the connection.
    let mut hold = pool.begin().await.expect("open the closure hold");
    take_closure_lock(&mut hold).await;
    let id = identity(&marker, "demand");
    let doc = format!("{marker}.md");
    let before = store
        .demand_closure()
        .await
        .expect("read the closure")
        .expect("a Postgres store answers the closure");

    store
        .declare(&[ChangeReading {
            identity: id.clone(),
            title: Some("a change the probe declares".into()),
            assertion: None,
            source: ChangeSource {
                doc: doc.clone(),
                verdict: None,
                priority: ChangePriority::Unstated,
                block: ChangeBlock::NONE,
            },
        }])
        .await
        .expect("queue the declaration");
    store
        .fold_declarations(10_000, Some("probe-rev-declare"))
        .await
        .expect("fold the declaration in");

    let declared = store
        .demand_closure()
        .await
        .expect("read the closure")
        .expect("a Postgres store answers the closure");
    assert!(
        declared.owed > before.owed,
        "a declared change that has never been measured is owed, not absent from both halves: \
         before {before:?}, after {declared:?}"
    );
    assert_eq!(
        declared.satisfied, before.satisfied,
        "declaring a change does not satisfy it -- only a measurement of the tree can"
    );

    store
        .apply_measurement(
            &ChangeMeasurement::new(id.clone(), ChangeState::Landed)
                .at_revision("probe-rev-delivered"),
        )
        .await
        .expect("land the change");

    let landed = store
        .demand_closure()
        .await
        .expect("read the closure")
        .expect("a Postgres store answers the closure");
    assert!(
        landed.satisfied > declared.satisfied,
        "the landing is what answers the demand: before {declared:?}, after {landed:?}"
    );

    hold.rollback().await.expect("release the closure hold");
    cleanup(&pool, &marker).await;
}

/// The face census reads every bucket and the age of a face's oldest owed row,
/// and the age has to arrive as a number rather than as an interval.
///
/// This is the one reading in the census that is computed by the server rather
/// than counted, and the type it comes back as is the database's decision, not
/// the statement's apparent one: `EXTRACT(EPOCH FROM ...)` yields `NUMERIC` on
/// a server new enough to have the interval arithmetic, so a row decoded as a
/// float fails to decode at all -- and a store that answered with an error there
/// published no face cell, which reads exactly like a deployment whose census
/// was never wired. Nothing without a live server can catch that: a fake store
/// returns the Rust type the fake chose, so the mismatch only exists across the
/// wire. The buckets are checked beside it for the same reason the retired cell
/// exists -- three buckets that add up to the face's population are what let a
/// face that finished be told from one that retired its work.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_face_census_reads_a_number_for_the_age_and_three_buckets_that_add_up() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "census");
    let doc = format!("{marker}.md");

    store
        .declare(&[ChangeReading {
            identity: id.clone(),
            title: Some("a change the probe files under a face".into()),
            assertion: None,
            source: ChangeSource {
                doc: doc.clone(),
                verdict: None,
                priority: ChangePriority::Unstated,
                block: ChangeBlock::named("principles").expect("a face name"),
            },
        }])
        .await
        .expect("queue the declaration");
    store
        .fold_declarations(10_000, Some("probe-rev-census"))
        .await
        .expect("fold the declaration in");

    let census = store
        .block_census()
        .await
        .expect("read the face census")
        .expect("a Postgres store answers the census");
    let face = census
        .iter()
        .find(|row| row.block == ChangeBlock::named("principles").expect("a face name"))
        .unwrap_or_else(|| panic!("the declared face is counted; the census holds {census:?}"));
    assert!(
        face.owed >= 1,
        "a declared change nobody measured is owed on its face: {face:?}"
    );
    assert!(
        face.oldest_unmeasured_secs.is_some(),
        "an owed row that was never measured has an age from the moment it first \
         appeared, and the age is a number -- `None` here is the read failing to \
         decode rather than every row in the face having been measured: {face:?}"
    );
    assert!(
        census.iter().any(|row| row.block == ChangeBlock::NONE),
        "a row whose documents name no face is counted too: a census that dropped \
         the unfiled population would answer about the programme while reading as \
         the whole account: {census:?}"
    );

    // The other two readings over the same account decode against the server as
    // well; they are one statement each and nothing without a server checks them.
    store
        .blocking_surface()
        .await
        .expect("read the blocking surface")
        .expect("a Postgres store answers the blocking surface");
    store
        .blocking_holders()
        .await
        .expect("read the blocking holders")
        .expect("a Postgres store answers the blocking holders");

    cleanup(&pool, &marker).await;
}

/// How long a face has been idle is measured, not inferred from the passes.
///
/// A face is proposed for retirement when nothing in it has moved for a period,
/// and the two ways to answer "for how long" are a counter in the loop and a
/// stamp in the account. The counter is the wrong one twice: it forgets on
/// restart, so a face that has been dead for a week reads as newly dead after
/// every rollout, and it counts passes rather than time, so a loop that fell
/// behind reads as a longer silence than the account actually had. This checks
/// the measurement exists and is a number -- the idle rule itself is unit-tested
/// where it is decided.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_face_reports_how_long_ago_anything_in_it_moved() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "idle");
    let face = ChangeBlock::named("infra_probe_idle").expect("a face name");

    store
        .declare(&[ChangeReading {
            identity: id.clone(),
            title: Some("a change that has just been filed".into()),
            assertion: None,
            source: ChangeSource {
                doc: format!("{marker}.md"),
                verdict: None,
                priority: ChangePriority::Unstated,
                block: face.clone(),
            },
        }])
        .await
        .expect("queue the declaration");
    store
        .fold_declarations(10_000, Some("probe-rev-idle"))
        .await
        .expect("fold the declaration in");

    let census = store
        .block_census()
        .await
        .expect("read the face census")
        .expect("a Postgres store answers the census");
    let row = census
        .iter()
        .find(|row| row.block == face)
        .unwrap_or_else(|| panic!("the face just filed under is counted: {census:?}"));
    let idle = row.newest_activity_secs.unwrap_or_else(|| {
        panic!(
            "a face with a row that has just been folded has a measurable idle time; \
             `None` here is the column failing to decode, and it reads at the \
             retirement rule as `never moved`, which never proposes anything: {row:?}"
        )
    });
    assert!(
        idle < 3600,
        "the row was folded a moment ago, so the newest movement in the face is \
         seconds old: {idle}"
    );

    cleanup(&pool, &marker).await;
}

/// A face the code has never heard of is filed and counted by naming it.
///
/// The census used to be a list of the faces the service carried, so a face
/// declared after that list was written could not be counted at all: the fold
/// did not know the name, the row landed under `none`, and the new face's work
/// read as work nobody filed. The fix is not a longer list -- the list is gone
/// on both sides -- so the reading has to be measured from the rows. The name
/// used here is one nothing in the tree mentions, which is the point: if it
/// appears in the census with its own count, no edit to the code is needed for
/// the next face either.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_face_the_code_never_heard_of_is_filed_and_counted_by_naming_it() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "new-face");
    let doc = format!("{marker}.md");
    let name = "infra_probe_face";

    store
        .declare(&[ChangeReading {
            identity: id.clone(),
            title: Some("a change filed under a face declared after the code was written".into()),
            assertion: None,
            source: ChangeSource {
                doc: doc.clone(),
                verdict: None,
                priority: ChangePriority::Unstated,
                block: ChangeBlock::named(name).expect("a face name"),
            },
        }])
        .await
        .expect("queue the declaration");
    store
        .fold_declarations(10_000, Some("probe-rev-new-face"))
        .await
        .expect("fold the declaration in");

    let stored: String =
        sqlx::query_scalar("SELECT block FROM change_ledger WHERE kind = $1 AND scope = $2")
            .bind(&id.kind)
            .bind(&id.scope)
            .fetch_one(&pool)
            .await
            .expect("read the folded face back");
    assert_eq!(
        stored, name,
        "the folded column keeps the name the document wrote; folding it to `none` files this \
         face's work as work nobody asked for"
    );

    let census = store
        .block_census()
        .await
        .expect("read the face census")
        .expect("a Postgres store answers the census");
    let face = census
        .iter()
        .find(|row| row.block.as_str() == name)
        .unwrap_or_else(|| panic!("the new face is counted; the census holds {census:?}"));
    assert!(
        face.owed >= 1,
        "a declared change nobody measured is owed on its face: {face:?}"
    );

    // The census is the account's own set of faces, not a list it carries: the
    // names it prints are exactly the ones the rows hold, `none` included even
    // when every row names a face.
    let named: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT block FROM change_ledger ORDER BY 1")
            .fetch_all(&pool)
            .await
            .expect("read the account's faces");
    let counted: Vec<String> = census
        .iter()
        .map(|row| row.block.as_str().to_string())
        .collect();
    for name in &named {
        assert!(
            counted.contains(name),
            "the account holds a row under {name:?} and the census does not print it: {counted:?}"
        );
    }
    assert!(
        counted.contains(&ChangeBlock::NONE.as_str().to_string()),
        "the unfiled population is printed even when it is empty, because a zero there is a \
         reading: {counted:?}"
    );

    cleanup(&pool, &marker).await;
}

/// A landing's parent reading is written once, survives a re-check, and a
/// landing that has none is handed back with the revision it landed at.
///
/// All three are promises the statement makes and nothing without a server can
/// check: "first one wins" is the `COALESCE` in the conflict clause, "survives a
/// re-check" is that clause under a second measurement, and "the revision it
/// landed at" is the event log's first transition rather than the row's
/// `measured_rev`, which a re-check has moved.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_landings_parent_reading_is_written_once_and_the_backfill_answers_with_its_own_revision()
{
    let (store, pool, marker) = store().await;
    let answered = identity(&marker, "arrived");
    let owed = identity(&marker, "recorded");

    store
        .apply_measurement(
            &ChangeMeasurement::new(answered.clone(), ChangeState::Landed)
                .at_revision("probe-rev-1")
                .with_parent_observation("absent"),
        )
        .await
        .expect("write a landing that brought its artifact");
    // A later pass re-checks the same row at a moved revision and finds it
    // already there. The landing's own reading must not follow the tree.
    store
        .apply_measurement(
            &ChangeMeasurement::new(answered.clone(), ChangeState::Landed)
                .at_revision("probe-rev-2")
                .with_parent_observation("satisfied"),
        )
        .await
        .expect("re-check the landing");

    let reading: Option<String> = sqlx::query_scalar(
        "SELECT parent_observation FROM change_ledger \
         WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&answered.kind)
    .bind(&answered.scope)
    .bind(&answered.literal)
    .fetch_one(&pool)
    .await
    .expect("read the parent reading back");
    assert_eq!(
        reading.as_deref(),
        Some("absent"),
        "the first landing's reading is the fact; a re-check at a later revision \
         must not rewrite it into 'already there'"
    );

    // A landing written without the column, as every row before it existed was,
    // is handed back with the revision of its first transition into `landed`.
    store
        .apply_measurement(
            &ChangeMeasurement::new(owed.clone(), ChangeState::Landed).at_revision("probe-rev-1"),
        )
        .await
        .expect("write a landing with no parent reading");
    store
        .apply_measurement(
            &ChangeMeasurement::new(owed.clone(), ChangeState::Landed).at_revision("probe-rev-2"),
        )
        .await
        .expect("re-check it");

    let pending = store
        .landed_awaiting_parent_observation(100_000)
        .await
        .expect("read the landings without a parent reading");
    let found = pending
        .iter()
        .find(|(identity, _)| identity == &owed)
        .map(|(_, rev)| rev.as_str());
    assert_eq!(
        found,
        Some("probe-rev-1"),
        "the backfill must ask the tree at the landing's revision, not at the one \
         the re-check moved measured_rev to"
    );
    assert!(
        !pending.iter().any(|(identity, _)| identity == &answered),
        "a landing that already has a reading is not backfilled again"
    );

    assert!(
        store
            .record_parent_observation(&owed, "satisfied")
            .await
            .expect("record a parent reading"),
        "a landing with no reading takes one"
    );
    assert!(
        !store
            .record_parent_observation(&owed, "absent")
            .await
            .expect("record a second parent reading"),
        "a landing that has a reading is not written again"
    );
    let reading: Option<String> = sqlx::query_scalar(
        "SELECT parent_observation FROM change_ledger \
         WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&owed.kind)
    .bind(&owed.scope)
    .bind(&owed.literal)
    .fetch_one(&pool)
    .await
    .expect("read the second row back");
    assert_eq!(
        reading.as_deref(),
        Some("satisfied"),
        "the second write must be refused, not applied"
    );

    cleanup(&pool, &marker).await;
}

/// The pool block is written narrow and released narrow: it lands only on the
/// rows still owed and unblocked, keeps the start the first pass gave it, leaves
/// a wait somebody else declared alone on the way in, and the clear takes back
/// only the wait the pool declared.
///
/// None of this can be read off the SQL text. The `COALESCE` keeps the start,
/// the `OR` clause is what refuses to relabel a person's block, and the clear's
/// key is the difference between "the pool serves again" and "every row is
/// free" -- a reader acting on the second would stop chasing rows nobody ever
/// unblocked. The row also has to be `pending`: a landed row has no work to wait
/// for, and a block left on it would count in a by-reason census forever.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_pool_block_lands_only_on_the_owed_rows_and_the_clear_takes_back_only_it() {
    let (store, pool, marker) = store().await;
    let unblocked = identity(&marker, "unblocked");
    let person = identity(&marker, "person");
    let settled = identity(&marker, "settled");
    for (id, state) in [
        (&unblocked, ChangeState::Pending),
        (&person, ChangeState::Pending),
        (&settled, ChangeState::Landed),
    ] {
        store
            .apply_measurement(&ChangeMeasurement::new(id.clone(), state))
            .await
            .expect("write a measurement");
    }
    // A block a person owns, already on one of the rows.
    sqlx::query(
        "UPDATE change_ledger \
            SET blocked_reason = 'owner_decision', \
                unblock_condition = 'a person decides this one', \
                blocked_since = now() \
          WHERE kind = $1 AND scope = $2 AND literal = $3",
    )
    .bind(&person.kind)
    .bind(&person.scope)
    .bind(&person.literal)
    .execute(&pool)
    .await
    .expect("seed the person's block");

    let reading = |pool: PgPool, id: ChangeIdentity| async move {
        sqlx::query_as::<
            _,
            (
                Option<String>,
                Option<String>,
                Option<chrono::DateTime<chrono::Utc>>,
            ),
        >(
            "SELECT blocked_reason, unblock_condition, blocked_since FROM change_ledger \
             WHERE kind = $1 AND scope = $2 AND literal = $3",
        )
        .bind(&id.kind)
        .bind(&id.scope)
        .bind(&id.literal)
        .fetch_one(&pool)
        .await
        .expect("read the blocked columns back")
    };

    store
        .stamp_pool_block("an upstream the pool can reach")
        .await
        .expect("stamp the pool block");
    let (reason, condition, since) = reading(pool.clone(), unblocked.clone()).await;
    assert_eq!(
        reason.as_deref(),
        Some("pool_unavailable"),
        "an owed, unblocked row is what the pass withheld"
    );
    assert_eq!(condition.as_deref(), Some("an upstream the pool can reach"));
    let since = since.expect("a block carries the moment it started");
    assert_eq!(
        reading(pool.clone(), person.clone()).await.0.as_deref(),
        Some("owner_decision"),
        "a block a person declared is not this pass's to relabel"
    );
    assert_eq!(
        reading(pool.clone(), settled.clone()).await.0,
        None,
        "a landed row has no work to wait for"
    );

    // Stamping again is what a second paused pass does; the wait it reports
    // started when it was first found, not when it was last noticed.
    store
        .stamp_pool_block("an upstream the pool can reach")
        .await
        .expect("stamp the pool block again");
    assert_eq!(
        reading(pool.clone(), unblocked.clone()).await.2,
        Some(since),
        "the start of the wait must not move with every pass that finds it"
    );

    store
        .clear_pool_block()
        .await
        .expect("clear the pool block");
    assert_eq!(
        reading(pool.clone(), unblocked.clone()).await,
        (None, None, None),
        "the row the pool was holding is free again"
    );
    assert_eq!(
        reading(pool.clone(), person.clone()).await.0.as_deref(),
        Some("owner_decision"),
        "a serving pool says nothing about a row waiting on a person"
    );

    cleanup(&pool, &marker).await;
}

/// A wait goes away with the work it described.
///
/// The three blocked columns are what every by-reason census counts, and a row
/// that lands or retires is not waiting for anything. Left behind, the row keeps
/// counting: measured on the live account on 2026-10-26, `--blocked` read 2067
/// rows while 2066 were pending, and the extra one was a row retired minutes
/// earlier that still carried the pool's reason. The count looked like a wait
/// somebody had to answer, and there was nobody.
///
/// A measurement that still says `pending` keeps the wait it found -- the stamp
/// and the clear are the passes that decide those, and re-measuring a row is not
/// one of them.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_that_leaves_the_account_takes_its_wait_with_it() {
    let (store, pool, marker) = store().await;
    let landed = identity(&marker, "wait-lands");
    let retired = identity(&marker, "wait-retires");
    let still_owed = identity(&marker, "wait-stays");
    for id in [&landed, &retired, &still_owed] {
        store
            .apply_measurement(&ChangeMeasurement::new(id.clone(), ChangeState::Pending))
            .await
            .expect("write a pending measurement");
    }
    store
        .stamp_pool_block("an upstream the pool can reach")
        .await
        .expect("stamp the pool block");

    let blocked = |id: ChangeIdentity| {
        let pool = pool.clone();
        async move {
            let (reason, condition, since) = sqlx::query_as::<
                _,
                (
                    Option<String>,
                    Option<String>,
                    Option<chrono::DateTime<chrono::Utc>>,
                ),
            >(
                "SELECT blocked_reason, unblock_condition, blocked_since FROM change_ledger \
                 WHERE kind = $1 AND scope = $2 AND literal = $3",
            )
            .bind(&id.kind)
            .bind(&id.scope)
            .bind(&id.literal)
            .fetch_one(&pool)
            .await
            .expect("read the blocked columns back");
            (reason, condition, since)
        }
    };
    for id in [&landed, &retired, &still_owed] {
        assert_eq!(
            blocked(id.clone()).await.0.as_deref(),
            Some("pool_unavailable"),
            "the stamp lands on every owed row"
        );
    }

    store
        .apply_measurement(&ChangeMeasurement::new(landed.clone(), ChangeState::Landed))
        .await
        .expect("measure the row landed");
    assert_eq!(
        blocked(landed.clone()).await,
        (None, None, None),
        "a landed row is owed nothing, so it cannot still be waiting"
    );

    store
        .apply_measurement(&ChangeMeasurement::retired(
            retired.clone(),
            RetireReason::Superseded,
        ))
        .await
        .expect("retire the row");
    assert_eq!(
        blocked(retired.clone()).await,
        (None, None, None),
        "a retired row's question is answered, and a wait on it counts forever"
    );

    let kept = blocked(still_owed.clone()).await;
    assert_eq!(
        kept.0.as_deref(),
        Some("pool_unavailable"),
        "a row still owed keeps the wait it was in"
    );
    assert!(
        kept.2.is_some(),
        "and keeps the start of it, not a fresh one"
    );

    cleanup(&pool, &marker).await;
}

async fn cleanup(pool: &PgPool, marker: &str) {
    // One transaction, and the closure lock first: the deletes below are what a
    // neighbour's two reads of the whole-account closure must not see half of,
    // so they land all at once and only while no probe is comparing readings.
    let mut tx = pool.begin().await.expect("open the cleanup transaction");
    take_closure_lock(&mut tx).await;
    sqlx::query("DELETE FROM change_ledger WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(&mut *tx)
        .await
        .expect("clean up the probe rows");
    // The declarations this run queued, which are also the demand side of the
    // account: a row left behind is not inert -- it keeps counting as a demand
    // the closure reports, so a probe that skipped this would make the next
    // run's closure read higher for a reason nobody could find in that run.
    sqlx::query("DELETE FROM change_ledger_declared WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(&mut *tx)
        .await
        .expect("clean up the probe declarations");
    // The intake is left as it was too: a queued retirement names the same
    // marker, and a row left behind would be folded by a later run's pass.
    sqlx::query("DELETE FROM change_ledger_retired WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(&mut *tx)
        .await
        .expect("clean up the probe retirements");
    // The re-measurement requests as well, for the same reason: a request left
    // behind is a row a later run's pass would measure under this run's name.
    sqlx::query("DELETE FROM change_ledger_remeasure WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(&mut *tx)
        .await
        .expect("clean up the probe remeasurements");
    // And the reopen series, which nothing else ever deletes: its rows name the
    // same marker and outlive the account rows they were written beside.
    sqlx::query("DELETE FROM change_reopen WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(&mut *tx)
        .await
        .expect("clean up the probe reopens");
    // The event half outlives the account by design, so it is deleted by its
    // own identity rather than following the row: a cascade is exactly what the
    // table was built not to have.
    sqlx::query("DELETE FROM change_ledger_event WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(&mut *tx)
        .await
        .expect("clean up the probe events");
    tx.commit().await.expect("commit the cleanup");
}

/// A declaration one fold has claimed is not handed to another fold.
///
/// `folded_at IS NULL` is a look, not a reservation: two folders reading it at
/// once both match the same rows, and the writes each of them makes are separate
/// orderings of the same batch. A document that corrects what it declares
/// re-queues the declaration, so one key can carry two readings of two different
/// texts -- and the slower folder then writes the older reading over the newer
/// one, marks the row folded, and the account cites what the document said
/// before it was corrected while every reading stays green.
///
/// The first folder is simulated by a transaction holding the row the way a fold
/// holds it. The assertion is about the row rather than the fold's count: the
/// fold is allowed to take whatever else is queued, so the only stable fact is
/// that this declaration is still unfolded when the fold returns. Once the claim
/// is released the next pass takes it -- a claim that never expired would stop
/// the fold rather than order it.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_declaration_claimed_by_one_fold_is_not_taken_by_another() {
    let _folder_lock = folder_lock().await;
    let (store, pool, marker) = store().await;
    let id = identity(&marker, "claimed");

    store
        .declare(&[ChangeReading {
            identity: id.clone(),
            title: Some("a change the probe declares".into()),
            assertion: Some("the probe's assertion".into()),
            source: ChangeSource {
                doc: format!("docs/{marker}.md"),
                verdict: Some("undecided".to_string()),
                priority: ChangePriority::Unstated,
                block: ChangeBlock::NONE,
            },
        }])
        .await
        .unwrap();

    // The first folder, holding its batch for as long as it is folding it.
    let mut holder = pool
        .acquire()
        .await
        .expect("acquire the claiming connection");
    let mut claim = holder.begin().await.expect("open the claim");
    let claimed: Vec<String> = sqlx::query_scalar(
        "SELECT scope FROM change_ledger_declared \
         WHERE folded_at IS NULL AND scope = $1 FOR UPDATE",
    )
    .bind(&id.scope)
    .fetch_all(&mut *claim)
    .await
    .expect("claim the declaration");
    assert_eq!(claimed.len(), 1, "the probe's declaration is queued");

    // The bound is on the wrong behaviour rather than on the right one: a fold
    // that skips returns in the time two statements take, while a fold that does
    // not skip waits for the claim -- and a fold that waits forever fails by
    // wedging the pass, which is not a shape a test can assert after the fact.
    // Thirty seconds is two orders of magnitude above the honest path.
    let _folded = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        store.fold_declarations(100, Some("probe-rev")),
    )
    .await
    .expect(
        "the fold waited on a declaration another folder had claimed instead of \
             skipping it, so two folders are separate orderings of one row and the \
             slower one decides what the account cites",
    )
    .expect("fold whatever is left");

    let unfolded = |pool: PgPool, scope: String| async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM change_ledger_declared \
             WHERE folded_at IS NULL AND scope = $1",
        )
        .bind(scope)
        .fetch_one(&pool)
        .await
        .expect("read the declaration back")
    };
    assert_eq!(
        unfolded(pool.clone(), id.scope.clone()).await,
        1,
        "a fold took a declaration another folder had claimed: the two are then \
         separate orderings of one row, and the slower one decides what the \
         account cites"
    );

    claim.rollback().await.expect("release the claim");
    drop(holder);
    store
        .fold_declarations(100, Some("probe-rev"))
        .await
        .expect("fold once the claim is released");
    assert_eq!(
        unfolded(pool.clone(), id.scope.clone()).await,
        0,
        "a claim that outlived the transaction would stop the fold rather than \
         order it"
    );

    cleanup(&pool, &marker).await;
}

/// The one-folder rule is a lock, and a lock nobody remembers to take is a
/// comment. Everything else in this file is `#[ignore]`d behind a live
/// database, so the concurrency they guard against is invisible on every run
/// that does not start one -- a new fold test written without the lock would
/// pass locally, pass in CI's ignored step whenever it happened to run alone,
/// and silently drop the rule this file exists to hold.
///
/// Read rather than executed: the shape being judged is source text (does the
/// test body take the lock), and the fold tests cannot run without a server.
#[test]
fn every_fold_test_holds_the_one_folder_lock() {
    fn close(name: &mut Option<String>, folds: bool, locked: bool, offenders: &mut Vec<String>) {
        if let Some(n) = name.take() {
            if folds && !locked {
                offenders.push(n);
            }
        }
    }

    let name = std::path::Path::new(file!())
        .file_name()
        .expect("the test file has a name")
        .to_string_lossy()
        .into_owned();
    let path = format!("{}/tests/{name}", env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(&path).expect("this test reads its own file");
    let mut offenders: Vec<String> = Vec::new();
    let mut name: Option<String> = None;
    let mut folds = false;
    let mut locked = false;
    let mut fold_bodies = 0usize;

    for line in source.lines() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        let is_item = indent <= 4
            && ["fn ", "async fn ", "pub fn ", "pub async fn "]
                .iter()
                .any(|prefix| trimmed.starts_with(prefix));
        if is_item {
            close(&mut name, folds, locked, &mut offenders);
            folds = false;
            locked = false;
            let head = trimmed
                .trim_start_matches("pub ")
                .trim_start_matches("async ")
                .trim_start_matches("fn ");
            name = Some(head.split('(').next().unwrap_or("?").to_string());
        }
        if name.is_some() {
            let folds_here =
                trimmed.contains("fold_declarations(") || trimmed.contains("fold_retirements(");
            if folds_here && !folds {
                folds = true;
                fold_bodies += 1;
            }
            if trimmed.contains("folder_lock()") {
                locked = true;
            }
        }
    }
    close(&mut name, folds, locked, &mut offenders);

    // A parser that finds nothing would read exactly like a file with no
    // offenders, which is the failure this test is supposed to catch.
    assert!(
        fold_bodies >= 5,
        "only {fold_bodies} fold bodies were found; the shape this test reads has changed \
         and it is no longer judging the file it was written for"
    );
    assert!(
        offenders.is_empty(),
        "these tests fold the account without taking the one-folder lock, so two of them \
         can interleave and the slower fold can land the older reading over the newer one: \
         {offenders:?}"
    );
}
