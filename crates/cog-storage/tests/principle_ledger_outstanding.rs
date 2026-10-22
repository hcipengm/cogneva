//! principle: feedback_a_verdict_the_queue_never_offers_the_row_to_is_not_a_fix
//! Which rows a measuring pass gets to look at, against a live PostgreSQL.
//!
//! The pass takes a bounded set and judges every row in it. The bound is the
//! whole question here: a row whose verdict is "a person holds this" is settled,
//! and it cannot take one of the three states -- a look stamps `measured_at` and
//! leaves the state empty.
//!
//! Before that stamp existed, such a row stayed null and, ordered by
//! `measured_at` with nulls first, came back at the head of every pass. That was
//! invisible while there were few of them and total once there were more than
//! the pass can take: every pass spent its whole budget on the same rows and no
//! other row was looked at again.
//!
//! Ordering the whole tier behind the debt fixed that and created the opposite
//! silence: a human-held row nothing had looked at was behind the debt, the debt
//! runs deeper than one pass takes, so the row was never looked at at all. That
//! is what `unsettled_judgement` is for -- a lane of its own with its own bound
//! -- and what the last case here checks.
//!
//! Three directions are checked, because any one of them alone can be satisfied
//! by breaking another:
//!
//! - A row the pass can settle is in the set even when person-held rows outnumber
//!   the bound. This is the regression.
//! - Person-held rows are still in the set when there is room for them, because
//!   the pass line counts them and "a person holds this" is a reading the account
//!   owes its reader. Filtering them out of the query would fix the first
//!   direction by making that count read zero forever.
//! - A person-held row nothing has looked at is offered by its own lane, and a
//!   look moves it out of that lane while leaving it held. Without the lane the
//!   row waits behind work that is re-read every pass, which is to say forever.
//!
//! None of these can be settled without a server: which order a `NULL`-heavy ordering
//! produces is a property of the server, not of a reading of the rows.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test principle_ledger_outstanding -- --ignored
//! ```
//!
//! The throwaway database starts empty, so the migrations are applied first, and
//! every row carries a per-run marker in its slug and is deleted at the end.

use sqlx::PgPool;
use tokio::sync::Mutex;

use cog_core::{
    PrincipleCheckKind, PrincipleLedgerStore, PrincipleMeasurement, PrincipleReading,
    PrincipleState,
};
use cog_storage::PostgresPrincipleLedgerStore;

/// One pass over one table.
///
/// The bound is applied to the whole table, so two cases running at once cut the
/// same set: a case that filed four rows can have its settleable row pushed out
/// of a two-row bound by the other case's rows. What each case asserts is a
/// sequence -- file, then read what a pass would take -- so they are serialized
/// rather than merged.
static PASS: Mutex<()> = Mutex::const_new(());

/// The bound the pass runs with in production. Spelled out here rather than
/// imported: the case is about how many person-held rows it takes to fill a
/// pass, and the number the pass uses lives in another crate.
const PASS_BOUND: usize = 200;

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
    // A case that panics skips its own cleanup, and the pass bound is global:
    // debris from an earlier run takes the head of the set and reddens cases
    // that are themselves sound. Clearing this probe's prefix at setup makes a
    // rerun mean what a first run meant.
    for table in ["principle_ledger", "principle_ledger_declared"] {
        sqlx::query(&format!(
            "DELETE FROM {table} WHERE slug LIKE 'probe-principle-outstanding:%'"
        ))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("clear leftover rows in {table}: {e}"));
    }
    (
        store,
        pool,
        format!("probe-principle-outstanding:{}", uuid::Uuid::new_v4()),
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
        .bind(97_003_i64)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    cog_storage::migrate::Migrator::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .run(url)
        .await
        .expect("apply the migrations to the throwaway database");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(97_003_i64)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

fn reading(slug: &str, tier: PrincipleCheckKind) -> PrincipleReading {
    PrincipleReading {
        slug: slug.to_string(),
        source: "host".to_string(),
        lead: Some("a rule the probe names".into()),
        carrier_path: Some("memory/probe-outstanding.md".into()),
        carrier_sha256: None,
        host_gate: None,
        where_red: None,
        check_kind: Some(tier),
        priority: None,
    }
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

/// File `count` rows of the tier `tier` under `marker`, and return their slugs in
/// the order they were filed.
async fn file(
    store: &PostgresPrincipleLedgerStore,
    marker: &str,
    label: &str,
    count: usize,
    tier: PrincipleCheckKind,
) -> Vec<String> {
    let slugs: Vec<String> = (0..count)
        .map(|i| format!("{marker}/{label}-{i:03}"))
        .collect();
    let readings: Vec<PrincipleReading> = slugs.iter().map(|s| reading(s, tier)).collect();
    store.declare(&readings).await.expect("queue the readings");
    for _ in 0..4 {
        let fold = store
            .fold_declarations(PASS_BOUND * 2)
            .await
            .expect("fold the queue");
        if fold.folded == 0 {
            break;
        }
    }
    slugs
}

/// A row the pass can settle is looked at even when the person-held rows alone
/// would fill the pass.
///
/// This is the shape the account was in: 220 rows a person holds, a pass that
/// takes 200, and every pass for hours judging those same rows while the
/// principles with a gate waited -- some of them for six days, with no line
/// anywhere saying they had stopped being looked at.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_held_row_does_not_crowd_a_settleable_one_out_of_the_pass() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    file(&store, &marker, "held", 3, PrincipleCheckKind::Judgement).await;
    let settleable = file(&store, &marker, "settle", 1, PrincipleCheckKind::Tree).await;

    // A bound smaller than the number of person-held rows filed: the old
    // ordering returns three of those and nothing else.
    let looked: Vec<String> = store
        .outstanding(2)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|slug| slug.starts_with(&marker))
        .collect();

    assert_eq!(
        looked.first().map(String::as_str),
        Some(settleable[0].as_str()),
        "a pass with room for two must spend one of them on the row it can settle; \
         got {looked:?}"
    );
    cleanup(&pool, &marker).await;
}

/// Settle a row as `enforced`, recording `gate` as the platform-side file, which
/// is what a measuring pass writes when it judges the row.
async fn settle(store: &PostgresPrincipleLedgerStore, slug: &str, gate: Option<&str>) -> String {
    let mut measurement =
        cog_core::PrincipleMeasurement::new(slug.to_string(), cog_core::PrincipleState::Enforced);
    measurement.platform_gate = gate.map(str::to_string);
    store
        .apply_principle_measurement(&measurement)
        .await
        .expect("write the measurement");
    slug.to_string()
}

/// A gate the platform gains after a row was settled re-opens that row.
///
/// A row a look has settled is out of the outstanding set, and no reading moves
/// it: only a measurement writes the state. So a declaration added to a file
/// after the row was judged would never be recorded against it, and the account
/// would keep answering "the platform names no gate here" for a principle the
/// platform now declares -- invisible on exactly the rows a gate is being moved
/// onto, since the host's side settles them first.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_gate_the_platform_gains_reopens_a_row_a_look_had_settled() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let slug = file(&store, &marker, "settled", 1, PrincipleCheckKind::Tree).await[0].clone();
    // Settled with a gate recorded, because a verdict naming no gate at all is
    // re-asked on its own (see the orphan case) and would be in the set already.
    settle(&store, &slug, Some("crates/probe/src/the_gate.rs")).await;

    let mut gates = std::collections::HashMap::new();
    gates.insert(slug.clone(), "crates/probe/src/moved_gate.rs".to_string());
    let reopened = store
        .reopen_platform_gate_drift(&gates)
        .await
        .expect("reconcile the platform gate");
    assert_eq!(
        reopened,
        vec![slug.clone()],
        "the row's gate moved and the row was not asked again"
    );

    let row = store
        .get(&slug)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(
        row.state, None,
        "a re-opened row is one nothing has looked at"
    );
    assert_eq!(
        row.measured_at, None,
        "nor has anything looked at it recently"
    );
    let looked: Vec<String> = store
        .outstanding(PASS_BOUND)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert!(
        looked.contains(&slug),
        "the row a pass is meant to judge is not in its set"
    );
    cleanup(&pool, &marker).await;
}

/// A row recorded as enforced with no gate in either registry is asked again.
///
/// The verdict and the two readings cannot all be right: the state says a machine
/// holds this principle, the host's column says its registry names no gate, and
/// the platform's column says nothing declares one. The only way there is a gate
/// withdrawn after the look -- the host's next reading moves the column to `-`
/// and the reconciliation compares the platform side alone, so nothing re-opens
/// the row. It has to be asked again, and at the head of the pass: a row that was
/// measured once sorts behind every row that never was, and the debt is deeper
/// than one pass takes, so it would never be reached.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn an_enforced_row_with_no_gate_recorded_is_asked_again() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let orphaned = file(&store, &marker, "orphaned", 1, PrincipleCheckKind::Tree).await[0].clone();
    settle(&store, &orphaned, None).await;

    let looked: Vec<String> = store
        .outstanding(PASS_BOUND)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert_eq!(
        looked.first().map(String::as_str),
        Some(orphaned.as_str()),
        "a verdict naming a gate the readings do not must be asked again, ahead of the \
         rows nothing has measured; got {looked:?}"
    );

    // A row settled with a gate recorded keeps its verdict: the readings and the
    // state agree, so nothing is contradictory and nothing is re-asked.
    let gated = file(&store, &marker, "gated", 1, PrincipleCheckKind::Tree).await[0].clone();
    settle(&store, &gated, Some("crates/probe/src/the_gate.rs")).await;
    let looked: Vec<String> = store
        .outstanding(PASS_BOUND)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert!(
        !looked.contains(&gated),
        "a row whose state and readings agree must not be re-asked; got {looked:?}"
    );
    cleanup(&pool, &marker).await;
}

/// A declaration removed from a `judgement` row re-opens it like any other.
///
/// A judgement row can be settled by a platform declaration -- the tier stops a
/// row being a debt when no machine names a gate, it does not stop a declaration
/// from settling one. That makes it an `enforced` row, and the reconciliation
/// has to see it: otherwise a declaration later taken out of the registry would
/// leave the row reported as enforced for good, with no file naming it.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_declaration_removed_from_a_judgement_row_reopens_it() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let slug = file(&store, &marker, "held", 1, PrincipleCheckKind::Judgement).await[0].clone();
    settle(&store, &slug, Some("crates/probe/src/the_gate.rs")).await;

    let reopened = store
        .reopen_platform_gate_drift(&std::collections::HashMap::new())
        .await
        .expect("reconcile the platform gate");
    assert_eq!(
        reopened,
        vec![slug.clone()],
        "the declaration this row was settled by is gone"
    );

    let row = store
        .get(&slug)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(
        row.state, None,
        "the tier's answer has to be asked again, not left settled"
    );
    cleanup(&pool, &marker).await;
}

/// A row whose recorded gate is what the registry says is not touched: the
/// reconciliation runs every pass, and re-opening an unchanged row would spend
/// the pass on the same rows forever.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_whose_recorded_gate_already_matches_is_left_alone() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let slug = file(&store, &marker, "settled", 1, PrincipleCheckKind::Tree).await[0].clone();
    settle(&store, &slug, Some("crates/probe/src/the_gate.rs")).await;

    let mut gates = std::collections::HashMap::new();
    gates.insert(slug.clone(), "crates/probe/src/the_gate.rs".to_string());
    let reopened = store
        .reopen_platform_gate_drift(&gates)
        .await
        .expect("reconcile the platform gate");
    assert!(
        reopened.is_empty(),
        "an unchanged gate re-opened {reopened:?}"
    );

    let row = store
        .get(&slug)
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(row.state, Some(cog_core::PrincipleState::Enforced));
    cleanup(&pool, &marker).await;
}

/// The rows a person holds stay in the set when there is room, because the pass
/// line counts them.
///
/// Dropping them from the query would also fix the crowding -- by making the
/// count of rows held on a person's behalf read zero on every pass, which is a
/// worse answer than a slow one.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn the_rows_a_person_holds_are_still_in_the_set() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let held = file(&store, &marker, "held", 3, PrincipleCheckKind::Judgement).await;
    let settleable = file(&store, &marker, "settle", 1, PrincipleCheckKind::Tree).await;

    let looked: Vec<String> = store
        .outstanding(PASS_BOUND)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|slug| slug.starts_with(&marker))
        .collect();

    for slug in held.iter().chain(settleable.iter()) {
        assert!(
            looked.contains(slug),
            "{slug} is not in the set a pass with room would look at; got {looked:?}"
        );
    }
    cleanup(&pool, &marker).await;
}

/// File one row of tier `tier` whose host column carries `host_gate`, and return
/// its slug.
async fn file_with_gate(
    store: &PostgresPrincipleLedgerStore,
    marker: &str,
    label: &str,
    tier: PrincipleCheckKind,
    host_gate: &str,
) -> String {
    let slug = format!("{marker}/{label}");
    let mut row = reading(&slug, tier);
    row.host_gate = Some(host_gate.to_string());
    store.declare(&[row]).await.expect("queue the reading");
    for _ in 0..4 {
        let fold = store
            .fold_declarations(PASS_BOUND * 2)
            .await
            .expect("fold the queue");
        if fold.folded == 0 {
            break;
        }
    }
    slug
}

/// A retired row leaves the outstanding set, and a carrier that comes back puts
/// it back in.
///
/// The first half is the point of the state: without it the row is looked at on
/// every pass, settles nothing, and returns to the head of the next one, while
/// every surface that reads "no state" reports it exactly like a principle
/// nobody has got to. The second half is what keeps that from being a delete:
/// the row's host column is written by the fold from the host's reading, so a
/// carrier that returns to the roster arrives as a changed value and the row is
/// measured again.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_retired_row_leaves_the_pass_and_returns_when_its_carrier_does() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let slug = file_with_gate(
        &store,
        &marker,
        "gone",
        PrincipleCheckKind::Tree,
        "retired:carrier_not_in_tree",
    )
    .await;

    let mut measurement =
        cog_core::PrincipleMeasurement::new(slug.clone(), cog_core::PrincipleState::Retired);
    measurement.measured_rev = Some("probe-retired-rev".to_string());
    store
        .apply_principle_measurement(&measurement)
        .await
        .expect("write the retirement");

    let looked: Vec<String> = store
        .outstanding(PASS_BOUND)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert!(
        !looked.contains(&slug),
        "a retired row is not waiting for a look; got {looked:?}"
    );

    // The carrier comes back: the host's next reading no longer carries the
    // retire marker, and that value change is the whole signal.
    file_with_gate(&store, &marker, "gone", PrincipleCheckKind::Tree, "-").await;
    let looked: Vec<String> = store
        .outstanding(PASS_BOUND)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert!(
        looked.contains(&slug),
        "a row whose carrier returned must be measured again; got {looked:?}"
    );
    cleanup(&pool, &marker).await;
}

/// A retirement reading for a `judgement` row is not buried by the debt ahead of
/// it.
///
/// The tier ordering exists because a person-held row never leaves the set; it
/// is wrong for a row that is about to leave. A retired `judgement` row has no
/// declaration left to rescue it either -- the carrier's removal is what removed
/// the declaration -- so with more debt than one pass takes it would keep the
/// marker and never reach the state the marker stands for, and every reader of
/// the state column would see the same empty cell as a row nobody has got to.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_retirement_reading_is_not_buried_by_the_debt_ahead_of_it() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    file(&store, &marker, "debt", 3, PrincipleCheckKind::Tree).await;
    let slug = file_with_gate(
        &store,
        &marker,
        "gone",
        PrincipleCheckKind::Judgement,
        "retired:carrier_not_in_tree",
    )
    .await;

    let looked: Vec<String> = store
        .outstanding(2)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();

    assert!(
        looked.contains(&slug),
        "a pass with room for two must reach the row that is waiting to leave; \
         got {looked:?}"
    );
    cleanup(&pool, &marker).await;
}

/// What the sweep lane offers this fixture, in the lane's own order.
async fn swept(store: &PostgresPrincipleLedgerStore, marker: &str) -> Vec<String> {
    store
        .unsettled_judgement(PASS_BOUND)
        .await
        .expect("read the sweep lane")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(marker))
        .collect()
}

/// A human-held row no look has reached is offered by its own lane, and a look
/// moves it out of that lane without moving its state.
///
/// The batch cannot serve these rows: the judgement tier sorts behind the debt,
/// and the debt is deeper than one pass takes. Without the lane the row's answer
/// ("no gate is meant to exist") waits behind work that is re-read every pass,
/// and reads the same as a row nothing has ever seen.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_never_looked_held_row_is_offered_even_when_the_debt_fills_the_pass() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let held = file(&store, &marker, "held", 2, PrincipleCheckKind::Judgement).await;
    let debt = file(&store, &marker, "debt", 3, PrincipleCheckKind::Tree).await;

    // The batch is filled by debt, so the held rows are not in it -- that is the
    // premise the lane exists for, not an accident of this fixture.
    let batch: Vec<String> = store
        .outstanding(2)
        .await
        .expect("read the outstanding set")
        .into_iter()
        .map(|row| row.slug)
        .filter(|s| s.starts_with(&marker))
        .collect();
    assert!(
        batch.iter().all(|s| debt.contains(s)),
        "the fixture must fill the batch with debt, or the lane is not being tested; \
         got {batch:?}"
    );

    let mut offered = swept(&store, &marker).await;
    offered.sort();
    let mut expected = held.clone();
    expected.sort();
    assert_eq!(
        offered, expected,
        "the lane offers exactly the never-looked held rows"
    );

    // A look records the answer and leaves the state empty: the row leaves the
    // lane because it has been seen, not because it stopped being held.
    store
        .record_human_only(&held[0], None)
        .await
        .expect("record the look");
    let offered = swept(&store, &marker).await;
    assert_eq!(offered, vec![held[1].clone()]);
    let row = store
        .get(&held[0])
        .await
        .expect("read the row")
        .expect("the row exists");
    assert_eq!(row.state, None, "a human-held row keeps no state");
    assert!(
        row.measured_at.is_some(),
        "the look is what takes it out of the lane"
    );

    cleanup(&pool, &marker).await;
}

/// A row looked at before the verdict read its host column keeps an empty state
/// while its own reading names a gate. The batch sorts the tier behind the debt,
/// so nothing else asks it again: the lane has to, or the row reports "no gate"
/// for as long as its own evidence says otherwise.
///
/// A stamped row whose host column names no gate stays out -- its answer has not
/// changed, and re-stamping it every pass would spend the lane on the same rows.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_stamped_held_row_whose_host_column_names_a_gate_is_asked_again() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let gated = format!("{marker}/gated-000");
    let ungated = format!("{marker}/ungated-000");

    let mut gated_reading = reading(&gated, PrincipleCheckKind::Judgement);
    gated_reading.host_gate = Some("test:probe-gated.test.sh".into());
    let ungated_reading = reading(&ungated, PrincipleCheckKind::Judgement);
    store
        .declare(&[gated_reading, ungated_reading])
        .await
        .expect("queue the readings");
    for _ in 0..4 {
        let fold = store
            .fold_declarations(PASS_BOUND * 2)
            .await
            .expect("fold the queue");
        if fold.folded == 0 {
            break;
        }
    }

    // The shape the verdict used to leave behind: both rows looked at, stamped,
    // state empty, and one of them still naming a gate.
    for slug in [&gated, &ungated] {
        store
            .record_human_only(slug, Some("probe-rev"))
            .await
            .expect("record the look");
    }

    let offered = swept(&store, &marker).await;
    assert_eq!(
        offered,
        vec![gated.clone()],
        "the lane asks again exactly the stamped row whose own reading names a gate"
    );

    // A verdict settles it, and the row leaves the lane because the state now
    // answers the question its host column raised.
    let mut measurement = PrincipleMeasurement::new(&gated, PrincipleState::Enforced);
    measurement.platform_gate = None;
    store
        .apply_principle_measurement(&measurement)
        .await
        .expect("write the verdict");
    assert!(
        swept(&store, &marker).await.is_empty(),
        "a settled row is not asked again"
    );

    cleanup(&pool, &marker).await;
}

/// A held row carrying the one state its tier cannot write is asked again.
///
/// `unenforced` is the verdict "both registries were read and neither names a
/// gate", which is a debt. For a principle a person holds that answer cannot be
/// right: the tier says no gate is meant to exist, so the registry's silence is
/// not a finding. A row measured before its carrier declared the tier keeps the
/// state anyway, and it reads as a debt on every surface -- the batch sorts the
/// tier behind the debt, and a lane for rows with no state never looks at a row
/// that has one. The state itself is what has to be re-read, whether or not the
/// host column names a gate, so the row is offered with neither.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_held_row_holding_the_state_its_tier_cannot_write_is_asked_again() {
    let _pass = PASS.lock().await;
    let (store, pool, marker) = store().await;
    let stale = file(&store, &marker, "stale", 1, PrincipleCheckKind::Judgement)
        .await
        .remove(0);
    let held = file(&store, &marker, "held", 1, PrincipleCheckKind::Judgement)
        .await
        .remove(0);
    store
        .record_human_only(&held, Some("probe-rev"))
        .await
        .expect("record the look");

    // The shape a look wrote before the tier was declared on the carrier: a
    // debt, on a row a person holds.
    let mut measurement = PrincipleMeasurement::new(&stale, PrincipleState::Unenforced);
    measurement.platform_gate = None;
    store
        .apply_principle_measurement(&measurement)
        .await
        .expect("write the verdict");

    assert_eq!(
        swept(&store, &marker).await,
        vec![stale.clone()],
        "the lane asks again the held row holding a state its tier can never write"
    );

    // The look that settles it clears the state, and the row leaves the lane.
    store
        .record_human_only(&stale, Some("probe-rev"))
        .await
        .expect("record the look");
    assert!(
        swept(&store, &marker).await.is_empty(),
        "a held row the tier has answered is not asked again"
    );

    cleanup(&pool, &marker).await;
}
