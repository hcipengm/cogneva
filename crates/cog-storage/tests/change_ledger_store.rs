//! The change ledger's storage, against a live PostgreSQL.
//!
//! What these tests check is what the database decides: that the reason a row is
//! `undecided` reaches its own column and reads back, that a later measurement of
//! another state clears it the way `landed` clears `landed_at`, and that a reason
//! outside the closed set is refused rather than stored. None of it can be checked
//! without a server -- the coupling between the reason and the state lives in the
//! statement and in a CHECK the server enforces, and "a reason outside the set is
//! refused" is a fact about an ENUM or a CHECK, not about our code.
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
    ChangeIdentity, ChangeLedgerStore, ChangeMeasurement, ChangeState,
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

async fn cleanup(pool: &PgPool, marker: &str) {
    sqlx::query("DELETE FROM change_ledger WHERE scope LIKE $1")
        .bind(format!("{marker}/%"))
        .execute(pool)
        .await
        .expect("clean up the probe rows");
}
