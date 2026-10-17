//! Which rows a measuring pass gets to look at, against a live PostgreSQL.
//!
//! The pass takes a bounded set and judges every row in it. The bound is the
//! whole question here: a row whose verdict is "a person holds this" is settled,
//! and writing that verdict is not something the account can hold -- no
//! measurement is applied, so `measured_at` stays null. Ordered by `measured_at`
//! with nulls first, such a row comes back at the head of every pass.
//!
//! That is invisible while there are few of them and total once there are more
//! than the pass can take: every pass spends its whole budget on the same rows
//! and no other row is looked at again. It does not read as a failure. The pass
//! line says it judged the set, and the set it judged was full.
//!
//! Two directions are checked, because either one alone can be satisfied by
//! breaking the other:
//!
//! - A row the pass can settle is in the set even when person-held rows outnumber
//!   the bound. This is the regression.
//! - Person-held rows are still in the set when there is room for them, because
//!   the pass line counts them and "a person holds this" is a reading the account
//!   owes its reader. Filtering them out of the query would fix the first
//!   direction by making that count read zero forever.
//!
//! Neither can be settled without a server: which order a `NULL`-heavy ordering
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

use cog_core::{PrincipleCheckKind, PrincipleLedgerStore, PrincipleReading};
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
