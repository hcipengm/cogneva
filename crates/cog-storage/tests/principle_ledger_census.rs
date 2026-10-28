//! Which rows the census counts as corpus, against a live PostgreSQL.
//!
//! The account holds one row per slug a gate refused work for, and a gate also
//! refuses under its own name: `principle-gate.sh:shape-table-unreadable` is a
//! row the outage recorder writes when the gate cannot read its material. Those
//! rows carry no state and no measurement by construction, so the census
//! counted them as principles nothing had ever looked at -- on 2026-10-23 that
//! cell read 2, and neither could be named from the reading, because the cell
//! is an aggregate and the rows behind it are not principles at all.
//!
//! Two directions are checked here, because either alone can be satisfied by
//! breaking the other:
//!
//! - A refusal filed under a gate's own name adds no row to the census, in
//!   either census (state or tier).
//! - That refusal still reaches `refused()`, which is the per-slug count the
//!   gate's own outage is read from. Filtering it out of the account -- rather
//!   than out of the census -- would have closed the first direction by
//!   deleting the count that made the widest gate outage ever recorded
//!   visible.
//!
//! Both are facts about a `WHERE` the server applies: which rows a grouped
//! count covered cannot be seen from the values alone, because a count is one
//! number per group either way.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test principle_ledger_census -- --ignored
//! ```
//!
//! The counts are of the whole table, so every case here asserts a **delta**
//! rather than a number, and cleans up the rows it filed.

use sqlx::PgPool;
use tokio::sync::Mutex;

use cog_core::{PrincipleLedgerStore, PrincipleRefusal};
use cog_storage::PostgresPrincipleLedgerStore;

/// The census is a reading of the whole table, so two cases running at once
/// take each other's rows into their delta. Serialized rather than merged: the
/// shared object is the table.
static TABLE: Mutex<()> = Mutex::const_new(());

use cog_test_support::database_url;

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
        format!("probe-principle-census:{}", uuid::Uuid::new_v4()),
    )
}

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

/// One refusal as a gate reports it, at an instant of this run.
fn refusal(slug: &str, second: i64, count: i64) -> PrincipleRefusal {
    PrincipleRefusal {
        slug: slug.to_string(),
        refused_at: chrono::DateTime::from_timestamp(second, 0)
            .expect("a second inside the representable range"),
        source: "probe-census".into(),
        count,
    }
}

async fn cleanup(pool: &PgPool, slugs: &[String]) {
    for table in ["principle_ledger", "principle_refusals"] {
        for slug in slugs {
            sqlx::query(&format!("DELETE FROM {table} WHERE slug = $1"))
                .bind(slug)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("clean up {table} for {slug}: {e}"));
        }
    }
}

/// A gate's own refusal identity is not corpus, and its count is still a
/// reading.
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_gate_refusal_identity_adds_no_row_to_the_census_but_keeps_its_count() {
    let _serialized = TABLE.lock().await;
    let (store, pool, marker) = store().await;

    // The shape the outage recorder writes, and the shape a principle has. The
    // gate slug is spelled the way the gate spells it -- a marker prefix would
    // change the shape under test.
    let gate_slug = format!("principle-gate.sh:probe-{marker}");
    let principle_slug = format!("{marker}/feedback_a_probe_principle");
    let slugs = [gate_slug.clone(), principle_slug.clone()];
    cleanup(&pool, &slugs).await;

    let before = store.census().await.expect("read the census");
    let tiers_before = store
        .check_kind_census()
        .await
        .expect("read the tier census");

    // A second no other row in this database can share: the queue's key is
    // (slug, refused_at, source, session), and the count is what the fold adds.
    let second = 1_800_000_000_i64;

    store
        .report_refusals(&[refusal(&gate_slug, second, 5)])
        .await
        .expect("report the gate's refusal");
    store
        .fold_refusals(1000)
        .await
        .expect("fold the gate's refusal");

    let after_gate = store.census().await.expect("read the census again");
    assert_eq!(
        after_gate.rows(),
        before.rows(),
        "a refusal filed under the gate's own name added a row to the census: \
         the account went from {} rows to {}",
        before.rows(),
        after_gate.rows()
    );
    let tiers_after = store
        .check_kind_census()
        .await
        .expect("read the tier census again");
    assert_eq!(
        tiers_after.total(),
        tiers_before.total(),
        "the tier census counted the gate's own refusal identity, so its total \
         disagrees with the state census"
    );

    // The count it was filed under is still published: this is the reading the
    // widest gate outage on record is read from, and a census fix that deleted
    // it would trade one blind spot for another.
    let counts = store.refused(1000).await.expect("read the refusal counts");
    let filed = counts
        .iter()
        .find(|row| row.slug == gate_slug)
        .unwrap_or_else(|| {
            panic!(
                "the gate's refusal count is missing from the account; rows: {:?}",
                counts.iter().map(|r| &r.slug).collect::<Vec<_>>()
            )
        });
    assert_eq!(
        filed.refusals, 5,
        "the count the gate reported was not folded"
    );

    // The other direction: a refusal filed under a principle's name is corpus,
    // and it does move the census. Without this the case above would also pass
    // against a store whose census read nothing at all.
    store
        .report_refusals(&[refusal(&principle_slug, second, 1)])
        .await
        .expect("report the principle's refusal");
    store
        .fold_refusals(1000)
        .await
        .expect("fold the principle's refusal");
    let after_principle = store.census().await.expect("read the census once more");
    assert_eq!(
        after_principle.rows(),
        before.rows() + 1,
        "a refusal filed under a principle's name did not become a row of the \
         account, so the previous assertion proved nothing about what the \
         predicate matches"
    );

    cleanup(&pool, &slugs).await;
}
