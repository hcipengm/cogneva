//! One principle's refusals, split by the shape that caused them, against a
//! live PostgreSQL.
//!
//! A refusal is reported against a principle, and a principle can be carried by
//! more than one shape. The shared-tree rule answers to a wide add and to a bare
//! commit; the deferral rule answers to a family of sentences. Until the shape
//! was recorded, both added to one number, and a reader could say "this rule was
//! refused 312 times" while being unable to say which of its shapes was doing
//! it -- the question that decides whether the rule is being broken or the gate
//! is too tight.
//!
//! Three facts are checked here, and the first two are the ones a count can hide:
//!
//! - Two shapes of one principle refused in the same second are two rows, not
//!   one row naming whichever arrived first.
//! - The text that matched is kept verbatim, so the row can be read without the
//!   call, the tree or the session that produced it.
//! - A refusal with no shape -- what every row written before the column existed
//!   looks like -- stays a row of its own and is not counted into a shape that
//!   never produced it.
//!
//! Ignored by default; needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway
//! database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test principle_refusal_shapes -- --ignored
//! ```

mod common;

use sqlx::PgPool;
use tokio::sync::Mutex;

use cog_core::{PrincipleLedgerStore, PrincipleRefusal};
use cog_storage::PostgresPrincipleLedgerStore;

/// The rows are read back by slug, so two cases running at once would read each
/// other's. Serialized rather than merged: the shared object is the table.
static TABLE: Mutex<()> = Mutex::const_new(());

use cog_test_support::database_url;

async fn store() -> (PostgresPrincipleLedgerStore, PgPool, String) {
    let url = database_url();
    common::migrate_once(&url, 97_002).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    let store = PostgresPrincipleLedgerStore::new(pool.clone());
    (
        store,
        pool,
        format!("probe-refusal-shape:{}", uuid::Uuid::new_v4()),
    )
}

fn refusal(slug: &str, second: i64, criterion: &str, span: &str) -> PrincipleRefusal {
    PrincipleRefusal {
        slug: slug.to_string(),
        refused_at: chrono::DateTime::from_timestamp(second, 0)
            .expect("a second inside the representable range"),
        source: "probe-shape".into(),
        count: 1,
        criterion: criterion.into(),
        span: span.into(),
    }
}

/// What the reading counts, one row per shape: the shape, how many refusals, and
/// the text of the last one.
async fn by_shape(pool: &PgPool, slug: &str) -> Vec<(String, i64, String)> {
    sqlx::query_as::<_, (String, i64, String)>(
        "SELECT criterion, sum(count)::bigint, max(span) \
           FROM principle_refusals WHERE slug = $1 GROUP BY criterion ORDER BY criterion",
    )
    .bind(slug)
    .fetch_all(pool)
    .await
    .expect("read the refusals back by shape")
}

async fn cleanup(pool: &PgPool, slug: &str) {
    for table in ["principle_ledger", "principle_refusals"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE slug = $1"))
            .bind(slug)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("clean up {table}: {e}"));
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn two_shapes_of_one_principle_refused_in_one_second_are_two_rows() {
    let _serialized = TABLE.lock().await;
    let (store, pool, marker) = store().await;
    let slug = format!("{marker}/feedback_a_probe_principle");
    cleanup(&pool, &slug).await;

    // The same second, the same source, the same principle: the only thing that
    // separates them is the shape. Under the previous key this was one row, and
    // the row named the shape that happened to be written first.
    store
        .report_refusals(&[
            refusal(&slug, 1_780_000_000, "NARROW_ADD_RE", "git add -A"),
            refusal(&slug, 1_780_000_000, "BARE_COMMIT_RE", "git commit -m x"),
        ])
        .await
        .expect("report two refusals of different shapes");

    let rows = by_shape(&pool, &slug).await;
    assert_eq!(
        rows,
        vec![
            (
                "BARE_COMMIT_RE".to_string(),
                1,
                "git commit -m x".to_string()
            ),
            ("NARROW_ADD_RE".to_string(), 1, "git add -A".to_string()),
        ],
        "two shapes refused in one second are two facts and have to read as two \
         rows; one row with a count of two cannot say which shape is doing it, \
         which is the whole reason the shape is recorded"
    );

    cleanup(&pool, &slug).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_refusal_with_no_shape_is_its_own_bucket_and_never_a_shape() {
    let _serialized = TABLE.lock().await;
    let (store, pool, marker) = store().await;
    let slug = format!("{marker}/feedback_a_probe_principle");
    cleanup(&pool, &slug).await;

    // An old writer reported these: no shape, no text. Grouping them into a
    // named shape would move refusals onto a shape that never produced them;
    // grouping them into anything but a bucket of their own makes the reading
    // say the account knows something it does not.
    store
        .report_refusals(&[
            refusal(&slug, 1_780_000_100, "", ""),
            refusal(&slug, 1_780_000_101, "CIRCLED_CHARS", "a numeral"),
        ])
        .await
        .expect("report an unshaped refusal beside a shaped one");

    let rows = by_shape(&pool, &slug).await;
    assert_eq!(
        rows,
        vec![
            ("".to_string(), 1, "".to_string()),
            ("CIRCLED_CHARS".to_string(), 1, "a numeral".to_string()),
        ],
        "a row written before the shape existed has no shape, and it must read \
         as a bucket of its own rather than be counted into one of the shapes -- \
         a reading that did that would credit a shape with refusals it never had, \
         and nothing on the row would say so"
    );

    cleanup(&pool, &slug).await;
}
