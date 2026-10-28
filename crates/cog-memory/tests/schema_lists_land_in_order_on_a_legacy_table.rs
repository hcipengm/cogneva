//! The three schema lists land in the order they have to, against a table that
//! predates two of its columns.
//!
//! Splitting a store's schema into several lists buys the readers of that
//! table the difference between "closed while the shape changes" and "closed
//! while the rows are walked", but it also turns one transaction into three --
//! so the order is now something a start depends on rather than something the
//! transaction enforced. `SET NOT NULL` on a column the repair has not filled
//! fails outright, and a repair that runs before the `ADD COLUMN` it needs
//! fails too. Both failures land at startup, on a database whose shape is old,
//! which is exactly the database a fresh test database is not.
//!
//! So this builds the old shape first: `first_seen` and `last_seen` present
//! and empty, `observed_by` absent, rows already written. Running the store's
//! own `init_table` then has to fill them, mark them, and come out idempotent
//! on a second start.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL`:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-memory --test schema_lists_land_in_order_on_a_legacy_table -- --ignored
//! ```
//!
//! Every run works inside its own schema and drops it, so the statements'
//! unqualified table name cannot reach a table another run uses.

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};

use cog_memory::{PostgresEntryStore, PostgresSchemaBackend};

use cog_test_support::database_url;

async fn own_schema(url: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let schema = format!("memorder_{}_{nanos}", std::process::id());
    let mut admin = PgConnection::connect(url)
        .await
        .expect("the throwaway database is reachable");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is created");
    schema
}

async fn schema_pool(url: &str, schema: &str) -> PgPool {
    let options = PgConnectOptions::from_str(url)
        .expect("the test database URL parses")
        .options([("search_path", schema)]);
    PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("the throwaway database is reachable")
}

async fn drop_schema(url: &str, schema: &str) {
    if let Ok(mut admin) = PgConnection::connect(url).await {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&mut admin)
            .await
            .ok();
    }
}

/// The table as it stood before `observed_by`, `first_seen` and `last_seen`
/// meant anything: the two timestamps are there but empty, and the observer
/// list does not exist at all.
const LEGACY_SCHEMA_ENTRIES: &str = r#"
CREATE TABLE schema_entries (
    id                TEXT        PRIMARY KEY,
    namespace         TEXT        NOT NULL DEFAULT 'default',
    kind              TEXT        NOT NULL,
    name              TEXT        NOT NULL,
    key               TEXT        NOT NULL,
    properties        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    raw_uri           TEXT        NOT NULL,
    range_spec        TEXT,
    extractor_version TEXT        NOT NULL,
    extracted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confidence        REAL        NOT NULL DEFAULT 1.0,
    importance        REAL        NOT NULL DEFAULT 0.5,
    first_seen        TIMESTAMPTZ,
    last_seen         TIMESTAMPTZ
);
INSERT INTO schema_entries (id, kind, name, key, raw_uri, extractor_version)
VALUES ('old', 'file', 'old', 'old', 'src/old.rs', 'v1');
"#;

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_lists_fill_a_table_that_predates_its_columns_and_are_safe_to_repeat() {
    let url = database_url();
    let schema = own_schema(&url).await;
    let pool = schema_pool(&url, &schema).await;

    sqlx::raw_sql(LEGACY_SCHEMA_ENTRIES)
        .execute(&pool)
        .await
        .expect("the legacy table is built with a row in it");

    // Two starts: the second is the one every restart after an upgrade does,
    // and it has to be a no-op rather than an error.
    for start in 1..=2 {
        PostgresSchemaBackend::from_pool(pool.clone())
            .init_table()
            .await
            .unwrap_or_else(|e| panic!("start {start} must apply the lists: {e}"));
    }

    let row: (Vec<String>, bool, bool) = sqlx::query_as(
        "SELECT observed_by, first_seen IS NOT NULL, last_seen IS NOT NULL \
           FROM schema_entries WHERE id = 'old'",
    )
    .fetch_one(&pool)
    .await
    .expect("the legacy row is still there");

    let not_null: Vec<String> = sqlx::query_scalar(
        "SELECT attname FROM pg_attribute \
          WHERE attrelid = 'schema_entries'::regclass AND attnotnull \
            AND attname IN ('first_seen', 'last_seen') ORDER BY attname",
    )
    .fetch_all(&pool)
    .await
    .expect("read the catalog");

    drop_schema(&url, &schema).await;

    assert_eq!(
        row.0,
        vec!["src/old.rs".to_string()],
        "the observer list has to be seeded from the row's own raw_uri, or \
         every reconciliation sweep re-extracts it"
    );
    assert!(
        row.1 && row.2,
        "the two timestamps have to be filled from extracted_at before the \
         columns are marked NOT NULL"
    );
    assert_eq!(
        not_null,
        vec!["first_seen".to_string(), "last_seen".to_string()],
        "both columns have to end up NOT NULL once the rows are filled"
    );
}

/// The same order for the summary store, whose second list is one statement
/// and whose first has nothing that depends on it -- so the failure this can
/// catch is a list that was put back into the first.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_summary_store_also_writes_its_rows_outside_the_shape() {
    let url = database_url();
    let schema = own_schema(&url).await;
    let pool = schema_pool(&url, &schema).await;

    PostgresEntryStore::from_pool(pool.clone())
        .init_table()
        .await
        .expect("a fresh database takes both lists");

    // A row of the shape the correction exists for: a run of zeros where an
    // embedding used to be, with a real model name beside it.
    sqlx::query(
        "INSERT INTO summary_entries \
             (id, text, embedding, embedding_model, raw_uri, extractor_version, extracted_at) \
         VALUES ('z', 'text', array_fill(0.0::real, ARRAY[4]), 'bge-m3', 'r', 'v1', NOW())",
    )
    .execute(&pool)
    .await
    .expect("the zero-vector row is inserted");

    PostgresEntryStore::from_pool(pool.clone())
        .init_table()
        .await
        .expect("the second start applies both lists again");

    let (embedding, model): (Vec<f32>, String) =
        sqlx::query_as("SELECT embedding, embedding_model FROM summary_entries WHERE id = 'z'")
            .fetch_one(&pool)
            .await
            .expect("the row is still there");

    drop_schema(&url, &schema).await;

    assert!(
        embedding.is_empty(),
        "a vector of zeros says nothing and reads as a match at score 0.0; \
         the correction has to empty it, not leave it: {embedding:?}"
    );
    assert!(
        model.is_empty(),
        "the model name has to go with the vector it described, or the row \
         claims a model it has no vector from"
    );
}
