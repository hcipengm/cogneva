//! What this crate's schema runner does to the process that is already writing.
//!
//! Stores create their tables and indexes at startup, in one text, on a
//! database another replica may still be using, and they hand that text to
//! [`cog_storage::schema_ddl::PgSchemaRunner`] -- including stores in other
//! crates, which know the contract and not this implementation. So the bound
//! is measured here, against the runner those stores are given.
//!
//! Two facts make that unsafe without a bound on the wait: a multi-statement
//! simple query is one implicit
//! transaction, so the locks an earlier statement took are held until the last
//! one finishes, and `CREATE INDEX IF NOT EXISTS` takes its lock before it
//! checks whether the index is already there. A writer holding the table
//! therefore both blocks the sweep and is blocked by it -- for as long as the
//! writer keeps its transaction open, which for a serving replica is until it
//! is shut down.
//!
//! What the bound buys is not speed, it is an end: the sweep gives its locks
//! back, says which table it could not get, and the startup path reports that
//! as a store that failed to initialise instead of a pod that never becomes
//! ready. Without it this file's first test does not fail -- it never
//! finishes.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test schema_lock_bound -- --ignored
//! ```
//!
//! Every run works inside its own schema and drops it at the end, so the
//! statements' unqualified table name cannot reach a table another run uses.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;

use cog_core::SchemaRunner;
use cog_storage::schema_ddl::PgSchemaRunner;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool_with};

/// The table the probe writes into, and the index the sweep creates over it.
///
/// A table plus an index in one text, which is the shape every store's schema
/// has: the index is the statement that takes a lock a writer conflicts with,
/// and the table is what makes the first attempt succeed so the second one is
/// the one that collides.
const PROBE_TABLE: &str = "events_probe";

fn probe_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {PROBE_TABLE} (\
           event_id text PRIMARY KEY, event_type text NOT NULL);\n\
         CREATE INDEX IF NOT EXISTS {PROBE_TABLE}_type ON {PROBE_TABLE} (event_type);"
    )
}

/// Open a schema of this run's own and hand back its name.
/// Hold a write open on `table` until `stop` is raised, then roll back.
///
/// A write, not a read: the statements this crate runs at startup create
/// indexes, and `CREATE INDEX` asks for a lock that a plain reader never
/// conflicts with. The replica this sweep must not wait out is the one still
/// writing, so that is the session held here.
///
/// The signal is not a courtesy. Sleeping for "long enough" that the task got
/// scheduled lets the sweep run before anything is held, and then the test
/// measures a collision that never happened.
fn hold_a_writer(
    pool: PgPool,
    table: String,
    stop: Arc<AtomicBool>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (taken, took_it) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("writer connection");
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .expect("begin the writer's transaction");
        sqlx::query(&format!(
            "INSERT INTO {table} (event_id, event_type) VALUES ('probe', 'probe')"
        ))
        .execute(&mut *conn)
        .await
        .expect("the writer's insert");
        let _ = taken.send(());
        while !stop.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        sqlx::query("ROLLBACK").execute(&mut *conn).await.ok();
    });
    (handle, took_it)
}

fn release_after(stop: Arc<AtomicBool>, after: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        stop.store(true, Ordering::SeqCst);
    });
}

/// A store whose schema cannot get its locks has to end and say which table,
/// because the caller's other outcome is a startup that never finishes.
///
/// The writer here holds the table for the whole of the sweep's attempts,
/// which is what a replica that is still serving does. Without a bound on the
/// wait this test does not fail -- it does not finish.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_store_schema_names_the_table_it_could_not_lock_instead_of_hanging() {
    let url = database_url();
    let schema = own_schema(&url, "schemabound").await;
    let pool = schema_pool_with(&url, &schema, 6).await;
    let table = PROBE_TABLE;
    let runner = PgSchemaRunner::new(pool.clone());
    runner
        .apply_schema(&probe_ddl())
        .await
        .expect("the table and its indexes are created before anything holds them");

    let stop = Arc::new(AtomicBool::new(false));
    let (writer, took_it) = hold_a_writer(pool.clone(), table.to_string(), stop.clone());
    took_it.await.expect("the writer took its lock");

    let started = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(40), runner.apply_schema(&probe_ddl()))
        .await
        .expect("the schema must give up on the lock, not wait out the writer");
    let waited = started.elapsed();

    stop.store(true, Ordering::SeqCst);
    writer.await.expect("the writer task");
    drop_schema(&url, &schema).await;

    assert!(
        outcome.is_err(),
        "the writer never let go, so the schema cannot report success"
    );
    let message = format!("{}", outcome.unwrap_err());
    assert!(
        message.contains(table),
        "the failure has to name the relation it could not lock: the caller's \
         other outcome is a startup that never finishes and says nothing at \
         all. Got: {message}"
    );
    assert!(
        waited >= Duration::from_millis(200),
        "the schema gave up after {waited:?}, which is short enough that it \
         never asked for the lock -- the bound would then be untested"
    );
}

/// The other half: a lock that clears while the sweep is still attempting has
/// to be taken, not reported as blocked.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_store_schema_takes_its_lock_once_the_writer_commits() {
    let url = database_url();
    let schema = own_schema(&url, "schemabound").await;
    let pool = schema_pool_with(&url, &schema, 6).await;
    let table = PROBE_TABLE;
    let runner = PgSchemaRunner::new(pool.clone());
    runner
        .apply_schema(&probe_ddl())
        .await
        .expect("the table and its indexes are created before anything holds them");

    let stop = Arc::new(AtomicBool::new(false));
    let (writer, took_it) = hold_a_writer(pool.clone(), table.to_string(), stop.clone());
    took_it.await.expect("the writer took its lock");
    release_after(stop.clone(), Duration::from_millis(900));

    let outcome = tokio::time::timeout(Duration::from_secs(40), runner.apply_schema(&probe_ddl()))
        .await
        .expect("the retry must not hang either");

    stop.store(true, Ordering::SeqCst);
    writer.await.expect("the writer task");
    drop_schema(&url, &schema).await;

    assert!(
        outcome.is_ok(),
        "the writer let go after a second, so the schema should have landed: \
         {outcome:?}"
    );
}
