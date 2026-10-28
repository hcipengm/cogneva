//! What this crate's schema sweep does to the process that is already writing.
//!
//! The stores here create their tables and indexes at startup, in one text, on
//! a database another replica may still be using. Two facts make that unsafe
//! without a bound on the wait: a multi-statement simple query is one implicit
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
//!   cargo test -p cog-observability --test schema_lock_bound -- --ignored
//! ```
//!
//! Every run works inside its own schema and drops it at the end, so the
//! statements' unqualified table name cannot reach a table another run uses.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};

use cog_observability::analytics::PostgresAnalyticsBackend;

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

async fn schema_pool(url: &str, schema: &str) -> PgPool {
    let options = PgConnectOptions::from_str(url)
        .expect("the test database URL parses")
        .options([("search_path", schema)]);
    PgPoolOptions::new()
        .max_connections(6)
        .connect_with(options)
        .await
        .expect("the throwaway database is reachable")
}

/// Open a schema of this run's own and hand back its name.
async fn own_schema(url: &str) -> String {
    let schema = format!("schemabound_{}", std::process::id());
    let schema = format!(
        "{schema}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let mut admin = PgConnection::connect(url)
        .await
        .expect("the throwaway database is reachable");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is created");
    schema
}

async fn drop_schema(url: &str, schema: &str) {
    if let Ok(mut admin) = PgConnection::connect(url).await {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&mut admin)
            .await
            .ok();
    }
}

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
    let schema = own_schema(&url).await;
    let pool = schema_pool(&url, &schema).await;
    let table = "events_probe";
    let store = PostgresAnalyticsBackend::new(pool.clone()).with_table(table);
    store
        .init_table()
        .await
        .expect("the table and its indexes are created before anything holds them");

    let stop = Arc::new(AtomicBool::new(false));
    let (writer, took_it) = hold_a_writer(pool.clone(), table.to_string(), stop.clone());
    took_it.await.expect("the writer took its lock");

    let started = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(40), store.init_table())
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
    let schema = own_schema(&url).await;
    let pool = schema_pool(&url, &schema).await;
    let table = "events_probe";
    let store = PostgresAnalyticsBackend::new(pool.clone()).with_table(table);
    store
        .init_table()
        .await
        .expect("the table and its indexes are created before anything holds them");

    let stop = Arc::new(AtomicBool::new(false));
    let (writer, took_it) = hold_a_writer(pool.clone(), table.to_string(), stop.clone());
    took_it.await.expect("the writer took its lock");
    release_after(stop.clone(), Duration::from_millis(900));

    let outcome = tokio::time::timeout(Duration::from_secs(40), store.init_table())
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
