//! What one replica's schema sweep does to a replica that is still serving.
//!
//! Run against a live PostgreSQL:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://t:t@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test schema_ddl_lock -- --ignored
//! ```
//!
//! Every table here is this test's own, named from a marker only this run can
//! produce, and dropped at the end -- so the shared database is left as it was
//! found and no other test's rows are read. The reader sessions are awaited
//! rather than abandoned, because a connection returned to the pool still
//! inside an open transaction would hold its lock against the cleanup below.
//!
//! The shape under test is the one a rolling restart walks into: an
//! `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` takes ACCESS EXCLUSIVE before it
//! checks whether the column is already there, so the incoming replica asks
//! for the heaviest lock on a table the outgoing replica is reading -- and
//! because PostgreSQL's lock queue is first-come, every later read of that
//! table then queues behind the ALTER. A schema sweep with no bound on that
//! wait holds the queue open for as long as the reader keeps coming back.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A marker only this run produces, so the tables below cannot collide with a
/// concurrent run of the same file or with rows another test left behind.
fn marker() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("sddl_{}_{}", std::process::id(), nanos)
}

use cog_test_support::database_url;

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(6)
        .connect(&database_url())
        .await
        .expect("connect to the throwaway database")
}

async fn create_table(pool: &PgPool, table: &str, columns: &str) {
    sqlx::query(&format!("CREATE TABLE {table} ({columns})"))
        .execute(pool)
        .await
        .expect("create the table");
}

/// Hold a plain reader on `table` until `stop` is raised, then roll back.
///
/// A reader is `BEGIN; SELECT ...` and nothing else: that is ACCESS SHARE, it
/// conflicts with the ACCESS EXCLUSIVE an ALTER asks for, and it is what the
/// outgoing replica holds while it is serving. The task is awaited by every
/// caller, so the rollback is what ends it -- nothing here is left for a drop
/// to unwind.
fn hold_a_reader(
    pool: PgPool,
    table: String,
    stop: Arc<AtomicBool>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    // The signal is not a courtesy: sleeping for "long enough" that the task
    // got scheduled lets the schema run before the reader is holding anything,
    // and then the test measures a collision that never happened.
    let (taken, took_it) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("reader connection");
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .expect("begin the reader's transaction");
        sqlx::query(&format!("SELECT count(*) FROM {table}"))
            .execute(&mut *conn)
            .await
            .expect("the reader's select");
        let _ = taken.send(());
        while !stop.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        sqlx::query("ROLLBACK").execute(&mut *conn).await.ok();
    });
    (handle, took_it)
}

/// Raise `stop` after `after`, so a held reader can be given a lifespan.
fn release_after(stop: Arc<AtomicBool>, after: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        stop.store(true, Ordering::SeqCst);
    });
}

/// The worst single-read latency seen while `until` is raised.
///
/// A read that never returns must not hang the whole test: it is the failure
/// this probe is looking for, and it shows up as the cap below rather than as
/// a stuck binary.
async fn worst_read_latency(pool: PgPool, table: String, until: Arc<AtomicBool>) -> Duration {
    let mut worst = Duration::ZERO;
    let statement = format!("SELECT count(*) FROM {table}");
    while !until.load(Ordering::SeqCst) {
        let started = Instant::now();
        let read = sqlx::query(&statement).execute(&pool);
        let _ = tokio::time::timeout(Duration::from_secs(5), read).await;
        worst = worst.max(started.elapsed());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    worst
}

async fn drop_tables(pool: &PgPool, tables: &[&str]) {
    for table in tables {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(pool)
            .await
            .ok();
    }
}

/// The unbounded wait: a schema statement that cannot get its lock must end,
/// not wait for the reader to go away.
///
/// The reader here holds the table for the whole of the schema's attempts,
/// which is what the outgoing replica does for as long as it is serving.
/// Without a bound on the wait this test does not fail -- it does not finish.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_schema_gives_up_rather_than_waiting_out_a_live_reader() {
    let pool = pool().await;
    let marker = marker();
    let table = format!("{marker}_t");
    create_table(&pool, &table, "a int, c2 int").await;

    let stop = Arc::new(AtomicBool::new(false));
    let (reader, took_it) = hold_a_reader(pool.clone(), table.clone(), stop.clone());
    took_it.await.expect("the reader took its lock");

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(40),
        cog_storage::schema_ddl::apply(
            &pool,
            &format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS c2 int;"),
        ),
    )
    .await
    .expect("the schema must give up on the lock, not wait for the reader");
    let waited = started.elapsed();

    stop.store(true, Ordering::SeqCst);
    reader.await.expect("the reader task");
    drop_tables(&pool, &[&table]).await;

    assert!(
        outcome.is_err(),
        "the lock never cleared, so the schema cannot report success"
    );
    let message = format!("{}", outcome.as_ref().unwrap_err());
    assert!(
        message.contains(&table),
        "the failure has to name the relation it could not lock: the caller's \
         other outcome is a container killed on a startup probe that says \
         nothing at all. Got: {message}"
    );
    assert!(
        waited >= Duration::from_millis(200),
        "the schema gave up after {waited:?}, which is short enough that it \
         never asked for the lock -- the bound would then be untested"
    );
}

/// The harm the bound exists to prevent: a table the schema already holds is
/// frozen for everyone else until the schema finishes.
///
/// The batch alters a free table first and then blocks on a held one, so the
/// first ALTER's ACCESS EXCLUSIVE is taken and kept while the second waits --
/// the accumulation an implicit transaction produces. A plain read of the
/// already-altered table is then either served between attempts (bound
/// present) or queued behind the schema for its whole duration (bound absent).
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_newcomers_schema_does_not_freeze_a_reader_of_a_table_it_already_locked() {
    let pool = pool().await;
    let marker = marker();
    let free = format!("{marker}_free");
    let held = format!("{marker}_held");
    for table in [&free, &held] {
        create_table(&pool, table, "a int, c2 int").await;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let (reader, took_it) = hold_a_reader(pool.clone(), held.clone(), stop.clone());
    took_it.await.expect("the reader took its lock");
    release_after(stop.clone(), Duration::from_secs(3));

    let probing = Arc::new(AtomicBool::new(false));
    let probe = tokio::spawn(worst_read_latency(
        pool.clone(),
        free.clone(),
        probing.clone(),
    ));

    let schema = format!(
        "ALTER TABLE {free} ADD COLUMN IF NOT EXISTS c2 int;\n\
         ALTER TABLE {held} ADD COLUMN IF NOT EXISTS c2 int;"
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(40),
        cog_storage::schema_ddl::apply(&pool, &schema),
    )
    .await
    .expect("the schema must not wait out the reader");

    probing.store(true, Ordering::SeqCst);
    let worst = probe.await.expect("the probe task");
    stop.store(true, Ordering::SeqCst);
    reader.await.expect("the reader task");
    drop_tables(&pool, &[&free, &held]).await;

    assert!(
        outcome.is_ok(),
        "the reader lets go after three seconds, so a retry has to land: {outcome:?}"
    );
    assert!(
        worst < Duration::from_secs(2),
        "a reader of {free} waited {worst:?} while the schema was blocked on \
         {held}: it was queueing behind a lock the schema had no reason to keep \
         holding, and on a real rollout that reader is the replica the new one \
         is replacing"
    );
}

/// The retry has to land: a lock that clears while the schema is still
/// attempting must be taken, not reported as blocked.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_schema_takes_its_lock_once_the_reader_stops() {
    let pool = pool().await;
    let marker = marker();
    let table = format!("{marker}_t");
    create_table(&pool, &table, "a int").await;

    let stop = Arc::new(AtomicBool::new(false));
    let (reader, took_it) = hold_a_reader(pool.clone(), table.clone(), stop.clone());
    took_it.await.expect("the reader took its lock");
    release_after(stop.clone(), Duration::from_millis(900));

    let outcome = tokio::time::timeout(
        Duration::from_secs(40),
        cog_storage::schema_ddl::apply(
            &pool,
            &format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS c2 int;"),
        ),
    )
    .await
    .expect("the retry must not hang either");

    stop.store(true, Ordering::SeqCst);
    reader.await.expect("the reader task");

    let added: Option<String> = sqlx::query_scalar(
        "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass \
         AND attname = 'c2' AND NOT attisdropped",
    )
    .bind(&table)
    .fetch_optional(&pool)
    .await
    .expect("read the catalog");
    drop_tables(&pool, &[&table]).await;

    assert!(
        outcome.is_ok(),
        "the reader let go after a second, so the schema should have landed: {outcome:?}"
    );
    assert_eq!(added.as_deref(), Some("c2"), "the column has to be there");
}
