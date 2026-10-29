// 每个集成测试文件各自编一次这份夹具，只用得到其中几个的文件不该为此报警。
#![allow(dead_code)]

//! What more than one of this crate's suites needs before it can ask its own
//! question: the migrations applied once, a raw Redis connection, and the two
//! statements a scratch-table case uses to count and drop its table.
//!
//! Each of these was written out verbatim in two or more suites here. None of
//! them is that suite's subject -- they are how the suite gets to its subject
//! -- so a change to one (the lock that makes the migration race-free, the
//! Redis client the probes share) reached only the copy somebody happened to
//! be editing, and every copy stayed green.

use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Raise `stop` after `after`, so a held reader can be given a lifespan.
///
/// Both suites that hold a lock open need the same end: a reader that never
/// lets go turns a case that should fail into one that never returns, and a
/// case that never returns reports nothing. The two copies this replaces
/// differed by a doc comment, which is exactly the shape the shared-steps gate
/// is for -- the day one of them learned to hold across a statement boundary,
/// the other would still have been testing the old rule.
pub fn release_after(stop: Arc<AtomicBool>, after: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        stop.store(true, Ordering::SeqCst);
    });
}

/// Apply this crate's migrations to `url`, one caller at a time.
///
/// `CREATE TABLE IF NOT EXISTS` statements race on a fresh database -- the
/// loser hits `pg_type_typname_nsp_index`. The advisory lock makes the DDL
/// mutually exclusive: the second caller waits for the first and then finds
/// every version already applied. It is held on a connection separate from the
/// migrator's own pool, so it stays held while the migrator runs.
///
/// `lock` is the caller's: a suite that already holds one number must not take
/// a second one, or two lock identities could be used as if they were one.
pub async fn migrate_once(url: &str, lock: i64) {
    let pool = PgPool::connect(url)
        .await
        .expect("connect to take the lock");
    let mut conn = pool.acquire().await.expect("acquire the schema lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock)
        .execute(&mut *conn)
        .await
        .expect("take the schema lock");
    cog_storage::migrate::Migrator::new(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .run(url)
        .await
        .expect("apply the migrations to the throwaway database");
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock)
        .execute(&mut *conn)
        .await
        .expect("release the schema lock");
}

/// A Redis connection the caller did not configure: the one this machine's
/// throwaway instance is at, through the same client the product uses.
pub async fn raw_conn() -> redis::aio::ConnectionManager {
    let client = redis::Client::open(cog_test_support::redis_url()).expect("redis url");
    cog_storage::redis::connect(&client)
        .await
        .expect("connection")
}

/// How many rows `table` holds.
pub async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Drop a scratch table the case created, whether or not the case got to the
/// end of its own assertions.
pub async fn drop_probe(pool: &PgPool, table: &str) {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(pool)
        .await
        .unwrap();
}

/// Delete the ledger rows a case filed under `marker`.
///
/// The marker is a slug prefix, so a case's own rows are named by the case and
/// a case clearing up after itself cannot reach a row it did not write.
pub async fn cleanup_ledger(pool: &PgPool, marker: &str) {
    for table in ["principle_ledger", "principle_ledger_declared"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE slug LIKE $1"))
            .bind(format!("{marker}/%"))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("clean up {table}: {e}"));
    }
}
