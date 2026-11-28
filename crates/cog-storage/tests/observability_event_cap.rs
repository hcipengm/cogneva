//! What the event-log sweep deletes, and what it must never delete.
//!
//! Two claims, and the second is the one a reading of the first cannot check:
//!
//! - past the capacity, the oldest rows go first, so a table over budget comes
//!   back under it;
//! - no row inside the read window goes, whatever the capacity says. A sweep
//!   that treated the capacity as the whole rule would answer the historical
//!   events view's own query with a silent gap during a storm — the rows would
//!   not be late, they would not exist, and "nothing happened then" reads
//!   exactly like "we trimmed it".
//!
//! Both need a server, so both are ignored by default:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test observability_event_cap -- --ignored
//! ```
//!
//! The test makes its own uniquely named table, shaped like the real one in the
//! columns the sweep reads, and drops it at the end. It never touches
//! `cog_observability_events` itself.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use cog_storage::EventLogCap;
use sqlx::{PgPool, Row};

use cog_test_support::database_url;

/// A table shaped like the event log in the two columns the sweep reads.
struct Scratch {
    name: String,
}

impl Scratch {
    async fn new(pool: &PgPool) -> Self {
        let name = format!(
            "cog_obs_event_cap_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        sqlx::query(&format!(
            "CREATE TABLE {name} (
                 id        SERIAL PRIMARY KEY,
                 payload   JSONB NOT NULL DEFAULT '{{}}',
                 timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
             )"
        ))
        .execute(pool)
        .await
        .expect("create the scratch table");
        Self { name }
    }

    /// Insert `count` rows, all at `age` before now.
    async fn fill(&self, pool: &PgPool, count: usize, age: ChronoDuration) {
        let at = Utc::now() - age;
        for _ in 0..count {
            sqlx::query(&format!(
                "INSERT INTO {name} (timestamp) VALUES ($1)",
                name = self.name
            ))
            .bind(at)
            .execute(pool)
            .await
            .expect("insert a scratch row");
        }
    }

    async fn count(&self, pool: &PgPool) -> i64 {
        sqlx::query(&format!("SELECT count(*) AS n FROM {}", self.name))
            .fetch_one(pool)
            .await
            .expect("count the scratch table")
            .get::<i64, _>("n")
    }

    async fn drop(self, pool: &PgPool) {
        sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.name))
            .execute(pool)
            .await
            .expect("drop the scratch table");
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_table_over_capacity_is_reported_and_nothing_is_deleted() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let scratch = Scratch::new(&pool).await;
    scratch.fill(&pool, 200, ChronoDuration::hours(3)).await;
    scratch.fill(&pool, 100, ChronoDuration::minutes(30)).await;

    let cap = EventLogCap::new(pool.clone(), 100)
        .with_table(scratch.name.clone())
        .with_read_window(Duration::from_secs(3_600));
    let outcome = cap.sweep_once().await.expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "a pass deletes nothing: the repair is to move the rows out, not to drop them"
    );
    assert_eq!(outcome.held, 300, "every row written is still there");
    assert!(
        outcome.window_held,
        "the pass has to say the table is over its reading, or the excess is invisible"
    );
    assert_eq!(scratch.count(&pool).await, 300);

    scratch.drop(&pool).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_window_that_alone_exceeds_the_reading_is_reported() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let scratch = Scratch::new(&pool).await;
    scratch.fill(&pool, 200, ChronoDuration::hours(3)).await;
    scratch.fill(&pool, 100, ChronoDuration::minutes(30)).await;

    // The capacity is below what the window holds, so reaching it would mean
    // deleting rows a reader may still ask for. Stopping is the answer, and
    // the verdict is what makes the stopping legible.
    let cap = EventLogCap::new(pool.clone(), 50)
        .with_table(scratch.name.clone())
        .with_read_window(Duration::from_secs(3_600));
    let outcome = cap.sweep_once().await.expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "no row is deletable at any capacity: the log is the fact, and a fact cannot be trimmed"
    );
    assert_eq!(outcome.held, 300, "every row written is still there");
    assert!(
        outcome.window_held,
        "a table the window holds over budget must say so, not report the capacity met"
    );
    assert_eq!(
        scratch.count(&pool).await,
        300,
        "not one row goes, inside the window or outside it"
    );

    scratch.drop(&pool).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn every_row_survives_whatever_the_capacity_says() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let scratch = Scratch::new(&pool).await;
    // One row just outside the window, one just inside it.
    scratch.fill(&pool, 1, ChronoDuration::seconds(3_700)).await;
    scratch.fill(&pool, 1, ChronoDuration::seconds(3_500)).await;

    let cap = EventLogCap::new(pool.clone(), 1)
        .with_table(scratch.name.clone())
        .with_read_window(Duration::from_secs(3_600));
    let outcome = cap.sweep_once().await.expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "nothing is deleted, inside the window or out"
    );
    assert_eq!(outcome.held, 2);
    assert!(
        outcome.window_held,
        "two rows over a capacity of one is a table over its reading"
    );

    scratch.drop(&pool).await;
}

/// 一趟不做删除，只看一眼，因此它也只打一拍——这一拍让「这趟还在跑」在读数面
/// 看得见，而不用等它跑完。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_pass_reports_progress_and_deletes_nothing() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let scratch = Scratch::new(&pool).await;
    sqlx::query(&format!(
        "INSERT INTO {name} (timestamp) \
         SELECT NOW() - (g || ' minutes')::interval \
         FROM generate_series(1, 200) AS g",
        name = scratch.name
    ))
    .execute(&pool)
    .await
    .expect("fill the scratch table");

    let beats = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&beats);
    let cap = EventLogCap::new(pool.clone(), 1)
        .with_table(scratch.name.clone())
        .with_read_window(Duration::from_secs(1));
    let outcome = cap
        .sweep_once_beating(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("sweep");

    assert_eq!(
        outcome.removed, 0,
        "nothing is deleted, whatever the capacity says"
    );
    assert_eq!(outcome.held, 200);
    assert!(
        outcome.window_held,
        "the pass says the table is over its reading"
    );
    assert!(
        beats.load(Ordering::Relaxed) >= 1,
        "the pass beats while it works"
    );

    scratch.drop(&pool).await;
}

/// 归档这条路的判据：校验通过才删；任何一步不成立，那一批一行都不删。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_batch_is_deleted_only_after_its_copy_reads_back_identical() {
    use cog_storage::observability_event_cap::{archive_batch, ArchivedEvent};
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let scratch = ArchiveScratch::new(&pool).await;
    scratch.fill(&pool, 3).await;

    let backend: std::sync::Arc<dyn cog_core::ObjectBackend> =
        std::sync::Arc::new(cog_test_support::MemoryObjectBackend::new());
    let outcome = archive_batch(
        &pool,
        &scratch.name,
        &backend,
        "events/archive",
        chrono::Utc::now() - ChronoDuration::hours(1),
        10,
    )
    .await
    .expect("archive")
    .expect("three rows to archive");

    assert_eq!(outcome.archived, 3);
    assert_eq!(outcome.deleted, 3, "the copy verified, so the rows may go");
    assert_eq!(scratch.count(&pool).await, 0);
    let stored = backend
        .get(&outcome.key)
        .await
        .expect("the object is readable")
        .expect("the object exists");
    let lines: Vec<&[u8]> = stored
        .split(|byte| *byte == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(lines.len(), 3, "one line per event");
    for line in lines {
        let event: ArchivedEvent = serde_json::from_slice(line).expect("a line reads as an event");
        assert!(!event.event_type.is_empty());
    }

    scratch.drop(&pool).await;
}

/// 对象存储不可写时一行都不删：先落副本、再删原件，顺序不能反。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn nothing_is_deleted_when_the_copy_cannot_be_written() {
    use cog_storage::observability_event_cap::archive_batch;
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let scratch = ArchiveScratch::new(&pool).await;
    scratch.fill(&pool, 2).await;

    let backend: std::sync::Arc<dyn cog_core::ObjectBackend> =
        std::sync::Arc::new(FailingPutBackend);
    let result = archive_batch(
        &pool,
        &scratch.name,
        &backend,
        "events/archive",
        chrono::Utc::now() - ChronoDuration::hours(1),
        10,
    )
    .await;
    assert!(result.is_err(), "a copy that cannot be written is an error");
    assert_eq!(
        scratch.count(&pool).await,
        2,
        "the rows stay: deleting them without a verified copy is the loss this shape exists to stop"
    );

    scratch.drop(&pool).await;
}

/// 一张与真表同形状的临时表：归档要读的是那些列，少一列就不是同一个问题。
struct ArchiveScratch {
    name: String,
}

impl ArchiveScratch {
    async fn new(pool: &PgPool) -> Self {
        let name = format!(
            "events_archive_test_{}",
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        sqlx::query(&format!(
            "CREATE TABLE {name} (
                 id         SERIAL PRIMARY KEY,
                 event_type TEXT NOT NULL DEFAULT 'MessageUpdate',
                 agent_id   TEXT,
                 task_id    TEXT,
                 crew_id    TEXT,
                 squad_id   TEXT,
                 payload    TEXT NOT NULL DEFAULT '{{}}',
                 timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW()
             )"
        ))
        .execute(pool)
        .await
        .expect("create the scratch table");
        Self { name }
    }

    async fn fill(&self, pool: &PgPool, count: usize) {
        for index in 0..count {
            sqlx::query(&format!(
                "INSERT INTO {name} (payload, timestamp) VALUES ($1, NOW() - make_interval(hours => 3))",
                name = self.name
            ))
            .bind(format!("{{\"n\":{index}}}"))
            .execute(pool)
            .await
            .expect("insert a scratch row");
        }
    }

    async fn count(&self, pool: &PgPool) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {}", self.name))
            .fetch_one(pool)
            .await
            .expect("count the scratch rows")
    }

    async fn drop(self, pool: &PgPool) {
        sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.name))
            .execute(pool)
            .await
            .expect("drop the scratch table");
    }
}

/// 写不进去的对象存储。
struct FailingPutBackend;

#[async_trait::async_trait]
impl cog_core::ObjectBackend for FailingPutBackend {
    async fn put(&self, _key: &str, _data: &[u8]) -> cog_core::SFResult<String> {
        Err(cog_core::SFError::IO("the archive is unreachable".into()))
    }
    async fn get(&self, _key: &str) -> cog_core::SFResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn delete(&self, _key: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn presign_url(&self, _key: &str, _expiry_secs: u64) -> cog_core::SFResult<String> {
        Ok(String::new())
    }
    async fn exists(&self, _key: &str) -> cog_core::SFResult<bool> {
        Ok(false)
    }
    async fn list(&self, _prefix: Option<&str>) -> cog_core::SFResult<Vec<String>> {
        Ok(Vec::new())
    }
}
