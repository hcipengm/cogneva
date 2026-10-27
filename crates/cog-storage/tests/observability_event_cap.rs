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

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use cog_storage::EventLogCap;
use sqlx::{PgPool, Row};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

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
async fn a_table_over_capacity_is_pruned_oldest_first() {
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
        outcome.removed, 200,
        "the two hundred old rows are the ones that go"
    );
    assert_eq!(outcome.held, 100, "the table comes back to its budget");
    assert!(
        !outcome.window_held,
        "nothing was refused: the capacity was reachable"
    );
    assert_eq!(scratch.count(&pool).await, 100);

    scratch.drop(&pool).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_window_that_alone_exceeds_the_budget_is_reported_and_not_crossed() {
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
        outcome.removed, 200,
        "every row outside the window is still deletable"
    );
    assert_eq!(outcome.held, 100, "the window's rows are all still there");
    assert!(
        outcome.window_held,
        "a table the window holds over budget must say so, not report the capacity met"
    );
    assert_eq!(
        scratch.count(&pool).await,
        100,
        "the window is the floor: not one row inside it may go"
    );

    scratch.drop(&pool).await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_row_exactly_at_the_window_edge_survives() {
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

    assert_eq!(outcome.removed, 1, "only the row outside the window goes");
    assert_eq!(outcome.held, 1);
    assert!(
        !outcome.window_held,
        "one row at the capacity is not a table held over it"
    );

    scratch.drop(&pool).await;
}
