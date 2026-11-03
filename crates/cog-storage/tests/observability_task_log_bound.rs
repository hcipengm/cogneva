//! What a task's log looks like once it has written more lines than it keeps,
//! against a live PostgreSQL.
//!
//! The route this table answers (`/api/v1/tasks/{id}/logs`) is read in tasks:
//! one task id, the newest N. The table is append-only, so without a bound its
//! size is the platform's uptime times its run rate — the shape the event log
//! already had to be bounded for. The writer therefore trims the task it just
//! wrote to, and this file is what says the trim took the old lines and left
//! the new ones.
//!
//! Three of those claims cannot be read off an unbounded table and one cannot
//! be read off a bounded one that trims the wrong end:
//!
//! - the count comes back at the bound rather than one short of it, which is
//!   what a `DELETE` sharing a statement with its `INSERT` produces — one
//!   statement cannot see its own CTE's row and takes the newest line too;
//! - the line that survives is the newest one, not the oldest;
//! - a second task keeps its own lines: the bound is per task, so a busy task
//!   cannot push a quiet one's only line out.
//!
//! Needs `COGNEVA_TEST_DATABASE_URL` pointing at a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test observability_task_log_bound -- --ignored
//! ```
//!
//! The table is the backend's own (`init_schema`), because the trim under test
//! is the one production runs. Every row carries a per-run marker and is
//! deleted at the end, so the database is left as it was found.

use chrono::{Duration, Utc};
use cog_core::{LogEntry, ObservabilityGateway};
use cog_storage::{PostgresObservabilityGateway, LOG_LINES_PER_TASK};
use sqlx::{PgPool, Row};

use cog_test_support::database_url;

/// One line, ordered by `n` against a base the caller fixes.
///
/// The base is passed in rather than read per line: two lines written inside
/// the same microsecond would order by the insert order instead of by `n`, and
/// then "the newest one survived" would be a claim about the clock rather than
/// about the trim.
fn line(base: chrono::DateTime<Utc>, n: i64) -> LogEntry {
    LogEntry {
        timestamp: base + Duration::milliseconds(n),
        level: "info".into(),
        source: "log-bound-test".into(),
        message: format!("line {n}"),
        metadata: serde_json::json!({ "n": n }),
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_task_keeps_only_its_newest_lines_and_a_second_task_keeps_its_own() {
    let url = database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    let gateway = PostgresObservabilityGateway::new(pool.clone());
    gateway.init_schema().await.expect("init the schema");

    let marker = format!(
        "log-bound-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let busy = format!("{marker}:busy");
    let quiet = format!("{marker}:quiet");
    let written = LOG_LINES_PER_TASK + 5;
    let base = Utc::now();

    gateway
        .record_task_log(&quiet, line(base, 0))
        .await
        .expect("record the quiet task's only line");

    for n in 0..written {
        gateway
            .record_task_log(&busy, line(base, n))
            .await
            .expect("record a line");
    }

    let kept: i64 = sqlx::query("SELECT COUNT(*) FROM cog_observability_logs WHERE task_id = $1")
        .bind(&busy)
        .fetch_one(&pool)
        .await
        .expect("count the task's lines")
        .get(0);
    assert_eq!(
        kept, LOG_LINES_PER_TASK,
        "a task holds its bound, not one under it and not one over"
    );

    let newest = gateway
        .get_task_logs(&busy, LOG_LINES_PER_TASK as usize)
        .await
        .expect("read the task's lines");
    assert_eq!(newest.len() as i64, LOG_LINES_PER_TASK);
    assert_eq!(
        newest[0].message,
        format!("line {}", written - 1),
        "the reader asks for the newest first and gets the newest first"
    );
    assert!(
        !newest.iter().any(|row| row.message == "line 0"),
        "the oldest lines are the ones the trim took"
    );

    let quiet_lines = gateway
        .get_task_logs(&quiet, 10)
        .await
        .expect("read the quiet task's lines");
    assert_eq!(
        quiet_lines.len(),
        1,
        "a busy task's overflow must not reach into another task's log"
    );
    assert_eq!(quiet_lines[0].message, "line 0");

    sqlx::query("DELETE FROM cog_observability_logs WHERE task_id LIKE $1")
        .bind(format!("{marker}%"))
        .execute(&pool)
        .await
        .expect("remove the probe rows");
}
