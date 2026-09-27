//! What one task's event sequence is, against a live PostgreSQL.
//!
//! The offset a reader resumes from is assigned by the backend, and it is the
//! position of an event rather than a copy of it: the sibling Redis backend
//! gets that for free, because its append returns the new list length, and no
//! contract says "unique when convenient". Postgres derives the same number
//! from `COUNT(*)`, which two writers can read before either has inserted — and
//! a task's owner is reclaimed on a lease, so the previous holder is presumed
//! stopped rather than stopped. Without a constraint on `(task_id, offset_num)`
//! the sequence can hand two events the same position, and a reader that
//! resumes from a position loses one of them for good.
//!
//! Three things are checked here. A position is handed out once — the table
//! refuses a second event at one the sequence already holds; appends that
//! overlap land on distinct positions, each with the number the row holds, and
//! none of them fails (retrying is not a substitute: appends keep pace with each
//! other, so a writer that only retries runs out of attempts and drops an
//! event); and a page costs the same read at the oldest and the newest position
//! of a task's history, which is what the same index is for.
//!
//! The rows are written into the real `cog_events` table under a `task_id`
//! prefix of their own, because the constraint under test is the one
//! `init_schema` creates: a probe table with a hand-made index would go on
//! passing after `init_schema` stopped creating it. The prefix is what the
//! cleanup deletes, so the database is left as it was found and a run that
//! failed midway does not leave rows a later run would count as history.
//!
//! Ignored by default, and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test events_offset_sequence -- --ignored
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;
use sqlx::{Connection, Executor, PgConnection, PgPool};

use cog_core::{Event, StateBackend};
use cog_storage::{PostgresStateBackend, EVENTS_PAGE_SQL};

/// Every task_id this binary writes.
const TASK_PREFIX: &str = "events-probe-";

/// Writers sharing one task, and appends each. The count and the insert are two
/// round trips, so one writer has no window at all and several writers are
/// exactly what the question is about.
const WRITERS: usize = 4;
const APPENDS: usize = 25;

/// Rows in the sequence the cost question reads: long enough that ranking the
/// task's whole history would be visible in the plan.
const HISTORY: i64 = 20_000;

/// The page every reading is taken with.
const PAGE: i64 = 10;

/// Held while the schema is put in place: `init_schema` creates its tables with
/// `IF NOT EXISTS`, and two sessions running that for the same table at once is
/// a race the server settles by failing one of them.
static SCHEMA_SETUP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// The backend, and the pool beside it for the readings taken of the table
/// itself.
async fn setup() -> (PostgresStateBackend, PgPool) {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let backend = PostgresStateBackend::new(pool.clone());
    {
        let _setup = SCHEMA_SETUP.lock().await;
        backend.init_schema().await.unwrap();
    }
    (backend, pool)
}

fn event(task_id: &str, event_type: &str) -> Event {
    Event {
        // Assigned by the backend when it appends.
        offset: 0,
        task_id: task_id.to_string(),
        event_type: event_type.to_string(),
        payload: serde_json::json!({ "probe": event_type }),
        timestamp: chrono::Utc::now(),
    }
}

/// The rows the task holds, and how many distinct positions they occupy: the
/// first counts the appends that landed, the second says whether any two landed
/// on one position.
async fn positions(pool: &PgPool, task_id: &str) -> (i64, i64) {
    sqlx::query_as("SELECT COUNT(*), COUNT(DISTINCT offset_num) FROM cog_events WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn cleanup(pool: &PgPool) {
    sqlx::query("DELETE FROM cog_events WHERE task_id LIKE $1")
        .bind(format!("{TASK_PREFIX}%"))
        .execute(pool)
        .await
        .unwrap();
}

/// The plan of the statement the backend reads a page with, built from the
/// producer's own text on a session of its own: a prepared statement does not
/// outlive its session, and a fresh one keeps the pool's connections out of it.
async fn explain_page(task: &str, offset: i64) -> Value {
    let mut conn = PgConnection::connect(&database_url()).await.unwrap();
    let prepared = format!("PREPARE probe_page(text, bigint, bigint) AS {EVENTS_PAGE_SQL}");
    conn.execute(prepared.as_str()).await.unwrap();
    // `EXPLAIN EXECUTE` takes the position and the bound as literals; the
    // statement being explained is still the one the backend runs.
    let explained = format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE probe_page('{task}', {offset}, {PAGE})"
    );
    let plan: Value = sqlx::query_scalar(explained.as_str())
        .fetch_one(&mut conn)
        .await
        .unwrap();
    plan[0]["Plan"].clone()
}

/// Every node of a plan, subplans included.
fn nodes(plan: &Value) -> Vec<&Value> {
    let mut found = Vec::new();
    walk(plan, &mut found);
    found
}

fn walk<'a>(node: &'a Value, out: &mut Vec<&'a Value>) {
    out.push(node);
    if let Some(children) = node.get("Plans").and_then(|v| v.as_array()) {
        for child in children {
            walk(child, out);
        }
    }
}

/// The blocks the whole plan touched, subplans included — a node's own
/// `Shared Hit Blocks` does not count the subplans hanging off it.
fn shared_blocks(plan: &Value) -> i64 {
    nodes(plan)
        .iter()
        .map(|n| {
            ["Shared Hit Blocks", "Shared Read Blocks"]
                .iter()
                .filter_map(|key| n.get(*key).and_then(Value::as_i64))
                .sum::<i64>()
        })
        .sum()
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_position_is_handed_out_once_and_the_writer_moves_on() {
    let (backend, pool) = setup().await;
    let task = format!("{TASK_PREFIX}positions");

    let mut given = Vec::new();
    for _ in 0..3 {
        given.push(
            backend
                .append_event(&task, &event(&task, "e"))
                .await
                .unwrap(),
        );
    }
    // The first append takes the first position, and the number the backend
    // returns is the one the row holds: it is the caller's resume point.
    assert_eq!(
        given,
        vec![1, 2, 3],
        "the sequence did not start at one and step by one"
    );

    // The same position again, asked for directly, in the shape the backend
    // writes it.
    let duplicate = sqlx::query(
        "INSERT INTO cog_events (task_id, event_type, payload, offset_num)
         VALUES ($1, 'duplicate', '{}'::jsonb, 1)",
    )
    .bind(&task)
    .execute(&pool)
    .await;

    let message = duplicate
        .expect_err(
            "the table took a second event at position 1: two events of one task then share a \
             position, and a reader resuming from it never sees the second",
        )
        .to_string();
    assert!(
        message.contains("idx_cog_events_task_offset") || message.contains("duplicate key"),
        "refused, but not by the sequence's constraint: {message}"
    );

    // And the append path is not stuck behind the row that got there first: the
    // next event takes the position after the last one the sequence holds.
    let next = backend
        .append_event(&task, &event(&task, "e"))
        .await
        .unwrap();
    assert_eq!(
        next, 4,
        "the append after a refused position did not take the next free one"
    );

    assert_eq!(
        positions(&pool, &task).await,
        (4, 4),
        "the sequence holds a repeated position"
    );

    cleanup(&pool).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn appends_that_overlap_land_on_distinct_positions() {
    let (backend, pool) = setup().await;
    let backend = Arc::new(backend);
    let task = format!("{TASK_PREFIX}overlap");

    // The writers start together, each appending on its own connection: with
    // the count and the insert two round trips apart, this is where a count can
    // be stale by the time its insert runs, which is the whole reason the
    // position has to be the table's to refuse.
    let mut writers = Vec::new();
    for _ in 0..WRITERS {
        let backend = backend.clone();
        let task = task.clone();
        writers.push(tokio::spawn(async move {
            let mut returned = Vec::new();
            for _ in 0..APPENDS {
                returned.push(
                    backend
                        .append_event(&task, &event(&task, "e"))
                        .await
                        .expect("an append failed, and a dropped event is not a reading"),
                );
            }
            returned
        }));
    }

    let mut all = Vec::new();
    for writer in writers {
        all.extend(writer.await.unwrap());
    }

    let expected = (WRITERS * APPENDS) as i64;
    let unique: HashSet<u64> = all.iter().copied().collect();
    assert_eq!(
        all.len() as i64,
        expected,
        "the writers did not all finish their appends"
    );
    assert_eq!(
        unique.len() as i64,
        expected,
        "{} appends landed on {} positions: the sequence handed one out twice",
        expected,
        unique.len()
    );
    assert_eq!(
        (*unique.iter().min().unwrap(), *unique.iter().max().unwrap()),
        (1, expected as u64),
        "the positions are not the first {expected} of the sequence"
    );

    let (rows, distinct) = positions(&pool, &task).await;
    assert_eq!(
        (rows, distinct),
        (expected, expected),
        "the stored rows do not hold {expected} distinct positions"
    );

    cleanup(&pool).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_page_costs_the_same_at_the_oldest_and_the_newest_position() {
    let (backend, pool) = setup().await;
    let task = format!("{TASK_PREFIX}page");

    sqlx::query(
        "INSERT INTO cog_events (task_id, event_type, payload, offset_num)
         SELECT $1, 'e', '{}'::jsonb, g FROM generate_series(1, $2) AS g",
    )
    .bind(&task)
    .bind(HISTORY)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("ANALYZE cog_events")
        .execute(&pool)
        .await
        .unwrap();

    // The answers first: the page is the window from the position asked for,
    // and nothing before it.
    let oldest_page = backend.get_events(&task, 1, PAGE as usize).await.unwrap();
    let offsets: Vec<u64> = oldest_page.iter().map(|e| e.offset).collect();
    assert_eq!(
        offsets,
        (1..=PAGE as u64).collect::<Vec<_>>(),
        "the page from the first position is not the first {PAGE} events"
    );
    let resume = HISTORY - PAGE + 1;
    let newest_page = backend
        .get_events(&task, resume as u64, PAGE as usize)
        .await
        .unwrap();
    let offsets: Vec<u64> = newest_page.iter().map(|e| e.offset).collect();
    assert_eq!(
        offsets,
        (resume as u64..=HISTORY as u64).collect::<Vec<_>>(),
        "the page from position {resume} is not the window that follows it"
    );

    // Then the plans those answers came from, at the two ends of the same
    // history: reaching a position is what a reader pays to resume.
    let oldest = explain_page(&task, 1).await;
    let newest = explain_page(&task, HISTORY - PAGE).await;

    for (position, plan) in [("the oldest", &oldest), ("the newest", &newest)] {
        let kinds: Vec<&str> = nodes(plan)
            .iter()
            .filter_map(|n| n.get("Node Type").and_then(Value::as_str))
            .collect();
        assert!(
            !kinds.contains(&"Sort"),
            "the page read {position} position sorts the task's history: {kinds:?}"
        );

        let indexes: Vec<&str> = nodes(plan)
            .iter()
            .filter_map(|n| n.get("Index Name").and_then(Value::as_str))
            .collect();
        assert!(
            indexes.contains(&"idx_cog_events_task_offset"),
            "the page read at {position} position does not follow the position index \
             (read {indexes:?})"
        );

        let answered: Vec<i64> = nodes(plan)
            .iter()
            .filter(|n| n.get("Node Type").and_then(Value::as_str) == Some("Limit"))
            .filter_map(|n| n.get("Actual Rows").and_then(Value::as_i64))
            .collect();
        assert_eq!(
            answered,
            vec![PAGE],
            "the page at {position} position did not return exactly its bound"
        );
    }

    // The same batch, the same rows, two positions in the history: a walk over
    // the task's events would grow with the position, a btree descent does not.
    let (oldest_blocks, newest_blocks) = (shared_blocks(&oldest), shared_blocks(&newest));
    assert!(
        newest_blocks <= oldest_blocks * 3,
        "reading at the newest position costs {newest_blocks} blocks against {oldest_blocks} at \
         the oldest one: the cost of a page follows the history rather than the position"
    );

    cleanup(&pool).await;
}
