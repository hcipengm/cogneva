//! What `get_events` hands back for `Event::payload`, against a live PostgreSQL.
//!
//! The column `cog_events.payload` holds the *serialized event*, not its
//! payload: `append_event` writes `serde_json::to_value(event)`, so the event's
//! own payload sits one level in, under the envelope's own `payload` key. The
//! read has to undo exactly that much. It did not: it put the column straight
//! into `Event::payload`, so a reader got the envelope back -- `event.payload`
//! was the whole event, and the thing it asked for was `event.payload.payload`.
//!
//! Nothing noticed, because until now no Rust code read a task's events back.
//! The two sibling backends are the tell: Redis round-trips the event through
//! `serde_json` and the in-memory one stores the `Event` itself, so both hand
//! back the payload -- the same event through `get_events` meant two different
//! things depending on which backend the process was wired with, and the one
//! that diverged is the one production runs.
//!
//! This is the first reader's precondition rather than a detail of it: a reader
//! that scans an event's payload for a field (the change ledger reads back what
//! a handover produced) would find nothing under Postgres and everything under
//! the in-memory backend its own tests run against, which is a green test over a
//! reading that is empty in production.
//!
//! The rows go into the real `cog_events` table under a `task_id` prefix of
//! their own, and each test deletes the task it wrote, so the database is left
//! as it was found.
//!
//! Ignored by default, and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test events_payload_round_trip -- --ignored
//! ```

use sqlx::PgPool;

use cog_core::{Event, StateBackend};
use cog_storage::{MemoryStateBackend, PostgresStateBackend};

/// The task this binary writes, and the events it appends to it.
const TASK: &str = "events-payload-probe-round-trip";

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

/// The payload an event carries. Distinctive in both directions: a reader that
/// returned the envelope would not equal it, and it carries a nested object so
/// a read that flattened a level would not equal it either.
fn payload() -> serde_json::Value {
    serde_json::json!({ "probe": "round-trip", "nested": { "deep": 1 } })
}

fn event() -> Event {
    Event {
        // Assigned by the backend when it appends.
        offset: 0,
        task_id: TASK.to_string(),
        event_type: "probe_event".to_string(),
        payload: payload(),
        timestamp: chrono::Utc::now(),
    }
}

async fn cleanup(pool: &PgPool) {
    sqlx::query("DELETE FROM cog_events WHERE task_id = $1")
        .bind(TASK)
        .execute(pool)
        .await
        .unwrap();
}

/// The same event, appended and read back through both backends, is the same
/// event -- payload included.
///
/// The pair is the point rather than two tests: one answer written twice by two
/// implementations is what makes "the payload came back" a property of the
/// contract instead of one backend's habit. A test that only ran the Postgres
/// half would go on being green if the in-memory half drifted the other way,
/// which is exactly the state this is written out of.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_payload_read_back_is_the_payload_appended_on_both_backends() {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    let backend = PostgresStateBackend::new(pool.clone());
    backend.init_schema().await.unwrap();
    cleanup(&pool).await;

    let sent = event();

    // A writer of its own, so what is read back is a row the database holds
    // rather than a value still in this process's hands.
    let pg = PostgresStateBackend::new(PgPool::connect(&database_url()).await.unwrap());
    pg.append_event(TASK, &sent).await.unwrap();

    let memory = MemoryStateBackend::new();
    memory.append_event(TASK, &sent).await.unwrap();

    let postgres = pg.get_events(TASK, 0, 10).await.unwrap();
    let memory = memory.get_events(TASK, 0, 10).await.unwrap();

    for (name, got) in [("postgres", &postgres), ("memory", &memory)] {
        assert_eq!(got.len(), 1, "{name}: one event was appended");
        let got = &got[0];
        assert_eq!(
            got.payload,
            payload(),
            "{name}: the payload read back is the payload appended, not the envelope it is \
             stored inside"
        );
        assert_eq!(got.event_type, "probe_event", "{name}: event_type");
        assert_eq!(got.task_id, TASK, "{name}: task_id");
    }

    // What the two do not agree on, and therefore what this test does not pin as
    // a contract: the `offset` a read-back event carries. Postgres takes it from
    // the row's position; the in-memory and Redis backends hand back whatever
    // the appending caller passed, which is zero, because for them the position
    // is the return value and not a field. Both facts are asserted rather than
    // left unsaid, so that settling it later has to be a deliberate act: the
    // read's own `offset` argument is in the same state, compared against a
    // position by Postgres and used to index a list by the other two, and no
    // reader asks a page for its positions today.
    assert_eq!(postgres[0].offset, 1, "the position the row holds");
    assert_eq!(memory[0].offset, 0, "the value the appender passed");

    cleanup(&pool).await;
}
