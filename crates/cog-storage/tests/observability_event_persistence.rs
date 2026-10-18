//! Whether an event published **through the trait** reaches the table.
//!
//! The gateway had two methods named `publish_event`: an inherent one that sent
//! to the in-process broadcast and wrote a row, and the trait arm that only
//! sent. Production holds the gateway as `Arc<dyn ObservabilityGateway>`, so it
//! called the arm that does not write, and `cog_observability_events` stayed
//! empty for as long as the pod ran with nothing failing: the send succeeded,
//! no warning fired, and the empty table read as "no events happened".
//!
//! A test that calls the concrete type cannot see this. It has to publish the
//! way the caller does -- through the trait object -- and then read the row
//! back, which is why this file needs a server:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test observability_event_persistence -- --ignored
//! ```
//!
//! The table is the backend's own (`init_schema`), the marker is per-run, and
//! the row is deleted at the end so the database is left as it was found.

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway};
use cog_storage::PostgresObservabilityGateway;
use sqlx::{PgPool, Row};

fn database_url() -> String {
    std::env::var("COGNEVA_TEST_DATABASE_URL").expect(
        "set COGNEVA_TEST_DATABASE_URL to a throwaway PostgreSQL database before \
         running these tests",
    )
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn an_event_published_through_the_trait_reaches_the_table() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let gateway = PostgresObservabilityGateway::new(pool.clone());
    gateway.init_schema().await.expect("init the schema");

    let marker = format!(
        "event-persist-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let event = AgentEvent::TurnStart {
        agent_id: marker.clone(),
        timestamp: Utc::now(),
    };

    // Through the trait object: this is the call production makes, and the
    // whole point is that it is not the same method as the concrete one was.
    let gateway: &dyn ObservabilityGateway = &gateway;
    gateway.publish_event(event);

    // The write is spawned so the caller does not block on the database; wait
    // for it instead of racing it, and fail with the count rather than a panic
    // from a missing row.
    let mut rows = 0i64;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        rows =
            sqlx::query("SELECT count(*) AS n FROM cog_observability_events WHERE agent_id = $1")
                .bind(&marker)
                .fetch_one(&pool)
                .await
                .expect("read the row back")
                .get::<i64, _>("n");
        if rows > 0 {
            break;
        }
    }
    assert_eq!(
        rows, 1,
        "publishing through `dyn ObservabilityGateway` left no row: the trait arm \
         does not persist, so every caller of the gateway writes nothing"
    );

    sqlx::query("DELETE FROM cog_observability_events WHERE agent_id = $1")
        .bind(&marker)
        .execute(&pool)
        .await
        .expect("clean up the test row");
}
