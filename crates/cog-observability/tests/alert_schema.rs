//! The alert table's notification mark, against a live PostgreSQL.
//!
//! `notified_at` is what turns the bell from an edge hook into a queue, and all
//! three of its faces are the server's: the column has to exist after the DDL
//! runs on a table that predates it, a firing row without it has to come back
//! from the queue query, and a new firing episode has to clear it so the second
//! episode is announced too. The DDL itself is one multi-statement string, so a
//! malformed statement anywhere inside it stops every statement before it from
//! landing -- which is exactly how the column was missing in production while
//! the code that writes it compiled and every DB-free test passed.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test alert_schema -- --ignored
//! ```
//!
//! Every run gets its own schema and drops it at the end, so the statements'
//! unqualified `alerts` name cannot reach a table another run is using.

use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection, PgPool};

use cog_observability::alert_store::{AlertTransition, NewAlert, PostgresAlertStore};

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
    PgPool::connect_with(options)
        .await
        .expect("the throwaway database is reachable")
}

fn probe_alert() -> NewAlert {
    NewAlert {
        rule: "probe_rule".to_string(),
        dedup_key: "probe_rule:probe".to_string(),
        severity: "warning".to_string(),
        message: "probe".to_string(),
        labels: serde_json::json!({}),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_mark_lands_and_a_new_episode_clears_it() {
    let url = database_url();
    let schema = format!("alert_schema_{}", uuid::Uuid::new_v4().simple());

    let mut admin = PgConnection::connect(&url)
        .await
        .expect("the throwaway database is reachable");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is created");

    let pool = schema_pool(&url, &schema).await;
    let store = PostgresAlertStore::new(pool.clone());
    store
        .init_schema()
        .await
        .expect("the alert DDL runs in one piece");

    let column: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'alerts' \
           AND column_name = 'notified_at')",
    )
    .fetch_one(&pool)
    .await
    .expect("the catalog is readable");
    assert!(
        column,
        "the DDL has to land the notification mark on a table that predates it"
    );

    let alert = probe_alert();
    assert_eq!(
        store
            .set_alert(true, &alert)
            .await
            .expect("the alert is written"),
        AlertTransition::Fired
    );
    let pending = store
        .unnotified_firing(10)
        .await
        .expect("the queue is read");
    assert_eq!(
        pending.len(),
        1,
        "a firing alert nobody was told about is exactly what the queue returns"
    );
    assert_eq!(pending[0].dedup_key, alert.dedup_key);

    assert!(
        store
            .mark_notified(&alert.dedup_key)
            .await
            .expect("the mark is written"),
        "the first mark lands on the unnotified row"
    );
    assert!(
        !store
            .mark_notified(&alert.dedup_key)
            .await
            .expect("the second mark is written"),
        "a second mark matches nothing, which is what keeps a re-run from \
         counting as a new delivery"
    );
    assert!(
        store
            .unnotified_firing(10)
            .await
            .expect("the queue is read")
            .is_empty(),
        "the announced alert has left the queue"
    );

    assert_eq!(
        store
            .set_alert(false, &alert)
            .await
            .expect("the alert resolves"),
        AlertTransition::Resolved
    );
    assert_eq!(
        store
            .set_alert(true, &alert)
            .await
            .expect("the alert fires again"),
        AlertTransition::Fired
    );
    assert_eq!(
        store
            .unnotified_firing(10)
            .await
            .expect("the queue is read")
            .len(),
        1,
        "a new episode is a new thing to announce: the mark belongs to the \
         episode, not to the row that carries it across episodes"
    );

    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&mut admin)
        .await
        .expect("the probe schema is dropped");
}
