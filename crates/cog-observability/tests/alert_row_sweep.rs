//! Closing the alert rows a dead process left firing, against a live PostgreSQL.
//!
//! What this checks is the database's answer to one question: which firing rows
//! count as "nothing is refreshing this any more". Two halves, and both are
//! decided by the statement rather than by the caller being careful:
//!
//! - a row whose liveness clock has not moved for longer than the grace is
//!   closed, and one that moved recently is not — the predicate is the clock,
//!   not the rule name;
//! - a stale row of a rule that some live producer still refreshes is left
//!   alone. Closing by rule alone would take out the rows of every other
//!   producer that happens to share the name, and those rows have no process to
//!   re-raise them.
//!
//! Measured before this existed: 3992 firing `agent_unhealthy` rows on
//! 2026-10-24, the oldest untouched since 08:40 that morning, while sixteen
//! live agents refreshed theirs every ten seconds. The supervisor's registry is
//! in memory, so those rows described agents from an earlier life of the
//! process, and nothing would ever have named them again.
//!
//! It is ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test alert_row_sweep -- --ignored
//! ```
//!
//! Every row this file writes carries a per-run suffix in its dedup key and is
//! the only row of its kind, so the counts below are statements about this
//! test's rows.

use chrono::{Duration, Utc};
use sqlx::PgPool;

use cog_observability::alert_store::{NewAlert, PostgresAlertStore};

fn alert(rule: &str, dedup_key: &str) -> NewAlert {
    NewAlert {
        rule: rule.to_string(),
        dedup_key: dedup_key.to_string(),
        severity: "warning".into(),
        message: "the agent stopped beating".into(),
        labels: serde_json::json!({ "agent_id": dedup_key }),
    }
}

async fn state_of(pool: &PgPool, dedup_key: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT state FROM alerts WHERE dedup_key = $1")
        .bind(dedup_key)
        .fetch_one(pool)
        .await
        .expect("the row this test wrote must be readable")
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_row_no_live_pass_refreshes_is_closed_and_a_fresh_one_is_not() {
    let url = cog_test_support::database_url();
    let pool = PgPool::connect(&url)
        .await
        .expect("database must be reachable");
    let store = PostgresAlertStore::new(pool.clone());
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    store
        .init_schema(&schema_runner)
        .await
        .expect("schema must apply");

    let run = uuid::Uuid::new_v4().simple().to_string();
    let stale_agent = format!("agent_unhealthy:test-stale-{run}");
    let fresh_agent = format!("agent_unhealthy:test-fresh-{run}");
    let stale_other = format!("some_other_rule:test-stale-{run}");

    for (rule, key) in [
        ("agent_unhealthy", &stale_agent),
        ("agent_unhealthy", &fresh_agent),
        ("some_other_rule", &stale_other),
    ] {
        store
            .set_alert(true, &alert(rule, key))
            .await
            .expect("a firing row must land");
    }

    // Age the two rows that no pass has refreshed: an hour back is far past the
    // grace the bridge uses (ten health passes, ten seconds each by default).
    for key in [&stale_agent, &stale_other] {
        sqlx::query("UPDATE alerts SET last_seen_at = $2 WHERE dedup_key = $1")
            .bind(key)
            .bind(Utc::now() - Duration::hours(1))
            .execute(&pool)
            .await
            .expect("aging a row must land");
    }

    let closed = store
        .resolve_unrefreshed("agent_unhealthy", Utc::now() - Duration::minutes(10))
        .await
        .expect("the sweep must run");
    assert_eq!(
        closed, 1,
        "the sweep closed {closed} rows; exactly the one stale agent row is past the grace"
    );

    assert_eq!(
        state_of(&pool, &stale_agent).await,
        "resolved",
        "a row nothing has refreshed for an hour must be closed"
    );
    assert_eq!(
        state_of(&pool, &fresh_agent).await,
        "firing",
        "a row a live pass refreshed just now must be left alone"
    );
    assert_eq!(
        state_of(&pool, &stale_other).await,
        "firing",
        "the grace closes this rule's rows only; another producer's rows have nobody \
         to re-raise them"
    );

    // The other direction: a grace wider than the staleness closes nothing, so
    // "the sweep ran" and "the sweep judged" stay two statements.
    let closed = store
        .resolve_unrefreshed("agent_unhealthy", Utc::now() - Duration::hours(2))
        .await
        .expect("the sweep must run");
    assert_eq!(
        closed, 0,
        "a grace older than every row's last sighting must close nothing"
    );

    for key in [&stale_agent, &fresh_agent, &stale_other] {
        sqlx::query("DELETE FROM alerts WHERE dedup_key = $1")
            .bind(key)
            .execute(&pool)
            .await
            .expect("cleanup must land");
    }
}
