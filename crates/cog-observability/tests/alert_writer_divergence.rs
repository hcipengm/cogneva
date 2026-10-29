//! Two writers on one alert table, against a live PostgreSQL.
//!
//! The application pod and the evolution pod both run the alert bridge against
//! the same `alerts` table. Each keeps its own view of every rule, and the row
//! keeps only the last writer's verdict -- so when the two disagree, the table
//! shows whichever evaluated last and nothing shows that a second process was
//! saying the opposite. These tests pin the data that makes that state
//! readable: the row names its last writer, and every writer's own verdict is
//! kept separately, tagged with the kind of process that reached it.
//!
//! What the readings do with those two facts lives in
//! `scripts/host/cogneva-alert-writer-divergence-reading.sh`, whose own test
//! owns the judgement. This file owns the write path: a verdict missing here is
//! a disagreement nobody can count, whatever the reader does.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test alert_writer_divergence -- --ignored
//! ```
//!
//! Every run gets its own schema and drops it at the end.

use cog_observability::alert_store::{AlertTransition, NewAlert, PostgresAlertStore};
use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};
use sqlx::{PgPool, Row};

fn probe_alert(dedup_key: &str) -> NewAlert {
    NewAlert {
        rule: "probe_rule".to_string(),
        dedup_key: dedup_key.to_string(),
        severity: "warning".to_string(),
        message: "probe".to_string(),
        labels: serde_json::json!({}),
    }
}

/// The `writer` column as the table currently reads it.
async fn row_writer(pool: &PgPool, dedup_key: &str) -> Option<String> {
    sqlx::query("SELECT writer FROM alerts WHERE dedup_key = $1")
        .bind(dedup_key)
        .fetch_one(pool)
        .await
        .expect("the alert row is readable")
        .get("writer")
}

/// One writer's verdict as the assertion table holds it: `(role, state)`.
async fn verdict(pool: &PgPool, dedup_key: &str, writer: &str) -> Option<(String, String)> {
    sqlx::query("SELECT role, state FROM alert_assertions WHERE dedup_key = $1 AND writer = $2")
        .bind(dedup_key)
        .bind(writer)
        .fetch_optional(pool)
        .await
        .expect("the assertion table is readable")
        .map(|row| (row.get("role"), row.get("state")))
}

async fn assertion_rows(pool: &PgPool, writer: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM alert_assertions WHERE writer = $1")
        .bind(writer)
        .fetch_one(pool)
        .await
        .expect("the assertion table is readable")
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn two_writers_leave_two_verdicts_and_the_row_names_the_last() {
    let url = database_url();
    let schema = own_schema(&url, "alert_div_two").await;
    let pool = schema_pool(&url, &schema).await;

    let app = PostgresAlertStore::with_identity(pool.clone(), "cogneva-app-0", "cogneva");
    let evolution =
        PostgresAlertStore::with_identity(pool.clone(), "cogneva-evolution-0", "cogneva-evolution");
    app.init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the alert DDL runs in one piece");

    let alert = probe_alert("probe_rule:disagree");
    assert_eq!(
        app.set_alert(true, &alert)
            .await
            .expect("the app pod fires"),
        AlertTransition::Fired
    );
    assert_eq!(
        row_writer(&pool, &alert.dedup_key).await.as_deref(),
        Some("cogneva-app-0"),
        "a freshly fired row says which process wrote it"
    );
    assert_eq!(
        evolution
            .set_alert(false, &alert)
            .await
            .expect("the evolution pod resolves"),
        AlertTransition::Resolved
    );
    assert_eq!(
        row_writer(&pool, &alert.dedup_key).await.as_deref(),
        Some("cogneva-evolution-0"),
        "the row names its last writer, so a verdict change can be attributed"
    );

    assert_eq!(
        verdict(&pool, &alert.dedup_key, "cogneva-app-0").await,
        Some(("cogneva".to_string(), "firing".to_string())),
        "the overwritten verdict survives here, tagged with the kind of process \
         that reached it -- this is the only place it survives at all"
    );
    assert_eq!(
        verdict(&pool, &alert.dedup_key, "cogneva-evolution-0").await,
        Some(("cogneva-evolution".to_string(), "resolved".to_string())),
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn one_writer_keeps_one_verdict_per_key() {
    let url = database_url();
    let schema = own_schema(&url, "alert_div_one").await;
    let pool = schema_pool(&url, &schema).await;

    let app = PostgresAlertStore::with_identity(pool.clone(), "cogneva-app-0", "cogneva");
    app.init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the alert DDL runs in one piece");

    let first = probe_alert("probe_rule:mind");
    let second = probe_alert("probe_rule:other");
    app.set_alert(true, &first).await.expect("it fires");
    app.set_alert(false, &first).await.expect("it resolves");
    app.set_alert(true, &first).await.expect("it fires again");
    app.set_alert(true, &second)
        .await
        .expect("a second key fires");

    assert_eq!(
        assertion_rows(&pool, "cogneva-app-0").await,
        2,
        "one verdict per key, not one per evaluation: this table is bounded by \
         the alert table's cardinality, not by the tick rate"
    );
    assert_eq!(
        verdict(&pool, &first.dedup_key, "cogneva-app-0").await,
        Some(("cogneva".to_string(), "firing".to_string())),
        "the verdict is the latest one, so a writer that changed its mind reads \
         as its present opinion and not as its first"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_rollout_overlap_carries_one_role_on_both_bodies() {
    let url = database_url();
    let schema = own_schema(&url, "alert_div_roll").await;
    let pool = schema_pool(&url, &schema).await;

    let old = PostgresAlertStore::with_identity(pool.clone(), "cogneva-old", "cogneva");
    let new = PostgresAlertStore::with_identity(pool.clone(), "cogneva-new", "cogneva");
    old.init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the alert DDL runs in one piece");

    let alert = probe_alert("probe_rule:rolled");
    old.set_alert(true, &alert)
        .await
        .expect("the old pod fires");
    new.set_alert(false, &alert)
        .await
        .expect("the new pod resolves");

    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role FROM alert_assertions WHERE dedup_key = $1 ORDER BY writer",
    )
    .bind(&alert.dedup_key)
    .fetch_all(&pool)
    .await
    .expect("the assertion table is readable");
    assert_eq!(
        roles,
        vec!["cogneva".to_string(), "cogneva".to_string()],
        "two bodies of one deployment share the role: a reader grouping by role \
         sees one writer handing over, not two writers disagreeing"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_writer_that_only_refreshes_still_counts_as_asserting() {
    let url = database_url();
    let schema = own_schema(&url, "alert_div_refresh").await;
    let pool = schema_pool(&url, &schema).await;

    let app = PostgresAlertStore::with_identity(pool.clone(), "cogneva-app-0", "cogneva");
    let evolution =
        PostgresAlertStore::with_identity(pool.clone(), "cogneva-evolution-0", "cogneva-evolution");
    app.init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the alert DDL runs in one piece");

    let alert = probe_alert("probe_rule:held");
    app.set_alert(true, &alert)
        .await
        .expect("the app pod fires");
    assert!(
        evolution
            .refresh_firing(&alert.dedup_key)
            .await
            .expect("the row is held up"),
        "a firing row can be held up by any writer"
    );
    assert_eq!(
        verdict(&pool, &alert.dedup_key, "cogneva-evolution-0").await,
        Some(("cogneva-evolution".to_string(), "firing".to_string())),
        "holding a row up is a verdict: a writer that only ever refreshes has \
         to appear in the assertions, or a disagreement it has would be hidden \
         behind its own missing row"
    );

    // A refresh that matches nothing is not a verdict about anything: the row
    // was closed by someone else, and recording `firing` for it would invent an
    // opinion this writer never had.
    let closed = probe_alert("probe_rule:closed");
    app.set_alert(true, &closed).await.expect("the app fires");
    app.set_alert(false, &closed).await.expect("the app closes");
    assert!(!evolution
        .refresh_firing(&closed.dedup_key)
        .await
        .expect("the refresh runs"),);
    assert_eq!(
        verdict(&pool, &closed.dedup_key, "cogneva-evolution-0").await,
        None,
        "a refresh that touched no row left no verdict behind"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_writer_identity_lands_on_a_table_that_predates_it() {
    let url = database_url();
    let schema = own_schema(&url, "alert_div_ddl").await;
    let pool = schema_pool(&url, &schema).await;

    // The DDL is one multi-statement string: a malformed statement in the
    // middle stops every statement after it, and an empty-schema CREATE hides
    // that entirely because it already carries the new shape. Building the old
    // shape first is the only way to witness the ALTER and the CREATE TABLE.
    sqlx::query(
        "CREATE TABLE alerts (
             id          TEXT PRIMARY KEY,
             rule        TEXT        NOT NULL,
             dedup_key   TEXT        NOT NULL UNIQUE,
             severity    TEXT        NOT NULL,
             state       TEXT        NOT NULL,
             message     TEXT        NOT NULL,
             labels      JSONB       NOT NULL DEFAULT '{}'::jsonb,
             fired_at    TIMESTAMPTZ NOT NULL,
             resolved_at TIMESTAMPTZ,
             updated_at  TIMESTAMPTZ NOT NULL,
             last_seen_at TIMESTAMPTZ,
             acknowledged_at TIMESTAMPTZ,
             acknowledged_by TEXT,
             notified_at TIMESTAMPTZ
         )",
    )
    .execute(&pool)
    .await
    .expect("the legacy alert table is created");

    let store = PostgresAlertStore::with_identity(pool.clone(), "cogneva-app-0", "cogneva");
    store
        .init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the alert DDL runs in one piece");

    let column: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'alerts' \
           AND column_name = 'writer')",
    )
    .fetch_one(&pool)
    .await
    .expect("the catalog is readable");
    assert!(column, "the writer column has to land on the legacy table");

    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'alert_assertions' \
         ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .expect("the catalog is readable");
    assert_eq!(
        columns,
        vec![
            "dedup_key".to_string(),
            "observed_at".to_string(),
            "role".to_string(),
            "state".to_string(),
            "writer".to_string(),
        ],
        "the per-writer verdict table has to be created with every column the \
         reading selects"
    );

    let alert = probe_alert("probe_rule:legacy");
    store
        .set_alert(true, &alert)
        .await
        .expect("a write on the legacy table");
    assert_eq!(
        assertion_rows(&pool, "cogneva-app-0").await,
        1,
        "the verdict table is wired into the write path, not just present"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}

/// A verdict table that stopped keeping up must not take the alert with it.
///
/// `set_alert` returns one value, and every caller reads `Err` as "nothing was
/// written" — they skip delivery on that branch and keep the firing row from
/// being announced. So a failure in the auxiliary verdict write must not be
/// reported as a failure of the alert: the caller cannot tell the two apart,
/// and the one it would act on is the wrong one. Measured on the cluster on
/// 2026-10-30: a missing column in the verdict table cost seven minutes in
/// which no outward notification left the process while `alerts` kept moving.
///
/// The drift is made the way it happens in production — the column exists when
/// the schema is applied and goes missing afterwards — rather than by writing a
/// table the DDL would repair on the next start.
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn a_broken_verdict_table_does_not_fail_the_alert_it_describes() {
    let url = database_url();
    let schema = own_schema(&url, "alert_div_drift").await;
    let pool = schema_pool(&url, &schema).await;

    let store = PostgresAlertStore::with_identity(pool.clone(), "cogneva-app-0", "cogneva");
    store
        .init_schema(&cog_test_support::SchemaTextRunner::new(pool.clone()))
        .await
        .expect("the alert DDL runs in one piece");

    sqlx::query("ALTER TABLE alert_assertions DROP COLUMN role")
        .execute(&pool)
        .await
        .expect("the verdict table loses a column after the fact");

    let alert = probe_alert("probe_rule:drift");
    let transition = store
        .set_alert(true, &alert)
        .await
        .expect("a broken verdict write is not the alert's failure");
    assert_eq!(
        transition,
        AlertTransition::Fired,
        "the caller has to be able to announce an alert whose row landed"
    );
    assert_eq!(
        row_writer(&pool, &alert.dedup_key).await.as_deref(),
        Some("cogneva-app-0"),
        "the alert row itself is written before the verdict is attempted"
    );
    assert_eq!(
        assertion_rows(&pool, "cogneva-app-0").await,
        0,
        "the verdict really was lost — this case is about not claiming otherwise"
    );

    // The same for the paths that keep a row up and close it: both are called
    // with the transition's answer in hand, and a lost verdict there would
    // otherwise read as "this writer no longer owns the row".
    assert!(
        store
            .refresh_firing(&alert.dedup_key)
            .await
            .expect("a refresh is about the alert row, not the verdict"),
        "the refresh touched the row it was asked about"
    );
    assert_eq!(
        store
            .set_alert(false, &alert)
            .await
            .expect("a close is about the alert row, not the verdict"),
        AlertTransition::Resolved,
        "the row was open and this writer closes it"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}
