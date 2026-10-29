//! Which open rows belong to one producer, against a live PostgreSQL.
//!
//! The alert mirror decides what to close by subtraction: the rows the store
//! says it owns, minus the keys this pass read from Prometheus. Both ends of
//! that subtraction are server decisions. `labels->>'source'` is a JSONB
//! lookup, and a lookup that silently matched nothing would make the
//! subtraction empty — the mirror would report a healthy reconciliation on
//! every tick while every row it ever opened stayed `firing`. That is not a
//! hypothetical: 2026-10-23 measured six such rows, each frozen at the same
//! `last_seen_at`, one of them naming a Pod that had been deleted hours
//! earlier.
//!
//! The three shapes below are the ones a DB-free test cannot see: a row is
//! selected by a label it carries in JSON, a row of another producer is not
//! selected by the same query, and a resolved row is not selected at all —
//! closing a row is what "no longer firing" looks like in this table, so
//! returning resolved rows would make the mirror re-announce recoveries.
//!
//! Ignored by default and needs `COGNEVA_TEST_DATABASE_URL` pointing at a
//! throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-observability --test alert_mirror_source -- --ignored
//! ```
//!
//! Every run gets its own schema and drops it at the end, so the statements'
//! unqualified `alerts` name cannot reach a table another run is using.

use cog_observability::alert_store::{AlertTransition, NewAlert, PostgresAlertStore};

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

fn alert(rule: &str, key: &str, source: Option<&str>) -> NewAlert {
    let labels = match source {
        Some(source) => serde_json::json!({ "source": source }),
        None => serde_json::json!({}),
    };
    NewAlert {
        rule: rule.to_string(),
        dedup_key: key.to_string(),
        severity: "warning".to_string(),
        message: rule.to_string(),
        labels,
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn only_the_firing_rows_of_the_named_source_are_returned() {
    let url = database_url();
    let schema = own_schema(&url, "alert_mirror").await;

    let pool = schema_pool(&url, &schema).await;
    let store = PostgresAlertStore::new(pool.clone());
    let schema_runner = cog_test_support::SchemaTextRunner::new(pool.clone());
    store
        .init_schema(&schema_runner)
        .await
        .expect("the alert DDL runs in one piece");

    let mirrored = alert(
        "KubePodCrashLooping",
        "KubePodCrashLooping:pod=a",
        Some("prometheus_alerts"),
    );
    let in_process = alert("llm_pool_down", "llm_pool_down", Some("infra_watch"));
    let unlabelled = alert("legacy_rule", "legacy_rule:x", None);
    store
        .set_alert(true, &mirrored)
        .await
        .expect("the mirrored row is written");
    store
        .set_alert(true, &in_process)
        .await
        .expect("the in-process row is written");
    store
        .set_alert(true, &unlabelled)
        .await
        .expect("the row without a source is written");

    let mut keys = store
        .firing_keys_from_source("prometheus_alerts")
        .await
        .expect("the open rows are read");
    keys.sort();
    assert_eq!(
        keys,
        vec![mirrored.dedup_key.clone()],
        "the mirror has to own exactly its own rows: another producer's row \
         would otherwise be closed by the mirror's next pass, and a row with no \
         source label at all is not the mirror's to close either"
    );

    // Resolving a row removes it from the open set: the mirror closing a row
    // and then reading it back as still open would send the same recovery
    // notice on every tick.
    assert_eq!(
        store
            .set_alert(false, &mirrored)
            .await
            .expect("the row is resolved"),
        AlertTransition::Resolved
    );
    let keys = store
        .firing_keys_from_source("prometheus_alerts")
        .await
        .expect("the open rows are read again");
    assert!(
        keys.is_empty(),
        "a resolved row is not one the mirror still owns"
    );

    drop_schema(&url, &schema).await;
}
