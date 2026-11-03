//! The hop from the memory plugin down to the refusal sweep.
//!
//! The sweep can be exercised on its own, but the defects that matter here live
//! one level up: code that is all there and nothing runs it, and a pass whose
//! only reading is "removed 0" so a dead wiring looks exactly like a clean
//! namespace. A test that drives `sweep_namespace` directly passes in both
//! cases. So this drives the plugin's real `init` + `start`, plants a row that
//! only this loop would take away, and waits for it to go — with a real lesson
//! planted beside it to show the loop is not simply emptying the namespace.
//!
//! It lives in its own test binary on purpose: it sets `COGNEVA_CONFIG_PATH`,
//! which is process-global.

use cog_core::{
    MemoryBackend, MetricsBackend, ObjectBackend, RawSource, SchemaEntry, SchemaKind, SourceRef,
    SystemPlugin,
};
use std::sync::Arc;

fn planted(id: &str, details: &str) -> (RawSource, SchemaEntry) {
    let payload = serde_json::json!({ "id": id, "summary": id, "details": details });
    let raw = RawSource::new(
        id,
        "reflection",
        cog_core::CONTENT_TYPE_JSON,
        serde_json::to_vec(&payload).unwrap(),
    );
    let mut entry = SchemaEntry::new(
        id,
        "reflection",
        SchemaKind::Learning,
        id,
        id,
        SourceRef::new(id, "test"),
    );
    entry.properties = payload;
    (raw, entry)
}

#[tokio::test]
async fn memory_plugin_start_wires_the_refusal_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cogneva.json");
    std::fs::write(
        &path,
        r#"{
            "memory": {
                "enabled": true,
                "backend_type": "composite",
                "embedding_dimension": 8,
                "auto_ingest": false,
                "maintenance": {
                    "decay_interval_secs": 0,
                    "refusal_sweep_interval_secs": 1,
                    "refusal_sweep_namespaces": ["reflection"]
                }
            }
        }"#,
    )
    .unwrap();
    std::env::set_var("COGNEVA_CONFIG_PATH", &path);

    let mut config = cog_core::Config::default();
    // There is no PostgreSQL here; non-strict lets the summary/schema layers
    // fall back to their in-memory defaults, which is enough to reach the
    // sweep-loop wiring.
    config.system.strict_persistence = false;
    let ctx = cog_core::PluginContext::new(config);
    ctx.publish_service::<dyn ObjectBackend>(Arc::new(cog_storage::MemoryObjectBackend::new()));
    let metrics = Arc::new(cog_observability::metrics::PrometheusMetricsBackend::new(
        "",
    ));
    ctx.publish_service::<dyn MetricsBackend>(metrics.clone());

    let mut plugin = cog_memory::plugin::MemoryPlugin::new();
    plugin.init(&ctx).await.unwrap();
    plugin.start(&ctx).await.unwrap();

    // The plugin publishes its own backend; writing through that handle is the
    // only way to seed the store the loop actually reads, and it also pins the
    // premise that the loop and the rest of the platform share one backend.
    let backend = ctx
        .consume_service::<dyn MemoryBackend>()
        .expect("MemoryPlugin::start publishes the backend it hands to the sweep");

    let noise = planted(
        "LRN-noise",
        "upstream_unavailable: environment_error: LLM upstream refused (server_error): HTTP 429",
    );
    let real = planted("LRN-real", "evaluator: criteria unmet — no test artifacts");
    for (raw, entry) in [&noise, &real] {
        backend.archive_raw(raw).await.unwrap();
        backend.store_schema("reflection", entry).await.unwrap();
    }

    // The first pass lands one period after start, so poll rather than race the
    // timer. One second per pass and ten seconds of room: this only waits long
    // enough to tell "wired" from "never runs", not to measure the cadence.
    let mut swept = false;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if backend
            .get_schema("reflection", "LRN-noise")
            .await
            .unwrap()
            .is_none()
        {
            swept = true;
            break;
        }
    }
    assert!(
        swept,
        "MemoryPlugin::start did not wire the refusal sweep; the rows the \
         write side stopped producing would stay in the store forever"
    );

    assert!(
        backend
            .get_raw("reflection", "LRN-noise")
            .await
            .unwrap()
            .is_none(),
        "the archive must go with the row: leaving it lets the repair loop \
         rebuild what the sweep just removed"
    );
    assert!(
        backend
            .get_schema("reflection", "LRN-real")
            .await
            .unwrap()
            .is_some(),
        "a lesson about the work must survive the sweep"
    );

    // The removal has to be readable afterwards: a pass whose only trace is a
    // log line cannot be watched from outside the process.
    let totals = metrics
        .query_counter_totals(cog_core::metric_names::MEMORY_DROPPED_ENTRIES_TOTAL.as_str())
        .await
        .unwrap();
    let removed: f64 = totals
        .iter()
        .filter(|s| s.labels.get("namespace").map(String::as_str) == Some("reflection"))
        .map(|s| s.value)
        .sum();
    assert!(
        removed >= 1.0,
        "the sweep took a row out but wrote no reading for it; got {totals:?}"
    );
}
