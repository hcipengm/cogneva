//! The hop from the memory plugin down to the decay loop.
//!
//! The decay loop can be exercised on its own, but the defect it fixes was a
//! promise with no caller: the code was all there and nothing ran it. A test
//! that drives `spawn_decay_loop` directly would still pass if `MemoryPlugin`
//! never called it — which is the exact shape of the original bug one level up.
//! So this drives the plugin's real `init` + `start` and reads the loop's own
//! counter back.
//!
//! It lives in its own test binary on purpose: it sets `COGNEVA_CONFIG_PATH`,
//! which is process-global, and a file of its own keeps that from racing the
//! config reads of any other test in the same process.

use cog_core::{MetricsBackend, ObjectBackend, SystemPlugin};
use std::sync::Arc;

#[tokio::test]
async fn memory_plugin_start_wires_the_decay_loop() {
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
                    "decay_interval_secs": 1,
                    "decay_age_threshold_secs": 0,
                    "decay_importance_threshold": 1.0,
                    "decay_namespaces": ["default"]
                }
            }
        }"#,
    )
    .unwrap();
    std::env::set_var("COGNEVA_CONFIG_PATH", &path);

    let mut config = cog_core::Config::default();
    // There is no PostgreSQL here; non-strict lets the summary/schema layers
    // fall back to their in-memory defaults, which is enough to reach the
    // decay-loop wiring.
    config.system.strict_persistence = false;
    let ctx = cog_core::PluginContext::new(config);
    // The composite backend needs an object backend; nothing else is mandatory
    // for `start` to reach the decay-loop wiring (metrics/vector fall back).
    ctx.publish_service::<dyn ObjectBackend>(Arc::new(cog_storage::MemoryObjectBackend::new()));
    let metrics = Arc::new(cog_observability::metrics::PrometheusMetricsBackend::new(
        "",
    ));
    ctx.publish_service::<dyn MetricsBackend>(metrics.clone());

    let mut plugin = cog_memory::plugin::MemoryPlugin::new();
    plugin.init(&ctx).await.unwrap();
    plugin.start(&ctx).await.unwrap();

    // With an empty summary layer every pass is idle; the loop's first sweep
    // lands one period after start, so poll rather than race the timer.
    let mut saw_idle = false;
    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let totals = metrics
            .query_counter_totals(cog_core::metric_names::MEMORY_DECAY_TOTAL.as_str())
            .await
            .unwrap();
        if totals
            .iter()
            .any(|s| s.labels.get("outcome").map(String::as_str) == Some("idle"))
        {
            saw_idle = true;
            break;
        }
    }
    assert!(
        saw_idle,
        "MemoryPlugin::start did not wire the decay loop; the documented \
         automatic decay would never run"
    );
}
