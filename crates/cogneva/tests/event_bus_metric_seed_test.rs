//! `cogneva_event_bus_emitted_total` is written only when the executor offers a
//! task event, and the alert rule that pairs it with the consumer's counter
//! reads both sides through `increase()`. That function returns nothing over a
//! window holding no samples, so on the scrape a kind that has never been
//! offered leaves no series at all -- and an absent series looks exactly like a
//! series that was never wired. The two answer different questions: "this
//! deployment has been idle" and "this counter is not connected". These tests
//! pin the boot-time seed that separates them, because the difference is
//! invisible from every reader otherwise.

use std::collections::HashSet;
use std::sync::Arc;

use cog_core::metric_names::EVENT_BUS_EMITTED_TOTAL;
use cog_core::MetricsBackend;
use cog_orchestrator::DagExecutor;

use crate::common;

use common::mocks::MockMetricsBackend;

/// Every kind the executor can offer is written as zero at boot, so a process
/// that has offered nothing reads as a row of zeros rather than as the absence
/// a build without the counter would show.
#[tokio::test]
async fn seeding_writes_every_kind_at_zero() {
    let recorder = Arc::new(MockMetricsBackend::new());
    let metrics: Arc<dyn MetricsBackend> = recorder.clone();

    let executor = DagExecutor::new("ws-seed".into());
    executor.attach_metrics(metrics);
    executor.seed_event_bus_kinds().await;

    let calls = recorder.calls_for(EVENT_BUS_EMITTED_TOTAL.as_str());
    assert_eq!(
        calls.len(),
        DagExecutor::TASK_EVENT_KINDS.len(),
        "a kind was left unseeded: its series would be missing from a scrape of \
         an executor that has not offered anything yet"
    );
    assert!(
        calls.iter().all(|c| c.value == 0.0),
        "the seed moved a total instead of creating a series at zero"
    );

    let seeded: HashSet<&str> = calls
        .iter()
        .map(|c| c.labels.get("kind").map(String::as_str).unwrap_or(""))
        .collect();
    let expected: HashSet<&str> = DagExecutor::TASK_EVENT_KINDS.into_iter().collect();
    assert_eq!(
        seeded, expected,
        "the seeded labels are not the closed set: a reader looking for a kind \
         by name would not find it"
    );
}

/// An executor with no metrics backend seeds nothing and does not fail: the
/// reading the rule needs is absent because nothing could record it, which is
/// a deployment shape rather than a fault, and a boot must not stop over it.
#[tokio::test]
async fn an_executor_with_no_backend_seeds_nothing_and_survives() {
    let executor = DagExecutor::new("ws-noseed".into());
    executor.seed_event_bus_kinds().await;
}
