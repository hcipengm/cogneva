use std::sync::Arc;
use std::time::Duration;

use cog_core::MultiBackendConsumerConfig;
use cog_supervisor::MultiBackendEventConsumer;
use cog_test_support::MemoryMessageBackend;
use tokio::sync::broadcast;

/// The group the backend is asked to subscribe under is the group the config
/// names. A literal in the constructor would be reachable by nobody: changing
/// `multi_backend_consumer.group` would leave the consumer in the old group,
/// and two deployments told to use different groups would share one.
#[tokio::test]
async fn changing_the_configured_group_changes_the_group_the_backend_sees() {
    let first = subscribed_group("group-a").await;
    let second = subscribed_group("group-b").await;

    assert_eq!(first, vec!["group-a".to_string()]);
    assert_eq!(second, vec!["group-b".to_string()]);
    assert_ne!(
        first, second,
        "the consumer group has to follow the config, not a fixed name"
    );
}

/// Starts one consumer on its own channel and waits until the backend has
/// recorded the subscription. The channel is part of the config too, so each
/// call observes only its own consumer.
async fn subscribed_group(group: &str) -> Vec<String> {
    let channel = format!("events-{group}");
    let backend = Arc::new(MemoryMessageBackend::new());
    let (event_tx, _event_rx) = broadcast::channel(8);
    let config = MultiBackendConsumerConfig {
        channel: channel.clone(),
        group: group.to_string(),
        ..Default::default()
    };

    let handle = MultiBackendEventConsumer::from_config(backend.clone(), event_tx, &config).spawn();

    let mut seen = Vec::new();
    for _ in 0..200 {
        seen = backend.subscribed_groups(&channel);
        if !seen.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.abort();

    seen
}
