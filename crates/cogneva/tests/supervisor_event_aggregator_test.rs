use cog_core::AgentEvent;
use cog_storage::{MemoryObservabilityGateway, MemoryStateBackend};
use cog_supervisor::event_aggregator::{EventAggregator, EventAggregatorStats};
use std::sync::Arc;
use tokio::sync::broadcast;

#[tokio::test]
async fn drain_collects_events_until_empty() {
    let (tx, rx) = broadcast::channel(64);
    let aggregator = EventAggregator::new(rx);

    for i in 0..3 {
        let _ = tx.send(AgentEvent::AgentStart {
            agent_id: format!("a-{i}"),
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        });
    }

    let stats = aggregator.drain(10).await.unwrap();
    assert_eq!(stats.received, 3);
}

#[tokio::test]
async fn drain_publishes_to_gateway() {
    let (tx, rx) = broadcast::channel(64);
    let backend: Arc<dyn cog_core::StateBackend> = Arc::new(MemoryStateBackend::new());
    let gateway = Arc::new(MemoryObservabilityGateway::new(backend));
    let aggregator = EventAggregator::new(rx).with_gateway(gateway.clone());

    for i in 0..2 {
        let _ = tx.send(AgentEvent::AgentStart {
            agent_id: format!("a-{i}"),
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        });
    }

    let stats = aggregator.drain(10).await.unwrap();
    assert_eq!(stats.received, 2);
    assert_eq!(stats.republished, 2);
}

#[tokio::test]
async fn build_supervisor_event_skips_empty_window() {
    let stats = EventAggregatorStats::default();
    assert!(EventAggregator::build_supervisor_event(&stats, 30).is_none());
}

#[tokio::test]
async fn build_supervisor_event_returns_aggregated() {
    let stats = EventAggregatorStats {
        received: 5,
        republished: 5,
        dropped: 0,
    };
    let ev = EventAggregator::build_supervisor_event(&stats, 30).unwrap();
    match ev {
        cog_core::SupervisorEvent::EventAggregated {
            count,
            window_seconds,
            ..
        } => {
            assert_eq!(count, 5);
            assert_eq!(window_seconds, 30);
        }
        _ => panic!("unexpected variant"),
    }
}
