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

/// The consumer is not bounded by what one pass could carry. A burst larger
/// than any per-pass cap is taken whole, because the ceiling a cap imposes is
/// what lets the ring overflow between two passes.
#[tokio::test]
async fn a_burst_larger_than_one_pass_is_taken_whole() {
    const BURST: usize = 4096;
    let (tx, rx) = broadcast::channel(8 * BURST);
    let backend: Arc<dyn cog_core::StateBackend> = Arc::new(MemoryStateBackend::new());
    let gateway = Arc::new(MemoryObservabilityGateway::new(backend));
    let aggregator = EventAggregator::new(rx).with_gateway(gateway.clone());

    for i in 0..BURST {
        let _ = tx.send(AgentEvent::AgentStart {
            agent_id: format!("a-{i}"),
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        });
    }

    let stop = cog_core::ShutdownSignal::new();
    assert!(
        aggregator.wait_and_drain(&stop).await,
        "the channel is open, so the consumer keeps running"
    );
    let stats = aggregator.take_stats().await;
    assert_eq!(
        stats.received, BURST as u64,
        "every event the ring held must be taken, not 1024 of them"
    );
    assert_eq!(stats.republished, BURST as u64);
}

/// Taking and reporting run on two rhythms, so the totals the report reads are
/// the ones the consumer accumulated — and taking them starts the next window
/// rather than handing the same numbers out twice.
#[tokio::test]
async fn the_window_reports_what_the_consumer_accumulated() {
    let (tx, rx) = broadcast::channel(64);
    let aggregator = EventAggregator::new(rx);
    let stop = cog_core::ShutdownSignal::new();

    for round in 0..2 {
        for i in 0..3 {
            let _ = tx.send(AgentEvent::AgentStart {
                agent_id: format!("r{round}-a{i}"),
                crew_id: None,
                squad_id: None,
                timestamp: chrono::Utc::now(),
            });
        }
        assert!(aggregator.wait_and_drain(&stop).await);
    }

    let first = aggregator.take_stats().await;
    assert_eq!(first.received, 6, "both rounds land in one window");
    let second = aggregator.take_stats().await;
    assert_eq!(
        second.received, 0,
        "the window starts empty once it has been reported"
    );
}

/// A stopping process ends the consuming loop instead of holding it open: the
/// wait is on the channel, which may never yield again.
#[tokio::test]
async fn a_triggered_stop_ends_the_consumer() {
    let (_tx, rx) = broadcast::channel(8);
    let aggregator = EventAggregator::new(rx);
    let stop = cog_core::ShutdownSignal::new();
    stop.trigger();
    assert!(
        !aggregator.wait_and_drain(&stop).await,
        "a stopping process must not keep waiting on an idle channel"
    );
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
