use std::sync::Arc;

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway};
use tokio::sync::broadcast;
use tokio::sync::Mutex;

use crate::error::SupervisorResult;

/// Statistics emitted by the [`EventAggregator`] every aggregation
/// window.
#[derive(Debug, Clone, Default)]
pub struct EventAggregatorStats {
    /// Number of events received during the last aggregation pass.
    pub received: u64,
    /// Number of events successfully republished to the gateway.
    pub republished: u64,
    /// Number of events dropped due to channel lag.
    pub dropped: u64,
}

/// Aggregates AgentEvents from the runtime broadcast channel and
/// republishes them via the [`ObservabilityGateway`].
/// This component bridges the Agent broadcast channel (a high-volume
/// pub/sub stream consumed by HTTP handlers, the Web UI, and the
/// memory ingestor) with the durable observability layer.  It does
/// not block the broadcast publisher: if the receiver lags it will
/// drop events and report them in the aggregator stats.
pub struct EventAggregator {
    receiver: Mutex<broadcast::Receiver<AgentEvent>>,
    gateway: Option<Arc<dyn ObservabilityGateway>>,
}

impl EventAggregator {
    pub fn new(receiver: broadcast::Receiver<AgentEvent>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            gateway: None,
        }
    }

    pub fn with_gateway(mut self, gateway: Arc<dyn ObservabilityGateway>) -> Self {
        self.gateway = Some(gateway);
        self
    }

    /// Drain the broadcast receiver up to `max_events` and forward each
    /// one to the configured gateway.  Non-blocking — returns immediately
    /// once the receiver yields `Empty`.
    pub async fn drain(&self, max_events: usize) -> SupervisorResult<EventAggregatorStats> {
        let mut stats = EventAggregatorStats::default();
        let mut rx = self.receiver.lock().await;

        for _ in 0..max_events {
            match rx.try_recv() {
                Ok(event) => {
                    stats.received += 1;
                    if let Some(ref gateway) = self.gateway {
                        gateway.publish_event(event);
                        stats.republished += 1;
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Closed) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    stats.dropped = stats.dropped.saturating_add(skipped);
                }
            }
        }
        Ok(stats)
    }

    /// Block (with a small wait timeout) for events on the broadcast
    /// channel.  Used by the Supervisor's main `tokio::select!` loop
    /// so the aggregator runs continuously.
    pub async fn next_event(&self) -> Option<AgentEvent> {
        let mut rx = self.receiver.lock().await;
        rx.recv().await.ok()
    }

    /// Convenience publisher used internally to record aggregated stats
    /// alongside other events.
    pub fn build_supervisor_event(
        stats: &EventAggregatorStats,
        window_seconds: u64,
    ) -> Option<cog_core::SupervisorEvent> {
        if stats.received == 0 {
            return None;
        }
        Some(cog_core::SupervisorEvent::EventAggregated {
            count: stats.received,
            window_seconds,
            timestamp: Utc::now(),
        })
    }
}
