use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway};
use tokio::sync::broadcast;
use tokio::sync::Mutex;

use crate::error::SupervisorResult;

/// The loop that consumes the agent broadcast continuously.
///
/// Its value is the `consumer` label both loss counters carry, so the lag this
/// consumer is charged with and the loop charged with it read as one name.
pub const EVENT_AGGREGATOR_LOOP: &str = "supervisor_event_aggregator";

/// How long one drain round may run before it yields.
///
/// The bound is wall-clock rather than a count of events. A count is a
/// throughput ceiling — the pass that used to drive this consumer took at most
/// 1024 events once per window — and the ring behind it is only as deep as the
/// channel's capacity, so a producer can overflow it between two passes no
/// matter how cheap each event is to take. A deadline lets a burst run as long
/// as it stays cheap without letting it hold the wake-up, and it is what gives
/// the stop signal a chance to be observed while the queue is busy.
const DRAIN_BUDGET: Duration = Duration::from_millis(250);

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
    /// What the consumer took since the last window report.
    ///
    /// Taking and reporting run on two rhythms now: the consumer wakes on the
    /// channel, the report keeps the window's cadence. Without this the report
    /// could only ever count what arrived during its own pass, which is what
    /// made the pass double as the consumer.
    since_report: Mutex<EventAggregatorStats>,
}

impl EventAggregator {
    pub fn new(receiver: broadcast::Receiver<AgentEvent>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            gateway: None,
            since_report: Mutex::new(EventAggregatorStats::default()),
        }
    }

    pub fn with_gateway(mut self, gateway: Arc<dyn ObservabilityGateway>) -> Self {
        self.gateway = Some(gateway);
        self
    }

    /// Hand one event to the gateway, counting both halves.
    fn hand_on(&self, event: AgentEvent, stats: &mut EventAggregatorStats) {
        stats.received += 1;
        if let Some(ref gateway) = self.gateway {
            gateway.publish_event(event);
            stats.republished += 1;
        }
    }

    /// Add one round's counts to the totals the window report reads.
    async fn accumulate(&self, round: EventAggregatorStats) {
        let mut totals = self.since_report.lock().await;
        totals.received = totals.received.saturating_add(round.received);
        totals.republished = totals.republished.saturating_add(round.republished);
        totals.dropped = totals.dropped.saturating_add(round.dropped);
    }

    /// Take what the window has not reported yet, and start the next window.
    ///
    /// This is the report's half of the split: the consumer no longer reads the
    /// channel on the report's cadence, so the report has to read what the
    /// consumer left behind.
    pub async fn take_stats(&self) -> EventAggregatorStats {
        let mut totals = self.since_report.lock().await;
        std::mem::take(&mut *totals)
    }

    /// Wait for one event and then take everything already queued behind it.
    ///
    /// Returns false when the channel closed or the process is stopping, which
    /// is the consuming loop's cue to end. One blocking wait buys the wake-up;
    /// draining after it is what keeps a burst from paying one wake-up per
    /// event.
    pub async fn wait_and_drain(&self, stop: &cog_core::ShutdownSignal) -> bool {
        let mut round = EventAggregatorStats::default();
        let mut rx = self.receiver.lock().await;

        tokio::select! {
            biased;
            _ = stop.wait() => return false,
            received = rx.recv() => match received {
                Ok(event) => self.hand_on(event, &mut round),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    round.dropped = round.dropped.saturating_add(skipped);
                }
                Err(broadcast::error::RecvError::Closed) => return false,
            },
        }

        let deadline = Instant::now() + DRAIN_BUDGET;
        let mut open = true;
        while Instant::now() < deadline {
            match rx.try_recv() {
                Ok(event) => self.hand_on(event, &mut round),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    round.dropped = round.dropped.saturating_add(skipped);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    open = false;
                    break;
                }
            }
        }
        drop(rx);
        self.accumulate(round).await;
        open
    }

    /// Drain the broadcast receiver up to `max_events` and forward each
    /// one to the configured gateway.  Non-blocking — returns immediately
    /// once the receiver yields `Empty`.
    pub async fn drain(&self, max_events: usize) -> SupervisorResult<EventAggregatorStats> {
        let mut stats = EventAggregatorStats::default();
        let mut rx = self.receiver.lock().await;

        for _ in 0..max_events {
            match rx.try_recv() {
                Ok(event) => self.hand_on(event, &mut stats),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Closed) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    stats.dropped = stats.dropped.saturating_add(skipped);
                }
            }
        }
        Ok(stats)
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

/// Consume the agent broadcast for as long as this process runs.
///
/// Consuming and reporting are two rhythms. The report keeps the window's
/// cadence because it is a reading — one write per pass, a rate the store pays
/// once. The consumer wakes on the channel, because a consumer that only reads
/// on a timer has a throughput ceiling (1024 events per window) and the ring it
/// reads from is only as deep as the channel's capacity: on a busy platform the
/// producer refills it between two ticks and every event past the ring's depth
/// is gone, with a lag counter as the only trace. The loss is a consumer-side
/// fault, so the repair is here: wake on the channel, not on the clock.
pub async fn run_event_aggregator_loop(
    aggregator: Arc<EventAggregator>,
    stop: cog_core::ShutdownSignal,
    beat: cog_core::loop_health::Beat,
) {
    loop {
        // Every wake stamps, including the ones that find the queue empty: an
        // event-driven loop sits idle most of the time, and stamping only on an
        // event would report the queue's quiet as the loop's.
        beat.beat();
        if !aggregator.wait_and_drain(&stop).await {
            return;
        }
    }
}
