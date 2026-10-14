use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Statistics for the Supervisor's autonomous queues.
#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub pending_handoffs: usize,
    pub retry_count: usize,
    pub dlq_count: usize,
    /// Ready tasks — the denominator of the supervisor's load ratio. Read
    /// straight from the orchestrator; without it a reader sees the pending
    /// handoffs but not what they are being measured against.
    pub ready_tasks: usize,
    /// The supervisor's last health-check cycle, or `0` before the first tick.
    pub cycle: u64,
    /// The last autonomous scale-out recommendation seen on the supervisor's
    /// event channel, or `None` if none has been broadcast since startup.
    ///
    /// `None` is ambiguous by itself: the pass sends nothing both when the
    /// load was within bounds and when there were no ready tasks (its early
    /// return), so "no recommendation" cannot tell the two apart. Use `cycle`
    /// to tell "no pass has run yet" from "a pass ran and recommended nothing".
    pub scale_recommendation: Option<ScaleRecommendation>,
    pub timestamp: DateTime<Utc>,
}

/// The most recent autonomous scale-out recommendation the gateway has seen.
///
/// A view of [`cog_core::SupervisorEvent::ScaleRecommendation`] (the event
/// carries `timestamp`; here it is `decided_at`), cached rather than
/// recomputed — recomputing the ratio at read time would make the endpoint a
/// second place that decides whether the load is high, and the two could
/// disagree with the decision actually taken.
///
/// Read it as **the last event seen**, not as a count: the broadcast channel
/// drops the oldest events when a slow reader falls behind, so this must not
/// be read as "how many times scale-out was recommended". It also does not
/// mean the load is over the threshold *now* — the supervisor sends the event
/// only while the ratio is high, so a stale recommendation stays here after
/// the load drops. A reader that needs "is it high right now" must look at the
/// age via `decided_at`.
#[derive(Debug, Clone, Serialize)]
pub struct ScaleRecommendation {
    pub recommended_agents: u32,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
}

/// Gateway-side cache of the supervisor's event channel.
///
/// Three facts are read from the broadcast and nowhere else:
/// - the last [`ScaleRecommendation`],
/// - the last [`cog_core::SupervisorEvent::Tick`]'s `cycle`, which the
///   supervisor loop already broadcasts — its contract exposes no cycle
///   accessor, so the tick is the only source, and
/// - when the last rebalance plan was emitted, from the last
///   [`cog_core::SupervisorEvent::Rebalance`].
#[derive(Debug, Default)]
pub struct SupervisorEventCache {
    last_scale_recommendation: Mutex<Option<ScaleRecommendation>>,
    /// `0` reads as "no tick seen yet": the supervisor's cycle starts at 0 and
    /// its first tick carries 1.
    cycle: AtomicU64,
    last_plan_emitted_at: Mutex<Option<DateTime<Utc>>>,
}

impl SupervisorEventCache {
    /// Fold one event from the supervisor's channel into the cache.
    pub fn observe(&self, event: &cog_core::SupervisorEvent) {
        match event {
            cog_core::SupervisorEvent::ScaleRecommendation {
                reason,
                recommended_agents,
                timestamp,
            } => {
                *self
                    .last_scale_recommendation
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(ScaleRecommendation {
                    recommended_agents: *recommended_agents,
                    reason: reason.clone(),
                    decided_at: *timestamp,
                });
            }
            cog_core::SupervisorEvent::Tick { cycle, .. } => {
                self.cycle.store(*cycle, Ordering::Relaxed);
            }
            cog_core::SupervisorEvent::Rebalance { timestamp, .. } => {
                *self
                    .last_plan_emitted_at
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(*timestamp);
            }
            _ => {}
        }
    }

    /// The last scale-out recommendation seen, if any.
    pub fn scale_recommendation(&self) -> Option<ScaleRecommendation> {
        self.last_scale_recommendation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The last tick's cycle, or `0` before the first tick.
    pub fn cycle(&self) -> u64 {
        self.cycle.load(Ordering::Relaxed)
    }

    /// When the last rebalance plan was emitted, or `None` before any.
    ///
    /// This reads the last [`cog_core::SupervisorEvent::Rebalance`]. That event
    /// has two producers: the rebalance pass, when the plan it built is
    /// non-empty, and the health pass, when a checkpoint recovery for dead
    /// agents produced one. Both carry the same meaning for their `timestamp`
    /// ("a plan went out now"), so this is "when did a plan last go out".
    ///
    /// It is deliberately not named after the control plane's
    /// `SupervisorStatus.last_rebalance`, which reads a mutex set on every
    /// successful rebalance pass (each `rebalance_interval`, 300s) and so
    /// answers "when did the pass last run clean". The two are different facts,
    /// so they carry different names; do not read one as the other.
    pub fn last_plan_emitted_at(&self) -> Option<DateTime<Utc>> {
        *self
            .last_plan_emitted_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

/// Current state of the scheduler gate.
#[derive(Debug, Clone, Serialize)]
pub struct GateStatus {
    pub paused: bool,
    pub timestamp: DateTime<Utc>,
}

/// Last known control plane report status.
#[derive(Debug, Clone, Serialize)]
pub struct ControlPlaneStatus {
    pub enabled: bool,
    pub last_report: Option<DateTime<Utc>>,
    pub last_success: Option<bool>,
    pub endpoint: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(cycle: u64) -> cog_core::SupervisorEvent {
        cog_core::SupervisorEvent::Tick {
            timestamp: Utc::now(),
            cycle,
        }
    }

    fn scale(recommended_agents: u32, reason: &str) -> cog_core::SupervisorEvent {
        cog_core::SupervisorEvent::ScaleRecommendation {
            reason: reason.to_string(),
            recommended_agents,
            timestamp: Utc::now(),
        }
    }

    fn rebalance(at: DateTime<Utc>) -> cog_core::SupervisorEvent {
        cog_core::SupervisorEvent::Rebalance {
            ready_tasks: 1,
            active_agents: 1,
            plan_size: 1,
            checkpoint_recoveries: 0,
            timestamp: at,
        }
    }

    #[test]
    fn an_empty_cache_reads_as_no_recommendation_and_cycle_zero() {
        let cache = SupervisorEventCache::default();
        assert!(cache.scale_recommendation().is_none());
        assert_eq!(cache.cycle(), 0);
        assert!(cache.last_plan_emitted_at().is_none());
    }

    #[test]
    fn the_last_plan_wins_and_other_events_leave_it_alone() {
        // `last_plan_emitted_at` has no accessor on the supervisor's contract
        // either; the Rebalance event is its only source, so the tick and the
        // scale recommendation must not move it.
        let cache = SupervisorEventCache::default();
        let first = Utc::now();
        cache.observe(&rebalance(first));
        cache.observe(&tick(3));
        cache.observe(&scale(2, "ratio 1.1"));
        assert_eq!(cache.last_plan_emitted_at(), Some(first));

        let second = first + chrono::Duration::seconds(30);
        cache.observe(&rebalance(second));
        assert_eq!(cache.last_plan_emitted_at(), Some(second));
    }

    #[test]
    fn the_tick_sets_the_cycle_and_other_events_leave_it_alone() {
        // `cycle` has no accessor on the supervisor's contract; the tick is its
        // only source, so a non-tick event must not move it.
        let cache = SupervisorEventCache::default();
        cache.observe(&scale(3, "ratio 1.2"));
        assert_eq!(cache.cycle(), 0, "a non-tick event must not set the cycle");
        cache.observe(&tick(7));
        cache.observe(&scale(4, "ratio 1.5"));
        assert_eq!(cache.cycle(), 7);
    }

    #[test]
    fn the_last_recommendation_wins_and_carries_the_event_fields() {
        let cache = SupervisorEventCache::default();
        cache.observe(&scale(2, "first"));
        cache.observe(&tick(1));
        cache.observe(&scale(5, "second"));

        let last = cache.scale_recommendation().expect("one seen");
        assert_eq!(last.recommended_agents, 5);
        assert_eq!(last.reason, "second");
    }

    #[test]
    fn queue_stats_response_names_every_field_a_reader_needs() {
        // The response is the only place these reach an operator, so pin the
        // serialized names: a rename here silently empties the endpoint.
        let cache = SupervisorEventCache::default();
        cache.observe(&tick(9));
        cache.observe(&scale(4, "ratio 1.5"));
        let stats = QueueStats {
            pending_handoffs: 12,
            retry_count: 1,
            dlq_count: 0,
            ready_tasks: 3,
            cycle: cache.cycle(),
            scale_recommendation: cache.scale_recommendation(),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_value(&stats).expect("serializes");
        assert_eq!(json["ready_tasks"], 3);
        assert_eq!(json["cycle"], 9);
        assert_eq!(json["scale_recommendation"]["recommended_agents"], 4);
        assert!(json["scale_recommendation"]["decided_at"].is_string());
    }
}
