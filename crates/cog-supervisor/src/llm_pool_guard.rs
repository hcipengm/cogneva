//! LLM upstream pool awareness for the scheduler.
//!
//! The security gateway owns upstream health (it is the only process holding
//! upstream credentials) and publishes a snapshot to Redis. This module reads
//! that snapshot on a tick and cooperatively pauses the *LLM-dependent* task
//! class while every upstream is unusable, resuming it once one comes back.
//!
//! Pausing is class-scoped on purpose: builds, image publishing, mainline
//! deployment and metric collection keep running without an LLM, so a pool
//! outage must not stop them. Probing stays in the gateway process, which is
//! never gated, so a paused scheduler cannot deadlock recovery.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cog_core::{LlmPoolStatus, LlmPoolStatusSource, SupervisorEvent, TaskClass};
use tokio::sync::broadcast;

use crate::scheduler_gate::SchedulerGate;

/// Loop name reported through the background-loop liveness family.
pub const LLM_POOL_GUARD_LOOP: &str = "supervisor_llm_pool_guard";

/// Reads the snapshot the gateway writes to Redis.
///
/// The channel is established on demand and retried after a failure, so a redis
/// that is briefly away -- the gateway and redis rolling together, a resolver
/// not up yet -- does not disable the guard for the life of the process. Only a
/// url that cannot be parsed does that, and that one is a configuration fault
/// rather than a blip.
pub struct RedisLlmPoolStatusSource {
    channel: cog_redis::Reconnecting,
}

impl RedisLlmPoolStatusSource {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            channel: cog_redis::Reconnecting::new(client),
        })
    }
}

#[async_trait::async_trait]
impl LlmPoolStatusSource for RedisLlmPoolStatusSource {
    async fn status(&self) -> Option<LlmPoolStatus> {
        let mut conn = self.channel.get().await?;
        let raw: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(cog_core::LLM_POOL_STATUS_KEY)
            .query_async(&mut conn)
            .await;
        match raw {
            Ok(Some(text)) => match serde_json::from_str(&text) {
                Ok(status) => Some(status),
                Err(e) => {
                    tracing::warn!(error = %e, "LLM pool status snapshot malformed, no verdict this pass");
                    None
                }
            },
            _ => None,
        }
    }
}

/// Edge transition detected by one enforcement pass.
#[derive(Debug, Clone, PartialEq)]
pub enum PoolTransition {
    /// No change since the previous pass.
    Steady,
    /// Pool just became fully unavailable.
    Down(LlmPoolStatus),
    /// Pool just became usable again.
    Recovered,
}

/// Reads the pool snapshot and toggles the LLM-dependent class on the gate.
pub struct LlmPoolGuard {
    source: Arc<dyn LlmPoolStatusSource>,
    gate: Arc<SchedulerGate>,
    was_down: AtomicBool,
}

impl LlmPoolGuard {
    pub fn new(source: Arc<dyn LlmPoolStatusSource>, gate: Arc<SchedulerGate>) -> Self {
        Self {
            source,
            gate,
            was_down: AtomicBool::new(false),
        }
    }

    /// Run one pass. The gate is driven to match the observed snapshot whenever
    /// there is one, so a restarted scheduler converges; the returned transition
    /// lets the caller emit an event only on edges.
    ///
    /// A pass that reads no snapshot changes nothing: the gate keeps the state
    /// the last reading put it in and no edge is reported. Both halves of the
    /// signal roll — the gateway publishes it and redis holds it, and every
    /// deployment restarts both — so an absent snapshot is an ordinary sight
    /// during a rollout. Reading it as "the pool is fine" would resume work that
    /// cannot succeed, and would report a recovery on every restart, which makes
    /// the alert's own `fired_at` the time of the last rollout instead of the
    /// time the pool went down. Absence is not a verdict in either direction.
    pub async fn enforce(&self) -> PoolTransition {
        let Some(snapshot) = self.source.status().await else {
            return PoolTransition::Steady;
        };
        let down = snapshot.unavailable;

        if down {
            self.gate.pause_kind(TaskClass::LlmDependent);
        } else {
            self.gate.resume_kind(TaskClass::LlmDependent);
        }

        let was_down = self.was_down.swap(down, Ordering::SeqCst);
        match (was_down, down) {
            (false, true) => PoolTransition::Down(snapshot),
            (true, false) => PoolTransition::Recovered,
            _ => PoolTransition::Steady,
        }
    }

    /// Spawn the periodic loop. Emits `LlmUpstreamPoolDown`/`Recovered` on the
    /// supervisor broadcast channel so alerting and the UI see the edges.
    pub fn spawn(
        self: Arc<Self>,
        event_tx: broadcast::Sender<SupervisorEvent>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        // The handle comes from the supervised task itself: an outer wrapper would
        // complete as soon as it had spawned, and a caller awaiting it would read
        // "the guard is over" while the guard was still running.
        // Nothing hands this loop a stop signal: it is spawned for the life of
        // the process, so ending for any reason leaves the pool unguarded.
        cog_core::loop_health::spawn_unstoppable(
            LLM_POOL_GUARD_LOOP,
            cog_core::loop_health::Cadence::Periodic(interval),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = Arc::clone(&self);
                let event_tx = event_tx.clone();
                async move {
                    let mut ticker = tokio::time::interval(interval);
                    loop {
                        beat.beat();
                        ticker.tick().await;
                        match this.enforce().await {
                            PoolTransition::Down(status) => {
                                tracing::warn!(
                                    evidenced_recovery_unix = status.evidenced_recovery_unix,
                                    next_attempt_unix = status.next_attempt_unix,
                                    upstreams = ?status.unavailable_upstreams,
                                    "LLM 上游池全灭，暂停 LLM 依赖型任务"
                                );
                                let _ = event_tx.send(SupervisorEvent::LlmUpstreamPoolDown {
                                    evidenced_recovery_unix: status.evidenced_recovery_unix,
                                    next_attempt_unix: status.next_attempt_unix,
                                    unavailable: status.unavailable_upstreams,
                                    // 池的大小与窗口长度一起转发：本模块只是搬运，
                                    // 图里没有配置面，读 event 的那几处更没有。
                                    pool_size: status.pool_size,
                                    quota_window_secs: status.quota_window_secs,
                                    timestamp: chrono::Utc::now(),
                                });
                            }
                            PoolTransition::Recovered => {
                                tracing::info!("LLM 上游池恢复，LLM 依赖型任务自动恢复");
                                let _ = event_tx.send(SupervisorEvent::LlmUpstreamPoolRecovered {
                                    timestamp: chrono::Utc::now(),
                                });
                            }
                            PoolTransition::Steady => {}
                        }
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Scripted source: each `status()` call pops the next snapshot; the last
    /// one sticks.
    struct ScriptedSource {
        queue: Mutex<Vec<Option<LlmPoolStatus>>>,
    }

    impl ScriptedSource {
        fn new(states: Vec<Option<LlmPoolStatus>>) -> Self {
            let mut queue = states;
            queue.reverse();
            Self {
                queue: Mutex::new(queue),
            }
        }
    }

    #[async_trait::async_trait]
    impl LlmPoolStatusSource for ScriptedSource {
        async fn status(&self) -> Option<LlmPoolStatus> {
            let mut q = self.queue.lock().unwrap();
            if q.len() > 1 {
                q.pop().unwrap()
            } else {
                q.first().cloned().flatten()
            }
        }
    }

    fn down_status() -> LlmPoolStatus {
        LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 1_800_000_000,
            next_attempt_unix: 1_799_999_400,
            unavailable_upstreams: vec!["a|m".into(), "b|m".into()],
            ..Default::default()
        }
    }

    /// A snapshot that says the pool can serve again is a verdict, and only a
    /// verdict resumes the class.
    fn available_status() -> LlmPoolStatus {
        LlmPoolStatus::default()
    }

    #[tokio::test]
    async fn pauses_on_down_and_resumes_on_recovery() {
        let gate = Arc::new(SchedulerGate::new());
        let source = Arc::new(ScriptedSource::new(vec![
            Some(down_status()),
            Some(down_status()),
            Some(available_status()),
        ]));
        let guard = LlmPoolGuard::new(source, gate.clone());

        assert_eq!(guard.enforce().await, PoolTransition::Down(down_status()));
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));
        assert!(!gate.is_paused_kind(TaskClass::Mechanical));

        assert_eq!(guard.enforce().await, PoolTransition::Steady);
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));

        assert_eq!(guard.enforce().await, PoolTransition::Recovered);
        assert!(!gate.is_paused_kind(TaskClass::LlmDependent));
    }

    /// The gateway and redis both restart on every deployment, so a pass with no
    /// snapshot is routine. It must not undo the pause, and it must not report a
    /// recovery: a reported recovery resolves the alert and the next firing edge
    /// re-dates it, which turns the pool-down alert's start time into the time of
    /// the last rollout.
    #[tokio::test]
    async fn an_absent_snapshot_is_neither_a_recovery_nor_a_pause_lift() {
        let gate = Arc::new(SchedulerGate::new());
        let source = Arc::new(ScriptedSource::new(vec![
            Some(down_status()),
            None,
            None,
            Some(available_status()),
        ]));
        let guard = LlmPoolGuard::new(source, gate.clone());

        assert_eq!(guard.enforce().await, PoolTransition::Down(down_status()));

        assert_eq!(guard.enforce().await, PoolTransition::Steady);
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));

        // Still no verdict: the pause holds and no edge is invented.
        assert_eq!(guard.enforce().await, PoolTransition::Steady);
        assert!(gate.is_paused_kind(TaskClass::LlmDependent));

        // The absent passes did not move the latch either: the first readable
        // verdict after them still reads as the recovery edge.
        assert_eq!(guard.enforce().await, PoolTransition::Recovered);
        assert!(!gate.is_paused_kind(TaskClass::LlmDependent));
    }

    #[tokio::test]
    async fn healthy_pool_never_pauses() {
        let gate = Arc::new(SchedulerGate::new());
        let source = Arc::new(ScriptedSource::new(vec![Some(available_status())]));
        let guard = LlmPoolGuard::new(source, gate.clone());
        assert_eq!(guard.enforce().await, PoolTransition::Steady);
        assert!(!gate.is_paused_kind(TaskClass::LlmDependent));
    }
}
