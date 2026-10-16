//! LLM upstream pool awareness for the scheduler.
//!
//! The security gateway owns upstream health (it is the only process holding
//! upstream credentials) and publishes a snapshot to the shared key-value face.
//! This module reads that snapshot on a tick and cooperatively pauses the
//! *LLM-dependent* task class while every upstream is unusable, resuming it
//! once one comes back.
//!
//! Pausing is class-scoped on purpose: builds, image publishing, mainline
//! deployment and metric collection keep running without an LLM, so a pool
//! outage must not stop them. Probing stays in the gateway process, which is
//! never gated, so a paused scheduler cannot deadlock recovery.
//!
//! The publishing half is the gateway's `upstream_pool` module; this side
//! only reads the snapshot it writes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cog_core::{LlmPoolStatus, LlmPoolStatusSource, SupervisorEvent, TaskClass};
use tokio::sync::broadcast;

use crate::scheduler_gate::SchedulerGate;

/// Loop name reported through the background-loop liveness family.
pub const LLM_POOL_GUARD_LOOP: &str = "supervisor_llm_pool_guard";

/// Reads the snapshot the gateway writes, through the shared key-value face.
///
/// The face is handed in rather than built from a connection string: reaching
/// the store is the storage layer's business, and a reader that opens its own
/// connection is a reader that has to be rebuilt whenever the transport moves.
///
/// Nothing here connects on demand and nothing latches: a face that answers
/// with an error this pass is asked again next pass, so a store that is briefly
/// away -- the gateway and the store rolling together, a resolver not up yet --
/// does not disable the guard for the life of the process.
pub struct KeyValuePoolStatusSource {
    store: Arc<dyn cog_core::KeyValueStore>,
}

impl KeyValuePoolStatusSource {
    pub fn new(store: Arc<dyn cog_core::KeyValueStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl LlmPoolStatusSource for KeyValuePoolStatusSource {
    async fn status(&self) -> Option<LlmPoolStatus> {
        let raw = match self.store.get(cog_core::LLM_POOL_STATUS_KEY).await {
            Ok(raw) => raw,
            Err(e) => {
                // Not a verdict about the pool: nothing was read. Said out loud
                // because on this channel an unreadable store and an absent
                // snapshot both leave the gate where it was, and only one of
                // them is a fault.
                tracing::warn!(error = %e, "LLM pool status unreadable this pass, no verdict");
                return None;
            }
        };
        match raw {
            Some(text) => match serde_json::from_str(&text) {
                Ok(status) => Some(status),
                Err(e) => {
                    tracing::warn!(error = %e, "LLM pool status snapshot malformed, no verdict this pass");
                    None
                }
            },
            None => None,
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
    /// signal roll — the gateway publishes it and the store holds it, and every
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
                                    "LLM 上游池可用数低于冗余下限，暂停 LLM 依赖型任务"
                                );
                                let _ = event_tx.send(SupervisorEvent::LlmUpstreamPoolDown {
                                    evidenced_recovery_unix: status.evidenced_recovery_unix,
                                    next_attempt_unix: status.next_attempt_unix,
                                    unavailable: status.unavailable_upstreams,
                                    // 池的大小、声明的下限与窗口长度一起转发：本模块
                                    // 只是搬运，图里没有配置面，读 event 的那几处更没有。
                                    // 下限尤其不能漏——判定从"零家可服务"改成"低于声明的
                                    // 下限"之后，判词要从这两个数算出"按谁的界判的"。
                                    pool_size: status.pool_size,
                                    declared_min_healthy: status.declared_min_healthy,
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

    /// The gateway and the store both restart on every deployment, so a pass with no
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

    /// The store side of the source, scripted: one answer, whatever it is.
    ///
    /// Written against the contract rather than against a live store: what is
    /// under test is which answers count as a verdict, and that is a property of
    /// this adapter, not of the store's reachability. A real store would make
    /// the test a statement about whether one is running.
    struct ScriptedStore {
        answer: Mutex<Option<cog_core::SFResult<Option<String>>>>,
    }

    impl ScriptedStore {
        fn answering(result: cog_core::SFResult<Option<String>>) -> Arc<Self> {
            Arc::new(Self {
                answer: Mutex::new(Some(result)),
            })
        }
    }

    #[async_trait::async_trait]
    impl cog_core::KeyValueStore for ScriptedStore {
        async fn get(&self, _key: &str) -> cog_core::SFResult<Option<String>> {
            self.answer.lock().unwrap().take().expect("one answer")
        }

        async fn set(&self, _: &str, _: &str) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn set_ex(&self, _: &str, _: &str, _: u64) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn del(&self, _: &str) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn expire(&self, _: &str, _: i64) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn ttl(&self, _: &str) -> cog_core::SFResult<i64> {
            unreachable!("the pool status source only reads")
        }
        async fn incr(&self, _: &str, _: i64) -> cog_core::SFResult<i64> {
            unreachable!("the pool status source only reads")
        }
        async fn decr(&self, _: &str, _: i64) -> cog_core::SFResult<i64> {
            unreachable!("the pool status source only reads")
        }
        async fn incr_ex(&self, _: &str, _: i64, _: u64) -> cog_core::SFResult<i64> {
            unreachable!("the pool status source only reads")
        }
        async fn sadd(&self, _: &str, _: &str) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn srem(&self, _: &str, _: &str) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn smembers(&self, _: &str) -> cog_core::SFResult<Vec<String>> {
            unreachable!("the pool status source only reads")
        }
        async fn hset(&self, _: &str, _: &str, _: &str) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn hget(&self, _: &str, _: &str) -> cog_core::SFResult<Option<String>> {
            unreachable!("the pool status source only reads")
        }
        async fn hdel(&self, _: &str, _: &str) -> cog_core::SFResult<()> {
            unreachable!("the pool status source only reads")
        }
        async fn hvals(&self, _: &str) -> cog_core::SFResult<Vec<String>> {
            unreachable!("the pool status source only reads")
        }
        async fn hgetall(&self, _: &str) -> cog_core::SFResult<Vec<(String, String)>> {
            unreachable!("the pool status source only reads")
        }
        async fn hlen(&self, _: &str) -> cog_core::SFResult<u64> {
            unreachable!("the pool status source only reads")
        }
    }

    async fn read_from(result: cog_core::SFResult<Option<String>>) -> Option<LlmPoolStatus> {
        let store: Arc<dyn cog_core::KeyValueStore> = ScriptedStore::answering(result);
        KeyValuePoolStatusSource::new(store).status().await
    }

    #[tokio::test]
    async fn a_readable_snapshot_is_the_only_thing_that_reads_as_a_verdict() {
        assert_eq!(
            read_from(Ok(Some(
                serde_json::to_string(&down_status()).expect("serializes")
            )))
            .await,
            Some(down_status())
        );
        assert_eq!(
            read_from(Ok(Some(
                serde_json::to_string(&available_status()).expect("serializes")
            )))
            .await,
            Some(available_status())
        );
    }

    /// The three ways of having no verdict stay apart from the two ways of
    /// having one. A store that could not be read says nothing about the pool,
    /// and neither does a snapshot that does not parse: folding either into
    /// "available" would resume work that cannot succeed, folding them into
    /// "down" would report an outage nobody measured.
    #[tokio::test]
    async fn an_unreadable_or_unparsable_or_absent_snapshot_is_no_verdict() {
        assert_eq!(read_from(Ok(None)).await, None);
        assert_eq!(read_from(Ok(Some("not json".into()))).await, None);
        assert_eq!(
            read_from(Err(cog_core::SFError::Redis("store away".into()))).await,
            None
        );
    }
}
