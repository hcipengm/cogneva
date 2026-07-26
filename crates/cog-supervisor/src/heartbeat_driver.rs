//! Background heartbeat task driver.
//! Moved from `cog-core` so that `cog-core` only contains the trait + data
//! structures and concrete implementations live in `cog-supervisor`.

use cog_core::{AgentEvent, AgentRegistry, ShutdownSignal};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Background heartbeat task: periodically calls [`AgentRegistry::heartbeat`]
/// on the registry to keep the agent's Redis key alive.
pub struct HeartbeatDriver {
    handle: JoinHandle<()>,
}

impl HeartbeatDriver {
    /// Spawn a heartbeat loop for `agent_id` that fires every `interval_seconds`
    /// and stops when `cancel.wait()` resolves.
    pub fn spawn(
        registry: Arc<dyn AgentRegistry>,
        agent_id: impl Into<String>,
        interval_seconds: u64,
        cancel: ShutdownSignal,
    ) -> Self {
        Self::spawn_with_event_tx(registry, agent_id, interval_seconds, cancel, None)
    }

    /// Spawn a heartbeat loop that also emits heartbeat events on the
    /// supplied broadcast channel for observability archiving.
    pub fn spawn_with_event_tx(
        registry: Arc<dyn AgentRegistry>,
        agent_id: impl Into<String>,
        interval_seconds: u64,
        cancel: ShutdownSignal,
        event_tx: Option<broadcast::Sender<AgentEvent>>,
    ) -> Self {
        let agent_id = agent_id.into();
        let handle = tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(tokio::time::Duration::from_secs(interval_seconds.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = registry.heartbeat(&agent_id).await {
                            tracing::warn!(
                                agent_id = %agent_id,
                                "heartbeat failed: {e}"
                            );
                        }
                        if let Some(ref tx) = event_tx {
                            let event = AgentEvent::Heartbeat {
                                agent_id: agent_id.clone(),
                                timestamp: chrono::Utc::now(),
                            };
                            let _ = tx.send(event);
                        }
                    }
                    _ = cancel.wait() => {
                        break;
                    }
                }
            }
        });
        Self { handle }
    }

    /// Abort the heartbeat task immediately (used by graceful shutdown).
    pub fn abort(self) {
        self.handle.abort();
    }
}
