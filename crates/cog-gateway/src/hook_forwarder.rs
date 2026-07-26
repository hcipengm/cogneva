use std::sync::Arc;
use tokio::task::JoinHandle;

/// Forwards all HookEngine events to the WebSocket broadcast channel
/// (and optionally to a downstream [`EventPublisher`] for cross-process consumers).
/// Subscribes to the HookEngine's internal event broadcast and re-publishes
/// each hook event as an `AgentEvent::TaskStatusChange` so WebSocket clients
/// receive them.
pub struct HookToWsForwarder;

impl HookToWsForwarder {
    pub fn spawn(
        hook_engine: Arc<dyn cog_core::HookEngine>,
        event_tx: tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
        event_publisher: Option<Arc<dyn cog_core::EventPublisher>>,
    ) -> JoinHandle<()> {
        let mut rx = hook_engine.subscribe();

        tokio::spawn(async move {
            while let Ok(hook_event) = rx.recv().await {
                let event = cog_core::AgentEvent::TaskStatusChange {
                    task_id: hook_event
                        .task_id
                        .clone()
                        .unwrap_or_else(|| "hook".to_string()),
                    status: "hook_event".to_string(),
                    agent_id: hook_event.agent_id.clone(),
                    crew_id: hook_event.crew_id.clone(),
                    squad_id: hook_event.squad_id.clone(),
                    timestamp: chrono::Utc::now(),
                };

                // Publish to downstream event bus (e.g. MQ) if configured.
                if let Some(ref publisher) = event_publisher {
                    let publisher = publisher.clone();
                    let event_clone = event.clone();
                    tokio::spawn(async move {
                        if let Err(e) = publisher.publish(&event_clone).await {
                            tracing::warn!("Failed to publish hook AgentEvent: {}", e);
                        }
                    });
                }

                // Broadcast to WebSocket subscribers; ignore send errors (no receivers).
                let _ = event_tx.send(event);
            }
        })
    }
}
