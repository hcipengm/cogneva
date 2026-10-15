use std::sync::Arc;
use tokio::task::JoinHandle;

/// Forwards all HookEngine events to the WebSocket broadcast channel
/// (and optionally to a downstream [`cog_core::EventPublisher`] for cross-process consumers).
/// Subscribes to the HookEngine's internal event broadcast and re-publishes
/// each hook event as an `AgentEvent::TaskStatusChange` so WebSocket clients
/// receive them.
pub struct HookToWsForwarder;

impl HookToWsForwarder {
    pub fn spawn(
        hook_engine: Arc<dyn cog_core::HookEngine>,
        event_tx: tokio::sync::broadcast::Sender<cog_core::AgentEvent>,
        event_publisher: Option<Arc<dyn cog_core::EventPublisher>>,
        metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    ) -> JoinHandle<()> {
        let mut rx = hook_engine.subscribe();

        tokio::spawn(async move {
            // 三种结局分开写：`Lagged` 也是一个 `Err`，`while let Ok(..)` 会把广播
            // 里一次普通的背压读成「通道关了」，于是这条转发**静静退出**——钩子事件
            // 从此不再进 WebSocket，与「没有钩子触发」在读数面上同形。
            loop {
                let hook_event = match rx.recv().await {
                    Ok(hook_event) => hook_event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "hook forwarder fell behind; it keeps forwarding");
                        crate::record_consumer_lag(
                            &metrics,
                            crate::LAG_CONSUMER_HOOK_FORWARDER,
                            skipped,
                        )
                        .await;
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
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
