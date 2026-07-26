use std::sync::Arc;

use cog_core::{AgentEvent, MessageBackend};
use futures::StreamExt;
use tokio::sync::broadcast;

/// Consumes AgentEvents from multiple message backends with automatic fallback.
/// Primary backend (e.g. Redis Streams) is polled continuously.
/// If the primary fails, the consumer automatically switches to the fallback
/// (e.g. NATS JetStream) until the primary recovers.
pub struct MultiBackendEventConsumer {
    primary: Arc<dyn MessageBackend>,
    fallback: Option<Arc<dyn MessageBackend>>,
    event_tx: broadcast::Sender<AgentEvent>,
    channel: String,
    group: String,
    retry_interval_secs: u64,
}

impl MultiBackendEventConsumer {
    pub fn new(
        primary: Arc<dyn MessageBackend>,
        event_tx: broadcast::Sender<AgentEvent>,
        channel: impl Into<String>,
    ) -> Self {
        Self {
            primary,
            fallback: None,
            event_tx,
            channel: channel.into(),
            group: "supervisor-multi".into(),
            retry_interval_secs: 5,
        }
    }

    pub fn with_fallback(mut self, fallback: Arc<dyn MessageBackend>) -> Self {
        self.fallback = Some(fallback);
        self
    }

    pub fn with_retry_interval(mut self, secs: u64) -> Self {
        self.retry_interval_secs = secs;
        self
    }

    /// Spawn a background task that continuously consumes events.
    /// On primary failure, automatically switches to fallback backend.
    /// When primary recovers, switches back.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let primary = self.primary;
        let fallback = self.fallback;
        let event_tx = self.event_tx;
        let channel = self.channel;
        let group = self.group;
        let retry_interval_secs = self.retry_interval_secs;

        tokio::spawn(async move {
            let mut using_primary = true;

            loop {
                let backend: Arc<dyn MessageBackend> = if using_primary {
                    primary.clone()
                } else if let Some(ref fb) = fallback {
                    fb.clone()
                } else {
                    // No fallback available — wait and retry primary
                    tokio::time::sleep(tokio::time::Duration::from_secs(retry_interval_secs)).await;
                    using_primary = true;
                    continue;
                };

                match backend.subscribe(&channel, &group).await {
                    Ok(mut stream) => {
                        if !using_primary {
                            tracing::info!("MultiBackendEventConsumer: switched back to primary");
                            using_primary = true;
                        }
                        // Consume from this stream until it breaks
                        loop {
                            match stream.next().await {
                                Some(Ok((id, payload))) => {
                                    if let Ok(event) =
                                        serde_json::from_slice::<AgentEvent>(&payload)
                                    {
                                        let _ = event_tx.send(event);
                                    }
                                    if let Err(e) = backend.ack(&channel, &group, &[id]).await {
                                        tracing::warn!(
                                            "MultiBackendEventConsumer: ack failed: {}",
                                            e
                                        );
                                    }
                                }
                                Some(Err(e)) => {
                                    tracing::warn!(
                                        "MultiBackendEventConsumer: stream error: {}",
                                        e
                                    );
                                    break;
                                }
                                None => {
                                    tracing::warn!("MultiBackendEventConsumer: stream closed");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("MultiBackendEventConsumer: subscribe failed: {}", e);
                        using_primary = false;
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        })
    }
}
