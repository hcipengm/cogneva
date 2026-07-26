//! Stream plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Stream plugin that creates and publishes the message backend and event channels.
pub struct StreamPlugin {
    initialized: bool,
}

impl StreamPlugin {
    /// Create a plugin that will build the message backend during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for StreamPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for StreamPlugin {
    fn name(&self) -> &'static str {
        "stream"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let (nats_urls, redis_url, strict_persistence, event_channel_capacity) = {
            let config = ctx.config();
            (
                config.dag_executor.nats.urls.clone(),
                config.dag_executor.redis_url.clone(),
                config.system.strict_persistence,
                config.system.event_channel_capacity,
            )
        };

        let backend: Option<Arc<dyn cog_core::MessageBackend>> = if !nats_urls.is_empty() {
            match crate::NatsMessageBackend::new(&cog_core::NatsConfig {
                urls: nats_urls.clone(),
                ..Default::default()
            })
            .await
            {
                Ok(b) => {
                    info!("NatsMessageBackend primary connected");
                    Some(Arc::new(b))
                }
                Err(e) => {
                    if strict_persistence {
                        return Err(cog_core::SFError::Config(format!(
                            "NatsMessageBackend failed (strict_persistence=true): {}",
                            e
                        )));
                    }
                    warn!("NatsMessageBackend failed: {}. Falling back to Redis.", e);
                    match crate::RedisMessageBackend::new(&redis_url).await {
                        Ok(b) => Some(Arc::new(b)),
                        Err(e2) => {
                            warn!(
                                "RedisMessageBackend fallback failed: {}. Using in-memory.",
                                e2
                            );
                            Some(Arc::new(crate::MemoryMessageBackend::new()))
                        }
                    }
                }
            }
        } else {
            match crate::RedisMessageBackend::new(&redis_url).await {
                Ok(b) => Some(Arc::new(b)),
                Err(e) => {
                    if strict_persistence {
                        return Err(cog_core::SFError::Config(format!(
                            "RedisMessageBackend failed (strict_persistence=true): {}",
                            e
                        )));
                    }
                    warn!(
                        "RedisMessageBackend failed: {}. Using in-memory fallback.",
                        e
                    );
                    Some(Arc::new(crate::MemoryMessageBackend::new()))
                }
            }
        };

        if let Some(ref b) = backend {
            ctx.publish_service(b.clone());
            info!("StreamPlugin message backend published");

            // ── EventPublisher (semantic wrapper over MessageBackend) ──
            let channel = ctx.config().multi_backend_consumer.channel.clone();
            let publisher = Arc::new(crate::MqEventPublisher::new(b.clone(), channel));
            let publisher_dyn: Arc<dyn cog_core::EventPublisher> = publisher;
            ctx.publish_service(publisher_dyn);
            info!("StreamPlugin EventPublisher published");
        } else {
            info!("StreamPlugin no message backend configured");
        }

        // ── Event broadcast channel ──
        let (event_tx, _event_rx) =
            tokio::sync::broadcast::channel::<cog_core::AgentEvent>(event_channel_capacity);
        ctx.publish(Arc::new(event_tx));
        info!("StreamPlugin event sender published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("StreamPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "stream",
    requires: &[],
    optional_requires: &[],
    provides: &[
        "MessageBackend",
        "EventPublisher",
        "Sender<AgentEvent>",
        "Sender<TaskEvent>",
    ],
    consumes: &[],
    factory: || Box::new(StreamPlugin::new()),
};
