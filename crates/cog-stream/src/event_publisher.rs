use std::sync::Arc;

/// [`cog_core::EventPublisher`] implementation backed by [`cog_core::MessageBackend`].
/// Serializes `AgentEvent` to JSON and publishes it to a fixed channel on the
/// underlying message bus.  This lives in `cog-stream` (not `cog-gateway`) so
/// that Gateway only depends on the abstract `EventPublisher` trait.
pub struct MqEventPublisher {
    backend: Arc<dyn cog_core::MessageBackend>,
    channel: String,
}

impl MqEventPublisher {
    pub fn new(backend: Arc<dyn cog_core::MessageBackend>, channel: String) -> Self {
        Self { backend, channel }
    }
}

#[async_trait::async_trait]
impl cog_core::EventPublisher for MqEventPublisher {
    async fn publish(&self, event: &cog_core::AgentEvent) -> cog_core::SFResult<()> {
        let bytes = serde_json::to_vec(event).map_err(cog_core::SFError::Serialization)?;
        self.backend.publish(&self.channel, &bytes).await
    }
}
