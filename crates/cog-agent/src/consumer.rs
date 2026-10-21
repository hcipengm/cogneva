//! Agent inbox consumer.
//!
//! [`AgentConsumer`] runs a consumer-group subscription over any
//! [`cog_core::MessageBackend`]: it creates the group, reads the inbox stream,
//! hands each message to a handler and acks it only once the handler has
//! returned. Which transport is underneath — a bus, a stream store, a channel —
//! is not this crate's fact to know.
//!
//! Delivery is at-least-once for as long as the backend keeps unacked
//! messages: a handler that fails leaves its message pending rather than acked,
//! and a transport that brokers a persistent queue can hand it back. Reading
//! those back is [`cog_core::MessageBackend::claim_pending`], which this loop
//! does not call — nothing asks for reclaim yet, and a reclaim pass that no
//! caller waits on is a second moving part in the delivery path.

use std::sync::Arc;

use cog_core::SFResult;
use futures::StreamExt;

/// Generic agent consumer backed by any [`cog_core::MessageBackend`] implementation.
/// This type was originally in `cog-core` but has been moved to `cog-agent`
/// because it is a concrete implementation rather than a trait definition.
pub struct AgentConsumer {
    agent_id: String,
    backend: Arc<dyn cog_core::MessageBackend>,
    inbox_stream: String,
}

impl AgentConsumer {
    /// Create a new consumer for the given agent ID.
    pub fn new(agent_id: impl Into<String>, backend: Arc<dyn cog_core::MessageBackend>) -> Self {
        let agent_id = agent_id.into();
        let inbox_stream = cog_core::agent_inbox_stream(&agent_id);
        Self {
            agent_id,
            backend,
            inbox_stream,
        }
    }

    /// Return the inbox stream name for this consumer.
    pub fn inbox_stream(&self) -> &str {
        &self.inbox_stream
    }

    /// Return the agent ID.
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Run the consumer loop: create consumer group, subscribe to the inbox
    /// stream, and yield each [`cog_core::InboxMessage`] to the provided handler.
    pub async fn run<F, Fut>(self, mut handler: F) -> SFResult<()>
    where
        F: FnMut(cog_core::InboxMessage) -> Fut,
        Fut: std::future::Future<Output = SFResult<()>>,
    {
        let group_name = format!("agent-{}", self.agent_id);

        self.backend
            .create_consumer_group(&self.inbox_stream, &group_name)
            .await?;

        let mut stream = self
            .backend
            .subscribe(&self.inbox_stream, &group_name)
            .await?;

        while let Some(result) = stream.next().await {
            let (msg_id, bytes) = result?;
            let message: cog_core::InboxMessage = match serde_json::from_slice(&bytes)
                .map_err(cog_core::SFError::Serialization)
            {
                Ok(m) => m,
                Err(e) => {
                    // Poison message: ack it out, otherwise every redelivery
                    // fails the same parse and the consumer can never advance.
                    // The bytes are handed to the dead-letter queue before the
                    // ack, because the ack is what makes them unrecoverable and
                    // a log line is not somewhere a message is read back from.
                    // A failed DLQ write does not keep it pending: the parse can
                    // never succeed, so redelivery buys nothing.
                    tracing::warn!(msg_id = %msg_id, "Failed to deserialize InboxMessage: {e}");
                    if let Err(dlq_err) = self
                        .backend
                        .dlq(
                            &self.inbox_stream,
                            &msg_id,
                            &bytes,
                            &format!("unparsable InboxMessage: {e}"),
                        )
                        .await
                    {
                        tracing::error!(
                            msg_id = %msg_id,
                            "dead-letter write failed; this message's bytes are now lost: {dlq_err}"
                        );
                    }
                    if let Err(e) = self
                        .backend
                        .ack(
                            &self.inbox_stream,
                            &group_name,
                            std::slice::from_ref(&msg_id),
                        )
                        .await
                    {
                        tracing::warn!(msg_id = %msg_id, "Failed to ack poison inbox message: {e}");
                    }
                    continue;
                }
            };
            // Handler error propagates: the message stays pending for
            // redelivery instead of being acked as processed.
            handler(message).await?;
            if let Err(e) = self
                .backend
                .ack(
                    &self.inbox_stream,
                    &group_name,
                    std::slice::from_ref(&msg_id),
                )
                .await
            {
                tracing::warn!(msg_id = %msg_id, "Failed to ack inbox message: {e}");
            }
        }

        Ok(())
    }

    /// Send a message to another agent's inbox.
    pub async fn send_message(
        to_agent_id: &str,
        message: cog_core::InboxMessage,
        backend: &dyn cog_core::MessageBackend,
    ) -> SFResult<()> {
        let inbox_stream = cog_core::agent_inbox_stream(to_agent_id);
        let payload = serde_json::to_vec(&message).map_err(cog_core::SFError::Serialization)?;
        backend.publish(&inbox_stream, &payload).await
    }
}
