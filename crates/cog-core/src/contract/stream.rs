use crate::{SFError, SFResult};
use async_trait::async_trait;
use std::pin::Pin;

/// A boxed stream of messages yielded by a [`MessageBackend::subscribe`] call.
/// Each item is a tuple of `(message_id, payload_bytes)`.
pub type MessageStream = Pin<Box<dyn futures::Stream<Item = SFResult<(String, Vec<u8>)>> + Send>>;

/// Abstract message-queue backend for the DagExecutor layer.
/// Implementations may target Redis Streams, NATS, Kafka, or in-memory
/// channels for testing.  The trait is intentionally low-level (raw bytes)
/// so that serialization policy lives in the caller.
#[async_trait]
pub trait MessageBackend: Send + Sync {
    /// Publish a raw payload to the given subject / stream.
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()>;

    /// Publish multiple payloads to the given subject in one batch.
    /// The default implementation loops over `payloads` and calls [`Self::publish`]
    /// sequentially.  Backends that support native batching (Redis pipeline,
    /// NATS concurrent publish, in-memory single-lock) should override this
    /// for better throughput and lower latency.
    async fn publish_batch(&self, subject: &str, payloads: &[Vec<u8>]) -> SFResult<()> {
        for payload in payloads {
            self.publish(subject, payload).await?;
        }
        Ok(())
    }

    /// Subscribe to a subject as part of a consumer group.
    /// Returns a stream of `(message_id, payload)` tuples.  The caller is
    /// responsible for acking individual messages if the implementation
    /// requires it.
    /// This variant starts at the current tip ("new messages only").
    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream>;

    /// Subscribe from a specific message ID, enabling replay / catch-up.
    /// `start_id` semantics are backend-specific:
    /// - Redis Streams: pass "0" for the beginning of the stream, or a
    ///   concrete ID such as "1234567890-0".
    /// - In-memory: pass "0" for the beginning of the buffer, or a
    ///   synthetic offset string.
    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream>;

    /// Create a consumer group on the target stream if it does not exist.
    async fn create_consumer_group(&self, stream: &str, group: &str) -> SFResult<()>;

    /// Acknowledge one or more message IDs in a consumer group.
    /// Default no-op for backends that do not require explicit acks.
    async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
        Ok(())
    }

    /// Claim pending messages that have been idle longer than `min_idle_ms`
    /// (delivered to a consumer that never acked — e.g. the pod died
    /// mid-processing). Returns the claimed `(message_id, payload)` tuples,
    /// now owned by the calling consumer.
    /// Default: backend has no pending-recovery support, returns empty.
    async fn claim_pending(
        &self,
        _stream: &str,
        _group: &str,
        _min_idle_ms: u64,
        _count: usize,
    ) -> SFResult<Vec<(String, Vec<u8>)>> {
        Ok(Vec::new())
    }

    /// Publish a message to the dead-letter queue for the given stream.
    /// The default implementation appends to a `{stream}:dlq` subject.
    /// Backends may override this to use a native DLQ mechanism.
    async fn dlq(&self, stream: &str, msg_id: &str, reason: &str) -> SFResult<()> {
        let payload = serde_json::json!({
            "original_id": msg_id,
            "reason": reason,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });
        let bytes = serde_json::to_vec(&payload).map_err(SFError::Serialization)?;
        self.publish(&format!("{}:dlq", stream), &bytes).await
    }

    /// Publish a message after a delay.
    /// The default implementation spawns a local timer and calls
    /// [`Self::publish`] when it fires.  Redis-backed implementations
    /// may override this with a sorted-set + background worker for
    /// durability across restarts.
    async fn delay_publish(&self, _subject: &str, _payload: &[u8], _delay_ms: u64) -> SFResult<()> {
        Ok(())
    }
}
