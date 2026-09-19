use crate::{SFError, SFResult};
use async_trait::async_trait;
use std::pin::Pin;

/// A boxed stream of messages yielded by a [`MessageBackend::subscribe`] call.
/// Each item is a tuple of `(message_id, payload_bytes)`.
pub type MessageStream = Pin<Box<dyn futures::Stream<Item = SFResult<(String, Vec<u8>)>> + Send>>;

/// A consumer group's pending state: entries delivered to some consumer that
/// never acknowledged them.
///
/// This is the state a `subscribe` (new messages only) can never recover from
/// on its own, and the reason an age is reported alongside the count: an entry
/// that is being processed and an entry abandoned by a consumer that died have
/// the same count and differ only in how long they have been outstanding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingStats {
    /// Entries pending right now, being processed or abandoned.
    pub count: u64,
    /// Entries pending longer than the caller's reclaim threshold — i.e. work
    /// a reclaim pass running on its configured cadence would already have
    /// taken back. Non-zero means the reclaim path is not keeping up with its
    /// own policy.
    pub unreclaimed_count: u64,
    /// How long the longest-outstanding of those has been pending, in
    /// milliseconds. Zero when there are none.
    pub unreclaimed_oldest_idle_ms: u64,
}

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
    /// Backends without explicit acks (in-memory) must implement this as a
    /// deliberate no-op; it is required rather than defaulted so a backend
    /// that needs acks can never silently inherit a no-op through a trait
    /// object call.
    async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()>;

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

    /// Read a consumer group's pending state without changing it.
    ///
    /// `idle_threshold_ms` splits "being processed" from "abandoned": only the
    /// outstanding time tells the two apart, and callers pass the threshold
    /// they reclaim at, so the unreclaimed figures answer "is the reclaim path
    /// keeping up with its own policy?" instead of comparing against a number
    /// invented here.
    ///
    /// `None` means this backend holds no pending state to report — either it
    /// brokers no persistent queue, or it has no reclaim path, in which case a
    /// count of what is piling up would say nothing about whether anything can
    /// come back. Required rather than defaulted for the same reason as
    /// [`Self::ack`]: a backend that does have pending messages must not
    /// silently inherit "nothing is pending" through a trait object call.
    async fn pending_stats(
        &self,
        stream: &str,
        group: &str,
        idle_threshold_ms: u64,
    ) -> SFResult<Option<PendingStats>>;

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

/// 事件面专用消息后端的插件间传递 holder。StreamPlugin 发布：
/// `multi_backend_consumer.nats_urls` 非空时是独立的 NATS JetStream 连接
/// （事件面与任务队列的全局 MessageBackend 解耦），否则复用全局后端。
/// 消费方：cog-agent（发布 AgentEnd）、cog-supervisor（回灌 broadcast）、
/// cog-memory（摄取消费）。
pub struct EventPlaneBackend(pub std::sync::Arc<dyn MessageBackend>);

/// 事件面发布器——绑定了事件 channel 的 [`crate::EventPublisher`]，
/// 由 StreamPlugin 随 [`EventPlaneBackend`] 一起发布。
pub struct EventPlanePublisher(pub std::sync::Arc<dyn crate::EventPublisher>);
