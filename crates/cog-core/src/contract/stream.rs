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

/// The serialized record a dead-letter entry is made of.
///
/// One builder rather than one per backend, because the record's shape is what
/// a reader of the DLQ subject parses: three implementations each building their
/// own copy of the same JSON is three places to change when a field is added and
/// three chances to drop the payload, which is the field this exists to carry.
/// The payload is base64 so no byte of a binary message is lost on the way in —
/// the messages that reach here are the ones that could not be parsed, so
/// assuming anything about their encoding is exactly the assumption that failed.
pub fn dead_letter_record(msg_id: &str, payload: &[u8], reason: &str) -> SFResult<Vec<u8>> {
    use base64::Engine as _;
    let record = serde_json::json!({
        "original_id": msg_id,
        "reason": reason,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "payload_b64": base64::engine::general_purpose::STANDARD.encode(payload),
    });
    serde_json::to_vec(&record).map_err(SFError::Serialization)
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

    /// The largest single payload this transport will accept on `subject`, when
    /// it declares one.
    ///
    /// `Some(limit)` is the bound the transport itself enforces on one message
    /// — the server's frame and the stream's own message size, whichever is
    /// tighter — and it exists so a caller can compare *before* it takes a step
    /// it cannot undo. A caller that marks its work scheduled and only then
    /// hands the payload to the publish path has already spent that step when
    /// the payload turns out to be too large, and what was marked scheduled is
    /// not offered again. `None` means this backend imposes no bound of this
    /// kind (an in-memory or test backend), which is not the same statement as
    /// a declared bound of zero and must not be spelled as one.
    ///
    /// Required rather than defaulted: a default answering `None` would let a
    /// wrapper report "no bound" on behalf of a transport that has one, and the
    /// caller's pre-check would then wave through exactly the payload the
    /// publish path goes on to refuse — the ordering this method exists to fix.
    async fn payload_limit(&self, subject: &str) -> SFResult<Option<usize>>;

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

    /// End the read loop that [`Self::subscribe`] opened for `subject` and
    /// `group` in this process, by letting the read already on the wire finish
    /// rather than by dropping its future.
    ///
    /// The distinction is the whole point of this method. A cancelled
    /// `XREADGROUP` may already have been answered: the server put the message
    /// in the group's pending list and the client threw the answer away without
    /// acking it, and nothing in this tree reclaims a pending entry. A loop
    /// that checks a flag between reads leaves with at most one block period of
    /// latency and no message in flight, so that is the only way a consumer may
    /// be asked to stop.
    ///
    /// Returns once the loop has been told; the caller still joins its own
    /// task, because the loop ends after the read it is waiting on.
    ///
    /// Required rather than defaulted: an empty default would let a backend
    /// that really does block on a read inherit "stopped nothing" while every
    /// caller believes the read has ended, and the difference is only visible
    /// as a consumer that never exits.
    async fn stop_stream(&self, subject: &str, group: &str);

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
    ///
    /// `None` means this backend has no reclaim path at all — it brokers no
    /// persistent queue, or it redelivers on its own and needs no explicit
    /// claim. `Some(empty)` is a different answer: the backend can reclaim and
    /// there is nothing to reclaim right now. Folding the two into an empty
    /// list would let a backend that does hold pending messages inherit
    /// "nothing is pending", which is why this is required rather than
    /// defaulted, on the same terms as [`Self::ack`] and [`Self::pending_stats`].
    /// It answers with a value rather than an error for the reason
    /// [`Self::pending_stats`]'s `None` does: "this backend cannot" is not a
    /// failure, and a caller that reads it as one turns a correct backend into
    /// a per-tick alarm that never clears.
    async fn claim_pending(
        &self,
        stream: &str,
        group: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>>;

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

    /// How many entries in `stream` this group has not received yet.
    ///
    /// It answers a different question from [`Self::pending_stats`]: those are
    /// messages already handed to a consumer that have not been acknowledged,
    /// while these have not been handed out at all. A scheduler deciding
    /// whether a task stuck in `Scheduled` lost its message needs the second
    /// number — a message still queued behind a backlog is not lost, and
    /// republishing it only adds a copy the queue has to carry.
    ///
    /// `None` means this backend cannot say. Callers treat it as "no evidence
    /// of a backlog" and keep their ordinary behavior, because an unreadable
    /// queue is not a reading of an empty one.
    ///
    /// Required rather than defaulted on the same terms as
    /// [`Self::pending_stats`]: a backend that does hold undelivered messages
    /// must not inherit "cannot say" through a trait object call, and a
    /// backend that holds none has to say so itself. The two failures read
    /// differently to the caller — one keeps re-arming a message that is still
    /// in line, the other stops waiting for one that never will be.
    async fn stream_lag(&self, stream: &str, group: &str) -> SFResult<Option<u64>>;

    /// Publish a message to the dead-letter queue for the given stream.
    ///
    /// `payload` is the message's own bytes, and it is a parameter because a
    /// record holding only an id and a reason cannot answer the one question a
    /// dead-letter queue exists for — what was in the message. Every caller
    /// reaches this from a branch that is about to drop those bytes for good, so
    /// a queue that keeps the id and loses the message makes the loss look
    /// handled while the thing lost is still gone.
    ///
    /// The default implementation appends [`dead_letter_record`] to a
    /// `{stream}:dlq` subject; backends may override this to use a native DLQ
    /// mechanism, but the record has to carry the same four fields.
    async fn dlq(&self, stream: &str, msg_id: &str, payload: &[u8], reason: &str) -> SFResult<()> {
        let record = dead_letter_record(msg_id, payload, reason)?;
        self.publish(&format!("{}:dlq", stream), &record).await
    }

    /// Publish a message after a delay.
    ///
    /// The default waits `delay_ms` and then calls [`Self::publish`], so a
    /// backend that does not override this still delivers the message; what it
    /// does not do is deliver it *without holding the caller*: the default
    /// cannot spawn, because a trait-object method only ever has `&self` and a
    /// spawned task must own a `'static` handle. A backend that can hand one
    /// out overrides this and returns immediately — the NATS backend does, with
    /// a timer and a clone of itself, which is the shape the default cannot
    /// express.
    ///
    /// The body used to be `Ok(())`, while this comment promised a timer that
    /// published when it fired. Every backend that did not override it
    /// therefore dropped the message and reported success — the one outcome no
    /// caller can see, from the one method whose whole job is to not lose it.
    async fn delay_publish(&self, subject: &str, payload: &[u8], delay_ms: u64) -> SFResult<()> {
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        self.publish(subject, payload).await
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
