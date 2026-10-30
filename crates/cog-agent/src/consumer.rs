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
//! and a transport that brokers a persistent queue hands it back. Both halves of
//! that sentence are the loop's own work: it reclaims what a previous
//! incarnation was handed and never acked when it starts, and it keeps running
//! across a handler error rather than treating one failed command as the end of
//! the inbox.

use std::sync::Arc;

use cog_core::SFResult;
use futures::StreamExt;

/// How long a delivered-but-unacked message must sit before a starting
/// consumer takes it back.
///
/// Only this consumer reads the group -- the group name carries the agent id,
/// and an agent id names one process -- so at start nothing else can be holding
/// a message of ours. The threshold covers the narrow case where a producer
/// wrote just as the previous incarnation was dying: too small and it could
/// take back a message that a still-draining process is finishing; nothing
/// waits on this value, so it is chosen for safety rather than latency.
// cap: bound — 为的是不把一条还在排空的进程正在结束的消息抢回来；没有任何人在等这个值，所以按安全取而不是按延迟取
const INBOX_RECLAIM_MIN_IDLE_MS: u64 = 60_000;

/// How many pending messages one reclaim call asks for.
// cap: bound — 一次回收调用要多少条待处理消息
const INBOX_RECLAIM_BATCH: usize = 64;

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

        // Take back what a previous incarnation of this consumer was handed and
        // never acked, before reading anything new. The read below asks only
        // for messages never delivered (`>`), so without this a command that
        // arrived while the process was dying stays in the pending list for as
        // long as the group exists: accepted by the producer, executed by
        // nobody, and absent from every lag reading, because a pending entry is
        // not lag.
        let mut reclaimed = 0usize;
        loop {
            let Some(batch) = self
                .backend
                .claim_pending(
                    &self.inbox_stream,
                    &group_name,
                    INBOX_RECLAIM_MIN_IDLE_MS,
                    INBOX_RECLAIM_BATCH,
                )
                .await?
            else {
                // Backend has no reclaim path; its delivery guarantees are its
                // own to state.
                break;
            };
            let full = batch.len() == INBOX_RECLAIM_BATCH;
            for (msg_id, bytes) in batch {
                self.deliver(&msg_id, &bytes, &group_name, &mut handler)
                    .await;
                reclaimed += 1;
            }
            if !full {
                break;
            }
        }
        if reclaimed > 0 {
            tracing::warn!(
                agent_id = %self.agent_id,
                messages = reclaimed,
                "Took back inbox messages a previous consumer never acknowledged"
            );
        }

        let mut stream = self
            .backend
            .subscribe(&self.inbox_stream, &group_name)
            .await?;

        while let Some(result) = stream.next().await {
            let (msg_id, bytes) = result?;
            self.deliver(&msg_id, &bytes, &group_name, &mut handler)
                .await;
        }

        Ok(())
    }

    /// Hand one message to the handler and ack it, or leave it pending.
    ///
    /// Nothing here ends the loop. A handler that fails used to propagate out
    /// of `run`, which stopped the consumer for good: the agent stayed
    /// registered and kept beating while the group it owned read nothing, so a
    /// dead inbox looked exactly like a quiet one. A failed command now costs
    /// its own delivery and no more -- it stays pending, is reported, and is
    /// retried when this consumer next starts.
    async fn deliver<F, Fut>(&self, msg_id: &str, bytes: &[u8], group_name: &str, handler: &mut F)
    where
        F: FnMut(cog_core::InboxMessage) -> Fut,
        Fut: std::future::Future<Output = SFResult<()>>,
    {
        let message: cog_core::InboxMessage =
            match serde_json::from_slice(bytes).map_err(cog_core::SFError::Serialization) {
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
                            msg_id,
                            bytes,
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
                        .ack(&self.inbox_stream, group_name, &[msg_id.to_string()])
                        .await
                    {
                        tracing::warn!(msg_id = %msg_id, "Failed to ack poison inbox message: {e}");
                    }
                    crate::observable::global_observable().record_inbox_poisoned();
                    return;
                }
            };
        let kind = crate::observable::inbox_kind(&message);
        if let Err(e) = handler(message).await {
            // Left pending on purpose: the handler did not finish, so acking
            // would record a run that never happened. The retry waits for the
            // next start of this consumer, so the counter -- not the log line
            // -- is what an operator can still see an hour later.
            crate::observable::global_observable().record_inbox_undelivered(kind);
            tracing::warn!(
                agent_id = %self.agent_id,
                msg_id = %msg_id,
                kind,
                error = %e,
                "Inbox command failed; the message stays pending for retry"
            );
            return;
        }
        if let Err(e) = self
            .backend
            .ack(&self.inbox_stream, group_name, &[msg_id.to_string()])
            .await
        {
            tracing::warn!(msg_id = %msg_id, "Failed to ack inbox message: {e}");
            return;
        }
        crate::observable::global_observable().record_inbox_delivered(kind);
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
