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
//! incarnation was handed and never acked -- when it starts, and again on a
//! backstop cadence for the one that was still too young to reclaim at start --
//! and it keeps running across a handler error rather than treating one failed
//! command as the end of the inbox.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

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

/// How often a running consumer looks back at its own pending list.
///
/// A reclaim pass performed only at start misses precisely the message it
/// exists for: one written seconds before the previous incarnation died. At the
/// moment the new incarnation starts that message is younger than
/// [`INBOX_RECLAIM_MIN_IDLE_MS`], so the one pass skips it -- and nothing ever
/// asks again, because a pending entry is not lag and no reading shows it. A
/// backstop pass turns "never" into "one interval later".
// cap: bound — 与回收空闲阈值同量级；更小是白问一遍，更大只让那条消息多等一会儿
const INBOX_RECLAIM_INTERVAL: Duration = Duration::from_secs(60);

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
    pub async fn run<F, Fut>(self, handler: F) -> SFResult<()>
    where
        F: FnMut(cog_core::InboxMessage) -> Fut,
        Fut: std::future::Future<Output = SFResult<()>>,
    {
        self.run_with_reclaim_interval(handler, INBOX_RECLAIM_INTERVAL)
            .await
    }

    /// [`Self::run`] with the backstop cadence passed in.
    ///
    /// The cadence decides how long a message that became reclaimable after
    /// start waits; it is a parameter so a test can watch the backstop pass
    /// happen without waiting a minute for it.
    pub async fn run_with_reclaim_interval<F, Fut>(
        self,
        mut handler: F,
        reclaim_interval: Duration,
    ) -> SFResult<()>
    where
        F: FnMut(cog_core::InboxMessage) -> Fut,
        Fut: std::future::Future<Output = SFResult<()>>,
    {
        let group_name = format!("agent-{}", self.agent_id);

        self.backend
            .create_consumer_group(&self.inbox_stream, &group_name)
            .await?;

        // Commands this incarnation has already handed to the handler. A
        // message that ran and failed stays pending on purpose, and the
        // backstop pass would otherwise hand it over again every interval for
        // as long as the process lives -- retrying a command whose side effects
        // already happened, at full token cost, more often than a restart would.
        // It is retried when this consumer next starts, which is the same
        // guarantee as before.
        let mut handled: HashSet<String> = HashSet::new();

        // Take back what a previous incarnation of this consumer was handed and
        // never acked, before reading anything new. The read below asks only
        // for messages never delivered (`>`), so without this a command that
        // arrived while the process was dying stays in the pending list for as
        // long as the group exists: accepted by the producer, executed by
        // nobody, and absent from every lag reading, because a pending entry is
        // not lag.
        let reclaimed = self
            .reclaim_pending(&group_name, &mut handler, &mut handled)
            .await?;
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

        // The first tick is a full period out: the starting pass above already
        // asked once, and asking again immediately would only repeat it.
        let mut backstop = tokio::time::interval_at(
            tokio::time::Instant::now() + reclaim_interval,
            reclaim_interval,
        );
        // The read is raced against the backstop, so a tick can drop a read that
        // was already on the wire. That is safe here: a reply the store handed
        // out just before the drop is an entry in the group's pending list, and
        // taking pending entries back is exactly what the next pass is for.
        loop {
            tokio::select! {
                next = stream.next() => {
                    let Some(result) = next else {
                        break;
                    };
                    let (msg_id, bytes) = result?;
                    if !self.deliver(&msg_id, &bytes, &group_name, &mut handler).await {
                        // Only what was left pending can come back from a
                        // reclaim, so only that is worth remembering: a set
                        // that grew with every message ever read would grow
                        // for as long as the process stays up, and this one is
                        // meant to stay up for weeks.
                        handled.insert(msg_id);
                    }
                }
                _ = backstop.tick() => {
                    match self
                        .reclaim_pending(&group_name, &mut handler, &mut handled)
                        .await
                    {
                        Ok(0) => {}
                        Ok(messages) => {
                            tracing::warn!(
                                agent_id = %self.agent_id,
                                messages,
                                "Took back inbox messages that became reclaimable after start"
                            );
                        }
                        Err(e) => {
                            // A failed backstop pass is not the end of the
                            // loop: the next tick asks again, and the messages
                            // it would have found are still pending.
                            tracing::warn!(
                                agent_id = %self.agent_id,
                                error = %e,
                                "Backstop reclaim failed; trying again next interval"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Hand every reclaimable pending message to the handler, skipping the ones
    /// this incarnation has already handed over.
    ///
    /// Returns how many messages the handler ran. A backend without a reclaim
    /// path yields zero: its delivery guarantees are its own to state.
    async fn reclaim_pending<F, Fut>(
        &self,
        group_name: &str,
        handler: &mut F,
        handled: &mut HashSet<String>,
    ) -> SFResult<usize>
    where
        F: FnMut(cog_core::InboxMessage) -> Fut,
        Fut: std::future::Future<Output = SFResult<()>>,
    {
        let mut reclaimed = 0usize;
        loop {
            let Some(batch) = self
                .backend
                .claim_pending(
                    &self.inbox_stream,
                    group_name,
                    INBOX_RECLAIM_MIN_IDLE_MS,
                    INBOX_RECLAIM_BATCH,
                )
                .await?
            else {
                break;
            };
            let full = batch.len() == INBOX_RECLAIM_BATCH;
            let mut fresh = 0usize;
            for (msg_id, bytes) in batch {
                if !handled.insert(msg_id.clone()) {
                    continue;
                }
                fresh += 1;
                self.deliver(&msg_id, &bytes, group_name, handler).await;
                reclaimed += 1;
            }
            // A full batch of commands this incarnation already ran would
            // otherwise be asked for again forever: the skip above claims
            // nothing, so the batch never shrinks.
            if !full || fresh == 0 {
                break;
            }
        }
        Ok(reclaimed)
    }

    /// Hand one message to the handler and ack it, or leave it pending.
    ///
    /// Nothing here ends the loop. A handler that fails used to propagate out
    /// of `run`, which stopped the consumer for good: the agent stayed
    /// registered and kept beating while the group it owned read nothing, so a
    /// dead inbox looked exactly like a quiet one. A failed command now costs
    /// its own delivery and no more -- it stays pending, is reported, and is
    /// retried when this consumer next starts, not by the backstop pass of the
    /// incarnation that already ran it. Returns whether the message was
    /// settled (finished and acknowledged, or poisoned and handed to the dead
    /// letter queue); anything left pending returns false.
    async fn deliver<F, Fut>(
        &self,
        msg_id: &str,
        bytes: &[u8],
        group_name: &str,
        handler: &mut F,
    ) -> bool
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
                    // Settled: the parse can never succeed, so handing the same
                    // bytes over again would only dead-letter them twice.
                    return true;
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
            return false;
        }
        if let Err(e) = self
            .backend
            .ack(&self.inbox_stream, group_name, &[msg_id.to_string()])
            .await
        {
            tracing::warn!(msg_id = %msg_id, "Failed to ack inbox message: {e}");
            // The handler finished but the group still holds the message: it was
            // settled as far as this incarnation can tell, and re-running it
            // would be a second execution of a turn that already happened.
            return false;
        }
        crate::observable::global_observable().record_inbox_delivered(kind);
        true
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
