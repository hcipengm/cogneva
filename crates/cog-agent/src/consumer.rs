//! Agent inbox consumer backed by Redis Streams with consumer groups.
//! Provides [`AgentInboxConsumer`] for reliable, at-least-once delivery
//! of messages to an Agent via Redis Streams (`XREADGROUP`, `XACK`, `XCLAIM`).

use chrono::{DateTime, Utc};
use cog_core::{SFError, SFResult};
use futures::StreamExt;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisError};
use serde_json::Value;

/// A single message read from the Agent's inbox stream.
#[derive(Debug, Clone, PartialEq)]
pub struct InboxMessage {
    /// Redis Stream entry ID (e.g. `"1699999999999-0"`).
    pub id: String,
    /// Deserialised JSON payload.
    pub payload: Value,
    /// UTC timestamp when the message was received by this consumer.
    pub received_at: DateTime<Utc>,
}

/// Redis Streams consumer for an Agent inbox.
/// Each Agent gets its own consumer name inside a shared consumer group,
/// enabling load-balancing and automatic failover via `XCLAIM`.
pub struct AgentInboxConsumer {
    conn: ConnectionManager,
    stream_name: String,
    group_name: String,
    consumer_name: String,
}

impl AgentInboxConsumer {
    /// Create a new consumer.
    /// The caller is responsible for ensuring `redis_url` is reachable.
    /// Use [`AgentInboxConsumer::create_group_if_not_exists`] before the
    /// first call to `consume_batch`.
    pub async fn new(
        redis_url: &str,
        stream_name: impl Into<String>,
        group_name: impl Into<String>,
        consumer_name: impl Into<String>,
    ) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Redis(e.to_string()))?;
        let conn = cog_redis::connect(&client)
            .await
            .map_err(|e| SFError::Redis(e.to_string()))?;
        Ok(Self {
            conn,
            stream_name: stream_name.into(),
            group_name: group_name.into(),
            consumer_name: consumer_name.into(),
        })
    }

    /// Create the consumer group on the stream if it does not already exist.
    /// Uses `MKSTREAM` so the stream is created automatically when empty.
    pub async fn create_group_if_not_exists(&mut self) -> SFResult<()> {
        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream_name)
            .arg(&self.group_name)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut self.conn)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    /// Read up to `count` new messages from the stream via `XREADGROUP`.
    /// `block_ms` controls how long to wait when no messages are available.
    /// Pass `0` to block indefinitely.
    pub async fn consume_batch(
        &mut self,
        count: usize,
        block_ms: usize,
    ) -> SFResult<Vec<InboxMessage>> {
        let opts = redis::streams::StreamReadOptions::default()
            .group(&self.group_name, &self.consumer_name)
            .count(count)
            .block(block_ms);

        let reply: redis::streams::StreamReadReply = self
            .conn
            .xread_options(&[&self.stream_name], &[">"], &opts)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;

        let now = Utc::now();
        let mut messages = Vec::new();

        for stream_key in reply.keys {
            for item in stream_key.ids {
                if let Some(payload) = item.map.get("payload") {
                    let payload_str = match payload {
                        redis::Value::BulkString(bytes) => {
                            String::from_utf8_lossy(bytes).to_string()
                        }
                        redis::Value::SimpleString(s) => s.clone(),
                        _ => continue,
                    };
                    let value: Value =
                        serde_json::from_str(&payload_str).map_err(SFError::Serialization)?;
                    messages.push(InboxMessage {
                        id: item.id.to_string(),
                        payload: value,
                        received_at: now,
                    });
                }
            }
        }

        Ok(messages)
    }

    /// Acknowledge a message so it is not redelivered to this group.
    pub async fn ack(&mut self, message_id: &str) -> SFResult<()> {
        let _: () = self
            .conn
            .xack(&self.stream_name, &self.group_name, &[message_id])
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    /// Claim messages that have been idle for at least `min_idle_ms` milliseconds.
    /// Useful for failover: when another consumer crashes, its pending
    /// messages can be claimed by a replacement consumer after a timeout.
    /// Returns the claimed messages (same format as [`Self::consume_batch`]).
    pub async fn claim_stale(
        &mut self,
        min_idle_ms: usize,
        count: usize,
    ) -> SFResult<Vec<InboxMessage>> {
        // 1. Inspect pending entries for this consumer group.
        let pending: redis::streams::StreamPendingCountReply = self
            .conn
            .xpending_count(&self.stream_name, &self.group_name, "-", "+", count)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;

        let pending_ids: Vec<String> = pending
            .ids
            .into_iter()
            .filter(|e| e.last_delivered_ms >= min_idle_ms)
            .map(|e| e.id)
            .collect();

        if pending_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Claim the idle entries.
        let claim_opts = redis::streams::StreamClaimOptions::default().idle(min_idle_ms);
        let claim_reply: redis::streams::StreamClaimReply = self
            .conn
            .xclaim_options(
                &self.stream_name,
                &self.group_name,
                &self.consumer_name,
                min_idle_ms,
                &pending_ids,
                claim_opts,
            )
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;

        let now = Utc::now();
        let mut messages = Vec::new();

        for item in claim_reply.ids {
            if let Some(payload) = item.map.get("payload") {
                let payload_str = match payload {
                    redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    redis::Value::SimpleString(s) => s.clone(),
                    _ => continue,
                };
                let value: Value =
                    serde_json::from_str(&payload_str).map_err(SFError::Serialization)?;
                messages.push(InboxMessage {
                    id: item.id.to_string(),
                    payload: value,
                    received_at: now,
                });
            }
        }

        Ok(messages)
    }
}

/// Generic agent consumer backed by any [`cog_core::MessageBackend`] implementation.
/// This type was originally in `cog-core` but has been moved to `cog-agent`
/// because it is a concrete implementation rather than a trait definition.
pub struct AgentConsumer {
    agent_id: String,
    backend: std::sync::Arc<dyn cog_core::MessageBackend>,
    inbox_stream: String,
}

impl AgentConsumer {
    /// Create a new consumer for the given agent ID.
    pub fn new(
        agent_id: impl Into<String>,
        backend: std::sync::Arc<dyn cog_core::MessageBackend>,
    ) -> Self {
        let agent_id = agent_id.into();
        let inbox_stream = format!("orchestrator:agent:{}:inbox", agent_id);
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
    pub async fn run<F, Fut>(self, mut handler: F) -> cog_core::SFResult<()>
    where
        F: FnMut(cog_core::InboxMessage) -> Fut,
        Fut: std::future::Future<Output = cog_core::SFResult<()>>,
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
                    tracing::warn!(msg_id = %msg_id, "Failed to deserialize InboxMessage: {e}");
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
    ) -> cog_core::SFResult<()> {
        let inbox_stream = format!("orchestrator:agent:{}:inbox", to_agent_id);
        let payload = serde_json::to_vec(&message).map_err(cog_core::SFError::Serialization)?;
        backend.publish(&inbox_stream, &payload).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Redis endpoint for the suite. CI supplies one through a service
    /// container; a developer machine usually has one on the default port.
    ///
    /// Callers must tolerate its absence: this suite also runs inside the
    /// evolution sandbox, which has no Redis and where a red suite is not a
    /// statement about the code under test.
    fn test_redis_url() -> String {
        std::env::var("COGNEVA_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
    }

    /// Connect a consumer, or return `None` when no Redis is reachable so the
    /// test can skip instead of panicking.
    async fn open_test_consumer(
        stream: String,
        group: String,
        consumer: String,
    ) -> Option<AgentInboxConsumer> {
        open_test_consumer_at(&test_redis_url(), stream, group, consumer).await
    }

    async fn open_test_consumer_at(
        redis_url: &str,
        stream: String,
        group: String,
        consumer: String,
    ) -> Option<AgentInboxConsumer> {
        match AgentInboxConsumer::new(redis_url, stream, group, consumer).await {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("SKIP: Redis not available ({e})");
                None
            }
        }
    }

    /// Build a consumer pointing at the local Redis used by CI.
    /// Cleans up any pre-existing stream/group to avoid BUSYGROUP collisions.
    async fn make_test_consumer(suffix: &str) -> Option<AgentInboxConsumer> {
        let stream = format!("sf:test:inbox:{}", suffix);
        let group = format!("sf:test:group:{}", suffix);
        let consumer = format!("sf:test:consumer:{}", suffix);
        let mut c = open_test_consumer(stream.clone(), group.clone(), consumer).await?;
        // Best-effort cleanup of stale stream from a previous aborted run.
        let _: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg(&group)
            .query_async(&mut c.conn)
            .await;
        let _: redis::RedisResult<i64> = redis::cmd("DEL")
            .arg(&stream)
            .query_async(&mut c.conn)
            .await;
        Some(c)
    }

    /// Helper to push a raw JSON payload onto a stream.
    async fn push_payload(conn: &mut ConnectionManager, stream: &str, payload: &str) {
        let _: String = conn
            .xadd(stream, "*", &[("payload", payload)])
            .await
            .expect("xadd should succeed");
    }

    /// 沙盒里没有 Redis，判据不能因为连不上就 panic——panic 会被读成
    /// "变更有问题"，而它其实是"这个部署没有这个东西"。
    #[tokio::test]
    async fn unreachable_redis_skips_instead_of_panicking() {
        let consumer = open_test_consumer_at(
            "redis://127.0.0.1:1",
            "sf:test:inbox:unreachable".into(),
            "sf:test:group:unreachable".into(),
            "sf:test:consumer:unreachable".into(),
        )
        .await;
        assert!(consumer.is_none());
    }

    #[tokio::test]
    async fn test_create_group_and_consume() {
        let Some(mut consumer) = make_test_consumer("create_group").await else {
            return;
        };

        // Ensure group exists.
        consumer
            .create_group_if_not_exists()
            .await
            .expect("create_group_if_not_exists should succeed");

        // Publish a message.
        let payload = serde_json::json!({"task": "hello", "priority": 1});
        push_payload(
            &mut consumer.conn.clone(),
            &consumer.stream_name,
            &payload.to_string(),
        )
        .await;

        // Consume it.
        let msgs = consumer
            .consume_batch(10, 2000)
            .await
            .expect("consume_batch should succeed");

        assert_eq!(msgs.len(), 1, "expected exactly one message");
        assert_eq!(msgs[0].payload, payload);

        // Acknowledge.
        consumer.ack(&msgs[0].id).await.expect("ack should succeed");
    }

    #[tokio::test]
    async fn test_consume_empty_non_blocking() {
        let Some(mut consumer) = make_test_consumer("empty").await else {
            return;
        };
        consumer.create_group_if_not_exists().await.unwrap();

        let msgs = consumer
            .consume_batch(10, 1)
            .await
            .expect("consume_batch should succeed even when empty");
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn test_claim_stale_no_pending() {
        let Some(mut consumer) = make_test_consumer("claim_none").await else {
            return;
        };
        consumer.create_group_if_not_exists().await.unwrap();

        let claimed = consumer
            .claim_stale(100, 10)
            .await
            .expect("claim_stale should succeed when nothing pending");
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn test_claim_stale_recovers_message() {
        let Some(mut c1) = make_test_consumer("claim_recovery").await else {
            return;
        };
        c1.create_group_if_not_exists().await.unwrap();

        // Publish a message.
        let payload = serde_json::json!({"task": "recover_me"});
        push_payload(&mut c1.conn.clone(), &c1.stream_name, &payload.to_string()).await;

        // c1 reads but does NOT ack.
        let msgs = c1.consume_batch(10, 2000).await.unwrap();
        assert_eq!(msgs.len(), 1);

        // Create a second consumer in the *same* group with a different name.
        let Some(mut c2) = open_test_consumer(
            c1.stream_name.clone(),
            c1.group_name.clone(),
            "sf:test:consumer:claim_recovery_2".into(),
        )
        .await
        else {
            return;
        };

        // Claim with a very low idle threshold so the message is eligible.
        // Small sleep to ensure the message is actually idle.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let claimed = c2.claim_stale(1, 10).await.unwrap();
        assert_eq!(claimed.len(), 1, "expected the stale message to be claimed");
        assert_eq!(claimed[0].payload, payload);

        // Ack from c2.
        c2.ack(&claimed[0].id).await.unwrap();
    }
}
