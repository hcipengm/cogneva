//! Redis Streams-backed [`MessageBackend`] implementation.

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};

use cog_core::{MessageBackend, MessageStream, SFError, SFResult};

/// Redis Streams-backed [`MessageBackend`].
pub struct RedisMessageBackend {
    client: redis::Client,
    connection: MultiplexedConnection,
}

impl RedisMessageBackend {
    pub async fn new(redis_url: &str) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Redis(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SFError::Redis(e.to_string()))?;
        Ok(Self { client, connection })
    }

    /// Open a dedicated connection for one long-lived subscription.
    /// Blocking XREADGROUP commands serialize on a multiplexed connection:
    /// sharing one connection across N consumers makes every consumer poll at
    /// most once per N x block-time (observed: dozens of system + agent
    /// consumers slowed backlog drain to ~1 message/minute).
    async fn subscribe_connection(&self) -> SFResult<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SFError::Redis(e.to_string()))
    }
}

#[async_trait]
impl MessageBackend for RedisMessageBackend {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        let _: String = self
            .connection
            .clone()
            .xadd(subject, "*", &[("payload", payload)])
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn publish_batch(&self, subject: &str, payloads: &[Vec<u8>]) -> SFResult<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        let mut conn = self.connection.clone();
        let mut pipe = redis::pipe();
        for payload in payloads {
            pipe.cmd("XADD")
                .arg(subject)
                .arg("*")
                .arg("payload")
                .arg(payload.as_slice());
        }
        let _: Vec<String> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        let conn = self.subscribe_connection().await?;
        let subject = subject.to_string();
        let group = group.to_string();

        // XREADGROUP fails with NOGROUP when the group does not exist yet
        // (fresh stream or first consumer after a restart). Group creation is
        // idempotent (BUSYGROUP tolerated), so always ensure it up front.
        if let Err(e) = self.create_consumer_group(&subject, &group).await {
            tracing::warn!(
                "create consumer group failed, trying XREADGROUP anyway: {}",
                e
            );
        }

        // The read loop retries inside the stream instead of yielding Err:
        // try_unfold terminates permanently on Err, and consumer loops treat
        // the following None as a clean exit — one transient Redis error then
        // stalls the consumer group silently until the pod restarts (observed:
        // groups frozen for days while lag piled up).
        let stream = futures::stream::try_unfold((conn, 0u64), move |(mut conn, mut backoff)| {
            let subject = subject.clone();
            let group = group.clone();
            async move {
                loop {
                    match group_read(&mut conn, &subject, &group, ">", 5000).await {
                        Ok(Some(item)) => return Ok(Some((item, (conn, 0)))),
                        Ok(None) => backoff = 0,
                        Err(e) => {
                            tracing::warn!(
                                stream = %subject,
                                "XREADGROUP failed, retrying with backoff: {e}"
                            );
                            backoff = sleep_backoff(backoff).await;
                        }
                    }
                }
            }
        });

        Ok(Box::pin(stream))
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        let conn = self.subscribe_connection().await?;
        let subject = subject.to_string();
        let group = group.to_string();
        let start_id = start_id.to_string();

        // State carries the one-shot start id; the blocking tail reads ">".
        // Errors retry in place for the same reason as `subscribe` above.
        let stream = futures::stream::try_unfold(
            (conn, Some(start_id), 0u64),
            move |(mut conn, mut first_id, mut backoff)| {
                let subject = subject.clone();
                let group = group.clone();
                async move {
                    loop {
                        let id = first_id.as_deref().unwrap_or(">");
                        let block_ms = if first_id.is_some() { 1000 } else { 5000 };
                        match group_read(&mut conn, &subject, &group, id, block_ms).await {
                            Ok(Some(item)) => {
                                return Ok(Some((item, (conn, None, 0))));
                            }
                            Ok(None) => {
                                first_id = None;
                                backoff = 0;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    stream = %subject,
                                    "XREADGROUP failed, retrying with backoff: {e}"
                                );
                                backoff = sleep_backoff(backoff).await;
                            }
                        }
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }

    async fn create_consumer_group(&self, stream: &str, group: &str) -> SFResult<()> {
        let result: Result<(), RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut self.connection.clone())
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(e) if e.code() == Some("BUSYGROUP") => {
                // Group already exists from a previous pod/session; treat as
                // idempotent success so consumers can resume from the last
                // acknowledged ID instead of exiting.
                Ok(())
            }
            Err(e) => Err(SFError::Redis(e.to_string())),
        }
    }

    async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let _: i64 = self
            .connection
            .clone()
            .xack(stream, group, ids)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn claim_pending(
        &self,
        stream: &str,
        group: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> SFResult<Vec<(String, Vec<u8>)>> {
        // 认领对象固定为 subscribe 同款 "consumer-1"，保证认领回来的消息
        // 后续 ack（同组同消费者语义）能对上。
        let opts = redis::streams::StreamAutoClaimOptions::default().count(count);
        let reply: redis::streams::StreamAutoClaimReply = self
            .connection
            .clone()
            .xautoclaim_options(
                stream,
                group,
                "consumer-1",
                min_idle_ms as usize,
                "0-0",
                opts,
            )
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let mut messages = Vec::new();
        for item in reply.claimed {
            if let Some(redis::Value::BulkString(b)) = item.map.get("payload") {
                messages.push((item.id, b.clone()));
            }
        }
        Ok(messages)
    }

    async fn dlq(&self, stream: &str, msg_id: &str, reason: &str) -> SFResult<()> {
        let payload = serde_json::json!({
            "original_id": msg_id,
            "reason": reason,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });
        let bytes = serde_json::to_vec(&payload)?;
        let dlq_stream = format!("{}:dlq", stream);
        let _: String = self
            .connection
            .clone()
            .xadd(&dlq_stream, "*", &[("payload", &bytes as &[u8])])
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }
}

async fn group_read(
    conn: &mut MultiplexedConnection,
    subject: &str,
    group: &str,
    id: &str,
    block_ms: usize,
) -> redis::RedisResult<Option<(String, Vec<u8>)>> {
    let opts = redis::streams::StreamReadOptions::default()
        .group(group, "consumer-1")
        .count(1)
        .block(block_ms);
    let reply = conn.xread_options(&[subject], &[id], &opts).await?;
    Ok(extract_messages(reply).into_iter().next())
}

/// Sleep an exponential backoff (1s→30s cap) and return the next delay.
async fn sleep_backoff(backoff_secs: u64) -> u64 {
    let delay = if backoff_secs == 0 { 1 } else { backoff_secs };
    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    delay.saturating_mul(2).min(30)
}

fn extract_messages(reply: redis::streams::StreamReadReply) -> Vec<(String, Vec<u8>)> {
    let mut messages = Vec::new();
    for stream_key in reply.keys {
        for item in stream_key.ids {
            if let Some(payload) = item.map.get("payload") {
                let bytes = match payload {
                    redis::Value::BulkString(b) => b.clone(),
                    redis::Value::SimpleString(s) => s.as_bytes().to_vec(),
                    _ => continue,
                };
                messages.push((item.id.to_string(), bytes));
            }
        }
    }
    messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Arc;

    async fn pending_count(raw: &mut MultiplexedConnection, stream: &str, group: &str) -> i64 {
        let v: redis::Value = redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .query_async(raw)
            .await
            .expect("XPENDING");
        match v {
            redis::Value::Array(a) => match a.first() {
                Some(redis::Value::Int(n)) => *n,
                _ => 0,
            },
            _ => 0,
        }
    }

    #[tokio::test]
    async fn test_redis_publish_and_subscribe() {
        let redis_url = std::env::var("COGNEVA_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let backend = match RedisMessageBackend::new(&redis_url).await {
            Ok(b) => b,
            Err(_) => {
                eprintln!("SKIP: Redis not available");
                return;
            }
        };

        if backend
            .create_consumer_group("cog-test:pubsub", "test-group")
            .await
            .is_err()
        {
            eprintln!("SKIP: Redis XGROUP CREATE failed");
            return;
        }

        backend.publish("cog-test:pubsub", b"hello").await.unwrap();
        let mut stream = backend
            .subscribe("cog-test:pubsub", "test-group")
            .await
            .unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await;

        match result {
            Ok(Some(Ok((_, bytes)))) => assert_eq!(bytes, b"hello"),
            _ => eprintln!("SKIP: Redis stream read timed out or failed"),
        }
    }

    #[tokio::test]
    async fn test_ack_through_dyn_trait_clears_pending() {
        // Regression: ack was an inherent method while the trait supplied a
        // no-op default, so calls through `dyn MessageBackend` compiled fine
        // but never issued XACK — pending messages were redelivered forever.
        let redis_url = std::env::var("COGNEVA_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let raw_client = redis::Client::open(redis_url.as_str()).expect("redis client");
        let mut raw = match raw_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => {
                eprintln!("SKIP: Redis not available");
                return;
            }
        };
        let stream = "cog-test:dyn-ack";
        let group = "dyn-ack-group";
        let _: i64 = redis::cmd("DEL")
            .arg(stream)
            .query_async(&mut raw)
            .await
            .expect("del");

        let backend: Arc<dyn MessageBackend> =
            Arc::new(RedisMessageBackend::new(&redis_url).await.unwrap());

        backend.publish(stream, b"one").await.unwrap();
        backend.create_consumer_group(stream, group).await.unwrap();
        let mut sub = backend.subscribe(stream, group).await.unwrap();
        let (id, bytes) =
            match tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await {
                Ok(Some(Ok(msg))) => msg,
                _ => {
                    eprintln!("SKIP: Redis stream read timed out or failed");
                    return;
                }
            };
        assert_eq!(bytes, b"one");
        assert_eq!(pending_count(&mut raw, stream, group).await, 1);

        backend.ack(stream, group, &[id]).await.unwrap();
        assert_eq!(pending_count(&mut raw, stream, group).await, 0);

        let _: i64 = redis::cmd("DEL")
            .arg(stream)
            .query_async(&mut raw)
            .await
            .expect("del");
    }

    #[tokio::test]
    async fn test_blocked_subscription_does_not_head_of_line_block_others() {
        // Regression: subscriptions shared one multiplexed connection, and a
        // blocked XREADGROUP on an empty stream stalled every other consumer
        // on the same connection until its block expired.
        let redis_url = std::env::var("COGNEVA_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let raw_client = redis::Client::open(redis_url.as_str()).expect("redis client");
        let mut raw = match raw_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => {
                eprintln!("SKIP: Redis not available");
                return;
            }
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let idle_stream = format!("cog-test:hol-idle-{suffix}");
        let busy_stream = format!("cog-test:hol-busy-{suffix}");
        let idle_group = format!("hol-idle-{suffix}");
        let busy_group = format!("hol-busy-{suffix}");
        for k in [&idle_stream, &busy_stream] {
            let _: i64 = redis::cmd("DEL").arg(k).query_async(&mut raw).await.unwrap();
        }

        let backend: Arc<dyn MessageBackend> =
            Arc::new(RedisMessageBackend::new(&redis_url).await.unwrap());
        backend
            .create_consumer_group(&idle_stream, &idle_group)
            .await
            .unwrap();
        backend
            .create_consumer_group(&busy_stream, &busy_group)
            .await
            .unwrap();

        // Long-lived blocked read on the empty stream.
        let idle_backend = backend.clone();
        let idle_stream_task = idle_stream.clone();
        let idle_group_task = idle_group.clone();
        let idle_handle = tokio::spawn(async move {
            let mut sub = idle_backend
                .subscribe(&idle_stream_task, &idle_group_task)
                .await
                .unwrap();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(8), sub.next()).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // A message on the other stream must arrive immediately, not wait for
        // the idle reader's 5s block to expire.
        backend.publish(&busy_stream, b"b").await.unwrap();
        let mut busy_sub = backend.subscribe(&busy_stream, &busy_group).await.unwrap();
        match tokio::time::timeout(std::time::Duration::from_secs(3), busy_sub.next()).await {
            Ok(Some(Ok((_, bytes)))) => assert_eq!(bytes, b"b"),
            other => panic!("blocked subscription head-of-line blocked peer: {other:?}"),
        }

        idle_handle.abort();
        for k in [&idle_stream, &busy_stream] {
            let _: i64 = redis::cmd("DEL").arg(k).query_async(&mut raw).await.unwrap();
        }
    }
}
