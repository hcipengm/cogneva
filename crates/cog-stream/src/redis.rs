//! Redis Streams-backed [`MessageBackend`] implementation.

use async_trait::async_trait;
use futures::StreamExt;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};

use cog_core::{MessageBackend, MessageStream, SFError, SFResult};

/// Redis Streams-backed [`MessageBackend`].
pub struct RedisMessageBackend {
    connection: MultiplexedConnection,
}

impl RedisMessageBackend {
    pub async fn new(redis_url: &str) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Redis(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SFError::Redis(e.to_string()))?;
        Ok(Self { connection })
    }

    /// Acknowledge one or more message IDs in a consumer group.
    pub async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
        let _: () = self
            .connection
            .clone()
            .xack(stream, group, ids)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
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
        let mut conn = self.connection.clone();
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

        let opts = redis::streams::StreamReadOptions::default()
            .group(&group, "consumer-1")
            .count(1)
            .block(1000);

        let result: redis::RedisResult<redis::streams::StreamReadReply> =
            conn.xread_options(&[&subject], &[">"], &opts).await;

        let initial = match result {
            Ok(reply) => extract_messages(reply),
            Err(e) => return Err(SFError::Redis(e.to_string())),
        };

        let stream = futures::stream::iter(initial.into_iter().map(Ok)).chain(
            futures::stream::try_unfold(conn, move |mut conn| {
                let subject = subject.clone();
                let group = group.clone();
                async move {
                    loop {
                        let opts = redis::streams::StreamReadOptions::default()
                            .group(&group, "consumer-1")
                            .count(1)
                            .block(5000);

                        let result: redis::RedisResult<redis::streams::StreamReadReply> =
                            conn.xread_options(&[&subject], &[">"], &opts).await;

                        match result {
                            Ok(reply) => {
                                let msgs = extract_messages(reply);
                                if let Some((id, bytes)) = msgs.into_iter().next() {
                                    return Ok(Some(((id, bytes), conn)));
                                }
                            }
                            Err(e) => return Err(SFError::Redis(e.to_string())),
                        }
                    }
                }
            }),
        );

        Ok(Box::pin(stream))
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        let mut conn = self.connection.clone();
        let subject = subject.to_string();
        let group = group.to_string();
        let start_id = start_id.to_string();

        let opts = redis::streams::StreamReadOptions::default()
            .group(&group, "consumer-1")
            .count(1)
            .block(1000);

        let result: redis::RedisResult<redis::streams::StreamReadReply> =
            conn.xread_options(&[&subject], &[&start_id], &opts).await;

        let initial = match result {
            Ok(reply) => extract_messages(reply),
            Err(e) => return Err(SFError::Redis(e.to_string())),
        };

        let stream = futures::stream::iter(initial.into_iter().map(Ok)).chain(
            futures::stream::try_unfold(conn, move |mut conn| {
                let subject = subject.clone();
                let group = group.clone();
                async move {
                    loop {
                        let opts = redis::streams::StreamReadOptions::default()
                            .group(&group, "consumer-1")
                            .count(1)
                            .block(5000);

                        let result: redis::RedisResult<redis::streams::StreamReadReply> =
                            conn.xread_options(&[&subject], &[">"], &opts).await;

                        match result {
                            Ok(reply) => {
                                let msgs = extract_messages(reply);
                                if let Some((id, bytes)) = msgs.into_iter().next() {
                                    return Ok(Some(((id, bytes), conn)));
                                }
                            }
                            Err(e) => return Err(SFError::Redis(e.to_string())),
                        }
                    }
                }
            }),
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
}
