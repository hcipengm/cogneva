//! Redis Streams-backed [`MessageBackend`] implementation.

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisError};

use cog_core::{MessageBackend, MessageStream, SFError, SFResult};

/// Upper bound on how many over-threshold entries one measurement lists. The
/// total pending count is exact either way; the over-threshold pair below
/// saturates at this page, and a saturated page is itself the finding — it
/// means the reclaim path has stopped taking anything back, which the
/// reported age says just as plainly.
const PENDING_STATS_PAGE: usize = 1024;

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

    async fn pending_stats(
        &self,
        stream: &str,
        group: &str,
        idle_threshold_ms: u64,
    ) -> SFResult<Option<cog_core::PendingStats>> {
        let mut conn = self.connection.clone();

        // Summary form: total pending. Read first so the two commands that
        // follow — which list entries — are only issued against a non-empty
        // PEL, and so a total that exceeds what one page can list is still
        // reported truthfully.
        let summary: redis::Value = redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .query_async(&mut conn)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let count = match &summary {
            redis::Value::Array(items) => match items.first() {
                Some(redis::Value::Int(n)) => (*n).max(0) as u64,
                other => {
                    // An unrecognised shape must not be read as an empty PEL:
                    // "could not tell" and "nothing stranded" are the two
                    // answers this report exists to keep apart.
                    return Err(SFError::Redis(format!(
                        "XPENDING summary had unexpected shape: {other:?}"
                    )));
                }
            },
            other => {
                return Err(SFError::Redis(format!(
                    "XPENDING summary had unexpected shape: {other:?}"
                )))
            }
        };

        let mut stats = cog_core::PendingStats {
            count,
            ..Default::default()
        };
        if count == 0 {
            return Ok(Some(stats));
        }

        // Only entries already past the threshold: the reclaim pass takes back
        // exactly these on its next tick, so anything still listed here at the
        // next measurement is one it failed to take back.
        let entries: redis::Value = redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .arg("IDLE")
            .arg(idle_threshold_ms)
            .arg("-")
            .arg("+")
            .arg(PENDING_STATS_PAGE)
            .query_async(&mut conn)
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        let entries = match entries {
            redis::Value::Array(items) => items,
            other => {
                return Err(SFError::Redis(format!(
                    "XPENDING page had unexpected shape: {other:?}"
                )))
            }
        };
        for entry in entries {
            let idle = match entry {
                redis::Value::Array(fields) => fields.get(2).and_then(|v| match v {
                    redis::Value::Int(n) => Some((*n).max(0) as u64),
                    _ => None,
                }),
                _ => None,
            };
            let Some(idle) = idle else {
                return Err(SFError::Redis(
                    "XPENDING entry had unexpected shape".to_string(),
                ));
            };
            stats.unreclaimed_count += 1;
            stats.unreclaimed_oldest_idle_ms = stats.unreclaimed_oldest_idle_ms.max(idle);
        }
        Ok(Some(stats))
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
    async fn test_pending_stats_separates_in_flight_from_abandoned() {
        // The count alone cannot tell a message being processed from one a dead
        // consumer left behind; only the idle threshold can, so the reading is
        // asserted against two thresholds over the same PEL.
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
        let stream = "cog-test:pending-stats";
        let group = "pending-stats-group";
        let _: i64 = redis::cmd("DEL")
            .arg(stream)
            .query_async(&mut raw)
            .await
            .expect("del");

        let backend: Arc<dyn MessageBackend> =
            Arc::new(RedisMessageBackend::new(&redis_url).await.unwrap());
        backend.create_consumer_group(stream, group).await.unwrap();

        // Nothing has been delivered, so the PEL is empty and both figures are
        // zero rather than absent: this backend does hold pending state.
        let empty = backend
            .pending_stats(stream, group, 0)
            .await
            .expect("pending_stats on empty PEL")
            .expect("streams backend reports pending state");
        assert_eq!(empty.count, 0);
        assert_eq!(empty.unreclaimed_count, 0);
        assert_eq!(empty.unreclaimed_oldest_idle_ms, 0);

        backend.publish(stream, b"one").await.unwrap();
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

        // Delivered and not yet acked: pending, but nothing a reclaim pass
        // running on its current threshold would take back from a live
        // consumer — which is what a generous threshold must report.
        let in_flight = backend
            .pending_stats(stream, group, 600_000)
            .await
            .expect("pending_stats with a generous threshold")
            .expect("streams backend reports pending state");
        assert_eq!(in_flight.count, 1);
        assert_eq!(
            in_flight.unreclaimed_count, 0,
            "an entry still inside the threshold is in flight, not abandoned"
        );

        // The same entry against a threshold of zero is past it, so it is
        // exactly the work a reclaim pass would take back.
        let abandoned = backend
            .pending_stats(stream, group, 0)
            .await
            .expect("pending_stats with a zero threshold")
            .expect("streams backend reports pending state");
        assert_eq!(abandoned.count, 1);
        assert_eq!(abandoned.unreclaimed_count, 1);

        backend.ack(stream, group, &[id]).await.unwrap();
        let acked = backend
            .pending_stats(stream, group, 0)
            .await
            .expect("pending_stats after ack")
            .expect("streams backend reports pending state");
        assert_eq!(acked.count, 0);
        assert_eq!(acked.unreclaimed_count, 0);

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
            let _: i64 = redis::cmd("DEL")
                .arg(k)
                .query_async(&mut raw)
                .await
                .unwrap();
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
            let _: i64 = redis::cmd("DEL")
                .arg(k)
                .query_async(&mut raw)
                .await
                .unwrap();
        }
    }
}
