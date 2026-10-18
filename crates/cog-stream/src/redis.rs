//! Redis Streams-backed [`MessageBackend`] implementation.
//!
//! Refusals the read loops count are classified by the server's own error code,
//! because the same code answers both a group that does not exist yet and one
//! that is gone for good, and a counter that pools them admits no threshold.
//! The live tests below take both causes from the server rather than from a
//! fixture, since a fixture only proves the counter files what it is told.
//!
//! principle: feedback_two_opposite_causes_pooled_into_one_counter_admit_no_threshold

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, RedisError};
use std::sync::Arc;

use crate::read_health::{ReadHealth, ReadRefusal};
use cog_core::{MessageBackend, MessageStream, SFError, SFResult};

/// Upper bound on how many over-threshold entries one measurement lists. The
/// total pending count is exact either way; the over-threshold pair below
/// saturates at this page, and a saturated page is itself the finding — it
/// means the reclaim path has stopped taking anything back, which the
/// reported age says just as plainly.
const PENDING_STATS_PAGE: usize = 1024;

/// Redis Streams-backed [`MessageBackend`].
///
/// Hidden from the docs because the backend is chosen by the deployment's
/// configuration, never by a caller naming the type: consumers hold a
/// [`MessageBackend`], so the concrete type is an implementation detail rather
/// than part of the API this crate promises to keep stable.
#[doc(hidden)]
pub struct RedisMessageBackend {
    client: redis::Client,
    connection: ConnectionManager,
    /// Read health of every consumer this backend handed a stream to.
    health: Arc<ReadHealth>,
    /// Block periods the read loops issue with, from the deployment's config.
    /// Held as `usize` because that is what the read command takes; the config
    /// itself stays in milliseconds.
    read_block_ms: usize,
    resume_block_ms: usize,
}

impl RedisMessageBackend {
    /// Backend reading at the configured block periods' defaults.
    pub async fn new(redis_url: &str) -> SFResult<Self> {
        Self::with_block_periods(
            redis_url,
            cog_core::config::DEFAULT_REDIS_READ_BLOCK_MS,
            cog_core::config::DEFAULT_REDIS_READ_RESUME_BLOCK_MS,
            Arc::new(ReadHealth::new(
                cog_core::config::DEFAULT_REDIS_READ_BLOCK_MS,
            )),
        )
        .await
    }

    /// Backend whose consumers read with the given block periods and report
    /// their health into `health`.
    ///
    /// The periods are the consumer's quiet period: a stream with nothing to
    /// deliver still answers every block, so a consumer that goes silent for
    /// several blocks is failing rather than idle. Passing them in keeps the
    /// value in the deployment's configuration and lets the same value be
    /// published as the bound the staleness rule compares against.
    ///
    /// `health` is owned by the caller rather than by the backend so the same
    /// record is both written by the read loops and published as readings: a
    /// record created here and published from elsewhere would be two objects,
    /// and the published one would always be empty.
    pub async fn with_block_periods(
        redis_url: &str,
        read_block_ms: u64,
        resume_block_ms: u64,
        health: Arc<ReadHealth>,
    ) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Redis(e.to_string()))?;
        let connection = connect(&client).await?;
        Ok(Self {
            client,
            connection,
            health,
            read_block_ms: read_block_ms as usize,
            resume_block_ms: resume_block_ms as usize,
        })
    }

    /// The read health of this process's consumers, for publishing.
    pub fn read_health(&self) -> Arc<ReadHealth> {
        Arc::clone(&self.health)
    }

    /// Open a dedicated connection for one long-lived subscription.
    /// Blocking XREADGROUP commands serialize on a multiplexed connection:
    /// sharing one connection across N consumers makes every consumer poll at
    /// most once per N x block-time (observed: dozens of system + agent
    /// consumers slowed backlog drain to ~1 message/minute).
    /// Its own manager means its own socket and its own reconnect as well: one
    /// consumer's dead connection cannot stall the rest.
    async fn subscribe_connection(&self) -> SFResult<ConnectionManager> {
        connect(&self.client).await
    }
}

/// A connection that replaces its own socket when the socket dies.
///
/// A bare multiplexed connection does not: once the peer closes it, every
/// later command is issued on the same dead socket and fails with the same
/// I/O error, forever (observed in the cluster: a pod repeating "broken
/// pipe" for hours until it was restarted by hand, its consumers frozen on
/// a message stream nobody was reading). The manager swaps the socket in on
/// an I/O error and hands that one error back to the caller, so the next
/// command goes out on a fresh connection — retries that already exist
/// therefore recover, and a command is never issued twice, so a failed
/// publish is never silently duplicated.
///
/// The other half — that this connection is not worth waiting minutes for — is
/// [`cog_redis::connect`]'s, and it is a property of that one call site for
/// every backend in the workspace.
async fn connect(client: &redis::Client) -> SFResult<ConnectionManager> {
    cog_redis::connect(client)
        .await
        .map_err(|e| SFError::Redis(e.to_string()))
}

#[async_trait]
impl MessageBackend for RedisMessageBackend {
    async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
        // Redis Streams 不按「一条消息多少字节」拒绝：它服从 maxmemory 策略，
        // 那是整实例的容量面而不是单条载荷的界。所以这里没有界可报，而不是报 0。
        Ok(None)
    }

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
        //
        // Retrying in place is only half of it: a loop that never recovers
        // looks exactly like an idle one from outside. The guard and the health
        // record are the other half — they are what says, in the observation
        // surface, that this consumer has not been answered for a while. Every
        // write goes through this loop's own guard, which is what keeps two
        // loops on one stream from writing into each other's row.
        let health = self.read_health();
        let guard = ReadHealth::track(&health, &subject, &group);
        let block_ms = self.read_block_ms;
        let stream = futures::stream::try_unfold(
            (conn, 0u64, guard),
            move |(mut conn, mut backoff, guard)| {
                let subject = subject.clone();
                let group = group.clone();
                async move {
                    loop {
                        match group_read(&mut conn, &subject, &group, ">", block_ms).await {
                            Ok(Some(item)) => {
                                guard.note_ok();
                                return Ok(Some((item, (conn, 0, guard))));
                            }
                            Ok(None) => {
                                guard.note_ok();
                                backoff = 0;
                            }
                            Err(e) => {
                                guard.note_failure(read_refusal(&e));
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
        // Errors retry in place for the same reason as `subscribe` above, and
        // the health record is shared with it for the same reason too.
        let health = self.read_health();
        let guard = ReadHealth::track(&health, &subject, &group);
        let read_block_ms = self.read_block_ms;
        let resume_block_ms = self.resume_block_ms;
        let stream = futures::stream::try_unfold(
            (conn, Some(start_id), 0u64, guard),
            move |(mut conn, mut first_id, mut backoff, guard)| {
                let subject = subject.clone();
                let group = group.clone();
                async move {
                    loop {
                        let id = first_id.as_deref().unwrap_or(">");
                        let block_ms = if first_id.is_some() {
                            resume_block_ms
                        } else {
                            read_block_ms
                        };
                        match group_read(&mut conn, &subject, &group, id, block_ms).await {
                            Ok(Some(item)) => {
                                guard.note_ok();
                                return Ok(Some((item, (conn, None, 0, guard))));
                            }
                            Ok(None) => {
                                guard.note_ok();
                                first_id = None;
                                backoff = 0;
                            }
                            Err(e) => {
                                guard.note_failure(read_refusal(&e));
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
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
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
        Ok(Some(messages))
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
        //
        // XPENDING against a consumer group that does not exist yet answers
        // NOGROUP. That is a benign state, not a backend fault: nothing has
        // been delivered to the group, so nothing can be pending for it.
        // Answering it as Err made the pending observer count a measurement
        // failure every time this read raced ahead of group creation — the
        // false positive behind stream_pending_measure_failing. subscribe()
        // and spawn_pending_observer both create the group idempotently; this
        // arm keeps the read honest regardless of which of them ran first.
        let summary: redis::Value = match redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .query_async(&mut conn)
            .await
        {
            Ok(value) => value,
            Err(e) if e.code() == Some("NOGROUP") => return Ok(None),
            Err(e) => return Err(SFError::Redis(e.to_string())),
        };
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

    async fn stream_lag(&self, stream: &str, group: &str) -> SFResult<Option<u64>> {
        let mut conn = self.connection.clone();
        // `xinfo_groups` carries the server's own `lag` (Redis >= 7), which is
        // exactly "entries still waiting to be delivered to this group".
        // NOGROUP is the same benign race `pending_stats` answers with `None`:
        // nothing has been delivered, so nothing can be waiting behind us.
        let reply: redis::streams::StreamInfoGroupsReply = match redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream)
            .query_async(&mut conn)
            .await
        {
            Ok(reply) => reply,
            Err(e) if e.code() == Some("NOGROUP") => return Ok(None),
            Err(e) if e.code() == Some("ERR") && e.to_string().contains("no such key") => {
                return Ok(None)
            }
            Err(e) => return Err(SFError::Redis(e.to_string())),
        };
        Ok(reply
            .groups
            .iter()
            .find(|g| g.name == group)
            .and_then(|g| g.lag)
            .map(|lag| lag as u64))
    }

    async fn dlq(&self, stream: &str, msg_id: &str, payload: &[u8], reason: &str) -> SFResult<()> {
        // Redis Streams gets the record appended with XADD rather than
        // published, which is why this is an override and not the default: the
        // entry has to land in a stream the reclaim and read paths already
        // understand. The record itself comes from the shared builder so the
        // two surfaces cannot drift on its fields.
        let record = cog_core::dead_letter_record(msg_id, payload, reason)?;
        let dlq_stream = format!("{}:dlq", stream);
        let _: String = self
            .connection
            .clone()
            .xadd(&dlq_stream, "*", &[("payload", &record as &[u8])])
            .await
            .map_err(|e: RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }
}

async fn group_read(
    conn: &mut ConnectionManager,
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

/// Which of the two `NOGROUP` cases this refusal is, as far as the error says.
///
/// The same code is answered by a group that does not exist yet — the one read
/// that races `create_consumer_group` at startup, which the next attempt wins —
/// and by a group that is gone for good, which the consumer retries against
/// until the pod is replaced. The error cannot tell them apart and this function
/// does not pretend to: it only stops the two from being the same number, so the
/// rate can be read beside how long the consumer has gone unanswered.
///
/// `code()` is the server's own error name and the same test `pending_stats`
/// uses, so one spelling decides this question everywhere in the crate.
fn read_refusal(error: &RedisError) -> ReadRefusal {
    if error.code() == Some("NOGROUP") {
        ReadRefusal::NoGroup
    } else {
        ReadRefusal::Other
    }
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
    // The tests stand in for an outside client of the server, so they keep
    // using the bare connection the backend no longer does.
    use redis::aio::MultiplexedConnection;
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

    /// The two causes are told apart by the server's own error name, so the
    /// inputs have to come from the server.
    ///
    /// A fixture that hands `note_failure` a cause it picked itself would only
    /// prove the counter files what it is given, and the question is which
    /// cause a real refusal is given. Both halves are read off live replies:
    /// `XREADGROUP` against a stream whose group was never created answers
    /// `NOGROUP`, and the same command with an id the server will not parse
    /// answers `ERR` — neither `Test` nor a malformed string, the two errors
    /// this crate sees in the field.
    #[tokio::test]
    async fn a_live_refusal_is_filed_by_the_servers_own_error_code() {
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
        let stream = format!("cog-test:refusal-{suffix}");
        let group = format!("refusal-{suffix}");
        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");

        let backend = RedisMessageBackend::new(&redis_url).await.unwrap();
        let mut conn = backend.connection.clone();

        // The stream exists, the group does not: the read that races group
        // creation, which is the benign case the label has to separate.
        backend.publish(&stream, b"one").await.unwrap();
        let e = group_read(&mut conn, &stream, &group, ">", 0)
            .await
            .expect_err("reading a group that does not exist is refused");
        assert_eq!(
            e.code(),
            Some("NOGROUP"),
            "the server's own code, not this test's idea of it: {e}"
        );
        assert_eq!(
            read_refusal(&e),
            ReadRefusal::NoGroup,
            "a group that does not exist yet has to be counted apart from the \
             refusals that never clear: {e}"
        );

        // Now the group exists and the refusal is of another kind entirely.
        backend
            .create_consumer_group(&stream, &group)
            .await
            .unwrap();
        let e = group_read(&mut conn, &stream, &group, "not-an-id", 0)
            .await
            .expect_err("an unparsable id is refused");
        assert_ne!(
            read_refusal(&e),
            ReadRefusal::NoGroup,
            "only the server's NOGROUP goes under that label, and this one is \
             {e:?}"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");
    }

    /// Two loops reading one name are two rows, and the name carries both.
    ///
    /// The unit tests in `read_health` build the loops themselves, so what they
    /// can show is what the record does with rows it was handed. This one takes
    /// the loops from the backend: the refusals come from the server -- the
    /// stream key holds a string, so group creation and every read of it are
    /// refused -- and the questions are whether the two subscriptions the
    /// backend hands out are counted as two, whether the second one's
    /// registration leaves the first one's refusals in place, and whether the
    /// surviving loop keeps counting after its sibling is dropped.
    #[tokio::test]
    async fn two_live_loops_on_one_name_are_counted_together_and_outlive_each_other() {
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
        let stream = format!("cog-test:loops-{suffix}");
        let group = format!("loops-{suffix}");
        let _: () = redis::cmd("SET")
            .arg(stream.as_str())
            .arg("not-a-stream")
            .query_async(&mut raw)
            .await
            .expect("set");

        let backend = RedisMessageBackend::new(&redis_url).await.unwrap();
        let health = backend.read_health();
        let family = |health: &Arc<ReadHealth>| {
            health
                .families(crate::read_health::now_seconds())
                .into_iter()
                .find(|((seen, _), _)| seen == &stream)
                .map(|(_, family)| family)
                .unwrap_or_else(|| panic!("no family published for {stream}"))
        };

        // Two refusals of its own: one at once, one after the second of
        // backoff. One is enough for what is asserted, and the second makes the
        // reading below unlikely to be a single attempt's accident.
        let mut first = backend.subscribe(&stream, &group).await.unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_millis(2100), first.next()).await;
        let first_alone = family(&health).failures();
        assert!(
            first_alone >= 2,
            "the first loop's refusals are counted while it is the only one: {first_alone}"
        );

        // The second loop gets one refusal attempt, which is all it needs: the
        // question is whether its registration leaves the first loop's count
        // standing.
        let mut second = backend.subscribe(&stream, &group).await.unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_millis(700), second.next()).await;
        let both = family(&health);
        assert_eq!(both.loops, 2, "the name covers both loops: {both:?}");
        assert!(
            both.failures() > first_alone,
            "the second loop's refusal is in the name beside the first's, not \
             in place of it: {both:?} against {first_alone} before it registered"
        );

        // One loop leaves. The name is still read by the other, and the other
        // keeps counting: with the row keyed by the name, this drop deleted the
        // row and every later write was skipped, so the reading went to zero in
        // the one moment it mattered.
        drop(first);
        let _ = tokio::time::timeout(std::time::Duration::from_millis(2400), second.next()).await;
        let after = family(&health);
        assert_eq!(
            after.loops, 1,
            "the name outlives its first loop: {after:?}"
        );
        assert!(
            after.failures() >= 2,
            "the loop still reading keeps counting after its sibling is gone: {after:?}"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");
    }

    #[tokio::test]
    async fn test_pending_stats_missing_group_is_not_a_failure() {
        // Regression: pending_stats answered XPENDING's NOGROUP with Err, and
        // the pending observer counted every such read as a measurement
        // failure — stream_pending_measure_failing fired on a healthy queue
        // whose consumer group simply had not been created yet. A group that
        // does not exist holds nothing pending; that is Ok(None), not a
        // backend fault.
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
        let stream = format!("cog-test:nogroup-{suffix}");
        let group = format!("nogroup-{suffix}");
        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");

        let backend: Arc<dyn MessageBackend> =
            Arc::new(RedisMessageBackend::new(&redis_url).await.unwrap());
        backend.publish(&stream, b"one").await.unwrap();

        // The stream exists but no consumer group has been created on it: the
        // read must say "no pending state" instead of failing.
        let stats = backend
            .pending_stats(&stream, &group, 0)
            .await
            .expect("pending_stats must not fail when the consumer group is absent");
        assert!(
            stats.is_none(),
            "a missing consumer group has no pending state to report, got {stats:?}"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");
    }

    /// 未投递的条数与 PEL 里的条数是两个问题：清扫器要的是前者——消息还压在
    /// 队伍里就不是丢了，重投一份只是把队伍再加长一格。
    #[tokio::test]
    async fn stream_lag_counts_what_the_group_has_not_been_handed() {
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
        let stream = format!("cog-test:lag-{suffix}");
        let group = format!("lag-{suffix}");

        let backend: Arc<dyn MessageBackend> =
            Arc::new(RedisMessageBackend::new(&redis_url).await.unwrap());
        // Group first, at the tip: everything published afterwards is what the
        // group has not been handed yet.
        backend
            .create_consumer_group(&stream, &group)
            .await
            .unwrap();
        assert_eq!(
            backend.stream_lag(&stream, &group).await.unwrap(),
            Some(0),
            "a group that has been handed nothing yet, on an empty stream"
        );

        backend.publish(&stream, b"one").await.unwrap();
        backend.publish(&stream, b"two").await.unwrap();
        backend.publish(&stream, b"three").await.unwrap();
        assert_eq!(
            backend.stream_lag(&stream, &group).await.unwrap(),
            Some(3),
            "three entries are waiting for this group and none has been delivered"
        );

        // Deliver one and leave it unacked: it leaves the lag (it has been
        // handed out) and enters the PEL -- the two figures answering
        // different questions is the whole point.
        let delivered: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&group)
            .arg("consumer-1")
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&stream)
            .arg(">")
            .query_async(&mut raw)
            .await
            .expect("xreadgroup");
        assert_eq!(delivered.keys.len(), 1);
        assert_eq!(
            backend.stream_lag(&stream, &group).await.unwrap(),
            Some(2),
            "the delivered-but-unacked entry is pending, not lagging"
        );
        assert_eq!(
            backend
                .pending_stats(&stream, &group, 0)
                .await
                .unwrap()
                .map(|stats| stats.count),
            Some(1)
        );

        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");
    }

    /// 组不存在时没有「还欠多少条」可报：那是这个后端答不出，不是零。
    #[tokio::test]
    async fn stream_lag_without_a_group_is_not_a_reading() {
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
        let stream = format!("cog-test:lag-nogroup-{suffix}");
        let group = format!("lag-nogroup-{suffix}");

        let backend: Arc<dyn MessageBackend> =
            Arc::new(RedisMessageBackend::new(&redis_url).await.unwrap());
        backend.publish(&stream, b"one").await.unwrap();
        assert_eq!(
            backend.stream_lag(&stream, &group).await.unwrap(),
            None,
            "no group means nothing has been delivered, so there is no backlog to report"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(stream.as_str())
            .query_async(&mut raw)
            .await
            .expect("del");
    }
}
