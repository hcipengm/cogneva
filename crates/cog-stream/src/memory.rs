//! In-memory [`MessageBackend`] implementation for testing and local development.

use async_trait::async_trait;
use cog_core::{MessageBackend, MessageStream, SFError, SFResult};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct MemoryChannelState {
    sender: tokio::sync::broadcast::Sender<(String, Vec<u8>)>,
    buffer: Vec<(String, Vec<u8>)>,
    next_id: u64,
}

/// In-memory [`MessageBackend`] for testing and local development.
///
/// Hidden from the docs because the backend is chosen by the deployment's
/// configuration, never by a caller naming the type: consumers hold a
/// [`MessageBackend`], so the concrete type is an implementation detail rather
/// than part of the API this crate promises to keep stable.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct MemoryMessageBackend {
    channels: Arc<Mutex<HashMap<String, MemoryChannelState>>>,
    broadcast_capacity: usize,
}

impl MemoryMessageBackend {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            broadcast_capacity: 1024,
        }
    }

    pub fn with_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity;
        self
    }

    fn get_or_create_state(&self, subject: &str) -> MemoryChannelState {
        let mut channels = self.channels.lock().unwrap();
        channels
            .entry(subject.to_string())
            .or_insert_with(|| MemoryChannelState {
                sender: tokio::sync::broadcast::channel(self.broadcast_capacity).0,
                buffer: Vec::new(),
                next_id: 0,
            })
            .clone()
    }
}

impl Default for MemoryMessageBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageBackend for MemoryMessageBackend {
    async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
        // 进程内缓冲对单条载荷没有界：内存里放得下就收。所以这里没有界可报，
        // 而不是报一个 0 或某个「够大」的常数。
        Ok(None)
    }

    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        let mut channels = self.channels.lock().unwrap();
        let state = channels
            .entry(subject.to_string())
            .or_insert_with(|| MemoryChannelState {
                sender: tokio::sync::broadcast::channel(self.broadcast_capacity).0,
                buffer: Vec::new(),
                next_id: 0,
            });
        let id = format!("{}", state.next_id);
        state.next_id += 1;
        let msg = (id, payload.to_vec());
        state.buffer.push(msg.clone());
        let _ = state.sender.send(msg);
        Ok(())
    }

    async fn publish_batch(&self, subject: &str, payloads: &[Vec<u8>]) -> SFResult<()> {
        let mut channels = self.channels.lock().unwrap();
        let state = channels
            .entry(subject.to_string())
            .or_insert_with(|| MemoryChannelState {
                sender: tokio::sync::broadcast::channel(self.broadcast_capacity).0,
                buffer: Vec::new(),
                next_id: 0,
            });
        for payload in payloads {
            let id = format!("{}", state.next_id);
            state.next_id += 1;
            let msg = (id, payload.to_vec());
            state.buffer.push(msg.clone());
            let _ = state.sender.send(msg);
        }
        Ok(())
    }

    async fn subscribe(&self, subject: &str, _group: &str) -> SFResult<MessageStream> {
        let state = self.get_or_create_state(subject);
        let mut rx = state.sender.subscribe();

        let mut pending: Vec<(String, Vec<u8>)> = state.buffer.clone();
        while let Ok(msg) = rx.try_recv() {
            pending.push(msg);
        }

        let stream = futures::stream::iter(pending.into_iter().map(Ok)).chain(
            futures::stream::unfold(rx, |mut rx| async move {
                match rx.recv().await {
                    Ok((id, bytes)) => Some((Ok((id, bytes)), rx)),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        Some((Err(SFError::Backpressure), rx))
                    }
                }
            }),
        );

        Ok(Box::pin(stream))
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        _group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        let state = self.get_or_create_state(subject);
        let mut rx = state.sender.subscribe();

        let start_offset = if start_id == "0" || start_id.is_empty() {
            0u64
        } else {
            start_id.parse::<u64>().unwrap_or(0)
        };

        let mut pending: Vec<(String, Vec<u8>)> = state
            .buffer
            .iter()
            .filter(|(id, _)| id.parse::<u64>().unwrap_or(0) >= start_offset)
            .cloned()
            .collect();
        while let Ok((id, bytes)) = rx.try_recv() {
            if id.parse::<u64>().unwrap_or(0) >= start_offset {
                pending.push((id, bytes));
            }
        }

        let stream = futures::stream::iter(pending.into_iter().map(Ok)).chain(
            futures::stream::unfold(rx, |mut rx| async move {
                match rx.recv().await {
                    Ok((id, bytes)) => Some((Ok((id, bytes)), rx)),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        Some((Err(SFError::Backpressure), rx))
                    }
                }
            }),
        );

        Ok(Box::pin(stream))
    }

    async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
        Ok(())
    }

    async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
        Ok(())
    }

    /// Nothing to report: a subscriber reads the in-process buffer, so an
    /// unacked message is not held anywhere that outlives the process holding
    /// it, and there is no reclaim path for it to be recovered by.
    async fn pending_stats(
        &self,
        _stream: &str,
        _group: &str,
        _idle_threshold_ms: u64,
    ) -> SFResult<Option<cog_core::PendingStats>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    /// 死信记录里必须放着那条消息自己的字节。
    ///
    /// 这条断言的名字就是契约的一行：调用方走的是「这条消息再也读不出来」的那条分支，
    /// 死信队列是它之后唯一还留着这份内容的地方。只带 id 和 reason 的记录读起来像
    /// 「已经处理了」，而真正丢掉的东西还是一样的没了。
    #[tokio::test]
    async fn a_dead_letter_record_carries_the_bytes_it_is_about() {
        let backend = MemoryMessageBackend::new();
        // 这具载荷是**非法 JSON**，正是走到这一格的那些消息的样子：它们读不出来，
        // 所以不能假设里面是什么编码，二进制的也要原样带过去。
        let body: &[u8] = b"not json at all \x00\xff\xfe";

        backend
            .dlq("tasks:ready", "17-0", body, "unparsable DagMessage")
            .await
            .expect("dead-letter write");

        // 默认实现写的是 `{stream}:dlq`，所以这里从那个 subject 上读回来——断言的是
        // 「写到哪里」和「写了什么」两件事，改掉任一件这条就要红。
        let mut stream = backend
            .subscribe_from("tasks:ready:dlq", "g", "0")
            .await
            .expect("subscribe to the dead-letter subject");
        let (_id, record) = stream
            .next()
            .await
            .expect("a dead-letter record was published")
            .expect("readable record");

        let record: serde_json::Value =
            serde_json::from_slice(&record).expect("the record is JSON");
        assert_eq!(record["original_id"], "17-0");
        assert_eq!(record["reason"], "unparsable DagMessage");
        let payload = base64::engine::general_purpose::STANDARD
            .decode(record["payload_b64"].as_str().expect("payload field"))
            .expect("payload decodes");
        assert_eq!(
            payload, body,
            "字节原样带过去；丢了这一格，死信队列就只剩一条『有个消息死了』的读数"
        );
    }

    /// 延迟投递的默认实现要真的延迟、再真的投出去——不是「报个成功就完」。
    ///
    /// 这一具后端不覆写 `delay_publish`，走的就是契约默认体。默认体曾经是 `Ok(())`：
    /// 调用方拿到成功，消息没了——一个「不丢消息」的方法把消息丢了，而丢的方式恰好是
    /// 任何调用点都看不见的那种。断言因此必须落在**消息真的到了**上：只断言返回 `Ok`
    /// 的话，那条缺陷原样能过。等长也用 `timeout` 兜底，让「没投」表现为红而不是挂住。
    #[tokio::test]
    async fn a_delayed_publish_waits_then_delivers_instead_of_just_reporting_success() {
        let backend = MemoryMessageBackend::new();
        let mut stream = backend
            .subscribe("delayed:subject", "g")
            .await
            .expect("subscribe before the publish");

        let started = std::time::Instant::now();
        backend
            .delay_publish("delayed:subject", b"late but here", 50)
            .await
            .expect("the default reports the publish");
        assert!(
            started.elapsed().as_millis() >= 50,
            "默认体先等满 delay_ms 再投，不该提前返回"
        );

        let (_, payload) = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("延迟投递必须真的到达，而不是只报成功")
            .expect("stream stays open")
            .expect("readable message");
        assert_eq!(payload, b"late but here");
    }
}
