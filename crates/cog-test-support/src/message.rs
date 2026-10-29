//! 一个内存 `MessageBackend`：用例要的是「发出去的消息能读回来」。
//!
//! 契约在 core，所以任何一份合规实现都能当替身。用产线那份会让被测 crate 的用例
//! 跟着另一个 crate 的实现变红，还给它添了一条不该有的邻居边；那条边正是这份
//! 替身要消掉的东西。

use async_trait::async_trait;
use cog_core::{MessageBackend, MessageStream, SFError, SFResult};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// One stop signal per (subject, group): the sender lives here, the receiver
/// travels with the stream that subscription handed out.
type StopSignals = HashMap<(String, String), tokio::sync::watch::Sender<bool>>;

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
    /// Where a caller says "stop reading" to a stream this backend opened.
    ///
    /// A watch channel rather than a flag: the reader is parked on a broadcast
    /// receive, and a flag it only reads between messages would never be seen
    /// by an idle subscriber.
    stops: Arc<Mutex<StopSignals>>,
}

impl MemoryMessageBackend {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            broadcast_capacity: 1024,
            stops: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a fresh stop signal for a subscription and return its receiver.
    fn register_stop(&self, subject: &str, group: &str) -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut stops = self.stops.lock().unwrap();
        stops.insert((subject.to_string(), group.to_string()), tx);
        rx
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

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        let state = self.get_or_create_state(subject);
        let mut rx = state.sender.subscribe();
        let stop = self.register_stop(subject, group);

        let mut pending: Vec<(String, Vec<u8>)> = state.buffer.clone();
        while let Ok(msg) = rx.try_recv() {
            pending.push(msg);
        }

        let stream = futures::stream::iter(pending.into_iter().map(Ok)).chain(
            futures::stream::unfold((rx, stop), |(mut rx, mut stop)| async move {
                tokio::select! {
                    received = rx.recv() => match received {
                        Ok((id, bytes)) => Some((Ok((id, bytes)), (rx, stop))),
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            Some((Err(SFError::Backpressure), (rx, stop)))
                        }
                    },
                    // Both a signalled stop and a dropped sender end the read:
                    // nothing will be delivered either way, and a subscriber
                    // that stays parked would never find that out.
                    _ = stop.changed() => None,
                }
            }),
        );

        Ok(Box::pin(stream))
    }

    async fn delete_stream(&self, subject: &str) -> SFResult<()> {
        // Dropping the entry drops the broadcast sender with it, which ends
        // every read parked on this subject: the same "nothing will be
        // delivered here again" that `stop_stream` signals, without having to
        // name the groups that are reading it.
        let removed = {
            let mut channels = self.channels.lock().unwrap();
            channels.remove(subject)
        };
        if let Some(state) = removed {
            // The buffer is what a later subscriber would replay; it is the
            // part of the stream that outlives the request, so it has to go
            // with the key it belongs to.
            drop(state);
            // Stop signals are per (stream, group); the stream is gone, so the
            // signals that only exist to end reads on it are gone too.
            let mut stops = self.stops.lock().unwrap();
            stops.retain(|key, _flag| key.0 != subject);
        }
        Ok(())
    }

    async fn stop_stream(&self, subject: &str, group: &str) {
        let sender = {
            let stops = self.stops.lock().unwrap();
            stops
                .get(&(subject.to_string(), group.to_string()))
                .cloned()
        };
        if let Some(sender) = sender {
            let _ = sender.send(true);
        }
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

    /// No group state, so nothing to count: a subscriber reads the in-process
    /// buffer directly and the backend keeps no per-group position that would
    /// say what has not been handed out. Zero would be a made-up reading of an
    /// empty queue, so this answers "cannot say" instead.
    async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
        Ok(None)
    }

    /// No reclaim path, and saying so is the whole answer. Messages live in a
    /// map inside this process, so a message a consumer never took is not
    /// anywhere a claim could reach; there is nothing to hand back and no way
    /// to have failed to.
    async fn claim_pending(
        &self,
        _stream: &str,
        _group: &str,
        _min_idle_ms: u64,
        _count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        Ok(None)
    }
}
