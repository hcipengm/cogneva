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
}
