use crate::resilience::{BackpressureConfig, BackpressureError};
use crate::{AgentEvent, SFResult};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};

/// Default capacity for bounded event streams.
/// Backpressure kicks in when the producer outpaces the consumer.
pub const DEFAULT_STREAM_CAPACITY: usize = 256;

/// A push-based async event stream with true backpressure.
/// Design evolution from pi-ai's `EventStream<T, R>`:
/// - Producer and consumer are separated: `EventStreamProducer` for pushing,
///   `EventStream` for pulling via `futures::Stream`.
/// - Built on `tokio::sync::mpsc::channel` (bounded, lock-free, backpressure).
/// - When the channel is full, `push().await` blocks the producer until the
///   consumer makes room. This prevents unbounded memory growth.
/// # Example
/// ```ignore
/// let (mut stream, mut producer) = EventStream::<String, i32>::with_capacity(64);
/// tokio::spawn(async move {
///     producer.push("hello".to_string()).await;
///     producer.end(42);
/// });
/// while let Some(event) = stream.next().await {
///     println!("{}", event);
/// }
/// let result = stream.result().await;
/// ```
pub struct EventStream<T, R> {
    rx: mpsc::Receiver<T>,
    result_rx: Option<oneshot::Receiver<R>>,
    paused: Arc<AtomicBool>,
    buffer_count: Arc<AtomicUsize>,
    config: BackpressureConfig,
}

/// Producer handle for an `EventStream`.
/// The `mpsc::Sender<T>` can be cloned if you need multiple producers,
/// but the completion oneshot can only be sent once.
pub struct EventStreamProducer<T, R> {
    tx: mpsc::Sender<T>,
    result_tx: Option<oneshot::Sender<R>>,
    paused: Arc<AtomicBool>,
    buffer_count: Arc<AtomicUsize>,
    config: BackpressureConfig,
}

impl<T, R> EventStream<T, R> {
    /// Create a new bounded event stream with the given capacity.
    pub fn with_capacity(capacity: usize) -> (Self, EventStreamProducer<T, R>) {
        Self::with_config(capacity, BackpressureConfig::default())
    }

    /// Create a new bounded event stream with the given capacity and backpressure config.
    pub fn with_config(
        capacity: usize,
        config: BackpressureConfig,
    ) -> (Self, EventStreamProducer<T, R>) {
        let (tx, rx) = mpsc::channel(capacity);
        let (result_tx, result_rx) = oneshot::channel();
        let paused = Arc::new(AtomicBool::new(false));
        let buffer_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                rx,
                result_rx: Some(result_rx),
                paused: Arc::clone(&paused),
                buffer_count: Arc::clone(&buffer_count),
                config,
            },
            EventStreamProducer {
                tx,
                result_tx: Some(result_tx),
                paused,
                buffer_count,
                config,
            },
        )
    }

    /// Get the final result. Returns a future that resolves when `end()` is called.
    /// If the producer is dropped without calling `end()`, returns `R::default()`.
    pub fn result(&mut self) -> ResultFuture<R> {
        ResultFuture {
            rx: self.result_rx.take(),
        }
    }
}

impl<T, R> futures::Stream for EventStream<T, R> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                let prev = self.buffer_count.fetch_sub(1, Ordering::SeqCst);
                let new_count = prev.saturating_sub(1);
                if new_count <= self.config.low_watermark && self.paused.load(Ordering::SeqCst) {
                    self.paused.store(false, Ordering::SeqCst);
                }
                Poll::Ready(Some(event))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Future that resolves to the final result of an EventStream.
pub struct ResultFuture<R> {
    rx: Option<oneshot::Receiver<R>>,
}

impl<R: Default + Clone> Future for ResultFuture<R> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.rx.as_mut() {
            Some(rx) => {
                let pinned = std::pin::Pin::new(rx);
                match pinned.poll(cx) {
                    Poll::Ready(Ok(result)) => Poll::Ready(result),
                    // Producer dropped without calling end() — return default instead of hanging.
                    Poll::Ready(Err(_)) => Poll::Ready(R::default()),
                    Poll::Pending => Poll::Pending,
                }
            }
            None => Poll::Ready(R::default()),
        }
    }
}

impl<T, R> EventStreamProducer<T, R> {
    /// Push an event into the stream.
    /// **Backpressure**: if the buffer count reaches the high watermark, returns
    /// `Err(BackpressureError::HighWatermark)` immediately. Otherwise, if the
    /// channel is full, this awaits until space is available.
    /// Returns `Err(BackpressureError::Send(_))` if all receivers have been dropped.
    pub async fn push(&self, event: T) -> Result<(), BackpressureError<T>> {
        // high_watermark == 0 means backpressure is disabled.
        if self.config.high_watermark > 0 {
            let prev = self.buffer_count.fetch_add(1, Ordering::SeqCst);
            let new_count = prev + 1;
            if new_count >= self.config.high_watermark {
                self.paused.store(true, Ordering::SeqCst);
                self.buffer_count.fetch_sub(1, Ordering::SeqCst);
                return Err(BackpressureError::HighWatermark(event));
            }
        }
        match self.tx.send(event).await {
            Ok(()) => Ok(()),
            Err(e) => {
                if self.config.high_watermark > 0 {
                    self.buffer_count.fetch_sub(1, Ordering::SeqCst);
                }
                Err(BackpressureError::Send(e))
            }
        }
    }

    /// Try to push an event without awaiting.
    /// Returns `Err(BackpressureError::HighWatermark(_))` if the high watermark is reached.
    /// Returns `Err(BackpressureError::Send(TrySendError::Full(_)))` if the channel is full.
    pub fn try_push(&self, event: T) -> Result<(), BackpressureError<T>> {
        // high_watermark == 0 means backpressure is disabled.
        if self.config.high_watermark > 0 {
            let prev = self.buffer_count.fetch_add(1, Ordering::SeqCst);
            let new_count = prev + 1;
            if new_count >= self.config.high_watermark {
                self.paused.store(true, Ordering::SeqCst);
                self.buffer_count.fetch_sub(1, Ordering::SeqCst);
                return Err(BackpressureError::HighWatermark(event));
            }
        }
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(e) => {
                if self.config.high_watermark > 0 {
                    self.buffer_count.fetch_sub(1, Ordering::SeqCst);
                }
                match e {
                    mpsc::error::TrySendError::Full(v) => Err(BackpressureError::HighWatermark(v)),
                    mpsc::error::TrySendError::Closed(v) => {
                        Err(BackpressureError::Send(mpsc::error::SendError(v)))
                    }
                }
            }
        }
    }

    /// Signal that the stream is complete.
    /// Idempotent: subsequent calls are no-ops.
    pub fn end(&mut self, result: R) {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(result);
        }
    }

    /// Check whether `end()` has already been called.
    pub fn is_ended(&self) -> bool {
        self.result_tx.is_none()
    }

    /// Pause the producer.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Resume the producer.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    /// Check whether the producer is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
}

/// Abstract interface for publishing agent events to downstream consumers.
/// Gateway and other edge crates consume this trait instead of directly
/// depending on [`MessageBackend`], preserving the principle that:
/// - **producers** (e.g. `cog-gateway`) only know a semantic interface
/// - **implementors** (e.g. `cog-stream`) decide whether to use MQ, gRPC,
///   WebSocket, or an in-memory broadcast
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish an `AgentEvent` to all configured downstream channels.
    async fn publish(&self, event: &AgentEvent) -> SFResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_push_succeeds_normally() {
        let (mut stream, producer) = EventStream::<i32, ()>::with_capacity(10);
        producer.push(42).await.unwrap();
        assert_eq!(stream.next().await, Some(42));
    }

    #[tokio::test]
    async fn test_push_returns_high_watermark_error() {
        let config = BackpressureConfig::new(3, 1);
        let (mut stream, producer) = EventStream::<i32, ()>::with_config(10, config);

        // Push 2 items successfully (count becomes 2, which is < 3)
        producer.push(1).await.unwrap();
        producer.push(2).await.unwrap();

        // Third push: count becomes 3, which >= high_watermark (3)
        let err = producer.push(3).await.unwrap_err();
        assert!(matches!(err, BackpressureError::HighWatermark(3)));
        assert!(producer.is_paused());

        // Stream has [1, 2]. Consume 1 -> count 2->1, 1 <= low_watermark (1), resumes
        assert_eq!(stream.next().await, Some(1));
        assert!(!producer.is_paused());

        assert_eq!(stream.next().await, Some(2));
    }

    #[tokio::test]
    async fn test_producer_resumes_after_consumer_drains() {
        let config = BackpressureConfig::new(5, 2);
        let (mut stream, producer) = EventStream::<i32, ()>::with_config(10, config);

        // Push up to just below high watermark
        for i in 0..4 {
            producer.push(i).await.unwrap();
        }
        assert!(!producer.is_paused());

        // This should hit the watermark
        let err = producer.push(4).await.unwrap_err();
        assert!(matches!(err, BackpressureError::HighWatermark(4)));
        assert!(producer.is_paused());

        // Count starts at 4 (items 0,1,2,3 are in stream)
        // next() -> gets 0, count 4->3, 3 > 2, stays paused
        assert_eq!(stream.next().await, Some(0));
        assert!(producer.is_paused());

        // next() -> gets 1, count 3->2, 2 <= 2, resumes!
        assert_eq!(stream.next().await, Some(1));
        assert!(!producer.is_paused());

        // Can push again
        producer.push(5).await.unwrap();
    }

    #[tokio::test]
    async fn test_pause_and_resume_methods() {
        let (_stream, producer) = EventStream::<i32, ()>::with_capacity(10);
        assert!(!producer.is_paused());
        producer.pause();
        assert!(producer.is_paused());
        producer.resume();
        assert!(!producer.is_paused());
    }

    #[tokio::test]
    async fn test_try_push_backpressure() {
        let config = BackpressureConfig::new(3, 1);
        let (mut stream, producer) = EventStream::<i32, ()>::with_config(10, config);

        producer.try_push(1).unwrap();
        producer.try_push(2).unwrap();

        let err = producer.try_push(3).unwrap_err();
        assert!(matches!(err, BackpressureError::HighWatermark(3)));
        assert!(producer.is_paused());

        assert_eq!(stream.next().await, Some(1));
        assert!(!producer.is_paused());
    }
}
