//! What `start_consumer` promises when it returns.
//!
//! It used to spawn the loop and return, so its `Ok(())` said only that a task
//! had been created. Everything the loop does before it reads — creating the
//! group, taking back what a previous incarnation left pending, subscribing —
//! happened afterwards, unobserved. Two failures lived in that gap: a consumer
//! that never got as far as reading left an agent on the roster that answers
//! prompts and hears nothing, and it left it silently, because the only trace
//! was a log line and the handle looked exactly like a working one. The caller
//! had no way to tell "up" from "spawned".
//!
//! Both tests below pin the answer: `Ok(())` means the inbox is being read, and
//! a consumer that stops before that is an error the caller has to see.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;

use cog_agent::Agent;
use cog_core::{MessageBackend, MessageStream, PendingStats, RuntimeConfig, SFError, SFResult};
use cog_test_support::MemoryMessageBackend;

/// A backend whose start-up can be held, so the moment `start_consumer` is
/// allowed to return is decided by this test rather than by a race.
struct Held {
    inner: MemoryMessageBackend,
    entered: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    hold: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    fail_group: bool,
}

impl Held {
    fn new(fail_group: bool) -> (Arc<Self>, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let backend = Arc::new(Self {
            inner: MemoryMessageBackend::new(),
            entered: std::sync::Mutex::new(Some(entered_tx)),
            hold: tokio::sync::Mutex::new(Some(release_rx)),
            fail_group,
        });
        (backend, entered_rx, release_tx)
    }
}

#[async_trait]
impl MessageBackend for Held {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        self.inner.publish(subject, payload).await
    }

    async fn payload_limit(&self, subject: &str) -> SFResult<Option<usize>> {
        self.inner.payload_limit(subject).await
    }

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        // Held until the test releases it: this is the last step before the
        // loop starts reading, so it is exactly the boundary under test.
        if let Some(rx) = self.hold.lock().await.take() {
            let _ = rx.await;
        }
        self.inner.subscribe(subject, group).await
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        self.inner.subscribe_from(subject, group, start_id).await
    }

    async fn stop_stream(&self, subject: &str, group: &str) {
        self.inner.stop_stream(subject, group).await
    }

    async fn create_consumer_group(&self, stream: &str, group: &str) -> SFResult<()> {
        if let Some(tx) = self.entered.lock().expect("entered").take() {
            let _ = tx.send(());
        }
        if self.fail_group {
            return Err(SFError::Agent("the store refused the group".into()));
        }
        self.inner.create_consumer_group(stream, group).await
    }

    async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
        self.inner.ack(stream, group, ids).await
    }

    async fn claim_pending(
        &self,
        stream: &str,
        group: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        self.inner
            .claim_pending(stream, group, min_idle_ms, count)
            .await
    }

    async fn pending_stats(
        &self,
        stream: &str,
        group: &str,
        idle_threshold_ms: u64,
    ) -> SFResult<Option<PendingStats>> {
        self.inner
            .pending_stats(stream, group, idle_threshold_ms)
            .await
    }

    async fn stream_lag(&self, stream: &str, group: &str) -> SFResult<Option<u64>> {
        self.inner.stream_lag(stream, group).await
    }
}

fn agent_with(backend: Arc<dyn MessageBackend>) -> Arc<Agent> {
    let config = RuntimeConfig {
        agent_id: "start-consumer-probe".into(),
        ..Default::default()
    };
    Arc::new(Agent::new(config, Arc::new(SilentLlm)).with_message_backend(backend))
}

/// Answers nothing: neither test prompts.
struct SilentLlm;

#[async_trait]
impl cog_core::LlmClient for SilentLlm {
    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        unimplemented!("the start-up tests never prompt")
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        unimplemented!("the start-up tests never complete")
    }

    async fn chat(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> SFResult<cog_core::ChatResponse> {
        unimplemented!("the start-up tests never prompt")
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// One thread, so `spawn` order is the thing that decides who runs: the task
/// that called `start_consumer` finishes before the consumer it spawned gets a
/// turn, unless it is waiting on the consumer.
#[tokio::test]
async fn start_consumer_returns_only_once_the_inbox_is_being_read() {
    let (backend, entered, release) = Held::new(false);
    let agent = agent_with(backend.clone());

    let started = tokio::spawn(async move {
        agent
            .start_consumer(|_msg| async { Ok::<(), SFError>(()) })
            .await
            .expect("the inbox starts")
    });

    entered.await.expect("the consumer created its group");

    assert!(
        !started.is_finished(),
        "start_consumer returned while the loop was still one step short of reading: \
         a caller that publishes now hands work to an agent that is not listening, and \
         nothing in the tree can tell that apart from a slow one"
    );

    release.send(()).expect("the subscribe step is still held");
    started.await.expect("the start task does not panic");
}

#[tokio::test]
async fn a_consumer_that_dies_before_reading_is_an_error_not_a_running_agent() {
    let (backend, _entered, _release) = Held::new(true);
    let agent = agent_with(backend);

    let started = agent
        .start_consumer(|_msg| async { Ok::<(), SFError>(()) })
        .await;

    assert!(
        started.is_err(),
        "the consumer never reached its read loop, and start_consumer still said the \
         agent was up: the object stays on the roster, answers prompts and hears nothing"
    );
}
