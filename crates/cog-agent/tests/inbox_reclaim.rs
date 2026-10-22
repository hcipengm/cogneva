//! What the inbox consumer does with what it was handed and never finished.
//!
//! The consumer asks its backend only for messages that have never been
//! delivered (`>`), so a command written while the process was dying stayed in
//! the group's pending list for as long as the group existed: the producer had
//! been answered 202, nobody ever ran it, and no lag reading showed it, because
//! a pending entry is not lag. The same shape appeared without any crash at all
//! -- a handler error propagated out of the loop, which ended the consumer
//! while the agent stayed registered and kept beating.
//!
//! A backend here hands the loop a scripted pending batch, so both halves are
//! decided by one run rather than by a live Redis and a well-timed kill.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use cog_core::{InboxMessage, MessageBackend, MessageStream, PendingStats, SFError, SFResult};

/// Records what the loop did, and hands out a fixed pending batch once.
struct Scripted {
    pending: Mutex<Vec<(String, Vec<u8>)>>,
    acked: Mutex<Vec<String>>,
}

impl Scripted {
    fn with_pending(ids: &[&str]) -> Self {
        Self {
            pending: Mutex::new(
                ids.iter()
                    .map(|id| {
                        (
                            id.to_string(),
                            serde_json::to_vec(&InboxMessage::Abort).expect("encode"),
                        )
                    })
                    .collect(),
            ),
            acked: Mutex::new(Vec::new()),
        }
    }

    fn acked(&self) -> Vec<String> {
        self.acked.lock().expect("lock").clone()
    }
}

#[async_trait]
impl MessageBackend for Scripted {
    async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
        Ok(())
    }

    async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
        Ok(None)
    }

    async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
        // Nothing new to read: the run is decided entirely by the reclaim pass.
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        _start_id: &str,
    ) -> SFResult<MessageStream> {
        self.subscribe(subject, group).await
    }

    async fn stop_stream(&self, _subject: &str, _group: &str) {}

    async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
        Ok(())
    }

    async fn ack(&self, _stream: &str, _group: &str, ids: &[String]) -> SFResult<()> {
        self.acked.lock().expect("lock").extend_from_slice(ids);
        Ok(())
    }

    async fn claim_pending(
        &self,
        _stream: &str,
        _group: &str,
        _min_idle_ms: u64,
        count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        let mut pending = self.pending.lock().expect("lock");
        let take = count.min(pending.len());
        Ok(Some(pending.drain(..take).collect()))
    }

    async fn pending_stats(
        &self,
        _stream: &str,
        _group: &str,
        _idle_threshold_ms: u64,
    ) -> SFResult<Option<PendingStats>> {
        Ok(None)
    }

    async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
        Ok(None)
    }
}

/// Run the consumer over the scripted backend.
///
/// `refuse_nth` names the 1-based positions of commands the handler fails on;
/// the batch is handed over in the order it was scripted, so position is enough
/// to pick one out. Returns the run's own result and how many commands the
/// handler was given in all.
async fn drive(backend: Arc<Scripted>, refuse_nth: &[usize]) -> (SFResult<()>, usize) {
    let refuse = refuse_nth.to_vec();
    let seen = Arc::new(Mutex::new(0usize));
    let counter = seen.clone();
    let consumer = cog_agent::AgentConsumer::new("probe_reclaim:agent", backend);
    let result = consumer
        .run(move |_msg| {
            let refuse = refuse.clone();
            let counter = counter.clone();
            async move {
                let n = {
                    let mut seen = counter.lock().expect("lock");
                    *seen += 1;
                    *seen
                };
                // The handler is told the position, not the message id: the
                // loop owns the id, and the two are the same order.
                if refuse.contains(&n) {
                    return Err(SFError::Agent("handler refused this command".into()));
                }
                Ok(())
            }
        })
        .await;
    let attempts = *seen.lock().expect("lock");
    (result, attempts)
}

#[tokio::test]
async fn a_message_a_previous_consumer_never_acked_is_delivered_and_acked() {
    let backend = Arc::new(Scripted::with_pending(&["1-0", "2-0"]));
    let (result, finished) = drive(backend.clone(), &[]).await;
    result.expect("run");

    assert_eq!(finished, 2, "both reclaimed messages reach the handler");
    assert_eq!(
        backend.acked(),
        vec!["1-0".to_string(), "2-0".to_string()],
        "a command that ran is acknowledged, in the order it was delivered"
    );
}

#[tokio::test]
async fn a_failed_command_stays_pending_without_ending_the_consumer() {
    let backend = Arc::new(Scripted::with_pending(&["1-0", "2-0"]));
    let (result, _) = drive(backend.clone(), &[1]).await;
    result.expect("a failed command is not the end of the loop");

    assert_eq!(
        backend.acked(),
        vec!["2-0".to_string()],
        "only the command that finished is acknowledged; the failed one stays pending for retry"
    );
}
