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

/// A pending batch that only becomes reclaimable after the starting pass.
///
/// This is what a command written seconds before the previous incarnation died
/// looks like from the next one: still inside the idle threshold when the
/// starting pass asks, so that pass legitimately sees nothing.
struct LateArrival {
    quiet_claims: usize,
    /// Hand the same batch back on every claim, the way a store does while the
    /// entries stay pending and unacked, instead of handing each one out once.
    repeat: bool,
    claims: Mutex<usize>,
    pending: Mutex<Vec<(String, Vec<u8>)>>,
    acked: Mutex<Vec<String>>,
}

#[async_trait]
impl MessageBackend for LateArrival {
    async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
        Ok(())
    }

    async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
        Ok(None)
    }

    async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
        // No new messages: everything the run does has to come from a reclaim.
        Ok(Box::pin(futures::stream::pending()))
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
        let mut claims = self.claims.lock().expect("lock");
        *claims += 1;
        if *claims <= self.quiet_claims {
            return Ok(Some(Vec::new()));
        }
        let mut pending = self.pending.lock().expect("lock");
        let take = count.min(pending.len());
        if self.repeat {
            return Ok(Some(pending[..take].to_vec()));
        }
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

/// Wait until `done` holds or the budget runs out.
async fn until<F: Fn() -> bool>(done: F, budget: std::time::Duration) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    while tokio::time::Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    done()
}

/// The case the backstop exists for: a command that was too young to reclaim at
/// start is picked up by the running consumer, without anything restarting it.
#[tokio::test]
async fn a_message_that_became_reclaimable_after_start_is_picked_up_without_a_restart() {
    let backend = Arc::new(LateArrival {
        quiet_claims: 1,
        repeat: false,
        claims: Mutex::new(0),
        pending: Mutex::new(vec![(
            "9-0".to_string(),
            serde_json::to_vec(&InboxMessage::Abort).expect("encode"),
        )]),
        acked: Mutex::new(Vec::new()),
    });
    let consumer = cog_agent::AgentConsumer::new(
        "probe_late:agent",
        backend.clone() as Arc<dyn MessageBackend>,
    );
    let run = tokio::spawn(consumer.run_with_reclaim_interval(
        |_msg| async move { Ok(()) },
        std::time::Duration::from_millis(20),
    ));

    let watched = backend.clone();
    let delivered = until(
        move || !watched.acked.lock().expect("lock").is_empty(),
        std::time::Duration::from_secs(5),
    )
    .await;
    run.abort();

    assert!(
        delivered,
        "a command the starting pass was too early for has to be reclaimed by the running consumer"
    );
    assert_eq!(
        backend.acked.lock().expect("lock").as_slice(),
        ["9-0".to_string()],
        "the backstop pass takes the message back and, having run it, acknowledges it"
    );
}

/// A command this incarnation already ran and failed is not handed over again
/// by the backstop pass: retrying it belongs to the next start, not to a timer
/// that would re-run its side effects every interval.
#[tokio::test]
async fn a_command_this_incarnation_already_failed_is_not_retried_by_the_backstop() {
    let backend = Arc::new(LateArrival {
        // Every claim hands the same message back, as a store does while the
        // entry stays pending.
        quiet_claims: 0,
        repeat: true,
        claims: Mutex::new(0),
        pending: Mutex::new(vec![(
            "5-0".to_string(),
            serde_json::to_vec(&InboxMessage::Abort).expect("encode"),
        )]),
        acked: Mutex::new(Vec::new()),
    });
    let attempts = Arc::new(Mutex::new(0usize));
    let counter = attempts.clone();
    let consumer = cog_agent::AgentConsumer::new(
        "probe_sticky:agent",
        backend.clone() as Arc<dyn MessageBackend>,
    );
    let run = tokio::spawn(consumer.run_with_reclaim_interval(
        move |_msg| {
            let counter = counter.clone();
            async move {
                *counter.lock().expect("lock") += 1;
                Err::<(), SFError>(SFError::Agent("handler refused this command".into()))
            }
        },
        std::time::Duration::from_millis(20),
    ));

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    run.abort();

    assert_eq!(
        *attempts.lock().expect("lock"),
        1,
        "the failing command is handed to the handler once per incarnation, not once per interval"
    );
    assert!(
        backend.acked.lock().expect("lock").is_empty(),
        "a failed command is never acked"
    );
}
