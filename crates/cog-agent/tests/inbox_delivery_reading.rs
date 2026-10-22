//! What the inbox loop leaves behind after it has finished with a command.
//!
//! A command that ran and a command whose handler failed both leave the same
//! thing in the queue -- an entry -- and the failure is retried only when this
//! consumer next starts. Until the counters existed, the last evidence of a
//! failure was a log line, which is gone long before anyone asks why a run the
//! sender was told it had delivered never happened.
//!
//! One test in this file on purpose: the counters are process-global, and cargo
//! runs the tests in a file on concurrent threads, so a second test driving the
//! same loop would move the numbers under this one's assertions.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use cog_core::{
    InboxMessage, MessageBackend, MessageStream, Observable, PendingStats, SFError, SFResult,
};

/// Hands the loop one scripted batch of commands, then nothing.
struct Scripted {
    batch: Mutex<Vec<(String, Vec<u8>)>>,
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
        // The batch is delivered through the reclaim pass below, so a fresh read
        // has nothing to add.
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

    async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
        Ok(())
    }

    async fn claim_pending(
        &self,
        _stream: &str,
        _group: &str,
        _min_idle_ms: u64,
        count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        let mut batch = self.batch.lock().expect("lock");
        let take = count.min(batch.len());
        Ok(Some(batch.drain(..take).collect()))
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

/// The inbox counters as the scrape reads them.
async fn counts(kind: &str) -> (f64, f64) {
    let metrics = cog_agent::observable::global_observable()
        .collect_metrics("")
        .await
        .expect("collect_metrics");
    let read = |name: &str| {
        metrics
            .iter()
            .find(|m| m.name == name && m.labels.get("kind").map(String::as_str) == Some(kind))
            .map(|m| m.value)
            .unwrap_or(-1.0)
    };
    (
        read("agent_inbox_delivered_total"),
        read("agent_inbox_undelivered_total"),
    )
}

/// A failed command is booked as a failure and the one that finished beside it
/// as a delivery, in the same label. Reading only the delivered counter would
/// call a loop that refused every command healthy on the strength of the
/// commands it did run.
#[tokio::test]
async fn a_command_that_failed_is_counted_apart_from_one_that_finished() {
    let (delivered_before, undelivered_before) = counts("abort").await;
    assert_eq!(
        (delivered_before, undelivered_before),
        (0.0, 0.0),
        "the counters are written from zero in a process that has run nothing"
    );

    let backend: Arc<dyn MessageBackend> = Arc::new(Scripted {
        batch: Mutex::new(vec![
            (
                "1-0".to_string(),
                serde_json::to_vec(&InboxMessage::Abort).expect("encode"),
            ),
            (
                "2-0".to_string(),
                serde_json::to_vec(&InboxMessage::Abort).expect("encode"),
            ),
        ]),
    });

    let seen = Arc::new(Mutex::new(0usize));
    let counter = seen.clone();
    cog_agent::AgentConsumer::new("probe_reading:agent", backend)
        .run(move |_msg| {
            let counter = counter.clone();
            async move {
                let mut seen = counter.lock().expect("lock");
                *seen += 1;
                if *seen == 1 {
                    return Err(SFError::Agent("handler refused this command".into()));
                }
                Ok(())
            }
        })
        .await
        .expect("a failed command is not the end of the loop");

    assert_eq!(
        *seen.lock().expect("lock"),
        2,
        "both commands were handed over"
    );
    assert_eq!(
        counts("abort").await,
        (1.0, 1.0),
        "one command finished and one was left pending, and the two are counted apart"
    );
}
