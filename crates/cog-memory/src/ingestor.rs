use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use cog_core::MemoryExtractor;
use cog_core::{AgentEvent, SFResult};
use cog_core::{MemoryBackend, RawSource};

/// Configuration for [`MemoryIngestor`] retry and dead-letter behaviour.
#[derive(Debug, Clone)]
pub struct MemoryIngestorConfig {
    /// Maximum number of retry attempts before giving up on an event.
    pub max_retries: u32,
    /// Base delay in milliseconds for exponential backoff (1s, 2s, 4s, ...).
    pub retry_base_delay_ms: u64,
    /// Whether to write failed events to the DLQ namespace.
    pub enable_dlq: bool,
    /// Namespace used for dead-letter queue entries.
    pub dlq_namespace: String,
}

impl Default for MemoryIngestorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_base_delay_ms: 1000,
            enable_dlq: true,
            dlq_namespace: "dlq".into(),
        }
    }
}

/// Background service that listens to the AgentEvent broadcast stream and
/// automatically archives + extracts memories when conversations end.
/// Spawn this with [`MemoryIngestor::spawn`] and drop the returned handle
/// to stop listening.
pub struct MemoryIngestor {
    backend: Arc<dyn MemoryBackend>,
    extractor: Arc<dyn MemoryExtractor>,
    config: MemoryIngestorConfig,
}

impl MemoryIngestor {
    pub fn new(backend: Arc<dyn MemoryBackend>, extractor: Arc<dyn MemoryExtractor>) -> Self {
        Self {
            backend,
            extractor,
            config: MemoryIngestorConfig::default(),
        }
    }

    pub fn with_config(mut self, config: MemoryIngestorConfig) -> Self {
        self.config = config;
        self
    }

    /// Start a background task that consumes AgentEvents from `event_rx`.
    /// The task runs until the broadcast channel closes or a stop signal
    /// is sent via the returned [`tokio::sync::mpsc::Sender`].
    pub fn spawn(
        self,
        mut event_rx: broadcast::Receiver<AgentEvent>,
    ) -> tokio::sync::mpsc::Sender<()> {
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel::<()>(1);

        tokio::spawn(async move {
            info!("MemoryIngestor started");
            loop {
                tokio::select! {
                    result = event_rx.recv() => {
                        match result {
                            Ok(event) => {
                                if let Err(e) = self.handle_event(&event).await {
                                    warn!("Memory ingestion failed: {}", e);
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("MemoryIngestor lagged, skipped {} events", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                info!("AgentEvent broadcast closed; MemoryIngestor stopping");
                                break;
                            }
                        }
                    }
                    _ = stop_rx.recv() => {
                        info!("MemoryIngestor stopping");
                        break;
                    }
                }
            }
        });

        stop_tx
    }

    async fn handle_event(&self, event: &AgentEvent) -> SFResult<()> {
        match event {
            AgentEvent::AgentEnd {
                agent_id, messages, ..
            } => {
                debug!("Ingesting memory for agent {}", agent_id);

                // Serialize messages as raw source payload
                let payload = serde_json::to_vec(messages).unwrap_or_else(|_| b"[]".to_vec());
                let raw = RawSource::new(
                    format!("agent-{}", agent_id),
                    "default",
                    "conversation/transcript",
                    payload,
                );

                // Archive raw source
                let uri = self.backend.archive_raw(&raw).await?;
                info!("Archived raw source: {}", uri);

                // Try ingestion with exponential backoff
                if let Err(e) = self.try_ingest_with_retry(&raw).await {
                    error!(
                        "Memory ingestion failed for {} after {} retries: {}",
                        raw.id, self.config.max_retries, e
                    );

                    if self.config.enable_dlq {
                        if let Err(dlq_err) = self.write_dlq(&raw, &e.to_string()).await {
                            warn!("Failed to write DLQ entry: {}", dlq_err);
                        }
                    }
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn try_ingest_with_retry(&self, raw: &RawSource) -> SFResult<()> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            match self.try_ingest(raw).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        let delay_ms = self.config.retry_base_delay_ms * 2_u64.pow(attempt);
                        warn!(
                            "Ingestion attempt {}/{} failed for {}, retrying in {}ms",
                            attempt + 1,
                            self.config.max_retries + 1,
                            raw.id,
                            delay_ms
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| cog_core::SFError::Agent("unknown ingestion error".into())))
    }

    async fn try_ingest(&self, raw: &RawSource) -> SFResult<()> {
        // Extract schema
        let schema_entries = self.extractor.extract_schema(raw).await?;
        for entry in &schema_entries {
            self.backend.store_schema(&raw.namespace, entry).await?;
        }
        debug!("Stored {} schema entries", schema_entries.len());

        // Generate summary
        let summary = self.extractor.generate_summary(raw).await?;
        self.backend.store_summary(&raw.namespace, &summary).await?;
        debug!("Stored summary {}", summary.id);

        Ok(())
    }

    async fn write_dlq(&self, raw: &RawSource, error_msg: &str) -> SFResult<()> {
        let dlq_payload = serde_json::json!({
            "original_namespace": raw.namespace,
            "original_id": raw.id,
            "error": error_msg,
            "content_type": raw.content_type,
            "payload_preview": String::from_utf8_lossy(&raw.payload).chars().take(500).collect::<String>(),
            "dlq_timestamp": chrono::Utc::now().to_rfc3339(),
        });

        let dlq_raw = RawSource::new(
            format!("dlq-{}-{}", raw.id, chrono::Utc::now().timestamp_millis()),
            &self.config.dlq_namespace,
            "ingestion/failed",
            serde_json::to_vec(&dlq_payload).unwrap_or_default(),
        );

        let uri = self.backend.archive_raw(&dlq_raw).await?;
        info!("Wrote failed ingestion to DLQ: {}", uri);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryMemoryBackend, RuleBasedExtractor};
    use chrono::Utc;
    use cog_core::Message;

    fn agent_end(agent_id: &str) -> AgentEvent {
        AgentEvent::AgentEnd {
            agent_id: agent_id.into(),
            messages: vec![Message::user(
                "the deploy key lives in the security gateway",
            )],
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    async fn archived_count(backend: &MemoryMemoryBackend) -> usize {
        backend
            .list_raw("default", None)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
    }

    async fn wait_for_archives(backend: &MemoryMemoryBackend, expect: usize) -> bool {
        for _ in 0..100 {
            if archived_count(backend).await >= expect {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// Regression: the stop handle is the ingestor's lifetime anchor. A caller
    /// that discards it (`let _ = spawn(...)`) silently kills auto-ingest
    /// within milliseconds — the plugin used to do exactly that. While the
    /// handle is held, the task must keep consuming events across many rounds.
    #[tokio::test]
    async fn keeps_consuming_while_handle_is_held() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(16);
        let _handle = ingestor.spawn(event_tx.subscribe());

        for round in 0..3 {
            event_tx.send(agent_end(&format!("a-{round}"))).unwrap();
            assert!(
                wait_for_archives(&backend, round + 1).await,
                "event {round} must be archived while the handle is held"
            );
        }
    }

    /// Contract pin: dropping the handle stops the background task.
    #[tokio::test]
    async fn dropping_handle_stops_task() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(16);
        let handle = ingestor.spawn(event_tx.subscribe());
        drop(handle);

        let mut exited = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            // Once the task exits, its broadcast receiver is gone and send fails.
            if event_tx.send(agent_end("a-x")).is_err() {
                exited = true;
                break;
            }
        }
        assert!(exited, "task must exit after its stop handle is dropped");
    }

    /// Contract pin: an explicit stop signal through the handle ends the task.
    #[tokio::test]
    async fn explicit_stop_signal_stops_task() {
        let backend = Arc::new(MemoryMemoryBackend::new());
        let ingestor = MemoryIngestor::new(backend.clone(), Arc::new(RuleBasedExtractor::new()));
        let (event_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(16);
        let handle = ingestor.spawn(event_tx.subscribe());

        handle.send(()).await.unwrap();
        let mut exited = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if event_tx.send(agent_end("a-x")).is_err() {
                exited = true;
                break;
            }
        }
        assert!(exited, "task must exit after an explicit stop signal");
    }
}
