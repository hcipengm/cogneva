use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::SFResult;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::engine::HookPublisher;

/// Local WAL buffer for hook events.
/// When the primary publisher (Redis) fails, events are appended to a local
/// file.  A background task periodically replays buffered events when the
/// primary comes back online.
pub struct WalHookPublisher {
    primary: Arc<dyn HookPublisher>,
    wal_path: PathBuf,
}

impl WalHookPublisher {
    pub fn new(primary: Arc<dyn HookPublisher>, wal_path: impl Into<PathBuf>) -> Self {
        Self {
            primary,
            wal_path: wal_path.into(),
        }
    }

    /// Append a single event to the WAL file.
    async fn append_to_wal(&self, channel: &str, payload: &serde_json::Value) -> SFResult<()> {
        let line = serde_json::json!({
            "channel": channel,
            "payload": payload,
            "ts": chrono::Utc::now().to_rfc3339(),
        });
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("WAL open failed: {}", e)))?;
        file.write_all(line.to_string().as_bytes())
            .await
            .map_err(|e| cog_core::SFError::IO(format!("WAL write failed: {}", e)))?;
        file.write_all(
            b"
",
        )
        .await
        .map_err(|e| cog_core::SFError::IO(format!("WAL write failed: {}", e)))?;
        Ok(())
    }

    /// Replay all buffered events to the primary publisher, then truncate WAL.
    pub async fn replay(&self) -> SFResult<usize> {
        if !self.wal_path.exists() {
            return Ok(0);
        }
        let file = fs::File::open(&self.wal_path)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("WAL read failed: {}", e)))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut replayed = 0usize;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let record: serde_json::Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("WAL replay: skip malformed line: {}", e);
                    continue;
                }
            };
            let channel = record
                .get("channel")
                .and_then(|v| v.as_str())
                .unwrap_or("orchestrator:events");
            let payload = record
                .get("payload")
                .cloned()
                .unwrap_or(serde_json::json!({}));
            if let Err(e) = self.primary.publish_redis_stream(channel, &payload).await {
                tracing::warn!("WAL replay: primary still failing: {}", e);
                break;
            }
            replayed += 1;
        }
        // Truncate WAL if all events were replayed
        if replayed > 0 {
            fs::write(&self.wal_path, b"")
                .await
                .map_err(|e| cog_core::SFError::IO(format!("WAL truncate failed: {}", e)))?;
        }
        Ok(replayed)
    }
}

#[async_trait]
impl HookPublisher for WalHookPublisher {
    async fn publish_webhook(
        &self,
        url: &str,
        headers: &std::collections::HashMap<String, String>,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        // Webhooks are not buffered; delegate directly
        self.primary.publish_webhook(url, headers, payload).await
    }

    async fn publish_redis_stream(
        &self,
        channel: &str,
        payload: &serde_json::Value,
    ) -> SFResult<()> {
        match self.primary.publish_redis_stream(channel, payload).await {
            Ok(()) => {
                // Primary succeeded — try to replay any buffered events
                let _ = self.replay().await;
                Ok(())
            }
            Err(e) => {
                tracing::warn!("Primary Redis failed, buffering to WAL: {}", e);
                self.append_to_wal(channel, payload).await?;
                Ok(())
            }
        }
    }

    async fn notify_user(&self, user_id: &str, payload: &serde_json::Value) -> SFResult<()> {
        // Notifications fall back to Redis stream channel
        let channel = format!("sf:notify:{}", user_id);
        self.publish_redis_stream(&channel, payload).await
    }
}
