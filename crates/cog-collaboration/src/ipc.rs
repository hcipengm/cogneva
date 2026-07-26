use cog_core::{SFError, SFResult};
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// A single message exchanged via filesystem IPC.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct IpcMessage {
    pub id: String,
    pub sender: String,
    pub recipient: String,
    pub payload: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Filesystem-based IPC for Squad-level communication.
/// Uses a directory layout:
/// ```text
/// {base}/{squad_id}/inbox/2026-04-26.jsonl
/// {base}/{squad_id}/outbox/2026-04-26.jsonl
/// ```
/// Messages are appended as JSON Lines.  Each day gets its own file.
#[derive(Debug, Clone)]
pub struct FileSystemIpc {
    base: PathBuf,
}

/// Channel direction within a squad.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpcChannel {
    Inbox,
    Outbox,
}

impl std::fmt::Display for IpcChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IpcChannel::Inbox => write!(f, "inbox"),
            IpcChannel::Outbox => write!(f, "outbox"),
        }
    }
}

impl FileSystemIpc {
    /// Create a new IPC instance rooted at `base`.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// Ensure the directory for a squad channel exists.
    async fn ensure_dir(&self, squad_id: &str, channel: IpcChannel) -> SFResult<PathBuf> {
        let dir = self.base.join(squad_id).join(channel.to_string());
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(dir)
    }

    /// Current date-stamped filename.
    fn current_file() -> String {
        chrono::Utc::now().format("%Y-%m-%d").to_string() + ".jsonl"
    }

    /// Write a message to the given squad channel.
    /// The message is serialised as a single JSON line and atomically appended
    /// to today's file.
    pub async fn write_message(
        &self,
        squad_id: &str,
        channel: IpcChannel,
        message: &IpcMessage,
    ) -> SFResult<()> {
        let dir = self.ensure_dir(squad_id, channel).await?;
        let path = dir.join(Self::current_file());
        let line = serde_json::to_string(message)? + "\n";

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        file.write_all(line.as_bytes())
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        file.flush().await.map_err(|e| SFError::IO(e.to_string()))?;
        Ok(())
    }

    /// Read all messages from a squad channel that are newer than `since`.
    /// If `since` is `None`, returns **all** messages.
    pub async fn read_messages(
        &self,
        squad_id: &str,
        channel: IpcChannel,
        since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<Vec<IpcMessage>> {
        let dir = self.base.join(squad_id).join(channel.to_string());
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = fs::read_dir(&dir)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        let mut messages = Vec::new();

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?
        {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
                continue;
            }

            let file = fs::File::open(&path)
                .await
                .map_err(|e| SFError::IO(e.to_string()))?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Some(line) = lines
                .next_line()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
            {
                if line.trim().is_empty() {
                    continue;
                }
                let msg: IpcMessage =
                    serde_json::from_str(&line).map_err(SFError::Serialization)?;
                if let Some(since_ts) = since {
                    if msg.timestamp > since_ts {
                        messages.push(msg);
                    }
                } else {
                    messages.push(msg);
                }
            }
        }

        // Preserve chronological order (files are named by date, and we read
        // directory entries in whatever order the OS gives; sort by timestamp).
        messages.sort_by_key(|a| a.timestamp);
        Ok(messages)
    }

    /// Poll a squad channel for new messages, yielding them as they arrive.
    /// This is a simple polling loop — suitable for local development and
    /// testing.  Production deployments should use `MessageBackend` (Redis
    /// Streams / NATS) for real-time delivery.
    pub async fn poll(
        &self,
        squad_id: &str,
        channel: IpcChannel,
        interval_ms: u64,
    ) -> SFResult<Vec<IpcMessage>> {
        let since = Some(chrono::Utc::now());
        tokio::time::sleep(tokio::time::Duration::from_millis(interval_ms)).await;
        self.read_messages(squad_id, channel, since).await
    }

    /// List all squad IDs that currently have IPC directories.
    pub async fn list_squads(&self) -> SFResult<Vec<String>> {
        if !self.base.exists() {
            return Ok(Vec::new());
        }
        let mut entries = fs::read_dir(&self.base)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let mut squads = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?
        {
            let meta = entry
                .metadata()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?;
            if meta.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    squads.push(name.to_string());
                }
            }
        }
        Ok(squads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ipc_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let ipc = FileSystemIpc::new(tmp.path());

        let msg = IpcMessage {
            id: "msg-1".into(),
            sender: "agent-a".into(),
            recipient: "agent-b".into(),
            payload: serde_json::json!({"task": "plan"}),
            timestamp: chrono::Utc::now(),
        };

        ipc.write_message("squad-1", IpcChannel::Inbox, &msg)
            .await
            .unwrap();

        let fetched = ipc
            .read_messages("squad-1", IpcChannel::Inbox, None)
            .await
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id, "msg-1");
        assert_eq!(fetched[0].payload, serde_json::json!({"task": "plan"}));
    }

    #[tokio::test]
    async fn test_ipc_since_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let ipc = FileSystemIpc::new(tmp.path());

        let old = IpcMessage {
            id: "old".into(),
            sender: "a".into(),
            recipient: "b".into(),
            payload: serde_json::json!(null),
            timestamp: chrono::Utc::now() - chrono::Duration::seconds(10),
        };
        let new = IpcMessage {
            id: "new".into(),
            sender: "a".into(),
            recipient: "b".into(),
            payload: serde_json::json!(null),
            timestamp: chrono::Utc::now(),
        };

        ipc.write_message("squad-x", IpcChannel::Outbox, &old)
            .await
            .unwrap();
        ipc.write_message("squad-x", IpcChannel::Outbox, &new)
            .await
            .unwrap();

        let since = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
        let fetched = ipc
            .read_messages("squad-x", IpcChannel::Outbox, since)
            .await
            .unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id, "new");
    }

    #[tokio::test]
    async fn test_ipc_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let ipc = FileSystemIpc::new(tmp.path());
        let fetched = ipc
            .read_messages("nonexistent", IpcChannel::Inbox, None)
            .await
            .unwrap();
        assert!(fetched.is_empty());
    }
}
