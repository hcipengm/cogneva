//! 文件追加式审计流：JSONL 追加 + 启动时整链校验。
//!
//! 文件只追加、不改写；进程启动时 `open` 会校验已有链，
//! 链损坏时返回错误而不是静默续写。

use std::path::{Path, PathBuf};

use cog_core::{verify_chain, AuditEvent, AuditKind, AuditStream, SFError, SFResult};
use tokio::io::AsyncWriteExt;

/// 追加式 JSONL 审计流。
pub struct FileAuditStream {
    path: PathBuf,
    /// 当前链尾（None = 空链）。
    tail: tokio::sync::RwLock<Option<AuditEvent>>,
}

impl FileAuditStream {
    /// 打开（必要时创建）审计文件并校验已有链。
    pub async fn open(path: impl AsRef<Path>) -> SFResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SFError::IO(format!("create audit dir: {e}")))?;
        }

        let existing = read_jsonl(&path).await?;
        let verification = verify_chain(&existing);
        if !verification.valid {
            return Err(SFError::Validation(format!(
                "audit chain corrupted at seq {:?} in {}",
                verification.first_broken_seq,
                path.display()
            )));
        }

        Ok(Self {
            path,
            tail: tokio::sync::RwLock::new(existing.into_iter().last()),
        })
    }
}

async fn read_jsonl(path: &Path) -> SFResult<Vec<AuditEvent>> {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(SFError::IO(format!("read audit log: {e}"))),
    };
    let mut events = Vec::new();
    for (i, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: AuditEvent = serde_json::from_str(line)
            .map_err(|e| SFError::Validation(format!("audit log line {} malformed: {e}", i + 1)))?;
        events.push(event);
    }
    Ok(events)
}

#[async_trait::async_trait]
impl AuditStream for FileAuditStream {
    async fn append(
        &self,
        kind: AuditKind,
        actor: &str,
        target: &str,
        action: &str,
        detail: serde_json::Value,
    ) -> SFResult<AuditEvent> {
        let mut guard = self.tail.write().await;
        let event = AuditEvent::next(guard.as_ref(), kind, actor, target, action, detail);

        let mut line = serde_json::to_string(&event).map_err(SFError::Serialization)?;
        line.push('\n');
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| SFError::IO(format!("open audit log: {e}")))?;
        file.write_all(line.as_bytes())
            .await
            .map_err(|e| SFError::IO(format!("append audit log: {e}")))?;
        file.flush()
            .await
            .map_err(|e| SFError::IO(format!("flush audit log: {e}")))?;

        *guard = Some(event.clone());
        Ok(event)
    }

    async fn read_all(&self) -> SFResult<Vec<AuditEvent>> {
        read_jsonl(&self.path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn append_and_verify_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        let stream = FileAuditStream::open(&path).await.unwrap();
        stream
            .append(
                AuditKind::ChangeOperation,
                "system",
                "change-1",
                "change.apply",
                serde_json::json!({"files": 2}),
            )
            .await
            .unwrap();
        stream
            .append(
                AuditKind::AgentDecision,
                "agent-7",
                "task-9",
                "agent.plan",
                serde_json::json!({"mode": "pipeline"}),
            )
            .await
            .unwrap();

        let verification = stream.verify().await.unwrap();
        assert!(verification.valid);
        assert_eq!(verification.records_checked, 2);

        // 重新打开：链应续接而不是重零开始
        let reopened = FileAuditStream::open(&path).await.unwrap();
        let third = reopened
            .append(
                AuditKind::HookTrigger,
                "hook-engine",
                "hook-1",
                "hook.pre_prompt",
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(third.seq, 3);
        assert!(reopened.verify().await.unwrap().valid);
    }

    #[tokio::test]
    async fn corrupted_chain_rejected_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        let stream = FileAuditStream::open(&path).await.unwrap();
        stream
            .append(
                AuditKind::ChangeOperation,
                "system",
                "p1",
                "change.apply",
                serde_json::json!({}),
            )
            .await
            .unwrap();
        drop(stream);

        // 篡改文件内容
        let content = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, content.replace("change.apply", "change.forged")).unwrap();

        match FileAuditStream::open(&path).await {
            Ok(_) => panic!("corrupted chain should be rejected"),
            Err(e) => assert!(e.to_string().contains("corrupted")),
        }
    }
}
