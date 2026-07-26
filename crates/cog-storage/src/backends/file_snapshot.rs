use async_trait::async_trait;

use cog_core::{SFError, SFResult};

/// File-based snapshot store that serialises snapshots as JSON.
pub struct FileSnapshotStore {
    dir: std::path::PathBuf,
}

impl FileSnapshotStore {
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn path(&self, snapshot_id: &str) -> std::path::PathBuf {
        self.dir.join(format!("{}.json", snapshot_id))
    }
}

// ==========================================================================
// CheckpointStore implementation
// ==========================================================================

use cog_core::{AgentCheckpoint, CheckpointStore};

#[async_trait]
impl CheckpointStore for FileSnapshotStore {
    async fn save(&self, checkpoint: &AgentCheckpoint) -> SFResult<String> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let path = self.path(&checkpoint.checkpoint_id);
        let json = serde_json::to_string_pretty(checkpoint)?;
        tokio::fs::write(&path, json)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        Ok(checkpoint.checkpoint_id.clone())
    }

    async fn load(&self, checkpoint_id: &str) -> SFResult<Option<AgentCheckpoint>> {
        let path = self.path(checkpoint_id);
        match tokio::fs::read_to_string(&path).await {
            Ok(json) => {
                let cp: AgentCheckpoint =
                    serde_json::from_str(&json).map_err(SFError::Serialization)?;
                Ok(Some(cp))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SFError::IO(e.to_string())),
        }
    }

    async fn delete(&self, checkpoint_id: &str) -> SFResult<()> {
        let path = self.path(checkpoint_id);
        match tokio::fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(SFError::IO(e.to_string())),
        }
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentCheckpoint>> {
        let mut entries = tokio::fs::read_dir(&self.dir)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let mut cps = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?
        {
            if entry
                .file_type()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
                .is_file()
            {
                if let Ok(json) = tokio::fs::read_to_string(entry.path()).await {
                    if let Ok(cp) = serde_json::from_str::<AgentCheckpoint>(&json) {
                        cps.push(cp);
                    }
                }
            }
        }
        cps.sort_by_key(|a| std::cmp::Reverse(a.timestamp));
        cps.truncate(limit);
        Ok(cps)
    }
}
