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
        // state-file: store — 文件后端自己的介质：接上它时这里就是正本，不是第二份
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
        let path = self.path(&checkpoint.checkpoint_id);
        let json = serde_json::to_string_pretty(checkpoint)?;
        // Temp-and-rename: a listing that reads this file while a save
        // replaces it must see either the old checkpoint or the new one, never
        // half of either. `SurvivesPowerLoss` because the checkpoint is the
        // only copy of the state it records.
        cog_core::atomic_write::write_bytes_async(
            &path,
            json.as_bytes(),
            cog_core::atomic_write::Durability::SurvivesPowerLoss,
        )
        .await?;
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
            let path = entry.path();
            if !entry
                .file_type()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
                .is_file()
            {
                continue;
            }
            // Only this store's own files count. A save in flight leaves a
            // `.<name>.<pid>.<seq>.tmp` sibling, and any other stray file is
            // not a snapshot; reading either as one would turn a write that
            // has not landed yet into a parse failure for the whole listing.
            if !path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".json"))
            {
                continue;
            }
            let json = match tokio::fs::read_to_string(&path).await {
                Ok(json) => json,
                // Gone between the directory read and this one: a delete in
                // flight, not a listing that failed.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(SFError::IO(format!("reading {}: {}", path.display(), e))),
            };
            let cp: AgentCheckpoint = serde_json::from_str(&json)
                .map_err(|e| SFError::IO(format!("parsing {}: {}", path.display(), e)))?;
            cps.push(cp);
        }
        cps.sort_by_key(|a| std::cmp::Reverse(a.timestamp));
        cps.truncate(limit);
        Ok(cps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(id: &str) -> AgentCheckpoint {
        AgentCheckpoint {
            checkpoint_id: id.to_string(),
            task_id: "t".into(),
            agent_state: serde_json::json!({ "state": "running" }),
            context_window: Vec::new(),
            event_offset: 3,
            timestamp: chrono::Utc::now(),
        }
    }

    /// One file that is there but cannot be parsed makes the listing short by
    /// one snapshot. Reporting the path is the only way to tell that from a
    /// directory that genuinely holds fewer.
    #[tokio::test]
    async fn a_listing_reports_a_file_it_cannot_parse() {
        let dir = std::env::temp_dir().join(format!("fss-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("broken.json"), b"{ not json")
            .await
            .unwrap();

        let store = FileSnapshotStore::new(&dir);
        let err = store
            .list(10)
            .await
            .expect_err("an unparseable snapshot must fail the listing");
        assert!(
            err.to_string().contains("broken.json"),
            "the error must name the file to inspect, got: {err}"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// A save publishes through a rename; its temp sibling must not survive a
    /// successful save, and a save still in flight must not be read as a
    /// snapshot.
    #[tokio::test]
    async fn a_save_leaves_no_temporary_file_and_strays_are_not_read() {
        let dir = std::env::temp_dir().join(format!("fss-{}", uuid::Uuid::new_v4()));
        let store = FileSnapshotStore::new(&dir);
        store.save(&checkpoint("snap-1")).await.unwrap();

        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(&dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        assert_eq!(names, vec!["snap-1.json".to_string()]);

        // The shape `atomic_write` leaves beside a target while it is writing.
        tokio::fs::write(dir.join(".snap-2.json.4242.7.tmp"), b"{ half")
            .await
            .unwrap();
        let listed = store
            .list(10)
            .await
            .expect("a file that is not this store's is not a listing failure");
        assert_eq!(
            listed
                .iter()
                .map(|c| c.checkpoint_id.as_str())
                .collect::<Vec<_>>(),
            vec!["snap-1"],
            "only the published snapshot is listed"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }
}
