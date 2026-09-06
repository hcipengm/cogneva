//! Local staging for changes that cannot yet be published as PRs.
//!
//! The contribution channel may be unconfigured at evolution time (no wizard
//! step-2 connection yet, or the public platform is unreachable). A generated
//! change must not be dropped: it is serialized to the data dir and submitted
//! once a working sink exists. On startup, when the real PR sink is available,
//! staged changes are drained first — so configuring the channel later flushes
//! the backlog automatically.

use std::path::{Path, PathBuf};

use cog_core::{GeneratedChange, SFResult};

/// Directory holding one `<change_id>.json` file per staged change.
pub fn pending_dir() -> PathBuf {
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    PathBuf::from(dir).join("pending-changes")
}

/// Filesystem-safe form of a change id.
fn slug(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn path_for(change_id: &str) -> PathBuf {
    pending_dir().join(format!("{}.json", slug(change_id)))
}

/// Persist a change to the staging dir (idempotent per change id).
pub async fn stage_change(change: &GeneratedChange) -> SFResult<PathBuf> {
    let dir = pending_dir();
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| cog_core::SFError::IO(format!("create pending dir: {e}")))?;
    let path = path_for(&change.change_id);
    let json = serde_json::to_string_pretty(change)
        .map_err(|e| cog_core::SFError::Internal(format!("serialize pending change: {e}")))?;
    tokio::fs::write(&path, json)
        .await
        .map_err(|e| cog_core::SFError::IO(format!("write pending change: {e}")))?;
    Ok(path)
}

/// All changes currently staged, oldest first by file modification time.
/// Unreadable/corrupt entries are skipped (they stay on disk and are retried
/// by the next drain; a corrupt file never blocks the backlog).
pub async fn load_pending() -> Vec<GeneratedChange> {
    let mut entries: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    let Ok(mut it) = tokio::fs::read_dir(pending_dir()).await else {
        return Vec::new();
    };
    while let Ok(Some(file)) = it.next_entry().await {
        let path = file.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let mtime = file
            .metadata()
            .await
            .ok()
            .and_then(|m| m.modified().ok())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        entries.push((mtime, path));
    }
    entries.sort_by_key(|(mtime, _)| *mtime);

    let mut changes = Vec::new();
    for (_, path) in entries {
        if let Ok(text) = tokio::fs::read_to_string(&path).await {
            if let Ok(change) = serde_json::from_str::<GeneratedChange>(&text) {
                changes.push(change);
            }
        }
    }
    changes
}

/// Remove the staging file for a successfully published change.
pub async fn remove_staged(change_id: &str) {
    let _ = tokio::fs::remove_file(path_for(change_id)).await;
}

/// A [`cog_core::ChangeSink`] that only stages changes locally. Registered
/// when no PR sink can be configured, so generated changes survive until the
/// contribution channel is connected.
#[derive(Debug, Default, Clone)]
pub struct PendingChangeSink;

#[async_trait::async_trait]
impl cog_core::ChangeSink for PendingChangeSink {
    async fn submit_change(&self, change: GeneratedChange) -> SFResult<String> {
        let path = stage_change(&change).await?;
        Ok(format!("pending:{}", path.display()))
    }
}

/// Submit every staged change to `sink`, removing each file on success. A
/// failed submission is left in place for the next drain (one bad change does
/// not block the rest). Returns the number of changes successfully flushed.
pub async fn drain_into(sink: &dyn cog_core::ChangeSink) -> usize {
    let mut flushed = 0;
    for change in load_pending().await {
        match sink.submit_change(change.clone()).await {
            Ok(_) => {
                remove_staged(&change.change_id).await;
                flushed += 1;
            }
            Err(e) => {
                tracing::warn!(
                    change = %change.change_id,
                    error = %e,
                    "staged change flush failed; will retry on next drain"
                );
            }
        }
    }
    flushed
}

/// Whether a path lives under the staging directory (best-effort guard used by
/// the whitelist gate tests and diagnostics).
pub fn is_pending_path(path: &Path) -> bool {
    path.starts_with(pending_dir())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ENV_LOCK;

    fn sample(id: &str) -> GeneratedChange {
        GeneratedChange {
            change_id: id.into(),
            goal: "g".into(),
            content: "diff".into(),
            affected_files: vec![],
            rationale: None,
            pge_mode: "squad".into(),
            self_review_score: None,
        }
    }

    #[tokio::test]
    async fn stage_load_and_remove_roundtrip() {
        let _guard = ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        assert!(load_pending().await.is_empty());
        let c = sample("chg-1");
        let path = stage_change(&c).await.unwrap();
        assert!(path.exists());
        assert_eq!(load_pending().await.len(), 1);

        remove_staged("chg-1").await;
        assert!(load_pending().await.is_empty());

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn drain_flushes_each_and_keeps_failures() {
        let _guard = ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", dir.path());

        stage_change(&sample("ok-1")).await.unwrap();
        stage_change(&sample("ok-2")).await.unwrap();

        #[derive(Debug)]
        struct Recorder {
            fail_id: String,
        }
        #[async_trait::async_trait]
        impl cog_core::ChangeSink for Recorder {
            async fn submit_change(&self, change: GeneratedChange) -> SFResult<String> {
                if change.change_id == self.fail_id {
                    return Err(cog_core::SFError::Internal("synthetic failure".into()));
                }
                Ok(change.change_id)
            }
        }

        // Drain with a failing sink for one id: nothing removed yet semantics
        // are exercised by adding a failing entry then draining.
        stage_change(&sample("bad-1")).await.unwrap();
        let sink = Recorder {
            fail_id: "bad-1".into(),
        };
        let flushed = drain_into(&sink).await;
        assert_eq!(flushed, 2, "ok-1 and ok-2 flush; bad-1 stays");
        let remaining: Vec<String> = load_pending()
            .await
            .iter()
            .map(|c| c.change_id.clone())
            .collect();
        assert_eq!(remaining, vec!["bad-1".to_string()]);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }
}
