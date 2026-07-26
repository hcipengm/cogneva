use std::path::{Path, PathBuf};

use cog_core::{SFError, SFResult};

/// Represents a single git worktree entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeEntry {
    pub path: PathBuf,
    pub commit: String,
    pub branch: Option<String>,
    pub locked: bool,
    pub bare: bool,
}

/// Manager for git worktree operations.
/// Provides branch-level isolation via git worktrees, enabling:
/// - Parallel task execution in separate working directories
/// - Natural rollback by switching branches
/// - Cleanup of stale or completed worktrees
#[derive(Debug, Clone)]
pub struct WorktreeManager {
    repo_path: PathBuf,
}

impl WorktreeManager {
    /// Create a new manager for the repository at `repo_path`.
    pub fn new(repo_path: impl AsRef<Path>) -> Self {
        Self {
            repo_path: repo_path.as_ref().to_path_buf(),
        }
    }

    /// List all existing worktrees for the repository.
    pub fn list(&self) -> SFResult<Vec<WorktreeEntry>> {
        let output = std::process::Command::new("git")
            .args(["worktree", "list", "--porcelain"])
            .current_dir(&self.repo_path)
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree list failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree list error: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(parse_worktree_list(&stdout))
    }

    /// Add a new worktree at `path` tracking `branch`.
    /// If `branch` does not exist it is created. The worktree directory
    /// is created automatically.
    pub fn add(&self, path: impl AsRef<Path>, branch: &str) -> SFResult<WorktreeEntry> {
        let path = path.as_ref();
        std::fs::create_dir_all(path.parent().unwrap_or(path))
            .map_err(|e| SFError::Agent(format!("create worktree parent dir failed: {}", e)))?;

        let output = std::process::Command::new("git")
            .args(["worktree", "add", "-B", branch])
            .arg(path)
            .current_dir(&self.repo_path)
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree add failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree add error: {}",
                stderr
            )));
        }

        self.find_by_path(path)
            .ok_or_else(|| SFError::Agent("worktree added but not found in list".into()))
    }

    /// Add a detached worktree at `path` checked out to `commit`.
    pub fn add_detached(&self, path: impl AsRef<Path>, commit: &str) -> SFResult<WorktreeEntry> {
        let path = path.as_ref();
        std::fs::create_dir_all(path.parent().unwrap_or(path))
            .map_err(|e| SFError::Agent(format!("create worktree parent dir failed: {}", e)))?;

        let output = std::process::Command::new("git")
            .args(["worktree", "add", "--detach"])
            .arg(path)
            .arg(commit)
            .current_dir(&self.repo_path)
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree add failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree add error: {}",
                stderr
            )));
        }

        self.find_by_path(path)
            .ok_or_else(|| SFError::Agent("worktree added but not found in list".into()))
    }

    /// Remove a worktree at `path`.
    /// If `force` is true, the worktree is removed even if it is locked
    /// or contains uncommitted changes.
    pub fn remove(&self, path: impl AsRef<Path>, force: bool) -> SFResult<()> {
        let path = path.as_ref();
        let mut cmd = std::process::Command::new("git");
        cmd.args(["worktree", "remove"]);
        if force {
            cmd.arg("--force");
        }
        cmd.arg(path).current_dir(&self.repo_path);

        let output = cmd
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree remove failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree remove error: {}",
                stderr
            )));
        }
        Ok(())
    }

    /// Prune worktree administrative files that no longer exist on disk.
    pub fn prune(&self) -> SFResult<()> {
        let output = std::process::Command::new("git")
            .args(["worktree", "prune"])
            .current_dir(&self.repo_path)
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree prune failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree prune error: {}",
                stderr
            )));
        }
        Ok(())
    }

    /// Lock a worktree so it is not pruned automatically.
    pub fn lock(&self, path: impl AsRef<Path>, reason: Option<&str>) -> SFResult<()> {
        let path = path.as_ref();
        let mut cmd = std::process::Command::new("git");
        cmd.args(["worktree", "lock"]);
        if let Some(r) = reason {
            cmd.arg("--reason").arg(r);
        }
        cmd.arg(path).current_dir(&self.repo_path);

        let output = cmd
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree lock failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree lock error: {}",
                stderr
            )));
        }
        Ok(())
    }

    /// Unlock a previously locked worktree.
    pub fn unlock(&self, path: impl AsRef<Path>) -> SFResult<()> {
        let path = path.as_ref();
        let output = std::process::Command::new("git")
            .args(["worktree", "unlock"])
            .arg(path)
            .current_dir(&self.repo_path)
            .output()
            .map_err(|e| SFError::Agent(format!("git worktree unlock failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git worktree unlock error: {}",
                stderr
            )));
        }
        Ok(())
    }

    /// Find a worktree entry by its path.
    pub fn find_by_path(&self, path: impl AsRef<Path>) -> Option<WorktreeEntry> {
        let target = path.as_ref();
        self.list().ok()?.into_iter().find(|w| w.path == target)
    }

    /// Check whether a worktree exists at `path`.
    pub fn exists(&self, path: impl AsRef<Path>) -> bool {
        self.find_by_path(path).is_some()
    }
}

fn parse_worktree_list(output: &str) -> Vec<WorktreeEntry> {
    let mut entries = Vec::new();
    let mut current: Option<WorktreeEntry> = None;

    for line in output.lines() {
        if line.is_empty() {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            continue;
        }

        if line.starts_with("worktree ") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            let path = line.strip_prefix("worktree ").unwrap_or("").to_string();
            current = Some(WorktreeEntry {
                path: PathBuf::from(path),
                commit: String::new(),
                branch: None,
                locked: false,
                bare: false,
            });
        } else if let Some(ref mut entry) = current {
            if line.starts_with("HEAD ") {
                entry.commit = line.strip_prefix("HEAD ").unwrap_or("").to_string();
            } else if line.starts_with("branch ") {
                entry.branch = Some(line.strip_prefix("branch ").unwrap_or("").to_string());
            } else if line == "locked" || line.starts_with("locked ") {
                entry.locked = true;
            } else if line == "bare" {
                entry.bare = true;
            }
        }
    }

    if let Some(entry) = current {
        entries.push(entry);
    }

    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_worktree_list_porcelain() {
        let output = r#"worktree /home/user/project
HEAD abcd1234
branch refs/heads/main

worktree /home/user/project-feature
HEAD efgh5678
branch refs/heads/feature-x
locked

worktree /home/user/project-detached
HEAD 1234abcd
detached

worktree /home/user/project-bare
HEAD 00000000
bare
"#;

        let entries = parse_worktree_list(output);
        assert_eq!(entries.len(), 4);

        assert_eq!(entries[0].path, PathBuf::from("/home/user/project"));
        assert_eq!(entries[0].commit, "abcd1234");
        assert_eq!(entries[0].branch, Some("refs/heads/main".into()));
        assert!(!entries[0].locked);
        assert!(!entries[0].bare);

        assert_eq!(entries[1].path, PathBuf::from("/home/user/project-feature"));
        assert_eq!(entries[1].commit, "efgh5678");
        assert_eq!(entries[1].branch, Some("refs/heads/feature-x".into()));
        assert!(entries[1].locked);
        assert!(!entries[1].bare);

        assert_eq!(
            entries[2].path,
            PathBuf::from("/home/user/project-detached")
        );
        assert_eq!(entries[2].commit, "1234abcd");
        assert_eq!(entries[2].branch, None);
        assert!(!entries[2].locked);
        assert!(!entries[2].bare);

        assert_eq!(entries[3].path, PathBuf::from("/home/user/project-bare"));
        assert_eq!(entries[3].commit, "00000000");
        assert!(!entries[3].locked);
        assert!(entries[3].bare);
    }

    #[test]
    fn test_parse_worktree_list_empty() {
        let entries = parse_worktree_list("");
        assert!(entries.is_empty());
    }
}
