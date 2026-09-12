//! Deployment stage of L2 self-evolution.
//!
//! Responsibilities:
//! - Commit applied changes to the local git repository.
//! - Build a new release binary with `cargo build --release --bin cogneva`.
//! - If the build fails, roll back the git commit.
//! - Stage the new binary for the supervisor's binary switcher.

use std::path::{Path, PathBuf};
use std::time::Instant;

use cog_core::{SFError, SFResult};
use tracing::{info, warn};

/// Artifact produced by a successful build.
#[derive(Debug, Clone)]
pub struct BuildArtifact {
    pub change_id: String,
    pub commit_hash: String,
    pub new_binary_path: PathBuf,
    pub build_duration_secs: u64,
}

/// Deployer that turns tested source changes into a staged release binary.
#[derive(Debug, Clone)]
pub struct EvolutionDeployer {
    project_root: PathBuf,
    binary_dir: PathBuf,
    backup_dir: PathBuf,
    binary_name: String,
    build_timeout_secs: u64,
    git_name: String,
    git_email: String,
    /// 共享 CARGO_TARGET_DIR：产物不进临时工作树，工作树用完即弃不丢缓存。
    target_dir: Option<PathBuf>,
}

impl EvolutionDeployer {
    pub fn new(
        project_root: impl Into<PathBuf>,
        binary_dir: impl Into<PathBuf>,
        backup_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            project_root: project_root.into(),
            binary_dir: binary_dir.into(),
            backup_dir: backup_dir.into(),
            binary_name: "cogneva".to_string(),
            build_timeout_secs: 1800,
            git_name: "Cogneva Self-Evolution".to_string(),
            git_email: "self-evolution@cogneva.ai".to_string(),
            target_dir: None,
        }
    }

    pub fn with_target_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.target_dir = Some(dir.into());
        self
    }

    /// 构建产物目录：显式配置的共享 target，否则退回工作树内的 target。
    fn resolved_target_dir(&self, workdir: &Path) -> PathBuf {
        self.target_dir
            .clone()
            .unwrap_or_else(|| workdir.join("target"))
    }

    pub fn with_binary_name(mut self, name: impl Into<String>) -> Self {
        self.binary_name = name.into();
        self
    }

    pub fn with_build_timeout(mut self, secs: u64) -> Self {
        self.build_timeout_secs = secs;
        self
    }

    pub fn with_git_identity(mut self, name: impl Into<String>, email: impl Into<String>) -> Self {
        self.git_name = name.into();
        self.git_email = email.into();
        self
    }

    /// Commit all current working-tree changes, build a release binary,
    /// and stage it for the supervisor switcher.
    ///
    /// On build failure the git commit is rolled back with `git reset --hard HEAD~1`.
    pub async fn commit_and_build(&self, change_id: &str) -> SFResult<BuildArtifact> {
        self.commit_and_build_in(change_id, &self.project_root)
            .await
    }

    /// 在指定工作树里提交并构建。每轮演进用自己的临时工作树，构建产物仍落
    /// 共享 target 目录。
    pub async fn commit_and_build_in(
        &self,
        change_id: &str,
        workdir: &Path,
    ) -> SFResult<BuildArtifact> {
        info!(change_id = %change_id, "Committing evolution changes");
        self.git_add_all(workdir).await?;
        let commit_hash = self.git_commit(workdir, change_id).await?;

        info!(change_id = %change_id, "Building release binary");
        let start = Instant::now();
        let build_result = self.run_cargo_build(workdir).await;
        let duration = start.elapsed();

        match build_result {
            Ok(()) => {
                info!(change_id = %change_id, "Release binary built successfully");
                let new_binary_path = self.stage_new_binary(workdir).await?;
                Ok(BuildArtifact {
                    change_id: change_id.to_string(),
                    commit_hash,
                    new_binary_path,
                    build_duration_secs: duration.as_secs(),
                })
            }
            Err(e) => {
                warn!(
                    change_id = %change_id,
                    error = %e,
                    "Release build failed; rolling back commit"
                );
                self.git_reset_hard_parent(workdir).await?;
                Err(SFError::Agent(format!(
                    "Build failed and commit rolled back: {}",
                    e
                )))
            }
        }
    }

    async fn git_add_all(&self, workdir: &Path) -> SFResult<()> {
        let output = tokio::process::Command::new("git")
            .args(["add", "-A"])
            .current_dir(workdir)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run git add: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("git add failed: {}", stderr)));
        }
        Ok(())
    }

    async fn git_commit(&self, workdir: &Path, change_id: &str) -> SFResult<String> {
        let message = format!(
            "feat(evolution): apply self-generated change {}\n\nCo-Authored-By: {} <{}>",
            change_id, self.git_name, self.git_email
        );

        let output = tokio::process::Command::new("git")
            .args([
                "-c",
                &format!("user.name={}", self.git_name),
                "-c",
                &format!("user.email={}", self.git_email),
                "commit",
                "-m",
                &message,
            ])
            .current_dir(workdir)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run git commit: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("git commit failed: {}", stderr)));
        }

        // Return the short commit hash.
        let hash_output = tokio::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .current_dir(workdir)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to read commit hash: {}", e)))?;

        Ok(String::from_utf8_lossy(&hash_output.stdout)
            .trim()
            .to_string())
    }

    async fn git_reset_hard_parent(&self, workdir: &Path) -> SFResult<()> {
        let output = tokio::process::Command::new("git")
            .args(["reset", "--hard", "HEAD~1"])
            .current_dir(workdir)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to roll back commit: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("git reset failed: {}", stderr)));
        }
        Ok(())
    }

    async fn run_cargo_build(&self, workdir: &Path) -> SFResult<()> {
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(["build", "--release", "--bin", &self.binary_name])
            .current_dir(workdir)
            .kill_on_drop(true);
        if let Some(target) = &self.target_dir {
            cmd.env("CARGO_TARGET_DIR", target);
        }
        let output = cmd
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run cargo build: {}", e)))?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "cargo build --release failed:\n{}{}",
                stdout, stderr
            )));
        }
        Ok(())
    }

    async fn stage_new_binary(&self, workdir: &Path) -> SFResult<PathBuf> {
        tokio::fs::create_dir_all(&self.binary_dir)
            .await
            .map_err(|e| {
                SFError::IO(format!(
                    "Failed to create binary dir {}: {}",
                    self.binary_dir.display(),
                    e
                ))
            })?;

        tokio::fs::create_dir_all(&self.backup_dir)
            .await
            .map_err(|e| {
                SFError::IO(format!(
                    "Failed to create backup dir {}: {}",
                    self.backup_dir.display(),
                    e
                ))
            })?;

        let source = self
            .resolved_target_dir(workdir)
            .join("release")
            .join(&self.binary_name);
        let staged = self.binary_dir.join(format!("{}.new", self.binary_name));

        tokio::fs::copy(&source, &staged).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to copy {} to {}: {}",
                source.display(),
                staged.display(),
                e
            ))
        })?;

        info!(
            source = %source.display(),
            staged = %staged.display(),
            "Staged new binary"
        );
        Ok(staged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_artifact_fields() {
        let artifact = BuildArtifact {
            change_id: "change-123".into(),
            commit_hash: "abc123".into(),
            new_binary_path: PathBuf::from("/tmp/cogneva.new"),
            build_duration_secs: 42,
        };
        assert_eq!(artifact.change_id, "change-123");
        assert_eq!(artifact.commit_hash, "abc123");
    }
}
