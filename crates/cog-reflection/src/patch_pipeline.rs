//! Patch application pipeline for L2 self-evolution.
//!
//! Responsibilities:
//! - Scan `patch_dir` for `.patch` files (unified diff format).
//! - Validate every affected path: must exist, must live inside the workspace,
//!   and must not point to build/config/deployment files.
//! - Apply patches with `git apply`, run `cargo test --workspace`,
//!   and roll back on failure.
//! - Report results by updating the evolution status.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use cog_core::{SFError, SFResult};
use tracing::{info, warn};

use crate::types::{EvolutionKind, EvolutionResult, EvolutionStatus};
use crate::EvolutionEngine;

/// Result of applying and testing a single patch.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub patch_id: String,
    pub files_changed: Vec<PathBuf>,
    pub test_passed: bool,
    pub test_output: String,
    pub new_status: EvolutionStatus,
}

/// Pipeline that turns validated code patches into tested source changes.
#[derive(Debug, Clone)]
pub struct PatchPipeline {
    project_root: PathBuf,
    patch_dir: PathBuf,
    auto_apply: bool,
    test_timeout_secs: u64,
}

impl PatchPipeline {
    pub fn new(
        project_root: impl Into<PathBuf>,
        patch_dir: impl Into<PathBuf>,
        auto_apply: bool,
    ) -> Self {
        Self {
            project_root: project_root.into(),
            patch_dir: patch_dir.into(),
            auto_apply,
            test_timeout_secs: 600,
        }
    }

    pub fn with_test_timeout(mut self, secs: u64) -> Self {
        self.test_timeout_secs = secs;
        self
    }

    pub fn with_auto_apply(mut self, auto_apply: bool) -> Self {
        self.auto_apply = auto_apply;
        self
    }

    /// List code patches that are ready to be applied to the working tree.
    /// Scans `patch_dir` for `.patch` files and treats each one as a unified diff.
    /// This survives process restarts better than the in-memory
    /// `EvolutionEngine` results map.
    pub async fn pending_patches(
        &self,
        engine: Option<&EvolutionEngine>,
    ) -> SFResult<Vec<EvolutionResult>> {
        let mut results = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.patch_dir).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to read patch dir {}: {}",
                self.patch_dir.display(),
                e
            ))
        })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(format!("Failed to read patch dir entry: {}", e)))?
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("patch") {
                continue;
            }

            let content = tokio::fs::read_to_string(&path).await.map_err(|e| {
                SFError::IO(format!("Failed to read patch {}: {}", path.display(), e))
            })?;

            let artifact_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();

            // If the engine has a more accurate status, prefer it.
            let status = if let Some(engine) = engine {
                engine
                    .list_results()
                    .await
                    .into_iter()
                    .find(|r| r.artifact_id == artifact_id)
                    .map(|r| r.status)
                    .unwrap_or(EvolutionStatus::CompileChecked)
            } else {
                EvolutionStatus::CompileChecked
            };

            if !matches!(
                status,
                EvolutionStatus::CompileChecked | EvolutionStatus::AwaitingReview
            ) {
                continue;
            }

            results.push(EvolutionResult {
                kind: EvolutionKind::CodePatch,
                artifact_id,
                description: format!("Code patch from {}", path.display()),
                content,
                status,
                created_at: chrono::Utc::now(),
                eval_summary: None,
            });
        }

        results.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(results)
    }

    /// Apply a single patch to the working tree and run the workspace test suite.
    ///
    /// On success:
    /// - if `auto_apply` is true, the working tree is left dirty and ready for
    ///   `git commit` by the deployer;
    /// - if `auto_apply` is false, the working tree is rolled back to a clean
    ///   state and the patch stays `AwaitingReview` for manual approval.
    ///
    /// On test failure the working tree is always rolled back.
    pub async fn apply_and_test(&self, patch: &EvolutionResult) -> SFResult<ApplyResult> {
        info!(patch_id = %patch.artifact_id, "Applying evolution patch");

        let files_changed = Self::parse_patch(&patch.content)?;
        Self::validate_patch_files(&files_changed, &self.project_root)?;
        self.ensure_clean_workspace().await?;

        if let Err(e) = self.git_apply_check(&patch.content).await {
            return Ok(ApplyResult {
                patch_id: patch.artifact_id.clone(),
                files_changed,
                test_passed: false,
                test_output: format!("Patch pre-check failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        if let Err(e) = self.git_apply(&patch.content).await {
            return Ok(ApplyResult {
                patch_id: patch.artifact_id.clone(),
                files_changed,
                test_passed: false,
                test_output: format!("Patch application failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        let (test_passed, test_output) = match self.run_cargo_test().await {
            Ok(result) => result,
            Err(e) => {
                warn!(patch_id = %patch.artifact_id, error = %e, "cargo test execution failed");
                let _ = self.git_reset_hard().await;
                return Ok(ApplyResult {
                    patch_id: patch.artifact_id.clone(),
                    files_changed,
                    test_passed: false,
                    test_output: format!("Failed to execute cargo test: {}", e),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
        };

        let new_status = if test_passed {
            if self.auto_apply {
                info!(patch_id = %patch.artifact_id, "Patch applied and tests passed; waiting for commit");
                EvolutionStatus::Active
            } else {
                info!(patch_id = %patch.artifact_id, "Patch tests passed; rolling back for manual review");
                let _ = self.git_reset_hard().await;
                EvolutionStatus::AwaitingReview
            }
        } else {
            warn!(patch_id = %patch.artifact_id, "Patch tests failed; rolling back");
            let _ = self.git_reset_hard().await;
            EvolutionStatus::ValidationFailed
        };

        Ok(ApplyResult {
            patch_id: patch.artifact_id.clone(),
            files_changed,
            test_passed,
            test_output,
            new_status,
        })
    }

    /// Parse a unified diff patch and return the list of files it touches.
    ///
    /// Extracts paths from `+++ b/<path>` lines. New files appear as
    /// `+++ b/<path>` with `--- /dev/null`, so this also handles additions.
    pub fn parse_patch(content: &str) -> SFResult<Vec<PathBuf>> {
        let files = cog_core::parse_patch_affected_files(content)?;
        Ok(files.into_iter().map(PathBuf::from).collect())
    }

    /// Validate that every affected path is safe to modify.
    /// - Must resolve to a real file inside the project root.
    /// - Must not escape the project root.
    /// - Must not be a build/config/deployment/secret file.
    pub fn validate_patch_files(files: &[PathBuf], project_root: &Path) -> SFResult<()> {
        let canonical_root = project_root.canonicalize().map_err(|e| {
            SFError::IO(format!(
                "Failed to canonicalize project root {}: {}",
                project_root.display(),
                e
            ))
        })?;

        let forbidden_names: HashSet<&str> = [
            "Cargo.toml",
            "Cargo.lock",
            "cogneva.json",
            ".env",
            ".envrc",
            "Dockerfile",
            "Containerfile",
            "docker-compose.yml",
            "setup.sh",
        ]
        .iter()
        .cloned()
        .collect();

        let forbidden_extensions: HashSet<&str> =
            ["pem", "key", "crt", "p12"].iter().cloned().collect();

        for file in files {
            let absolute = canonical_root.join(file);
            let canonical = absolute.canonicalize().map_err(|e| {
                SFError::Validation(format!(
                    "Target path does not exist or is not accessible: {} ({})",
                    file.display(),
                    e
                ))
            })?;

            if !canonical.starts_with(&canonical_root) {
                return Err(SFError::Validation(format!(
                    "Target path escapes project root: {}",
                    file.display()
                )));
            }

            if !canonical.is_file() {
                return Err(SFError::Validation(format!(
                    "Target path is not a file: {}",
                    file.display()
                )));
            }

            if let Some(name) = canonical.file_name().and_then(|n| n.to_str()) {
                if forbidden_names.contains(name) {
                    return Err(SFError::Validation(format!(
                        "Modifying protected file {} is not allowed",
                        name
                    )));
                }
            }

            if let Some(ext) = canonical.extension().and_then(|e| e.to_str()) {
                if forbidden_extensions.contains(ext) {
                    return Err(SFError::Validation(format!(
                        "Modifying .{} files is not allowed",
                        ext
                    )));
                }
            }

            if !canonical
                .to_string_lossy()
                .replace('\\', "/")
                .contains("/src/")
            {
                warn!(
                    target = %file.display(),
                    "Patch target is outside a src directory; allowed but unusual"
                );
            }
        }

        Ok(())
    }

    /// Refuse to apply if the git working tree already has uncommitted changes.
    async fn ensure_clean_workspace(&self) -> SFResult<()> {
        let output = tokio::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&self.project_root)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to check git status: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.trim().is_empty() {
            return Err(SFError::Validation(format!(
                "Git workspace is not clean; refusing to apply patch:\n{}",
                stdout
            )));
        }
        Ok(())
    }

    /// Run `git apply --check` on patch content without modifying the tree.
    async fn git_apply_check(&self, patch_content: &str) -> SFResult<()> {
        self.run_git_apply(patch_content, true).await
    }

    /// Apply patch content to the working tree with `git apply`.
    async fn git_apply(&self, patch_content: &str) -> SFResult<()> {
        self.run_git_apply(patch_content, false).await
    }

    /// Shared implementation for `git apply [--check]`.
    async fn run_git_apply(&self, patch_content: &str, check_only: bool) -> SFResult<()> {
        let mut cmd = tokio::process::Command::new("git");
        cmd.arg("apply").arg("-v").current_dir(&self.project_root);
        if check_only {
            cmd.arg("--check");
        }

        let mut child = cmd
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| SFError::IO(format!("Failed to spawn git apply: {}", e)))?;

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(patch_content.as_bytes())
                .await
                .map_err(|e| {
                    SFError::IO(format!("Failed to write patch to git apply stdin: {}", e))
                })?;
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run git apply: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "git apply {}failed: {}",
                if check_only { "--check " } else { "" },
                stderr
            )));
        }

        Ok(())
    }

    /// Restore the working tree to HEAD.
    async fn git_reset_hard(&self) -> SFResult<()> {
        let output = tokio::process::Command::new("git")
            .args(["reset", "--hard", "HEAD"])
            .current_dir(&self.project_root)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run git reset: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!("git reset failed: {}", stderr)));
        }
        Ok(())
    }

    /// Run `cargo test --workspace` and return (success, combined_output).
    async fn run_cargo_test(&self) -> SFResult<(bool, String)> {
        info!("Running cargo test --workspace");
        let output = tokio::process::Command::new("cargo")
            .args(["test", "--workspace"])
            .current_dir(&self.project_root)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run cargo test: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{}{}", stdout, stderr);
        Ok((output.status.success(), combined))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_patch_extracts_files_from_unified_diff() {
        let patch = r#"diff --git a/crates/foo/src/bar.rs b/crates/foo/src/bar.rs
index 1234567..abcdefg 100644
--- a/crates/foo/src/bar.rs
+++ b/crates/foo/src/bar.rs
@@ -1,3 +1,4 @@
 fn old() {}
+fn new() {}
"#;
        let files = PatchPipeline::parse_patch(patch).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], PathBuf::from("crates/foo/src/bar.rs"));
    }

    #[test]
    fn parse_patch_handles_new_file() {
        let patch = r#"diff --git a/crates/foo/src/new.rs b/crates/foo/src/new.rs
new file mode 100644
index 0000000..1234567
--- /dev/null
+++ b/crates/foo/src/new.rs
@@ -0,0 +1 @@
+fn new() {}
"#;
        let files = PatchPipeline::parse_patch(patch).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], PathBuf::from("crates/foo/src/new.rs"));
    }

    #[tokio::test]
    async fn git_apply_and_check_apply_patch_to_working_tree() {
        use tokio::process::Command;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();

        // Init git repo and commit a file.
        Command::new("git")
            .args(["init"])
            .current_dir(root)
            .output()
            .await
            .expect("git init failed");
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(root)
            .output()
            .await
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(root)
            .output()
            .await
            .unwrap();

        let src_path = root.join("src").join("lib.rs");
        tokio::fs::create_dir_all(src_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&src_path, "fn old() {}\n").await.unwrap();

        Command::new("git")
            .args(["add", "."])
            .current_dir(root)
            .output()
            .await
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(root)
            .output()
            .await
            .unwrap();

        let patch = r#"diff --git a/src/lib.rs b/src/lib.rs
index 1111111..2222222 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -1 +1,2 @@
 fn old() {}
+fn new() {}
"#;

        let pipeline = PatchPipeline::new(root, root.join("patches"), true);
        pipeline.git_apply_check(patch).await.unwrap();
        pipeline.git_apply(patch).await.unwrap();

        let content = tokio::fs::read_to_string(&src_path).await.unwrap();
        assert!(content.contains("fn new()"));
    }

    #[test]
    fn parse_patch_rejects_empty_patch() {
        let patch = "This is not a unified diff\nJust some text\n";
        assert!(PatchPipeline::parse_patch(patch).is_err());
    }

    #[test]
    fn validate_patch_files_rejects_escape() {
        let root = PathBuf::from("/tmp/should-not-exist-for-test");
        let files = vec![PathBuf::from("../etc/passwd")];
        assert!(PatchPipeline::validate_patch_files(&files, &root).is_err());
    }
}
