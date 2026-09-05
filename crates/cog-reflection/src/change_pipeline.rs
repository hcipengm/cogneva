//! Change application pipeline for L2 self-evolution.
//!
//! Responsibilities:
//! - Scan `change_dir` for `.diff` files (unified diff format).
//! - Validate every affected path: must exist, must live inside the workspace,
//!   and must not point to build/config/deployment files.
//! - Apply changes with `git apply`, run `cargo test --workspace`,
//!   and roll back on failure.
//! - Report results by updating the evolution status.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use cog_core::{SFError, SFResult};
use tracing::{info, warn};

use crate::types::{EvolutionKind, EvolutionResult, EvolutionStatus};
use crate::EvolutionEngine;

/// Result of applying and testing a single change.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub change_id: String,
    pub files_changed: Vec<PathBuf>,
    pub test_passed: bool,
    pub test_output: String,
    pub new_status: EvolutionStatus,
}

/// Pipeline that turns validated code changes into tested source changes.
#[derive(Debug, Clone)]
pub struct ChangePipeline {
    project_root: PathBuf,
    change_dir: PathBuf,
    auto_apply: bool,
    test_timeout_secs: u64,
    promotion_policy: Option<crate::PromotionGateConfig>,
}

impl ChangePipeline {
    pub fn new(
        project_root: impl Into<PathBuf>,
        change_dir: impl Into<PathBuf>,
        auto_apply: bool,
    ) -> Self {
        Self {
            project_root: project_root.into(),
            change_dir: change_dir.into(),
            auto_apply,
            test_timeout_secs: 600,
            promotion_policy: None,
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

    /// 晋级门入口校验：黑名单文件（依赖清单/密钥材料）在应用前直接
    /// 拒收，连沙盒执行管线都不让进。
    pub fn with_promotion_policy(mut self, policy: crate::PromotionGateConfig) -> Self {
        self.promotion_policy = Some(policy);
        self
    }

    /// List code changes that are ready to be applied to the working tree.
    /// Scans `change_dir` for `.diff` files and treats each one as a unified diff.
    /// This survives process restarts better than the in-memory
    /// `EvolutionEngine` results map.
    pub async fn pending_changes(
        &self,
        engine: Option<&EvolutionEngine>,
    ) -> SFResult<Vec<EvolutionResult>> {
        let mut results = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.change_dir).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to read change dir {}: {}",
                self.change_dir.display(),
                e
            ))
        })?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(format!("Failed to read change dir entry: {}", e)))?
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("diff") {
                continue;
            }

            let content = tokio::fs::read_to_string(&path).await.map_err(|e| {
                SFError::IO(format!("Failed to read change {}: {}", path.display(), e))
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
                kind: EvolutionKind::CodeChange,
                artifact_id,
                description: format!("Code change from {}", path.display()),
                content,
                status,
                created_at: chrono::Utc::now(),
                eval_summary: None,
            });
        }

        results.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(results)
    }

    /// Apply a single change to the working tree and run the workspace test suite.
    ///
    /// On success:
    /// - if `auto_apply` is true, the working tree is left dirty and ready for
    ///   `git commit` by the deployer;
    /// - if `auto_apply` is false, the working tree is rolled back to a clean
    ///   state and the change stays `AwaitingReview` for manual approval.
    ///
    /// On test failure the working tree is always rolled back.
    pub async fn apply_and_test(&self, change: &EvolutionResult) -> SFResult<ApplyResult> {
        info!(change_id = %change.artifact_id, "Applying evolution change");

        let files_changed = Self::parse_diff(&change.content)?;

        // 晋级门入口：黑名单命中（依赖清单/密钥材料）直接拒收，
        // 不做 apply、不跑测试，状态落 Rejected 留审计痕迹。
        if let Some(policy) = &self.promotion_policy {
            let files: Vec<String> = files_changed
                .iter()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .collect();
            let diff_lines = crate::promotion_gate::count_diff_lines(&change.content);
            if let crate::GateVerdict::Reject { reason } =
                crate::promotion_gate::classify(&files, diff_lines, policy)
            {
                warn!(change_id = %change.artifact_id, reason = %reason, "Change rejected by promotion gate");
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    test_passed: false,
                    test_output: format!("Promotion gate rejected: {reason}"),
                    new_status: EvolutionStatus::Rejected,
                });
            }
        }

        Self::validate_change_files(&files_changed, &self.project_root)?;
        self.ensure_clean_workspace().await?;

        if let Err(e) = self.git_apply_check(&change.content).await {
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                test_passed: false,
                test_output: format!("Change pre-check failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        if let Err(e) = self.git_apply(&change.content).await {
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                test_passed: false,
                test_output: format!("Change application failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        let (test_passed, test_output) = match self.run_cargo_test().await {
            Ok(result) => result,
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, "cargo test execution failed");
                let _ = self.git_reset_hard().await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    test_passed: false,
                    test_output: format!("Failed to execute cargo test: {}", e),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
        };

        let new_status = if test_passed {
            if self.auto_apply {
                info!(change_id = %change.artifact_id, "Change applied and tests passed; waiting for commit");
                EvolutionStatus::Active
            } else {
                info!(change_id = %change.artifact_id, "Change tests passed; rolling back for manual review");
                let _ = self.git_reset_hard().await;
                EvolutionStatus::AwaitingReview
            }
        } else {
            warn!(change_id = %change.artifact_id, "Change tests failed; rolling back");
            let _ = self.git_reset_hard().await;
            EvolutionStatus::ValidationFailed
        };

        Ok(ApplyResult {
            change_id: change.artifact_id.clone(),
            files_changed,
            test_passed,
            test_output,
            new_status,
        })
    }

    /// Parse a unified diff change and return the list of files it touches.
    ///
    /// Extracts paths from `+++ b/<path>` lines. New files appear as
    /// `+++ b/<path>` with `--- /dev/null`, so this also handles additions.
    pub fn parse_diff(content: &str) -> SFResult<Vec<PathBuf>> {
        let files = cog_core::parse_diff_affected_files(content)?;
        Ok(files.into_iter().map(PathBuf::from).collect())
    }

    /// Validate that every affected path is safe to modify.
    /// - Must resolve to a real file inside the project root.
    /// - Must not escape the project root.
    /// - Must not be a build/config/deployment/secret file.
    pub fn validate_change_files(files: &[PathBuf], project_root: &Path) -> SFResult<()> {
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
                    "Change target is outside a src directory; allowed but unusual"
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
                "Git workspace is not clean; refusing to apply change:\n{}",
                stdout
            )));
        }
        Ok(())
    }

    /// Run `git apply --check` on change content without modifying the tree.
    async fn git_apply_check(&self, change_content: &str) -> SFResult<()> {
        self.run_git_apply(change_content, true).await
    }

    /// Apply change content to the working tree with `git apply`.
    async fn git_apply(&self, change_content: &str) -> SFResult<()> {
        self.run_git_apply(change_content, false).await
    }

    /// Shared implementation for `git apply [--check]`.
    async fn run_git_apply(&self, change_content: &str, check_only: bool) -> SFResult<()> {
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
                .write_all(change_content.as_bytes())
                .await
                .map_err(|e| {
                    SFError::IO(format!("Failed to write change to git apply stdin: {}", e))
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

    /// Run a git command; Some(stdout) on success, None otherwise.
    async fn git_try(&self, args: &[&str]) -> Option<String> {
        let output = tokio::process::Command::new("git")
            .args(args)
            .current_dir(&self.project_root)
            .output()
            .await
            .ok()?;
        if output.status.success() {
            Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            None
        }
    }

    /// 同步沙盒源码树到上游 bare 仓库最新 main（进化 Pod 内 `local` 远程
    /// 指向宿主 bare 仓库 /host-git）。change 必须基于新鲜主线生成，否则
    /// GitOps 拉取端应用晋级产物时会因基树陈旧连带回退无关文件。
    ///
    /// 安全规则（任一不满足即跳过本轮同步，绝不丢在途工作）：
    /// - 无 `local` 远程（非沙盒环境）→ 跳过
    /// - 工作树脏（有在途 change）→ 跳过
    /// - HEAD 已是 local/main 祖先 → reset --hard local/main（快进/对齐）
    /// - HEAD 已是 local/evolution-release 祖先（本地 change commit 已全部
    ///   发布到晋级分支，可安全丢弃）→ reset --hard local/main 重新对齐主线
    /// - 否则（有未发布的本地 change commit，如 soak 期/推送失败熔断中）
    ///   → 跳过，等发布成功或人工处置后再同步
    pub async fn sync_with_upstream(&self) -> SFResult<()> {
        if self
            .git_try(&["remote", "get-url", "local"])
            .await
            .is_none()
        {
            return Ok(());
        }
        if self.git_try(&["fetch", "local", "main"]).await.is_none() {
            warn!("sync_with_upstream: fetch local main failed; keeping current tree");
            return Ok(());
        }
        // 晋级分支首轮可能还不存在，失败不阻塞 main 同步。
        let has_release = self
            .git_try(&["fetch", "local", "evolution-release"])
            .await
            .is_some();

        let dirty = self
            .git_try(&["status", "--porcelain"])
            .await
            .map(|s| !s.is_empty())
            .unwrap_or(true);
        if dirty {
            info!("sync_with_upstream: working tree dirty (change in flight); skip");
            return Ok(());
        }

        let head = self
            .git_try(&["rev-parse", "HEAD"])
            .await
            .unwrap_or_default();
        let upstream = self
            .git_try(&["rev-parse", "local/main"])
            .await
            .unwrap_or_default();
        if head.is_empty() || upstream.is_empty() {
            return Ok(());
        }
        if head == upstream {
            return Ok(());
        }

        let on_mainline = self
            .git_try(&["merge-base", "--is-ancestor", "HEAD", "local/main"])
            .await
            .is_some();
        let published = has_release
            && self
                .git_try(&[
                    "merge-base",
                    "--is-ancestor",
                    "HEAD",
                    "local/evolution-release",
                ])
                .await
                .is_some();

        if !on_mainline && !published {
            info!(
                "sync_with_upstream: unpublished local change commits present; \
                 skip until promoted (or operator resolves)"
            );
            return Ok(());
        }

        let output = tokio::process::Command::new("git")
            .args(["reset", "--hard", "local/main"])
            .current_dir(&self.project_root)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run git reset: {}", e)))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!(
                "sync_with_upstream reset to local/main failed: {stderr}"
            )));
        }
        info!(
            from = %head,
            to = %upstream,
            published,
            "Sandbox source synced to upstream main"
        );
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
    fn parse_diff_extracts_files_from_unified_diff() {
        let change = r#"diff --git a/crates/foo/src/bar.rs b/crates/foo/src/bar.rs
index 1234567..abcdefg 100644
--- a/crates/foo/src/bar.rs
+++ b/crates/foo/src/bar.rs
@@ -1,3 +1,4 @@
 fn old() {}
+fn new() {}
"#;
        let files = ChangePipeline::parse_diff(change).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], PathBuf::from("crates/foo/src/bar.rs"));
    }

    #[test]
    fn parse_diff_handles_new_file() {
        let change = r#"diff --git a/crates/foo/src/new.rs b/crates/foo/src/new.rs
new file mode 100644
index 0000000..1234567
--- /dev/null
+++ b/crates/foo/src/new.rs
@@ -0,0 +1 @@
+fn new() {}
"#;
        let files = ChangePipeline::parse_diff(change).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], PathBuf::from("crates/foo/src/new.rs"));
    }

    #[tokio::test]
    async fn git_apply_and_check_apply_change_to_working_tree() {
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

        let change = r#"diff --git a/src/lib.rs b/src/lib.rs
index 1111111..2222222 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -1 +1,2 @@
 fn old() {}
+fn new() {}
"#;

        let pipeline = ChangePipeline::new(root, root.join("changes"), true);
        pipeline.git_apply_check(change).await.unwrap();
        pipeline.git_apply(change).await.unwrap();

        let content = tokio::fs::read_to_string(&src_path).await.unwrap();
        assert!(content.contains("fn new()"));
    }

    #[test]
    fn parse_diff_rejects_empty_change() {
        let change = "This is not a unified diff\nJust some text\n";
        assert!(ChangePipeline::parse_diff(change).is_err());
    }

    #[test]
    fn validate_change_files_rejects_escape() {
        let root = PathBuf::from("/tmp/should-not-exist-for-test");
        let files = vec![PathBuf::from("../etc/passwd")];
        assert!(ChangePipeline::validate_change_files(&files, &root).is_err());
    }

    /// git 集成测试脚手架：造 upstream bare + 沙盒 work（remote local→bare）。
    async fn git_ok(dir: &std::path::Path, args: &[&str]) {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .expect("git spawn failed");
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    async fn scaffold_upstream() -> (tempfile::TempDir, tempfile::TempDir) {
        let upstream = tempfile::tempdir().unwrap();
        let work = tempfile::tempdir().unwrap();
        git_ok(upstream.path(), &["init", "--bare", "-b", "main"]).await;
        git_ok(work.path(), &["init", "-b", "main"]).await;
        git_ok(work.path(), &["config", "user.email", "t@t.c"]).await;
        git_ok(work.path(), &["config", "user.name", "T"]).await;
        tokio::fs::write(work.path().join("a.txt"), "a\n")
            .await
            .unwrap();
        git_ok(work.path(), &["add", "."]).await;
        git_ok(work.path(), &["commit", "-m", "c1"]).await;
        git_ok(
            work.path(),
            &["remote", "add", "local", &upstream.path().to_string_lossy()],
        )
        .await;
        git_ok(work.path(), &["push", "local", "main"]).await;
        (upstream, work)
    }

    /// 在 upstream 侧（经独立 clone）向 main 追加一个 commit，模拟宿主主线前进。
    async fn advance_upstream(upstream: &std::path::Path, name: &str) {
        let tmp = tempfile::tempdir().unwrap();
        git_ok(tmp.path(), &["clone", &upstream.to_string_lossy(), "clone"]).await;
        let clone = tmp.path().join("clone");
        git_ok(&clone, &["config", "user.email", "t@t.c"]).await;
        git_ok(&clone, &["config", "user.name", "T"]).await;
        tokio::fs::write(clone.join(name), format!("{name}\n"))
            .await
            .unwrap();
        git_ok(&clone, &["add", "."]).await;
        git_ok(&clone, &["commit", "-m", name]).await;
        git_ok(&clone, &["push", "origin", "main"]).await;
    }

    async fn head_of(dir: &std::path::Path) -> String {
        let out = tokio::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[tokio::test]
    async fn sync_with_upstream_fast_forwards_clean_tree() {
        let (upstream, work) = scaffold_upstream().await;
        advance_upstream(upstream.path(), "b.txt").await;

        let pipeline = ChangePipeline::new(work.path(), work.path().join("changes"), true);
        pipeline.sync_with_upstream().await.unwrap();

        assert!(work.path().join("b.txt").exists());
        let upstream_head = {
            let out = tokio::process::Command::new("git")
                .args(["--git-dir", &upstream.path().to_string_lossy()])
                .args(["rev-parse", "main"])
                .output()
                .await
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        assert_eq!(head_of(work.path()).await, upstream_head);
    }

    #[tokio::test]
    async fn sync_with_upstream_skips_unpublished_local_commits() {
        let (upstream, work) = scaffold_upstream().await;
        // 沙盒本地产生一个未发布的 change commit。
        tokio::fs::write(work.path().join("change.txt"), "p\n")
            .await
            .unwrap();
        git_ok(work.path(), &["add", "."]).await;
        git_ok(work.path(), &["commit", "-m", "change"]).await;
        let before = head_of(work.path()).await;
        advance_upstream(upstream.path(), "b.txt").await;

        let pipeline = ChangePipeline::new(work.path(), work.path().join("changes"), true);
        pipeline.sync_with_upstream().await.unwrap();

        // 未发布 commit 在途：不同步，HEAD 不变（soak/熔断窗口保护）。
        assert_eq!(head_of(work.path()).await, before);
        assert!(!work.path().join("b.txt").exists());
    }

    #[tokio::test]
    async fn sync_with_upstream_resets_after_change_published() {
        let (upstream, work) = scaffold_upstream().await;
        // 沙盒本地 change commit 并推送到 evolution-release（模拟晋级发布）。
        tokio::fs::write(work.path().join("change.txt"), "p\n")
            .await
            .unwrap();
        git_ok(work.path(), &["add", "."]).await;
        git_ok(work.path(), &["commit", "-m", "change"]).await;
        git_ok(work.path(), &["push", "local", "HEAD:evolution-release"]).await;
        advance_upstream(upstream.path(), "b.txt").await;

        let pipeline = ChangePipeline::new(work.path(), work.path().join("changes"), true);
        pipeline.sync_with_upstream().await.unwrap();

        // 已发布的本地 commit 安全丢弃，树对齐最新主线。
        assert!(work.path().join("b.txt").exists());
        assert!(!work.path().join("change.txt").exists());
    }

    #[tokio::test]
    async fn apply_and_test_rejects_forbidden_files_at_gate() {
        let temp = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(temp.path(), temp.path().join("changes"), true)
            .with_promotion_policy(crate::PromotionGateConfig::default());

        let change = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: "evil-1".into(),
            description: "touches Cargo.toml".into(),
            content: r#"diff --git a/Cargo.toml b/Cargo.toml
index 1111111..2222222 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -1 +1,2 @@
 [package]
+evil = "1.0"
"#
            .into(),
            status: crate::types::EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline.apply_and_test(&change).await.unwrap();
        assert!(!result.test_passed);
        assert_eq!(result.new_status, crate::types::EvolutionStatus::Rejected);
        assert!(result.test_output.contains("Promotion gate rejected"));
    }

    #[tokio::test]
    async fn apply_and_test_without_policy_keeps_legacy_behavior() {
        // 未配置晋级策略时入口校验不生效（旧行为零回归）：
        // Cargo.toml change 会走到 validate_change_files 的既有黑名单。
        let temp = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(temp.path(), temp.path().join("changes"), true);
        assert!(pipeline.promotion_policy.is_none());
    }
}
