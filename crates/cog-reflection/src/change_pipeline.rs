//! Change application pipeline for L2 self-evolution.
//!
//! Responsibilities:
//! - Scan `change_dir` for `.diff` files (unified diff format).
//! - Validate every affected path: must exist, must live inside the workspace,
//!   and must not point to build/config/deployment files.
//! - Apply changes with `git apply`, run `cargo test --workspace`,
//!   and roll back on failure.
//! - Report results by updating the evolution status.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cog_core::{SFError, SFResult};
use tracing::{info, warn};

use crate::types::{EvolutionKind, EvolutionResult, EvolutionStatus};
use crate::EvolutionEngine;

/// Environment the verification test process is allowed to inherit.
///
/// The verdict has to be a function of the change alone. An inherited
/// environment lets the deployment's own configuration reach the assertions
/// (its endpoints, claim names, feature flags), so the same change is accepted
/// on one deployment and rejected on another. Everything listed here is what
/// it takes to launch the toolchain and find its caches; nothing that
/// describes a deployment.
///
/// A compiler *wrapper* is deliberately not on the list even though it belongs
/// to neither category. A wrapper is a substitution of the compiler, so what
/// the verification builds would be whatever the parent process's build
/// configuration says it is — and a wrapper that only serves the build that
/// set it does not merely change the answer, it fails outright: a missing
/// compiler makes every verification fail, so every change is rejected for a
/// reason that has nothing to do with the change. The pipeline cannot tell a
/// transparent cache from a substitution, and this is not a place to guess.
/// Compile speed for a verification comes from the shared target directory
/// instead, which the pipeline sets itself.
const VERIFICATION_ENV_PASSTHROUGH: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LANGUAGE",
    "TZ",
    "TERM",
    "TMPDIR",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "RUSTFLAGS",
    "SCCACHE_DIR",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
];

/// Default wall-clock bound on the format check. Formatted-or-not is settled by
/// parsing every file in the workspace, which takes seconds; a run that
/// outlives this bound is a hung `rustfmt`, not a large workspace.
const DEFAULT_FMT_TIMEOUT_SECS: u64 = 60;

/// Resolve the environment for the verification test process: the passthrough
/// set as reported by `get`, plus an explicit target directory.
///
/// Split out from the command so the rule can be checked without spawning
/// anything: a variable that describes the deployment must never reach an
/// assertion.
fn verification_env(
    get: impl Fn(&str) -> Option<String>,
    target_dir: Option<&Path>,
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = VERIFICATION_ENV_PASSTHROUGH
        .iter()
        .filter_map(|key| get(key).map(|value| (key.to_string(), value)))
        .collect();
    if let Some(dir) = target_dir {
        env.push(("CARGO_TARGET_DIR".to_string(), dir.display().to_string()));
    }
    env
}

/// What the pipeline decided about one change.
///
/// The cause travels with the refusal instead of sitting beside it as a second
/// field, so a refused change without a reason cannot be built: every caller
/// that dispatches on the verdict has the criterion in hand, and no caller has
/// to re-derive it by matching on prose. The prose stays in
/// [`ApplyResult::test_output`] for whoever reads the record; it is not the
/// protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeVerdict {
    /// The change is applied, or held for review, and nothing refused it.
    Passed,
    /// A deterministic criterion refused it. The change is not going to start
    /// fitting the tree by being tried again, so the caller retires it — unless
    /// the cause is one that names the run rather than the artifact, which is
    /// the caller's call to make.
    Refused(cog_core::RejectionCause),
}

impl ChangeVerdict {
    pub fn passed(&self) -> bool {
        matches!(self, Self::Passed)
    }

    /// The criterion that refused the change, if one did.
    pub fn cause(&self) -> Option<cog_core::RejectionCause> {
        match self {
            Self::Passed => None,
            Self::Refused(cause) => Some(*cause),
        }
    }
}

/// Result of applying and testing a single change.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub change_id: String,
    pub files_changed: Vec<PathBuf>,
    pub verdict: ChangeVerdict,
    pub test_output: String,
    pub new_status: EvolutionStatus,
}

/// Pipeline that turns validated code changes into tested source changes.
#[derive(Debug, Clone)]
pub struct ChangePipeline {
    project_root: PathBuf,
    change_dir: PathBuf,
    auto_apply: bool,
    /// Wall-clock budget for the verification test run. A cold verification
    /// compiles the whole workspace on the deployment's own hardware before it
    /// runs a single test, so this bounds a build-plus-test, not just a test;
    /// a budget shorter than one real run turns every verdict into a timeout.
    test_timeout_secs: u64,
    /// Wall-clock bound on the format check. Short on purpose: the check does
    /// not compile anything, so a run this side of the bound is a hung process
    /// rather than a slow one — and a hung process would hold the executor's
    /// only in-flight slot, which is the state this bound exists to end.
    fmt_timeout_secs: u64,
    promotion_policy: Option<crate::PromotionGateConfig>,
    /// 共享 CARGO_TARGET_DIR：把编译产物留在工作树之外，临时工作树用完即弃
    /// 也不会丢增量缓存。
    target_dir: Option<PathBuf>,
    /// Where the runs this pipeline bounds report what they did against the
    /// budget. Absent for a pipeline nothing observes — the tests and the admin
    /// service build their own — and an absent sink reports nothing rather than
    /// reporting a budget nobody enforced.
    budget: Option<Arc<crate::verification_budget::VerificationBudget>>,
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
            test_timeout_secs: 3600,
            fmt_timeout_secs: DEFAULT_FMT_TIMEOUT_SECS,
            promotion_policy: None,
            target_dir: None,
            budget: None,
        }
    }

    /// Attach the observation sink and take the budget from it.
    ///
    /// The budget is read out of the sink rather than passed separately so that
    /// the number reported and the number enforced cannot be two numbers: a
    /// deployment whose reading said 3600 while its runs were being killed at
    /// 1800 would be an observation surface that disagrees with the decision it
    /// is supposed to explain.
    pub fn with_verification_budget(
        mut self,
        budget: Arc<crate::verification_budget::VerificationBudget>,
    ) -> Self {
        self.test_timeout_secs = budget.timeout_secs(crate::verification_budget::KIND_TEST);
        self.budget = Some(budget);
        self
    }

    pub fn with_target_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.target_dir = Some(dir.into());
        self
    }

    /// 构造时给定的默认工作目录。未接分配器的调用方据此退回进程工作目录，
    /// 接了分配器的调用方一律改用分配出来的工作树。
    pub fn project_root(&self) -> &std::path::Path {
        &self.project_root
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

            // The engine knows this change's own status and the goal it was
            // generated for; the directory alone knows neither.
            let record = match engine {
                Some(engine) => engine
                    .list_results()
                    .await
                    .into_iter()
                    .find(|r| r.artifact_id == artifact_id),
                None => None,
            };
            let status = record
                .as_ref()
                .map(|r| r.status)
                .unwrap_or(EvolutionStatus::CompileChecked);

            if !matches!(
                status,
                EvolutionStatus::CompileChecked | EvolutionStatus::AwaitingReview
            ) {
                continue;
            }

            // The description becomes the landed commit's subject line, so it
            // must carry the change's goal rather than the scratch file it was
            // read from. After a restart the in-memory record is gone and the
            // path is all that is left.
            let description = record
                .map(|r| r.description)
                .filter(|d| !d.trim().is_empty())
                .unwrap_or_else(|| format!("Code change from {}", path.display()));

            results.push(EvolutionResult {
                kind: EvolutionKind::CodeChange,
                artifact_id,
                description,
                content,
                status,
                created_at: chrono::Utc::now(),
                eval_summary: None,
            });
        }

        results.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(results)
    }

    /// Take a change out of the pending queue, keeping it under `retired/` for
    /// a post-mortem.
    ///
    /// The queue is the change directory itself and carries no memory of its
    /// own: the statuses live in the engine, which every restart empties. Once
    /// a record is gone a leftover `.diff` is indistinguishable from a brand
    /// new one — it reads back as `CompileChecked` — so a change that can never
    /// apply is re-applied on every cycle for the life of the deployment,
    /// recording a learning each time and holding the landing channel shut.
    /// Recording the outcome is the audit trail; retiring the artifact is what
    /// makes a rejection final.
    pub async fn retire_change(&self, artifact_id: &str, reason: &str) -> SFResult<()> {
        let from = self.change_dir.join(format!("{artifact_id}.diff"));
        if !from.exists() {
            return Ok(());
        }

        let retired_dir = self.change_dir.join("retired");
        tokio::fs::create_dir_all(&retired_dir).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to create retire dir {}: {}",
                retired_dir.display(),
                e
            ))
        })?;

        let to = retired_dir.join(format!("{artifact_id}.diff"));
        tokio::fs::rename(&from, &to).await.map_err(|e| {
            SFError::IO(format!("Failed to retire change {}: {}", from.display(), e))
        })?;

        info!(
            change_id = %artifact_id,
            reason = %reason,
            retired_to = %to.display(),
            "Change retired from the pending queue"
        );
        Ok(())
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
        self.apply_and_test_in(change, &self.project_root).await
    }

    /// 在指定工作树里应用并测试变更。每轮演进取一棵临时工作树，处理后归还，
    /// 因而这里不能沿用构造时的固定目录。
    pub async fn apply_and_test_in(
        &self,
        change: &EvolutionResult,
        workdir: &Path,
    ) -> SFResult<ApplyResult> {
        info!(change_id = %change.artifact_id, "Applying evolution change");

        // 判定类失败一律走 `Ok(ApplyResult { verdict: Refused(..), .. })`，`Err` 只留给
        // "管线没能对一个变更做出判定"（工作树脏、git 起不来这类环境问题）。调用方
        // 据此决定要不要把变更移出待处理队列：环境问题重试有意义，判定不是。把解析/
        // 校验的失败塞进 `Err` 会让这条界线失效——分不清"这个变更不行"和"现在这个
        // 环境不行"，一个有效变更可能因一次 git 抖动被永久退休。
        // `SFError::Validation` 是"对变更本身的断言"，其余变体是环境。
        let targets = match Self::parse_diff(&change.content) {
            Ok(targets) => targets,
            Err(SFError::Validation(e)) => {
                warn!(change_id = %change.artifact_id, error = %e, "Change is not a usable diff");
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed: Vec::new(),
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::MalformedDiff),
                    test_output: format!("Change is not a usable diff: {e}"),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
            Err(e) => return Err(e),
        };
        // 报告面记的是这次变更写了哪些文件；删除没有可写文件，不入此列。
        let files_changed: Vec<PathBuf> = targets
            .iter()
            .filter(|t| t.kind != cog_core::DiffTargetKind::Delete)
            .map(|t| PathBuf::from(&t.path))
            .collect();

        // 晋级门入口：黑名单命中（依赖清单/密钥材料）直接拒收，
        // 不做 apply、不跑测试，状态落 Rejected 留审计痕迹。判据面取全部目标
        // 含删除：删掉一份受保护文件与改写它同样要拦。
        if let Some(policy) = &self.promotion_policy {
            let files: Vec<String> = targets.iter().map(|t| t.path.replace('\\', "/")).collect();
            let diff_lines = crate::promotion_gate::count_diff_lines(&change.content);
            if let crate::GateVerdict::Reject { reason, .. } =
                crate::promotion_gate::classify(&files, diff_lines, policy)
            {
                warn!(change_id = %change.artifact_id, reason = %reason, "Change rejected by promotion gate");
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::PromotionGateRefused),
                    test_output: format!("Promotion gate rejected: {reason}"),
                    new_status: EvolutionStatus::Rejected,
                });
            }
        }

        match Self::validate_change_files(&targets, workdir) {
            Ok(()) => {}
            Err(SFError::Validation(e)) => {
                warn!(change_id = %change.artifact_id, error = %e, "Change touches a file it may not");
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::ForbiddenPath),
                    test_output: format!("Change touches a forbidden or missing path: {e}"),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
            Err(e) => return Err(e),
        }

        // `description` is the field a change carries its goal in: the engine
        // sets it from the module it was asked to improve, and a landed change
        // from the goal the author submitted. Judging the artifact against it
        // is the only check here that can tell "this is not the file you meant"
        // from "this file is wrong", which no gate downstream of the diff can.
        if let Some(reason) = Self::intent_alignment_reason(&change.description, &targets, workdir)
        {
            warn!(change_id = %change.artifact_id, reason = %reason, "Change does not address the goal it was generated for");
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::IntentMismatch),
                test_output: format!("Change does not answer its goal: {reason}"),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        self.ensure_clean_workspace(workdir).await?;

        if let Err(e) = self.git_apply_check(workdir, &change.content).await {
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::ContextDoesNotApply),
                test_output: format!("Change pre-check failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        if let Err(e) = self.git_apply(workdir, &change.content).await {
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::ApplyFailed),
                test_output: format!("Change application failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        // Formatted before it is compiled, and rolled back either way: a change
        // refused here must leave the tree as it found it, or the next run would
        // be verifying this one's leftovers.
        match self.run_cargo_fmt(workdir).await {
            Ok((true, _)) => {}
            Ok((false, output)) => {
                warn!(change_id = %change.artifact_id, "Change is not formatted; rolling back");
                let _ = self.git_reset_hard(workdir).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::FormattingDiffers),
                    test_output: format!(
                        "Change is not what this workspace's formatter produces:\n{output}"
                    ),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, "cargo fmt execution failed");
                let _ = self.git_reset_hard(workdir).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::TestRunUnavailable),
                    test_output: format!("Failed to execute cargo fmt: {}", e),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
        }

        let (test_passed, test_output) = match self.run_cargo_test(workdir).await {
            Ok(result) => result,
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, "cargo test execution failed");
                let _ = self.git_reset_hard(workdir).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::TestRunUnavailable),
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
                let _ = self.git_reset_hard(workdir).await;
                EvolutionStatus::AwaitingReview
            }
        } else {
            warn!(change_id = %change.artifact_id, "Change tests failed; rolling back");
            let _ = self.git_reset_hard(workdir).await;
            EvolutionStatus::ValidationFailed
        };

        let verdict = if test_passed {
            ChangeVerdict::Passed
        } else {
            ChangeVerdict::Refused(cog_core::RejectionCause::TestsFailed)
        };

        Ok(ApplyResult {
            change_id: change.artifact_id.clone(),
            files_changed,
            verdict,
            test_output,
            new_status,
        })
    }

    /// Parse a unified diff change and return the files it touches, each with
    /// the way the diff treats it.
    ///
    /// A patch that creates a file is the one shape whose target does not exist
    /// yet, and the diff says so itself: its old side is `/dev/null`. Carrying
    /// that distinction here is what keeps the path checks below from rejecting
    /// a legitimate creation and from accepting an invented path.
    pub fn parse_diff(content: &str) -> SFResult<Vec<cog_core::DiffTarget>> {
        let targets = cog_core::parse_diff_targets(content);
        if targets.is_empty() {
            return Err(SFError::Validation(
                "No file paths found in change (expected '+++ b/<path>' lines)".into(),
            ));
        }
        Ok(targets)
    }

    /// Validate that every target is safe to touch.
    /// - A file the diff rewrites or deletes must resolve to a real file inside
    ///   the project root.
    /// - A file the diff creates must stay inside the project root by
    ///   construction, since there is no file yet for `canonicalize` to speak
    ///   for.
    /// - Nothing may escape the project root.
    /// - Nothing may be a build/config/deployment/secret file.
    pub fn validate_change_files(
        targets: &[cog_core::DiffTarget],
        project_root: &Path,
    ) -> SFResult<()> {
        let canonical_root = project_root.canonicalize().map_err(|e| {
            SFError::IO(format!(
                "Failed to canonicalize project root {}: {}",
                project_root.display(),
                e
            ))
        })?;

        for target in targets {
            let file = Path::new(&target.path);

            if let Some(reason) = cog_core::forbidden_target_reason(&target.path) {
                return Err(SFError::Validation(reason));
            }

            let absolute = canonical_root.join(file);

            let resolved = match target.kind {
                cog_core::DiffTargetKind::Create => {
                    resolve_created_path(&absolute, &canonical_root, file)?
                }
                _ => {
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
                    canonical
                }
            };

            // The raw path was judged above. Judge the resolved path too: a
            // symlink's own name says nothing about the file the diff ends up
            // rewriting, so the leaf that survives `canonicalize` is the one
            // worth asking about. Same policy function, two inputs.
            let relative = resolved.strip_prefix(&canonical_root).map_err(|_| {
                SFError::Validation(format!(
                    "Resolved target path escapes project root: {}",
                    resolved.display()
                ))
            })?;
            if let Some(reason) = cog_core::forbidden_target_reason(&relative.to_string_lossy()) {
                return Err(SFError::Validation(reason));
            }

            if !resolved
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

    /// Whether a change delivered what the goal that asked for it named, or
    /// `None` when there is nothing to hold it to.
    ///
    /// A goal to extend `README.md` comes back as a brand-new
    /// `crates/cogneva/src/windows_quickstart.rs`: a well-formed diff of a
    /// well-formed file that compiles, so format, lint and test gates all pass
    /// while the file somebody asked about is untouched and a file nobody asked
    /// for appears. Nothing in the artifact says which of the two was wanted —
    /// the goal text is the only place the intent was ever written down, and
    /// the checkout is the only place that can say whether the file it names
    /// exists. This puts the two side by side.
    ///
    /// Everything runs on names and existence, so the verdict is a function of
    /// the goal, the diff's own `/dev/null` sides, and the tree — no model
    /// reads the goal and no threshold is guessed.
    ///
    /// It fails open, and deliberately so on two counts. A goal that names
    /// nothing the checkout can confirm yields no anchor, because a name that
    /// resolves to nothing cannot be contradicted by an artifact. And a change
    /// that rewrites or deletes a real file is left alone even when it adds an
    /// unrelated one, because "the goal mentioned another file too" is not
    /// evidence that this file was the wrong one to touch. What is left is the
    /// one shape with no reading in which it is right: a goal that names a file
    /// which exists, answered by a change that only creates files somewhere
    /// else.
    pub fn intent_alignment_reason(
        goal: &str,
        targets: &[cog_core::DiffTarget],
        project_root: &Path,
    ) -> Option<String> {
        let anchors = goal_anchors(goal, project_root);
        if anchors.is_empty() {
            return None;
        }
        if !targets
            .iter()
            .all(|t| t.kind == cog_core::DiffTargetKind::Create)
        {
            return None;
        }
        let touched: Vec<String> = targets.iter().map(|t| t.path.replace('\\', "/")).collect();
        if touched
            .iter()
            .any(|t| anchors.iter().any(|a| names_same_or_nested(t, a)))
        {
            return None;
        }
        Some(format!(
            "the goal names {}, which exists, but the change only creates {} — none of them is that file or lives under it, so the file the goal is about was left untouched",
            anchors.join(", "),
            touched.join(", ")
        ))
    }

    /// Refuse to apply if the git working tree already has uncommitted changes.
    async fn ensure_clean_workspace(&self, workdir: &Path) -> SFResult<()> {
        let output = tokio::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(workdir)
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
    async fn git_apply_check(&self, workdir: &Path, change_content: &str) -> SFResult<()> {
        self.run_git_apply(workdir, change_content, true).await
    }

    /// Apply change content to the working tree with `git apply`.
    async fn git_apply(&self, workdir: &Path, change_content: &str) -> SFResult<()> {
        self.run_git_apply(workdir, change_content, false).await
    }

    /// Shared implementation for `git apply [--check]`.
    async fn run_git_apply(
        &self,
        workdir: &Path,
        change_content: &str,
        check_only: bool,
    ) -> SFResult<()> {
        let mut cmd = tokio::process::Command::new("git");
        cmd.arg("apply").arg("-v").current_dir(workdir);
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
    async fn git_reset_hard(&self, workdir: &Path) -> SFResult<()> {
        let output = tokio::process::Command::new("git")
            .args(["reset", "--hard", "HEAD"])
            .current_dir(workdir)
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
    async fn git_try(&self, workdir: &Path, args: &[&str]) -> Option<String> {
        let output = tokio::process::Command::new("git")
            .args(args)
            .current_dir(workdir)
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
        self.sync_with_upstream_in(&self.project_root).await
    }

    /// 在指定工作树里同步上游主线。工作树里 `local` 远程指向裸仓库，既有
    /// `fetch local` / `reset --hard local/main` 语义不变。
    pub async fn sync_with_upstream_in(&self, workdir: &Path) -> SFResult<()> {
        if self
            .git_try(workdir, &["remote", "get-url", "local"])
            .await
            .is_none()
        {
            return Ok(());
        }
        if self
            .git_try(workdir, &["fetch", "local", "main"])
            .await
            .is_none()
        {
            warn!("sync_with_upstream: fetch local main failed; keeping current tree");
            return Ok(());
        }
        // 晋级分支首轮可能还不存在，失败不阻塞 main 同步。
        let has_release = self
            .git_try(workdir, &["fetch", "local", "evolution-release"])
            .await
            .is_some();

        let dirty = self
            .git_try(workdir, &["status", "--porcelain"])
            .await
            .map(|s| !s.is_empty())
            .unwrap_or(true);
        if dirty {
            info!("sync_with_upstream: working tree dirty (change in flight); skip");
            return Ok(());
        }

        let head = self
            .git_try(workdir, &["rev-parse", "HEAD"])
            .await
            .unwrap_or_default();
        let upstream = self
            .git_try(workdir, &["rev-parse", "local/main"])
            .await
            .unwrap_or_default();
        if head.is_empty() || upstream.is_empty() {
            return Ok(());
        }
        if head == upstream {
            return Ok(());
        }

        let on_mainline = self
            .git_try(
                workdir,
                &["merge-base", "--is-ancestor", "HEAD", "local/main"],
            )
            .await
            .is_some();
        let published = has_release
            && self
                .git_try(
                    workdir,
                    &[
                        "merge-base",
                        "--is-ancestor",
                        "HEAD",
                        "local/evolution-release",
                    ],
                )
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
            .current_dir(workdir)
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

    /// Hand `cmd` the environment a verdict is allowed to see: cleared, then the
    /// passthrough allowlist with the shared target directory.
    ///
    /// Single-sourced on purpose. The rule it enforces — a process that judges a
    /// change must not inherit what the parent was compiled or configured with —
    /// is the entire reason the allowlist exists, and a second cargo-invoking
    /// site that rebuilt these lines by hand would be free to drop it. That is
    /// not hypothetical: a wrapper left in the passthrough set made a change
    /// fail a compilation it was never given, and the fix only holds as long as
    /// every caller goes through one place.
    fn apply_verification_env(&self, cmd: &mut tokio::process::Command) {
        cmd.env_clear();
        for (key, value) in verification_env(|k| std::env::var(k).ok(), self.target_dir.as_deref())
        {
            cmd.env(key, value);
        }
    }

    /// Spawn one `cargo fmt` invocation under the verification environment and
    /// the format bound, returning (succeeded, combined_output).
    ///
    /// The probe and the check both come through here so that "what may the
    /// formatter see" and "how long may it take" keep one answer each rather
    /// than one per call site.
    async fn run_cargo_fmt_cmd(&self, workdir: &Path, args: &[&str]) -> SFResult<(bool, String)> {
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(args).current_dir(workdir).kill_on_drop(true);
        self.apply_verification_env(&mut cmd);

        let output =
            match tokio::time::timeout(Duration::from_secs(self.fmt_timeout_secs), cmd.output())
                .await
            {
                Ok(result) => result.map_err(|e| {
                    SFError::IO(format!("Failed to run cargo {}: {}", args.join(" "), e))
                })?,
                Err(_) => {
                    return Err(SFError::IO(format!(
                        "cargo {} exceeded the {}s format budget and was killed",
                        args.join(" "),
                        self.fmt_timeout_secs
                    )))
                }
            };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok((output.status.success(), format!("{}{}", stdout, stderr)))
    }

    /// Run `cargo fmt --all -- --check` and return (clean, combined_output).
    ///
    /// A file the formatter would rewrite is refused here rather than landed,
    /// because the commit it lands as fails CI's format check: the deployer
    /// reads that, resets the commit, and the change ends up retired with
    /// nothing having judged what it does. The check is deterministic, so the
    /// cost of refusing is one reformat by whoever generated the change, while
    /// the cost of letting it through is a whole landing-and-rollback cycle —
    /// which is how this gate came to exist.
    ///
    /// Two questions rather than one, because a single exit code cannot answer
    /// both. `cargo fmt --check` exits 1 for a file it would rewrite — and exits
    /// 1 just as well when the toolchain it was told to use is not installed,
    /// which is reachable here: `RUSTUP_TOOLCHAIN` is in the passthrough set.
    /// Reading those as one answer would refuse every change that a deployment
    /// whose toolchain had moved was asked to verify, which is a change refused
    /// for being unverifiable rather than for being wrong. So the probe goes
    /// first and settles whether there is a formatter to ask at all; only after
    /// that does a nonzero exit mean the tree is what the formatter rewrites.
    ///
    /// Runs before the test run and takes no build slot: the check parses the
    /// workspace without compiling any of it, so it is both the cheapest way a
    /// change can be sent back and no competition for the host while a real
    /// build is in flight.
    async fn run_cargo_fmt(&self, workdir: &Path) -> SFResult<(bool, String)> {
        let (available, why) = self
            .run_cargo_fmt_cmd(workdir, &["fmt", "--version"])
            .await?;
        if !available {
            return Err(SFError::IO(format!(
                "the format check has no formatter to ask: {why}"
            )));
        }
        info!("Running cargo fmt --all -- --check");
        self.run_cargo_fmt_cmd(workdir, &["fmt", "--all", "--", "--check"])
            .await
    }

    /// Run `cargo test --workspace` and return (success, combined_output).
    ///
    /// `--no-fail-fast` because the verdict is read by whoever investigates a
    /// rejection: stopping at the first failing crate hides the rest of the
    /// failure surface and makes an environmental problem look like the only
    /// problem.
    async fn run_cargo_test(&self, workdir: &Path) -> SFResult<(bool, String)> {
        info!("Running cargo test --workspace");
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(["test", "--workspace", "--no-fail-fast"])
            .current_dir(workdir)
            .kill_on_drop(true);
        self.apply_verification_env(&mut cmd);

        let started = Instant::now();
        let output =
            match tokio::time::timeout(Duration::from_secs(self.test_timeout_secs), cmd.output())
                .await
            {
                Ok(result) => {
                    let output = result
                        .map_err(|e| SFError::IO(format!("Failed to run cargo test: {}", e)))?;
                    if let Some(budget) = &self.budget {
                        budget.record_run(
                            crate::verification_budget::KIND_TEST,
                            started.elapsed().as_secs(),
                        );
                    }
                    output
                }
                Err(_) => {
                    if let Some(budget) = &self.budget {
                        budget.record_timeout(crate::verification_budget::KIND_TEST);
                    }
                    return Err(SFError::IO(format!(
                        "cargo test exceeded the {}s verification budget and was killed",
                        self.test_timeout_secs
                    )));
                }
            };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{}{}", stdout, stderr);
        Ok((output.status.success(), combined))
    }
}

/// Resolve the path of a file a patch is about to create.
///
/// There is no file for `canonicalize` to speak for — the patch is what brings
/// it into being — and its parent directories need not exist either, since a
/// patch creates them on the way (verified against `git apply`). What can
/// still be checked is the deepest ancestor that does exist: canonicalizing it
/// stops a symlinked directory pointing out of the tree from being followed on
/// the way to the new file.
///
/// `..` never reaches here. It is refused before the kind is even consulted,
/// because `git apply` refuses it too ("invalid path") — resolving it lexically
/// instead would let a path the apply gate is about to reject pass this
/// judgement, which is the same kind of unactionable defect this whole path
/// exists to name early.
fn resolve_created_path(
    absolute: &Path,
    canonical_root: &Path,
    reported: &Path,
) -> SFResult<PathBuf> {
    let normalized: PathBuf = absolute
        .components()
        .filter(|component| !matches!(component, std::path::Component::CurDir))
        .collect();

    if !normalized.starts_with(canonical_root) {
        return Err(SFError::Validation(format!(
            "Target path escapes project root: {}",
            reported.display()
        )));
    }

    let mut existing = normalized.as_path();
    while !existing.exists() {
        match existing.parent() {
            Some(parent) => existing = parent,
            None => break,
        }
    }
    let canonical_existing = existing.canonicalize().map_err(|e| {
        SFError::IO(format!(
            "Failed to canonicalize {} while resolving {}: {}",
            existing.display(),
            reported.display(),
            e
        ))
    })?;
    if !canonical_existing.starts_with(canonical_root) {
        return Err(SFError::Validation(format!(
            "Target path escapes project root: {}",
            reported.display()
        )));
    }

    Ok(normalized)
}

/// The files a goal names that this checkout can confirm are real.
///
/// Two ways a goal names a file, and each needs a different side to be right.
/// A path it spells out (`crates/x/y.rs`) is checked where it points. A bare
/// word (`README`) is not a name at all until something says so — so the
/// checkout supplies the candidates and the goal is asked which of them it
/// means. Running the question that way is what keeps `improve` and `module`
/// out of the answer without a stopword list to maintain.
///
/// Only the checkout root is offered as candidates. A goal naming `README` is
/// taken to mean the one the project shows at its top, and widening the search
/// would make the same goal resolve differently as the tree grows.
fn goal_anchors(goal: &str, project_root: &Path) -> Vec<String> {
    let mut anchors: Vec<String> = Vec::new();
    let mut push = |name: String| {
        if !anchors.iter().any(|seen| seen == &name) {
            anchors.push(name);
        }
    };

    for named in cog_core::paths_named_in_goal(goal) {
        if project_root.join(&named).exists() {
            push(named);
        }
    }

    let Ok(entries) = std::fs::read_dir(project_root) else {
        return anchors;
    };
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let stem = Path::new(file_name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(file_name);
        if cog_core::mentions_name(goal, file_name) || cog_core::mentions_name(goal, stem) {
            push(file_name.to_string());
        }
    }

    anchors
}

/// Whether `target` is `anchor` or sits inside it. Written bare, `README` and
/// `README` are the same name; written with a trailing slash, a goal naming a
/// directory still covers the files under it.
fn names_same_or_nested(target: &str, anchor: &str) -> bool {
    let target = target.trim_matches('/');
    let anchor = anchor.trim_matches('/');
    target == anchor
        || target.starts_with(&format!("{anchor}/"))
        || anchor.starts_with(&format!("{target}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The seeded source of the formatting fixtures: line 2 is the line the two
    /// tests change, so they differ in nothing else.
    const FORMATTED_PROBE_SOURCE: &str = "pub fn answer() -> i32 {\n    41\n}\n";

    /// A crate whose only test sleeps, so a run against it is slow for a reason
    /// the budget can measure rather than one that depends on how warm this
    /// machine's build cache happens to be.
    fn write_sleeping_crate(root: &std::path::Path, sleep_secs: u64) {
        std::fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"budget-probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(
            root.join("src/lib.rs"),
            format!(
                "#[cfg(test)]\nmod tests {{\n    #[test]\n    fn slow() {{\n        \
                 std::thread::sleep(std::time::Duration::from_secs({sleep_secs}));\n    }}\n}}\n"
            ),
        )
        .unwrap();
    }

    /// The `kind = "test"` reading the scrape would carry.
    async fn test_kind_reading(
        budget: &crate::verification_budget::VerificationBudget,
        metric: &str,
    ) -> Option<f64> {
        use cog_core::observability::Observable;
        budget
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.name == metric
                    && m.labels
                        .get(crate::verification_budget::KIND_LABEL)
                        .map(String::as_str)
                        == Some(crate::verification_budget::KIND_TEST)
            })
            .map(|m| m.value)
    }

    /// A verification run that does not fit its budget is killed, and the error
    /// names the budget that killed it.
    ///
    /// What this replaces was not a wrong verdict but an absent one: the knob
    /// was stored on the pipeline and never read, so a stuck
    /// `cargo test --workspace` held the executor's only in-flight slot
    /// indefinitely and every change behind it queued forever. The reason
    /// matters as much as the kill — a retired change is read afterwards by
    /// whoever investigates it, and only "exceeded the Ns budget" says the
    /// change was never judged.
    ///
    /// The sleep is what makes this deterministic. The run is slow for a reason
    /// the test controls, so the budget is exceeded whether the kill lands
    /// during the compile or during the test.
    #[tokio::test]
    async fn a_run_that_outlives_its_budget_is_killed_with_the_budget_named() {
        let root = tempfile::tempdir().unwrap();
        write_sleeping_crate(root.path(), 30);
        let budget = Arc::new(crate::verification_budget::VerificationBudget::new(1, 60));
        let pipeline = ChangePipeline::new(root.path(), root.path(), false)
            .with_verification_budget(budget.clone());

        let err = pipeline
            .run_cargo_test(root.path())
            .await
            .expect_err("a run that cannot finish in a second must not be waited on");

        assert!(
            err.to_string()
                .contains("exceeded the 1s verification budget"),
            "the rejection has to name the budget that caused it: {err}"
        );
        assert_eq!(
            test_kind_reading(&budget, crate::verification_budget::TIMEOUTS_TOTAL_METRIC).await,
            Some(1.0),
            "a killed run has to be readable as a kill, not only as a log line"
        );
        assert_eq!(
            test_kind_reading(&budget, crate::verification_budget::LAST_RUN_SECONDS_METRIC).await,
            None,
            "a run cut short at the budget has no duration of its own to report"
        );
    }

    /// A run that finishes inside its budget is not counted as a kill, and its
    /// duration reaches the scrape — the reading that says the budget is close
    /// to binding before anything has been retired for it.
    #[tokio::test]
    async fn a_run_that_fits_its_budget_reports_its_duration() {
        let root = tempfile::tempdir().unwrap();
        write_sleeping_crate(root.path(), 1);
        let budget = Arc::new(crate::verification_budget::VerificationBudget::new(120, 60));
        let pipeline = ChangePipeline::new(root.path(), root.path(), false)
            .with_verification_budget(budget.clone());

        let (passed, output) = pipeline
            .run_cargo_test(root.path())
            .await
            .expect("a run inside its budget must be waited on");

        assert!(
            passed,
            "the sleeping crate passes once it finishes: {output}"
        );
        assert_eq!(
            test_kind_reading(&budget, crate::verification_budget::TIMEOUTS_TOTAL_METRIC).await,
            Some(0.0)
        );
        let elapsed =
            test_kind_reading(&budget, crate::verification_budget::LAST_RUN_SECONDS_METRIC)
                .await
                .expect("a finished run has a duration to report");
        assert!(elapsed >= 1.0, "the run slept a second: {elapsed}");
    }

    /// A diff that introduces one brand-new file, the shape the generator
    /// produces when it answers a request about an existing file by writing a
    /// different one.
    fn creates(path: &str) -> String {
        format!(
            "diff --git a/{path} b/{path}\n\
             new file mode 100644\n\
             --- /dev/null\n\
             +++ b/{path}\n\
             @@ -0,0 +1 @@\n\
             +fn generated() {{}}\n"
        )
    }

    /// A diff that rewrites an existing file in place.
    fn rewrites(path: &str) -> String {
        format!(
            "diff --git a/{path} b/{path}\n\
             --- a/{path}\n\
             +++ b/{path}\n\
             @@ -1 +1 @@\n\
             -old\n\
             +new\n"
        )
    }

    fn targets_of(diff: &str) -> Vec<cog_core::DiffTarget> {
        ChangePipeline::parse_diff(diff).expect("test diff must parse")
    }

    /// The shape G3 is: a goal about a file that is right there, answered by a
    /// creation somewhere else. Every gate downstream of the diff passes it —
    /// the diff is well formed, the new file compiles — so this is the only
    /// place the two can be compared.
    #[test]
    fn a_goal_about_a_file_the_change_never_touches_is_refused() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("README.md"), "# project\n").unwrap();
        let targets = targets_of(&creates("crates/cogneva/src/windows_quickstart.rs"));

        let reason = ChangePipeline::intent_alignment_reason(
            "建议在 README.md 中补充 Windows 快速开始",
            &targets,
            root.path(),
        )
        .expect("a creation elsewhere must not answer a goal about README.md");

        assert!(reason.contains("README.md"), "{reason}");
        assert!(reason.contains("windows_quickstart.rs"), "{reason}");
    }

    /// The same goal written the way a person writes it in prose: the file
    /// named without its extension. Only the checkout can say `README` means
    /// `README.md`, so it is asked.
    #[test]
    fn a_bare_name_the_checkout_confirms_still_anchors_the_goal() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("README.md"), "# project\n").unwrap();
        let targets = targets_of(&creates("crates/cogneva/src/windows_quickstart.rs"));

        assert!(
            ChangePipeline::intent_alignment_reason(
                "改 README 里的快速开始",
                &targets,
                root.path()
            )
            .is_some(),
            "`README` beside a real README.md names that file"
        );
    }

    #[test]
    fn a_goal_the_change_actually_carries_out_is_allowed() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("README.md"), "# project\n").unwrap();

        assert!(
            ChangePipeline::intent_alignment_reason(
                "补充 README.md 的 Windows 快速开始",
                &targets_of(&rewrites("README.md")),
                root.path(),
            )
            .is_none(),
            "rewriting the file the goal named is the answer to the goal"
        );
    }

    /// Naming a directory covers what is put inside it, so a goal about a
    /// module is satisfied by creating a file in that module rather than by
    /// creating the directory itself.
    #[test]
    fn a_creation_inside_a_named_directory_is_allowed() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates/cog-core/src")).unwrap();
        std::fs::write(root.path().join("crates/cog-core/src/lib.rs"), "//\n").unwrap();

        assert!(ChangePipeline::intent_alignment_reason(
            "add a helper to crates/cog-core/src",
            &targets_of(&creates("crates/cog-core/src/helper.rs")),
            root.path(),
        )
        .is_none());
    }

    /// A goal naming a file that does not exist yet is a request to create it,
    /// and a creation is exactly the right answer — the `/dev/null` side of the
    /// diff is where that is written down.
    #[test]
    fn creating_the_file_the_goal_names_is_the_answer() {
        let root = tempfile::tempdir().unwrap();
        assert!(!root.path().join("CHANGELOG.md").exists());

        assert!(ChangePipeline::intent_alignment_reason(
            "add a CHANGELOG.md",
            &targets_of(&creates("CHANGELOG.md")),
            root.path(),
        )
        .is_none());
    }

    /// A goal that names nothing the checkout can confirm says nothing about
    /// which file was meant, and a name that resolves to nothing cannot be
    /// contradicted by an artifact.
    #[test]
    fn a_goal_without_a_confirmable_name_is_not_judged() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("README.md"), "# project\n").unwrap();

        assert!(ChangePipeline::intent_alignment_reason(
            "make the frontend module faster",
            &targets_of(&creates("crates/cogneva/src/windows_quickstart.rs")),
            root.path(),
        )
        .is_none());
        assert!(
            ChangePipeline::intent_alignment_reason(
                "补充 docs/NOTES.md 的内容",
                &targets_of(&creates("crates/cogneva/src/windows_quickstart.rs")),
                root.path(),
            )
            .is_none(),
            "a named path that does not exist is not a claim this can check"
        );
    }

    /// A goal often names context as well as the target. A change that rewrites
    /// a real file did the work it was asked to do somewhere, and "you also
    /// mentioned another file" is not evidence that this was the wrong one.
    #[test]
    fn a_change_that_rewrites_a_real_file_is_left_alone() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates/x/src")).unwrap();
        std::fs::write(root.path().join("README.md"), "# project\n").unwrap();
        std::fs::write(root.path().join("crates/x/src/lib.rs"), "//\n").unwrap();

        assert!(ChangePipeline::intent_alignment_reason(
            "see README.md, fix the parser in crates/x/src/lib.rs",
            &targets_of(&rewrites("crates/x/src/lib.rs")),
            root.path(),
        )
        .is_none());
    }

    /// The verdict has to be reachable from the flow that applies changes, not
    /// only from the function: a rejected change must land as a judgement about
    /// the change, never as an environment error the caller would retry.
    #[tokio::test]
    async fn the_apply_flow_refuses_a_change_that_misses_its_goal() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("README.md"), "# project\n").unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "change-GOAL".into(),
            description: "补充 README.md 的 Windows 快速开始".into(),
            content: creates("crates/cogneva/src/windows_quickstart.rs"),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::IntentMismatch)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
        assert!(
            result.test_output.contains("README.md"),
            "{}",
            result.test_output
        );
    }

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
        let targets = ChangePipeline::parse_diff(change).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].path, "crates/foo/src/bar.rs");
        assert_eq!(targets[0].kind, cog_core::DiffTargetKind::Modify);
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
        let targets = ChangePipeline::parse_diff(change).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].path, "crates/foo/src/new.rs");
        assert_eq!(targets[0].kind, cog_core::DiffTargetKind::Create);
    }

    #[test]
    fn a_created_file_may_be_absent_and_still_be_valid() {
        // A patch that creates a file names a path that does not exist yet.
        // Requiring the file to be there rejects exactly the change the patch
        // is; requiring nothing lets an invented path through to a gate whose
        // verdict is a line number. The diff's own `/dev/null` side is what
        // separates the two.
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates/foo/src")).unwrap();
        let change = "diff --git a/crates/foo/src/new.rs b/crates/foo/src/new.rs\n\
                      new file mode 100644\n\
                      --- /dev/null\n\
                      +++ b/crates/foo/src/new.rs\n\
                      @@ -0,0 +1 @@\n\
                      +fn new() {}\n";
        let targets = ChangePipeline::parse_diff(change).unwrap();
        assert!(!root.path().join("crates/foo/src/new.rs").exists());
        ChangePipeline::validate_change_files(&targets, root.path())
            .expect("a declared creation must validate");
    }

    #[test]
    fn a_rewrite_of_a_missing_file_is_still_rejected() {
        let root = tempfile::tempdir().unwrap();
        let change = "diff --git a/crates/foo/src/ghost.rs b/crates/foo/src/ghost.rs\n\
                      --- a/crates/foo/src/ghost.rs\n\
                      +++ b/crates/foo/src/ghost.rs\n\
                      @@ -1 +1 @@\n\
                      -old\n\
                      +new\n";
        let targets = ChangePipeline::parse_diff(change).unwrap();
        assert!(ChangePipeline::validate_change_files(&targets, root.path()).is_err());
    }

    #[test]
    fn a_created_file_may_not_escape_through_dot_dot_or_a_symlink() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates")).unwrap();

        let escaping = "diff --git a/crates/../../etc/passwd b/crates/../../etc/passwd\n\
                        new file mode 100644\n\
                        --- /dev/null\n\
                        +++ b/crates/../../etc/passwd\n\
                        @@ -0,0 +1 @@\n\
                        +root\n";
        let targets = ChangePipeline::parse_diff(escaping).unwrap();
        assert!(
            ChangePipeline::validate_change_files(&targets, root.path()).is_err(),
            "a creation that walks out of the root must be rejected"
        );

        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("crates/link")).unwrap();
        let through_link = "diff --git a/crates/link/passwd b/crates/link/passwd\n\
                            new file mode 100644\n\
                            --- /dev/null\n\
                            +++ b/crates/link/passwd\n\
                            @@ -0,0 +1 @@\n\
                            +root\n";
        let targets = ChangePipeline::parse_diff(through_link).unwrap();
        assert!(
            ChangePipeline::validate_change_files(&targets, root.path()).is_err(),
            "a creation whose ancestor links out of the root must be rejected"
        );
    }

    #[test]
    fn a_created_protected_file_is_still_rejected() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates/foo")).unwrap();
        let change = "diff --git a/crates/foo/Cargo.toml b/crates/foo/Cargo.toml\n\
                      new file mode 100644\n\
                      --- /dev/null\n\
                      +++ b/crates/foo/Cargo.toml\n\
                      @@ -0,0 +1,2 @@\n\
                      +[package]\n\
                      +name = \"foo\"\n";
        let targets = ChangePipeline::parse_diff(change).unwrap();
        assert!(ChangePipeline::validate_change_files(&targets, root.path()).is_err());
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

        let pipeline = ChangePipeline::new(root.to_path_buf(), root.join("changes"), true);
        pipeline.git_apply_check(root, change).await.unwrap();
        pipeline.git_apply(root, change).await.unwrap();

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
        let targets = vec![cog_core::DiffTarget {
            path: "../etc/passwd".into(),
            kind: cog_core::DiffTargetKind::Modify,
        }];
        assert!(ChangePipeline::validate_change_files(&targets, &root).is_err());
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
        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::PromotionGateRefused)
        );
        assert_eq!(result.new_status, crate::types::EvolutionStatus::Rejected);
        assert!(result.test_output.contains("Promotion gate rejected"));
    }

    /// 变更必须落在传入的工作树里，兄弟工作树与构造时的固定目录都不受影响
    /// ——这是「按任务动态分配工作树」的核心隔离性质。
    #[tokio::test]
    async fn apply_lands_in_given_worktree_not_siblings() {
        let (upstream, _work) = scaffold_upstream().await;
        let tmp = tempfile::tempdir().unwrap();
        let mgr = crate::workspace::WorkspaceManager::new(
            upstream.path(),
            tmp.path().join("workspaces"),
            tmp.path().join("target"),
        );
        let mine = mgr
            .acquire_ephemeral("cycle", crate::workspace::BaseRef::Branch("main".into()))
            .await
            .unwrap();
        let sibling = mgr
            .acquire_ephemeral("cycle", crate::workspace::BaseRef::Branch("main".into()))
            .await
            .unwrap();

        // project_root 指到一个无关目录，确保走的是传入的 workdir 而不是构造值。
        let decoy = tmp.path().join("decoy");
        let pipeline = ChangePipeline::new(&decoy, decoy.join("changes"), true);
        let diff = r#"diff --git a/a.txt b/a.txt
index 1111111..2222222 100644
--- a/a.txt
+++ b/a.txt
@@ -1 +1,2 @@
 a
+b
"#;
        pipeline.ensure_clean_workspace(&mine.path).await.unwrap();
        pipeline.git_apply_check(&mine.path, diff).await.unwrap();
        pipeline.git_apply(&mine.path, diff).await.unwrap();

        assert_eq!(
            tokio::fs::read_to_string(mine.path.join("a.txt"))
                .await
                .unwrap(),
            "a\nb\n"
        );
        assert_eq!(
            tokio::fs::read_to_string(sibling.path.join("a.txt"))
                .await
                .unwrap(),
            "a\n",
            "兄弟工作树必须保持基线"
        );
        assert!(!decoy.exists(), "构造时的固定目录不应被写入");
    }

    #[tokio::test]
    async fn apply_and_test_without_policy_keeps_legacy_behavior() {
        // 未配置晋级策略时入口校验不生效（旧行为零回归）：
        // Cargo.toml change 会走到 validate_change_files 的既有黑名单。
        let temp = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(temp.path(), temp.path().join("changes"), true);
        assert!(pipeline.promotion_policy.is_none());
    }

    /// 对变更本身的判定必须以 `Ok(test_passed = false)` 返回，不能是 `Err`。
    /// 调用方靠这个区分"变更不行"（要移出队列，重试无意义）和"环境不行"（要留着重
    /// 试）；判成 `Err` 会让一个语法就不合法的变更每次周期被重新提交一遍。
    #[tokio::test]
    async fn an_unparsable_change_is_a_verdict_not_an_error() {
        let temp = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(temp.path(), temp.path().join("changes"), true);
        let change = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: "garbage-1".into(),
            description: "not a diff at all".into(),
            content: "this is not a unified diff\n".into(),
            status: crate::types::EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test(&change)
            .await
            .expect("不可解析的变更是一个判定，不是管线错误");
        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::MalformedDiff)
        );
        assert_eq!(
            result.new_status,
            crate::types::EvolutionStatus::ValidationFailed
        );
    }

    /// Every refusal names the criterion it hit, and the criteria are not
    /// interchangeable: a path the change may not touch calls for telling the
    /// generator what it may write, a patch whose context is gone calls for
    /// looking at what it was given to read. Sharing a word between them would
    /// make both readings useless.
    #[tokio::test]
    async fn a_change_that_leaves_the_tree_is_refused_for_its_path() {
        let root = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), true);
        let change = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: "escaping-1".into(),
            description: "补一个模块".into(),
            content: creates("../outside.rs"),
            status: crate::types::EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::ForbiddenPath)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
    }

    /// A patch whose context no longer fits the tree is refused for the tree,
    /// not for its syntax — the diff is well formed, it is the file it was
    /// written against that moved.
    #[tokio::test]
    async fn a_change_whose_context_is_gone_is_refused_for_the_tree() {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        tokio::fs::write(root.path().join("a.txt"), "now\n")
            .await
            .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "stale-1".into(),
            description: "调整这一行的取值".into(),
            // The diff expects the file to still say `old`.
            content: rewrites("a.txt"),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::ContextDoesNotApply)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
    }

    /// The suite ran and the change broke it — the one refusal whose evidence
    /// is the failing run itself rather than a check that can be re-read from
    /// the artifact alone.
    #[tokio::test]
    async fn a_change_that_breaks_the_suite_is_refused_for_the_tests() {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        tokio::fs::write(
            root.path().join("Cargo.toml"),
            "[package]\nname = \"probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(root.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(
            root.path().join("src/lib.rs"),
            "pub fn answer() -> i32 {\n    41\n}\n\n#[test]\nfn answer_is_42() {\n    assert_eq!(answer(), 42);\n}\n",
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "breaking-1".into(),
            description: "修正这个取值的计算".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -1,3 +1,3 @@\n\
                      \x20pub fn answer() -> i32 {\n\
                      -    41\n\
                      +    40\n\
                      \x20}\n"
                .into(),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::TestsFailed)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
        // Control for the control: the suite really ran and really failed. Any
        // nonzero exit lands in this cause — a build that never started
        // included — so the output has to name the test that broke.
        assert!(
            result.test_output.contains("answer_is_42"),
            "the failing test is not in the evidence: {}",
            result.test_output
        );
    }

    /// A change the formatter would rewrite is refused before anything compiles
    /// it, and the tree is left as it was found.
    ///
    /// The refusal exists because the commit this would land as fails CI's
    /// format check, and the answer to that is to reset the commit — so the
    /// change gets retired without anything having judged what it does. Note
    /// what the fixture has to get right for a whole-tree check to mean
    /// anything: the seeded tree is already formatted, so the diff is the only
    /// thing this refusal can be about. A tree that was not clean to begin with
    /// could not use this check to blame a change.
    ///
    /// The change compiles and its tests would pass — `41+1` is valid Rust that
    /// `rustfmt` spells `41 + 1`. That is deliberate: it keeps the refusal
    /// attributable to the formatting alone.
    #[tokio::test]
    async fn a_change_that_is_not_formatted_is_refused_before_it_is_compiled() {
        let (root, pipeline) = formatted_probe_workspace().await;
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "unformatted-1".into(),
            description: "修正这个取值的计算".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -1,3 +1,3 @@\n\
                      \x20pub fn answer() -> i32 {\n\
                      -    41\n\
                      +    41+1\n\
                      \x20}\n"
                .into(),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::FormattingDiffers)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
        assert!(
            result.test_output.contains("src/lib.rs"),
            "the evidence has to name what the formatter would rewrite: {}",
            result.test_output
        );
        assert_eq!(
            tokio::fs::read_to_string(root.path().join("src/lib.rs"))
                .await
                .unwrap(),
            FORMATTED_PROBE_SOURCE,
            "a refused change must not leave its own text behind for the next run to judge"
        );
    }

    /// The other side: a change that is formatted is not refused for it. A gate
    /// is only worth having if it can be told apart from one that always fires,
    /// and a false refusal here is the more expensive mistake — it retires a
    /// change that was fine and spends a generation to get it back.
    #[tokio::test]
    async fn a_formatted_change_is_not_refused_for_its_formatting() {
        let (root, pipeline) = formatted_probe_workspace().await;
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "formatted-1".into(),
            description: "修正这个取值的计算".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -1,3 +1,3 @@\n\
                      \x20pub fn answer() -> i32 {\n\
                      -    41\n\
                      +    42\n\
                      \x20}\n"
                .into(),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Passed,
            "a formatted change reached a verdict: {:?}",
            result.verdict
        );
    }

    /// A crate that is formatted to begin with, and a pipeline over it.
    ///
    /// Shared by the two formatting tests so that the only thing between them is
    /// the line the change writes: one of them has to be refused and the other
    /// must not be, and a fixture that differed in any other way could not show
    /// which of the two the gate is answering.
    async fn formatted_probe_workspace() -> (tempfile::TempDir, ChangePipeline) {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        tokio::fs::write(
            root.path().join("Cargo.toml"),
            "[package]\nname = \"fmt-probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(root.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(root.path().join("src/lib.rs"), FORMATTED_PROBE_SOURCE)
            .await
            .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        (root, pipeline)
    }

    /// 另一侧：工作树脏是环境问题，必须保持 `Err`。若它变成 `Ok(verdict: Refused(..))`，
    /// 一次并发残留就会把一个完好的变更永久退休掉。
    #[tokio::test]
    async fn a_dirty_workspace_stays_an_error_so_the_change_is_not_retired() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for args in [
            vec!["init"],
            vec!["config", "user.email", "t@t.com"],
            vec!["config", "user.name", "t"],
        ] {
            tokio::process::Command::new("git")
                .args(&args)
                .current_dir(root)
                .output()
                .await
                .unwrap();
        }
        tokio::fs::write(root.join("a.txt"), "a\n").await.unwrap();
        tokio::process::Command::new("git")
            .args(["add", "."])
            .current_dir(root)
            .output()
            .await
            .unwrap();
        tokio::process::Command::new("git")
            .args(["commit", "-m", "seed"])
            .current_dir(root)
            .output()
            .await
            .unwrap();

        // 变更指向真实存在的 a.txt，本身完全合法——失败只来自工作树状态。
        let change = crate::types::EvolutionResult {
            kind: crate::types::EvolutionKind::CodeChange,
            artifact_id: "ok-1".into(),
            description: "valid change".into(),
            content: r#"diff --git a/a.txt b/a.txt
index 1111111..2222222 100644
--- a/a.txt
+++ b/a.txt
@@ -1 +1,2 @@
 a
+b
"#
            .into(),
            status: crate::types::EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        // 弄脏工作树。
        tokio::fs::write(root.join("a.txt"), "dirty\n")
            .await
            .unwrap();

        let pipeline = ChangePipeline::new(root, root.join("changes"), true);
        assert!(
            pipeline.apply_and_test(&change).await.is_err(),
            "工作树脏是环境问题，必须走 Err 而不是判定"
        );
    }

    /// 退休必须真的把变更移出待处理队列。队列就是变更目录本身，`pending_changes`
    /// 不认识 `retired/`，所以这一移动是让"拒绝"成为终局的唯一手段：留在原地的
    /// 记录会在进程重启后失去它的状态，被读成一个全新的待处理变更。
    #[tokio::test]
    async fn retiring_a_change_removes_it_from_the_pending_queue() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);

        let id = "task-1-abc123";
        tokio::fs::write(change_dir.join(format!("{id}.diff")), "not a real diff\n")
            .await
            .unwrap();
        assert_eq!(pipeline.pending_changes(None).await.unwrap().len(), 1);

        pipeline.retire_change(id, "corrupt patch").await.unwrap();

        assert!(
            pipeline.pending_changes(None).await.unwrap().is_empty(),
            "退休后的变更不得再出现在待处理队列里"
        );
        assert!(
            change_dir
                .join("retired")
                .join(format!("{id}.diff"))
                .exists(),
            "变更要留在 retired/ 供事后取证，不是被删掉"
        );
    }

    /// 缺席即无事：调用方可能在失败路径上重复调用，或对一条已被别的分支退休的
    /// 变更再调一次，这都不该变成错误。
    #[tokio::test]
    async fn retiring_an_absent_change_is_a_no_op() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);

        pipeline
            .retire_change("never-existed", "n/a")
            .await
            .unwrap();

        assert!(!change_dir.join("retired").exists());
    }

    /// 部署自己的配置一旦渗进验证进程，同一个变更在不同部署上就会得到不同判定：
    /// 某部署的 `COGNEVA_GITEE_API_BASE` / `COGNEVA_DATA_VOLUME_CLAIM` 会盖掉测试
    /// 临时文件里的期望值，于是门禁变成"部署像不像 CI"而不是"变更对不对"。
    /// 这条不变式同时挡住将来有人把产品命名空间的变量加进白名单。
    #[test]
    fn verification_env_carries_no_deployment_configuration() {
        let polluted = |key: &str| match key {
            "PATH" => Some("/usr/local/bin:/usr/bin".to_string()),
            "HOME" => Some("/root".to_string()),
            "CARGO_HOME" => Some("/usr/local/cargo".to_string()),
            "COGNEVA_GITEE_API_BASE" => Some("http://gw:8081/gitee".to_string()),
            "COGNEVA_DATA_VOLUME_CLAIM" => Some("cogneva-evolution-data-pvc".to_string()),
            "COGNEVA_SELF_EVOLUTION_PROMOTION_ENABLED" => Some("true".to_string()),
            "PG_PASSWORD" => Some("not-a-real-password".to_string()),
            _ => None,
        };

        let env = verification_env(polluted, None);
        let keys: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();

        assert!(keys.contains(&"PATH"), "工具链需要 PATH 才能启动");
        assert!(keys.contains(&"CARGO_HOME"), "cargo 需要它的缓存目录");
        assert!(
            !keys.iter().any(|k| k.starts_with("COGNEVA_")),
            "产品命名空间的变量不得进入判据进程，实测进了：{keys:?}"
        );
        assert!(!keys.contains(&"PG_PASSWORD"), "部署侧凭证不得进入判据进程");
    }

    /// 编译器包装器不得进入判据进程，两个方向都钉死：白名单里现在没有，将来加回来
    /// 也会在第一条断言上红；就算绕过白名单直接 `env` 进来，第二条断言看的是判定进程
    /// 实际拿到的键。
    ///
    /// 病因不是"判定变慢"：包装器换掉的是编译器本身，父进程的构建配置于是决定了判据
    /// 进程编译出什么。实测过一次真实的失败形态——父进程（覆盖率插桩）设了
    /// `RUSTC_WRAPPER`，判据进程继承后 cargo 探测编译器即失败（`<wrapper> <rustc> -vV`），
    /// 变更**从未被编译**就被判为不通过。一个从未被评判的变更被判为不合格，是这套判定
    /// 面最坏的失效方向：它看起来和"变更确实是坏的"一模一样。
    ///
    /// `RUSTC_WORKSPACE_WRAPPER` 一并钉住：它是同一个机制的另一半，漏掉就等于留了
    /// 一条绕过路径。
    #[test]
    fn verification_env_carries_no_compiler_wrapper() {
        assert!(
            !VERIFICATION_ENV_PASSTHROUGH
                .iter()
                .any(|key| key.ends_with("_WRAPPER")),
            "编译器包装器不得进白名单（判据必须只依赖变更本身，实测白名单：\
             {VERIFICATION_ENV_PASSTHROUGH:?}）"
        );

        let polluted = |key: &str| match key {
            "PATH" => Some("/usr/local/bin:/usr/bin".to_string()),
            "RUSTC_WRAPPER" => Some("/home/runner/.cargo/bin/cargo-llvm-cov".to_string()),
            "RUSTC_WORKSPACE_WRAPPER" => Some("/usr/local/bin/sccache".to_string()),
            _ => None,
        };
        let env = verification_env(polluted, None);
        let keys: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();

        assert!(keys.contains(&"PATH"), "工具链需要 PATH 才能启动");
        for wrapper in ["RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"] {
            assert!(
                !keys.contains(&wrapper),
                "{wrapper} 进了判据进程：判定结果会随部署的构建配置变化，\
                 而只为父进程构建服务的包装器会让判定直接失败"
            );
        }
    }

    /// 共享 target 目录必须在清空环境后显式补回，否则验证会退回工作树内的
    /// target，每轮都全量重编。
    #[test]
    fn verification_env_keeps_the_shared_target_dir() {
        let env = verification_env(|_| None, Some(Path::new("/var/cache/cogneva-target")));
        assert_eq!(
            env,
            vec![(
                "CARGO_TARGET_DIR".to_string(),
                "/var/cache/cogneva-target".to_string()
            )]
        );
    }
}
