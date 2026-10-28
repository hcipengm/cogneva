//! principle: feedback_a_promise_shared_by_two_knobs_is_not_proven_by_a_test_on_one_of_them
//! principle: feedback_a_gate_never_fired_because_its_input_has_no_producer
//! Deployment stage of L2 self-evolution.
//!
//! Responsibilities:
//! - Commit applied changes to the local git repository.
//! - Build a new release binary with `cargo build --release --bin cogneva`.
//! - If the build fails, roll back the git commit.
//! - Stage the new binary for the supervisor's binary switcher.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cog_core::{EvolutionIntent, SFError, SFResult};
use tracing::{info, warn};

use crate::evolution_build_readings::{BuildEnding, EvolutionBuildReadings};

/// Artifact produced by a successful build.
#[derive(Debug, Clone)]
pub struct BuildArtifact {
    pub change_id: String,
    pub commit_hash: String,
    pub new_binary_path: PathBuf,
    pub build_duration_secs: u64,
}

/// What the last build reported: how it ended, and how much resident memory it
/// peaked at while it ran.
///
/// One struct rather than two fields because these are one attempt. The peak is
/// optional and the ending is not: a build nothing could sample still ended a
/// particular way, and that is a different statement from "no build ran".
#[derive(Debug, Clone, Copy)]
struct BuildReport {
    ending: BuildEnding,
    peak_rss_bytes: Option<u64>,
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
    /// Where the builds this deployer bounds report what they did against the
    /// budget. Absent for a deployer nothing observes.
    budget: Option<Arc<crate::verification_budget::VerificationBudget>>,
    /// Where the builds this deployer runs report what they cost. Absent for a
    /// deployer nothing observes.
    build_readings: Option<Arc<EvolutionBuildReadings>>,
    /// 最近一次构建的结局，连同它的内存峰值。判定在这里做出——只有跑到 cargo 的
    /// 那段代码知道它是被预算杀的、自己失败的、还是压根没起来——所以读数也留在这里，
    /// 由调用方取走。执行搬进一个一过性 Job 之后，调用方就是那条边界上的回程：
    /// 不把它留出来，搬到别的进程里的构建就没有任何结局可记。
    ///
    /// 两半放在一个槽里、而不是各占一个：读的人会把「这次怎么结束的」和「它花了
    /// 多少内存」配成一行，两个槽就会在两个构建并发时配出一次并不存在的尝试。
    last_report: std::sync::Arc<std::sync::Mutex<Option<BuildReport>>>,
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
            binary_name: cog_core::paths::APP_BINARY_NAME.to_string(),
            build_timeout_secs: 3600,
            git_name: "Cogneva Self-Evolution".to_string(),
            git_email: "self-evolution@cogneva.ai".to_string(),
            target_dir: None,
            budget: None,
            build_readings: None,
            last_report: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Attach the observation sink and take the budget from it, for the same
    /// reason the pipeline does: one number enforced and reported, not two.
    ///
    /// The field this sets is the fallback for a deployer with no sink. With one
    /// attached the budget is read at every build (see
    /// [`Self::build_budget_secs`]): the sink's number moves as runs are
    /// measured, and a copy taken here would keep killing at a wall the reading
    /// no longer reports.
    pub fn with_verification_budget(
        mut self,
        budget: Arc<crate::verification_budget::VerificationBudget>,
    ) -> Self {
        self.build_timeout_secs = budget.timeout_secs(crate::verification_budget::KIND_BUILD);
        self.budget = Some(budget);
        self
    }

    /// The build budget this run is held to, as of now.
    fn build_budget_secs(&self) -> u64 {
        match &self.budget {
            Some(budget) => budget.timeout_secs(crate::verification_budget::KIND_BUILD),
            None => self.build_timeout_secs,
        }
    }

    /// Attach the sink the builds report their cost to. One instance is shared
    /// by every surface that deploys through this deployer, so the reading
    /// covers the builds of the automatic cycle and the admin-triggered ones
    /// alike.
    pub fn with_build_readings(mut self, readings: Arc<EvolutionBuildReadings>) -> Self {
        self.build_readings = Some(readings);
        self
    }

    /// 最近一次构建的结局；这条部署器还没构建过就是 `None`。`None` 与
    /// 「构建过但结局未知」不是一回事，所以这里不拿某个默认结局顶上。
    pub fn last_build_ending(&self) -> Option<BuildEnding> {
        self.last_report
            .lock()
            .map(|slot| slot.map(|report| report.ending))
            .unwrap_or_default()
    }

    /// The peak the same build reported, when something could measure it.
    ///
    /// Read out of the same slot as the ending rather than a second one: the
    /// two are one attempt, and a reader pairing them must not be able to pair
    /// the ending of one build with the memory of another.
    pub fn last_build_peak_rss_bytes(&self) -> Option<u64> {
        self.last_report
            .lock()
            .map(|slot| slot.and_then(|report| report.peak_rss_bytes))
            .unwrap_or_default()
    }

    fn record_build(
        &self,
        change_id: &str,
        intent: Option<EvolutionIntent>,
        ending: BuildEnding,
        peak_rss_bytes: Option<u64>,
    ) {
        if let Ok(mut slot) = self.last_report.lock() {
            *slot = Some(BuildReport {
                ending,
                peak_rss_bytes,
            });
        }
        if let Some(readings) = &self.build_readings {
            readings.record(change_id, intent, ending, peak_rss_bytes);
        }
    }

    /// 这几次构建报给了哪一族读数；没挂就是 `None`，与「挂了但一次没构建过」
    /// 不是一件事，所以这里不拿一个空容器顶上。
    pub fn build_readings(&self) -> Option<&Arc<EvolutionBuildReadings>> {
        self.build_readings.as_ref()
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
        self.commit_and_build_in(change_id, &self.project_root, None)
            .await
    }

    /// 在指定工作树里提交并构建。每轮演进用自己的临时工作树，构建产物仍落
    /// 共享 target 目录。
    ///
    /// `intent` is which entry point the change came from, for the reading that
    /// splits build cost by change kind. The call site is the only place that
    /// knows it, and a change that arrives without one is recorded as
    /// unattributed rather than under a kind someone guessed.
    pub async fn commit_and_build_in(
        &self,
        change_id: &str,
        workdir: &Path,
        intent: Option<EvolutionIntent>,
    ) -> SFResult<BuildArtifact> {
        // The build slot is taken before the commit, not inside the build: a
        // refusal has to leave the tree untouched. Taken after the commit, a
        // host that was busy for the whole wait budget would end up in the
        // rollback arm below -- the change would be destroyed and the host's
        // load would be on its record.
        //
        // A refusal is that arm's whole error path, and it ends here: the gate
        // refuses with the one error it has, and nothing after this point can
        // produce it, so a refused build never reaches the rollback. It is
        // recorded as a build that never ran, which is what it was.
        let build_slot = match crate::build_gate::acquire("evolution build").await {
            Ok(slot) => slot,
            Err(e) => {
                warn!(
                    change_id = %change_id,
                    error = %e,
                    "Release build got no slot; commit kept, nothing judged"
                );
                self.record_build(change_id, intent, BuildEnding::Unstarted, None);
                return Err(e);
            }
        };

        info!(change_id = %change_id, "Committing evolution changes");
        self.git_add_all(workdir).await?;
        let commit_hash = self.git_commit(workdir, change_id).await?;

        info!(change_id = %change_id, "Building release binary");
        let start = Instant::now();
        let build_result = self
            .run_cargo_build(workdir, build_slot, change_id, intent)
            .await;
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
            // Everything that reaches here is a build that ran and did not
            // produce a binary. A refusal cannot: the slot was taken before the
            // commit, so a refused build never got this far.
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
        let message = self_evolution_commit_message(change_id);

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

        // Full hash: the artifact hash travels out of this process (landing
        // pins it as a ref, promotion republishes it), and an abbreviation is
        // only unambiguous against one object store at one point in time.
        let hash_output = tokio::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
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

    /// The slot is passed in rather than taken here: the build must run under a
    /// slot its caller already holds, and a caller that holds none cannot reach
    /// this function.
    async fn run_cargo_build(
        &self,
        workdir: &Path,
        _slot: Option<crate::build_gate::BuildPermit>,
        change_id: &str,
        intent: Option<EvolutionIntent>,
    ) -> SFResult<()> {
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args(["build", "--release", "--bin", &self.binary_name])
            .current_dir(workdir)
            .kill_on_drop(true);
        if let Some(target) = &self.target_dir {
            cmd.env("CARGO_TARGET_DIR", target);
        }
        let started = Instant::now();
        let budget_secs = self.build_budget_secs();
        // Spawned rather than handed to `output()`: the pid is what the memory
        // sampler watches, and there is no way to learn it from a call that
        // waits for the process to end. The pipes are attached here so the
        // failure text below is the build's own, and `kill_on_drop` keeps the
        // budget arm's kill: dropping the wait future drops the child.
        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                // cargo could not be spawned at all, so no build happened: same
                // class as a build the gate refused, and no duration is
                // reported for it.
                self.record_build(change_id, intent, BuildEnding::Unstarted, None);
                return Err(SFError::IO(format!("Failed to run cargo build: {}", e)));
            }
        };
        let watcher = child.id().map(|pid| {
            tokio::spawn(crate::build_rss_peak::watch(
                PathBuf::from("/proc"),
                pid,
                crate::build_rss_peak::SAMPLE_PERIOD,
            ))
        });
        let waited =
            tokio::time::timeout(Duration::from_secs(budget_secs), child.wait_with_output()).await;
        // Read before the arms below, because two of them return: the number
        // belongs to this build and there is no later point at which it could
        // be attributed to this one rather than to whatever runs next. The wait
        // is bounded — the sampler stops within one period of the tree going
        // away — so a process the kernel will not reap costs a reading, not the
        // build.
        let peak_rss_bytes = match watcher {
            Some(handle) => match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(peak)) => peak.map(|p| p.bytes),
                _ => None,
            },
            None => None,
        };
        // Handed to the gate before the arms below, because a build the budget
        // killed still measured what it took: the divisor is about what a build
        // costs, and a build that ran out of time cost that much regardless of
        // how it ended.
        if let Some(bytes) = peak_rss_bytes {
            crate::build_gate::observe_build_memory(bytes);
        }
        let output = match waited {
            Ok(result) => {
                let output = match result {
                    Ok(output) => output,
                    Err(e) => {
                        self.record_build(change_id, intent, BuildEnding::Unstarted, None);
                        return Err(SFError::IO(format!("Failed to run cargo build: {}", e)));
                    }
                };
                if let Some(budget) = &self.budget {
                    budget.record_run(
                        crate::verification_budget::KIND_BUILD,
                        started.elapsed().as_secs(),
                    );
                }
                output
            }
            Err(_) => {
                // An unbounded release build outlives the pod's own resources
                // long before it is useful; give up and let the caller roll the
                // commit back.
                if let Some(budget) = &self.budget {
                    budget.record_timeout(crate::verification_budget::KIND_BUILD);
                }
                // The host time this build spent is the budget it was cut off
                // at, and it is reported as such rather than as a measurement
                // of the work. What it took in memory before it was cut off is
                // still a real measurement, so it is reported.
                self.record_build(
                    change_id,
                    intent,
                    BuildEnding::TimedOut(Duration::from_secs(budget_secs)),
                    peak_rss_bytes,
                );
                return Err(SFError::IO(format!(
                    "cargo build --release exceeded the {}s deployment budget and was killed",
                    budget_secs
                )));
            }
        };

        if !output.status.success() {
            self.record_build(
                change_id,
                intent,
                BuildEnding::Failed(started.elapsed()),
                peak_rss_bytes,
            );
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::Agent(format!(
                "cargo build --release failed:\n{}{}",
                stdout, stderr
            )));
        }
        self.record_build(
            change_id,
            intent,
            BuildEnding::Built(started.elapsed()),
            peak_rss_bytes,
        );
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

/// The message a self-generated change lands under.
///
/// No `Co-Authored-By:` trailer: this repository forbids AI attribution in a
/// commit message, the author identity already says who wrote the commit, and
/// the subject already says it was self-generated -- a trailer would repeat
/// that in the one form the rule forbids. A trailer added here would live in
/// the history the release tag and every clone carry, where the only repair is
/// a rewrite, which is why the check happens at the message and not later.
///
/// The landing trailer is the opposite case and is required. It is the only
/// writer of the fact the change ledger reads back to answer "which commit
/// delivered this row": the reader walks the log for [`cog_core::LANDING_TRAILER`]
/// and records the first revision that names a handover id. Nothing else on the
/// production path emits one -- only fixtures did -- so the account held a
/// delivery column that could only ever be filled by hand. With the trailer
/// absent the column reads as "never delivered" for every change the platform
/// landed itself, which is indistinguishable from having landed none.
///
/// The id is safe to publish: it is the handover's own task id, a hash of the
/// change identity, so it names nothing the reader did not already hold. The
/// form is taken from the constant the reader matches on rather than written
/// out here, because a second spelling of the same trailer would be a reader and
/// a writer that agree today and are free to disagree later.
fn self_evolution_commit_message(change_id: &str) -> String {
    format!(
        "feat(evolution): apply self-generated change {change_id}\n\n{}{change_id}",
        cog_core::LANDING_TRAILER
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一个在构建阶段就睡着的 crate：`build.rs` 睡够，`cargo build --release`
    /// 必定超过一秒的预算，而慢的原因握在测试手里，与机器快慢无关。
    fn write_slow_build_crate(root: &std::path::Path) {
        std::fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"budget-probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
             \n[[bin]]\nname = \"budget-probe\"\npath = \"src/main.rs\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/main.rs"), "fn main() {}\n").unwrap();
        std::fs::write(
            root.join("build.rs"),
            "fn main() {\n    std::thread::sleep(std::time::Duration::from_secs(30));\n}\n",
        )
        .unwrap();
    }

    /// 部署侧那条界与测试侧是两条：`build_timeout_secs` 与 `test_timeout_secs`。
    /// 「超预算被杀、且错误里点名是哪条预算杀的」原先只在测试侧有实测（真跑一次
    /// `cargo test`），部署侧的界只剩「代码里写着」这一个证据——调这个旋钮时，
    /// 需要的正是「这个界真的在拦」的读数。
    ///
    /// 真跑一次 `cargo build --release`。杀掉的这个动作要留两处痕：错误里点名预算
    /// （否则事后读这条变更的人分不出「没判过」与「判失败」），结局记成 `TimedOut`
    /// 而不是「构建失败」（两者占用的宿主时间含义相反，合成一个数就没有哪个数可
    /// 用来定界）。
    ///
    /// 槽位传 `None`：这个测试量的是界本身，不是闸门；在测试里拿一个真槽位会把本
    /// 进程其它同样真跑构建的测试挤进排队。
    #[tokio::test]
    async fn a_release_build_that_outlives_its_budget_is_killed_with_the_budget_named() {
        let root = tempfile::tempdir().unwrap();
        write_slow_build_crate(root.path());

        let deployer = EvolutionDeployer::new(root.path(), root.path(), root.path())
            .with_binary_name("budget-probe")
            .with_build_timeout(1);

        let err = deployer
            .run_cargo_build(root.path(), None, "change-budget-probe", None)
            .await
            .expect_err("a build that cannot finish in a second must not be waited on");

        assert!(
            err.to_string()
                .contains("cargo build --release exceeded the 1s deployment budget"),
            "the rejection has to name the budget that caused it: {err}"
        );
        assert_eq!(
            deployer.last_build_ending(),
            Some(BuildEnding::TimedOut(Duration::from_secs(1))),
            "a build cut off at the budget is a timeout, not a failure of the change"
        );
    }

    /// The self-generated commit is the one this repository would otherwise
    /// make about itself in a form it forbids: the trailer is invisible until
    /// the commit is already in the history the release tag carries.
    #[test]
    fn a_self_generated_commit_carries_no_attribution_trailer() {
        let message = self_evolution_commit_message("change-abc123");
        assert!(
            message.starts_with("feat(evolution): apply self-generated change change-abc123"),
            "the subject has to name the change it applies: {message}"
        );
        let lowered = message.to_lowercase();
        for forbidden in ["co-authored-by", "generated with", "claude", "anthropic"] {
            assert!(
                !lowered.contains(forbidden),
                "a self-generated commit must not carry {forbidden:?}: {message}"
            );
        }
    }

    /// The other half: the message must carry the landing trailer, because
    /// nothing else on the production path writes one.
    ///
    /// Read the way the reading reads it -- a line whose trimmed text starts
    /// with the trailer constant, taking the next whitespace-delimited token as
    /// the id -- rather than substring-matching the id somewhere in the body.
    /// A message that mentioned the id only inside the subject would pass a
    /// substring check and still deliver nothing.
    #[test]
    fn a_self_generated_commit_names_the_change_it_lands() {
        let message = self_evolution_commit_message("change-ledger-367c97da26b1a416-3f9a2b1c");
        let named: Vec<&str> = message
            .lines()
            .filter_map(|line| line.trim().strip_prefix(cog_core::LANDING_TRAILER))
            .filter_map(|rest| rest.split_whitespace().next())
            .collect();
        assert_eq!(
            named,
            vec!["change-ledger-367c97da26b1a416-3f9a2b1c"],
            "the change ledger finds a delivery only through this trailer: {message}"
        );
    }

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
