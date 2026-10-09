//! Change application pipeline for L2 self-evolution.
//!
//! A refused build slot stays an error: the tests never ran, the change was never judged, and the caller retries it.
//! Responsibilities:
//! - Scan `change_dir` for `.diff` files (unified diff format).
//! - Validate every affected path: must exist, must live inside the workspace,
//!   and must not point to build/config/deployment files.
//! - Apply changes with `git apply`, run the configured test command (the
//!   default judges this Rust workspace), and roll back on failure.
//! - Report results by updating the evolution status.

use std::collections::{BTreeMap, BTreeSet};
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
    /// Whether the formatter had to rewrite this tree before the verdict was
    /// reached. Read from the verdict's own result rather than inferred from
    /// the evidence: it is the count of changes that arrived unformatted, which
    /// is a fact about the producer, and text meant to be read by a person is
    /// not a reading a counter can be derived from.
    pub reformatted: bool,
    /// How many tests this revision was already failing when the change was let
    /// through despite a failing run.
    ///
    /// Zero for every ordinary verdict, including a refused one: this counts
    /// only the runs where the suite failed and none of the failures belonged
    /// to the change. It is carried here for the same reason `reformatted` is —
    /// it is the count of a thing that happened, and a count read out of the
    /// failure text would be a number nobody could alert on. A run of changes
    /// with this above zero is the reading that a mainline is red, which is
    /// otherwise invisible: each change looks fine on its own.
    pub pre_existing_failures: usize,
}

/// What a failing whole-suite run says about the change that was applied.
///
/// The distinction is the whole point of reading the tree's own baseline: a
/// test that fails with and without the change says nothing about the change,
/// and convicting the change of it costs a generation that was already paid
/// for — and, on a tree that is red, convicts every change, including the one
/// that would turn it green.
#[derive(Debug)]
enum FailureAttribution {
    /// Every failing test fails on the untouched tree too.
    PreExisting(Vec<String>),
    /// These tests fail only once the change is applied.
    Introduced(Vec<String>),
    /// The run failed without naming a test — a build failure, or a harness
    /// that died. There is nothing to compare against the baseline, and the
    /// change is the only difference from it.
    NotATestFailure,
}

/// Which tests a revision fails with no change applied, and the revision they
/// were read at. Named because the pair travels through a lock and three
/// methods, and spelling it out each time hides which part is the reading.
type BaselineFailures = Option<(String, BTreeSet<String>)>;

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
    /// The command that judges a change, as argv. Comes from the deployment's
    /// `self_evolution.test_command`; the default judges a Rust workspace, so a
    /// project that is not one has to say so here rather than be judged by a
    /// command that cannot run in its tree.
    test_command: Vec<String>,
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
    /// Which tests fail on this workdir's revision with no change applied,
    /// keyed by the revision they were read at.
    ///
    /// Telling "this change broke a test" from "this test was already broken"
    /// takes a second run of the same suite on the untouched tree. That reading
    /// is a property of the tree, not of any one change, so it is kept until
    /// the revision moves: a tree that is already red costs one extra run, not
    /// one per change. Losing it to a restart costs time and nothing else —
    /// it is recomputed from the same tree.
    baseline_failures: Arc<tokio::sync::Mutex<BaselineFailures>>,
    /// 变更忠实度读数的去处。判定与读数同源：这个 oracle 就是这里的
    /// `git apply --check`，所以读数放在这里而不是生成侧——生成侧那份是同一
    /// 事实的第二份判据。缺席时读数只进日志，丢聚合不该让 apply 失败。
    metrics: Option<MetricsSink>,
}

/// 一个 metrics 句柄，连同给它写的 `Debug`。
///
/// `ChangePipeline` 派生 `Debug`，而 trait object 没有 `Debug`；这个包装把
/// 「有没有接读数」印成一个词，而不是把整个 sink 展开进日志行。
#[derive(Clone)]
struct MetricsSink(Arc<dyn cog_core::MetricsBackend>);

impl std::fmt::Debug for MetricsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MetricsSink(..)")
    }
}

/// The metadata record that sits beside a change's `.diff`.
///
/// The queue is a directory and keeps no memory of its own: every change's
/// status lived in a map a restart empties, so after a restart a `.diff` left
/// over from a change that had already been judged and a brand new one were the
/// same file, and the listing had to invent a status for both. The record is
/// what makes them two different files on disk, and unlike the map it survives
/// the process that wrote it. Shared by the writer (the engine, which knows the
/// artifact's metadata the moment it is produced) and the readers here, so the
/// path is spelled once.
pub(crate) fn change_record_path(dir: &Path, artifact_id: &str) -> PathBuf {
    dir.join(format!("{artifact_id}.json"))
}

/// The record of a change that has left the pending queue. Retired beside the
/// `.diff` it describes, so the pair moves together and neither can be read
/// alone.
pub(crate) fn retired_change_record_path(dir: &Path, artifact_id: &str) -> PathBuf {
    dir.join("retired").join(format!("{artifact_id}.json"))
}

/// Write a change's metadata record into `dir`.
///
/// Free rather than a method because the writer is the engine that produces the
/// change (it holds the artifact's metadata the moment it has it) and the
/// readers are here: both name the same path through
/// [`change_record_path`], and one of them is not a `ChangePipeline`.
/// Metadata only -- the artifact text stays where it is written.
pub(crate) async fn write_record_file(dir: &Path, result: &EvolutionResult) -> SFResult<()> {
    let path = change_record_path(dir, &result.artifact_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to create change dir {}: {}",
                parent.display(),
                e
            ))
        })?;
    }
    let mut record = result.clone();
    record.content.clear();
    let text = serde_json::to_string(&record).map_err(SFError::Serialization)?;
    tokio::fs::write(&path, text).await.map_err(|e| {
        SFError::IO(format!(
            "Failed to write change record {}: {}",
            path.display(),
            e
        ))
    })
}

/// The timestamp a change carries when no record states one.
///
/// A `.diff` written before the record mechanism existed has no creation time
/// anywhere, and the alternatives are both worse than saying so: a timestamp
/// taken from the file's own metadata is a claim the filesystem, not the
/// change, is making (a copy or a restore rewrites it), and the moment of the
/// read is a claim that changes every pass and orders the listing by when it
/// was looked at. The minimum representable instant is not a time any change
/// was made at; it sorts such a change behind every one whose record does
/// state a time, which is the only ordering claim that follows from "no record
/// knows".
pub(crate) const UNKNOWN_CREATED_AT: chrono::DateTime<chrono::Utc> =
    chrono::DateTime::<chrono::Utc>::MIN_UTC;

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
            test_command: cog_core::SelfEvolutionConfig::default().test_command,
            fmt_timeout_secs: DEFAULT_FMT_TIMEOUT_SECS,
            promotion_policy: None,
            target_dir: None,
            budget: None,
            baseline_failures: Arc::new(tokio::sync::Mutex::new(None)),
            metrics: None,
        }
    }

    /// Where the per-artifact fidelity readings go.
    pub fn with_metrics(mut self, metrics: Arc<dyn cog_core::MetricsBackend>) -> Self {
        self.metrics = Some(MetricsSink(metrics));
        self
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

    /// The deployment's test command. Taken from the config where the pipeline
    /// is built, not defaulted here, so the command recorded in a change's
    /// evidence is the one the deployment configured.
    pub fn with_test_command(mut self, command: Vec<String>) -> Self {
        self.test_command = command;
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
    ///
    /// Each change's state comes from the record beside it, which is the copy a
    /// restart cannot lose; the resident index is consulted first only because
    /// it is the same information without a read, and it is written through to
    /// the record so the two cannot say different things. A `.diff` with no
    /// record is not read as `CompileChecked`: nothing here knows what happened
    /// to it, and calling it compiled is what let a change nobody had judged
    /// look like one that had passed. Its status is the honest unknown, which
    /// is still verified, and the verification's conclusion is written back so
    /// the state is answered rather than left standing.
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
            // generated for, and holding it in memory is the same fact as the
            // record on disk; the record is what answers when the engine does
            // not know the change at all.
            let resident = match engine {
                Some(engine) => engine.get_result(&artifact_id).await,
                None => None,
            };
            let record = match resident {
                Some(record) => Some(record),
                None => self.read_change_record(&artifact_id).await,
            };

            let (status, description, created_at) = match record {
                Some(record) => (
                    record.status,
                    if record.description.trim().is_empty() {
                        format!("Code change from {}", path.display())
                    } else {
                        record.description
                    },
                    record.created_at,
                ),
                // A `.diff` written before the record existed. Nothing states
                // when it was made, so nothing here claims a time for it: the
                // sentinel says "not known" rather than inventing an instant
                // from the file's own metadata.
                None => (
                    EvolutionStatus::Unrecorded,
                    format!("Code change from {}", path.display()),
                    UNKNOWN_CREATED_AT,
                ),
            };

            if !matches!(
                status,
                EvolutionStatus::CompileChecked
                    | EvolutionStatus::AwaitingReview
                    | EvolutionStatus::Unrecorded
            ) {
                continue;
            }

            results.push(EvolutionResult {
                kind: EvolutionKind::CodeChange,
                artifact_id,
                description,
                content,
                status,
                created_at,
                eval_summary: None,
            });
        }

        results.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(results)
    }

    /// Write a change's metadata record beside its `.diff`.
    ///
    /// The record is the durable half of what the resident index holds: the
    /// index is per-process and a restart empties it, so a change whose whole
    /// state lives there stops existing the moment the process does.
    pub async fn write_change_record(&self, result: &EvolutionResult) -> SFResult<()> {
        write_record_file(&self.change_dir, result).await
    }

    /// Read a change's metadata record, from the pending queue or `retired/`.
    ///
    /// `retired/` is searched second for the same reason the artifact text is:
    /// that is where a change goes once it lands or is refused, and its record
    /// moves with it. A record that cannot be parsed is reported as absent
    /// rather than as a record with defaulted fields -- a record whose contents
    /// are unreadable states nothing, and defaulting it would put a fabricated
    /// status back in the one place built to stop that.
    pub async fn read_change_record(&self, artifact_id: &str) -> Option<EvolutionResult> {
        let pending = change_record_path(&self.change_dir, artifact_id);
        if let Ok(text) = tokio::fs::read_to_string(&pending).await {
            if let Ok(record) = serde_json::from_str(&text) {
                return Some(record);
            }
        }
        let retired = retired_change_record_path(&self.change_dir, artifact_id);
        tokio::fs::read_to_string(retired)
            .await
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
    }

    /// Every change record this queue can see, pending and retired, newest
    /// first.
    ///
    /// This is the durable face a listing is built from. A change's record
    /// outlives the process that produced it and stays with the artifact after
    /// it leaves the queue, so a listing that reads it does not lose a change
    /// when the resident index that used to carry it drops the entry.
    pub async fn known_change_records(&self) -> Vec<EvolutionResult> {
        let mut records: Vec<EvolutionResult> = Vec::new();
        for dir in [self.change_dir.clone(), self.change_dir.join("retired")] {
            let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
                continue;
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                if let Ok(text) = tokio::fs::read_to_string(&path).await {
                    if let Ok(record) = serde_json::from_str(&text) {
                        records.push(record);
                    }
                }
            }
        }
        records.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        records
    }

    /// Whether a change's artifact has already left the pending queue.
    ///
    /// The condition a resident record may be dropped under. Dropping a record
    /// is only safe once the artifact is out of the queue and its own record is
    /// beside it: until then the index is the last thing holding that change's
    /// state, and a drop would take the state with it.
    pub fn artifact_is_retired(&self, artifact_id: &str) -> bool {
        self.change_dir
            .join("retired")
            .join(format!("{artifact_id}.diff"))
            .exists()
            && retired_change_record_path(&self.change_dir, artifact_id).exists()
    }

    /// Read a change's diff text back from disk by `artifact_id`.
    ///
    /// The engine's resident index deliberately keeps no artifact text (see
    /// `resident_record`), so a reader that has an id but no content -- the
    /// admin listing, for a change that has already left the pending queue --
    /// reads it here instead. `retired/` is searched second because that is
    /// where the queue puts every change that lands or is refused, and those
    /// are exactly the ones the pending scan no longer returns.
    pub async fn read_change_content(&self, artifact_id: &str) -> Option<String> {
        let pending = self.change_dir.join(format!("{artifact_id}.diff"));
        if let Ok(text) = tokio::fs::read_to_string(&pending).await {
            return Some(text);
        }
        let retired = self
            .change_dir
            .join("retired")
            .join(format!("{artifact_id}.diff"));
        tokio::fs::read_to_string(retired).await.ok()
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

        // The record moves with the change it describes. Left behind, it would
        // keep a retired change in the pending listing, and the pair would
        // disagree about which queue the change is in. A record that cannot
        // move is not fatal to the retirement -- the artifact is out of the
        // queue, which is what retirement is for -- but it is what
        // `artifact_is_retired` refuses a record drop over, so it is named.
        let record_from = change_record_path(&self.change_dir, artifact_id);
        if record_from.exists() {
            let record_to = retired_change_record_path(&self.change_dir, artifact_id);
            if let Err(e) = tokio::fs::rename(&record_from, &record_to).await {
                warn!(
                    change_id = %artifact_id,
                    error = %e,
                    "Change retired but its record could not be moved with it"
                );
            }
        }

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
                    reformatted: false,
                    pre_existing_failures: 0,
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
                    reformatted: false,
                    pre_existing_failures: 0,
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
                    reformatted: false,
                    pre_existing_failures: 0,
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
                reformatted: false,
                pre_existing_failures: 0,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::IntentMismatch),
                test_output: format!("Change does not answer its goal: {reason}"),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        self.ensure_clean_workspace(workdir).await?;

        let applies = self.git_apply_check(workdir, &change.content).await;
        // 读数在判词之后、早退之前：这一步的 oracle 与下面这道门是同一个
        // `git apply --check`，所以读数说的是「这份 diff 与目标树有多契合」，
        // 而不是另一套判定。
        self.report_fidelity(workdir, &change.content, applies.is_ok())
            .await;
        if let Err(e) = applies {
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                reformatted: false,
                pre_existing_failures: 0,
                verdict: ChangeVerdict::Refused(apply_failure_cause(&e.to_string())),
                test_output: format!("Change pre-check failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        // Whether this change's diff will be the only difference between the
        // tree the linter reads and its baseline.
        //
        // Read here, while the tree still is the baseline, because after the
        // change is applied the two writers are indistinguishable: the formatter
        // conforms the whole tree rather than only the change, so on a baseline
        // it would rewrite, the worktree diff carries that rewrite as well as
        // the change, and a lint sitting on a line the formatter rewrote would
        // be read as this change's — which is the one reading the criterion
        // exists to avoid. Nothing in the tree says which of the two wrote a
        // line after the fact, so the question is asked while it can still be
        // answered.
        let baseline_is_conformed = match self
            .run_cargo_fmt_cmd(workdir, &["fmt", "--all", "--", "--check"])
            .await
        {
            Ok((clean, _)) => clean,
            Err(e) => {
                warn!(
                    change_id = %change.artifact_id,
                    error = %e,
                    "Could not read whether this tree's baseline is what the formatter produces"
                );
                false
            }
        };

        if let Err(e) = self.git_apply(workdir, &change.content).await {
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                reformatted: false,
                pre_existing_failures: 0,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::ApplyFailed),
                test_output: format!("Change application failed: {}", e),
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        // A change whose only effect is a default no deployment reads is refused
        // here, before the formatter and the linter, because both of those are
        // whole-tree compiles: the round this criterion saves is the one that
        // would have spent them proving a value nothing can reach.
        //
        // The worktree diff against `HEAD` is the change and nothing else at
        // this point — the formatter has not run yet, so it has not rewritten
        // the baseline into the same diff.
        if let Some(reason) = self
            .unreachable_default_reason(workdir, &change.content)
            .await
        {
            warn!(
                change_id = %change.artifact_id,
                "Change writes only defaults every shipped document overrides"
            );
            let _ = self.git_reset_hard(workdir).await;
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                reformatted: false,
                pre_existing_failures: 0,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::UnreachableDefault),
                test_output: reason,
                new_status: EvolutionStatus::ValidationFailed,
            });
        }

        // Conformed before it is compiled, and rolled back either way: a change
        // refused here must leave the tree as it found it, or the next run would
        // be verifying this one's leftovers.
        let reformatted = match self.run_cargo_fmt(workdir).await {
            Ok((true, reformatted, _)) => reformatted,
            Ok((false, _, output)) => {
                warn!(
                    change_id = %change.artifact_id,
                    "Change is not what this workspace's formatter produces, and conforming it did not settle that; rolling back"
                );
                let _ = self.git_reset_hard(workdir).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::FormattingDiffers),
                    test_output: format!(
                        "The formatter could not make this tree what it produces:\n{output}"
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
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::TestRunUnavailable),
                    test_output: format!("Failed to execute cargo fmt: {}", e),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
        };

        // This tree's lints, minus the ones it already carried. Read before the
        // suite so a change the linter refuses is refused without also paying
        // for the suite: the linter is a whole-tree compile of its own — cargo
        // does not hand clippy's artifacts to the test run, they are built with
        // a wrapper — so the only place the stage can still save that second
        // compile is here, by refusing before it is spent.
        //
        // What is given up is the criterion's reach, not its evidence — a lint
        // on a line this change did not write is the tree's, and a tree that
        // does not compile is the suite's to answer for. Both are recorded
        // where the decision is made rather than left to be inferred from a
        // green run.
        let clippy_note = match self.run_cargo_clippy(workdir).await {
            Ok((true, _)) => "cargo clippy --workspace: no diagnostics on this tree".to_string(),
            Ok((false, output)) => {
                // A change is answerable for the lints reported on a line it
                // wrote, and that reading needs a diff with one writer in it.
                // Where the baseline is not what the formatter produces, the
                // tree the linter read differs from its baseline by the
                // formatter's rewrite of that baseline as well, and no line can
                // be told from the other: nothing is attributed, and the note
                // says which of the two readings it is instead of reporting the
                // tree's lints as this change's.
                let mut written: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
                if baseline_is_conformed {
                    written = match self.applied_change_lines(workdir, &change.content).await {
                        Ok(written) => written,
                        Err(e) => {
                            // Without the lines the change wrote there is nothing to
                            // attribute against, and a verdict read off the raw
                            // report would convict this change of the tree's lints.
                            // No verdict keeps it for an attempt that can read them.
                            warn!(
                                change_id = %change.artifact_id,
                                error = %e,
                                "Could not read the lines this change wrote; reaching no verdict"
                            );
                            let _ = self.git_reset_hard(workdir).await;
                            return Err(e);
                        }
                    };
                }
                let attribution =
                    cog_core::contract::reflection::introduced_lints(&written, &output);
                if !attribution.introduced.is_empty() {
                    warn!(
                        change_id = %change.artifact_id,
                        count = attribution.introduced.len(),
                        "Change writes lines the linter reports on"
                    );
                    let evidence = attribution
                        .introduced
                        .iter()
                        .map(|lint| {
                            format!(
                                "{}:{} {} — {}",
                                lint.file, lint.line, lint.code, lint.message
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n  ");
                    let _ = self.git_reset_hard(workdir).await;
                    return Ok(ApplyResult {
                        change_id: change.artifact_id.clone(),
                        files_changed,
                        reformatted,
                        pre_existing_failures: 0,
                        verdict: ChangeVerdict::Refused(cog_core::RejectionCause::LintIntroduced),
                        test_output: format!(
                            "The change writes lines the linter reports on:\n  {evidence}\n\n{output}"
                        ),
                        new_status: EvolutionStatus::ValidationFailed,
                    });
                }
                if !baseline_is_conformed {
                    info!(
                        change_id = %change.artifact_id,
                        diagnostics = attribution.diagnostics,
                        "The linter reports this tree's lints, and the tree it read differs from its baseline beyond this change"
                    );
                    format!(
                        "cargo clippy --workspace: {} diagnostic(s) on this tree, which differs from its baseline by more than this change (the baseline is not what the formatter produces), so none is read as this change's",
                        attribution.diagnostics
                    )
                } else {
                    info!(
                        change_id = %change.artifact_id,
                        diagnostics = attribution.diagnostics,
                        "The linter reports this tree's lints; none of them is on a line this change wrote"
                    );
                    format!(
                        "cargo clippy --workspace: {} diagnostic(s) on this tree, none of them on a line this change wrote",
                        attribution.diagnostics
                    )
                }
            }
            Err(e) if e.is_build_slot_refused() => {
                warn!(change_id = %change.artifact_id, error = %e, "cargo clippy got no build slot");
                let _ = self.git_reset_hard(workdir).await;
                return Err(e);
            }
            Err(e) => {
                // A linter that reached no verdict is not a change that failed
                // one: the criterion is one of several, and retiring a sound
                // change for a judge that never spoke is the same mistake as
                // reading a busy host as a defect. What it did is recorded in
                // the change's own evidence rather than only in a log line, so
                // that "found no lint" and "no linter ran" are not one entry.
                warn!(
                    change_id = %change.artifact_id,
                    error = %e,
                    "cargo clippy did not run; this change is judged without it"
                );
                format!("cargo clippy --workspace: not evaluated ({e})")
            }
        };

        let (test_passed, test_output) = match self.run_test_command(workdir).await {
            Ok(result) => result,
            // No slot for the whole wait budget: the tests never ran, so nothing
            // about the change was judged. This stays an `Err` -- the caller
            // treats an error as "no verdict yet" and keeps the change, while a
            // `Refused` retires it. Folding a busy host in here would retire a
            // sound change for the load the machine happened to be under.
            Err(e) if e.is_build_slot_refused() => {
                warn!(change_id = %change.artifact_id, error = %e, command = %self.test_command_line(), "the test command got no build slot");
                let _ = self.git_reset_hard(workdir).await;
                return Err(e);
            }
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, command = %self.test_command_line(), "the test command failed to execute");
                let _ = self.git_reset_hard(workdir).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::TestRunUnavailable),
                    test_output: format!("Failed to execute {}: {}", self.test_command_line(), e),
                    new_status: EvolutionStatus::ValidationFailed,
                });
            }
        };

        // A whole-suite run also fails for reasons this revision was already
        // carrying, and those are not the change's doing. Only read the tree's
        // own baseline when the run failed with tests named, so a green tree
        // never pays for it and the cost lands on the runs that need the
        // distinction.
        let mut test_passed = test_passed;
        // What the linter did rides in front of what the suite did: this field
        // is the record a refused or landed change is read from, and a run that
        // was not evaluated has to be readable as that rather than as silence.
        let mut test_output = if clippy_note.is_empty() {
            test_output
        } else {
            format!("{clippy_note}\n\n{test_output}")
        };
        let mut pre_existing_failures = 0usize;
        if !test_passed {
            match self
                .failures_beyond_baseline(workdir, &change.content, &test_output)
                .await
            {
                Ok(FailureAttribution::PreExisting(already)) => {
                    warn!(
                        change_id = %change.artifact_id,
                        count = already.len(),
                        "Change adds no failure: this revision fails these with or without it"
                    );
                    pre_existing_failures = already.len();
                    test_output = format!(
                        "The change adds no failure: this revision fails these with or without it:\n  {}\n\n{test_output}",
                        already.join("\n  ")
                    );
                    test_passed = true;
                }
                Ok(FailureAttribution::Introduced(introduced)) => {
                    warn!(
                        change_id = %change.artifact_id,
                        count = introduced.len(),
                        "Change fails tests this revision was passing before it was applied"
                    );
                    test_output = format!(
                        "The change fails tests that this revision passed before it was applied:\n  {}\n\n{test_output}",
                        introduced.join("\n  ")
                    );
                }
                // Nothing to attribute: the change stays the only difference
                // from the baseline, so the refusal stands as it did before.
                Ok(FailureAttribution::NotATestFailure) => {}
                Err(e) => {
                    // The baseline run took the change off the tree and could
                    // not put it back, so there is no tree left to judge and no
                    // verdict to reach. An `Err` keeps the change for a later
                    // attempt; a `Refused` would retire it over a failure to
                    // read something that was not about the change at all.
                    warn!(
                        change_id = %change.artifact_id,
                        error = %e,
                        "Could not read the tests this revision fails on its own; reaching no verdict"
                    );
                    let _ = self.git_reset_hard(workdir).await;
                    return Err(e);
                }
            }
        }

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
            reformatted,
            pre_existing_failures,
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

    /// 逐文件/逐块读出这份 diff 有多少真的在目标树里找到上下文，并上报。
    ///
    /// oracle 是本模块的 `git apply --check`：决定变更能否落地的那个判定，
    /// 与决定这条读数是什么的那个判定必须是同一个，否则读数可以绿着而门是红的。
    /// 上报摆在这里而不是生成侧，是因为每条变更真正被判定 apply 的地方在这里；
    /// 生成侧那份是同一事实的第二份判据。
    async fn report_fidelity(&self, root: &Path, diff: &str, whole_patch_ok: bool) {
        let fidelity = crate::diff_fidelity::measure(root, diff, whole_patch_ok).await;
        // 没解析出任何文件段落＝没东西可测：这条变更根本没走到门，缺席不是零。
        if fidelity.is_empty() {
            return;
        }
        info!(
            files_total = fidelity.files_total,
            files_faithful = fidelity.files_faithful,
            hunks_total = fidelity.hunks_total,
            hunks_faithful = fidelity.hunks_faithful,
            "generated change fidelity"
        );
        let Some(metrics) = self.metrics.as_ref().map(|sink| &sink.0) else {
            return;
        };
        for (name, value) in [
            (
                cog_core::metric_names::EVOLUTION_GENERATED_CHANGE_FILES_TOTAL,
                fidelity.files_total as f64,
            ),
            (
                cog_core::metric_names::EVOLUTION_GENERATED_CHANGE_FILES_FAITHFUL,
                fidelity.files_faithful as f64,
            ),
            (
                cog_core::metric_names::EVOLUTION_GENERATED_CHANGE_HUNKS_TOTAL,
                fidelity.hunks_total as f64,
            ),
            (
                cog_core::metric_names::EVOLUTION_GENERATED_CHANGE_HUNKS_FAITHFUL,
                fidelity.hunks_faithful as f64,
            ),
        ] {
            if let Err(e) = metrics
                .record_counter(name, value, std::collections::HashMap::new())
                .await
            {
                warn!(metric = name.as_str(), error = %e, "could not record change fidelity");
            }
        }
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

    /// Judge this tree's formatting, conforming it when the formatter would
    /// rewrite it: returns (judgeable, rewritten, combined_output).
    ///
    /// A file the formatter rewrites is rewritten here rather than sent back.
    /// The commit a change lands as is the tree the tests ran against, so
    /// conforming it first is what keeps CI's format check green — and the
    /// alternative costs a whole generation round rather than one reformat:
    /// the producer is a generator that does not carry the formatter's rules,
    /// so an unformatted change returned to it comes back unformatted. The
    /// rewrite is deterministic, parses without compiling, and takes no build
    /// slot, so paying it here is cheaper than any round it saves.
    ///
    /// A rewrite is not a way to land anything: the check runs again after it,
    /// and a tree the formatter still rewrites is refused. That is the state a
    /// change with a syntax error produces, and it is a different fact from one
    /// that merely arrived unformatted — the second is what the rewrite exists
    /// for, and only the first is a defect in the change.
    ///
    /// Two questions before the first check, because a single exit code cannot
    /// answer both. `cargo fmt --check` exits 1 for a file it would rewrite — and
    /// exits 1 just as well when the toolchain it was told to use is not
    /// installed, which is reachable here: `RUSTUP_TOOLCHAIN` is in the
    /// passthrough set. Reading those as one answer would leave every change a
    /// formatter-less deployment was asked to verify unrewritten and refused,
    /// which is a change refused for being unverifiable rather than for being
    /// wrong. So the probe goes first and settles whether there is a formatter
    /// to ask at all; only after that does a nonzero exit mean the tree is what
    /// the formatter rewrites.
    async fn run_cargo_fmt(&self, workdir: &Path) -> SFResult<(bool, bool, String)> {
        let (available, why) = self
            .run_cargo_fmt_cmd(workdir, &["fmt", "--version"])
            .await?;
        if !available {
            return Err(SFError::IO(format!(
                "the format check has no formatter to ask: {why}"
            )));
        }
        info!("Running cargo fmt --all -- --check");
        let (clean, checked) = self
            .run_cargo_fmt_cmd(workdir, &["fmt", "--all", "--", "--check"])
            .await?;
        if clean {
            return Ok((true, false, checked));
        }
        info!("Conforming the change to what this workspace's formatter produces");
        let (ran, rewrite_output) = self.run_cargo_fmt_cmd(workdir, &["fmt", "--all"]).await?;
        let evidence = format!("{checked}\n{rewrite_output}");
        if !ran {
            return Ok((false, false, evidence));
        }
        let (conformed, rechecked) = self
            .run_cargo_fmt_cmd(workdir, &["fmt", "--all", "--", "--check"])
            .await?;
        let evidence = format!("{evidence}\n{rechecked}");
        Ok(if conformed {
            (true, true, evidence)
        } else {
            (false, false, evidence)
        })
    }

    /// Run the configured test command and return (success, combined_output).
    ///
    /// The command is the deployment's, not this crate's: a project that is not
    /// a Rust workspace is judged by its own suite, and the default is the one
    /// every deployment ran while this was hardcoded.
    ///
    /// An empty command is refused rather than treated as "nothing to run":
    /// accepting a change because no criterion was reachable is the one reading
    /// of a missing command that must not be available, and it is the reading a
    /// typo in a config file would produce.
    async fn run_test_command(&self, workdir: &Path) -> SFResult<(bool, String)> {
        let Some(program) = self.test_command.first() else {
            return Err(SFError::Config(
                "self_evolution.test_command is empty: no command would judge this change".into(),
            ));
        };
        // The heaviest build this host runs, so it is the one the slot exists
        // for. Bound to a named variable, not `_`: an underscore drops the
        // permit on the spot and would bound nothing at all.
        let _slot = cog_core::build_gate::acquire("change verification").await?;
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(&self.test_command[1..])
            .current_dir(workdir)
            .kill_on_drop(true);
        self.apply_verification_env(&mut cmd);

        let started = Instant::now();
        let output =
            match tokio::time::timeout(Duration::from_secs(self.test_timeout_secs), cmd.output())
                .await
            {
                Ok(result) => {
                    let output = result.map_err(|e| {
                        SFError::IO(format!("Failed to run {}: {}", self.test_command_line(), e))
                    })?;
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
                        "{} exceeded the {}s verification budget and was killed",
                        self.test_command_line(),
                        self.test_timeout_secs
                    )));
                }
            };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{}{}", stdout, stderr);
        Ok((output.status.success(), combined))
    }

    /// The configured test command as it is written to the change's evidence.
    ///
    /// This is what makes the config item readable: the record left behind by a
    /// rejection names the command that judged it, so a deployment running a
    /// command other than the default can be told from one that is not.
    fn test_command_line(&self) -> String {
        self.test_command.join(" ")
    }

    /// Run CI's lint command over the workspace and return (succeeded, output).
    ///
    /// The same command the gate runs, in the one form that lets the verdict be
    /// attributed at all: `--message-format=json` changes what the run says,
    /// not what it finds, and leaves the exit status alone.
    ///
    /// Bounded by the test budget rather than by a knob of its own. It is a
    /// whole-workspace compile of the same scale, on the same tree, that budget
    /// already bounds, so one number covers both jobs, and a second knob would
    /// have to be set to the same value to be right — one more way for a
    /// deployment to be configured into a criterion that refuses everything or
    /// nothing.
    ///
    /// Nothing is recorded in the budget sink under its own name: the sink's
    /// kinds are the two budgets, and writing a third job's duration into the
    /// `test` kind would make that reading mean "the last workspace compile of
    /// either job" while still claiming to be one kind.
    async fn run_cargo_clippy(&self, workdir: &Path) -> SFResult<(bool, String)> {
        info!("Running cargo clippy --workspace");
        let _slot = cog_core::build_gate::acquire("change lint verification").await?;
        let mut cmd = tokio::process::Command::new("cargo");
        cmd.args([
            "clippy",
            "--workspace",
            "--message-format=json",
            "--",
            "-D",
            "warnings",
        ])
        .current_dir(workdir)
        .kill_on_drop(true);
        self.apply_verification_env(&mut cmd);

        let output =
            match tokio::time::timeout(Duration::from_secs(self.test_timeout_secs), cmd.output())
                .await
            {
                Ok(result) => {
                    result.map_err(|e| SFError::IO(format!("Failed to run cargo clippy: {}", e)))?
                }
                Err(_) => {
                    return Err(SFError::IO(format!(
                        "cargo clippy exceeded the {}s verification budget and was killed",
                        self.test_timeout_secs
                    )))
                }
            };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok((output.status.success(), format!("{}{}", stdout, stderr)))
    }

    /// The lines the applied change wrote, read from the tree that will land.
    ///
    /// The artifact's own line numbers are the numbers of a tree that no longer
    /// exists by the time the linter speaks: the formatter conforms the tree
    /// first, and a rewrite that splits one written line into two moves every
    /// line after it. A reader holding compiled spans against the artifact's
    /// numbering would then attribute findings to lines the change never wrote
    /// — and, where the shift went the other way, miss the ones it did.
    ///
    /// Read this way the diff has one writer only as long as the baseline is
    /// what the formatter produces: on a baseline it would rewrite, the formatter
    /// puts its rewrite of that baseline into the same diff, and a line the
    /// change never wrote arrives here as one it did. That is the caller's to
    /// establish before it reads a verdict off this — see the check the stage
    /// runs while the tree still is the baseline. This function cannot tell the
    /// two writers apart and does not try to.
    ///
    /// A file the change creates is untracked, and a worktree diff does not
    /// list those. The change itself says which files it creates, and for those
    /// the whole file is what it wrote.
    async fn applied_change_lines(
        &self,
        workdir: &Path,
        change_content: &str,
    ) -> SFResult<BTreeMap<String, BTreeSet<u64>>> {
        let diff = self
            .git_try(workdir, &["diff", "HEAD"])
            .await
            .ok_or_else(|| {
                SFError::IO(format!(
                    "could not read the applied change from {}",
                    workdir.display()
                ))
            })?;
        let mut written = cog_core::contract::reflection::diff_added_lines(&diff);

        for target in cog_core::parse_diff_targets(change_content) {
            if target.kind != cog_core::DiffTargetKind::Create {
                continue;
            }
            let Ok(text) = tokio::fs::read_to_string(workdir.join(&target.path)).await else {
                continue;
            };
            let lines: BTreeSet<u64> = (1..=text.lines().count() as u64).collect();
            written.insert(target.path, lines);
        }
        Ok(written)
    }

    /// The refusal this change earns for writing only defaults nothing reads,
    /// `None` when it has an effect somewhere.
    ///
    /// Two documents stand for the deployment's configuration here:
    /// `cogneva.example.json` and the chart's own `cogneva.json`. Those are the
    /// pair the configuration surface is already judged on, and `deploy/k3s` /
    /// `deploy/rendered` are held to the chart by the parity gate rather than
    /// carrying keys of their own. A document that cannot be read or parsed, or
    /// a written file that cannot be read back, answers `None`: the criterion
    /// is "every document writes the key", and an unread document has not been
    /// shown to. Reading no documents is not the same finding as reading them
    /// and not finding the key, and only the second one refuses a change.
    async fn unreachable_default_reason(
        &self,
        workdir: &Path,
        change_content: &str,
    ) -> Option<String> {
        const DOCUMENTS: &[&str] = &[
            "cogneva.example.json",
            "deploy/helm/cogneva/files/cogneva.json",
        ];
        let mut documents = Vec::with_capacity(DOCUMENTS.len());
        for path in DOCUMENTS {
            let text = tokio::fs::read_to_string(workdir.join(path)).await.ok()?;
            documents.push(serde_json::from_str(&text).ok()?);
        }

        let written = self
            .applied_change_lines(workdir, change_content)
            .await
            .ok()?;
        let mut sources = BTreeMap::new();
        for file in written.keys() {
            let text = tokio::fs::read_to_string(workdir.join(file)).await.ok()?;
            sources.insert(file.clone(), text);
        }

        let unreachable =
            cog_core::contract::reflection::unreachable_defaults(&written, &sources, &documents);
        if unreachable.is_empty() {
            return None;
        }
        let named = unreachable
            .iter()
            .map(|d| format!("{}:{} {}", d.file, d.line, d.key))
            .collect::<Vec<_>>()
            .join("\n  ");
        Some(format!(
            "Every line this change writes is a literal in an `impl Default for *Config`, \
             and every configuration document this deployment ships writes that key \
             ({}) — the document's value is the one the process reads, so these defaults \
             are unreachable and the change has no effect:\n  {named}",
            DOCUMENTS.join(", ")
        ))
    }

    /// Split a failing run's tests into the ones this revision was already
    /// failing and the ones it was passing.
    ///
    /// Only the second group is evidence about the change. The first is
    /// evidence about the tree, and it is what a red mainline looks like from
    /// inside one change's verification: every change inherits the same
    /// failures, so convicting on them retires every change and the chain
    /// stops — including the change that would have repaired the tree.
    async fn failures_beyond_baseline(
        &self,
        workdir: &Path,
        change_content: &str,
        output: &str,
    ) -> SFResult<FailureAttribution> {
        let failing = cog_core::contract::reflection::failing_tests(output);
        // A run can fail without naming a test — a compile error, a dead
        // harness. There is no baseline that names tests to compare against,
        // and the change is the only difference from the tree the baseline was
        // read on, so it stays the change's to answer for.
        if failing.is_empty() {
            return Ok(FailureAttribution::NotATestFailure);
        }
        let baseline = self.baseline_failing_tests(workdir, change_content).await?;
        let introduced: Vec<String> = failing.difference(&baseline).cloned().collect();
        if introduced.is_empty() {
            Ok(FailureAttribution::PreExisting(
                failing.into_iter().collect(),
            ))
        } else {
            Ok(FailureAttribution::Introduced(introduced))
        }
    }

    /// The tests this workdir's revision fails with no change applied.
    ///
    /// Read on the same tree, at the same revision, with the change taken back
    /// off — not from the base revision the change was generated against. The
    /// question is what *this* tree does on its own, and the tree is what the
    /// change is about to be committed onto.
    ///
    /// The change is put back exactly as this run found it, formatter and all:
    /// the tree being judged is the conformed one, and re-applying the raw
    /// content would silently undo the formatting this same run just applied
    /// and commit a tree the format check rejects.
    async fn baseline_failing_tests(
        &self,
        workdir: &Path,
        change_content: &str,
    ) -> SFResult<BTreeSet<String>> {
        let rev = self.workspace_rev(workdir).await?;
        if let Some((cached_rev, tests)) = self.baseline_failures.lock().await.as_ref() {
            if *cached_rev == rev {
                return Ok(tests.clone());
            }
        }

        self.git_reset_hard(workdir).await?;
        let (_, baseline_output) = self.run_test_command(workdir).await?;

        self.git_apply(workdir, change_content).await.map_err(|e| {
            SFError::IO(format!(
                "could not put the change back after reading the tree's own test failures: {e}"
            ))
        })?;
        match self.run_cargo_fmt(workdir).await {
            Ok((true, _, _)) => {}
            Ok((false, _, output)) => {
                return Err(SFError::IO(format!(
                    "could not conform the change to the formatter after reading the tree's own test failures:\n{output}"
                )));
            }
            Err(e) => {
                return Err(SFError::IO(format!(
                    "could not run the formatter after reading the tree's own test failures: {e}"
                )));
            }
        }

        let tests = cog_core::contract::reflection::failing_tests(&baseline_output);
        info!(
            rev = %rev,
            count = tests.len(),
            "Read the tests this revision fails before any change is applied"
        );
        *self.baseline_failures.lock().await = Some((rev, tests.clone()));
        Ok(tests)
    }

    /// The revision of the workspace being verified.
    async fn workspace_rev(&self, workdir: &Path) -> SFResult<String> {
        self.git_try(workdir, &["rev-parse", "HEAD"])
            .await
            .ok_or_else(|| {
                SFError::IO(format!(
                    "could not read the revision of the workspace at {}",
                    workdir.display()
                ))
            })
    }
}

/// Which defect a failed `git apply --check` reported.
///
/// The check answers with one exit status, but it names two different defects
/// on stderr: a patch whose hunk headers do not add up never was a unified
/// diff, while a patch whose context is not in the tree is a diff about the
/// wrong revision. Both are refusals, and the cause is not decoration — it is
/// the requirement the next attempt is generated against, and the label the
/// refusal is counted under. Reading only the exit status files the first
/// defect under the second's name, and the generator is then told to go check
/// its context when the artifact it was handed is malformed.
///
/// The string git already printed is where the distinction lives, so it is
/// read rather than re-derived: a second parser here would be a second judge
/// of whether the patch is well-formed, and the one that decides the refusal
/// has to be the same one that names it.
///
/// Anything unrecognised keeps the context verdict. A message nobody has
/// classified is not evidence of a defect nobody has seen, and the refusal it
/// already produced is no worse for keeping the name it had.
fn apply_failure_cause(stderr: &str) -> cog_core::RejectionCause {
    if stderr.contains("corrupt patch") {
        cog_core::RejectionCause::MalformedDiff
    } else {
        cog_core::RejectionCause::ContextDoesNotApply
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

    /// The same file with the unformatted change applied and conformed: what the
    /// commitment carries when the gate rewrites `41+1` into the formatter's
    /// spelling.
    const CONFORMED_PROBE_SOURCE: &str = "pub fn answer() -> i32 {\n    41 + 1\n}\n";

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
            .run_test_command(root.path())
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

    /// 一笔改动给一个「每份部署文档都写着的键」抬默认值，效果落在没人读的载体
    /// 上：文档反序列化进类型，缺键才落回 `Default`，所以这个值在任何部署里都
    /// 读不到。判据要两侧同时成立才出口，这里两侧都造出来。
    #[tokio::test]
    async fn a_change_that_only_moves_an_overridden_default_has_no_effect() {
        let root = tempfile::tempdir().unwrap();
        let workdir = root.path();
        let config = workdir.join("crates/cog-core/src/config.rs");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        let document = serde_json::json!({
            "metrics": { "sample_max_rows": 200000, "log_at_floor": false },
        });
        let documents = [
            workdir.join("cogneva.example.json"),
            workdir.join("deploy/helm/cogneva/files/cogneva.json"),
        ];
        for path in &documents {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, serde_json::to_string_pretty(&document).unwrap()).unwrap();
        }
        let source = "\
pub struct MetricsConfig {
    pub sample_max_rows: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            sample_max_rows: 200_000,
        }
    }
}
";
        std::fs::write(&config, source).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["add", "-A"],
            vec![
                "-c",
                "user.email=t@example.com",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                "base",
            ],
        ] {
            let out = std::process::Command::new("git")
                .args(&args)
                .current_dir(workdir)
                .output()
                .expect("git");
            assert!(out.status.success(), "git {args:?} failed");
        }
        let pipeline = ChangePipeline::new(workdir, workdir, false);

        // 落地闸要拒的那一笔：只把那个被文档钉死的默认值抬上去。
        std::fs::write(&config, source.replace("200_000", "500_000")).unwrap();
        let reason = pipeline
            .unreachable_default_reason(workdir, "")
            .await
            .expect("这个默认值没有读者，这笔变更没有任何效果");
        assert!(
            reason.contains("sample_max_rows"),
            "拒因要点名读不到的键：{reason}"
        );
        assert!(
            reason.contains("cogneva.example.json"),
            "拒因要点名它据以判定的文档：{reason}"
        );

        // 同一棵树，改的是 impl 之外的一行 ⇒ 有别的效果，不出口。
        std::fs::write(
            &config,
            source.replace("pub sample_max_rows: usize,", "pub sample_max_rows: u32,"),
        )
        .unwrap();
        assert!(
            pipeline
                .unreachable_default_reason(workdir, "")
                .await
                .is_none(),
            "字段类型改了就不是「全部改动都是默认值里的字面量」"
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
            .run_test_command(root.path())
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

    /// The command that judges a change is the deployment's, not this crate's:
    /// a project that is not a Rust workspace is judged by its own suite, and
    /// the verdict follows the configured command rather than the default.
    #[tokio::test]
    async fn the_configured_test_command_decides_the_verdict() {
        let root = tempfile::tempdir().unwrap();

        let failing = ChangePipeline::new(root.path(), root.path(), false)
            .with_test_command(vec!["false".into()]);
        let (passed, _) = failing
            .run_test_command(root.path())
            .await
            .expect("`false` runs");
        assert!(!passed, "the configured command decides the verdict");

        let passing = ChangePipeline::new(root.path(), root.path(), false)
            .with_test_command(vec!["true".into()]);
        let (passed, _) = passing
            .run_test_command(root.path())
            .await
            .expect("`true` runs");
        assert!(passed);
        assert_eq!(passing.test_command_line(), "true");
    }

    /// A command that cannot be started is recorded with the command in it, so
    /// a deployment running something other than the default can be told from
    /// one that never left it — the reading this config item exists to produce.
    #[tokio::test]
    async fn a_test_command_that_cannot_start_names_itself() {
        let root = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path(), false)
            .with_test_command(vec!["cogneva-no-such-test-command".into()]);

        let err = pipeline
            .run_test_command(root.path())
            .await
            .expect_err("a command that does not exist cannot return a verdict");
        assert!(
            err.to_string().contains("cogneva-no-such-test-command"),
            "the failure has to name the command that was configured: {err}"
        );
    }

    /// An empty command is a deployment that has not said how a change is
    /// judged. Reading it as "nothing failed" would accept every change on a
    /// config typo, which is the one reading a missing criterion must not have.
    #[tokio::test]
    async fn an_empty_test_command_is_refused_rather_than_read_as_a_pass() {
        let root = tempfile::tempdir().unwrap();
        let pipeline =
            ChangePipeline::new(root.path(), root.path(), false).with_test_command(Vec::new());

        let err = pipeline
            .run_test_command(root.path())
            .await
            .expect_err("an empty command judges nothing and must not pass a change");
        assert!(err.to_string().contains("test_command is empty"), "{err}");
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

    /// The same body as [`rewrites`], with a hunk header that promises two
    /// lines on each side and delivers one. `git apply` refuses it while it is
    /// still reading the artifact — before it compares a single line to the
    /// tree — which is what makes it a defect of the diff rather than of its
    /// context.
    fn header_undercounts(path: &str) -> String {
        format!(
            "diff --git a/{path} b/{path}\n\
             --- a/{path}\n\
             +++ b/{path}\n\
             @@ -1,2 +1,2 @@\n\
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

    /// The other refusal git apply can hand back, and not the same one: a
    /// header that does not count its own body is refused while the artifact
    /// is still being read, so the tree was never the problem. The two are
    /// told apart by what git printed, and the cause travels as the
    /// requirement the next attempt is generated against — filing this as a
    /// context mismatch sends the generator to re-read a tree that was never
    /// wrong while the header it actually wrote goes unnamed.
    #[tokio::test]
    async fn a_change_whose_hunk_header_is_wrong_is_refused_for_its_form() {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        // The tree says exactly what the hunk body says it says, so the only
        // thing this diff gets wrong is its own header.
        tokio::fs::write(root.path().join("a.txt"), "old\n")
            .await
            .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "corrupt-1".into(),
            description: "调整这一行的取值".into(),
            content: header_undercounts("a.txt"),
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
            ChangeVerdict::Refused(cog_core::RejectionCause::MalformedDiff)
        );
        assert!(
            result.test_output.contains("corrupt patch"),
            "the refusal was filed under this reading and has to keep it: {}",
            result.test_output
        );
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
        // The seed matches what the test asserts, so this tree is green before
        // the change is applied. It has to be: the refusal this test is about
        // is "the change broke a test", and that is only answerable against a
        // tree that was passing it — a red seed would make the change's failure
        // indistinguishable from the tree's own.
        tokio::fs::write(
            root.path().join("src/lib.rs"),
            "pub fn answer() -> i32 {\n    42\n}\n\n#[test]\nfn answer_is_42() {\n    assert_eq!(answer(), 42);\n}\n",
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
                      -    42\n\
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

    /// A change that writes a line the linter reports on is refused for it, and
    /// the refusal names the line.
    ///
    /// The fixture is a real crate and the run is the real command. What has to
    /// hold here is that a run's machine-readable output joins to the lines this
    /// change wrote — the diagnostic names its file the way the workspace root
    /// spells it, and a reader that guessed a different root would attribute
    /// nothing and pass everything. A fixture that fed the reader a transcript
    /// would hold the reader and not the join.
    #[tokio::test]
    async fn a_change_that_writes_a_lint_is_refused_for_it() {
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
            "pub fn answer() -> i32 {\n    42\n}\n",
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        // `&Vec<u8>` is `clippy::ptr_arg` and an uncalled private function is
        // `dead_code` — two lints from two sources on one written line, so the
        // refusal has to list what it found rather than the first thing.
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "linting-1".into(),
            description: "给取值模块补一个辅助函数".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -1,3 +1,4 @@\n\
                      \x20pub fn answer() -> i32 {\n\
                      \x20    42\n\
                      \x20}\n\
                      +fn probe(v: &Vec<u8>) -> usize { v.len() }\n"
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
            ChangeVerdict::Refused(cog_core::RejectionCause::LintIntroduced)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
        // The line the change wrote is the fourth of the file it wrote it into.
        assert!(
            result.test_output.contains("src/lib.rs:4"),
            "the refusal does not name the line: {}",
            result.test_output
        );
        assert!(
            result.test_output.contains("clippy::ptr_arg"),
            "the refusal does not name the lint: {}",
            result.test_output
        );
        // Refused before the suite ran: the refusal is the change's evidence,
        // and the test the fixture carries never appears in it.
        assert!(
            !result.test_output.contains("answer_is_42"),
            "the suite ran even though the lint refused the change: {}",
            result.test_output
        );
        // Tracked files only: the linter and the suite leave their own build
        // output behind (`target/`, `Cargo.lock`), and neither is the change.
        let status = tokio::process::Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=no"])
            .current_dir(root.path())
            .output()
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&status.stdout).trim().is_empty(),
            "the refused change was left on the tree"
        );
    }

    /// A tree that already carries a lint is not a reason to refuse a change
    /// that writes none.
    ///
    /// This is the half that makes the criterion about the change rather than
    /// about the tree, and it is the incident this gate exists for read the
    /// other way: judged on the whole-tree report, a change landing on a tree
    /// with one lint is refused for it — and on such a tree that is every
    /// change, the one that would clear it included.
    #[tokio::test]
    async fn a_tree_that_already_carries_a_lint_does_not_refuse_a_clean_change() {
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
        // The seed carries `dead_code` and `clippy::ptr_arg` on its fourth line,
        // and this change never touches that line. It is written the way the
        // formatter spells it: a seed the formatter would rewrite would put its
        // own rewrite into the diff the lines are read from, and the reading
        // would be of two writers at once rather than of the change.
        tokio::fs::write(
            root.path().join("src/lib.rs"),
            "pub fn answer() -> i32 {\n    42\n}\n\
             fn unused_probe(v: &Vec<u8>) -> usize {\n    v.len()\n}\n\n\
             #[test]\nfn answer_is_42() {\n    assert_eq!(answer(), 42);\n}\n",
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "clean-1".into(),
            description: "给取值模块补一个辅助函数".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -11,1 +11,2 @@\n\
                      \x20}\n\
                      +pub fn doubled() -> i32 { answer() * 2 }\n"
                .into(),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(result.verdict, ChangeVerdict::Passed);
        // Control for the control: the linter did speak, and it did report the
        // tree's own lints — otherwise a green verdict here would be
        // indistinguishable from a criterion that never ran.
        assert!(
            result
                .test_output
                .contains("none of them on a line this change wrote"),
            "the lint criterion left no reading: {}",
            result.test_output
        );
        assert!(
            !result.test_output.contains("not evaluated"),
            "the lint criterion did not run at all: {}",
            result.test_output
        );
    }

    /// A baseline the formatter would rewrite is not one this criterion can
    /// read a diff against, and it declines rather than guessing which of two
    /// writers a line belongs to.
    ///
    /// The formatter conforms the whole tree, not only the change, so on such a
    /// tree the lines a worktree diff reports include its rewrite of the
    /// baseline too — and a lint the tree already carried, sitting on a line the
    /// formatter happened to rewrite, would be read as this change's. Nothing
    /// in the tree says afterwards which of the two wrote a line, so the stage
    /// asks while the tree still is the baseline and, when the answer is no,
    /// attributes nothing. That is a narrower reading than a refusal would look
    /// like and it is the honest one: the change is judged on the rest, the
    /// note names which reading this is, and the linter's own report still
    /// rides in the change's evidence in full.
    #[tokio::test]
    async fn a_rewritten_baseline_is_not_read_as_the_changes_lines() {
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
        // The same lint-bearing line as the control above, spelled the way a
        // generator would leave it: the formatter rewrites it into three lines,
        // so the tree the linter reads differs from its baseline by that
        // rewrite and by nothing else this change did not do.
        tokio::fs::write(
            root.path().join("src/lib.rs"),
            "pub fn answer() -> i32 {\n    42\n}\n\
             fn unused_probe(v: &Vec<u8>) -> usize { v.len() }\n\n\
             #[test]\nfn answer_is_42() {\n    assert_eq!(answer(), 42);\n}\n",
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "clean-2".into(),
            description: "给取值模块补一个辅助函数".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -9,1 +9,2 @@\n\
                      \x20}\n\
                      +pub fn doubled() -> i32 { answer() * 2 }\n"
                .into(),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(result.verdict, ChangeVerdict::Passed);
        // The linter did speak — otherwise this verdict would be the same one a
        // tree with no lints gets, and the test would pass without the premise
        // ever being read.
        assert!(
            !result.test_output.contains("no diagnostics on this tree"),
            "the linter never reported anything: {}",
            result.test_output
        );
        assert!(
            result
                .test_output
                .contains("differs from its baseline by more than this change"),
            "the declined reading is not legible in the evidence: {}",
            result.test_output
        );
        assert!(
            !result
                .test_output
                .contains("none of them on a line this change wrote"),
            "the criterion claimed a reading it could not make: {}",
            result.test_output
        );
    }

    /// A change the formatter would rewrite is conformed to it and then judged
    /// on what it does, and what would land is the conformed text.
    ///
    /// The commitment is the text the tests ran against, so conforming before
    /// the tests is what keeps CI's format check green without spending a
    /// generation: the producer is a generator that does not carry the
    /// formatter's spelling, so returning this change sends it back to a
    /// generator that would write it the same way again. Note what the fixture
    /// has to get right for a whole-tree rewrite to mean anything: the seeded
    /// tree is already formatted, so the change is the only thing there is to
    /// rewrite. A tree that was not clean to begin with could not show which of
    /// the two the gate answered.
    ///
    /// The change compiles and its tests pass — `41+1` is valid Rust that
    /// `rustfmt` spells `41 + 1` — so the verdict is `Passed` and the file on
    /// disk is the formatter's spelling, not the change's.
    #[tokio::test]
    async fn a_change_that_is_not_formatted_is_conformed_and_then_judged() {
        let (root, pipeline) = formatted_probe_workspace(true).await;
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
            ChangeVerdict::Passed,
            "a change that was conformed reached a verdict: {:?}",
            result.verdict
        );
        assert!(
            result.reformatted,
            "the verdict has to say the formatter rewrote this tree, or the count of changes that arrived unformatted cannot be read off it"
        );
        assert_eq!(
            tokio::fs::read_to_string(root.path().join("src/lib.rs"))
                .await
                .unwrap(),
            CONFORMED_PROBE_SOURCE,
            "what would land is the conformed text, not the text the change carried"
        );
    }

    /// A change the formatter cannot make conform is still refused, and the tree
    /// is left as it was found. The rewrite is not a way to land anything: it is
    /// run and then the check runs again, and this is the arm where it did not
    /// come out clean — the shape a change with a syntax error arrives in.
    #[tokio::test]
    async fn a_change_the_formatter_cannot_conform_is_refused() {
        let (root, pipeline) = formatted_probe_workspace(false).await;
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "unparsable-1".into(),
            description: "修正这个取值的计算".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -1,3 +1,3 @@\n\
                      \x20pub fn answer() -> i32 {\n\
                      -    41\n\
                      +    41 +\n\
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
            ChangeVerdict::Refused(cog_core::RejectionCause::FormattingDiffers),
            "a tree the formatter cannot make conform is a refusal: {:?}",
            result.verdict
        );
        assert!(!result.reformatted);
        assert_eq!(
            tokio::fs::read_to_string(root.path().join("src/lib.rs"))
                .await
                .unwrap(),
            FORMATTED_PROBE_SOURCE,
            "a refused change must not leave its own text behind for the next run to judge"
        );
    }

    /// The other side: a change that is already what the formatter produces is
    /// neither refused nor rewritten. A gate is only worth having if it can be
    /// told apart from one that always fires, and both mistakes here are
    /// expensive: a false refusal retires a change that was fine, and a false
    /// rewrite mangles text nobody asked it to touch — and would count a
    /// generator that writes clean code as one that does not.
    #[tokio::test]
    async fn a_formatted_change_is_neither_refused_nor_rewritten() {
        let (root, pipeline) = formatted_probe_workspace(false).await;
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
        assert!(
            !result.reformatted,
            "nothing was rewritten, so nothing may be counted as rewritten"
        );
    }

    /// A crate that is formatted to begin with, and a pipeline over it.
    ///
    /// Shared by the formatting tests so that the only thing between them is the
    /// line the change writes: one of them has to come out conformed, one
    /// untouched and one refused, and a fixture that differed in any other way
    /// could not show which of the three the gate is answering.
    ///
    /// `auto_apply` decides whether the tree is left standing after a pass,
    /// which is what lets the conforming test read what would land.
    async fn formatted_probe_workspace(auto_apply: bool) -> (tempfile::TempDir, ChangePipeline) {
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

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), auto_apply);
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

    /// 队列是一份字节，记录是这份字节之外唯一还带着状态的东西。
    fn written_record(
        id: &str,
        status: EvolutionStatus,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> EvolutionResult {
        EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: id.to_string(),
            description: format!("goal-{id}"),
            content: String::new(),
            status,
            created_at,
            eval_summary: None,
        }
    }

    /// A `.diff` nobody wrote a record for has no state, and the queue says so
    /// rather than calling it compiled. Inferring `CompileChecked` is what made a
    /// leftover file indistinguishable from a change that had passed its gate.
    #[tokio::test]
    async fn a_change_with_no_record_is_offered_as_unrecorded() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);
        tokio::fs::write(change_dir.join("leftover.diff"), "not a real diff\n")
            .await
            .unwrap();

        let pending = pipeline.pending_changes(None).await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "unknown is still verified: refusing to guess the state must not take it out of the queue"
        );
        assert_eq!(pending[0].status, EvolutionStatus::Unrecorded);
        assert_eq!(
            pending[0].created_at, UNKNOWN_CREATED_AT,
            "nothing knows when this was made, so nothing may claim an instant for it"
        );
    }

    /// The record is what decides the state, and it decides it on its own -- the
    /// resident index is only a copy the process happens to hold, and this
    /// pipeline is given none here.
    #[tokio::test]
    async fn the_record_beside_a_change_decides_its_state() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);
        tokio::fs::write(change_dir.join("judged.diff"), "not a real diff\n")
            .await
            .unwrap();

        let made_at = chrono::Utc::now() - chrono::Duration::hours(3);
        pipeline
            .write_change_record(&written_record(
                "judged",
                EvolutionStatus::AwaitingReview,
                made_at,
            ))
            .await
            .unwrap();

        let pending = pipeline.pending_changes(None).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status, EvolutionStatus::AwaitingReview);
        assert_eq!(pending[0].created_at, made_at);
        assert_eq!(pending[0].description, "goal-judged");
    }

    /// A state that means "not to be worked on" keeps the change out of the
    /// queue, which is what makes the record worth writing at all.
    #[tokio::test]
    async fn a_record_that_settles_a_change_keeps_it_out_of_the_queue() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);
        tokio::fs::write(change_dir.join("settled.diff"), "not a real diff\n")
            .await
            .unwrap();
        pipeline
            .write_change_record(&written_record(
                "settled",
                EvolutionStatus::ValidationFailed,
                chrono::Utc::now(),
            ))
            .await
            .unwrap();

        assert!(pipeline.pending_changes(None).await.unwrap().is_empty());
        // The file is still there: retiring it is a separate decision, and the
        // queue's silence about it is not the same as it being gone.
        assert!(change_dir.join("settled.diff").exists());
    }

    /// Retirement moves the record with the artifact it describes. Left behind,
    /// the record would keep a retired change in the pending listing, and the
    /// two halves of the pair would disagree about which queue it is in.
    #[tokio::test]
    async fn retiring_a_change_moves_its_record_too() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);
        let id = "task-2-def456";
        tokio::fs::write(change_dir.join(format!("{id}.diff")), "not a real diff\n")
            .await
            .unwrap();
        pipeline
            .write_change_record(&written_record(
                id,
                EvolutionStatus::CompileChecked,
                chrono::Utc::now(),
            ))
            .await
            .unwrap();
        assert!(!pipeline.artifact_is_retired(id), "not retired yet");

        pipeline.retire_change(id, "corrupt patch").await.unwrap();

        assert!(
            !change_dir.join(format!("{id}.json")).exists(),
            "the record may not stay in the pending queue describing a change that left it"
        );
        assert!(
            change_dir
                .join("retired")
                .join(format!("{id}.json"))
                .exists(),
            "the record has to travel with the artifact"
        );
        assert!(pipeline.artifact_is_retired(id));
        assert_eq!(
            pipeline.read_change_record(id).await.map(|r| r.status),
            Some(EvolutionStatus::CompileChecked),
            "the retired record is still readable where the artifact is"
        );
    }

    /// The guard under the drop of a resident record: only a change that is out
    /// of the queue *and* has its record beside it may be forgotten, or the
    /// index is the last thing holding the state and dropping it loses the state.
    #[tokio::test]
    async fn a_change_is_retired_only_with_both_halves_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        let retired = change_dir.join("retired");
        tokio::fs::create_dir_all(&retired).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);

        tokio::fs::write(retired.join("half.diff"), "not a real diff\n")
            .await
            .unwrap();
        assert!(
            !pipeline.artifact_is_retired("half"),
            "the artifact alone leaves the state it was retired for unrecorded"
        );

        tokio::fs::write(retired.join("half.json"), "{}")
            .await
            .unwrap();
        assert!(pipeline.artifact_is_retired("half"));

        tokio::fs::write(change_dir.join("pending.json"), "{}")
            .await
            .unwrap();
        assert!(
            !pipeline.artifact_is_retired("pending"),
            "a record still in the queue describes a change still in the queue"
        );
    }

    /// The listing is built from the records, so it has to see both the ones
    /// still in the queue and the ones that left it -- a listing that lost the
    /// retired half would answer "no such change" for work that had just landed.
    #[tokio::test]
    async fn the_known_records_cover_the_queue_and_the_retired_half() {
        let temp = tempfile::tempdir().unwrap();
        let change_dir = temp.path().join("changes");
        tokio::fs::create_dir_all(&change_dir).await.unwrap();
        let pipeline = ChangePipeline::new(temp.path(), &change_dir, true);

        let older = chrono::Utc::now() - chrono::Duration::hours(2);
        pipeline
            .write_change_record(&written_record(
                "waiting",
                EvolutionStatus::Generated,
                older,
            ))
            .await
            .unwrap();
        tokio::fs::write(change_dir.join("waiting.diff"), "not a real diff\n")
            .await
            .unwrap();
        pipeline
            .write_change_record(&written_record(
                "gone",
                EvolutionStatus::AwaitingReview,
                chrono::Utc::now(),
            ))
            .await
            .unwrap();
        tokio::fs::write(change_dir.join("gone.diff"), "not a real diff\n")
            .await
            .unwrap();
        pipeline.retire_change("gone", "landed").await.unwrap();

        let known: Vec<String> = pipeline
            .known_change_records()
            .await
            .into_iter()
            .map(|r| r.artifact_id)
            .collect();
        assert_eq!(
            known,
            vec!["gone".to_string(), "waiting".to_string()],
            "both halves, newest first"
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

    /// A crate with two tests that fail independently of each other, each
    /// asserting a value its own function returns.
    ///
    /// The two functions are what makes the baseline readable: a test can be
    /// made red or green by choosing a seed, without the change under test
    /// having anything to do with it. One test could not tell "the tree was
    /// already failing" from "the change broke the only test there is".
    fn two_answer_probe_source(answer: i32, other: i32) -> String {
        format!(
            "pub fn answer() -> i32 {{\n    {answer}\n}}\n\n\
             pub fn other() -> i32 {{\n    {other}\n}}\n\n\
             #[test]\nfn answer_is_42() {{\n    assert_eq!(answer(), 42);\n}}\n\n\
             #[test]\nfn other_is_7() {{\n    assert_eq!(other(), 7);\n}}\n"
        )
    }

    /// A change that rewrites the single seed line under `context`.
    ///
    /// The hunk is located by the line above it rather than by a count, so a
    /// test says which function it is moving and the two functions' bodies
    /// (`41` and `7`, both one line) stay indistinguishable to the diff.
    fn a_seed_moving(start: usize, context: &str, from: i32, to: i32) -> String {
        format!(
            "diff --git a/src/lib.rs b/src/lib.rs\n\
             --- a/src/lib.rs\n\
             +++ b/src/lib.rs\n\
             @@ -{start},3 +{start},3 @@\n\
             \x20{context}\n\
             -    {from}\n\
             +    {to}\n\
             \x20}}\n"
        )
    }

    fn a_change_moving_seeds(artifact_id: &str, content: String) -> EvolutionResult {
        EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: artifact_id.into(),
            description: "修正这个取值的计算".into(),
            content,
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
        }
    }

    /// A workspace whose tree is red or green before anything is applied,
    /// decided by the seeds.
    async fn two_answer_probe_workspace(
        answer: i32,
        other: i32,
    ) -> (tempfile::TempDir, ChangePipeline) {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        tokio::fs::write(
            root.path().join("Cargo.toml"),
            "[package]\nname = \"answer-probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(root.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(
            root.path().join("src/lib.rs"),
            two_answer_probe_source(answer, other),
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), true);
        (root, pipeline)
    }

    /// The test names the verdict says it is convicting the change of, read
    /// from the prefix the verdict writes and not from the suite's own output.
    fn blamed_by_the_verdict(test_output: &str) -> String {
        test_output
            .split("\n\n")
            .next()
            .unwrap_or_default()
            .to_string()
    }

    /// A tree that is already red must not convict every change that passes
    /// through it.
    ///
    /// A mainline can be red for reasons no change in the queue had anything to
    /// do with — a merge, a dependency, a test that only fails on this
    /// hardware. Reading the failure as the change's would retire every change
    /// in the queue, and the change that repairs the tree is one of them: the
    /// gate would be refusing the one change that tree most needs to accept.
    /// So the run that fails is compared against the same tree without the
    /// change, and only the difference is the change's to answer for.
    #[tokio::test]
    async fn a_change_is_not_refused_for_failures_the_tree_already_had() {
        // `answer` is 41 while its test demands 42: the tree is red before
        // anything is applied.
        let (root, pipeline) = two_answer_probe_workspace(41, 7).await;
        let change = a_change_moving_seeds(
            "already-red-1",
            a_seed_moving(1, "pub fn answer() -> i32 {", 41, 40),
        );

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Passed,
            "the change introduces no failure, so nothing about it was refused: {}",
            result.test_output
        );
        assert_eq!(
            result.pre_existing_failures, 1,
            "the verdict has to say how many failures it did not blame on the change, \
             or a red mainline is invisible: {}",
            result.test_output
        );
        let blamed = blamed_by_the_verdict(&result.test_output);
        assert!(
            blamed.contains("answer_is_42"),
            "the reading has to name the test it is not blaming the change for: {blamed}"
        );
    }

    /// The other side of the same rule, and the reason it is a difference
    /// rather than a count: a change that breaks a test the tree was passing is
    /// still refused, and the refusal names that test.
    #[tokio::test]
    async fn a_change_is_refused_when_it_breaks_a_test_the_tree_was_passing() {
        // Both seeds match their assertions: the tree is green.
        let (root, pipeline) = two_answer_probe_workspace(42, 7).await;
        let change = a_change_moving_seeds(
            "breaks-one-1",
            a_seed_moving(1, "pub fn answer() -> i32 {", 42, 40),
        );

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::TestsFailed),
            "the change is what broke this test: {}",
            result.test_output
        );
        assert_eq!(
            result.pre_existing_failures, 0,
            "nothing here was failing before the change: {}",
            result.test_output
        );
        let blamed = blamed_by_the_verdict(&result.test_output);
        assert!(
            blamed.contains("answer_is_42"),
            "the reading has to name the test it is convicting the change of: {blamed}"
        );
    }

    /// The two functions make the sharpest case available: one test is red
    /// before the change and one is green, and the change breaks only the green
    /// one — so a gate that read the run as a whole, or the tree as a whole,
    /// would get this wrong in one direction or the other.
    #[tokio::test]
    async fn only_the_failure_the_change_introduced_is_convicted() {
        // `answer_is_42` is red, `other_is_7` is green.
        let (root, pipeline) = two_answer_probe_workspace(41, 7).await;
        let change =
            a_change_moving_seeds("mixed-1", a_seed_moving(5, "pub fn other() -> i32 {", 7, 6));

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::TestsFailed),
            "the change broke a test this tree was passing: {}",
            result.test_output
        );
        let blamed = blamed_by_the_verdict(&result.test_output);
        assert!(
            blamed.contains("other_is_7"),
            "the test the change broke has to be the one it is convicted of: {blamed}"
        );
        assert!(
            !blamed.contains("answer_is_42"),
            "the test that was already failing must not be in the conviction: {blamed}"
        );
    }
}
