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
// cap: bound — 一次格式化以秒计；超过它说明 rustfmt 挂了，不是工作区大
pub(crate) const DEFAULT_FMT_TIMEOUT_SECS: u64 = 60;

/// The formatter this workspace's deployments ran while the stage was
/// hardcoded, in the three forms the stage needs: a probe that settles whether
/// there is a formatter to ask at all, a check that answers whether the tree is
/// what it produces, and the rewrite that makes it so.
///
/// A deployment that is not a Rust workspace declares its own in
/// `self_evolution.fmt_check_command` — one command, the check, because a
/// declared formatter is not assumed to be able to rewrite anything. The probe
/// has no declared counterpart: a command that cannot be started is already the
/// answer it exists to give.
const BUILTIN_FMT_PROBE: &[&str] = &["cargo", "fmt", "--version"];
const BUILTIN_FMT_CHECK: &[&str] = &["cargo", "fmt", "--all", "--", "--check"];
const BUILTIN_FMT_CONFORM: &[&str] = &["cargo", "fmt", "--all"];

/// Wall-clock bound on one deploy admission command.
///
/// Measured on the host that judges these changes (2026-10-17): the chart
/// render-freshness check took 3.7s and the parity check 1.6s. The bound is
/// two orders of magnitude above that pair on purpose — see the test-side
/// bound's reasoning — while still being a bound: a command that outlives it
/// is hung, and a hung command would hold the executor's only in-flight slot.
// cap: bound — 超了就是挂住了，而挂住的命令会把执行器唯一的在飞槽占死
const DEPLOY_ADMISSION_TIMEOUT_SECS: u64 = 300;

/// The linter this workspace's deployments ran while the stage was hardcoded.
/// `--message-format=json` is what makes its findings comparable against the
/// lines a change wrote, and it changes what the run says, not what it finds.
///
/// Every other argument is the CI job's, `--all-targets` included, and that one
/// is not a detail: without it the run lints the library and binary targets and
/// leaves the test targets out of the compile, so a change whose only offence is
/// in a test target passes the stage and dies in the job -- which is the whole
/// of what this stage exists to prevent. The two commands are held equal by
/// [`tests::the_stages_linter_is_the_ci_jobs_linter`] rather than by this
/// comment: a comment cannot go red.
const BUILTIN_LINT: &[&str] = &[
    "cargo",
    "clippy",
    "--keep-going",
    "--workspace",
    "--message-format=json",
    "--all-targets",
    "--",
    "-D",
    "warnings",
];

/// The sentence a change's own record carries when its deployment declares no
/// linter.
///
/// A linter that never ran and a linter that ran and found nothing are
/// different readings, and only one of them is true here: declaring none is
/// that fact, written where the verdict is read, rather than left to be
/// inferred from a run that was never refused.
const NO_LINTER_NOTE: &str =
    "This deployment declares no linter: the change was not checked against one.";

/// The built-in linter as it is named in a change's evidence: the command a
/// reader recognises, not the full argv that carries `--message-format=json`
/// and the warning promotion.
///
/// The scope stays in: a reader of the evidence is being told which files were
/// judged, and "test targets too" is part of that answer. What is dropped is
/// only what this stage adds to the *report* -- the machine-readable format and
/// the promotion that turns a finding into a refusal.
const BUILTIN_LINT_LINE: &str = "cargo clippy --workspace --all-targets";

/// [`ChangePipeline::run_cargo_fmt`] 里最多几趟命令：探针、判、改写、复判。
/// 判干净就提前返回，所以这是上界不是次数。
// cap: bound — 探针、判、改写、复判四趟；判干净就提前返回，所以是上界不是次数
const FMT_CMDS_PER_RUN_FMT: u64 = 4;

/// `run_cargo_fmt` 之外直接跑的那趟基线检查（apply 之前，读这棵树本来就
/// 是不是格式化过的样子）。
// cap: bound — 直接跑的那趟基线检查只有一次
const FMT_BASELINE_CHECKS_PER_CHANGE: u64 = 1;

/// `run_cargo_fmt` 在一条变更里最多被调用几次：apply 之后那趟，加上测试失败后
/// 读基线失败集合的那趟。
// cap: bound — apply 之后那趟，加测试失败后读基线失败集合那趟
const FMT_RUNS_PER_CHANGE: u64 = 2;

/// 一条变更在格式化这一段上最长的墙钟时间。
///
/// 这一段既不被宿主构建槽约束、也不被任何单一超时约束，只能按"最多跑几趟命令"
/// 封顶。验证循环把它加进自己声明的工期界里，所以它必须是数出来的：
/// `the_one_change_wall_counts_every_call_site_it_names` 数着源码里的调用点守着
/// 上面那三个数，免得加了新调用点而界还停在旧值上。
pub(crate) const FMT_SEGMENT_WALL_SECS: u64 = DEFAULT_FMT_TIMEOUT_SECS
    * (FMT_BASELINE_CHECKS_PER_CHANGE + FMT_RUNS_PER_CHANGE * FMT_CMDS_PER_RUN_FMT);

/// 一条变更要跑几条吃**测试**预算的命令：`run_cargo_clippy` 一条，
/// `run_test_command` 两条（套件一条，测试失败后读基线失败集合再一条）。
// cap: bound — clippy 一条加测试命令两条
const TEST_BUDGET_RUNS_PER_CHANGE: u64 = 3;

/// 吃**构建**预算的那一条：发布构建，它的 acquire 在 `evolution_deployer` 里。
// cap: bound — 吃构建预算的只有发布构建那一条
const BUILD_BUDGET_RUNS_PER_CHANGE: u64 = 1;

/// 一条变更上不受节拍约束的那几段活的墙钟上界——一个心跳到下一个心跳之间最长
/// 的一段。加的是三段：
///
/// - 每条 gated 命令各排一次宿主构建槽。闸门每次排队给的界是 `gate_wait_secs`；
///   闸门关着（或并发上限为 0）时调用方传 0，那时 acquire 不排队。
/// - 每条命令各吃一份自己的超时预算：`run_cargo_clippy` 与两条
///   `run_test_command` 吃测试预算，发布构建吃构建预算。
/// - 格式化那一段，见 [`FMT_SEGMENT_WALL_SECS`]。
///
/// 谁在读它：`reflection_change_verification` 把它声明成
/// [`cog_core::loop_health::Cadence::PeriodicWithWork`] 的工期界。**声明节拍的人
/// 和拿这个数当存活界的人，量的必须是同一段活**——所以这里加的全是强制那几段活
/// 的数，不是挑一个整数把规则按住。
pub(crate) fn one_change_wall_secs(
    gate_wait_secs: u64,
    test_budget_secs: u64,
    build_budget_secs: u64,
) -> u64 {
    // 每条 gated 命令恰好 acquire 一次，所以排队次数就是命令条数。
    let gate_waits = TEST_BUDGET_RUNS_PER_CHANGE + BUILD_BUDGET_RUNS_PER_CHANGE;
    gate_waits * gate_wait_secs
        + TEST_BUDGET_RUNS_PER_CHANGE * test_budget_secs
        + BUILD_BUDGET_RUNS_PER_CHANGE * build_budget_secs
        + FMT_SEGMENT_WALL_SECS
}

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
    /// 这条变更被路由到哪些门。
    ///
    /// 档位在这里算一次，之后由消费方取用、不重算：判据面是从一棵 checkout
    /// 上读出来的，而 apply 之后这棵树还会被下游继续改写。早期早退（在算档位
    /// 之前就返回的几次拒绝）没有档位可言，故为 `None`。
    pub tiering: Option<crate::criteria_face::Tiering>,
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

/// What this deployment's formatting stage read for a tree.
#[derive(Debug)]
enum FormatVerdict {
    /// The tree is what the formatter produces. `rewritten` says whether the
    /// stage had to conform it — only the built-in formatter can. `note` is
    /// what the change's own record carries about the stage: empty for the
    /// built-in formatter, whose evidence is reported when it refuses, and a
    /// sentence for a deployment that declares its own or none, because a
    /// change verified without a formatter has to be readable as that.
    Conformed { rewritten: bool, note: String },
    /// The tree is not what the formatter produces. The change is refused, and
    /// this is the evidence the refusal carries.
    Differs(String),
}

/// What this deployment's lint stage read for a tree.
#[derive(Debug)]
enum LintVerdict {
    /// The linter found nothing on this tree. The string is the sentence the
    /// change's record carries.
    Clean(String),
    /// The linter reports on this tree in the compiler's diagnostic
    /// interchange, which is the one form whose findings can be compared
    /// against the lines the change wrote. `line` is the command, so the note
    /// this produces names the judge that spoke rather than this crate's.
    Attributable { line: String, output: String },
    /// The linter reports on this tree in a form nothing here can compare, so
    /// no finding can be shown to be the tree's own.
    Unattributable(String),
    /// The linter exited non-zero and said nothing this stage can read: not one
    /// diagnostic in the compiler's interchange, on a linter that always speaks
    /// it. A run that exits non-zero under `-D warnings` always emits at least
    /// one diagnostic, so the absence of diagnostics is the absence of a
    /// judgement -- the command was not found, the toolchain would not start,
    /// or the run died before the tree was read. Kept apart from
    /// [`Self::Attributable`] because an empty set of findings from a judge that
    /// never spoke reads exactly like a clean tree, and only one of those is a
    /// measurement.
    Unmeasured(String),
    /// This deployment declares no linter.
    NotDeclared,
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
    /// The command that judges a change, as argv. Comes from the deployment's
    /// `self_evolution.test_command`; the default judges a Rust workspace, so a
    /// project that is not one has to say so here rather than be judged by a
    /// command that cannot run in its tree.
    test_command: Vec<String>,
    /// The command that judges the tree's formatting, when the deployment
    /// declares one. `None` means this workspace's own formatter, which is the
    /// built-in pair below: it answers both whether the tree is what it
    /// produces and the rewrite that makes it so.
    fmt_check_command: Option<Vec<String>>,
    /// The command that lints the tree, when the deployment declares one.
    /// `None` means this workspace's own linter, the only one whose findings
    /// this pipeline can compare against the lines the change wrote.
    lint_command: Option<Vec<String>>,
    /// The executable admission conditions for changes that touch `deploy/**`.
    /// `None` means this deployment declared none, and such a change is
    /// refused by name rather than sent to a person.
    deploy_admission: Option<cog_core::DeployAdmissionConfig>,
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
    /// Whether the tier cells have been laid down at zero yet.
    ///
    /// A tier that never fired and a tier reading that was never wired are the
    /// same absent series otherwise, and only the second is a defect. The cells
    /// are seeded on the first change the pipeline tiers rather than at
    /// construction, because a process that tiers nothing has no distribution to
    /// report — an all-zero cross product there would read as a fleet that
    /// routed everything to the cheapest tier.
    tier_cells_seeded: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the kind cells have been written at zero yet. Seeded on the
    /// first dispatch for the same reason the tier cells are: before any change
    /// has been dispatched there is no distribution to report, and an all-zero
    /// product would read as a fleet whose projects all declared the built-in.
    project_kind_cells_seeded: Arc<std::sync::atomic::AtomicBool>,
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

/// 变更目录下放「已经离开待办队列」那一半的目录名。
const RETIRED_SUBDIR: &str = "retired";

/// Where a change goes once it has left the pending queue.
///
/// Written by the retire step, read by the listing, the content reader and the
/// retire predicate: five spellings of one directory, and the ones that lose a
/// rename answer "no such change" rather than failing.
pub(crate) fn retired_dir(change_dir: &Path) -> PathBuf {
    change_dir.join(RETIRED_SUBDIR)
}

/// The record of a change that has left the pending queue. Retired beside the
/// `.diff` it describes, so the pair moves together and neither can be read
/// alone.
pub(crate) fn retired_change_record_path(dir: &Path, artifact_id: &str) -> PathBuf {
    retired_dir(dir).join(format!("{artifact_id}.json"))
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
            fmt_check_command: None,
            lint_command: None,
            deploy_admission: None,
            fmt_timeout_secs: DEFAULT_FMT_TIMEOUT_SECS,
            promotion_policy: None,
            target_dir: None,
            budget: None,
            baseline_failures: Arc::new(tokio::sync::Mutex::new(None)),
            metrics: None,
            tier_cells_seeded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            project_kind_cells_seeded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
    ///
    /// The field this sets is the fallback for a pipeline with no sink attached.
    /// A pipeline that has one reads the budget at every run (see
    /// [`Self::test_budget_secs`]), because the sink's number moves: a run that
    /// came within the headroom of the pin raises the budget for the next one,
    /// and a pipeline holding a copy taken at construction would keep enforcing
    /// the old wall while the reading advertised the new one.
    pub fn with_verification_budget(
        mut self,
        budget: Arc<crate::verification_budget::VerificationBudget>,
    ) -> Self {
        self.test_timeout_secs = budget.timeout_secs(crate::verification_budget::KIND_TEST);
        self.budget = Some(budget);
        self
    }

    /// The test budget this run is held to.
    ///
    /// Read at the moment of the run, not at wiring time: the sink derives it
    /// from the runs that already happened, so the number that applies to this
    /// change is the one as of now. Without a sink the pipeline keeps the value
    /// it was configured with.
    fn test_budget_secs(&self) -> u64 {
        match &self.budget {
            Some(budget) => budget.timeout_secs(crate::verification_budget::KIND_TEST),
            None => self.test_timeout_secs,
        }
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

    /// The deployment's formatter, when it declares one. `None` keeps this
    /// workspace's own; an empty list is a deployment declaring it has none.
    pub fn with_fmt_check_command(mut self, command: Option<Vec<String>>) -> Self {
        self.fmt_check_command = command;
        self
    }

    /// The deployment's linter, when it declares one. `None` keeps this
    /// workspace's own; an empty list is a deployment declaring it has none.
    pub fn with_lint_command(mut self, command: Option<Vec<String>>) -> Self {
        self.lint_command = command;
        self
    }

    /// The executable conditions a change touching `deploy/**` has to clear.
    /// `None` means the deployment declared none, which is a refusal with a
    /// reason, not a fallback to a person.
    pub fn with_deploy_admission(
        mut self,
        config: Option<cog_core::DeployAdmissionConfig>,
    ) -> Self {
        self.deploy_admission = config;
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
                tiering: None,
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
        for dir in [self.change_dir.clone(), retired_dir(&self.change_dir)] {
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
        retired_dir(&self.change_dir)
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
        let retired = retired_dir(&self.change_dir).join(format!("{artifact_id}.diff"));
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

        let dir = retired_dir(&self.change_dir);
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            SFError::IO(format!(
                "Failed to create retire dir {}: {}",
                dir.display(),
                e
            ))
        })?;

        let to = dir.join(format!("{artifact_id}.diff"));
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
                    tiering: None,
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
                    tiering: None,
                });
            }
        }

        // Which gates this change is entitled to, decided once, here, from what
        // it touches. The value is computed at this point and nowhere else: the
        // criteria face is read off a checkout at a moment, so a second
        // computation further down would be a second reading of a tree that may
        // have moved — and the two would disagree with no line of code saying
        // so. Whatever consumes this later takes the `Tiering` as an argument
        // rather than recomputing it.
        //
        // It is a reading today, not a gate: no gate's behaviour changes on it
        // yet. That is on purpose — the distribution has to accumulate before
        // anything is decided from it, and it can only accumulate while nothing
        // is reacting to it.
        let tiering = self.change_tier(workdir, &targets, &change.content);
        self.report_change_tier(&tiering).await;

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
                    tiering: Some(tiering.clone()),
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
                tiering: Some(tiering.clone()),
            });
        }

        self.ensure_clean_workspace(workdir).await?;

        // Which landing recipe this change is dispatched under, read from the
        // project's own declaration and at this exact point: from here until
        // the diff is applied the tree is the revision the change is being
        // applied to, so this is the project saying what it is rather than the
        // change saying what it wants to be judged by. The order matters twice
        // over — the document is a path no change may write, so nothing is in
        // flight to read past, and reading the revision under test rather than
        // the deployment's checkout keeps the answer from describing a tree
        // that is not the one being judged whenever the running binary and the
        // checkout have moved apart.
        let declaration = crate::project_kind::declaration_in(workdir);
        let (project_kind, kind_source) = match declaration.resolved() {
            Ok(resolved) => resolved,
            Err(reason) => {
                warn!(
                    change_id = %change.artifact_id,
                    reason = %reason,
                    "The project's kind declaration cannot be dispatched on"
                );
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::ProjectKindUnusable),
                    test_output: format!(
                        "The project's kind declaration cannot be dispatched on: {reason}"
                    ),
                    new_status: EvolutionStatus::Rejected,
                    tiering: Some(tiering.clone()),
                });
            }
        };
        self.report_project_kind(project_kind, kind_source).await;

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
                tiering: Some(tiering.clone()),
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
        //
        // Only the built-in formatter rewrites anything: a deployment that
        // declares a check, or no formatter at all, leaves this tree with one
        // writer by construction, and there is nothing to ask.
        let baseline_is_conformed = match self.fmt_check_command {
            Some(_) => true,
            None => match self.run_cargo_fmt_cmd(workdir, BUILTIN_FMT_CHECK).await {
                Ok((clean, _)) => clean,
                Err(e) => {
                    warn!(
                        change_id = %change.artifact_id,
                        error = %e,
                        "Could not read whether this tree's baseline is what the formatter produces"
                    );
                    false
                }
            },
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
                tiering: Some(tiering.clone()),
            });
        }

        // 部署拓扑这一族由可执行条件判，不由人判。判定放在应用之后：渲染和
        // parity 要读的正是被改过的那棵树。没有声明条件的部署，这一族就是拒收
        // ——「等人工」不是条件，一张没人认领的待办既不会落地也不会消失。
        if let Some(reason) = self
            .deploy_admission_reason(workdir, &targets, &change.content)
            .await?
        {
            warn!(
                change_id = %change.artifact_id,
                reason = %reason,
                "Change refused by the deployment admission conditions"
            );
            let _ = self.git_reset_hard(workdir, &change.content).await;
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                reformatted: false,
                pre_existing_failures: 0,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::DeployAdmissionFailed),
                test_output: reason,
                new_status: EvolutionStatus::ValidationFailed,
                tiering: Some(tiering.clone()),
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
            let _ = self.git_reset_hard(workdir, &change.content).await;
            return Ok(ApplyResult {
                change_id: change.artifact_id.clone(),
                files_changed,
                reformatted: false,
                pre_existing_failures: 0,
                verdict: ChangeVerdict::Refused(cog_core::RejectionCause::UnreachableDefault),
                test_output: reason,
                new_status: EvolutionStatus::ValidationFailed,
                tiering: Some(tiering.clone()),
            });
        }

        // Conformed before it is compiled, and rolled back either way: a change
        // refused here must leave the tree as it found it, or the next run would
        // be verifying this one's leftovers.
        let (reformatted, fmt_note) = match self.judge_format(workdir).await {
            Ok(FormatVerdict::Conformed { rewritten, note }) => (rewritten, note),
            Ok(FormatVerdict::Differs(evidence)) => {
                warn!(
                    change_id = %change.artifact_id,
                    "Change is not what this deployment's formatter produces; rolling back"
                );
                let _ = self.git_reset_hard(workdir, &change.content).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::FormattingDiffers),
                    test_output: evidence,
                    new_status: EvolutionStatus::ValidationFailed,
                    tiering: Some(tiering.clone()),
                });
            }
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, "the format check did not run");
                let _ = self.git_reset_hard(workdir, &change.content).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::TestRunUnavailable),
                    test_output: format!("Failed to execute the format check: {}", e),
                    new_status: EvolutionStatus::ValidationFailed,
                    tiering: Some(tiering.clone()),
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
        let clippy_note = match self.judge_lint(workdir, &change.artifact_id).await {
            Ok(LintVerdict::Clean(note)) => note,
            Ok(LintVerdict::NotDeclared) => NO_LINTER_NOTE.to_string(),
            Ok(LintVerdict::Unattributable(evidence)) => {
                let _ = self.git_reset_hard(workdir, &change.content).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::LintRefused),
                    test_output: evidence,
                    new_status: EvolutionStatus::ValidationFailed,
                    tiering: Some(tiering.clone()),
                });
            }
            Ok(LintVerdict::Unmeasured(evidence)) => {
                // The stage did not get a reading of this tree, and a stage that
                // did not read is not a change that failed it: the change stays
                // for an attempt that can lint, rather than landing with a note
                // that says "no diagnostics" about a run that never spoke.
                warn!(
                    change_id = %change.artifact_id,
                    "The built-in linter reached no verdict; this change is not judged yet"
                );
                let _ = self.git_reset_hard(workdir, &change.content).await;
                return Err(SFError::IO(evidence));
            }
            Ok(LintVerdict::Attributable { line, output }) => {
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
                            let _ = self.git_reset_hard(workdir, &change.content).await;
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
                    let _ = self.git_reset_hard(workdir, &change.content).await;
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
                    tiering: Some(tiering.clone()),
                    });
                }
                if !baseline_is_conformed {
                    info!(
                        change_id = %change.artifact_id,
                        diagnostics = attribution.diagnostics,
                        "The linter reports this tree's lints, and the tree it read differs from its baseline beyond this change"
                    );
                    format!(
                        "{line}: {} diagnostic(s) on this tree, which differs from its baseline by more than this change (the baseline is not what the formatter produces), so none is read as this change's",
                        attribution.diagnostics
                    )
                } else {
                    info!(
                        change_id = %change.artifact_id,
                        diagnostics = attribution.diagnostics,
                        "The linter reports this tree's lints; none of them is on a line this change wrote"
                    );
                    format!(
                        "{line}: {} diagnostic(s) on this tree, none of them on a line this change wrote",
                        attribution.diagnostics
                    )
                }
            }
            Err(e) if e.is_build_slot_refused() => {
                warn!(change_id = %change.artifact_id, error = %e, "the linter got no build slot");
                let _ = self.git_reset_hard(workdir, &change.content).await;
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
                    command = %self.lint_command_line(),
                    "The linter did not run; this change is judged without it"
                );
                format!("{}: not evaluated ({e})", self.lint_command_line())
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
                let _ = self.git_reset_hard(workdir, &change.content).await;
                return Err(e);
            }
            Err(e) => {
                warn!(change_id = %change.artifact_id, error = %e, command = %self.test_command_line(), "the test command failed to execute");
                let _ = self.git_reset_hard(workdir, &change.content).await;
                return Ok(ApplyResult {
                    change_id: change.artifact_id.clone(),
                    files_changed,
                    reformatted: false,
                    pre_existing_failures: 0,
                    verdict: ChangeVerdict::Refused(cog_core::RejectionCause::TestRunUnavailable),
                    test_output: format!("Failed to execute {}: {}", self.test_command_line(), e),
                    new_status: EvolutionStatus::ValidationFailed,
                    tiering: Some(tiering.clone()),
                });
            }
        };

        // A whole-suite run also fails for reasons this revision was already
        // carrying, and those are not the change's doing. Only read the tree's
        // own baseline when the run failed with tests named, so a green tree
        // never pays for it and the cost lands on the runs that need the
        // distinction.
        let mut test_passed = test_passed;
        // What the earlier criteria did rides in front of what the suite did, in
        // the order they ran: this field is the record a refused or landed
        // change is read from, and a stage that was not evaluated — or ran and
        // was declared absent — has to be readable as that rather than as
        // silence.
        let mut test_output = {
            let notes: Vec<&str> = [fmt_note.as_str(), clippy_note.as_str()]
                .into_iter()
                .filter(|note| !note.is_empty())
                .collect();
            if notes.is_empty() {
                test_output
            } else {
                format!("{}\n\n{test_output}", notes.join("\n"))
            }
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
                    let _ = self.git_reset_hard(workdir, &change.content).await;
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
                let _ = self.git_reset_hard(workdir, &change.content).await;
                EvolutionStatus::AwaitingReview
            }
        } else {
            warn!(change_id = %change.artifact_id, "Change tests failed; rolling back");
            let _ = self.git_reset_hard(workdir, &change.content).await;
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
            tiering: Some(tiering.clone()),
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

    /// Decide which gates this change is entitled to.
    ///
    /// The face is walked from the workdir the change is applied to, which is
    /// the only revision that stays correct when the running binary and the
    /// checkout are not the same commit — and the only one that can see a
    /// criteria carrier the change itself adds.
    fn change_tier(
        &self,
        workdir: &Path,
        targets: &[cog_core::DiffTarget],
        content: &str,
    ) -> crate::criteria_face::Tiering {
        crate::criteria_face::tier(
            &crate::criteria_face::criteria_face(workdir),
            targets,
            &cog_core::diff_shape(content),
        )
    }

    /// Publish which gates this change was routed to, and why.
    ///
    /// Takes the decision rather than recomputing it: the tier is computed once
    /// at the call site and this is one of its readers, so a second walk here
    /// would be a second reading of a tree that may have moved since.
    async fn report_change_tier(&self, tiering: &crate::criteria_face::Tiering) {
        info!(
            tier = tiering.tier.as_cell(),
            touches_criteria_code = tiering.touches_criteria_code,
            reasons = ?tiering.reasons,
            "change tier"
        );
        let Some(metrics) = self.metrics.as_ref().map(|sink| &sink.0) else {
            return;
        };
        if !self
            .tier_cells_seeded
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            for tier in crate::criteria_face::Tier::ALL {
                let mut labels = std::collections::HashMap::new();
                labels.insert(crate::LABEL_TIER.to_string(), tier.as_cell().to_string());
                if let Err(e) = metrics
                    .record_counter(cog_core::metric_names::CHANGE_TIER_TOTAL, 0.0, labels)
                    .await
                {
                    warn!(error = %e, "change tier cell seed failed");
                }
            }
        }
        let mut labels = std::collections::HashMap::new();
        labels.insert(
            crate::LABEL_TIER.to_string(),
            tiering.tier.as_cell().to_string(),
        );
        if let Err(e) = metrics
            .record_counter(cog_core::metric_names::CHANGE_TIER_TOTAL, 1.0, labels)
            .await
        {
            warn!(error = %e, "change tier emit failed");
        }
    }

    /// Publish which recipe this change was dispatched under, and whether the
    /// project named it or the built-in supplied it.
    ///
    /// Takes the decision rather than re-reading the declaration, so the
    /// recipe reported is the one the dispatch used: a second read here could
    /// land on a different document state than the one the change was dispatched
    /// on, and the reading would then explain a decision that was never made.
    ///
    /// The two sources are published as one axis rather than the second being
    /// left to silence. "This project never declared a kind" and "this project
    /// declared the built-in" dispatch identically, and only a dispatch that
    /// used the built-in because the project said nothing is the silent
    /// fallback this surface exists to make visible.
    async fn report_project_kind(
        &self,
        project_kind: cog_core::ProjectKind,
        source: cog_core::ProjectKindSource,
    ) {
        info!(
            kind = project_kind.as_str(),
            source = source.as_str(),
            "project kind"
        );
        let Some(metrics) = self.metrics.as_ref().map(|sink| &sink.0) else {
            return;
        };
        if !self
            .project_kind_cells_seeded
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            for kind in cog_core::ProjectKind::ALL {
                for source in cog_core::ProjectKindSource::ALL {
                    let mut labels = std::collections::HashMap::new();
                    labels.insert(
                        crate::verification_budget::KIND_LABEL.to_string(),
                        kind.as_str().to_string(),
                    );
                    labels.insert(crate::LABEL_SOURCE.to_string(), source.as_str().to_string());
                    if let Err(e) = metrics
                        .record_counter(cog_core::metric_names::PROJECT_KIND_TOTAL, 0.0, labels)
                        .await
                    {
                        warn!(error = %e, "project kind cell seed failed");
                    }
                }
            }
        }
        let mut labels = std::collections::HashMap::new();
        labels.insert(
            crate::verification_budget::KIND_LABEL.to_string(),
            project_kind.as_str().to_string(),
        );
        labels.insert(crate::LABEL_SOURCE.to_string(), source.as_str().to_string());
        if let Err(e) = metrics
            .record_counter(cog_core::metric_names::PROJECT_KIND_TOTAL, 1.0, labels)
            .await
        {
            warn!(error = %e, "project kind emit failed");
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
        self.run_git_apply_args(
            workdir,
            change_content,
            if check_only { &["--check"] } else { &[] },
        )
        .await
    }

    /// `git apply` with the caller's flags, patch on stdin.
    ///
    /// The reverse run goes through the same door as the forward one: a second
    /// spawn site would be free to drift from this one's environment and its
    /// error text, and the two would then disagree about what "applied" means.
    async fn run_git_apply_args(
        &self,
        workdir: &Path,
        change_content: &str,
        flags: &[&str],
    ) -> SFResult<()> {
        let mut cmd = tokio::process::Command::new("git");
        cmd.arg("apply").arg("-v").current_dir(workdir);
        cmd.args(flags);

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
                flags.iter().map(|f| format!("{f} ")).collect::<String>(),
                stderr
            )));
        }

        Ok(())
    }

    /// Restore the working tree to HEAD.
    /// Put the worktree back the way `HEAD` has it, including the files the
    /// change added.
    ///
    /// `git reset --hard` restores tracked files and nothing else. A change
    /// that creates a file leaves it behind as untracked, so the next
    /// `git apply` of that same change fails on it ("already exists") -- and a
    /// change that adds a file and is rolled back could then never be applied
    /// a second time. That is not hypothetical: it is how one change sat in
    /// the pending queue for two days, re-tried every cycle and never judged
    /// (2026-10-20, a new test file plus a failing suite, so the baseline read
    /// took the change back off and could not put it on again).
    ///
    /// The clean is scoped to the paths this patch names. A tree-wide clean
    /// would also delete work in progress the pipeline did not create, and the
    /// paths come from the same parser the rest of the pipeline reads them
    /// with, so what is removed here is exactly what was added there.
    async fn git_reset_hard(&self, workdir: &Path, change_content: &str) -> SFResult<()> {
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

        let paths: Vec<String> =
            cog_core::contract::reflection::parse_diff_affected_files(change_content)
                .unwrap_or_default()
                .into_iter()
                // A path outside the worktree would make `git clean` refuse the
                // whole command and leave every leftover in place; the pipeline
                // has already refused such a patch, so this only keeps a
                // malformed one from failing the reset instead.
                .filter(|path| !path.starts_with('/') && !path.split('/').any(|part| part == ".."))
                .collect();
        if paths.is_empty() {
            return Ok(());
        }
        let output = tokio::process::Command::new("git")
            .args(["clean", "-fd", "--"])
            .args(&paths)
            .current_dir(workdir)
            .output()
            .await
            .map_err(|e| SFError::IO(format!("Failed to run git clean: {}", e)))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SFError::IO(format!(
                "git clean of the change's own paths failed: {}",
                stderr
            )));
        }
        Ok(())
    }

    /// Run a git command and answer its stdout **byte for byte**, trailing
    /// newline included.
    ///
    /// [`Self::git_try`] trims, which is right for reading a revision or a
    /// status line and wrong for reading a patch: `git diff` output ends in a
    /// newline, and feeding a trimmed copy back to `git apply` is a corrupt
    /// patch. Two readers of the same command disagreeing about its last byte
    /// is exactly the shape that would make a rollback look impossible.
    async fn git_capture(&self, workdir: &Path, args: &[&str]) -> Option<String> {
        let output = tokio::process::Command::new("git")
            .args(args)
            .current_dir(workdir)
            .output()
            .await
            .ok()?;
        if output.status.success() {
            Some(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            None
        }
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

    /// The targets that belong to the deployment topology family.
    ///
    /// Matched by directory component, not by prefix alone: `deploy2/x` does
    /// not ride along on `deploy/`. The path is normalised the same way the
    /// rest of the pipeline normalises diff paths, so the family is decided on
    /// the same spelling the change was parsed into.
    fn deploy_targets(targets: &[cog_core::DiffTarget]) -> Vec<String> {
        targets
            .iter()
            .map(|t| t.path.replace('\\', "/"))
            .filter(|p| {
                p.strip_prefix("deploy/").is_some_and(|rest| {
                    !rest.is_empty()
                        && rest
                            .split('/')
                            .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
                })
            })
            .collect()
    }

    /// Whether the executable admission conditions for a topology change hold.
    ///
    /// `Some(reason)` refuses the change and names the condition that failed;
    /// `None` means the family was not touched, or every declared condition
    /// passed. A deployment that declared no condition does not fall back to a
    /// person: the family's most severe executable tier is refusal, and the
    /// reason says which declaration is missing.
    async fn deploy_admission_reason(
        &self,
        workdir: &Path,
        targets: &[cog_core::DiffTarget],
        change_content: &str,
    ) -> SFResult<Option<String>> {
        let touched = Self::deploy_targets(targets);
        if touched.is_empty() {
            return Ok(None);
        }

        let Some(config) = self.deploy_admission.as_ref().filter(|c| c.is_executable()) else {
            return Ok(Some(format!(
                "触及部署拓扑（{}），而这一族没有声明可执行的准入条件。\
                 在 self_evolution.deploy_admission 里声明渲染 / parity / 服务端 dry-run；\
                 这一档不转人工——没有条件的族按最严的可执行档处理。",
                touched.join(", ")
            )));
        };

        for command in &config.commands {
            let (ok, output) = self
                .run_verification_command(workdir, command, DEPLOY_ADMISSION_TIMEOUT_SECS)
                .await?;
            if !ok {
                return Ok(Some(format!(
                    "部署准入条件未通过：`{}` 退出非零。{}",
                    command.join(" "),
                    output.trim()
                )));
            }
        }

        if let Some(prefix) = &config.server_dry_run {
            for path in touched.iter().filter(|p| p.starts_with("deploy/rendered/")) {
                let mut argv = prefix.clone();
                argv.push(path.clone());
                let (ok, output) = self
                    .run_verification_command(workdir, &argv, DEPLOY_ADMISSION_TIMEOUT_SECS)
                    .await?;
                if !ok {
                    return Ok(Some(format!(
                        "服务端 dry-run 拒绝了 {path}：{}",
                        output.trim()
                    )));
                }
            }
        }

        // 拓扑与核心放开之后，平台能在无人值守下改自己的控制面；缓释不是留一个人，
        // 而是「改得回去，且回去那条路被走过一次」。这里走的就是那一次：把这一笔
        // 逆向执行、确认树回到 HEAD，再把它应用回来并确认树与之前逐字节一致。
        // 只做 `git apply -R --check` 不够——那证明的是补丁能对上，不是这条路能跑完。
        if let Some(reason) = self
            .deploy_rollback_reason(workdir, &touched, change_content)
            .await?
        {
            return Ok(Some(reason));
        }

        Ok(None)
    }

    /// Take the change back out, then put it in again, and report the reading.
    ///
    /// `Some(reason)` refuses: a tree the change cannot be rewound out of is one
    /// the deployment cannot back out of, and the time to learn that is before
    /// it lands rather than during an incident. The worktree is left holding the
    /// change either way — a refusal resets it, a pass has already re-applied it.
    async fn deploy_rollback_reason(
        &self,
        workdir: &Path,
        touched: &[String],
        change_content: &str,
    ) -> SFResult<Option<String>> {
        let Some(before) = self.git_capture(workdir, &["diff", "HEAD"]).await else {
            return Ok(Some(
                "回滚路径无法执行：判定工作树不是 git 工作树，这一笔改动出去之后没有可执行的倒回方式。"
                    .to_string(),
            ));
        };
        let untracked_of_change = |listing: &str| -> Vec<String> {
            listing
                .lines()
                .map(str::trim)
                .filter(|path| !path.is_empty() && touched.iter().any(|t| t == path))
                .map(str::to_string)
                .collect()
        };
        let added_before = untracked_of_change(
            &self
                .git_capture(workdir, &["ls-files", "--others", "--exclude-standard"])
                .await
                .unwrap_or_default(),
        );

        if !before.is_empty() {
            if let Err(e) = self.run_git_apply_args(workdir, &before, &["-R"]).await {
                return Ok(Some(format!(
                    "回滚路径执行失败：逆向补丁打不回去（{e}）。这一笔改动落在树上之后没有可执行的回退。"
                )));
            }
        }
        for path in &added_before {
            if let Err(e) = tokio::fs::remove_file(workdir.join(path)).await {
                return Ok(Some(format!(
                    "回滚路径执行失败：新增文件 {path} 删不掉（{e}）。"
                )));
            }
        }

        // 倒回之后，这一族在树里不该留下任何东西。别的会话或别的族留下的改动不在
        // 判据里：这条读的是「这一笔能不能被完整拿掉」，不是「这棵树干净」。
        let leftovers: Vec<String> = self
            .git_try(workdir, &["status", "--porcelain"])
            .await
            .unwrap_or_default()
            .lines()
            .map(str::trim)
            .filter(|line| touched.iter().any(|t| line.ends_with(t.as_str())))
            .map(str::to_string)
            .collect();
        if !leftovers.is_empty() {
            return Ok(Some(format!(
                "回滚走过一遍之后，树上仍留着这一族的改动：{}。",
                leftovers.join("、")
            )));
        }

        if let Err(e) = self.git_apply(workdir, change_content).await {
            return Ok(Some(format!(
                "回滚走过一遍之后，这次变更重新应用失败（{e}）：它只能落一次，落不了第二次。"
            )));
        }
        let after = self
            .git_capture(workdir, &["diff", "HEAD"])
            .await
            .unwrap_or_default();
        let added_after = untracked_of_change(
            &self
                .git_capture(workdir, &["ls-files", "--others", "--exclude-standard"])
                .await
                .unwrap_or_default(),
        );
        if after != before || added_after != added_before {
            return Ok(Some(
                "回滚并重新应用之后，树与之前读到的不是同一棵：这次变更的逆不是它的重复。"
                    .to_string(),
            ));
        }

        Ok(None)
    }

    /// Spawn one judge command under the verification environment and the
    /// caller's bound, returning (succeeded, combined_output).
    ///
    /// Every command that judges a change comes through here — the formatter's
    /// probe and check, the linter, the suite — so that "what may it see" and
    /// "what may it run under" keep one answer each rather than one per call
    /// site: a second site that rebuilt these lines by hand would be free to
    /// drop the environment the verdict is only valid in.
    async fn run_verification_command(
        &self,
        workdir: &Path,
        argv: &[String],
        timeout_secs: u64,
    ) -> SFResult<(bool, String)> {
        let Some(program) = argv.first() else {
            return Err(SFError::Config(
                "a verification command with no program judges nothing".into(),
            ));
        };
        let line = argv.join(" ");
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(&argv[1..]).current_dir(workdir).kill_on_drop(true);
        self.apply_verification_env(&mut cmd);

        let output = match tokio::time::timeout(Duration::from_secs(timeout_secs), cmd.output())
            .await
        {
            Ok(result) => result.map_err(|e| SFError::IO(format!("Failed to run {line}: {e}")))?,
            Err(_) => {
                return Err(SFError::IO(format!(
                    "{line} exceeded the {timeout_secs}s verification budget and was killed"
                )))
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok((output.status.success(), format!("{}{}", stdout, stderr)))
    }

    /// Spawn one invocation of the built-in formatter, under the format bound.
    /// The argv is the whole command line, program included.
    async fn run_cargo_fmt_cmd(&self, workdir: &Path, argv: &[&str]) -> SFResult<(bool, String)> {
        let argv: Vec<String> = argv.iter().map(|arg| (*arg).to_string()).collect();
        self.run_verification_command(workdir, &argv, self.fmt_timeout_secs)
            .await
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
        let (available, why) = self.run_cargo_fmt_cmd(workdir, BUILTIN_FMT_PROBE).await?;
        if !available {
            return Err(SFError::IO(format!(
                "the format check has no formatter to ask: {why}"
            )));
        }
        info!("Running cargo fmt --all -- --check");
        let (clean, checked) = self.run_cargo_fmt_cmd(workdir, BUILTIN_FMT_CHECK).await?;
        if clean {
            return Ok((true, false, checked));
        }
        info!("Conforming the change to what this workspace's formatter produces");
        let (ran, rewrite_output) = self.run_cargo_fmt_cmd(workdir, BUILTIN_FMT_CONFORM).await?;
        let evidence = format!("{checked}\n{rewrite_output}");
        if !ran {
            return Ok((false, false, evidence));
        }
        let (conformed, rechecked) = self.run_cargo_fmt_cmd(workdir, BUILTIN_FMT_CHECK).await?;
        let evidence = format!("{evidence}\n{rechecked}");
        Ok(if conformed {
            (true, true, evidence)
        } else {
            (false, false, evidence)
        })
    }

    /// What this deployment's formatting stage reads for this tree.
    ///
    /// Three declarations, three readings. The built-in formatter is the one
    /// path that can rewrite the tree, and the one whose probe separates "no
    /// formatter to ask" from "not what the formatter produces". A declared
    /// command is asked the same question and nothing more: it is a check, and
    /// a tree it does not produce is refused rather than conformed, because a
    /// declared command is never assumed to be able to rewrite anything. A
    /// deployment that declares none gets no reading, and its change's own
    /// record carries that fact — a stage that never ran and a stage that ran
    /// and found nothing are different readings, and only one of them is true.
    async fn judge_format(&self, workdir: &Path) -> SFResult<FormatVerdict> {
        match self.fmt_check_command.as_deref() {
            None => match self.run_cargo_fmt(workdir).await? {
                (true, rewritten, _) => Ok(FormatVerdict::Conformed {
                    rewritten,
                    note: String::new(),
                }),
                (false, _, evidence) => Ok(FormatVerdict::Differs(format!(
                    "The formatter could not make this tree what it produces:\n{evidence}"
                ))),
            },
            Some([]) => Ok(FormatVerdict::Conformed {
                rewritten: false,
                note:
                    "This deployment declares no formatter: the change was not checked against one."
                        .to_string(),
            }),
            Some(argv) => {
                let line = argv.join(" ");
                info!(command = %line, "Running the formatter this deployment declares");
                match self
                    .run_verification_command(workdir, argv, self.fmt_timeout_secs)
                    .await
                {
                    Ok((true, _)) => Ok(FormatVerdict::Conformed {
                        rewritten: false,
                        note: format!(
                            "{line}: the tree is what this deployment's formatter produces"
                        ),
                    }),
                    Ok((false, evidence)) => Ok(FormatVerdict::Differs(format!(
                        "The formatter this deployment declares does not produce this tree \
                         (the change must arrive already conformed; a declared formatter is \
                         not asked to rewrite it):\n{evidence}"
                    ))),
                    // A command that cannot be started is the same fact the
                    // built-in probe exists to name: there is no formatter to
                    // ask, so nothing was judged.
                    Err(e) => Err(SFError::IO(format!(
                        "the format check has no formatter to ask: {e}"
                    ))),
                }
            }
        }
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
        let _slot = crate::build_gate::acquire("change verification").await?;
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(&self.test_command[1..])
            .current_dir(workdir)
            .kill_on_drop(true);
        self.apply_verification_env(&mut cmd);

        let started = Instant::now();
        let budget_secs = self.test_budget_secs();
        let output =
            match tokio::time::timeout(Duration::from_secs(budget_secs), cmd.output()).await {
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
                        budget_secs
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

    /// The command this deployment's lint stage would run, as it is written to
    /// the change's evidence.
    ///
    /// Declaring no linter is not the same as declaring the built-in one, but
    /// the line a deployment reads here is the line that ran: with nothing
    /// declared that is the built-in linter, and with one declared it is that
    /// command, so a record that says the linter did not run names the linter
    /// the deployment actually asked for.
    fn lint_command_line(&self) -> String {
        match self.lint_command.as_deref() {
            Some(argv) if !argv.is_empty() => argv.join(" "),
            _ => BUILTIN_LINT_LINE.to_string(),
        }
    }

    /// Run the built-in linter's command over the workspace and return
    /// (succeeded, output).
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
    ///
    /// No slot is taken here: the stage takes one for whichever linter it runs,
    /// so that this command and a deployment's own are one gated command each
    /// to the wall bound rather than two.
    async fn run_cargo_clippy(&self, workdir: &Path) -> SFResult<(bool, String)> {
        let argv: Vec<String> = BUILTIN_LINT.iter().map(|arg| (*arg).to_string()).collect();
        info!(command = %argv.join(" "), "Running the built-in linter");
        self.run_verification_command(workdir, &argv, self.test_budget_secs())
            .await
    }

    /// What this deployment's linter reads for this tree, as the sentence the
    /// change's own record carries, or a refusal.
    ///
    /// The build slot is taken once, here, for whichever linter runs: a declared
    /// linter is the same kind of work as the built-in one — a whole-tree read
    /// that compiles — so a deployment that declares one is not thereby allowed
    /// to run it beside the host's other builds, and the two are one gated
    /// command to the wall bound rather than two. A deployment that declares no
    /// linter returns before the slot is taken, because there is no work for a
    /// slot to bound.
    ///
    /// A declared linter is judged by its exit status on this tree, and that is
    /// all it can be judged by: the comparison that separates a finding the
    /// change wrote from one the tree already carried reads the compiler's
    /// diagnostic interchange, and no other linter speaks it. So a declared
    /// linter that refuses the tree refuses the change — reading its findings
    /// as the tree's own would need the comparison that is not available, and
    /// reading the absence of parseable findings as "nothing was reported"
    /// would be the reading a linter that never ran produces. A deployment
    /// whose linter cannot be attributed either declares one that can, or
    /// declares none.
    ///
    /// The un-attributable answer is the declared linter's alone. The built-in
    /// one always speaks the interchange, and where it exits non-zero on a tree
    /// it cannot parse there is a compilation error for the suite to answer
    /// for, not a linter refusing the change; reading that as `LintRefused`
    /// would retire a change for a verdict the lint criterion never made.
    ///
    /// A built-in linter that exits non-zero with nothing in the interchange is
    /// not a tree with no findings, it is a tree no linter read: the command was
    /// not found, the toolchain would not start, or the run died first. That
    /// answers [`LintVerdict::Unmeasured`] and never a clean tree, so a host
    /// without the linter cannot pass changes through the stage unlinted.
    async fn judge_lint(&self, workdir: &Path, change_id: &str) -> SFResult<LintVerdict> {
        let declared = match self.lint_command.as_deref() {
            Some([]) => return Ok(LintVerdict::NotDeclared),
            Some(argv) => Some(argv),
            None => None,
        };
        let _slot = crate::build_gate::acquire("change lint verification").await?;
        let (line, passed, output) = match declared {
            Some(argv) => {
                let line = argv.join(" ");
                info!(command = %line, "Running the linter this deployment declares");
                let (passed, output) = self
                    .run_verification_command(workdir, argv, self.test_budget_secs())
                    .await?;
                (line, passed, output)
            }
            None => {
                let (passed, output) = self.run_cargo_clippy(workdir).await?;
                (BUILTIN_LINT_LINE.to_string(), passed, output)
            }
        };
        if passed {
            return Ok(LintVerdict::Clean(format!(
                "{line}: no diagnostics on this tree"
            )));
        }
        if declared.is_none() {
            // The built-in linter always speaks the interchange, and under
            // `-D warnings` a non-zero exit always carries at least one
            // diagnostic. So a non-zero exit with nothing in it is a command
            // that did not run -- a missing `clippy`, a toolchain that would
            // not start, a run killed before it read the tree. Reporting that
            // as "no diagnostic is on a line this change wrote" would turn a
            // tree nobody linted into a tree that linted clean, which is the
            // one reading this stage exists to refuse.
            if !cog_core::contract::reflection::has_compiler_messages(&output) {
                warn!(
                    change_id = %change_id,
                    command = %line,
                    "The built-in linter exited non-zero without reporting anything"
                );
                return Ok(LintVerdict::Unmeasured(format!(
                    "{line} exited non-zero and reported nothing in the compiler's diagnostic \
                     interchange, so this tree was not judged by a linter at all:\n{output}"
                )));
            }
            return Ok(LintVerdict::Attributable { line, output });
        }
        if cog_core::contract::reflection::has_compiler_messages(&output) {
            return Ok(LintVerdict::Attributable { line, output });
        }
        warn!(
            change_id = %change_id,
            command = %line,
            "The declared linter refuses this tree and its findings cannot be attributed to lines"
        );
        Ok(LintVerdict::Unattributable(format!(
            "The linter this deployment declares refuses this tree, and its findings are not in \
             the compiler's diagnostic interchange, so none of them can be shown to be the \
             tree's own rather than this change's:\n{output}"
        )))
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
    /// and commit a tree the format check rejects. A deployment that declares
    /// its own formatter, or none, has no rewrite in it to undo — a declared
    /// formatter only ever checks, and the change went on the tree as it
    /// arrived — so the raw content is what the run found.
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

        self.git_reset_hard(workdir, change_content).await?;
        let (_, baseline_output) = self.run_test_command(workdir).await?;

        self.git_apply(workdir, change_content).await.map_err(|e| {
            SFError::IO(format!(
                "could not put the change back after reading the tree's own test failures: {e}"
            ))
        })?;
        // Only the built-in formatter rewrites the tree, so only it has a
        // rewrite to re-apply here. A declared one checks and does not rewrite,
        // and a deployment that declares none has nothing to run.
        if self.fmt_check_command.is_none() {
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
    use crate::rust_production::production_text;

    use crate::checkout::repo_root;

    /// [`one_change_wall_secs`] 加的是哪几段活、`FMT_SEGMENT_WALL_SECS` 那个数
    /// 乘的是哪几趟命令，都是数着源码里的调用点写下来的。加一条 gated 命令、
    /// 或让 `run_cargo_fmt` 多跑一趟，界就少算一段——而界少了的症状是停摆告警
    /// 在健康的轮上响，所以调用点数必须钉住。这里数的都是**生产**调用点：测试
    /// 模块里那几处调用带着 `root.path()` 这种实参，与下面这些模式不匹配。
    #[test]
    fn the_one_change_wall_counts_every_call_site_it_names() {
        const PIPELINE: &str = include_str!("change_pipeline.rs");
        const DEPLOYER: &str = include_str!("evolution_deployer.rs");
        // 只看产出那一段：这个测试自己的源码里也写着下面这些模式串，整份数会把
        // 测试自己数进去。边界由共享算法算，不按「第一处 `#[cfg(test)]`」切。
        let pipeline = production_text(PIPELINE);
        let deployer = production_text(DEPLOYER);

        // 每条 gated 命令**函数**里恰好一次 acquire：测试与 clippy 两条在流水线里，
        // 发布构建一条在部署器里。
        let acquire_sites = pipeline.matches("build_gate::acquire(").count()
            + deployer.matches("build_gate::acquire(").count();
        assert_eq!(acquire_sites, 3, "gated 命令的函数个数变了");

        // 吃测试预算的：clippy 一条，套件与读基线失败集合各一次。
        let test_runs = pipeline.matches("self.run_test_command(").count();
        assert_eq!(
            pipeline.matches("self.run_cargo_clippy(").count(),
            1,
            "run_cargo_clippy 的调用点数变了"
        );
        assert_eq!(
            test_runs as u64 + 1,
            TEST_BUDGET_RUNS_PER_CHANGE,
            "吃测试预算的命令 = clippy 一条 + run_test_command 那几条"
        );

        // 排队次数 = acquire 点 + run_test_command 多走的那一次。
        assert_eq!(
            acquire_sites as u64 + (test_runs as u64 - 1),
            TEST_BUDGET_RUNS_PER_CHANGE + BUILD_BUDGET_RUNS_PER_CHANGE,
            "构建槽排队次数与它该覆盖的命令条数对不上了"
        );

        // 格式化那一段：run_cargo_fmt 里几趟命令，加外面那趟基线检查。
        assert_eq!(
            FMT_RUNS_PER_CHANGE as usize,
            pipeline.matches("self.run_cargo_fmt(").count(),
            "run_cargo_fmt 的调用点数变了"
        );
        assert_eq!(
            pipeline.matches(".run_cargo_fmt_cmd(").count() as u64,
            FMT_BASELINE_CHECKS_PER_CHANGE + FMT_CMDS_PER_RUN_FMT,
            "格式化命令的调用点数变了：run_cargo_fmt 里那几处加外面那趟基线检查"
        );

        // 加数与用法：界是"每条命令一份等待 + 一份预算"，没有别的东西混进来。
        assert_eq!(
            one_change_wall_secs(0, 0, 0),
            FMT_SEGMENT_WALL_SECS,
            "闸门与预算都是 0 时剩下的只能是格式化那一段"
        );
        assert_eq!(
            one_change_wall_secs(1, 10, 100),
            4 + 30 + 100 + FMT_SEGMENT_WALL_SECS,
            "每条 gated 命令一份等待，测试预算三条、构建预算一条"
        );
    }

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

    /// A declared formatter is asked the one question a check answers and no
    /// more: is this tree what the formatter produces. It is never asked to
    /// rewrite — a declared command is not assumed to be able to — so a tree it
    /// does not produce is refused rather than conformed, and the change's
    /// record says which reading it got.
    #[tokio::test]
    async fn a_declared_formatter_is_asked_and_never_rewrites() {
        let root = tempfile::tempdir().unwrap();
        let declaring = |argv: Vec<String>| {
            ChangePipeline::new(root.path(), root.path(), false).with_fmt_check_command(Some(argv))
        };

        match declaring(vec!["true".into()])
            .judge_format(root.path())
            .await
            .unwrap()
        {
            FormatVerdict::Conformed { rewritten, note } => {
                assert!(!rewritten, "a declared formatter is a check, not a rewrite");
                assert!(note.contains("true"), "the note names the judge: {note}");
            }
            other => panic!("expected a conformed verdict, got {other:?}"),
        }

        match declaring(vec!["false".into()])
            .judge_format(root.path())
            .await
            .unwrap()
        {
            FormatVerdict::Differs(evidence) => assert!(
                evidence.contains("not asked to rewrite"),
                "the refusal has to say the declared formatter was not asked to rewrite: {evidence}"
            ),
            other => panic!("expected a differing verdict, got {other:?}"),
        }

        // Declaring none is not the same as declaring the built-in one: the
        // stage does not run, and the change's own record carries that fact.
        match declaring(Vec::new())
            .judge_format(root.path())
            .await
            .unwrap()
        {
            FormatVerdict::Conformed { rewritten, note } => {
                assert!(!rewritten);
                assert!(note.contains("no formatter"), "{note}");
            }
            other => panic!("expected a conformed verdict, got {other:?}"),
        }
    }

    /// A declared linter is judged by its exit status, and a refusal it cannot
    /// attribute is not read as a clean tree. The built-in linter is left out of
    /// that reading: its non-zero exit on a tree it cannot parse is a
    /// compilation error for the suite, not a lint refusal.
    #[tokio::test]
    async fn a_declared_linter_decides_and_an_unreadable_refusal_is_not_clean() {
        let root = tempfile::tempdir().unwrap();
        let declaring = |argv: Vec<String>| {
            ChangePipeline::new(root.path(), root.path(), false).with_lint_command(Some(argv))
        };

        match declaring(vec!["true".into()])
            .judge_lint(root.path(), "c")
            .await
            .unwrap()
        {
            LintVerdict::Clean(note) => {
                assert!(note.contains("true"), "the note names the judge: {note}")
            }
            other => panic!("expected a clean verdict, got {other:?}"),
        }

        // `false` speaks nothing, so nothing here can show its refusal is not
        // the change's doing.
        match declaring(vec!["false".into()])
            .judge_lint(root.path(), "c")
            .await
            .unwrap()
        {
            LintVerdict::Unattributable(evidence) => assert!(
                evidence.contains("diagnostic interchange"),
                "the refusal has to name why it cannot be attributed: {evidence}"
            ),
            other => panic!("expected an unattributable verdict, got {other:?}"),
        }

        assert!(matches!(
            declaring(Vec::new())
                .judge_lint(root.path(), "c")
                .await
                .unwrap(),
            LintVerdict::NotDeclared
        ));
    }

    /// A built-in linter that never read the tree is not a tree that lints
    /// clean. The fixture is a manifest cargo refuses before it compiles
    /// anything, so the run exits non-zero with nothing in the compiler's
    /// diagnostic interchange -- the same shape a host without `clippy`
    /// produces, where the empty set of findings is the absence of a
    /// judgement rather than a verdict about the code.
    #[tokio::test]
    async fn a_builtin_linter_that_exits_non_zero_without_a_diagnostic_is_unmeasured() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::write(
            root.path().join("Cargo.toml"),
            "[package]\nname = \"probe\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[package]\n",
        )
        .await
        .unwrap();

        let verdict = ChangePipeline::new(root.path(), root.path(), false)
            .judge_lint(root.path(), "c")
            .await
            .unwrap();
        match verdict {
            LintVerdict::Unmeasured(evidence) => assert!(
                evidence.contains("diagnostic interchange"),
                "the note has to name what was missing rather than report a count: {evidence}"
            ),
            other => panic!("a linter that never read the tree is not a clean tree, got {other:?}"),
        }
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
            tiering: None,
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

    /// A change that adds a file can be taken off and put back on.
    ///
    /// `git reset --hard` restores tracked files only, so the file the change
    /// created stays behind as untracked and the second apply of that same
    /// change fails on it ("already exists"). A change whose suite failed is
    /// exactly the one the pipeline takes back off to read the baseline, so
    /// without this a change that adds a file and fails would get no verdict at
    /// all: retried every cycle, never judged, never retired.
    #[tokio::test]
    async fn a_change_that_adds_a_file_can_be_applied_again_after_a_reset() {
        use tokio::process::Command;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for args in [
            vec!["init"],
            vec!["config", "user.email", "test@test.com"],
            vec!["config", "user.name", "Test"],
        ] {
            Command::new("git")
                .args(args)
                .current_dir(root)
                .output()
                .await
                .unwrap();
        }
        tokio::fs::create_dir_all(root.join("src")).await.unwrap();
        tokio::fs::write(root.join("src/lib.rs"), "fn old() {}\n")
            .await
            .unwrap();
        for args in [vec!["add", "."], vec!["commit", "-m", "initial"]] {
            Command::new("git")
                .args(args)
                .current_dir(root)
                .output()
                .await
                .unwrap();
        }

        let change = "diff --git a/src/added.rs b/src/added.rs\n\
                      new file mode 100644\n\
                      index 0000000..1111111\n\
                      --- /dev/null\n\
                      +++ b/src/added.rs\n\
                      @@ -0,0 +1,2 @@\n\
                      +fn added() {}\n\
                      +fn also() {}\n";

        let pipeline = ChangePipeline::new(root.to_path_buf(), root.join("changes"), true);
        pipeline.git_apply(root, change).await.unwrap();
        pipeline.git_reset_hard(root, change).await.unwrap();
        assert!(
            !root.join("src/added.rs").exists(),
            "the file the change created must not outlive the reset"
        );
        pipeline
            .git_apply(root, change)
            .await
            .expect("the same change must apply a second time, or it can never be judged");
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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

    /// A declared linter that refuses the tree without speaking the compiler's
    /// diagnostic interchange refuses the change, and the refusal says why.
    ///
    /// A finding can only be excused as the tree's own through that interchange:
    /// it is where a diagnostic's span is written down. A linter that refuses a
    /// tree without it has left nothing that separates the findings it reports
    /// from the ones the tree already carried, and reading its silence as "no
    /// findings" would be the reading a linter that never ran produces.
    #[tokio::test]
    async fn a_declared_linter_that_speaks_no_interchange_refuses_the_change() {
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

        // `false` refuses every tree, and its refusal is plain text rather than
        // the interchange, so nothing here can show its findings are the tree's
        // rather than this change's.
        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false)
            .with_lint_command(Some(vec!["false".into()]));
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "opaque-lint-1".into(),
            description: "给取值模块补一个辅助函数".into(),
            content: "diff --git a/src/lib.rs b/src/lib.rs\n\
                      --- a/src/lib.rs\n\
                      +++ b/src/lib.rs\n\
                      @@ -1,3 +1,4 @@\n\
                      \x20pub fn answer() -> i32 {\n\
                      \x20    42\n\
                      \x20}\n\
                      +fn generated() {}\n"
                .into(),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
            tiering: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the change is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::LintRefused)
        );
        assert_eq!(result.new_status, EvolutionStatus::ValidationFailed);
        assert!(
            result.test_output.contains("diagnostic interchange"),
            "the refusal has to say why the linter's word could not be taken: {}",
            result.test_output
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
            tiering: None,
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
    /// principle: feedback_a_re_emission_reading_must_split_by_cause_and_report_the_increment
    ///
    /// The reading is taken on one tree at one revision and keyed by the
    /// failing tests' own names, never by the change id: the causes the
    /// revision already carried are split from the ones this content
    /// introduced, and only the introduced increment is convicted.
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

    // ── 变更档位读数 ────────────────────────────────────────────────

    /// 档位读数真的被生产：五格按 0 摆出，真跑的那两条各自只动一格。
    ///
    /// 也钉住播种只发生一次——两次判定之后仍是五格，而不是十格或「第一格被重置」。
    #[tokio::test]
    async fn the_change_tier_reading_is_produced_and_seeds_every_cell_once() {
        use cog_core::MetricsBackend;

        let root = tempfile::tempdir().unwrap();
        // 一棵带真载体的树：档位要能读出「这条变更动了判据面」。
        let carrier = root.path().join("deploy/scripts");
        tokio::fs::create_dir_all(&carrier).await.unwrap();
        tokio::fs::write(carrier.join("check-x.sh"), "grep -q max_diff_lines\n")
            .await
            .unwrap();

        let metrics = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let pipeline =
            ChangePipeline::new(root.path(), root.path(), true).with_metrics(metrics.clone());
        let name = cog_core::metric_names::CHANGE_TIER_TOTAL.as_str();

        assert!(
            metrics.query_counter_totals(name).await.unwrap().is_empty(),
            "播种前一条序列都没有——「没接线」与「没跑过」在这里同形"
        );

        let code_diff = "diff --git a/crates/cog-core/src/lib.rs b/crates/cog-core/src/lib.rs\n\
index 1111111..2222222 100644\n\
--- a/crates/cog-core/src/lib.rs\n\
+++ b/crates/cog-core/src/lib.rs\n\
@@ -1 +1 @@\n\
-old\n\
+new\n";
        let code_targets = ChangePipeline::parse_diff(code_diff).unwrap();
        let code_tier = pipeline.change_tier(root.path(), &code_targets, code_diff);
        assert_eq!(code_tier.tier, crate::criteria_face::Tier::Tests);
        pipeline.report_change_tier(&code_tier).await;

        let carrier_diff = "diff --git a/deploy/scripts/check-x.sh b/deploy/scripts/check-x.sh\n\
index 1111111..2222222 100644\n\
--- a/deploy/scripts/check-x.sh\n\
+++ b/deploy/scripts/check-x.sh\n\
@@ -1 +1 @@\n\
-grep -q max_diff_lines\n\
+grep -q other_threshold\n";
        let carrier_targets = ChangePipeline::parse_diff(carrier_diff).unwrap();
        let carrier_tier = pipeline.change_tier(root.path(), &carrier_targets, carrier_diff);
        assert_eq!(carrier_tier.tier, crate::criteria_face::Tier::RealGate);
        pipeline.report_change_tier(&carrier_tier).await;

        let cells: std::collections::BTreeMap<String, f64> = metrics
            .query_counter_totals(name)
            .await
            .unwrap()
            .iter()
            .map(|s| (s.labels["tier"].clone(), s.value))
            .collect();
        assert_eq!(
            cells.len(),
            crate::criteria_face::Tier::ALL.len(),
            "播种的是全叉积，且只播一次"
        );
        assert_eq!(cells["tests"], 1.0, "源码变更落到测试档");
        assert_eq!(cells["real_gate"], 1.0, "动载体的变更落到真门档");
        for cell in ["minimal", "structural"] {
            assert_eq!(cells[cell], 0.0, "{cell} 没人走过，读到的必须是 0");
        }
        // `compile` 不在这张表里，不是因为它没人走过，而是因为**没有一条判词能落到它**：
        // 播种一个走不到的格，读到的 0 与「接了线、只是还没人落进去」完全同形，而这两件事
        // 正是这套播种存在的理由要分开的。可达性由 `criteria_face` 的
        // `every_tier_the_reading_publishes_has_a_producer` 逐档钉住。
    }

    /// The dispatch reading has to publish the whole cross product of recipes
    /// and sources on the first dispatch, because the one fact it exists for is
    /// the fallback: a cell that was never seeded is absent, and absent reads
    /// the same as "the built-in has never been used" — the reading a project
    /// that declares nothing would then be invisible behind.
    #[tokio::test]
    async fn the_project_kind_reading_is_produced_and_seeds_every_cell_once() {
        use cog_core::MetricsBackend;

        let root = tempfile::tempdir().unwrap();
        let metrics = std::sync::Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let pipeline =
            ChangePipeline::new(root.path(), root.path(), true).with_metrics(metrics.clone());
        let name = cog_core::metric_names::PROJECT_KIND_TOTAL.as_str();

        assert!(
            metrics.query_counter_totals(name).await.unwrap().is_empty(),
            "播种前一条序列都没有——「没接线」与「没跑过」在这里同形"
        );

        pipeline
            .report_project_kind(
                cog_core::ProjectKind::Rust,
                cog_core::ProjectKindSource::Declared,
            )
            .await;

        let cells: std::collections::BTreeMap<String, f64> = metrics
            .query_counter_totals(name)
            .await
            .unwrap()
            .iter()
            .map(|s| {
                (
                    format!("{}/{}", s.labels["kind"], s.labels["source"]),
                    s.value,
                )
            })
            .collect();
        assert_eq!(
            cells.len(),
            cog_core::ProjectKind::ALL.len() * cog_core::ProjectKindSource::ALL.len(),
            "播种的是全叉积，且只播一次"
        );
        assert_eq!(cells["rust/declared"], 1.0, "项目自己声明的那一跳");
        assert_eq!(
            cells["rust/builtin"], 0.0,
            "没人走内置时读到的必须是 0，不能是缺席"
        );
    }

    /// A project that carries a declaration and cannot say what it is does not
    /// land. The refusal is a judgement about the deployment rather than about
    /// the change, so it comes back as a verdict and not as an environment
    /// error: a `Err` here would be retried on every pass and would hold the
    /// change forever instead of reporting the project's document once.
    #[tokio::test]
    async fn a_project_whose_kind_declaration_cannot_be_read_is_refused() {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        tokio::fs::write(root.path().join("a.txt"), "old\n")
            .await
            .unwrap();
        tokio::fs::write(
            root.path().join(cog_core::PROJECT_KIND_DOCUMENT),
            r#"{"kind": "cobol"}"#,
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "misdeclared-1".into(),
            description: "调整这一行的取值".into(),
            content: rewrites("a.txt"),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
            tiering: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the project is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::ProjectKindUnusable)
        );
        assert_eq!(result.new_status, EvolutionStatus::Rejected);
        // The refusal names what the project wrote and what would have worked,
        // because the one reading an operator has is this string.
        assert!(
            result.test_output.contains("cobol") && result.test_output.contains("rust"),
            "拒绝的理由没把写下的和能用的都说出来：{}",
            result.test_output
        );
    }

    /// The document the kind is declared in is one a change may not rewrite.
    /// That floor is what makes reading it before the apply enough: the
    /// dispatch reads the pre-change revision, and the path it reads is one no
    /// diff can move in the same run. A name here that the floor does not carry
    /// would leave the dispatch reading a file the change could rewrite first.
    #[tokio::test]
    async fn the_document_the_kind_is_declared_in_is_one_a_change_may_not_rewrite() {
        let root = tempfile::tempdir().unwrap();
        git_ok(root.path(), &["init", "-q"]).await;
        git_ok(root.path(), &["config", "user.email", "t@t.com"]).await;
        git_ok(root.path(), &["config", "user.name", "t"]).await;
        tokio::fs::write(
            root.path().join(cog_core::PROJECT_KIND_DOCUMENT),
            "{\"kind\": \"cobol\"}\n",
        )
        .await
        .unwrap();
        git_ok(root.path(), &["add", "."]).await;
        git_ok(root.path(), &["commit", "-q", "-m", "seed"]).await;

        let pipeline = ChangePipeline::new(root.path(), root.path().join("changes"), false);
        let change = EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: "self-serving-1".into(),
            description: "把项目的 kind 改成 rust".into(),
            content: format!(
                "diff --git a/{doc} b/{doc}\n\
                 --- a/{doc}\n\
                 +++ b/{doc}\n\
                 @@ -1 +1 @@\n\
                 -{{\"kind\": \"cobol\"}}\n\
                 +{{\"kind\": \"rust\"}}\n",
                doc = cog_core::PROJECT_KIND_DOCUMENT
            ),
            status: EvolutionStatus::CompileChecked,
            created_at: chrono::Utc::now(),
            eval_summary: None,
            tiering: None,
        };

        let result = pipeline
            .apply_and_test_in(&change, root.path())
            .await
            .expect("a judgement about the project is not an environment error");

        assert_eq!(
            result.verdict,
            ChangeVerdict::Refused(cog_core::RejectionCause::ForbiddenPath),
            "声明文档必须落在受保护清单里，否则一条变更可以先改写它、再让分派按改写后的判它：{:?}",
            result.test_output
        );
    }

    /// The workflow whose gating jobs the verification stage answers for.
    /// 仓库根下的这一条；根由 [`repo_root`] 算。
    const CI_WORKFLOW: &str = ".github/workflows/ci.yml";

    /// The job the stage's built-in linter answers.
    ///
    /// Named rather than searched for: the block is read out of this job, and
    /// finding a `clippy` somewhere else in the file would turn the assertion
    /// below into one that judges another job's command.
    const CI_LINT_JOB: &str = "clippy";

    /// What the stage adds to that command to read the run rather than to change
    /// it. Everything else in [`BUILTIN_LINT`] has to be the job's.
    const STAGE_ONLY_LINT_ARGS: &[&str] = &["--message-format=json"];

    /// The tokens the argv carries and the evidence line does not: the two ways
    /// this stage turns a run into a reading, the flag that makes one run report
    /// every crate that failed instead of stopping at the first, and the
    /// promotion that turns a finding into a refusal. A reader of the evidence is
    /// being told which command read the tree; `--` and `-D warnings` are the
    /// policy, not the command, and `--keep-going` is how much of the tree the
    /// one run reports, not what it says about any line.
    const NOT_IN_THE_EVIDENCE_LINE: &[&str] = &[
        "--message-format=json",
        "--keep-going",
        "--",
        "-D",
        "warnings",
    ];

    /// The `clippy` job's body, read out of the workflow.
    ///
    /// The block runs from the job's own line to the next line that is a job,
    /// which is the next non-blank, non-comment line indented two spaces.
    /// Refusing to find it is a failure rather than an empty block: a parse that
    /// reads nothing agrees with everything, and this file's whole subject is a
    /// command that quietly stops matching.
    fn ci_lint_job_block(workflow: &str) -> &str {
        let header = format!("\n  {CI_LINT_JOB}:\n");
        let body = workflow
            .split_once(&header)
            .unwrap_or_else(|| {
                panic!("{CI_WORKFLOW} has no `{CI_LINT_JOB}` job at two spaces of indent")
            })
            .1;
        let end = body
            .lines()
            .take_while(|line| {
                if line.trim().is_empty() || line.trim_start().starts_with('#') {
                    return true;
                }
                line.len() - line.trim_start().len() > 2
            })
            .map(|line| line.len() + 1)
            .sum::<usize>();
        &body[..end]
    }

    /// The stage's linter is the CI job's linter, flags and all.
    ///
    /// The two commands are spelled in two files and nothing else compares them,
    /// so a flag added to one is invisible from the other: CI gets a scope the
    /// stage never exercises, and the difference shows only as a change that
    /// passes verification, is committed, is built in release and dies in the
    /// job — the arrival this stage exists to make unnecessary. That is not
    /// hypothetical: the job gained `--all-targets` on 2026-10-06 and the stage
    /// did not, and on 2026-10-15 a change whose only offence was a `dead_code`
    /// field in a shared test helper was green here and red there.
    ///
    /// Read from the workflow rather than restated, so a rename or a re-scoping
    /// of the job lands here on the next run instead of licensing a linter that
    /// no longer matches. Comparison is by token multiset: the order cargo
    /// accepts is the job's business, membership is the claim.
    #[test]
    fn the_stages_linter_is_the_ci_jobs_linter() {
        let path = repo_root().join(CI_WORKFLOW);
        let workflow = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()));
        let block = ci_lint_job_block(&workflow);

        let commands: Vec<&str> = block
            .lines()
            .filter_map(|line| line.trim().strip_prefix("- run: "))
            .collect();
        assert_eq!(
            commands.len(),
            1,
            "the `{CI_LINT_JOB}` job has {} run steps, so this test cannot tell which \
             command the stage is supposed to answer: {commands:?}",
            commands.len()
        );
        let job_argv: Vec<&str> = commands[0].split_whitespace().collect();
        assert!(
            job_argv.first() == Some(&"cargo") && job_argv.contains(&"clippy"),
            "the `{CI_LINT_JOB}` job's only run step is {job_argv:?}, which is not cargo \
             clippy; the job this test reads is not the one it names"
        );

        // Both sides as multisets, with the stage's own argument taken out of
        // its side rather than added to the job's: what is being asserted is
        // that the stage runs the job's command, and the one thing it adds is
        // the run's format.
        let mut expected: Vec<&str> = job_argv.clone();
        expected.sort_unstable();
        let mut actual: Vec<&str> = BUILTIN_LINT
            .iter()
            .copied()
            .filter(|arg| !STAGE_ONLY_LINT_ARGS.contains(arg))
            .collect();
        actual.sort_unstable();
        assert_eq!(
            actual, expected,
            "the verification stage lints with {BUILTIN_LINT:?}, which is not what CI's \
             `{CI_LINT_JOB}` job runs ({job_argv:?}) once this stage's own \
             {STAGE_ONLY_LINT_ARGS:?} is taken out. A change that fails only what the job \
             checks and the stage does not passes verification, is committed, is built in \
             release, and is refused after all that — which is the cost this equality exists \
             to avoid."
        );

        // And the line the change's own record carries names that command: it is
        // the argv with the report's formatting and the verdict's policy taken
        // out, and nothing else, so a flag added above cannot leave the evidence
        // describing a different command than the one that ran.
        let line_tokens: Vec<&str> = BUILTIN_LINT_LINE.split_whitespace().collect();
        let line_expected: Vec<&str> = BUILTIN_LINT
            .iter()
            .copied()
            .filter(|arg| !NOT_IN_THE_EVIDENCE_LINE.contains(arg))
            .collect();
        assert_eq!(
            line_tokens, line_expected,
            "BUILTIN_LINT_LINE is {BUILTIN_LINT_LINE:?}, which is not {BUILTIN_LINT:?} with \
             {NOT_IN_THE_EVIDENCE_LINE:?} taken out; the evidence a change carries would \
             name a command the stage did not run"
        );
    }

    fn targets(paths: &[&str]) -> Vec<cog_core::DiffTarget> {
        paths
            .iter()
            .map(|p| cog_core::DiffTarget {
                path: (*p).to_string(),
                kind: cog_core::DiffTargetKind::Modify,
            })
            .collect()
    }

    /// The change the admission tests apply: one field of a tracked file.
    ///
    /// A tracked file rather than a new one, because the reversal reads
    /// `git diff HEAD` and a new file that was never staged shows up only in
    /// the untracked list — the path a test should exercise is the ordinary one.
    const PATCH: &str = "diff --git a/deploy/helm/x.yaml b/deploy/helm/x.yaml\n\
                         --- a/deploy/helm/x.yaml\n\
                         +++ b/deploy/helm/x.yaml\n\
                         @@ -1 +1 @@\n\
                         -x: 1\n\
                         +x: 2\n";

    /// A worktree that already holds [`PATCH`], so that "can this be taken
    /// back out" has a tree to answer about.
    fn worktree_holding_the_change(dir: &std::path::Path) {
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(args)
                .current_dir(dir)
                .output()
                .expect("git should be on PATH for these tests");
            assert!(
                out.status.success(),
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "tests@cogneva.invalid"]);
        git(&["config", "user.name", "admission tests"]);
        std::fs::create_dir_all(dir.join("deploy/helm")).unwrap();
        std::fs::write(dir.join("deploy/helm/x.yaml"), "x: 1\n").unwrap();
        git(&["add", "deploy/helm/x.yaml"]);
        git(&["commit", "-q", "-m", "base"]);
        std::fs::write(dir.join("change.patch"), PATCH).unwrap();
        git(&["apply", "change.patch"]);
        std::fs::remove_file(dir.join("change.patch")).unwrap();
    }

    /// 拓扑族按目录分量认，不按前缀认。
    #[test]
    fn the_deploy_family_is_a_directory_not_a_prefix() {
        let family = ChangePipeline::deploy_targets(&targets(&[
            "deploy/helm/cogneva/values.yaml",
            "deploy/k3s/nats-deployment.yaml",
            "deploy2/helm/values.yaml",
            "crates/x/src/deploy/mod.rs",
            "deploy/../.git/config",
            "deploy/",
        ]));
        assert_eq!(
            family,
            vec![
                "deploy/helm/cogneva/values.yaml",
                "deploy/k3s/nats-deployment.yaml"
            ],
            "只有真正落在 deploy/ 下的路径属于这一族"
        );
    }

    /// 没有声明条件的族不转人工：拒收，并点名缺的是哪份声明。
    #[tokio::test]
    async fn a_topology_change_without_a_declared_condition_is_refused_by_name() {
        let root = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path(), false);
        let reason = pipeline
            .deploy_admission_reason(
                root.path(),
                &targets(&["deploy/helm/cogneva/values.yaml"]),
                PATCH,
            )
            .await
            .unwrap()
            .expect("a family with no executable condition has to be refused");

        assert!(
            reason.contains("deploy/helm/cogneva/values.yaml"),
            "拒收要点名是哪一族：{reason}"
        );
        assert!(
            reason.contains("deploy_admission"),
            "拒收要说清楚下一个动作是改哪份声明：{reason}"
        );
        assert!(
            reason.contains("不转人工"),
            "这一档不是「等人批」的另一种写法：{reason}"
        );
    }

    /// 声明了就照跑：全过即准入，一条不过即拒收，理由里带着那条命令。
    #[tokio::test]
    async fn a_declared_condition_decides_the_family_by_its_exit_status() {
        let root = tempfile::tempdir().unwrap();
        worktree_holding_the_change(root.path());
        let passing = ChangePipeline::new(root.path(), root.path(), false).with_deploy_admission(
            Some(cog_core::DeployAdmissionConfig {
                commands: vec![vec!["true".into()]],
                server_dry_run: None,
            }),
        );
        assert_eq!(
            passing
                .deploy_admission_reason(root.path(), &targets(&["deploy/helm/x.yaml"]), PATCH)
                .await
                .unwrap(),
            None,
            "命令全过、且这一笔能被倒回再应用，就该准入"
        );
        // 回滚走过一遍之后，树必须还拿着这一笔：准入不是「把它撤掉了」。
        assert_eq!(
            std::fs::read_to_string(root.path().join("deploy/helm/x.yaml")).unwrap(),
            "x: 2\n",
            "回滚证明之后必须把这一笔应用回来"
        );

        let failing = ChangePipeline::new(root.path(), root.path(), false).with_deploy_admission(
            Some(cog_core::DeployAdmissionConfig {
                commands: vec![vec!["false".into()]],
                server_dry_run: None,
            }),
        );
        let reason = failing
            .deploy_admission_reason(root.path(), &targets(&["deploy/helm/x.yaml"]), PATCH)
            .await
            .unwrap()
            .expect("a failing command is a refusal");
        assert!(
            reason.contains("false"),
            "拒收要点名是哪条命令没过：{reason}"
        );
    }

    /// 服务端 dry-run 逐文件跑在变更触及的那份渲染产物上，命令由部署声明。
    #[tokio::test]
    async fn the_server_dry_run_judges_only_the_rendered_files_the_change_touched() {
        let root = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path(), false).with_deploy_admission(
            Some(cog_core::DeployAdmissionConfig {
                commands: vec![vec!["true".into()]],
                server_dry_run: Some(vec!["false".into()]),
            }),
        );

        let reason = pipeline
            .deploy_admission_reason(
                root.path(),
                &targets(&["deploy/rendered/k3s-single/30-service-cogneva.yaml"]),
                PATCH,
            )
            .await
            .unwrap()
            .expect("the dry run refused");
        assert!(
            reason.contains("deploy/rendered/k3s-single/30-service-cogneva.yaml"),
            "拒收要点名是哪份产物：{reason}"
        );

        // 只改 chart、没动渲染产物时，dry-run 没有可问的对象，不该自己造一个。
        let tree = tempfile::tempdir().unwrap();
        worktree_holding_the_change(tree.path());
        assert_eq!(
            pipeline
                .deploy_admission_reason(tree.path(), &targets(&["deploy/helm/x.yaml"]), PATCH)
                .await
                .unwrap(),
            None,
            "没有渲染产物被触及就不跑逐文件 dry-run"
        );
    }

    /// 命令全过也不算准入：倒不回去的树不收。
    ///
    /// 判的是「这条路走过一次」，不是「补丁看起来能反向打」——所以这里用一棵**不是
    /// git 工作树**的目录：`git diff` 跑不了，回滚路径无从执行，这一族就拒收。
    #[tokio::test]
    async fn a_family_the_tree_cannot_give_back_is_refused_even_when_every_command_passes() {
        let root = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path(), false).with_deploy_admission(
            Some(cog_core::DeployAdmissionConfig {
                commands: vec![vec!["true".into()]],
                server_dry_run: None,
            }),
        );
        let reason = pipeline
            .deploy_admission_reason(root.path(), &targets(&["deploy/helm/x.yaml"]), PATCH)
            .await
            .unwrap()
            .expect("a tree with no rollback path is a refusal");
        assert!(
            reason.contains("回滚"),
            "拒收要点名缺的是回滚路径：{reason}"
        );
    }

    /// 别的族不该被这条闸碰到。
    #[tokio::test]
    async fn a_change_outside_the_family_is_not_judged_by_its_conditions() {
        let root = tempfile::tempdir().unwrap();
        let pipeline = ChangePipeline::new(root.path(), root.path(), false);
        assert_eq!(
            pipeline
                .deploy_admission_reason(root.path(), &targets(&["crates/x/src/lib.rs"]), PATCH)
                .await
                .unwrap(),
            None
        );
    }
}
