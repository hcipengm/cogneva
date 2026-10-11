//! 一条变更的一次执行：进程边界这一侧的交付与那一侧的回程。
//!
//! 构建槽位被宿主拒绝时，回程以 `UnavailableCause::NoBuildSlot` 过边界——这条变更没被判定过，不记成变更失败。
//! 执行从常驻进程里搬出来之后，判定与产物都必须跨过一次进程边界，而边界两边是
//! 两个互不知情的进程：一边写完就退出，另一边只看得见文件与退出码。这里把那条
//! 边界写成类型，并且把「看不见」的几种情形都变成显式的坏读，而不是各自落进某个
//! 默认值：
//!
//! - 请求只有一份正文（`EvolutionResult` 本身就带 diff 文本）。`change_dir` 仍是
//!   发现变更的入口，但不再是跨进程的投递口——投递走按变更 id 命名的一份文件。
//! - 回程走两条通道：**类别走退出码**（通过 / 变更被拒 / 环境不可用），细节走结果
//!   文件。退出码只有几档，「坏在哪一处」塞不进去，所以两条都要有；这个形状与
//!   `cogneva mainline-rollout` 已经用的那条一致，不新立协议。
//! - 结果文件**缺失或半写一律不是成功**。原子写（临时文件 + rename）保证读端读
//!   不到半个 JSON；而读不到就是读不到——退出码与结果文件对不上时按不可用处理，
//!   不挑一个信。
//!
//! 判定与策略的分界：执行侧只报告它**观测到了什么**（判词、原因、产物、各阶段
//! 的耗时与超时计数），不决定这条变更要不要退休、要不要记一次失败——那属于常驻
//! 进程的账本与阈值。所以这里的每个字段都是"事实"，没有一个字段是"结论"。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cog_core::{EvolutionIntent, FaultClassifier, SFError, SFResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use crate::change_pipeline::{ChangePipeline, ChangeVerdict};
use crate::config::{ChangeJobConfig, PromotionGateConfig};
use crate::evolution_deployer::EvolutionDeployer;
use crate::types::{EvolutionResult, EvolutionStatus};
use crate::verification_budget::{VerificationBudget, KIND_BUILD, KIND_TEST};
use crate::workspace::{BaseRef, WorkspaceManager};

/// 请求文件与结果文件的固定名字。两侧按同一个目录交付，名字只此一处。
pub const REQUEST_FILE: &str = "request.json";
pub const OUTCOME_FILE: &str = "outcome.json";

/// 执行到位并给出了判定：变更通过，产物在结果文件里。
pub const EXIT_PASSED: i32 = 0;
/// 执行到位并给出了判定：变更被拒，判词与原因在结果文件里。
pub const EXIT_REFUSED: i32 = 1;
/// 没能做出判定：工作树、git、cargo 或构建闸门没能让这次执行跑完。
///
/// 与变更本身无关，也不含任何结论——调用方据此把变更留在队列里重试，而不是记成
/// 一次失败。取 75（EX_TEMPFAIL），与一次性滚动入口的环境类退出码同源。
pub const EXIT_UNAVAILABLE: i32 = 75;

/// 容器终止消息（kubelet 的 `terminationMessagePath` 默认值）。类别在退出码上，
/// 这里放的是给人看的落点；写不进去不影响类别。
const TERMINATION_LOG: &str = "/dev/termination-log";

/// 回程的类别。它就是退出码，不另立一套词汇。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCategory {
    Passed,
    Refused,
    /// 没能做出判定。认不出的退出码也归到这里：一档认不出的码最可能来自一个
    /// 根本没跑起来的进程（容器运行时起不来会用 128 一类），把读不懂的码读成
    /// 判定，等于凭空造一个结论出来。
    Unavailable,
}

impl ExecutionCategory {
    pub fn of_exit_code(code: i32) -> Self {
        match code {
            EXIT_PASSED => Self::Passed,
            EXIT_REFUSED => Self::Refused,
            _ => Self::Unavailable,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Refused => "refused",
            Self::Unavailable => "unavailable",
        }
    }
}

/// 某一类运行的时间读数与它被预算杀掉的次数。
///
/// 跨进程搬运它是为了让常驻进程的 `cogneva_verification_*` 族在执行搬走之后仍然
/// 覆盖这些运行：读数跟着工作一起搬，不随着进程留下。同一个量、同一个标签，不是
/// 另造一个近似值顶上去。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReading {
    /// 这类运行最近一次的花费；从未跑过是 `None`。0 秒是一次真实读数，与
    /// 「没跑过」必须分开。
    #[serde(default)]
    pub last_secs: Option<u64>,
    #[serde(default)]
    pub timeouts: u64,
}

impl RunReading {
    pub fn of(budget: &VerificationBudget, kind: &str) -> Self {
        Self {
            last_secs: budget.last_run_secs(kind),
            timeouts: budget.timeouts(kind),
        }
    }
}

/// 一条变更跑起来需要的那份世界：路径、预算、策略。
///
/// 它是**解析后的值**，不是「让执行进程自己去读配置」：晋级门策略、两个超时
/// 预算、构建闸门目录，在这里都是常驻进程启动时解析好的那一份。让执行进程重新
/// 解析等于给同一份策略造第二个解析器，而两个进程的 env 只要差一项，同一条变更
/// 就会得到两个判词——判词不一致时无从判断哪个是对的。
///
/// 与变更本体分开，是因为这半**不随变更变**：一个批次构造一份，逐条变更只填自己
/// 的正文、基线与入口。而它必须与流水线、部署器取同一份值——它们各自构造一次，
/// 两处只要差一项，子进程就站在另一个世界里。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeExecutionWorld {
    pub project_root: PathBuf,
    pub change_dir: PathBuf,
    pub workspace_root: PathBuf,
    pub bare_repo: PathBuf,
    pub target_dir: PathBuf,
    pub binary_dir: PathBuf,
    pub backup_dir: PathBuf,
    pub test_timeout_secs: u64,
    /// The command that judges a change. Travels in the world rather than being
    /// read from the config again on the executing side: a request is built and
    /// run by two processes, and a second read of the config is a second value
    /// that can differ from the one the request was built against.
    #[serde(default = "cog_core::SelfEvolutionConfig::default_test_command")]
    pub test_command: Vec<String>,
    pub build_timeout_secs: u64,
    pub auto_apply: bool,
    pub manual_approve: bool,
    pub promotion: PromotionGateConfig,
    pub build_gate: cog_core::BuildGateConfig,
}

impl ChangeExecutionWorld {
    /// 把一条变更放进这份世界里，得到一份**完整**的请求：没有任何一项留给执行进程
    /// 去猜、或去配置里补。三个字段由调用方给全——变更本体、它要站上去的那个提交、
    /// 以及这条变更来自哪个入口（执行侧只有这一条记录，没有别处可查，而它决定这次
    /// 构建记在谁的账上）。
    pub fn request(
        &self,
        change: EvolutionResult,
        base: impl Into<String>,
        intent: Option<EvolutionIntent>,
    ) -> ChangeExecutionRequest {
        ChangeExecutionRequest {
            change,
            base: base.into(),
            world: self.clone(),
            intent,
        }
    }

    /// 执行进程会读写的共享路径，带一句它是什么（拒绝派发时要报告是哪一个）。
    pub fn shared_paths(&self) -> Vec<(&'static str, &Path)> {
        vec![
            ("the project root", self.project_root.as_path()),
            ("the change directory", self.change_dir.as_path()),
            ("the workspace root", self.workspace_root.as_path()),
            ("the bare repository", self.bare_repo.as_path()),
            ("the shared target directory", self.target_dir.as_path()),
            ("the binary staging directory", self.binary_dir.as_path()),
            ("the backup directory", self.backup_dir.as_path()),
        ]
    }
}

/// 交付给执行进程的一份请求：一条变更，加上它跑起来需要的那份世界。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeExecutionRequest {
    /// 变更本体，正文（diff 文本）在 `content` 里。
    pub change: EvolutionResult,
    /// 这棵工作树的起点（`git worktree add` 的 rev）。常驻进程把**派发这一刻**自己
    /// 那棵树上的 HEAD 传进来，执行侧因此与它站在同一个基线上。
    pub base: String,
    /// 世界单独一层：读这份文件的人一眼看得出哪半随变更走、哪半不随。
    pub world: ChangeExecutionWorld,
    /// 这条变更来自哪个入口。
    #[serde(default)]
    pub intent: Option<EvolutionIntent>,
}

/// 判词。与执行侧内部的 `ChangeVerdict` 一一对应，只是可序列化。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "cause")]
pub enum OutcomeVerdict {
    Passed,
    Refused(cog_core::RejectionCause),
}

/// 通过那一步的产物：提交、暂存好的二进制、构建耗时。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutedArtifact {
    pub commit_hash: String,
    pub new_binary_path: PathBuf,
    pub build_duration_secs: u64,
}

/// 没能做出判定的原因类别。调用方按类别归因——「宿主没给槽位」与「变更让构建失败」
/// 要落到两个不同的读数上（前者什么都没失败，后者要记一次变更失败），把它们合并
/// 成一团文本就是把归因丢掉。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableCause {
    /// 整个等待预算里没拿到构建槽位。这条变更没被判定过，也没有任何东西失败。
    NoBuildSlot,
    /// 别的环境问题：工作树、git、cargo 没能让这一步跑起来。
    Environment,
}

/// 没做出判定的那一步是验证还是构建。调用方按它写回自己的读数——「管线没能对变更
/// 做出判定」与「构建失败」在常驻进程里是两条不同的话、两笔不同的账，所以这一步也
/// 得过边界，不能由调用方猜。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableStage {
    Verification,
    Build,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unavailable {
    pub cause: UnavailableCause,
    pub stage: UnavailableStage,
    /// 原文，给人读的。判据是上面两个字段，不是这段文本——跨进程的措辞不该被解析。
    pub reason: String,
}

/// 回程的全部细节。
///
/// `verdict` 为 `None` 只出现在环境不可用那条路上：那条变更**没有被判定**，不是被
/// 判否。两者共用一个「非通过」的形状，会让一次环境抖动读成一次否决。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeExecutionOutcome {
    pub change_id: String,
    #[serde(default)]
    pub verdict: Option<OutcomeVerdict>,
    #[serde(default)]
    pub new_status: Option<EvolutionStatus>,
    #[serde(default)]
    pub files_changed: Vec<PathBuf>,
    #[serde(default)]
    pub test_output: String,
    #[serde(default)]
    pub reformatted: bool,
    #[serde(default)]
    pub pre_existing_failures: usize,
    /// 这条变更被路由到哪些门，随回程一起搬回常驻进程。
    ///
    /// 档位是从一棵 checkout 上读出来的、只读一次：执行侧算过之后，消费侧再算
    /// 一次就是在另一棵（可能已被下游改写过的）树上读第二次，两个读数会不一致，
    /// 而没有任何一行代码变化。所以它作为数据搬，不重算。
    #[serde(default)]
    pub tiering: Option<crate::criteria_face::Tiering>,
    #[serde(default)]
    pub artifact: Option<ExecutedArtifact>,
    #[serde(default)]
    pub unavailable: Option<Unavailable>,
    /// 这次构建是怎么结束的（跑完 / 失败 / 被预算杀 / 压根没起来）。只有执行侧
    /// 知道这件事，而按它分类的那个读数族在常驻进程里——所以结局随回程一起搬，
    /// 在那边记进同一个族。`None` 表示这次执行没有走到构建那一步。
    #[serde(default)]
    pub build_ending: Option<crate::evolution_build_readings::BuildEnding>,
    /// 判定已出但没有产物，因为这条变更在等人工批准：管线按 `auto_apply` /
    /// `manual_approve` 把树回滚了，这条路上本来就没有构建。它与「该有产物却没有」
    /// 是两回事，所以显式记一笔——读回时缺产物必须能与坏读分开。
    #[serde(default)]
    pub held_for_approval: bool,
    #[serde(default)]
    pub test: RunReading,
    #[serde(default)]
    pub build: RunReading,
}

impl ChangeExecutionOutcome {
    /// 一个还没有任何观测的空结果。`change_id` 一拿到就先填上：执行侧后面每一步
    /// 都可能失败，而失败时的结果文件也得说得清它是谁的。
    pub fn empty(change_id: &str) -> Self {
        Self {
            change_id: change_id.to_string(),
            verdict: None,
            new_status: None,
            files_changed: Vec::new(),
            test_output: String::new(),
            reformatted: false,
            pre_existing_failures: 0,
            tiering: None,
            artifact: None,
            unavailable: None,
            build_ending: None,
            held_for_approval: false,
            test: RunReading::default(),
            build: RunReading::default(),
        }
    }

    /// 回程的那份判定，形状与进程内执行给出的那一份相同。
    ///
    /// 消费那一侧因此只有一条路：它拿到的永远是 `ApplyResult`，从哪里来不影响它
    /// 怎么判。「执行在哪个进程里发生」是这一项要改的事，而它不该改变任何一条消费
    /// 规则——需要改的只是产物的来源（成功那条路上，产物已经随回程带回来了）。
    pub fn apply_result(&self) -> SFResult<crate::change_pipeline::ApplyResult> {
        let verdict = match self.verdict {
            Some(OutcomeVerdict::Passed) => ChangeVerdict::Passed,
            Some(OutcomeVerdict::Refused(cause)) => ChangeVerdict::Refused(cause),
            None => {
                return Err(SFError::IO(format!(
                    "change {} reached no verdict; there is no apply result to consume",
                    self.change_id
                )))
            }
        };
        Ok(crate::change_pipeline::ApplyResult {
            change_id: self.change_id.clone(),
            files_changed: self.files_changed.clone(),
            verdict,
            test_output: self.test_output.clone(),
            new_status: self.new_status.unwrap_or(EvolutionStatus::Generated),
            reformatted: self.reformatted,
            pre_existing_failures: self.pre_existing_failures,
            tiering: self.tiering.clone(),
        })
    }

    /// 回程里那件产物，形状与本进程构建出来的那一件相同。
    ///
    /// 只在 `interpret` 认过的回程上调用它：通过那条路上 `interpret` 已经保证
    /// 「有产物」与「在等人工审批」恰好一个成立，这里就不再判一遍——判据只有一份，
    /// 判两次就有了两份会各自漂移的判据。`None` 因此只意味着"这条通过停在审批上"。
    pub fn build_artifact(&self) -> Option<crate::evolution_deployer::BuildArtifact> {
        self.artifact
            .as_ref()
            .map(|a| crate::evolution_deployer::BuildArtifact {
                change_id: self.change_id.clone(),
                commit_hash: a.commit_hash.clone(),
                new_binary_path: a.new_binary_path.clone(),
                build_duration_secs: a.build_duration_secs,
            })
    }
}

/// 原子写：同目录临时文件 + rename。
///
/// 读端因此只可能读到完整的旧内容或完整的新内容。半写的 JSON 会被读成「解析失败」，
/// 而解析失败与成功之间必须隔着一个明确的错误——不能靠「反正能 parse 出来」。
///
/// 不 fsync：写端与读端在同一个内核上（共享卷是同节点的一个目录），rename 当场对
/// 另一个进程可见，耐用性边界不是断电。
pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> SFResult<()> {
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| SFError::IO(format!("serialize {}: {e}", path.display())))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| SFError::IO(format!("create {}: {e}", parent.display())))?;
    }
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, &body)
        .map_err(|e| SFError::IO(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        SFError::IO(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            path.display()
        ))
    })
}

/// 读回一份 JSON。读不到就报错——缺席不是「没有结论」，它是「这份读数不存在」，
/// 只能由调用方按类别决定怎么处理。
pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> SFResult<T> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| SFError::IO(format!("read {}: {e}", path.display())))?;
    serde_json::from_str(&text).map_err(|e| SFError::IO(format!("parse {}: {e}", path.display())))
}

/// 一次执行的回程：类别是判据，细节（有的话）是证据。
#[derive(Debug, Clone)]
pub struct ExecutionReturn {
    pub category: ExecutionCategory,
    pub outcome: Option<ChangeExecutionOutcome>,
}

/// 把「退出码 + 结果文件」读成一次回程。
///
/// 三条规则，都是为了让坏读落在一侧固定的一面：
///
/// - 通过与被拒**必须**有结果文件，且判词与退出码一致。文件缺失、读不动、判词对
///   不上，一律 `Err`——调用方按「没做出判定」处理，把变更留在队列里。不能反过来：
///   读不到就当成失败，会把一个可能通过的变更报销掉；读不到就当成通过，会把一个
///   没有产物的「通过」送进落地。
/// - 通过必须带**恰好一样**东西：带非空提交号的产物，或者一条「在等人工批准」的
///   记录。前者是构建出来的，后者那条路上按定义没有构建（管线把树回滚了）。两样都
///   没有的通过、两样都有的通过，都是坏读。
/// - 环境不可用**不需要**结果文件：执行进程可能连请求都没读进去（那时候它连
///   change_id 都还不知道），而类别本身已经说清了该做什么。有文件就带上，它里面的
///   原因是诊断用的。
pub fn interpret(exit_code: i32, outcome_path: &Path) -> SFResult<ExecutionReturn> {
    let category = ExecutionCategory::of_exit_code(exit_code);
    let outcome = match read_json::<ChangeExecutionOutcome>(outcome_path) {
        Ok(outcome) => outcome,
        Err(e) => {
            if category == ExecutionCategory::Unavailable {
                return Ok(ExecutionReturn {
                    category,
                    outcome: None,
                });
            }
            return Err(SFError::IO(format!(
                "exit code {exit_code} says {} but the outcome could not be read: {e}",
                category.as_str()
            )));
        }
    };

    let mismatch = |detail: &str| {
        SFError::IO(format!(
            "exit code {exit_code} says {} but the outcome file says {detail}",
            category.as_str()
        ))
    };
    match category {
        ExecutionCategory::Passed => {
            if outcome.verdict != Some(OutcomeVerdict::Passed) {
                return Err(mismatch(&format!("{:?}", outcome.verdict)));
            }
            match (&outcome.artifact, outcome.held_for_approval) {
                (Some(artifact), false) => {
                    if artifact.commit_hash.is_empty() {
                        return Err(mismatch("passed with an empty commit hash"));
                    }
                }
                (None, true) => {}
                (Some(_), true) => {
                    return Err(mismatch(
                        "passed with an artifact and held for approval: a held change is not built",
                    ))
                }
                (None, false) => return Err(mismatch("passed with no artifact")),
            }
        }
        ExecutionCategory::Refused => {
            if !matches!(outcome.verdict, Some(OutcomeVerdict::Refused(_))) {
                return Err(mismatch(&format!("{:?}", outcome.verdict)));
            }
            if outcome.held_for_approval {
                return Err(mismatch("refused and held for approval at once"));
            }
        }
        ExecutionCategory::Unavailable => {
            if outcome.verdict.is_some() {
                return Err(mismatch(&format!(
                    "a verdict ({:?}) on the path where nothing was judged",
                    outcome.verdict
                )));
            }
        }
    }

    Ok(ExecutionReturn {
        category,
        outcome: Some(outcome),
    })
}

/// 一条变更一条 Job 的名字：`cogneva-change-<slug>-<digest8>`。
///
/// 名字必须**只由变更 id 决定**：派发方重启后重派同一条变更要落到同一个名字上，
/// 否则 `kubectl apply` 会在旁边建出第二条 Job，同一条变更被执行两次。slug 只是
/// 给人看的，判据是末尾那个摘要——id 里的字符（斜杠、点、大写）在 k8s 名字里非法，
/// slug 会把它们折叠掉，两条不同的 id 因此可能撞到同一个 slug 上。
pub fn change_job_name(change_id: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(change_id.as_bytes()));
    let mut slug = String::with_capacity(change_id.len());
    for c in change_id.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug: String = slug.trim_matches('-').chars().take(28).collect();
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        format!("cogneva-change-{}", &digest[..8])
    } else {
        format!("cogneva-change-{slug}-{}", &digest[..8])
    }
}

/// 执行一条变更：建树、应用、验证、提交、构建、暂存，然后把判定写回调用方。
///
/// 这里不做任何策略判断：拒了就是拒了，环境坏了就是坏了，退休与记账留给调用方
/// ——它才拿着阈值与账本。
pub async fn execute(request: &ChangeExecutionRequest) -> (i32, ChangeExecutionOutcome) {
    let change_id = request.change.artifact_id.clone();
    let world = &request.world;
    let mut outcome = ChangeExecutionOutcome::empty(&change_id);

    // 构建闸门装在本进程里：执行方也是一个构建方，槽位文件在共享文件系统上，与
    // 常驻进程用的是同一个目录。不装的话「宿主同时只跑一个构建」就退化成每个进程
    // 各自成立的空话，而这一项的整个前提正是两个执行器不会互相踩。
    let _ = cog_core::build_gate::install_for(&world.build_gate, true);

    let workspaces =
        WorkspaceManager::new(&world.bare_repo, &world.workspace_root, &world.target_dir);
    let workspace = match workspaces
        .acquire_ephemeral(&change_id, BaseRef::Commit(request.base.clone()))
        .await
    {
        Ok(ws) => ws,
        Err(e) => {
            // 建树失败先过故障规则：连工作树都拿不到时，这串原因是这次不可用
            // 唯一跨进程边界的证据，分类让调用方区分网络、资源与配置类环境问题。
            let fault = crate::RuleBasedFaultClassifier::new().classify(&e.to_string());
            warn!(
                change_id = %change_id,
                fault_category = ?fault.category,
                matched_rule = %fault.matched_rule,
                confidence = fault.confidence,
                "ephemeral workspace acquisition failed"
            );
            outcome.unavailable = Some(Unavailable {
                cause: UnavailableCause::Environment,
                stage: UnavailableStage::Verification,
                reason: e.to_string(),
            });
            return (EXIT_UNAVAILABLE, outcome);
        }
    };

    let (code, outcome) = run_in(request, &workspace.path, outcome).await;

    // 归还临时工作树，无论判定是什么、走到哪一步：这棵树不再有人看。构建产物在
    // 共享 target 里、提交在共享对象库里，删树不带走任何在途的东西。
    if let Err(e) = workspaces.release(&workspace).await {
        // 归还失败不是判定的问题：树留在盘上，由陈旧工作树回收按存活时长与登记里的
        // pid 接手。写一条日志，不改类别。
        warn!(change_id = %outcome.change_id, error = %e, "ephemeral workspace could not be released");
    }
    (code, outcome)
}

/// 在建好的工作树里跑完这条变更的每一步，把观测填进 `outcome`。
async fn run_in(
    request: &ChangeExecutionRequest,
    workdir: &Path,
    mut outcome: ChangeExecutionOutcome,
) -> (i32, ChangeExecutionOutcome) {
    let world = &request.world;
    let budget = Arc::new(VerificationBudget::new(
        world.test_timeout_secs,
        world.build_timeout_secs,
    ));
    let pipeline = ChangePipeline::new(
        world.project_root.as_path(),
        world.change_dir.as_path(),
        world.auto_apply && !world.manual_approve,
    )
    .with_verification_budget(budget.clone())
    .with_promotion_policy(world.promotion.clone())
    .with_test_command(world.test_command.clone())
    .with_target_dir(world.target_dir.as_path());
    let deployer = EvolutionDeployer::new(
        world.project_root.as_path(),
        world.binary_dir.as_path(),
        world.backup_dir.as_path(),
    )
    .with_verification_budget(budget.clone())
    .with_target_dir(world.target_dir.as_path());

    let verdict = pipeline.apply_and_test_in(&request.change, workdir).await;
    // 读数在下一步动它之前取走：构建会写同一份预算的另一个 kind，而验证这一格的
    // 读数只有这一刻在同一个进程里读得到。
    outcome.test = RunReading::of(&budget, KIND_TEST);
    let result = match verdict {
        Ok(result) => result,
        Err(e) => {
            outcome.unavailable = Some(Unavailable {
                cause: UnavailableCause::Environment,
                stage: UnavailableStage::Verification,
                reason: e.to_string(),
            });
            return (EXIT_UNAVAILABLE, outcome);
        }
    };

    outcome.change_id = result.change_id.clone();
    outcome.new_status = Some(result.new_status);
    outcome.files_changed = result.files_changed.clone();
    outcome.test_output = result.test_output.clone();
    outcome.reformatted = result.reformatted;
    outcome.pre_existing_failures = result.pre_existing_failures;
    outcome.tiering = result.tiering.clone();

    if let ChangeVerdict::Refused(cause) = result.verdict {
        outcome.verdict = Some(OutcomeVerdict::Refused(cause));
        return (EXIT_REFUSED, outcome);
    }

    // 等人工批准：管线按同一个判断把树回滚了，这条路上没有构建，也不该有——常驻
    // 进程里这一支同样在建之前就返回。判词照给，"没有产物"作为一个事实记下来，
    // 让读回那一侧能把它与"该有产物却缺了"分开。
    if !world.auto_apply || world.manual_approve {
        outcome.verdict = Some(OutcomeVerdict::Passed);
        outcome.held_for_approval = true;
        return (EXIT_PASSED, outcome);
    }

    match deployer
        .commit_and_build_in(&outcome.change_id, workdir, request.intent)
        .await
    {
        Ok(artifact) => {
            outcome.build = RunReading::of(&budget, KIND_BUILD);
            outcome.build_ending = deployer.last_build_ending();
            outcome.artifact = Some(ExecutedArtifact {
                commit_hash: artifact.commit_hash,
                new_binary_path: artifact.new_binary_path,
                build_duration_secs: artifact.build_duration_secs,
            });
            outcome.verdict = Some(OutcomeVerdict::Passed);
            (EXIT_PASSED, outcome)
        }
        Err(e) => {
            outcome.build = RunReading::of(&budget, KIND_BUILD);
            outcome.build_ending = deployer.last_build_ending();
            outcome.unavailable = Some(Unavailable {
                cause: if e.is_build_slot_refused() {
                    UnavailableCause::NoBuildSlot
                } else {
                    UnavailableCause::Environment
                },
                stage: UnavailableStage::Build,
                reason: e.to_string(),
            });
            (EXIT_UNAVAILABLE, outcome)
        }
    }
}

/// 尽力把这一轮的落点写进容器终止消息。
fn report_termination_message(text: &str) {
    let _ = std::fs::write(TERMINATION_LOG, text);
}

/// `cogneva execute-change` 子命令入口（一过性执行进程内执行）。
///
/// 参数只有两个文件路径：请求从哪读、结果往哪写。世界本身在请求里，命令行不重复
/// 声明它——两处声明同一件事，就一定有一处会先过期。
pub async fn run_execute_change_cli() -> Result<(), Box<dyn std::error::Error>> {
    // 这个进程不经 run_app，不初始化订阅者的话日志全部不落，Job 失败时
    // kubectl logs 是空的，无法诊断。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(2).collect();
    let mut request_path: Option<String> = None;
    let mut outcome_path: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        let value = |i: usize| -> Result<String, Box<dyn std::error::Error>> {
            args.get(i + 1)
                .cloned()
                .ok_or_else(|| format!("--{} requires a value", args[i]).into())
        };
        match args[i].as_str() {
            "--request" => {
                request_path = Some(value(i)?);
                i += 2;
            }
            "--outcome" => {
                outcome_path = Some(value(i)?);
                i += 2;
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    let request_path = PathBuf::from(request_path.ok_or("--request is required")?);
    let outcome_path = PathBuf::from(outcome_path.ok_or("--outcome is required")?);

    // 上一次尝试的结果文件先删掉。同一个路径被两次尝试写过时，读到一个过期的
    // 「通过」比读不到更糟：那是把上一次的判定当成这一次的。删不掉就不做——宁可
    // 报不可用，也不留一份可能被读错的旧判定。
    match std::fs::remove_file(&outcome_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            error!(error = %e, path = %outcome_path.display(), "stale outcome file could not be cleared");
            report_termination_message(&format!("stale outcome could not be cleared: {e}"));
            std::process::exit(EXIT_UNAVAILABLE);
        }
    }

    let request: ChangeExecutionRequest = match read_json(&request_path) {
        Ok(request) => request,
        Err(e) => {
            // 连请求都读不动：连 change_id 都还不知道，结果文件写不出有意义的内容。
            // 类别走退出码，落点走终止消息。
            error!(error = %e, path = %request_path.display(), "change execution request unreadable");
            report_termination_message(&format!("request unreadable: {e}"));
            std::process::exit(EXIT_UNAVAILABLE);
        }
    };

    info!(change_id = %request.change.artifact_id, "Executing one evolution change");
    let (code, outcome) = execute(&request).await;

    if let Err(e) = write_json_atomic(&outcome_path, &outcome) {
        // 判定写不出去比判定本身更要紧：调用方按「读不到结果」处理，这条变更留在
        // 队列里再跑一次（可能多付一次构建），而把它当成通过会送一个没有产物的
        // 变更去落地。所以以不可用退出，并把原因写进终止消息。
        error!(error = %e, change_id = %outcome.change_id, "change execution outcome could not be written");
        report_termination_message(&format!("outcome unwritable: {e}"));
        std::process::exit(EXIT_UNAVAILABLE);
    }

    match ExecutionCategory::of_exit_code(code) {
        ExecutionCategory::Passed => {
            let commit = outcome
                .artifact
                .as_ref()
                .map(|a| a.commit_hash.as_str())
                .unwrap_or("");
            info!(change_id = %outcome.change_id, commit = %commit, "Change passed and was staged");
            report_termination_message(&format!(
                "passed change={} commit={commit}",
                outcome.change_id
            ));
        }
        ExecutionCategory::Refused => {
            let cause = match outcome.verdict {
                Some(OutcomeVerdict::Refused(cause)) => cause.as_str(),
                _ => "unknown",
            };
            info!(change_id = %outcome.change_id, cause = %cause, "Change was refused");
            report_termination_message(&format!(
                "refused change={} cause={cause}",
                outcome.change_id
            ));
        }
        ExecutionCategory::Unavailable => {
            let reason = outcome
                .unavailable
                .as_ref()
                .map(|u| u.reason.as_str())
                .unwrap_or("no reason recorded");
            warn!(change_id = %outcome.change_id, error = %reason, "Change execution could not reach a verdict");
            report_termination_message(&format!(
                "unavailable change={} reason={reason}",
                outcome.change_id
            ));
        }
    }
    std::process::exit(code)
}

// ===== 派发侧：常驻进程这一端 =====
//
// 这一侧只做「把它派出去、等它结束、把回程读懂」。落地、切二进制、晋升留在原处：
// 那些动作要么按定义只能由正在跑的进程做（execve），要么需要一个跨变更串行的
// 账本（落地顺序、晋升窗口）。

/// 执行 Job 里那个容器的名字。回程的退出码挂在它的 `state.terminated` 上。
const JOB_CONTAINER: &str = "change-execution";
/// 父进程容器的名字：镜像摘要与挂载面都从**这个容器**的现场读。写错名字的后果
/// 是一条指名道姓的拒绝，而不是一个别处的镜像。
const PARENT_CONTAINER: &str = "cogneva";

/// 每次 kubectl 调用的墙钟上界。它是控制面的读，不是构建：超过这个数说明
/// API 侧已经不正常，再等也只是把常驻进程挂在这里。
const KUBECTL_TIMEOUT_SECS: u64 = 60;

/// 等待预算比 Job 自己的 `activeDeadlineSeconds` 多出来的那一段：Job 到点后
/// 还要被控制器判死、状态要回写，派发方等到恰好等于它就会在最后几秒里抢跑。
const WAIT_MARGIN_SECS: u64 = 300;

/// 容器运行时会在 `imageID` 前面挂一段 scheme（`docker-pullable://` 等）。
fn strip_image_scheme(image_id: &str) -> &str {
    image_id.rsplit("://").next().unwrap_or(image_id).trim()
}

/// 从 `imageID` 里取**带摘要**的镜像引用；没有摘要就报错。
///
/// 用 tag 派 Job 是不行的：浮动签（`:local`）的内容会被重新播种，子 Job 于是可能
/// 跑的不是父进程这套代码——而它的判词会被当成这条变更的判词，两份不同的代码给出
/// 同一个结论。摘要读不到就不派，不猜。
///
/// 摘要还有个副作用：内容由摘要定死，所以 `imagePullPolicy` 取 `IfNotPresent` 在
/// 这里**不是**「可能跑旧内容」——浮动签才有那个问题，摘要没有。
pub fn image_by_digest(image_id: &str) -> SFResult<String> {
    let reference = strip_image_scheme(image_id);
    if reference.is_empty() {
        return Err(SFError::IO("the running container reports no image".into()));
    }
    if !reference.contains("@sha256:") {
        return Err(SFError::IO(format!(
            "the running container's image carries no digest, so a child job could not be \
             shown to run the same bytes ({reference})"
        )));
    }
    Ok(reference.to_string())
}

/// 父进程 Pod 实际挂着的一个卷。
#[derive(Debug, Clone, PartialEq)]
pub struct ParentMount {
    pub name: String,
    pub mount_path: PathBuf,
    /// 卷来源原样保留（`persistentVolumeClaim` / `hostPath` / ...）。形态随部署
    /// 变化（单机是 hostPath、多机换 PVC），所以只能照抄现场那一份。
    pub source: serde_json::Value,
    pub read_only: bool,
}

/// 派一条执行 Job 所需的、从**本 Pod 的现场对象**里读到的事实。
///
/// 全部读自现场而不是配置文件里的第二份声明：镜像要带摘要、挂载要跟父进程实际
/// 挂着的那几个一致，两件事都随部署形态变，写死一份就会与现场漂移——而漂移的
/// 后果是子进程在另一个世界里得出一个被判成这条变更的判词。
#[derive(Debug, Clone, PartialEq)]
pub struct ParentPodFacts {
    pub image: String,
    pub working_dir: Option<String>,
    pub security_context: Option<serde_json::Value>,
    pub service_account: Option<String>,
    pub mounts: Vec<ParentMount>,
}

/// 可以照搬进子 Job 的卷来源。**不收 `secret` / `projected`**：那会把取值抄进
/// 清单，而清单经 `kubectl apply` 交付、留在 Job 的 spec 上可读——「Secret 不经
/// 清单」是同级约束，何况执行这一侧根本不需要凭证。
const COPYABLE_VOLUME_KINDS: [&str; 4] =
    ["persistentVolumeClaim", "hostPath", "emptyDir", "configMap"];

/// 卷来源的种类：对象里那一个键（`name` 已在读入时去掉）。
fn volume_kind(source: &serde_json::Value) -> Option<&str> {
    source.as_object()?.keys().next().map(|k| k.as_str())
}

/// 从一份 Pod JSON 读出派发所需的事实。
///
/// `container` 是父进程容器的名字：镜像摘要在**那个容器**的 `status
/// .containerStatuses[]` 上，挂载取它的 `volumeMounts`。按名字取而不取第一个：
/// 一个 Pod 里可以有多个容器，「第一个」是清单里的书写顺序，不是身份。
pub fn parent_pod_facts(pod: &serde_json::Value, container: &str) -> SFResult<ParentPodFacts> {
    let containers = pod
        .pointer("/spec/containers")
        .and_then(|c| c.as_array())
        .ok_or_else(|| SFError::IO("the parent pod has no container list".into()))?;
    let spec = containers
        .iter()
        .find(|c| c.get("name").and_then(|n| n.as_str()) == Some(container))
        .ok_or_else(|| {
            SFError::IO(format!(
                "the parent pod has no container named {container} to take its image from"
            ))
        })?;

    let image_id = pod
        .pointer("/status/containerStatuses")
        .and_then(|c| c.as_array())
        .and_then(|statuses| {
            statuses
                .iter()
                .find(|s| s.get("name").and_then(|n| n.as_str()) == Some(container))
        })
        .and_then(|s| s.get("imageID"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SFError::IO(format!(
                "the parent pod reports no imageID for container {container}"
            ))
        })?;
    let image = image_by_digest(image_id)?;

    let volumes: Vec<&serde_json::Value> = pod
        .pointer("/spec/volumes")
        .and_then(|v| v.as_array())
        .map(|v| v.iter().collect())
        .unwrap_or_default();
    let source_of = |name: &str| -> Option<serde_json::Value> {
        let volume = volumes
            .iter()
            .find(|v| v.get("name").and_then(|n| n.as_str()) == Some(name))?;
        let mut source = volume.as_object()?.clone();
        source.remove("name");
        Some(serde_json::Value::Object(source))
    };

    let mut mounts = Vec::new();
    for mount in spec
        .get("volumeMounts")
        .and_then(|m| m.as_array())
        .map(|m| m.as_slice())
        .unwrap_or_default()
    {
        let name = mount
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or_default();
        let mount_path = mount
            .get("mountPath")
            .and_then(|p| p.as_str())
            .unwrap_or_default();
        if name.is_empty() || mount_path.is_empty() {
            return Err(SFError::IO(
                "the parent pod has a volume mount with no name or no path".into(),
            ));
        }
        let source = source_of(name).ok_or_else(|| {
            SFError::IO(format!(
                "the parent pod mounts {name} but declares no volume of that name"
            ))
        })?;
        mounts.push(ParentMount {
            name: name.to_string(),
            mount_path: PathBuf::from(mount_path),
            source,
            read_only: mount
                .get("readOnly")
                .and_then(|r| r.as_bool())
                .unwrap_or(false),
        });
    }

    Ok(ParentPodFacts {
        image,
        working_dir: spec
            .get("workingDir")
            .and_then(|w| w.as_str())
            .filter(|w| !w.is_empty())
            .map(String::from),
        security_context: spec
            .get("securityContext")
            .filter(|s| !s.is_null())
            .cloned(),
        // 身份面在 **Pod** 上，不在容器上：`spec` 是容器对象，容器里没有这个字段。
        // 从容器上读会永远读到"父进程没有 SA"，于是清单里那一项静默缺席、子 Job 变成
        // 跑在 `default` 上——一个声明与产出方对不上的典型形状。
        service_account: pod
            .pointer("/spec/serviceAccountName")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from),
        mounts,
    })
}

/// 请求里那些**必须落在共享卷上**的路径，各自归哪个父挂载点承载。
///
/// 由路径反推卷，而不是手写一份「Job 要挂 A、B、C」的清单：手写的那份在部署形态
/// 变化之后不会跟着变，而漏挂的后果是子进程在自己的可写层里读到空目录**照跑**，
/// 判词于是来自一个别处的世界。反推还白得一个闸门：任何一个路径没有父挂载点承载
/// 就直接拒绝派发——那种情况下产物落不到共享存储上，Job 一退出就没了。
pub fn required_mounts<'a>(
    paths: &[(&str, &Path)],
    mounts: &'a [ParentMount],
) -> SFResult<Vec<&'a ParentMount>> {
    let mut picked: Vec<&ParentMount> = Vec::new();
    for (label, path) in paths {
        if !path.is_absolute() {
            return Err(SFError::IO(format!(
                "{label} is not an absolute path ({}): the executing process would resolve it \
                 against its own working directory",
                path.display()
            )));
        }
        // 取最深的那一个：一个路径可以同时落在 `/sandbox` 与 `/sandbox/src` 之下，
        // 而承载它的是后者。
        let mount = mounts
            .iter()
            .filter(|m| path.starts_with(&m.mount_path))
            .max_by_key(|m| m.mount_path.components().count())
            .ok_or_else(|| {
                SFError::IO(format!(
                    "nothing the parent pod mounts carries {label} ({}): the executing process \
                     would write it into its own writable layer",
                    path.display()
                ))
            })?;
        let kind = volume_kind(&mount.source).unwrap_or("malformed");
        if !COPYABLE_VOLUME_KINDS.contains(&kind) {
            return Err(SFError::IO(format!(
                "{label} is carried by a {kind} volume ({}), which must not be copied into a job \
                 manifest",
                mount.name
            )));
        }
        if !picked.iter().any(|m| m.name == mount.name) {
            picked.push(mount);
        }
    }
    Ok(picked)
}

/// 一次派发的全部落点。分开成结构体，是为了让「名字从哪来、两个文件在哪、清单里
/// 写了什么」在不起集群的情况下也读得出来。
#[derive(Debug, Clone)]
pub struct ChangeJobPlan {
    pub name: String,
    pub namespace: String,
    pub request_path: PathBuf,
    pub outcome_path: PathBuf,
    pub manifest: serde_json::Value,
}

/// 定下一次派发的落点与清单。
pub fn plan_change_job(
    request: &ChangeExecutionRequest,
    config: &ChangeJobConfig,
    namespace: &str,
    facts: &ParentPodFacts,
) -> SFResult<ChangeJobPlan> {
    let delivery_dir = Path::new(&config.delivery_dir);
    if config.delivery_dir.is_empty() || !delivery_dir.is_absolute() {
        return Err(SFError::IO(format!(
            "the change-job delivery directory must be an absolute path ({}): both processes \
             have to name the same directory, and a relative one names two",
            config.delivery_dir
        )));
    }
    if namespace.is_empty() {
        return Err(SFError::IO(
            "no namespace to dispatch into: the pod's service account namespace could not be read"
                .into(),
        ));
    }

    let name = change_job_name(&request.change.artifact_id);
    let dir = delivery_dir.join(&name);
    let request_path = dir.join(REQUEST_FILE);
    let outcome_path = dir.join(OUTCOME_FILE);

    let mut paths = request.world.shared_paths();
    paths.push(("the delivery directory", delivery_dir));
    let mounts = required_mounts(&paths, &facts.mounts)?;

    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();
    for mount in &mounts {
        let mut volume = serde_json::Map::new();
        volume.insert("name".into(), mount.name.clone().into());
        if let Some(fields) = mount.source.as_object() {
            for (key, value) in fields {
                volume.insert(key.clone(), value.clone());
            }
        }
        volumes.push(serde_json::Value::Object(volume));

        let mut entry = serde_json::Map::new();
        entry.insert("name".into(), mount.name.clone().into());
        entry.insert(
            "mountPath".into(),
            mount.mount_path.display().to_string().into(),
        );
        if mount.read_only {
            entry.insert("readOnly".into(), true.into());
        }
        volume_mounts.push(serde_json::Value::Object(entry));
    }

    let mut container = serde_json::json!({
        "name": JOB_CONTAINER,
        "image": facts.image,
        "imagePullPolicy": config.image_pull_policy,
        "command": ["/opt/cogneva/cogneva"],
        "args": [
            "execute-change",
            "--request",
            request_path.display().to_string(),
            "--outcome",
            outcome_path.display().to_string(),
        ],
        "volumeMounts": volume_mounts,
        // 必须显式声明：命名空间的 LimitRange 会把未声明的容器按 defaultCpu
        // （500m）补齐，而那对一次 release 构建是实测过的坏值——限流削掉的正是
        // 判定要用的那段时间。声明出来也就意味着这一份要占配额。
        "resources": {
            "requests": { "cpu": config.job_cpu_request, "memory": config.job_memory_request },
            "limits": { "cpu": config.job_cpu_limit, "memory": config.job_memory_limit },
        },
    });
    // 工作目录照抄父进程那一份：请求里的路径可能相对（默认配置里 change_dir 就是
    // 相对路径），而相对路径按各自的 cwd 解析就成了两个路径。
    if let Some(dir) = &facts.working_dir {
        container["workingDir"] = dir.clone().into();
    }
    // 权限面照抄父进程：判词的有效性建立在「子进程就是父进程的环境」上，收窄权限是
    // 另一个决定，得有它自己的一次实跑证明没有哪一步需要它。
    if let Some(context) = &facts.security_context {
        container["securityContext"] = context.clone();
    }

    let labels = serde_json::json!({
        "app.kubernetes.io/name": "cogneva",
        "app.kubernetes.io/component": "change-execution",
    });
    let mut spec = serde_json::json!({
        // 这个 Job 不做任何 API 调用（派发、等待、读退出码都在常驻进程那一侧），
        // 所以不挂 SA token：Job 持有什么凭证是个能照着清单读出来的事实。
        "automountServiceAccountToken": false,
        "restartPolicy": "Never",
        "containers": [container],
        "volumes": volumes,
    });
    if let Some(account) = &facts.service_account {
        spec["serviceAccountName"] = account.clone().into();
    }

    let manifest = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": { "name": name, "namespace": namespace, "labels": labels },
        "spec": {
            // 0：重试的代价是一次重复的构建与一次重复的提交，而「这条变更要不要再试」
            // 是常驻进程按队列与账本决定的，不是 Job 控制器按次数决定的。
            "backoffLimit": 0,
            "activeDeadlineSeconds": config.deadline_secs,
            "ttlSecondsAfterFinished": config.ttl_secs_after_finished,
            "template": { "metadata": { "labels": labels }, "spec": spec },
        },
    });

    Ok(ChangeJobPlan {
        name,
        namespace: namespace.to_string(),
        request_path,
        outcome_path,
        manifest,
    })
}

/// 本 Pod 所在的命名空间。
///
/// kubectl 自己的默认命名空间来自 kubeconfig，而这个 Pod 没有 kubeconfig；能回答
/// 「我在哪个命名空间」的只有服务账号挂载里的那个文件。
pub fn in_cluster_namespace() -> SFResult<String> {
    const PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    let text = std::fs::read_to_string(PATH)
        .map_err(|e| SFError::IO(format!("cannot read {PATH}: {e}")))?;
    let namespace = text.trim().to_string();
    if namespace.is_empty() {
        return Err(SFError::IO(format!("{PATH} is empty")));
    }
    Ok(namespace)
}

/// 本 Pod 的名字，由向下 API 注入。
///
/// 没有它就取不到自己的 `imageID`，也就没有可派发的镜像——这时停下并指名缺的是
/// 哪一条，而不是退回去用一个 tag。
pub fn parent_pod_name() -> SFResult<String> {
    let name = std::env::var("COGNEVA_POD_NAME").unwrap_or_default();
    let name = name.trim().to_string();
    if name.is_empty() {
        return Err(SFError::IO(
            "COGNEVA_POD_NAME is not set: without it the pod cannot read its own image digest"
                .into(),
        ));
    }
    Ok(name)
}

/// 一次 kubectl 调用，返回解析后的 JSON。
async fn kubectl_json(
    kubectl: &str,
    namespace: &str,
    args: &[&str],
) -> SFResult<serde_json::Value> {
    let output = run_kubectl(kubectl, namespace, args).await?;
    serde_json::from_slice(&output.stdout).map_err(|e| {
        SFError::IO(format!(
            "kubectl {}: output is not JSON: {e}",
            args.join(" ")
        ))
    })
}

/// 一次 kubectl 读取，对象**允许不存在**：空输出读成 `None`。
///
/// 与 `kubectl_json` 分开，是因为空输出在两个调用点上不是一回事：读本 Pod 的状态时
/// 它是坏输出（那东西必然存在），而这里问的是一个按变更名去认领的 Job——第一次派发
/// 时它当然不在，`--ignore-not-found` 的空回答正是要问的那个答案。把空当解析失败，
/// 会让第一次派发永远失败：那恰好是 Job 还不存在的那一次。
async fn kubectl_json_optional(
    kubectl: &str,
    namespace: &str,
    args: &[&str],
) -> SFResult<Option<serde_json::Value>> {
    let output = run_kubectl(kubectl, namespace, args).await?;
    if output.stdout.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    serde_json::from_slice(&output.stdout)
        .map(Some)
        .map_err(|e| {
            SFError::IO(format!(
                "kubectl {}: output is not JSON: {e}",
                args.join(" ")
            ))
        })
}

/// 一次 kubectl 调用，只要成功与否（清单经 stdin 传入）。
async fn kubectl_apply_stdin(
    kubectl: &str,
    namespace: &str,
    body: &[u8],
    what: &str,
) -> SFResult<()> {
    use tokio::io::AsyncWriteExt;

    let mut child = tokio::process::Command::new(kubectl)
        .args(["-n", namespace, "apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| SFError::IO(format!("spawn kubectl apply: {e}")))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(body)
            .await
            .map_err(|e| SFError::IO(format!("write {what} to kubectl: {e}")))?;
        drop(stdin);
    }
    let output = tokio::time::timeout(
        Duration::from_secs(KUBECTL_TIMEOUT_SECS),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| SFError::IO(format!("kubectl apply {what} timed out")))?
    .map_err(|e| SFError::IO(format!("kubectl apply {what}: {e}")))?;
    if !output.status.success() {
        return Err(SFError::IO(format!(
            "kubectl apply {what} exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

/// kubectl 的一次执行，带墙钟上界。
async fn run_kubectl(
    kubectl: &str,
    namespace: &str,
    args: &[&str],
) -> SFResult<std::process::Output> {
    let mut full: Vec<&str> = vec!["-n", namespace];
    full.extend_from_slice(args);
    let child = tokio::process::Command::new(kubectl)
        .args(&full)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| SFError::IO(format!("spawn kubectl {}: {e}", args.join(" "))))?;
    let output = tokio::time::timeout(
        Duration::from_secs(KUBECTL_TIMEOUT_SECS),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| SFError::IO(format!("kubectl {} timed out", args.join(" "))))?
    .map_err(|e| SFError::IO(format!("kubectl {}: {e}", args.join(" "))))?;
    if !output.status.success() {
        return Err(SFError::IO(format!(
            "kubectl {} exited {:?}: {}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

/// 读本 Pod 的现场事实。
pub async fn read_parent_pod(
    kubectl: &str,
    namespace: &str,
    pod_name: &str,
) -> SFResult<ParentPodFacts> {
    let pod = kubectl_json(kubectl, namespace, &["get", "pod", pod_name, "-o", "json"]).await?;
    parent_pod_facts(&pod, PARENT_CONTAINER)
}

/// Job 结束了吗；结束了成没成。`None` 是还在跑。
///
/// 读 `status.conditions` 里的 `Complete`/`Failed`，不从 `status.succeeded` /
/// `failed` 的计数推：计数为 0 既可能是还没跑完，也可能是重试被限住了，而
/// conditions 是控制器对「这个 Job 结束了没有」的正式回答。
pub fn job_ended(job: &serde_json::Value) -> Option<bool> {
    let conditions = job.pointer("/status/conditions")?.as_array()?;
    for condition in conditions {
        if condition.get("status").and_then(|s| s.as_str()) != Some("True") {
            continue;
        }
        match condition.get("type").and_then(|t| t.as_str()) {
            Some("Complete") => return Some(true),
            Some("Failed") => return Some(false),
            _ => {}
        }
    }
    None
}

/// 容器终止时的退出码。读不到就是读不到——不从 Job 的成败反推一个：类别要按退出码
/// 分（通过 / 变更被拒 / 环境不可用），反推出来的类别会把一次环境抖动记成一次否决。
pub fn terminated_exit_code(pod: &serde_json::Value, container: &str) -> Option<i32> {
    let statuses = pod.pointer("/status/containerStatuses")?.as_array()?;
    let status = statuses
        .iter()
        .find(|s| s.get("name").and_then(|n| n.as_str()) == Some(container))?;
    let code = status.pointer("/state/terminated/exitCode")?.as_i64()?;
    Some(code as i32)
}

/// 从 Job 的那个 Pod 上读退出码。
///
/// 两个标签都试：`job-name` 是老的（弃用但仍在写），`batch.kubernetes.io/job-name`
/// 是新的。两个都问不到就是那个 Pod 已经不在了——这时报错，不拿 Job 的成败凑一个
/// 类别出来。
async fn job_exit_code(kubectl: &str, namespace: &str, job_name: &str) -> SFResult<i32> {
    for label in ["job-name", "batch.kubernetes.io/job-name"] {
        let selector = format!("{label}={job_name}");
        let pods = kubectl_json(
            kubectl,
            namespace,
            &["get", "pods", "-l", &selector, "-o", "json"],
        )
        .await?;
        let items = pods
            .pointer("/items")
            .and_then(|i| i.as_array())
            .cloned()
            .unwrap_or_default();
        for pod in &items {
            if let Some(code) = terminated_exit_code(pod, JOB_CONTAINER) {
                return Ok(code);
            }
        }
    }
    Err(SFError::IO(format!(
        "the pod of job {job_name} is gone or never terminated: no exit code to read"
    )))
}

/// 派发与领取用的现场：kubectl、命名空间、本 Pod 的镜像与挂载面、上界旋钮。
///
/// 现场读一次，一个批次共用：本 Pod 的镜像摘要与挂载面在进程生命周期里不变。读现场
/// 而不是从配置里取，是因为声明它的是别处（部署清单）——配置里再写一份就是第二份，
/// 而这一份漂移的后果是子进程跑在一个别处的世界里。
pub struct ChangeJobContext {
    pub kubectl: String,
    pub namespace: String,
    pub facts: ParentPodFacts,
    pub config: ChangeJobConfig,
}

impl ChangeJobContext {
    /// 读一次现场。任何一项读不到都不派 Job——镜像没有摘要、命名空间读不出来、
    /// Pod 名没被注入，都只能停下；猜一个的代价是判词来自另一份代码。
    pub async fn read(config: &ChangeJobConfig) -> SFResult<Self> {
        let namespace = in_cluster_namespace()?;
        let pod_name = parent_pod_name()?;
        let facts = read_parent_pod(&config.kubectl_bin, &namespace, &pod_name).await?;
        Ok(Self {
            kubectl: config.kubectl_bin.clone(),
            namespace,
            facts,
            config: config.clone(),
        })
    }

    /// 派发一条变更（不等它）。**幂等**：Job 名只由变更 id 决定，重启之后重派同一条
    /// 变更落在同一个名字上。同名 Job 还在跑或已经结束时接管它，不重派也不动它的
    /// 文件——已经结束的那个尤其不能重派：`kubectl apply` 是 upsert，不会重新执行
    /// 一个已完成的 Job，删掉再 apply 才是重跑，而重跑等于把同一份变更算两遍、
    /// 构建两次。
    pub async fn dispatch(&self, request: &ChangeExecutionRequest) -> SFResult<ChangeJobPlan> {
        let plan = plan_change_job(request, &self.config, &self.namespace, &self.facts)?;
        let job = kubectl_json_optional(
            &self.kubectl,
            &self.namespace,
            &["get", "job", &plan.name, "-o", "json", "--ignore-not-found"],
        )
        .await?;
        match job.as_ref().and_then(job_ended) {
            // 已经有结论了：什么都不做，等 `collect` 把那份结论读回来。
            Some(_) => {}
            // 同名 Job 还在跑：接管。
            None if job.is_some() => {
                info!(job = %plan.name, "adopting the change job already running");
            }
            None => dispatch_fresh_job(&self.kubectl, &plan, request).await?,
        }
        Ok(plan)
    }

    /// 等它结束，把回程读成一次判定。
    ///
    /// 与 `dispatch` 分开是为了让调用方能交错：并行发生在集群里（两个 Job Pod 各跑
    /// 各的），这一侧只是写文件与轮询，所以「派两条、再等它们」不需要并发 future。
    pub async fn collect(&self, plan: &ChangeJobPlan) -> SFResult<ExecutionReturn> {
        wait_for_job(&self.kubectl, &self.namespace, &plan.name, &self.config).await?;
        let exit_code = job_exit_code(&self.kubectl, &self.namespace, &plan.name).await?;
        info!(job = %plan.name, exit_code, "change job finished");
        interpret(exit_code, &plan.outcome_path)
    }
}

/// 新派一个 Job：清掉上一份结果、写请求、apply。
///
/// 结果文件在**派发方**这一侧也删一次：执行进程自己启动时也删，但那时它已经跑起来
/// 了——镜像拉不动、Pod 起不来的情况下它根本没机会删，而盘上留着的是上一次尝试的
/// 「通过」。读到一个过期的通过比读不到更糟：那是把上一次的判定当成这一次的。
async fn dispatch_fresh_job(
    kubectl: &str,
    plan: &ChangeJobPlan,
    request: &ChangeExecutionRequest,
) -> SFResult<()> {
    match std::fs::remove_file(&plan.outcome_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(SFError::IO(format!(
                "the previous outcome at {} cannot be cleared: {e}",
                plan.outcome_path.display()
            )));
        }
    }
    write_json_atomic(&plan.request_path, request)?;
    let body = serde_json::to_vec_pretty(&plan.manifest)
        .map_err(|e| SFError::IO(format!("serialize job manifest: {e}")))?;
    kubectl_apply_stdin(kubectl, &plan.namespace, &body, &plan.name).await
}

/// 等 Job 结束：轮询它的状态，等到自己的等待预算用尽。
///
/// 预算是 Job 的 `activeDeadlineSeconds` 加上一段余量：Job 到点之后还要被控制器判死
/// 并回写状态，派发方等到恰好等于它就可能在最后几秒里抢跑。预算用尽时报错而不是
/// 换一个类别——那说明 Job 还在跑，这一条没有判定，下一轮接管它。
async fn wait_for_job(
    kubectl: &str,
    namespace: &str,
    job_name: &str,
    config: &ChangeJobConfig,
) -> SFResult<()> {
    let interval = Duration::from_secs(config.poll_interval_secs.max(1));
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(config.deadline_secs + WAIT_MARGIN_SECS);
    loop {
        let job = kubectl_json_optional(
            kubectl,
            namespace,
            &["get", "job", job_name, "-o", "json", "--ignore-not-found"],
        )
        .await?;
        // Job 不在（还没被创建，或已被 TTL 回收）不算结论：继续等，等到自己的
        // 等待预算用尽。缺席与"跑完了"是两回事，读成后者会把一条没跑过的变更
        // 当成有判定的。
        if let Some(complete) = job.as_ref().and_then(job_ended) {
            if !complete {
                warn!(job = %job_name, "the change job reports failure");
            }
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SFError::IO(format!(
                "job {job_name} was still running after the wait budget ({} s)",
                config.deadline_secs + WAIT_MARGIN_SECS
            )));
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EvolutionKind;
    use chrono::Utc;

    fn change(id: &str) -> EvolutionResult {
        EvolutionResult {
            kind: EvolutionKind::CodeChange,
            artifact_id: id.to_string(),
            description: "make the thing do the other thing".to_string(),
            content: "--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n".to_string(),
            status: EvolutionStatus::Generated,
            created_at: Utc::now(),
            eval_summary: None,
            tiering: None,
        }
    }

    fn world() -> ChangeExecutionWorld {
        ChangeExecutionWorld {
            project_root: PathBuf::from("/opt/cogneva/sandbox/src"),
            change_dir: PathBuf::from("/opt/cogneva/sandbox/changes"),
            workspace_root: PathBuf::from("/opt/cogneva/sandbox/workspaces"),
            bare_repo: PathBuf::from("/host-git"),
            target_dir: PathBuf::from("/opt/cogneva/sandbox/src/target"),
            binary_dir: PathBuf::from("/opt/cogneva/bin"),
            backup_dir: PathBuf::from("/opt/cogneva/bin/backups"),
            test_timeout_secs: 3600,
            test_command: cog_core::SelfEvolutionConfig::default_test_command(),
            build_timeout_secs: 3600,
            auto_apply: true,
            manual_approve: false,
            promotion: PromotionGateConfig::default(),
            build_gate: cog_core::BuildGateConfig::default(),
        }
    }

    fn request(id: &str) -> ChangeExecutionRequest {
        world().request(change(id), "0".repeat(40), Some(EvolutionIntent::SelfAudit))
    }

    fn passed(id: &str) -> ChangeExecutionOutcome {
        let mut outcome = ChangeExecutionOutcome::empty(id);
        outcome.verdict = Some(OutcomeVerdict::Passed);
        outcome.new_status = Some(EvolutionStatus::CompileChecked);
        outcome.artifact = Some(ExecutedArtifact {
            commit_hash: "a".repeat(40),
            new_binary_path: PathBuf::from("/opt/cogneva/bin/cogneva.new"),
            build_duration_secs: 412,
        });
        outcome
    }

    #[test]
    fn a_request_survives_the_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(REQUEST_FILE);
        let sent = request("change-1");
        write_json_atomic(&path, &sent).unwrap();
        let read: ChangeExecutionRequest = read_json(&path).unwrap();
        assert_eq!(read.change.artifact_id, "change-1");
        assert_eq!(read.change.content, sent.change.content);
        assert_eq!(read.base, sent.base);
        assert_eq!(
            read.world.build_gate.lock_dir,
            sent.world.build_gate.lock_dir
        );
        assert_eq!(read.intent, sent.intent);
        assert_eq!(read.world.test_timeout_secs, sent.world.test_timeout_secs);
    }

    #[test]
    fn the_artifact_comes_back_in_the_shape_the_consumer_already_knows() {
        let outcome = passed("change-1");
        let artifact = outcome
            .build_artifact()
            .expect("a passed change carries its artifact");
        assert_eq!(artifact.change_id, "change-1");
        assert_eq!(artifact.commit_hash, "a".repeat(40));
        assert_eq!(
            artifact.new_binary_path,
            PathBuf::from("/opt/cogneva/bin/cogneva.new")
        );
        assert_eq!(artifact.build_duration_secs, 412);

        // 停在审批上的通过按定义没有产物——它不是"该有产物却丢了"，所以这里是
        // None，而不是一个空壳。
        let mut held = ChangeExecutionOutcome::empty("change-2");
        held.verdict = Some(OutcomeVerdict::Passed);
        held.held_for_approval = true;
        assert!(held.build_artifact().is_none());
    }

    #[test]
    fn the_build_ending_crosses_the_boundary_with_its_duration() {
        use crate::evolution_build_readings::BuildEnding;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);
        let mut outcome = passed("change-1");
        // 预算杀的：时长就是那段预算，不是对工作的测量。跨进程之后这个区别必须还在，
        // 否则常驻进程会把一格预算记成一次实测。
        outcome.build_ending = Some(BuildEnding::TimedOut(Duration::from_secs(42)));
        write_json_atomic(&path, &outcome).unwrap();
        let read: ChangeExecutionOutcome = read_json(&path).unwrap();
        assert_eq!(
            read.build_ending,
            Some(BuildEnding::TimedOut(Duration::from_secs(42)))
        );

        // 没走到构建那一步的执行没有结局可报：读回来是 None，不拿某个默认结局顶上。
        let mut never_built = ChangeExecutionOutcome::empty("change-2");
        never_built.verdict = Some(OutcomeVerdict::Passed);
        never_built.held_for_approval = true;
        write_json_atomic(&path, &never_built).unwrap();
        let read: ChangeExecutionOutcome = read_json(&path).unwrap();
        assert_eq!(read.build_ending, None);
    }

    /// 档位也要随回程跨进程：执行侧算过一次，常驻进程的晋级判定只能读回它，
    /// 不能拿另一棵已经改过的树重算。老的执行结果文件没有这个键，读回来是
    /// `None`，不是反序列化失败——升级窗口里在途的那一份不能炸。
    #[test]
    fn the_routing_tier_crosses_the_boundary_and_an_old_record_reads_as_none() {
        use crate::criteria_face::{Tier, TierReason, Tiering};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);
        let mut outcome = passed("change-1");
        outcome.tiering = Some(Tiering {
            tier: Tier::RealGate,
            reasons: vec![TierReason::CriteriaCarrier],
            touches_criteria_code: false,
        });
        write_json_atomic(&path, &outcome).unwrap();
        let read: ChangeExecutionOutcome = read_json(&path).unwrap();
        assert_eq!(read.tiering, outcome.tiering);
        assert_eq!(read.apply_result().unwrap().tiering, outcome.tiering);

        let mut old = passed("change-2");
        old.tiering = None;
        write_json_atomic(&path, &old).unwrap();
        let read: ChangeExecutionOutcome = read_json(&path).unwrap();
        assert_eq!(read.tiering, None);
    }

    #[test]
    fn the_categories_are_the_exit_codes_and_an_unknown_code_is_not_a_verdict() {
        assert_eq!(
            ExecutionCategory::of_exit_code(0),
            ExecutionCategory::Passed
        );
        assert_eq!(
            ExecutionCategory::of_exit_code(1),
            ExecutionCategory::Refused
        );
        assert_eq!(
            ExecutionCategory::of_exit_code(EXIT_UNAVAILABLE),
            ExecutionCategory::Unavailable
        );
        // 认不出的码归到不可用：容器运行时起不来会用 128 一类，读成判定就是凭空
        // 造一个结论。
        assert_eq!(
            ExecutionCategory::of_exit_code(128),
            ExecutionCategory::Unavailable
        );
        assert_eq!(
            ExecutionCategory::of_exit_code(137),
            ExecutionCategory::Unavailable
        );
    }

    #[test]
    fn a_pass_needs_its_artifact_and_the_reading_must_be_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);

        write_json_atomic(&path, &passed("change-1")).unwrap();
        let read = interpret(EXIT_PASSED, &path).unwrap();
        assert_eq!(read.category, ExecutionCategory::Passed);
        assert_eq!(
            read.outcome.unwrap().artifact.unwrap().commit_hash,
            "a".repeat(40)
        );

        // 通过但没有产物：读成错，不能读成通过——调用方会拿着一个不存在的提交去
        // 落地。
        let mut without = passed("change-1");
        without.artifact = None;
        write_json_atomic(&path, &without).unwrap();
        assert!(interpret(EXIT_PASSED, &path).is_err());

        // 通过但提交号是空的：形状像产物，对象不存在。
        let mut empty_commit = passed("change-1");
        empty_commit.artifact.as_mut().unwrap().commit_hash = String::new();
        write_json_atomic(&path, &empty_commit).unwrap();
        assert!(interpret(EXIT_PASSED, &path).is_err());
    }

    #[test]
    fn a_missing_or_half_written_outcome_never_reads_as_a_pass() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);

        assert!(interpret(EXIT_PASSED, &path).is_err());
        assert!(interpret(EXIT_REFUSED, &path).is_err());

        std::fs::write(&path, b"{\"change_id\":\"change-1\",\"verdict\":{\"kin").unwrap();
        assert!(interpret(EXIT_PASSED, &path).is_err());
        assert!(interpret(EXIT_REFUSED, &path).is_err());

        // 判词与退出码对不上：以退出码为类别，但两份证据不一致本身是坏读，不挑一个信。
        std::fs::write(&path, b"").unwrap();
        assert!(interpret(EXIT_PASSED, &path).is_err());
    }

    #[test]
    fn a_refusal_comes_back_with_its_cause() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);
        let mut outcome = ChangeExecutionOutcome::empty("change-1");
        outcome.verdict = Some(OutcomeVerdict::Refused(
            cog_core::RejectionCause::MalformedDiff,
        ));
        outcome.new_status = Some(EvolutionStatus::ValidationFailed);
        write_json_atomic(&path, &outcome).unwrap();

        let read = interpret(EXIT_REFUSED, &path).unwrap();
        assert_eq!(read.category, ExecutionCategory::Refused);
        assert_eq!(
            read.outcome.unwrap().verdict,
            Some(OutcomeVerdict::Refused(
                cog_core::RejectionCause::MalformedDiff
            ))
        );

        // 退出码说拒了，文件里是通过：不一致。
        write_json_atomic(&path, &passed("change-1")).unwrap();
        assert!(interpret(EXIT_REFUSED, &path).is_err());
    }

    #[test]
    fn unavailability_needs_no_outcome_file_but_never_carries_a_verdict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);

        let read = interpret(EXIT_UNAVAILABLE, &path).unwrap();
        assert_eq!(read.category, ExecutionCategory::Unavailable);
        assert!(read.outcome.is_none());

        // 有文件就带上：原因是诊断用的，类别仍是退出码那一档。
        let mut outcome = ChangeExecutionOutcome::empty("change-1");
        outcome.unavailable = Some(Unavailable {
            cause: UnavailableCause::NoBuildSlot,
            stage: UnavailableStage::Build,
            reason: "no slot within the wait budget".to_string(),
        });
        outcome.build = RunReading {
            last_secs: None,
            timeouts: 0,
        };
        write_json_atomic(&path, &outcome).unwrap();
        let read = interpret(EXIT_UNAVAILABLE, &path).unwrap();
        let carried = read.outcome.unwrap();
        assert_eq!(
            carried.unavailable.unwrap().cause,
            UnavailableCause::NoBuildSlot
        );

        // 「没判定」却带判词：这是两条路混了，读成坏读——环境抖动不能被读成一次否决。
        write_json_atomic(&path, &passed("change-1")).unwrap();
        assert!(interpret(EXIT_UNAVAILABLE, &path).is_err());
    }

    #[test]
    fn the_readings_of_the_runs_travel_with_the_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);
        let mut outcome = passed("change-1");
        outcome.test = RunReading {
            last_secs: Some(37),
            timeouts: 0,
        };
        outcome.build = RunReading {
            last_secs: None,
            timeouts: 2,
        };
        write_json_atomic(&path, &outcome).unwrap();

        let read = interpret(EXIT_PASSED, &path).unwrap().outcome.unwrap();
        assert_eq!(read.test.last_secs, Some(37));
        assert_eq!(read.build.timeouts, 2);
        // 「没跑过」过边界后仍是没跑过，不会变成 0 秒。
        assert_eq!(read.build.last_secs, None);
    }

    #[test]
    fn an_atomic_write_leaves_no_partial_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OUTCOME_FILE);
        write_json_atomic(&path, &passed("first")).unwrap();
        write_json_atomic(&path, &passed("second")).unwrap();

        let entries: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(entries, vec![OUTCOME_FILE.to_string()]);
        let read: ChangeExecutionOutcome = read_json(&path).unwrap();
        assert_eq!(read.change_id, "second");
    }

    #[test]
    fn the_job_name_is_decided_by_the_change_id_alone() {
        let id = "2026-09-29T14-00_Change_42";
        assert_eq!(change_job_name(id), change_job_name(id));

        // 折叠掉非法字符之后 slug 相同的两条 id，名字必须仍然不同——否则
        // `kubectl apply` 会把第二条变更当成第一条的重派，静默丢掉一条变更。
        let a = change_job_name("change/1");
        let b = change_job_name("change.1");
        let c = change_job_name("change_1");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);

        for name in [a, b, c] {
            assert!(name.len() <= 63, "{name}");
            assert!(name.starts_with("cogneva-change-"));
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "{name}"
            );
            assert!(!name.ends_with('-'), "{name}");
        }

        // id 全是非法字符时不能只剩下一个尾随的连字符。
        let bare = change_job_name("///");
        assert!(bare.starts_with("cogneva-change-"));
        assert!(!bare.ends_with('-'));
        assert_eq!(bare, change_job_name("///"));
    }

    #[test]
    fn adopting_a_reading_does_not_overwrite_a_real_one_with_an_absent_one() {
        let budget = VerificationBudget::new(60, 120);
        assert_eq!(budget.last_run_secs(KIND_TEST), None);

        budget.adopt_run(KIND_TEST, Some(9), 1);
        assert_eq!(budget.last_run_secs(KIND_TEST), Some(9));
        assert_eq!(budget.timeouts(KIND_TEST), 1);

        // 另一次运行没有耗时读数（被预算杀掉的运行故意不带），不得把上一次的真实
        // 读数抹成"没跑过"。
        budget.adopt_run(KIND_TEST, None, 1);
        assert_eq!(budget.last_run_secs(KIND_TEST), Some(9));
        assert_eq!(budget.timeouts(KIND_TEST), 2);
    }

    // ===== 派发侧 =====

    fn parent_pod(image_id: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": { "name": "cogneva-evolution-7d9f", "namespace": "cogneva" },
            "spec": {
                "serviceAccountName": "cogneva-evolution",
                "containers": [{
                    "name": "cogneva",
                    "image": "localhost:30500/cogneva:local",
                    "workingDir": "/opt/cogneva/sandbox/workspaces",
                    "securityContext": { "privileged": true, "runAsUser": 0 },
                    "volumeMounts": [
                        { "name": "sandbox-data", "mountPath": "/opt/cogneva/sandbox" },
                        { "name": "source", "mountPath": "/opt/cogneva/sandbox/src" },
                        { "name": "git-remote", "mountPath": "/host-git" },
                        { "name": "app-data", "mountPath": "/var/lib/cogneva-data" },
                        { "name": "cogneva-json", "mountPath": "/etc/cogneva" },
                        { "name": "prompts", "mountPath": "/etc/cogneva/prompts", "readOnly": true },
                        { "name": "kubectl-bin", "mountPath": "/usr/local/bin/kubectl", "readOnly": true },
                        { "name": "cogneva-secrets", "mountPath": "/secrets" }
                    ]
                }],
                "volumes": [
                    { "name": "sandbox-data", "persistentVolumeClaim": { "claimName": "cogneva-evolution-pvc" } },
                    { "name": "source", "persistentVolumeClaim": { "claimName": "cogneva-evolution-source-pvc" } },
                    { "name": "git-remote", "hostPath": { "path": "/host-git", "type": "Directory" } },
                    { "name": "app-data", "persistentVolumeClaim": { "claimName": "cogneva-evolution-data-pvc" } },
                    { "name": "cogneva-json", "configMap": { "name": "cogneva-json" } },
                    { "name": "prompts", "configMap": { "name": "cogneva-prompts" } },
                    { "name": "kubectl-bin", "hostPath": { "path": "/usr/local/bin/k3s", "type": "File" } },
                    { "name": "cogneva-secrets", "secret": { "secretName": "cogneva-secrets" } }
                ]
            },
            "status": {
                "containerStatuses": [{ "name": "cogneva", "imageID": image_id }]
            }
        })
    }

    const DIGEST: &str =
        "docker-pullable://localhost:30500/cogneva@sha256:9f2c4a1d5e6b7c8d9e0f1a2b3c4d5e6f";

    /// 一份**与部署现场同形**的请求：路径取自 evolution-configmap 里那几项。
    fn deployed_request(id: &str) -> ChangeExecutionRequest {
        let mut request = request(id);
        request.world.binary_dir = PathBuf::from("/opt/cogneva/sandbox/bin");
        request.world.backup_dir = PathBuf::from("/opt/cogneva/sandbox/backups");
        request
    }

    fn plan(id: &str) -> ChangeJobPlan {
        let facts = parent_pod_facts(&parent_pod(DIGEST), PARENT_CONTAINER).unwrap();
        plan_change_job(
            &deployed_request(id),
            &ChangeJobConfig::default(),
            "cogneva",
            &facts,
        )
        .unwrap()
    }

    /// 递归收集 JSON 里出现过的所有键名。
    fn json_keys(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(fields) => {
                for (key, child) in fields {
                    out.push(key.clone());
                    json_keys(child, out);
                }
            }
            serde_json::Value::Array(items) => items.iter().for_each(|i| json_keys(i, out)),
            _ => {}
        }
    }

    fn keys(value: &serde_json::Value) -> Vec<String> {
        let mut out = Vec::new();
        json_keys(value, &mut out);
        out
    }

    #[test]
    fn a_floating_tag_is_not_a_digest_and_nothing_is_dispatched_from_one() {
        // 运行时会给 imageID 挂一段 scheme，剥掉之后才是引用本身。
        assert_eq!(
            image_by_digest(DIGEST).unwrap(),
            "localhost:30500/cogneva@sha256:9f2c4a1d5e6b7c8d9e0f1a2b3c4d5e6f"
        );
        assert_eq!(
            image_by_digest("containerd://reg:5000/x@sha256:abc").unwrap(),
            "reg:5000/x@sha256:abc"
        );
        // 浮动签：子 Job 的内容可能被重新播种，判词就来自另一份代码。
        let err = image_by_digest("docker-pullable://localhost:30500/cogneva:local").unwrap_err();
        assert!(err.to_string().contains("no digest"), "{err}");
        assert!(image_by_digest("").is_err());
    }

    #[test]
    fn the_parent_facts_come_from_the_live_pod_object() {
        let facts = parent_pod_facts(&parent_pod(DIGEST), PARENT_CONTAINER).unwrap();
        assert_eq!(
            facts.image,
            "localhost:30500/cogneva@sha256:9f2c4a1d5e6b7c8d9e0f1a2b3c4d5e6f"
        );
        assert_eq!(
            facts.working_dir.as_deref(),
            Some("/opt/cogneva/sandbox/workspaces")
        );
        assert_eq!(facts.service_account.as_deref(), Some("cogneva-evolution"));
        assert_eq!(
            facts
                .security_context
                .as_ref()
                .and_then(|c| c.get("privileged")),
            Some(&serde_json::Value::Bool(true))
        );
        assert!(facts.mounts.iter().any(|m| m.name == "source"));
        // 挂载与其卷来源成对：只有挂载没有卷是坏的 Pod 对象，拒绝而不是猜。
        let mut broken = parent_pod(DIGEST);
        broken["spec"]["containers"][0]["volumeMounts"][3]["name"] =
            serde_json::Value::String("nowhere".into());
        assert!(parent_pod_facts(&broken, PARENT_CONTAINER).is_err());
        // 容器名不认识时指名拒绝，而不是退回第一个容器。
        assert!(parent_pod_facts(&parent_pod(DIGEST), "not-the-container").is_err());
    }

    #[test]
    fn the_manifest_runs_the_parent_image_by_digest_on_the_mounts_that_carry_the_paths() {
        let manifest = plan("change-1").manifest;
        let container = &manifest["spec"]["template"]["spec"]["containers"][0];

        assert_eq!(
            container["image"],
            serde_json::Value::String(
                "localhost:30500/cogneva@sha256:9f2c4a1d5e6b7c8d9e0f1a2b3c4d5e6f".into()
            )
        );
        // 请求里的路径分布在三个卷上：sandbox（change/workspace/bin/backup/投递）、
        // source（项目根与共享 target）、git-remote（裸仓）。多挂的（app-data、
        // /etc/cogneva、kubectl）不进清单。
        let mut mounted: Vec<String> = container["volumeMounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["mountPath"].as_str().unwrap().to_string())
            .collect();
        mounted.sort();
        assert_eq!(
            mounted,
            vec![
                "/host-git".to_string(),
                "/opt/cogneva/sandbox".to_string(),
                "/opt/cogneva/sandbox/src".to_string(),
            ]
        );
        // 卷按**名字**取，不按下标：这份清单里三项的顺序不承载任何含义（kubelet
        // 按名字认领），钉住它只会让一次无关的重排变红。
        let volumes = manifest["spec"]["template"]["spec"]["volumes"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(volumes.len(), 3);
        let mut names: Vec<&str> = volumes
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, vec!["git-remote", "sandbox-data", "source"]);
        let with_name = |name: &str| -> serde_json::Value {
            volumes
                .iter()
                .find(|v| v["name"] == name)
                .unwrap_or_else(|| panic!("the job manifest has no volume named {name}"))
                .clone()
        };
        assert_eq!(
            with_name("sandbox-data")["persistentVolumeClaim"]["claimName"],
            "cogneva-evolution-pvc"
        );
        assert_eq!(with_name("git-remote")["hostPath"]["path"], "/host-git");

        // 工作目录与权限面照抄父进程：判词的有效性建立在"子进程就是父进程的环境"上。
        assert_eq!(container["workingDir"], "/opt/cogneva/sandbox/workspaces");
        assert_eq!(container["securityContext"]["privileged"], true);
        assert_eq!(container["securityContext"]["runAsUser"], 0);

        // 回程的两条通道写在参数里，读的是同一份请求文件。
        let args: Vec<&str> = container["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert_eq!(args[0], "execute-change");
        assert_eq!(args[1], "--request");
        assert!(args[2].ends_with(&format!("/{REQUEST_FILE}")));
        assert_eq!(args[3], "--outcome");
        assert!(args[4].ends_with(&format!("/{OUTCOME_FILE}")));
    }

    #[test]
    fn the_manifest_carries_no_environment_and_no_secret() {
        let manifest = plan("change-1").manifest;
        let names = keys(&manifest);

        // 执行侧的环境面由镜像与代码定（校验用的那份环境本身就是一个白名单，
        // 里面没有部署变量），所以清单里不该有 env——加一份透传就是给"子进程的
        // 世界"造第二个声明处。
        for forbidden in ["env", "envFrom", "envFromValue", "secret", "secretKeyRef"] {
            assert!(
                !names.iter().any(|k| k == forbidden),
                "the job manifest carries {forbidden}"
            );
        }
        // 这个 Job 不做任何 API 调用，所以不挂 SA token：它持有什么凭证是可读的。
        let pod_spec = &manifest["spec"]["template"]["spec"];
        assert_eq!(pod_spec["automountServiceAccountToken"], false);
        assert_eq!(pod_spec["serviceAccountName"], "cogneva-evolution");
        assert_eq!(pod_spec["restartPolicy"], "Never");
    }

    #[test]
    fn a_volume_that_must_not_be_copied_refuses_the_dispatch() {
        // 承载请求路径的挂载若是个 Secret，就不派：那会把取值抄进清单，而执行侧
        // 不需要任何凭证，需要的是"不许"，不是"用不上"。
        let mut request = deployed_request("change-1");
        request.world.binary_dir = PathBuf::from("/secrets/bin");
        let facts = parent_pod_facts(&parent_pod(DIGEST), PARENT_CONTAINER).unwrap();
        let err =
            plan_change_job(&request, &ChangeJobConfig::default(), "cogneva", &facts).unwrap_err();
        assert!(err.to_string().contains("secret"), "{err}");
        assert!(err.to_string().contains("binary staging"), "{err}");
    }

    #[test]
    fn a_path_no_parent_mount_carries_refuses_the_dispatch() {
        // 现场那条真实的坑：cogneva.json 里的默认 binary_dir 是 /opt/cogneva/bin，
        // 而部署把它改到了 /opt/cogneva/sandbox/bin。谁把这项改回去，都不该得到
        // 一个"产物落在容器可写层、Job 一退出就没了"的执行。
        let mut request = deployed_request("change-1");
        request.world.binary_dir = PathBuf::from("/opt/cogneva/bin");
        let facts = parent_pod_facts(&parent_pod(DIGEST), PARENT_CONTAINER).unwrap();
        let err =
            plan_change_job(&request, &ChangeJobConfig::default(), "cogneva", &facts).unwrap_err();
        assert!(err.to_string().contains("binary staging"), "{err}");
        assert!(err.to_string().contains("/opt/cogneva/bin"), "{err}");

        // 相对路径按各自的 cwd 解析会变成两个路径。
        let mut relative = deployed_request("change-1");
        relative.world.change_dir = PathBuf::from("./evolution-changes");
        let err =
            plan_change_job(&relative, &ChangeJobConfig::default(), "cogneva", &facts).unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
    }

    #[test]
    fn the_deadline_ttl_and_pull_policy_are_the_configured_ones() {
        let facts = parent_pod_facts(&parent_pod(DIGEST), PARENT_CONTAINER).unwrap();
        let config = ChangeJobConfig {
            deadline_secs: 1234,
            ttl_secs_after_finished: 77,
            image_pull_policy: "Always".into(),
            ..Default::default()
        };
        let plan =
            plan_change_job(&deployed_request("change-1"), &config, "cogneva", &facts).unwrap();

        let spec = &plan.manifest["spec"];
        assert_eq!(spec["activeDeadlineSeconds"], 1234);
        assert_eq!(spec["ttlSecondsAfterFinished"], 77);
        // 0：重试是常驻进程按队列与账本决定的，不是 Job 控制器按次数决定的。
        assert_eq!(spec["backoffLimit"], 0);
        assert_eq!(
            spec["template"]["spec"]["containers"][0]["imagePullPolicy"],
            "Always"
        );

        // 投递目录不可用（空或相对）时不派：两个进程必须指同一个目录。
        let broken = ChangeJobConfig {
            delivery_dir: "change-exec".into(),
            ..Default::default()
        };
        assert!(
            plan_change_job(&deployed_request("change-1"), &broken, "cogneva", &facts).is_err()
        );
    }

    #[test]
    fn each_change_gets_its_own_delivery_directory() {
        let first = plan("change-1");
        let second = plan("change-2");
        assert_ne!(first.request_path, second.request_path);
        assert_ne!(first.outcome_path, second.outcome_path);
        // 两条变更的文件不共用目录：并行的两个执行器各写各的，读回也不会串。
        assert_ne!(first.request_path.parent(), second.request_path.parent());
        assert_eq!(
            first.request_path.parent().unwrap(),
            Path::new("/opt/cogneva/sandbox/change-exec").join(&first.name)
        );
        assert_eq!(
            first.outcome_path,
            first.request_path.parent().unwrap().join(OUTCOME_FILE)
        );
    }

    #[test]
    fn a_job_is_finished_only_when_the_controller_says_so() {
        let running = serde_json::json!({ "status": { "succeeded": 0, "failed": 0 } });
        assert_eq!(job_ended(&running), None);

        let complete = serde_json::json!({
            "status": { "conditions": [{ "type": "Complete", "status": "True" }] }
        });
        assert_eq!(job_ended(&complete), Some(true));

        let failed = serde_json::json!({
            "status": { "conditions": [{ "type": "Failed", "status": "True" }] }
        });
        assert_eq!(job_ended(&failed), Some(false));

        // 别的条件（statuses 里还有 Suspended 一类）不是结论。
        let suspended = serde_json::json!({
            "status": { "conditions": [{ "type": "Suspended", "status": "True" }] }
        });
        assert_eq!(job_ended(&suspended), None);
    }

    #[test]
    fn the_exit_code_comes_from_the_terminated_state_or_not_at_all() {
        let terminated = serde_json::json!({
            "status": { "containerStatuses": [{
                "name": JOB_CONTAINER,
                "state": { "terminated": { "exitCode": 75, "reason": "Error" } }
            }] }
        });
        assert_eq!(terminated_exit_code(&terminated, JOB_CONTAINER), Some(75));

        // 还在跑：没有 terminated 就没有退出码，不从 Job 的成败反推一个。
        let running = serde_json::json!({
            "status": { "containerStatuses": [{
                "name": JOB_CONTAINER,
                "state": { "running": { "startedAt": "2026-09-29T14:00:00Z" } }
            }] }
        });
        assert_eq!(terminated_exit_code(&running, JOB_CONTAINER), None);
        // Pod 已经不在了（被删掉的那条验收）。
        assert_eq!(
            terminated_exit_code(&serde_json::json!({}), JOB_CONTAINER),
            None
        );
        assert_eq!(terminated_exit_code(&running, "other"), None);
    }
}
