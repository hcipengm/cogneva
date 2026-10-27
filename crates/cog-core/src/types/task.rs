use crate::contract::llm::UpstreamFailure;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Goal Message ──────────────────────────────────────────────────────────

/// 目标提交的线上形态，由 DagExecutorRuntime 从 `goals:{workspace_id}`
/// stream 消费并驱动后续编排。
///
/// 目前没有生产者：Gateway、GitHub discovery、reflection 等入口都在进程内
/// 直接调 `OrchestratorControl::submit_goal_auto`，这条流自部署以来没有收到
/// 过消息。保留它是为跨进程投递留的通道，但任何依赖"消息会被重投"的假设
/// 在这条流上没有依据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalMessage {
    pub message_id: String,
    pub timestamp: DateTime<Utc>,
    pub workspace_id: String,
    pub goal_id: String,
    pub goal: String,
    pub tasks: Vec<Task>,
    pub priority: i32,
    pub source: GoalSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalSource {
    Api,
    WebSocket,
    Scheduler,
    Internal,
}

/// ActionPlanner 对任务的来源标记。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ActionPlannerSource {
    /// 用户直接提供的任务，未经 ActionPlanner 处理。
    UserProvided,
    /// 基于用户提供的任务经过 ActionPlanner 优化后的任务。
    Optimized,
    /// 由 ActionPlanner 通过 Collaboration 完整分解生成的任务。
    Decomposed,
}

/// 一条计划基线：这条任务「打算怎么做」。
///
/// 单独一行的理由不是分层好看，而是**打算**与**做成**会朝不同方向变：
/// 任务图上的行每次状态迁移都要重写，而计划一旦排定就不该跟着动——把两者放进
/// 同一张表，后续每一次状态迁移都会顺带改写计划的证据，事后没人分得清哪一版是
/// 当初排的。所以这张表**没有状态列**：状态的唯一正本在任务图（`cog_dag_tasks`），
/// 两张表各存一份状态就是两份真值。
///
/// 预算与工期用的是分解器已经算出来的那两个数（`AtomicTask::estimated_tokens` /
/// `estimated_seconds`），不是在这里另估一遍：估算器的正本只有一处，读者要问的是
/// 「当初按什么排的」。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskPlan {
    pub workspace_id: String,
    pub task_id: String,
    /// 计划起止。分解没给出工期时两者都是 `None`：预算已知、排期未知——
    /// 这与「计划长度为零」是两件事，编一个零长度条会让甘特面把它画成已完成。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_start: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_end: Option<DateTime<Utc>>,
    /// 计划花掉的 token。与 token 账同一把尺子（`gateway_llm_usage`）。
    pub budget_tokens: u64,
    /// 这一版基线是哪一次规划写下的。同一批任务共享一个值；重新规划写入的值
    /// 单调前进，读者据此分辨两版基线，而不是把它们读成同一版的两个部分。
    pub baseline_rev: String,
    pub updated_at: DateTime<Utc>,
}

/// 一种任务跑完过的样本：中位花销与中位时长。
///
/// 它是**测量**而不是估计器的参数表：读的数来自任务图里已完成的行（创建到最近
/// 一次更新的时长）与 token 清点表（那件任务自己的累计花销）。派发侧拿它给同
/// 种类的新任务排一次期，前提是样本够——样本不够就没有这一行，没有这一行就不
/// 排期，而不是拿一个凭空的零充数。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskKindRecord {
    /// 与 [`TaskType::kind_key`] 同一个拼写，也就是任务 JSON 里那个字符串。
    pub kind: String,
    /// 参与计算的已完成任务条数。
    pub samples: u64,
    /// 中位花销（token）。0 表示这一类跑完的任务一条带花销的清点都没有。
    pub median_tokens: u64,
    /// 中位时长（秒）：从创建到最近一次更新。
    pub median_duration_secs: u64,
}

/// DagExecutor 任务类型
/// 标记任务是否经过 ActionPlanner 验证/分解。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionPlannerMeta {
    /// 该任务是否已被 ActionPlanner 验证为可靠。
    pub verified: bool,
    /// 验证/分解时使用的 ActionPlanner 版本或签名。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// 验证时产生的附加信息（如 LLM 评估分数、优化建议等）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// 任务的来源标记。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<ActionPlannerSource>,
    /// LLM 评估置信度（0.0 ~ 1.0）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    /// 验证/标记时间戳。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: String,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub input: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 失败原因的类型，仅当传输层给过信号（HTTP 状态码）时才填。
    ///
    /// [`Self::error`] 是给人看的文本，它随上游措辞和语言变化；任何判定读这个
    /// 字段。为 `None` 表示这次失败只有文本可依——那时下游不得从文本里猜类型。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_cause: Option<UpstreamFailure>,
    pub blocked_by: Vec<String>,
    pub blocks: Vec<String>,
    pub priority: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub retry_count: u32,
    pub max_retries: u32,
    /// 重新允许本任务被执行的最早时刻，由失败路径按重试策略的退避算出。
    ///
    /// 退避必须落在任务自身而不是调度循环的内存里：判定重试的进程与最终把它
    /// 重新投递出去的进程可能不是同一个，值只留在内存里就会随重启或换 pod 丢
    /// 失，退避静默退化成零延迟重试——配置上的退避策略看着还在，实际一次都没
    /// 生效。`None` 表示随时可执行。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_not_before: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    pub timeout_seconds: u64,
    /// 持有本任务执行权的进程身份，随进程消亡而失效。
    ///
    /// 它不是实例身份：同一台机器上换一个进程，持有者就换了一个值，而这正是
    /// 「原来那个进程已经不在了」能被读出来的唯一依据。放在任务行里而不是进程
    /// 内存里，是因为要发现孤儿的是**后来那个进程**——它的内存里从来没有过这条
    /// 记录，只有落盘的持有者能让它知道该不该接手。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner: Option<String>,
    /// 租约到期时刻，由持有者心跳续期。没有续期证据即视为过期：判断发生在持有者
    /// 之外的进程里，它看不到持有者是否还在跑，只能看到这个时刻有没有被推后。
    ///
    /// 读到 `None` 而状态是 Running，说明这一行由不认识租约的旧版本写入；回收
    /// 判据按「开始时刻 + 租约时长」兜底，所以它仍会在一个租约时长内被接走，
    /// 不会退回「等满一个超时窗」。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// ActionPlanner 验证标记。存在且 verified=true 时可直接进入 DagExecutor。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_planner_meta: Option<ActionPlannerMeta>,
    /// 所属 goal 的全局唯一标识。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    /// 父任务 ID。原子任务指向其所属的整体任务。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    /// 这次分解里承载答案的那一条任务的 id，写在**目标行**（不可执行的那个占位）上。
    ///
    /// 目标行自己不执行，它的终态是从这条声明指向的那行抄来的：`Some(id)` 读作「这道
    /// 目标的答案就是 id 那条任务的终态与结果」。所以问「谁在最后」要读这条声明，不能
    /// 按「谁没有下游」重算——别的目标的行、单独提交的行都会落进那种算法里。
    /// 没有恰好一条这样的任务（零条＝各片从不收口，两条以上＝没有哪一条承载答案）是
    /// 一次分解失败，整份提交判退，不会静默挑一条。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_task_id: Option<String>,
    /// 是否可被 DagExecutor 调度执行。
    /// 原始整体任务保留注入时设为 false，仅作为层级占位与查询用。
    #[serde(default = "default_true")]
    pub is_executable: bool,
    /// 这道任务属于哪个面。
    ///
    /// 面本身也是任务图里的一行（不可执行、只做登记），所以这一列在两处都有意义：
    /// 面节点的 `face_id` 指向自己，工作的 `face_id` 指向它服务的那一面。`None`
    /// 表示不服务任何面——平台内部任务与旧行都落在这里，读面不得把「没填」读成
    /// 「属于某个默认面」。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub face_id: Option<String>,
    /// 这道任务指定由谁执行。
    ///
    /// 与 `agent_id` 的区别是答的问题不同：`agent_id` 是「结果是谁交的」，
    /// `owner_unit` 是「谁被指定来做」。带归属的任务只有该 unit 能开工与完成，
    /// 所以它不能停在共享就绪队列里等人抢——认领必须在写入时就按这一列裁决。
    /// 面节点上它表示面主。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_unit: Option<String>,
    /// 这次归属是由哪张交接单签过来的。
    ///
    /// 没有交接单的归属变化是漂移，不是改派；留下这一条引用，读面才不必比较
    /// 两份快照去猜谁先谁后。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_by_handover_id: Option<String>,
    /// 这条活包**怎样算做完**，来自分解时给出的验收标准。
    ///
    /// 放在任务行上而不是计划基线表里，有两个理由。验收标准是范围（这片活答应
    /// 交付什么），不是计划（什么时候做、花多少）；计划表只该有可重排的数。
    /// 更要紧的是：基线行只在有估算时才写，没有估算的工作包不落基线行，验收
    /// 标准就会在入图那一刻丢掉——而它正是重规划时要读的东西。任务行一定在，
    /// 它跟着任务走多久就存多久。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// 承载一道目标的那一行的类型拼写。
///
/// 写侧按它造行、读侧按它认行（分解时它被排除在规划输入之外，读面靠它区分「目标行」
/// 与「活」），所以这个字符串只留一处。
const GOAL_ROW_TASK_TYPE: &str = "goal";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaskType {
    // Agent role types
    Planner,
    Generator,
    Evaluator,
    Reviewer,

    // Operation types for retry matrix (Layer 9)
    LlmCall,
    ToolCall,
    FileOp,
    DbTransaction,
    NetworkRequest,
    WasmSkill,
    Skill,
    DagNode,

    Custom(String),
}

impl TaskType {
    /// The spelling a retrieval keys its rows on and builds its query from.
    ///
    /// One spelling, in one place, because the write side derives the key from
    /// it and the read side derives the query from it: two spellings of the
    /// same type are two strings that have to stay equal, and the store
    /// matches a query as a substring of a key, so the day they diverge the
    /// namespace reads back empty with every row still sitting in it.
    pub fn retrieval_class(&self) -> String {
        format!("{self:?}")
    }

    /// 这一种类在任务 JSON 里的拼写，按样本分组用的那一把尺子。
    ///
    /// 读样本的那条 SQL 从 `task->'task_type'` 里取同一个字符串（自定义类型取
    /// `custom`，其余取变体名本身）。两处必须是同一把尺子：拼写一旦分叉，按种类
    /// 查出来的样本读成空，于是每一件新任务都排不上期，而读数上只表现成「没有
    /// 样本」，看不出是拼写对不上。
    pub fn kind_key(&self) -> String {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(s)) => s,
            Ok(serde_json::Value::Object(m)) => m
                .get("custom")
                .and_then(|v| v.as_str())
                .unwrap_or("other")
                .to_string(),
            _ => "other".to_string(),
        }
    }
}

/// Where a goal's class was read from.
///
/// Kept apart rather than folded into the class itself: a class carried with
/// the goal and a class taken from whatever task happens to hold it are two
/// different pieces of evidence about the same string, and the second one is
/// the shape a missing producer takes — the retrieval still answers, under a
/// class that names the run instead of the work.
///
/// No serde form: this value never travels. Its only outward word is the metric
/// label [`Self::as_str`] produces, and a derived spelling would have been a
/// second one the compiler keeps in step with the variants but nothing keeps in
/// step with the label — `Carried` next to `carried`, read on a face that
/// selects by the label and finds nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalClassSource {
    /// The goal carried it: whoever re-hosted the goal named the class.
    Carried,
    /// The class is the host task's own type.
    HostType,
}

impl GoalClassSource {
    /// The label this source is published under.
    pub fn as_str(self) -> &'static str {
        match self {
            GoalClassSource::Carried => "carried",
            GoalClassSource::HostType => "host_type",
        }
    }

    /// Both sources, so a caller publishing them can publish the empty cell
    /// too: a source nothing uses and a source that stopped being used have to
    /// read differently.
    pub const ALL: [GoalClassSource; 2] = [GoalClassSource::Carried, GoalClassSource::HostType];
}

/// The class a goal is retrievable under.
///
/// A goal outlives the task that carried it: the decomposition loop plans it
/// on a synthetic task whose only job is to hold the goal text, and that task's
/// own type says nothing about the work. So the class travels with the goal
/// under the `goal_class` input field — the same channel the goal text itself
/// uses — and a task that does not carry one is the goal's original host, whose
/// own type is the class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalClass {
    pub value: String,
    pub source: GoalClassSource,
}

impl GoalClass {
    /// The field a re-hosting caller writes the class into.
    pub const INPUT_FIELD: &'static str = "goal_class";

    pub fn of(task: &Task) -> Self {
        match task
            .input
            .get(Self::INPUT_FIELD)
            .and_then(|v| v.as_str())
            .filter(|class| !class.is_empty())
        {
            Some(class) => Self {
                value: class.to_string(),
                source: GoalClassSource::Carried,
            },
            None => Self {
                value: task.task_type.retrieval_class(),
                source: GoalClassSource::HostType,
            },
        }
    }
}

/// Which entry point put a self-evolution task into the main flow.
///
/// Bounded on purpose. A producer spells its task kind as a free-form string —
/// `TaskType::Custom(String)` or the `task_kind` input field — and a metric
/// label carrying those verbatim would take a new value every time someone
/// typed a new name, so the series count would grow with the vocabulary rather
/// than with the work. The set below is fixed; anything outside it reads as
/// `Unattributed`, which is itself a finding about the producer rather than a
/// silent drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionIntent {
    /// A condition the platform detected about itself: a recurring task
    /// failure, a queue that is not draining, or a firing alert.
    SelfSignal,
    /// The periodic audit of the platform's own repository.
    SelfAudit,
    /// A red CI run on the base branch, including the re-drive that a failed
    /// landing triggers.
    CiFix,
    /// A tracked public issue.
    IssueFix,
    /// An external pull request whose intent should be carried out.
    PrIntent,
    /// A move of the tracked upstream baseline invalidated an earlier attempt.
    BaselineRework,
    /// A gate refused a change, and the defect it named is being generated for
    /// again through the main flow.
    ChangeRework,
    /// The explicit `self_evolution` task form, without a narrower producer.
    SelfEvolution,
    /// A self-evolution task whose kind is not one of the above.
    Unattributed,
}

impl EvolutionIntent {
    /// Every value. A producer that publishes one series per class needs the
    /// classes it has none of as well, or an empty class is indistinguishable
    /// from a class that was never wired up. The list is the enum, so adding a
    /// variant cannot leave a reader silently short of a series.
    pub const ALL: [EvolutionIntent; 9] = [
        EvolutionIntent::SelfSignal,
        EvolutionIntent::SelfAudit,
        EvolutionIntent::CiFix,
        EvolutionIntent::IssueFix,
        EvolutionIntent::PrIntent,
        EvolutionIntent::BaselineRework,
        EvolutionIntent::ChangeRework,
        EvolutionIntent::SelfEvolution,
        EvolutionIntent::Unattributed,
    ];

    /// The label spelling. The variants are the whole domain, so these strings
    /// are too.
    pub fn as_str(self) -> &'static str {
        match self {
            EvolutionIntent::SelfSignal => "self_signal",
            EvolutionIntent::SelfAudit => "self_audit",
            EvolutionIntent::CiFix => "ci_fix",
            EvolutionIntent::IssueFix => "issue_fix",
            EvolutionIntent::PrIntent => "pr_intent",
            EvolutionIntent::BaselineRework => "baseline_rework",
            EvolutionIntent::ChangeRework => "change_rework",
            EvolutionIntent::SelfEvolution => "self_evolution",
            EvolutionIntent::Unattributed => "unattributed",
        }
    }

    /// Classify a producer's own spelling of a task kind.
    ///
    /// One definition, because the reading is only useful if every producer is
    /// classified the same way wherever it is counted.
    pub fn classify(kind: &str) -> Self {
        match kind {
            "self_signal" => EvolutionIntent::SelfSignal,
            "self_audit" => EvolutionIntent::SelfAudit,
            "platform_ci_fix" => EvolutionIntent::CiFix,
            "platform_issue_fix" => EvolutionIntent::IssueFix,
            "platform_pr_intent" => EvolutionIntent::PrIntent,
            "baseline_port_rework" => EvolutionIntent::BaselineRework,
            "change_rework" => EvolutionIntent::ChangeRework,
            "self_evolution" => EvolutionIntent::SelfEvolution,
            // The change ledger dispatches an owed row as a `change_ledger`
            // task. It is the plain self-evolution form: the audit found the
            // row and the main flow generates the change, with no narrower
            // class naming the producer. Missing from this table, the kind
            // fell through to `unattributed` and the funnel could not say who
            // held 55 of its retired changes.
            "change_ledger" => EvolutionIntent::SelfEvolution,
            _ => EvolutionIntent::Unattributed,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    /// Every variant, so a gate can cover the whole enum instead of the
    /// subset someone happened to remember.
    pub const ALL: [TaskStatus; 6] = [
        TaskStatus::Pending,
        TaskStatus::Scheduled,
        TaskStatus::Running,
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Cancelled,
    ];

    /// The spelling stored in a task-state table's `status` column.
    ///
    /// The single place a variant becomes a string. Writers, readers and any
    /// query over the resulting rows go through it, so a status cannot mean
    /// one thing in a row and another in a count over those rows.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Scheduled => "scheduled",
            TaskStatus::Running => "running",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    /// Read back what [`Self::as_str`] wrote.
    ///
    /// `None` for a string this version does not know: the caller has to
    /// decide what an unrecognised status means, and quietly filing it under a
    /// default would hide a producer and a consumer that no longer share a
    /// vocabulary — the rows would keep counting while their states stopped
    /// being told apart.
    pub fn parse(status: &str) -> Option<Self> {
        match status {
            "pending" => Some(TaskStatus::Pending),
            "scheduled" => Some(TaskStatus::Scheduled),
            "running" => Some(TaskStatus::Running),
            "completed" => Some(TaskStatus::Completed),
            "failed" => Some(TaskStatus::Failed),
            "cancelled" => Some(TaskStatus::Cancelled),
            _ => None,
        }
    }

    /// Whether the task has stopped moving on its own: no transition leaves
    /// these states, so a row in one of them is history rather than work
    /// standing in a column.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }
}

/// What the task list and graph endpoints accept in their `status` query.
///
/// The status words are the ones [`TaskStatus::as_str`] writes -- the same
/// spelling a row carries to the reader -- and [`TaskFilter::Ready`] names the
/// queue instead: never started, every blocker done. `ready` is not a state any
/// row holds, so a reader that treated it as one selects nothing, and an empty
/// list reads exactly like "nothing is waiting" -- the reading the tab exists
/// to give.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskFilter {
    Ready,
    Status(TaskStatus),
}

impl TaskFilter {
    /// Every word the endpoints accept, so a client's tabs and a gate can be
    /// held to this declaration instead of to whoever typed the list.
    pub const ALL: [TaskFilter; 7] = [
        TaskFilter::Ready,
        TaskFilter::Status(TaskStatus::Pending),
        TaskFilter::Status(TaskStatus::Scheduled),
        TaskFilter::Status(TaskStatus::Running),
        TaskFilter::Status(TaskStatus::Completed),
        TaskFilter::Status(TaskStatus::Failed),
        TaskFilter::Status(TaskStatus::Cancelled),
    ];

    /// The word to send and the word this declaration spells for it.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskFilter::Ready => "ready",
            TaskFilter::Status(status) => status.as_str(),
        }
    }

    /// Read back a query word.
    ///
    /// Case-insensitive: the parameter always was, and a reader typing `Failed`
    /// asked for the same rows as one typing `failed`. `None` for a word this
    /// version does not know -- the caller answers with the words it does
    /// accept rather than with an empty list.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|filter| filter.as_str().eq_ignore_ascii_case(word))
    }
}

/// The bucket id for tasks that belong to no goal.
///
/// Named rather than dropped: a reading that leaves those rows out makes the
/// projects sum to less than the whole graph, and the difference is a number
/// no surface reports. The writer that labels the workspace totals and the
/// board reading that labels a column both use this spelling, so a limit
/// configured for one is read back under the name the other publishes.
pub const UNGROUPED_PROJECT: &str = "ungrouped";

impl Task {
    /// Which project this task belongs to: the goal root it was submitted
    /// under, or [`UNGROUPED_PROJECT`] when it was submitted outside a goal.
    ///
    /// The publisher's WIP limits and the project reading both key on this, so
    /// it is defined once. An empty goal id counts as none: a row written by a
    /// path that set the field to `""` was not submitted under a project either,
    /// and treating it as one would invent a project named after nothing.
    pub fn project_key(&self) -> &str {
        match self.goal_id.as_deref() {
            Some(goal) if !goal.is_empty() => goal,
            _ => UNGROUPED_PROJECT,
        }
    }
}

/// How the tasks a store holds distribute over the states the cluster
/// overview reports.
///
/// Three buckets plus the total; terminal non-failing states (completed,
/// cancelled) carry no counter of their own and show up in `total` only,
/// because the overview has no field for them and inventing one here would
/// put a reading in a place nobody asked for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatusCounts {
    /// Every task the store holds, in any state.
    pub total: usize,
    /// Tasks a worker is executing right now.
    pub active: usize,
    /// Tasks waiting for a worker: never started, or published to the ready
    /// stream and not yet claimed.
    pub queued: usize,
    /// Tasks that ended in failure.
    pub failed: usize,
}

impl TaskStatusCounts {
    /// Count a bag of statuses.
    pub fn of<'a>(statuses: impl IntoIterator<Item = &'a TaskStatus>) -> Self {
        let mut counts = Self::default();
        for status in statuses {
            counts.add(status, 1);
        }
        counts
    }

    /// Fold `count` tasks in `status` into the counts.
    pub fn add(&mut self, status: &TaskStatus, count: usize) {
        self.total = self.total.saturating_add(count);
        match status {
            TaskStatus::Running => self.active = self.active.saturating_add(count),
            TaskStatus::Pending | TaskStatus::Scheduled => {
                self.queued = self.queued.saturating_add(count)
            }
            TaskStatus::Failed => self.failed = self.failed.saturating_add(count),
            TaskStatus::Completed | TaskStatus::Cancelled => {}
        }
    }
}

/// DAG 任务图
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskDAG {
    pub goal: String,
    pub tasks: HashMap<String, Task>,
    pub dependencies: HashMap<String, Vec<String>>,
}

/// DagExecutor 消息协议
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DagMessage {
    TaskAssign {
        message_id: String,
        timestamp: DateTime<Utc>,
        payload: TaskPayload,
        sender: String,
        recipient: String,
    },
    TaskComplete {
        message_id: String,
        timestamp: DateTime<Utc>,
        task_id: String,
        result: serde_json::Value,
        sender: String,
        recipient: String,
    },
    TaskFailed {
        message_id: String,
        timestamp: DateTime<Utc>,
        task_id: String,
        error: String,
        /// `error` 的类型化原因。错误在总线上以文本存活，类型靠这个字段过桥；
        /// 老版本发布者不带它，反序列化按 `None` 处理。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_cause: Option<UpstreamFailure>,
        /// 上游这次拒绝时自己说的等待时长（秒）。与同一变体的 `error_cause` 并行
        /// 传递，理由相同：它是判定重试时刻的输入，而判定发生在消费这条消息的
        /// 进程里，不是产生它的进程里。老版本发布者不带它，按 `None` 处理。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_secs: Option<u64>,
        sender: String,
        recipient: String,
    },
    EventNotify {
        message_id: String,
        timestamp: DateTime<Utc>,
        payload: serde_json::Value,
        sender: String,
        recipient: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPayload {
    pub task_id: String,
    pub task_type: TaskType,
    pub input: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,
    pub priority: i32,
}

// ─── Self-evolution markers on a task's own input ──────────────────────────
//
// These live on `task.input`, which is free-form JSON: producers in other
// crates write them and this crate decides what they mean. Spelling them by
// hand on both sides makes the reader's copy authoritative over nothing — the
// writer keeps filling in the old word, the reader keeps comparing against the
// new one, and every task quietly stops being recognised. Nothing fails; the
// flow just degrades into ordinary output.

/// The `task.input` key that marks a task as part of the self-evolution flow.
pub const INPUT_EVOLUTION_MODE: &str = "evolution_mode";

/// The `evolution_mode` value that selects the change-generation flow.
pub const EVOLUTION_MODE_GENERATE_CHANGE: &str = "generate_change";

/// The `task.input` key that names which entry point submitted the task.
pub const INPUT_TASK_KIND: &str = "task_kind";

impl Task {
    pub fn new(id: impl Into<String>, task_type: TaskType, input: serde_json::Value) -> Self {
        let now = Utc::now();
        Task {
            id: id.into(),
            task_type,
            status: TaskStatus::Pending,
            input,
            result: None,
            error: None,
            error_cause: None,
            blocked_by: Vec::new(),
            blocks: Vec::new(),
            priority: 1,
            created_at: now,
            updated_at: now,
            agent_id: None,
            workspace_id: None,
            retry_count: 0,
            max_retries: 3,
            retry_not_before: None,
            started_at: None,
            timeout_seconds: 300,
            lease_owner: None,
            lease_expires_at: None,
            action_planner_meta: None,
            goal_id: None,
            parent_task_id: None,
            sink_task_id: None,
            is_executable: true,
            face_id: None,
            owner_unit: None,
            bound_by_handover_id: None,
            acceptance: Vec::new(),
        }
    }

    /// 一次目标提交里承载这道目标的那一行：不可执行，只做层级与查询。
    ///
    /// 这道目标的答案落在这里。分解出的子任务从这一行取 `goal_id`，用 `parent_task_id`
    /// 指回它；分解点出的唯一收口任务写在它的 `sink_task_id` 上；它自己不执行，终态与
    /// 结果是收口任务跑完后抄回来的。判退也一样落在这行上（判成 Failed 并带着原因）——
    /// 没有这一行，一次提交无论成败在读面上都不存在。
    ///
    /// 所以「一次提交至少要有一条这样的行」是提交侧的契约：调用方带了自己的目标行就
    /// 用它自己的（id 就是客户端拿到的那个），一条行都不给时由分解侧补一条。形状只留
    /// 这一处构造，两处各写一遍会各自漂——读面按 `Custom("goal")`、`{"goal": …}`、
    /// `is_executable=false` 认这一行。
    pub fn goal_row(id: impl Into<String>, goal: &str) -> Self {
        let id = id.into();
        let mut task = Task::new(
            id.clone(),
            TaskType::Custom(GOAL_ROW_TASK_TYPE.to_string()),
            serde_json::json!({ "goal": goal }),
        );
        task.goal_id = Some(id);
        task.is_executable = false;
        task
    }

    /// 这一行是不是一次目标提交的承载行（[`Self::goal_row`] 造出来的那种）。
    ///
    /// 判据只有一处，因为标记本身只在这里：让读面各自去比 `Custom("goal")`，
    /// 就是把同一个私有字面量抄进别的 crate，改名时只会改到其中一份，而认错的
    /// 后果是「列出任务时多出一堆分解出来的子任务」或「一行都列不出来」——两种
    /// 都不会报错。
    pub fn is_goal_row(&self) -> bool {
        matches!(&self.task_type, TaskType::Custom(kind) if kind == GOAL_ROW_TASK_TYPE)
    }

    /// 进入 Running 的唯一入口：开始时刻与租约一起落定。
    ///
    /// 两者必须同时写，因为回收判据读的是租约而状态机给的是 Running，任何一处
    /// 只设状态不设租约的写法都会造出一行「看起来有人持有、其实谁也接不走」的
    /// 任务。收在一个方法里，这个不变式就有了唯一的写点。
    pub fn begin_run(&mut self, owner: &str, lease: chrono::Duration, now: DateTime<Utc>) {
        self.status = TaskStatus::Running;
        self.started_at = Some(now);
        self.lease_owner = Some(owner.to_string());
        self.lease_expires_at = Some(now + lease);
    }

    /// 结束一次运行，与 [`Self::begin_run`] 对称：开始时刻与租约一起清掉，回到
    /// 「没有人在跑它」。回到 Pending 的行若留着一位持有者，读起来就是一件还有
    /// 人在做的任务，而它其实正等着被重新派发。
    pub fn clear_run(&mut self) {
        self.started_at = None;
        self.lease_owner = None;
        self.lease_expires_at = None;
    }

    /// 持有者心跳：把到期时刻推后。只有仍在 Running 的任务需要续期——其它状态
    /// 已经不由任何人持有，续它等于伪造一个持有者。返回是否真的续上了。
    pub fn renew_lease(
        &mut self,
        owner: &str,
        lease: chrono::Duration,
        now: DateTime<Utc>,
    ) -> bool {
        if self.status != TaskStatus::Running || self.lease_owner.as_deref() != Some(owner) {
            return false;
        }
        self.lease_expires_at = Some(now + lease);
        true
    }

    /// 回收判据读的到期时刻：显式租约优先，没有租约时按「开始时刻 + 租约时长」
    /// 兜底。`None` 表示既没有租约也没有开始时刻——两种证据都没有，不构成过期。
    pub fn lease_expiry(&self, lease: chrono::Duration) -> Option<DateTime<Utc>> {
        self.lease_expires_at
            .or_else(|| self.started_at.map(|s| s + lease))
    }

    /// Whether this task opts into the self-evolution change-generation flow.
    ///
    /// Producers mark the task's own input with
    /// `evolution_mode == "generate_change"`; `TaskType::Custom("self_evolution")`
    /// is the equivalent explicit form, and `platform_issue_fix` /
    /// `platform_ci_fix` use the input marker. One definition, because the
    /// routing decision, the planner, the generator and the evaluator must all
    /// answer the same question.
    ///
    /// Read it from the task, never from a role-shaped prompt context. Those
    /// carry only the fields a role needs — the generator nests the whole task
    /// input under `input` and the evaluator omits it — so a predicate that
    /// inspects the built context silently never matches, and change generation
    /// degrades into ordinary narrative output.
    pub fn is_self_evolution(&self) -> bool {
        matches!(&self.task_type, TaskType::Custom(s) if s == "self_evolution")
            || self
                .input
                .get(INPUT_EVOLUTION_MODE)
                .and_then(|v| v.as_str())
                == Some(EVOLUTION_MODE_GENERATE_CHANGE)
    }

    /// Which entry point submitted this self-evolution task, or `None` for a
    /// task that is not one.
    ///
    /// Read from the task, like [`Self::is_self_evolution`], and for the same
    /// reason: the kind lives on the task and disappears from every context
    /// built out of it.
    ///
    /// The `task_kind` input field wins over the task type because a producer
    /// that sets it is naming its own entry point more precisely than the type
    /// string does — the signal watcher submits several distinct signals under
    /// one type. Producers that only fill in the type are read from there.
    pub fn evolution_intent(&self) -> Option<EvolutionIntent> {
        if !self.is_self_evolution() {
            return None;
        }
        let kind = self
            .input
            .get(INPUT_TASK_KIND)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or(match &self.task_type {
                TaskType::Custom(s) => Some(s.as_str()),
                _ => None,
            })
            .unwrap_or_default();
        Some(EvolutionIntent::classify(kind))
    }

    pub fn is_ready(&self, dag: &TaskDAG) -> bool {
        self.blocked_by.iter().all(|dep_id| {
            dag.tasks
                .get(dep_id)
                .map(|t| t.status == TaskStatus::Completed)
                .unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 状态字符串必须双向可逆：写进 `status` 列的那套拼写，是所有按状态数行
    /// 的查询唯一的依据，往一个方向漂了就会去数另一套拼写写下的行——计数看着
    /// 有值，实际一条都对不上。
    #[test]
    fn every_status_round_trips_through_its_stored_spelling() {
        for status in TaskStatus::ALL {
            let spelling = status.as_str();
            assert_eq!(
                TaskStatus::parse(spelling),
                Some(status),
                "{status:?} does not survive as_str -> parse"
            );
        }
    }

    /// 认不出的拼写不许被猜成某个状态：那样一行来历不明的状态会混进某个计数，
    /// 而"产出方与消费方已经不同语"这件事就再也看不见了。
    #[test]
    fn an_unknown_status_spelling_is_not_guessed() {
        assert_eq!(TaskStatus::parse("suspended"), None);
        assert_eq!(TaskStatus::parse("Running"), None);
        assert_eq!(TaskStatus::parse(""), None);
    }

    /// 每个状态对总数的贡献恰好是一，且落在三个具名桶里至多一个。
    ///
    /// 三个具名桶不覆盖整个枚举：跑完与取消的任务既不在跑、也不在等、也不算
    /// 失败，只进总数。要守的不是"桶加起来等于几"，而是"没有状态被数两遍、也没
    /// 有状态整个丢掉"——总数与三个桶放在一起自相矛盾时，读的人分不清集群真的
    /// 有这么多任务，还是某个状态被重复计入。
    #[test]
    fn every_status_is_counted_once_and_lands_in_at_most_one_bucket() {
        for status in TaskStatus::ALL {
            let mut counts = TaskStatusCounts::default();
            counts.add(&status, 1);

            assert_eq!(counts.total, 1, "{status:?} must move the total by one");
            let bucketed = counts.active + counts.queued + counts.failed;
            assert!(
                bucketed <= 1,
                "{status:?} landed in {bucketed} named buckets at once"
            );
            assert!(
                bucketed == 1 || matches!(status, TaskStatus::Completed | TaskStatus::Cancelled),
                "{status:?} reached no named bucket and is not a closed state"
            );
        }
    }

    /// 全体状态各来一次：总数六个，三个具名桶合起来四个，差的正是跑完与取消。
    /// 四个数字因此互相可推——余数不是被吞掉，而是由总数减出来。
    #[test]
    fn the_named_buckets_leave_the_closed_states_in_the_total_only() {
        let counts = TaskStatusCounts::of(TaskStatus::ALL.iter());
        assert_eq!(counts.total, TaskStatus::ALL.len());
        assert_eq!(counts.active, 1, "running is the only active state");
        assert_eq!(counts.queued, 2, "pending and scheduled are both waiting");
        assert_eq!(counts.failed, 1);
        assert_eq!(
            counts.total - (counts.active + counts.queued + counts.failed),
            2,
            "completed and cancelled are the remainder"
        );
    }

    /// 计数是可加的，且按状态而不是按调用次数累计。
    #[test]
    fn counts_accumulate_by_status() {
        let counts = TaskStatusCounts::of([
            &TaskStatus::Running,
            &TaskStatus::Running,
            &TaskStatus::Failed,
        ]);
        assert_eq!(
            counts,
            TaskStatusCounts {
                total: 3,
                active: 2,
                queued: 0,
                failed: 1
            }
        );
    }

    /// 失败的类型必须能过桥：任务记录与失败消息都要带着它往返，且没有这个
    /// 字段的老发布者发的消息仍要能被读成"没有类型"。
    #[test]
    fn failure_cause_survives_the_wire_and_older_messages_do_not() {
        let mut task = Task::new("t1", TaskType::DagNode, serde_json::json!({}));
        task.error = Some("LLM upstream refused (quota_exhausted): spent".into());
        task.error_cause = Some(UpstreamFailure::QuotaExhausted);
        let json = serde_json::to_string(&task).unwrap();
        let back: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(back.error_cause, Some(UpstreamFailure::QuotaExhausted));

        let msg = DagMessage::TaskFailed {
            message_id: "m1".into(),
            timestamp: Utc::now(),
            task_id: "t1".into(),
            error: "boom".into(),
            error_cause: Some(UpstreamFailure::Auth),
            retry_after_secs: Some(90),
            sender: "executor-loop".into(),
            recipient: "dag-executor".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        match serde_json::from_str::<DagMessage>(&json).unwrap() {
            DagMessage::TaskFailed {
                error_cause,
                retry_after_secs,
                ..
            } => {
                assert_eq!(error_cause, Some(UpstreamFailure::Auth));
                assert_eq!(retry_after_secs, Some(90));
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }

        // 老发布者的消息里没有这两个字段：只能读成"没有类型、没有明说的等待
        // 可依"，不能猜。
        let older = serde_json::json!({
            "type": "task_failed",
            "message_id": "m2",
            "timestamp": Utc::now(),
            "task_id": "t2",
            "error": "quota exceeded",
            "sender": "executor-loop",
            "recipient": "dag-executor",
        });
        match serde_json::from_value::<DagMessage>(older).unwrap() {
            DagMessage::TaskFailed {
                error_cause,
                retry_after_secs,
                ..
            } => {
                assert_eq!(error_cause, None);
                assert_eq!(retry_after_secs, None);
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }

    #[test]
    fn self_evolution_task_type_is_self_evolution() {
        let task = Task::new(
            "t1",
            TaskType::Custom("self_evolution".into()),
            serde_json::json!({}),
        );
        assert!(task.is_self_evolution());
    }

    #[test]
    fn evolution_mode_marker_is_self_evolution() {
        // The marker is how the platform integrations opt in: the task type is
        // the platform's own (platform_issue_fix / platform_ci_fix), not
        // self_evolution.
        let task = Task::new(
            "t2",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it", "evolution_mode": "generate_change"}),
        );
        assert!(task.is_self_evolution());
    }

    #[test]
    fn other_evolution_modes_and_plain_tasks_are_not_self_evolution() {
        let baseline = Task::new(
            "t3",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"evolution_mode": "baseline_port"}),
        );
        assert!(!baseline.is_self_evolution());

        let plain = Task::new(
            "t4",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it"}),
        );
        assert!(!plain.is_self_evolution());

        let generated = Task::new(
            "t5",
            TaskType::Custom("generator".into()),
            serde_json::json!({}),
        );
        assert!(!generated.is_self_evolution());
    }

    /// The `task_kind` field names the entry point more precisely than the task
    /// type does, and wins where both are present: the signal watcher submits
    /// several distinct signals under the one `self_signal` type.
    #[test]
    fn the_entry_point_is_read_from_the_task_kind_field_first() {
        let task = Task::new(
            "t1",
            TaskType::Custom("self_signal".into()),
            serde_json::json!({"evolution_mode": "generate_change", "task_kind": "self_audit"}),
        );
        assert_eq!(task.evolution_intent(), Some(EvolutionIntent::SelfAudit));
    }

    /// Producers that only fill in the task type are read from there, and an
    /// empty `task_kind` is treated as absent rather than as an unnamed class —
    /// a producer that leaves the field blank means "use the type".
    #[test]
    fn the_task_type_carries_the_entry_point_when_the_field_is_blank() {
        let typed = Task::new(
            "t1",
            TaskType::Custom("platform_pr_intent".into()),
            serde_json::json!({"evolution_mode": "generate_change"}),
        );
        assert_eq!(typed.evolution_intent(), Some(EvolutionIntent::PrIntent));

        let blank = Task::new(
            "t2",
            TaskType::Custom("platform_ci_fix".into()),
            serde_json::json!({"evolution_mode": "generate_change", "task_kind": ""}),
        );
        assert_eq!(blank.evolution_intent(), Some(EvolutionIntent::CiFix));
    }

    /// A task that is not generating changes has no entry point, and saying
    /// `None` is what keeps an ordinary task out of the funnel rather than in
    /// it under a borrowed class.
    #[test]
    fn a_task_that_is_not_self_evolution_has_no_intent() {
        let plain = Task::new(
            "t1",
            TaskType::Custom("platform_issue_fix".into()),
            serde_json::json!({"goal": "fix it"}),
        );
        assert_eq!(plain.evolution_intent(), None);
    }

    /// Every task kind a producer in this workspace declares is a named class.
    ///
    /// The file is read and searched for the kind: a producer that is renamed
    /// or deleted fails here instead of quietly leaving its class unreachable,
    /// and a kind that a new producer invents lands in `Unattributed` — which
    /// the census publishes as its own series and the alert rules watch, so an
    /// entry point added later is visible rather than absorbed.
    #[test]
    fn every_declared_task_kind_classifies_to_a_named_entry_point() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let declared: &[(&str, &[&str], EvolutionIntent)] = &[
            (
                "self_signal",
                &["crates/cog-reflection/src/signal_watcher.rs"],
                EvolutionIntent::SelfSignal,
            ),
            (
                "self_audit",
                &["crates/cog-reflection/src/signal_watcher.rs"],
                EvolutionIntent::SelfAudit,
            ),
            (
                "platform_ci_fix",
                &[
                    "crates/cog-github/src/discovery_loop.rs",
                    "crates/cog-github/src/landing.rs",
                ],
                EvolutionIntent::CiFix,
            ),
            (
                "platform_issue_fix",
                &["crates/cog-github/src/discovery_loop.rs"],
                EvolutionIntent::IssueFix,
            ),
            (
                "platform_pr_intent",
                &["crates/cog-github/src/discovery_loop.rs"],
                EvolutionIntent::PrIntent,
            ),
            (
                "baseline_port_rework",
                &["crates/cog-reflection/src/baseline_port.rs"],
                EvolutionIntent::BaselineRework,
            ),
            (
                "change_rework",
                &["crates/cog-reflection/src/lib.rs"],
                EvolutionIntent::ChangeRework,
            ),
            (
                "self_evolution",
                &["crates/cog-core/src/types/task.rs"],
                EvolutionIntent::SelfEvolution,
            ),
            (
                "change_ledger",
                &["crates/cog-orchestrator/src/change_ledger.rs"],
                EvolutionIntent::SelfEvolution,
            ),
        ];

        let mut problems: Vec<String> = Vec::new();
        for (kind, files, expected) in declared {
            let mapped = EvolutionIntent::classify(kind);
            if mapped != *expected {
                problems.push(format!("{kind} classifies as {mapped:?}, not {expected:?}"));
            }
            if mapped == EvolutionIntent::Unattributed {
                problems.push(format!("{kind} is declared by a producer but unattributed"));
            }
            for file in *files {
                let text = std::fs::read_to_string(root.join(file)).unwrap_or_default();
                if !text.contains(kind) {
                    problems.push(format!("{kind} is no longer declared in {file}"));
                }
            }
        }

        // A class with no declared producer is either a producer this table
        // has not caught up with or a class nothing can reach; both are worth
        // failing over, and neither is visible from the mapping alone.
        let named: Vec<EvolutionIntent> = declared.iter().map(|(_, _, i)| *i).collect();
        for intent in EvolutionIntent::ALL {
            if intent != EvolutionIntent::Unattributed && !named.contains(&intent) {
                problems.push(format!("{intent:?} has no producer declaring it"));
            }
        }

        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    /// 每个来源都发布在自己的标签上，且都在 `ALL` 上。
    ///
    /// 下面这个 match 是刻意穷尽的：新增一个来源会让它编译不过，逼着人回到这
    /// 份断言，而不是只加变体、让 `ALL` 少一格——指标面发布的是 `ALL` 上每一格
    /// 的计数，少的那一格永远是零，读起来与"没有任何来源走过这条路"一样，而它
    /// 真正的意思是"这个来源从来没有被发布过"。
    #[test]
    fn every_goal_class_source_is_published_under_its_own_label() {
        let mut labels = std::collections::BTreeSet::new();
        for source in [GoalClassSource::Carried, GoalClassSource::HostType] {
            match source {
                GoalClassSource::Carried | GoalClassSource::HostType => {}
            }
            assert!(
                GoalClassSource::ALL.contains(&source),
                "{source:?} 不在 ALL 上，指标面永远不会发布这一格"
            );
            assert!(
                !source.as_str().is_empty(),
                "{source:?} 的标签是空串，按标签取值的读者找不到它"
            );
            assert!(
                labels.insert(source.as_str()),
                "两个来源渲染成同一个标签 {:?}，两格会在指标面上合并成一格",
                source.as_str()
            );
        }
        assert_eq!(labels.len(), GoalClassSource::ALL.len());
    }

    /// 目标行的形状由这一个构造定死：读面靠这几个字段认它，任一处漂了都会让
    /// 「这道目标的答案在哪一行」重新变成猜。
    #[test]
    fn the_goal_row_carries_its_identity_and_is_not_work() {
        let row = Task::goal_row("goal-1", "刷新接口未区分访问令牌");
        assert_eq!(row.id, "goal-1");
        assert_eq!(row.goal_id.as_deref(), Some("goal-1"));
        assert_eq!(row.task_type, TaskType::Custom("goal".to_string()));
        assert_eq!(
            row.input["goal"],
            serde_json::json!("刷新接口未区分访问令牌")
        );
        assert!(
            !row.is_executable,
            "the goal row is a marker for the goal, never schedulable work"
        );
        assert!(
            !row.is_self_evolution(),
            "a goal row must not be read as an evolution intent"
        );
        assert!(row.sink_task_id.is_none(), "the sink is declared later");
    }
}
