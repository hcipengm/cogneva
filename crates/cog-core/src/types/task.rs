use crate::contract::llm::UpstreamFailure;
use crate::contract::outcome::FailureCause;
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
/// 当初排的。所以这张表**没有状态列**：状态的唯一正本是任务状态账本的最后一条迁移
/// （`task_transition`），两张表各存一份状态就是两份真值。
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
    /// 中位时长（秒）：从开工到最近一次更新，排队时间不算工期。
    pub median_duration_secs: u64,
    /// 跑完的这一类里，九成九在多少秒内做完。
    ///
    /// 与中位数分开取，因为两者的用途不能互换：中位数回答「这一类活一般要多久」，
    /// 拿它当硬停等于让一半的活必然停在半路。时间界要的是分布的末端——它回答
    /// 「这一类活跑完时最多用了多久」，所以取的是长尾而不是中段。
    ///
    /// 取九成九而不是最大值：一个病态样本（机器卡住、等人批了很久）就能把最大值
    /// 拉到一个再也不会被越过的数，墙就永远不亮了；而长尾本身要是常见，九成九
    /// 自己就在长尾上。0 表示这一类还没有可用的时长样本。
    pub p99_duration_secs: u64,
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
    /// 失败原因的类型：上游拒绝（来自 HTTP 状态码）或环境拒绝（池停供、进程被
    /// 要求停下）。
    ///
    /// [`Self::error`] 是给人看的文本，它随上游措辞和语言变化；任何判定读这个
    /// 字段。为 `None` 表示这次失败只有文本可依——那时下游不得从文本里猜类型。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_cause: Option<UpstreamFailure>,
    /// 这笔账记在谁头上：工作的，还是环境的。
    ///
    /// 与 [`Self::error_cause`] 分开。那一格只说「哪个上游拒了这次调用」，是
    /// 传输层给出的事实；这一格答的是重试判决要问的问题，并且覆盖两个没有上游
    /// 参与的原因（池停供、进程被要求停下）。分开的另一个理由是滚动部署：加在
    /// `error_cause` 里意味着老进程读到一个不认识的原因名，那一行乃至整个工作区
    /// 的读都会失败；新增一个可选字段，老进程直接忽略它。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_payer: Option<FailureCause>,
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
    /// 造这条任务的那一侧自己愿意等多久（秒）。
    ///
    /// **它不是平台给这条任务的一道墙。** 平台不按已用时长回收或取消任何一条
    /// 正在跑的尝试：一条慢而一直在产出的执行买到的是产出，砍掉它就是把那一轮
    /// 的钱扔掉。判「它是不是卡住了」读的是实测的步间隔
    /// （`cog_core::progress`），与这个数无关。
    ///
    /// 留着它，是因为还有调用方**自己**要等：评测桥要决定一个用例最多等多久，
    /// 沙盒工具要把一个外部进程按住不让它跑飞。那些等待由发起方兑现，不由平台
    /// 兑现——平台这边读它的地方只有读数（「按经验这条活该有结局了」），没有
    /// 一处拿它做删除或取消。
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
    /// 这道活落在**哪个场地**上：memory、wiki、eval…
    ///
    /// 三个问题是分开的：`owner_unit` / `agent_id` 回答「谁在做」，这一列回答
    /// 「在哪儿做」，`acceptance` 回答「怎么算做完」。合成一列就会长出「memory
    /// 员工」这种角色——同一个通用员工今天在这个场地、明天在另一个，场地跟着活
    /// 走，不跟着身份走。
    ///
    /// 场地不是「面」（`face_id`）：面是组织上的责任归属，一个面可以同时往几个
    /// 场地交活，一个场地也可以收几个面的活。`None` 表示这件活不属于任何场地
    /// （平台内部任务、旧行），读面不得把「没填」读成某个默认场地。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
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
    /// 这件活是**谁交上来的**。
    ///
    /// 与 `owner_unit` 和 `agent_id` 答的是三个不同的问题：`owner_unit` 是「指定谁
    /// 来做」，`agent_id` 是「结果谁交的」，这一列是「谁为这件活付钱」。它只由提交
    /// 口在验过令牌之后写，调用方在请求体里自报的名字不算——那一格决定人的当日额度，
    /// 一个能自己填它的调用方等于能把自己的花销记到别人头上。
    ///
    /// 放在任务行上而不是只在请求上过一遍，是因为钱是在**任务跑起来之后**才花的：
    /// 分解出的子任务、重试、后台重跑，没有一处在处理那次请求，它们只有这一行可读。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_by: Option<String>,
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

    /// Whether a row in this state can still change a dispatch decision.
    ///
    /// A ready scan reads the whole graph to answer three questions: what may
    /// start, what is already moving, and which dependencies have been met.
    /// Two of the six states cannot bear on any of them. A `Failed` row never
    /// starts, and a `Cancelled` row never will; `blocked_by` resolves only
    /// through `Completed`, so neither can satisfy a dependency either.
    /// `Completed` is the one terminal state that stays: it is what makes a
    /// successor ready.
    ///
    /// This is a separate question from [`Self::is_terminal`] and is not its
    /// negation. The difference is exactly `Completed`, and a reader that
    /// conflated the two would stop resolving dependencies the moment it
    /// skipped terminal rows — every dependent would read as blocked forever.
    /// Measured on 2026-11-05: 5,843 of 9,916 rows in one workspace were
    /// `Cancelled` or `Failed`, and every dispatch pass was reading them back.
    pub fn affects_dispatch(&self) -> bool {
        !matches!(self, TaskStatus::Failed | TaskStatus::Cancelled)
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

    /// The venue this task is filed under, trimmed, or `None` when it carries
    /// no label.
    ///
    /// Defined once because three readers decide membership by it -- the
    /// board's fold, the store query that prefilters rows for it, and the
    /// in-memory graph path -- and a row the query keeps but the fold drops
    /// (or the other way round) is a column that silently disagrees with the
    /// tasks it is supposed to be counting. A label of only spaces is not a
    /// venue: the submit path already drops empty labels, and a row written by
    /// something that did not would otherwise appear as a venue with no name.
    pub fn venue_label(&self) -> Option<&str> {
        self.venue
            .as_deref()
            .map(str::trim)
            .filter(|venue| !venue.is_empty())
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

/// 一个工作区的任务结果流名前缀，后面接 workspace id。
///
/// 这条名字的两头在不同 crate 里：执行器把结果发上来（编排器与自带执行能力的
/// 进程各发各的），DagExecutor 的消费循环在另一处读它并建消费组。两头各自
/// `format!` 一遍时，改名只改一侧不会有任何报错——发布方报「已发送」，消费组的
/// 那一侧读到的是空流，而空流与「还没有任务完成」长得一模一样，任务就停在
/// running，依赖它的下游永远不就绪，没有任何一个面能说出丢了什么。
pub const TASK_RESULT_STREAM_PREFIX: &str = "orchestrator:results:";

/// 一个工作区的任务结果流名。生产方与消费方都从这里取。
pub fn task_result_stream(workspace_id: &str) -> String {
    format!("{TASK_RESULT_STREAM_PREFIX}{workspace_id}")
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
        /// 这笔账该记在谁头上，类型化的完整原因（含没有上游参与的两种）。
        /// 与 `error_cause` 分开传递，理由同任务行上的同名字段：老版本订阅者
        /// 不认识这个字段，直接忽略；塞进 `error_cause` 会让它整条解析失败。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure_payer: Option<FailureCause>,
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

/// The `task.input` key that carries the principles this unit of work is held
/// to, as the reading named them.
///
/// The producer fills it because the producer is the only one that knows which
/// principles bear on this piece of work — the face it serves, the paths it will
/// touch. The generator reads it and prints it; it does not select. A key rather
/// than a sentence laid out at each call site, for the same reason as the two
/// above: a caller that spells the word by hand and a reader that compares
/// against a constant are two spellings that drift apart silently.
pub const INPUT_PRINCIPLES: &str = "principles";

/// The `task.input` key that names the ready-queue lane this task must travel in.
///
/// The queue is a first-in-first-out stream with one consumer: a message
/// appended today is read after every message already in front of it. That is
/// the right shape for ordinary work — it lets a slow producer meet a fast
/// consumer without losing anything — and the wrong shape for work whose whole
/// value is that it happens now. A producer that says "nothing else may go
/// first" needs a lane of its own, because no priority, however high, reorders
/// a message that was already handed to the stream.
///
/// The producer writes the lane; the queue reads it when it decides where a
/// ready task is published. One key rather than a string spelled at both ends,
/// for the reason the markers above state.
pub const INPUT_READY_LANE: &str = "ready_lane";

/// The `task.input` key by which a task says its work changes the checkout.
///
/// 「这件活要动工作树」是任务自己的事，不是接手的人是谁的事。按角色判时，同一个
/// 人接了另一类活就判错：一位工程师做评审时被当成该写树的（于是被「半小时没动」
/// 收住），而一个小队队员做变更时被当成只读的（于是绕圈一小时没人叫停）。生产
/// 者在提交时说清楚这一件活要交什么，读的人只问任务。
///
/// 写成键而不是一句话，理由与上面几个标记相同：两端各拼一遍，写的那一端继续写
/// 旧词、读的那一端比对新词，任务只是安静地不再被认出来。
pub const INPUT_DECLARES_CHANGE: &str = "declares_change";

/// The `task.input` key by which a task says what it delivers.
///
/// 这是 [`INPUT_DECLARES_CHANGE`] 所代理的那个量本身。判据、输出形状、任务类型都跟
/// 「这份活交什么」走，而这件事只有任务说得出来；以前它们从接手人的角色名反推，于是
/// 同一件活换个标签接手就换一套待遇。声明写明时以声明为准，`declares_change` 只作为
/// 旧生产者还不会写新键时的兼容读法。
pub const INPUT_DELIVERS: &str = "delivers";

/// The `task.input` key under which a signal-filed task carries the detail of
/// the reading that filed it.
///
/// The reader that filed the row (`signal_watcher`) puts the whole detail here,
/// so a task made from an outside judgement still carries what was said when
/// it is judged later. Read through [`Task::stated_directions`]; spelled as a
/// key for the same reason as the markers above — two hand-written copies drift.
pub const INPUT_SIGNAL: &str = "signal";

/// The detail's own `kind` for a taste submission.
///
/// A signal detail is tagged by kind, and only this kind carries a taste
/// payload. Matching on anything else would let an alert whose detail happens
/// to have a `payload` key be read as a stated direction.
pub const TASTE_SIGNAL_KIND: &str = "taste_intent";

/// What a piece of work delivers.
///
/// The closed set the treatment of the work follows: the rubric that judges it,
/// the shape its output is asked for in, the task type it runs as. Who takes the
/// work is not one of the inputs — the same work taken by two labels must be
/// treated the same.
///
/// `Unstated` is a state rather than a kind: the row did not say what it
/// delivers. It is spelled out instead of defaulting to the commonest kind,
/// because a default would make "nobody said" and "somebody said work product"
/// read the same, and telling those two apart is the whole point of recording
/// the declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Deliverable {
    /// A decomposition of a task into sub-tasks.
    Plan,
    /// A change to the checkout: an expected state of the tree.
    Change,
    /// A judgement about someone else's work.
    Judgement,
    /// A final answer to the task's question.
    Answer,
    /// Anything else the task asked for: a report, a build artifact, one run.
    WorkProduct,
    /// The row did not say. Not a kind; a state that readers must answer for.
    Unstated,
}

impl Deliverable {
    /// Read the word a producer writes, or nothing for a word outside the set.
    ///
    /// The two spellings `judgement` / `judgment` and the dashed form of
    /// `work_product` are accepted because they are what people actually type in
    /// a JSON body; anything else is not silently coerced to a kind.
    pub fn from_word(word: &str) -> Option<Self> {
        match word.trim() {
            "plan" => Some(Self::Plan),
            "change" => Some(Self::Change),
            "judgement" | "judgment" => Some(Self::Judgement),
            "answer" => Some(Self::Answer),
            "work_product" | "work-product" => Some(Self::WorkProduct),
            "unstated" => Some(Self::Unstated),
            _ => None,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Change => "change",
            Self::Judgement => "judgement",
            Self::Answer => "answer",
            Self::WorkProduct => "work_product",
            Self::Unstated => "unstated",
        }
    }

    /// Whether a run for this work is expected to change the checkout.
    ///
    /// The one question the runtime asked before the set existed; kept as a
    /// reading of the declaration rather than as a second field.
    pub const fn declares_change(&self) -> bool {
        matches!(self, Self::Change)
    }
}

/// The lane for work that must run before ordinary work, whether or not the
/// ordinary lane is full.
///
/// Its one user is the repair of a red main: the mainline deployer holds every
/// rollout while upstream CI reports a failure, so every fix waiting behind the
/// ordinary queue is a fix the whole platform waits on. The lane is served by a
/// consumer with a budget of its own, which is what makes the word "priority"
/// mean something here rather than only describing the order of a list.
pub const READY_LANE_STOP_THE_LINE: &str = "stop_the_line";

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
            failure_payer: None,
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
            venue: None,
            face_id: None,
            owner_unit: None,
            bound_by_handover_id: None,
            submitted_by: None,
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
    ///
    /// 上一轮留下的判词也在这里清掉：`error` 说的是**这一行为什么停在现在这个
    /// 状态**，而一次新运行开始时它已经不再说明任何事。回队的那一条尤其要说清
    /// ——「在 Scheduled 停了 600 秒、从没启动过，于是放回队里」是一句关于**等
    /// 待**的话，行一旦真的跑起来，这句话就是假的；留着它，一个跑成了的任务会
    /// 带着「failed 似的」字样停在 completed 上（2026-11-05 实测 6 小时内 41 条
    /// completed 里有 37 条带着这句回队词），任何按「有 error 就是坏了」取的读
    /// 者都会把它读成一次失败。
    ///
    /// 判词在派发时先读、后清：[`Self::error`] 只在下一次失败时被覆盖，所以
    /// 「上一次是不是被时限截断的、下一轮该给多少时间」这一问必须在清掉之前问。
    /// 清在这里而不是每个调用点各写一遍，是因为「一次运行从哪一刻开始」只有
    /// 这一个入口，漏写一处就会留下一行说自己失败过的成功任务。
    pub fn begin_run(&mut self, owner: &str, lease: chrono::Duration, now: DateTime<Utc>) {
        self.status = TaskStatus::Running;
        self.started_at = Some(now);
        self.lease_owner = Some(owner.to_string());
        self.lease_expires_at = Some(now + lease);
        self.error = None;
        self.error_cause = None;
        self.failure_payer = None;
    }

    /// 结束一次运行，与 [`Self::begin_run`] 对称：开始时刻与租约一起清掉，回到
    /// 「没有人在跑它」。回到 Pending 的行若留着一位持有者，读起来就是一件还有
    /// 人在做的任务，而它其实正等着被重新派发。
    pub fn clear_run(&mut self) {
        self.started_at = None;
        self.lease_owner = None;
        self.lease_expires_at = None;
    }

    /// 接活：待接 → 已认领的唯一写点。
    ///
    /// 认领写在 `agent_id` 上，不新开一列。这一格在认领到交付之间只有一个含义
    /// ——「谁为这条行的结果负责」：认领时是接过它的人，跑起来以后仍是同一个人
    /// （一次运行只能由它的认领者来跑），交付以后还是它。中间不会出现第二个人，
    /// 所以一个格子够。开一列 `claimed_by` 的代价是把同一句话存两处，然后要
    /// 回答「两处不一致时信哪个」，而那正是这里最不该有的一个问题。
    ///
    /// `Pending` 行上的 `agent_id` 不读作认领（[`Self::claimed_by`] 就在这一处
    /// 收口）：回到队里的行是一次跑完或回了队的行的旧值，它按定义没人持有。
    ///
    /// 租约与认领同时写，和 [`Self::begin_run`] 同一个理由：只写认领不写租约的
    /// 行，在认领者消失以后永远没人能接手，而它看起来像是有人在干。
    pub fn take_claim(&mut self, claimant: &str, lease: chrono::Duration, now: DateTime<Utc>) {
        self.status = TaskStatus::Scheduled;
        self.agent_id = Some(claimant.to_string());
        self.lease_owner = Some(claimant.to_string());
        self.lease_expires_at = Some(now + lease);
        self.updated_at = now;
    }

    /// 这条行此刻有没有人认领。`Pending` 是「待接」，不是「已认领」。
    ///
    /// 交了活的行（`Completed` / `Failed` / `Cancelled`）也不再有人持有：那一格
    /// 记的是谁交的，不是谁还拿着。
    pub fn claimed_by(&self) -> Option<&str> {
        match self.status {
            TaskStatus::Scheduled | TaskStatus::Running => self.agent_id.as_deref(),
            _ => None,
        }
    }

    /// 认领者的租约还有没有效。没有效 = 那个人已经不在了，这条行可以再被接走。
    ///
    /// 与 [`Self::lease_expiry`] 分开命名，因为两者答的不是一回事：那个问「这个
    /// 进程还持不持有」，这个问「这条行还有没有人认」。认领者消失之后进程身份
    /// 已经换过，只有租约还留在行上。
    pub fn claim_is_live(&self, lease: chrono::Duration, now: DateTime<Utc>) -> bool {
        self.claimed_by().is_some()
            && self
                .lease_expiry(lease)
                .is_some_and(|expires| expires > now)
    }

    /// 这条行此刻被谁持有——只认行上写着的到期时刻。
    ///
    /// 与 [`Self::claim_is_live`] 分开：那个用调用方手里的租约长度去补一条没写
    /// 到期时刻的老行，这个只读板上那一个数。存储里的守卫（认领只能落一次）
    /// 手里没有租约长度，它读的就是这一条：有名字、且到期时刻还没到，才算有人
    /// 持有。已经过期或没写到期时刻的认领不拦人——那是「那个人不在了」，不是
    /// 「这件活有人干」。
    pub fn claim_held_now(&self, now: DateTime<Utc>) -> Option<&str> {
        self.claimed_by()
            .filter(|_| self.lease_expires_at.is_some_and(|expires| expires > now))
    }

    /// Whether re-running this task could succeed, judged on both channels a
    /// failure can arrive by: the typed `error_cause` (what the transport
    /// saw) and the in-band declaration inside the rendered `error` text,
    /// which is what carries the verdict when the failure was found one or
    /// more layers below the transport and arrives wrapped in another layer's
    /// prose. Either channel saying the failure is deterministic settles it:
    /// a retry that reads only the near one pays again for every failure
    /// whose cause was found far away.
    pub fn is_retryable_failure(&self) -> bool {
        !self.error_cause.is_some_and(UpstreamFailure::is_terminal)
            && !self
                .error
                .as_deref()
                .is_some_and(crate::contract::outcome::is_deterministic_failure)
    }

    /// 退避是否已到期：没有退避约束，或约束的时刻已过。失败回投把下次可执行
    /// 时刻写在这行的 `retry_not_before` 上，判定重试的进程与之后重新投递它的
    /// 进程可能不是同一个，所以「到点没有」只能从这个字段读，投递侧不另算。
    pub fn backoff_elapsed(&self, now: DateTime<Utc>) -> bool {
        self.retry_not_before.is_none_or(|due| due <= now)
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

    /// 这件活要不要动工作树——由任务说，不由接手的人的角色说。
    ///
    /// 两种声明都算数：
    ///
    /// - 输入里明说（[`INPUT_DECLARES_CHANGE`] 为真）。生产者在提交时知道这一件
    ///   活要交一份改动还是只交一段结论。
    /// - 自进化生成变更这一类（[`Self::is_self_evolution`]）。它的标记本身就是
    ///   「产出一份变更」那句话，再让生产者抄一遍只是多一个会漂的副本。
    ///
    /// 读的人要的是「这一件活得改树吗」，问出来的答案只取决于任务本身：同一个人
    /// 接了另一件活，答案跟着那件活变；角色标签怎么变都不影响它。
    pub fn deliverable(&self) -> Deliverable {
        // 写明的声明优先：它是这套读法要补的那个量本身，`declares_change`
        // 只是它缺席时的兼容读法。两者同时出现且互相矛盾时以声明为准——
        // 让布尔盖住声明，等于让旧键继续决定新键该说什么。
        if let Some(word) = self.input.get(INPUT_DELIVERS).and_then(|v| v.as_str()) {
            if let Some(named) = Deliverable::from_word(word) {
                return named;
            }
        }
        let named = match self.input.get(INPUT_DECLARES_CHANGE) {
            Some(serde_json::Value::Bool(flag)) => *flag,
            Some(serde_json::Value::String(word)) => {
                matches!(word.trim(), "true" | "yes" | "1")
            }
            _ => false,
        };
        if named || self.is_self_evolution() {
            Deliverable::Change
        } else {
            Deliverable::Unstated
        }
    }

    pub fn declares_change(&self) -> bool {
        self.deliverable().declares_change()
    }

    /// 有人就这份活说过的方向，原文照录；没人说过就是空的。
    ///
    /// 「这一轮什么算好」不只有规格说了算。外部提交的判词里，`direction_preference`
    /// 说了两个方向里偏好哪一个，`rubric_override` 说判据本身该改成什么——这两类此前
    /// 只变成一件新活，改不动判活的标准。文件这条任务的那次读数把提交的内容留在
    /// [`INPUT_SIGNAL`] 里，所以从任务自己读得出来：与 [`Self::deliverable`] 同一个
    /// 读法，读任务，不读接手的人。
    ///
    /// 只收这两类。`evaluator_verdict` 说的是某份材料够不够格，不是「以后按什么算
    /// 好」；把它也当成方向，等于让一次对结果的判断永久改写判据。读不出、种类不对、
    /// 字段缺，一律当没说过——这里不猜。
    pub fn stated_directions(&self) -> Vec<String> {
        let Some(detail) = self.input.get(INPUT_SIGNAL) else {
            return Vec::new();
        };
        if detail.get("kind").and_then(|v| v.as_str()) != Some(TASTE_SIGNAL_KIND) {
            return Vec::new();
        }
        let Some(payload) = detail.get("payload") else {
            return Vec::new();
        };
        let Ok(payload) = serde_json::from_value::<crate::TasteIntentPayload>(payload.clone())
        else {
            return Vec::new();
        };
        if matches!(payload, crate::TasteIntentPayload::EvaluatorVerdict(_)) {
            return Vec::new();
        }
        vec![payload.summary()]
    }

    /// The producer's own spelling of this task's entry point, or `None` for a
    /// task that is not a self-evolution one.
    ///
    /// This is the input the classification table is asked about, kept apart
    /// from its answer on purpose: a class written down today is only right for
    /// as long as the table is, and the table is exactly what gets fixed when an
    /// entry point was classified wrongly. A record that carries the spelling
    /// can be re-classified by whatever the table says later; a record that
    /// carries only the class is frozen at the answer of the day it was written.
    ///
    /// The `task_kind` input field wins over the task type because a producer
    /// that sets it is naming its own entry point more precisely than the type
    /// string does — the signal watcher submits several distinct signals under
    /// one type. Producers that only fill in the type are read from there.
    pub fn evolution_kind(&self) -> Option<&str> {
        if !self.is_self_evolution() {
            return None;
        }
        Some(
            self.input
                .get(INPUT_TASK_KIND)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .or(match &self.task_type {
                    TaskType::Custom(s) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or_default(),
        )
    }

    /// Which entry point submitted this self-evolution task, or `None` for a
    /// task that is not one.
    ///
    /// Read from the task, like [`Self::is_self_evolution`], and for the same
    /// reason: the kind lives on the task and disappears from every context
    /// built out of it.
    pub fn evolution_intent(&self) -> Option<EvolutionIntent> {
        self.evolution_kind().map(EvolutionIntent::classify)
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
    use crate::checkout::repo_root;

    /// 一个场地标签只有一种「算有」的拼法。
    ///
    /// 三个读者拿它判成员：看板的折叠、替它预筛行的存储查询、内存里那条
    /// 图路径。任何一处自己再写一遍「非空」，就会有一行被查询留下、被折叠扔掉
    /// （或者反过来），而两边的读数各自都说得通——一栏数出四件活、却只有三件的
    /// 写回量。空格串也在这里落网：提交那一路已经丢掉了空标签，别处写进来的
    /// `"   "` 若算作场地，看板上就多出一栏没有名字的。
    #[test]
    fn a_venue_label_is_what_it_says_after_trimming_and_nothing_else_counts() {
        let mut task = Task::new("t1", TaskType::DagNode, serde_json::json!({}));
        assert_eq!(task.venue_label(), None);
        for spelled_as_empty in ["", "   ", "\t\n"] {
            task.venue = Some(spelled_as_empty.to_string());
            assert_eq!(task.venue_label(), None, "{spelled_as_empty:?}");
        }
        task.venue = Some("  memory ".to_string());
        assert_eq!(task.venue_label(), Some("memory"));
    }

    #[test]
    fn only_the_two_states_that_can_never_move_are_left_out_of_a_dispatch_read() {
        // 逐个变体过一遍 `ALL`，不点名字集：新增一个状态却忘了判它，这条会红，
        // 而不是让新状态悄悄落进「不该被读出来」或者「必须每轮都读」里。
        let kept: Vec<&str> = TaskStatus::ALL
            .iter()
            .filter(|s| s.affects_dispatch())
            .map(|s| s.as_str())
            .collect();
        assert_eq!(kept, vec!["pending", "scheduled", "running", "completed"]);
        // 与 `is_terminal` 的差正好是 `completed`：把两者当成一个，依赖就永远
        // 解不开——被依赖的那一行读不回来，后继每一轮都判成阻塞。
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Completed.affects_dispatch());
    }

    /// 一次新的运行开始，上一轮留在行上的判词就不作数了。
    ///
    /// 回队那一句（「在 Scheduled 停了 N 秒、从没启动过，于是放回队里」）说的是
    /// 等待，不是失败：行一旦跑起来，它就成了假话。留着它的代价在读数面上——
    /// 2026-11-05 实测 6 小时内 41 条 completed 里有 37 条带着这句，按「有 error
    /// 就是坏了」取的读者会把跑成的活读成失败。
    #[test]
    fn a_run_that_begins_carries_no_verdict_from_the_run_before_it() {
        let mut task = Task::new("t1", TaskType::DagNode, serde_json::json!({}));
        task.error = Some(
            "never started: the task sat in Scheduled for more than 600s without ever being \
             started, so it was put back in line without charging an attempt"
                .into(),
        );
        task.error_cause = Some(UpstreamFailure::QuotaExhausted);

        task.begin_run("unit-a", chrono::Duration::seconds(300), Utc::now());

        assert_eq!(task.status, TaskStatus::Running);
        assert!(task.error.is_none(), "开始跑了，上一轮的判词不再说明任何事");
        assert!(task.error_cause.is_none(), "类型通道同理");
    }

    /// 接活的迁移写下了负责人与租约，租约到期后这条行重新算「没人认领」。
    ///
    /// 三处读法必须同源：`claimed_by` 答「谁接了」，`claim_is_live` 答「他还在
    /// 不在」，而认领者消失之后这条行要能被下一个人接走。判据若分三处写，最先
    /// 漂的总是「租约到期」那一份。
    #[test]
    fn a_claim_names_who_took_the_row_and_lapses_with_its_lease() {
        let now = Utc::now();
        let lease = chrono::Duration::seconds(300);
        let mut task = Task::new("t1", TaskType::DagNode, serde_json::json!({}));
        assert_eq!(task.claimed_by(), None, "待接的行没有人认领");

        task.take_claim("E0412", lease, now);
        assert_eq!(task.status, TaskStatus::Scheduled);
        assert_eq!(task.claimed_by(), Some("E0412"));
        assert_eq!(task.lease_owner.as_deref(), Some("E0412"));
        assert_eq!(task.lease_expires_at, Some(now + lease));
        assert!(task.claim_is_live(lease, now + chrono::Duration::seconds(299)));
        assert!(
            !task.claim_is_live(lease, now + chrono::Duration::seconds(301)),
            "认领者过了自己的租约就不再持有它"
        );
        assert_eq!(
            task.claim_held_now(now + chrono::Duration::seconds(299)),
            Some("E0412"),
            "板上的守卫读同一个口径：有名字且租约没到期"
        );
        assert_eq!(
            task.claim_held_now(now + chrono::Duration::seconds(301)),
            None,
            "过期之后这个名字不再拦人"
        );

        task.begin_run("run-7", lease, now);
        assert_eq!(
            task.claimed_by(),
            Some("E0412"),
            "跑起来以后责任人不变：接活的人就是干活的人"
        );
        assert_eq!(
            task.lease_owner.as_deref(),
            Some("run-7"),
            "进程身份接管租约，认领者的名字留在责任人那一格"
        );

        task.clear_run();
        task.status = TaskStatus::Pending;
        assert_eq!(task.claimed_by(), None, "回到队里的行按定义没人持有");
    }

    /// 两个通道各自都能判死一次重试：类型通道（`error_cause` 为配额/鉴权这类
    /// 终态拒绝）与带内文本通道（`error` 里经 declares 命中的 terminal 前缀，
    /// 含被错误包装后的形态）——任一命中即不可重试；都没有时保留重试。
    #[test]
    fn either_failure_channel_settles_the_retry_question() {
        let mut task = Task::new("t1", TaskType::DagNode, serde_json::json!({}));
        assert!(task.is_retryable_failure());

        task.error_cause = Some(UpstreamFailure::QuotaExhausted);
        task.error = Some("LLM upstream refused (quota_exhausted): spent".into());
        assert!(!task.is_retryable_failure());

        // 类型通道缺失、只剩被包装过的文本时，带内通道独立成立。
        task.error_cause = None;
        task.error = Some("Agent execution error: terminal_env_failure: prompt failed".into());
        assert!(!task.is_retryable_failure());

        task.error = Some("upstream is unreachable".into());
        assert!(task.is_retryable_failure());
    }

    /// 退避时刻写在这行上：未到期不就绪，到期或没有退避约束就绪。
    #[test]
    fn the_backoff_deadline_gates_readiness() {
        let now = Utc::now();
        let mut task = Task::new("t1", TaskType::DagNode, serde_json::json!({}));
        assert!(task.backoff_elapsed(now));
        task.retry_not_before = Some(now + chrono::Duration::seconds(60));
        assert!(!task.backoff_elapsed(now));
        task.retry_not_before = Some(now - chrono::Duration::seconds(1));
        assert!(task.backoff_elapsed(now));
    }

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
            failure_payer: Some(FailureCause::Upstream(UpstreamFailure::Auth)),
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

    /// 要不要动工作树是任务说的，且换个任务答案就跟着变。
    ///
    /// 旧判据问的是接手的人的角色：同一位工程师做评审时被当成「该写树的」，
    /// 做变更时若不是 generator 又被当成「只读的」。这里把同一份输入换一换，
    /// 只看任务的声明变不变。
    #[test]
    fn a_task_says_whether_its_work_changes_the_checkout() {
        let writes = Task::new(
            "t-review",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ "goal": "raise the timeout", INPUT_DECLARES_CHANGE: true }),
        );
        let reads = Task::new(
            "t-review",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ "goal": "read the timeout" }),
        );
        assert!(writes.declares_change());
        assert!(!reads.declares_change());

        // 生产者从环境里读来的配置常常是字符串。
        let spelled = Task::new(
            "t-spelled",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ INPUT_DECLARES_CHANGE: "true" }),
        );
        assert!(spelled.declares_change());

        // 自进化的生成变更那类任务，标记本身就是声明。
        let evolution = Task::new(
            "t-evo",
            TaskType::Custom("self_evolution".into()),
            serde_json::json!({}),
        );
        assert!(
            evolution.declares_change(),
            "自进化生成变更的任务没有另抄一遍声明就该被认出来"
        );
    }

    /// 闭集里每一个显式取值都读得出来，两种缺席都读成「没说」。
    ///
    /// 「没说」不许兜底成最常见的种类：兜底之后「没人说过」与「有人说是
    /// work_product」在读数上同形，而把这两者分开正是记这一格的用处。
    #[test]
    fn a_task_reads_the_closed_set_of_deliverables() {
        for (word, expected) in [
            ("plan", Deliverable::Plan),
            ("change", Deliverable::Change),
            ("judgement", Deliverable::Judgement),
            ("judgment", Deliverable::Judgement),
            ("answer", Deliverable::Answer),
            ("work_product", Deliverable::WorkProduct),
            ("work-product", Deliverable::WorkProduct),
        ] {
            let task = Task::new(
                "t-word",
                TaskType::Custom("platform_task".into()),
                serde_json::json!({ INPUT_DELIVERS: word }),
            );
            assert_eq!(task.deliverable(), expected, "声明 {word} 读错了");
        }

        let silent = Task::new(
            "t-silent",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ "goal": "read the timeout" }),
        );
        assert_eq!(silent.deliverable(), Deliverable::Unstated);

        // 闭集外的词不猜：一个拼错的声明不比没有声明多知道什么。
        let outside = Task::new(
            "t-outside",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ INPUT_DELIVERS: "plan-ish" }),
        );
        assert_eq!(outside.deliverable(), Deliverable::Unstated);
        assert!(!outside.declares_change(), "不在闭集里的词不该被读成变更");

        // 写明的声明优先于旧键：两者矛盾时以声明为准。
        let both = Task::new(
            "t-both",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ INPUT_DELIVERS: "plan", INPUT_DECLARES_CHANGE: true }),
        );
        assert_eq!(both.deliverable(), Deliverable::Plan);
        assert!(!both.declares_change(), "旧键不该盖过声明");
    }

    /// 外部就这份活说过的方向，任务自己带得住；没说、说了别的种类、读不出来，都不编。
    ///
    /// 这两类判词此前只变成一件新活，改不动判活的标准。任务带着提交时的 detail
    /// 走，判它的时候就还在；这条测试钉的是「读得回来」与「不猜」两半。
    #[test]
    fn a_task_carries_the_direction_stated_about_it() {
        fn filed(input: serde_json::Value) -> Task {
            Task::new("t-direction", TaskType::Custom("self_signal".into()), input)
        }
        let detail = |payload: serde_json::Value| {
            serde_json::json!({
                INPUT_SIGNAL: {
                    "kind": TASTE_SIGNAL_KIND,
                    "taste_kind": payload["kind"].clone(),
                    "payload": payload,
                }
            })
        };

        let preferred = filed(detail(serde_json::json!({
            "kind": "direction_preference",
            "preferred": "the stricter one",
            "alternative": "the looser one",
            "rationale": "the looser one hid a regression"
        })));
        assert_eq!(
            preferred.stated_directions(),
            vec![
                "prefers \"the stricter one\" over \"the looser one\": the looser one hid a \
                 regression"
                    .to_string()
            ],
            "提交原文要原样回到判它的人手里，不是重新概括一遍"
        );

        let override_ = filed(detail(serde_json::json!({
            "kind": "rubric_override",
            "criterion": "the review rubric",
            "new_wording": "quote the failing line",
            "rationale": "prose verdicts could not be checked"
        })));
        assert_eq!(
            override_.stated_directions(),
            vec![
                "the evaluation criterion \"the review rubric\" should read \"quote the failing \
                 line\": prose verdicts could not be checked"
                    .to_string()
            ],
            "改判据的那一类也是方向，同样要到得了判活的那一侧"
        );

        // 对某份材料的判断不是「以后按什么算好」：把它当方向，等于让一次对结果的
        // 判断永久改写判据。
        let verdict = filed(detail(serde_json::json!({
            "kind": "evaluator_verdict",
            "verdict": "insufficient",
            "rationale": "the log lines do not show the retry"
        })));
        assert!(verdict.stated_directions().is_empty());

        // 没有 detail、detail 不是这一种、payload 读不出来：都当没说过。
        let plain = Task::new(
            "t-plain",
            TaskType::Custom("platform_task".into()),
            serde_json::json!({ "goal": "read the timeout" }),
        );
        assert!(plain.stated_directions().is_empty());

        let alert = filed(serde_json::json!({
            INPUT_SIGNAL: { "kind": "persisted_alert", "rule": "ApiDown",
                            "payload": { "kind": "rubric_override", "criterion": "x",
                                         "new_wording": "y", "rationale": "z" } }
        }));
        assert!(
            alert.stated_directions().is_empty(),
            "别的信号碰巧带一个 payload 键，不该被读成方向"
        );

        let broken = filed(serde_json::json!({
            INPUT_SIGNAL: {
                "kind": TASTE_SIGNAL_KIND,
                "payload": { "kind": "direction_preference", "preferred": "a" }
            }
        }));
        assert!(
            broken.stated_directions().is_empty(),
            "字段缺一半的提交读不成方向：猜出来的那半句会冒充提交者的话"
        );
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
        let root = repo_root();
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

    /// 这条名字是线上的事实，不是格式选择：部署里已经存在按这个拼法建的消费
    /// 组，发到别的拼法上的结果**没有任何人读**，而发布方仍然报成功。所以这里
    /// 钉的是拼法本身，不是「函数等于自己」。
    #[test]
    fn the_result_stream_name_does_not_drift() {
        assert_eq!(
            task_result_stream("ws-1"),
            "orchestrator:results:ws-1",
            "改这条名字等于让执行器把结果发进一条没人订阅的流"
        );
    }
}
