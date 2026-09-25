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
    /// 是否可被 DagExecutor 调度执行。
    /// 原始整体任务保留注入时设为 false，仅作为层级占位与查询用。
    #[serde(default = "default_true")]
    pub is_executable: bool,
}

fn default_true() -> bool {
    true
}

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
}

/// Where a goal's class was read from.
///
/// Kept apart rather than folded into the class itself: a class carried with
/// the goal and a class taken from whatever task happens to hold it are two
/// different pieces of evidence about the same string, and the second one is
/// the shape a missing producer takes — the retrieval still answers, under a
/// class that names the run instead of the work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    pub const ALL: [EvolutionIntent; 8] = [
        EvolutionIntent::SelfSignal,
        EvolutionIntent::SelfAudit,
        EvolutionIntent::CiFix,
        EvolutionIntent::IssueFix,
        EvolutionIntent::PrIntent,
        EvolutionIntent::BaselineRework,
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
            "self_evolution" => EvolutionIntent::SelfEvolution,
            _ => EvolutionIntent::Unattributed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
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
            is_executable: true,
        }
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
            || self.input.get("evolution_mode").and_then(|v| v.as_str()) == Some("generate_change")
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
            .get("task_kind")
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
                "self_evolution",
                &["crates/cog-core/src/types/task.rs"],
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
}
