use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Goal Message ──────────────────────────────────────────────────────────

/// 外部目标提交消息，由 Gateway 发布到 `goals:{workspace_id}` stream，
/// 由 DagExecutorRuntime 消费并驱动后续编排。
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    pub timeout_seconds: u64,
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
            blocked_by: Vec::new(),
            blocks: Vec::new(),
            priority: 1,
            created_at: now,
            updated_at: now,
            agent_id: None,
            workspace_id: None,
            retry_count: 0,
            max_retries: 3,
            started_at: None,
            timeout_seconds: 300,
            action_planner_meta: None,
            goal_id: None,
            parent_task_id: None,
            is_executable: true,
        }
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
