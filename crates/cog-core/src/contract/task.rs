use crate::{skill::SkillRegistry, SFResult, Task, TaskType};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Unified result container returned by every `TaskExecutor` implementation.
/// `cog-core` only defines the generic envelope — the schema of `output`
/// and `metadata.extensions` is agreed between the producer
/// (e.g. `CollaborationExecutor`) and the consumer
/// (e.g. `ActionPlanOrchestrator`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub success: bool,
    pub output: Value,
    pub metadata: TaskResultMetadata,
}

/// Execution metadata common to all executor backends.
/// `extensions` is a transparent JSON object so that individual backends
/// can attach domain-specific fields without polluting `cog-core`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultMetadata {
    pub executor_id: String,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Value>,
}

impl TaskResultMetadata {
    pub fn new(executor_id: impl Into<String>) -> Self {
        Self {
            executor_id: executor_id.into(),
            execution_time_ms: 0,
            score: None,
            feedback: None,
            extensions: None,
        }
    }

    pub fn with_execution_time_ms(mut self, ms: u64) -> Self {
        self.execution_time_ms = ms;
        self
    }

    pub fn with_score(mut self, score: f64) -> Self {
        self.score = Some(score);
        self
    }

    pub fn with_feedback(mut self, feedback: impl Into<String>) -> Self {
        self.feedback = Some(feedback.into());
        self
    }

    pub fn with_extensions(mut self, extensions: Value) -> Self {
        self.extensions = Some(extensions);
        self
    }
}

/// Abstract interface for task execution backends.
/// Decouples `cog-orchestrator` from concrete execution crates
/// (`cog-collaboration`, `cog-extension`) so that new backends can be added
/// without modifying the orchestrator.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Returns true when this executor can handle the given task type.
    fn supports(&self, task_type: &TaskType) -> bool;

    /// Execute a single task and return a unified `TaskResult`.
    async fn execute(&self, task: &Task) -> SFResult<TaskResult>;
}

// ─── Action Planner ────────────────────────────────────────────────────────

/// High-level goal-decomposition interface.
#[async_trait]
pub trait ActionPlanner: Send + Sync {
    /// Process a goal with optional pre-existing tasks.
    /// If `tasks` is empty, the planner decomposes the goal into atomic tasks.
    /// If `tasks` have `action_planner_meta.verified == true`, they are trusted
    /// and injected directly. Otherwise the planner evaluates/optimizes them.
    async fn process_goal(
        &self,
        goal: &str,
        tasks: Vec<Task>,
        skill_registry: &SkillRegistry,
    ) -> SFResult<Vec<String>>;
}

// ─── Task Execution Callback ───────────────────────────────────────────────

/// Callback invoked by agent-pool workers to execute a single task.
#[async_trait]
pub trait TaskExecutionCallback: Send + Sync {
    async fn execute_task(&self, task: Task);
}
