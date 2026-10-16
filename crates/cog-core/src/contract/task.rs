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

// ─── Discovery probe verdict ───────────────────────────────────────────────

/// Key under which a capability-discovery probe's **own** verdict travels back
/// in a task result, and the closed set of values it may carry.
///
/// A probe asks one question: whether the tool/skill combination named by
/// `explored_key` in the task input is worth promoting. That answer does not
/// follow from how the run that carried the probe ended. The probe is claimed
/// by whichever executor accepts the task type, and a generic codegen run
/// reports whether *its own* work succeeded — a different proposition about a
/// different subject. A reader that takes that outcome as the probe's verdict
/// files a finding about a question nobody asked, and files it into the same
/// learning pipeline that promotes skills. So the answer travels under its own
/// key, and a reader that does not find the key records that the probe was
/// **not measured** — a state of its own rather than a silence.
///
/// It travels in [`TaskResult::output`], not in `metadata.extensions`. The
/// executor loop publishes the result's `output` alone onto the results
/// stream, so a producer that writes its verdict into the metadata has written
/// it somewhere that is dropped before the task row is ever completed — the
/// reader then sees the same absence as a producer that never ran.
///
/// Producer (the executor that runs the probe) and consumer (`cog-reflection`,
/// which reads the completed task back) live in different crates, so the key
/// and its values are defined here, beside the envelope their schema is agreed
/// in. A consumer that re-types the literal keeps compiling and silently stops
/// matching the day a producer changes the wording.
pub const DISCOVERY_VERDICT_FIELD: &str = "discovery_verdict";

/// Free-text reason a producer attaches beside its verdict. Optional: a verdict
/// without it is still a verdict, and the absence of the text must not be read
/// as the absence of the answer.
pub const DISCOVERY_NOTES_FIELD: &str = "discovery_notes";

/// Values [`DISCOVERY_VERDICT_FIELD`] may carry. Closed on purpose, and spelled
/// the same as the verdicts `cog-reflection` records, so the wire value and the
/// recorded state are one vocabulary instead of two that can drift.
pub const DISCOVERY_VERDICT_VALIDATED: &str = "validated";
pub const DISCOVERY_VERDICT_REJECTED: &str = "rejected";
pub const DISCOVERY_VERDICT_INCONCLUSIVE: &str = "inconclusive";

/// What a reader got back for a probe: the producer's answer, or the reason
/// there is none.
///
/// The two unanswered cases are separate variants because they are separate
/// findings. `Absent` is a producer that ran and said nothing — today's norm,
/// and the reading that says the probe has no judge. `Unrecognized` is a
/// producer that answered in a vocabulary this reader does not know, which is
/// a defect in the agreement rather than an unfinished path; folding the two
/// together would make a change to the wording on one side read as a slow
/// rollout on the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryVerdict {
    /// The combination was evaluated and holds.
    Validated,
    /// The combination was evaluated and does not hold.
    Rejected,
    /// The combination was evaluated; the reading settles nothing either way.
    Inconclusive,
    /// No verdict field in the result.
    Absent,
    /// A verdict field whose value is outside the closed set.
    Unrecognized,
}

impl DiscoveryVerdict {
    /// Read the verdict off a completed task's result payload.
    ///
    /// Anything that is not one of the closed values reads as one of the two
    /// unanswered cases rather than defaulting to a verdict, because a default
    /// here would be a judgement about the combination that no producer made.
    pub fn of(result: Option<&Value>) -> Self {
        match result.and_then(|r| r.get(DISCOVERY_VERDICT_FIELD)) {
            None => Self::Absent,
            Some(v) => match v.as_str() {
                Some(DISCOVERY_VERDICT_VALIDATED) => Self::Validated,
                Some(DISCOVERY_VERDICT_REJECTED) => Self::Rejected,
                Some(DISCOVERY_VERDICT_INCONCLUSIVE) => Self::Inconclusive,
                _ => Self::Unrecognized,
            },
        }
    }

    /// Name for logs and for the text of a learning. The two unanswered cases
    /// are named too, and differently from each other: a log line that prints
    /// both the same way cannot be used to tell a producer that changed its
    /// wording from a path that was never finished.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validated => DISCOVERY_VERDICT_VALIDATED,
            Self::Rejected => DISCOVERY_VERDICT_REJECTED,
            Self::Inconclusive => DISCOVERY_VERDICT_INCONCLUSIVE,
            Self::Absent => "absent",
            Self::Unrecognized => "unrecognized",
        }
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
