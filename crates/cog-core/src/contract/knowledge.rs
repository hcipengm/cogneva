use crate::{SFResult, Task};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single knowledge entry returned by unified retrieval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KnowledgeEntry {
    pub id: String,
    pub source: String, // e.g. "memory:schema", "memory:summary", "wiki"
    pub title: String,
    pub content: String,
    pub relevance_score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Historical pattern of how a goal was decomposed into tasks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskDecompositionPattern {
    pub pattern_id: String,
    pub goal_summary: String,
    pub task_types: Vec<String>,
    pub avg_success_rate: f32,
    pub used_count: u64,
    pub last_used: DateTime<Utc>,
}

/// A previously executed implementation of a specific task type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImplementationExample {
    pub example_id: String,
    pub task_type: String,
    pub input_summary: String,
    pub output_summary: String,
    pub score: f32,
    pub used_count: u64,
}

/// A documented failure pattern for a given task type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FailurePattern {
    pub pattern_id: String,
    pub task_type: String,
    pub failure_summary: String,
    pub root_cause: String,
    pub occurrence_count: u64,
    pub last_occurrence: DateTime<Utc>,
}

/// A record of a single task execution for history retrieval.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskExecutionRecord {
    pub record_id: String,
    pub task_id: String,
    pub task_type: String,
    pub status: String,
    pub result_summary: String,
    pub executed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
}

/// Unified knowledge retrieval interface, aggregating MemoryBackend + WikiBackend + StateBackend.
/// PGE actors retrieve relevant context through this interface without caring about underlying storage.
#[async_trait]
pub trait KnowledgeBackend: Send + Sync {
    /// Retrieve knowledge relevant to a task (memory + wiki unified).
    async fn retrieve_relevant(
        &self,
        task: &Task,
        query: &str,
        top_k: usize,
    ) -> SFResult<Vec<KnowledgeEntry>>;

    /// Retrieve historical task decomposition patterns (for Planner meta-tasks).
    async fn retrieve_similar_decompositions(
        &self,
        goal: &str,
        top_k: usize,
    ) -> SFResult<Vec<TaskDecompositionPattern>>;

    /// Retrieve historical similar task implementations (for Generator atom-tasks).
    async fn retrieve_similar_implementations(
        &self,
        task_type: &str,
        input_summary: &str,
        top_k: usize,
    ) -> SFResult<Vec<ImplementationExample>>;

    /// Retrieve common failure patterns for a task type (for Evaluator).
    async fn retrieve_failure_patterns(
        &self,
        task_type: &str,
        top_k: usize,
    ) -> SFResult<Vec<FailurePattern>>;

    /// Retrieve full execution history for a given task (for Moderator).
    async fn retrieve_task_history(&self, task_id: &str) -> SFResult<Vec<TaskExecutionRecord>>;

    /// Archive the current task execution result into long-term memory.
    async fn archive_execution(&self, task: &Task, result: &crate::TaskResult) -> SFResult<()>;
}
