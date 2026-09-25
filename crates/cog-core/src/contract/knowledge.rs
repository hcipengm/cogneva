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

/// Historical pattern of how goals of one class were decomposed into tasks.
///
/// One per class, holding the newest decomposition and the aggregate of the
/// ones before it, like [`ImplementationExample`] holds one per task type. A
/// row per planning run would grow the namespace with the work while every
/// query kept returning the same shape of answer, and the class is what the
/// store can match on: it compares a query against an entry's name and key as
/// substrings, and a goal is free text no key of another goal contains.
///
/// `avg_sub_task_count` is the mean width of the recorded decompositions — the
/// one per-run quantity the archive observes. It is deliberately not a success
/// rate: the archive runs when a task ends, and a run that delivered no
/// sub-tasks is not a failed decomposition but the absence of one, so a rate
/// over "recorded runs" could only ever read 1.0.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskDecompositionPattern {
    pub pattern_id: String,
    pub goal_summary: String,
    pub task_types: Vec<String>,
    pub avg_sub_task_count: f32,
    pub used_count: u64,
    pub last_used: DateTime<Utc>,
}

/// A previously executed implementation of a specific task type.
///
/// One per task type, holding the most recent successful run of it:
/// `observed_count` is how many successes of this type have been recorded,
/// and the summaries describe the newest one. One row per type rather than one
/// per run is what keeps the namespace growing with the vocabulary of task
/// types instead of with the work done — and the vocabulary is what
/// [`crate::TaskType`] is: a fixed set of names plus whatever a producer puts
/// in [`crate::TaskType::Custom`], which can extend it.
///
/// The field was named `used_count` while nothing wrote it, which made it read
/// as "how often this example was retrieved" in a prompt that could never
/// count that. It is not a usage counter: the write side observes runs, not
/// retrievals.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImplementationExample {
    pub example_id: String,
    pub task_type: String,
    pub input_summary: String,
    pub output_summary: String,
    pub score: f32,
    pub observed_count: u64,
}

/// A documented failure pattern for a given task type.
///
/// One per task type, holding the most recent failure of it; see
/// [`ImplementationExample`] for why the cardinality is the task type.
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
    ///
    /// `goal_class` is what the entries are keyed and matched on; `goal` ranks
    /// the matches that come back. As with
    /// [`Self::retrieve_similar_implementations`], the store matches a query as
    /// a substring of an entry's name or key and has no scoring of its own, so
    /// a query built from the goal text — free text, matched against keys that
    /// name a class — would answer "no prior decomposition" for every goal and
    /// answer it the same way for a namespace nothing was ever written to.
    ///
    /// An empty result therefore means this class has no recorded
    /// decomposition, not that a match was missed.
    async fn retrieve_similar_decompositions(
        &self,
        goal_class: &str,
        goal: &str,
        top_k: usize,
    ) -> SFResult<Vec<TaskDecompositionPattern>>;

    /// Retrieve historical similar task implementations (for Generator atom-tasks).
    ///
    /// `task_type` is what the entries are keyed and matched on; `input_summary`
    /// ranks the matches that come back, because the store matches a query as a
    /// substring of an entry's name or key and a summary — free text, often a
    /// serialized object — is not a substring of anything. A summary used as the
    /// query would therefore answer "no prior implementation" for every task,
    /// which is exactly what an empty namespace answers too.
    ///
    /// One implementation per task type is what the write side records, so an
    /// empty result means this type has never succeeded, not that a match was
    /// missed.
    async fn retrieve_similar_implementations(
        &self,
        task_type: &str,
        input_summary: &str,
        top_k: usize,
    ) -> SFResult<Vec<ImplementationExample>>;

    /// Retrieve common failure patterns for a task type (for Evaluator).
    ///
    /// As with [`Self::retrieve_similar_implementations`], the task type is the
    /// key the entries are written under and the only dimension a query can
    /// match on.
    async fn retrieve_failure_patterns(
        &self,
        task_type: &str,
        top_k: usize,
    ) -> SFResult<Vec<FailurePattern>>;

    /// Retrieve full execution history for a given task (for Moderator).
    async fn retrieve_task_history(&self, task_id: &str) -> SFResult<Vec<TaskExecutionRecord>>;

    /// Archive the current task execution result into long-term memory.
    ///
    /// This is the write side of every retrieval above that asks about past
    /// runs: an implementation that succeeded and a failure that was observed
    /// are both recorded from here, under the task type those queries key on.
    /// The retrieval namespaces are the backend's own, so the key shape that
    /// makes an entry findable is derived here rather than by a caller that
    /// would have to guess the query it has to match.
    async fn archive_execution(&self, task: &Task, result: &crate::TaskResult) -> SFResult<()>;

    /// Archive a decomposition a planning run delivered, for the class the goal
    /// belongs to.
    ///
    /// Separate from [`Self::archive_execution`] because the deliverable is not
    /// in the task result: a decomposition is the sub-task list a plan produced,
    /// and only the caller holding that plan can say what it was. The class is
    /// read from the task the goal belongs to, so the key this builds is the one
    /// [`Self::retrieve_similar_decompositions`] queries with.
    ///
    /// A run that delivered no sub-tasks records nothing: the observation is the
    /// decomposition, and a row is a statement about the decompositions that
    /// happened.
    async fn archive_decomposition(&self, task: &Task, sub_task_types: &[String]) -> SFResult<()>;
}
