use crate::{SFResult, Task};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

// ---------------------------------------------------------------------------
// The retrieval outcome series
// ---------------------------------------------------------------------------

/// The layers one knowledge retrieval consults, in the order it consults them.
///
/// These are the `layer` values of `cogneva_knowledge_retrieval_total`, and the
/// composition of the backend is what they name: a retrieval asks memory and
/// wiki. They are defined here rather than at the backend that names them,
/// because the caller that has to report a *missing* backend writes the same
/// cell names, and two places that decide what a layer is called are two places
/// that can disagree about one series.
pub const RETRIEVAL_LAYER_MEMORY: &str = "memory";
pub const RETRIEVAL_LAYER_WIKI: &str = "wiki";

/// Both layers, for a writer that walks the set rather than naming one.
pub const RETRIEVAL_LAYERS: [&str; 2] = [RETRIEVAL_LAYER_MEMORY, RETRIEVAL_LAYER_WIKI];

/// The cells a consulted layer's answer lands in.
///
/// `hit` (the layer answered with rows), `empty` (it answered and had none),
/// `error` (its backend refused), `absent` (this process holds no such layer).
/// At every caller the first three are the same empty list, which is the whole
/// reason the four are separated where they are written rather than where they
/// are read.
pub const RETRIEVAL_OUTCOME_HIT: &str = "hit";
pub const RETRIEVAL_OUTCOME_EMPTY: &str = "empty";
pub const RETRIEVAL_OUTCOME_ERROR: &str = "error";
pub const RETRIEVAL_OUTCOME_ABSENT: &str = "absent";

/// All four cells, for a writer that walks the set rather than naming one.
pub const RETRIEVAL_OUTCOMES: [&str; 4] = [
    RETRIEVAL_OUTCOME_HIT,
    RETRIEVAL_OUTCOME_EMPTY,
    RETRIEVAL_OUTCOME_ERROR,
    RETRIEVAL_OUTCOME_ABSENT,
];

/// Publish one cell of `cogneva_knowledge_retrieval_total`.
///
/// A write that fails is logged and dropped: the retrieval it describes has
/// already answered, and turning that answer into an error because the metrics
/// store refused would let the reading change what it measures.
pub async fn record_retrieval_cell(
    metrics: &Arc<dyn crate::MetricsBackend>,
    layer: &str,
    outcome: &str,
    value: f64,
) {
    let mut labels = HashMap::new();
    labels.insert("layer".to_string(), layer.to_string());
    labels.insert("outcome".to_string(), outcome.to_string());
    if let Err(e) = metrics
        .record_counter(
            crate::metric_names::KNOWLEDGE_RETRIEVAL_TOTAL,
            value,
            labels,
        )
        .await
    {
        tracing::warn!("could not publish the {layer}/{outcome} retrieval cell: {e}");
    }
}

/// Publish every cell of the series as zero.
///
/// The cells are written only when a retrieval consults a layer, and a
/// deployment can come up and consult none -- no work to run, or the work that
/// would run held upstream. With nothing written, "this boot consulted nothing"
/// and "this build carries no such reading" are the same empty face, which is
/// the confusion the series exists to remove; seeding leaves absence to mean
/// only the second. Written as a zero increment, so a cell another process has
/// already counted up is not reset.
///
/// The seed rests on the counter write adding rather than assigning, which is
/// what [`crate::MetricsBackend::record_counter`] states and what the live
/// store test next to the backend pins; a set would make every restart clear
/// the family.
///
/// The label sets are the fixed cross above, whatever the number of pods, so
/// seeding on every boot writes into the same rows rather than adding a row per
/// rollout: this is not the shape where a per-boot writer leaves a permanent
/// floor under a table.
pub async fn seed_retrieval_cells(metrics: &Arc<dyn crate::MetricsBackend>) {
    for layer in RETRIEVAL_LAYERS {
        for outcome in RETRIEVAL_OUTCOMES {
            record_retrieval_cell(metrics, layer, outcome, 0.0).await;
        }
    }
}

/// Publish that this process obtained no knowledge backend, so no layer could
/// be consulted at all.
///
/// The backend is built only when the wiki layer is reachable, so a process
/// without it holds no layer and every layer's cell is `absent`. Written once
/// per process rather than once per retrieval, because there is no retrieval to
/// count, where the sibling cells are written per retrieval; a reader has to
/// name the outcome for that reason. On the wiki cell an increase can only be
/// this: a process holding the backend never writes `absent` for wiki, so it
/// names a boot with no backend. On the memory cell it cannot be read alone --
/// a live backend writes `absent` there as ordinary traffic whenever it answers
/// for wiki and holds no memory -- so the wiki cell is the one that
/// disambiguates the pair.
pub async fn publish_no_knowledge_backend(metrics: &Arc<dyn crate::MetricsBackend>) {
    for layer in RETRIEVAL_LAYERS {
        record_retrieval_cell(metrics, layer, RETRIEVAL_OUTCOME_ABSENT, 1.0).await;
    }
}
