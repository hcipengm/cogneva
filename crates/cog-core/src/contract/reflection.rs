use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A detected pattern that groups related learnings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pattern {
    pub key: String,
    pub description: String,
    pub learning_ids: Vec<String>,
    pub recurrence_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// A single usage observation of a skill in production.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillOutcome {
    pub skill_id: String,
    /// Task type fingerprint (e.g. "backend:api:migration").
    pub task_signature: String,
    pub success: bool,
    /// Self-review score (0.0–1.0) if available.
    pub score: Option<f32>,
    /// Wall-clock latency in milliseconds.
    pub latency_ms: u64,
    /// Total token cost (input + output).
    pub token_cost: u64,
    pub observed_at: DateTime<Utc>,
}

/// Cross-session reflection engine for learning detection and self-improvement.
#[async_trait::async_trait]
pub trait ReflectionEngine: Send + Sync + std::fmt::Debug {
    /// Process tool execution results for pattern detection.
    async fn process_tool_result(
        &self,
        tool_name: &str,
        result: &serde_json::Value,
        is_error: bool,
    ) -> crate::SFResult<()>;

    /// Process a full context window after a run completes.
    async fn process_context(&self, messages: &[crate::Message]) -> crate::SFResult<()>;

    /// Process an agent event through the learning pipeline.
    async fn process_event(&self, event: &crate::AgentEvent) -> crate::SFResult<()>;

    /// Trigger skill extraction from a mature pattern.
    async fn extract_and_insert(&self, pattern: &Pattern) -> crate::SFResult<Option<String>>;

    /// Feed a skill usage outcome into the effectiveness tracker.
    async fn process_skill_outcome(&self, outcome: SkillOutcome) -> crate::SFResult<()>;

    /// Start the background periodic reviewer if configured.
    fn start_reviewer(&self) -> Option<tokio::task::JoinHandle<()>>;

    /// Record the outcome of a Squad run so reflection can learn from
    /// collaboration quality.
    async fn record_squad_result(
        &self,
        task_id: &str,
        goal: &str,
        success: bool,
        pge_mode: &str,
        score: Option<f32>,
        latency_ms: u64,
    ) -> crate::SFResult<()>;

    /// Record the outcome of a generated change after it has been applied,
    /// tested, built, or deployed.
    async fn record_change_outcome(
        &self,
        change_id: &str,
        success: bool,
        test_output: &str,
    ) -> crate::SFResult<()>;
}

/// A code change produced by the collaboration pipeline for self-evolution.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GeneratedChange {
    pub change_id: String,
    pub goal: String,
    pub content: String,
    pub affected_files: Vec<String>,
    pub rationale: Option<String>,
    pub pge_mode: String,
    pub self_review_score: Option<f32>,
    /// Public issue this change resolves, when the intent came from a tracked
    /// issue. Sinks use it to link the PR back (`Fixes #N`) so competing
    /// solutions for the same issue can be grouped.
    pub issue_number: Option<u64>,
}

/// Sink for collaboration-generated changes. Implemented by the reflection
/// layer so that collaboration does not depend on reflection concrete types.
#[async_trait::async_trait]
pub trait ChangeSink: Send + Sync + std::fmt::Debug {
    /// Submit a generated change for persistence and downstream deployment.
    /// Returns the artifact id assigned by the sink.
    async fn submit_change(&self, change: GeneratedChange) -> crate::SFResult<String>;
}

/// Parse a unified diff change and return the list of files it touches.
///
/// Extracts paths from `+++ b/<path>` lines. New files appear as
/// `+++ b/<path>` with `--- /dev/null`, so this also handles additions.
/// This is a pure function shared by collaboration (static validation) and
/// reflection (change pipeline).
pub fn parse_diff_affected_files(content: &str) -> crate::SFResult<Vec<String>> {
    let mut files = Vec::new();

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("+++") {
            let rest = rest.trim();
            // Unified diff produced by git uses "+++ b/<path>".
            // Strip the "b/" prefix when present.
            let path_str = rest.strip_prefix("b/").unwrap_or(rest);

            // Skip the timestamp header that `git diff` sometimes emits.
            let path_str = path_str.split_whitespace().next().unwrap_or(path_str);

            if path_str == "/dev/null" {
                continue;
            }

            files.push(path_str.to_string());
        }
    }

    if files.is_empty() {
        return Err(crate::SFError::Validation(
            "No file paths found in change (expected '+++ b/<path>' lines)".into(),
        ));
    }

    Ok(files)
}

// ============================================================================
// Crew / Squad reflection types (migrated from cog-reflection to break
// cog-collaboration → cog-reflection dependency).
// ============================================================================

/// Summary of a single agent's contribution within a Squad run.
#[derive(Debug, Clone)]
pub struct AgentSquadContribution {
    pub agent_id: String,
    pub role: String,
    pub learnings: Vec<Learning>,
    pub errors: Vec<ErrorEntry>,
    pub result: Option<serde_json::Value>,
}

/// Result of a Squad-level reflection pass.
#[derive(Debug, Clone)]
pub struct SquadReflectionResult {
    pub squad_id: String,
    pub task_id: String,
    pub patterns: Vec<Pattern>,
    pub learnings: Vec<Learning>,
    pub upgrade_recommended: bool,
    pub upgrade_reason: Option<String>,
}

/// Aggregates individual-agent learnings into squad-level insights.
#[async_trait::async_trait]
pub trait SquadReflection: Send + Sync {
    /// Run the full squad reflection pipeline.
    async fn reflect(
        &self,
        squad_id: &str,
        task_id: &str,
        contributions: &[AgentSquadContribution],
        retry_count: u32,
    ) -> crate::SFResult<SquadReflectionResult>;

    /// Detect disagreement patterns in a Roundtable (plan vs generation mismatch).
    async fn detect_disagreements(&self, contributions: &[AgentSquadContribution])
        -> Vec<Learning>;

    /// Detect signals that suggest upgrading Pipeline → Roundtable.
    async fn detect_upgrade_signals(
        &self,
        contributions: &[AgentSquadContribution],
        retry_count: u32,
    ) -> Vec<Learning>;
}

// ============================================================================
// Meta-learning types
// ============================================================================

/// Features extracted from a task that serve as input to the mode selector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFeatures {
    pub task_type: String,
    pub domain_tags: Vec<String>,
    pub estimated_complexity: f32,
    pub has_external_dependencies: bool,
    pub historical_success_rate: f32,
    pub required_skills: Vec<String>,
}

/// Recommendation returned by the meta-learning engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeRecommendation {
    Pipeline,
    Roundtable,
    /// Not enough data — fall back to the fixed-threshold heuristic.
    UseDefault,
}

/// Generic decision category for the meta-learning engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionCategory {
    PgeMode,
    ResetStrategy,
    RetryPolicy,
    SelfReviewThreshold,
}

/// Outcome of a single decision recorded by the meta-learning engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionOutcome {
    Success,
    Failed,
    Escalated,
}

// ============================================================================
// Core learning data model (migrated from cog-reflection to break
// cog-collaboration → cog-reflection dependency).
// ============================================================================

/// Category of a learning entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningCategory {
    Correction,
    Insight,
    KnowledgeGap,
    BestPractice,
}

/// Lifecycle status of a learning entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningStatus {
    Pending,
    InProgress,
    Resolved,
    Promoted,
    WontFix,
}

/// Priority level for a learning or error entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

/// Functional area affected by the learning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    Frontend,
    Backend,
    Infra,
    Tests,
    Docs,
    Config,
}

/// Source of the learning signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningSource {
    Conversation,
    Error,
    UserFeedback,
    SelfReview,
}

/// How a learning entry was resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Resolution {
    Resolved { resolution: String },
    WontFix { reason: String },
}

/// A single learning entry — the central data model of the reflection layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Learning {
    pub id: String,
    pub category: LearningCategory,
    pub priority: Priority,
    pub status: LearningStatus,
    pub area: Area,
    pub summary: String,
    pub details: String,
    pub suggested_action: String,
    pub source: LearningSource,
    pub related_files: Vec<String>,
    pub tags: Vec<String>,
    pub see_also: Vec<String>,
    pub pattern_key: Option<String>,
    pub recurrence_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub related_tasks: Vec<String>,
}

impl Learning {
    pub fn generate_id() -> String {
        let now = Utc::now();
        format!(
            "LRN-{}-{}",
            now.format("%Y%m%d"),
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        )
    }

    pub fn new(
        category: LearningCategory,
        priority: Priority,
        area: Area,
        summary: impl Into<String>,
        details: impl Into<String>,
        suggested_action: impl Into<String>,
        source: LearningSource,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Self::generate_id(),
            category,
            priority,
            status: LearningStatus::Pending,
            area,
            summary: summary.into(),
            details: details.into(),
            suggested_action: suggested_action.into(),
            source,
            related_files: Vec::new(),
            tags: Vec::new(),
            see_also: Vec::new(),
            pattern_key: None,
            recurrence_count: 1,
            first_seen: now,
            last_seen: now,
            related_tasks: Vec::new(),
        }
    }

    pub fn bump_recurrence(&mut self) {
        self.recurrence_count += 1;
        self.last_seen = Utc::now();
    }

    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.first_seen
    }
}

/// A structured error entry for persistent failure tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEntry {
    pub id: String,
    pub priority: Priority,
    pub status: LearningStatus,
    pub summary: String,
    pub error_message: String,
    pub context: String,
    pub suggested_fix: String,
    pub reproducible: Option<bool>,
    pub related_files: Vec<String>,
    pub see_also: Vec<String>,
    pub pattern_key: Option<String>,
    pub recurrence_count: u32,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

impl ErrorEntry {
    pub fn generate_id() -> String {
        let now = Utc::now();
        format!(
            "ERR-{}-{}",
            now.format("%Y%m%d"),
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        )
    }

    pub fn new(
        priority: Priority,
        summary: impl Into<String>,
        error_message: impl Into<String>,
        context: impl Into<String>,
        suggested_fix: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Self::generate_id(),
            priority,
            status: LearningStatus::Pending,
            summary: summary.into(),
            error_message: error_message.into(),
            context: context.into(),
            suggested_fix: suggested_fix.into(),
            reproducible: None,
            related_files: Vec::new(),
            see_also: Vec::new(),
            pattern_key: None,
            recurrence_count: 1,
            first_seen: now,
            last_seen: now,
        }
    }
}

/// Lightweight trait abstracting the meta-learning mode selector.
#[async_trait::async_trait]
pub trait MetaLearning: Send + Sync + std::fmt::Debug {
    /// Recommend a mode based on historical data for this task category.
    async fn recommend_mode(&self, features: &TaskFeatures) -> ModeRecommendation;

    /// Record the actual outcome of a mode decision so the model can learn.
    async fn record_outcome(
        &self,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> crate::SFResult<()>;

    /// Recommend a decision for the given category based on historical data.
    async fn recommend(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
    ) -> Option<String>;

    /// Record the outcome of a decision so the model can learn.
    async fn record(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> crate::SFResult<()>;
}
