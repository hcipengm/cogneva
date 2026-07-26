//! Core data models for the reflection / self-improvement subsystem.
//! Aligned with the openclaw `self-improving-agent` skill data model,
//! but adapted to cogneva's structured-storage architecture.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use cog_core::{Area, Learning, LearningCategory, LearningSource, LearningStatus, Priority};

/// Complexity estimate for a feature request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Complexity {
    Simple,
    Medium,
    Complex,
}

/// Frequency of a feature request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Frequency {
    FirstTime,
    Recurring,
}

/// Result of a promotion attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PromotionResult {
    Promoted { target: String, value: String },
    NotReady { reason: String },
}

/// A feature request captured from user conversations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureRequest {
    /// Unique identifier: `FEAT-YYYYMMDD-XXX`
    pub id: String,
    pub capability: String,
    pub user_context: String,
    pub complexity: Complexity,
    pub suggested_implementation: String,
    pub frequency: Frequency,
    pub related_features: Vec<String>,
    pub status: LearningStatus,
    pub priority: Priority,
    pub area: Area,
    pub created_at: DateTime<Utc>,
}

impl FeatureRequest {
    pub fn generate_id() -> String {
        let now = Utc::now();
        format!(
            "FEAT-{}-{}",
            now.format("%Y%m%d"),
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        )
    }

    pub fn new(
        capability: impl Into<String>,
        user_context: impl Into<String>,
        complexity: Complexity,
        suggested_implementation: impl Into<String>,
        area: Area,
    ) -> Self {
        Self {
            id: Self::generate_id(),
            capability: capability.into(),
            user_context: user_context.into(),
            complexity,
            suggested_implementation: suggested_implementation.into(),
            frequency: Frequency::FirstTime,
            related_features: Vec::new(),
            status: LearningStatus::Pending,
            priority: Priority::Medium,
            area,
            created_at: Utc::now(),
        }
    }
}

use cog_core::Pattern;

/// Filter for listing learnings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LearningFilter {
    pub status: Option<LearningStatus>,
    pub priority: Option<Priority>,
    pub area: Option<Area>,
    pub category: Option<LearningCategory>,
    pub source: Option<LearningSource>,
    pub pattern_key: Option<String>,
    pub tags: Vec<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
}

/// Report produced by a periodic review pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewReport {
    pub total_pending: usize,
    pub total_in_progress: usize,
    pub high_priority_pending: Vec<Learning>,
    pub recently_resolved: Vec<Learning>,
    pub patterns_detected: Vec<Pattern>,
    pub promotions_ready: Vec<Learning>,
    pub reviewed_at: DateTime<Utc>,
}

// ============================================================================
// Deep Self-Evolution Types — Skill Effectiveness, Meta-Learning, Discovery
// ============================================================================

/// Action recommended by the effectiveness tracker for a skill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectivenessAction {
    /// Skill is performing well — increase priority / promote.
    Strengthen,
    /// Skill is under-performing — remove from active registry.
    Deprecate,
    /// Skill is mediocre — trigger LLM-based refinement.
    Refine,
    /// No action needed (insufficient data or score in neutral band).
    NoAction,
}

use cog_core::SkillOutcome;

/// Aggregated effectiveness record for a (skill, task_signature) pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillEffectivenessRecord {
    pub skill_id: String,
    pub task_signature: String,
    pub used_count: u32,
    pub success_count: u32,
    pub total_score: f32,
    pub total_latency_ms: u64,
    pub total_token_cost: u64,
    pub last_evaluated: DateTime<Utc>,
    /// Composite effectiveness score 0.0–1.0.
    pub effectiveness_score: f32,
}

impl SkillEffectivenessRecord {
    pub fn new(skill_id: impl Into<String>, task_signature: impl Into<String>) -> Self {
        Self {
            skill_id: skill_id.into(),
            task_signature: task_signature.into(),
            used_count: 0,
            success_count: 0,
            total_score: 0.0,
            total_latency_ms: 0,
            total_token_cost: 0,
            last_evaluated: Utc::now(),
            effectiveness_score: 0.5,
        }
    }

    pub fn accumulate(&mut self, outcome: &SkillOutcome) {
        self.used_count += 1;
        if outcome.success {
            self.success_count += 1;
        }
        if let Some(s) = outcome.score {
            self.total_score += s;
        }
        self.total_latency_ms += outcome.latency_ms;
        self.total_token_cost += outcome.token_cost;
        self.last_evaluated = Utc::now();
    }

    pub fn recompute_score(&mut self) {
        let n = self.used_count.max(1) as f32;
        let success_rate = self.success_count as f32 / n;
        let avg_score = if self.total_score > 0.0 {
            self.total_score / n
        } else {
            success_rate
        };
        let avg_latency = self.total_latency_ms as f32 / n;
        let avg_tokens = self.total_token_cost as f32 / n;

        // Normalize latency and tokens against soft caps.
        let latency_score = (1.0f32).min(10_000.0 / (avg_latency + 100.0));
        let token_score = (1.0f32).min(50_000.0 / (avg_tokens + 500.0));

        // Weighted composite: success 40% + quality 30% + efficiency 30%
        self.effectiveness_score =
            success_rate * 0.40 + avg_score * 0.30 + latency_score * 0.15 + token_score * 0.15;
        self.effectiveness_score = self.effectiveness_score.clamp(0.0, 1.0);
    }

    pub fn recommend_action(&self) -> EffectivenessAction {
        if self.used_count < 5 {
            return EffectivenessAction::NoAction;
        }
        if self.effectiveness_score < 0.3 {
            EffectivenessAction::Deprecate
        } else if self.effectiveness_score > 0.8 {
            EffectivenessAction::Strengthen
        } else {
            EffectivenessAction::Refine
        }
    }
}

/// Generic per-decision statistics tracked by the meta-learning engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DecisionStatistics {
    /// decision string -> (attempts, successes)
    pub counts: std::collections::HashMap<String, (u32, u32)>,
}

// ============================================================================
// Meta-Learning Types
// ============================================================================

use cog_core::TaskFeatures;

/// Outcome of a single PgeMode decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeDecisionRecord {
    pub task_features: TaskFeatures,
    pub selected_mode: String,
    pub actual_success: bool,
    pub actual_score: f32,
    pub actual_latency_ms: u64,
    pub timestamp: DateTime<Utc>,
}

/// Aggregated statistics for a task category.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModeStatistics {
    pub pipeline_attempts: u32,
    pub pipeline_successes: u32,
    pub roundtable_attempts: u32,
    pub roundtable_successes: u32,
}

// ============================================================================
// Discovery Types
// ============================================================================

/// Status of an exploratory discovery task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryStatus {
    Pending,
    Running,
    Validated,
    Rejected,
    Inconclusive,
}

/// Strategy used to generate a discovery task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryStrategy {
    ToolCombination,
    ParameterSpace,
    CrossDomainTransfer,
    BoundaryStressTest,
}

/// A self-generated exploratory task to discover new capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryTask {
    pub id: String,
    pub hypothesis: String,
    pub strategy: DiscoveryStrategy,
    pub tools: Vec<String>,
    pub prompt_variants: Vec<String>,
    pub evaluation_criteria: Vec<String>,
    pub status: DiscoveryStatus,
    pub created_at: DateTime<Utc>,
}

impl DiscoveryTask {
    pub fn generate_id() -> String {
        let now = Utc::now();
        format!(
            "DSC-{}-{}",
            now.format("%Y%m%d"),
            uuid::Uuid::new_v4().to_string()[..8].to_uppercase()
        )
    }
}

// ============================================================================
// Evolution Types
// ============================================================================

/// Kind of evolution produced by the evolution engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionKind {
    SkillRefinement,
    HookSynthesis,
    ToolVariant,
    CodePatch,
    /// 产物级进化（§14.3）：策略产物版本升级提议，经 z-test 统计门控 +
    /// 人工审批后热替换，不改源码。
    PolicyUpdate,
}

/// Honest lifecycle status for evolution artifacts.
/// Replaces the previous free-form `String` to prevent misleading
/// `"applied"` claims when the artifact has only been written to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionStatus {
    /// Artifact generated by LLM and written to `patch_dir`.
    Generated,
    /// Artifact registered into the runtime system (e.g. HookEngine, ToolRegistry).
    Registered,
    /// Passed isolated or project-context compilation check.
    CompileChecked,
    /// Failed validation (missing required fields, bad JSON schema, etc.).
    ValidationFailed,
    /// Failed compilation after all retry attempts.
    CompileError,
    /// Code patch compiled but awaits human/CI review before merge.
    AwaitingReview,
    /// Artifact actively running in the system.
    /// Reserved for future use (e.g. post-review merge into main codebase).
    /// Currently no automated path transitions into this status.
    Active,
    /// 评估门否决：z-test 不显著或候选劣于基线（产物级进化 Reject /
    /// Inconclusive 终态）。
    Rejected,
}

/// Result of an evolution attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvolutionResult {
    pub kind: EvolutionKind,
    pub artifact_id: String,
    pub description: String,
    pub content: String,
    pub status: EvolutionStatus,
    pub created_at: DateTime<Utc>,
    /// 评估门结论（两比例 z-test），如 "Adopt z=2.31 uplift +18%"。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval_summary: Option<String>,
}
