use serde::{Deserialize, Serialize};

/// Specification of a single atomic task produced by the Planner.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct TaskSpec {
    pub id: String,
    pub name: String,
    pub task_type: String,
    pub input: serde_json::Value,
    #[serde(alias = "blockedBy")]
    pub blocked_by: Vec<String>,
}

/// Output produced by a Planner agent.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct PlannerOutput {
    /// Human-readable plan summary.
    pub summary: String,
    /// Machine-consumable structured plan.
    pub plan: serde_json::Value,
    /// Atom tasks decomposed from the meta-task.
    pub sub_tasks: Vec<TaskSpec>,
}

/// A named artifact produced by the Generator.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct Artifact {
    pub name: String,
    pub content: String,
    pub artifact_type: String,
}

/// Output produced by a Generator agent.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct GeneratorOutput {
    /// Primary output content.
    pub content: serde_json::Value,
    /// Named deliverables.
    pub artifacts: Vec<Artifact>,
}

/// A single evaluation criterion.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct Criterion {
    pub name: String,
    pub score: u32,
    pub comment: String,
}

/// Verdict of an evaluation.
#[derive(Debug, Clone, Copy, Serialize, schemars::JsonSchema, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    #[default]
    Fail,
    Pass,
    Partial,
    NeedsReview,
    Retry,
}

impl<'de> serde::Deserialize<'de> for Verdict {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VerdictVisitor;
        impl<'de> serde::de::Visitor<'de> for VerdictVisitor {
            type Value = Verdict;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a verdict string or boolean")
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value.to_lowercase().as_str() {
                    "pass" | "passed" => Ok(Verdict::Pass),
                    "fail" | "failed" => Ok(Verdict::Fail),
                    "partial" => Ok(Verdict::Partial),
                    "needs_review" | "needsreview" => Ok(Verdict::NeedsReview),
                    "retry" => Ok(Verdict::Retry),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["pass", "fail", "partial", "needs_review", "retry"],
                    )),
                }
            }
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value {
                    Ok(Verdict::Pass)
                } else {
                    Ok(Verdict::Fail)
                }
            }
        }
        deserializer.deserialize_any(VerdictVisitor)
    }
}

/// Result of a single evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct EvaluationResult {
    #[serde(default)]
    pub verdict: Verdict,
    pub feedback: String,
    pub score: Option<u32>,
    pub criteria: Vec<Criterion>,
    pub details: Option<serde_json::Value>,
}

/// A single local repair cycle inside a pipeline attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalRepairAttempt {
    pub repair_iteration: u32,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
    pub feedback: String,
}

/// Result of a single parallel PGE branch in a roundtable iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PgeBranchResult {
    pub branch_id: u32,
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
}

/// Strategy for merging parallel branch results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum BranchMergeStrategy {
    /// Pick the branch with the highest evaluation score.
    #[default]
    BestScore,
    /// Majority vote across branch verdicts.
    MajorityVote,
    /// Union artifacts and pick the best generation by score.
    UnionArtifacts,
    /// Delegate to a MergerActor LLM.
    Custom,
}

/// Summary of how parallel branches were merged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MergeSummary {
    pub selected_branch_id: u32,
    pub strategy: BranchMergeStrategy,
    pub reasoning: String,
}

/// A single roundtable iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PgeRoundtableIteration {
    pub iteration: u32,
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
    /// Parallel branch results that produced this iteration, if any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<PgeBranchResult>,
    /// How the branches were merged, if parallel branches were used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_summary: Option<MergeSummary>,
}
