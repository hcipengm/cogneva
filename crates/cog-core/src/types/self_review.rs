use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Configuration for the self-review loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfReviewConfig {
    /// Maximum number of review-revision cycles before accepting the output.
    pub max_iterations: u32,
    /// Quality threshold (0.0–1.0). Scores above this are considered a pass.
    pub quality_threshold: f32,
    /// Optional specification the output is compared against.
    pub spec: Option<String>,
    /// Optional list of best-practice guidelines.
    pub best_practices: Vec<String>,
}

impl Default for SelfReviewConfig {
    fn default() -> Self {
        Self {
            max_iterations: 2,
            quality_threshold: 0.8,
            spec: None,
            best_practices: Vec::new(),
        }
    }
}

/// Result of a single self-review cycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelfReviewResult {
    /// Output meets quality standards.
    Pass {
        /// The final quality score (0.0–1.0).
        score: f32,
        /// Human-readable summary of the review.
        summary: String,
    },
    /// Output needs revision.
    NeedRevision {
        /// Critical assessment of what's wrong / missing.
        critique: String,
        /// Actionable suggestions for improvement.
        suggestions: Vec<String>,
        /// Quality score (0.0–1.0), below threshold.
        score: f32,
    },
}

/// A complete self-review record for persistence and observability.
/// Filled by the implementation crate (e.g. cog-agent) and stored via
/// KnowledgeBackend so historical review patterns can be queried.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfReviewRecord {
    pub agent_id: String,
    pub original_output: String,
    pub revised_output: Option<String>,
    pub config: SelfReviewConfig,
    pub result: SelfReviewResult,
    pub issues: Vec<String>,
    pub missing: Vec<String>,
    pub strengths: Vec<String>,
    pub gaps: Vec<String>,
    pub aligned: Vec<String>,
    pub iteration_count: u32,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
}
