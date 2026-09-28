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

impl SelfReviewConfig {
    /// Take `spec` as the specification in force when the configuration
    /// declares none.
    ///
    /// The specification is half of what the output is compared against (the
    /// other half being `best_practices`). With both empty, the comparison step
    /// reads only the critique the previous step wrote, and the score it
    /// returns answers nothing the caller could contradict. A caller that knows
    /// what it asked the output to satisfy fills that gap here; a specification
    /// configured deliberately still wins, because that is the standard which
    /// was meant to be applied.
    pub fn with_declared_spec(mut self, spec: &str) -> Self {
        let unset = self.spec.as_deref().is_none_or(|s| s.trim().is_empty());
        if unset && !spec.trim().is_empty() {
            self.spec = Some(spec.to_string());
        }
        self
    }

    /// Whether anything outside the review itself is being compared against.
    ///
    /// Reads the same two fields the comparison step is handed, so a review
    /// that would run on its own output alone can be told apart from one that
    /// has a standard to answer to.
    pub fn has_external_criterion(&self) -> bool {
        !self.spec.as_deref().unwrap_or("").trim().is_empty() || !self.best_practices.is_empty()
    }
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

#[cfg(test)]
mod tests {
    use super::SelfReviewConfig;

    #[test]
    fn a_declared_specification_fills_a_configuration_that_has_none() {
        let filled = SelfReviewConfig::default().with_declared_spec("the task's own words");
        assert_eq!(filled.spec.as_deref(), Some("the task's own words"));
        assert!(filled.has_external_criterion());
    }

    #[test]
    fn a_configured_specification_is_not_displaced() {
        let configured = SelfReviewConfig {
            spec: Some("the operator's standard".into()),
            ..Default::default()
        };
        assert_eq!(
            configured.with_declared_spec("the task's own words").spec,
            Some("the operator's standard".to_string())
        );
    }

    #[test]
    fn best_practices_alone_are_an_external_criterion() {
        let with_practices = SelfReviewConfig {
            best_practices: vec!["no unwrap in library code".into()],
            ..Default::default()
        };
        assert!(with_practices.has_external_criterion());
        assert!(
            !SelfReviewConfig::default().has_external_criterion(),
            "an empty configuration compares the output against nothing outside the review"
        );
    }

    #[test]
    fn a_blank_specification_is_not_a_criterion() {
        let blank = SelfReviewConfig {
            spec: Some("   ".into()),
            ..Default::default()
        };
        assert!(!blank.has_external_criterion());
        assert_eq!(
            blank.with_declared_spec("the task's own words").spec,
            Some("the task's own words".to_string()),
            "whitespace is not a standard the output was held to"
        );
    }
}
