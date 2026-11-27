use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Configuration for the self-review loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfReviewConfig {
    /// Maximum number of review-revision cycles before accepting the output.
    pub max_iterations: u32,
    /// Quality threshold (0.0–1.0). Scores above this are considered a pass.
    ///
    /// Only the rewrite loop (`revise: true`) compares a score against this. The
    /// one-call table review does not: it passes when no stated requirement is
    /// `unmet` and none claims `met` without a quote from the text, and it
    /// reports the fraction it computed for readers, not as a criterion.
    pub quality_threshold: f32,
    /// Whether a judgement below the threshold buys a rewrite of the output.
    ///
    /// `true` is the full loop: judge, then have a model rewrite the whole text
    /// and judge the rewrite again. `false` is judge-only, and it is a single
    /// call: the output is judged against the requirements the task states, one
    /// table row per requirement, each row `met`/`unmet`/`undetermined`, and a
    /// `met` only counts if the quote it gives is found in the text. No rewrite
    /// is bought — the text handed back is the one that was judged, and the
    /// caller's own retry path is what acts on the gaps the table names.
    ///
    /// The rewrite is the expensive half (one full-output call, measured at
    /// roughly twice the critique and four times the comparison) and it is
    /// judged against the same standard the producer was already given. A
    /// producer that can be asked again through its normal path does not need
    /// a second model paid to rewrite its answer in prose.
    #[serde(default = "revise_by_default")]
    pub revise: bool,
    /// Optional specification the output is compared against.
    pub spec: Option<String>,
    /// Optional list of best-practice guidelines.
    pub best_practices: Vec<String>,
}

/// The rewrite the loop has always bought, kept for configurations written
/// before the knob existed: a missing key must keep the old behaviour, not
/// silently switch every deployment to judge-only.
fn revise_by_default() -> bool {
    true
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

// 两轮：第一轮是写，第二轮是照着第一轮的评审改；第二轮仍不过，说明问题不在
// 「改一遍」够不够，再改只是在同一处打转。
// cap: fixed — 两轮：写一轮、按评审改一轮；第三轮还是在同一处打转
// keep: 删掉后评审轮次没有上限，第三轮仍在同一处打转；由「写一轮、改一轮」兜住
const SELF_REVIEW_MAX_ITERATIONS: u32 = 2;

// 0.8：评审给分的量纲是「缺了什么」——八成分的产出还留着两成明确的缺口，
// 而要求满分会让任何一处主观项把循环永远钉在原地。
// cap: fixed — 0.8：留两成缺口可接受，满分则被主观项钉死
// keep: 删掉后满分才通过，主观项把每一次评审钉死；由「留两成缺口可接受」兜住
const SELF_REVIEW_QUALITY_THRESHOLD: f32 = 0.8;

impl Default for SelfReviewConfig {
    fn default() -> Self {
        Self {
            max_iterations: SELF_REVIEW_MAX_ITERATIONS,
            quality_threshold: SELF_REVIEW_QUALITY_THRESHOLD,
            revise: true,
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
    use super::{SelfReviewConfig, SELF_REVIEW_MAX_ITERATIONS, SELF_REVIEW_QUALITY_THRESHOLD};

    /// 自审的两个数只有一处声明：默认值读常量，源码面 `impl Default` 的字段里
    /// 不再有数字（下一笔写回裸字面量就红）。
    #[test]
    fn the_self_review_default_reads_its_names() {
        let cfg = SelfReviewConfig::default();
        assert_eq!(cfg.max_iterations, SELF_REVIEW_MAX_ITERATIONS);
        assert_eq!(cfg.quality_threshold, SELF_REVIEW_QUALITY_THRESHOLD);

        let body = include_str!("self_review.rs")
            .split("impl Default for SelfReviewConfig")
            .nth(1)
            .expect("the impl is in this file")
            .split("\n}\n")
            .next()
            .unwrap();
        let bare: Vec<&str> = body
            .lines()
            .filter(|line| {
                line.split_once(':').is_some_and(|(field, value)| {
                    let value = value.trim().trim_end_matches(',');
                    !field.is_empty()
                        && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                        && value.starts_with(|c: char| c.is_ascii_digit())
                        && value != "0"
                })
            })
            .map(str::trim)
            .collect();
        assert!(bare.is_empty(), "默认值要读具名常量：{bare:?}");
    }

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
    fn a_configuration_written_before_the_rewrite_knob_keeps_rewriting() {
        // Every deployment config in the tree predates this field. A missing
        // key must read as the behaviour those deployments were tuned against,
        // not as the cheaper mode switched on by a default.
        let read: SelfReviewConfig = serde_json::from_str(
            r#"{"max_iterations": 2, "quality_threshold": 0.8,
                "spec": null, "best_practices": []}"#,
        )
        .expect("a config without the knob is still a config");
        assert!(
            read.revise,
            "an absent `revise` must not turn the rewrite off"
        );
        assert!(SelfReviewConfig::default().revise);
    }

    #[test]
    fn judge_only_is_a_readable_configuration() {
        let read: SelfReviewConfig = serde_json::from_str(
            r#"{"max_iterations": 2, "quality_threshold": 0.8,
                "spec": null, "best_practices": [], "revise": false}"#,
        )
        .expect("the knob is a field");
        assert!(!read.revise);
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
