//! Degenerate-loop detection on progress signals — the criterion is whether
//! spend buys progress, never whether spend crossed a flat cap.
//!
//! Each pipeline attempt / roundtable iteration yields a [`ProgressSignals`]
//! snapshot. An attempt counts as progress when ANY signal moved versus the
//! previous one:
//!
//! - evaluation score improved
//! - artifacts grew (count or total bytes)
//! - the failure class is novel (normalized feedback signature changed — a
//!   new error class is new information, even at the same score)
//!
//! A generation that merely rephrases content while score, artifacts, and
//! error class all stay flat is spin, not progress. After `threshold`
//! consecutive non-progress attempts the loop is declared degenerate: the
//! caller stops paying for further attempts and marks the run with
//! [`DEGENERATE_LOOP_PREFIX`] so outer layers treat it as non-retryable and
//! the failure record becomes reflection fuel instead of being retried
//! silently.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::types::{EvaluationResult, GeneratorOutput};

/// Feedback prefix marking a run stopped by stall detection. Outer loops
/// (squad escalation) match on this prefix to skip paid retries — same
/// convention as the terminal environment failure prefix.
pub const DEGENERATE_LOOP_PREFIX: &str = "degenerate_loop";

/// Progress signals extracted from one attempt/iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressSignals {
    pub score: Option<u32>,
    pub artifact_count: usize,
    pub artifact_bytes: usize,
    /// Normalized fingerprint of the evaluator feedback — identifies the
    /// error *class*, insensitive to run ids, numbers, and timestamps.
    pub feedback_signature: u64,
}

impl ProgressSignals {
    pub fn from_attempt(generation: &GeneratorOutput, evaluation: &EvaluationResult) -> Self {
        Self {
            score: evaluation.score,
            artifact_count: generation.artifacts.len(),
            artifact_bytes: generation.artifacts.iter().map(|a| a.content.len()).sum(),
            feedback_signature: feedback_signature(&evaluation.feedback),
        }
    }
}

/// Normalize feedback into an error-class signature: strip digits (run ids,
/// line numbers), collapse whitespace, lowercase, cap length — then hash.
/// Same root cause must map to the same signature across attempts.
fn feedback_signature(feedback: &str) -> u64 {
    let first_line = feedback.lines().next().unwrap_or("").trim();
    let mut sig = String::with_capacity(first_line.len());
    let mut last_space = true;
    for ch in first_line.chars() {
        if ch.is_ascii_digit() {
            if !last_space {
                sig.push(' ');
                last_space = true;
            }
            continue;
        }
        if ch.is_whitespace() {
            if !last_space {
                sig.push(' ');
            }
            last_space = true;
            continue;
        }
        sig.push(ch.to_ascii_lowercase());
        last_space = false;
    }
    let normalized: String = sig.trim().chars().take(80).collect();
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish()
}

/// Did `cur` make progress over `prev`? Progress means the evaluation moved
/// (score up), the deliverable grew (more/larger artifacts), or the failure
/// mode shifted to a class not seen in the previous attempt.
fn made_progress(prev: &ProgressSignals, cur: &ProgressSignals) -> bool {
    cur.score.unwrap_or(0) > prev.score.unwrap_or(0)
        || cur.artifact_count > prev.artifact_count
        || cur.artifact_bytes > prev.artifact_bytes
        || cur.feedback_signature != prev.feedback_signature
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallVerdict {
    Progressing,
    /// The last `threshold` consecutive attempts bought no progress.
    Stalled,
}

/// Tracks consecutive non-progress attempts within one pipeline/roundtable
/// run. Not persistent — a run-level guard, not a cross-task budget.
pub struct StallDetector {
    /// Consecutive attempts (newest last) with no progress over their
    /// predecessor. Only the streak length matters, but keeping the signals
    /// aids debugging via logs.
    streak: Vec<ProgressSignals>,
    last: Option<ProgressSignals>,
    threshold: u32,
}

impl StallDetector {
    /// `threshold`: consecutive non-progress attempts that declare the loop
    /// degenerate. 0 disables detection (never stalls).
    pub fn new(threshold: u32) -> Self {
        Self {
            streak: Vec::new(),
            last: None,
            threshold,
        }
    }

    pub fn observe(&mut self, signals: ProgressSignals) -> StallVerdict {
        if self.threshold == 0 {
            self.last = Some(signals);
            return StallVerdict::Progressing;
        }
        let progressed = match &self.last {
            None => true, // first observation establishes the baseline
            Some(prev) => made_progress(prev, &signals),
        };
        if progressed {
            self.streak.clear();
        } else {
            self.streak.push(signals.clone());
        }
        self.last = Some(signals);
        if self.streak.len() as u32 >= self.threshold {
            StallVerdict::Stalled
        } else {
            StallVerdict::Progressing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squad::pge::types::Artifact;

    fn generation(artifact_bytes: usize) -> GeneratorOutput {
        GeneratorOutput {
            content: serde_json::json!({"code": "x"}),
            artifacts: if artifact_bytes == 0 {
                Vec::new()
            } else {
                vec![Artifact {
                    name: "a".into(),
                    content: "x".repeat(artifact_bytes),
                    artifact_type: "text".into(),
                }]
            },
        }
    }

    fn evaluation(score: u32, feedback: &str) -> EvaluationResult {
        EvaluationResult {
            verdict: super::super::types::Verdict::Fail,
            feedback: feedback.into(),
            score: Some(score),
            criteria: Vec::new(),
            details: None,
        }
    }

    #[test]
    fn flat_signals_stall_after_threshold() {
        let mut d = StallDetector::new(2);
        // baseline
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(0),
                &evaluation(30, "missing tests")
            )),
            StallVerdict::Progressing
        );
        // flat 1
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(0),
                &evaluation(30, "missing tests")
            )),
            StallVerdict::Progressing
        );
        // flat 2 → stalled
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(0),
                &evaluation(30, "missing tests")
            )),
            StallVerdict::Stalled
        );
    }

    #[test]
    fn score_improvement_resets_streak() {
        let mut d = StallDetector::new(2);
        d.observe(ProgressSignals::from_attempt(
            &generation(0),
            &evaluation(30, "missing tests"),
        ));
        d.observe(ProgressSignals::from_attempt(
            &generation(0),
            &evaluation(30, "missing tests"),
        ));
        // score improves → streak cleared
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(0),
                &evaluation(45, "missing tests"),
            )),
            StallVerdict::Progressing
        );
        // needs two fresh flat attempts to stall again
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(0),
                &evaluation(45, "missing tests"),
            )),
            StallVerdict::Progressing
        );
    }

    #[test]
    fn artifact_growth_counts_as_progress() {
        let mut d = StallDetector::new(1);
        d.observe(ProgressSignals::from_attempt(
            &generation(10),
            &evaluation(30, "incomplete"),
        ));
        // same score, same error class, but artifacts grew
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(20),
                &evaluation(30, "incomplete"),
            )),
            StallVerdict::Progressing
        );
    }

    #[test]
    fn novel_error_class_counts_as_progress() {
        let mut d = StallDetector::new(1);
        d.observe(ProgressSignals::from_attempt(
            &generation(0),
            &evaluation(30, "missing tests"),
        ));
        // same score/artifacts, but the failure mode shifted — new information
        assert_eq!(
            d.observe(ProgressSignals::from_attempt(
                &generation(0),
                &evaluation(30, "wrong API usage"),
            )),
            StallVerdict::Progressing
        );
    }

    #[test]
    fn feedback_signature_ignores_digits() {
        let a = feedback_signature("CI run 34075549276 failed on job 12");
        let b = feedback_signature("CI run 999 failed on job 3");
        assert_eq!(a, b);
    }

    #[test]
    fn zero_threshold_disables_detection() {
        let mut d = StallDetector::new(0);
        for _ in 0..10 {
            assert_eq!(
                d.observe(ProgressSignals::from_attempt(
                    &generation(0),
                    &evaluation(30, "same"),
                )),
                StallVerdict::Progressing
            );
        }
    }
}
