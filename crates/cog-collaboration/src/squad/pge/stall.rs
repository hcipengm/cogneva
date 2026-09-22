//! Degenerate-loop detection on progress signals — the criterion is whether
//! spend buys progress, never whether spend crossed a flat cap.
//!
//! Each pipeline attempt / roundtable iteration yields a [`ProgressSignals`]
//! snapshot. An attempt counts as progress when the **evaluator's own
//! judgement** moved versus the previous one:
//!
//! - the overall evaluation score improved
//! - at least one acceptance criterion was judged better and none worse
//!
//! Nothing about the generation itself is a reading, and the constructor takes
//! only the evaluation so there is no way to reintroduce one. Artifact size and
//! the wording of the feedback were both removed: both are proxies a loop can
//! move without getting any closer to passing — padding a deliverable, or
//! rephrasing the same failure so its fingerprint changes. When neither the
//! score nor the criteria move, the round bought nothing, however different it
//! looked. After `threshold` consecutive non-progress attempts the loop is
//! declared degenerate: the caller stops paying for further attempts and marks
//! the run with [`DEGENERATE_LOOP_PREFIX`] so outer layers treat it as
//! non-retryable and the failure record becomes reflection fuel instead of
//! being retried silently.

use super::types::{EvaluationResult, RoundOutcome};
use cog_core::contract::outcome::DEGENERATE_LOOP_PREFIX;

/// 声明本次运行是退化环：带前缀的 feedback 只在构造一次，同时记一次
/// 「已声明」。边界若把这段文本翻成别的 reason，这个分类的序列就会结构性
/// 恒 0，可达性自查靠这次计数把分叉自己报出来。
pub fn degenerate_loop_feedback(detail: String) -> String {
    crate::squad::classify::declare_for(format!("{DEGENERATE_LOOP_PREFIX}: {detail}"))
}

/// The evaluator's judgement of one attempt, reduced to what progress can be
/// measured on. Persisted with the Ralph iteration history, so it doubles as
/// the record of "what this round was worth" across restarts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProgressSignals {
    #[serde(default)]
    pub score: Option<u32>,
    /// Per-criterion evaluations as (criterion name, score). Names come from
    /// the plan's acceptance criteria, so the same criterion is comparable
    /// across attempts while the emitted order is not relied on.
    #[serde(default)]
    pub criteria: Vec<(String, u32)>,
}

impl ProgressSignals {
    pub fn from_evaluation(evaluation: &EvaluationResult) -> Self {
        Self {
            score: evaluation.score,
            criteria: evaluation
                .criteria
                .iter()
                .map(|c| (c.name.clone(), c.score))
                .collect(),
        }
    }

    /// Reading of a round, judged or not. A round that reached no judgement has
    /// no score and no criteria to compare, which is the flat reading it is: the
    /// stall detector counts it as an attempt that bought nothing rather than
    /// reading an invented zero as a real bad review.
    pub fn from_outcome(outcome: &RoundOutcome) -> Self {
        match outcome.judgement() {
            Some(evaluation) => Self::from_evaluation(evaluation),
            None => Self {
                score: None,
                criteria: Vec::new(),
            },
        }
    }
}

/// Did `cur` make progress over `prev`? Progress is the evaluator moving:
/// a higher overall score, or at least one acceptance criterion judged better.
pub fn made_progress(prev: &ProgressSignals, cur: &ProgressSignals) -> bool {
    cur.score.unwrap_or(0) > prev.score.unwrap_or(0) || criteria_improved(prev, cur)
}

/// Per-criterion movement, compared by name over the criteria both readings
/// judged. Better means some shared criterion scored strictly higher and none
/// scored lower: trading one acceptance criterion away to gain another is a
/// different result, not a better one.
fn criteria_improved(prev: &ProgressSignals, cur: &ProgressSignals) -> bool {
    let mut gained = false;
    for (name, score) in &cur.criteria {
        let Some((_, before)) = prev.criteria.iter().find(|(n, _)| n == name) else {
            continue;
        };
        if score > before {
            gained = true;
        } else if score < before {
            return false;
        }
    }
    gained
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
    use crate::squad::pge::types::Criterion;

    fn evaluation(score: u32, feedback: &str) -> EvaluationResult {
        EvaluationResult {
            verdict: super::super::types::Verdict::Fail,
            feedback: feedback.into(),
            score: Some(score),
            criteria: Vec::new(),
            details: None,
        }
    }

    fn with_criteria(score: u32, criteria: &[(&str, u32)]) -> EvaluationResult {
        let mut e = evaluation(score, "criterion run");
        e.criteria = criteria
            .iter()
            .map(|(name, score)| Criterion {
                name: (*name).into(),
                score: *score,
                comment: String::new(),
            })
            .collect();
        e
    }

    #[test]
    fn flat_signals_stall_after_threshold() {
        let mut d = StallDetector::new(2);
        // baseline
        assert_eq!(
            d.observe(ProgressSignals::from_evaluation(&evaluation(
                30,
                "missing tests"
            ))),
            StallVerdict::Progressing
        );
        // flat 1
        assert_eq!(
            d.observe(ProgressSignals::from_evaluation(&evaluation(
                30,
                "missing tests"
            ))),
            StallVerdict::Progressing
        );
        // flat 2 → stalled
        assert_eq!(
            d.observe(ProgressSignals::from_evaluation(&evaluation(
                30,
                "missing tests"
            ))),
            StallVerdict::Stalled
        );
    }

    #[test]
    fn score_improvement_resets_streak() {
        let mut d = StallDetector::new(2);
        d.observe(ProgressSignals::from_evaluation(&evaluation(
            30,
            "missing tests",
        )));
        d.observe(ProgressSignals::from_evaluation(&evaluation(
            30,
            "missing tests",
        )));
        // score improves → streak cleared
        assert_eq!(
            d.observe(ProgressSignals::from_evaluation(&evaluation(
                45,
                "missing tests",
            ))),
            StallVerdict::Progressing
        );
        // needs two fresh flat attempts to stall again
        assert_eq!(
            d.observe(ProgressSignals::from_evaluation(&evaluation(
                45,
                "missing tests",
            ))),
            StallVerdict::Progressing
        );
    }

    /// 换个说法重述同一个失败不是进展：反馈措辞不进入读数，所以改写既不能
    /// 制造"新错误类"，也不能把停滞判据骗过去。
    #[test]
    fn reworded_failure_is_not_progress() {
        let mut d = StallDetector::new(1);
        d.observe(ProgressSignals::from_evaluation(&evaluation(
            30,
            "missing tests",
        )));
        assert_eq!(
            d.observe(ProgressSignals::from_evaluation(&evaluation(
                30,
                "tests are absent, add them",
            ))),
            StallVerdict::Stalled
        );
    }

    /// 评估标准项真的被判定得更好了才算进展——分不动但某一项从 40 抬到 100
    /// 是实打实的收敛。
    #[test]
    fn criteria_improvement_is_progress() {
        let prev = ProgressSignals::from_evaluation(&with_criteria(
            70,
            &[("compiles", 100), ("has tests", 40)],
        ));
        let cur = ProgressSignals::from_evaluation(&with_criteria(
            70,
            &[("compiles", 100), ("has tests", 100)],
        ));
        assert!(made_progress(&prev, &cur));
    }

    /// 一项涨、另一项跌是权衡不是进展：接受标准不能被互相抵消。
    #[test]
    fn criteria_trade_off_is_not_progress() {
        let prev = ProgressSignals::from_evaluation(&with_criteria(
            70,
            &[("compiles", 100), ("has tests", 40)],
        ));
        let cur = ProgressSignals::from_evaluation(&with_criteria(
            70,
            &[("compiles", 60), ("has tests", 100)],
        ));
        assert!(!made_progress(&prev, &cur));
    }

    #[test]
    fn zero_threshold_disables_detection() {
        let mut d = StallDetector::new(0);
        for _ in 0..10 {
            assert_eq!(
                d.observe(ProgressSignals::from_evaluation(&evaluation(30, "same"))),
                StallVerdict::Progressing
            );
        }
    }
}
