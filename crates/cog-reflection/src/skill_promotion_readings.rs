//! What the learning-to-skill promotion gate decides, and what it decided on.
//!
//! A learning becomes a system-level configuration once it has recurred enough
//! times, across enough distinct tasks, recently enough. The stored
//! `recurrence_count` is the input to that decision, and the store is what the
//! next recurrence reads back -- so a count that can never rise above 1 makes a
//! threshold of 3 unreachable, and the gate reads as permanently closed with
//! nothing on any reading to say so. That is not a hypothetical: it is what the
//! pipeline did for three weeks.
//!
//! Two series, because a closed gate and a gate that has never met a mature
//! learning look the same on the decision counter alone:
//!
//! - `cogneva_evolution_learning_promotion_decisions_total{decision}` -- how
//!   the gate resolved. `not_ready` with no `promoted` beside it is what a gate
//!   that cannot open looks like from outside; a single `promoted` says it can.
//! - `cogneva_evolution_learning_recurrence_decided_total{recurrence}` -- the
//!   recurrence the decisions were taken on, bucketed. This is the reading that
//!   moves **first**, and it is the one that separates the two cases: a
//!   distribution still sitting entirely in `1` is a count that is not
//!   accumulating, while one that reaches `3` is a gate that is merely waiting.
//!
//! The buckets stop at `4+` because the question is whether the count is
//! climbing through the threshold, not how large it eventually gets. The
//! threshold itself is configurable (`DefaultLearningPromoter::with_min_recurrence`,
//! default 3), so read the distribution against the configured value rather
//! than against these bucket names.

use std::collections::BTreeMap;
use std::sync::Mutex;

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// How the promotion gate resolved, by resolution.
pub const PROMOTION_DECISIONS_TOTAL_METRIC: &str =
    "cogneva_evolution_learning_promotion_decisions_total";

/// The recurrence the promotion decisions were taken on, bucketed.
pub const RECURRENCE_DECIDED_TOTAL_METRIC: &str =
    "cogneva_evolution_learning_recurrence_decided_total";

/// The label naming how a promotion attempt resolved.
pub const DECISION_LABEL: &str = "decision";

/// The label naming the recurrence bucket a decision was taken on.
pub const RECURRENCE_LABEL: &str = "recurrence";

/// Every value the decision label can take.
///
/// Published as a set rather than as whatever has been seen, so that a
/// resolution which has never happened reads as a zero rather than as a series
/// that was never wired up.
pub const DECISIONS: [&str; 2] = [PromotionDecision::PROMOTED, PromotionDecision::NOT_READY];

/// Every bucket the recurrence label can take.
pub const RECURRENCE_BUCKETS: [&str; 4] = ["1", "2", "3", "4+"];

/// The recurrence value below which a count cannot open the default gate, and
/// above which it has.
///
/// A count of 0 is impossible for a stored learning -- the row is created with
/// 1 -- but it is folded into the `1` bucket rather than given a bucket of its
/// own: a reading that grows a row for a state the store cannot hold invites
/// the reader to treat that row as a measurement.
pub fn recurrence_bucket(recurrence_count: u32) -> &'static str {
    match recurrence_count {
        0 | 1 => "1",
        2 => "2",
        3 => "3",
        _ => "4+",
    }
}

/// How a promotion attempt resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionDecision {
    /// The learning did not meet the maturity criteria; nothing was written.
    NotReady,
    /// A system-level configuration was written from the learning.
    Promoted,
}

impl PromotionDecision {
    const NOT_READY: &'static str = "not_ready";
    const PROMOTED: &'static str = "promoted";

    /// The label value.
    pub fn as_str(self) -> &'static str {
        match self {
            PromotionDecision::NotReady => Self::NOT_READY,
            PromotionDecision::Promoted => Self::PROMOTED,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    /// Attempts per resolution.
    decisions: BTreeMap<&'static str, u64>,
    /// Attempts per recurrence bucket.
    recurrences: BTreeMap<&'static str, u64>,
}

/// The promotion decisions this process has taken and the counts they were
/// taken on.
#[derive(Debug, Default)]
pub struct SkillPromotionReadings {
    counters: Mutex<Counters>,
}

impl SkillPromotionReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one promotion decision along with the recurrence it was taken on.
    ///
    /// Both series move together in one call because they are two views of one
    /// event: a decision counter that could advance without its recurrence
    /// being counted would let the pair disagree about how many decisions were
    /// taken.
    pub fn record(&self, decision: PromotionDecision, recurrence_count: u32) {
        // A reading must not be the thing that breaks the loop: a poisoned lock
        // means some other thread panicked while holding it, and the counter
        // this decision would have moved is not worth failing the promotion for.
        let mut c = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        *c.decisions.entry(decision.as_str()).or_default() += 1;
        *c.recurrences
            .entry(recurrence_bucket(recurrence_count))
            .or_default() += 1;
    }

    /// How many decisions resolved this way. Zero for a resolution that has
    /// never happened.
    pub fn decisions(&self, decision: PromotionDecision) -> u64 {
        self.counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .decisions
            .get(decision.as_str())
            .copied()
            .unwrap_or(0)
    }

    /// How many decisions were taken on a recurrence in this bucket.
    pub fn decided_on(&self, bucket: &str) -> u64 {
        self.counters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .recurrences
            .get(bucket)
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl Observable for SkillPromotionReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let c = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::with_capacity(DECISIONS.len() + RECURRENCE_BUCKETS.len());
        for decision in DECISIONS {
            out.push(
                RawMetric::new(
                    PROMOTION_DECISIONS_TOTAL_METRIC,
                    c.decisions.get(decision).copied().unwrap_or(0) as f64,
                )
                .with_label(DECISION_LABEL, decision),
            );
        }
        for bucket in RECURRENCE_BUCKETS {
            out.push(
                RawMetric::new(
                    RECURRENCE_DECIDED_TOTAL_METRIC,
                    c.recurrences.get(bucket).copied().unwrap_or(0) as f64,
                )
                .with_label(RECURRENCE_LABEL, bucket),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every configured bucket and resolution is published even with nothing
    /// recorded, so that a closed gate and an unwired reading do not both read
    /// as "no series".
    #[tokio::test]
    async fn an_unmoved_reading_still_names_every_bucket() {
        let readings = SkillPromotionReadings::new();
        let metrics = readings.collect_metrics("").await.unwrap();
        assert_eq!(metrics.len(), DECISIONS.len() + RECURRENCE_BUCKETS.len());
        assert!(metrics.iter().all(|m| m.value == 0.0));
    }

    /// The recurrence a decision was taken on is reported beside the decision,
    /// on the bucket that contains it -- including above the top bucket, which
    /// must not silently vanish.
    #[tokio::test]
    async fn a_decision_carries_the_recurrence_it_was_taken_on() {
        let readings = SkillPromotionReadings::new();
        readings.record(PromotionDecision::NotReady, 1);
        readings.record(PromotionDecision::NotReady, 2);
        readings.record(PromotionDecision::Promoted, 3);
        readings.record(PromotionDecision::NotReady, 9);

        assert_eq!(readings.decisions(PromotionDecision::NotReady), 3);
        assert_eq!(readings.decisions(PromotionDecision::Promoted), 1);
        assert_eq!(readings.decided_on("1"), 1);
        assert_eq!(readings.decided_on("2"), 1);
        assert_eq!(readings.decided_on("3"), 1);
        assert_eq!(readings.decided_on("4+"), 1);
        assert_eq!(recurrence_bucket(0), "1");
    }
}
