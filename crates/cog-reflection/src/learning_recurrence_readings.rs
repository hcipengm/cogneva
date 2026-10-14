//! Where a learning's recurrence stands when the thresholds that act on it look
//! at it.
//!
//! `recurrence_count` is what the store holds for a learning and it is the input
//! to the two thresholds that act on a pattern: the hook threshold (default 3,
//! `ReflectionEngine::hook_recurrence_threshold`) and the change threshold
//! (default 5, `change_recurrence_threshold`). Both are read off one column, and
//! until this reading existed nothing published that column: a corpus in which
//! nothing is ever seen twice and a corpus whose counts are climbing read exactly
//! the same from every other series, which is how the store said `1` for three
//! weeks with every reading green -- the increment was computed and then dropped,
//! because the matcher's bump ran after the only writer of the column.
//!
//! One series, sampled where the thresholds are consulted:
//!
//! - `cogneva_evolution_learning_recurrence_at_threshold_total{gate,recurrence}`
//!   -- every consultation of a threshold, bucketed by the count it was
//!   consulted on. `gate` says which threshold asked (`hook` for every detected
//!   learning, `change` for the code-defect half), because the two act at
//!   different counts and a single distribution would average them.
//!
//! It samples the **consultation**, not the crossing: a distribution sitting
//! entirely in `1` over a window says the patterns are detected and never
//! recur, which is the thing to see before wondering why nothing matures. That
//! is also what makes it readable before the counts climb -- waiting for a
//! crossing to publish anything would make "never crossed" and "never measured"
//! the same reading.
//!
//! The buckets stop at `4+`. The change gate's threshold is 5, so the top bucket
//! is the one below it: what the reading answers is whether counts climb at all,
//! and a count that is climbing through the hook threshold is visible at `3`
//! before it reaches `4+`. The thresholds themselves are fields on the engine
//! rather than published series, so read the distribution against the configured
//! values instead of against these bucket names.

use std::collections::BTreeMap;
use std::sync::Mutex;

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Where a learning's recurrence stood when a threshold was consulted on it, by
/// which threshold asked.
pub const RECURRENCE_AT_THRESHOLD_TOTAL_METRIC: &str =
    "cogneva_evolution_learning_recurrence_at_threshold_total";

/// The label naming which threshold consulted the count.
pub const GATE_LABEL: &str = "gate";

/// The label naming the recurrence bucket the count fell in.
pub const RECURRENCE_LABEL: &str = "recurrence";

/// Every value the gate label can take.
///
/// Published as a set rather than as whatever has been seen, so a threshold
/// nothing has reached yet reads as a zero rather than as a series that was
/// never wired up -- the two are the same absence otherwise.
pub const GATES: [&str; 2] = [Gate::HOOK, Gate::CHANGE];

/// Every bucket the recurrence label can take.
pub const RECURRENCE_BUCKETS: [&str; 4] = ["1", "2", "3", "4+"];

/// Which threshold was consulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// The hook threshold, consulted on every detected learning.
    Hook,
    /// The change threshold, consulted on the code-defect half only.
    Change,
}

impl Gate {
    const HOOK: &'static str = "hook";
    const CHANGE: &'static str = "change";

    /// The label value.
    pub fn as_str(self) -> &'static str {
        match self {
            Gate::Hook => Self::HOOK,
            Gate::Change => Self::CHANGE,
        }
    }
}

/// The bucket a stored count falls in.
///
/// A count of 0 is impossible for a stored learning -- the row is created with 1
/// -- but it is folded into the `1` bucket rather than given a bucket of its
/// own: a reading that grows a row for a state the store cannot hold invites the
/// reader to treat that row as a measurement.
pub fn recurrence_bucket(recurrence_count: u32) -> &'static str {
    match recurrence_count {
        0 | 1 => "1",
        2 => "2",
        3 => "3",
        _ => "4+",
    }
}

/// The counts the thresholds have been consulted on, by threshold and bucket.
#[derive(Debug, Default)]
pub struct LearningRecurrenceReadings {
    counts: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
}

impl LearningRecurrenceReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one consultation: this threshold looked at a count in this bucket.
    pub fn record(&self, gate: Gate, recurrence_count: u32) {
        // A reading must not be the thing that breaks the loop: a poisoned lock
        // means some other thread panicked while holding it, and the count this
        // consultation would have moved is not worth failing the learning for.
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        *counts
            .entry((gate.as_str(), recurrence_bucket(recurrence_count)))
            .or_default() += 1;
    }

    /// How many times this threshold was consulted on a count in this bucket.
    /// Zero for a cell nothing has reached.
    pub fn consulted(&self, gate: Gate, bucket: &str) -> u64 {
        self.counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(gate.as_str(), bucket))
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl Observable for LearningRecurrenceReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::with_capacity(GATES.len() * RECURRENCE_BUCKETS.len());
        for gate in GATES {
            for bucket in RECURRENCE_BUCKETS {
                out.push(
                    RawMetric::new(
                        RECURRENCE_AT_THRESHOLD_TOTAL_METRIC,
                        counts.get(&(gate, bucket)).copied().unwrap_or(0) as f64,
                    )
                    .with_label(GATE_LABEL, gate)
                    .with_label(RECURRENCE_LABEL, bucket),
                );
            }
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

    /// Every configured gate and bucket is published even with nothing
    /// recorded, so that a threshold nothing has reached and an unwired reading
    /// do not both read as "no series".
    #[tokio::test]
    async fn an_unmoved_reading_still_names_every_cell() {
        let readings = LearningRecurrenceReadings::new();
        let metrics = readings.collect_metrics("").await.unwrap();
        assert_eq!(metrics.len(), GATES.len() * RECURRENCE_BUCKETS.len());
        assert!(metrics.iter().all(|m| m.value == 0.0));
    }

    /// A consultation lands in the cell of the gate that asked and the bucket of
    /// the count it asked about -- including above the top bucket, which must
    /// not silently vanish.
    #[tokio::test]
    async fn a_consultation_lands_in_the_cell_of_its_gate_and_count() {
        let readings = LearningRecurrenceReadings::new();
        readings.record(Gate::Hook, 1);
        readings.record(Gate::Hook, 2);
        readings.record(Gate::Change, 3);
        readings.record(Gate::Hook, 9);

        assert_eq!(readings.consulted(Gate::Hook, "1"), 1);
        assert_eq!(readings.consulted(Gate::Hook, "2"), 1);
        assert_eq!(readings.consulted(Gate::Hook, "3"), 0);
        assert_eq!(readings.consulted(Gate::Hook, "4+"), 1);
        assert_eq!(readings.consulted(Gate::Change, "3"), 1);
        assert_eq!(readings.consulted(Gate::Change, "1"), 0);
        assert_eq!(recurrence_bucket(0), "1");
    }
}
