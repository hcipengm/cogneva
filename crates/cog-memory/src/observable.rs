//! Observable implementation for cog-memory.
//! Exposes D4 (Context & Memory) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

static GLOBAL: OnceLock<Arc<MemoryObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<MemoryObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(MemoryObservable::new()))
        .clone()
}

use std::sync::Arc;

/// Series: extraction prompts this process built, by how the input budget
/// shaped the payload they carried.
pub const EXTRACTION_PAYLOAD_METRIC: &str = "cogneva_memory_extraction_payloads_total";

/// Label on [`EXTRACTION_PAYLOAD_METRIC`]: which [`PayloadOutcome`] the prompt
/// came out as.
pub const OUTCOME_LABEL: &str = "outcome";

/// How one extraction prompt's payload was shaped before it went out.
///
/// The four values are the whole answer to "did the input budget do anything to
/// this call", and they are kept apart because what to do about each is
/// different. `Verbatim` is the budget not binding. `Elided` is the budget
/// binding and the middle of the conversation dropped -- the only value a
/// smaller budget turns into fewer tokens. `Unsplittable` is a payload over
/// budget that could not be shortened at all (one single message, or the head
/// and the tail meeting): it leaves over budget on purpose, so a smaller budget
/// never reaches it. `CharCut` is a payload that is not a message array and was
/// cut by characters instead. Folding those last two into `Elided` would credit
/// a budget change with tokens it never removed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(usize)]
pub enum PayloadOutcome {
    Verbatim = 0,
    Elided = 1,
    Unsplittable = 2,
    CharCut = 3,
}

impl PayloadOutcome {
    /// Every value, in the order the counters are indexed by. The order is
    /// checked against the label each value publishes rather than trusted.
    pub const ALL: [Self; 4] = [
        Self::Verbatim,
        Self::Elided,
        Self::Unsplittable,
        Self::CharCut,
    ];

    /// The value the `outcome` label carries.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Verbatim => "verbatim",
            Self::Elided => "elided",
            Self::Unsplittable => "unsplittable",
            Self::CharCut => "char_cut",
        }
    }
}

/// Memory-layer observable state.
///
/// What this publishes is memory *operation* behaviour: how many backend
/// operations this process ran and how long they took. Token spend is
/// deliberately not among them. The layer does make model calls -- the
/// extractor labels them with actor `memory` -- but the tokens an upstream
/// charges are settled by the upstream's own answer, which arrives at the
/// gateway, and the gateway is the single point all model traffic converges on.
/// A counter here could only ever restate what the gateway already attributes,
/// one process at a time, and a second record of one fact is a second thing to
/// disagree with. This type carried such a counter and a context-overflow
/// counter once; nothing ever wrote either, so both were published as constants
/// and read as measurements of zero. They are gone rather than wired, and the
/// published set is pinned below so a constant cannot come back quietly.
///
/// The payload outcomes are not that counter. They do not say how many tokens
/// went out -- the gateway settles that -- but how the prompt was assembled
/// before it went, and no other surface can see it: an upstream bills a prompt
/// that was truncated and one that fitted exactly the same way, and the
/// extractor's own log line says only that a call happened. So the one question
/// an input budget raises -- how often it binds, and whether lowering it would
/// reach anything -- has no answer anywhere else.
#[derive(Default)]
pub struct MemoryObservable {
    memory_op_latency_ms: AtomicU64,
    memory_op_count: AtomicU64,
    /// Indexed by the discriminant of [`PayloadOutcome`], which [`PayloadOutcome::ALL`]
    /// enumerates in the same order.
    payload_outcomes: [AtomicU64; 4],
}

impl MemoryObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_memory_op(&self, latency_ms: u64) {
        self.memory_op_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.memory_op_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one extraction prompt by how the input budget shaped its payload.
    pub fn record_extraction_payload(&self, outcome: PayloadOutcome) {
        self.payload_outcomes[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl Observable for MemoryObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D4" {
            let op_count = self.memory_op_count.load(Ordering::Relaxed);
            if op_count > 0 {
                let total_latency = self.memory_op_latency_ms.load(Ordering::Relaxed);
                metrics.push(RawMetric::new(
                    "memory_avg_op_latency_ms",
                    total_latency as f64 / op_count as f64,
                ));
            }

            // Every outcome is published once a single prompt has been built,
            // zeros included: "no prompt has been built yet" and "prompts ran
            // and none of them was truncated" are different states, and only
            // the first one is an absence of data. Publishing the zeros only
            // after the first prompt keeps an idle process from putting four
            // zero rows on the scrape for a budget nothing has tested.
            let outcomes: Vec<(&'static str, u64)> = PayloadOutcome::ALL
                .iter()
                .map(|outcome| {
                    (
                        outcome.label(),
                        self.payload_outcomes[*outcome as usize].load(Ordering::Relaxed),
                    )
                })
                .collect();
            if outcomes.iter().map(|(_, count)| count).sum::<u64>() > 0 {
                for (outcome, count) in outcomes {
                    metrics.push(
                        RawMetric::new(EXTRACTION_PAYLOAD_METRIC, count as f64)
                            .with_label(OUTCOME_LABEL, outcome),
                    );
                }
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// D4 的量是进程级累计值，键固定，序列基数有界，可以进周期抓取。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D4")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The published set is pinned, and pinned to *writers*: every name here
    /// has a call site feeding it. A series that no code writes still renders --
    /// as a constant -- and a constant published next to real ones reads as a
    /// measurement. That is what happened to `memory_token_usage` and
    /// `memory_context_overflow_count`, so this test fails if a name reappears
    /// without the recording call beside it.
    #[tokio::test]
    async fn d4_publishes_only_series_something_writes() {
        let observable = MemoryObservable::new();

        // Nothing has run yet: the average has no denominator, so the dimension
        // publishes nothing at all rather than a zero.
        assert!(
            observable.collect_metrics("D4").await.unwrap().is_empty(),
            "an unrun dimension must publish nothing, not a zero reading"
        );

        observable.record_memory_op(30);
        observable.record_memory_op(10);

        let names: Vec<String> = observable
            .collect_metrics("D4")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.name)
            .collect();
        assert_eq!(names, vec!["memory_avg_op_latency_ms".to_string()]);

        let value = observable.collect_metrics("D4").await.unwrap()[0].value;
        assert_eq!(value, 20.0, "mean latency over the operations recorded");
    }

    /// Each outcome has to come out under its own label, and the counters are
    /// indexed by the enum's discriminant while the labels are spelled in a
    /// `match` -- two orders that no compiler ties together. Recording one of
    /// each and reading a `1` under each name is what ties them: a swapped pair
    /// would publish one value twice and lose another, and a shifted index
    /// would show up here rather than as a plausible-looking rate downstream.
    #[tokio::test]
    async fn each_outcome_is_published_under_its_own_label() {
        let observable = MemoryObservable::new();
        for outcome in PayloadOutcome::ALL {
            observable.record_extraction_payload(outcome);
        }

        let published: BTreeSet<String> = observable
            .collect_metrics("D4")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == EXTRACTION_PAYLOAD_METRIC)
            .map(|m| {
                let outcome = m
                    .labels
                    .get(OUTCOME_LABEL)
                    .expect("every payload sample carries the outcome label");
                format!("{outcome}={}", m.value)
            })
            .collect();

        let expected: BTreeSet<String> = PayloadOutcome::ALL
            .iter()
            .map(|outcome| format!("{}=1", outcome.label()))
            .collect();
        assert_eq!(published, expected, "one prompt of each outcome, once each");
    }

    /// Before a prompt is built the family is absent rather than zero: a row of
    /// zeros would read as "the budget was measured and did not bind", which is
    /// a stronger claim than "nothing has been extracted on this process".
    #[tokio::test]
    async fn the_payload_family_is_absent_until_a_prompt_is_built() {
        let observable = MemoryObservable::new();
        observable.record_memory_op(5);

        let names: Vec<String> = observable
            .collect_metrics("D4")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.name)
            .collect();

        assert_eq!(names, vec!["memory_avg_op_latency_ms".to_string()]);
    }
}
