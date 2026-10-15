//! Observable implementation for cog-memory.
//! Exposes D4 (Context & Memory) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Gauge: which of the pull gate's holders is in power *right now*, one cell
/// per holder, all five published.
///
/// The gate's other reading is a change count on `memory_operations_total`: it
/// answers how often the gate changed hands, which is a different question from
/// who holds it. A pause that never flips leaves the change count flat for
/// exactly as long as the pause lasts, so a reader asking "is the pull paused,
/// and by which branch" gets the same flat line for a paused process and a
/// running one. This gauge is that question's own reading, and it is what lets
/// an alert tell a legitimate local-backoff hold apart from a stalled pass
/// without inferring either from a rate.
///
/// The five cells are published together, including the four reading zero:
/// "this holder is not in power" and "this process has never reported a holder"
/// are different states, and only the second one is an absence of data. Seeded
/// at zero so the first scrape after boot answers rather than missing.
pub const PULL_GATE_HOLDER_METRIC: &str = "cogneva_memory_pull_gate_holder";

/// Label on [`PULL_GATE_HOLDER_METRIC`]: which holder the cell belongs to.
/// Deliberately the same label and the same five values as the change count
/// carries on `memory_operations_total` -- one vocabulary for one question, so
/// a reader can put the two side by side without a translation table.
pub const HOLDER_LABEL: &str = "operation";

/// The five holders, in the order the cells are indexed by -- the same order
/// the gate's own `GateHolder::ALL` enumerates, which is the order its index
/// comes from. This is the single spelling of those five values: the change
/// count's constants index into it, so a value cannot drift between the two
/// readings of one question.
pub const PULL_GATE_OPERATIONS: [&str; 5] = [
    "pull_gate_open",
    "pull_gate_blind",
    "pull_gate_local",
    "pull_gate_shared",
    "pull_gate_both",
];

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
    /// Indexed like [`PULL_GATE_OPERATIONS`]. Exactly one of the five is one
    /// once the gate has reported a holder; all five are zero before that.
    pull_gate_holders: [AtomicU64; 5],
    /// Whether a holder has ever been reported. The five cells are a state, not
    /// a count, so "all zero" is a legitimate reading (the gate is open) and
    /// cannot double as "nothing has reported yet" -- that distinction is this
    /// flag, and it is what keeps an unrun process from publishing a row of
    /// zeros that reads as a measured state.
    pull_gate_reported: AtomicBool,
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

    /// Publish the five holder cells at zero, before any holder is known.
    ///
    /// Called at startup for the same reason the change count's cells are
    /// seeded: a query over series that exist cannot tell "this holder never
    /// took power" from "this process never reported a holder", and the two
    /// call for different readings.
    pub fn seed_pull_gate_holder(&self) {
        for cell in &self.pull_gate_holders {
            cell.store(0, Ordering::Relaxed);
        }
        self.pull_gate_reported.store(true, Ordering::Relaxed);
    }

    /// Move the holder: one cell to one, the other four to zero.
    ///
    /// A gauge that only ever raised the new cell would report two holders in
    /// power at once after the first change of hands, and a reader that sums
    /// the family would then read a paused-and-open gate. The index comes from
    /// the gate's own enum, whose order [`PULL_GATE_OPERATIONS`] shares.
    pub fn set_pull_gate_holder(&self, index: usize) {
        assert!(
            index < PULL_GATE_OPERATIONS.len(),
            "a holder index outside the five cells would publish no cell at all"
        );
        for (i, cell) in self.pull_gate_holders.iter().enumerate() {
            cell.store(u64::from(i == index), Ordering::Relaxed);
        }
        self.pull_gate_reported.store(true, Ordering::Relaxed);
    }

    /// The holder the cells currently say, for the tests that tie the index
    /// order to the label order. Not a reading: it reads this process' own
    /// memory, and the scrape is the reading.
    #[cfg(test)]
    fn reported_holder(&self) -> Option<&'static str> {
        if !self.pull_gate_reported.load(Ordering::Relaxed) {
            return None;
        }
        self.pull_gate_holders
            .iter()
            .position(|cell| cell.load(Ordering::Relaxed) == 1)
            .map(|i| PULL_GATE_OPERATIONS[i])
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

            // The gate holder is a state, so all five cells go out together
            // once the gate has reported one: an open gate is four zeros and a
            // one, and publishing only the one would leave a reader unable to
            // tell a closed gate from a scrape that lost the family.
            if self.pull_gate_reported.load(Ordering::Relaxed) {
                for (operation, cell) in PULL_GATE_OPERATIONS
                    .iter()
                    .zip(self.pull_gate_holders.iter())
                {
                    metrics.push(
                        RawMetric::new(
                            PULL_GATE_HOLDER_METRIC,
                            cell.load(Ordering::Relaxed) as f64,
                        )
                        .with_label(HOLDER_LABEL, *operation),
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

    /// The holder cells are absent until the gate reports one, and then they
    /// are five rows rather than one. The first half is what the flag is for:
    /// without it a process that never wired the gate would publish five zeros,
    /// which reads as "the gate is open" rather than as "nothing is reporting".
    #[tokio::test]
    async fn the_holder_family_is_absent_until_the_gate_reports() {
        let observable = MemoryObservable::new();
        observable.record_memory_op(5);

        let unseeded: Vec<String> = observable
            .collect_metrics("D4")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.name)
            .filter(|name| name == PULL_GATE_HOLDER_METRIC)
            .collect();
        assert!(
            unseeded.is_empty(),
            "no holder has been reported, so the family must be absent rather than a row of zeros"
        );

        observable.seed_pull_gate_holder();
        let seeded: Vec<f64> = observable
            .collect_metrics("D4")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == PULL_GATE_HOLDER_METRIC)
            .map(|m| m.value)
            .collect();
        assert_eq!(
            seeded,
            vec![0.0; PULL_GATE_OPERATIONS.len()],
            "the seed puts every cell on the scrape at zero, one row per holder"
        );
    }

    /// The index the gate passes and the label that goes out are two orders no
    /// compiler ties together, and getting one step out would attribute the
    /// power to the neighbouring holder -- a reading that looks entirely
    /// plausible, since the holders are all zero-or-one. So each index is set
    /// and read back under its own label.
    #[tokio::test]
    async fn each_holder_index_publishes_under_its_own_label() {
        for (index, operation) in PULL_GATE_OPERATIONS.iter().enumerate() {
            let observable = MemoryObservable::new();
            observable.set_pull_gate_holder(index);

            let published: Vec<(String, f64)> = observable
                .collect_metrics("D4")
                .await
                .unwrap()
                .into_iter()
                .filter(|m| m.name == PULL_GATE_HOLDER_METRIC)
                .map(|m| {
                    (
                        m.labels
                            .get(HOLDER_LABEL)
                            .expect("every holder sample carries the holder label")
                            .clone(),
                        m.value,
                    )
                })
                .collect();

            let raised: Vec<&(String, f64)> = published.iter().filter(|(_, v)| *v == 1.0).collect();
            assert_eq!(
                raised.len(),
                1,
                "exactly one holder is in power at a time: {published:?}"
            );
            assert_eq!(
                raised[0].0,
                operation.to_string(),
                "the cell raised is the one the holder's own index names: {published:?}"
            );
            assert_eq!(observable.reported_holder(), Some(*operation));
        }
    }

    /// A gauge that only raised the incoming cell would report two holders in
    /// power after one change of hands, and a reader summing the family would
    /// see a gate that is paused and open at once.
    #[tokio::test]
    async fn a_change_of_hands_lowers_the_holder_it_leaves() {
        let observable = MemoryObservable::new();
        observable.set_pull_gate_holder(2);
        observable.set_pull_gate_holder(4);

        let values: Vec<f64> = observable
            .collect_metrics("D4")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == PULL_GATE_HOLDER_METRIC)
            .map(|m| m.value)
            .collect();

        assert_eq!(
            values.iter().sum::<f64>(),
            1.0,
            "one holder in power after a change of hands, not two: {values:?}"
        );
        assert_eq!(observable.reported_holder(), Some(PULL_GATE_OPERATIONS[4]));
    }
}
