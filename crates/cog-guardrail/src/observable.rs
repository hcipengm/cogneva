//! Observable implementation for cog-guardrail.
//! Exposes D6 (Safety & Compliance) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<GuardrailObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<GuardrailObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(GuardrailObservable::new()))
        .clone()
}

/// Guardrail-layer observable state.
///
/// What this publishes is what this layer decides: how many checks it blocked,
/// warned about, or let through, and how many of the blocks came from the
/// harmful-content detector. A refusal count is deliberately not among them.
/// Refusal is an axis of the safety design, but it is not a decision this layer
/// reaches -- `GuardResult` has no refusal variant and no check here can
/// produce one -- so a counter for it could only be published as a constant
/// zero beside real counts, and "no refusals happened" is not the same reading
/// as "nothing here can tell". It is gone rather than wired to a nearby verdict
/// that would be something else under that name, and the published set is
/// pinned below so a constant cannot come back quietly.
#[derive(Default)]
pub struct GuardrailObservable {
    block_count: AtomicU64,
    warn_count: AtomicU64,
    pass_count: AtomicU64,
    harmful_detected: AtomicU64,
}

impl GuardrailObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_pass(&self) {
        self.pass_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_block(&self) {
        self.block_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_warn(&self) {
        self.warn_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_harmful(&self) {
        self.harmful_detected.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl Observable for GuardrailObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D6" {
            let pass = self.pass_count.load(Ordering::Relaxed);
            let block = self.block_count.load(Ordering::Relaxed);
            let warn = self.warn_count.load(Ordering::Relaxed);
            let total = pass + block + warn;

            metrics.push(RawMetric::new("guard_block_count", block as f64));
            metrics.push(RawMetric::new("guard_warn_count", warn as f64));
            metrics.push(RawMetric::new("guard_pass_count", pass as f64));
            metrics.push(RawMetric::new(
                "guard_harmful_detected",
                self.harmful_detected.load(Ordering::Relaxed) as f64,
            ));
            if total > 0 {
                metrics.push(RawMetric::new(
                    "guard_block_rate",
                    block as f64 / total as f64,
                ));
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D6")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published set is pinned, and pinned to *verdicts this layer reaches*.
    /// A series nothing can write still renders, as a constant, and a constant
    /// published beside real counts reads as a measurement of zero. That is what
    /// `guard_refusal_count` was: `GuardResult` has no refusal variant, so no
    /// check could ever reach a call that fed it. It is gone rather than wired
    /// to a nearby verdict that would be something else under that name, and
    /// this test fails if a name reappears without a verdict behind it.
    #[tokio::test]
    async fn d6_publishes_only_verdicts_this_layer_reaches() {
        let observable = GuardrailObservable::new();
        assert_eq!(
            observable.collect_metrics("D6").await.unwrap().len(),
            4,
            "with nothing checked yet the counts are zero but each is a real \
             verdict this layer can reach"
        );

        observable.record_block();
        observable.record_harmful();
        observable.record_warn();
        observable.record_pass();

        let names: Vec<String> = observable
            .collect_metrics("D6")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.name)
            .collect();
        assert_eq!(
            names,
            vec![
                "guard_block_count",
                "guard_warn_count",
                "guard_pass_count",
                "guard_harmful_detected",
                "guard_block_rate",
            ],
            "the rate appears once there is a denominator to divide by"
        );

        let rate = observable.collect_metrics("D6").await.unwrap()[4].value;
        assert!(
            (rate - 1.0 / 3.0).abs() < f64::EPSILON,
            "one block out of a block, a warn and a pass, got {rate}"
        );
    }
}
