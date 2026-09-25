//! Observable implementation for cog-observability.
//! Exposes D5 (Observability & Debuggability) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<ObservabilityObservable>> = OnceLock::new();

/// One counter per rejection criterion, so the axis has no second hand-written
/// ordering to drift from — the list in `cog_core` is the only one.
const REJECTION_CAUSES: usize = cog_core::RejectionCause::ALL.len();

pub fn global_observable() -> Arc<ObservabilityObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(ObservabilityObservable::new()))
        .clone()
}

/// Observability-layer observable state.
/// Tracks snapshot latency, event counts, rendering metrics, and self-evolution
/// outcomes.
pub struct ObservabilityObservable {
    snapshot_latency_ms: AtomicU64,
    event_count: AtomicU64,
    rendering_latency_ms: AtomicU64,
    evolution_event_total: AtomicU64,
    evolution_event_failed_total: AtomicU64,
    evolution_change_applied_total: AtomicU64,
    evolution_change_failed_total: AtomicU64,
    /// One slot per [`cog_core::RejectionCause`], addressed by `slot()` so the
    /// axis has no second hand-written ordering to drift from.
    evolution_change_rejected_total: [AtomicU64; REJECTION_CAUSES],
}

impl Default for ObservabilityObservable {
    fn default() -> Self {
        Self {
            snapshot_latency_ms: AtomicU64::new(0),
            event_count: AtomicU64::new(0),
            rendering_latency_ms: AtomicU64::new(0),
            evolution_event_total: AtomicU64::new(0),
            evolution_event_failed_total: AtomicU64::new(0),
            evolution_change_applied_total: AtomicU64::new(0),
            evolution_change_failed_total: AtomicU64::new(0),
            evolution_change_rejected_total: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl ObservabilityObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_snapshot_latency(&self, ms: u64) {
        self.snapshot_latency_ms.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn record_event(&self) {
        self.event_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rendering_latency(&self, ms: u64) {
        self.rendering_latency_ms.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn record_evolution_event(&self, failed: bool) {
        self.evolution_event_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.evolution_event_failed_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_evolution_change_applied(&self) {
        self.evolution_change_applied_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_evolution_change_failed(&self) {
        self.evolution_change_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_evolution_change_rejected(&self, cause: cog_core::RejectionCause) {
        self.evolution_change_rejected_total[cause.slot()].fetch_add(1, Ordering::Relaxed);
    }

    /// Every cause's count, zeros included.
    ///
    /// The whole axis is published, not the causes that have happened: absent
    /// and zero read alike, and the interesting reading is the one that is zero
    /// because the loop never gets that far — which is exactly what an omitted
    /// series would hide.
    pub fn evolution_change_rejected(&self) -> Vec<(cog_core::RejectionCause, u64)> {
        cog_core::RejectionCause::ALL
            .iter()
            .map(|cause| {
                (
                    *cause,
                    self.evolution_change_rejected_total[cause.slot()].load(Ordering::Relaxed),
                )
            })
            .collect()
    }
}

#[async_trait]
impl Observable for ObservabilityObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D5" {
            metrics.push(RawMetric::new(
                "obs_snapshot_latency_ms",
                self.snapshot_latency_ms.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "obs_event_count",
                self.event_count.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "obs_rendering_latency_ms",
                self.rendering_latency_ms.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "evolution_event_total",
                self.evolution_event_total.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "evolution_event_failed_total",
                self.evolution_event_failed_total.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "evolution_change_applied_total",
                self.evolution_change_applied_total.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "evolution_change_failed_total",
                self.evolution_change_failed_total.load(Ordering::Relaxed) as f64,
            ));
            for (cause, count) in self.evolution_change_rejected() {
                metrics.push(
                    RawMetric::new("evolution_change_rejected_total", count as f64)
                        .with_label("cause", cause.as_str()),
                );
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D5")]
    }
}

#[async_trait]
impl cog_core::EvolutionMetrics for ObservabilityObservable {
    async fn record_event(&self, failed: bool) {
        self.record_evolution_event(failed);
    }

    async fn record_change_applied(&self) {
        self.record_evolution_change_applied();
    }

    async fn record_change_failed(&self) {
        self.record_evolution_change_failed();
    }

    async fn record_change_rejected(&self, cause: cog_core::RejectionCause) {
        self.record_evolution_change_rejected(cause);
    }
}

#[cfg(test)]
mod rejection_counter_tests {
    use super::*;
    use cog_core::RejectionCause;

    /// The published lines for one series, as `(label value, value)`.
    async fn published(observable: &ObservabilityObservable, name: &str) -> Vec<(String, f64)> {
        observable
            .collect_metrics("D5")
            .await
            .expect("collecting D5 metrics")
            .into_iter()
            .filter(|metric| metric.name == name)
            .map(|metric| {
                (
                    metric.labels.get("cause").cloned().unwrap_or_default(),
                    metric.value,
                )
            })
            .collect()
    }

    /// The whole axis is published from the first scrape, zeros included. An
    /// omitted series and a zero read alike, and the zero that matters here is
    /// the criterion nothing ever reaches — a reader has to be able to see that
    /// zero, or "this repair never happens" and "this counter was never wired
    /// up" are the same reading.
    #[tokio::test]
    async fn every_criterion_is_published_before_anything_is_refused() {
        let observable = ObservabilityObservable::new();
        let lines = published(&observable, "evolution_change_rejected_total").await;
        assert_eq!(lines.len(), RejectionCause::ALL.len());
        for cause in RejectionCause::ALL {
            let line = lines
                .iter()
                .find(|(label, _)| label == cause.as_str())
                .unwrap_or_else(|| panic!("{cause:?} has no published line"));
            assert_eq!(line.1, 0.0, "{cause:?} is not zero on a fresh observable");
        }
    }

    /// A refusal lands under its own criterion and nowhere else: the axis is
    /// what a reader splits by, and a count that leaked into another criterion
    /// would answer the wrong repair. The aggregate counter is deliberately not
    /// moved here — it counts every way a change can die, which is a different
    /// question the caller answers separately.
    #[tokio::test]
    async fn a_refusal_is_counted_under_its_own_criterion() {
        let observable = ObservabilityObservable::new();
        observable.record_evolution_change_rejected(RejectionCause::ContextDoesNotApply);

        let lines = published(&observable, "evolution_change_rejected_total").await;
        for (label, value) in &lines {
            let expected = if label == RejectionCause::ContextDoesNotApply.as_str() {
                1.0
            } else {
                0.0
            };
            assert_eq!(*value, expected, "{label} holds {value}");
        }

        let aggregate = published(&observable, "evolution_change_failed_total").await;
        assert_eq!(aggregate.len(), 1);
        assert_eq!(aggregate[0].1, 0.0);
    }
}
