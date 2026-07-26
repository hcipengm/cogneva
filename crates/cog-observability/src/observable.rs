//! Observable implementation for cog-observability.
//! Exposes D5 (Observability & Debuggability) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<ObservabilityObservable>> = OnceLock::new();

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
    evolution_patch_applied_total: AtomicU64,
    evolution_patch_failed_total: AtomicU64,
}

impl Default for ObservabilityObservable {
    fn default() -> Self {
        Self {
            snapshot_latency_ms: AtomicU64::new(0),
            event_count: AtomicU64::new(0),
            rendering_latency_ms: AtomicU64::new(0),
            evolution_event_total: AtomicU64::new(0),
            evolution_event_failed_total: AtomicU64::new(0),
            evolution_patch_applied_total: AtomicU64::new(0),
            evolution_patch_failed_total: AtomicU64::new(0),
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

    pub fn record_evolution_patch_applied(&self) {
        self.evolution_patch_applied_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_evolution_patch_failed(&self) {
        self.evolution_patch_failed_total
            .fetch_add(1, Ordering::Relaxed);
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
                "evolution_patch_applied_total",
                self.evolution_patch_applied_total.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "evolution_patch_failed_total",
                self.evolution_patch_failed_total.load(Ordering::Relaxed) as f64,
            ));
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D5".into()]
    }
}

#[async_trait]
impl cog_core::EvolutionMetrics for ObservabilityObservable {
    async fn record_event(&self, failed: bool) {
        self.record_evolution_event(failed);
    }

    async fn record_patch_applied(&self) {
        self.record_evolution_patch_applied();
    }

    async fn record_patch_failed(&self) {
        self.record_evolution_patch_failed();
    }
}
