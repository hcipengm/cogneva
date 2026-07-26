//! Observable implementation for cog-guardrail.
//! Exposes D6 (Safety & Compliance) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
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
#[derive(Default)]
pub struct GuardrailObservable {
    block_count: AtomicU64,
    warn_count: AtomicU64,
    pass_count: AtomicU64,
    harmful_detected: AtomicU64,
    refusal_count: AtomicU64,
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

    pub fn record_refusal(&self) {
        self.refusal_count.fetch_add(1, Ordering::Relaxed);
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
            metrics.push(RawMetric::new(
                "guard_refusal_count",
                self.refusal_count.load(Ordering::Relaxed) as f64,
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

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D6".into()]
    }
}
