//! Observable implementation for cog-memory.
//! Exposes D4 (Context & Memory) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
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

/// Memory-layer observable state.
/// Tracks token usage, context overflow, and memory operation latency.
#[derive(Default)]
pub struct MemoryObservable {
    token_usage: AtomicU64,
    context_overflow_count: AtomicU64,
    memory_op_latency_ms: AtomicU64,
    memory_op_count: AtomicU64,
}

impl MemoryObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_token_usage(&self, tokens: u64) {
        self.token_usage.fetch_add(tokens, Ordering::Relaxed);
    }

    pub fn record_context_overflow(&self) {
        self.context_overflow_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_memory_op(&self, latency_ms: u64) {
        self.memory_op_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.memory_op_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl Observable for MemoryObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D4" {
            metrics.push(RawMetric::new(
                "memory_token_usage",
                self.token_usage.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "memory_context_overflow_count",
                self.context_overflow_count.load(Ordering::Relaxed) as f64,
            ));
            let op_count = self.memory_op_count.load(Ordering::Relaxed);
            if op_count > 0 {
                let total_latency = self.memory_op_latency_ms.load(Ordering::Relaxed);
                metrics.push(RawMetric::new(
                    "memory_avg_op_latency_ms",
                    total_latency as f64 / op_count as f64,
                ));
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D4".into()]
    }
}
