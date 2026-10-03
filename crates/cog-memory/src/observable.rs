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
#[derive(Default)]
pub struct MemoryObservable {
    memory_op_latency_ms: AtomicU64,
    memory_op_count: AtomicU64,
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
}
