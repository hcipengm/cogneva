//! Observable implementation for cog-llm.
//! Exposes D9 (Cost & Performance) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// 全局 LLM Observable 实例。
static GLOBAL: OnceLock<Arc<LlmObservable>> = OnceLock::new();

/// 获取全局 LLM Observable 实例。
pub fn global_observable() -> Arc<LlmObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(LlmObservable::new()))
        .clone()
}

/// LLM-layer observable state.
#[derive(Default)]
pub struct LlmObservable {
    call_count: AtomicU64,
    token_in: AtomicU64,
    token_out: AtomicU64,
    total_latency_ms: AtomicU64,
    ttft_ms: AtomicU64,
    ttft_count: AtomicU64,
    error_count: AtomicU64,
}

impl LlmObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_call(&self, tokens_in: u64, tokens_out: u64, latency_ms: u64) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.token_in.fetch_add(tokens_in, Ordering::Relaxed);
        self.token_out.fetch_add(tokens_out, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
    }

    pub fn record_ttft(&self, ms: u64) {
        self.ttft_ms.fetch_add(ms, Ordering::Relaxed);
        self.ttft_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl Observable for LlmObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D9" {
            let calls = self.call_count.load(Ordering::Relaxed);
            metrics.push(RawMetric::new("llm_call_count", calls as f64));
            metrics.push(RawMetric::new(
                "llm_token_in",
                self.token_in.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "llm_token_out",
                self.token_out.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "llm_error_count",
                self.error_count.load(Ordering::Relaxed) as f64,
            ));
            if calls > 0 {
                let total_latency = self.total_latency_ms.load(Ordering::Relaxed);
                metrics.push(RawMetric::new(
                    "llm_avg_latency_ms",
                    total_latency as f64 / calls as f64,
                ));
            }
            let ttft_count = self.ttft_count.load(Ordering::Relaxed);
            if ttft_count > 0 {
                let total_ttft = self.ttft_ms.load(Ordering::Relaxed);
                metrics.push(RawMetric::new(
                    "llm_avg_ttft_ms",
                    total_ttft as f64 / ttft_count as f64,
                ));
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D9".into()]
    }
}
