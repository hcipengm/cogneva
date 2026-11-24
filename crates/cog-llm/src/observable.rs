//! Observable implementation for cog-llm.
//! Exposes D9 (Cost & Performance) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
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

/// Why a call failed, as a closed set.
///
/// One number for every failure says a call went wrong and nothing about what to
/// do next. The two shapes here are repaired in different places: a call that
/// ended before a stream existed never reached the model, so the endpoint, the
/// key or the transport is the subject; a response that came back carrying an
/// error did reach the model, so the subject is what was asked for -- the
/// prompt, the token count, the model name. Measured on 2026-11-01 the platform
/// was dark for hours with the reason nowhere on this face: the gateway's own
/// series names the upstream, and the layer that holds the conversation could
/// not say whether a "no answer" was a refusal in transit or an answer that was
/// itself an error.
///
/// The two are emitted as two series every time, including at zero. An absent
/// series and a zero series are different readings -- "nobody measured this" and
/// "measured, nothing failed" -- and a reader that sees neither cannot tell a
/// quiet layer from one that was never wired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallFailure {
    /// The call ended without producing a stream or a response.
    NoStream,
    /// A response arrived and it carries an error body.
    InResponse,
}

impl CallFailure {
    /// Both members, in the order they are published. Written out rather than
    /// derived, because it is the list the emitter iterates: a variant left out
    /// here would be counted and never printed.
    pub const ALL: [Self; 2] = [Self::NoStream, Self::InResponse];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoStream => "no_stream",
            Self::InResponse => "in_response",
        }
    }
}

/// LLM-layer observable state.
#[derive(Default)]
pub struct LlmObservable {
    /// 每一笔调用也落成事件流上的一行（正本）。没有装配时（部署里没有那条流）
    /// 就只留进程内计数——「这一路没记账」与「记了零笔」在读数上是两件事，
    /// 前者由装配时的告警说出，后者由计数说出。
    call_events: OnceLock<Arc<dyn cog_core::LlmCallEvents>>,
    call_count: AtomicU64,
    token_in: AtomicU64,
    token_out: AtomicU64,
    total_latency_ms: AtomicU64,
    ttft_ms: AtomicU64,
    ttft_count: AtomicU64,
    error_no_stream: AtomicU64,
    error_in_response: AtomicU64,
}

impl LlmObservable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 装配事件流那一侧的写者。全局计数器只有进程里那一份，装配一次即可；
    /// 再装一次以第一次为准（第二份写者只会把同一笔写成两行）。
    pub fn attach_call_events(&self, sink: Arc<dyn cog_core::LlmCallEvents>) {
        let _ = self.call_events.set(sink);
    }

    pub fn call_events(&self) -> Option<Arc<dyn cog_core::LlmCallEvents>> {
        self.call_events.get().cloned()
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

    pub fn record_error(&self, failure: CallFailure) {
        let cell = match failure {
            CallFailure::NoStream => &self.error_no_stream,
            CallFailure::InResponse => &self.error_in_response,
        };
        cell.fetch_add(1, Ordering::Relaxed);
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
            for failure in CallFailure::ALL {
                let count = match failure {
                    CallFailure::NoStream => &self.error_no_stream,
                    CallFailure::InResponse => &self.error_in_response,
                };
                metrics.push(
                    RawMetric::new("llm_error_count", count.load(Ordering::Relaxed) as f64)
                        .with_label(crate::LABEL_REASON, failure.as_str()),
                );
            }
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

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D9")]
    }
}
