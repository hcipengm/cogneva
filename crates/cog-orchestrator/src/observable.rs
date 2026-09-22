//! Observable implementation for cog-orchestrator.
//! Exposes D1 (Outcome) and D8 (Multi-Agent Collaboration) raw metrics, plus
//! the live pending state of the streams this process consumes.

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<OrchestratorObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<OrchestratorObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(OrchestratorObservable::new()))
        .clone()
}

static STREAM_PENDING: OnceLock<Arc<StreamPendingObservable>> = OnceLock::new();

/// The process-wide record of what each consumed stream is holding without an
/// ack. One instance per process: the consumers that feed it are the ones that
/// know which groups they read, and the exposition asks this one place.
pub fn stream_pending_observable() -> Arc<StreamPendingObservable> {
    STREAM_PENDING
        .get_or_init(|| Arc::new(StreamPendingObservable::new()))
        .clone()
}

/// Measure one consumed stream's pending state on its own cadence, in its own
/// task, alongside whatever sweeper reads the same stream.
///
/// Sweeping and measuring want the same fact but answer different questions:
/// the sweeper acts on its own rhythm, the measurement reports on its own, and
/// sharing one tick makes "is anything stuck?" depend on "is the sweeper
/// getting through?" — which is exactly the reading that must not be lost. A
/// sweep that blocks its loop (permits exhausted, or a whole reclaimed message
/// awaited in place) freezes the series, and a frozen series is
/// indistinguishable at the scrape from a stream with nothing pending: the
/// stall hides itself precisely when it is largest. Both callers had this
/// shape, so the cadence lives here once.
///
/// Measured once before the loop, so the series exists before the first tick.
/// The interval's first tick is immediately ready and is consumed, or it would
/// measure a second time right after that first measurement; one measurement
/// per tick after.
pub fn spawn_pending_observer(
    backend: Arc<dyn cog_core::MessageBackend>,
    stream: String,
    group: String,
    claim_idle_ms: u64,
    interval: std::time::Duration,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let observer = stream_pending_observable();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            observer
                .measure(
                    &*backend,
                    &stream,
                    &group,
                    claim_idle_ms,
                    interval.as_secs(),
                )
                .await;
            tokio::select! {
                biased;
                _ = shutdown.wait() => break,
                _ = ticker.tick() => {}
            }
        }
    })
}

/// Entries pending right now on one consumed stream.
pub const STREAM_PENDING_COUNT_METRIC: &str = "cogneva_stream_pending_count";
/// Entries pending longer than the reclaim threshold — work a reclaim pass
/// running on its configured cadence would already have taken back.
pub const STREAM_PENDING_UNRECLAIMED_METRIC: &str = "cogneva_stream_pending_unreclaimed_count";
/// How long the longest-outstanding of those has been pending. This is the
/// series an alert can be built on: it is the age of the thing that should not
/// exist, so a threshold can be a multiple of the deployment's own reclaim
/// policy instead of a number picked here.
pub const STREAM_PENDING_UNRECLAIMED_AGE_METRIC: &str =
    "cogneva_stream_pending_unreclaimed_oldest_idle_seconds";
/// The reclaim threshold the figures above were measured against.
pub const STREAM_PENDING_CLAIM_IDLE_METRIC: &str = "cogneva_stream_pending_claim_idle_seconds";
/// Unix seconds of the last measurement that succeeded. A stuck reader leaves
/// the rows above frozen rather than absent, so staleness has to be its own
/// series — otherwise a dead reclaim loop reads exactly like a clean stream.
pub const STREAM_PENDING_MEASURE_LAST_METRIC: &str = "cogneva_stream_pending_measure_last_seconds";
/// The cadence the measurement is supposed to run at, so the staleness rule's
/// threshold is a multiple of what this deployment actually configured.
pub const STREAM_PENDING_MEASURE_INTERVAL_METRIC: &str =
    "cogneva_stream_pending_measure_interval_seconds";
/// Measurements that failed. A reader that is running but cannot ask the
/// backend is a different fault from one that stopped, and both leave the age
/// above untrustworthy.
pub const STREAM_PENDING_MEASURE_FAILURES_METRIC: &str =
    "cogneva_stream_pending_measure_failures_total";

/// Names of the rules that consume these gauges, so the pairing can be
/// asserted rather than assumed.
pub const STREAM_PENDING_UNRECLAIMED_METRIC_RULE: &str = "stream_pending_unreclaimed";
pub const STREAM_PENDING_STALE_METRIC_RULE: &str = "stream_pending_measure_stale";
pub const STREAM_PENDING_FAILURES_METRIC_RULE: &str = "stream_pending_measure_failing";

/// What one stream's last measurement found. Kept per stream because the
/// streams are independent: one clean stream must not hide another's stranded
/// message, and a per-stream gauge is what lets a rule name which one.
#[derive(Clone, Copy)]
struct StreamPendingState {
    count: u64,
    unreclaimed_count: u64,
    unreclaimed_oldest_idle_ms: u64,
    claim_idle_ms: u64,
    measure_interval_secs: u64,
    last_measure_seconds: u64,
    measure_failures: u64,
}

/// Pending state of the streams this process consumes, fed by the consumer
/// loops themselves. The loops that reclaim pending messages are the only
/// place that knows both the stream name and the threshold it reclaims at, so
/// measuring there keeps one copy of that knowledge instead of a second list
/// to drift.
#[derive(Default)]
pub struct StreamPendingObservable {
    streams: tokio::sync::Mutex<HashMap<String, StreamPendingState>>,
}

impl StreamPendingObservable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Measure one stream and record the result.
    ///
    /// A backend that reports no pending state (in-memory, or one with no
    /// reclaim path) records nothing: an absent series is the truth about
    /// "nothing here can strand a message", and a zero would be a number
    /// nothing measured.
    ///
    /// A failed measurement keeps the previous figures and does not advance
    /// `last_measure_seconds`: the numbers stay readable as "the last thing we
    /// knew", while the staleness and failure series say how old that is.
    ///
    /// The lock is never held across the backend call. Scraping takes the same
    /// lock, so holding it through a slow Redis round trip would turn a slow
    /// backend into a process-wide scrape stall — every series this process
    /// exports would go missing, which reads exactly like the process being
    /// gone. Each phase therefore takes the lock, does one thing, and lets go.
    pub async fn measure(
        &self,
        backend: &dyn cog_core::MessageBackend,
        stream: &str,
        group: &str,
        claim_idle_ms: u64,
        measure_interval_secs: u64,
    ) {
        {
            let mut streams = self.streams.lock().await;
            let entry = streams
                .entry(stream.to_string())
                .or_insert(StreamPendingState {
                    count: 0,
                    unreclaimed_count: 0,
                    unreclaimed_oldest_idle_ms: 0,
                    claim_idle_ms,
                    measure_interval_secs,
                    last_measure_seconds: 0,
                    measure_failures: 0,
                });
            entry.claim_idle_ms = claim_idle_ms;
            entry.measure_interval_secs = measure_interval_secs;
        }

        let measured = backend.pending_stats(stream, group, claim_idle_ms).await;

        let mut streams = self.streams.lock().await;
        let Some(entry) = streams.get_mut(stream) else {
            // The entry is created above and nothing removes entries, so this is
            // unreachable; falling back to a fresh insert would be worse than
            // saying so.
            tracing::warn!(stream = %stream, "pending measurement found no entry");
            return;
        };
        match measured {
            Ok(Some(stats)) => {
                entry.count = stats.count;
                entry.unreclaimed_count = stats.unreclaimed_count;
                entry.unreclaimed_oldest_idle_ms = stats.unreclaimed_oldest_idle_ms;
                entry.last_measure_seconds = chrono::Utc::now().timestamp().max(0) as u64;
            }
            Ok(None) => {}
            Err(e) => {
                entry.measure_failures = entry.measure_failures.saturating_add(1);
                tracing::warn!(
                    stream = %stream,
                    group = %group,
                    "pending measurement failed: {e}"
                );
            }
        }
    }
}

#[async_trait]
impl Observable for StreamPendingObservable {
    /// The dimension is ignored on purpose, exactly as for the trace tier
    /// gauges: none of this varies by dimension, so declaring none is what gets
    /// this observable pulled once. Answering only one dimension would instead
    /// have it absent from the scrape — invisible rather than unlabelled.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let streams = self.streams.lock().await;
        let mut out = Vec::new();
        for (stream, state) in streams.iter() {
            // The liveness pair needs a measurement to be stale against, which
            // is why it is emitted only once one has succeeded. Without that
            // anchor a stream that was never measured and a stream measured and
            // found clean are the same absence of evidence.
            if state.last_measure_seconds == 0 {
                continue;
            }
            out.push(
                RawMetric::new(STREAM_PENDING_COUNT_METRIC, state.count as f64)
                    .with_label("stream", stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_UNRECLAIMED_METRIC,
                    state.unreclaimed_count as f64,
                )
                .with_label("stream", stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_UNRECLAIMED_AGE_METRIC,
                    state.unreclaimed_oldest_idle_ms as f64 / 1000.0,
                )
                .with_label("stream", stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_CLAIM_IDLE_METRIC,
                    state.claim_idle_ms as f64 / 1000.0,
                )
                .with_label("stream", stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_MEASURE_INTERVAL_METRIC,
                    state.measure_interval_secs as f64,
                )
                .with_label("stream", stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_MEASURE_LAST_METRIC,
                    state.last_measure_seconds as f64,
                )
                .with_label("stream", stream.as_str()),
            );
            if state.measure_failures > 0 {
                out.push(
                    RawMetric::new(
                        STREAM_PENDING_MEASURE_FAILURES_METRIC,
                        state.measure_failures as f64,
                    )
                    .with_label("stream", stream.as_str()),
                );
            }
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

use tokio::sync::Mutex;

/// Orchestrator-level observable state.
#[derive(Default)]
pub struct OrchestratorObservable {
    task_count: AtomicU64,
    task_success_count: AtomicU64,
    message_count: AtomicU64,
    crew_rounds: Arc<Mutex<HashMap<String, u64>>>,
}

impl OrchestratorObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_task(&self, success: bool) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
        if success {
            self.task_success_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_message(&self) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_crew_round(&self, crew_id: impl Into<String>) {
        let mut map = self.crew_rounds.lock().await;
        *map.entry(crew_id.into()).or_insert(0) += 1;
    }
}

#[async_trait]
impl Observable for OrchestratorObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        match dimension {
            "D1" => {
                let total = self.task_count.load(Ordering::Relaxed);
                let success = self.task_success_count.load(Ordering::Relaxed);
                metrics.push(RawMetric::new("orch_task_count", total as f64));
                metrics.push(RawMetric::new("orch_task_success_count", success as f64));
                if total > 0 {
                    metrics.push(RawMetric::new(
                        "orch_task_success_rate",
                        success as f64 / total as f64,
                    ));
                }
            }
            "D8" => {
                metrics.push(RawMetric::new(
                    "orch_message_count",
                    self.message_count.load(Ordering::Relaxed) as f64,
                ));
                let rounds = self.crew_rounds.lock().await;
                for (crew_id, count) in rounds.iter() {
                    metrics.push(
                        RawMetric::new("orch_crew_rounds", *count as f64)
                            .with_label("crew_id", crew_id),
                    );
                }
            }
            _ => {}
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// 这里的 D1 与 agent 面的 D1 不是同一件事：本面记的是编排级累计计数
    /// （任务数、成功数、比率），键固定、基数有界；agent 面在 D1 上是逐 step
    /// 按 task_id 记的，同一个维度名在这两处有不同的有界性。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D1"), DimensionSpec::bounded("D8")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{MessageBackend, MessageStream, PendingStats, SFError};

    /// What the stub answers when asked for a stream's pending state.
    enum Reply {
        Stats(PendingStats),
        /// A backend with no pending state to report.
        Unobservable,
        Failing,
    }

    struct StubBackend(Reply);

    #[async_trait]
    impl MessageBackend for StubBackend {
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
            Ok(())
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<PendingStats>> {
            match self.0 {
                Reply::Stats(stats) => Ok(Some(stats)),
                Reply::Unobservable => Ok(None),
                Reply::Failing => Err(SFError::Redis("stub failure".into())),
            }
        }
    }

    const STREAM: &str = "orchestrator:results:ws-obs-test";
    const GROUP: &str = "cgrp-obs-test";
    const CLAIM_IDLE_MS: u64 = 600_000;

    fn metric<'a>(metrics: &'a [RawMetric], name: &str) -> Option<&'a RawMetric> {
        metrics.iter().find(|m| m.name == name)
    }

    async fn measured(reply: Reply) -> Vec<RawMetric> {
        let observer = StreamPendingObservable::new();
        let backend = StubBackend(reply);
        observer
            .measure(&backend, STREAM, GROUP, CLAIM_IDLE_MS, 60)
            .await;
        observer.collect_metrics("D8").await.unwrap()
    }

    /// The gauges carry the stream the figures belong to: with several streams
    /// consumed by one process, an unlabelled number would leave the reader
    /// unable to tell which one is holding the stranded message.
    #[tokio::test]
    async fn pending_gauges_are_published_per_stream() {
        let metrics = measured(Reply::Stats(PendingStats {
            count: 3,
            unreclaimed_count: 1,
            unreclaimed_oldest_idle_ms: 1_800_000,
        }))
        .await;

        for name in [
            STREAM_PENDING_COUNT_METRIC,
            STREAM_PENDING_UNRECLAIMED_METRIC,
            STREAM_PENDING_UNRECLAIMED_AGE_METRIC,
            STREAM_PENDING_CLAIM_IDLE_METRIC,
            STREAM_PENDING_MEASURE_LAST_METRIC,
        ] {
            let m = metric(&metrics, name).unwrap_or_else(|| panic!("{name} missing"));
            assert_eq!(m.labels.get("stream").map(String::as_str), Some(STREAM));
        }
        assert_eq!(
            metric(&metrics, STREAM_PENDING_COUNT_METRIC).unwrap().value,
            3.0
        );
        assert_eq!(
            metric(&metrics, STREAM_PENDING_UNRECLAIMED_METRIC)
                .unwrap()
                .value,
            1.0
        );
        // Seconds, so the deployed rule's threshold can be written in the same
        // unit as the idle threshold the sweeper reclaims at.
        assert_eq!(
            metric(&metrics, STREAM_PENDING_UNRECLAIMED_AGE_METRIC)
                .unwrap()
                .value,
            1800.0
        );
        assert_eq!(
            metric(&metrics, STREAM_PENDING_CLAIM_IDLE_METRIC)
                .unwrap()
                .value,
            600.0
        );
        assert_eq!(
            metric(&metrics, STREAM_PENDING_MEASURE_INTERVAL_METRIC)
                .unwrap()
                .value,
            60.0
        );
        // No failure has happened, so there is no failure series to read.
        assert!(metric(&metrics, STREAM_PENDING_MEASURE_FAILURES_METRIC).is_none());
    }

    /// A stream nobody measured must be absent, not zero: a zero would claim
    /// the stream is clean, which is the opposite of "nobody looked".
    #[tokio::test]
    async fn an_unmeasured_stream_publishes_nothing() {
        let observer = StreamPendingObservable::new();
        assert!(observer.collect_metrics("D8").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_backend_without_pending_state_publishes_nothing() {
        assert!(measured(Reply::Unobservable).await.is_empty());
    }

    /// A failing read must not present the previous figures as fresh, and must
    /// not erase them either: the age is the last thing known, and staleness
    /// plus the failure count are what tell the reader how much to trust it.
    #[tokio::test]
    async fn a_failed_measurement_keeps_the_last_reading_and_counts_itself() {
        let observer = StreamPendingObservable::new();
        let ok = StubBackend(Reply::Stats(PendingStats {
            count: 2,
            unreclaimed_count: 1,
            unreclaimed_oldest_idle_ms: 900_000,
        }));
        observer
            .measure(&ok, STREAM, GROUP, CLAIM_IDLE_MS, 60)
            .await;
        let after_ok = observer.collect_metrics("D8").await.unwrap();
        let last_ok = metric(&after_ok, STREAM_PENDING_MEASURE_LAST_METRIC)
            .unwrap()
            .value;

        let failing = StubBackend(Reply::Failing);
        observer
            .measure(&failing, STREAM, GROUP, CLAIM_IDLE_MS, 60)
            .await;
        observer
            .measure(&failing, STREAM, GROUP, CLAIM_IDLE_MS, 60)
            .await;
        let after_failures = observer.collect_metrics("D8").await.unwrap();

        assert_eq!(
            metric(&after_failures, STREAM_PENDING_UNRECLAIMED_AGE_METRIC)
                .unwrap()
                .value,
            900.0
        );
        assert_eq!(
            metric(&after_failures, STREAM_PENDING_MEASURE_LAST_METRIC)
                .unwrap()
                .value,
            last_ok,
            "a failed read must not move the last-successful timestamp forward"
        );
        assert_eq!(
            metric(&after_failures, STREAM_PENDING_MEASURE_FAILURES_METRIC)
                .unwrap()
                .value,
            2.0
        );
    }

    /// A rename on either side of a gauge/rule pair leaves both halves
    /// internally consistent and the signal silently absent, so the deployed
    /// rules are pinned against the metric names this module publishes.
    #[test]
    fn deployed_rules_query_the_metrics_this_module_publishes() {
        let chart = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        let rules = root
            .pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present");

        for (rule_name, metric) in [
            (
                STREAM_PENDING_UNRECLAIMED_METRIC_RULE,
                STREAM_PENDING_UNRECLAIMED_AGE_METRIC,
            ),
            (
                STREAM_PENDING_STALE_METRIC_RULE,
                STREAM_PENDING_MEASURE_LAST_METRIC,
            ),
            (
                STREAM_PENDING_FAILURES_METRIC_RULE,
                STREAM_PENDING_MEASURE_FAILURES_METRIC,
            ),
        ] {
            let rule = rules
                .iter()
                .find(|r| r["name"] == rule_name)
                .unwrap_or_else(|| panic!("rule {rule_name} missing"));
            let promql = rule["promql"].as_str().expect("promql is a string");
            assert!(
                promql.contains(metric),
                "rule {rule_name} must query {metric}, got: {promql}"
            );
        }
    }
}
