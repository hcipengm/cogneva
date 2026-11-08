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
    loop_name: &'static str,
    backend: Arc<dyn cog_core::MessageBackend>,
    stream: String,
    group: String,
    claim_idle_ms: u64,
    interval: std::time::Duration,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    let observer = stream_pending_observable();
    cog_core::loop_health::spawn(
        loop_name,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let observer = Arc::clone(&observer);
            let backend = Arc::clone(&backend);
            let stream = stream.clone();
            let group = group.clone();
            let shutdown = shutdown.clone();
            async move {
                // The measurement reads what the group holds unacked, so the group
                // must exist before the first read. A pending_stats that answers
                // NOGROUP now reports "nothing pending" instead of failing, but the
                // reclaim pass this series reports on still cannot run without the
                // group, and leaving its existence to whichever consumer subscribes
                // first is exactly the race that fired stream_pending_measure_failing
                // on a healthy queue. Creating it here is idempotent (BUSYGROUP is
                // tolerated by the backend), mirroring the guarantee subscribe()
                // already gives the read loops.
                if let Err(e) = backend.create_consumer_group(&stream, &group).await {
                    tracing::warn!(
                        stream = %stream,
                        group = %group,
                        "pending observer: consumer group creation failed, measuring anyway: {e}"
                    );
                }
                let mut ticker = tokio::time::interval(interval);
                ticker.tick().await;
                loop {
                    beat.beat();
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
            }
        },
    )
}

/// Loop name reported through the background-loop liveness family. Each
/// observed stream gets its own name: a shared one would let a dead observer
/// hide behind its live sibling's beats.
pub const RESULT_STREAM_PENDING_LOOP: &str = "orchestrator_result_stream_pending";
/// Loop name reported through the background-loop liveness family.
pub const READY_STREAM_PENDING_LOOP: &str = "orchestrator_ready_stream_pending";

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
                    .with_label(crate::LABEL_STREAM, stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_UNRECLAIMED_METRIC,
                    state.unreclaimed_count as f64,
                )
                .with_label(crate::LABEL_STREAM, stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_UNRECLAIMED_AGE_METRIC,
                    state.unreclaimed_oldest_idle_ms as f64 / 1000.0,
                )
                .with_label(crate::LABEL_STREAM, stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_CLAIM_IDLE_METRIC,
                    state.claim_idle_ms as f64 / 1000.0,
                )
                .with_label(crate::LABEL_STREAM, stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_MEASURE_INTERVAL_METRIC,
                    state.measure_interval_secs as f64,
                )
                .with_label(crate::LABEL_STREAM, stream.as_str()),
            );
            out.push(
                RawMetric::new(
                    STREAM_PENDING_MEASURE_LAST_METRIC,
                    state.last_measure_seconds as f64,
                )
                .with_label(crate::LABEL_STREAM, stream.as_str()),
            );
            if state.measure_failures > 0 {
                out.push(
                    RawMetric::new(
                        STREAM_PENDING_MEASURE_FAILURES_METRIC,
                        state.measure_failures as f64,
                    )
                    .with_label(crate::LABEL_STREAM, stream.as_str()),
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

// -- execution permits of the ready-task read loops --

/// Execution permits the ready read loops may hold at once, as a gauge.
///
/// One budget per process, shared by every ready stream that process reads and
/// by the pending-reclaim sweeper beside them: a second queue is not a second
/// allowance of concurrent work. The series is per stream because the loops are,
/// but it is the same number on each of them.
pub const TASK_ROUTER_CLAIM_SLOTS_METRIC: &str = "cogneva_task_router_claim_slots";

/// 停摆判据用的那个窗口，以及撑起它的样本数。
///
/// 两条读数缺一不可。窗口本身是判「一个尝试停住了没有」的唯一尺度；样本数是
/// 判「这个窗口是不是一个读数」的唯一证据——没有前者那一族，停摆判据在读数面上
/// 根本不存在；没有后者，窗口停在 `None` 时没人分得出「这份部署还没跑够步数去
/// 量」与「判据没人接上」，而两种情况的后果一样：一条真卡死的活永远不被收。
///
/// 窗口在量出来之前**不发这一条**，不发 0：0 秒是一个合法的窗口（任何静默都
/// 超过它），发出去就是一条会立刻触发告警的假读数。
pub const STALL_WINDOW_SECONDS_METRIC: &str = "cogneva_stall_window_seconds";
pub const STALL_GAP_SAMPLES_METRIC: &str = "cogneva_stall_gap_samples";

/// `cogneva_task_router_claim_slots` 上那个 `source` 标签的两个取值。
///
/// 同一个数有两种来路：一条读数每窗算出来的界，和部署钉死的一个数。没有这个
/// 标签时两者在抓取面上完全同形——而「界不再跟着内存走了」这件事，正是它被钉死
/// 之后唯一看得见的样子。注册表的 `derived` 行要求发布方自己说出这一格（见
/// `deploy/helm/cogneva/tunables.yaml` 与 `tunable_registry_contract_test.rs`）。
pub const CLAIM_SLOTS_SOURCE_LABEL: &str = crate::outward::LABEL_SOURCE;
/// 界由读数导出（`lane_memory` 每窗重算）。
pub const SOURCE_DERIVED: &str = "derived";
/// 界由部署钉死（环境变量给了正数）。
pub const SOURCE_OVERRIDE: &str = "override";
/// Permits held right now, as a gauge — task work this process is running and
/// has not finished. Read from the semaphore itself, so a permit taken by the
/// reclaim sweeper is counted exactly as one taken by the read loop.
pub const TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC: &str = "cogneva_task_router_claim_in_flight";
/// How long the read loop for this stream has been blocked waiting for a
/// permit, in seconds; 0 when it is not waiting.
///
/// The read loop asks the bus for a message first and only then asks for a
/// permit, and while it waits no further read is issued. A silent read loop is
/// therefore ambiguous on its own — `cogneva_stream_read_silent_seconds` grows
/// both when the bus stopped answering and when this process simply had no
/// room — and this is the series that has to be read beside it to tell the two
/// apart.
pub const TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC: &str = "cogneva_task_router_claim_wait_seconds";
/// The longest execution the permits held right now are still allowed, in
/// seconds; 0 when no held permit has reached execution yet.
///
/// Every permit carries the timeout of the task it is running, and the task
/// executor enforces it, so a permit held by this process for longer than its
/// bound is not something the work itself can explain. Published as a wall
/// rather than a constant so the rule that reads it moves with the task
/// timeouts instead of going stale when one of them changes.
///
/// The figure is the bound of the **budget**, and it is published on every
/// stream that draws from that budget. The permits are one per process, so a
/// stream reading 0 here is not "nothing to compare against" — it is a stream
/// that is holding none, precisely because another stream on the same budget
/// is holding them all. Comparing that stream's own wait against its own empty
/// set makes `wait > 0` true of any ordinary backpressure.
pub const TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC: &str =
    "cogneva_task_router_claim_hold_bound_seconds";
/// The deployed rule that reads the pair above.
pub const TASK_ROUTER_CLAIM_STALL_RULE: &str = "task_router_claim_stall";

static CLAIM_SLOTS: OnceLock<Arc<ClaimSlotsObservable>> = OnceLock::new();

/// The process-wide record of the execution permits the ready read loops share.
pub fn claim_slots_observable() -> Arc<ClaimSlotsObservable> {
    CLAIM_SLOTS
        .get_or_init(|| Arc::new(ClaimSlotsObservable::new()))
        .clone()
}

/// One permit this process took, identified so the timeout of the task that
/// ends up running on it can be attached to it. Dropped when the permit is.
pub struct ClaimHold {
    observable: Arc<ClaimSlotsObservable>,
    stream: String,
    token: u64,
}

impl ClaimHold {
    /// Record how long this deployment's attempts are allowed to stay quiet
    /// while still counting as working.
    ///
    /// Fed from the measured inter-beat window, not from a declared budget: the
    /// question a waiting loop has is "is the permit stuck", and a declared
    /// budget answered it only because the platform killed runs at that budget.
    /// With the kill gone, the only bound left that means anything is the one
    /// the fleet's own step cadence produces.
    pub fn note_stall_window(&self, seconds: u64) {
        self.observable
            .set_timeout(&self.stream, self.token, seconds);
    }
}

impl Drop for ClaimHold {
    fn drop(&mut self) {
        self.observable.release(&self.stream, self.token);
    }
}

struct ClaimStreamState {
    semaphore: Arc<tokio::sync::Semaphore>,
    slots: usize,
    /// 这个界是从读数导出的，还是部署钉死的；见 [`SOURCE_DERIVED`] /
    /// [`SOURCE_OVERRIDE`]。两个数在读数上是同一个数，只有这个标签把它们分开。
    source: &'static str,
    waiting_since: Option<std::time::Instant>,
    held: HashMap<u64, u64>,
}

impl ClaimStreamState {
    fn in_flight(&self) -> usize {
        self.slots
            .saturating_sub(self.semaphore.available_permits())
    }

    /// The longest timeout among *this stream's* holds that have one. A hold
    /// with no parsed task yet contributes nothing: its payload may be a poison
    /// one that never reaches execution, and inventing a bound for it would let
    /// a wall be compared against a number nothing stated.
    ///
    /// This is the per-stream half of the answer; what gets published is the
    /// budget's, see [`budget_bound_seconds`].
    fn hold_bound_seconds(&self) -> Option<u64> {
        // `permit_taken` 先写下 0，意思是「这个许可还没说出自己的上界」，不是
        // 一堵零秒的墙。把它算进来，没量到窗口时发布面就会给出一个 0——而 0 是
        // 一个合法窗口（任何静默都超过它），读者会立刻点火。「量不到」与「量到
        // 就是这个值」必须是两个形状，所以哨兵在这里被滤掉。真正的窗口在调用点
        // 已经 `.max(1)`，撞不上这个哨兵。
        self.held.values().copied().filter(|bound| *bound > 0).max()
    }
}

/// The longest timeout stated by any hold drawing from the same budget.
///
/// Two streams that share one semaphore are one budget: a stream waiting on it
/// is waiting for work another stream is running, so the only wall its wait can
/// be compared against is that work's. Keyed on the semaphore rather than on
/// "every stream this process has", so a deployment that later gives a stream
/// its own budget keeps two answers instead of one blended one.
fn budget_bound_seconds(
    streams: &HashMap<String, ClaimStreamState>,
    semaphore: &Arc<tokio::sync::Semaphore>,
) -> Option<u64> {
    streams
        .values()
        .filter(|state| Arc::ptr_eq(&state.semaphore, semaphore))
        .filter_map(ClaimStreamState::hold_bound_seconds)
        .max()
}

/// The permits, and what is holding them, as this process sees them.
///
/// Written on the task router's hot path and read at scrape time, so the state
/// is behind a plain `std::sync::Mutex` and no lock is ever held across an
/// await. Nothing here awaits at all: the numbers are computed at the scrape
/// from state the router keeps current.
#[derive(Default)]
pub struct ClaimSlotsObservable {
    streams: std::sync::Mutex<HashMap<String, ClaimStreamState>>,
    next_token: AtomicU64,
}

impl ClaimSlotsObservable {
    pub fn new() -> Self {
        Self {
            streams: std::sync::Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(0),
        }
    }

    fn streams(&self) -> std::sync::MutexGuard<'_, HashMap<String, ClaimStreamState>> {
        // A panic elsewhere must not take the reading surface down with it; the
        // state behind the lock is a set of counters, and the worst a poisoned
        // guard can hide is one scrape's accounting.
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Declare the budget a stream's read loop draws from. Called once per
    /// consumed stream before it starts reading; idempotent across resubscribes.
    pub fn register(
        self: &Arc<Self>,
        stream: &str,
        semaphore: Arc<tokio::sync::Semaphore>,
        slots: usize,
        source: &'static str,
    ) {
        self.streams()
            .entry(stream.to_string())
            .or_insert_with(|| ClaimStreamState {
                semaphore,
                slots,
                source,
                waiting_since: None,
                held: HashMap::new(),
            });
    }

    /// The budget moved at runtime.
    ///
    /// A bound that is derived is a measurement, so it moves without a restart.
    /// The reading surface has to move with it, or `claim_slots` would keep
    /// reporting the number the process started with while the semaphore admits
    /// another -- the same split between "configured" and "in effect" that a
    /// hardcoded number hides.
    pub fn set_slots(&self, stream: &str, slots: usize) {
        if let Some(state) = self.streams().get_mut(stream) {
            state.slots = slots;
        }
    }

    /// The read loop has asked for a permit and cannot have one yet.
    pub fn wait_started(&self, stream: &str) {
        if let Some(state) = self.streams().get_mut(stream) {
            state
                .waiting_since
                .get_or_insert_with(std::time::Instant::now);
        }
    }

    /// The wait is over — a permit came, shutdown arrived, or the semaphore
    /// closed. All three end the wait, and leaving the start time behind would
    /// report a blocked loop that is not blocked.
    pub fn wait_ended(&self, stream: &str) {
        if let Some(state) = self.streams().get_mut(stream) {
            state.waiting_since = None;
        }
    }

    /// A permit was taken. The returned guard releases the accounting when the
    /// permit is dropped, so a panicking or cancelled processing task cannot
    /// leave a hold behind.
    pub fn permit_taken(self: &Arc<Self>, stream: &str) -> ClaimHold {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        if let Some(state) = self.streams().get_mut(stream) {
            state.held.insert(token, 0);
        }
        ClaimHold {
            observable: Arc::clone(self),
            stream: stream.to_string(),
            token,
        }
    }

    fn set_timeout(&self, stream: &str, token: u64, seconds: u64) {
        if let Some(state) = self.streams().get_mut(stream) {
            if let Some(bound) = state.held.get_mut(&token) {
                *bound = seconds;
            }
        }
    }

    fn release(&self, stream: &str, token: u64) {
        if let Some(state) = self.streams().get_mut(stream) {
            state.held.remove(&token);
        }
    }
}

#[async_trait]
impl Observable for ClaimSlotsObservable {
    /// Dimensionless for the same reason the pending observer is: these are
    /// facts about this process's work queue, not rollups of an outcome, and a
    /// dimension declaration would have the scrape skip them.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let streams = self.streams();
        let mut out = Vec::new();
        // 停摆判据的两条证据先发，且**不发在流下面**：窗口与样本数都是这个进程
        // 的量（`cog_core::progress` 一份环形缓冲），挂在每条流下面时同一份读数
        // 会被数成几份判据——一个读 `count by (stream)` 的人会以为有三条判据在
        // 跑。挂在循环里只是当时顺手，标签轴是假的。
        out.push(RawMetric::new(
            STALL_GAP_SAMPLES_METRIC,
            cog_core::progress::gap_sample_count() as f64,
        ));
        if let Some(window) = cog_core::progress::silence_window() {
            out.push(RawMetric::new(
                STALL_WINDOW_SECONDS_METRIC,
                window.as_secs_f64(),
            ));
        }
        for (stream, state) in streams.iter() {
            // Only the streams that registered exist here, so a stream this
            // process does not read publishes nothing rather than a zero.
            // 只有 `slots` 带 `source`：另外三个是行为读数（在跑几条、等了多久），
            // 它们不随界怎么来而变化；把同一个标签摊到四个序列上，只会让「界是不是
            // 导出的」这件事在三条无关的读数上重复三遍。
            out.push(
                RawMetric::new(TASK_ROUTER_CLAIM_SLOTS_METRIC, state.slots as f64)
                    .with_label(crate::LABEL_STREAM, stream.as_str())
                    .with_label(CLAIM_SLOTS_SOURCE_LABEL, state.source),
            );
            for (name, value) in [
                (TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC, state.in_flight() as f64),
                (
                    TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC,
                    state
                        .waiting_since
                        .map(|since| since.elapsed().as_secs_f64())
                        .unwrap_or(0.0),
                ),
            ] {
                out.push(
                    RawMetric::new(name, value).with_label(crate::LABEL_STREAM, stream.as_str()),
                );
            }
            // 量不到就不发这一条，不发 0：这份部署还没跑够步数、窗口没量出来
            // 时，判这条腿的读者该看到「没有读数」，而不是一个会立刻触发告警
            // 的零。「量不到」与「量到就是这个值」在读数面上必须是两个形状。
            if let Some(bound) = budget_bound_seconds(&streams, &state.semaphore) {
                out.push(
                    RawMetric::new(TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC, bound as f64)
                        .with_label(crate::LABEL_STREAM, stream.as_str()),
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

/// Tasks in the graph right now, one series per status.
pub const TASKS_CURRENT_METRIC: &str = "cogneva_tasks_current";
/// The task graph's readings as named readouts, one series per readout.
///
/// `cogneva_tasks_current` answers "how many rows are in each state", but a
/// reader that wants the size of the graph has to sum label cells, and a
/// reader that names the reading it wants cannot express that sum without
/// inventing it per reader. This rollup names the total the way every other
/// face names a readout. It is computed from the same snapshot as the status
/// vector: a second spelling of one measurement, not a second measurement.
pub const TASKS_READINGS_METRIC: &str = "cogneva_tasks_readings";
/// The readout name for the whole graph's row count.
pub const TASKS_ROWS_READOUT: &str = "tasks_rows_read";
/// How long the oldest task an agent still owes has gone without changing
/// state, in seconds.
///
/// This is the reading a stalled executor shows up in: the count by status says
/// how much is waiting, this one says how long the oldest piece has been
/// waiting, and only the second can grow while the first holds steady.
pub const TASK_OLDEST_UNTOUCHED_METRIC: &str = "cogneva_task_oldest_untouched_seconds";
/// Rows that can still move but have no agent assigned, one series per status.
///
/// The age above says how long the oldest unowned row has waited; this says how
/// many of them there are. An age alone reads the same whether one row or a
/// thousand are waiting, so it cannot say how much of the graph has nobody
/// behind it -- which is the question a reader deciding whether work is being
/// picked up at all is asking.
pub const TASK_UNOWNED_ROWS_METRIC: &str = "cogneva_task_unowned_rows";
/// Rows the dispatcher is leaving pending because they state no criterion, one
/// series per kind.
///
/// A held row is not stuck for a reason of the graph's own making: the
/// dispatcher refuses it because it does not say what would close it, and the
/// only thing that fixes that is the producer or a settle pass writing one. The
/// refusal therefore has to be readable -- the log line naming the kind fires
/// once per process, which says the defect exists and never says whether it is
/// still there.
///
/// Only kinds that hold rows are published: the rollup below carries the zero,
/// so "nothing is held" and "nobody looked" stay different readings without
/// paying a zero series for every kind the platform has ever filed.
pub const TASK_HELD_UNSTATED_METRIC: &str = "cogneva_task_held_unstated_rows";
/// The readout name for the same count rolled up over every kind.
///
/// The rollup is the cell a rule can read without knowing the kinds in advance,
/// and it is published on every successful read including the zeros.
pub const HELD_UNSTATED_READOUT: &str = "held_unstated_rows";
/// Unix seconds of the last successful read of the task graph. The rows above
/// are kept across a failed read, so staleness has to be its own series —
/// otherwise a reader that stopped is indistinguishable from a graph that is
/// exactly as it last appeared.
pub const TASK_GRAPH_MEASURE_LAST_METRIC: &str = "cogneva_task_graph_measure_last_seconds";
/// Reads of the task graph that failed.
pub const TASK_GRAPH_MEASURE_FAILURES_METRIC: &str = "cogneva_task_graph_measure_failures_total";

/// The `agent` label for tasks nobody has been assigned to.
///
/// They are the tasks a "who is stuck" reading would otherwise drop: an
/// unassigned Pending row is the one shape where nothing is wrong with any
/// single agent and the work still is not moving.
pub const UNASSIGNED_AGENT: &str = "unassigned";

/// What one read of the task graph found. Kept between scrapes so a failed
/// read publishes the last known shape next to its age, not a zero.
#[derive(Default)]
struct TaskGraphSnapshot {
    /// `None` until the first successful read. An empty graph and a graph
    /// nobody has read yet must not publish the same zeros.
    counts: Option<Vec<(&'static str, u64)>>,
    oldest_by_agent: Vec<(String, f64)>,
    unowned_by_status: Vec<(&'static str, u64)>,
    /// Pending rows the dispatcher is holding for want of a criterion, per
    /// kind, and their sum. Kept beside the status vector because they come
    /// from the same read: a held row is still counted in `pending`, and the
    /// two numbers together are what says how much of the queue is not moving.
    held_unstated_by_kind: Vec<(String, u64)>,
    held_unstated_total: u64,
    last_measure_seconds: u64,
    measure_failures: u64,
}

/// How long one read of the graph answers every scrape that arrives after it.
///
/// `/metrics` is pulled by more than one collector — the monitoring stack on
/// its own interval, and the GitOps puller once per side of a canary — and a
/// scrape is what asks this observable to read. On the live graph as measured
/// 2026-11-06 (9,899 rows, 26 MB of JSON) one read costs 4.5 s and the two
/// processes holding an orchestrator were doing 13 and 9 of them a minute, so
/// `/metrics` itself took 1.1–6.3 s and the graph was being read continuously
/// by collectors none of which needed their own copy.
///
/// The window is shorter than the interval the monitoring stack pulls at (30 s
/// against this 15 s), so a collector that scrapes on that interval is never
/// answered from an older read than the one it would have done itself; what the
/// window removes is exactly the duplicate reads arriving inside it. The
/// staleness rule reads the stamp of the last successful read against a 900 s
/// bound, so it does not see this at all.
// cap: derived — half the interval the monitoring stack scrapes at, so no collector on that interval is ever handed a read older than one it would have taken itself.
pub const TASK_GRAPH_MEASURE_WINDOW: std::time::Duration = std::time::Duration::from_secs(15);

/// The live shape of the task graph.
///
/// Read at scrape time rather than on its own cadence: the graph is the thing
/// being asked about, not a subscription this process maintains, and a scrape
/// is already the moment someone wants the answer. The counters in
/// [`OrchestratorObservable`] are cumulative outcomes of this process; these are
/// the current graph, which outlives any one process.
pub struct TaskGraphObservable {
    source: Arc<crate::DagExecutor>,
    snapshot: tokio::sync::Mutex<TaskGraphSnapshot>,
    /// When the last read that succeeded finished, or `None` before the first.
    /// Taken at the end rather than the start, so the window covers the time
    /// the read itself took: a scrape that waited out a 4.5 s read is not then
    /// told the window has already half expired.
    ///
    /// Its own lock, taken across the read: two scrapes in the same window have
    /// to be answered by one read, and the second one can only learn that the
    /// first happened by waiting for it. The snapshot lock is deliberately not
    /// reused for this — it is taken only after a read returns, so a slow store
    /// does not hold up the other observables' scrapes, and holding it here
    /// would do exactly that.
    measured_at: tokio::sync::Mutex<Option<std::time::Instant>>,
    /// How long one read answers later scrapes. A field rather than the constant
    /// so a test can shrink it: the behaviour under test is the sharing, and the
    /// number of real seconds it takes to observe is not part of it.
    window: std::time::Duration,
}

impl TaskGraphObservable {
    pub fn new(source: Arc<crate::DagExecutor>) -> Self {
        Self::with_window(source, TASK_GRAPH_MEASURE_WINDOW)
    }

    pub fn with_window(source: Arc<crate::DagExecutor>, window: std::time::Duration) -> Self {
        Self {
            source,
            snapshot: tokio::sync::Mutex::new(TaskGraphSnapshot::default()),
            measured_at: tokio::sync::Mutex::new(None),
            window,
        }
    }

    /// Read the graph unless a read inside the window already answered.
    ///
    /// A read that failed does not open the window: the next scrape asks again
    /// and the failure counter is what climbs. Suppressing the retry would make
    /// a store refusing reads look like a quiet graph for the length of the
    /// window, which is the one thing this observable must not do.
    async fn refresh_if_due(&self) {
        let mut measured_at = self.measured_at.lock().await;
        if measured_at.is_some_and(|at| at.elapsed() < self.window) {
            return;
        }
        if self.refresh().await {
            *measured_at = Some(std::time::Instant::now());
        }
    }

    /// Read the graph and record what it holds. Answers whether the read
    /// succeeded, because that is what decides if the window opens.
    ///
    /// A failed read keeps the previous figures and does not advance
    /// `last_measure_seconds`: the numbers stay readable as "the last thing we
    /// knew", while the staleness and failure series say how old that is. The
    /// lock is taken only after the read returns, so a slow store cannot hold
    /// up the other observables' scrapes.
    async fn refresh(&self) -> bool {
        let read = self.source.all_tasks_checked().await;
        self.record(read).await
    }

    /// Fold one read into the snapshot.
    ///
    /// Split from [`Self::refresh`] so the failure branch can be exercised
    /// without a store that refuses to read: the distinction these tests pin —
    /// "nothing to report" versus "could not ask" — is the whole reason the
    /// read keeps its error.
    ///
    /// Answers whether the read succeeded, which is the one thing the caller
    /// needs to decide if the window opens: a failed read that opened it would
    /// keep the next scrape from asking again.
    async fn record(&self, read: SFResult<Vec<cog_core::Task>>) -> bool {
        let mut snapshot = self.snapshot.lock().await;
        match read {
            Ok(tasks) => {
                let now = chrono::Utc::now();
                let mut counts: Vec<(&'static str, u64)> = cog_core::TaskStatus::ALL
                    .iter()
                    .map(|status| (status.as_str(), 0))
                    .collect();
                let mut oldest: HashMap<String, f64> = HashMap::new();
                // The states that can still move, named once: the age below and
                // the unowned count both ask about exactly this set, and a
                // second copy of it would be free to drift from this one.
                let movable = [
                    cog_core::TaskStatus::Pending,
                    cog_core::TaskStatus::Scheduled,
                    cog_core::TaskStatus::Running,
                ];
                let mut unowned: Vec<(&'static str, u64)> =
                    movable.iter().map(|s| (s.as_str(), 0)).collect();
                let mut held: HashMap<String, u64> = HashMap::new();
                for task in &tasks {
                    if let Some(cell) = counts
                        .iter_mut()
                        .find(|(status, _)| *status == task.status.as_str())
                    {
                        cell.1 += 1;
                    }
                    // Counted before the `movable` filter below and narrowed to
                    // Pending by hand, because the two are not the same set: the
                    // dispatcher only ever starts a pending row, so a scheduled
                    // or running one that states no criterion is not being held
                    // by this rule and counting it would grow a backlog figure
                    // from rows nobody refused.
                    if task.status == cog_core::TaskStatus::Pending
                        && crate::ready_queue::held_for_want_of_a_criterion(task)
                    {
                        *held.entry(task.task_type.kind_key()).or_default() += 1;
                    }
                    // Only the states that can still move count as owed work.
                    // A finished or cancelled row will not be touched again, so
                    // its age is history rather than a stall.
                    if !movable.contains(&task.status) {
                        continue;
                    }
                    // 一条**执行者永远不会开走**的行不是「欠着的活」。读数的名字说的是
                    // 「还有多少活没人接手，最老的一条等了多久」，而 `is_executable=false`
                    // 的行按定义不在任何执行者的队列里：读数登记行（`Custom("reading")`，
                    // 那一族是被读的清单，不是要跑的活）在 `pending` 上停十天，与「积压
                    // 没人接」在这个面上逐字同形——2026-11-01 起一直亮的
                    // `unowned_task_untouched_past_pickup` 就是被 75 条这样的登记行点亮的。
                    //
                    // 面节点是同一族里最容易被漏掉的一支：它不可执行，但它带着
                    // `goal_id`——面节点是自己那个项目的根——所以只按「有没有目标」
                    // 分不出来。它的寿命是无限的：面一直在那儿，没有任何东西会把它写回
                    // 终态。2026-11-03 实测：最老的一条「没人接的活」是 10-24 建的
                    // `写操作窗口`，两条工作区各一份（roster 里那份，加上发布到执行
                    // 工作区、和它的职责行放在一起的那份），都已经站了十天。
                    // 按类型认，不按工作区认：发布出去的那一份按设计躺在执行工作区里，
                    // 它照样不是活。
                    //
                    // 目标承载行留在这组里：它同样不可执行，但它会被收口任务写回终态，
                    // 所以「它一动不动」是真的异常，不是一条站着不动的声明。
                    let is_face_row = matches!(&task.task_type, cog_core::TaskType::Custom(kind)
                            if kind == cog_core::FACE_TASK_TYPE);
                    if !task.is_executable && (task.goal_id.is_none() || is_face_row) {
                        continue;
                    }
                    let agent = task.agent_id.clone().filter(|id| !id.is_empty());
                    // Counted before the age, because an unowned row is exactly
                    // the one whose cell in the age reading is the shared
                    // `unassigned` bucket: that bucket can only say how long the
                    // oldest one waited, never how many rows are in it.
                    if agent.is_none() {
                        if let Some(cell) = unowned
                            .iter_mut()
                            .find(|(status, _)| *status == task.status.as_str())
                        {
                            cell.1 += 1;
                        }
                    }
                    let age_ms = (now - task.updated_at).num_milliseconds().max(0);
                    let age = age_ms as f64 / 1000.0;
                    let agent = agent.unwrap_or_else(|| UNASSIGNED_AGENT.to_string());
                    oldest
                        .entry(agent)
                        .and_modify(|held| *held = held.max(age))
                        .or_insert(age);
                }
                snapshot.held_unstated_total = held.values().sum();
                snapshot.held_unstated_by_kind = held.into_iter().collect();
                snapshot.counts = Some(counts);
                snapshot.oldest_by_agent = oldest.into_iter().collect();
                snapshot.unowned_by_status = unowned;
                snapshot.last_measure_seconds = now.timestamp().max(0) as u64;
                true
            }
            Err(e) => {
                snapshot.measure_failures = snapshot.measure_failures.saturating_add(1);
                tracing::warn!("task graph measurement failed: {e}");
                false
            }
        }
    }
}

#[async_trait]
impl Observable for TaskGraphObservable {
    /// The dimension is ignored for the same reason as the stream-pending
    /// gauges: none of this varies by dimension, and declaring none is what
    /// gets it pulled once per scrape instead of once per dimension.
    ///
    /// The read is shared between scrapes rather than repeated for each: see
    /// [`TASK_GRAPH_MEASURE_WINDOW`]. A collector arriving inside the window
    /// gets the reading that is already there, stamped with the moment it was
    /// measured, so the freshness rule answers the same question it always did.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        self.refresh_if_due().await;
        Ok(self.readings().await)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

impl TaskGraphObservable {
    /// The published shape of the last recorded read, success or failure.
    async fn readings(&self) -> Vec<RawMetric> {
        let snapshot = self.snapshot.lock().await;
        let mut out = Vec::new();
        // A status at zero and a status that has never been mentioned are
        // different readings: the first says the graph is clean on that state,
        // the second says nobody has looked. That is why the counts are only
        // published once a read has succeeded, and then all of them at once.
        if let Some(counts) = &snapshot.counts {
            for (status, count) in counts {
                out.push(
                    RawMetric::new(TASKS_CURRENT_METRIC, *count as f64)
                        .with_label(crate::LABEL_STATUS, *status),
                );
            }
            let rows: f64 = counts.iter().map(|(_, count)| *count as f64).sum();
            out.push(
                RawMetric::new(TASKS_READINGS_METRIC, rows)
                    .with_label(crate::LABEL_READOUT, TASKS_ROWS_READOUT),
            );
            for (agent, age) in &snapshot.oldest_by_agent {
                out.push(
                    RawMetric::new(TASK_OLDEST_UNTOUCHED_METRIC, *age)
                        .with_label(crate::LABEL_AGENT, agent.as_str()),
                );
            }
            // The held rows: the rollup always (so a clean graph states its
            // zero), the per-kind cells only where there is something to
            // attribute. A reader that wants to know *which* producer is
            // starving reads the labelled cells; a rule that wants to know
            // whether anything is reads the rollup and needs no list of kinds.
            out.push(
                RawMetric::new(TASKS_READINGS_METRIC, snapshot.held_unstated_total as f64)
                    .with_label(crate::LABEL_READOUT, HELD_UNSTATED_READOUT),
            );
            for (kind, count) in &snapshot.held_unstated_by_kind {
                if *count > 0 {
                    out.push(
                        RawMetric::new(TASK_HELD_UNSTATED_METRIC, *count as f64)
                            .with_label(crate::LABEL_KIND, kind.as_str()),
                    );
                }
            }
            // Published per movable state even at zero: "no unowned work" and
            // "nobody counted" have to be different readings, and an absent
            // series is the second one.
            for (status, count) in &snapshot.unowned_by_status {
                out.push(
                    RawMetric::new(TASK_UNOWNED_ROWS_METRIC, *count as f64)
                        .with_label(crate::LABEL_STATUS, *status),
                );
            }
            out.push(RawMetric::new(
                TASK_GRAPH_MEASURE_LAST_METRIC,
                snapshot.last_measure_seconds as f64,
            ));
        }
        if snapshot.measure_failures > 0 {
            out.push(RawMetric::new(
                TASK_GRAPH_MEASURE_FAILURES_METRIC,
                snapshot.measure_failures as f64,
            ));
        }
        out
    }
}

/// Orchestrator-level observable state.
///
/// It publishes the orchestrator's own task outcomes and nothing else. Message
/// and crew-round counts are deliberately not among them: both facts are
/// already metered where they happen, in the collaboration layer's D8 series
/// (`collab_message_count`, `collab_round_count`), whose recorders have live
/// callers. A second counter here for the same fact would be two numbers that
/// must agree and can drift. The crew-round one was worse than redundant — it
/// was keyed per `crew_id` while this producer declares its dimensions bounded,
/// so wiring it would have made a bounded declaration false.
#[derive(Default)]
pub struct OrchestratorObservable {
    task_count: AtomicU64,
    task_success_count: AtomicU64,
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
}

#[async_trait]
impl Observable for OrchestratorObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D1" {
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
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// 这里的 D1 与 agent 面的 D1 不是同一件事：本面记的是编排级累计计数
    /// （任务数、成功数、比率），键固定、基数有界；agent 面在 D1 上是逐 step
    /// 按 task_id 记的，同一个维度名在这两处有不同的有界性。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        vec![DimensionSpec::bounded("D1")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkout::repo_root;
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
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(None)
        }

        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {
            // 这个夹具没有长轮询：它的流要么立刻结束，要么永久 pending，
            // 没有一条已发出的读需要等它返回，所以没有可停的东西。
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
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            Ok(None)
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

        /// This stub speaks for the pending reading only; a lag question is
        /// not one it was built to answer.
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
    }

    const STREAM: &str = "orchestrator:results:ws-obs-test";
    const GROUP: &str = "cgrp-obs-test";
    const CLAIM_IDLE_MS: u64 = 600_000;

    fn metric<'a>(metrics: &'a [RawMetric], name: &str) -> Option<&'a RawMetric> {
        metrics.iter().find(|m| m.name == name)
    }

    /// 同一族里按流取那一格：一条进程读几条流时，只按名字取到的是「碰巧排在第
    /// 一的那条」，用它去判另一条流等于是拿别人的数答自己的问题。
    fn metric_of(metrics: &[RawMetric], name: &str, stream: &str) -> f64 {
        metrics
            .iter()
            .find(|m| {
                m.name == name
                    && m.labels.get(crate::LABEL_STREAM).map(String::as_str) == Some(stream)
            })
            .unwrap_or_else(|| panic!("{name} for stream {stream} missing"))
            .value
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
            assert_eq!(
                m.labels.get(crate::LABEL_STREAM).map(String::as_str),
                Some(STREAM)
            );
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

    /// A backend that answers "no pending state to report" — the Redis answer
    /// for a consumer group that does not exist yet — is not a failed
    /// measurement. The failure counter feeds stream_pending_measure_failing,
    /// which must reflect only genuine backend read faults; and a
    /// nothing-to-report answer must not move the last-successful timestamp,
    /// or the staleness rule would read the absence of a fault as the absence
    /// of evidence.
    #[tokio::test]
    async fn a_backend_with_nothing_to_report_is_not_a_failure() {
        let observer = StreamPendingObservable::new();
        let ok = StubBackend(Reply::Stats(PendingStats {
            count: 1,
            unreclaimed_count: 0,
            unreclaimed_oldest_idle_ms: 0,
        }));
        observer
            .measure(&ok, STREAM, GROUP, CLAIM_IDLE_MS, 60)
            .await;
        let after_ok = observer.collect_metrics("D8").await.unwrap();
        let last_ok = metric(&after_ok, STREAM_PENDING_MEASURE_LAST_METRIC)
            .unwrap()
            .value;

        let none = StubBackend(Reply::Unobservable);
        observer
            .measure(&none, STREAM, GROUP, CLAIM_IDLE_MS, 60)
            .await;
        let after = observer.collect_metrics("D8").await.unwrap();

        assert_eq!(
            metric(&after, STREAM_PENDING_MEASURE_LAST_METRIC)
                .unwrap()
                .value,
            last_ok,
            "a nothing-to-report answer must not move the last-successful timestamp"
        );
        assert!(
            metric(&after, STREAM_PENDING_MEASURE_FAILURES_METRIC).is_none(),
            "a nothing-to-report answer must not increment the failure counter"
        );
    }

    /// A rename on either side of a gauge/rule pair leaves both halves
    /// internally consistent and the signal silently absent, so the deployed
    /// rules are pinned against the metric names this module publishes.
    #[test]
    fn deployed_rules_query_the_metrics_this_module_publishes() {
        let chart = repo_root().join("deploy/helm/cogneva/files/cogneva.json");
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
            (
                TASK_ROUTER_CLAIM_STALL_RULE,
                TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC,
            ),
            (
                TASK_ROUTER_CLAIM_STALL_RULE,
                TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC,
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

    /// The permit gauges exist only for streams this process declared, and each
    /// carries the stream: a bare number would leave the reader unable to say
    /// which read loop is starved.
    #[tokio::test]
    async fn claim_gauges_are_published_per_registered_stream() {
        let observer = Arc::new(ClaimSlotsObservable::new());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
        observer.register(STREAM, semaphore.clone(), 4, SOURCE_DERIVED);

        let metrics = observer.collect_metrics("D8").await.unwrap();
        // Not the hold bound: it is a reading of the budget, and until a stall
        // window has been measured the series is absent on purpose (the
        // assertions further down spell that out). Listing it here would ask
        // for a zero the rule would trip on.
        for name in [
            TASK_ROUTER_CLAIM_SLOTS_METRIC,
            TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC,
            TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC,
        ] {
            let m = metric(&metrics, name).unwrap_or_else(|| panic!("{name} missing"));
            assert_eq!(
                m.labels.get(crate::LABEL_STREAM).map(String::as_str),
                Some(STREAM)
            );
        }
        assert_eq!(
            metric(&metrics, TASK_ROUTER_CLAIM_SLOTS_METRIC)
                .unwrap()
                .value,
            4.0
        );
        // 界是导出的还是钉死的，必须由读数自己说出来：注册表里那条 `derived`
        // 行指的正是这个标签（`tunable_registry_contract_test.rs` 会来查）。
        assert_eq!(
            metric(&metrics, TASK_ROUTER_CLAIM_SLOTS_METRIC)
                .unwrap()
                .labels
                .get(CLAIM_SLOTS_SOURCE_LABEL)
                .map(String::as_str),
            Some(SOURCE_DERIVED),
            "claim_slots 没带 source 标签，读者分不出这个界是量出来的还是配的"
        );
        assert_eq!(
            metric(&metrics, TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC)
                .unwrap()
                .value,
            0.0
        );

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let hold = observer.permit_taken(STREAM);
        let held = observer.collect_metrics("D8").await.unwrap();
        assert_eq!(
            metric(&held, TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC)
                .unwrap()
                .value,
            1.0
        );
        // Nothing stated a bound yet, so the series is absent rather than zero:
        // "not measured" and "measured at zero" must not be the same reading —
        // a zero would trip the stall rule for every waiting loop.
        assert!(
            metric(&held, TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC).is_none(),
            "没有量出窗口时不许发一个 0"
        );

        hold.note_stall_window(300);
        let bounded = observer.collect_metrics("D8").await.unwrap();
        assert_eq!(
            metric(&bounded, TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC)
                .unwrap()
                .value,
            300.0
        );

        // Releasing the guard is what the processing task does by dropping the
        // permit, and it must be visible without a second call from the caller.
        drop(hold);
        drop(permit);
        let released = observer.collect_metrics("D8").await.unwrap();
        assert_eq!(
            metric(&released, TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC)
                .unwrap()
                .value,
            0.0
        );
        // 上界随许可一起走：没有许可在跑，就没有谁在说自己的超时，序列回到
        // 「没有读数」，而不是 0——0 会被读成一堵零秒的墙。
        assert!(
            metric(&released, TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC).is_none(),
            "许可放回之后不许留下一个 0 的上界"
        );
    }

    /// The wait is a state, not a history: once the permit arrives the reading
    /// must go back to zero, or the rule would keep firing on a loop that is
    /// reading again.
    /// The state this pair exists for: the budget is spent, the loop is waiting,
    /// and another message is in hand. Everything about this process looks
    /// healthy from the stream's own readings — the bus answered, no read was
    /// refused — so only these gauges can say the queue stopped being consumed
    /// because of this process's own concurrency budget.
    #[tokio::test]
    async fn an_exhausted_budget_and_a_waiting_loop_are_readable() {
        let observer = Arc::new(ClaimSlotsObservable::new());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        observer.register(STREAM, semaphore.clone(), 1, SOURCE_OVERRIDE);

        // The one permit is running a task; the next message arrives and the
        // loop blocks on the semaphore.
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let hold = observer.permit_taken(STREAM);
        hold.note_stall_window(600);
        observer.wait_started(STREAM);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let waiting = observer.collect_metrics("D8").await.unwrap();
        assert_eq!(
            metric(&waiting, TASK_ROUTER_CLAIM_SLOTS_METRIC)
                .unwrap()
                .value,
            1.0
        );
        assert_eq!(
            metric(&waiting, TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC)
                .unwrap()
                .value,
            1.0
        );
        assert!(
            metric(&waiting, TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC)
                .unwrap()
                .value
                > 0.0
        );
        assert_eq!(
            metric(&waiting, TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC)
                .unwrap()
                .value,
            600.0
        );

        drop(hold);
        drop(permit);
        observer.wait_ended(STREAM);

        let ended = observer.collect_metrics("D8").await.unwrap();
        assert_eq!(
            metric(&ended, TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC)
                .unwrap()
                .value,
            0.0
        );
        assert_eq!(
            metric(&ended, TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC)
                .unwrap()
                .value,
            0.0
        );
    }

    /// A stream waiting on the shared budget must state the bound of the work
    /// that is holding it, not the bound of its own empty set.
    ///
    /// 2026-11-06 实测：`:local` 上两条 3600 秒的任务占满 6/6，`default` 一条
    /// 都没持有却在等 1171 秒；上界按流算出来是 0，于是 `wait > 0` 恒成立，
    /// `task_router_claim_stall` 在一条正常背压的预算上天天亮，亮出来的单再各
    /// 占一格，把同一条误判做得更响。
    ///
    /// 另一份预算留在旁边：按预算算不能退化成「按进程算」，否则一条自己不
    /// 紧张的流会被另一条毫无关系的流的时限替它说话。
    #[tokio::test]
    async fn a_stream_waiting_on_a_budget_states_the_bound_of_the_work_holding_it() {
        let observer = Arc::new(ClaimSlotsObservable::new());
        let budget = Arc::new(tokio::sync::Semaphore::new(1));
        let elsewhere = Arc::new(tokio::sync::Semaphore::new(1));
        observer.register(STREAM, budget.clone(), 1, SOURCE_OVERRIDE);
        observer.register(
            "waiting-on-the-same-budget",
            budget.clone(),
            1,
            SOURCE_OVERRIDE,
        );
        observer.register("another-budget", elsewhere.clone(), 1, SOURCE_OVERRIDE);

        let permit = budget.clone().acquire_owned().await.unwrap();
        let running = observer.permit_taken(STREAM);
        running.note_stall_window(3600);
        let other_permit = elsewhere.clone().acquire_owned().await.unwrap();
        let other = observer.permit_taken("another-budget");
        other.note_stall_window(60);
        observer.wait_started("waiting-on-the-same-budget");

        let read = observer.collect_metrics("D8").await.unwrap();
        assert_eq!(
            metric_of(
                &read,
                TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC,
                "waiting-on-the-same-budget"
            ),
            3600.0,
            "the stream that holds nothing is waiting for the work that holds the budget"
        );
        assert_eq!(
            metric_of(&read, TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC, "another-budget"),
            1.0,
            "each budget counts its own occupancy"
        );
        assert_eq!(
            metric_of(
                &read,
                TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC,
                "another-budget"
            ),
            60.0,
            "another budget's holds are not this one's wall"
        );

        drop(running);
        drop(permit);
        drop(other);
        drop(other_permit);
        observer.wait_ended("waiting-on-the-same-budget");
    }

    /// A stream nobody registered publishes nothing at all: an absent series is
    /// the truth about "this process does not read that queue", while a zero
    /// would claim it reads it and is never blocked.
    #[tokio::test]
    async fn an_unregistered_stream_publishes_nothing() {
        // 判的是「没有这个流就没有这个流的序列」，不是「这个 Observable 一条
        // 也不发」：停摆判据的两条证据是进程的读数，与读了哪条流无关，空闲时
        // 也该有——它们的缺席在读数面上读作「判据没接上」。
        let observer = ClaimSlotsObservable::new();
        let metrics = observer.collect_metrics("D8").await.unwrap();
        assert!(
            metrics
                .iter()
                .all(|m| !m.labels.contains_key(crate::LABEL_STREAM)),
            "没有登记过的流不该发出任何带 stream 标签的序列: {metrics:?}"
        );
    }

    /// The orchestrator's published set is pinned to the outcomes it itself
    /// produces.
    ///
    /// A message count and a per-crew round count used to be published here
    /// with no writer at all — and the round one was keyed per `crew_id`
    /// against a dimension this producer declares bounded, so wiring it would
    /// have made that declaration false. Both facts are metered where they
    /// happen, in the collaboration layer's D8 series, so this fails if either
    /// name comes back here without a caller behind it.
    #[tokio::test]
    async fn d1_publishes_only_the_orchestrators_own_task_outcomes() {
        let observable = OrchestratorObservable::new();
        observable.record_task(true);
        observable.record_task(false);

        let names: Vec<String> = observable
            .collect_metrics("D1")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.name)
            .collect();
        assert_eq!(
            names,
            vec![
                "orch_task_count",
                "orch_task_success_count",
                "orch_task_success_rate",
            ],
            "two tasks with one success publish the counts and the rate"
        );

        assert!(
            observable.collect_metrics("D8").await.unwrap().is_empty(),
            "nothing is published under D8 any more"
        );
        assert_eq!(
            observable.available_dimensions().len(),
            1,
            "the orchestrator declares a single dimension"
        );
    }

    fn task(id: &str, status: cog_core::TaskStatus) -> cog_core::Task {
        let mut task = cog_core::Task::new(
            id,
            cog_core::TaskType::Custom("task-ledger-observable".into()),
            serde_json::json!({}),
        );
        task.status = status;
        task
    }

    fn labeled<'a>(
        metrics: &'a [RawMetric],
        name: &str,
        label: &str,
        value: &str,
    ) -> &'a RawMetric {
        metrics
            .iter()
            .find(|m| m.name == name && m.labels.get(label).map(String::as_str) == Some(value))
            .unwrap_or_else(|| panic!("{name}{{{label}={value}}} missing"))
    }

    /// The reading a stalled executor shows up in: every status is published,
    /// including the clean ones as zeros, and the oldest untouched age is
    /// grouped by the agent that owes the work — with the terminal states left
    /// out, since those will not move again.
    #[tokio::test]
    async fn the_task_graph_reads_count_by_status_and_age_by_agent() {
        let dag = Arc::new(crate::DagExecutor::new("ws-task-obs".into()));
        let now = chrono::Utc::now();

        let mut running = task("t-running", cog_core::TaskStatus::Running);
        running.agent_id = Some("agent-a".into());
        running.updated_at = now - chrono::Duration::seconds(300);
        let mut pending = task("t-pending", cog_core::TaskStatus::Pending);
        pending.agent_id = Some("agent-a".into());
        pending.updated_at = now - chrono::Duration::seconds(900);
        let mut done = task("t-done", cog_core::TaskStatus::Completed);
        done.agent_id = Some("agent-a".into());
        done.updated_at = now - chrono::Duration::seconds(3600);
        let mut unassigned = task("t-unassigned", cog_core::TaskStatus::Pending);
        unassigned.updated_at = now - chrono::Duration::seconds(600);
        for row in [running, pending, done, unassigned] {
            dag.add_task(row).await.unwrap();
        }

        let observable = TaskGraphObservable::new(dag);
        let metrics = observable.collect_metrics("").await.unwrap();

        assert_eq!(
            labeled(&metrics, TASKS_CURRENT_METRIC, "status", "pending").value,
            2.0
        );
        assert_eq!(
            labeled(&metrics, TASKS_CURRENT_METRIC, "status", "running").value,
            1.0
        );
        assert_eq!(
            labeled(&metrics, TASKS_CURRENT_METRIC, "status", "completed").value,
            1.0
        );
        // A status nothing holds is a zero, not an absent series: the two
        // readings differ in whether anyone looked at all.
        for clean in ["scheduled", "failed", "cancelled"] {
            assert_eq!(
                labeled(&metrics, TASKS_CURRENT_METRIC, "status", clean).value,
                0.0
            );
        }

        // The named readout is the whole graph, and it is the sum of the cells
        // above rather than a count taken somewhere else: a reader that asks
        // for "how many rows" must not have to re-derive it from the vector.
        assert_eq!(
            labeled(
                &metrics,
                TASKS_READINGS_METRIC,
                crate::LABEL_READOUT,
                TASKS_ROWS_READOUT
            )
            .value,
            4.0
        );

        // The oldest of agent-a's unfinished rows is the Pending one at 900s;
        // the Completed row is an hour old and must not win.
        let agent_a = labeled(&metrics, TASK_OLDEST_UNTOUCHED_METRIC, "agent", "agent-a").value;
        assert!(
            (895.0..=905.0).contains(&agent_a),
            "agent-a's oldest untouched task is the 900s-old Pending row, got {agent_a}"
        );
        // Work nobody was assigned is where "no agent is stuck and the work
        // still is not moving" shows up, so it gets its own cell.
        let nobody = labeled(
            &metrics,
            TASK_OLDEST_UNTOUCHED_METRIC,
            "agent",
            UNASSIGNED_AGENT,
        )
        .value;
        assert!(
            (595.0..=605.0).contains(&nobody),
            "the unassigned Pending row is 600s old, got {nobody}"
        );
        // The age above says how long the oldest unowned row waited, not how
        // many are waiting: one row and a thousand read alike there. The count
        // is the other half, and only the movable states get a cell -- a
        // completed row with no agent is finished, not unowned.
        assert_eq!(
            labeled(&metrics, TASK_UNOWNED_ROWS_METRIC, "status", "pending").value,
            1.0,
            "one Pending row has no agent"
        );
        for idle in ["scheduled", "running"] {
            assert_eq!(
                labeled(&metrics, TASK_UNOWNED_ROWS_METRIC, "status", idle).value,
                0.0,
                "{idle} rows are all owned, but the cell must still be there"
            );
        }
        assert!(
            metrics.iter().all(|m| m.name != TASK_UNOWNED_ROWS_METRIC
                || matches!(
                    m.labels.get(crate::LABEL_STATUS).map(String::as_str),
                    Some("pending" | "scheduled" | "running")
                )),
            "terminal states have no unowned bucket: those rows will not be picked up"
        );
        assert!(
            metric(&metrics, TASK_GRAPH_MEASURE_LAST_METRIC)
                .unwrap()
                .value
                > 0.0,
            "a measured graph must carry the timestamp its staleness is judged against"
        );
        assert!(metric(&metrics, TASK_GRAPH_MEASURE_FAILURES_METRIC).is_none());
    }

    /// 一条执行者永远开不走的行，不是「欠着的活」。
    ///
    /// 两个读数的名字都在说「还有多少活没人接手、最老的一条等了多久」。而
    /// `is_executable=false` 的行按定义不在任何执行者的队列里——读数登记行
    /// （`Custom("reading")`）是被读的清单，不是要跑的活，它在 `pending` 上停十天，
    /// 与「积压没人接」在这一面上逐字同形：2026-11-01 起一直亮的
    /// `unowned_task_untouched_past_pickup` 就是被 75 条这样的登记行点亮的，而它
    /// 点名的「最老的一条」自 10-29 起一次都没动过——因为它本来就不会动。
    ///
    /// 目标承载行留在这组里：它同样不可执行，但它会被收口任务写回终态，「一动不动」
    /// 对它才是异常。这条判据的两半都要，缺了后半句，一个永远等不到答案的目标提交
    /// 会从这张面上消失。
    #[tokio::test]
    async fn a_row_no_executor_can_start_is_not_owed_work() {
        let dag = Arc::new(crate::DagExecutor::new("ws-not-executable".into()));
        let now = chrono::Utc::now();

        // 登记行：不可执行，停在 pending 十天。
        let mut roster = task("reading:a2a_tck", cog_core::TaskStatus::Pending);
        roster.is_executable = false;
        roster.updated_at = now - chrono::Duration::days(10);
        // 真的活：可执行，没人领，等了两分钟。
        let mut real = task("t-real-work", cog_core::TaskStatus::Pending);
        real.updated_at = now - chrono::Duration::seconds(120);
        // 目标承载行：不可执行，但它会被写回，等十分钟是异常。
        let mut goal = cog_core::Task::goal_row("t-goal", "把这一版滚上去");
        goal.status = cog_core::TaskStatus::Pending;
        goal.updated_at = now - chrono::Duration::seconds(600);

        for row in [roster, real, goal] {
            dag.add_task(row).await.unwrap();
        }

        let observable = TaskGraphObservable::new(dag);
        let metrics = observable.collect_metrics("").await.unwrap();

        let oldest = labeled(
            &metrics,
            TASK_OLDEST_UNTOUCHED_METRIC,
            "agent",
            UNASSIGNED_AGENT,
        )
        .value;
        assert!(
            (595.0..=605.0).contains(&oldest),
            "最老的一条该是那条目标承载行（600 秒），不是停着不动的登记行（十天）：{oldest}"
        );
        assert_eq!(
            labeled(&metrics, TASK_UNOWNED_ROWS_METRIC, "status", "pending").value,
            2.0,
            "欠着的活是两条：真的活与目标承载行；登记行不是活"
        );
        // 总数那一格仍然数整张图：登记行是图里的一行，只是不欠谁。
        assert_eq!(
            labeled(
                &metrics,
                TASKS_READINGS_METRIC,
                crate::LABEL_READOUT,
                TASKS_ROWS_READOUT
            )
            .value,
            3.0
        );
    }

    /// 面节点不是欠着的活，哪怕它带着 `goal_id`。
    ///
    /// 面节点是自己那个项目的根，所以「不可执行」这一条分不出它和一条真的目标
    /// 承载行；而它没有终态——面一直在那儿，谁也不会把它写回完成。2026-11-03
    /// 实测就是这么亮的：`unowned_task_untouched_past_pickup` 点名的最老一行是
    /// 10-24 建的 `写操作窗口`，两份（roster 里的定义，以及发布到执行工作区、
    /// 和它的职责行放在一起的那一份），站着不动是它们的常态。
    ///
    /// 这条测试同时钉住「按类型认」：发布出去的那一份住在执行工作区里，用工作区
    /// 认面会漏掉它，读数就还是会被一条永远不会动的行点着。
    #[tokio::test]
    async fn a_face_node_is_not_owed_work_even_though_it_names_a_goal() {
        let dag = Arc::new(crate::DagExecutor::new("ws-faces".into()));
        let now = chrono::Utc::now();

        // roster 里那份面节点。
        let mut roster = task("写操作窗口", cog_core::TaskStatus::Pending);
        roster.task_type = cog_core::TaskType::Custom(cog_core::FACE_TASK_TYPE.into());
        roster.workspace_id = Some(cog_core::GOVERNANCE_WORKSPACE.to_string());
        roster.goal_id = Some("写操作窗口".into());
        roster.is_executable = false;
        roster.updated_at = now - chrono::Duration::days(10);
        // 发布到执行工作区的那一份：同一张面，住在职责行旁边。图里按 id 存，
        // 所以这里换一个 id 让两份同时在一张图上——生产里它们分在两个工作区，
        // 各自按 (工作区, id) 存。
        let mut published = task("通信", cog_core::TaskStatus::Pending);
        published.task_type = cog_core::TaskType::Custom(cog_core::FACE_TASK_TYPE.into());
        published.workspace_id = Some("ws-faces".into());
        published.goal_id = Some("通信".into());
        published.is_executable = false;
        published.updated_at = now - chrono::Duration::days(9);
        // 真的活：可执行，没人领，等了两分钟。
        let mut real = task("t-real-work", cog_core::TaskStatus::Pending);
        real.updated_at = now - chrono::Duration::seconds(120);

        for row in [roster, published, real] {
            dag.add_task(row).await.unwrap();
        }

        let observable = TaskGraphObservable::new(dag);
        let metrics = observable.collect_metrics("").await.unwrap();

        let oldest = labeled(
            &metrics,
            TASK_OLDEST_UNTOUCHED_METRIC,
            "agent",
            UNASSIGNED_AGENT,
        )
        .value;
        assert!(
            (115.0..=125.0).contains(&oldest),
            "最老的一条该是那条真的活（两分钟），不是站着不动的面节点（十天）：{oldest}"
        );
        assert_eq!(
            labeled(&metrics, TASK_UNOWNED_ROWS_METRIC, "status", "pending").value,
            1.0,
            "欠着的活只有那一条真的活；两份面节点都不是活"
        );
        // 总数那一格仍然数整张图：面节点是图里的行，只是不欠谁。
        assert_eq!(
            labeled(
                &metrics,
                TASKS_READINGS_METRIC,
                crate::LABEL_READOUT,
                TASKS_ROWS_READOUT
            )
            .value,
            3.0
        );
    }

    /// Rows the dispatcher refuses for want of a criterion are on the graph and
    /// say so.
    ///
    /// The refusal itself is the safe half -- the row stays pending instead of
    /// running to a completion nobody can check against anything -- but a
    /// refusal nobody can read is an outage of the queue that looks exactly like
    /// a quiet one. The log line names the kind once per process; this is the
    /// cell that says whether it is still true.
    #[tokio::test]
    async fn the_graph_says_how_much_work_is_held_for_want_of_a_criterion() {
        let dag = Arc::new(crate::DagExecutor::new("ws-held-unstated".into()));

        let mut closable = task("t-closable", cog_core::TaskStatus::Pending);
        closable.acceptance.push("门测试跑绿".into());

        let mut bare = task("t-bare", cog_core::TaskStatus::Pending);
        bare.task_type = cog_core::TaskType::Custom("change_rework".into());
        let mut second = task("t-bare-2", cog_core::TaskStatus::Pending);
        second.task_type = cog_core::TaskType::Custom("change_rework".into());

        // Not this rule's population, each for a reason of its own: a running
        // row was already started, a face duty is left alone by design, and a
        // goal row has no body to run.
        let running = task("t-running-bare", cog_core::TaskStatus::Running);
        let mut duty = task("t-duty", cog_core::TaskStatus::Pending);
        duty.task_type =
            cog_core::TaskType::Custom(crate::governance_acts::FACE_DUTY_TASK_TYPE.into());
        let goal = cog_core::Task::goal_row("goal-held", "ship it");

        for row in [closable, bare, second, running, duty, goal] {
            dag.add_task(row).await.unwrap();
        }

        let metrics = TaskGraphObservable::new(dag)
            .collect_metrics("")
            .await
            .unwrap();

        assert_eq!(
            labeled(
                &metrics,
                TASKS_READINGS_METRIC,
                crate::LABEL_READOUT,
                HELD_UNSTATED_READOUT
            )
            .value,
            2.0,
            "the two bare change_rework rows are the ones being held"
        );
        assert_eq!(
            labeled(
                &metrics,
                TASK_HELD_UNSTATED_METRIC,
                crate::LABEL_KIND,
                "change_rework"
            )
            .value,
            2.0,
            "the cell has to name the kind: the fix is a producer, and the kind is its name"
        );
        assert_eq!(
            metrics
                .iter()
                .filter(|m| m.name == TASK_HELD_UNSTATED_METRIC)
                .count(),
            1,
            "no zero cells for kinds that hold nothing: the rollup already states the zero"
        );
    }

    /// 图上一件也没被留住时，读数说 0 而不是缺席。
    ///
    /// 缺席与 0 在这里是两个意思：0 是「派发器没有拦下任何东西」，缺席是「这一版
    /// 根本没量」。按缺席写的规则会在第一次部署、图还是空的时候报警。
    #[tokio::test]
    async fn a_graph_that_holds_nothing_states_its_zero() {
        let dag = Arc::new(crate::DagExecutor::new("ws-held-none".into()));
        let mut closable = task("t-closable", cog_core::TaskStatus::Pending);
        closable.acceptance.push("门测试跑绿".into());
        dag.add_task(closable).await.unwrap();

        let metrics = TaskGraphObservable::new(dag)
            .collect_metrics("")
            .await
            .unwrap();

        assert_eq!(
            labeled(
                &metrics,
                TASKS_READINGS_METRIC,
                crate::LABEL_READOUT,
                HELD_UNSTATED_READOUT
            )
            .value,
            0.0
        );
        assert!(
            metrics.iter().all(|m| m.name != TASK_HELD_UNSTATED_METRIC),
            "a kind with nothing held gets no cell"
        );
    }

    /// A failed read must not present the last known shape as fresh and must
    /// not erase it either: the age and the failure count are what tell the
    /// reader how much to trust it.
    #[tokio::test]
    async fn a_failed_task_graph_read_keeps_the_last_shape_and_counts_itself() {
        let dag = Arc::new(crate::DagExecutor::new("ws-task-obs-fail".into()));
        dag.add_task(task("t-1", cog_core::TaskStatus::Pending))
            .await
            .unwrap();
        let observable = TaskGraphObservable::new(dag);

        let first = observable.collect_metrics("").await.unwrap();
        let last_ok = metric(&first, TASK_GRAPH_MEASURE_LAST_METRIC)
            .unwrap()
            .value;

        assert!(
            !observable
                .record(Err(cog_core::SFError::Redis("stub failure".into())))
                .await,
            "a failed read answers no, and that is what keeps it from opening the \
             window: a failure that counted as a measurement would hold the next \
             scrape off for the length of it"
        );
        let after = observable.readings().await;

        assert_eq!(
            metric(&after, TASK_GRAPH_MEASURE_LAST_METRIC)
                .unwrap()
                .value,
            last_ok,
            "a failed read must not move the last-successful timestamp forward"
        );
        assert_eq!(
            labeled(&after, TASKS_CURRENT_METRIC, "status", "pending").value,
            1.0,
            "the last known shape must survive a failed read"
        );
        assert_eq!(
            metric(&after, TASK_GRAPH_MEASURE_FAILURES_METRIC)
                .unwrap()
                .value,
            1.0
        );
    }

    /// A graph nobody has read yet publishes no counts at all. Zeros there
    /// would be a claim ("nothing is pending") that nobody has earned; count
    /// the failure that kept us from earning it instead.
    #[tokio::test]
    async fn a_graph_nobody_read_yet_publishes_no_zeros() {
        let dag = Arc::new(crate::DagExecutor::new("ws-task-obs-unread".into()));
        let observable = TaskGraphObservable::new(dag);
        assert!(observable.readings().await.is_empty());

        observable
            .record(Err(cog_core::SFError::Redis("stub failure".into())))
            .await;
        let readings = observable.readings().await;
        assert_eq!(readings.len(), 1, "{readings:?}");
        assert_eq!(readings[0].name, TASK_GRAPH_MEASURE_FAILURES_METRIC);
    }

    /// A scrape is not a reason to read the graph again.
    ///
    /// `/metrics` is pulled by more than one collector — the monitoring stack
    /// on its interval, and the GitOps puller once per side of a canary — and
    /// on the live graph a read costs seconds (measured 2026-11-06: 4.5 s for
    /// 9,899 rows, and the two processes holding an orchestrator between them
    /// read it 22 times a minute). The second collector in a window is asking
    /// the same question the first one just answered, so the answer has to cost
    /// nothing.
    ///
    /// A row filed between the two scrapes is what makes the difference
    /// visible: a second read would see it, an answer served from the window
    /// cannot.
    #[tokio::test]
    async fn a_scrape_inside_the_window_does_not_read_the_graph_again() {
        let dag = Arc::new(crate::DagExecutor::new("ws-read-window".into()));
        dag.add_task(task("t-first", cog_core::TaskStatus::Pending))
            .await
            .unwrap();
        let observable =
            TaskGraphObservable::with_window(dag.clone(), std::time::Duration::from_millis(300));

        let first = observable.collect_metrics("").await.unwrap();
        assert_eq!(
            labeled(&first, TASKS_CURRENT_METRIC, "status", "pending").value,
            1.0
        );

        dag.add_task(task("t-second", cog_core::TaskStatus::Pending))
            .await
            .unwrap();
        let second = observable.collect_metrics("").await.unwrap();
        assert_eq!(
            labeled(&second, TASKS_CURRENT_METRIC, "status", "pending").value,
            1.0,
            "the scrape inside the window read the graph again: the whole reason \
             the window exists is that it answers from the read already taken"
        );

        // And the window is a window, not a cache that stops asking: the next
        // scrape after it has to see the row.
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        let third = observable.collect_metrics("").await.unwrap();
        assert_eq!(
            labeled(&third, TASKS_CURRENT_METRIC, "status", "pending").value,
            2.0,
            "the scrape after the window was still answered from the old read"
        );
    }
}
