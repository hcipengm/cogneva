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
    /// Record the execution contract of the task running on this permit. Called
    /// once the payload parsed into a task whose timeout is known; a permit held
    /// by a payload that never parsed carries no bound, because there is no
    /// work it could be running.
    pub fn note_timeout(&self, seconds: u64) {
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
    waiting_since: Option<std::time::Instant>,
    held: HashMap<u64, u64>,
}

impl ClaimStreamState {
    fn in_flight(&self) -> usize {
        self.slots
            .saturating_sub(self.semaphore.available_permits())
    }

    /// The longest timeout among the holds that have one. A hold with no parsed
    /// task yet contributes nothing: its payload may be a poison one that never
    /// reaches execution, and inventing a bound for it would let a wall be
    /// compared against a number nothing stated.
    fn hold_bound_seconds(&self) -> u64 {
        self.held.values().copied().max().unwrap_or(0)
    }
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
    ) {
        self.streams()
            .entry(stream.to_string())
            .or_insert_with(|| ClaimStreamState {
                semaphore,
                slots,
                waiting_since: None,
                held: HashMap::new(),
            });
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
        for (stream, state) in streams.iter() {
            // Only the streams that registered exist here, so a stream this
            // process does not read publishes nothing rather than a zero.
            for (name, value) in [
                (TASK_ROUTER_CLAIM_SLOTS_METRIC, state.slots as f64),
                (TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC, state.in_flight() as f64),
                (
                    TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC,
                    state
                        .waiting_since
                        .map(|since| since.elapsed().as_secs_f64())
                        .unwrap_or(0.0),
                ),
                (
                    TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC,
                    state.hold_bound_seconds() as f64,
                ),
            ] {
                out.push(
                    RawMetric::new(name, value).with_label(crate::LABEL_STREAM, stream.as_str()),
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
    last_measure_seconds: u64,
    measure_failures: u64,
}

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
}

impl TaskGraphObservable {
    pub fn new(source: Arc<crate::DagExecutor>) -> Self {
        Self {
            source,
            snapshot: tokio::sync::Mutex::new(TaskGraphSnapshot::default()),
        }
    }

    /// Read the graph and record what it holds.
    ///
    /// A failed read keeps the previous figures and does not advance
    /// `last_measure_seconds`: the numbers stay readable as "the last thing we
    /// knew", while the staleness and failure series say how old that is. The
    /// lock is taken only after the read returns, so a slow store cannot hold
    /// up the other observables' scrapes.
    async fn refresh(&self) {
        let read = self.source.all_tasks_checked().await;
        self.record(read).await;
    }

    /// Fold one read into the snapshot.
    ///
    /// Split from [`Self::refresh`] so the failure branch can be exercised
    /// without a store that refuses to read: the distinction these tests pin —
    /// "nothing to report" versus "could not ask" — is the whole reason the
    /// read keeps its error.
    async fn record(&self, read: SFResult<Vec<cog_core::Task>>) {
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
                for task in &tasks {
                    if let Some(cell) = counts
                        .iter_mut()
                        .find(|(status, _)| *status == task.status.as_str())
                    {
                        cell.1 += 1;
                    }
                    // Only the states that can still move count as owed work.
                    // A finished or cancelled row will not be touched again, so
                    // its age is history rather than a stall.
                    if !movable.contains(&task.status) {
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
                snapshot.counts = Some(counts);
                snapshot.oldest_by_agent = oldest.into_iter().collect();
                snapshot.unowned_by_status = unowned;
                snapshot.last_measure_seconds = now.timestamp().max(0) as u64;
            }
            Err(e) => {
                snapshot.measure_failures = snapshot.measure_failures.saturating_add(1);
                tracing::warn!("task graph measurement failed: {e}");
            }
        }
    }
}

#[async_trait]
impl Observable for TaskGraphObservable {
    /// The dimension is ignored for the same reason as the stream-pending
    /// gauges: none of this varies by dimension, and declaring none is what
    /// gets it pulled once per scrape instead of once per dimension.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        self.refresh().await;
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
        observer.register(STREAM, semaphore.clone(), 4);

        let metrics = observer.collect_metrics("D8").await.unwrap();
        for name in [
            TASK_ROUTER_CLAIM_SLOTS_METRIC,
            TASK_ROUTER_CLAIM_IN_FLIGHT_METRIC,
            TASK_ROUTER_CLAIM_WAIT_SECONDS_METRIC,
            TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC,
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
        // Nothing parsed into a task yet, so no execution bound is stated. Zero
        // here is "nothing to compare against", which is what keeps the rule
        // from reading an invented wall.
        assert_eq!(
            metric(&held, TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC)
                .unwrap()
                .value,
            0.0
        );

        hold.note_timeout(300);
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
        assert_eq!(
            metric(&released, TASK_ROUTER_CLAIM_HOLD_BOUND_SECONDS_METRIC)
                .unwrap()
                .value,
            0.0
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
        observer.register(STREAM, semaphore.clone(), 1);

        // The one permit is running a task; the next message arrives and the
        // loop blocks on the semaphore.
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let hold = observer.permit_taken(STREAM);
        hold.note_timeout(600);
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

    /// A stream nobody registered publishes nothing at all: an absent series is
    /// the truth about "this process does not read that queue", while a zero
    /// would claim it reads it and is never blocked.
    #[tokio::test]
    async fn an_unregistered_stream_publishes_nothing() {
        let observer = ClaimSlotsObservable::new();
        assert!(observer.collect_metrics("D8").await.unwrap().is_empty());
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

        observable
            .record(Err(cog_core::SFError::Redis("stub failure".into())))
            .await;
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
}
