//! Which background loops this process started, and whether they are still
//! running.
//!
//! A background loop here is a bare `tokio::spawn` around a `select!` on a
//! ticker: it is nobody's child, and when it returns or panics the task is
//! simply gone. Every series that loop published then stops being written, and a
//! series that stopped being written is indistinguishable from one whose
//! subsystem has nothing to say — the cache watcher being dead and the cache
//! having nothing cached read the same from Prometheus. The readings a loop
//! produces are exactly the readings that cannot report its death.
//!
//! So the liveness reading comes from somewhere else: this module keeps a
//! timestamp per loop, and the age is computed **when the scrape arrives**. There
//! is deliberately no publisher task — a publisher is one more thing that can die
//! silently, and its silence would be the very state it exists to report. What
//! carries the reading is the process, and a process that is gone is covered by
//! `up`.
//!
//! Two readings answer two different questions:
//!
//! - **Age** answers "is this loop still cycling". It is only a question when the
//!   loop has a cadence, and the cadence is part of what the loop declares: a
//!   loop that beats once per tick — whether or not the tick found work — has an
//!   age that stays near its period while it lives, so an age of several periods
//!   means it is not cycling. That is what separates "this loop is stuck" from
//!   "this cycle had nothing to do", and it is why the beat is unconditional at
//!   the top of the tick rather than a report of work done. A loop that wakes
//!   only when there is work declares [`Cadence::EventDriven`] instead, and its
//!   age is then published but not judged: a consumer waiting quietly on an empty
//!   queue and a consumer stuck inside a handler look the same from the outside,
//!   and the reading that separates them is the queue's, not this one's.
//! - **Deaths** answer "did this loop end while the process is still running".
//!   A loop is meant to live as long as the process does, so ending for any
//!   reason other than the shutdown it was handed is a defect, and the counter
//!   stays at zero for a loop that has never done it. Zero is published rather
//!   than omitted, because "never died" and "not being counted" are otherwise
//!   the same silence.
//!
//! The count is what a shutdown request is compared against: a stopping process
//! triggers the signal its loops select on, those loops break, and the guard that
//! notices the exit asks whether that is why it exited. Without that comparison
//! every clean shutdown would be reported as one death per loop, which is a
//! reading nobody would keep looking at.
//!
//! What no reading here can show is a loop that was never started. A loop whose
//! plugin is disabled, whose role excludes it, or which someone simply did not
//! wrap does not appear at all — it has no series rather than a zero, so no rule
//! over this family can fire on it. That state is a property of the configuration
//! that decides which plugins run, and it is judged there.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::contract::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use crate::contract::shutdown::ShutdownSignal;
use crate::SFResult;

/// Loops this process started, per loop; the census of what is being watched.
pub const LOOP_REGISTERED: &str = "cogneva_loop_registered";
/// The cadence a loop declared, in seconds; 0 means the loop has no cadence and
/// its age is not a judgement of anything.
pub const LOOP_PERIOD_SECONDS: &str = "cogneva_loop_period_seconds";
/// Seconds since a loop last beat, computed when the scrape arrives.
pub const LOOP_TICK_AGE_SECONDS: &str = "cogneva_loop_tick_age_seconds";
/// Times a loop ended while the process was still running, as a counter.
pub const LOOP_DEATHS_TOTAL: &str = "cogneva_loop_deaths_total";
/// Label naming the loop. Its value set is bounded by configuration rather than
/// by traffic: one value per loop instance the process starts, so a site that
/// runs one instance per configured stream or workspace names it after that
/// configured value. A name built from message content would make the series
/// unbounded, and a name shared by two instances would let a dead one hide
/// behind the beats of its live sibling.
pub const LOOP_LABEL: &str = "loop";

/// How often a loop is expected to reach the top of its cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cadence {
    /// Every `Duration`, whether or not the cycle found work. This is what makes
    /// a stalled loop distinguishable from an idle one.
    Periodic(Duration),
    /// Only when there is work to do. The age is still published — it is a
    /// reading — but nothing may conclude "stuck" from it.
    EventDriven,
}

impl Cadence {
    /// The declared period in whole seconds, or 0 for a loop with no cadence.
    pub fn period_secs(self) -> u64 {
        match self {
            Cadence::Periodic(d) => d.as_secs(),
            Cadence::EventDriven => 0,
        }
    }
}

/// What this module knows about one loop.
struct LoopState {
    name: String,
    period_secs: u64,
    /// Milliseconds since the process's own epoch, which is the monotone clock
    /// rather than the wall clock: a step of the host clock must not read as a
    /// loop that has not beaten since before it started.
    last_beat_ms: AtomicU64,
    deaths: AtomicU64,
}

impl LoopState {
    fn age_secs(&self) -> u64 {
        now_ms().saturating_sub(self.last_beat_ms.load(Ordering::Relaxed)) / 1000
    }
}

/// The loops this process started, by name.
///
/// One registry for the process, because the readings are compared against the
/// process's lifetime: a loop that registered into a per-plugin registry would
/// disappear from the census along with the plugin that owned it, which is the
/// shape of the failure being watched for.
pub struct LoopHealth {
    loops: Mutex<BTreeMap<String, Arc<LoopState>>>,
}

impl Default for LoopHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl LoopHealth {
    pub fn new() -> Self {
        Self {
            loops: Mutex::new(BTreeMap::new()),
        }
    }

    /// Declare a loop and take its beat handle.
    ///
    /// Idempotent by name: two starts of the same loop in one process share one
    /// series, so a retry that re-registers does not double the census. The
    /// cadence of the first registration stands; a later one that disagrees is
    /// logged, because two cadences under one name means one of the two loops is
    /// being judged against the other's period.
    pub fn register(&self, name: impl Into<String>, cadence: Cadence) -> Beat {
        let name = name.into();
        let period_secs = cadence.period_secs();
        let mut loops = self.loops.lock().unwrap_or_else(|e| e.into_inner());
        let state = loops.entry(name.clone()).or_insert_with(|| {
            Arc::new(LoopState {
                name,
                period_secs,
                // Registration is a beat: a loop that hangs before its first tick
                // must age like one, not look newborn forever.
                last_beat_ms: AtomicU64::new(now_ms()),
                deaths: AtomicU64::new(0),
            })
        });
        if state.period_secs != period_secs {
            tracing::warn!(
                loop_name = %state.name,
                registered_secs = state.period_secs,
                declared_secs = period_secs,
                "loop registered twice with different cadences; the first one is in force"
            );
        }
        Beat {
            state: Arc::clone(state),
        }
    }

    fn states(&self) -> Vec<Arc<LoopState>> {
        self.loops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// The loops this process started, by name, for callers that report the
    /// census rather than read it.
    pub fn names(&self) -> Vec<String> {
        self.loops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}

/// The process's registry.
pub fn registry() -> Arc<LoopHealth> {
    static REGISTRY: OnceLock<Arc<LoopHealth>> = OnceLock::new();
    Arc::clone(REGISTRY.get_or_init(|| Arc::new(LoopHealth::new())))
}

/// The readings, as the metrics endpoint collects them.
pub fn observable() -> Arc<dyn Observable> {
    let registry: Arc<LoopHealth> = registry();
    registry
}

/// Declare a loop in the process's registry.
pub fn register(name: impl Into<String>, cadence: Cadence) -> Beat {
    registry().register(name, cadence)
}

/// Start a loop that lives as long as the process does.
///
/// The body is handed a [`Beat`] to stamp once per cycle, and its exit is
/// classified against `shutdown`: a loop that ends while that signal has not been
/// triggered is counted as a death, and one that ends because it was triggered is
/// the ordinary stop.
pub fn spawn<F, Fut>(
    name: impl Into<String>,
    cadence: Cadence,
    shutdown: ShutdownSignal,
    body: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnOnce(Beat) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let beat = register(name, cadence);
    let watch = beat.watch_death(shutdown);
    tokio::spawn(async move {
        let _mortality = watch;
        body(beat).await;
    })
}

/// A loop's beat handle: one stamp per cycle.
#[derive(Clone)]
pub struct Beat {
    state: Arc<LoopState>,
}

impl Beat {
    /// Stamp the top of a cycle.
    ///
    /// Called whether or not the cycle found work: the age is a reading of the
    /// loop, and a loop that only stamps when it did something reports the state
    /// of its input as its own liveness.
    pub fn beat(&self) {
        self.state.last_beat_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// The loop's name, as it appears in the label.
    pub fn name(&self) -> &str {
        &self.state.name
    }

    /// Record this loop's exit as a death unless `shutdown` was triggered.
    ///
    /// Held as a guard so that a loop ending by return and a loop ending by panic
    /// are counted the same way — an unwinding task runs its drops, and a panic
    /// is exactly the exit that must not be silent. A process that aborts
    /// instead is not counted here and does not need to be: it stops being
    /// scaped, which `up` already reports.
    pub fn watch_death(&self, shutdown: ShutdownSignal) -> DeathWatch {
        DeathWatch {
            state: Arc::clone(&self.state),
            shutdown: Some(shutdown),
        }
    }

    /// Record this loop's exit as a death with nothing that can excuse it.
    ///
    /// For a loop whose owner never gave it a way to stop: it has no signal to
    /// be triggered, so every exit is one nobody asked for. Passing a signal that
    /// is never triggered would read the same at runtime and say less — a reader
    /// of the next such loop would have to check whether the signal is real.
    pub fn watch_death_unconditionally(&self) -> DeathWatch {
        DeathWatch {
            state: Arc::clone(&self.state),
            shutdown: None,
        }
    }
}

/// Counts the loop it is dropped with as a death, unless the process is stopping.
pub struct DeathWatch {
    state: Arc<LoopState>,
    /// The signal a stop would have come through; `None` when the loop has no
    /// way to be stopped, and then every exit is a death.
    shutdown: Option<ShutdownSignal>,
}

impl Drop for DeathWatch {
    fn drop(&mut self) {
        if self.shutdown.as_ref().is_some_and(|s| s.is_triggered()) {
            tracing::debug!(
                loop_name = self.state.name,
                "background loop stopped for shutdown"
            );
            return;
        }
        let deaths = self.state.deaths.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::error!(
            loop_name = self.state.name,
            deaths,
            "background loop ended while the process is still running; everything it \
             published from now on is missing rather than zero"
        );
    }
}

#[async_trait]
impl Observable for LoopHealth {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::new();
        for state in self.states() {
            let label = state.name.as_str();
            out.push(RawMetric::new(LOOP_REGISTERED, 1.0).with_label(LOOP_LABEL, label));
            out.push(
                RawMetric::new(LOOP_PERIOD_SECONDS, state.period_secs as f64)
                    .with_label(LOOP_LABEL, label),
            );
            out.push(
                RawMetric::new(LOOP_TICK_AGE_SECONDS, state.age_secs() as f64)
                    .with_label(LOOP_LABEL, label),
            );
            out.push(
                RawMetric::new(
                    LOOP_DEATHS_TOTAL,
                    state.deaths.load(Ordering::Relaxed) as f64,
                )
                .with_label(LOOP_LABEL, label),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// Loops are not a per-dimension question: the same loops are running
    /// whichever dimension is being examined, so this is pulled once.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

/// Milliseconds since this process first asked for the time.
///
/// A monotone clock rather than the wall clock, so that a host clock step is not
/// readable as a loop that has not beaten for hours.
fn now_ms() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric(metrics: &[RawMetric], name: &str, label: &str) -> Option<f64> {
        metrics
            .iter()
            .find(|m| m.name == name && m.labels.get(LOOP_LABEL).is_some_and(|v| v == label))
            .map(|m| m.value)
    }

    async fn collect(readings: &LoopHealth) -> Vec<RawMetric> {
        readings.collect_metrics("").await.unwrap()
    }

    #[tokio::test]
    async fn a_registered_loop_is_in_the_census_before_it_ever_beats() {
        let readings = LoopHealth::new();
        let beat = readings.register("census_probe", Cadence::Periodic(Duration::from_secs(30)));
        assert_eq!(beat.name(), "census_probe");
        let metrics = collect(&readings).await;
        // Registration stamps the clock, so a loop that hangs before its first
        // tick ages from startup instead of looking newborn for as long as it
        // hangs.
        assert_eq!(metric(&metrics, LOOP_REGISTERED, "census_probe"), Some(1.0));
        assert_eq!(
            metric(&metrics, LOOP_PERIOD_SECONDS, "census_probe"),
            Some(30.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_TICK_AGE_SECONDS, "census_probe"),
            Some(0.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "census_probe"),
            Some(0.0)
        );
    }

    #[tokio::test]
    async fn a_loop_with_no_cadence_says_so_instead_of_reporting_a_period() {
        let readings = LoopHealth::new();
        readings.register("event_probe", Cadence::EventDriven);
        let metrics = collect(&readings).await;
        // Zero is the reading that keeps age out of judgement; an absent series
        // would instead be indistinguishable from a loop nobody registered.
        assert_eq!(
            metric(&metrics, LOOP_PERIOD_SECONDS, "event_probe"),
            Some(0.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_TICK_AGE_SECONDS, "event_probe"),
            Some(0.0)
        );
    }

    #[tokio::test]
    async fn the_age_is_computed_at_the_scrape() {
        let readings = LoopHealth::new();
        let beat = readings.register("stale_probe", Cadence::Periodic(Duration::from_secs(5)));
        beat.beat();
        assert_eq!(
            metric(
                &collect(&readings).await,
                LOOP_TICK_AGE_SECONDS,
                "stale_probe"
            ),
            Some(0.0)
        );
        // Nobody touches the loop, and the age grows anyway: it is derived when
        // the scrape arrives, from a stamp the loop left behind. A publisher task
        // would be one more thing that can die silently, and its silence is the
        // very state this reading exists to report.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let aged = metric(
            &collect(&readings).await,
            LOOP_TICK_AGE_SECONDS,
            "stale_probe",
        );
        assert_eq!(aged, Some(1.0));
    }

    #[tokio::test]
    async fn a_stamp_from_the_future_is_not_a_negative_age() {
        let readings = LoopHealth::new();
        let beat = readings.register(
            "clock_step_probe",
            Cadence::Periodic(Duration::from_secs(5)),
        );
        beat.state.last_beat_ms.store(u64::MAX, Ordering::Relaxed);
        assert_eq!(
            metric(
                &collect(&readings).await,
                LOOP_TICK_AGE_SECONDS,
                "clock_step_probe"
            ),
            Some(0.0)
        );
    }

    #[test]
    fn a_loop_that_ends_while_the_process_runs_is_counted_once() {
        let readings = LoopHealth::new();
        let beat = readings.register("dies_probe", Cadence::Periodic(Duration::from_secs(1)));
        let shutdown = ShutdownSignal::new();
        {
            let _watch = beat.watch_death(shutdown.clone());
        }
        assert_eq!(beat.state.deaths.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_loop_with_no_way_to_be_stopped_counts_every_exit() {
        let readings = LoopHealth::new();
        let beat = readings.register("unstop_probe", Cadence::Periodic(Duration::from_secs(1)));
        {
            let _watch = beat.watch_death_unconditionally();
        }
        assert_eq!(beat.state.deaths.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_loop_that_stops_for_shutdown_is_not_counted() {
        let readings = LoopHealth::new();
        let beat = readings.register("stopping_probe", Cadence::Periodic(Duration::from_secs(1)));
        let shutdown = ShutdownSignal::new();
        shutdown.trigger();
        {
            let _watch = beat.watch_death(shutdown.clone());
        }
        // Otherwise every clean restart would report one death per loop, and the
        // counter would be noise nobody reads.
        assert_eq!(beat.state.deaths.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn registering_the_same_name_twice_shares_one_series() {
        let readings = LoopHealth::new();
        let first = readings.register("shared_probe", Cadence::Periodic(Duration::from_secs(10)));
        let second = readings.register("shared_probe", Cadence::Periodic(Duration::from_secs(10)));
        first.beat();
        // The second handle must be the same loop, not a second series that would
        // double the census and let one start hide the other's stall.
        assert_eq!(
            metric(&collect(&readings).await, LOOP_REGISTERED, "shared_probe"),
            Some(1.0)
        );
        assert_eq!(readings.names(), vec![String::from("shared_probe")]);
        second.beat();
        assert_eq!(first.state.age_secs(), second.state.age_secs());
    }

    #[tokio::test]
    async fn a_death_survives_later_scrapes() {
        let readings = LoopHealth::new();
        let beat = readings.register("dead_probe", Cadence::Periodic(Duration::from_secs(2)));
        {
            let _watch = beat.watch_death(ShutdownSignal::new());
        }
        beat.beat();
        // A loop that died and a loop that is merely quiet must not read the
        // same: the counter does not reset when the (now absent) loop stops
        // stamping.
        let metrics = collect(&readings).await;
        assert_eq!(metric(&metrics, LOOP_DEATHS_TOTAL, "dead_probe"), Some(1.0));
        assert_eq!(metric(&metrics, LOOP_REGISTERED, "dead_probe"), Some(1.0));
    }

    #[tokio::test]
    async fn a_started_loop_stamps_and_its_exit_is_counted_as_a_death() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);
        let handle = spawn(
            "spawned_probe",
            Cadence::Periodic(Duration::from_secs(5)),
            ShutdownSignal::new(),
            move |beat| async move {
                beat.beat();
                let _ = tx.send(beat.name().to_string()).await;
            },
        );
        assert_eq!(rx.recv().await, Some(String::from("spawned_probe")));
        handle.await.unwrap();
        let metrics = registry().collect_metrics("").await.unwrap();
        assert_eq!(
            metric(&metrics, LOOP_TICK_AGE_SECONDS, "spawned_probe"),
            Some(0.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "spawned_probe"),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn a_started_loop_that_stops_for_shutdown_is_not_a_death() {
        let shutdown = ShutdownSignal::new();
        shutdown.trigger();
        let handle = spawn(
            "shutdown_probe",
            Cadence::EventDriven,
            shutdown,
            |beat| async move {
                beat.beat();
            },
        );
        handle.await.unwrap();
        let metrics = registry().collect_metrics("").await.unwrap();
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "shutdown_probe"),
            Some(0.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_PERIOD_SECONDS, "shutdown_probe"),
            Some(0.0)
        );
    }
}
