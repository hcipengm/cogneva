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
//! A loop that panics is run again, because a panic is the one exit whose intent
//! is not ambiguous: the loop did not decide to stop, a cycle killed it. The
//! attempts are bounded, and the bound is what keeps the repair honest — a loop
//! that cannot survive even with a fresh start is a defect, and running it again
//! forever would only replace a dead loop that is reported with a live one that
//! panics invisibly. Past the budget the loop stays dead and the readings above
//! report it, which is also why the restarts have a counter of their own: a
//! repair that leaves no reading turns "this is broken" into "this is quiet".
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
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::FutureExt;

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
/// Times a loop was restarted after panicking, as a counter.
///
/// Separate from the deaths, because the two answer different questions and are
/// repaired differently: a death is a loop that stayed dead, while a restart is
/// a loop that panicked and is running again. Counting restarts as deaths would
/// make the two indistinguishable exactly in the case a reader has to act on,
/// and counting them nowhere would let a loop panic every hour forever without
/// anything saying so — a repair that leaves no reading is a defect made quiet.
pub const LOOP_RESTARTS_TOTAL: &str = "cogneva_loop_restarts_total";
/// Label naming the loop. Its value set is bounded by configuration rather than
/// by traffic: one value per loop instance the process starts, so a site that
/// runs one instance per configured stream or workspace names it after that
/// configured value. A name built from message content would make the series
/// unbounded, and a name shared by two instances would let a dead one hide
/// behind the beats of its live sibling.
pub const LOOP_LABEL: &str = "loop";

/// The deployed rule that reads the age this module publishes.
pub const STALL_RULE: &str = "background_loop_stalled";
/// The deployed rule that reads the death counter this module publishes.
pub const DEATH_RULE: &str = "background_loop_died";
/// The deployed rule that reads the restart counter this module publishes.
pub const RESTART_RULE: &str = "background_loop_restarted";

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

/// What this process allows a loop that panicked, before it is left dead.
///
/// Installed once at startup from the configuration document, because the budget
/// belongs to the process's loops rather than to any one of them: three loops
/// holding three different budgets would make the same defect read differently
/// depending on which loop happened to hit it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartSettings {
    /// How many times in a row a loop is run again after panicking.
    ///
    /// Three, because a fault that a restart repairs is repaired by the first
    /// one: the second and third exist for the faults that need the state they
    /// tripped over to be gone, and a loop that panics past all three is not
    /// having a bad cycle — it is a defect, and a fourth attempt would only
    /// postpone the reading that says so. Zero turns restarting off and leaves
    /// a panic as fatal as it was before this existed.
    pub max_consecutive: u32,
    /// The floor under a loop's first wait between attempts, in seconds.
    ///
    /// A loop with a cadence waits its own period; a loop that only wakes on work
    /// has no rhythm to wait, so it waits this. Five seconds is long enough that
    /// a loop which panics immediately does not fill the log with attempts (the
    /// waits double from here, so a spent budget is tens of seconds, not
    /// milliseconds) and short enough that an event-driven loop is back before
    /// the next burst of work.
    pub backoff_floor_secs: u64,
}

impl Default for RestartSettings {
    fn default() -> Self {
        Self {
            max_consecutive: 3,
            backoff_floor_secs: 5,
        }
    }
}

/// The process's restart budget. First install wins; see
/// [`install_restart_settings`].
static RESTART_SETTINGS: OnceLock<RestartSettings> = OnceLock::new();

/// Install the process-wide restart budget, from the configuration document.
///
/// The first install wins, and a later one that disagrees is logged rather than
/// applied: the loops this governs are already running by the time a second
/// caller could install one, so a budget that moved underneath them would judge
/// the same panic differently depending on when it happened.
pub fn install_restart_settings(settings: RestartSettings) {
    if RESTART_SETTINGS.set(settings).is_err() && RESTART_SETTINGS.get().copied() != Some(settings)
    {
        tracing::warn!(
            installed_max_consecutive = RESTART_SETTINGS.get().map(|s| s.max_consecutive),
            offered_max_consecutive = settings.max_consecutive,
            "restart budget installed twice with different values; the first one is in force"
        );
    }
}

/// The restart budget in force, or the documented default when nothing installed
/// one — a process that never reads a configuration document still restarts its
/// loops.
fn restart_settings() -> RestartSettings {
    *RESTART_SETTINGS.get_or_init(RestartSettings::default)
}

/// The restart budget one loop is judged by, derived from what it declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartPolicy {
    /// Restarts allowed back to back before the loop is left dead.
    pub max_consecutive: u32,
    /// The wait before the first attempt to run the loop again; it doubles from
    /// there, up to [`Self::backoff_max`].
    pub backoff: Duration,
    /// The longest wait, and the lifetime an attempt has to reach to count as
    /// having taken.
    pub backoff_max: Duration,
}

/// How many of a loop's own periods the longest restart wait spans.
///
/// Six is not a value chosen here: it is the multiple the deployed stall rule
/// already measures a loop against. Waiting longer than that would keep a loop
/// from beating past the point where it is announced as stalled, so the two
/// readings would describe the same state in opposite directions.
const STALL_PERIODS: u32 = 6;

impl RestartPolicy {
    /// The budget for a loop with this cadence under these settings.
    pub fn for_cadence(cadence: Cadence, settings: RestartSettings) -> Self {
        let declared = match cadence {
            Cadence::Periodic(period) => period,
            Cadence::EventDriven => Duration::ZERO,
        };
        let base = declared.max(Duration::from_secs(settings.backoff_floor_secs));
        Self {
            max_consecutive: settings.max_consecutive,
            backoff: base,
            backoff_max: base * STALL_PERIODS,
        }
    }

    /// How long to wait before the `run`th attempt in a row.
    fn wait_before(&self, run: u32) -> Duration {
        self.backoff
            .saturating_mul(1u32 << (run - 1).min(16))
            .min(self.backoff_max)
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
    restarts: AtomicU64,
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
                restarts: AtomicU64::new(0),
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
///
/// A body that panics is run again, up to the process's restart budget. The body
/// is called once per attempt, so it has to be able to start over: what it
/// consumes it creates inside itself, and what it shares it clones on the way in.
/// A body that returns, by contrast, is never restarted — a return is the loop
/// saying it is done, which is a decision this module cannot overrule and has no
/// reading that would let it.
pub fn spawn<F, Fut>(
    name: impl Into<String>,
    cadence: Cadence,
    shutdown: ShutdownSignal,
    body: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(Beat) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let policy = RestartPolicy::for_cadence(cadence, restart_settings());
    spawn_with_policy(name, cadence, Some(shutdown), policy, body)
}

/// Start a loop whose owner never gave it a way to stop.
///
/// Symmetric with [`Beat::watch_death_unconditionally`]: there is no signal that
/// could excuse an exit, so every exit is one nobody asked for — and a panic is
/// run again for the same reason it is counted, because nothing about this loop's
/// exit was intended.
pub fn spawn_unstoppable<F, Fut>(
    name: impl Into<String>,
    cadence: Cadence,
    body: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(Beat) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let policy = RestartPolicy::for_cadence(cadence, restart_settings());
    spawn_with_policy(name, cadence, None, policy, body)
}

/// Start a loop under a budget given here rather than derived.
///
/// For the caller that has to judge a specific panic rate, and for tests, which
/// need waits they can drive instead of wall-clock waits.
pub fn spawn_with_policy<F, Fut>(
    name: impl Into<String>,
    cadence: Cadence,
    shutdown: Option<ShutdownSignal>,
    policy: RestartPolicy,
    body: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(Beat) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let beat = register(name, cadence);
    tokio::spawn(supervise(beat, shutdown, policy, body))
}

/// Run a loop's body, and run it again if it panics.
///
/// The waits are taken from the loop's own cadence rather than from a timer of
/// this module's: a loop that beats every minute and a loop that beats every
/// second are not equally broken when they panic, and a single wait for both
/// would be too long for one and too short for the other.
async fn supervise<F, Fut>(
    beat: Beat,
    shutdown: Option<ShutdownSignal>,
    policy: RestartPolicy,
    mut body: F,
) where
    F: FnMut(Beat) -> Fut,
    Fut: Future<Output = ()>,
{
    let _mortality = match &shutdown {
        Some(signal) => beat.watch_death(signal.clone()),
        None => beat.watch_death_unconditionally(),
    };
    let mut run: u32 = 0;
    loop {
        let attempt = tokio::time::Instant::now();
        let payload = match AssertUnwindSafe(body(beat.clone())).catch_unwind().await {
            // The loop chose to end. Its exit is not this module's to overrule,
            // and the guard above is what decides what the exit meant.
            Ok(()) => return,
            Err(payload) => payload,
        };
        if shutdown.as_ref().is_some_and(|s| s.is_triggered()) {
            tracing::debug!(
                loop_name = beat.name(),
                "a background loop panicked while the process was stopping; not running it again"
            );
            return;
        }
        // An attempt that lived as long as the longest wait counts as having
        // taken, so the panic that ended it starts a new run instead of
        // extending one. Without this a loop that panics once a week would be
        // given up on after three panics spread over a month.
        if attempt.elapsed() >= policy.backoff_max {
            run = 0;
        }
        if run + 1 > policy.max_consecutive {
            tracing::error!(
                loop_name = beat.name(),
                restarts = policy.max_consecutive,
                "a background loop panicked again with its restart budget spent; leaving it \
                 dead, which the death counter and the stall rule report from here. The panic \
                 message just above this line is the defect"
            );
            return;
        }
        run += 1;
        let restarts = beat.note_restart();
        let wait = policy.wait_before(run);
        tracing::warn!(
            loop_name = beat.name(),
            restarts,
            consecutive = run,
            wait_ms = wait.as_millis() as u64,
            panic = panic_message(&payload),
            "a background loop panicked; running it again"
        );
        match &shutdown {
            Some(signal) => tokio::select! {
                biased;
                _ = signal.wait() => return,
                _ = tokio::time::sleep(wait) => {}
            },
            None => tokio::time::sleep(wait).await,
        }
    }
}

/// A panic payload as text, for the line that reports the panic.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> &str {
    if let Some(text) = payload.downcast_ref::<&'static str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.as_str()
    } else {
        "a panic whose payload is not text"
    }
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

    /// Count a restart of this loop and return the running total.
    fn note_restart(&self) -> u64 {
        self.state.restarts.fetch_add(1, Ordering::Relaxed) + 1
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
            out.push(
                RawMetric::new(
                    LOOP_RESTARTS_TOTAL,
                    state.restarts.load(Ordering::Relaxed) as f64,
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

    /// A body that panics on the attempts `panic_on` picks and returns on the
    /// rest, with the attempts counted where the test can read them.
    fn flaky_body(
        attempts: Arc<std::sync::atomic::AtomicU32>,
        panic_on: Arc<dyn Fn(u32) -> bool + Send + Sync>,
    ) -> impl FnMut(Beat) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static
    {
        move |beat| {
            let attempts = Arc::clone(&attempts);
            let panic_on = Arc::clone(&panic_on);
            Box::pin(async move {
                beat.beat();
                let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                if panic_on(attempt) {
                    panic!("attempt {attempt} panicked on purpose");
                }
            })
        }
    }

    fn policy(max_consecutive: u32, backoff_ms: u64) -> RestartPolicy {
        RestartPolicy {
            max_consecutive,
            backoff: Duration::from_millis(backoff_ms),
            backoff_max: Duration::from_millis(backoff_ms * 4),
        }
    }

    /// The restart budget a loop is judged by follows the cadence the loop
    /// declared, because a loop that beats every minute and one that beats every
    /// second are not equally broken when they panic.
    #[test]
    fn the_restart_waits_are_derived_from_the_loop_cadence() {
        let settings = RestartSettings {
            max_consecutive: 2,
            backoff_floor_secs: 5,
        };
        let slow = RestartPolicy::for_cadence(Cadence::Periodic(Duration::from_secs(60)), settings);
        assert_eq!(slow.backoff, Duration::from_secs(60));
        assert_eq!(slow.backoff_max, Duration::from_secs(360));
        // A loop with no cadence has no rhythm to wait, so the floor stands in.
        let idle = RestartPolicy::for_cadence(Cadence::EventDriven, settings);
        assert_eq!(idle.backoff, Duration::from_secs(5));
        assert_eq!(idle.backoff_max, Duration::from_secs(30));
        // A loop faster than the floor waits the floor: a one-second loop that
        // panics immediately must not be run again every second.
        let fast = RestartPolicy::for_cadence(Cadence::Periodic(Duration::from_secs(1)), settings);
        assert_eq!(fast.backoff, Duration::from_secs(5));
        assert_eq!(fast.max_consecutive, 2);
        assert_eq!(slow.wait_before(1), Duration::from_secs(60));
        assert_eq!(slow.wait_before(2), Duration::from_secs(120));
        assert_eq!(slow.wait_before(3), Duration::from_secs(240));
        assert_eq!(slow.wait_before(4), Duration::from_secs(360));
        assert_eq!(slow.wait_before(9), Duration::from_secs(360));
    }

    #[tokio::test(start_paused = true)]
    async fn a_loop_that_panics_is_run_again_and_the_restart_is_counted() {
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let handle = spawn_with_policy(
            "restarted_probe",
            Cadence::EventDriven,
            None,
            policy(3, 100),
            {
                let attempts = Arc::clone(&attempts);
                move |beat| {
                    let attempts = Arc::clone(&attempts);
                    Box::pin(async move {
                        beat.beat();
                        let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                        if attempt == 1 {
                            panic!("the first attempt panicked on purpose");
                        }
                        // The second attempt is the loop back at work, and it
                        // stays at work: the reading below is taken while it is
                        // running, which is the whole difference between a
                        // restart and a death.
                        std::future::pending::<()>().await;
                    }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
                }
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let metrics = collect(&registry()).await;
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(
            metric(&metrics, LOOP_RESTARTS_TOTAL, "restarted_probe"),
            Some(1.0)
        );
        // A panic that was repaired is not a death: a reader that saw both
        // counters move could not tell whether the loop came back.
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "restarted_probe"),
            Some(0.0)
        );
        handle.abort();
        let _ = handle.await;
    }

    /// The budget is what makes the repair honest: a loop that cannot survive a
    /// fresh start is left dead and reported, rather than run again forever.
    #[tokio::test(start_paused = true)]
    async fn a_loop_that_keeps_panicking_is_left_dead_once_its_budget_is_spent() {
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let handle = spawn_with_policy(
            "broken_probe",
            Cadence::Periodic(Duration::from_secs(5)),
            None,
            policy(2, 100),
            flaky_body(Arc::clone(&attempts), Arc::new(|_| true)),
        );
        // Two restarts happen, and the third panic ends it: three attempts.
        handle.await.unwrap();
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        let metrics = collect(&registry()).await;
        assert_eq!(
            metric(&metrics, LOOP_RESTARTS_TOTAL, "broken_probe"),
            Some(2.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "broken_probe"),
            Some(1.0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_loop_that_returns_is_not_run_again() {
        let handle = spawn_with_policy(
            "returning_probe",
            Cadence::EventDriven,
            None,
            policy(3, 100),
            |beat| {
                beat.beat();
                async move {}
            },
        );
        handle.await.unwrap();
        let metrics = collect(&registry()).await;
        // A return is the loop saying it is done. Restarting it would make this
        // module the author of a loop the body decided to end.
        assert_eq!(
            metric(&metrics, LOOP_RESTARTS_TOTAL, "returning_probe"),
            Some(0.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "returning_probe"),
            Some(1.0)
        );
    }

    /// An attempt that lived as long as the longest wait counts as having taken,
    /// so its panic starts a new run instead of spending the rest of the budget.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_that_stayed_up_clears_the_run() {
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let handle = spawn_with_policy(
            "recovering_probe",
            Cadence::EventDriven,
            None,
            policy(1, 100),
            {
                let attempts = Arc::clone(&attempts);
                move |beat| {
                    let attempts = Arc::clone(&attempts);
                    Box::pin(async move {
                        beat.beat();
                        let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                        if attempt < 3 {
                            // The second attempt outlives the longest wait, so
                            // the third panic is the first of a new run rather
                            // than the second of the old one.
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            panic!("attempt {attempt} panicked on purpose");
                        }
                    }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
                }
            },
        );
        handle.await.unwrap();
        let metrics = collect(&registry()).await;
        // With a budget of one and no clearing, the second panic would have ended
        // it: two restarts is what says the run was cleared in between.
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert_eq!(
            metric(&metrics, LOOP_RESTARTS_TOTAL, "recovering_probe"),
            Some(2.0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_loop_that_panics_while_the_process_is_stopping_is_not_run_again() {
        let shutdown = ShutdownSignal::new();
        let handle = spawn_with_policy(
            "stopping_probe",
            Cadence::EventDriven,
            Some(shutdown.clone()),
            policy(3, 100),
            {
                let shutdown = shutdown.clone();
                move |beat| {
                    let shutdown = shutdown.clone();
                    async move {
                        beat.beat();
                        shutdown.trigger();
                        panic!("panicked while stopping");
                    }
                }
            },
        );
        handle.await.unwrap();
        let metrics = collect(&registry()).await;
        assert_eq!(
            metric(&metrics, LOOP_RESTARTS_TOTAL, "stopping_probe"),
            Some(0.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "stopping_probe"),
            Some(0.0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_loop_with_no_way_to_stop_is_run_again_after_a_panic() {
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let handle = spawn_unstoppable(
            "unstoppable_probe",
            Cadence::EventDriven,
            flaky_body(Arc::clone(&attempts), Arc::new(|attempt| attempt == 1)),
        );
        // Nothing about this loop's exit is intended, so the run ends only with
        // the body returning on its own.
        handle.await.unwrap();
        let metrics = collect(&registry()).await;
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(
            metric(&metrics, LOOP_RESTARTS_TOTAL, "unstoppable_probe"),
            Some(1.0)
        );
        assert_eq!(
            metric(&metrics, LOOP_DEATHS_TOTAL, "unstoppable_probe"),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn a_started_loop_stamps_and_its_exit_is_counted_as_a_death() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);
        let handle = spawn(
            "spawned_probe",
            Cadence::Periodic(Duration::from_secs(5)),
            ShutdownSignal::new(),
            // The clone is the shape every restartable body has to have: the
            // attempt that runs gets its own handle, so a second attempt can be
            // built from the same closure.
            move |beat| {
                let tx = tx.clone();
                async move {
                    beat.beat();
                    let _ = tx.send(beat.name().to_string()).await;
                }
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

    /// The three rules, each asserted against the series this module publishes.
    ///
    /// Renaming either side leaves both ends self-consistent and the signal
    /// gone: the rule keeps its shape, the reading keeps being published, and
    /// nothing connects them. The restart rule is asserted the same way as the
    /// other two even though a restart repairs the loop, because the reading
    /// exists precisely so that a repair is not a way to keep a defect quiet.
    #[test]
    fn deployed_rules_query_the_metrics_this_module_publishes() {
        let rules = chart_rules();
        let find = |name: &str| -> String {
            rules
                .iter()
                .find(|(rule, _)| rule == name)
                .map(|(_, promql)| promql.clone())
                .unwrap_or_else(|| panic!("rule {name} missing"))
        };

        let stalled = find(STALL_RULE);
        assert!(
            stalled.contains(LOOP_TICK_AGE_SECONDS) && stalled.contains(LOOP_PERIOD_SECONDS),
            "rule {STALL_RULE} must compare the age against the declared period, got: {stalled}"
        );

        let died = find(DEATH_RULE);
        assert!(
            died.contains(LOOP_DEATHS_TOTAL),
            "rule {DEATH_RULE} must query {LOOP_DEATHS_TOTAL}, got: {died}"
        );

        let restarted = find(RESTART_RULE);
        assert!(
            restarted.contains(LOOP_RESTARTS_TOTAL),
            "rule {RESTART_RULE} must query {LOOP_RESTARTS_TOTAL}, got: {restarted}"
        );
    }

    fn chart_rules() -> Vec<(String, String)> {
        let chart = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        root.pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present")
            .iter()
            .map(|r| {
                (
                    r["name"].as_str().expect("rule name").to_string(),
                    r["promql"].as_str().expect("rule promql").to_string(),
                )
            })
            .collect()
    }
}
