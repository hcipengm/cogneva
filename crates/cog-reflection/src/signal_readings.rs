//! What the self-discovery watcher did with each round's signals.
//!
//! The watcher runs on a timer and, most rounds, submits nothing. Silence is
//! the ordinary state of a healthy system, and it is the state this family
//! exists to keep readable -- because the watcher is silent for five different
//! reasons that nothing else distinguishes:
//!
//! - nothing was wrong, so no signal was found at all;
//! - a signal was found and a task for it is already in hand, so the submission
//!   was refused as a duplicate (that idempotence has to live here: the
//!   orchestrator raises on a duplicate id rather than skipping it);
//! - a signal was found and is still inside its report cooldown, so it was not
//!   even offered for submission;
//! - a signal was found whose task had already reached a terminal state while
//!   the signal kept firing -- the finished row was cleared and the work filed
//!   again;
//! - a signal was found and the submission never reached the orchestrator.
//!
//! Read from the task store alone, all five are the same absence. Reading them
//! as one number is what makes "the watcher has gone quiet" indistinguishable
//! from "the system is fine", and it is the wrong conclusion in four of the
//! five cases: a watcher whose every signal is refused as a duplicate, or
//! throttled by its own cooldown, is blind in exactly the way a stopped watcher
//! is blind.
//!
//! Six series, and the division between them is deliberate:
//!
//! - `cogneva_signal_watcher_running` -- 1 on the process that armed the
//!   watcher loop, 0 on every other. Published by every process, because the
//!   question is about the deployment: a control plane that does not run
//!   self-discovery still has to say so, or its flat counters would be read as
//!   a watcher finding nothing rather than as a watcher that was never there.
//! - `cogneva_signal_watcher_ticks_total` -- rounds this process's watcher has
//!   completed, counted whether or not the round found anything. This is the
//!   denominator: signals over ticks is a rate, and a rate from a watcher that
//!   has stopped ticking is not a small number, it is no number at all. A held
//!   round ran the gate and stood down, so it counts here too -- if it did not,
//!   the denominator would freeze exactly when a reader most needs to tell a
//!   pause from a death.
//! - `cogneva_signal_watcher_held_total` -- rounds this watcher stood down on the
//!   LLM pool gate instead of watching. It is a counter at the tick count's own
//!   cadence, one increment per round that stood down, and that is what makes
//!   the pause countable rather than inferred: `ticks - held` is the rounds that
//!   actually looked, and a window where `held` grows is a window this watcher
//!   spent not watching. A pause left to be read off a denominator that stopped
//!   moving would not be distinguishable from a loop that died, and the two want
//!   opposite responses.
//! - `cogneva_signal_watcher_signals_total{outcome}` -- one increment per signal
//!   the watcher found, by what it did with it.
//! - `cogneva_signal_watcher_guard_entries` -- keys the report-cooldown store
//!   holds, as of the last completed round. This is the size of the memory the
//!   watcher carries between rounds, and it is a set rather than an
//!   accumulation because the store is reclaimed: a reading that could only go
//!   up would not show that the reclaimer works. It is withheld until the first
//!   round has read the store, since a zero before that is a claim about a
//!   store nobody has looked at.
//! - `cogneva_signal_watcher_guard_reclaimed_total` -- keys dropped from that
//!   store because their cooldown had run out. Published from the start: zero
//!   entries reclaimed is a true statement about this process.
//!
//! The outcome label takes a closed set, and every value is published on every
//! scrape even at zero. That is the whole point of the family: a rule asking
//! whether signals are being refused as duplicates has to be able to tell "none
//! were" from "this watcher never published that series", and only an always
//! present zero can do that. Nothing here is inferred from absence.
//!
//! What separates this from the liveness reading is the question each answers.
//! The loop census (`cogneva_loop_tick_age_seconds`, `cogneva_loop_registered`)
//! says whether the loop is still turning; this says what it found when it
//! turned. A watcher that ticks on schedule and finds nothing and a watcher
//! whose loop died are told apart by those, not by these counters, which is why
//! the tick count here is a denominator rather than a second health signal.
//!
//! The counters are per process and reset with it, so a reader takes their rate
//! rather than their value: what matters is the change across a window long
//! enough to contain several rounds, not the total since a restart that may
//! have been a minute ago.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// 1 on the process that armed the watcher loop, 0 on the ones that did not.
pub const SIGNAL_WATCHER_RUNNING_METRIC: &str = "cogneva_signal_watcher_running";

/// Rounds this process's watcher has completed, signal or no signal.
pub const SIGNAL_TICKS_METRIC: &str = "cogneva_signal_watcher_ticks_total";

/// Rounds that stood down on the LLM pool gate instead of watching.
pub const SIGNAL_HELD_METRIC: &str = "cogneva_signal_watcher_held_total";

/// Signals the watcher found, by what it did with each one.
pub const SIGNAL_OUTCOMES_METRIC: &str = "cogneva_signal_watcher_signals_total";

/// Keys the report-cooldown store holds, as of the last completed round.
pub const SIGNAL_GUARD_ENTRIES_METRIC: &str = "cogneva_signal_watcher_guard_entries";

/// Keys dropped because their cooldown had run out, since this process started.
pub const SIGNAL_GUARD_RECLAIMED_METRIC: &str = "cogneva_signal_watcher_guard_reclaimed_total";

/// The label naming what became of a signal.
pub const OUTCOME_LABEL: &str = "outcome";

/// A new task was created for the signal.
pub const OUTCOME_REGISTERED: &str = "registered";

/// A task for this signal is already in hand, so the submission was refused as
/// a duplicate. The signal is real and is being worked on; nothing new was
/// submitted.
pub const OUTCOME_TRACKED: &str = "tracked";

/// A previously failed attempt was reset and will run again.
pub const OUTCOME_REDRIVEN: &str = "redriven";

/// The signal is still firing and its earlier task had already finished, so the
/// terminal row was cleared and the work filed as a new task. Distinct from
/// `redriven`, which resets an attempt that *failed*, and from `tracked`, which
/// leaves in hand an attempt that has not finished: here the previous attempt
/// ended successfully, and the signal outlived it.
pub const OUTCOME_RESUBMITTED: &str = "resubmitted";

/// The submission never reached the orchestrator. Distinct from `tracked`:
/// both submit nothing, and one of them is a broken channel while the other is
/// work already in hand.
pub const OUTCOME_FAILED: &str = "failed";

/// The signal was still inside its report cooldown and was not offered for
/// submission. Distinct from finding nothing: the signal is present and
/// recurring, and is being deliberately throttled.
pub const OUTCOME_COOLDOWN: &str = "cooldown";

/// Every value the outcome label takes, so a reader can see the whole domain
/// with zeros rather than inferring it from whichever values happened to occur.
pub const SIGNAL_OUTCOMES: &[&str] = &[
    OUTCOME_REGISTERED,
    OUTCOME_TRACKED,
    OUTCOME_REDRIVEN,
    OUTCOME_RESUBMITTED,
    OUTCOME_FAILED,
    OUTCOME_COOLDOWN,
];

/// One signal's fate in a round, as this module counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    Registered,
    Tracked,
    Redriven,
    Resubmitted,
    Failed,
    Cooldown,
}

impl SignalOutcome {
    /// The value this outcome is counted under. The variant and the label are
    /// one list, so a new outcome cannot be counted under a name no reader
    /// knows or published under a name nothing counts.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Registered => OUTCOME_REGISTERED,
            Self::Tracked => OUTCOME_TRACKED,
            Self::Redriven => OUTCOME_REDRIVEN,
            Self::Resubmitted => OUTCOME_RESUBMITTED,
            Self::Failed => OUTCOME_FAILED,
            Self::Cooldown => OUTCOME_COOLDOWN,
        }
    }

    /// The slot this outcome counts in, in the order [`SIGNAL_OUTCOMES`] lists
    /// it. A test pins the two together, so a variant added here without a
    /// matching entry there fails rather than being counted under whichever
    /// name happens to sit at that position.
    fn index(self) -> usize {
        match self {
            Self::Registered => 0,
            Self::Tracked => 1,
            Self::Redriven => 2,
            Self::Resubmitted => 3,
            Self::Failed => 4,
            Self::Cooldown => 5,
        }
    }
}

/// The watcher's counters for this process.
///
/// Held by the process that armed the loop and passed to it, rather than read
/// from a global: a process-global here would be one set of counters written by
/// whichever test ran first, and the outcome a test asserts would depend on the
/// tests beside it.
pub struct SignalWatcherReadings {
    running: AtomicBool,
    held: AtomicU64,
    ticks: AtomicU64,
    signals: [AtomicU64; SIGNAL_OUTCOMES.len()],
    guard_entries: AtomicU64,
    guard_reclaimed: AtomicU64,
}

impl Default for SignalWatcherReadings {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalWatcherReadings {
    pub fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            held: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
            signals: std::array::from_fn(|_| AtomicU64::new(0)),
            guard_entries: AtomicU64::new(0),
            guard_reclaimed: AtomicU64::new(0),
        }
    }

    /// Record that this process started the watcher loop.
    ///
    /// Called by the loop's own spawn, so the flag cannot claim a watcher is
    /// running in a process where the spawn was skipped for want of an
    /// orchestrator, a disabled channel or a different role.
    pub fn mark_running(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    /// One completed round, whether or not it found anything. This is what
    /// makes "no signal" countable rather than inferred.
    ///
    /// A round the pool gate held off counts too: the loop ran it and decided
    /// to stand down, which [`Self::held_round`] records. Counting only the
    /// rounds that watched would freeze this series for as long as the gate is
    /// closed, which is the reading a dead loop leaves.
    pub fn tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }

    /// One round that stood down on the LLM pool gate instead of watching.
    ///
    /// At the tick count's own cadence, one increment per stood-down round, so
    /// the two are read together: `ticks - held` is the rounds that looked, and
    /// the growth of this series is the pause. A flag instead would say the
    /// watcher is standing down now but not for how long, and "how long" is the
    /// question the two want answered together.
    pub fn held_round(&self) {
        self.held.fetch_add(1, Ordering::Relaxed);
    }

    /// One signal and what became of it.
    pub fn record(&self, outcome: SignalOutcome) {
        self.signals[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// The report-cooldown store as this round left it: how many keys it holds,
    /// and how many this round dropped for having outlived their cooldown.
    ///
    /// The size is a set, not an accumulation -- the store shrinks as well as
    /// grows, and a reading that could only go up could not show that the
    /// reclaimer works. The drop count is an accumulation, and a zero there
    /// says something true about this process rather than something false about
    /// the store, which is why the two are separate series.
    pub fn guard_store(&self, entries: usize, reclaimed: usize) {
        self.guard_entries.store(entries as u64, Ordering::Relaxed);
        self.guard_reclaimed
            .fetch_add(reclaimed as u64, Ordering::Relaxed);
    }

    fn count(&self, outcome: &str) -> u64 {
        SIGNAL_OUTCOMES
            .iter()
            .position(|name| *name == outcome)
            .map(|i| self.signals[i].load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

#[async_trait]
impl Observable for SignalWatcherReadings {
    /// The role flag is published before anything is counted, so the series a
    /// rule asks about exists even in a process that runs no watcher at all.
    /// Below it, nothing is published when the watcher is not running: a zero
    /// tick count from a process that never armed the loop is the reading this
    /// module is about, not a reading of it.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = vec![RawMetric::new(
            SIGNAL_WATCHER_RUNNING_METRIC,
            if self.running.load(Ordering::Relaxed) {
                1.0
            } else {
                0.0
            },
        )];
        if !self.running.load(Ordering::Relaxed) {
            return Ok(out);
        }
        out.push(RawMetric::new(
            SIGNAL_TICKS_METRIC,
            self.ticks.load(Ordering::Relaxed) as f64,
        ));
        // Published from the start, like the reclaim count: zero rounds stood
        // down is a true statement about a watcher that has run and watched.
        // Withholding it until the first stood-down round would leave "never
        // stood down" and "never published the question" as the same missing
        // series.
        out.push(RawMetric::new(
            SIGNAL_HELD_METRIC,
            self.held.load(Ordering::Relaxed) as f64,
        ));
        for outcome in SIGNAL_OUTCOMES {
            out.push(
                RawMetric::new(SIGNAL_OUTCOMES_METRIC, self.count(outcome) as f64)
                    .with_label(OUTCOME_LABEL, *outcome),
            );
        }
        // The store's size is published only once a round has read it. Before
        // that, a zero would be a claim about a store nobody has looked at --
        // the same "have not looked" read as "found nothing" that the tick
        // count exists to break. The reclaim count has no such problem: zero
        // entries dropped is a true statement about this process.
        if self.ticks.load(Ordering::Relaxed) > 0 {
            out.push(RawMetric::new(
                SIGNAL_GUARD_ENTRIES_METRIC,
                self.guard_entries.load(Ordering::Relaxed) as f64,
            ));
        }
        out.push(RawMetric::new(
            SIGNAL_GUARD_RECLAIMED_METRIC,
            self.guard_reclaimed.load(Ordering::Relaxed) as f64,
        ));
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The watcher is one loop per process and answers one question, so
    /// declaring no dimension is what tells the collector to pull this
    /// observable once rather than once per question.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn values(readings: &SignalWatcherReadings, name: &str) -> Vec<(Option<String>, f64)> {
        readings
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.name == name)
            .map(|m| (m.labels.get(OUTCOME_LABEL).cloned(), m.value))
            .collect()
    }

    async fn outcome_values(readings: &SignalWatcherReadings) -> Vec<(String, f64)> {
        values(readings, SIGNAL_OUTCOMES_METRIC)
            .await
            .into_iter()
            .map(|(label, value)| (label.unwrap_or_default(), value))
            .collect()
    }

    /// A process that never armed the watcher reports the role and nothing
    /// else. A zero tick count published here would read as a watcher that is
    /// running and finding nothing, which is the state this family must not
    /// confuse with "the role is held elsewhere".
    #[tokio::test]
    async fn a_process_with_no_watcher_publishes_only_the_role() {
        let readings = SignalWatcherReadings::new();
        assert_eq!(
            values(&readings, SIGNAL_WATCHER_RUNNING_METRIC).await.len(),
            1
        );
        assert_eq!(
            values(&readings, SIGNAL_WATCHER_RUNNING_METRIC).await[0].1,
            0.0
        );
        assert!(values(&readings, SIGNAL_TICKS_METRIC).await.is_empty());
        assert!(values(&readings, SIGNAL_HELD_METRIC).await.is_empty());
        assert!(values(&readings, SIGNAL_OUTCOMES_METRIC).await.is_empty());
    }

    /// A round the pool gate held off is still a round. It moves the tick count
    /// and counts one stood-down round, and neither of those is a signal: the
    /// pair is how a reader tells a watcher that is standing down from one that
    /// died -- both stop producing signals, and only the first keeps ticking.
    #[tokio::test]
    async fn a_held_round_ticks_and_counts_the_stood_down_round() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        assert_eq!(values(&readings, SIGNAL_HELD_METRIC).await[0].1, 0.0);

        readings.tick();
        readings.held_round();
        readings.tick();
        readings.held_round();
        // A round that watched: it ticks and adds nothing to the stood-down
        // count, which is what makes `ticks - held` the rounds that looked.
        readings.tick();

        assert_eq!(values(&readings, SIGNAL_TICKS_METRIC).await[0].1, 3.0);
        assert_eq!(values(&readings, SIGNAL_HELD_METRIC).await[0].1, 2.0);

        for (label, value) in outcome_values(&readings).await {
            assert_eq!(
                value, 0.0,
                "{label} moved on a round that looked at nothing"
            );
        }
    }

    /// The stood-down count is there before the first round can raise it: a zero
    /// on a running watcher says "nothing stood down", which is a claim it can
    /// make from the start. Publishing it only once the gate had closed would
    /// leave that state missing rather than zero, and a watcher that never
    /// stood down would read the same as one that never published the question.
    #[tokio::test]
    async fn the_stood_down_count_is_published_before_the_first_round() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        assert_eq!(
            values(&readings, SIGNAL_HELD_METRIC).await,
            vec![(None, 0.0)]
        );
    }

    /// Arming the watcher publishes every outcome value, before any signal has
    /// been seen. This is the property the family is for: a rule asking whether
    /// signals are being refused as duplicates reads a zero, not a missing
    /// series, so "none were refused" and "this watcher never published" are
    /// different answers.
    #[tokio::test]
    async fn every_outcome_has_a_series_before_anything_is_counted() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        assert_eq!(
            values(&readings, SIGNAL_WATCHER_RUNNING_METRIC).await[0].1,
            1.0
        );
        let outcomes = outcome_values(&readings).await;
        assert_eq!(outcomes.len(), SIGNAL_OUTCOMES.len());
        for (label, value) in &outcomes {
            assert_eq!(*value, 0.0, "{label} should start at zero");
        }
        let labels: Vec<&str> = outcomes.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(labels, SIGNAL_OUTCOMES);
    }

    /// A quiet round moves the tick count and no outcome. The pair is the
    /// reading of "no signal": the round happened, and every outcome stayed
    /// where it was.
    #[tokio::test]
    async fn a_round_with_no_signal_moves_only_the_tick_count() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        readings.tick();
        readings.tick();
        assert_eq!(values(&readings, SIGNAL_TICKS_METRIC).await[0].1, 2.0);
        for (label, value) in outcome_values(&readings).await {
            assert_eq!(value, 0.0, "{label} moved on a round that found nothing");
        }
    }

    /// Each outcome counts in its own series, and the series sum to the number
    /// of signals: the outcomes partition the signals rather than overlapping
    /// or leaving one uncounted.
    #[tokio::test]
    async fn each_outcome_counts_under_its_own_label() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        readings.record(SignalOutcome::Registered);
        readings.record(SignalOutcome::Tracked);
        readings.record(SignalOutcome::Tracked);
        readings.record(SignalOutcome::Redriven);
        readings.record(SignalOutcome::Resubmitted);
        readings.record(SignalOutcome::Failed);
        readings.record(SignalOutcome::Cooldown);

        let outcomes = outcome_values(&readings).await;
        let by_label = |want: &str| {
            outcomes
                .iter()
                .find(|(l, _)| l == want)
                .map(|(_, v)| *v)
                .unwrap_or(-1.0)
        };
        assert_eq!(by_label(OUTCOME_REGISTERED), 1.0);
        assert_eq!(by_label(OUTCOME_TRACKED), 2.0);
        assert_eq!(by_label(OUTCOME_REDRIVEN), 1.0);
        assert_eq!(by_label(OUTCOME_RESUBMITTED), 1.0);
        assert_eq!(by_label(OUTCOME_FAILED), 1.0);
        assert_eq!(by_label(OUTCOME_COOLDOWN), 1.0);
        assert_eq!(
            outcomes.iter().map(|(_, v)| *v).sum::<f64>(),
            7.0,
            "the outcomes have to account for every signal that was counted"
        );
    }

    /// The variant-to-label map and the published domain are the same list. A
    /// variant counted under a label no reader knows, or a label published that
    /// nothing can increment, would leave a signal visible in the counter and
    /// invisible in the scrape.
    /// The slot an outcome counts in is the position its name holds in the
    /// published list, and the two cover each other exactly. Without this a
    /// variant added to the enum but not the list would still be counted, into
    /// whichever slot its index landed on, and one kind of signal would be
    /// reported under another's name.
    #[test]
    fn the_slot_an_outcome_counts_in_is_the_slot_its_name_is_published_at() {
        let every = [
            SignalOutcome::Registered,
            SignalOutcome::Tracked,
            SignalOutcome::Redriven,
            SignalOutcome::Resubmitted,
            SignalOutcome::Failed,
            SignalOutcome::Cooldown,
        ];
        for outcome in every {
            assert_eq!(
                SIGNAL_OUTCOMES.get(outcome.index()),
                Some(&outcome.as_str()),
                "{outcome:?} counts in a slot its own name is not published at"
            );
        }
        let mut claimed: Vec<usize> = every.iter().map(|o| o.index()).collect();
        claimed.sort_unstable();
        assert_eq!(
            claimed,
            (0..SIGNAL_OUTCOMES.len()).collect::<Vec<_>>(),
            "every published slot has to be one an outcome counts in"
        );
    }

    #[tokio::test]
    async fn the_counted_labels_are_the_published_domain() {
        for outcome in [
            SignalOutcome::Registered,
            SignalOutcome::Tracked,
            SignalOutcome::Redriven,
            SignalOutcome::Resubmitted,
            SignalOutcome::Failed,
            SignalOutcome::Cooldown,
        ] {
            assert!(
                SIGNAL_OUTCOMES.contains(&outcome.as_str()),
                "{:?} is counted but never published",
                outcome
            );
        }
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        for outcome in [
            SignalOutcome::Registered,
            SignalOutcome::Tracked,
            SignalOutcome::Redriven,
            SignalOutcome::Resubmitted,
            SignalOutcome::Failed,
            SignalOutcome::Cooldown,
        ] {
            readings.record(outcome);
        }
        for (label, value) in outcome_values(&readings).await {
            assert_eq!(
                value, 1.0,
                "{label} is published but nothing counts under it"
            );
        }
    }

    /// The store's size is not published until a round has read the store. A
    /// zero before that would be a claim about a store nobody looked at, which
    /// is the same "have not looked" read as "it is empty" that the tick count
    /// exists to break -- while the reclaim count is a statement about this
    /// process and is honest from the start.
    #[tokio::test]
    async fn the_store_size_waits_for_a_round_and_the_reclaim_count_does_not() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        assert!(values(&readings, SIGNAL_GUARD_ENTRIES_METRIC)
            .await
            .is_empty());
        assert_eq!(
            values(&readings, SIGNAL_GUARD_RECLAIMED_METRIC).await[0].1,
            0.0
        );

        readings.tick();
        readings.guard_store(261, 0);
        assert_eq!(
            values(&readings, SIGNAL_GUARD_ENTRIES_METRIC).await[0].1,
            261.0
        );
    }

    /// The size is a set, not an accumulation. A store that only ever grew is
    /// exactly what went unnoticed, so the reading has to be able to come back
    /// down when the reclaimer runs -- and the reclaim count is what says it
    /// did, rather than the two moving together and neither one explaining.
    #[tokio::test]
    async fn the_store_size_falls_when_entries_are_reclaimed() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        readings.tick();
        readings.guard_store(261, 0);
        readings.guard_store(39, 222);

        assert_eq!(
            values(&readings, SIGNAL_GUARD_ENTRIES_METRIC).await[0].1,
            39.0
        );
        assert_eq!(
            values(&readings, SIGNAL_GUARD_RECLAIMED_METRIC).await[0].1,
            222.0
        );

        // A round that reclaimed nothing leaves the count where it was: the
        // series is cumulative, so a quiet round must not read as a reset.
        readings.guard_store(38, 0);
        assert_eq!(
            values(&readings, SIGNAL_GUARD_RECLAIMED_METRIC).await[0].1,
            222.0
        );
        assert_eq!(
            values(&readings, SIGNAL_GUARD_ENTRIES_METRIC).await[0].1,
            38.0
        );
    }

    /// A process that holds no watcher publishes none of the store's readings.
    /// Its store is not empty; it is not this process's store.
    #[tokio::test]
    async fn a_process_with_no_watcher_publishes_no_store_reading() {
        let readings = SignalWatcherReadings::new();
        readings.guard_store(5, 5);
        assert!(values(&readings, SIGNAL_GUARD_ENTRIES_METRIC)
            .await
            .is_empty());
        assert!(values(&readings, SIGNAL_GUARD_RECLAIMED_METRIC)
            .await
            .is_empty());
    }

    /// The tick count is the denominator the outcomes are read against, so a
    /// round that finds nothing has to leave the sum of the outcomes below it.
    /// If the two ever matched on a quiet round, "no signal" would be
    /// unreadable from the counters alone.
    #[tokio::test]
    async fn a_quiet_round_leaves_the_outcome_sum_below_the_tick_count() {
        let readings = SignalWatcherReadings::new();
        readings.mark_running();
        readings.tick();
        readings.record(SignalOutcome::Cooldown);
        readings.tick();

        let ticks = values(&readings, SIGNAL_TICKS_METRIC).await[0].1;
        let signals: f64 = outcome_values(&readings)
            .await
            .iter()
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(ticks, 2.0);
        assert_eq!(signals, 1.0);
        assert!(
            ticks - signals == 1.0,
            "the gap is the count of rounds that found no signal"
        );
    }
}
