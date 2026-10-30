//!The resource account's event half: what moved a bound and what a gate turned
//!away, carried out of the process that saw it and onto durable ground.
//!
//!The account is two halves, and they are kept apart on purpose. Values live
//!where they are produced -- what we asked for is a line in the chart, what the
//!cluster admits is a projection of it, what the host is running with is a
//!reading -- so no table here holds a bound. Events are the other half: a bound
//!the control law moved at runtime left no commit behind, and a refusal exists
//!only as a fact with an identity and an instant. Both die with the pod unless
//!something carries them out, and the in-process ring that holds them between
//!control ticks is bounded, so a process that sits out a pass loses the fact
//!rather than delaying it.
//!
//!This loop is that carrier, and it plays the four parts the other accounts
//!play: the table is the authoritative copy, the loop is the resident reader,
//!it is the consumer that folds what it reads, and the fold marker is the write
//!back. The parts are split across the lease in one place, deliberately: the
//!seat guards the *reading*, not the writing. Two processes' rings hold
//!disjoint facts -- each saw its own refusals -- so refusing to append because
//!another process holds the seat would discard exactly the events the seat
//!exists to preserve. Appends are additive and deduplicated by identity, which
//!is what makes writing without the seat safe; the census, the fold and the
//!published series have one author, which is what the seat is actually for.
//!
//!What a pass publishes is read back out of the table rather than counted in
//!this process. A count kept here would start at zero on every restart and
//!answer "nothing ever happened" to the one question the account exists for.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::{debug, info, warn};

use cog_core::{
    metric_names, MetricsBackend, OwnerLeaseBroker, ResourceEventStore, ShutdownSignal,
};

/// This loop's name in the process liveness registry, and the value of
/// `cogneva_loop_tick_age_seconds{loop=...}`.
pub const RESOURCE_LEDGER_LOOP: &str = "resource_ledger";

/// This loop's role name on the owner lease: who reads and folds the account.
///
/// The same value as the loop name, but the two read different surfaces: the
/// loop name labels the liveness and ownership readings (`{loop=...}`), the role
/// name is the row in `cogneva_owner_leases.role`.
pub const RESOURCE_LEDGER_ROLE: &str = "resource_ledger";

/// How long one pass waits for the next.
///
/// The events are produced by a control loop that ticks twice a second, and the
/// ring between them and this loop holds a few hundred events. Ten seconds is
/// therefore a wide margin: the ring would have to see
/// more than twenty-five events a second to overflow between two passes, and a
/// refusal is one line of work, not a stream. Shorter would put a query against
/// the shared database on every few control ticks for an account that is read
/// by people; longer starts to look like the ring's bound.
// cap: bound
pub const RESOURCE_LEDGER_PASS_INTERVAL: Duration = Duration::from_secs(10);

/// How many queued events one pass folds.
///
/// A pass is bounded so a backlog cannot hold the seat for an unbounded time,
/// but generous, because the common case is two or three events and anything
/// that reached this bound is a producer that looped rather than a corpus being
/// read.
// cap: bound
const FOLD_PER_PASS: usize = 500;

/// How many events this loop holds on to when the account cannot be written.
///
/// A failed append must not be a lost event: the ring was drained before the
/// write, so the events exist nowhere else. They are carried to the next pass
/// and written then; the bound exists because a database that stays unreachable
/// must not turn this loop into an unbounded buffer. Four rings' worth is
/// chosen so the carry-over only fills when the account has been unwritable for
/// far longer than any transient, and what it gives up at the bound is said in
/// the log rather than counted as a ring drop, because the two losses are
/// different facts: one is a producer outrunning its consumer, the other is a
/// consumer that cannot reach its store at all.
// cap: bound
const CARRY_CAPACITY: usize = 1024;

/// How many recent events one pass looks at when it says what it saw.
///
/// Only for the log line, so a bound that keeps a few is enough; the table is
/// what a reader who wants the whole thing reads.
// cap: bound
const RECENT_PEEK: usize = 5;

/// One pass: carry this process's events out, then read and fold the account.
///
/// Split this way and not the other way round: the carry has to happen even
/// when this process does not hold the seat (see the module header), while the
/// reading half must not.
async fn pass(
    store: &dyn ResourceEventStore,
    metrics: Option<&Arc<dyn MetricsBackend>>,
    seated: bool,
    carry: &mut Vec<cog_core::ResourceEvent>,
    drained: Vec<cog_core::ResourceEvent>,
) {
    carry.extend(drained);
    let overflow = carry.len().saturating_sub(CARRY_CAPACITY);
    if overflow > 0 {
        carry.drain(..overflow);
        warn!(
            overflow,
            held = carry.len(),
            "resource account: the account has been unwritable long enough that this loop's own \
             buffer of events overflowed, and the oldest were dropped -- they existed nowhere else, \
             so they are gone. This is not a producer outrunning its consumer; it is a store this \
             process cannot reach, and it stays as long as that does"
        );
    }
    if !carry.is_empty() {
        match store.append_events(carry.as_slice()).await {
            // The whole buffer is cleared only on success. A store that failed
            // partway through leaves the batch carried over and rewritten next
            // pass; re-appending is safe because identity is the event's
            // content, so a row already written takes the retry as its own and
            // its count grows rather than a second row appearing.
            Ok(0) => {
                carry.clear();
                debug!(
                    "resource account: every event this process carried was already in the \
                     account, so a reporter that retried after a restart did not write a second row"
                );
            }
            Ok(inserted) => {
                carry.clear();
                debug!(
                    inserted,
                    "resource account: events carried out of the build gate"
                );
            }
            Err(e) => warn!(
                error = %e,
                held = carry.len(),
                "resource account: the events this process saw could not be written down this \
                 pass; they are held and retried rather than dropped, so the account falls behind \
                 instead of forgetting"
            ),
        }
    }

    if !seated {
        return;
    }

    match store.fold_events(FOLD_PER_PASS).await {
        Ok(folded) if folded.len() >= FOLD_PER_PASS => info!(
            folded = folded.len(),
            "resource account: a full pass of events was folded and more are waiting, so the \
             backlog is growing faster than one pass consumes it"
        ),
        Ok(folded) => debug!(folded = folded.len(), "resource account: events folded"),
        Err(e) => warn!(
            error = %e,
            "resource account: this pass could not fold the queue, so the rows stay unfolded and \
             the pending count keeps saying so"
        ),
    }

    publish_account_once(store, metrics).await;
}

/// Read what the account holds and put it on the metrics face.
///
/// Every number here is read back out of the table. The dropped count is the
/// one exception, and it is a counter for that reason: a lost event leaves no
/// row to count, so the only place the loss exists is the producer's own tally,
/// and a counter is what survives a restart of that producer.
async fn publish_account_once(
    store: &dyn ResourceEventStore,
    metrics: Option<&Arc<dyn MetricsBackend>>,
) {
    let Some(metrics) = metrics else {
        return;
    };

    match store.census().await {
        Ok(census) => {
            for (kind, outcome, count) in census {
                let mut labels = HashMap::new();
                labels.insert(crate::LABEL_KIND.to_string(), kind.as_str().to_string());
                labels.insert(
                    crate::LABEL_OUTCOME.to_string(),
                    outcome.as_str().to_string(),
                );
                if let Err(e) = metrics
                    .record_gauge(metric_names::RESOURCE_EVENTS, count as f64, labels)
                    .await
                {
                    warn!(
                        error = %e,
                        kind = kind.as_str(),
                        outcome = outcome.as_str(),
                        "resource account: this census cell could not be published, so the reading \
                         face is missing a cell the account holds"
                    );
                }
            }
        }
        Err(e) => warn!(
            error = %e,
            "resource account: the census could not be read this pass, so the reading face keeps \
             the last cells it had -- which is the shape of a loop that has stopped counting"
        ),
    }

    match store.pending_events().await {
        Ok(pending) => {
            if let Err(e) = metrics
                .record_gauge(
                    metric_names::RESOURCE_EVENTS_PENDING,
                    pending as f64,
                    HashMap::new(),
                )
                .await
            {
                warn!(error = %e, "resource account: the pending count could not be published");
            }
        }
        Err(e) => warn!(
            error = %e,
            "resource account: the pending count could not be read, so \"is anything consuming \
             this account\" has no answer this pass"
        ),
    }

    // The age of the oldest unfolded event is what turns the pending count from
    // a number into a verdict: a handful of events from the last few seconds is
    // a queue working, the same handful from yesterday is a consumer that is
    // not. Absent rather than zero when nothing is pending, because a zero would
    // claim an event is waiting right now.
    match store.oldest_unfolded_at().await {
        Ok(Some(oldest)) => {
            let age = (Utc::now() - oldest).num_seconds().max(0) as f64;
            if let Err(e) = metrics
                .record_gauge(
                    metric_names::RESOURCE_EVENTS_OLDEST_UNFOLDED_AGE_SECONDS,
                    age,
                    HashMap::new(),
                )
                .await
            {
                warn!(
                    error = %e,
                    "resource account: the age of the oldest unfolded event could not be published"
                );
            }
        }
        Ok(None) => {}
        Err(e) => warn!(
            error = %e,
            "resource account: the age of the oldest unfolded event could not be read"
        ),
    }

    if let Ok(recent) = store.recent(RECENT_PEEK).await {
        for row in &recent {
            debug!(
                kind = row.event.kind.as_str(),
                outcome = row.event.outcome.as_str(),
                subject = row.event.subject.as_str(),
                actor = row.event.actor.as_str(),
                reading = row.event.reading.as_str(),
                from_value = ?row.event.from_value,
                to_value = ?row.event.to_value,
                count = row.event.count,
                occurred_at = %row.event.occurred_at,
                folded = row.folded_at.is_some(),
                "resource account: recent event"
            );
        }
    }
}

/// Give "has never dropped anything" a row of its own.
///
/// [`publish_drops_once`] only writes when the running total moved, so a
/// process that never lost an event leaves no row at all -- and a missing row
/// is the same shape, on every surface that reads this counter, as a counter
/// nobody wired up. One of those states needs nothing done and the other needs
/// the producer fixed, so a reader cannot tell which it is looking at. A zero
/// written once at start separates them: a row holding zero means the question
/// was asked and the answer was none, no row means nobody asked.
///
/// Once, not every pass: the sample table is append-only and capped, so a zero
/// per tick would push other series' history out of it. The counter contract
/// states that a zero write adds to whatever total the series already holds
/// rather than assigning to it, which is what makes the seed safe to repeat
/// across restarts.
async fn seed_drops(metrics: Option<&Arc<dyn MetricsBackend>>) {
    let Some(metrics) = metrics else {
        return;
    };
    if let Err(e) = metrics
        .record_counter(
            metric_names::RESOURCE_EVENTS_DROPPED_TOTAL,
            0.0,
            HashMap::new(),
        )
        .await
    {
        // A failed seed is not a failure to do the work; it is this one grid
        // going back to being absent, and absent reads as "never dropped".
        warn!(
            error = %e,
            "resource account: the zero seed for dropped events was not written, so a process \
             that has dropped nothing will read the same as one that never wired the counter"
        );
    }
}

/// Publish the events this process could not hand over in time.
///
/// Called once per pass with the producer's running total, so the counter is
/// incremented by the difference rather than by the total: an event pushed out
/// of the ring is lost once, and a total added every pass would count it as a
/// new loss every ten seconds.
async fn publish_drops_once(metrics: Option<&Arc<dyn MetricsBackend>>, reported: &mut u64) {
    let Some(metrics) = metrics else {
        return;
    };
    let total = cog_core::contract::build_gate::dropped_resource_events();
    if total <= *reported {
        return;
    }
    let delta = total - *reported;
    if let Err(e) = metrics
        .record_counter(
            metric_names::RESOURCE_EVENTS_DROPPED_TOTAL,
            delta as f64,
            HashMap::new(),
        )
        .await
    {
        // The tally is not advanced, so the next pass retries this difference
        // rather than dropping it: a loss that was never reported is a loss no
        // one will look for.
        warn!(error = %e, delta, "resource account: dropped events could not be published");
        return;
    }
    *reported = total;
}

/// Start the loop that carries, folds and publishes the resource account.
///
/// Started only where the event store was published, and not being started is
/// not a fault: a deployment without a shared database has nowhere to put a
/// refusal, and the process still counts it in its own reading.
pub fn spawn_resource_ledger_loop(
    store: Arc<dyn ResourceEventStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    shutdown: ShutdownSignal,
) {
    info!(
        interval_secs = RESOURCE_LEDGER_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        readable = metrics.is_some(),
        "Resource ledger loop enabled"
    );
    if metrics.is_none() {
        warn!(
            "resource account: no metrics backend is published, so the account is carried and \
             folded and nothing is published -- a refusal will be visible only to whoever opens \
             the table"
        );
    }

    drop(cog_core::loop_health::spawn(
        RESOURCE_LEDGER_LOOP,
        cog_core::loop_health::Cadence::Periodic(RESOURCE_LEDGER_PASS_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let metrics = metrics.clone();
            let shutdown = shutdown.clone();
            async move {
                let hold =
                    cog_core::RoleHold::start(role, RESOURCE_LEDGER_ROLE, beat.clone(), &shutdown);

                // What this process has already reported as dropped. Counted
                // here rather than in the gate so that the counter is only ever
                // advanced by what was actually written down.
                //
                // Zero, not the gate's current total, even though a body that
                // panics is run again while the tally it reads survives: the
                // restart then publishes the whole total a second time. That is
                // the error to prefer. Seeding from the total would instead
                // swallow whatever this process dropped before the first pass
                // could write it -- and a counter whose silent failure is
                // "the loss never happened" is worse than one that says twice
                // that something did.
                let mut reported_drops: u64 = 0;

                // Events the account refused to take, kept so that a database
                // that is briefly unreachable costs latency rather than facts.
                let mut carry: Vec<cog_core::ResourceEvent> = Vec::new();

                // Before the loop and outside it: before, so the series exists
                // before the first pass can publish a difference; outside,
                // because the sample table keeps every row it is given and a
                // zero per tick would be a row written every ten seconds that
                // says nothing happened. This body runs once -- only the
                // `loop` below repeats -- which is what makes it the place for
                // a one-time write, and it is inside the loop's liveness
                // registration, so a seed that never completes is visible as a
                // stalled loop rather than as an orphan task.
                seed_drops(metrics.as_ref()).await;

                let mut ticker = tokio::time::interval(RESOURCE_LEDGER_PASS_INTERVAL);
                // The first tick is immediately ready, and this loop is the
                // only thing standing between a control tick's refusal and the
                // ring's bound, so it eats the tick rather than waiting a
                // period: a process that just started has no backlog of its own
                // to wait out.
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    let seated = hold.may_act().await;
                    // Drained before the seat is consulted, and unconditionally:
                    // see the module header for why the writing half does not
                    // wait for it.
                    let events = cog_core::contract::build_gate::drain_resource_events();
                    pass(&*store, metrics.as_ref(), seated, &mut carry, events).await;
                    publish_drops_once(metrics.as_ref(), &mut reported_drops).await;
                }
            }
        },
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    use cog_core::{
        ResourceEvent, ResourceEventKind, ResourceEventOutcome, ResourceEventRow, SFResult,
    };

    /// A store that remembers what it was handed, so the pass can be asked what
    /// it did without a database.
    #[derive(Default)]
    struct RecordingStore {
        appended: std::sync::Mutex<Vec<ResourceEvent>>,
        folded: std::sync::Mutex<usize>,
        /// When set, every append fails, so the carry-over path can be judged.
        unwritable: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ResourceEventStore for RecordingStore {
        async fn append_events(&self, events: &[ResourceEvent]) -> SFResult<usize> {
            if self.unwritable.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            self.appended.lock().unwrap().extend_from_slice(events);
            Ok(events.len())
        }
        async fn fold_events(&self, _limit: usize) -> SFResult<Vec<ResourceEventRow>> {
            *self.folded.lock().unwrap() += 1;
            Ok(Vec::new())
        }
        async fn pending_events(&self) -> SFResult<u64> {
            Ok(0)
        }
        async fn oldest_unfolded_at(&self) -> SFResult<Option<DateTime<Utc>>> {
            Ok(None)
        }
        async fn census(&self) -> SFResult<Vec<(ResourceEventKind, ResourceEventOutcome, u64)>> {
            Ok(Vec::new())
        }
        async fn recent(&self, _limit: usize) -> SFResult<Vec<ResourceEventRow>> {
            Ok(Vec::new())
        }
    }

    fn event(subject: &str) -> ResourceEvent {
        ResourceEvent {
            kind: ResourceEventKind::AdmissionRefusal,
            occurred_at: Utc::now(),
            subject: subject.to_string(),
            actor: "test".to_string(),
            reading: "cogneva_build_gate_slots".to_string(),
            reading_value: Some(1.0),
            from_value: None,
            to_value: None,
            outcome: ResourceEventOutcome::Refused,
            detail: serde_json::json!({}),
            count: 1,
        }
    }

    /// Counters only: the seed and the delta are the two writes this module
    /// makes, and both go through `record_counter`.
    #[derive(Default)]
    struct RecordingCounters {
        counters: std::sync::Mutex<Vec<(String, f64)>>,
    }

    impl RecordingCounters {
        fn writes(&self) -> Vec<(String, f64)> {
            self.counters.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MetricsBackend for RecordingCounters {
        async fn record_gauge(
            &self,
            _: cog_core::MetricName,
            _: f64,
            _: HashMap<String, String>,
        ) -> SFResult<()> {
            unimplemented!()
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            value: f64,
            _: HashMap<String, String>,
        ) -> SFResult<()> {
            self.counters
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value));
            Ok(())
        }
        async fn record_histogram(
            &self,
            _: cog_core::MetricName,
            _: f64,
            _: HashMap<String, String>,
        ) -> SFResult<()> {
            unimplemented!()
        }
        async fn query_gauge_range(
            &self,
            _: &str,
            _: DateTime<Utc>,
            _: DateTime<Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_gauge_latest(&self, _: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_counter_range(
            &self,
            _: &str,
            _: DateTime<Utc>,
            _: DateTime<Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_counter_totals(&self, _: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_histogram_totals(
            &self,
            _: &str,
        ) -> SFResult<Vec<cog_core::HistogramTotals>> {
            unimplemented!()
        }
        async fn query_histogram_range(
            &self,
            _: &str,
            _: DateTime<Utc>,
            _: DateTime<Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn list_metric_names(&self, _: cog_core::MetricType) -> SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn health_check(&self) -> SFResult<()> {
            Ok(())
        }
    }

    /// 「从没丢过」与「这一格没接线」在读数面上同形，种子就是分开它们的那一行。
    /// 没有这次改动，一个从没丢过事件的进程在这条序列上零行，而读的人会把它
    /// 读成生产者缺席。
    #[tokio::test]
    async fn a_process_that_dropped_nothing_still_publishes_the_series() {
        let counters = Arc::new(RecordingCounters::default());
        let metrics: Arc<dyn MetricsBackend> = counters.clone();

        seed_drops(Some(&metrics)).await;

        let writes = counters.writes();
        assert_eq!(writes.len(), 1, "the seed wrote more than one series");
        assert_eq!(
            writes[0].0,
            metric_names::RESOURCE_EVENTS_DROPPED_TOTAL.as_str()
        );
        assert_eq!(writes[0].1, 0.0, "the seed must report no loss");
    }

    /// 种子只播一次：样本表 append-only 且有容量上限，每拍写一个零会挤掉别人的历史。
    /// 所以种子之后的那一拍，在没有新损失时不能再写。
    #[tokio::test]
    async fn a_pass_after_the_seed_writes_nothing_more() {
        let counters = Arc::new(RecordingCounters::default());
        let metrics: Arc<dyn MetricsBackend> = counters.clone();
        seed_drops(Some(&metrics)).await;

        // 起手就把此刻的累计当作已经播过，于是这一拍没有差值可播。
        let mut reported = cog_core::contract::build_gate::dropped_resource_events();
        publish_drops_once(Some(&metrics), &mut reported).await;

        let writes = counters.writes();
        assert_eq!(
            writes.len(),
            1,
            "a pass with nothing to report wrote the seed again: {writes:?}"
        );
    }

    /// The seat guards the reading, not the writing: a process that does not
    /// hold it still has to hand over what it saw. Losing that would be losing
    /// the refusals of every replica that is not the elected one.
    #[tokio::test]
    async fn an_unseated_pass_still_carries_its_events_out() {
        let store = RecordingStore::default();
        let produced = event("builder:wall");
        let mut carry = Vec::new();

        pass(&store, None, false, &mut carry, vec![produced.clone()]).await;

        let appended = store.appended.lock().unwrap();
        assert_eq!(appended.len(), 1, "the unseated pass dropped the event");
        assert_eq!(appended[0].subject, produced.subject);
        assert!(
            carry.is_empty(),
            "a written event was left in the carry buffer"
        );
        assert_eq!(
            *store.folded.lock().unwrap(),
            0,
            "an unseated pass folded rows"
        );
    }

    /// The other half: a seated pass folds. Together with the test above this
    /// pins the split -- either half alone would pass a loop that did both or
    /// neither.
    #[tokio::test]
    async fn a_seated_pass_folds_the_queue() {
        let store = RecordingStore::default();
        pass(&store, None, true, &mut Vec::new(), Vec::new()).await;
        assert_eq!(*store.folded.lock().unwrap(), 1);
    }

    /// A pass with nothing to carry must not turn that into an append of an
    /// empty list, which some stores would answer with an error and a warning
    /// every ten seconds.
    #[tokio::test]
    async fn a_pass_with_no_events_appends_nothing() {
        let store = RecordingStore::default();
        pass(&store, None, true, &mut Vec::new(), Vec::new()).await;
        assert!(store.appended.lock().unwrap().is_empty());
    }

    /// A store that is unreachable for one pass costs latency, not facts: the
    /// events stay in the buffer and go in on the next pass. Driven through
    /// the same entry point as the other three, so the test is about the pass
    /// and not about a helper the pass does not use.
    #[tokio::test]
    async fn an_event_the_store_refused_is_retried_rather_than_lost() {
        let store = RecordingStore::default();
        store
            .unwritable
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut carry = Vec::new();

        pass(&store, None, false, &mut carry, vec![event("builder:busy")]).await;
        assert_eq!(carry.len(), 1, "the refused event was dropped");
        assert!(store.appended.lock().unwrap().is_empty());

        store
            .unwritable
            .store(false, std::sync::atomic::Ordering::Relaxed);
        pass(&store, None, false, &mut carry, Vec::new()).await;
        assert!(carry.is_empty(), "the retried event stayed in the buffer");
        assert_eq!(store.appended.lock().unwrap().len(), 1);
    }
}
