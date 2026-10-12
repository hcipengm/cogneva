//! The resident maintenance loop for the change ledger.
//!
//! The ledger's authoritative copy is the platform's own `change_ledger` table,
//! which nobody edits by hand: one row per change, identity `kind:scope:literal`,
//! and a state only a measurement may move. What maintains it is this resident
//! interval loop -- registered in the main process's plugin and elected through
//! the owner lease, so the table has exactly one writer at any moment.
//!
//! As with the memory-decay loop, a process that is not qualified for the seat
//! is excluded by *not starting this loop* rather than by the lease refusing it:
//! a process whose storage holds no shared database never reaches this point, so
//! it never becomes a candidate.
//!
//! Four things happen here, and the third is the only one that moves a row. The
//! loop folds the intake into the account -- declarations queued by other parties
//! become readings, through the same reading path a direct write would use -- it
//! reads the outstanding set back out, saying plainly when a read failed, and it
//! measures that set against the tree, through the one call that may move a
//! state. The fold is mechanical: it carries what a document asked for and adds
//! nothing of its own, so this loop cannot make a change look done by folding.
//!
//! The fourth thing is what makes the ledger a driver rather than a record: the
//! rows a measurement found `pending` are handed to the goal path, one task
//! each, so "the ledger says this is still owed" is where a change gets worked
//! on from. Handing over is not finishing. A dispatched change keeps its state
//! and only a later measurement -- of the tree, after someone has changed it --
//! can move the row, so a task that reported success changes nothing by itself.
//!
//! A row whose check could not be run keeps the state a document gave it and is
//! named in the pass's own line, so "nothing has looked at this" and "something
//! looked and could not tell" stay apart -- and the count of the second is a
//! reading rather than a silence. Not being able to tell is not final either:
//! such a row is asked again after [`UNMEASURABLE_RETRY`], because why it could
//! not be settled is a property of a tree that moves.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use cog_core::{
    ChangeIdentity, ChangeLedgerRow, ChangeLedgerStore, ChangeMeasurement, ChangeState,
    OrchestratorControl, OwnerLeaseBroker, ShutdownSignal, Task, TaskType,
};

use crate::change_measure::{CheckTree, UnsettledReason};

/// This loop's name in the process liveness registry, and the value of
/// `cogneva_loop_tick_age_seconds{loop=...}`.
pub const CHANGE_LEDGER_LOOP: &str = "change_ledger";

/// This loop's role name on the owner lease: who maintains the ledger.
///
/// The same value as the loop name, but the two read different surfaces: the
/// loop name labels the liveness and ownership readings (`{loop=...}`), the role
/// name is the row in `cogneva_owner_leases.role`. They agree now; changing one
/// is not changing the other.
pub const CHANGE_LEDGER_ROLE: &str = "change_ledger";

/// How long one round waits for the next.
///
/// The ledger moves on two events only: a document being read into it, or a
/// change being measured again. Both are human-driven and measured in hours, so
/// this period is not "how fast do changes arrive" but "how long before someone
/// notices it stopped being maintained". Minutes rather than seconds keeps the
/// steady state out of the log. The deployed liveness alert derives its
/// threshold from this period (`6 * period`), so changing it changes that
/// threshold.
pub const CHANGE_LEDGER_PASS_INTERVAL: Duration = Duration::from_secs(300);

/// How many outstanding rows one round reads.
///
/// The round exists for one thing: to make "the ledger is readable, and this is
/// how much is outstanding" a line in the log with a reading behind it. It is
/// not a work list, so the bound is a fixed small number; a round that does hand
/// work over will pick its own batch size from its own throttle rather than
/// reusing this one.
const OUTSTANDING_PEEK: usize = 50;

/// How many queued declarations one pass folds into the account.
///
/// A pass is a maintenance tick, not a queue drain, so it takes a bounded slice
/// and the next tick takes the rest: an intake that grew large must not make a
/// single tick unbounded, and a tick that ran long would hold the owner lease
/// past the cadence it is renewed on. The queue is written by a person or a
/// script, in batches of tens, so this bound is generous for how it is used --
/// reaching it means the fold is behind, which the backlog reading says anyway.
const DECLARATIONS_PER_PASS: usize = 200;

/// How many rows one pass measures against the tree.
///
/// The ledger holds rows in the thousands and a pass that walked all of them
/// would spend the whole interval asking git questions while holding the owner
/// lease. Rows are taken stalest-first, so this bound costs latency rather than
/// coverage: everything outstanding is still reached, in as many passes as it
/// takes.
const MEASUREMENTS_PER_PASS: usize = 200;

/// How long a check that could not be settled is left before it is asked again.
///
/// Held alone it would be permanent, and a permanent answer to a question about
/// a moving tree is the wrong shape: the check could not be settled because of
/// what the tree holds right now, and a tree that gains the path makes it
/// answerable. An hour is long enough that re-asking costs nothing (the pass
/// runs every few minutes and the unanswered set is a few hundred rows) and
/// short enough that a tree that changed is noticed within the same working
/// session.
const UNMEASURABLE_RETRY: Duration = Duration::from_secs(3600);

/// A check this process could not settle, and when it last tried.
///
/// The reason is kept so a repeat is not said twice, and the time so that the
/// silence is a window rather than a decision. Both are this process's memory of
/// what it already said, not facts about the account, so a restarting process
/// re-learns them by asking again -- the honest way to recover, since what makes
/// a check unsettleable is a property of the tree and can change.
#[derive(Debug)]
struct UnsettledCheck {
    why: String,
    since: Instant,
}

/// The prefix a dispatched task's id carries, so a task sitting in the graph
/// says which loop put it there.
const DISPATCH_TASK_PREFIX: &str = "change-ledger-";

/// The task kind a dispatched change is submitted under.
///
/// A name of the platform's own flows rather than a kind of hand: the change is
/// not handed to a generic worker, it is handed to the change-generation path,
/// and the kind is what that path is looked up by.
const DISPATCH_TASK_KIND: &str = "change_ledger";

/// The task id a change is dispatched under.
///
/// Derived from the identity, because that is the whole mechanism: this loop
/// re-reads the same outstanding set every few minutes, and an id that moved
/// between passes would file the same work again on every one of them.
///
/// Hashed rather than rendered from the parts. A literal is source code and
/// routinely contains the separator a flattened identity is made of, so two
/// different changes can render the same string -- and a shared id is not a
/// duplicate cost, it is a change that is never dispatched at all, because the
/// second one finds the first one's row in the graph and takes it for its own.
/// Each part is terminated as it is fed in, so no split of the same characters
/// hashes the same either.
fn dispatch_task_id(identity: &ChangeIdentity) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [&identity.kind, &identity.scope, &identity.literal] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    let hex = hasher.finalize().to_hex();
    format!("{DISPATCH_TASK_PREFIX}{}", &hex[..16])
}

/// What a dispatched change is worked on as.
///
/// The change's own words, in one line: the title a reading restated it with,
/// or failing that what the change asserts. An identity is the last resort
/// rather than the first, because a goal that is only `grep:path:literal` tells
/// the generator where to look and not what is wanted there.
fn dispatch_goal(row: &ChangeLedgerRow) -> String {
    let text = row
        .title
        .as_deref()
        .or(row.assertion.as_deref())
        .unwrap_or("");
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        row.identity.as_key()
    } else {
        text
    }
}

/// Start the loop. Not started when there is no ledger store (see the module
/// header).
///
/// `orchestrator` is the goal path the pending set is handed to. It is optional
/// for the same reason the lease role is: a process that cannot submit a goal
/// still keeps an account, and refusing to run the loop at all would trade a
/// missing driver for a missing ledger. Where it is absent the pass says so
/// once, by name, rather than skipping the handover silently.
pub fn spawn_change_ledger_loop(
    store: Arc<dyn ChangeLedgerStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    shutdown: ShutdownSignal,
    tree: CheckTree,
) {
    info!(
        interval_secs = CHANGE_LEDGER_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        dispatching = orchestrator.is_some(),
        "Change ledger loop enabled"
    );
    if orchestrator.is_none() {
        warn!(
            "change ledger: no goal path is available, so a change found still owed will be \
             recorded and not handed to anyone"
        );
    }

    drop(cog_core::loop_health::spawn(
        CHANGE_LEDGER_LOOP,
        cog_core::loop_health::Cadence::Periodic(CHANGE_LEDGER_PASS_INTERVAL),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let orchestrator = orchestrator.clone();
            let shutdown = shutdown.clone();
            let tree = tree.clone();
            async move {
                // The role is renewed on the lease contract's own cadence rather
                // than once per pass: a pass can be long, and a term that ran out
                // mid-pass would hand the seat to a second process working the
                // same table. The loop still asks once per pass, because a role
                // lost has to stop the work in the pass it was lost.
                let hold =
                    cog_core::RoleHold::start(role, CHANGE_LEDGER_ROLE, beat.clone(), &shutdown);

                // Checks a look could not settle, by identity, with the reason
                // it could not and when it was last tried.
                let mut unsettled: HashMap<ChangeIdentity, UnsettledCheck> = HashMap::new();

                let mut ticker = tokio::time::interval(CHANGE_LEDGER_PASS_INTERVAL);
                // The first tick is immediately ready; eating it puts the first
                // read one period after the start rather than contending with
                // startup for the same table.
                ticker.tick().await;
                loop {
                    beat.beat();
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = shutdown.wait() => break,
                    }
                    // One process at a time works this table. The answer is
                    // recorded on the beat (held / held elsewhere / unprovable);
                    // this only obeys it, and obeys it strictly: unprovable also
                    // means do not act, because not knowing that nobody holds the
                    // role is not the same as knowing it.
                    if !hold.may_act().await {
                        continue;
                    }
                    read_pass(&*store).await;
                    measure_pass(&*store, &tree, &mut unsettled).await;
                    // Handing over comes last, and that order is the point: the
                    // set dispatched is then the set as of this pass, so a change
                    // this pass just found in the tree is never handed to anyone,
                    // and a change this pass just found missing is handed over on
                    // the same pass that found it, rather than one period later.
                    if let Some(ref orchestrator) = orchestrator {
                        dispatch_pass(&*store, orchestrator).await;
                    }
                }
            }
        },
    ));
}

/// One round: read the ledger's outstanding set, record what was read, and say
/// so plainly when nothing could be.
///
/// A failed read is neither raised nor retried here: in a periodic loop the next
/// round is the retry, and repeated failure is never silent -- every round
/// leaves a warning.
async fn read_pass(store: &dyn ChangeLedgerStore) {
    fold_pass(store).await;

    match store.outstanding(OUTSTANDING_PEEK).await {
        Ok(rows) if rows.is_empty() => {
            debug!("change ledger read: nothing outstanding");
        }
        Ok(rows) => {
            info!(summary = %outstanding_summary(&rows), "change ledger read");
        }
        Err(e) => {
            warn!(error = %e, "change ledger read failed; this cycle read nothing");
        }
    }
}

/// Fold what has been queued into the account, then say how much is still
/// queued.
///
/// Folding first is what makes a change appear in the same pass's outstanding
/// reading; the backlog is read either way, because a backlog that stays non-zero
/// across passes is the one reading that says the account has not caught up with
/// what it was told. Neither the fold's failure nor the count's is raised or
/// retried -- the next pass is the retry, and every pass leaves a line.
async fn fold_pass(store: &dyn ChangeLedgerStore) {
    match store.fold_declarations(DECLARATIONS_PER_PASS).await {
        Ok(0) => {}
        Ok(folded) => info!(
            folded,
            "change ledger: declarations folded into the account"
        ),
        Err(e) => warn!(
            error = %e,
            "change ledger: folding the intake failed; nothing entered the account this pass"
        ),
    }
    match store.pending_declarations().await {
        Ok(0) => {}
        Ok(pending) => info!(
            pending,
            "change ledger: declarations queued and not yet folded"
        ),
        Err(e) => warn!(error = %e, "change ledger: could not read the intake backlog"),
    }
}

/// Measure the outstanding set against the tree, and record what each row came
/// back as.
///
/// The snapshot is resolved once, before any check runs, so every row in the pass
/// answers for one revision: a ref resolved per row would let a push land in the
/// middle and leave half the pass describing one tree and half another, which
/// reads as a burst of disagreements rather than as a tree that moved.
///
/// A row whose check could not be settled is left exactly as it is. Its state is
/// what a document said, and this pass has nothing to put in its place; what it
/// does have is the reason, which is said once per identity and then counted, so
/// the steady state is a number rather than a wall of repeats.
///
/// Leaving a row alone is not the same as giving up on it. What could not be
/// settled is a property of the tree, and the tree moves: a path that is not
/// there this hour can be there the next. So an identity is unanswered for
/// [`UNMEASURABLE_RETRY`] and then asked again, which is what keeps "we cannot
/// tell" from hardening into a row that is never looked at again.
///
/// Nothing here is raised: a pass is a maintenance tick, the next tick is the
/// retry, and every tick leaves a line either way.
async fn measure_pass(
    store: &dyn ChangeLedgerStore,
    tree: &CheckTree,
    unsettled: &mut HashMap<ChangeIdentity, UnsettledCheck>,
) {
    // The batch is read before the tree is opened, so that a revision which
    // cannot be resolved still reports how many rows it left unmeasured. Opening
    // the tree first and returning on failure would leave that pass with a
    // warning and no count, which is the one shape a reader cannot tell from a
    // pass with nothing to do.
    //
    // A row that cannot be settled keeps its `measured_at`, so it holds the head
    // of a staleness-ordered read on every pass. Asking for the known ones on top
    // of the batch is what stops them crowding out the rows a look can answer
    // for: without it, a large enough unanswerable set would mean nothing below
    // it is ever reached.
    let take = MEASUREMENTS_PER_PASS.saturating_add(unsettled.len());
    let rows = match store.outstanding(take).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the outstanding set could not be read, so nothing was measured this pass"
            );
            return;
        }
    };

    let now = Instant::now();
    let snapshot = match tree.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(why) => {
            // One fault, one reason, every row that this pass would have judged:
            // a revision that cannot be resolved must not read as a pass with
            // nothing outstanding. Not remembered in `unsettled` either, because
            // this is not a fact about any one change -- it is the tree being
            // unreadable, and it is re-asked in full on every pass until it is
            // not.
            warn!(
                why = %why,
                reason = UnsettledReason::RevisionUnreadable.as_str(),
                rows = would_judge(&rows, unsettled, now),
                "change ledger: the tree could not be opened, so nothing was measured this pass"
            );
            return;
        }
    };

    let mut landed = 0usize;
    let mut absent = 0usize;
    let mut by_reason: HashMap<UnsettledReason, usize> = HashMap::new();
    let mut known_unsettled = 0usize;
    let mut considered = 0usize;
    for row in rows {
        if let Some(known) = unsettled.get(&row.identity) {
            if still_unanswered(known, now) {
                known_unsettled += 1;
                continue;
            }
            // Past its window: asked again rather than assumed unchanged.
        }
        if considered == MEASUREMENTS_PER_PASS {
            break;
        }
        considered += 1;
        let outcome = tree.judge(&snapshot, &row.identity).await;
        let Some(state) = outcome.state() else {
            let reason = outcome.reason().unwrap_or(UnsettledReason::GitFailed);
            let why = outcome
                .why()
                .unwrap_or("the look settled nothing and gave no reason")
                .to_string();
            // Said when the wording is new, not every time it is re-confirmed:
            // what the re-confirmation is for is the answer changing, and a line
            // per hour per row says nothing the count does not.
            if unsettled.get(&row.identity).map(|k| k.why.as_str()) != Some(why.as_str()) {
                warn!(
                    change = %row.identity,
                    reason = reason.as_str(),
                    why = %why,
                    "change ledger: this change's check cannot be settled against the tree; \
                     its state is left as it was"
                );
            }
            unsettled.insert(row.identity.clone(), UnsettledCheck { why, since: now });
            *by_reason.entry(reason).or_default() += 1;
            continue;
        };
        // Settled this time, so whatever this identity was unanswered for is
        // over and its reason is dropped: a stale reason left in the map would
        // report a fault the tree no longer has.
        unsettled.remove(&row.identity);
        match state {
            ChangeState::Landed => landed += 1,
            ChangeState::Pending => absent += 1,
            // A settled look answers with one of exactly two states, both named
            // above. A third reaching here would be one the evaluator grew
            // without this count being told, and counting it as "not landed"
            // would hide that.
            other => warn!(
                change = %row.identity,
                state = other.as_str(),
                "change ledger: a settled look named a state this pass does not count"
            ),
        }
        // No observation travels with the write. What this look saw is the check
        // and the revision, and the revision rides in its own column rather than
        // in a rendered note the storage does not keep.
        let measurement =
            ChangeMeasurement::new(row.identity.clone(), state).at_revision(snapshot.rev());
        if let Err(e) = store.apply_measurement(&measurement).await {
            // Counted as measured because it was, not because it was stored: the
            // line below is what says the write failed, and a state that did not
            // land must not also be subtracted from the reading.
            warn!(
                change = %row.identity,
                error = %e,
                "change ledger: a measurement could not be written back"
            );
        }
    }

    info!(
        measured = considered,
        landed,
        absent,
        unsettled = %reason_summary(&by_reason),
        already_unsettled = known_unsettled,
        revision = snapshot.rev(),
        "change ledger: measured the outstanding set against the tree"
    );
}

/// Hand the changes still owed to the goal path, one task each.
///
/// Only `pending` is handed over, and the rest of the outstanding set is named
/// in the line rather than left to be inferred from an absence. The three
/// excluded states are excluded for three different reasons: `undecided` is
/// "nothing can tell yet", which is a check to rewrite rather than work to
/// start; `conflict` is two documents disagreeing about what the artifact should
/// be, which no amount of code generation resolves; and `partial` is a change
/// already under way, whose remaining half this loop cannot name. A state that is
/// not handed over is still a row this pass read.
///
/// The task's id is derived from the change's identity, so a change handed over
/// once is never handed over twice: this loop re-reads the same outstanding set
/// every period, and the graph's own row is what remembers the handover. That is
/// also why nothing here re-submits a change whose task has failed. Retrying is a
/// policy with a budget, and the task already carries one; a second retry
/// authority inside the ledger would be a second answer to "has this been tried
/// enough", in the same table whose point is one answer per question.
///
/// A dispatched change's state is not touched. Handing work over is not doing it,
/// and a row moved because a task was filed would have the ledger reporting a
/// tree that nothing had looked at.
async fn dispatch_pass(store: &dyn ChangeLedgerStore, orchestrator: &Arc<dyn OrchestratorControl>) {
    // The read is bounded by the ledger's own census rather than by a number
    // chosen here, and it has to be that wide because of where pending rows sit
    // in it. A state of `pending` *is* a measurement's result, so those rows are
    // the freshest in a staleness-ordered read, behind every row nothing has
    // looked at; measured on the live ledger, the first fifty outstanding rows
    // were all of them `undecided`. A bound the size of the one this pass
    // measures with would therefore hand over nothing for as long as unmeasured
    // rows remain ahead of them. Taking the whole non-landed set is what makes
    // "the pending set" the thing this reads, and it needs no constant to stay
    // true as the ledger grows.
    let outstanding_total = match store.census().await {
        Ok(census) => census
            .iter()
            .filter(|(state, _)| *state != ChangeState::Landed)
            .map(|(_, count)| *count)
            .sum::<u64>(),
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the census could not be read, so this pass handed nothing over"
            );
            return;
        }
    };
    let rows = match store.outstanding(outstanding_total as usize).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the outstanding set could not be read, so this pass handed \
                 nothing over"
            );
            return;
        }
    };

    let pending: Vec<ChangeLedgerRow> = rows
        .iter()
        .filter(|row| row.state == ChangeState::Pending)
        .cloned()
        .collect();

    let mut submitted = 0usize;
    let mut already = 0usize;
    let mut failed = 0usize;
    for row in &pending {
        let task_id = dispatch_task_id(&row.identity);
        // The graph is read before the submission rather than the submission's
        // answer being read after it: the goal path treats a repeated id as a
        // fault, so a pass that submitted blind would leave a line saying the
        // handover failed while the change was in fact handed over.
        if orchestrator.get_task(&task_id).await.is_some() {
            already += 1;
            continue;
        }
        let goal = dispatch_goal(row);
        let task = Task::new(
            task_id,
            TaskType::Custom(DISPATCH_TASK_KIND.into()),
            serde_json::json!({
                "goal": goal,
                "evolution_mode": "generate_change",
                "task_kind": DISPATCH_TASK_KIND,
                "change_identity": {
                    "kind": row.identity.kind,
                    "scope": row.identity.scope,
                    "literal": row.identity.literal,
                },
                "change_sources": row
                    .sources
                    .iter()
                    .map(|source| source.doc.as_str())
                    .collect::<Vec<_>>(),
            }),
        );
        match orchestrator.submit_goal_auto(&goal, vec![task]).await {
            Ok(_) => submitted += 1,
            Err(e) => {
                failed += 1;
                warn!(
                    change = %row.identity,
                    error = %e,
                    "change ledger: a change still owed could not be handed over"
                );
            }
        }
    }

    info!(
        pending = pending.len(),
        submitted,
        already_handed_over = already,
        failed,
        not_handed_over = %held_back_summary(&rows),
        "change ledger: handed the pending set to the goal path"
    );
}

/// The outstanding rows this pass did not hand over, by state.
///
/// Named as a field rather than left to be inferred from the counts above: the
/// states left out of the handover are left out for reasons, and a line that
/// reported only what was submitted would leave a reader unable to tell a ledger
/// with nothing owed from one whose whole outstanding set sits in a state nothing
/// can act on.
fn held_back_summary(rows: &[ChangeLedgerRow]) -> String {
    ChangeState::ALL
        .into_iter()
        .filter(|state| *state != ChangeState::Pending)
        .filter_map(|state| {
            let n = rows.iter().filter(|row| row.state == state).count();
            (n > 0).then(|| format!("{} {}", state.as_str(), n))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// How many of these rows a pass would have judged.
///
/// The same two rules the loop applies -- an identity inside its unanswered
/// window is not asked again, and the batch is bounded -- so a count taken when
/// the tree could not be opened reports what went unmeasured rather than what
/// was read.
fn would_judge(
    rows: &[ChangeLedgerRow],
    unsettled: &HashMap<ChangeIdentity, UnsettledCheck>,
    now: Instant,
) -> usize {
    rows.iter()
        .filter(|row| match unsettled.get(&row.identity) {
            Some(known) => !still_unanswered(known, now),
            None => true,
        })
        .take(MEASUREMENTS_PER_PASS)
        .count()
}

/// One pass's unanswered rows by reason, as a single field.
///
/// Every reason in the closed set is named, zero included. A field per reason
/// would be a second copy of the set, and it would go stale the moment the set
/// grew; a line naming only what happened leaves the reader counting absences.
/// The set is the code's, so the line is rendered from it.
fn reason_summary(by_reason: &HashMap<UnsettledReason, usize>) -> String {
    UnsettledReason::ALL
        .iter()
        .map(|reason| {
            format!(
                "{} {}",
                reason.as_str(),
                by_reason.get(reason).copied().unwrap_or(0)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether a check already known not to be settleable is left alone this pass.
///
/// The window is what makes leaving a row alone a deferral rather than a
/// verdict: past it the check is run again, so a tree that gained what the check
/// was looking for is noticed without anything having to be restarted.
fn still_unanswered(known: &UnsettledCheck, now: Instant) -> bool {
    now.duration_since(known.since) < UNMEASURABLE_RETRY
}

/// Compress one round's outstanding rows into one line.
///
/// The two kinds of "oldest" are named separately: never measured (`measured_at`
/// is empty) and measured while still outstanding. The first says something
/// entered the account that nothing has looked at; the second says it was looked
/// at and is still owed. One number covering both would state them as one fact.
/// A `landed` row is not outstanding -- the query excludes it -- so one showing
/// up here means the storage filter broke, not that the ledger grew a row.
fn outstanding_summary(rows: &[ChangeLedgerRow]) -> String {
    let by_state = ChangeState::ALL
        .into_iter()
        .filter_map(|state| {
            let n = rows.iter().filter(|row| row.state == state).count();
            (n > 0).then(|| format!("{} {}", state.as_str(), n))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let never_measured = rows.iter().filter(|row| row.measured_at.is_none()).count();
    match rows.iter().filter_map(|row| row.measured_at).min() {
        Some(oldest) => format!(
            "{} outstanding: {by_state}; never measured {never_measured}; oldest measurement {}",
            rows.len(),
            oldest.to_rfc3339()
        ),
        None => format!(
            "{} outstanding: {by_state}; never measured {never_measured}",
            rows.len()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use cog_core::ChangeIdentity;

    fn row(state: ChangeState, measured_at: Option<i64>) -> ChangeLedgerRow {
        ChangeLedgerRow {
            identity: ChangeIdentity::new("grep", "crates/x", "fn thing"),
            state,
            title: None,
            assertion: None,
            sources: Vec::new(),
            first_seen_at: Utc.timestamp_opt(1_760_000_000, 0).unwrap(),
            measured_at: measured_at.map(|s| Utc.timestamp_opt(s, 0).unwrap()),
            measured_rev: None,
            landed_at: None,
        }
    }

    /// How many rows, in which states, and how many nobody has looked at -- all
    /// three have to be readable off the one line.
    #[test]
    fn the_summary_names_every_state_present_and_the_unmeasured_count() {
        let rows = vec![
            row(ChangeState::Pending, None),
            row(ChangeState::Undecided, Some(1_760_000_000)),
            row(ChangeState::Pending, Some(1_760_000_100)),
        ];
        assert_eq!(
            outstanding_summary(&rows),
            "3 outstanding: pending 2, undecided 1; never measured 1; \
             oldest measurement 2025-10-09T08:53:20+00:00"
        );
    }

    /// With nothing measured, there is no oldest measurement -- an absent one
    /// must not be reported as a time.
    #[test]
    fn all_unmeasured_rows_report_no_measurement_time() {
        let rows = vec![
            row(ChangeState::Pending, None),
            row(ChangeState::Conflict, None),
        ];
        assert_eq!(
            outstanding_summary(&rows),
            "2 outstanding: conflict 1, pending 1; never measured 2"
        );
    }

    /// States are ordered by the closed set, not by row order: the same rows in
    /// a different order read out as the same line, or every diff of two readings
    /// would be noise.
    #[test]
    fn the_state_order_does_not_follow_the_row_order() {
        let rows = vec![
            row(ChangeState::Undecided, None),
            row(ChangeState::Conflict, None),
        ];
        assert_eq!(
            outstanding_summary(&rows),
            "2 outstanding: conflict 1, undecided 1; never measured 2"
        );
    }

    /// The same row under a different scope, so a set of rows can be told apart
    /// by the identity the pass keys its memory on.
    fn row_at(scope: &str) -> ChangeLedgerRow {
        ChangeLedgerRow {
            identity: ChangeIdentity::new("grep", scope, "fn thing"),
            ..row(ChangeState::Pending, None)
        }
    }

    /// Every reason in the closed set is named on the line, zeroes included. A
    /// reason that emptied has to read as zero rather than vanish: a missing key
    /// and a reason nobody thought to count look the same to the reader.
    #[test]
    fn the_reason_line_names_every_reason_including_the_empty_ones() {
        let mut by_reason = HashMap::new();
        by_reason.insert(UnsettledReason::ScopeOutsideTree, 12);
        by_reason.insert(UnsettledReason::GitFailed, 1);
        assert_eq!(
            reason_summary(&by_reason),
            "revision_unreadable 0, kind_without_predicate 0, scope_outside_tree 12, git_failed 1"
        );
    }

    /// With nothing unanswerable the line still says so, all four at zero: the
    /// absence of the field would otherwise be the only thing saying it, and an
    /// absent field is not a reading.
    #[test]
    fn a_pass_that_could_settle_everything_still_names_the_reasons() {
        assert_eq!(
            reason_summary(&HashMap::new()),
            "revision_unreadable 0, kind_without_predicate 0, scope_outside_tree 0, git_failed 0"
        );
    }

    /// What a dead tree left unjudged, counted the way the loop would have read
    /// it: a row inside its unanswered window is not one this pass was going to
    /// judge, and once that window is up it is.
    #[test]
    fn a_dead_tree_counts_what_this_pass_would_have_judged() {
        let now = Instant::now();
        let rows: Vec<ChangeLedgerRow> = (0..5).map(|n| row_at(&format!("crates/{n}"))).collect();
        let mut unsettled = HashMap::new();
        unsettled.insert(
            ChangeIdentity::new("grep", "crates/0", "fn thing"),
            UnsettledCheck {
                why: "the scope is not in this revision".into(),
                since: now,
            },
        );
        assert_eq!(would_judge(&rows, &unsettled, now), 4);
        assert_eq!(would_judge(&rows, &HashMap::new(), now), 5);
        assert_eq!(would_judge(&rows, &unsettled, now + UNMEASURABLE_RETRY), 5);
    }

    /// The count is of what a pass judges, not of what it read: past the batch
    /// bound the rows beyond it were not going to be judged either, and counting
    /// them would overstate what the failure cost.
    #[test]
    fn the_batch_bound_is_what_a_pass_judges_not_what_it_read() {
        let rows: Vec<ChangeLedgerRow> = (0..MEASUREMENTS_PER_PASS + 1)
            .map(|n| row_at(&format!("crates/{n}")))
            .collect();
        assert_eq!(
            would_judge(&rows, &HashMap::new(), Instant::now()),
            MEASUREMENTS_PER_PASS
        );
    }

    /// A row left alone for not being settleable is asked again once its window
    /// is up. Without the second half, "cannot tell" would harden into "never
    /// looked at again" and a tree that gained the path would go unnoticed.
    #[test]
    fn a_row_is_left_alone_only_until_its_window_is_up() {
        let now = Instant::now();
        let fresh = UnsettledCheck {
            why: "the scope is not in this revision".into(),
            since: now,
        };
        assert!(still_unanswered(&fresh, now));
        assert!(still_unanswered(
            &fresh,
            now + UNMEASURABLE_RETRY - Duration::from_secs(1)
        ));
        assert!(!still_unanswered(&fresh, now + UNMEASURABLE_RETRY));
    }

    /// A landed row in the outstanding set is a broken storage filter, and this
    /// line has to name it rather than fold it into the total.
    #[test]
    fn a_landed_row_in_the_outstanding_set_is_named_as_such() {
        let rows = vec![row(ChangeState::Landed, Some(1_760_000_000))];
        assert_eq!(
            outstanding_summary(&rows),
            "1 outstanding: landed 1; never measured 0; oldest measurement 2025-10-09T08:53:20+00:00"
        );
    }

    /// The id has to be a function of the identity and of nothing else: this loop
    /// re-reads the same outstanding set every period, and an id that moved
    /// between passes would file the same change again on every one of them.
    #[test]
    fn the_task_id_is_the_same_string_for_the_same_identity() {
        let one = ChangeIdentity::new("grep", "crates/x", "fn thing");
        let two = ChangeIdentity::new("grep", "crates/x", "fn thing");
        assert_eq!(dispatch_task_id(&one), dispatch_task_id(&two));
        assert!(dispatch_task_id(&one).starts_with(DISPATCH_TASK_PREFIX));
    }

    /// Two identities that render the same string must not share an id. The
    /// flattened form separates its parts with the same character a literal is
    /// full of, so a split of `a:b:c` is two changes by what they name and one
    /// string by how they read -- and an id read off that string would hand over
    /// whichever of them came second and leave the other owed forever.
    #[test]
    fn identities_that_render_alike_get_different_task_ids() {
        let split_early = ChangeIdentity::new("grep", "a", "b:c");
        let split_late = ChangeIdentity::new("grep", "a:b", "c");
        assert_eq!(split_early.as_key(), split_late.as_key());
        assert_ne!(
            dispatch_task_id(&split_early),
            dispatch_task_id(&split_late)
        );
    }

    /// The goal is the change's own words. A title is a reading's restatement of
    /// what is wanted; the identity is only where to look, so it is the fallback
    /// and not the first choice.
    #[test]
    fn the_goal_prefers_the_title_and_falls_back_to_the_identity() {
        let titled = ChangeLedgerRow {
            title: Some("  make the gate read the  reading  ".into()),
            ..row(ChangeState::Pending, None)
        };
        assert_eq!(dispatch_goal(&titled), "make the gate read the reading");

        let asserted = ChangeLedgerRow {
            title: None,
            assertion: Some("the gate reads the reading".into()),
            ..row(ChangeState::Pending, None)
        };
        assert_eq!(dispatch_goal(&asserted), "the gate reads the reading");

        let bare = row(ChangeState::Pending, None);
        assert_eq!(dispatch_goal(&bare), bare.identity.as_key());
    }

    /// What was left out is on the line, by state. Otherwise a ledger whose whole
    /// outstanding set sits in a state nothing can act on reads exactly like one
    /// with nothing owed.
    #[test]
    fn the_held_back_line_names_every_state_that_was_not_handed_over() {
        let rows = vec![
            row(ChangeState::Conflict, None),
            row(ChangeState::Pending, None),
            row(ChangeState::Undecided, None),
            row(ChangeState::Undecided, None),
        ];
        assert_eq!(held_back_summary(&rows), "conflict 1, undecided 2");
        assert_eq!(held_back_summary(&[row(ChangeState::Pending, None)]), "");
    }
}
