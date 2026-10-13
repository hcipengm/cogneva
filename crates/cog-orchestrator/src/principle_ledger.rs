//! The resident maintenance loop for the principle ledger.
//!
//! The account's authoritative copy is the platform's own `principle_ledger`
//! table: one row per principle, identity the memory's own file name, and a state
//! only a look at the two registries may move. What maintains it is this resident
//! interval loop, registered in the main process's plugin and elected through the
//! owner lease, so the table has exactly one writer at any moment. It is the
//! change ledger's loop with a different subject and the same shape -- same
//! lease, same liveness family, same rule that the account is written in exactly
//! one place and read everywhere.
//!
//! Five things happen here, and the third is the only one that moves a row. The
//! loop folds the intake into the account, reporting by name any principle whose
//! carrier's bytes moved since it was last measured and holding those back from
//! measurement for the pass. It reads what is outstanding back out, with the
//! census of the whole account beside it, because "how much of the corpus has a
//! gate" is the reading this account exists to produce. It looks at the
//! outstanding set against both registries, through the one call that may move a
//! state. It reports the reverse direction -- declarations in the platform's own
//! tree that name no row of the account -- by name, because a mistyped slug and a
//! principle the account never heard of are one count and two fixes. And it hands
//! the debt rows to the goal path, so "the account records this is owed" is where
//! the work gets started from.
//!
//! What it hands over is the *debt*, not the ungated corpus, and the two are kept
//! apart on purpose. A principle with no gate is "no criterion has been designed
//! for this yet", and the work that would close it is *designing a criterion* --
//! a thing whose endpoint is a blank, so a task for it could name only a slug.
//! The debt rows are the opposite: each is an internal dependency that already
//! names its own destination (the core contract an edge should go through, or the
//! crate a module should move to), so a task for it says what to build, not where
//! to look. Only a row that can state its endpoint is handed over; the ungated
//! corpus stays a reading that names, honestly, how much of it nobody has
//! designed a gate for.
//!
//! The handover is bounded, and the bound is read from the host rather than
//! chosen here: a dispatched debt row is a code change, orders of magnitude
//! dearer than a measurement, so a pass that handed the whole registry over would
//! flood a goal path that can only build a fixed number of things at once (see
//! [`dispatch_bound`]). A row already handed over is not handed over again -- its
//! task id is derived from the row's own identity, so the graph's row is what
//! remembers the handover -- and handing over moves no state, because a task filed
//! is not a debt paid.
//!
//! A row the look could not settle keeps its state and is named in the pass's own
//! line, with every reason of the closed set counted on every pass, zeroes
//! included. That is what keeps "nothing has looked at this" and "something
//! looked and could not tell" apart -- and a third answer that vanishes when it
//! is zero is a third answer nobody can see going away.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use cog_core::{
    OrchestratorControl, OwnerLeaseBroker, PrincipleCensus, PrincipleCheckKind,
    PrincipleCheckKindCensus, PrincipleDebtReading, PrincipleLedgerStore, PrincipleMeasurement,
    PrincipleOrphan, PrincipleRow, PrincipleState, ShutdownSignal, Task, TaskStatus, TaskType,
};

use crate::change_measure::CheckTree;
use crate::principle_measure::{
    drift_annotation, judge, measurable, Declarations, UnsettledReason, Verdict,
    PLATFORM_DECLARATION_PATTERN, PLATFORM_REGISTRY_FILES,
};

/// This loop's name in the process liveness registry, and the value of
/// `cogneva_loop_tick_age_seconds{loop=...}`.
pub const PRINCIPLE_LEDGER_LOOP: &str = "principle_ledger";

/// This loop's role name on the owner lease: who maintains the account.
///
/// The same value as the loop name, but the two read different surfaces: the
/// loop name labels the liveness and ownership readings (`{loop=...}`), the role
/// name is the row in `cogneva_owner_leases.role`.
pub const PRINCIPLE_LEDGER_ROLE: &str = "principle_ledger";

/// How long one round waits for the next.
///
/// The account moves on two events only: the host pushing a reading of the
/// corpus, or a contract test being added to the platform's tree. Both are
/// human-driven and measured in hours, so this period is not "how fast do
/// principles arrive" but "how long before someone notices the account stopped
/// being maintained". Minutes rather than seconds keeps the steady state out of
/// the log. The deployed liveness alert derives its threshold from this period
/// (`6 * period`), so changing it changes that threshold.
pub const PRINCIPLE_LEDGER_PASS_INTERVAL: Duration = Duration::from_secs(300);

/// How many outstanding rows one round reads for its own census line.
///
/// The round exists for one thing: to make "the account is readable, and this is
/// how much is outstanding" a line in the log with a reading behind it.
const OUTSTANDING_PEEK: usize = 50;

/// How many queued readings one pass folds into the account.
///
/// Generous on purpose, and deliberately larger than the corpus. The intake is
/// one list of the whole corpus pushed in one go, so a bound below the corpus
/// size would leave the account chasing the list across passes and every count
/// read in between would be of a half-folded corpus -- which is exactly the kind
/// of reading this account must not publish. A pass is still bounded: a queue
/// that grew past this is not a corpus being read, it is a writer looping.
const DECLARATIONS_PER_PASS: usize = 1000;

/// How many queued refusal events one pass counts.
const REFUSALS_PER_PASS: usize = 500;

/// How many rows one pass looks at against the registries.
///
/// The account holds rows in the hundreds and a pass that walked all of them
/// would spend the whole interval asking git questions while holding the owner
/// lease. Rows are taken stalest-first, so this bound costs latency rather than
/// coverage: everything outstanding is still reached, in as many passes as it
/// takes.
const MEASUREMENTS_PER_PASS: usize = 200;

/// How long a look that could not be settled is left before it is asked again.
///
/// Held alone it would be permanent, and a permanent answer to a question about
/// two moving registries is the wrong shape: the look could not be settled
/// because of what the tree holds right now, and a tree that gains a contract
/// test makes it answerable. An hour is long enough that re-asking costs nothing
/// (the pass runs every few minutes and the unanswered set is a few hundred rows)
/// and short enough that a tree that changed is noticed within the same working
/// session.
const UNMEASURABLE_RETRY: Duration = Duration::from_secs(3600);

/// The prefix a dispatched debt task's id carries, so a task sitting in the graph
/// says which loop put it there.
const DISPATCH_TASK_PREFIX: &str = "principle-ledger-";

/// The task kind a dispatched debt row is submitted under.
///
/// A name of the platform's own flows rather than a kind of hand: the row is not
/// handed to a generic worker, it is handed to the change-generation path, and
/// the kind is what that path is looked up by. It mirrors this loop's own name,
/// as the change ledger's kind mirrors its own, and is distinct from that kind so
/// that a reading can tell which loop put a task on the path -- the two streams
/// share one goal path, and a counter without a label naming the asker cannot say
/// which one was starved.
const DISPATCH_TASK_KIND: &str = "principle_ledger";

/// The debt registry, relative to the repository root.
///
/// The very file the contract tests read, so a row added to a test's table is a
/// row this loop hands over and the two cannot drift. A running binary cannot
/// read the test's `const`, so the registry lives in the tree and both readers
/// take it from there.
const DEBT_EDGE_FILE: &str = "crates/cogneva/debt/dependency-edges.tsv";

/// The `kind` value an allowed edge row carries, and the label its task's reading
/// is split by.
const EDGE_KIND: &str = "dependency_edge";

/// The `kind` value a test-time allowed edge row carries.
const DEV_EDGE_KIND: &str = "dev_dependency_edge";

/// The module registry, relative to the repository root.
///
/// The same file the core boundary contract test reads. A module row names the
/// module, why it is a contract, and -- when a move is owed -- the crate it is
/// going to and the shape of the move, so a module owed a move names its own
/// endpoint and is handed over exactly as an edge row is.
const DEBT_MODULE_FILE: &str = "crates/cogneva/debt/core-modules.tsv";

/// The `reason` value a module row carries when it is owed a move.
///
/// A module row with a destination under any other reason would be a row the
/// contract test refuses; read here so a registry that grew such a row hands
/// nothing over for it rather than dispatching a move nobody registered.
const RELOCATION_REASON: &str = "pending-relocation";

/// The `kind` value a module-relocation row carries, and the label its task's
/// reading is split by.
const MODULE_KIND: &str = "module_relocation";

/// The file the host's build gate publishes its bound in, under the lock
/// directory both it and this process read from the same config.
const BUILD_BOUND_FILE: &str = "buildgate.bound";

/// One debt row read out of the tree's own registry: an internal dependency that
/// already names where it should go.
///
/// The identity -- `(kind, scope, literal)` -- is the row's own, and it is what
/// its task id is derived from. Rendering the id from the parts instead of
/// hashing them is what [`debt_task_id`] exists to avoid (see there).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DebtRow {
    /// Which registry the row came from and what shape it is, as a label a
    /// reading is split by.
    kind: String,
    /// The crate that carries the dependency, or the module owed a move.
    scope: String,
    /// The crate that is depended on, or the crate a module is going to.
    literal: String,
    /// The core contract the edge should go through instead. Named so the goal
    /// can say *what* to build rather than where to look. Empty for a module row,
    /// which names a destination crate rather than a contract.
    contract: String,
    /// How a module leaves: `direct` or `split`, the shape the registry's
    /// destination column carried. Empty for an edge row, which has no such
    /// shape.
    mode: String,
}

/// A principle this process could not settle, and when it last tried.
///
/// The reason is kept so a repeat is not said twice, and the time so that the
/// silence is a window rather than a decision. Both are this process's memory of
/// what it already said, not facts about the account, so a restarting process
/// re-learns them by asking again -- the honest way to recover, since what makes
/// a look unsettleable is a property of the registries and can change.
#[derive(Debug)]
struct UnsettledPrinciple {
    why: String,
    since: Instant,
}

/// Start the loop. Not started when there is no ledger store (see the module
/// header).
///
/// `orchestrator` is the goal path the debt registry is handed to. It is optional
/// for the same reason the lease role is: a process that cannot submit a goal
/// still keeps an account, and refusing to run the loop at all would trade a
/// missing driver for a missing ledger. Where it is absent the pass says so once,
/// by name, rather than skipping the handover silently.
///
/// `lock_dir` is the build gate's lock directory, from the same config the gate
/// itself reads. The bound this loop hands work over under is read from the bound
/// the gate publishes there (see [`dispatch_bound`]).
pub fn spawn_principle_ledger_loop(
    store: Arc<dyn PrincipleLedgerStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    lock_dir: String,
    shutdown: ShutdownSignal,
    tree: CheckTree,
) {
    info!(
        interval_secs = PRINCIPLE_LEDGER_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        dispatching = orchestrator.is_some(),
        "Principle ledger loop enabled"
    );
    if orchestrator.is_none() {
        warn!(
            "principle ledger: no goal path is available, so a debt row read from the registry \
             will be counted and not handed to anyone"
        );
    }

    drop(cog_core::loop_health::spawn(
        PRINCIPLE_LEDGER_LOOP,
        cog_core::loop_health::Cadence::Periodic(PRINCIPLE_LEDGER_PASS_INTERVAL),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let orchestrator = orchestrator.clone();
            let lock_dir = lock_dir.clone();
            let shutdown = shutdown.clone();
            let tree = tree.clone();
            async move {
                // The role is renewed on the lease contract's own cadence rather
                // than once per pass: a pass can be long, and a term that ran out
                // mid-pass would hand the seat to a second process working the
                // same table. The loop still asks once per pass, because a role
                // lost has to stop the work in the pass it was lost.
                let hold =
                    cog_core::RoleHold::start(role, PRINCIPLE_LEDGER_ROLE, beat.clone(), &shutdown);

                // Looks this process could not settle, by slug, with the reason
                // it could not and when it was last tried.
                let mut unsettled: HashMap<String, UnsettledPrinciple> = HashMap::new();

                // The revision the last hand-over read the debt registry at. Held
                // in this process rather than read back off the row the last pass
                // wrote: the trend is measured against the tree at that revision,
                // and a loop that took its own last row as the term to subtract
                // would be grading itself. A restart loses it, so the first pass
                // after one records no trend -- which is the honest answer, since
                // that process has not seen an earlier revision.
                let mut last_debt_rev: Option<String> = None;

                let mut ticker = tokio::time::interval(PRINCIPLE_LEDGER_PASS_INTERVAL);
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
                    // The fold runs first and hands back what drifted, so a row
                    // whose carrier's bytes just moved is held back from this
                    // pass's measurement rather than being measured against the
                    // text the account was carrying until a moment ago.
                    let drift = fold_pass(&*store).await;
                    read_pass(&*store).await;
                    measure_pass(&*store, &tree, &drift, &mut unsettled).await;
                    // Handing over comes last, so the registry it reads is the
                    // tree as of this pass: a row this pass just judged is not
                    // handed to anyone differently for having been judged, and a
                    // row added since the last pass is handed over on the pass
                    // that first sees it rather than one period later. It reads
                    // the tree, not the account -- the debt lives in the tree's
                    // own registry, the same bytes the contract tests read.
                    if let Some(ref orchestrator) = orchestrator {
                        dispatch_pass(&tree, orchestrator, &lock_dir, &*store, &mut last_debt_rev)
                            .await;
                    }
                }
            }
        },
    ));
}

/// One round: read the account's outstanding set and its census, and say so
/// plainly when either could not be read.
///
/// A failed read is neither raised nor retried here: in a periodic loop the next
/// round is the retry, and repeated failure is never silent -- every round leaves
/// a warning.
async fn read_pass(store: &dyn PrincipleLedgerStore) {
    match store.outstanding(OUTSTANDING_PEEK).await {
        Ok(rows) if rows.is_empty() => {
            debug!("principle ledger read: nothing outstanding");
        }
        Ok(rows) => {
            info!(summary = %outstanding_summary(&rows), "principle ledger read");
        }
        Err(e) => {
            warn!(error = %e, "principle ledger read failed; this cycle read nothing");
        }
    }

    // The census is the reading the account exists for: how much of the corpus
    // has a gate, how much is a debt, and how much nothing has looked at. Read
    // every pass and printed every pass, because a coverage that is only computed
    // when someone asks is one nobody watches.
    match store.census().await {
        Ok(census) => info!(account = %account_summary(&census), "principle ledger census"),
        Err(e) => warn!(error = %e, "principle ledger: the census could not be read"),
    }

    // The tier census beside it, because the state census alone cannot say why a
    // row is ungated. A debt and a principle a person holds both read as
    // `unenforced` there; only this reading tells them apart, and the number work
    // can move is the debt with the `judgement` rows subtracted.
    match store.check_kind_census().await {
        Ok(census) => info!(
            check_kind = %check_kind_summary(&census),
            "principle ledger check-kind census"
        ),
        Err(e) => warn!(
            error = %e,
            "principle ledger: the check-kind census could not be read"
        ),
    }
}

/// Fold what has been queued into the account, count what has been refused, and
/// hand back the principles whose carrier moved.
///
/// Folding first is what makes a principle appear in the same pass's outstanding
/// reading; the backlog is read either way, because a backlog that stays non-zero
/// across passes is the one reading that says the account has not caught up with
/// what it was told. Neither the fold's failure nor the count's is raised or
/// retried -- the next pass is the retry, and every pass leaves a line.
async fn fold_pass(store: &dyn PrincipleLedgerStore) -> HashSet<String> {
    let mut drifted = HashSet::new();
    match store.fold_declarations(DECLARATIONS_PER_PASS).await {
        Ok(fold) if fold.folded == 0 && fold.drifted.is_empty() => {}
        Ok(fold) => {
            // By name, with both hashes: the question a drift raises is "did the
            // gate follow the text", and a count of drifted principles cannot be
            // answered without going to look up what moved. Said here and not
            // carried into the measurement, because what the account does about
            // it is nothing -- no state is written for a drifted row.
            for drift in &fold.drifted {
                warn!(
                    slug = %drift.slug,
                    "principle ledger: this principle's carrier bytes moved, so this pass does \
                     not judge it -- {}",
                    drift_annotation(drift)
                );
                drifted.insert(drift.slug.clone());
            }
            info!(
                folded = fold.folded,
                drifted = fold.drifted.len(),
                "principle ledger: readings folded into the account"
            );
        }
        Err(e) => warn!(
            error = %e,
            "principle ledger: folding the intake failed; nothing entered the account this pass"
        ),
    }
    match store.pending_declarations().await {
        Ok(0) => {}
        Ok(pending) => info!(
            pending,
            "principle ledger: readings queued and not yet folded"
        ),
        Err(e) => warn!(error = %e, "principle ledger: could not read the intake backlog"),
    }

    // The refusal counts. The side that refused work cannot add to the account
    // itself -- a count written by the party it counts is the number this table
    // exists to replace -- so it reports events and this is where they are added
    // up. Nothing here decides whether a refusal was justified; it counts what
    // was reported and keeps the last instant.
    match store.fold_refusals(REFUSALS_PER_PASS).await {
        Ok(0) => {}
        Ok(folded) => info!(
            folded,
            "principle ledger: gate refusals counted against their principles"
        ),
        Err(e) => warn!(error = %e, "principle ledger: counting refusals failed"),
    }
    match store.pending_refusals().await {
        Ok(0) => {}
        Ok(pending) => info!(
            pending,
            "principle ledger: refusals reported and not yet counted"
        ),
        Err(e) => warn!(error = %e, "principle ledger: could not read the refusal backlog"),
    }

    drifted
}

/// Read the platform's own registry out of one revision of the tree.
///
/// The registry is this platform's source: a contract test whose header states
/// which principle it enforces. It is read the same way a change's check is
/// measured -- out of the repository, at one resolved revision, through the same
/// tree -- because a running deployment has no checkout of its own source and
/// because a ref resolved once per pass is what makes every row in the pass
/// answer for one tree.
///
/// A failure comes back as the reason it failed rather than as an empty registry:
/// the two are the difference between "no gate found" and "no registry read", and
/// that difference is the whole point of the third answer.
async fn read_registry(
    tree: &CheckTree,
) -> Result<(Declarations, String), (UnsettledReason, String)> {
    let snapshot = tree.snapshot().await.map_err(|why| {
        (
            UnsettledReason::RevisionUnreadable,
            format!("the platform's own tree could not be resolved, so no declaration could be read: {why}"),
        )
    })?;
    let lines = tree
        .matching_lines(
            snapshot.rev(),
            PLATFORM_REGISTRY_FILES,
            PLATFORM_DECLARATION_PATTERN,
        )
        .await
        .map_err(|why| {
            (
                UnsettledReason::RegistryUnreadable,
                format!(
                    "the declaration lines could not be read at {}: {why}",
                    snapshot.rev()
                ),
            )
        })?;
    Ok((Declarations::parse(&lines), snapshot.rev().to_string()))
}

/// The measurement a row's verdict turns into, or `None` for one that moves
/// nothing.
///
/// The verdict is taken as an argument rather than looked up again here: the same
/// look decides both whether to write and what to write, and a second call would
/// be a second answer to the same question.
///
/// The platform-side gate column is written with the verdict, from the same
/// declaration the verdict was drawn from, so the column and the state agree by
/// construction: a row whose state says a gate exists and whose gate column names
/// nothing is a row no reader can argue with.
fn measurement_of(
    row: &PrincipleRow,
    verdict: &Verdict,
    declarations: &Declarations,
    revision: &str,
) -> Option<PrincipleMeasurement> {
    let state = verdict.state()?;
    let mut measurement = PrincipleMeasurement::new(row.slug.clone(), state).at_revision(revision);
    measurement.platform_gate = declarations.gate_for(&row.slug).map(str::to_string);
    Some(measurement)
}

/// Look at the outstanding set against both registries, and record what each row
/// came back as.
///
/// The batch is read before the tree is opened, so that a registry which cannot
/// be read still reports how many rows it left unmeasured. Opening the registry
/// first and returning on failure would leave that pass with a warning and no
/// count, which is the one shape a reader cannot tell from a pass with nothing to
/// do.
///
/// A row whose look could not be settled is left exactly as it is: its state is
/// what an earlier look found, and this pass has nothing to put in its place.
/// What it does have is the reason, said once per slug and then counted, so the
/// steady state is a number rather than a wall of repeats. Leaving it alone is a
/// deferral rather than a verdict -- the registries move, and the row is asked
/// again after [`UNMEASURABLE_RETRY`].
async fn measure_pass(
    store: &dyn PrincipleLedgerStore,
    tree: &CheckTree,
    drifted: &HashSet<String>,
    unsettled: &mut HashMap<String, UnsettledPrinciple>,
) {
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
                "principle ledger: the outstanding set could not be read, so nothing was \
                 judged this pass"
            );
            return;
        }
    };

    let (declarations, revision) = match read_registry(tree).await {
        Ok(read) => read,
        Err((reason, why)) => {
            // One fault, one reason, every row that this pass would have judged:
            // a registry nobody could read must not read as a pass with nothing
            // outstanding. Not remembered in `unsettled` either, because this is
            // not a fact about any one principle -- it is the registry being
            // unreadable, and it is re-asked in full on every pass until it is
            // not.
            warn!(
                why = %why,
                reason = reason.as_str(),
                rows = would_judge(&rows, unsettled, Instant::now(), drifted),
                "principle ledger: the platform's registry could not be read, so nothing was \
                 judged this pass"
            );
            return;
        }
    };

    // The reverse direction, before any verdict: a declaration naming a slug the
    // account has no row for is reported by name and with the file that says it.
    // A mistyped slug and a principle the account never heard of are one count
    // and two different fixes, and a silence here would judge the first of them
    // as the second.
    //
    // Carried as an `Option` rather than reported only when it is non-zero: the
    // reconciliation runs on every pass, so its result belongs on the pass line
    // whether or not it found anything, and an absent count would leave "read and
    // found none" indistinguishable from "the account's side of the comparison
    // could not be read". `None` is the second of those, and it is not a zero.
    let orphan_declarations = match store.slugs().await {
        Ok(known) => {
            let known: HashSet<String> = known.into_iter().collect();
            let orphans = declarations.orphans(&known);
            if !orphans.is_empty() {
                warn!(
                    orphan_declarations = orphans.len(),
                    orphans = %orphan_summary(&orphans),
                    "principle ledger: the platform's registry names principles the account has \
                     no row for"
                );
            }
            Some(orphans.len())
        }
        Err(e) => {
            warn!(
                error = %e,
                "principle ledger: the account's slugs could not be read, so the reverse \
                 reconciliation was skipped this pass"
            );
            None
        }
    };

    // A declaration the registry could not read is named too: it is a file's
    // specific line, and the account is about to report every principle it might
    // have been the gate for as ungated, so the reader needs to know which line
    // to look at.
    if declarations.has_unreadable() {
        warn!(
            unreadable_declarations = declarations.unreadable_count(),
            declarations = %unreadable_summary(&declarations),
            "principle ledger: the platform's registry holds declarations whose slug cannot be \
             read, so no principle can be found ungated on this pass"
        );
    }

    let now = Instant::now();
    let mut enforced = 0usize;
    let mut unenforced = 0usize;
    let mut human_only = 0usize;
    let mut by_reason: HashMap<UnsettledReason, usize> = HashMap::new();
    let mut known_unsettled = 0usize;
    let mut considered = 0usize;
    let mut held_back_for_drift = 0usize;
    for row in &rows {
        if drifted.contains(&row.slug) {
            held_back_for_drift += 1;
            continue;
        }
        if let Some(known) = unsettled.get(&row.slug) {
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
        let verdict = judge(row, &declarations);
        // A principle a person holds is not a row this pass can move, and it is
        // not a row that failed to be moved either. Handled before the unsettled
        // path rather than through it: that path is keyed by a reason from the
        // closed set, and none of them is "no gate is meant to exist", so a
        // judgement row reaching it would be filed under whichever reason a
        // fallback happened to name. Counted on its own so the pass line says how
        // many rows the account is holding on a person's behalf.
        if verdict == Verdict::HumanOnly {
            human_only += 1;
            unsettled.remove(&row.slug);
            continue;
        }
        let Some(measurement) = measurement_of(row, &verdict, &declarations, &revision) else {
            let reason = verdict
                .reason()
                .unwrap_or(UnsettledReason::RegistryUnreadable);
            let why = verdict
                .why()
                .unwrap_or("the look settled nothing and gave no reason")
                .to_string();
            // Said when the wording is new, not every time it is re-confirmed:
            // what the re-confirmation is for is the answer changing, and a line
            // per hour per row says nothing the count does not.
            if unsettled.get(&row.slug).map(|k| k.why.as_str()) != Some(why.as_str()) {
                warn!(
                    slug = %row.slug,
                    reason = reason.as_str(),
                    why = %why,
                    "principle ledger: this principle cannot be settled against the two \
                     registries; its state is left as it was"
                );
            }
            unsettled.insert(row.slug.clone(), UnsettledPrinciple { why, since: now });
            *by_reason.entry(reason).or_default() += 1;
            continue;
        };
        // Settled this time, so whatever this slug was unanswered for is over and
        // its reason is dropped: a stale reason left in the map would report a
        // fault the registries no longer have.
        unsettled.remove(&row.slug);
        match measurement.state {
            PrincipleState::Enforced => enforced += 1,
            PrincipleState::Unenforced => unenforced += 1,
        }
        if let Err(e) = store.apply_principle_measurement(&measurement).await {
            // Counted as judged because it was, not because it was stored: the
            // line below is what says the write failed, and a state that did not
            // land must not also be subtracted from the reading.
            warn!(
                slug = %row.slug,
                error = %e,
                "principle ledger: a measurement could not be written back"
            );
        }
    }

    info!(
        judged = considered,
        enforced,
        unenforced,
        human_only,
        held_back_for_drift,
        unsettled = %reason_summary(&by_reason),
        already_unsettled = known_unsettled,
        orphan_declarations = ?orphan_declarations,
        revision = %revision,
        "principle ledger: judged the outstanding set against the two registries"
    );
}

/// The task id a debt row is dispatched under.
///
/// Derived from the row's identity, because that is the whole mechanism: this
/// loop re-reads the same registry every few minutes, and an id that moved
/// between passes would file the same work again on every one of them. A row that
/// leaves the registry stops producing its id, so its task is never filed again
/// -- withdrawal needs no state to remember it.
///
/// Hashed rather than rendered from the parts, and each part is terminated as it
/// is fed in: a literal is source code and routinely contains the separator a
/// flattened identity is made of, so two different rows could render the same
/// string -- and a shared id is not a duplicate cost, it is a row that is never
/// dispatched at all, because the second one finds the first one's task in the
/// graph and takes it for its own.
fn debt_task_id(row: &DebtRow) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [&row.kind, &row.scope, &row.literal] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    let hex = hasher.finalize().to_hex();
    format!("{DISPATCH_TASK_PREFIX}{}", &hex[..16])
}

/// What a dispatched debt row is worked on as.
///
/// The row's own words, in one line: the two crates the edge joins and the core
/// contract it should go through. A goal that is only `grep:path:literal` tells
/// the generator where to look and not what is wanted there, and the point of
/// handing a debt row over -- rather than an ungated principle -- is that it can
/// say what to build.
fn debt_goal(row: &DebtRow) -> String {
    if row.kind == MODULE_KIND {
        let shape = if row.mode == "split" {
            " The module is a contract and a mechanism in one file: the half that keeps state \
             moves, and the contract half stays in the core's contract layer."
        } else {
            ""
        };
        return format!(
            "Move the core module `{}` out of cog-core's source root into the `{}` crate.{shape}",
            row.scope, row.literal
        );
    }
    if row.contract.is_empty() {
        return format!(
            "Remove the {} -> {} dependency by routing it through the core's contract layer.",
            row.scope, row.literal
        );
    }
    let when = if row.kind == DEV_EDGE_KIND {
        "in its tests "
    } else {
        ""
    };
    format!(
        "Make {} reach {} through the core contract {} {when}instead of depending on it directly.",
        row.scope, row.literal, row.contract
    )
}

/// The edge rows carried by the dependency registry's own bytes.
///
/// One row per line, tab-separated, exactly as the contract test reads it: a
/// `#`-led line and the `kind consumer dep contract` header carry no work, and a
/// `kind` outside the two edge kinds is not a row this handing-over knows how to
/// aim (the module registry's rows are read by [`module_debt_rows`]).
fn edge_debt_rows(lines: &[(String, String)]) -> Vec<DebtRow> {
    let mut rows = Vec::new();
    for (_path, text) in lines {
        let text = text.trim();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let mut field = text.split('\t');
        let kind = match field.next() {
            Some("allowed") => EDGE_KIND,
            Some("allowed_dev") => DEV_EDGE_KIND,
            _ => continue,
        };
        let (Some(consumer), Some(dep)) = (field.next(), field.next()) else {
            continue;
        };
        rows.push(DebtRow {
            kind: kind.to_string(),
            scope: consumer.to_string(),
            literal: dep.to_string(),
            contract: field.next().unwrap_or("").to_string(),
            mode: String::new(),
        });
    }
    rows
}

/// The module rows carried by the module registry's own bytes.
///
/// One row per line, tab-separated `module reason destination`, exactly as the
/// core boundary contract test reads it. Only a module the registry marks as
/// awaiting relocation *and* gives a destination is a row this handing-over can
/// aim: a module owed a move with no destination has no endpoint to name, so it
/// stays a reading rather than a task -- the same rule the edges follow for a row
/// whose contract is not written yet. The destination is `crate shape`, the shape
/// `direct` or `split`, and the shape rides into the goal so the generator is
/// told a split module leaves in halves.
fn module_debt_rows(lines: &[(String, String)]) -> Vec<DebtRow> {
    let mut rows = Vec::new();
    for (_path, text) in lines {
        let text = text.trim();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let mut field = text.split('\t');
        let (Some(module), Some(reason)) = (field.next(), field.next()) else {
            continue;
        };
        if module == "module" || reason != RELOCATION_REASON {
            continue;
        }
        let Some(destination) = field.next().map(str::trim) else {
            continue;
        };
        let mut parts = destination.split_whitespace();
        let (Some(crate_name), Some(mode)) = (parts.next(), parts.next()) else {
            continue;
        };
        rows.push(DebtRow {
            kind: MODULE_KIND.to_string(),
            scope: module.to_string(),
            literal: crate_name.to_string(),
            contract: String::new(),
            mode: mode.to_string(),
        });
    }
    rows
}

/// Read the debt registries out of one revision of the tree.
///
/// Both registries -- the dependency edges and the module relocations -- off the
/// same tree, one resolved revision, the same timeout as every other read of the
/// platform's own source: a ref resolved once per pass is what makes every row in
/// the pass answer for one tree. A failure is the reason it failed rather than an
/// empty registry, because "no debt" and "no registry read" are different answers
/// and only one of them is good news.
async fn read_debt(tree: &CheckTree) -> Result<(Vec<DebtRow>, String), String> {
    let snapshot = tree
        .snapshot()
        .await
        .map_err(|why| format!("the platform's own tree could not be resolved: {why}"))?;
    let rev = snapshot.rev().to_string();
    let rows = read_debt_at(tree, &rev).await?;
    Ok((rows, rev))
}

/// The debt rows one revision of the tree owed, read from that revision's own
/// carrier files.
///
/// Split out from [`read_debt`] so the same reading can be taken at a revision
/// that is not the current one: the trend a pass records is the difference
/// between two such readings, and taking both from the tree is what makes the
/// difference something any reader holding the two revisions can recompute.
async fn read_debt_at(tree: &CheckTree, rev: &str) -> Result<Vec<DebtRow>, String> {
    let edge_lines = tree
        .matching_lines(rev, DEBT_EDGE_FILE, ".")
        .await
        .map_err(|why| {
            format!("the debt registry {DEBT_EDGE_FILE} could not be read at {rev}: {why}")
        })?;
    let module_lines = tree
        .matching_lines(rev, DEBT_MODULE_FILE, ".")
        .await
        .map_err(|why| {
            format!("the debt registry {DEBT_MODULE_FILE} could not be read at {rev}: {why}")
        })?;
    let mut rows = edge_debt_rows(&edge_lines);
    rows.extend(module_debt_rows(&module_lines));
    Ok(rows)
}

/// How many debt rows one pass hands over, and where that number came from.
///
/// Not a constant: a bare literal here would be a number with nothing behind it,
/// and the constitution this platform holds itself to forbids exactly that. What
/// the number has to be is *how many code changes this host can have in flight at
/// once*, and that is a fact about the machine, not about this loop. The build
/// gate already works it out -- from the host's cpu, memory and disk, or from the
/// operator's override -- and publishes it where every process on the host reads
/// it, so this reads that. Handing over more than the host can build only queues
/// work behind the build face.
///
/// The build gate's own bound is never below one in any branch, so one is its
/// floor and not a number chosen here. An unreadable bound leaves this pass at
/// that floor: it hands over slowly rather than not at all, and the source string
/// says which file it could not read so the reading is not silent about it.
fn dispatch_bound(lock_dir: &str) -> (usize, String) {
    let path = std::path::Path::new(lock_dir).join(BUILD_BOUND_FILE);
    let shown = path.display().to_string();
    match std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| t.trim().parse::<usize>().ok())
    {
        Some(bound) if bound > 0 => (bound, shown),
        Some(_) => (
            1,
            format!("{shown} (the gate's bound is zero; using its floor of one)"),
        ),
        None => (
            1,
            format!("{shown} (unreadable; using the host gate's floor of one)"),
        ),
    }
}

/// The debt rows this pass read, by kind, as a single field.
///
/// Split by `kind` rather than totalled: the debt and the corpus of ungated
/// principles share one goal path, and a count that did not name which registry
/// it came from could not say which one the path was starving. Every kind the
/// reader knows is named, so a kind that emptied reads as zero rather than
/// vanishing.
fn debt_kind_summary(rows: &[DebtRow]) -> String {
    [EDGE_KIND, DEV_EDGE_KIND, MODULE_KIND]
        .iter()
        .map(|kind| {
            let n = rows.iter().filter(|row| row.kind == *kind).count();
            format!("{kind} {n}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Hand the debt rows to the goal path, one task each, bounded by what the host
/// can build.
///
/// A row already in the graph is not handed over again -- the derived id is the
/// whole mechanism -- and a row whose task has failed is not re-submitted here.
/// Retrying is a policy with a budget, and the task already carries one; a second
/// retry authority inside this loop would be a second answer to "has this been
/// tried enough", in the same path whose point is one answer per question.
///
/// A row left over because the bound was reached is counted and named by kind
/// rather than left to be inferred from an absence: the bound is what is holding
/// it, not anything about the row, and a pass that quietly stopped handing rows
/// over would otherwise read as a registry with less in it. Handing over moves no
/// state -- there is no state here to move -- so a task filed is not a debt paid;
/// the registry shrinks only when the tree's own bytes change.
///
/// The pass is recorded as well as logged, and the two are different readings of
/// it. A line is read by whoever is looking at that moment; the row is the series
/// a later pass is compared against, and that comparison is the only thing that
/// answers whether the debt is moving at all. The trend it records is read off
/// the tree rather than remembered -- the count it subtracts is taken from the
/// older revision's own files -- so the number is one any reader holding the two
/// revisions can recompute rather than the dispatcher's word about itself. A pass
/// whose registry could not be read records nothing -- it handed nothing over and
/// measured nothing, so it is a gap in the series and the warning says so, where
/// a zero row would read as a pass that found no debt.
async fn dispatch_pass(
    tree: &CheckTree,
    orchestrator: &Arc<dyn OrchestratorControl>,
    lock_dir: &str,
    store: &dyn PrincipleLedgerStore,
    last_rev: &mut Option<String>,
) {
    let (rows, rev) = match read_debt(tree).await {
        Ok(read) => read,
        Err(why) => {
            warn!(
                why = %why,
                "principle ledger: the debt registry could not be read, so this pass handed \
                 nothing over"
            );
            return;
        }
    };
    let (bound, source) = dispatch_bound(lock_dir);

    // The trend, taken from the tree rather than remembered. The count this pass
    // subtracted from is the one the *older revision's own files* hold, so the
    // number is reproducible by anyone who has the two revisions -- where a delta
    // this loop carried in its own head, or read back off its own last row, would
    // be the dispatcher's word about itself, in the one reading whose whole point
    // is whether the debt is really falling.
    //
    // A revision it cannot read leaves the delta unsaid rather than zero: the
    // absence of a comparison and a comparison that came out even are opposite
    // findings, and only one of them means the debt stopped moving.
    let delta = match last_rev.as_deref() {
        None => None,
        // The tree has not moved since the last pass, so the difference is zero
        // by construction and re-reading the same bytes would only restate it.
        Some(prev) if prev == rev => Some(0),
        Some(prev) => match read_debt_at(tree, prev).await {
            Ok(prior) => Some(rows.len() as i64 - prior.len() as i64),
            Err(why) => {
                warn!(
                    rev = %prev,
                    why = %why,
                    "principle ledger: the debt at the last revision could not be read, so this \
                     pass records no trend"
                );
                None
            }
        },
    };
    *last_rev = Some(rev.clone());

    let mut submitted = 0usize;
    let mut already: Vec<TaskStatus> = Vec::new();
    let mut failed = 0usize;
    let mut over_bound = 0usize;
    let mut not_handed_over: Vec<String> = Vec::new();
    for row in &rows {
        let task_id = debt_task_id(row);
        // The graph is read before the submission rather than the submission's
        // answer being read after it: the goal path treats a repeated id as a
        // fault, so a pass that submitted blind would leave a line saying the
        // handover failed while the row was in fact handed over.
        //
        // The existing task's status is counted rather than just its presence,
        // because "handed over" is otherwise one word for three states: still
        // in flight, finished, and dead. A row whose task is dead is not
        // re-submitted here -- retrying is the task's own budget -- so unless
        // the status is on the line a row that was dispatched and died reads
        // exactly like one that is still being worked on, and the registry's
        // failure to shrink has no visible cause.
        if let Some(existing) = orchestrator.get_task(&task_id).await {
            already.push(existing.status);
            continue;
        }
        if submitted >= bound {
            over_bound += 1;
            not_handed_over.push(debt_row_label(row));
            continue;
        }
        let goal = debt_goal(row);
        let task = Task::new(
            task_id,
            TaskType::Custom(DISPATCH_TASK_KIND.into()),
            serde_json::json!({
                "goal": goal,
                "evolution_mode": "generate_change",
                "task_kind": DISPATCH_TASK_KIND,
                "debt_row": {
                    "kind": row.kind,
                    "scope": row.scope,
                    "literal": row.literal,
                    "contract": row.contract,
                    "mode": row.mode,
                },
            }),
        );
        match orchestrator.submit_goal_auto(&goal, vec![task]).await {
            Ok(_) => submitted += 1,
            Err(e) => {
                failed += 1;
                warn!(
                    debt = %debt_row_label(row),
                    error = %e,
                    "principle ledger: a debt row still owed could not be handed over"
                );
            }
        }
    }

    // Rendered once and used by both the line and the row: two renderings of the
    // same pass could disagree, and then the durable reading and the one a
    // person reads side by side would be two accounts of one pass.
    let by_kind = debt_kind_summary(&rows);
    let already_handed_over = already_summary(&already);
    let held_back = not_handed_over.join(", ");
    // Said as a sign rather than as a bare number, so "0" -- a pass that changed
    // nothing -- cannot be read as "nobody compared": the one case with no
    // comparison at all is spelled out.
    let trend = match delta {
        Some(n) => format!("{n:+}"),
        None => "none".to_string(),
    };

    info!(
        read = rows.len(),
        rev = %rev,
        delta = %trend,
        by_kind = %by_kind,
        submitted,
        already_handed_over = %already_handed_over,
        failed,
        over_bound,
        // Named, not left to the count: the rows the bound held back are the ones
        // a reader must be able to see waiting, and "over_bound 3" says three
        // exist without saying which three a later pass will pick up. Empty when
        // nothing was held back, so the field itself says "none" rather than
        // reading the same as a pass that never looked.
        not_handed_over = %held_back,
        bound,
        bound_source = %source,
        "principle ledger: handed the debt registry to the goal path"
    );

    // The line above is the second-hand reading; this row is the durable one. The
    // loop's stdout reaches no collector on this host, so a pass recorded only in
    // the log is one nobody can compare with the pass before it -- and the
    // comparison is the only thing that answers whether the debt is moving.
    let reading = debt_reading(
        by_kind, submitted, &already, failed, over_bound, bound, source, rev, delta,
    );
    if let Err(e) = store.record_debt_reading(&reading).await {
        // Said out loud rather than swallowed: the pass did its work, and the one
        // thing lost is the record of it, so the line above is the only copy of
        // this reading and a reader has to be told that is what it is.
        warn!(
            error = %e,
            "principle ledger: the pass could not be recorded; the line above is the only \
             copy of this reading"
        );
    }
}

/// One pass as the durable reading records it.
///
/// Built from the numbers the line is built from and in one place, because the
/// two are read side by side: a row that disagreed with the line written beside
/// it would be two accounts of one pass with nothing to say which one the pass
/// was. The counts are taken as the columns hold them rather than as the loop
/// counts them -- the loop counts rows, the table counts rows.
///
/// The revision rides in with them, because the counts are a claim about a tree:
/// a row that did not name its revision would be a count of debt at no
/// particular time, and no reader could check it or compare it with the row
/// before it -- which is the only thing this table is for.
fn debt_reading(
    by_kind: String,
    submitted: usize,
    already: &[TaskStatus],
    failed: usize,
    over_bound: usize,
    bound: usize,
    bound_source: String,
    rev: String,
    delta: Option<i64>,
) -> PrincipleDebtReading {
    PrincipleDebtReading {
        by_kind,
        submitted: submitted as u64,
        already: already.len() as u64,
        failed: failed as u64,
        over_bound: over_bound as u64,
        bound: bound as u64,
        bound_source,
        rev: Some(rev),
        delta,
    }
}

/// One debt row as a reader names it: the two ends of the edge it owes, so the
/// pass's line points at the dependency rather than at an opaque id.
fn debt_row_label(row: &DebtRow) -> String {
    format!("{} -> {}", row.scope, row.literal)
}

/// The rows already handed over, by the state their task is in.
///
/// Every status is named, zeroes included, because the whole reason the split is
/// here is to tell three cases apart that one count states as one: a row whose
/// task is finished, one still in flight, and one that died. A status that
/// emptied has to read as zero rather than vanish, or "no task has died" and
/// "nobody counted whether tasks died" would look the same.
fn already_summary(already: &[TaskStatus]) -> String {
    TaskStatus::ALL
        .iter()
        .map(|status| {
            let n = already.iter().filter(|s| *s == status).count();
            format!("{} {n}", status.as_str())
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// How many of these rows a pass would have judged.
///
/// The same rules the loop applies -- a row held back for drift is not judged, a
/// slug inside its unanswered window is not asked again, and the batch is bounded
/// -- so a count taken when the registry could not be read reports what went
/// unjudged rather than what was read.
fn would_judge(
    rows: &[PrincipleRow],
    unsettled: &HashMap<String, UnsettledPrinciple>,
    now: Instant,
    drifted: &HashSet<String>,
) -> usize {
    measurable(rows, drifted)
        .into_iter()
        .filter(|row| match unsettled.get(&row.slug) {
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

/// Whether a look already known not to be settleable is left alone this pass.
///
/// The window is what makes leaving a row alone a deferral rather than a verdict:
/// past it the look is run again, so a tree that gained a contract test is
/// noticed without anything having to be restarted.
fn still_unanswered(known: &UnsettledPrinciple, now: Instant) -> bool {
    now.duration_since(known.since) < UNMEASURABLE_RETRY
}

/// The whole account in one line: the three counts, the total, and the coverage
/// they make.
///
/// The coverage is computed here rather than left to the reader because it is the
/// number this account exists to produce, and a ratio someone has to work out
/// from two counts is one that gets quoted wrong. The unmeasured count is named
/// beside it and never folded into the debt: a principle no look has settled is
/// not a principle without a gate.
fn account_summary(census: &PrincipleCensus) -> String {
    let coverage = match census.coverage() {
        Some(ratio) => format!("{:.1}%", ratio * 100.0),
        None => "no rows".to_string(),
    };
    format!(
        "enforced {} unenforced {} unmeasured {} total {} coverage {}",
        census.enforced,
        census.unenforced,
        census.unmeasured,
        census.total(),
        coverage
    )
}

/// The whole account by tier in one line: every value of the closed set, the
/// undeclared count, the total, and the two groupings a reader needs.
///
/// The values are rendered from [`PrincipleCheckKind::ALL`] rather than written
/// out, so a fifth tier would appear here without this line being edited -- and a
/// field per tier that nobody remembered to add would be a count that silently
/// stopped covering the account. The two groupings are computed here because they
/// are the whole reason the line exists: the three *tiers* a reader thinks in are
/// `shape | tree + live | judgement`, and the debt that work can move is the three
/// machine values with the `judgement` rows -- and the undeclared ones -- held out.
fn check_kind_summary(census: &PrincipleCheckKindCensus) -> String {
    let by_value = PrincipleCheckKind::ALL
        .iter()
        .map(|kind| {
            let n = match kind {
                PrincipleCheckKind::Shape => census.shape,
                PrincipleCheckKind::Tree => census.tree,
                PrincipleCheckKind::Live => census.live,
                PrincipleCheckKind::Judgement => census.judgement,
            };
            format!("{} {}", kind.as_str(), n)
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{by_value}, undeclared {}, machine_tiers {}, held_by_a_person {}, total {}",
        census.undeclared,
        census.total() - census.judgement - census.undeclared,
        census.judgement,
        census.total()
    )
}

/// Compress one round's outstanding rows into one line.
///
/// The two kinds of "oldest" are named separately: never measured (`measured_at`
/// is empty) and measured while still outstanding. The first says something
/// entered the account that nothing has looked at; the second says it was looked
/// at and is still owed. One number covering both would state them as one fact.
fn outstanding_summary(rows: &[PrincipleRow]) -> String {
    let by_state = PrincipleState::ALL
        .into_iter()
        .filter_map(|state| {
            let n = rows.iter().filter(|row| row.state == Some(state)).count();
            (n > 0).then(|| format!("{} {}", state.as_str(), n))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let unmeasured = rows.iter().filter(|row| row.state.is_none()).count();
    let by_state = if by_state.is_empty() {
        format!("unmeasured {unmeasured}")
    } else {
        format!("{by_state}, unmeasured {unmeasured}")
    };
    match rows.iter().filter_map(|row| row.measured_at).min() {
        Some(oldest) => format!(
            "{} outstanding: {by_state}; oldest look {}",
            rows.len(),
            oldest.to_rfc3339()
        ),
        None => format!("{} outstanding: {by_state}", rows.len()),
    }
}

/// The declarations the account has no row for, one per line.
///
/// By name and with the file, never as a bare total: the work a mistyped slug
/// calls for is in the declaration and the work a never-read principle calls for
/// is in the reading, and a reader given only the count cannot tell which of the
/// two they are looking at.
fn orphan_summary(orphans: &[PrincipleOrphan]) -> String {
    orphans
        .iter()
        .map(|orphan| format!("{}<-{}", orphan.slug, orphan.source))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The declarations the registry held but could not read, one per line.
fn unreadable_summary(declarations: &Declarations) -> String {
    declarations
        .unreadable()
        .iter()
        .map(|(path, text)| format!("{path}: {text}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn row(slug: &str, host: Option<&str>, state: Option<PrincipleState>) -> PrincipleRow {
        PrincipleRow {
            slug: slug.to_string(),
            lead: None,
            carrier_path: None,
            carrier_sha256: None,
            host_gate: host.map(str::to_string),
            platform_gate: None,
            check_kind: None,
            state,
            refusals: 0,
            last_refused_at: None,
            first_seen_at: Utc.timestamp_opt(1_760_000_000, 0).unwrap(),
            measured_at: None,
            measured_rev: None,
        }
    }

    fn declarations(lines: &[(&str, &str)]) -> Declarations {
        Declarations::parse(
            &lines
                .iter()
                .map(|(path, text)| (path.to_string(), text.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    /// Every reason in the closed set is named on the line, zeroes included. A
    /// reason that emptied has to read as zero rather than vanish: a missing key
    /// and a reason nobody thought to count look the same to the reader.
    #[test]
    fn the_reason_line_names_every_reason_including_the_empty_ones() {
        let mut by_reason = HashMap::new();
        by_reason.insert(UnsettledReason::HostColumnAbsent, 12);
        by_reason.insert(UnsettledReason::CarrierNotInTree, 1);
        assert_eq!(
            reason_summary(&by_reason),
            "registry_unreadable 0, revision_unreadable 0, host_column_absent 12, \
             carrier_not_in_tree 1, declaration_unparsable 0"
        );
        assert_eq!(
            reason_summary(&HashMap::new()),
            "registry_unreadable 0, revision_unreadable 0, host_column_absent 0, \
             carrier_not_in_tree 0, declaration_unparsable 0"
        );
    }

    /// The account's own line: the three counts, the total, and the ratio. The
    /// unmeasured rows are in the total -- they are rows the account holds, and
    /// leaving them out would make the coverage read as if it were computed over
    /// only the principles someone had got to.
    #[test]
    fn the_account_line_counts_the_unmeasured_rows_against_the_total() {
        let census = PrincipleCensus {
            enforced: 5,
            unenforced: 556,
            unmeasured: 0,
        };
        assert_eq!(
            account_summary(&census),
            "enforced 5 unenforced 556 unmeasured 0 total 561 coverage 0.9%"
        );

        let partly = PrincipleCensus {
            enforced: 1,
            unenforced: 0,
            unmeasured: 2,
        };
        assert_eq!(
            account_summary(&partly),
            "enforced 1 unenforced 0 unmeasured 2 total 3 coverage 33.3%"
        );
    }

    /// An account holding nothing has no coverage, and a zero would read as an
    /// account whose every principle is ungated.
    #[test]
    fn an_empty_account_has_no_coverage_rather_than_none() {
        assert_eq!(
            account_summary(&PrincipleCensus::default()),
            "enforced 0 unenforced 0 unmeasured 0 total 0 coverage no rows"
        );
    }

    /// The check-kind line names every tier of the closed set, the undeclared
    /// rows apart, and the two groupings. The number that matters is the one the
    /// state census cannot produce: the machine-checkable rows are the three
    /// machine values with the judgement rows -- and the undeclared ones -- held
    /// out, which is the debt work can actually move.
    #[test]
    fn the_check_kind_line_names_every_tier_and_the_two_groupings() {
        let census = PrincipleCheckKindCensus {
            shape: 6,
            tree: 3,
            live: 1,
            judgement: 4,
            undeclared: 608,
        };
        assert_eq!(
            check_kind_summary(&census),
            "shape 6, tree 3, live 1, judgement 4, undeclared 608, machine_tiers 10, \
             held_by_a_person 4, total 622"
        );
    }

    /// Never measured and measured-but-still-outstanding are two facts, and the
    /// line says both: one is something entered that nothing has looked at, the
    /// other is something looked at and still owed.
    #[test]
    fn the_outstanding_line_separates_unmeasured_rows_from_stale_ones() {
        let rows = vec![
            row("a", Some("A_RE"), None),
            row("b", Some("-"), Some(PrincipleState::Unenforced)),
            row("c", Some("-"), Some(PrincipleState::Unenforced)),
        ];
        assert_eq!(
            outstanding_summary(&rows),
            "3 outstanding: unenforced 2, unmeasured 1"
        );
    }

    /// The state order does not follow the row order: the same rows in a
    /// different order read out as the same line, or every diff of two readings
    /// would be noise.
    #[test]
    fn the_state_order_does_not_follow_the_row_order() {
        let one = vec![
            row("b", Some("-"), Some(PrincipleState::Unenforced)),
            row("a", Some("-"), Some(PrincipleState::Enforced)),
        ];
        let two = vec![
            row("a", Some("-"), Some(PrincipleState::Enforced)),
            row("b", Some("-"), Some(PrincipleState::Unenforced)),
        ];
        assert_eq!(outstanding_summary(&one), outstanding_summary(&two));
    }

    /// The reverse direction is by name and with the file. A total cannot tell a
    /// mistyped slug from a principle the account never heard of, and the two
    /// call for different work.
    #[test]
    fn the_orphan_line_names_each_declaration_and_its_file() {
        let orphans = vec![
            PrincipleOrphan {
                slug: "feedback_typo".into(),
                source: "crates/cogneva/tests/a.rs".into(),
            },
            PrincipleOrphan {
                slug: "feedback_ghost".into(),
                source: "crates/cogneva/tests/b.rs".into(),
            },
        ];
        assert_eq!(
            orphan_summary(&orphans),
            "feedback_typo<-crates/cogneva/tests/a.rs, \
             feedback_ghost<-crates/cogneva/tests/b.rs"
        );
    }

    /// A declaration nothing can read is named with its file and its text: the
    /// account is about to report every principle it might have been the gate for
    /// as ungated, so the reader needs the line, not a total.
    #[test]
    fn the_unreadable_line_names_the_file_and_the_line() {
        let registry = declarations(&[("crates/cogneva/tests/a.rs", "//! principle: Bad-Slug")]);
        assert_eq!(
            unreadable_summary(&registry),
            "crates/cogneva/tests/a.rs: //! principle: Bad-Slug"
        );
    }

    /// The state written with a verdict comes from the same declaration the
    /// verdict was drawn from, so the two cannot disagree.
    #[test]
    fn the_measurement_carries_the_gate_it_was_judged_by() {
        let registry = declarations(&[("crates/cogneva/tests/a.rs", "//! principle: feedback_x")]);
        let judged = row("feedback_x", Some("-"), None);
        let verdict = judge(&judged, &registry);
        let platformed = measurement_of(&judged, &verdict, &registry, "rev1")
            .expect("a gate in the platform's registry settles the row");
        assert_eq!(platformed.state, PrincipleState::Enforced);
        assert_eq!(
            platformed.platform_gate.as_deref(),
            Some("crates/cogneva/tests/a.rs")
        );
        assert_eq!(platformed.measured_rev.as_deref(), Some("rev1"));
    }

    /// A look that settled nothing produces no measurement at all: there is no
    /// state to write, and a measurement carrying the state the row already had
    /// would stamp a fresh `measured_at` on a look nobody made.
    #[test]
    fn a_look_that_settled_nothing_writes_nothing() {
        let judged = row("feedback_x", None, None);
        let verdict = judge(&judged, &Declarations::default());
        assert!(measurement_of(&judged, &verdict, &Declarations::default(), "rev1").is_none());
    }

    /// A drift that is not judged this pass does not count against the pass's
    /// batch, and the same rows with the drift over are judged.
    #[test]
    fn a_drifted_row_is_not_counted_as_one_the_pass_would_judge() {
        let rows = vec![row("a", Some("A_RE"), None), row("b", Some("B_RE"), None)];
        let now = Instant::now();
        let drifted: HashSet<String> = ["a".to_string()].into_iter().collect();
        assert_eq!(would_judge(&rows, &HashMap::new(), now, &drifted), 1);
        assert_eq!(would_judge(&rows, &HashMap::new(), now, &HashSet::new()), 2);
    }

    /// What a dead registry left unjudged, counted the way the loop would have
    /// read it: a slug inside its unanswered window is not one this pass was
    /// going to judge, and once that window is up it is.
    #[test]
    fn a_dead_registry_counts_what_this_pass_would_have_judged() {
        let now = Instant::now();
        let rows = vec![
            row("a", Some("A_RE"), None),
            row("b", Some("B_RE"), None),
            row("c", Some("C_RE"), None),
        ];
        let mut unsettled = HashMap::new();
        unsettled.insert(
            "a".to_string(),
            UnsettledPrinciple {
                why: "the host's column never arrived".into(),
                since: now,
            },
        );
        assert_eq!(would_judge(&rows, &unsettled, now, &HashSet::new()), 2);
        assert_eq!(
            would_judge(&rows, &unsettled, now + UNMEASURABLE_RETRY, &HashSet::new()),
            3
        );
    }

    /// The count is of what a pass judges, not of what it read: past the batch
    /// bound the rows beyond it were not going to be judged either, and counting
    /// them would overstate what the failure cost.
    #[test]
    fn the_batch_bound_is_what_a_pass_judges_not_what_it_read() {
        let rows: Vec<PrincipleRow> = (0..MEASUREMENTS_PER_PASS + 1)
            .map(|n| row(&format!("feedback_{n}"), Some("A_RE"), None))
            .collect();
        assert_eq!(
            would_judge(&rows, &HashMap::new(), Instant::now(), &HashSet::new()),
            MEASUREMENTS_PER_PASS
        );
    }

    /// A row left alone for not being settleable is asked again once its window
    /// is up. Without the second half, "cannot tell" would harden into "never
    /// looked at again" and a registry that gained a declaration would go
    /// unnoticed.
    #[test]
    fn a_row_is_left_alone_only_until_its_window_is_up() {
        let now = Instant::now();
        let fresh = UnsettledPrinciple {
            why: "the host's column never arrived".into(),
            since: now,
        };
        assert!(still_unanswered(&fresh, now));
        assert!(still_unanswered(
            &fresh,
            now + UNMEASURABLE_RETRY - Duration::from_secs(1)
        ));
        assert!(!still_unanswered(&fresh, now + UNMEASURABLE_RETRY));
    }

    fn edge(kind: &str, consumer: &str, dep: &str, contract: &str) -> DebtRow {
        DebtRow {
            kind: kind.to_string(),
            scope: consumer.to_string(),
            literal: dep.to_string(),
            contract: contract.to_string(),
            mode: String::new(),
        }
    }

    fn module(module: &str, destination: &str, mode: &str) -> DebtRow {
        DebtRow {
            kind: MODULE_KIND.to_string(),
            scope: module.to_string(),
            literal: destination.to_string(),
            contract: String::new(),
            mode: mode.to_string(),
        }
    }

    /// The registry carries a header and comments, and only the two edge kinds
    /// are rows this handing-over knows how to aim. A row of another kind is read
    /// by its own reader; taking it here would hand a goal over that named no
    /// endpoint.
    #[test]
    fn the_debt_rows_read_only_the_edge_kinds_and_skip_non_rows() {
        let lines: Vec<(String, String)> = [
            "# a comment",
            "kind\tconsumer\tdep\tcontract",
            "allowed\tcog-agent\tcog-redis\tcontract::stream::MessageBackend",
            "allowed_dev\tcog-wiki\tcog-memory\tcontract::memory::MemoryBackend",
            "something_else\ta\tb\tc",
            "",
        ]
        .iter()
        .map(|s| {
            (
                "crates/cogneva/debt/dependency-edges.tsv".to_string(),
                s.to_string(),
            )
        })
        .collect();
        let rows = edge_debt_rows(&lines);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, EDGE_KIND);
        assert_eq!(rows[0].scope, "cog-agent");
        assert_eq!(rows[0].literal, "cog-redis");
        assert_eq!(rows[0].contract, "contract::stream::MessageBackend");
        assert_eq!(rows[1].kind, DEV_EDGE_KIND);
    }

    /// The module registry's rows are read by their own reader, and only the ones
    /// that name an endpoint are handed over: a module owed a move without a
    /// destination has nothing to aim at, so it stays a reading. A row of another
    /// reason, however it is spelled, is not this reader's to dispatch.
    #[test]
    fn the_module_rows_read_only_the_ones_owed_a_move_with_a_destination() {
        let lines: Vec<(String, String)> = [
            "# a comment",
            "module\treason\tdestination",
            "admission\tpending-relocation\tcog-eval split",
            "lib\tcrate-root",
            "audit\tshared-definition",
            "build_gate\tpending-relocation\tcog-reflection direct",
            "halfway\tpending-relocation",
            "",
        ]
        .iter()
        .map(|s| {
            (
                "crates/cogneva/debt/core-modules.tsv".to_string(),
                s.to_string(),
            )
        })
        .collect();
        let rows = module_debt_rows(&lines);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, MODULE_KIND);
        assert_eq!(rows[0].scope, "admission");
        assert_eq!(rows[0].literal, "cog-eval");
        assert_eq!(rows[0].mode, "split");
        assert_eq!(rows[1].scope, "build_gate");
        assert_eq!(rows[1].literal, "cog-reflection");
        assert_eq!(rows[1].mode, "direct");
    }

    /// The id is derived from the row's identity, so the same row read on every
    /// pass lands on the same task -- which is what stops this loop from filing
    /// the same work every five minutes -- and two different rows do not collide,
    /// which would be a row never dispatched because another took its task.
    #[test]
    fn the_task_id_follows_the_row_identity_and_separates_rows() {
        let one = edge(
            EDGE_KIND,
            "cog-agent",
            "cog-redis",
            "contract::stream::MessageBackend",
        );
        let same = edge(
            EDGE_KIND,
            "cog-agent",
            "cog-redis",
            "contract::stream::MessageBackend",
        );
        let other = edge(
            EDGE_KIND,
            "cog-gateway",
            "cog-redis",
            "contract::kv::KeyValueStore",
        );
        assert_eq!(debt_task_id(&one), debt_task_id(&same));
        assert_ne!(debt_task_id(&one), debt_task_id(&other));
        assert!(debt_task_id(&one).starts_with(DISPATCH_TASK_PREFIX));
    }

    /// The goal says what to build -- the two ends and the contract to go through
    /// -- rather than only where to look, which is the whole reason a debt row can
    /// be handed over and an ungated principle cannot.
    #[test]
    fn the_goal_names_both_ends_and_the_contract_to_go_through() {
        let goal = debt_goal(&edge(
            EDGE_KIND,
            "cog-agent",
            "cog-redis",
            "contract::stream::MessageBackend",
        ));
        assert!(goal.contains("cog-agent"), "{goal}");
        assert!(goal.contains("cog-redis"), "{goal}");
        assert!(goal.contains("contract::stream::MessageBackend"), "{goal}");
        assert!(!goal.contains("grep:"), "{goal}");
        let dev = debt_goal(&edge(
            DEV_EDGE_KIND,
            "cog-wiki",
            "cog-memory",
            "contract::memory::MemoryBackend",
        ));
        assert!(dev.contains("tests"), "{dev}");

        // A module row names the module and where it is going, and a split one
        // also says it leaves in halves -- the shape rides into the goal so the
        // generator is told what a whole-module goal would hide.
        let split = debt_goal(&module("admission", "cog-eval", "split"));
        assert!(split.contains("admission"), "{split}");
        assert!(split.contains("cog-eval"), "{split}");
        assert!(split.contains("contract"), "{split}");
        let whole = debt_goal(&module("admission", "cog-eval", "direct"));
        assert!(whole.contains("admission"), "{whole}");
        assert!(whole.contains("cog-eval"), "{whole}");
        assert!(!whole.contains("contract"), "{whole}");
    }

    /// The handover bound is the number the host published, not a literal: a
    /// bound the loop chose would be a number with nothing behind it. A zero or
    /// an unreadable bound falls to the gate's own floor of one rather than to
    /// nothing -- a pass that handed over slowly beats one that handed over not
    /// at all -- and the source string names the file so the reading is not
    /// silent about which number it used.
    #[test]
    fn the_bound_is_the_one_the_host_published() {
        let dir =
            std::env::temp_dir().join(format!("cog-principle-debt-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(BUILD_BOUND_FILE), b"3\n").unwrap();
        let (bound, source) = dispatch_bound(dir.to_str().unwrap());
        assert_eq!(bound, 3);
        assert!(source.contains(BUILD_BOUND_FILE), "{source}");

        std::fs::write(dir.join(BUILD_BOUND_FILE), b"0\n").unwrap();
        let (floor, source) = dispatch_bound(dir.to_str().unwrap());
        assert_eq!(floor, 1);
        assert!(source.contains("floor"), "{source}");

        let missing = dir.join("nope");
        let (unreadable, source) = dispatch_bound(missing.to_str().unwrap());
        assert_eq!(unreadable, 1);
        assert!(source.contains("unreadable"), "{source}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The line names every kind the reader knows, zero included, so a kind that
    /// emptied reads as zero rather than vanishing -- the two streams share one
    /// goal path and the split is what says which one is being starved.
    #[test]
    fn the_kind_line_names_every_kind_zero_included() {
        let rows = vec![edge(EDGE_KIND, "a", "b", "c")];
        assert_eq!(
            debt_kind_summary(&rows),
            format!("{EDGE_KIND} 1, {DEV_EDGE_KIND} 0, {MODULE_KIND} 0")
        );
        assert_eq!(
            debt_kind_summary(&[]),
            format!("{EDGE_KIND} 0, {DEV_EDGE_KIND} 0, {MODULE_KIND} 0")
        );
    }

    /// The already-handover line splits the rows by the state their task is in,
    /// counting the dead ones, and names every state zero included -- so a row
    /// that was dispatched and died does not read like one still being worked on,
    /// and "no task in this state" does not read like "nobody looked".
    #[test]
    fn the_already_line_splits_the_dead_from_the_in_flight_and_the_done() {
        let already = vec![
            TaskStatus::Completed,
            TaskStatus::Running,
            TaskStatus::Failed,
            TaskStatus::Failed,
        ];
        let text = already_summary(&already);
        assert!(text.contains("completed 1"), "{text}");
        assert!(text.contains("running 1"), "{text}");
        assert!(text.contains("failed 2"), "{text}");

        // The states nothing landed in are named as zero rather than dropped: an
        // absent state and an uncounted one have to look different.
        let empty = already_summary(&[]);
        for status in TaskStatus::ALL {
            assert!(empty.contains(&format!("{} 0", status.as_str())), "{empty}");
        }
    }

    /// The pass's row is built from the same numbers its line is, so the durable
    /// reading and the line a person reads beside it cannot come out as two
    /// accounts of one pass. The ones that are not obvious are the ones stated:
    /// `already` counts rows and not the states they are in -- the split lives on
    /// the line, the row wants the count -- the ceiling travels beside the source
    /// that named it, so a reader can tell a bound discovered from the host apart
    /// from the gate's fallback, and the revision travels with the counts because
    /// they are a claim about a tree.
    #[test]
    fn the_debt_reading_counts_the_same_pass_the_line_describes() {
        let rows = vec![
            edge(EDGE_KIND, "a", "b", "c"),
            module("admission", "cog-eval", "split"),
        ];
        let already = vec![
            TaskStatus::Completed,
            TaskStatus::Running,
            TaskStatus::Failed,
        ];
        let reading = debt_reading(
            debt_kind_summary(&rows),
            2,
            &already,
            1,
            3,
            4,
            "bound file".to_string(),
            "rev-1".to_string(),
            Some(2),
        );
        assert_eq!(reading.by_kind, debt_kind_summary(&rows));
        assert_eq!(reading.submitted, 2);
        assert_eq!(reading.already, 3, "three states, counted as three rows");
        assert_eq!(reading.failed, 1);
        assert_eq!(reading.over_bound, 3);
        assert_eq!(reading.bound, 4);
        assert_eq!(reading.bound_source, "bound file");
        assert_eq!(reading.rev.as_deref(), Some("rev-1"));
        assert_eq!(reading.delta, Some(2));
    }

    /// A pass with no earlier revision to compare against records no trend, and
    /// keeps it distinct from a trend that came out even: a process that has just
    /// started has not seen the debt move, and a week of no progress is a
    /// different finding from a week nobody compared.
    #[test]
    fn a_first_pass_records_no_trend_rather_than_a_flat_one() {
        let reading = debt_reading(
            "k 0".to_string(),
            0,
            &[],
            0,
            0,
            1,
            "floor".to_string(),
            "rev-1".to_string(),
            None,
        );
        assert_eq!(reading.delta, None);

        let flat = debt_reading(
            "k 0".to_string(),
            0,
            &[],
            0,
            0,
            1,
            "floor".to_string(),
            "rev-1".to_string(),
            Some(0),
        );
        assert_eq!(flat.delta, Some(0));
        assert_ne!(reading, flat, "no comparison is not a comparison of zero");
    }
}
