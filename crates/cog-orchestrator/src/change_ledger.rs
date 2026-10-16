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
//! Five things happen here, and the measuring is the only one that moves a row.
//! The loop folds the intake into the account -- declarations queued by other
//! parties become readings, through the same reading path a direct write would
//! use -- it reads the tree's own debt registries into the account the same way,
//! it reads what the platform's own principle ledger still calls unpaid the same
//! way again, it reads the outstanding set back out, saying plainly when a read
//! failed, and it measures that set against the tree, through the one call that
//! may move a state. Every intake is mechanical: it carries what a document, a
//! registry or another account asked for and adds nothing of its own, so this
//! loop cannot make a change look done by reading anything into it.
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
//! reading rather than a silence. Whether the row is asked again turns on
//! whether a re-run could answer differently: a look that failed, or a path this
//! revision happens not to have, is a property of a tree that moves and is asked
//! again after [`UNMEASURABLE_RETRY`]; a reason that is a property of the row
//! itself -- no evaluator for its kind, a predicate no line can hold, a path the
//! tree structurally cannot hold -- is retracted to `undecided` instead of being
//! re-asked forever, because re-asking measures the same nothing every time.
//!
//! The second reading source is the project's own debt registries: two files in
//! the tree listing dependencies that are owed removal and core modules that are
//! owed a move. They are readings and not a second account -- every row they name
//! enters through the same call a declaration does, and only a measurement of the
//! tree can move it. What makes them a source rather than a table is where the
//! news comes from and how a row is retired: nothing queues these, the loop reads
//! them out of the tree it is already measuring against, and a row stops being
//! owed when the tree stops carrying what it asked for -- not when the registry
//! stops listing it. A registry nobody edited would otherwise be able to close an
//! account by deleting its own line, which is the self-reported completion this
//! whole loop is built to refuse.
//!
//! The third is the platform's own principle ledger, and that it is an account
//! rather than a file is the whole of the difference. Its rows are principles
//! nothing has found a gate for, and this intake takes the ones whose tier a
//! reading has already declared: the tier is what turns a slug into a goal,
//! because it says which lane the missing check belongs in, and a slug alone
//! tells a generator where to look and not what is wanted there. A principle a
//! person holds is left out, for the reason that ledger's own look refuses to
//! call it a debt -- no machine gate is meant to exist for it, so a task would be
//! work nothing could ever close. What closes one of these rows is the tree
//! carrying a header declaring the gate, which is the same declaration the
//! principle ledger's own look reads, so both accounts close a row on one fact.
//!
//! And the last reading is the second face of "is it done". The tree answers
//! whether the project's own line carries a change, and nothing in the tree
//! answers whether the workloads are running that revision -- they are apart for
//! the whole of every rollout, and no reading of the tree can see across the
//! gap. That second answer comes from a source published by whoever can read the
//! cluster, and this loop compares the account's landed rows against it, naming
//! the ones the running revision does not carry and saying so differently when
//! the comparison cannot be taken at all. It moves no row: being out of effect
//! is a fact about a face this account does not own, and the tree's own answer
//! stands.
//!
//! principle: feedback_a_version_bit_on_a_dispatch_id_must_keep_generation_zero_identical
//! principle: feedback_a_typed_field_that_exists_may_never_have_been_filled

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use cog_core::{
    metric_names, ChangeIdentity, ChangeLedgerRow, ChangeLedgerStore, ChangeMeasurement,
    ChangePriority, ChangeReading, ChangeReopen, ChangeSource, ChangeState, DispatchDecision,
    DispatchState, LiveRevision, LiveRevisionSource, LlmPoolStatus, MetricsBackend,
    OrchestratorControl, OwnerLeaseBroker, PoolReading, PrincipleCheckKind, PrincipleLedgerStore,
    PrincipleRow, ShutdownSignal, Task, TaskStatus, TaskType,
};

use cog_core::contract::llm::UpstreamFailure;
use cog_core::contract::outcome::{declares, TERMINAL_ENV_FAILURE_PREFIX};

use crate::change_measure::{
    CheckTree, Snapshot, UnsettledReason, DEPENDENCY_EDGE_KIND, DEV_DEPENDENCY_EDGE_KIND,
    GREP_KIND, MODULE_RELOCATION_KIND,
};

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

/// How many queued retirements one pass folds into the account.
///
/// The same shape as the declaration fold's bound and for the same reason: a
/// pass takes a bounded slice and the next takes the rest, so an intake that grew
/// large cannot make a single tick unbounded or hold the owner lease past its
/// cadence. Retirements are written by a classifier in batches of tens, so this
/// bound is generous for how it is used -- reaching it means the fold is behind,
/// which the backlog reading says anyway.
const RETIREMENTS_PER_PASS: usize = 200;

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

/// How long a row the account calls done may go before its check is asked again.
///
/// `landed` is the account's strongest claim -- that a check came out satisfied
/// -- and it was the one claim nothing re-read: the outstanding set excludes the
/// state by definition, so a landed row kept whatever it was first given while
/// the tree moved under it. Measured before this bound existed: the oldest of
/// 1369 landed rows had been measured a day and fifteen hours earlier, and eight
/// rows whose checks had stopped holding were still being reported as done.
///
/// The number is not chosen here, it is taken from the bound the platform
/// already gives a weaker claim. A row nothing could settle is re-asked within
/// [`UNMEASURABLE_RETRY`], and "I could not answer this" is a smaller claim than
/// "this is done"; a stronger claim answered less often than a weaker one would
/// be the account trusting the statement it makes most. So a landed row is
/// re-asked within the same window, and the compile-time assertion below is what
/// keeps that true: raising this past it, or lowering that one below it, is a
/// decision about which claim the account trusts more, and it does not get to be
/// made by editing one constant.
const REOPEN_BOUND: Duration = UNMEASURABLE_RETRY;

const _: () = assert!(
    REOPEN_BOUND.as_secs() <= UNMEASURABLE_RETRY.as_secs(),
    "a landed row may not be re-asked less often than a row nothing could settle: \
     that would make the account's strongest claim its least checked one"
);

/// How many landed rows one pass re-asks, given how many there are.
///
/// Derived rather than picked, because the number that matters is the window,
/// not the batch: a quota of `K` rows per pass over `N` landed rows at a pass
/// interval of `T` sweeps the state in `N * T / K`, and what a reader wants to be
/// true is that this is no longer than [`REOPEN_BOUND`]. Solving for the batch
/// gives the ceiling below, so the constant a person would otherwise have to
/// re-tune every time the ledger grows does not exist.
///
/// Rounded up: a quota that rounds down sweeps a little later than the bound
/// promises, and the promise is the point. Zero when there are no landed rows,
/// which keeps a caller from asking for a batch of nothing.
fn reopen_quota(landed: u64, pass: Duration, bound: Duration) -> usize {
    if landed == 0 || bound.is_zero() {
        return 0;
    }
    // In seconds, floored at one on both sides: a sub-second pass or bound would
    // otherwise divide by zero, and neither is a period this loop can run at.
    let swept = (landed as u128).saturating_mul(pass.as_secs().max(1) as u128);
    let window = bound.as_secs().max(1) as u128;
    // Rounded up: a partially swept window buys one re-ask, so the quota is the
    // number of windows the sweep lands in, not the number it fits inside.
    swept.div_ceil(window).min(usize::MAX as u128) as usize
}

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

/// The dependency registry, relative to the repository root.
///
/// One of the two reading sources this loop reads out of the tree. The file is
/// the same one the dependency contract test reads its own table from, so a row
/// registered there is a row that reaches the account, and the two readers cannot
/// drift apart: a running binary cannot read a test's constant, so the registry
/// lives in the tree and both take it from there.
const DEBT_EDGE_FILE: &str = "crates/cogneva/debt/dependency-edges.tsv";

/// The module registry, relative to the repository root.
///
/// The other reading source, the same file the core boundary contract test reads:
/// one row per module of the core's source root, with the reason it is a contract
/// and -- when a move is owed -- where it is going.
const DEBT_MODULE_FILE: &str = "crates/cogneva/debt/core-modules.tsv";

/// The `reason` a module row carries when it is owed a move.
///
/// Read here so that a registry which grew a destination under any other reason
/// names nothing to work on, rather than putting a move in the account that
/// nobody registered.
const RELOCATION_REASON: &str = "pending-relocation";

/// The core's own module list, relative to the repository root.
///
/// Where a module's move is measured: a module owed a move out of the core is
/// gone when this file stops declaring it, which is a fact about the tree and not
/// about the registry row.
const CORE_MODULE_LIST: &str = "crates/cog-core/src/lib.rs";

/// Where a principle's gate is looked for, as a path of the repository.
///
/// The platform's own sources, which is where the principle ledger reads its own
/// registry from: a contract test states in its header which principle it
/// enforces, and a running deployment has no checkout of its own source, so both
/// readings take it out of the same tree. A narrower scope would report a
/// principle as ungated while the ledger's look, over the whole of the sources,
/// finds the very gate that row was measured by -- two accounts disagreeing about
/// one fact, in the direction that mints work nobody owes.
const PRINCIPLE_GATE_SCOPE: &str = "crates";

/// The words a gate for a principle is declared with, up to its slug.
///
/// Spelled without the comment marker the header itself carries, and that is not
/// a shorthand. The judge anchors a literal whose ends are both word characters
/// on word boundaries, and the marker's last character is not one: a literal
/// beginning with it would land this change on a longer slug that merely begins
/// with this one, which is a closed row with no gate behind it. Measured on the
/// tree at `38167225`, with the pair the fixtures use -- `feedback_x` searched
/// against a file holding only `feedback_xy` -- the anchored search misses, the
/// plain fixed-string search hits, and the exact slug hits either way.
const PRINCIPLE_DECLARATION_PREFIX: &str = "principle: ";

/// What the source column of a principle reading says.
///
/// The account the rows were read out of, where the other two intakes name a file
/// in the tree: this reading's origin is another ledger, and the column is what a
/// reader follows back to it.
const PRINCIPLE_LEDGER_SOURCE: &str = "principle_ledger";

/// The task id a change is dispatched under, at a given handover generation.
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
///
/// The generation is the one part that is not the identity. A handover whose
/// task died to the environment leaves the change exactly as owed, so handing it
/// over again needs an id the graph does not already hold. Generation 0 is the
/// identity alone -- the id every handover carried before generations existed --
/// so a change dispatched earlier is still found under its own id and taken to
/// be in flight, not filed a second time. Only a re-arm moves the generation, and
/// only on the arm of [`classify_handover`] that says the environment, not the
/// change, is why the last attempt did not happen.
///
/// This is also the edge a reader uses to get back to a change's product. The
/// graph's task table is pruned and cleared per workspace, so a row there is not
/// something to join against, but a ledger row is kept, and its identity fixes
/// the id of every handover the change ever made. Recompute the id, match it as a
/// prefix against the event stream, and the product is found again without
/// anything having been stored to connect the two.
fn dispatch_task_id(identity: &ChangeIdentity, generation: u32) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [&identity.kind, &identity.scope, &identity.literal] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    let hex = hasher.finalize().to_hex();
    if generation == 0 {
        format!("{DISPATCH_TASK_PREFIX}{}", &hex[..16])
    } else {
        format!("{DISPATCH_TASK_PREFIX}{}-r{generation}", &hex[..16])
    }
}

/// What the graph's row for a change says about that change.
///
/// Reading the row rather than its mere presence. `is_some` was the whole test
/// until now, and it cannot tell a change being worked from one whose task died
/// to the environment: both leave a row under the id. The two are opposite. An
/// environment refusal is not the change being unworkable, it is the work not
/// having happened -- the change is still owed, and its row is not the handover
/// it looks like. A failure for a reason that is the change's own is the
/// opposite: the graph is the one retry authority, so a task that died on what
/// the change asked for is a row this loop must not file again.
enum Handover {
    /// A task is queued, scheduled or running: the change is being worked now.
    InFlight,
    /// The task finished. The tree is about to show it and the next measurement
    /// is what moves the row, so this loop files nothing.
    Completed,
    /// The task died for a reason that is the work's. This is an answer about
    /// the change, and handing it over again would be a second attempt at a
    /// question the graph has already answered.
    DeadWork,
    /// The task died to the environment -- a refusal that says "not this time",
    /// not "not this change". The change is still owed.
    DeadEnvironment,
}

/// Whether a failure named the environment rather than the work.
///
/// Read from the two channels the fail path writes, because the typed one is not
/// always there: the transport fills `error_cause` only when it saw a status
/// code, and a refusal discovered a crate deeper travels as text alone. A reason
/// that declares `terminal_env_failure` is an environment refusal even when its
/// typed cause was lost, and a reason that names a typed environment failure is
/// one even when the text never wrote the word.
fn is_environment_failure(reason: Option<&str>, cause: Option<UpstreamFailure>) -> bool {
    if cause.is_some_and(UpstreamFailure::is_environment_failure) {
        return true;
    }
    let Some(reason) = reason else {
        return false;
    };
    declares(reason, TERMINAL_ENV_FAILURE_PREFIX)
        || UpstreamFailure::named_in(reason).is_some_and(UpstreamFailure::is_environment_failure)
}

/// What a dispatched change's graph row says about it. See [`Handover`].
fn classify_handover(task: &Task) -> Handover {
    match task.status {
        TaskStatus::Pending | TaskStatus::Scheduled | TaskStatus::Running => Handover::InFlight,
        TaskStatus::Completed => Handover::Completed,
        TaskStatus::Failed | TaskStatus::Cancelled => {
            if is_environment_failure(task.error.as_deref(), task.error_cause) {
                Handover::DeadEnvironment
            } else {
                Handover::DeadWork
            }
        }
    }
}

/// How many generations a change may be handed over under before this loop
/// leaves it to the graph.
///
/// A choice this module makes, sourced rather than invented: the executor
/// declares a retry budget per task kind, and a handover is re-made up to that
/// many times ([`crate::dag_executor::RetryMatrix::defaults`]). The executor's
/// own answer for the failures this re-arms is *none* -- it judges them
/// terminal, so the budget it applies is zero -- and that difference is the
/// point: what the graph will not retry, the ledger hands over again.
///
/// The number is read from the same defaults the executor builds its own matrix
/// from, so it is written in one place; a matrix override would move the
/// executor's budget without moving this bound, and nothing overrides one
/// today. The cap only binds the pathological case -- an upstream reporting
/// itself available and then refusing every handover -- and reaching it leaves
/// the change owed and named in the pass line rather than re-filed forever.
fn dispatch_rearm_limit() -> u32 {
    crate::dag_executor::RetryMatrix::defaults()
        .max_retries(&TaskType::Custom(DISPATCH_TASK_KIND.to_string()))
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
///
/// `principles` is the account the third intake reads. Also optional, and for
/// the same reason: a deployment that publishes no principle ledger has no
/// unpaid principles to read, which is different from having some and not
/// looking.
///
/// `live_revision` is the second face of "is it done": a source that can read
/// which revision the workloads are running, published by the component whose
/// client and grants reach the cluster. Optional like the others, and the loop
/// says which it is at start-up rather than measuring landed rows against
/// nothing and reporting that as nothing being out of effect.
///
/// One parameter per collaborator the process may or may not publish, so the
/// list grows with the number of source accounts rather than with anything this
/// loop decides; grouping them into a struct would only move the same list to a
/// type that exists to hold it.
#[allow(clippy::too_many_arguments)]
pub fn spawn_change_ledger_loop(
    store: Arc<dyn ChangeLedgerStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    pool: Option<Arc<dyn cog_core::LlmPoolStatusSource>>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    shutdown: ShutdownSignal,
    tree: CheckTree,
    principles: Option<Arc<dyn PrincipleLedgerStore>>,
    live_revision: Option<Arc<dyn LiveRevisionSource>>,
) {
    info!(
        interval_secs = CHANGE_LEDGER_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        dispatching = orchestrator.is_some(),
        pool_gated = pool.is_some(),
        readable = metrics.is_some(),
        live_revision_read = live_revision.is_some(),
        "Change ledger loop enabled"
    );
    if principles.is_none() {
        // Said once at start-up rather than once per pass: a deployment with no
        // principle ledger has none to read, and a pass that repeated this every
        // five minutes would be a wall of lines saying what the start-up line
        // already said.
        info!(
            "change ledger: no principle ledger is published, so this loop reads the tree's own \
             registries and not the principles that ledger would call unpaid"
        );
    }
    if orchestrator.is_none() {
        warn!(
            "change ledger: no goal path is available, so a change found still owed will be \
             recorded and not handed to anyone"
        );
    }
    if pool.is_none() {
        warn!(
            "change ledger: no pool snapshot source is published, so this loop hands work over \
             without knowing whether any upstream can serve it -- seen, and recorded as absent"
        );
    }
    if live_revision.is_none() {
        // Said at start-up, once, rather than by a pass that measures nothing
        // against nothing: the tree's answer is then the account's only answer,
        // and a reader of the account has to know that the second face is not
        // being read rather than read as empty.
        warn!(
            "change ledger: nothing publishes the running revision, so this loop cannot say \
             whether the changes it has landed are in effect -- the tree's answer stands alone"
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
            let pool = pool.clone();
            let metrics = metrics.clone();
            let shutdown = shutdown.clone();
            let tree = tree.clone();
            let principles = principles.clone();
            let live_revision = live_revision.clone();
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

                // The pool posture the last pass that read the pool put dispatch
                // in. Kept across ticks and nowhere else: the only pass that
                // needs it is one whose source answered nothing, and the
                // posture a rollout interrupted is always the one from just
                // before it. A restart loses this, and losing it lands back on
                // the cold-start answer -- a state fact worth knowing, and one
                // the row keeps: a carried pause records `absent` as its
                // reading, so "blind since when" is readable from the series.
                let mut pool_posture: Option<PoolVerdict> = None;

                // The landed rows the running revision does not carry, by
                // identity, with the revision each of them was measured at. Held
                // across ticks because what a reader acts on is a row *entering*
                // or *leaving* this set: a set rebuilt from scratch every pass
                // can only say how many are in it, not which way any row moved,
                // and the arrival is the event -- the duration is not news.
                // Nothing here is written to the account; see
                // [`live_revision_pass`] for why the state stays where the tree
                // put it.
                let mut out_of_effect: HashMap<ChangeIdentity, String> = HashMap::new();

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
                    // The revision the pass folds and measures against, resolved
                    // once here so a row's entry revision is the pass's and not a
                    // second resolution's. A tree that cannot be opened leaves it
                    // `None`: the fold still happens -- declarations must enter
                    // the account whether or not the tree can be read -- and a row
                    // that enters then records no revision rather than a guessed
                    // one.
                    let first_seen_rev = tree.rev().await.ok();
                    read_pass(&*store, first_seen_rev.as_deref()).await;
                    // The tree's own registries are the second intake, and they
                    // go in after the fold and before the measurement for the
                    // reason [`registry_pass`] gives: a row read this pass is
                    // judged by this pass.
                    registry_pass(&tree, first_seen_rev.as_deref(), &*store).await;
                    // The third intake, from the platform's own principle ledger,
                    // in beside the registries and for the same reason: a row
                    // read this pass is judged by this pass. Skipped where no
                    // ledger was published, which the start-up line says.
                    if let Some(ref principles) = principles {
                        principle_pass(&**principles, first_seen_rev.as_deref(), &*store).await;
                    }
                    // The classifier's retirements go in after the readings and
                    // before the measurement, for the reason [`retire_pass`]
                    // gives: a fixture retired this pass is not measured and
                    // re-said as unsettled by the same pass.
                    retire_pass(&*store).await;
                    measure_pass(&*store, &tree, &mut unsettled).await;
                    // The second face of "is it done", read after the
                    // measurement so that a row this pass just found landed is
                    // checked in the same pass it landed in -- the gap between
                    // landing and running opens with the landing, and a check
                    // one period behind would report the first period of every
                    // rollout as nothing being out of effect.
                    //
                    // It moves no row, so its position relative to the hand-over
                    // below carries no ordering: what it reads is the landed
                    // set, and the hand-over reads the pending one.
                    if let Some(ref live) = live_revision {
                        live_revision_pass(&*store, &tree, &**live, &mut out_of_effect).await;
                    }
                    // Handing over comes last, and that order is the point: the
                    // set dispatched is then the set as of this pass, so a change
                    // this pass just found in the tree is never handed to anyone,
                    // and a change this pass just found missing is handed over on
                    // the same pass that found it, rather than one period later.
                    if let Some(ref orchestrator) = orchestrator {
                        let pool = pool.as_deref();
                        if let Some(read) = dispatch_pass(
                            &*store,
                            orchestrator,
                            &unsettled,
                            pool,
                            pool_posture,
                            metrics.as_ref(),
                        )
                        .await
                        {
                            pool_posture = Some(read);
                        }
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
async fn read_pass(store: &dyn ChangeLedgerStore, first_seen_rev: Option<&str>) {
    fold_pass(store, first_seen_rev).await;

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
async fn fold_pass(store: &dyn ChangeLedgerStore, first_seen_rev: Option<&str>) {
    match store
        .fold_declarations(DECLARATIONS_PER_PASS, first_seen_rev)
        .await
    {
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

/// Fold the retirements queued by the classifier, then say how many are still
/// queued.
///
/// A retirement is not a reading and does not ride the declaration intake: it
/// asserts that a row's identity is one the account should not hold, which is a
/// state, and the declaration intake's contract is that it never carries one. So
/// the classifier queues into a second intake and this pass -- the account's one
/// writer -- folds it, the same single-writer shape the declarations use. Folding
/// here, before the measurement, means a fixture the classifier retired this pass
/// is not handed back to the tree to be re-measured and re-said as unsettled in
/// the same pass.
///
/// Neither the fold's failure nor the count's is raised or retried: the next pass
/// is the retry, and every pass that had something to say leaves a line. A
/// non-zero backlog across passes is the reading that says the fold is behind
/// what it was told.
async fn retire_pass(store: &dyn ChangeLedgerStore) {
    match store.fold_retirements(RETIREMENTS_PER_PASS).await {
        Ok(0) => {}
        Ok(folded) => info!(folded, "change ledger: retirements folded into the account"),
        Err(e) => warn!(
            error = %e,
            "change ledger: folding the retirement intake failed; no row was retired this pass"
        ),
    }
    match store.pending_retirements().await {
        Ok(0) => {}
        Ok(pending) => info!(
            pending,
            "change ledger: retirements queued and not yet folded"
        ),
        Err(e) => warn!(error = %e, "change ledger: could not read the retirement backlog"),
    }
}

/// Read the tree's own debt registries into the account.
///
/// The second intake, beside the queued declarations, and run after the fold so
/// that a row named by both a document and a registry is one row either way: both
/// doors lead to the same reading path, which is an idempotent upsert keyed by
/// the identity.
///
/// Run before the measurement rather than after it, so a row read for the first
/// time in a pass is judged in that same pass: the registry is read at the
/// revision the pass measures, and a row read after the measurement would wait a
/// whole period for its first look. The hand-over that follows the measurement
/// reads the account, so it would not see such a row either.
///
/// Nothing here is raised: a pass is a maintenance tick and the next tick is the
/// retry, which is the rule the fold and the read follow too. A registry that
/// could not be read is not recorded as an empty one -- the account keeps the
/// rows it already holds and the warning says this pass contributed none, so "no
/// debt" and "no registry read" stay apart.
async fn registry_pass(
    tree: &CheckTree,
    first_seen_rev: Option<&str>,
    store: &dyn ChangeLedgerStore,
) {
    let readings = match registry_readings(tree).await {
        Ok(readings) => readings,
        Err(why) => {
            warn!(
                why = %why,
                "change ledger: the tree's own debt registries could not be read, so this pass \
                 contributed no readings from them"
            );
            return;
        }
    };
    match store.record_readings(&readings, first_seen_rev).await {
        Ok(_) => info!(
            readings = readings.len(),
            "change ledger: read the tree's own debt registries into the account"
        ),
        // The account keeps what it has: this pass added nothing, and the next is
        // the retry. Said with the count it was carrying so a reader can tell a
        // rejection from a registry that was empty.
        Err(e) => warn!(
            error = %e,
            readings = readings.len(),
            "change ledger: the tree's own debt registries could not be written into the account"
        ),
    }
}

/// What the tree's own debt registries ask for, as readings.
///
/// The registries are read at their own snapshot rather than one carried in from
/// the measuring pass. A reading carries no state and moves nothing: it says what
/// a row is about and is measured a moment later by the same pass, so a revision
/// that moved in between would only put a row in the account a few seconds early
/// for a tree that has since changed -- where sharing the measurement's snapshot
/// would mean threading one pass's revision through an intake that a caller other
/// than the loop could not supply.
async fn registry_readings(tree: &CheckTree) -> Result<Vec<ChangeReading>, String> {
    let snapshot = tree
        .snapshot()
        .await
        .map_err(|why| format!("the platform's own tree could not be resolved: {why}"))?;
    let rev = snapshot.rev();
    let edges = tree
        .matching_lines(rev, DEBT_EDGE_FILE, ".")
        .await
        .map_err(|why| {
            format!("the dependency registry {DEBT_EDGE_FILE} could not be read at {rev}: {why}")
        })?;
    let modules = tree
        .matching_lines(rev, DEBT_MODULE_FILE, ".")
        .await
        .map_err(|why| {
            format!("the module registry {DEBT_MODULE_FILE} could not be read at {rev}: {why}")
        })?;
    let mut readings = edge_readings(&edges);
    readings.extend(module_readings(&modules));
    Ok(readings)
}

/// Where a crate's manifest is, relative to the repository root.
///
/// A dependency is declared in its consumer's manifest, so that file is the scope
/// the debt is measured under -- and the literal searched for in it is the
/// dependency's own assignment.
fn manifest_of(consumer: &str) -> String {
    format!("crates/{consumer}/Cargo.toml")
}

/// What an edge row asks for, in the words the goal path is handed.
///
/// The row's own two ends and the contract it should go through instead. The
/// point of handing a registered debt over, rather than an ungated principle, is
/// that it can say what to build and not only where to look.
fn edge_goal(consumer: &str, dep: &str, contract: &str, dev: bool) -> String {
    if contract.is_empty() {
        return format!("Remove the {consumer} -> {dep} dependency by routing it through the core's contract layer.");
    }
    let when = if dev { "in its tests " } else { "" };
    format!("Make {consumer} reach {dep} through the core contract {contract} {when}instead of depending on it directly.")
}

/// What a module row asks for, in the words the goal path is handed.
///
/// The module and the crate it is going to, and -- for a module that keeps state
/// as well as defining a contract -- the shape of the move, so the generator is
/// told the module leaves in halves rather than whole.
fn module_goal(module: &str, destination: &str, split: bool) -> String {
    let shape = if split {
        " The module is a contract and a mechanism in one file: the half that keeps state moves, and the contract half stays in the core's contract layer."
    } else {
        ""
    };
    format!("Move the core module `{module}` out of cog-core's source root into the `{destination}` crate.{shape}")
}

/// The edge rows the dependency registry carries, as readings.
///
/// One row per line, tab-separated `kind consumer dep contract`, exactly as the
/// contract test reads it: a `#`-led line and the `kind consumer dep contract`
/// header carry no work, and a kind outside the two edge kinds is not a row this
/// intake knows how to aim.
///
/// The literal is the dependency's own assignment and not its bare name. A
/// manifest documents itself where the dependency is declared -- a comment naming
/// the crate a few lines above it is ordinary writing -- and a bare name matches
/// that comment too, which would keep a dependency that is already gone reading
/// as owed for good. The trailing `=` also keeps a shorter name from matching a
/// longer one that merely begins with it.
fn edge_readings(lines: &[(String, String)]) -> Vec<ChangeReading> {
    let mut readings = Vec::new();
    for (_path, text) in lines {
        let text = text.trim();
        if text.is_empty() || text.starts_with('#') {
            continue;
        }
        let mut field = text.split('\t');
        let (kind, dev) = match field.next() {
            Some("allowed") => (DEPENDENCY_EDGE_KIND, false),
            Some("allowed_dev") => (DEV_DEPENDENCY_EDGE_KIND, true),
            _ => continue,
        };
        let (Some(consumer), Some(dep)) = (field.next(), field.next()) else {
            continue;
        };
        let contract = field.next().unwrap_or("").trim();
        readings.push(ChangeReading {
            identity: ChangeIdentity::new(kind, manifest_of(consumer), format!("{dep} =")),
            title: Some(edge_goal(consumer, dep, contract, dev)),
            assertion: None,
            source: ChangeSource {
                doc: DEBT_EDGE_FILE.to_string(),
                verdict: None,
                // The debt file names the edge, not how urgent it is: nothing
                // in it states a band, so the reading carries the one that says
                // so rather than a band this path invented.
                priority: ChangePriority::Unstated,
            },
        });
    }
    readings
}

/// The module rows the module registry carries, as readings.
///
/// One row per line, tab-separated `module reason destination`, exactly as the
/// core boundary contract test reads it. Only a module the registry marks as
/// awaiting relocation *and* gives a destination is a row this intake can aim: a
/// module owed a move with no destination has no endpoint to name, so it stays
/// out of the account rather than entering it as work nobody registered.
///
/// The destination is `crate shape`, and the shape -- `direct` or `split` --
/// rides into the goal so a split module is described as leaving in halves.
fn module_readings(lines: &[(String, String)]) -> Vec<ChangeReading> {
    let mut readings = Vec::new();
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
        let (Some(crate_name), Some(shape)) = (parts.next(), parts.next()) else {
            continue;
        };
        readings.push(ChangeReading {
            identity: ChangeIdentity::new(
                MODULE_RELOCATION_KIND,
                CORE_MODULE_LIST,
                format!("mod {module};"),
            ),
            title: Some(module_goal(module, crate_name, shape == "split")),
            assertion: None,
            source: ChangeSource {
                doc: DEBT_MODULE_FILE.to_string(),
                verdict: None,
                priority: ChangePriority::Unstated,
            },
        });
    }
    readings
}

/// Read the principles the platform's own ledger still calls unpaid into the
/// account.
///
/// The third intake, and the first whose source is an account rather than a file
/// in the tree: what it carries is another account's finding that nothing
/// enforces this rule, which is a reading rather than a second answer -- the row
/// enters through the same call a declaration does, and only a measurement of the
/// tree moves it.
///
/// Run beside the registries and before the measurement, for the reason
/// [`registry_pass`] gives: a row read for the first time in a pass is judged in
/// that same pass.
///
/// Nothing here is raised: a pass is a maintenance tick and the next tick is the
/// retry. A ledger that could not be read is not recorded as an empty one -- the
/// account keeps the rows it already holds and the warning says this pass
/// contributed none, so "no principle is owed a gate" and "the ledger could not
/// be read" stay apart.
async fn principle_pass(
    principles: &dyn PrincipleLedgerStore,
    first_seen_rev: Option<&str>,
    store: &dyn ChangeLedgerStore,
) {
    let readings = match principle_readings(principles).await {
        Ok(readings) => readings,
        Err(why) => {
            warn!(
                why = %why,
                "change ledger: the platform's principle ledger could not be read, so this pass \
                 contributed no readings from it"
            );
            return;
        }
    };
    match store.record_readings(&readings, first_seen_rev).await {
        Ok(_) => info!(
            readings = readings.len(),
            "change ledger: read the principles the platform's own ledger calls unpaid"
        ),
        // The account keeps what it has: this pass added nothing, and the next is
        // the retry. Said with the count it was carrying so a reader can tell a
        // rejection from a ledger that named nothing.
        Err(e) => warn!(
            error = %e,
            readings = readings.len(),
            "change ledger: the principles the platform's own ledger calls unpaid could not be \
             written into the account"
        ),
    }
}

/// What the platform's own principle ledger says is unpaid, as readings.
///
/// The batch is sized from the source account's own census rather than from a
/// number chosen here: the population this reads is exactly the rows that account
/// calls unpaid, and a constant would be a second answer to a question it already
/// answers -- one that goes stale as principles are added. Unmeasured rows are
/// part of that population, because a row no look has settled is not a row with a
/// gate: the count this reads is the one the account's own coverage reading is
/// taken against.
async fn principle_readings(
    principles: &dyn PrincipleLedgerStore,
) -> Result<Vec<ChangeReading>, String> {
    let census = principles
        .census()
        .await
        .map_err(|e| format!("the principle ledger's census could not be read: {e}"))?;
    let owed = census.unenforced.saturating_add(census.unmeasured);
    let rows = principles
        .outstanding(usize::try_from(owed).unwrap_or(usize::MAX))
        .await
        .map_err(|e| format!("the principle ledger's unpaid set could not be read: {e}"))?;
    Ok(principle_debt_readings(&rows))
}

/// The unpaid principles whose tier a machine can check, as readings.
///
/// A row enters on two conditions and both are the source account's own: a
/// reading has declared its tier, and that tier is one of the three a machine
/// checks. The first is what makes it work rather than a note -- the tier says
/// which lane the missing check belongs in, so the goal can name what is wanted
/// instead of only where to look. The second is why `judgement` stays out: it is
/// the record that the rule is a person's to decide, and that account's own look
/// writes no state for such a row at all, so it stays in the unpaid set forever
/// by construction. Reading it here would mint a task no gate could ever close,
/// which is the same permanent debt its own verdict refuses to state.
///
/// The check is the tree carrying a header that declares the gate, which is the
/// declaration the principle ledger's own look reads as the platform-side gate
/// column. Both accounts therefore close this row on one fact, and neither can
/// close it while the other still calls it owed.
fn principle_debt_readings(rows: &[PrincipleRow]) -> Vec<ChangeReading> {
    let mut readings = Vec::new();
    for row in rows {
        let Some(kind) = row.check_kind else {
            continue;
        };
        let Some(lane) = check_lane(kind) else {
            continue;
        };
        readings.push(ChangeReading {
            identity: ChangeIdentity::new(
                GREP_KIND,
                PRINCIPLE_GATE_SCOPE,
                principle_literal(&row.slug),
            ),
            title: Some(principle_goal(row, lane)),
            assertion: None,
            source: ChangeSource {
                doc: PRINCIPLE_LEDGER_SOURCE.to_string(),
                verdict: None,
                // The ledger names the principle and its tier, not how urgent it
                // is: nothing there states a band, so the reading carries the one
                // that says so rather than a band this path invented.
                priority: ChangePriority::Unstated,
            },
        });
    }
    readings
}

/// The lane a tier's check answers from, or `None` for the tier a person holds.
///
/// One place rather than two, so what this intake reads into the account and what
/// it leaves out cannot disagree about a tier. The match is exhaustive over the
/// closed set, so a tier added later is a compile error here until someone
/// decides which of the two it is -- a tier silently falling out of the intake
/// would be a debt nothing ever asks for.
fn check_lane(kind: PrincipleCheckKind) -> Option<&'static str> {
    match kind {
        PrincipleCheckKind::Shape => {
            Some("the shape of the act it rules out, decided before the act")
        }
        PrincipleCheckKind::Tree => {
            Some("this tree at one revision, the way the platform's own tests answer")
        }
        PrincipleCheckKind::Live => Some("the running deployment's own readings"),
        PrincipleCheckKind::Judgement => None,
    }
}

/// The line a principle's gate is declared on, as the judge looks for it.
///
/// The declaration prefix and the slug, and nothing else: what the judge is asked
/// is whether the tree carries the declaration, and the rest of the header line
/// is the declaring test's own business.
fn principle_literal(slug: &str) -> String {
    format!("{PRINCIPLE_DECLARATION_PREFIX}{slug}")
}

/// What a row of the principle ledger asks for, in the words the goal path is
/// handed.
///
/// The slug, the lane its declared tier puts the missing check in, and the rule's
/// own sentence. The sentence is carried because a goal that names only where to
/// look is a goal a generator cannot act on, and the rule's own words are what
/// the gate has to answer to -- the same reason the registry intake carries the
/// destination and not only the module.
fn principle_goal(row: &PrincipleRow, lane: &str) -> String {
    let rule = row
        .lead
        .as_deref()
        .map(str::trim)
        .filter(|lead| !lead.is_empty())
        .map(|lead| format!(" The rule itself: {lead}"))
        .unwrap_or_default();
    format!(
        "Give the principle `{}` the gate it is declared to be owed: a check answering from \
         {lane}, declared in the header of the test that carries it.{rule}",
        row.slug
    )
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
/// Leaving a row alone is not the same as giving up on it. When what could not
/// be settled is a property of the tree, the tree moves: a path that is not
/// there this hour can be there the next, so an identity is unanswered for
/// [`UNMEASURABLE_RETRY`] and then asked again. That covers only the reasons a
/// re-run could change -- a failed look, a scope this revision lacks. A reason
/// that is a property of the row itself never resolves on a re-run, so it is not
/// left in this window at all: the evaluator retracts the row to `undecided`
/// (see [`UnsettledReason::is_about_the_row`]), which is what keeps a
/// bad check from being re-asked every hour forever.
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
    let mut undecided = 0usize;
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
            // A row whose reason is a property of the row rather than of the
            // look -- no evaluator for its kind, a predicate that is a finding
            // written out, a path this tree structurally cannot hold -- is
            // retracted to `undecided` by the evaluator: it carried a state no
            // later pass could ever reach. Counted separately from the unsettled
            // kinds below, because this one *did* write its state -- it is the
            // count of rows that left the outstanding set by being unmeasurable
            // rather than by being done.
            ChangeState::Undecided => undecided += 1,
            // A settled look otherwise answers with one of exactly two states,
            // both named above. A fourth reaching here would be one the
            // evaluator grew without this count being told, and counting it as
            // "not landed" would hide that.
            other => warn!(
                change = %row.identity,
                state = other.as_str(),
                "change ledger: a settled look named a state this pass does not count"
            ),
        }
        // No observation travels with the write. What this look saw is the check
        // and the revision, and the revision rides in its own column rather than
        // in a rendered note the storage does not keep.
        //
        // An undecided row is retracted for a reason that is a property of the
        // row itself, and that reason is the whole answer to "why will this never
        // settle by retrying". It is written with the state so a reader can ask
        // *which* rows are stuck for *which* reason instead of only how many are
        // stuck: the reason travels as its name, because the vocabulary is this
        // judge's and the storage contract does not depend on this module.
        let measurement = match state {
            ChangeState::Undecided => ChangeMeasurement::undecided(
                row.identity.clone(),
                outcome
                    .reason()
                    .expect("an undecided state is reached only through a reason")
                    .as_str(),
            )
            .at_revision(snapshot.rev()),
            _ => ChangeMeasurement::new(row.identity.clone(), state).at_revision(snapshot.rev()),
        };
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

    let (rechecked, reopened) = reopen_pass(store, tree, &snapshot, unsettled).await;

    info!(
        measured = considered,
        landed,
        absent,
        undecided,
        rechecked,
        reopened,
        unsettled = %reason_summary(&by_reason),
        already_unsettled = known_unsettled,
        revision = snapshot.rev(),
        "change ledger: measured the outstanding set against the tree"
    );
}

/// Ask the rows the account calls done whether that is still true.
///
/// The outstanding set is the work, and it excludes `landed` by definition --
/// which left the account's strongest claim as the only one nothing re-read: a
/// landed row kept whatever it was first given while the tree moved under it.
/// Measured before this ran: eight rows whose checks had stopped holding were
/// still being reported as done, and nothing in the account distinguished them
/// from the ones that were.
///
/// A read of its own, with a bound of its own, and the two bounds are not
/// interchangeable: [`OUTSTANDING_PEEK`]'s ordering puts the least-measured rows
/// first, so a batch wide enough to sweep the work would put a day-old landed row
/// at the head of it and starve the work of the pass it shares. The batch here is
/// [`reopen_quota`]'s answer -- whatever sweeps every landed row within
/// [`REOPEN_BOUND`] -- so the ledger growing changes how much one pass spends
/// without changing how long a wrong answer stands.
///
/// A row whose check cannot be settled is left exactly as it is. Its state is the
/// account's last word on it and a look that settled nothing has nothing to put
/// in its place; the answer, not the asking, is what moves a row.
///
/// Returns how many rows were re-asked and how many came back as something other
/// than what they said. The second is the reading this whole pass exists for: a
/// count of moments the account was wrong, and the rows it names are in
/// `change_reopen`, which is where the identity survives the state it loses.
async fn reopen_pass(
    store: &dyn ChangeLedgerStore,
    tree: &CheckTree,
    snapshot: &Snapshot,
    unsettled: &mut HashMap<ChangeIdentity, UnsettledCheck>,
) -> (usize, usize) {
    let landed = match store.census().await {
        Ok(census) => census
            .iter()
            .filter(|(state, _)| *state == ChangeState::Landed)
            .map(|(_, count)| *count)
            .sum::<u64>(),
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the census could not be read, so no landed row was re-asked"
            );
            return (0, 0);
        }
    };
    let quota = reopen_quota(landed, CHANGE_LEDGER_PASS_INTERVAL, REOPEN_BOUND);
    if quota == 0 {
        return (0, 0);
    }
    // The batch is widened by the rows already known unsettleable, the same way
    // the outstanding pass widens its own, and for the same reason: a row whose
    // check cannot be settled keeps its `measured_at`, so it holds the head of
    // this order every pass and would spend the whole batch on itself. Without
    // the widening the quota would be a claim the pass makes and quietly stops
    // keeping -- the sweep would lengthen by exactly how many rows the tree
    // cannot answer for, which is the state the bound is supposed to survive.
    //
    // Widened by the whole map rather than by its landed part: the read that
    // would filter it is the one being widened.
    let take = quota.saturating_add(unsettled.len());
    let rows = match store.landed_due(take).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the landed set could not be read, so nothing was re-asked"
            );
            return (0, 0);
        }
    };

    let now = Instant::now();
    let mut rechecked = 0usize;
    let mut reopened = 0usize;
    for row in rows {
        // The same window the outstanding pass keeps, for the same reason: a row
        // whose check cannot be settled keeps its old `measured_at`, so it would
        // hold the head of the staleness order and spend this pass's batch every
        // pass. Asking again before the window is up would buy nothing -- what
        // makes the check unsettleable is a property of the tree, and the tree is
        // what the window is waiting for.
        if let Some(known) = unsettled.get(&row.identity) {
            if still_unanswered(known, now) {
                continue;
            }
        }
        let outcome = tree.judge(snapshot, &row.identity).await;
        let Some(state) = outcome.state() else {
            let reason = outcome.reason().unwrap_or(UnsettledReason::GitFailed);
            let why = outcome
                .why()
                .unwrap_or("the look settled nothing and gave no reason")
                .to_string();
            unsettled.insert(row.identity.clone(), UnsettledCheck { why, since: now });
            debug!(
                change = %row.identity,
                reason = reason.as_str(),
                "change ledger: a landed row's check cannot be settled, so it keeps its state"
            );
            continue;
        };
        unsettled.remove(&row.identity);
        rechecked += 1;
        if state != ChangeState::Landed {
            // Written before the measurement, and that order is the point: the
            // measurement clears `landed_at` on the way out of `landed`, so
            // afterwards there is nothing left in the row that says it was ever
            // done. This is the only place that time survives, and it has to be
            // read off the row before the write that takes it away.
            reopened += 1;
            let reopen = ChangeReopen {
                identity: row.identity.clone(),
                was_landed_at: row.landed_at,
                measured_rev: snapshot.rev().to_string(),
            };
            if let Err(e) = store.record_reopen(&reopen).await {
                warn!(
                    change = %row.identity,
                    error = %e,
                    "change ledger: a reopen could not be recorded, so the row loses its \
                     landed time with nothing to say it was there"
                );
            }
            warn!(
                change = %row.identity,
                was_landed_at = ?row.landed_at,
                state = state.as_str(),
                revision = snapshot.rev(),
                "change ledger: a row the account had as done no longer holds"
            );
        }
        // Written back even when it still holds, and that write is not a
        // formality: `measured_at` is what this read orders by, so a pass that
        // re-asked and wrote nothing would re-ask the same head of the set
        // forever and never reach the rest of it.
        let measurement = match state {
            ChangeState::Undecided => ChangeMeasurement::undecided(
                row.identity.clone(),
                outcome
                    .reason()
                    .expect("an undecided state is reached only through a reason")
                    .as_str(),
            )
            .at_revision(snapshot.rev()),
            _ => ChangeMeasurement::new(row.identity.clone(), state).at_revision(snapshot.rev()),
        };
        if let Err(e) = store.apply_measurement(&measurement).await {
            warn!(
                change = %row.identity,
                error = %e,
                "change ledger: a second look could not be written back"
            );
        }
    }
    (rechecked, reopened)
}

/// Check the account's landed rows against the revision the workloads are
/// running.
///
/// "Landed" is a claim about the project's own line of development, and
/// [`measure_pass`] reads it out of the tree. Whether that revision is also the
/// one *running* is a question about a second face, and the two are apart for
/// the whole of every rollout -- a build, a CI verdict, an admission decision and
/// a rolling update all sit inside that gap, which is measured in minutes and
/// sometimes hours. No reading of the tree crosses it, so an account that only
/// looks at the tree reports a merged change as done while nothing is running it
/// and goes on saying so until the rollout lands, with nothing anywhere that says
/// the two are apart.
///
/// So this pass takes the second reading and compares the two. It **moves no
/// row**: the account's own answer about the tree is right, and being out of
/// effect is a fact about a face this account does not own. What it produces is
/// the annotation -- one line per row the running revision does not carry, said
/// when the row *enters* that set, beside a line per pass carrying the counts.
///
/// Said on arrival rather than every pass, and that is not a saving. A row out of
/// effect is out of effect for exactly as long as the rollout takes, so a line
/// per row per pass would repeat the arrival until it could not be told from the
/// duration, and the reader who has to act -- somebody is looking at a rollout
/// that is not finishing -- would be reading the one fact this pass exists to
/// report. The count is on every pass, so "how many" is never lost; only the
/// repetition is.
///
/// Three readings, and the third is why this is not a boolean. The workloads may
/// be on one main revision, they may be split across two because a rollout is
/// between them, or the cluster may not answer at all. The last two are different
/// faults with different remedies -- one is a rollout in progress, the other is a
/// grant or an API server -- and neither is "nothing is out of effect". A bare
/// revision plus a default would say exactly that about both, which is why the
/// source answers with a shape that has no default in it.
///
/// Nothing here is raised: this is a maintenance tick, the next tick is the retry,
/// and a pass that could not take the reading leaves a line saying so rather than
/// leaving a count that reads as zero.
async fn live_revision_pass(
    store: &dyn ChangeLedgerStore,
    tree: &CheckTree,
    source: &dyn LiveRevisionSource,
    out_of_effect: &mut HashMap<ChangeIdentity, String>,
) {
    let live = match source.live_revision().await {
        LiveRevision::Known(rev) => rev,
        LiveRevision::Unmeasurable(gap) => {
            warn!(
                gap = gap.as_str(),
                held = out_of_effect.len(),
                "change ledger: the revision the workloads are running could not be read, so no \
                 landed row was checked against it this pass -- which is not the same as a pass \
                 that found nothing out of effect"
            );
            return;
        }
    };
    // A revision the tree cannot place is the third fault, and it belongs with
    // the two above rather than with the rows: nothing was learned about any row
    // here either. It is also the one fault that can be a row's own property
    // inverted -- the account may hold revisions this repository was never given
    // -- so it is named with the revision rather than counted as an answer.
    let reach = match tree.reach(&live).await {
        Ok(reach) => reach,
        Err(why) => {
            warn!(
                live_rev = %live,
                why = %why,
                held = out_of_effect.len(),
                "change ledger: the revision the workloads are running names no commit in this \
                 tree, so no landed row was checked against it this pass -- a revision this tree \
                 cannot place is not a row that is out of effect"
            );
            return;
        }
    };
    let landed = match store.census().await {
        Ok(census) => census
            .iter()
            .filter(|(state, _)| *state == ChangeState::Landed)
            .map(|(_, count)| *count)
            .sum::<u64>(),
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the landed count could not be read, so no landed row was checked \
                 against the revision the workloads are running"
            );
            return;
        }
    };
    // The bound is the population itself rather than a number chosen for it. The
    // question is "is anything the account calls landed not in effect", and an
    // answer over a share of the landed rows would read as an answer over all of
    // them -- a count of the rows that happened to sort first, which is the
    // shape the dispatch bound's own comment calls a claim the pass stops
    // keeping. A constant here would be a guess at how large the account gets,
    // and the account already answered that: its own count is the read.
    let rows = match store.landed_due(landed as usize).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the landed rows could not be read, so nothing was checked against \
                 the revision the workloads are running"
            );
            return;
        }
    };

    let mut still_landed: HashSet<ChangeIdentity> = HashSet::with_capacity(rows.len());
    let mut checked = 0usize;
    let mut in_effect = 0usize;
    let mut entered = 0usize;
    let mut recovered = 0usize;
    let mut no_revision = 0usize;
    for row in rows {
        let Some(measured) = row.measured_rev.as_deref() else {
            // Landed with no revision named: a measurement taken by a process
            // that had none to name. It cannot be placed on any line, so it is
            // counted apart rather than folded into either side -- the same
            // three-valued discipline the tree reading keeps, one level up.
            no_revision += 1;
            continue;
        };
        still_landed.insert(row.identity.clone());
        checked += 1;
        if reach.contains(measured) {
            in_effect += 1;
            if out_of_effect.remove(&row.identity).is_some() {
                recovered += 1;
                info!(
                    change = %row.identity,
                    measured_rev = %measured,
                    live_rev = reach.head(),
                    "change ledger: this change is in the revision the workloads are running now"
                );
            }
            continue;
        }
        let arrived = match out_of_effect.get(&row.identity) {
            Some(known) => known != measured,
            None => true,
        };
        if arrived {
            out_of_effect.insert(row.identity.clone(), measured.to_string());
            entered += 1;
            warn!(
                change = %row.identity,
                measured_rev = %measured,
                live_rev = reach.head(),
                "change ledger: this change's revision is not in the live rev -- the project's own \
                 line carries it and the workloads are not running it"
            );
        }
    }
    // A row that is no longer landed at all -- reopened, or retired -- is not out
    // of effect any more, and dropping it here is what keeps the next landing of
    // the same identity an arrival rather than a state it was already in.
    out_of_effect.retain(|identity, _| still_landed.contains(identity));

    info!(
        landed_rows = landed,
        checked,
        in_effect,
        out_of_effect = out_of_effect.len(),
        entered,
        back_in_effect = recovered,
        no_revision,
        live_rev = reach.head(),
        "change ledger: checked the account's landed rows against the revision the workloads are \
         running"
    );
}

/// The rows a pass hands over: the ones still owed that this pass has not just
/// found it cannot measure.
///
/// A row the pass left unanswered is dropped here rather than at submission, so
/// that "what is outstanding" and "what is owed to somebody" stay two readable
/// sets. A row may be both -- a bad check keeps its `pending` state -- and
/// splitting them here is what keeps the handover from becoming a claim about
/// the tree.
fn dispatchable(
    rows: &[ChangeLedgerRow],
    unsettled: &HashMap<ChangeIdentity, UnsettledCheck>,
) -> Vec<ChangeLedgerRow> {
    rows.iter()
        .filter(|row| row.state == ChangeState::Pending)
        .filter(|row| !unsettled.contains_key(&row.identity))
        .cloned()
        .collect()
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
/// A `pending` row this same pass could not settle is left out too, and that one
/// is not a state: it is what the pass learned. Being unsettled moves no state --
/// the row keeps what a document gave it, which for a bad check is `pending` --
/// so the state alone would hand over exactly the rows nothing can measure. The
/// task would then be work no artifact can close, sitting on the goal path for
/// good, and the tree would never show why.
///
/// The task's id is derived from the change's identity, so a change handed over
/// once is not handed over again by accident: this loop re-reads the same
/// outstanding set every period, and the graph's own row is what remembers the
/// handover. The ledger runs no retry authority of its own -- it never re-runs a
/// failed attempt. What it does is read the row the graph holds rather than only
/// whether it holds one, and act on the one answer that is not a retry question
/// at all: an attempt that did not happen -- a handover the environment returned
/// -- is not a change that has been tried, so it is handed over again under the
/// next generation. Which reason is the environment's is [`classify_handover`]'s
/// answer, and each re-made handover spends a generation of the budget the task
/// kind declares ([`dispatch_rearm_limit`]). The executor's own verdict on these
/// failures is terminal, with no budget applied at all, so this is the ledger
/// acting where the graph declined to rather than the graph retrying after all.
///
/// A dispatched change's state is not touched. Handing work over is not doing it,
/// and a row moved because a task was filed would have the ledger reporting a
/// tree that nothing had looked at.
async fn dispatch_pass(
    store: &dyn ChangeLedgerStore,
    orchestrator: &Arc<dyn OrchestratorControl>,
    unsettled: &HashMap<ChangeIdentity, UnsettledCheck>,
    pool: Option<&dyn cog_core::LlmPoolStatusSource>,
    carried: Option<PoolVerdict>,
    metrics: Option<&Arc<dyn MetricsBackend>>,
) -> Option<PoolVerdict> {
    // The read is bounded by the ledger's own census rather than by a number
    // chosen here, and it has to be that wide because of where pending rows sit
    // in it. A state of `pending` *is* a measurement's result, so those rows are
    // the freshest in a staleness-ordered read, behind every row nothing has
    // looked at; measured on the live ledger, the first fifty outstanding rows
    // were all of them `undecided`. A bound the size of the one this pass
    // measures with would therefore hand over nothing for as long as unmeasured
    // rows remain ahead of them. Taking the whole outstanding set -- both
    // terminal states excluded, the same exclusion the read itself applies, so the
    // bound and the set it bounds stay one definition -- is what makes "the
    // pending set" the thing this reads, and it needs no constant to stay true as
    // the ledger grows.
    let outstanding_total = match store.census().await {
        Ok(census) => census
            .iter()
            .filter(|(state, _)| state.is_outstanding())
            .map(|(_, count)| *count)
            .sum::<u64>(),
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the census could not be read, so this pass handed nothing over"
            );
            return None;
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
            return None;
        }
    };

    let pending = dispatchable(&rows, unsettled);

    // Counted apart from the states because these rows are `pending`: the state
    // line cannot show them, and a pass that quietly stopped handing rows over
    // would otherwise read as a pass with fewer changes owed in it.
    let unmeasurable = rows
        .iter()
        .filter(|row| row.state == ChangeState::Pending)
        .filter(|row| unsettled.contains_key(&row.identity))
        .count();

    // Three cases, and the middle one is why this is not one call to
    // `pool_verdict`: no source published at all, a snapshot that says the pool
    // is sick or well, and a source that is published and answers nothing this
    // pass. The last one is not a verdict about the pool -- both halves of the
    // signal roll with every deployment, so an unanswered read is an ordinary
    // sight during a rollout -- and taking it as a healthy pool is what hands a
    // whole batch over blind, measured: one such tick handed 133 rows to a pool
    // whose next request came back refused. The posture the last real read put
    // dispatch in is the answer, and the reading column says plainly that this
    // pass read nothing, so "handed over blind" stays distinguishable from
    // "handed over behind a healthy pool". A pass with nothing to carry over is
    // the first pass after a start, and it proceeds as it always did.
    let now = now_unix();
    let snapshot = match pool {
        Some(source) => source.status().await,
        None => None,
    };
    let verdict = match pool {
        None => pool_verdict(None, now, CHANGE_LEDGER_PASS_INTERVAL.as_secs()),
        Some(_) => match snapshot.as_ref() {
            Some(status) => pool_verdict(Some(status), now, CHANGE_LEDGER_PASS_INTERVAL.as_secs()),
            None => match carried.filter(|posture| posture.reading != PoolReading::Absent) {
                Some(posture) => PoolVerdict {
                    reading: PoolReading::Absent,
                    ..posture
                },
                None => pool_verdict(None, now, CHANGE_LEDGER_PASS_INTERVAL.as_secs()),
            },
        },
    };

    // A pass whose tasks could only fail is not a pass: no configured upstream
    // can serve a request, so every task handed over now is work that will come
    // back with the pool's own error and leave the change exactly as owed as it
    // was. The handover stops, and the stop is written down where it survives
    // the process -- a pending set that nothing is acting on reads the same as a
    // pending set nobody has reached yet, which is how a whole day of dispatch
    // could land on a dead pool without the account saying so.
    //
    // The classification below runs either way. What the pause withholds is the
    // handover, and only the handover: whether a task already holds a row, and
    // whether the environment has already killed one, are answers that come from
    // the graph and are not the pool's to give. A pass that wrote zeros for
    // every count but `pending` on exactly the passes it exists to describe
    // would be a reading face that is blind where it is read.
    let pausing = verdict.decision == DispatchDecision::PausedLlmPoolUnavailable;

    let rearm_limit = dispatch_rearm_limit();
    let mut counts = TickCounts::default();
    for row in &pending {
        match owed_row(orchestrator, row, rearm_limit).await {
            Owed::Held(Handover::DeadEnvironment) => counts.dead_environment += 1,
            Owed::Held(_) => counts.already += 1,
            Owed::NoTask { generation } => {
                if pausing {
                    // Owed, held by no task, and not handed over because this
                    // pass decided to hand nothing over. Not `failed`: nothing
                    // was attempted, and a refusal the pool never gave is not
                    // this change's answer.
                    counts.held_back += 1;
                    continue;
                }
                if generation > 0 {
                    counts.rearmed += 1;
                }
                let task_id = dispatch_task_id(&row.identity, generation);
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
                    Ok(_) => counts.submitted += 1,
                    Err(e) => {
                        counts.failed += 1;
                        warn!(
                            change = %row.identity,
                            error = %e,
                            "change ledger: a change still owed could not be handed over"
                        );
                    }
                }
            }
        }
    }

    if pausing {
        warn!(
            pending = pending.len(),
            unmeasurable,
            held_back = counts.held_back,
            already_handed_over = counts.already,
            already_dead_environment = counts.dead_environment,
            resume_at_unix = verdict.resume_at_unix,
            evidenced_recovery_unix = verdict.evidenced_recovery_unix,
            quota_window_secs = verdict.quota_window_secs,
            upstreams = snapshot
                .as_ref()
                .map(|status| status.unavailable_upstreams.join(","))
                .unwrap_or_default(),
            "change ledger: no configured upstream can serve, so this pass handed nothing over"
        );
    } else {
        info!(
            pending = pending.len(),
            submitted = counts.submitted,
            rearmed = counts.rearmed,
            already_handed_over = counts.already,
            already_dead_environment = counts.dead_environment,
            failed = counts.failed,
            unmeasurable,
            pool = verdict.reading.as_str(),
            not_handed_over = %held_back_summary(&rows),
            "change ledger: handed the pending set to the goal path"
        );
    }
    record_dispatch_state(
        store,
        metrics,
        &verdict,
        pending.len(),
        unmeasurable,
        &counts,
    )
    .await;

    // Back to the loop, which keeps it for the next pass's empty read. Handed
    // back only when the pool was actually read: an `absent` verdict carried
    // forward would be the posture of a pass that knew nothing, which is the
    // thing being fixed.
    (verdict.reading != PoolReading::Absent).then_some(verdict)
}

/// What one owed row's graph row says about it.
///
/// Two answers, and every row read reaches exactly one of them: that is what
/// makes the pass's counts add up to the set it read, whichever way the pass
/// went. A pass that hands nothing over is not a pass that found nothing.
enum Owed {
    /// A task holds the row, and its own row says what became of the change.
    Held(Handover),
    /// No task holds it: this is the generation its handover goes out under.
    NoTask { generation: u32 },
}

/// Whether a task holds this owed row, and the generation the handover goes out
/// under when none does.
///
/// The graph is read before the submission rather than the submission's answer
/// being read after it: the goal path treats a repeated id as a fault, so a pass
/// that submitted blind would leave a line saying the handover failed while the
/// change was in fact handed over. What is read is the row and not its presence:
/// a change whose last handover the environment returned is still owed, and the
/// id it already holds would otherwise read as work in progress for the rest of
/// the account's life. The generation the handover goes out under is the first
/// slot the graph does not hold.
async fn owed_row(
    orchestrator: &Arc<dyn OrchestratorControl>,
    row: &ChangeLedgerRow,
    rearm_limit: u32,
) -> Owed {
    let mut generation = 0u32;
    while generation <= rearm_limit {
        let id = dispatch_task_id(&row.identity, generation);
        let Some(task) = orchestrator.get_task(&id).await else {
            break;
        };
        match classify_handover(&task) {
            // An environment's refusal is not this change's answer, so the next
            // generation is where its handover goes -- unless the budget is
            // spent, and then the change is left owed and named instead of
            // re-filed forever.
            Handover::DeadEnvironment if generation < rearm_limit => generation += 1,
            verdict => return Owed::Held(verdict),
        }
    }
    Owed::NoTask { generation }
}

/// One pass's own counts, as the account records them and the reading face
/// publishes them.
///
/// The fields are the columns of one row of the dispatch table and the cells of
/// one tick of the metric store, and they are one struct so the two cannot
/// drift: a count added here is added to both or to neither. `rearmed` is the
/// one that is an annotation rather than an outcome -- a row re-armed and then
/// submitted is in both numbers -- so the pass's arithmetic is `submitted +
/// already + failed + dead_environment + held_back == pending`, with
/// `unmeasurable` counted separately because those rows were never in the
/// dispatched set to begin with.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TickCounts {
    submitted: usize,
    already: usize,
    dead_environment: usize,
    held_back: usize,
    rearmed: usize,
    failed: usize,
}

/// The names the per-pass counts are published under.
///
/// A closed set, so a reader counts cells rather than parsing names, and the
/// order here is the order they are written in. The values are the account's own
/// numbers, taken after they were computed and before they were written -- the
/// same ones, not a second computation of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchReadout {
    Pending,
    Submitted,
    Already,
    Failed,
    DeadEnvironment,
    HeldBack,
    Rearmed,
    Unmeasurable,
}

impl DispatchReadout {
    const ALL: &'static [DispatchReadout] = &[
        DispatchReadout::Pending,
        DispatchReadout::Submitted,
        DispatchReadout::Already,
        DispatchReadout::Failed,
        DispatchReadout::DeadEnvironment,
        DispatchReadout::HeldBack,
        DispatchReadout::Rearmed,
        DispatchReadout::Unmeasurable,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Submitted => "submitted",
            Self::Already => "already",
            Self::Failed => "failed",
            Self::DeadEnvironment => "dead_environment",
            Self::HeldBack => "held_back",
            Self::Rearmed => "rearmed",
            Self::Unmeasurable => "unmeasurable",
        }
    }
}

/// Publish one pass's counts, one cell per readout.
///
/// The readout is the whole label set, and the pass's own answer to why the tick
/// looks the way it does is a family of its own -- [`publish_dispatch_verdict`].
/// It used to be two more labels here, so that a count and its reason were one
/// series, and that is the shape a gauge cannot carry: a label whose value
/// changes with the tick is not part of a series' identity, and the store keeps
/// the newest row of every label set it has ever held and exempts it from its
/// own sweep, so every combination a pass ever reached stayed behind as its last
/// value and was served for ever. A combination nothing produces any more then
/// read as a live count -- and the panel's `max` across label sets preferred it
/// whenever it was the larger of the two, which for a count that falls to zero
/// is every time it matters. The same reasoning, and the same repair, as the
/// verdict's own family: enumerate a closed set under a fixed key set and write
/// every cell, rather than let the value move the key.
///
/// A failure to publish is raised and not fatal, for the reason a failure to
/// record the row is: the pass already happened, and a loop that gave up the
/// handover because the reading face was closed would trade the working half for
/// the recorded half.
async fn publish_dispatch_rows(metrics: Option<&Arc<dyn MetricsBackend>>, state: &DispatchState) {
    let Some(metrics) = metrics else {
        return;
    };
    for readout in DispatchReadout::ALL {
        let value = match readout {
            DispatchReadout::Pending => state.pending,
            DispatchReadout::Submitted => state.submitted,
            DispatchReadout::Already => state.already,
            DispatchReadout::Failed => state.failed,
            DispatchReadout::DeadEnvironment => state.dead_environment,
            DispatchReadout::HeldBack => state.held_back,
            DispatchReadout::Rearmed => state.rearmed,
            DispatchReadout::Unmeasurable => state.unmeasurable,
        };
        let mut labels = HashMap::new();
        labels.insert("readout".to_string(), readout.as_str().to_string());
        if let Err(e) = metrics
            .record_gauge(metric_names::CHANGE_DISPATCH_READOUTS, value as f64, labels)
            .await
        {
            warn!(
                error = %e,
                readout = readout.as_str(),
                "change ledger: this pass's count could not be published, so the reading face \
                 is missing a cell this pass held"
            );
        }
    }
}

/// Publish the verdict the pass reached, as one cell per combination of the two
/// closed sets: `1` on the combination this pass holds and `0` on the rest.
///
/// Every cell on every pass, rather than the one the pass reached. A key set
/// whose cells are written only when they occur is a key set that goes on being
/// served: the store renders a gauge from the newest row of each of its label
/// sets and releases none of them, so the pass that moves from one verdict to
/// another would leave the old one at `1` and a reader asking "what is the
/// reason now" would be answered by both for ever. Writing the zero is also what
/// makes the two closed sets readable as a set: a reader sees that a combination
/// it never encounters is not one the pass is holding, which an absent cell
/// cannot say, because absent is also what a deployment with no dispatch loop at
/// all renders as.
///
/// Bounded by construction, and small: the cells a fleet of passes can create
/// are the product of the two enumerations, not of the ticks they have run.
/// The reason half of the reading is not on the counts' own family, which is
/// where it was: the reason is what changes, and a value that changes belongs in
/// the cells rather than in the key.
///
/// A failure to publish is raised and not fatal, on the same grounds as the
/// counts: the pass already happened, and the verdict is in the account's own
/// row whether or not the reading face took it.
async fn publish_dispatch_verdict(
    metrics: Option<&Arc<dyn MetricsBackend>>,
    state: &DispatchState,
) {
    let Some(metrics) = metrics else {
        return;
    };
    for decision in DispatchDecision::ALL {
        for pool in PoolReading::ALL {
            let holds = decision == state.decision && pool == state.pool;
            let mut labels = HashMap::new();
            labels.insert("decision".to_string(), decision.as_str().to_string());
            labels.insert("pool_reading".to_string(), pool.as_str().to_string());
            if let Err(e) = metrics
                .record_gauge(
                    metric_names::CHANGE_DISPATCH_VERDICT,
                    if holds { 1.0 } else { 0.0 },
                    labels,
                )
                .await
            {
                warn!(
                    error = %e,
                    decision = decision.as_str(),
                    pool_reading = pool.as_str(),
                    "change ledger: this pass's verdict could not be published, so the reading \
                     face is missing a cell of it"
                );
            }
        }
    }
}

/// Write one pass's own reading, and never let a failure to write it stop the
/// pass.
///
/// A pass that could not record what it did still did it, and a loop that gave
/// up on the handover because the reading's table was missing would trade the
/// working half for the recorded half. The failure is raised where it is a
/// problem for a reader: it says the pause is not on record, which reads as a
/// silent stop, and that is the one thing this table exists to prevent.
async fn record_dispatch_state(
    store: &dyn ChangeLedgerStore,
    metrics: Option<&Arc<dyn MetricsBackend>>,
    verdict: &PoolVerdict,
    pending: usize,
    unmeasurable: usize,
    counts: &TickCounts,
) {
    let state = DispatchState {
        decision: verdict.decision,
        pool: verdict.reading,
        pending: pending as u64,
        submitted: counts.submitted as u64,
        already: counts.already as u64,
        failed: counts.failed as u64,
        dead_environment: counts.dead_environment as u64,
        held_back: counts.held_back as u64,
        rearmed: counts.rearmed as u64,
        unmeasurable: unmeasurable as u64,
        resume_at_unix: verdict.resume_at_unix,
        evidenced_recovery_unix: verdict.evidenced_recovery_unix,
        quota_window_secs: verdict.quota_window_secs,
    };
    if let Err(e) = store.record_dispatch_state(&state).await {
        warn!(
            error = %e,
            decision = state.decision.as_str(),
            "change ledger: what this pass decided could not be recorded, so the decision is \
             only in this line"
        );
    }
    // Published from the same struct the row was built from, and after it: the
    // account is the record and the reading face is a view of it, so the two
    // cannot say different things about one pass.
    publish_dispatch_rows(metrics, &state).await;
    publish_dispatch_verdict(metrics, &state).await;
}

/// What the pool's snapshot says this pass should do.
///
/// Pure, and that is on purpose: this is the judgement that has to be right
/// exactly when the pool is down, which is when it cannot be exercised on the
/// live system. The three answers are the three states a snapshot can be in, and
/// the third one is a state rather than an error -- no snapshot is not a healthy
/// pool. Treating it as one would leave a process with no source at all handing
/// work over for ever, which is the behaviour this gate exists to end; treating
/// it as a sick pool would stop dispatch for a missing observer, which is a
/// driver traded for a reading.
///
/// `None` here means one thing only: whatever asked has nothing to go on. The
/// two ways that happens are told apart by the caller, not by this function --
/// a process with no pool source published at all, which is a deployment fact
/// and is warned about where the loop is built, and a source that is published
/// and answered nothing this pass, which is what the caller carries the last
/// real reading over for. Reading only this function would show the cold-start
/// answer being taken in both cases; that is the caller's business to narrow,
/// and it is narrowed there because the answer differs.
///
/// The resume bound is whatever the snapshot's own evidence supports, read
/// through the contract's [`LlmPoolStatus::resume_wait_secs`] rather than chosen
/// here: an upstream that stated when it serves again is waited for to that
/// instant, and our own probe cadence gets one observation window added, because
/// resuming at a probe is resuming before the probe's outcome exists. When both
/// are stated the *earlier* one is the answer -- it is the first moment worth
/// looking again -- so the bound recorded is the soonest supportable one, while
/// the upstream's own stated instant travels beside it in
/// `evidenced_recovery_unix` and a reader can always tell the upstream's promise
/// from this process's cadence. The window is this loop's own period, the next
/// time it would look anyway, so no new number enters the account.
fn pool_verdict(
    status: Option<&LlmPoolStatus>,
    now_unix: i64,
    observation_secs: u64,
) -> PoolVerdict {
    let Some(status) = status else {
        return PoolVerdict {
            decision: DispatchDecision::Proceed,
            reading: PoolReading::Absent,
            resume_at_unix: 0,
            evidenced_recovery_unix: 0,
            quota_window_secs: 0,
        };
    };
    let resume_at_unix = if status.unavailable {
        status
            .resume_wait_secs(now_unix, observation_secs)
            .map(|wait| now_unix.saturating_add(wait as i64))
            .unwrap_or(0)
    } else {
        0
    };
    PoolVerdict {
        decision: if status.unavailable {
            DispatchDecision::PausedLlmPoolUnavailable
        } else {
            DispatchDecision::Proceed
        },
        reading: if status.unavailable {
            PoolReading::Unavailable
        } else {
            PoolReading::Available
        },
        resume_at_unix,
        evidenced_recovery_unix: status.evidenced_recovery_unix,
        quota_window_secs: status.quota_window_secs,
    }
}

/// One pass's decision and the bounds it was read against.
///
/// `Copy` because the loop keeps the last one that read the pool and hands it to
/// the next pass as its fallback: a small value travels better than a shared
/// cell, and nothing in it needs to be mutated in place.
#[derive(Debug, Clone, Copy, PartialEq)]
struct PoolVerdict {
    decision: DispatchDecision,
    reading: PoolReading,
    resume_at_unix: i64,
    evidenced_recovery_unix: i64,
    quota_window_secs: u64,
}

/// Wall-clock seconds, for comparing against instants an upstream stated.
///
/// The database clock is used for everything this loop writes; this is the other
/// direction -- the snapshot's bounds are unix seconds from the gateway's own
/// clock, and a bound can only be compared with a clock of the same kind.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
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
///
/// The undecided rows are broken down by the reason they are stuck, because that
/// reason is the whole answer to why they will never settle and it is a property
/// of the row, not of any one pass. The pass's own `unsettled` field cannot give
/// it: it counts only the rows *that* pass asked, so a row the pass did not reach
/// -- which, over passes, is most of them -- never appears there. The reason is
/// rendered in the closed set's order so two readings diff cleanly, and a row
/// measured before the reason was persisted reads as `unstated` rather than being
/// dropped, so the breakdown always sums to the undecided count.
fn outstanding_summary(rows: &[ChangeLedgerRow]) -> String {
    let by_state = ChangeState::ALL
        .into_iter()
        .filter_map(|state| {
            let n = rows.iter().filter(|row| row.state == state).count();
            if n == 0 {
                return None;
            }
            if state != ChangeState::Undecided {
                return Some(format!("{} {}", state.as_str(), n));
            }
            let stuck: Vec<&ChangeLedgerRow> = rows
                .iter()
                .filter(|row| row.state == ChangeState::Undecided)
                .collect();
            let mut parts: Vec<String> = UnsettledReason::ALL
                .iter()
                .filter_map(|reason| {
                    let m = stuck
                        .iter()
                        .filter(|row| row.unsettled_reason.as_deref() == Some(reason.as_str()))
                        .count();
                    (m > 0).then(|| format!("{} {}", reason.as_str(), m))
                })
                .collect();
            let unstated = stuck
                .iter()
                .filter(|row| row.unsettled_reason.is_none())
                .count();
            if unstated > 0 {
                parts.push(format!("unstated {unstated}"));
            }
            Some(format!("undecided {n} ({})", parts.join(", ")))
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
    use cog_core::{ChangeIdentity, LiveRevisionGap, SFError, SFResult};

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
            unsettled_reason: None,
            retire_reason: None,
            retired_at: None,
        }
    }

    /// The quota is the smallest batch that sweeps the landed set within the
    /// bound, at both ends: the sweep it buys fits, and one row fewer does not.
    ///
    /// Only the first half would pass for any large number, and a quota that is
    /// merely big enough is a quota nobody can argue about when it is raised --
    /// the reading that has to hold is that nothing smaller would do.
    #[test]
    fn the_quota_is_the_smallest_batch_that_sweeps_within_the_bound() {
        let pass = Duration::from_secs(300);
        let bound = Duration::from_secs(3600);
        let landed = 1369u64;
        let quota = reopen_quota(landed, pass, bound) as u128;
        // The window the sweep takes: quota passes, each `pass` long, covering
        // `landed` rows.
        let swept_in = |batch: u128| (landed as u128) * pass.as_secs() as u128 / batch;
        assert!(
            swept_in(quota) <= bound.as_secs() as u128,
            "a batch of {quota} takes {}s to sweep the landed set, past the {}s bound",
            swept_in(quota),
            bound.as_secs()
        );
        assert!(
            swept_in(quota - 1) > bound.as_secs() as u128,
            "a batch of {} would already have stayed within the bound",
            quota - 1
        );
    }

    /// A ledger with nothing landed asks for no batch at all.
    #[test]
    fn no_landed_rows_ask_for_nothing() {
        assert_eq!(reopen_quota(0, Duration::from_secs(300), REOPEN_BOUND), 0);
    }

    /// The quota moves with the size of the set, so the window does not: the
    /// parameter a person would otherwise re-tune as the ledger grows is derived
    /// instead, and this is the reading that says so.
    ///
    /// Stated as a number of passes rather than as a ratio, because a ratio is
    /// what the rounding breaks: ten times the rows buys a batch one ceiling
    /// short of ten times the batch, and the window -- which is the thing the
    /// bound is about -- is unmoved.
    #[test]
    fn the_window_holds_as_the_landed_set_grows() {
        let pass = CHANGE_LEDGER_PASS_INTERVAL;
        let budget = REOPEN_BOUND.as_secs() / pass.as_secs();
        for landed in [1u64, 10, 1000, 10_000, 100_000, 1_000_000] {
            let quota = reopen_quota(landed, pass, REOPEN_BOUND) as u64;
            assert!(quota > 0, "a landed set of {landed} must get a batch");
            let rounds = landed.div_ceil(quota);
            assert!(
                rounds <= budget,
                "a landed set of {landed} sweeps in {rounds} passes, past the {budget} \
                 the bound allows -- the batch is too small for the window it promises"
            );
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
            "3 outstanding: pending 2, undecided 1 (unstated 1); never measured 1; \
             oldest measurement 2025-10-09T08:53:20+00:00"
        );
    }

    /// The undecided rows are split by the reason they are stuck, in the closed
    /// set's order, and a row with no reason persisted yet reads as unstated
    /// rather than vanishing -- so the parts always sum to the undecided count.
    ///
    /// This is the reading the pass's own `unsettled` field cannot give: that
    /// field counts only the rows the pass asked, so a row the pass did not reach
    /// is missing from it entirely, and "why is this stuck" is unanswerable from
    /// the ledger once the log rolls.
    #[test]
    fn the_summary_splits_the_undecided_rows_by_reason() {
        let stuck = |reason: Option<&str>| ChangeLedgerRow {
            state: ChangeState::Undecided,
            unsettled_reason: reason.map(str::to_string),
            ..row(ChangeState::Undecided, Some(1_760_000_000))
        };
        let rows = vec![
            stuck(Some("scope_outside_tree")),
            stuck(Some("kind_without_predicate")),
            stuck(Some("scope_outside_tree")),
            stuck(None),
            row(ChangeState::Pending, Some(1_760_000_000)),
        ];
        assert_eq!(
            outstanding_summary(&rows),
            "5 outstanding: pending 1, undecided 4 \
             (kind_without_predicate 1, scope_outside_tree 2, unstated 1); \
             never measured 0; oldest measurement 2025-10-09T08:53:20+00:00"
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
            "2 outstanding: conflict 1, undecided 1 (unstated 1); never measured 2"
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
            "revision_unreadable 0, kind_without_predicate 0, predicate_is_a_rendering 0, scope_outside_tree 12, scope_absent_in_revision 0, git_failed 1"
        );
    }

    /// With nothing unanswerable the line still says so, all four at zero: the
    /// absence of the field would otherwise be the only thing saying it, and an
    /// absent field is not a reading.
    #[test]
    fn a_pass_that_could_settle_everything_still_names_the_reasons() {
        assert_eq!(
            reason_summary(&HashMap::new()),
            "revision_unreadable 0, kind_without_predicate 0, predicate_is_a_rendering 0, scope_outside_tree 0, scope_absent_in_revision 0, git_failed 0"
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
        assert_eq!(dispatch_task_id(&one, 0), dispatch_task_id(&two, 0));
        assert!(dispatch_task_id(&one, 0).starts_with(DISPATCH_TASK_PREFIX));
    }

    /// Generation 0 is the identity alone -- the id every handover carried before
    /// generations existed -- so a change dispatched earlier is still found under
    /// its own id and read as in flight rather than filed a second time. A re-arm
    /// is the one thing that moves it, and it has to move it, or the goal path
    /// would take the repeated id for the handover already under way.
    #[test]
    fn only_a_rearm_moves_the_generation() {
        let id = ChangeIdentity::new("grep", "crates/x", "fn thing");
        let base = dispatch_task_id(&id, 0);
        assert_eq!(base.len(), DISPATCH_TASK_PREFIX.len() + 16);
        assert_ne!(dispatch_task_id(&id, 1), base);
        assert_ne!(dispatch_task_id(&id, 1), dispatch_task_id(&id, 2));
        assert!(!dispatch_task_id(&id, 1).starts_with(&format!("{base}-r0")));
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
            dispatch_task_id(&split_early, 0),
            dispatch_task_id(&split_late, 0)
        );
    }

    /// The derivation is the edge between a ledger row and the product of its
    /// handover: the graph's task table is pruned and cleared per workspace, so a
    /// reader cannot join to it, but a ledger row is kept, and its identity fixes
    /// the id of every handover it ever made. Recomputing the id and matching it
    /// as a prefix against the event stream is how the product is found again --
    /// which only works while this function still produces the id the graph was
    /// actually given. The pairs below were read off the live platform; if this
    /// function ever changes, those readers stop finding their rows and this goes
    /// red first, so the change has to be a migration rather than a quiet edit.
    #[test]
    fn the_id_a_ledger_row_derives_is_the_one_the_live_graph_was_given() {
        let veto = ChangeIdentity::new("grep", "crates/cog-eval/src", "veto");
        assert_eq!(dispatch_task_id(&veto, 0), "change-ledger-be840a05eb11635e");
        let gap = ChangeIdentity::new("grep", "crates/cog-reflection/src", "capability_gap");
        assert_eq!(dispatch_task_id(&gap, 0), "change-ledger-6622446d99af92a0");
    }

    /// A re-arm is a different row in the graph under a different id, and the
    /// ledger fixes that id too -- so a reader looking for the product has to
    /// reach the generation the change actually died on, not only the first.
    #[test]
    fn a_re_arm_derives_an_id_the_first_handover_did_not_use() {
        let identity = ChangeIdentity::new("grep", "crates/cog-eval/src", "veto");
        assert_eq!(
            dispatch_task_id(&identity, 1),
            "change-ledger-be840a05eb11635e-r1"
        );
        assert_ne!(
            dispatch_task_id(&identity, 1),
            dispatch_task_id(&identity, 0)
        );
    }

    /// The two verdicts a finished task can carry are opposites, and the row the
    /// graph holds does not say which without being read. An environment refusal
    /// leaves the change owed; the change's own failure does not.
    #[test]
    fn an_environment_refusal_is_told_from_the_works_own_failure() {
        let env = Task::new(
            "id".to_string(),
            TaskType::Custom("change_ledger".into()),
            serde_json::json!({}),
        );
        let mut dead_env = env.clone();
        dead_env.status = TaskStatus::Failed;
        dead_env.error = Some(
            "Agent execution error: terminal_env_failure: environment_error: \
             LLM upstream refused (auth_rejected)"
                .into(),
        );
        assert!(matches!(
            classify_handover(&dead_env),
            Handover::DeadEnvironment
        ));

        let mut dead_work = env.clone();
        dead_work.status = TaskStatus::Failed;
        dead_work.error = Some("degenerate_loop: the same failure repeated".into());
        assert!(matches!(classify_handover(&dead_work), Handover::DeadWork));

        // The typed channel answers on its own: a task that carries the cause as
        // a value is judged by it even when its text names nothing. The live
        // table leaves this field empty today, so both channels have to reach
        // the same verdict or the answer would depend on which one a task wrote.
        let mut typed = env.clone();
        typed.status = TaskStatus::Failed;
        typed.error = Some("Agent execution error: the run did not finish".into());
        typed.error_cause = Some(UpstreamFailure::Auth);
        assert!(matches!(
            classify_handover(&typed),
            Handover::DeadEnvironment
        ));

        let mut in_flight = env;
        in_flight.status = TaskStatus::Running;
        assert!(matches!(classify_handover(&in_flight), Handover::InFlight));
    }

    /// The re-arm budget is read from the executor's own matrix rather than
    /// written here: a literal would keep the two in step only until somebody
    /// changed the declared budget. The executor's *effective* budget for these
    /// failures is zero, and acting on that difference is what this module is.
    #[test]
    fn the_rearm_budget_is_the_task_types_rearm_budget() {
        assert_eq!(
            dispatch_rearm_limit(),
            crate::dag_executor::RetryMatrix::defaults()
                .max_retries(&TaskType::Custom(DISPATCH_TASK_KIND.to_string()))
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

    /// A `pending` row the pass could not settle is not handed over. It carries
    /// the state work is handed over in -- an unsettled look moves no state, so
    /// the row keeps whatever a document gave it -- and handing it over anyway
    /// would put a task on the goal path that no artifact can ever close, for a
    /// change the tree may already hold.
    #[test]
    fn a_row_the_pass_could_not_settle_is_not_handed_over() {
        let rows = vec![
            row_at("crates/a"),
            row_at("crates/b"),
            ChangeLedgerRow {
                state: ChangeState::Landed,
                ..row_at("crates/c")
            },
        ];
        let mut unsettled = HashMap::new();
        unsettled.insert(
            ChangeIdentity::new("grep", "crates/b", "fn thing"),
            UnsettledCheck {
                why: "the predicate is a finding written out".into(),
                since: Instant::now(),
            },
        );
        let handed = dispatchable(&rows, &unsettled);
        assert_eq!(handed.len(), 1, "{handed:?}");
        assert_eq!(handed[0].identity.scope, "crates/a");

        // And with nothing unanswered, the pending row comes back: the exclusion
        // is the pass's own finding and not a property of the state.
        assert_eq!(dispatchable(&rows, &HashMap::new()).len(), 2);
    }

    /// The identity a registered dependency becomes: the consumer's manifest as
    /// the scope, and the dependency's own assignment as the literal.
    ///
    /// Both halves are the mapping and not a preference. The scope has to be the
    /// file the dependency is declared in, or the check would search the crate
    /// rather than its manifest; and the literal has to be the assignment, or a
    /// comment that merely names the crate would keep a removed dependency owed
    /// for good.
    #[test]
    fn a_registered_dependency_becomes_a_check_against_its_consumers_manifest() {
        let rows = edge_readings(&[(
            "crates/cogneva/debt/dependency-edges.tsv".to_string(),
            "allowed\tcog-gateway\tcog-llm\tcontract::llm::LlmClient".to_string(),
        )]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].identity.kind, DEPENDENCY_EDGE_KIND);
        assert_eq!(rows[0].identity.scope, "crates/cog-gateway/Cargo.toml");
        assert_eq!(rows[0].identity.literal, "cog-llm =");
        assert_eq!(rows[0].source.doc, DEBT_EDGE_FILE);
        assert_eq!(rows[0].source.verdict, None);
    }

    /// A test-time edge is a kind of its own, so the census can tell the two
    /// apart, and the goal says the reach is from the tests rather than from the
    /// crate.
    #[test]
    fn a_test_time_edge_is_a_kind_of_its_own() {
        let rows = edge_readings(&[(
            "registry".to_string(),
            "allowed_dev\tcog-gateway\tcog-llm\tcontract::llm::LlmClient".to_string(),
        )]);
        assert_eq!(rows[0].identity.kind, DEV_DEPENDENCY_EDGE_KIND);
        assert_eq!(rows[0].identity.scope, "crates/cog-gateway/Cargo.toml");
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Make cog-gateway reach cog-llm through the core contract \
                 contract::llm::LlmClient in its tests instead of depending on it directly."
            )
        );
    }

    /// Lines that carry no work -- the header and a `#` comment -- and a kind
    /// this intake cannot aim are skipped, rather than entering the account as a
    /// row nothing measures. A row naming no contract still enters: the edge is
    /// owed either way, and what carries it instead is what the goal then cannot
    /// say.
    #[test]
    fn only_registered_edge_rows_become_readings() {
        let rows = edge_readings(&[
            (
                "registry".to_string(),
                "kind\tconsumer\tdep\tcontract".to_string(),
            ),
            (
                "registry".to_string(),
                "# allowed\tcog-y\tcog-z\tcontract::z".to_string(),
            ),
            (
                "registry".to_string(),
                "something\tcog-y\tcog-z\t".to_string(),
            ),
            (
                "registry".to_string(),
                "allowed\tcog-short\tcog-y".to_string(),
            ),
            (
                "registry".to_string(),
                "allowed\tcog-x\tcog-y\t".to_string(),
            ),
        ]);
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0].identity.scope, "crates/cog-short/Cargo.toml");
        // A row whose contract column is empty -- whether the field is absent or
        // blank -- still names an edge that is owed.
        assert_eq!(rows[1].identity.scope, "crates/cog-x/Cargo.toml");
        assert_eq!(
            rows[1].title.as_deref(),
            Some(
                "Remove the cog-x -> cog-y dependency by routing it through the core's \
                 contract layer."
            )
        );
    }

    /// A module is measured against the core's own module list and named the way
    /// that file declares it, so the row still owed is the one whose declaration
    /// is still there.
    #[test]
    fn a_registered_relocation_becomes_a_check_against_the_cores_module_list() {
        let rows = module_readings(&[
            (
                "registry".to_string(),
                "module\treason\tdestination".to_string(),
            ),
            (
                "registry".to_string(),
                "admission\tpending-relocation\tcog-eval split".to_string(),
            ),
            (
                "registry".to_string(),
                "audit\tshared-definition\t".to_string(),
            ),
            (
                "registry".to_string(),
                "build_gate\tpending-relocation\tcog-orchestrator".to_string(),
            ),
        ]);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].identity.kind, MODULE_RELOCATION_KIND);
        assert_eq!(rows[0].identity.scope, CORE_MODULE_LIST);
        assert_eq!(rows[0].identity.literal, "mod admission;");
        assert_eq!(rows[0].source.doc, DEBT_MODULE_FILE);
        // The shape rides into the goal, so a module that leaves in halves is
        // described that way rather than as a file that moves whole.
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Move the core module `admission` out of cog-core's source root into the \
                 `cog-eval` crate. The module is a contract and a mechanism in one file: \
                 the half that keeps state moves, and the contract half stays in the \
                 core's contract layer."
            )
        );
    }

    /// A principle ledger row, with only the columns this intake reads set.
    fn principle_row(
        slug: &str,
        check_kind: Option<PrincipleCheckKind>,
        lead: Option<&str>,
    ) -> PrincipleRow {
        PrincipleRow {
            slug: slug.to_string(),
            lead: lead.map(str::to_string),
            carrier_path: None,
            carrier_sha256: None,
            host_gate: None,
            platform_gate: None,
            check_kind,
            state: None,
            refusals: 0,
            last_refused_at: None,
            first_seen_at: Utc.timestamp_opt(1_760_000_000, 0).unwrap(),
            measured_at: None,
            measured_rev: None,
        }
    }

    /// An unpaid principle whose tier is declared becomes a check against the
    /// tree's own declaration of its gate.
    ///
    /// All three parts of the identity are the mapping. The kind is `grep`,
    /// because what lands the row is the tree carrying a declaration. The scope is
    /// the platform's sources, where the principle ledger's own look reads that
    /// declaration from -- a narrower one would report a principle as ungated
    /// while the account that says it is owed finds its gate. The literal is the
    /// declaration's own words up to the slug, without the comment marker: both
    /// ends are word characters, which is what makes the judge anchor it and what
    /// keeps a longer slug from closing this row.
    #[test]
    fn an_unpaid_principle_becomes_a_check_against_the_trees_declaration() {
        let rows = principle_debt_readings(&[principle_row(
            "feedback_x",
            Some(PrincipleCheckKind::Tree),
            Some("a reading must be quoted with its construction"),
        )]);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].identity.kind, GREP_KIND);
        assert_eq!(rows[0].identity.scope, PRINCIPLE_GATE_SCOPE);
        assert_eq!(rows[0].identity.literal, "principle: feedback_x");
        assert_eq!(rows[0].source.doc, PRINCIPLE_LEDGER_SOURCE);
        assert_eq!(rows[0].source.verdict, None);
        assert_eq!(rows[0].source.priority, ChangePriority::Unstated);
        // The goal names the lane the tier puts the check in and the rule's own
        // sentence: a goal that named only the slug would tell a generator where
        // to look and not what is wanted there.
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Give the principle `feedback_x` the gate it is declared to be owed: a check \
                 answering from this tree at one revision, the way the platform's own tests \
                 answer, declared in the header of the test that carries it. The rule itself: \
                 a reading must be quoted with its construction"
            )
        );
    }

    /// Both ends of the literal are word characters, which is the condition the
    /// judge reads as "anchor this on word boundaries".
    ///
    /// Asserted here rather than left to a comment because the marker this
    /// constant deliberately omits is the thing that would break it: a literal
    /// beginning with the comment marker ends on a character no word can follow,
    /// so it stops being anchored and a longer slug that merely begins with this
    /// one closes the row. Measured in a fixture repository holding only
    /// `//! principle: feedback_xy`: `principle: feedback_x` misses when anchored,
    /// hits when not, and the exact slug hits either way.
    #[test]
    fn the_principle_literal_is_anchored_at_both_ends() {
        let literal = principle_literal("feedback_x");
        let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
        assert!(literal.starts_with(PRINCIPLE_DECLARATION_PREFIX));
        assert!(!literal.starts_with("//"));
        assert!(word(literal.chars().next().expect("a literal has a first")));
        assert!(word(literal.chars().last().expect("a literal has a last")));
        // And the slug's own end is where it ends: nothing is appended that a
        // boundary would have to be found after.
        assert!(literal.ends_with("feedback_x"));
    }

    /// The tier a person holds is not work, and neither is a row with no tier.
    ///
    /// The first is the reading the source account itself makes: its look writes
    /// no state for a `judgement` row, so such a row stays in its unpaid set
    /// forever -- reading it here as work would mint a task no gate could close.
    /// The second is a principle nobody has said anything about: it has no lane to
    /// aim at, so it stays a number in that account's census rather than becoming
    /// a change with a goal nothing can act on.
    #[test]
    fn a_principle_a_person_holds_and_one_with_no_tier_are_not_work() {
        let rows = principle_debt_readings(&[
            principle_row(
                "feedback_held",
                Some(PrincipleCheckKind::Judgement),
                Some("a person's"),
            ),
            principle_row("feedback_untiered", None, Some("nobody said")),
            principle_row("feedback_owed", Some(PrincipleCheckKind::Live), None),
        ]);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].identity.literal, "principle: feedback_owed");
        // A row that arrived with no sentence still gets a goal: the rule's words
        // are carried when the reading has them, and their absence is not a reason
        // to hand over a bare slug.
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Give the principle `feedback_owed` the gate it is declared to be owed: a check \
                 answering from the running deployment's own readings, declared in the header \
                 of the test that carries it."
            )
        );
    }

    /// Every tier of the closed set is classified, and exactly three of them are
    /// work.
    ///
    /// Read off `PrincipleCheckKind::ALL` rather than written out as three names,
    /// so a tier added to the contract without a decision here fails this test as
    /// well as the exhaustive match -- the count is the assertion that catches a
    /// tier silently falling out of the intake.
    #[test]
    fn the_machine_tiers_are_the_three_the_intake_aims_at() {
        let lanes: Vec<&str> = PrincipleCheckKind::ALL
            .iter()
            .filter_map(|kind| check_lane(*kind))
            .collect();
        assert_eq!(lanes.len(), 3, "{lanes:?}");
        assert_eq!(check_lane(PrincipleCheckKind::Judgement), None);
        for kind in PrincipleCheckKind::ALL
            .iter()
            .filter(|kind| **kind != PrincipleCheckKind::Judgement)
        {
            let lane = check_lane(*kind).unwrap_or_else(|| panic!("{kind} has no lane"));
            assert!(!lane.is_empty());
        }
    }

    /// A snapshot saying no upstream can serve stops the pass, and the bound it
    /// stops until is the earliest one the evidence supports -- here the instant
    /// the upstream itself stated, which is sooner than our next probe would be.
    #[test]
    fn an_unavailable_pool_pauses_the_pass_at_the_earliest_stated_bound() {
        let now = 1_760_000_000i64;
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: now + 100,
            next_attempt_unix: now + 1_000,
            unavailable_upstreams: vec!["https://a.example|m".to_string()],
            quota_window_secs: 2_592_000,
            ..Default::default()
        };
        let verdict = pool_verdict(Some(&status), now, 300);
        assert_eq!(verdict.decision, DispatchDecision::PausedLlmPoolUnavailable);
        assert_eq!(verdict.reading, PoolReading::Unavailable);
        // The upstream's instant is sooner than the probe's outcome would be, so
        // it is the bound -- and it is carried as itself, not as the derived
        // number, so a reader can still see it was the upstream that said so.
        assert_eq!(verdict.resume_at_unix, now + 100);
        assert_eq!(verdict.evidenced_recovery_unix, now + 100);
        assert_eq!(verdict.quota_window_secs, 2_592_000);
    }

    /// With no instant stated, the only bound is our own probe cadence, and one
    /// observation window is added to it: the pass may not resume at a probe it
    /// has not yet seen the result of.
    #[test]
    fn a_probe_cadence_bound_gets_one_observation_window_added() {
        let now = 1_760_000_000i64;
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 0,
            next_attempt_unix: now + 100,
            ..Default::default()
        };
        let verdict = pool_verdict(Some(&status), now, 300);
        assert_eq!(verdict.decision, DispatchDecision::PausedLlmPoolUnavailable);
        assert_eq!(verdict.resume_at_unix, now + 400);
        // No instant was stated, so nothing is claimed to have been.
        assert_eq!(verdict.evidenced_recovery_unix, 0);
    }

    /// An unavailable pool with no bound in the future is a pause with no known
    /// end, which is a state of its own -- not "resume now". Reading it as an
    /// imminent recovery is exactly the reading a spent weekly quota produces.
    ///
    /// The stated instant is already past and no probe cadence is on record, so
    /// nothing lies ahead: the contract's own function answers `None` and the
    /// pass records no instant rather than inventing one.
    #[test]
    fn a_pause_with_no_future_bound_states_no_resume_instant() {
        let now = 1_760_000_000i64;
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: now - 1,
            next_attempt_unix: 0,
            ..Default::default()
        };
        let verdict = pool_verdict(Some(&status), now, 300);
        assert_eq!(verdict.decision, DispatchDecision::PausedLlmPoolUnavailable);
        assert_eq!(verdict.resume_at_unix, 0);
        // The spent instant is still reported as what the upstream said, even
        // though it cannot be the bound: it is what a reader needs to tell a
        // stale statement from no statement at all.
        assert_eq!(verdict.evidenced_recovery_unix, now - 1);
    }

    /// A healthy pool dispatches, and the reading says the pass knew it could.
    #[test]
    fn an_available_pool_dispatches_and_says_it_knew() {
        let now = 1_760_000_000i64;
        let status = LlmPoolStatus {
            unavailable: false,
            ..Default::default()
        };
        let verdict = pool_verdict(Some(&status), now, 300);
        assert_eq!(verdict.decision, DispatchDecision::Proceed);
        assert_eq!(verdict.reading, PoolReading::Available);
        assert_eq!(verdict.resume_at_unix, 0);
    }

    /// No snapshot is not a healthy pool: the pass proceeds -- a missing observer
    /// must not become a driver -- but the reading keeps "dispatched blind" apart
    /// from "dispatched behind a healthy pool".
    #[test]
    fn an_absent_snapshot_dispatches_but_is_not_reported_as_healthy() {
        let now = 1_760_000_000i64;
        let verdict = pool_verdict(None, now, 300);
        assert_eq!(verdict.decision, DispatchDecision::Proceed);
        assert_eq!(verdict.reading, PoolReading::Absent);
        assert_eq!(verdict.resume_at_unix, 0);
        assert_ne!(verdict.reading, PoolReading::Available);
    }

    // ── The pass's own arithmetic, driven end to end ──
    //
    // The counts a pass writes are the only reading of what it did, and the way
    // they fail is by not adding up to the set the pass read -- most of all on
    // the pass that handed nothing over, whose counts used to be four structural
    // zeroes because the pause returned before the classification ran. So these
    // drive the real `dispatch_pass` over a ledger in memory and a graph the test
    // writes, and read back the row and the cells it produced.

    /// A ledger holding what a test put in it, keeping the rows the pass wrote.
    ///
    /// The read side is the two calls the pass makes, with the same exclusions
    /// the real store applies; everything else answers empty, because a pass that
    /// never asks for it has no business getting an answer out of a double.
    #[derive(Default)]
    struct FakeLedger {
        rows: Vec<ChangeLedgerRow>,
        written: std::sync::Mutex<Vec<DispatchState>>,
    }

    impl FakeLedger {
        fn written(&self) -> Vec<DispatchState> {
            self.written.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ChangeLedgerStore for FakeLedger {
        async fn record_readings(
            &self,
            _readings: &[ChangeReading],
            _first_seen_rev: Option<&str>,
        ) -> SFResult<usize> {
            Ok(0)
        }
        async fn declare(&self, _readings: &[ChangeReading]) -> SFResult<usize> {
            Ok(0)
        }
        async fn fold_declarations(
            &self,
            _limit: usize,
            _first_seen_rev: Option<&str>,
        ) -> SFResult<usize> {
            Ok(0)
        }
        async fn fold_retirements(&self, _limit: usize) -> SFResult<usize> {
            Ok(0)
        }
        async fn pending_declarations(&self) -> SFResult<u64> {
            Ok(0)
        }
        async fn pending_retirements(&self) -> SFResult<u64> {
            Ok(0)
        }
        async fn apply_measurement(&self, _measurement: &ChangeMeasurement) -> SFResult<()> {
            Ok(())
        }
        async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
            Ok(self
                .rows
                .iter()
                .filter(|row| row.state.is_outstanding())
                .take(limit)
                .cloned()
                .collect())
        }
        async fn landed_due(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
            Ok(self
                .rows
                .iter()
                .filter(|row| row.state == ChangeState::Landed)
                .take(limit)
                .cloned()
                .collect())
        }
        async fn record_reopen(&self, _reopen: &ChangeReopen) -> SFResult<()> {
            Ok(())
        }
        async fn get(&self, identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>> {
            Ok(self
                .rows
                .iter()
                .find(|row| row.identity == *identity)
                .cloned())
        }
        async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>> {
            let mut by_state: HashMap<ChangeState, u64> = HashMap::new();
            for row in &self.rows {
                *by_state.entry(row.state).or_default() += 1;
            }
            Ok(by_state.into_iter().collect())
        }
        async fn record_dispatch_state(&self, state: &DispatchState) -> SFResult<()> {
            self.written.lock().unwrap().push(state.clone());
            Ok(())
        }
    }

    /// A graph the test writes, with the two calls the pass makes answered from
    /// it: `get_task` says what holds a row, and `submit_goal_auto` records what
    /// was handed over -- or refuses everything, for the pass whose handovers
    /// cannot be taken.
    #[derive(Default)]
    struct FakeGraph {
        tasks: HashMap<String, Task>,
        refuse_submissions: bool,
        submitted: std::sync::Mutex<Vec<String>>,
    }

    impl FakeGraph {
        fn holding(mut self, id: &str, status: TaskStatus) -> Self {
            let mut task = Task::new(
                id.to_string(),
                TaskType::Custom(DISPATCH_TASK_KIND.into()),
                serde_json::json!({}),
            );
            task.status = status;
            if status == TaskStatus::Failed {
                // The environment's own refusal, which is the one failure a
                // re-arm is for.
                task.error_cause = Some(UpstreamFailure::Auth);
            }
            self.tasks.insert(id.to_string(), task);
            self
        }

        fn holding_dead_work(mut self, id: &str) -> Self {
            let mut task = Task::new(
                id.to_string(),
                TaskType::Custom(DISPATCH_TASK_KIND.into()),
                serde_json::json!({}),
            );
            task.status = TaskStatus::Failed;
            task.error = Some("degenerate_loop: the same failure repeated".into());
            self.tasks.insert(id.to_string(), task);
            self
        }

        fn refusing(mut self) -> Self {
            self.refuse_submissions = true;
            self
        }

        fn submitted(&self) -> Vec<String> {
            self.submitted.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl OrchestratorControl for FakeGraph {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(&self, _goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
            if self.refuse_submissions {
                return Err(SFError::Agent("the goal path would not take it".into()));
            }
            let ids = tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>();
            self.submitted.lock().unwrap().extend(ids.clone());
            Ok(ids)
        }
        async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn add_task(&self, _task: Task) -> SFResult<()> {
            Ok(())
        }
        async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            Vec::new()
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            self.tasks.values().cloned().collect()
        }
        async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
            Ok(false)
        }
        async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn dlq_len(&self) -> SFResult<usize> {
            Ok(0)
        }
        async fn start_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn complete_task(
            &self,
            _task_id: &str,
            _result: serde_json::Value,
        ) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn fail_task(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<UpstreamFailure>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            Ok((false, Vec::new(), false))
        }
        async fn fail_task_after(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<UpstreamFailure>,
            _retry_after_secs: Option<u64>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            Ok((false, Vec::new(), false))
        }
        async fn cancel_task(&self, _task_id: &str) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn get_task(&self, task_id: &str) -> Option<Task> {
            self.tasks.get(task_id).cloned()
        }
        async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            Vec::new()
        }
        async fn get_dependents(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (Vec::new(), Vec::new())
        }
        async fn delete_task(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn all_completed(&self) -> bool {
            false
        }
        async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
            Ok(false)
        }
    }

    /// A pool whose snapshot says no configured upstream can serve.
    struct DeadPool;

    #[async_trait::async_trait]
    impl cog_core::LlmPoolStatusSource for DeadPool {
        async fn status(&self) -> Option<LlmPoolStatus> {
            Some(LlmPoolStatus {
                unavailable: true,
                ..Default::default()
            })
        }
    }

    /// A pool source that is published and answers nothing this pass -- the
    /// gateway and the store rolling together, which is an ordinary sight during
    /// a deployment rather than a fault.
    struct SilentPool;

    #[async_trait::async_trait]
    impl cog_core::LlmPoolStatusSource for SilentPool {
        async fn status(&self) -> Option<LlmPoolStatus> {
            None
        }
    }

    /// The identity every column of one pass has to add up to: the rows it was
    /// owed are exactly the rows it placed somewhere, whichever way it went.
    ///
    /// `rearmed` is not in the sum because it is an annotation rather than a
    /// placement -- a row re-armed and then submitted is in both numbers -- and
    /// `unmeasurable` is not either because those rows were never dispatched:
    /// they are `pending` in state and excluded from the set by the very
    /// predicate that reads it.
    fn assert_the_counts_add_up(state: &DispatchState) {
        assert_eq!(
            state.submitted
                + state.already
                + state.failed
                + state.dead_environment
                + state.held_back,
            state.pending,
            "the columns do not add up to what was owed: {state:?}"
        );
        assert!(
            state.rearmed <= state.submitted,
            "a row re-armed and not submitted is not a re-arm the pass performed: {state:?}"
        );
    }

    // ── The second face of "is it done" ──
    //
    // Driven over a real repository and a ledger in memory, because what the pass
    // turns on is a revision being or not being on a line of development, and
    // that question is answered by git. The state a reader acts on is the set the
    // pass keeps across ticks -- a row arriving in it, leaving it, or staying
    // where it was when the reading could not be taken -- so the tests read that
    // set back rather than the counts, which are the pass's own line.

    /// A repository with two commits on `main`, and both hashes: a landed row
    /// can have been measured at either, and only one of them is running.
    fn two_revision_repo() -> (tempfile::TempDir, String, String) {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let bare = dir.path().join("bare");
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).expect("the scratch worktree");
        let bare = bare
            .to_str()
            .expect("a path this host can spell")
            .to_string();
        let work = work
            .to_str()
            .expect("a path this host can spell")
            .to_string();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(args)
                .output()
                .expect("git on the test host");
            assert!(
                out.status.success(),
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).into_owned()
        };
        git(&["init", "-q", "--bare", "-b", "main", &bare]);
        git(&[
            "--git-dir",
            &bare,
            "config",
            "user.email",
            "tree@cogneva.invalid",
        ]);
        git(&["--git-dir", &bare, "config", "user.name", "tree"]);
        let commit = |text: &str| {
            std::fs::write(std::path::Path::new(&work).join("file"), text)
                .expect("the fixture file");
            git(&["--git-dir", &bare, "--work-tree", &work, "add", "--all"]);
            git(&[
                "--git-dir",
                &bare,
                "--work-tree",
                &work,
                "commit",
                "-q",
                "-m",
                "fixture",
            ]);
            git(&["--git-dir", &bare, "rev-parse", "HEAD"])
                .trim()
                .to_string()
        };
        let first = commit("first");
        let second = commit("second");
        (dir, first, second)
    }

    /// A landed row that names the revision it was measured at.
    fn landed_row(scope: &str, measured_rev: Option<&str>) -> ChangeLedgerRow {
        ChangeLedgerRow {
            state: ChangeState::Landed,
            measured_rev: measured_rev.map(str::to_string),
            ..pending_row(scope)
        }
    }

    /// The revision the workloads are running, held fixed for one test.
    struct FixedLive(LiveRevision);

    #[async_trait::async_trait]
    impl LiveRevisionSource for FixedLive {
        async fn live_revision(&self) -> LiveRevision {
            self.0.clone()
        }
    }

    /// A landed row the running revision does not carry is named, and one it
    /// does carry is not: the same tree, the same account, two rows, and the
    /// second face is the only thing that tells them apart.
    #[tokio::test]
    async fn a_landed_row_the_running_revision_does_not_carry_is_named() {
        let (dir, first, second) = two_revision_repo();
        let store = FakeLedger {
            rows: vec![
                landed_row("crates/in-effect", Some(&first)),
                landed_row("crates/undeployed", Some(&second)),
                // Landed with no revision named. It cannot be placed on any
                // line, which is a third answer and not "in effect".
                landed_row("crates/unnamed", None),
            ],
            ..Default::default()
        };
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let mut out_of_effect = HashMap::new();

        live_revision_pass(
            &store,
            &tree,
            &FixedLive(LiveRevision::Known(first.clone())),
            &mut out_of_effect,
        )
        .await;

        assert_eq!(
            out_of_effect.len(),
            1,
            "only the row measured past the running revision is out of effect: {out_of_effect:?}"
        );
        let undeployed = ChangeIdentity::new("grep", "crates/undeployed", "fn thing");
        assert_eq!(
            out_of_effect.get(&undeployed).map(String::as_str),
            Some(second.as_str()),
            "the row is held with the revision it was measured at, not with a flag"
        );
        assert!(
            !out_of_effect.contains_key(&ChangeIdentity::new("grep", "crates/unnamed", "fn thing")),
            "a row nothing can place is counted apart, not reported as out of effect"
        );

        // And the same account against a revision that carries both: the row
        // leaves the set, which is the reading that says the rollout finished.
        live_revision_pass(
            &store,
            &tree,
            &FixedLive(LiveRevision::Known(second.clone())),
            &mut out_of_effect,
        )
        .await;
        assert!(
            out_of_effect.is_empty(),
            "a row in the running revision is not out of effect: {out_of_effect:?}"
        );
    }

    /// A row that stops being landed leaves the set: it is not out of effect any
    /// more, it is not done any more, and those are two different facts that the
    /// next landing of the same identity would otherwise read as one.
    #[tokio::test]
    async fn a_row_that_is_no_longer_landed_leaves_the_set() {
        let (dir, first, second) = two_revision_repo();
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let mut out_of_effect = HashMap::new();

        let landed = FakeLedger {
            rows: vec![landed_row("crates/x", Some(&second))],
            ..Default::default()
        };
        live_revision_pass(
            &landed,
            &tree,
            &FixedLive(LiveRevision::Known(first.clone())),
            &mut out_of_effect,
        )
        .await;
        assert_eq!(out_of_effect.len(), 1);

        let reopened = FakeLedger {
            rows: vec![pending_row("crates/x")],
            ..Default::default()
        };
        live_revision_pass(
            &reopened,
            &tree,
            &FixedLive(LiveRevision::Known(first)),
            &mut out_of_effect,
        )
        .await;
        assert!(
            out_of_effect.is_empty(),
            "a row that is no longer landed is not held as out of effect: {out_of_effect:?}"
        );
    }

    /// Every way this reading can fail to be taken leaves the set exactly as it
    /// was. That is not the same as clearing it and not the same as reporting
    /// nothing out of effect: what the pass knew is still true as of the last
    /// time it could look, and the line says which reading could not be taken.
    #[tokio::test]
    async fn a_reading_that_cannot_be_taken_moves_nothing() {
        let (dir, first, _second) = two_revision_repo();
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        // A row that a reading, if it were taken, would find in effect -- so a
        // pass that took the reading and failed to take it would be visible as a
        // change to the set rather than as nothing happening.
        let store = FakeLedger {
            rows: vec![landed_row("crates/in-effect", Some(&first))],
            ..Default::default()
        };
        let held = ChangeIdentity::new("grep", "crates/undeployed", "fn thing");
        let mut out_of_effect = HashMap::new();
        out_of_effect.insert(held.clone(), "0".repeat(40));

        for live in [
            LiveRevision::Unmeasurable(LiveRevisionGap::NoUnanimousMainTag),
            LiveRevision::Unmeasurable(LiveRevisionGap::ClusterUnreadable),
            // A revision the cluster names and this tree cannot place: a third
            // fault, and the one that would otherwise be read as every landed
            // row being out of effect at once.
            LiveRevision::Known("0".repeat(40)),
        ] {
            live_revision_pass(&store, &tree, &FixedLive(live.clone()), &mut out_of_effect).await;
            assert_eq!(
                out_of_effect.keys().collect::<Vec<_>>(),
                vec![&held],
                "{live:?} moved the set instead of leaving it as it was"
            );
        }
    }

    fn pending_row(scope: &str) -> ChangeLedgerRow {
        ChangeLedgerRow {
            identity: ChangeIdentity::new("grep", scope, "fn thing"),
            ..row(ChangeState::Pending, None)
        }
    }

    /// A pass that hands over places every owed row: the one the graph already
    /// holds, the one whose handover the environment killed and which therefore
    /// goes out again, and the one nothing has ever held.
    #[tokio::test]
    async fn the_counts_of_a_pass_that_hands_over_add_up_to_the_pending_set() {
        let store = FakeLedger {
            rows: vec![
                pending_row("crates/a"),
                pending_row("crates/b"),
                pending_row("crates/c"),
                // A terminal state is not owed, and must not be counted.
                ChangeLedgerRow {
                    state: ChangeState::Landed,
                    ..pending_row("crates/d")
                },
            ],
            ..Default::default()
        };
        let at =
            |row: &ChangeLedgerRow, generation: u32| dispatch_task_id(&row.identity, generation);
        let graph = Arc::new(
            FakeGraph::default()
                .holding(&at(&store.rows[0], 0), TaskStatus::Running)
                .holding(&at(&store.rows[1], 0), TaskStatus::Failed),
        );
        let control: Arc<dyn OrchestratorControl> = graph.clone();

        dispatch_pass(&store, &control, &HashMap::new(), None, None, None).await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.decision, DispatchDecision::Proceed);
        assert_eq!(state.pending, 3);
        assert_eq!(state.already, 1, "the row the graph already holds");
        assert_eq!(state.submitted, 2, "the re-armed row and the fresh one");
        assert_eq!(state.rearmed, 1, "the row whose first handover was killed");
        assert_eq!(state.failed, 0);
        assert_eq!(state.dead_environment, 0);
        assert_eq!(state.held_back, 0);
        assert_the_counts_add_up(&state);
        // And the handovers are on the graph: the re-arm goes out under the
        // first generation nothing holds, and the fresh row under generation 0.
        let mut handed_over = graph.submitted();
        handed_over.sort();
        assert_eq!(
            handed_over,
            vec![at(&store.rows[1], 1), at(&store.rows[2], 0)]
        );
    }

    /// A source that answers nothing does not become permission to hand work
    /// over: the posture the last real reading put dispatch in is what a silent
    /// pass does, and the reading says the pass read nothing so a reader can tell
    /// it from one that read a healthy pool.
    ///
    /// This is the shape that cost a batch: one tick during a rollout read
    /// nothing, took it for a healthy pool, and handed 133 rows to a pool whose
    /// every request came back refused.
    #[tokio::test]
    async fn a_silent_source_keeps_the_posture_the_last_reading_put_dispatch_in() {
        let store = FakeLedger {
            rows: vec![pending_row("crates/a"), pending_row("crates/b")],
            ..Default::default()
        };
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();
        let paused = PoolVerdict {
            decision: DispatchDecision::PausedLlmPoolUnavailable,
            reading: PoolReading::Unavailable,
            resume_at_unix: 1_760_000_000,
            evidenced_recovery_unix: 1_759_999_000,
            quota_window_secs: 2_592_000,
        };

        let read_back = dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            Some(&SilentPool),
            Some(paused),
            None,
        )
        .await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.decision, DispatchDecision::PausedLlmPoolUnavailable);
        assert_eq!(
            state.pool,
            PoolReading::Absent,
            "the reading is this pass's own: it read nothing"
        );
        assert_eq!(state.pending, 2);
        assert_eq!(state.held_back, 2, "owed, and not handed anywhere");
        assert_eq!(state.submitted, 0);
        assert!(
            graph.submitted().is_empty(),
            "an unanswered read must not hand work over"
        );
        // The bounds travel with the posture: a pause carried over without them
        // would read as a pause with no known end, which is a different fact.
        assert_eq!(state.resume_at_unix, 1_760_000_000);
        assert_eq!(state.evidenced_recovery_unix, 1_759_999_000);
        assert_eq!(state.quota_window_secs, 2_592_000);
        assert!(
            read_back.is_none(),
            "an absent reading is not the posture to carry into the next pass"
        );
    }

    /// The first pass after a start has nothing to carry over, and it behaves as
    /// it always did: it proceeds. A source that has never answered is the one
    /// case where a silent read is not evidence about the pool at all.
    #[tokio::test]
    async fn a_silent_source_with_nothing_to_carry_over_proceeds() {
        let store = FakeLedger {
            rows: vec![pending_row("crates/a")],
            ..Default::default()
        };
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            Some(&SilentPool),
            None,
            None,
        )
        .await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.decision, DispatchDecision::Proceed);
        assert_eq!(state.pool, PoolReading::Absent);
        assert_eq!(state.submitted, 1, "the cold start still hands over");
        assert!(
            !graph.submitted().is_empty(),
            "nothing to carry over is not a reason to stop"
        );
    }

    /// A reading that was taken is what the loop carries forward. Read off the
    /// return value rather than the row, because that is the channel the loop
    /// uses and an absent reading must not travel through it.
    #[tokio::test]
    async fn only_a_reading_that_was_taken_travels_to_the_next_pass() {
        let store = FakeLedger::default();
        let control: Arc<dyn OrchestratorControl> = Arc::new(FakeGraph::default());

        let read = dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            Some(&DeadPool),
            None,
            None,
        )
        .await
        .expect("a snapshot that said the pool is sick is a reading");

        assert_eq!(read.decision, DispatchDecision::PausedLlmPoolUnavailable);
        assert_eq!(read.reading, PoolReading::Unavailable);
    }

    /// The pass that hands nothing over still places every owed row. This is the
    /// one that used to fail: the pause returned before the classification ran,
    /// so the row said four zeroes beside a `pending` that was not zero, and the
    /// reading face was blind on exactly the passes it exists to describe.
    #[tokio::test]
    async fn a_pass_that_hands_nothing_over_still_accounts_for_every_owed_row() {
        let store = FakeLedger {
            rows: vec![
                pending_row("crates/a"),
                pending_row("crates/b"),
                pending_row("crates/c"),
            ],
            ..Default::default()
        };
        let held = dispatch_task_id(&store.rows[0].identity, 0);
        let graph = Arc::new(FakeGraph::default().holding(&held, TaskStatus::Running));
        let control: Arc<dyn OrchestratorControl> = graph.clone();

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            Some(&DeadPool),
            None,
            None,
        )
        .await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.decision, DispatchDecision::PausedLlmPoolUnavailable);
        assert_eq!(state.pending, 3);
        // Whether a task holds a row, and what became of it, are answers the
        // graph gives and the pool's state does not change.
        assert_eq!(state.already, 1);
        assert_eq!(state.held_back, 2, "owed, held by nothing, handed nowhere");
        assert_eq!(state.submitted, 0);
        assert_eq!(state.failed, 0, "nothing was attempted, so nothing failed");
        assert_the_counts_add_up(&state);
        assert!(
            graph.submitted().is_empty(),
            "a paused pass hands nothing over"
        );
    }

    /// A row whose handovers the environment refused, at every generation the
    /// budget allows, is placed in the column that names it -- and it is not the
    /// same column as `already`, which is what the rows the graph holds read as.
    #[tokio::test]
    async fn the_environment_killed_rows_are_counted_apart_from_the_ones_in_flight() {
        let limit = dispatch_rearm_limit();
        let store = FakeLedger {
            rows: vec![pending_row("crates/a")],
            ..Default::default()
        };
        let mut graph = FakeGraph::default();
        for generation in 0..=limit {
            let id = dispatch_task_id(&store.rows[0].identity, generation);
            graph = graph.holding(&id, TaskStatus::Failed);
        }
        let graph: Arc<dyn OrchestratorControl> = Arc::new(graph);

        dispatch_pass(&store, &graph, &HashMap::new(), None, None, None).await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.pending, 1);
        assert_eq!(state.dead_environment, 1);
        assert_eq!(state.already, 0);
        assert_eq!(state.submitted, 0);
        assert_eq!(state.rearmed, 0, "the budget is spent, so nothing re-armed");
        assert_the_counts_add_up(&state);
    }

    /// A handover the goal path would not take is a row placed, not a row
    /// dropped: the pass failed to file it, and the change is exactly as owed as
    /// it was.
    #[tokio::test]
    async fn a_handover_the_goal_path_would_not_take_is_counted_as_failed() {
        let store = FakeLedger {
            rows: vec![pending_row("crates/a"), pending_row("crates/b")],
            ..Default::default()
        };
        let graph: Arc<dyn OrchestratorControl> = Arc::new(FakeGraph::default().refusing());

        dispatch_pass(&store, &graph, &HashMap::new(), None, None, None).await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.pending, 2);
        assert_eq!(state.failed, 2);
        assert_eq!(state.submitted, 0);
        assert_eq!(state.held_back, 0);
        assert_the_counts_add_up(&state);
    }

    /// The generation a row's handover goes out under, read off the graph
    /// without submitting anything: nothing held is generation zero, an
    /// environment refusal moves it on, and anything else the graph holds is the
    /// row's answer rather than a reason to file it again.
    #[tokio::test]
    async fn the_generation_walk_stops_at_the_first_slot_nothing_holds() {
        let limit = dispatch_rearm_limit();
        let row = pending_row("crates/a");
        let id = |generation: u32| dispatch_task_id(&row.identity, generation);
        let empty: Arc<dyn OrchestratorControl> = Arc::new(FakeGraph::default());
        assert!(matches!(
            owed_row(&empty, &row, limit).await,
            Owed::NoTask { generation: 0 }
        ));

        let killed_once = FakeGraph::default().holding(&id(0), TaskStatus::Failed);
        let killed_once: Arc<dyn OrchestratorControl> = Arc::new(killed_once);
        assert!(matches!(
            owed_row(&killed_once, &row, limit).await,
            Owed::NoTask { generation: 1 }
        ));

        // The re-armed handover is in flight: the row is placed, and no third
        // generation is filed behind it.
        let in_flight = FakeGraph::default()
            .holding(&id(0), TaskStatus::Failed)
            .holding(&id(1), TaskStatus::Running);
        let in_flight: Arc<dyn OrchestratorControl> = Arc::new(in_flight);
        assert!(matches!(
            owed_row(&in_flight, &row, limit).await,
            Owed::Held(Handover::InFlight)
        ));

        // The work's own failure is the change's answer, and it is not re-armed:
        // filing it again would hand over a change the graph has already judged.
        let dead_work: Arc<dyn OrchestratorControl> =
            Arc::new(FakeGraph::default().holding_dead_work(&id(0)));
        assert!(matches!(
            owed_row(&dead_work, &row, limit).await,
            Owed::Held(Handover::DeadWork)
        ));

        // Every generation the budget allows was refused by the environment, so
        // there is no next slot to file under: the row is left owed and named.
        let mut spent = FakeGraph::default();
        for generation in 0..=limit {
            spent = spent.holding(&id(generation), TaskStatus::Failed);
        }
        let spent: Arc<dyn OrchestratorControl> = Arc::new(spent);
        assert!(matches!(
            owed_row(&spent, &row, limit).await,
            Owed::Held(Handover::DeadEnvironment)
        ));
    }

    /// Every count the pass holds is a cell a reader can name, and the cells
    /// carry the numbers the row carries.
    ///
    /// One publish per readout, and the readout is the whole label set: the
    /// reason is a family of its own, and what this pins is that the counts did
    /// not take a label that moves. A count that carries the tick's reason is a
    /// count whose series changes identity whenever the reason does, and every
    /// identity the pass leaves behind is served for ever -- so the assertion
    /// below is the one that caught the old shape being unreadable, in the
    /// direction that matters: the frozen cell was the larger of the two and the
    /// reader's `max` believed it.
    #[tokio::test]
    async fn the_reading_face_publishes_one_cell_per_count_and_the_passs_verdict() {
        let state = DispatchState {
            decision: DispatchDecision::PausedLlmPoolUnavailable,
            pool: PoolReading::Unavailable,
            pending: 143,
            submitted: 3,
            already: 12,
            failed: 1,
            dead_environment: 7,
            held_back: 120,
            rearmed: 2,
            unmeasurable: 5,
            resume_at_unix: 0,
            evidenced_recovery_unix: 0,
            quota_window_secs: 0,
        };
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        let metrics: Arc<dyn MetricsBackend> = metrics.clone();
        publish_dispatch_rows(Some(&metrics), &state).await;
        publish_dispatch_verdict(Some(&metrics), &state).await;

        let end = Utc::now() + chrono::Duration::seconds(1);
        let start = end - chrono::Duration::seconds(600);
        let samples = metrics
            .query_gauge_range(metric_names::CHANGE_DISPATCH_READOUTS.as_str(), start, end)
            .await
            .expect("the reading face is readable");
        let mut published: HashMap<String, f64> = HashMap::new();
        for sample in &samples {
            assert_eq!(
                sample.labels.len(),
                1,
                "a count carries its readout and nothing else: a second label whose value \
                 moves with the tick is a new series on every move, and the store serves the \
                 last value of every series it has ever held: {:?}",
                sample.labels
            );
            let readout = sample
                .labels
                .get("readout")
                .expect("every cell names its readout")
                .clone();
            assert!(
                published.insert(readout, sample.value).is_none(),
                "a readout published twice would be a reader's sum counting it twice"
            );
        }
        // The closed set, all of it and nothing else: a count with no cell is a
        // number no reader can select, and a cell with no count is a series
        // nothing fills.
        assert_eq!(
            published
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            DispatchReadout::ALL
                .iter()
                .map(|readout| readout.as_str().to_string())
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(published["pending"], 143.0);
        assert_eq!(published["submitted"], 3.0);
        assert_eq!(published["already"], 12.0);
        assert_eq!(published["failed"], 1.0);
        assert_eq!(published["dead_environment"], 7.0);
        assert_eq!(published["held_back"], 120.0);
        assert_eq!(published["rearmed"], 2.0);
        assert_eq!(published["unmeasurable"], 5.0);
        // The reader's own arithmetic, done on the cells rather than on the row:
        // the numbers a dashboard subtracts are the numbers the pass placed.
        assert_eq!(
            published["submitted"]
                + published["already"]
                + published["failed"]
                + published["dead_environment"]
                + published["held_back"],
            published["pending"]
        );

        // The verdict: the whole product of the two closed sets, every pass, so
        // that the cell the previous pass held is written back to zero rather
        // than left standing as the reason the debt is not moving.
        let samples = metrics
            .query_gauge_range(metric_names::CHANGE_DISPATCH_VERDICT.as_str(), start, end)
            .await
            .expect("the verdict is readable");
        let mut verdicts: HashMap<(String, String), f64> = HashMap::new();
        for sample in &samples {
            let decision = sample
                .labels
                .get("decision")
                .expect("every cell names its decision")
                .clone();
            let pool = sample
                .labels
                .get("pool_reading")
                .expect("every cell names its pool reading")
                .clone();
            assert!(
                verdicts.insert((decision, pool), sample.value).is_none(),
                "a combination published twice is two cells a reader cannot take apart"
            );
        }
        let expected: std::collections::BTreeSet<(String, String)> = DispatchDecision::ALL
            .iter()
            .flat_map(|decision| {
                PoolReading::ALL
                    .iter()
                    .map(move |pool| (decision.as_str().to_string(), pool.as_str().to_string()))
            })
            .collect();
        assert_eq!(
            verdicts
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            expected,
            "the covered cells are the product of the two closed sets: a cell that is only \
             written when its combination occurs is a cell that goes on being served after \
             the pass has moved off it"
        );
        let held: Vec<&(String, String)> = verdicts
            .iter()
            .filter(|(_, value)| **value == 1.0)
            .map(|(cell, _)| cell)
            .collect();
        assert_eq!(
            held,
            vec![&(
                "paused_llm_pool_unavailable".to_string(),
                "unavailable".to_string()
            )],
            "exactly this pass's combination reads as one; two ones is a reader asking which \
             pass is current and being told there are two"
        );
    }
}
