//! principle: feedback_a_cell_named_for_a_contradiction_counts_the_normal_lifecycle
//! principle: feedback_an_expected_reading_counts_the_set_the_reader_walks_not_the_source_population
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
//! it reads what the platform's own principle account says about the rules it
//! holds the same way again, it reads the outstanding set back out, saying
//! plainly when a read failed, and it measures that set against the tree, through
//! the one call that may move a state. Every intake is mechanical: it carries
//! what a document, a registry or another account asked for and adds nothing of
//! its own, so this loop cannot make a change look done by reading anything into
//! it.
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
//! The second reading source is the project's own debt registries: files in the
//! tree listing dependencies that are owed removal, core modules that are owed a
//! move, and `.rs` files that no `mod` declaration reaches. They are readings and
//! not a second account -- every row they name
//! enters through the same call a declaration does, and only a measurement of the
//! tree can move it. What makes them a source rather than a table is where the
//! news comes from and how a row is retired: nothing queues these, the loop reads
//! them out of the tree it is already measuring against, and a row stops being
//! owed when the tree stops carrying what it asked for -- not when the registry
//! stops listing it. A registry nobody edited would otherwise be able to close an
//! account by deleting its own line, which is the self-reported completion this
//! whole loop is built to refuse.
//!
//! The third is the platform's own principle account, and that it is an account
//! rather than a file is the whole of the difference. What is wrong with a rule
//! is read there, and there are two things it knows: the rules nothing has found
//! a gate for, and the rules a gate has had to refuse work for. They are one
//! intake and not two, because the subject is one -- a gate is the organ that
//! enforces a rule, not the rule -- and a principle that is in both faces has one
//! piece of work, which two intakes would write twice under one identity with the
//! second overwriting the first one's words on every tick.
//!
//! The tier a memory declares says which lane a missing check belongs in, and it
//! is reported rather than required: a principle whose tier nobody has declared
//! has no lane to name, and the goal says that instead of naming one this loop
//! would have had to invent. Leaving such a rule out is the one place this intake
//! could lose a rule that only ever existed as a memory, which is the largest
//! part of that account. A principle a person holds is left out, for the reason
//! that account's own look refuses to call it a debt -- no machine gate is meant
//! to exist for it, so a task would be work nothing could ever close -- and it is
//! counted, because how many rules no machine may ever check is a reading in its
//! own right. What closes one of these rows is the tree carrying a header
//! declaring the gate, which is the same declaration the principle account's own
//! look reads, so both accounts close a row on one fact.
//!
//! The refusal reading rides on the row rather than being an intake of its own: a
//! rule screened only at the moment of the act is one whose state the platform
//! cannot see, so a rule that keeps being broken there wants exactly what an
//! ungated rule wants, and the ask is the same. What a refusal adds is the reason
//! the row exists rather than a different row. Closing it therefore says the rule
//! became judgeable and says nothing about the refusals; what would settle "they
//! stopped" is a fact about the account and not about the tree, and a row that
//! claimed it without naming such a fact would be the permanent debt this loop
//! refuses to mint.
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
//! principle: feedback_a_reader_that_knows_one_path_answers_no_product_about_a_product_that_was_produced
//! principle: feedback_a_stronger_claim_may_not_be_checked_less_often_than_a_weaker_one
//! principle: feedback_two_renderings_of_one_quantity_may_differ_by_a_declared_window

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use cog_core::{
    metric_names, BlockCensus, BlockingSurface, ChangeBlock, ChangeIdentity, ChangeLedgerRow,
    ChangeLedgerStore, ChangeMeasurement, ChangePriority, ChangeReading, ChangeReopen,
    ChangeSource, ChangeState, DemandClosure, DispatchDecision, DispatchState, LiveRevision,
    LiveRevisionSource, LlmPoolStatus, LlmWindowPace, MetricsBackend, OrchestratorControl,
    OwnerLeaseBroker, PoolReading, PrincipleCheckKind, PrincipleLedgerStore, PrincipleRow,
    RetireReason, SFResult, ShutdownSignal, StateBackend, Task, TaskStatus, TaskType, WriteWindow,
};

use cog_core::contract::llm::UpstreamFailure;
use cog_core::contract::outcome::{declares, TERMINAL_ENV_FAILURE_PREFIX};

use crate::change_measure::{CheckTree, Snapshot, UnsettledReason, GREP_KIND};
use crate::principle_measure::HOST_REGISTRY_READ_NO_GATE;

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

/// How many landed rows one pass re-asks because a path their check reads moved.
///
/// The window in [`REOPEN_BOUND`] says how long a wrong `landed` may stand; this
/// says how much of the correction one pass takes. A revision that touched most
/// of the tree must not make one tick unbounded, so the batch is bounded the same
/// way the measurement batch is. What the bound leaves behind is not lost: the
/// window still reaches it, so a wide change costs latency on the rows that did
/// not fit rather than their freshness.
const TOUCHED_PER_PASS: usize = MEASUREMENTS_PER_PASS;

/// How many retired rows one pass asks whether their surface came back.
///
/// This bound is of a different kind from the batches above, and the difference
/// is stated rather than blurred. Those batches rotate: the write a row's answer
/// produces moves it to the back, so a bound is a delay. Here a row whose scope
/// is still absent writes nothing -- deliberately, because writing a "still
/// gone" marker every pass would touch every retired row forever and make the set
/// never quiet -- so the same rows sit at the head of the order every pass, and a
/// retirement set larger than this bound would ask its oldest rows forever and
/// never reach the rest.
///
/// So this bound is a coverage bound and it has to be larger than the set, not a
/// slice of it. It is set well above the set measured when this was written (58
/// rows) so that ordinary growth stays inside it, and the pass warns when it
/// reads a full batch, which is the only signal that the assumption has been
/// outgrown. What that warning asks for is a rotation key in the read (a cursor
/// persisted with the row), not a larger number here.
const RETIRED_RECHECK_PER_PASS: usize = 500;

/// How many owed rows one pass hands over.
///
/// The same bound, of the same shape, as the three folds above: a pass is a
/// maintenance tick, not a queue drain, so it hands over a bounded slice and the
/// next tick takes the rest -- an owed set that grew large must not make a
/// single tick unbounded, and unlike a fold a hand-over is not a write this
/// process finishes: it is work the goal path then pays for, so handing over a
/// whole backlog at once spends the pool on one tick's worth of rows.
///
/// This number is a **value with a stated source, and the source is not a
/// measurement**: it is the tolerance this same tick already applies to every
/// other set it touches, and those three were themselves chosen, for a reason
/// that does not transfer here (their queues are written by a person or a script,
/// in batches of tens). What would make it derived is a reading this tree does
/// not have -- **what one hand-over costs**, in pool time and in run time. With
/// that, the batch could be solved from "one tick's hand-overs must be paid for
/// inside one tick" rather than borrowed. Until it exists this is a number a
/// person may have to re-tune, and it says so instead of dressing the borrowing
/// up as a derivation.
///
/// This is the **ceiling**, not the batch: a pass whose pool snapshot measured a
/// quota window hands over the smaller of this and what the window's remaining
/// allowance pays for ([`handovers_for_this_pass`]). The bound below therefore
/// describes the largest batch a pass can make, which is the case the sweep
/// promise has to hold for.
///
/// What the bound buys is checked rather than asserted in prose: an owed set of
/// up to `HANDOVERS_PER_PASS * UNMEASURABLE_RETRY / CHANGE_LEDGER_PASS_INTERVAL`
/// rows is swept inside `UNMEASURABLE_RETRY`, and the assertion below fails to
/// compile if the three numbers drift apart.
const HANDOVERS_PER_PASS: usize = 200;

const _: () = assert!(
    CHANGE_LEDGER_PASS_INTERVAL.as_secs() <= UNMEASURABLE_RETRY.as_secs()
        && (HANDOVERS_PER_PASS as u64) * CHANGE_LEDGER_PASS_INTERVAL.as_secs()
            >= UNMEASURABLE_RETRY.as_secs(),
    "an owed row is handed over within UNMEASURABLE_RETRY, and that is a relation \
     between three numbers rather than a property of any one of them: a pass \
     interval longer than the window cannot meet it for any batch, and a batch \
     smaller than the number of passes the window holds sweeps a one-batch set \
     more slowly than the window promises. Which way the account is allowed to be \
     late is not decided by editing one constant."
);

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

/// The queue rank this loop declares for work whose documents stated no band:
/// one below the default every other task lands on.
///
/// Declared by the submitter, not derived from what kind of task it is — the
/// scheduler reads a number and a source field that never reaches admission.
/// The rank matters because admission takes highest-priority first, and this
/// loop is the graph's largest producer: over a thousand of its rows sat
/// pending while a submitted goal was one row among them at the same rank, and
/// equal rank does not mean an even share when one side is a thousand rows
/// deep. One below the default keeps a dispatched change from taking the slot a
/// submitted goal could have used, without making it ineligible: it still runs
/// whenever the graph has room this pass did not claim.
const DISPATCH_TASK_PRIORITY: i32 = 0;

/// The task kind a dispatched change is submitted under.
///
/// A name of the platform's own flows rather than a kind of hand: the change is
/// not handed to a generic worker, it is handed to the change-generation path,
/// and the kind is what that path is looked up by.
const DISPATCH_TASK_KIND: &str = "change_ledger";

/// The name a pass writes into `sources_unread` when the principle ledger could
/// not be read.
///
/// Stable across releases: a reader counts passes carrying the same word, so
/// renaming it resets the streak it was measuring and reads as "the outage
/// ended". The tree's own registries were a second intake under a second name;
/// they left with the registries themselves -- the debt they carried is in the
/// platform's ledger now, declared under the same identities, so the tree no
/// longer has an intake whose failure could go unwitnessed.
const INTAKE_PRINCIPLES: &str = "principles";

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
///
/// One name for one subject, and the subject is the principle. A gate is the
/// organ that enforces a rule, not the thing the change account is about, so a
/// rule nothing enforces and a rule a gate has had to refuse work for cite this
/// same account: naming the face each was read off would put two names on one
/// row's subject, and reading an account's two faces as two sources is the
/// mistake this one name exists to not make.
const PRINCIPLE_LEDGER_SOURCE: &str = "principle_ledger";

/// The id prefix every handover of one change carries.
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
/// This is also the edge a reader uses to get back to a change's product. The
/// graph's task table is pruned and cleared per workspace, so a row there is not
/// something to join against, but a ledger row is kept, and its identity fixes
/// the id of every handover the change ever made. The account's own row is
/// therefore enough: recompute the id from the identity it already holds, and the
/// product is found again without anything having been stored to connect the two.
/// A stored copy of a derivable hash is a second original of an edge that cannot
/// disagree with the identity while being read as though it could.
pub fn handover_task_prefix(identity: &ChangeIdentity) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [&identity.kind, &identity.scope, &identity.literal] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    let hex = hasher.finalize().to_hex();
    format!("{DISPATCH_TASK_PREFIX}{}", &hex[..16])
}

/// The task id a change is dispatched under, at a given handover generation.
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
/// The generations are a closed set ([`dispatch_rearm_limit`]), which is what a
/// reader enumerates: the ids are exact and few, so a reader gets from the
/// identity to a change's product without a pattern query and without anything
/// from the storage contract beyond the read every backend already has.
fn dispatch_task_id(identity: &ChangeIdentity, generation: u32) -> String {
    let prefix = handover_task_prefix(identity);
    if generation == 0 {
        prefix
    } else {
        format!("{prefix}-r{generation}")
    }
}

/// How far back one pass reads the commit log for delivery trailers.
///
/// A count rather than a revision range: a delivery that arrived while this
/// process was down is still a delivery, and a range anchored at the last pass
/// would forget it. The landing machine writes at most a few commits a day, and
/// this bound is what keeps a repository with years of history from turning one
/// pass into a walk of it.
const LANDING_LOG_COMMITS: usize = 2_000;

/// The changes a commit log says it landed, keyed by the id each commit names.
///
/// Read off the tree rather than written by the landing machine, because the
/// writer and the reader are in different processes: a write-back would have to
/// survive the landing process dying between the commit and the record, and then
/// the record would be missing for a change that is plainly on the line.
///
/// Two forms of trailer name the same fact. The current one is
/// [`cog_core::LANDING_TRAILER`]; the producer generation that landed every
/// change before it wrote the id only in a `Change-Id:` trailer (with a
/// `chore(<scope>): land change <id>` subject, which this also covers through
/// the trailer). Reading only the current form would report the whole existing
/// history as undelivered -- which is the inversion the trailer exists to make
/// impossible -- so both are accepted. Neither can name a row by accident: the
/// id is matched against ledger ids, whose hash prefix no other commit knows.
const CHANGE_ID_TRAILER: &str = "Change-Id: ";

/// The change id a line names, if the line is a landing trailer.
fn trailer_named_change(line: &str) -> Option<&str> {
    let named = line
        .trim()
        .strip_prefix(cog_core::LANDING_TRAILER)
        .or_else(|| line.trim().strip_prefix(CHANGE_ID_TRAILER))?;
    let id = named.split_whitespace().next()?;
    (!id.is_empty()).then_some(id)
}

/// The trailer carries the id of the task the change was handed over under, and
/// one commit may name more than one change. First sighting wins as the log is
/// read newest first, so a change that was landed twice is attributed to the
/// later commit.
fn landing_trailers(log: &str) -> HashMap<String, String> {
    let mut landed = HashMap::new();
    for record in log.split('\u{1e}') {
        let mut fields = record.splitn(2, '\u{1f}');
        let (Some(rev), Some(body)) = (fields.next(), fields.next()) else {
            continue;
        };
        let rev = rev.trim();
        if rev.is_empty() {
            continue;
        }
        for line in body.lines() {
            let Some(id) = trailer_named_change(line) else {
                continue;
            };
            landed
                .entry(id.to_string())
                .or_insert_with(|| rev.to_string());
        }
    }
    landed
}

/// The revision that delivered this row, if one names it.
///
/// Matched by task-id prefix rather than by equality: a re-armed handover
/// appends its generation to the id (`-r1`), and a change landed under any
/// generation was delivered. The prefix is the identity's hash, so no other
/// change's id begins with it.
fn delivered_revision<'a>(
    identity: &ChangeIdentity,
    deliveries: &'a HashMap<String, String>,
) -> Option<&'a str> {
    let prefix = handover_task_prefix(identity);
    deliveries
        .iter()
        .filter(|(id, _)| id.starts_with(&prefix))
        .max_by(|a, b| a.0.cmp(b.0))
        .map(|(_, rev)| rev.as_str())
}

/// The name a run's deliverable carries.
///
/// Named rather than matched as a kind of artifact: the acceptance this reading
/// answers is stated at the field layer under *this* name, so a reader that took
/// any artifact of a change-like type would count runs its own criterion does
/// not, and the reading and the criterion would be two definitions of one fact.
const HANDOVER_ARTIFACT: &str = "changes.diff";

/// Where the plan-generation segment of a run records its deliverable: one
/// artifact, at the top of the snapshot's generation.
const HANDOVER_GENERATION_ARTIFACTS: &str = "/snapshot/generation/artifacts";

/// Where the roundtable segment of a run records its deliverable: one per round,
/// under the round's own generation.
///
/// Both segments are read because both are measured in the stream: the
/// plan-generation path writes the first shape and the roundtable path the
/// second, and a reader that knew one of them would answer "no product" about a
/// run that produced one -- which is the one answer this reading exists to keep
/// separate from "produced nothing". The roundtable's own final generation is
/// not read as well: every measured round about which it is set also has its
/// artifact in the history, so history is the wider of the two and the final
/// entry is not a second fact.
const HANDOVER_ROUNDTABLE_HISTORY: &str = "/snapshot/roundtable/history";

/// How many events of one handover are read per page.
///
/// Measured, not chosen: the longest task in the live stream holds 146 events
/// (p99 is 48, the mean 4.9), and one of the deliverable-carrying events sits at
/// position 146, so a single page would have to be longer than the longest run
/// there has ever been. Paging at a bound under that keeps the read correct for
/// a stream longer than any of them rather than correct for the ones observed.
const HANDOVER_EVENTS_PER_PAGE: usize = 100;

/// The deliverable one handover left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverProduct {
    /// The generation the handover that produced it went out under.
    pub generation: u32,
    /// The field path inside the run's own payload the artifact was recorded at,
    /// so a reader can tell which of the run's segments produced it.
    pub recorded_at: &'static str,
    /// The diff itself, verbatim, as the run wrote it.
    pub diff: String,
}

/// What one change's handovers left in the event stream.
///
/// Two answers rather than one, because the question has two that a single
/// number cannot tell apart: a generation whose events are gone is a change
/// nothing was ever handed to, and a generation with events and no artifact is a
/// run that ran and recorded nothing. Read as one number, a segment that never
/// ran becomes a product that was lost.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandoverReadback {
    /// The generations whose events are still in the stream, oldest first.
    pub generations: Vec<u32>,
    /// The newest handover that left a deliverable. Newest rather than first,
    /// for the same reason the newest generation is the one a handover goes out
    /// under: a later handover is the change's current attempt at itself.
    pub product: Option<HandoverProduct>,
}

/// The deliverable inside one `artifacts` array, if the array holds one.
fn change_artifact_in(artifacts: Option<&serde_json::Value>) -> Option<&str> {
    artifacts?
        .as_array()?
        .iter()
        .find(|artifact| {
            artifact.get("name").and_then(|name| name.as_str()) == Some(HANDOVER_ARTIFACT)
        })
        .and_then(|artifact| artifact.get("content"))
        .and_then(|content| content.as_str())
}

/// The deliverable one run's own payload carries, and the field path it was
/// recorded at.
///
/// Read from the event's payload rather than from the text of the event: a
/// serialized run quotes the deliverable's name in the plan and in the
/// evaluation's feedback as well, so a reader matching the serialized event as a
/// string counts the request echoed back to it as the product itself. Measured on
/// the live stream: 97 events carry the artifact at the plan-generation path, 82
/// at the roundtable one, and 179 match the diff's own header as text -- the
/// third number is the first two added together, which is what makes the text
/// layer an upper bound and not an answer.
fn change_artifact(payload: &serde_json::Value) -> Option<(&'static str, &str)> {
    if let Some(diff) = change_artifact_in(payload.pointer(HANDOVER_GENERATION_ARTIFACTS)) {
        return Some((HANDOVER_GENERATION_ARTIFACTS, diff));
    }
    payload
        .pointer(HANDOVER_ROUNDTABLE_HISTORY)?
        .as_array()?
        .iter()
        .rev()
        .find_map(|round| {
            change_artifact_in(round.pointer("/generation/artifacts"))
                .map(|diff| (HANDOVER_ROUNDTABLE_HISTORY, diff))
        })
}

/// Read back what a change's handovers left in the event stream.
///
/// The edge is the identity and nothing else. Every handover of a change is
/// filed under an id the identity fixes ([`handover_task_prefix`]), so the
/// product is found again with nothing stored to connect the two -- no mapping
/// table, and no join onto the graph, whose task rows are cleared per workspace
/// while the account's rows are kept. That is why this is a function of the
/// identity: storing the edge would be keeping a second copy of something
/// already derivable, and the copy is the one that goes stale.
///
/// The generations are enumerated over the closed set the dispatcher can produce
/// rather than matched as a pattern. Enumeration is what keeps the reader off the
/// storage contract: exact ids and the read every backend already has, so there
/// is no new method to implement three times and no pattern query to get wrong,
/// and the same code path runs wherever the loop does -- which is also what lets
/// its own tests exercise it without a database.
///
/// What this does not answer: whether the change was worked on. That is the
/// graph's answer, read by [`owed_row`], and the two are deliberately not one
/// call -- this one has to keep answering after the graph's row is gone.
pub async fn read_handover_products(
    backend: &dyn StateBackend,
    identity: &ChangeIdentity,
) -> SFResult<HandoverReadback> {
    let mut readback = HandoverReadback::default();
    for generation in 0..=dispatch_rearm_limit() {
        let task_id = dispatch_task_id(identity, generation);
        let mut from = 0u64;
        let mut any = false;
        loop {
            let page = backend
                .get_events(&task_id, from, HANDOVER_EVENTS_PER_PAGE)
                .await?;
            let Some(last) = page.last() else { break };
            any = true;
            for event in &page {
                if let Some((recorded_at, diff)) = change_artifact(&event.payload) {
                    readback.product = Some(HandoverProduct {
                        generation,
                        recorded_at,
                        diff: diff.to_string(),
                    });
                }
            }
            if page.len() < HANDOVER_EVENTS_PER_PAGE {
                break;
            }
            // Where the next page starts, from the backend's own answer about
            // the page it just returned. The two answers are not the same one:
            // Postgres reads the argument as a row's position, and hands back
            // the position each row holds, while the in-memory and Redis
            // backends read it as an index into the run's events and hand back
            // the offset the appender passed, which is zero. The larger of the
            // two is the one that is further along under both readings, and
            // taking the maximum is also what makes this loop able to finish:
            // the count always advances, so a backend whose events all carry the
            // same offset cannot hold it at one page for ever.
            from = last.offset.saturating_add(1).max(from + page.len() as u64);
        }
        if any {
            readback.generations.push(generation);
        }
    }
    Ok(readback)
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
/// The task this loop hands one owed change over as.
///
/// Separate from the pass so the row it produces can be read on its own: the id,
/// the kind the change-generation path is looked up by, and the rank the
/// submitter declares are all decided here, and a test can state them without
/// running a whole ledger pass.
/// The rank a handover is submitted at: the floor, lifted by the band its
/// documents stated.
///
/// The band already decides the order this loop hands rows over in, and until
/// this function existed it stopped deciding anything at the boundary that
/// actually settles what runs next. Every handover was filed at one flat rank,
/// so the P0 row the pass had just taken first went into the graph indistinguishable
/// from the two thousand rows nobody prioritised behind it and waited the same
/// turn -- the ordering was undone one step after it was computed.
///
/// Lifting by the band's own rank leaves the floor's rule intact where it was
/// measured: a row whose documents stated no band still lands one below the
/// default. A stated band overtakes that floor because it is a claim somebody
/// made about urgency rather than this loop's own guess -- the same reason the
/// account keeps the band from the documents instead of deriving one. How much
/// of the owed set carries a band is a reading in its own right
/// (`owed_p0` .. `owed_unstated`), so a band large enough to hold the graph is
/// visible there before it can be mistaken for the graph simply being busy.
fn dispatch_priority(row: &ChangeLedgerRow) -> i32 {
    // The blocking term is the account's own answer to "what holds the line",
    // and it outranks every declared band: the pass hands such a row over
    // first, and handing it over at a lower rank would undo that one step
    // later. It is the same rank a P0 carries, not a sixth number, because a
    // consumer reading priorities should not have to learn a scale the
    // documents do not state.
    let blocking = if row.holds_the_line() {
        ChangePriority::P0.rank()
    } else {
        0
    };
    DISPATCH_TASK_PRIORITY.saturating_add(i32::from(row.priority().rank().max(blocking)))
}

fn dispatch_task(row: &ChangeLedgerRow, generation: u32) -> Task {
    let mut task = Task::new(
        dispatch_task_id(&row.identity, generation),
        TaskType::Custom(DISPATCH_TASK_KIND.into()),
        serde_json::json!({
            "goal": dispatch_goal(row),
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
    task.priority = dispatch_priority(row);
    task
}

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
    events: Option<Arc<dyn StateBackend>>,
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
    if events.is_none() {
        // The events are where a handover's product lands, and this is the one
        // reader that gets back to them from the account's own rows. Said at
        // start-up for the reason the others are: without it the readback cells
        // are zero on every pass, which is the same number a pass that read
        // nothing writes.
        warn!(
            "change ledger: no event store is published, so this loop cannot read back what \
             the changes it handed over produced -- the handover counts are not read at all"
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
            let events = events.clone();
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
                // The revision the previous pass read, so this pass can name the
                // paths that moved since and ask the landed rows they reach
                // again. It is the last pass that finished, not the last that
                // started: a pass that could not take the hold never read a
                // revision, and widening the diff to the last one that did is
                // what keeps that gap from becoming a window of unseen changes.
                let mut previous_rev: Option<String> = None;
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
                    // The paths that moved since the last pass read this tree,
                    // resolved before the passes that use them. A diff that
                    // cannot be read is not a pass without changes: it is one
                    // reason to fall back on the window, said once, with the
                    // sweep still covering every landed row it would have
                    // reached.
                    let changed = match (&previous_rev, &first_seen_rev) {
                        (Some(prev), Some(now)) if prev != now => {
                            match tree.changed_paths(prev, now).await {
                                Ok(paths) => paths,
                                Err(why) => {
                                    debug!(
                                        why = %why,
                                        "change ledger: the paths between this revision and the \
                                         last one cannot be read, so landed rows are re-asked on \
                                         the window alone this pass"
                                    );
                                    Vec::new()
                                }
                            }
                        }
                        _ => Vec::new(),
                    };
                    read_pass(&*store, first_seen_rev.as_deref()).await;
                    // The intakes this pass could not read, named for the row the
                    // hand-over writes at the end of the pass. Collected here and
                    // not read off the logs: a warning rolls away with the
                    // process, and the account it left empty keeps looking like a
                    // pass that found nothing to hold.
                    let mut sources_unread: Vec<&'static str> = Vec::new();
                    // The intake, from the platform's own principle ledger,
                    // after the fold and before the measurement: a row read this
                    // pass is judged by this pass. Skipped where no ledger was
                    // published, which the start-up line says.
                    if let Some(ref principles) = principles {
                        // Both faces of that account in one read, because they are
                        // two things known about one subject: the rules nothing
                        // enforces and the rules a gate has had to refuse work
                        // for. In beside the rest, for the reason the registries
                        // give -- a row read this pass is judged by this pass --
                        // and read whatever the tree did, because what a refusal
                        // has news of is an act somebody took and not a line in
                        // this revision.
                        if let Some(unread) =
                            principle_pass(&**principles, first_seen_rev.as_deref(), &*store).await
                        {
                            sources_unread.push(unread);
                        }
                    }
                    // The classifier's retirements go in after the readings and
                    // before the measurement, for the reason [`retire_pass`]
                    // gives: a fixture retired this pass is not measured and
                    // re-said as unsettled by the same pass.
                    retire_pass(&*store).await;
                    measure_pass(&*store, &tree, &mut unsettled, &changed).await;
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
                        // What the line says it has landed, read from the same
                        // repository the measurement reads. A change whose diff
                        // went in is still owed until its check holds, and the
                        // two facts are only together in the pass -- which is
                        // why the count is taken here rather than by a write
                        // from the landing process, whose record would be lost
                        // the day it died between the commit and the write.
                        let deliveries = match tree.landing_log(LANDING_LOG_COMMITS).await {
                            Ok(log) => Some(landing_trailers(&log)),
                            Err(why) => {
                                warn!(
                                    why = %why,
                                    "change ledger: the commit log could not be read, so this \
                                     pass cannot say which owed changes were delivered"
                                );
                                None
                            }
                        };
                        let pool = pool.as_deref();
                        if let Some(read) = dispatch_pass(
                            &*store,
                            orchestrator,
                            &unsettled,
                            pool,
                            pool_posture,
                            metrics.as_ref(),
                            events.as_deref(),
                            &sources_unread,
                            deliveries.as_ref(),
                        )
                        .await
                        {
                            pool_posture = Some(read);
                        }
                    }
                    // Set last, and only here: a pass that returns early never
                    // reached the reads this revision describes, so advancing to
                    // it would hide the paths between the two from the next diff.
                    previous_rev = first_seen_rev;
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

/// The rows of a registry this intake read and could not turn into a check, by
/// cause.
///
/// Counted rather than dropped, and by cause rather than in one total, because an
/// empty account has two very different explanations: a registry with nothing in
/// it, and a registry whose every row is one this intake cannot aim. Both
/// contribute zero readings, and only one of them is news. The causes are
/// separate for the second reason: the fix for a disposition nobody has taught
/// this intake is not the fix for a file it deliberately refuses to resolve.
#[derive(Debug, Default)]
struct Unaimed {
    /// How many rows each cause accounted for. A `BTreeMap` rather than a
    /// `HashMap` so two passes over one registry print their counts in the same
    /// order and can be compared by eye.
    counts: BTreeMap<&'static str, usize>,
}

impl Unaimed {
    fn note(&mut self, cause: &'static str) {
        *self.counts.entry(cause).or_default() += 1;
    }

    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// The counts as one field, causes in a fixed order: `{cause}={n}` pairs
    /// separated by spaces; the empty string when there is nothing to say.
    fn summary(&self) -> String {
        self.counts
            .iter()
            .map(|(cause, n)| format!("{cause}={n}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
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
/// Run before the measurement: a row read for the first time in a pass is
/// judged in that same pass.
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
) -> Option<&'static str> {
    let (readings, unaimed) = match principle_readings(principles).await {
        Ok(read) => read,
        Err(why) => {
            warn!(
                why = %why,
                "change ledger: the platform's principle ledger could not be read, so this pass \
                 contributed no readings from it"
            );
            return Some(INTAKE_PRINCIPLES);
        }
    };
    // Said before the write and whether or not the write lands: the rows this
    // intake cannot aim are a property of that account, and a pass that read them
    // has to report them even on a tick whose write was refused.
    if !unaimed.is_empty() {
        warn!(
            unaimed = %unaimed.summary(),
            "change ledger: the principle ledger holds rows this intake cannot aim"
        );
    }
    match store.record_readings(&readings, first_seen_rev).await {
        Ok(_) => info!(
            readings = readings.len(),
            "change ledger: read the principle account for the rules it says are not enforced"
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
    // Said only for a read that failed: the ledger answered, and what failed was
    // the write into this account.
    None
}

/// What the platform's own principle account says about each of its rules, as
/// readings.
///
/// Two faces of one account, and the reason they are one intake rather than two
/// is the subject: the account is about principles, and a gate is the organ that
/// enforces one. So "nothing enforces this rule" and "a gate has had to refuse
/// work for this rule" are two things known about one principle, not two
/// principles -- and a rule that is both must enter the change account once, with
/// both readings on it. Read as two intakes they would write the same identity
/// twice, and the second write would overwrite the first one's goal on every
/// tick, making the row's words depend on which pass ran last.
///
/// The two faces are independent, which is why both are read. A rule can be
/// enforced and refused at once: that is a gate working, and the reading it
/// leaves is that the rule keeps being broken at the moment of the act -- work
/// whose end is a check the tree can be judged by, exactly as for a rule nothing
/// enforces. A rule with no gate at all is refused by nothing, so the unpaid set
/// alone would miss every one of them.
///
/// Each batch is sized from the source account's own census rather than from a
/// number chosen here: a constant would be a second answer to a question the
/// account already answers, and one that goes stale as principles are added. The
/// refused face is bounded by the account's whole size and not by its unpaid
/// part, because a refused row can be one the account counts as enforced.
async fn principle_readings(
    principles: &dyn PrincipleLedgerStore,
) -> Result<(Vec<ChangeReading>, Unaimed), String> {
    let census = principles
        .census()
        .await
        .map_err(|e| format!("the principle ledger's census could not be read: {e}"))?;
    // Unmeasured rows are part of the unpaid population, because a row no look
    // has settled is not a row with a gate: the count this is taken against is
    // the one the account's own coverage reading is taken against.
    let owed = census.unenforced.saturating_add(census.unmeasured);
    let unpaid = principles
        .outstanding(usize::try_from(owed).unwrap_or(usize::MAX))
        .await
        .map_err(|e| format!("the principle ledger's unpaid set could not be read: {e}"))?;
    let refused = principles
        .refused(usize::try_from(census.total()).unwrap_or(usize::MAX))
        .await
        .map_err(|e| format!("the principle ledger's refused set could not be read: {e}"))?;
    Ok(principle_debt_readings(&principle_faces(unpaid, refused)))
}

/// The account's two faces as one list, one row per principle.
///
/// The unpaid set's order is kept (stalest first) and what only the refused face
/// holds is appended. A rule can be in both at once -- unenforced and refused is
/// an ordinary combination -- and both queries answer with the account's own row
/// from the same table, so the copy kept here carries the refusal count either
/// way and nothing is lost by dropping the second. What must not happen is two
/// copies: the account is written by identity, and two writers of one identity
/// leave the row's words depending on which one ran last, because the write keeps
/// the title it was handed.
///
/// Split out of the reading so this is what a test can stand on: whether one
/// subject becomes one row is not a property of how the read went.
fn principle_faces(unpaid: Vec<PrincipleRow>, refused: Vec<PrincipleRow>) -> Vec<PrincipleRow> {
    let mut seen = HashSet::new();
    let mut rows = Vec::with_capacity(unpaid.len() + refused.len());
    for row in unpaid.into_iter().chain(refused) {
        if seen.insert(row.slug.clone()) {
            rows.push(row);
        }
    }
    rows
}

/// The principles the account says are not enforced, or that a gate has had to
/// refuse work for, as readings.
///
/// A row enters on one condition, and it is the account's own: its tier is not
/// the one a person holds. Everything else in that account is a rule whose state
/// this platform cannot see, and that is the whole of what a change needs to
/// exist about it.
///
/// `judgement` stays out, and it is the class the account itself defines as the
/// rules that cannot be made into a gate: its own look writes no state for such a
/// row at all, so it stays in the unpaid set forever by construction, and reading
/// it as work would mint a task no gate could ever close -- the same permanent
/// debt that account's own verdict refuses to state. It is counted rather than
/// dropped, because "how many of these rules no machine may ever check" is a
/// reading somebody has to be able to take.
///
/// A tier nobody has declared is work and not a gap in the reading: the tier says
/// which lane the missing check belongs in, and its absence is what the goal
/// reports. Leaving it out charged nobody for the rule -- which is the account's
/// largest bucket, and the one where a rule that only ever existed as a memory
/// would go unnoticed.
///
/// The check is the tree carrying a header that declares the gate, which is the
/// declaration the principle ledger's own look reads as the platform-side gate
/// column. Both accounts therefore close this row on one fact, and neither can
/// close it while the other still calls it owed.
fn principle_debt_readings(rows: &[PrincipleRow]) -> (Vec<ChangeReading>, Unaimed) {
    let mut readings = Vec::new();
    let mut unaimed = Unaimed::default();
    for row in rows {
        let lane = match row.check_kind {
            Some(kind) => match check_lane(kind) {
                Some(lane) => Some(lane),
                None => {
                    unaimed.note("a-person-holds-this-by-its-own-declaration");
                    continue;
                }
            },
            None => None,
        };
        readings.push(ChangeReading {
            identity: ChangeIdentity::new(
                GREP_KIND,
                PRINCIPLE_GATE_SCOPE,
                principle_literal(&row.slug),
            ),
            title: Some(principle_goal(row, lane)),
            // The violation rides on the row and not only inside the goal: a
            // reader of the account has to be able to see that this rule was
            // refused in practice, which is the difference between "nobody has
            // written the check yet" and "the rule keeps being broken at the
            // moment of the act". A rule the account has never seen refused
            // carries none -- an assertion is not a place to say nothing happened.
            assertion: refusal_sentence(row),
            source: ChangeSource {
                doc: PRINCIPLE_LEDGER_SOURCE.to_string(),
                verdict: None,
                // The ledger names the principle and its tier, not how urgent it
                // is: nothing there states a band, so the reading carries the one
                // that says so rather than a band this path invented.
                priority: ChangePriority::Unstated,
                // The face, by contrast, this path does know: a principle's
                // reading is filed under principles and nowhere else.
                block: ChangeBlock::Principles,
            },
        });
    }
    (readings, unaimed)
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

/// How many characters a goal's first line may occupy.
///
/// The first line is the landing header's text, and every scope the landing
/// writes leaves room for this much inside its subject cap. A title written
/// within the budget reaches `git log --oneline` whole; a longer one is
/// shortened there, and a shortened title is a view a reader has to scroll
/// past.
const GOAL_TITLE_CHARS: usize = 50;

/// What a row of the principle ledger asks for, in the words the goal path is
/// handed.
///
/// The goal is a title and then the details, because that is the shape the
/// landing header reads: its first line becomes the commit subject and the
/// rest becomes the body. The title is the rule's name in words, short enough
/// to survive the header intact -- a title derived at the landing layer used
/// to spend most of the header on the template and the shared category prefix,
/// which left sibling rules reading alike.
///
/// The details carry the slug, the lane its declared tier puts the missing
/// check in, the rule's own sentence, and the violation if a gate has had to
/// refuse work for it. The sentence is carried because a goal that names only
/// where to look is a goal a generator cannot act on, and the rule's own words
/// are what the gate has to answer to -- the same reason the registry intake
/// carries the destination and not only the module.
///
/// The lane is optional and its absence is said rather than filled in. A principle
/// whose memory declares no tier has no lane to name, and inventing one here would
/// hand a generator this path's guess about the rule; what the goal can still say
/// is the part that does not depend on the tier: what lands the row is a check the
/// tree can be judged by. The criterion is the same either way, and it is the
/// criterion that has to be worded exactly -- a goal ending in "decide how to
/// check this" would be work whose end nothing could read.
fn principle_goal(row: &PrincipleRow, lane: Option<&str>) -> String {
    let owed = match lane {
        Some(lane) => format!("a check answering from {lane}, declared"),
        None => "a check the tree can be judged by, declared".to_string(),
    };
    let mut details = format!(
        "Give the principle `{}` the gate it is owed: {owed} in the header of the test that carries \
         it.{}",
        row.slug,
        rule_sentence(row)
    );
    if let Some(violation) = refusal_sentence(row) {
        // The rule's own sentence is quoted as the memory wrote it, and a memory
        // is free to write a fragment; the violation is a sentence of its own and
        // must not run into one. Terminated here rather than in the rule's own
        // words, because a goal that added a stop to a quotation would be quoting
        // something the memory does not say.
        if !details.ends_with('.') {
            details.push('.');
        }
        details.push(' ');
        details.push_str(&violation);
    }
    format!("{}\n\n{details}", goal_title(row))
}

/// The short first line of a principle handover: the rule's name in words.
///
/// The slug is the rule's name, and its words are the closest thing to a title
/// the account holds; the category prefix is dropped because it says where the
/// rule is kept, not what it governs, and every peer row carries it. The full
/// slug is in the details, where an exact name belongs.
fn goal_title(row: &PrincipleRow) -> String {
    const ACTION: &str = "Gate: ";
    let budget = GOAL_TITLE_CHARS.saturating_sub(ACTION.chars().count());
    format!(
        "{ACTION}{}",
        clip_at_word_boundary(&slug_phrase(&row.slug), budget)
    )
}

/// A slug as the words of the rule it names.
fn slug_phrase(slug: &str) -> String {
    let name = match slug.split_once('_') {
        Some(("feedback" | "project" | "reference" | "fact", rest)) => rest,
        _ => slug,
    };
    // The memory names a possessive by the character that follows its
    // underscore, and restoring the apostrophe is what keeps a title from
    // reading like a column name. Only shares and negatives are repaired;
    // `_d` and `_re` would turn ordinary word endings into contractions.
    name.replace("_s_", "'s ")
        .replace("_t_", "'t ")
        .replace("_s", "'s")
        .replace("_t", "'t")
        .replace('_', " ")
}

/// A title cut to `limit` characters at the last word boundary inside it,
/// followed by an ellipsis.
///
/// The landing layer applies the same rule to a goal that ignored this one, but
/// a producer that cuts its own title keeps the words it meant to show: the
/// landing header has less room than the goal's first line.
fn clip_at_word_boundary(title: &str, limit: usize) -> String {
    let title = title.trim();
    if title.chars().count() <= limit {
        return title.to_string();
    }
    let budget = limit.saturating_sub(1);
    let mut end = 0;
    for (taken, (offset, ch)) in title.char_indices().enumerate() {
        if taken == budget {
            break;
        }
        end = offset + ch.len_utf8();
    }
    let head = &title[..end];
    let cut = if title[end..].chars().next().is_none_or(is_title_boundary) {
        head.trim_end()
    } else {
        head.rfind(is_title_boundary)
            .map_or(head, |at| &head[..at])
            .trim_end()
    };
    format!("{cut}…")
}

/// Whitespace, and the marks Chinese uses between clauses: a title in Chinese
/// has no spaces to cut at, so a sentence mark is the only boundary a reader
/// would recognise.
fn is_title_boundary(c: char) -> bool {
    c.is_whitespace() || matches!(c, '，' | '。' | '、' | '；' | '：' | '！' | '？')
}

/// The violation as a sentence in its own right, or nothing when the account has
/// never seen the rule refused.
///
/// One function for the two places it is used -- appended to the goal a handover
/// carries and read on the row's own assertion -- because they say the same thing
/// about the same subject, and the subject is the principle: the gates are named
/// as the organ that refused, not as the thing the sentence is about.
///
/// Empty rather than a sentence saying nothing was refused: the account holds a
/// count and not a claim about its absence, and "has not been refused yet" is not
/// something a zero in that column establishes.
fn refusal_sentence(row: &PrincipleRow) -> Option<String> {
    (row.refusals > 0).then(|| {
        format!(
            "This principle has been refused work {}.",
            refusal_evidence(row)
        )
    })
}

/// The rule's own sentence, as a clause to append to a goal, or nothing.
///
/// Carried because a goal that names only where to look is one a generator cannot
/// act on, and the rule's own words are what its gate has to answer to. Absent
/// rather than replaced when the reading carried no sentence: a clause introducing
/// a rule and then naming none reads as a rule that says nothing, which is a worse
/// goal than one that stops where it knows.
fn rule_sentence(row: &PrincipleRow) -> String {
    row.lead
        .as_deref()
        .map(str::trim)
        .filter(|lead| !lead.is_empty())
        .map(|lead| format!(" The rule itself: {lead}"))
        .unwrap_or_default()
}

/// The refusal a row was read for, as the counted part of a sentence.
///
/// Phrased to fit the two places it is used: the goal a handover carries, where it
/// says why the row exists at all, and the row's own assertion, where it is what a
/// reader of the account has to be able to find. A count with no instant drops the
/// instant rather than substituting one -- events counted before the column was
/// kept are not events from long ago.
///
/// The gate column has a sentinel and it is not a gate: `-` is what that column
/// holds when the host's registry was read and the rule was not in it, which is a
/// reading of the registry and not a gate, so it is dropped rather than printed as
/// the name of one. Empty is dropped with it -- the column's own contract is that
/// it always carries something, so a blank here is a value nobody meant to write
/// and naming it as a gate would turn it into one.
fn refusal_evidence(row: &PrincipleRow) -> String {
    let last = match row.last_refused_at {
        Some(at) => format!(", most recently at {}", at.to_rfc3339()),
        None => String::new(),
    };
    let gate = match row.host_gate.as_deref().map(str::trim) {
        Some(HOST_REGISTRY_READ_NO_GATE) | None | Some("") => String::new(),
        Some(gate) => format!(", by the host gate declared as `{gate}`"),
    };
    format!("{}{last}{gate}", times(row.refusals))
}

/// How many times, in words: a bare numeral in a sentence about a count of one
/// reads as a placeholder, and this count is small by construction.
fn times(n: i64) -> String {
    match n {
        1 => "once".to_string(),
        n => format!("{n} times"),
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
    changed: &[String],
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

    let reopened_pass = reopen_pass(store, tree, &snapshot, unsettled, changed).await;

    info!(
        measured = considered,
        landed,
        absent,
        undecided,
        rechecked = reopened_pass.rechecked,
        reopened = reopened_pass.reopened,
        touched = reopened_pass.touched,
        // A row whose scope left the tree is settled here rather than counted as
        // unsettleable forever. It is its own number because it is its own
        // outcome: the row is no longer landed, and reading it as a rechecked
        // row would say the claim still held.
        landed_retired = reopened_pass.retired,
        // The other direction, and the reason a retirement is not a verdict
        // about the account: a surface that came back makes the row answerable
        // again, and this is the count of rows that stopped being retired.
        landed_restored = reopened_pass.restored,
        // The landed half of the same two readings the outstanding pass already
        // gives. A row that cannot be settled keeps its old `measured_at`, so
        // without these two numbers the staleness order reads the same whether a
        // landed row was skipped inside its retry window or asked and answered
        // with nothing.
        landed_unsettleable = %reason_summary(&reopened_pass.unsettleable),
        landed_held = reopened_pass.held,
        changed_paths = changed.len(),
        unsettled = %reason_summary(&by_reason),
        already_unsettled = known_unsettled,
        revision = snapshot.rev(),
        "change ledger: measured the outstanding set against the tree"
    );
}

/// What one pass over the landed set did, including what it could not do.
///
/// The last two fields are the difference between "the sweep finished" and "the
/// sweep spent its batch": a landed row whose check cannot be settled keeps its
/// `measured_at`, so it holds the head of the order and is skipped until its
/// retry window is up. Counted by reason rather than left to the per-row line,
/// because the rows are re-asked on a timer and a line per row per hour says
/// less than the count that is on every pass.
#[derive(Default)]
struct ReopenPass {
    rechecked: usize,
    reopened: usize,
    touched: usize,
    unsettleable: HashMap<UnsettledReason, usize>,
    held: usize,
    /// Rows a look settled as `retired` because their scope had left the tree.
    /// Separate from `rechecked`, which counts rows whose check answered: a
    /// retirement is what the history answered, and a pass that retires rows is
    /// not a pass that found their claims still holding.
    retired: usize,
    /// Retired rows whose scope is back in the tree and which were measured
    /// again. The mirror image of `retired`: one count says a surface left, the
    /// other says it came back, and a retirement that nothing could undo would
    /// make the first count a statement about the account rather than about the
    /// tree it was reading when it said so.
    restored: usize,
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
/// Returns what the pass did: how many rows were re-asked, how many came back as
/// something other than what they said, and how many of the re-asked rows were
/// reached by a path that moved rather than by the window. The second is the
/// reading this whole pass exists for: a count of moments the account was wrong,
/// and the rows it names are in `change_reopen`, which is where the identity
/// survives the state it loses. The third says which half of the freshness the
/// pass is paying for.
///
/// It also returns the two counts that say the pass did *not* finish: the landed
/// rows the tree could not settle, by reason, and the ones held back because
/// their last failure is still inside [`UNMEASURABLE_RETRY`]. Without them a
/// settled-row count reads the same whether the sweep reached every landed row
/// or spent its batch walking into rows that answer nothing, and a landed row
/// that cannot be settled keeps its `measured_at` -- so it sits at the head of
/// the staleness order forever while every other reading says the set is fresh.
async fn reopen_pass(
    store: &dyn ChangeLedgerStore,
    tree: &CheckTree,
    snapshot: &Snapshot,
    unsettled: &mut HashMap<ChangeIdentity, UnsettledCheck>,
    changed: &[String],
) -> ReopenPass {
    // Asked before the landed sweep and independently of it: the quota below is
    // derived from how many rows are landed and is zero when there are none, but
    // a retired row is answerable again regardless of how many rows are left to
    // check. Tying the two would mean an account whose only movement is a surface
    // returning never asking about it.
    let mut pass = restore_retired(store, tree, snapshot).await;
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
            return pass;
        }
    };
    let quota = reopen_quota(landed, CHANGE_LEDGER_PASS_INTERVAL, REOPEN_BOUND);
    if quota == 0 {
        return pass;
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
    let mut rows = match store.landed_due(take).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the landed set could not be read, so nothing was re-asked"
            );
            return pass;
        }
    };

    // The event-shaped half of the same question. A check's answer is a property
    // of the bytes under its scope, so a revision that touched one of those bytes
    // owes the row a new answer now -- not when a quota of a sweep happens to
    // reach it. Without this read the window in [`REOPEN_BOUND`] is the only
    // thing keeping `landed` fresh, and the freshness of the account's strongest
    // claim would be however long the sweep takes rather than however long the
    // tree takes to change under it.
    let mut touched = 0usize;
    if !changed.is_empty() {
        match store.landed_touching(changed, TOUCHED_PER_PASS).await {
            Ok(due) => {
                let mut seen: HashSet<ChangeIdentity> =
                    rows.iter().map(|row| row.identity.clone()).collect();
                for row in due {
                    if seen.insert(row.identity.clone()) {
                        rows.push(row);
                        touched += 1;
                    }
                }
            }
            Err(e) => warn!(
                error = %e,
                "change ledger: the landed rows a moved path reaches could not be read, so this \
                 pass re-asked only the ones the window made due"
            ),
        }
    }

    let now = Instant::now();
    for row in rows {
        // The same window the outstanding pass keeps, for the same reason: a row
        // whose check cannot be settled keeps its old `measured_at`, so it would
        // hold the head of the staleness order and spend this pass's batch every
        // pass. Asking again before the window is up would buy nothing -- what
        // makes the check unsettleable is a property of the tree, and the tree is
        // what the window is waiting for.
        //
        // Counted rather than silently skipped: a batch spent on rows that are
        // waiting out their window is a batch that did not reach the rest of the
        // set, and the sweep's length is only the bound while this number is
        // zero.
        if let Some(known) = unsettled.get(&row.identity) {
            if still_unanswered(known, now) {
                pass.held += 1;
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
            // A scope absent from this revision is not always a check the tree
            // cannot answer. When the history carries its deletion, the surface
            // was moved out on purpose: the work was real and was measured here,
            // and there is nothing left in this tree for a later look to settle.
            // Leaving the row `landed` says the account still holds something it
            // can no longer see, and `measured_at` never advances, so the age of
            // that timestamp reads as neglect nobody can repair.
            //
            // Asked of the history rather than assumed from the absence: a scope
            // this line never held is a misplaced check, and filing it under the
            // same name as a deliberate move would put "the criterion is wrong"
            // and "the surface left" behind one word.
            //
            // The revision recorded is where the scope exited, not the tree this
            // pass read: the retirement is a fact about the history, and the
            // deletion is the moment it became true. A later reader asking when
            // the row stopped being settleable gets that answer from the row
            // itself.
            if reason == UnsettledReason::ScopeAbsentInRevision {
                match tree.scope_left_at(&row.identity).await {
                    Ok(Some(left_at)) => {
                        let retirement = ChangeMeasurement::retired(
                            row.identity.clone(),
                            RetireReason::SurfaceMoved,
                        )
                        .at_revision(left_at.clone());
                        match store.apply_measurement(&retirement).await {
                            Ok(()) => {
                                unsettled.remove(&row.identity);
                                pass.retired += 1;
                                warn!(
                                    change = %row.identity,
                                    exited_in = %left_at,
                                    read_at = snapshot.rev(),
                                    "change ledger: a landed row's scope has left the tree, so \
                                     the row is retired as surface_moved; the revision it \
                                     carries is where the scope exited, not the tree this pass \
                                     read"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    change = %row.identity,
                                    exited_in = %left_at,
                                    error = %e,
                                    "change ledger: a landed row's scope left the tree, but the \
                                     retirement could not be written, so the row keeps its state"
                                );
                                unsettled.insert(
                                    row.identity.clone(),
                                    UnsettledCheck { why, since: now },
                                );
                                *pass.unsettleable.entry(reason).or_default() += 1;
                            }
                        }
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) => warn!(
                        change = %row.identity,
                        error = %e,
                        "change ledger: a landed row's scope is absent and the history that \
                         would say whether it left cannot be read; the row keeps its state"
                    ),
                }
            }
            // Said when the wording is new, as the outstanding pass does: the row
            // is re-asked on a timer, and a line per row per hour would bury the
            // count below rather than add to it. Unlike that pass this one is a
            // warning, because the row is one the account has already called
            // done and the claim is now being made without a look.
            if unsettled.get(&row.identity).map(|k| k.why.as_str()) != Some(why.as_str()) {
                warn!(
                    change = %row.identity,
                    reason = reason.as_str(),
                    why = %why,
                    "change ledger: a landed row's check cannot be settled against the tree; \
                     it keeps its state and its old measured time, so it stays at the head \
                     of the staleness order"
                );
            }
            unsettled.insert(row.identity.clone(), UnsettledCheck { why, since: now });
            *pass.unsettleable.entry(reason).or_default() += 1;
            continue;
        };
        unsettled.remove(&row.identity);
        pass.rechecked += 1;
        if state != ChangeState::Landed {
            // Written before the measurement, and that order is the point: the
            // measurement clears `landed_at` on the way out of `landed`, so
            // afterwards there is nothing left in the row that says it was ever
            // done. This is the only place that time survives, and it has to be
            // read off the row before the write that takes it away.
            pass.reopened += 1;
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
    ReopenPass { touched, ..pass }
}

/// Ask the rows settled as retired because their scope left whether it is back.
///
/// Retirement is a fact about the history, and the history keeps moving. A
/// surface moved out can be moved back, and the row settled at its exit revision
/// becomes answerable again the moment the bytes return. Without this read,
/// retirement would be the one terminal state nothing could falsify: a row would
/// keep saying the surface is gone while the surface sits in the head of the
/// line, and the only way to notice would be for a person to remember the
/// retirement and go looking.
///
/// The question is asked in two steps and only the second one writes. Whether
/// the scope is in the tree is a cheap look and the answer for most rows is
/// still no; writing a "still gone" marker for those would touch every retired
/// row on every pass and leave the set never quiet. Only a row whose scope is
/// back is measured, and the measurement is what takes it out of the retirement
/// set -- by the row's state changing rather than by anything this pass
/// remembers, so a restart loses no cursor.
///
/// A scope that is back but whose check still cannot be settled keeps its
/// retirement. The retirement says the surface left at a named revision and that
/// is still true of that revision; the new question has no answer yet, and the
/// answer, not the asking, is what moves a row.
async fn restore_retired(
    store: &dyn ChangeLedgerStore,
    tree: &CheckTree,
    snapshot: &Snapshot,
) -> ReopenPass {
    let mut pass = ReopenPass::default();
    let rows = match store.retired_surface_moved(RETIRED_RECHECK_PER_PASS).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the retired set could not be read, so no retirement was                  questioned this pass"
            );
            return pass;
        }
    };
    if rows.len() == RETIRED_RECHECK_PER_PASS {
        warn!(
            bound = RETIRED_RECHECK_PER_PASS,
            "change ledger: the retired set filled the pass's read, so rows beyond it are not              being re-asked at all -- the bound is coverage here, not a slice, and it needs to              grow or the read needs a rotation key before the set does"
        );
    }
    for row in rows {
        match tree.scope_present(snapshot, &row.identity).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => {
                warn!(
                    change = %row.identity,
                    error = %e,
                    "change ledger: a retired row's scope could not be looked for in the tree,                      so the row keeps its retirement"
                );
                continue;
            }
        }
        let outcome = tree.judge(snapshot, &row.identity).await;
        let Some(state) = outcome.state() else {
            warn!(
                change = %row.identity,
                reason = outcome.reason().map(|r| r.as_str()).unwrap_or("none"),
                why = outcome.why().unwrap_or("the look settled nothing and gave no reason"),
                revision = %snapshot.rev(),
                "change ledger: a retired row's scope is back, but its check still cannot be                  settled; the row keeps its retirement, which is still true of the revision it                  names"
            );
            continue;
        };
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
        match store.apply_measurement(&measurement).await {
            Ok(()) => {
                pass.restored += 1;
                warn!(
                    change = %row.identity,
                    state = state.as_str(),
                    retired_measured_rev = row.measured_rev.as_deref().unwrap_or("none"),
                    revision = snapshot.rev(),
                    "change ledger: a retired row's scope is back in the tree, so the row is                      measured again; its retirement was true of the revision the surface left                      at, not of the tree now"
                );
            }
            Err(e) => warn!(
                change = %row.identity,
                state = state.as_str(),
                error = %e,
                "change ledger: a retired row's scope is back, but the new measurement could not                  be written, so the row keeps its retirement"
            ),
        }
    }
    pass
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
///
/// One parameter per collaborator the process may or may not publish, plus the
/// sources whose readings this pass could not take; grouping them into a struct
/// would only move the same list to a type that exists to hold it, and the list
/// grows with the number of source accounts rather than with anything this pass
/// decides.
#[allow(clippy::too_many_arguments)]
async fn dispatch_pass(
    store: &dyn ChangeLedgerStore,
    orchestrator: &Arc<dyn OrchestratorControl>,
    unsettled: &HashMap<ChangeIdentity, UnsettledCheck>,
    pool: Option<&dyn cog_core::LlmPoolStatusSource>,
    carried: Option<PoolVerdict>,
    metrics: Option<&Arc<dyn MetricsBackend>>,
    events: Option<&dyn StateBackend>,
    sources_unread: &[&'static str],
    // The changes the commit log says it landed, by task id. `None` when the
    // log could not be read: delivery is then unmeasured, not zero.
    deliveries: Option<&HashMap<String, String>>,
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
    let mut rows = match store.outstanding(outstanding_total as usize).await {
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

    // Delivery is written before anything is counted, so a change a commit
    // landed since the last pass is in this pass's reading rather than the
    // next one's. Only rows that have never been delivered are written: the
    // account keeps the first sighting, and re-writing the same fact on every
    // pass would be a write that says nothing new.
    if let Some(deliveries) = deliveries {
        let mut recorded = 0usize;
        for row in rows.iter_mut() {
            if row.delivered_rev.is_some() {
                continue;
            }
            let Some(rev) = delivered_revision(&row.identity, deliveries) else {
                continue;
            };
            match store.record_delivery(&row.identity, rev).await {
                Ok(()) => {
                    row.delivered_rev = Some(rev.to_string());
                    recorded += 1;
                }
                Err(e) => warn!(
                    change = %row.identity,
                    error = %e,
                    "change ledger: a delivery could not be recorded, so the row will read as \
                     never delivered until the next pass finds it again"
                ),
            }
        }
        if recorded > 0 {
            info!(
                recorded,
                "change ledger: owed changes a commit on the line names as landed"
            );
        }
    }

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

    // The batch this pass may hand over: the tick's own ceiling, lowered by what
    // the window left when the snapshot measured one. Derived once, from the same
    // verdict the pass writes down, so the row and the batch cannot disagree about
    // which window they were sized against.
    let bound = handovers_for_this_pass(&verdict, HANDOVERS_PER_PASS);
    let handover_budget = bound.budget;
    // The reason is logged because three of these five branches return the
    // ceiling: without it, a pass that could derive no bound at all (a degraded
    // pool, or a ledger too thin to price a job) is indistinguishable from one
    // whose window simply pays for more than a pass carries.
    info!(
        handover_budget,
        bound_reason = bound.reason,
        pending = pending.len(),
        pool_size = verdict.pool_size,
        decision = verdict.decision.as_str(),
        "change ledger: dispatch batch"
    );

    let rearm_limit = dispatch_rearm_limit();
    let mut counts = TickCounts {
        // Owed and delivered: the diff reached the line while the claim the row
        // makes is still not true of the tree. An annotation over the outcomes
        // counted below rather than one of them -- the row is still owed -- and
        // the only reading that separates "nobody finished it" from "it went in
        // and did not do what it said".
        delivered: pending
            .iter()
            .filter(|row| row.delivered_rev.is_some())
            .count(),
        ..TickCounts::default()
    };
    let mut readback = HandoverCensus::default();
    for row in &pending {
        // What the change's handovers left behind, read before anything is
        // decided about the row: this is a reading of the event stream, and it
        // is asked either way -- a pass that decided to hand nothing over still
        // read the same set, and a reading face that went blind on exactly the
        // passes it exists to describe would be the fault this whole loop is
        // written against.
        if let Some(backend) = events {
            match read_handover_products(backend, &row.identity).await {
                Ok(found) => readback.absorb(&found),
                Err(e) => {
                    readback.unreadable += 1;
                    warn!(
                        change = %row.identity,
                        error = %e,
                        "change ledger: a change's handovers could not be read back, so this \
                         pass cannot say what it produced"
                    );
                }
            }
        }
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
                if counts.submitted >= handover_budget {
                    // Owed, handable, and left for a later pass because this one
                    // has handed over its slice. Counted with the rows a paused
                    // pass withholds, because the fact about the row is the same
                    // in both -- owed, no task holds it, and this pass did not
                    // hand it over -- while what differs is the pass's reason,
                    // which is the verdict's to say and not the count's.
                    //
                    // Left, not skipped over: the loop runs to the end so the
                    // census of what the handovers left behind stays a reading of
                    // the whole set on every pass, which is what it says it is.
                    counts.held_back += 1;
                    continue;
                }
                if generation > 0 {
                    counts.rearmed += 1;
                }
                let goal = dispatch_goal(row);
                let task = dispatch_task(row, generation);
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
            // Owed, delivered by a commit, and still not true of the tree: the
            // gap between the diff going in and the claim it makes holding.
            delivered = counts.delivered,
            handovers_on_record = readback.events,
            handovers_with_product = readback.products,
            handovers_unreadable = readback.unreadable,
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
            held_back = counts.held_back,
            rearmed = counts.rearmed,
            already_handed_over = counts.already,
            already_dead_environment = counts.dead_environment,
            delivered = counts.delivered,
            failed = counts.failed,
            unmeasurable,
            handovers_on_record = readback.events,
            handovers_with_product = readback.products,
            handovers_unreadable = readback.unreadable,
            pool = verdict.reading.as_str(),
            not_handed_over = %held_back_summary(&rows),
            "change ledger: handed this pass's slice of the pending set to the goal path"
        );
    }
    record_dispatch_state(
        store,
        metrics,
        &verdict,
        pending.len(),
        unmeasurable,
        &counts,
        sources_unread,
        // `None`, not zero, when the log could not be read: a failed read is
        // not a pass that found nothing delivered.
        deliveries.map(|_| counts.delivered as u64),
    )
    .await;
    // The readback is published from here rather than from the row's own state:
    // it is a reading of the event stream and not a column of the account's
    // dispatch row, and giving the row columns it does not need would make the
    // account carry a second copy of something the stream already answers. The
    // two families name the same metric and are written from the same pass, so a
    // reader that selects by name still sees one tick.
    publish_handover_readback(metrics, &readback).await;

    // The write windows are read once per pass, beside the ledger's own
    // reading: the face that owes them has no other producer, and the pass is
    // the only loop that runs on this account's clock. A store with no window
    // table, and a read that failed, both publish nothing -- what the face must
    // never see is "no window is approved" standing in for "nobody looked".
    let windows = store.write_windows().await;
    if let Err(e) = &windows {
        warn!(
            error = %e,
            "change ledger: the write windows could not be read, so the writing face's reading \
             is absent this pass rather than zero"
        );
    }
    match write_window_reading_from(&windows, Utc::now()) {
        Some(reading) => publish_write_windows(metrics, &reading).await,
        None => debug!(
            "change ledger: this store answered no write windows, so no window reading was \
             published -- an absent cell, not a zero"
        ),
    }

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
///
/// `held_back` is the one count with two causes: a pass with no upstream that
/// can serve hands nothing over at all, and a pass that has already handed over
/// [`HANDOVERS_PER_PASS`] rows leaves the rest to a later one. The fact about
/// the row is the same either way -- owed, no task holds it, and this pass did
/// not hand it over -- and which of the two it was is the pass's verdict rather
/// than its count, since a paused pass submits nothing at all: a pass that
/// submitted and still has rows left is one whose slice was spent. Keeping both
/// in one cell is also what keeps the arithmetic above an equality. A cell added
/// to absorb what the bound left behind, without being subtracted from the same
/// sum, would put the reading a reader already takes off this row -- owed minus
/// everything accounted for -- off zero, and that line sitting at zero is what
/// catches a pass that lost track of rows.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TickCounts {
    submitted: usize,
    already: usize,
    dead_environment: usize,
    held_back: usize,
    rearmed: usize,
    failed: usize,
    /// Owed rows a commit on the line claims to have landed. An annotation over
    /// the outcomes above, not one of them: the row is still owed until its
    /// check measures true, and delivery is the separate fact that its diff
    /// reached the line.
    delivered: usize,
}

/// What one pass read back off the set it handed over.
///
/// Counted over the same rows the counts above are counted over, and in the same
/// pass, so the two readings are about one set: `events` is how many owed changes
/// still have their handovers in the stream, `products` how many of those left a
/// deliverable, and `unreadable` how many the pass could not read at all. The
/// third one is not a nicety -- without it a read that failed and a run that
/// produced nothing are the same two numbers, which is the confusion this whole
/// reading exists to end.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct HandoverCensus {
    events: usize,
    products: usize,
    unreadable: usize,
}

impl HandoverCensus {
    /// Count one change's handovers into the pass's totals.
    ///
    /// One row per change, so the entries add: a change with handovers and
    /// without a deliverable is counted in the first number only, and that is
    /// the difference between the two that a reader subtracts.
    fn absorb(&mut self, readback: &HandoverReadback) {
        if !readback.generations.is_empty() {
            self.events += 1;
        }
        if readback.product.is_some() {
            self.products += 1;
        }
    }
}

/// The names the per-pass counts are published under.
///
/// A closed set, so a reader counts cells rather than parsing names, and the
/// order here is the order they are written in. The values are the account's own
/// numbers, taken after they were computed and before they were written -- the
/// same ones, not a second computation of them.
///
/// This is one half of the metric's label set and [`HandoverReadout`] is the
/// other: these are what the pass did with the set it read, and those are what
/// the handovers had left behind when it read them.
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
    /// The size of the ledger's pending set as the ledger holds it, which is
    /// `pending` and `unmeasurable` together: the two cells the pass splits that
    /// set into by whether it could measure each row this tick. It is a cell of
    /// its own rather than something a reader adds up because the sum is only
    /// the ledger's number while both halves stay published -- and the face that
    /// owes the reading asks for the account's own count, not for a validation
    /// of the pass's arithmetic.
    LedgerPending,
    /// How many rows are owed at each band, from the whole owed set rather than
    /// from this pass's slice -- like [`DispatchReadout::LedgerPending`], an
    /// account-level count carried in the same family because it is read at the
    /// same tick about the same set.
    ///
    /// Five cells rather than one cell with a band label: the family's label set
    /// is the readout, and a second label whose value is which band the count is
    /// about would make the series identity depend on the reading's subject.
    ///
    /// These are the cells that make the band order visible. The pass hands out
    /// the highest band it holds, so a P0 that stays owed across passes reads as
    /// a number that is not falling while the rest of the set moves -- which is
    /// the only shape in which "the urgent work is not getting through" is
    /// sayable without watching individual rows.
    OwedP0,
    OwedP1,
    OwedP2,
    OwedP3,
    OwedUnstated,
    /// How many owed rows the account holds in each non-terminal state, from
    /// its own census rather than from this pass's slice.
    ///
    /// The four states are here because the pass's own counts cannot answer for
    /// them. `pending` and `unmeasurable` together are the pending-state rows
    /// this pass read; a row the account holds as `undecided`, `conflict` or
    /// `partial` is still owed work, and it appears in none of the pass's
    /// columns. Without these cells a pass whose owed set had moved entirely
    /// into `undecided` -- every check unsettleable, which is what a tree that
    /// lost a directory looks like -- would publish `change_ledger_pending` at
    /// zero while two thousand rows still asked for something: the one answer a
    /// reader acts on, and it would be wrong.
    ///
    /// The two terminal states are left out because they are the definition of
    /// not owed; a reader asking for those reads the account, where they are the
    /// complement of these four.
    StateConflict,
    StatePending,
    StatePartial,
    StateUndecided,
    /// The demand side of the same loop: how many of the declarations the
    /// intake holds have been answered by a landing, and how many are still
    /// owed.
    ///
    /// Every other cell here answers "what does the account still owe". These
    /// two are the only place the platform says whether the work it was asked
    /// for was actually done, and they are read from the intake joined to the
    /// account rather than from this pass's slice: a demand is answered by a
    /// measurement that may have happened many passes ago, on a row this pass
    /// never looked at.
    ///
    /// They are cells of this metric because they are the closure of the same
    /// set: `state_pending` falling while `demand_satisfied` does not move is
    /// the shape of a ledger that is emptying itself by retiring rows, which is
    /// the reading these two exist to make visible.
    DemandSatisfied,
    DemandOwed,
    /// What the account is holding in front of the rest of itself: how many
    /// changes a commit delivered while the account still owes them, and how
    /// many other rows are measured at a revision one of them delivered.
    ///
    /// The ordering half of the account. A band says what a document asked for;
    /// this says what the line is actually carrying, which is the one quantity
    /// that can move a change to the front without anybody stating that it is
    /// urgent. `held_landings` alone cannot say whether one change is holding
    /// one row or the whole owed set, and `blocking_rows` alone cannot say how
    /// much is behind them, so the pair is the reading.
    BlockingRows,
    HeldLandings,
    /// Rows whose documents name more than one face. The folded `block` column
    /// answers `none` both for "nobody named a face" and for "the documents
    /// disagree", so this cell is what keeps the second from reading as the
    /// first.
    BlockConflicts,
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
        DispatchReadout::LedgerPending,
        DispatchReadout::OwedP0,
        DispatchReadout::OwedP1,
        DispatchReadout::OwedP2,
        DispatchReadout::OwedP3,
        DispatchReadout::OwedUnstated,
        DispatchReadout::StateConflict,
        DispatchReadout::StatePending,
        DispatchReadout::StatePartial,
        DispatchReadout::StateUndecided,
        DispatchReadout::DemandSatisfied,
        DispatchReadout::DemandOwed,
        DispatchReadout::BlockingRows,
        DispatchReadout::HeldLandings,
        DispatchReadout::BlockConflicts,
    ];

    /// The band a count cell is about, for the five that are about one.
    ///
    /// `None` for every other cell, which is how the publish loop tells the two
    /// kinds of value apart without a second list.
    fn band(self) -> Option<ChangePriority> {
        match self {
            DispatchReadout::OwedP0 => Some(ChangePriority::P0),
            DispatchReadout::OwedP1 => Some(ChangePriority::P1),
            DispatchReadout::OwedP2 => Some(ChangePriority::P2),
            DispatchReadout::OwedP3 => Some(ChangePriority::P3),
            DispatchReadout::OwedUnstated => Some(ChangePriority::Unstated),
            _ => None,
        }
    }

    /// The state a census cell is about, for the four that are about one.
    ///
    /// Separate from [`Self::band`] because the two axes cross: the bands
    /// partition the owed set and the states partition it again, and the point
    /// of publishing both is that a reader can see the two partitions are of the
    /// same set.
    fn state(self) -> Option<ChangeState> {
        match self {
            DispatchReadout::StateConflict => Some(ChangeState::Conflict),
            DispatchReadout::StatePending => Some(ChangeState::Pending),
            DispatchReadout::StatePartial => Some(ChangeState::Partial),
            DispatchReadout::StateUndecided => Some(ChangeState::Undecided),
            _ => None,
        }
    }

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
            Self::LedgerPending => "change_ledger_pending",
            Self::OwedP0 => "owed_p0",
            Self::OwedP1 => "owed_p1",
            Self::OwedP2 => "owed_p2",
            Self::OwedP3 => "owed_p3",
            Self::OwedUnstated => "owed_unstated",
            Self::StateConflict => "state_conflict",
            Self::StatePending => "state_pending",
            Self::StatePartial => "state_partial",
            Self::StateUndecided => "state_undecided",
            Self::DemandSatisfied => "demand_satisfied",
            Self::DemandOwed => "demand_owed",
            Self::BlockingRows => "blocking_rows",
            Self::HeldLandings => "held_landings",
            Self::BlockConflicts => "block_conflicts",
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
async fn publish_dispatch_rows(
    metrics: Option<&Arc<dyn MetricsBackend>>,
    state: &DispatchState,
    owed: Option<&[(ChangePriority, usize)]>,
    census: Option<&[(ChangeState, u64)]>,
    demand: Option<DemandClosure>,
    blocking: Option<BlockingSurface>,
) {
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
            DispatchReadout::LedgerPending => state.pending + state.unmeasurable,
            // Both cells or neither. A store that cannot answer publishes
            // nothing for either, because a satisfied count without the owed
            // one cannot be read: the pair is the reading, and half of it would
            // let a dashboard show demands being answered while the unpaid
            // remainder it is to be judged against is missing.
            DispatchReadout::DemandSatisfied | DispatchReadout::DemandOwed => {
                let Some(closure) = demand else {
                    continue;
                };
                if matches!(readout, DispatchReadout::DemandSatisfied) {
                    closure.satisfied
                } else {
                    closure.owed
                }
            }
            // All three or none, for the reason the demand pair gives: a
            // "held landings" count with no "holding rows" beside it cannot be
            // read as anything, and a conflict count taken from a store that
            // could not answer would say the documents agree.
            DispatchReadout::BlockingRows
            | DispatchReadout::HeldLandings
            | DispatchReadout::BlockConflicts => {
                let Some(surface) = blocking else {
                    continue;
                };
                match readout {
                    DispatchReadout::BlockingRows => surface.holders,
                    DispatchReadout::HeldLandings => surface.held_landings,
                    _ => surface.block_conflicts,
                }
            }
            // A band cell is written only when this pass managed to count the
            // owed set. Skipped rather than zeroed: a zero says the urgent work
            // is done, which is the one wrong answer, and a cell left at its
            // last value is at least the truth as of a known earlier tick --
            // the store stamps every write, so the staleness reads.
            _ => {
                // The census cells skip for the same reason the band cells do:
                // a zero written for a count that failed is the account saying
                // it owes nothing in a state nobody managed to read.
                if let Some(wanted) = readout.state() {
                    let Some(count) = census.and_then(|counts| {
                        counts
                            .iter()
                            .find(|(found, _)| *found == wanted)
                            .map(|(_, rows)| *rows)
                    }) else {
                        continue;
                    };
                    count
                } else {
                    let Some(band) = readout.band() else {
                        continue;
                    };
                    let Some(count) = owed.and_then(|counts| {
                        counts
                            .iter()
                            .find(|(found, _)| *found == band)
                            .map(|(_, rows)| *rows)
                    }) else {
                        continue;
                    };
                    u64::try_from(count).unwrap_or(u64::MAX)
                }
            }
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

/// The names what the pass read back off the handovers is published under.
///
/// A closed set of its own, on the same metric and the same `readout` label as
/// [`DispatchReadout`]: both answer questions about the one set the pass read, and
/// a reader selecting by that name sees one tick rather than two. They are two
/// enumerations rather than one because one of them is a column of the account's
/// dispatch row and this one is not -- a readback that went into that struct
/// would need columns for numbers the event stream already answers, and a column
/// is a second original. Together the two are the metric's whole label set, which
/// is what the reading face's own test asserts.
///
/// The three cells are one reading and have to be taken together: a change whose
/// handovers left nothing and a change whose handovers could not be read are both
/// absent from the second cell, and a reader that took the second cell alone
/// would call a failed read a change that produced nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandoverReadout {
    Events,
    Products,
    Unreadable,
}

impl HandoverReadout {
    const ALL: &'static [HandoverReadout] = &[
        HandoverReadout::Events,
        HandoverReadout::Products,
        HandoverReadout::Unreadable,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Events => "handover_events",
            Self::Products => "handover_products",
            Self::Unreadable => "handover_unreadable",
        }
    }
}

/// Publish what the pass read back off the handovers, one cell per readout.
///
/// Every cell on every pass, like the counts beside it and for the same reason:
/// the store renders a gauge from the newest row of every label set it has ever
/// held and releases none, so a cell written only when it has a value would go on
/// being served at its last one. A failure to publish is raised and not fatal, on
/// the same grounds as the rest of the reading face: the pass happened, and the
/// reading is its view rather than its record.
async fn publish_handover_readback(
    metrics: Option<&Arc<dyn MetricsBackend>>,
    readback: &HandoverCensus,
) {
    let Some(metrics) = metrics else {
        return;
    };
    for readout in HandoverReadout::ALL {
        let value = match readout {
            HandoverReadout::Events => readback.events,
            HandoverReadout::Products => readback.products,
            HandoverReadout::Unreadable => readback.unreadable,
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
                "change ledger: what this pass read back off the handovers could not be \
                 published, so the reading face is missing a cell this pass held"
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

/// What the account's write windows say at one instant.
///
/// Five counts, and no one of them is derivable from the others: `approved` is
/// the windows that stand, `overlap` is the scopes with more than one live
/// approved writer, `inconsistent` is the windows that were approved and then
/// withdrawn, `self_approved` is the open windows nobody but their writer
/// approved, and `registered` is every row the table holds whichever state it is
/// in.
///
/// `registered` is here so the others cannot lie by being zero. A table with no
/// writer publishes `approved = 0` every pass, and a zero on that cell is read as
/// "measured: no window is in force" -- the same words a system where nobody
/// records windows at all would produce. Publishing the row count beside it makes
/// the pair say which of the two is true without the reader needing the help text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct WriteWindowReading {
    approved: u64,
    overlap: u64,
    inconsistent: u64,
    self_approved: u64,
    registered: u64,
}

/// Reduce the account's write windows to those five counts.
///
/// Pure, and the only implementation of the rule: the store hands back every row
/// it holds, and which of them are approvals is decided here, so a second reader
/// cannot answer it differently. The counting is over the rows and not over a
/// query, because the revoked row and the self-approved row are the rows the
/// reading exists for, and a query filtered by the approval rule would make both
/// of them unreachable.
fn write_window_reading(windows: &[WriteWindow], now: DateTime<Utc>) -> WriteWindowReading {
    let mut reading = WriteWindowReading {
        registered: windows.len() as u64,
        ..WriteWindowReading::default()
    };
    // One live approved writer per scope, so a scope two accounts are writing
    // under is counted once however many rows either of them left behind.
    let mut writers_by_scope: HashMap<&str, HashSet<&str>> = HashMap::new();
    for window in windows {
        if window.inconsistent() {
            reading.inconsistent += 1;
        }
        if window.self_approved(now) {
            reading.self_approved += 1;
        }
        if !window.approved(now) {
            continue;
        }
        reading.approved += 1;
        writers_by_scope
            .entry(window.path_scope.as_str())
            .or_default()
            .insert(window.writer.as_str());
    }
    reading.overlap = writers_by_scope
        .values()
        .filter(|writers| writers.len() > 1)
        .count() as u64;
    reading
}

/// What a pass may publish from the store's answer.
///
/// `Ok(Some(rows))` is a measurement, even when the rows are empty: a table that
/// can be read and holds nothing says no window is approved, and that is a zero
/// the face may show. `Ok(None)` and a failed read are both "nothing was
/// measured", and the two are folded here rather than at the publisher because
/// the publisher's only question is whether there is a reading -- the difference
/// between them is a log line, and it is written where the answer arrives.
fn write_window_reading_from(
    answer: &SFResult<Option<Vec<WriteWindow>>>,
    now: DateTime<Utc>,
) -> Option<WriteWindowReading> {
    match answer {
        Ok(Some(windows)) => Some(write_window_reading(windows, now)),
        Ok(None) | Err(_) => None,
    }
}

/// The names the write-window counts are published under.
///
/// A closed set for the same reason the dispatch counts are one: the face that
/// owes this reading takes a name out of the roster and asks the store for it,
/// and a cell written under a name the roster does not hold would be a series
/// no reader selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteWindowReadout {
    Approved,
    Overlap,
    Inconsistent,
    SelfApproved,
    Registered,
}

impl WriteWindowReadout {
    const ALL: &'static [WriteWindowReadout] = &[
        WriteWindowReadout::Approved,
        WriteWindowReadout::Overlap,
        WriteWindowReadout::Inconsistent,
        WriteWindowReadout::SelfApproved,
        WriteWindowReadout::Registered,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "write_window_approved",
            Self::Overlap => "write_window_overlap",
            Self::Inconsistent => "write_window_inconsistent",
            Self::SelfApproved => "write_window_self_approved",
            Self::Registered => "write_window_registered",
        }
    }

    fn count(self, reading: &WriteWindowReading) -> u64 {
        match self {
            Self::Approved => reading.approved,
            Self::Overlap => reading.overlap,
            Self::Inconsistent => reading.inconsistent,
            Self::SelfApproved => reading.self_approved,
            Self::Registered => reading.registered,
        }
    }
}

/// Publish one pass's write-window counts, every cell on every pass.
///
/// Every cell, so a window that lapsed stops being counted rather than standing
/// at the last value it had while it was open -- the same reason the dispatch
/// sheet enumerates its cells. A failure to publish is raised and not fatal:
/// the pass already happened, and the face losing one tick is smaller than the
/// pass stopping to keep it.
async fn publish_write_windows(
    metrics: Option<&Arc<dyn MetricsBackend>>,
    reading: &WriteWindowReading,
) {
    let Some(metrics) = metrics else {
        return;
    };
    for readout in WriteWindowReadout::ALL {
        let mut labels = HashMap::new();
        labels.insert("readout".to_string(), readout.as_str().to_string());
        if let Err(e) = metrics
            .record_gauge(
                metric_names::CHANGE_WRITE_WINDOWS,
                readout.count(reading) as f64,
                labels,
            )
            .await
        {
            warn!(
                error = %e,
                readout = readout.as_str(),
                "change ledger: this pass's write-window count could not be published, so the \
                 reading face is missing a cell this pass held"
            );
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
///
/// One parameter per column written, for the same reason the pass itself takes
/// one per collaborator: a struct here would restate the row's shape in a type
/// that exists only to hold it, and the next column would then have to be added
/// in two places.
#[allow(clippy::too_many_arguments)]
async fn record_dispatch_state(
    store: &dyn ChangeLedgerStore,
    metrics: Option<&Arc<dyn MetricsBackend>>,
    verdict: &PoolVerdict,
    pending: usize,
    unmeasurable: usize,
    counts: &TickCounts,
    sources_unread: &[&'static str],
    delivered: Option<u64>,
) {
    let state = DispatchState {
        // Written for every pass, empty when every intake answered: `None` is
        // reserved for rows that predate the column, and a pass that reused it
        // would make "we read everything" and "this row never said" one value.
        sources_unread: Some(sources_unread.join(",")),
        decision: verdict.decision,
        pool: verdict.reading,
        pending: pending as u64,
        submitted: counts.submitted as u64,
        already: counts.already as u64,
        failed: counts.failed as u64,
        dead_environment: counts.dead_environment as u64,
        held_back: counts.held_back as u64,
        rearmed: counts.rearmed as u64,
        delivered,
        unmeasurable: unmeasurable as u64,
        resume_at_unix: verdict.resume_at_unix,
        evidenced_recovery_unix: verdict.evidenced_recovery_unix,
        quota_window_secs: verdict.quota_window_secs,
        window_pace: verdict.window_pace,
    };
    if let Err(e) = store.record_dispatch_state(&state).await {
        warn!(
            error = %e,
            decision = state.decision.as_str(),
            "change ledger: what this pass decided could not be recorded, so the decision is \
             only in this line"
        );
    }
    // The band counts are the account's own, read after the pass acted rather
    // than derived from the slice it handed out. They answer "what is still owed
    // at each band", which is the cell a reader watches to see whether the
    // urgent work is draining; the slice answers a different question and would
    // make a band that this pass happened to fill out look empty.
    //
    // A failed count publishes nothing, and says so: the cells then hold their
    // last values, and a reader that wants to know how old they are has the
    // stamp every write carries.
    let owed = match store.owed_by_band().await {
        Ok(counts) => Some(counts),
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the owed set could not be counted by band, so this pass \
                 publishes no band cell"
            );
            None
        }
    };
    // The account's census, read here rather than reused from the read that
    // sized the outstanding batch: that one was taken before the pass acted, and
    // these cells answer what the account holds as of this pass. A failed read
    // publishes nothing for the same reason a failed band count does.
    let census = match store.census().await {
        Ok(counts) => Some(counts),
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the account's census could not be read, so this pass publishes \
                 no state cell"
            );
            None
        }
    };
    // Published from the same struct the row was built from, and after it: the
    // account is the record and the reading face is a view of it, so the two
    // cannot say different things about one pass.
    // The demand side, read here for the same reason the census is: it is a
    // property of the account as of this pass, not of the slice this pass handed
    // out. `None` covers both "this store cannot answer" and "the read failed",
    // and both publish nothing rather than a zero -- a demand count of zero and
    // a demand count nobody took read identically on the panel, and it is the
    // second that a failing join produces.
    let demand = match store.demand_closure().await {
        Ok(closure) => closure,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the demand closure could not be read, so this pass publishes \
                 no demand cell"
            );
            None
        }
    };
    // What the account is holding in front of itself, read beside the demand
    // side and for the same reason: it is a property of the whole account as of
    // this pass. A failed read publishes nothing rather than zeros -- "nothing
    // is blocked" and "nobody could look" are different answers, and the first
    // is the one that stops anyone from looking twice.
    let blocking = match store.blocking_surface().await {
        Ok(surface) => surface,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the blocking surface could not be read, so this pass publishes \
                 no blocking cell"
            );
            None
        }
    };
    publish_dispatch_rows(
        metrics,
        &state,
        owed.as_deref(),
        census.as_deref(),
        demand,
        blocking,
    )
    .await;
    // The same account, grouped by the face its documents named. Read here and
    // not from the owed slice for the reason the census is not: a face is a
    // property of everything the account holds, not of the rows one pass
    // happened to look at.
    let blocks = match store.block_census().await {
        Ok(counts) => counts,
        Err(e) => {
            warn!(
                error = %e,
                "change ledger: the per-face census could not be read, so this pass publishes \
                 no face cell"
            );
            None
        }
    };
    publish_block_census(metrics, blocks.as_deref()).await;
    publish_dispatch_verdict(metrics, &state).await;
}

/// Publish the account grouped by face: landed, owed, and how long the face's
/// longest-waiting owed row has gone without a measurement.
///
/// Every face on every pass, including the ones that owe nothing, because a
/// face that stopped being written would go on being served at its last value
/// and a face that finished would be indistinguishable from one that is still
/// being asked about. A store that cannot group publishes nothing at all rather
/// than a set of zeros: zeros would say every face is empty, which is the one
/// answer that stops anybody from asking again.
async fn publish_block_census(
    metrics: Option<&Arc<dyn MetricsBackend>>,
    census: Option<&[BlockCensus]>,
) {
    let Some(metrics) = metrics else {
        return;
    };
    let Some(census) = census else {
        return;
    };
    for face in census {
        let mut cells = vec![
            ("landed", face.landed as f64),
            ("owed", face.owed as f64),
            ("retired", face.retired as f64),
        ];
        // An absence is not a zero: no age says every owed row in this face has
        // been measured, which is a different fact from none of them having
        // waited. The cell is skipped rather than written as zero, and the stamp
        // on the last write is what tells a reader it is old.
        if let Some(secs) = face.oldest_unmeasured_secs {
            cells.push(("oldest_unmeasured_seconds", secs as f64));
        }
        for (reading, value) in cells {
            if let Err(e) = publish_face_reading(metrics, face.block, reading, value).await {
                warn!(
                    error = %e,
                    block = face.block.as_str(),
                    reading,
                    "change ledger: a face's count could not be published"
                );
            }
        }
    }
}

async fn publish_face_reading(
    metrics: &Arc<dyn MetricsBackend>,
    block: ChangeBlock,
    reading: &str,
    value: f64,
) -> SFResult<()> {
    let mut labels = HashMap::new();
    labels.insert("block".to_string(), block.as_str().to_string());
    labels.insert("reading".to_string(), reading.to_string());
    metrics
        .record_gauge(metric_names::CHANGE_BLOCK_CENSUS, value, labels)
        .await
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
            pool_size: 0,
            window_pace: None,
            measurable_pool_size: 0,
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
        pool_size: status.pool_size,
        // `measured_upstreams == 0` is "not paced" -- the field's own meaning --
        // and writing that pace down would make "no window was measured" and
        // "a window measured to allow nothing" the same three zeros.
        window_pace: (status.pace.measured_upstreams > 0).then_some(status.pace),
        // Members that answer `entitlement` can never be part of a measurement,
        // so they leave the denominator rather than holding it below the pool
        // size forever.
        measurable_pool_size: crate::ready_queue::measurable_pool_size(status),
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
    /// How many upstreams the pool is configured with, from the same snapshot.
    ///
    /// Carried so the batch bound can tell a pool-wide measurement from one
    /// that covered a subset: the pace sums the upstreams that reported a
    /// window, and a batch sized on that sum while other members went
    /// unmeasured would be sized by one ruler and refused by another.
    pool_size: usize,
    /// The window pace the same snapshot carried, when it carried a measurement.
    ///
    /// It rides the verdict rather than being read a second time from the
    /// snapshot: the batch bound and the recorded row then rest on one read of
    /// one payload, so a pass cannot size its batch from one window and write
    /// down another.
    window_pace: Option<LlmWindowPace>,
    /// How many members the measurement has to cover, from the same snapshot.
    ///
    /// Not `pool_size`: a member whose last refusal was `entitlement` will
    /// refuse the same way until someone changes the configuration, so a pool
    /// that contains one can never have a full measurement in those terms and
    /// the bound would fall back to the ceiling forever -- which is the
    /// unbounded dispatch this gate exists to stop.
    measurable_pool_size: usize,
}

/// How many rows this pass may hand over, given what the pool said about the
/// window it is spending.
///
/// The ceiling is the tick's own bound -- a maintenance tick, not a queue drain
/// -- and it stays as the answer whenever no window was measured, because a bound
/// this pass cannot derive is not a bound it may invent. What the pace adds is the
/// other direction: a window whose remaining allowance fits fewer than the ceiling
/// lowers the batch to what the window can actually pay for. Handing over more
/// than that is not extra throughput, it is a batch the same window refuses
/// request by request: a weekly window is spent in about twenty-six hours of
/// dispatch, and after it resets the allowance comes back along a time ramp, so
/// a batch sized to the ceiling meets a window that can pay for a fraction of
/// it. The refusal is not caused by the batch -- a smaller batch meets the same
/// wall -- but the batch is what turns one refusal into one per row.
///
/// `reserve_tokens` is one whole job, measured from finished jobs, and it is the
/// right divisor rather than an average call: the pace's own `blocked` rule
/// compares the same two numbers, so a batch of one is admitted exactly when the
/// gateway would not refuse it. `measured_upstreams == 0` or a zero reserve is
/// "nothing measured", which is the ceiling, never zero -- zero here would stop
/// dispatch for a missing measurement, which is a driver traded for a reading.
/// What this pass may hand over, and which branch produced that number.
///
/// The reason travels with the number because every fallback lands on the same
/// ceiling: "the window pays for more than one pass may carry" and "no bound
/// could be derived" are the same integer and opposite facts, and the second is
/// what happens on a degraded pool -- only the upstreams that still report a
/// window are measured, which is exactly the state this gate exists for. A pass
/// that handed over unbounded must not read like a healthy one, so the pass
/// logs this pair rather than only the arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchBound {
    budget: usize,
    reason: &'static str,
}

fn handovers_for_this_pass(verdict: &PoolVerdict, ceiling: usize) -> BatchBound {
    let Some(pace) = verdict.window_pace else {
        return BatchBound {
            budget: ceiling,
            reason: "no_measurement",
        };
    };
    // The same derivation the ready-queue publisher uses, so the two dispatch
    // paths cannot size their batches by different rules. It also carries the
    // guard this file used to lack: a pace summed over a subset of the pool is
    // not a bound for the pool.
    let Some(fits) =
        crate::ready_queue::jobs_the_window_can_pay_for(&pace, verdict.measurable_pool_size)
    else {
        // `window_pace` is only set when at least one upstream was measured, so
        // the remaining ways to get no bound are a missing job cost and a
        // measurement that covers fewer upstreams than the pool has. They are
        // different operational facts: the first says the ledger of finished
        // jobs is too thin to price one, the second says the pool is degraded.
        let reason = if pace.reserve_tokens == 0 {
            "no_reserve"
        } else {
            "partial_pool"
        };
        return BatchBound {
            budget: ceiling,
            reason,
        };
    };
    // The cast cannot lose a real batch: `ceiling` is single digits in the
    // hundreds and `fits` only matters below it, so saturating at `ceiling`
    // first keeps a huge remaining allowance from wrapping anything.
    if fits >= ceiling as u64 {
        BatchBound {
            budget: ceiling,
            reason: "ceiling",
        }
    } else {
        BatchBound {
            budget: fits as usize,
            reason: "window",
        }
    }
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
            delivered_rev: None,
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

    /// A dispatched change declares a rank below the one a task lands on by
    /// default, and the rest of the row is unchanged by that.
    ///
    /// The rank is what admission reads, so this is the whole difference between
    /// a customer's goal waiting behind a thousand housekeeping rows and not: the
    /// loop states its own rank instead of leaving it at the default the graph
    /// fills in. Asserting the payload too keeps the extraction honest -- moving
    /// the construction into its own function must not quietly drop the marker
    /// the change-generation path is routed on.
    #[test]
    fn a_dispatched_change_declares_a_rank_below_the_default() {
        let task = dispatch_task(&row(ChangeState::Pending, Some(1_760_000_000)), 0);
        let default_rank =
            Task::new("t", TaskType::Custom("x".into()), serde_json::json!({})).priority;
        assert!(
            task.priority < default_rank,
            "a dispatched change must rank below the default ({default_rank}), got {}",
            task.priority
        );
        assert!(task.id.starts_with(DISPATCH_TASK_PREFIX));
        assert_eq!(task.input["task_kind"], DISPATCH_TASK_KIND);
        assert_eq!(task.input["evolution_mode"], "generate_change");
    }

    /// A stated band lifts the handover by exactly the band's own rank, so the
    /// order the pass hands rows over in survives into the graph that decides
    /// what runs next.
    ///
    /// Asserted as a ladder rather than as four literals: what has to hold is
    /// that the rank rises with the band, that the least urgent stated band is
    /// still above every unstated row, and that a row nobody prioritised keeps
    /// the floor the previous rule established. Pinning the numbers would pass
    /// for a mapping that is ordered and wrong about the floor.
    #[test]
    fn a_stated_band_lifts_the_handover_the_way_it_orders_the_read() {
        let with_band = |band: ChangePriority| {
            let mut handed = row(ChangeState::Pending, Some(1_760_000_000));
            handed.sources = vec![ChangeSource {
                doc: "2026-10-21_doc.md".into(),
                verdict: None,
                priority: band,
                block: ChangeBlock::None,
            }];
            dispatch_task(&handed, 0).priority
        };
        let ladder: Vec<i32> = [
            ChangePriority::Unstated,
            ChangePriority::P3,
            ChangePriority::P2,
            ChangePriority::P1,
            ChangePriority::P0,
        ]
        .into_iter()
        .map(with_band)
        .collect();
        assert!(
            ladder.windows(2).all(|pair| pair[0] < pair[1]),
            "the rank must rise with the band, got {ladder:?}"
        );
        let default_rank =
            Task::new("t", TaskType::Custom("x".into()), serde_json::json!({})).priority;
        assert_eq!(ladder[0], DISPATCH_TASK_PRIORITY);
        assert!(
            ladder[0] < default_rank,
            "a row nobody prioritised keeps the floor below the default ({default_rank})"
        );
    }

    /// A change holding the line is filed at P0 whatever its documents said.
    ///
    /// The rank is the account's own measured answer, not a document's claim:
    /// a commit delivered this change and the tree has not confirmed it, so
    /// everything else measured at that revision stands on it. The pass takes
    /// it first, and filing the handover at the band its documents stated
    /// would undo that ordering one step later -- which is exactly the fault
    /// the band lift was added to fix.
    #[test]
    fn a_change_holding_the_line_is_filed_at_p0_whatever_its_documents_said() {
        let mut owed = row(ChangeState::Pending, Some(1_760_000_000));
        owed.sources = vec![ChangeSource {
            doc: "2026-10-21_doc.md".into(),
            verdict: None,
            priority: ChangePriority::Unstated,
            block: ChangeBlock::None,
        }];
        let quiet = dispatch_task(&owed, 0).priority;

        owed.delivered_rev = Some("abc1234".into());
        let holding = dispatch_task(&owed, 0).priority;

        assert_eq!(
            holding,
            DISPATCH_TASK_PRIORITY + i32::from(ChangePriority::P0.rank()),
            "a delivered but unconfirmed change is filed at the top band"
        );
        assert!(
            holding > quiet,
            "holding the line outranks every stated band"
        );

        // A document that states P0 and a row that holds the line are the same
        // rank: the account adds no sixth level for a consumer to learn.
        owed.state = ChangeState::Landed;
        owed.delivered_rev = Some("abc1234".into());
        let landed = dispatch_task(&owed, 0).priority;
        assert_eq!(landed, quiet, "a landed change holds nothing");
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
            where_red: None,
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

    /// A principle ledger row as the gates left it: refused, with the count, the
    /// instant of the last refusal and the gate that did the refusing.
    ///
    /// Built on `principle_row` so that the two readings' fixtures differ in
    /// exactly what the readings differ in -- a fixture assembled from scratch
    /// could drift in a column neither intake reads and hide that.
    fn refused_principle_row(
        slug: &str,
        check_kind: Option<PrincipleCheckKind>,
        lead: Option<&str>,
        host_gate: Option<&str>,
        refusals: i64,
        last_refused_at: Option<i64>,
    ) -> PrincipleRow {
        PrincipleRow {
            host_gate: host_gate.map(str::to_string),
            refusals,
            last_refused_at: last_refused_at.map(|at| Utc.timestamp_opt(at, 0).unwrap()),
            ..principle_row(slug, check_kind, lead)
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
        let (rows, unaimed) = principle_debt_readings(&[principle_row(
            "feedback_x",
            Some(PrincipleCheckKind::Tree),
            Some("a reading must be quoted with its construction"),
        )]);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert!(unaimed.is_empty(), "{}", unaimed.summary());
        assert_eq!(rows[0].identity.kind, GREP_KIND);
        assert_eq!(rows[0].identity.scope, PRINCIPLE_GATE_SCOPE);
        assert_eq!(rows[0].identity.literal, "principle: feedback_x");
        assert_eq!(rows[0].source.doc, PRINCIPLE_LEDGER_SOURCE);
        assert_eq!(rows[0].source.verdict, None);
        assert_eq!(rows[0].source.priority, ChangePriority::Unstated);
        // A rule nothing has refused carries no assertion: the account holds a
        // count, and a zero in it is not a statement that the rule was kept.
        assert_eq!(rows[0].assertion, None);
        // The goal is a short title and then the details. The first line is what
        // the landing header shows, so it has to name the rule in few words; the
        // details name the lane the tier puts the check in and the rule's own
        // sentence, because a goal that named only the slug would tell a
        // generator where to look and not what is wanted there.
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Gate: x\n\nGive the principle `feedback_x` the gate it is owed: a check answering \
                 from this tree at one revision, the way the platform's own tests answer, declared \
                 in the header of the test that carries it. The rule itself: a reading must be \
                 quoted with its construction"
            )
        );
    }

    /// The goal's first line is the landing header, so it is written to fit
    /// there and to tell sibling rules apart.
    ///
    /// The two slugs are from the corpus and share their first 43 characters.
    /// Under a title budget of the goal's own the tails still differ; the
    /// collision this replaces came from the landing layer, where the header's
    /// template and the shared category prefix left too little room for a tail.
    #[test]
    fn a_principle_goal_opens_with_a_title_that_fits_and_differs() {
        let (rows, _) = principle_debt_readings(&[
            principle_row(
                "feedback_an_absence_claim_needs_the_reader_s_own_root",
                Some(PrincipleCheckKind::Tree),
                None,
            ),
            principle_row(
                "feedback_an_absence_claim_needs_the_reader_that_could_answer_it",
                Some(PrincipleCheckKind::Tree),
                None,
            ),
        ]);
        assert_eq!(rows.len(), 2, "{rows:?}");
        let titles: Vec<&str> = rows
            .iter()
            .map(|row| {
                row.title
                    .as_deref()
                    .expect("a row carries its goal")
                    .lines()
                    .next()
                    .unwrap_or_default()
            })
            .collect();

        for (title, row) in titles.iter().zip(&rows) {
            assert!(title.chars().count() <= GOAL_TITLE_CHARS, "{title}");
            let visible = title
                .strip_prefix("Gate: ")
                .expect("the title names the work")
                .trim_end_matches('…');
            // The row is the account's reading, not the ledger row it was read
            // off; its identity carries the slug the goal was derived from.
            let slug = row
                .identity
                .literal
                .strip_prefix(PRINCIPLE_DECLARATION_PREFIX)
                .expect("the identity literal is the declaration");
            let phrase = slug_phrase(slug);
            assert!(phrase.starts_with(visible), "severed word: {title}");
            let next = phrase[visible.len()..].chars().next();
            assert!(
                next.is_none_or(is_title_boundary),
                "the cut lands on a word boundary, got {next:?}: {title}"
            );
        }
        assert_ne!(
            titles[0], titles[1],
            "sibling rules have to be distinguishable from the first line"
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

    /// A tier a person holds is not work; a tier nobody declared still is.
    ///
    /// The first is the class the account itself defines as the rules that cannot
    /// be made into a gate: its look writes no state for a `judgement` row, so
    /// such a row stays in its unpaid set forever, and reading it here as work
    /// would mint a task no gate could close. It is counted rather than dropped,
    /// because how many rules no machine may ever check is a reading in its own
    /// right.
    ///
    /// The second is a rule that exists only as a memory. It enters, and the goal
    /// says what is missing about it -- the tier, which is what names the lane --
    /// instead of naming a lane this path would have had to invent. The criterion
    /// does not depend on the tier and so is the same either way: what lands the
    /// row is the tree carrying a header that declares the gate.
    #[test]
    fn a_principle_a_person_holds_is_not_work_and_one_with_no_tier_is() {
        let (rows, unaimed) = principle_debt_readings(&[
            principle_row(
                "feedback_held",
                Some(PrincipleCheckKind::Judgement),
                Some("a person's"),
            ),
            principle_row("feedback_untiered", None, Some("nobody said")),
            principle_row("feedback_owed", Some(PrincipleCheckKind::Live), None),
        ]);
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0].identity.literal, "principle: feedback_untiered");
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Gate: untiered\n\nGive the principle `feedback_untiered` the gate it is owed: a \
                 check the tree can be judged by, declared in the header of the test that carries \
                 it. The rule itself: nobody said"
            )
        );
        assert_eq!(rows[1].identity.literal, "principle: feedback_owed");
        // A row that arrived with no sentence still gets a goal: the rule's words
        // are carried when the reading has them, and their absence is not a reason
        // to hand over a bare slug. The title comes from the slug either way, so
        // the first line survives the landing header for a row the reading only
        // named.
        assert_eq!(
            rows[1].title.as_deref(),
            Some(
                "Gate: owed\n\nGive the principle `feedback_owed` the gate it is owed: a check \
                 answering from the running deployment's own readings, declared in the header of \
                 the test that carries it."
            )
        );
        assert_eq!(
            unaimed.summary(),
            "a-person-holds-this-by-its-own-declaration=1"
        );
    }

    /// The account's two faces are one subject, and the violation rides on the
    /// row the ungated face would have minted anyway.
    ///
    /// A rule can be enforced and refused at once -- that is a gate working -- and
    /// such a row is in the refused face and not in the unpaid one. Read as two
    /// intakes it would be written twice under one identity, and the second write
    /// would overwrite the first one's goal on every tick; read as one, the words
    /// do not depend on which face a slug arrived by.
    #[test]
    fn a_refused_principle_carries_the_violation_on_the_row_it_already_had() {
        let (rows, unaimed) = principle_debt_readings(&[
            refused_principle_row(
                "feedback_refused",
                Some(PrincipleCheckKind::Shape),
                Some("only stage your own paths"),
                Some("NARROW_ADD_RE"),
                3,
                Some(1_762_000_000),
            ),
            // Refused, and counted rather than aimed at, because the tier says no
            // machine may judge it: the count is the reading, and it is counted
            // apart from the rows this intake can aim.
            refused_principle_row(
                "feedback_refused_held",
                Some(PrincipleCheckKind::Judgement),
                Some("a person's"),
                Some("NARROW_ADD_RE"),
                1,
                Some(1_762_000_000),
            ),
        ]);
        assert_eq!(rows.len(), 1, "{rows:?}");
        // Refused is not the same as aimable: the second fixture is in the
        // account's refused face and is still counted rather than read as work,
        // because its tier says no machine may ever judge it.
        assert_eq!(
            unaimed.summary(),
            "a-person-holds-this-by-its-own-declaration=1"
        );
        assert_eq!(rows[0].identity.kind, GREP_KIND);
        assert_eq!(rows[0].identity.scope, PRINCIPLE_GATE_SCOPE);
        assert_eq!(rows[0].identity.literal, "principle: feedback_refused");
        // One name for one subject, whichever face the row was read off.
        assert_eq!(rows[0].source.doc, PRINCIPLE_LEDGER_SOURCE);
        assert_eq!(
            rows[0].assertion.as_deref(),
            Some(
                "This principle has been refused work 3 times, most recently at \
                 2025-11-01T12:26:40+00:00, by the host gate declared as `NARROW_ADD_RE`."
            )
        );
        assert_eq!(
            rows[0].title.as_deref(),
            Some(
                "Gate: refused\n\nGive the principle `feedback_refused` the gate it is owed: a \
                 check answering from the shape of the act it rules out, decided before the act, \
                 declared in the header of the test that carries it. The rule itself: only stage \
                 your own paths. This principle has been refused work 3 times, most recently at \
                 2025-11-01T12:26:40+00:00, by the host gate declared as `NARROW_ADD_RE`."
            )
        );
    }

    /// The gate column's sentinel is not a gate, and a refusal whose evidence has
    /// no gate to name is still a refusal.
    ///
    /// `-` is what that column holds when the host's registry was read and the
    /// rule was not in it -- a reading of the registry and not an organ that
    /// refused -- so printing it would put a gate named `` `-` `` into the goal and
    /// the assertion. The count and the instant are the evidence either way, and a
    /// count with no instant drops the instant instead of substituting one.
    #[test]
    fn a_refusal_with_no_gate_to_name_still_carries_its_count() {
        let evidence = refusal_evidence(&refused_principle_row(
            "feedback_x",
            Some(PrincipleCheckKind::Tree),
            None,
            Some(HOST_REGISTRY_READ_NO_GATE),
            1,
            None,
        ));
        assert_eq!(evidence, "once");
        // And a count with no instant names the gate when there is one.
        assert_eq!(
            refusal_evidence(&refused_principle_row(
                "feedback_x",
                Some(PrincipleCheckKind::Tree),
                None,
                Some("CIRCLED_CHARS"),
                2,
                None,
            )),
            "2 times, by the host gate declared as `CIRCLED_CHARS`"
        );
    }

    /// One principle is one row, however many faces read it.
    ///
    /// A rule can be unenforced and refused at once, which puts it in both faces,
    /// and both queries answer with the account's own row -- so the refusal count
    /// is on whichever copy is kept and the dropped one carries nothing the kept
    /// one lacks. Asserted rather than argued because two rows for one subject is
    /// what the write cannot express: identity is the key, and the title of
    /// whichever write ran last is the one that stays.
    #[test]
    fn a_rule_read_off_both_faces_becomes_one_row() {
        let faces = principle_faces(
            vec![refused_principle_row(
                "feedback_x",
                Some(PrincipleCheckKind::Tree),
                Some("a rule"),
                None,
                2,
                None,
            )],
            vec![
                refused_principle_row(
                    "feedback_x",
                    Some(PrincipleCheckKind::Tree),
                    Some("a rule"),
                    None,
                    2,
                    None,
                ),
                refused_principle_row(
                    "feedback_y",
                    Some(PrincipleCheckKind::Tree),
                    Some("another rule"),
                    None,
                    1,
                    None,
                ),
            ],
        );
        assert_eq!(faces.len(), 2, "{faces:?}");
        assert_eq!(faces[0].slug, "feedback_x");
        assert_eq!(faces[1].slug, "feedback_y");
        // And the readings minted from the merged list are one identity per
        // subject, each carrying the violation it was read with.
        let (rows, unaimed) = principle_debt_readings(&faces);
        assert!(unaimed.is_empty(), "{}", unaimed.summary());
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0].identity.literal, "principle: feedback_x");
        assert_eq!(rows[1].identity.literal, "principle: feedback_y");
        assert!(rows.iter().all(|row| row.assertion.is_some()), "{rows:?}");
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

    /// The verdict carries the window only when one was measured, so "not paced"
    /// and "a window measured to allow nothing" stay two readings.
    #[test]
    fn the_verdict_carries_the_window_only_when_one_was_measured() {
        let now = 1_760_000_000i64;
        let measured = LlmPoolStatus {
            pace: LlmWindowPace {
                measured_upstreams: 2,
                allowance_tokens: 10_000_000,
                spent_tokens: 9_000_000,
                reserve_tokens: 500_000,
                reserve_population: 5,
                window_secs: 604_800,
                window_start_unix: now - 86_400,
            },
            ..Default::default()
        };
        assert_eq!(
            pool_verdict(Some(&measured), now, 300).window_pace,
            Some(measured.pace),
            "a measured window rides the verdict"
        );

        let unmeasured = LlmPoolStatus {
            pace: LlmWindowPace {
                measured_upstreams: 0,
                allowance_tokens: 0,
                spent_tokens: 0,
                reserve_tokens: 0,
                reserve_population: 0,
                window_secs: 0,
                window_start_unix: 0,
            },
            ..Default::default()
        };
        assert_eq!(
            pool_verdict(Some(&unmeasured), now, 300).window_pace,
            None,
            "no measurement is not a window of zero"
        );
        assert_eq!(pool_verdict(None, now, 300).window_pace, None);
    }

    /// The batch is what the remaining allowance pays for, never more than the
    /// tick's ceiling, and never zero for a window nobody measured.
    #[test]
    fn the_handover_batch_is_bounded_by_what_the_window_can_pay_for() {
        let now = 1_760_000_000i64;
        let with_pace = |measured: usize, allowance: u64, spent: u64, reserve: u64| {
            let status = LlmPoolStatus {
                pace: LlmWindowPace {
                    measured_upstreams: measured,
                    allowance_tokens: allowance,
                    spent_tokens: spent,
                    reserve_tokens: reserve,
                    reserve_population: 5,
                    window_secs: 604_800,
                    window_start_unix: 0,
                },
                ..Default::default()
            };
            pool_verdict(Some(&status), now, 300)
        };

        // The reason rides with the number: three of these branches return the
        // ceiling, and the pass's log line has to be able to say which one.
        let bound = |verdict: &PoolVerdict, ceiling: usize| {
            let b = handovers_for_this_pass(verdict, ceiling);
            (b.budget, b.reason)
        };

        // Nothing to derive a bound from: no snapshot, a snapshot with no
        // measurement, and a window with no job cost yet. All three are the
        // ceiling, and the first two are one reason because the verdict keeps
        // only a measured pace (`measured_upstreams == 0` is not a pace).
        assert_eq!(
            bound(&pool_verdict(None, now, 300), 200),
            (200, "no_measurement")
        );
        assert_eq!(bound(&with_pace(0, 0, 0, 0), 200), (200, "no_measurement"));
        assert_eq!(
            bound(&with_pace(1, 9_000_000, 0, 0), 200),
            (200, "no_reserve")
        );

        // Four whole jobs fit: four, not two hundred -- the point of the bound.
        assert_eq!(
            bound(&with_pace(1, 5_000_000, 3_000_000, 500_000), 200),
            (4, "window")
        );

        // A window with more than the ceiling left is capped by the ceiling.
        assert_eq!(
            bound(&with_pace(1, 100_000_000, 0, 500_000), 200),
            (200, "ceiling")
        );

        // Less left than one job costs is zero handovers: every task this pass
        // would hand over is one the gateway refuses on the same numbers.
        assert_eq!(
            bound(&with_pace(1, 1_000_000, 900_000, 500_000), 200),
            (0, "window")
        );
        // Spent past the allowance (a snapshot read after the window moved) is
        // still zero, not a wrapped-around huge number.
        assert_eq!(
            bound(&with_pace(1, 1_000_000, 2_000_000, 500_000), 200),
            (0, "window")
        );

        // A pace summed over one member of a three-upstream pool is not a bound
        // for the pool: the other two were never measured, so nothing says the
        // batch they would serve is refused. The ceiling stands.
        let partial_pool = LlmPoolStatus {
            pace: LlmWindowPace {
                measured_upstreams: 1,
                allowance_tokens: 1_000_000,
                spent_tokens: 900_000,
                reserve_tokens: 500_000,
                reserve_population: 5,
                window_secs: 604_800,
                window_start_unix: 0,
            },
            pool_size: 3,
            ..Default::default()
        };
        assert_eq!(
            bound(&pool_verdict(Some(&partial_pool), now, 300), 200),
            (200, "partial_pool")
        );
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
        measured: std::sync::Mutex<Vec<ChangeMeasurement>>,
        delivered: std::sync::Mutex<Vec<(ChangeIdentity, String)>>,
    }

    impl FakeLedger {
        fn written(&self) -> Vec<DispatchState> {
            self.written.lock().unwrap().clone()
        }

        fn deliveries(&self) -> Vec<(ChangeIdentity, String)> {
            self.delivered.lock().unwrap().clone()
        }

        fn measured(&self) -> Vec<ChangeIdentity> {
            self.measurements()
                .into_iter()
                .map(|measurement| measurement.identity)
                .collect()
        }

        fn measurements(&self) -> Vec<ChangeMeasurement> {
            self.measured.lock().unwrap().clone()
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
        async fn record_delivery(&self, identity: &ChangeIdentity, rev: &str) -> SFResult<()> {
            self.delivered
                .lock()
                .unwrap()
                .push((identity.clone(), rev.to_string()));
            Ok(())
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
        async fn apply_measurement(&self, measurement: &ChangeMeasurement) -> SFResult<()> {
            self.measured.lock().unwrap().push(measurement.clone());
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
        async fn owed_by_band(&self) -> SFResult<Vec<(ChangePriority, usize)>> {
            Ok(ChangePriority::ALL
                .into_iter()
                .map(|band| {
                    let rows = self
                        .rows
                        .iter()
                        .filter(|row| row.state.is_outstanding() && row.priority() == band)
                        .count();
                    (band, rows)
                })
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
        async fn landed_touching(
            &self,
            paths: &[String],
            limit: usize,
        ) -> SFResult<Vec<ChangeLedgerRow>> {
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    row.state == ChangeState::Landed && scope_is_touched(&row.identity, paths)
                })
                .take(limit)
                .cloned()
                .collect())
        }
        async fn retired_surface_moved(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    row.state == ChangeState::Retired
                        && row.retire_reason == Some(RetireReason::SurfaceMoved)
                })
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
    /// Run git and insist it succeeded, so a fixture that cannot build fails at
    /// the line that broke rather than as a mystery in the test below.
    fn git_ok(args: &[&str]) -> String {
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
    }

    /// An empty bare repository beside a work tree, both in a temporary
    /// directory.
    fn scratch_repo() -> (tempfile::TempDir, String, String) {
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
        git_ok(&["init", "-q", "--bare", "-b", "main", &bare]);
        git_ok(&[
            "--git-dir",
            &bare,
            "config",
            "user.email",
            "tree@cogneva.invalid",
        ]);
        git_ok(&["--git-dir", &bare, "config", "user.name", "tree"]);
        (dir, bare, work)
    }

    /// Commit whatever the work tree holds and name the revision it made.
    fn commit_all(bare: &str, work: &str, message: &str) -> String {
        git_ok(&["--git-dir", bare, "--work-tree", work, "add", "--all"]);
        git_ok(&[
            "--git-dir",
            bare,
            "--work-tree",
            work,
            "commit",
            "-q",
            "-m",
            message,
        ]);
        git_ok(&["--git-dir", bare, "rev-parse", "HEAD"])
            .trim()
            .to_string()
    }

    fn two_revision_repo() -> (tempfile::TempDir, String, String) {
        let (dir, bare, work) = scratch_repo();
        std::fs::write(std::path::Path::new(&work).join("file"), "first")
            .expect("the fixture file");
        let first = commit_all(&bare, &work, "fixture");
        std::fs::write(std::path::Path::new(&work).join("file"), "second")
            .expect("the fixture file");
        let second = commit_all(&bare, &work, "fixture");
        (dir, first, second)
    }

    /// A repository in which one file was present and then deleted, returning
    /// the revision the deletion was recorded in.
    ///
    /// The deletion is not the tip: a surface that left is followed by a tree
    /// that moved on, which is the shape the real account has and the one in
    /// which "the scope is absent now" and "the commit that took it out" are
    /// different revisions.
    fn repo_with_a_deleted_scope() -> (tempfile::TempDir, String) {
        let (dir, bare, work) = scratch_repo();
        // Nested under a top-level entry that stays: a scope whose top level is
        // gone reads as a check about another tree, and never reaches the
        // history question this fixture is for.
        std::fs::create_dir_all(std::path::Path::new(&work).join("crates"))
            .expect("the fixture directory");
        std::fs::write(std::path::Path::new(&work).join("crates/gone.rs"), "here")
            .expect("the fixture file");
        std::fs::write(std::path::Path::new(&work).join("crates/stays.rs"), "here")
            .expect("the fixture file");
        commit_all(&bare, &work, "fixture");
        std::fs::remove_file(std::path::Path::new(&work).join("crates/gone.rs"))
            .expect("the fixture file can be removed");
        let exited = commit_all(&bare, &work, "remove the surface");
        std::fs::write(
            std::path::Path::new(&work).join("crates/stays.rs"),
            "moved on",
        )
        .expect("the fixture file");
        commit_all(&bare, &work, "move on");
        (dir, exited)
    }

    /// A repository in which one file was present, deleted, and then put back,
    /// with the deletion and the return separated by a commit that touched
    /// something else.
    ///
    /// The gap is the shape the real account has and the one that makes the two
    /// facts different: "the scope is absent" was true at the revision that
    /// retired the row and is false at the tip, and neither revision is the
    /// deletion commit. The literal is written into the restored file so the
    /// check has something to find; a literal absent from it is the same fixture
    /// with a predicate that does not hold.
    fn repo_with_a_restored_scope(literal: &str) -> (tempfile::TempDir, String) {
        let (dir, bare, work) = scratch_repo();
        std::fs::create_dir_all(std::path::Path::new(&work).join("crates"))
            .expect("the fixture directory");
        std::fs::write(std::path::Path::new(&work).join("crates/gone.rs"), "here")
            .expect("the fixture file");
        std::fs::write(std::path::Path::new(&work).join("crates/stays.rs"), "here")
            .expect("the fixture file");
        commit_all(&bare, &work, "fixture");
        std::fs::remove_file(std::path::Path::new(&work).join("crates/gone.rs"))
            .expect("the fixture file can be removed");
        let exited = commit_all(&bare, &work, "remove the surface");
        std::fs::write(
            std::path::Path::new(&work).join("crates/stays.rs"),
            "moved on",
        )
        .expect("the fixture file");
        commit_all(&bare, &work, "move on");
        std::fs::write(
            std::path::Path::new(&work).join("crates/gone.rs"),
            format!("back\n{literal}\n"),
        )
        .expect("the fixture file");
        commit_all(&bare, &work, "the surface returns");
        (dir, exited)
    }

    /// A row already settled as retired because its scope left the tree.
    fn retired_row(scope: &str, measured_rev: Option<&str>) -> ChangeLedgerRow {
        ChangeLedgerRow {
            state: ChangeState::Retired,
            measured_rev: measured_rev.map(str::to_string),
            retire_reason: Some(RetireReason::SurfaceMoved),
            retired_at: Some(Utc.timestamp_opt(1_760_000_100, 0).unwrap()),
            ..pending_row(scope)
        }
    }

    /// A landed row that names the revision it was measured at.
    fn landed_row(scope: &str, measured_rev: Option<&str>) -> ChangeLedgerRow {
        ChangeLedgerRow {
            state: ChangeState::Landed,
            measured_rev: measured_rev.map(str::to_string),
            ..pending_row(scope)
        }
    }

    /// The same match the store's query makes: the whole tree is touched by any
    /// path, a path equals the scope, or it sits below the scope read as a
    /// directory. The spellings the account holds are folded first, because the
    /// double has to answer for the same rows the query would.
    fn scope_is_touched(identity: &ChangeIdentity, paths: &[String]) -> bool {
        let scope = identity
            .scope
            .trim()
            .trim_start_matches("./")
            .trim_end_matches('/');
        if scope.is_empty() || scope == "." {
            return !paths.is_empty();
        }
        paths.iter().any(|path| {
            let path = path.trim_start_matches("./");
            path == scope || path.starts_with(&format!("{scope}/"))
        })
    }

    /// The window and the diff answer the same question from two sides, and the
    /// diff is the side that does not wait: a landed row whose scope a revision
    /// moved is asked in the pass that sees the move, even though the window's
    /// quota over this account reaches a different row first.
    #[tokio::test]
    async fn a_landed_row_whose_scope_moved_is_asked_without_waiting_for_the_window() {
        let (dir, first, second) = two_revision_repo();
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let snapshot = tree.snapshot().await.expect("the fixture is readable");
        // Two landed rows, so the window's quota is one. The double hands rows
        // back in the order they were written, so the untouched row is the one
        // the window reaches; the moved one is reached only by the diff.
        let store = FakeLedger {
            rows: vec![
                landed_row("crates/untouched", Some(&second)),
                landed_row("file", Some(&first)),
            ],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();

        let pass = reopen_pass(
            &store,
            &tree,
            &snapshot,
            &mut unsettled,
            &["file".to_string()],
        )
        .await;
        let touched = pass.touched;

        let measured = store.measured();
        assert_eq!(
            touched, 1,
            "the moved path reaches exactly the one landed row it names: {measured:?}"
        );
        assert!(
            measured.contains(&ChangeIdentity::new("grep", "file", "fn thing")),
            "the row whose file moved was asked again: {measured:?}"
        );

        // The same account with no moved path: the window alone reaches one row,
        // which is what makes the assertion above a reading about the diff rather
        // than about the pass asking everything it can see.
        let quiet = FakeLedger {
            rows: vec![
                landed_row("crates/untouched", Some(&second)),
                landed_row("file", Some(&first)),
            ],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();
        let touched = reopen_pass(&quiet, &tree, &snapshot, &mut unsettled, &[])
            .await
            .touched;

        assert_eq!(
            touched, 0,
            "no path moved, so nothing was reached by the diff"
        );
        assert!(
            !quiet
                .measured()
                .contains(&ChangeIdentity::new("grep", "file", "fn thing")),
            "without a moved path the row waits for the window: {:?}",
            quiet.measured()
        );
    }

    /// A landed row whose scope this revision does not hold cannot be settled,
    /// and it keeps its `measured_at`, so it holds the head of the staleness
    /// order forever. The pass has to say so: the settled-row count is the same
    /// whether the sweep finished or spent its batch on rows that answer nothing,
    /// and the second pass has to show the same row held rather than re-asked.
    #[tokio::test]
    async fn a_landed_row_the_tree_cannot_settle_is_counted_and_then_held() {
        let (dir, first, _second) = two_revision_repo();
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let snapshot = tree.snapshot().await.expect("the fixture is readable");
        let store = FakeLedger {
            rows: vec![landed_row("file/inner", Some(&first))],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();

        let pass = reopen_pass(&store, &tree, &snapshot, &mut unsettled, &[]).await;

        assert_eq!(
            pass.unsettleable
                .get(&UnsettledReason::ScopeAbsentInRevision)
                .copied(),
            Some(1),
            "the row the tree could not settle is counted by its reason: {:?}",
            pass.unsettleable
        );
        assert_eq!(
            pass.rechecked, 0,
            "a row that settled nothing was not re-checked"
        );
        assert!(
            store.measured().is_empty(),
            "a row that settled nothing was not written back: {:?}",
            store.measured()
        );
        assert!(
            unsettled.contains_key(&ChangeIdentity::new("grep", "file/inner", "fn thing")),
            "the row is remembered as unanswered"
        );

        // Inside its retry window the same row is skipped rather than asked
        // again, and the skip is a number the pass reports: the batch was spent
        // on waiting, not on the rest of the set.
        let held = reopen_pass(&store, &tree, &snapshot, &mut unsettled, &[]).await;
        assert_eq!(
            held.held, 1,
            "the row is held by its retry window, not re-asked"
        );
        assert!(
            held.unsettleable.is_empty(),
            "a held row was not asked, so it was not counted again: {:?}",
            held.unsettleable
        );
    }

    /// A landed row whose scope the history deleted is settled as retired, at
    /// the revision the scope left, rather than held as unsettleable forever.
    ///
    /// The check cannot be answered and no later look will answer it: the
    /// surface is gone. Held as `unsettleable`, the row keeps its `measured_at`
    /// and sits at the head of the staleness order with an age that reads as
    /// neglect nobody can repair. Retired, it says the thing that is true -- the
    /// work was measured here and its surface left -- and it names the commit
    /// that took the surface out, which is the fact a reader would otherwise
    /// have to reconstruct from the history.
    ///
    /// The row below [`a_landed_row_the_tree_cannot_settle_is_counted_and_then_held`]
    /// is the other half: a scope this line never held stays unsettleable,
    /// because there is no deletion to name and no surface that moved.
    #[tokio::test]
    async fn a_landed_row_whose_scope_was_deleted_is_retired_at_the_revision_it_left() {
        let (dir, exited) = repo_with_a_deleted_scope();
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let snapshot = tree.snapshot().await.expect("the fixture is readable");
        let store = FakeLedger {
            rows: vec![landed_row("crates/gone.rs", Some(&exited))],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();

        let pass = reopen_pass(&store, &tree, &snapshot, &mut unsettled, &[]).await;

        assert_eq!(pass.retired, 1, "the row was settled as retired");
        assert_eq!(
            pass.rechecked, 0,
            "a retirement is not a re-check: the claim did not still hold"
        );
        assert!(
            pass.unsettleable.is_empty(),
            "the row is settled, not counted as a check the tree cannot answer: {:?}",
            pass.unsettleable
        );
        assert!(
            !unsettled.contains_key(&ChangeIdentity::new("grep", "crates/gone.rs", "fn thing")),
            "a retired row is not left waiting to be asked again"
        );

        let measurements = store.measurements();
        assert_eq!(measurements.len(), 1, "the retirement was written back");
        let retirement = &measurements[0];
        assert_eq!(retirement.state, ChangeState::Retired);
        assert_eq!(
            retirement.retire_reason,
            Some(RetireReason::SurfaceMoved),
            "the reason distinguishes a surface that moved from a check that was\n             never about this tree"
        );
        assert_eq!(
            retirement.measured_rev.as_deref(),
            Some(exited.as_str()),
            "the revision named is where the scope exited, not the tree the pass read"
        );
        assert_ne!(
            retirement.measured_rev.as_deref(),
            Some(snapshot.rev()),
            "the deletion commit and the tree read are different revisions here, so the\n             assertion above is about the history and not about the tip"
        );
    }

    /// A retired row whose scope came back is measured again.
    ///
    /// `landed` names the deletion commit and is true of it; the surface is not
    /// gone from the tree. Without this read nothing in the account would ask
    /// again -- the row is out of the outstanding set by state and out of the
    /// landed set by state -- so the retirement would be the account's last word
    /// about a tree that no longer matches it.
    #[tokio::test]
    async fn a_retired_row_whose_scope_came_back_is_measured_again() {
        let (dir, exited) = repo_with_a_restored_scope("fn thing");
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let snapshot = tree.snapshot().await.expect("the fixture is readable");
        let store = FakeLedger {
            rows: vec![retired_row("crates/gone.rs", Some(&exited))],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();

        let pass = reopen_pass(&store, &tree, &snapshot, &mut unsettled, &[]).await;

        assert_eq!(
            pass.restored, 1,
            "the retired row's scope is back, so it was measured again"
        );
        assert_eq!(
            pass.retired, 0,
            "the row is already retired; the pass that questions a retirement does not \
             write another one"
        );
        let measurements = store.measurements();
        assert_eq!(measurements.len(), 1, "the new measurement was written");
        assert_eq!(
            measurements[0].state,
            ChangeState::Landed,
            "the restored surface holds the check: {:?}",
            measurements[0]
        );
        assert_eq!(
            measurements[0].measured_rev.as_deref(),
            Some(snapshot.rev()),
            "the new measurement names the tree it read, not the revision the surface left at"
        );
        assert_ne!(
            measurements[0].measured_rev.as_deref(),
            Some(exited.as_str()),
            "the two revisions are different in this fixture, so the assertion above is about \
             the tree and not about the retirement's carried revision"
        );
    }

    /// A retired row whose scope is still gone writes nothing.
    ///
    /// The read reaches every retired row every pass, so a pass that wrote
    /// "still absent" would touch the whole set forever: the rows would never be
    /// quiet and the write would be a fact about the pass rather than about the
    /// tree. Silence is the answer here, and the count says the pass looked.
    #[tokio::test]
    async fn a_retired_row_whose_scope_is_still_gone_is_left_alone() {
        let (dir, exited) = repo_with_a_deleted_scope();
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let snapshot = tree.snapshot().await.expect("the fixture is readable");
        let store = FakeLedger {
            rows: vec![retired_row("crates/gone.rs", Some(&exited))],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();

        let pass = reopen_pass(&store, &tree, &snapshot, &mut unsettled, &[]).await;

        assert_eq!(pass.restored, 0, "the scope is still absent");
        assert!(
            store.measurements().is_empty(),
            "nothing was settled, so nothing was written: {:?}",
            store.measurements()
        );
    }

    /// A restored scope whose predicate no longer holds is measured to that
    /// answer rather than left retired.
    ///
    /// The question the second look asks is the ordinary one -- does the check
    /// hold at this revision -- and its answer is whatever the judge says. The
    /// retirement said the surface left, which is why the row is here at all;
    /// it does not say the surface came back holding what was asked of it.
    #[tokio::test]
    async fn a_restored_scope_that_no_longer_holds_is_measured_to_its_new_answer() {
        let (dir, exited) = repo_with_a_restored_scope("fn something else");
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let snapshot = tree.snapshot().await.expect("the fixture is readable");
        let store = FakeLedger {
            rows: vec![retired_row("crates/gone.rs", Some(&exited))],
            ..Default::default()
        };
        let mut unsettled = HashMap::new();

        let pass = reopen_pass(&store, &tree, &snapshot, &mut unsettled, &[]).await;

        assert_eq!(
            pass.restored, 1,
            "the scope is back, so the row was asked again"
        );
        let measurements = store.measurements();
        assert_eq!(measurements.len(), 1, "the new answer was written");
        assert_eq!(
            measurements[0].state,
            ChangeState::Pending,
            "the file is back without the literal: {:?}",
            measurements[0]
        );
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

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            None,
        )
        .await;

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

    /// One pass hands over a bounded slice, and the rows it leaves are counted
    /// where it counts the ones a pause withholds -- so the arithmetic still
    /// closes on the set the pass read, not on the part of it the pass acted on.
    ///
    /// This is the row the bound exists for: without it a tick hands over
    /// whatever the backlog happens to be, and a hundred and thirty-nine rows in
    /// one tick is a burst the pool pays for at once and a reading no one can
    /// compare with the tick before it. What it costs is one cell's worth of
    /// meaning, which is why the rows left behind land in a cell that is already
    /// published: the residual line a reader takes off this row -- owed minus
    /// everything accounted for -- still has to come out at zero.
    #[tokio::test]
    async fn a_pass_hands_over_its_slice_and_the_rest_still_accounts_for_itself() {
        let over = HANDOVERS_PER_PASS + 1;
        let store = FakeLedger {
            rows: (0..over)
                .map(|i| pending_row(&format!("crates/slice{i}")))
                .collect(),
            ..Default::default()
        };
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            None,
        )
        .await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.decision, DispatchDecision::Proceed);
        assert_eq!(state.pending, over as u64);
        assert_eq!(
            state.submitted, HANDOVERS_PER_PASS as u64,
            "the pass handed over its slice and no more"
        );
        assert_eq!(state.held_back, 1, "and the row past the slice is counted");
        assert_eq!(graph.submitted().len(), HANDOVERS_PER_PASS);
        assert_the_counts_add_up(&state);
    }

    /// A change a commit names is recorded against its row, and stays owed.
    ///
    /// Delivery is not the account's `landed`: the diff reached the line, and
    /// whether the claim the row makes is true of the tree is still the
    /// measurement's to say. The pass therefore records the revision and hands
    /// the change over like any other owed row -- what changes is that the row
    /// now carries the one fact that tells "nobody finished this" apart from
    /// "it went in and did not do what it said".
    #[tokio::test]
    async fn a_change_a_commit_names_is_recorded_as_delivered_and_still_owed() {
        let store = FakeLedger {
            rows: vec![pending_row("crates/delivered")],
            ..Default::default()
        };
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();
        let mut deliveries = HashMap::new();
        deliveries.insert(
            dispatch_task_id(&store.rows[0].identity, 0),
            "abcdef123456".to_string(),
        );

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            Some(&deliveries),
        )
        .await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(
            state.delivered,
            Some(1),
            "a change the line names is delivered"
        );
        assert_eq!(
            state.submitted, 1,
            "delivery does not take the change out of the owed set"
        );
        assert_eq!(
            store.deliveries(),
            vec![(store.rows[0].identity.clone(), "abcdef123456".to_string())],
            "the revision is written against the row, not only counted"
        );
        assert_the_counts_add_up(&state);
    }

    /// A settled row is not one the delivery pass walks, so a commit naming it
    /// records nothing against it.
    ///
    /// The pass reads the outstanding set, and `landed` / `retired` are not in
    /// it -- so "which commit carried this change" is answered only for rows
    /// that were still owed when the line carried them. A change that landed
    /// and settled before this column existed never gets the annotation, which
    /// is why the first reading of the count is the number of owed rows with a
    /// landing commit, not the number of landing commits on the line: the two
    /// counts are of different sets, and reading the larger one as the expected
    /// value would call a healthy zero a broken reader.
    #[tokio::test]
    async fn a_settled_row_is_never_walked_so_its_landing_commit_records_nothing() {
        let store = FakeLedger {
            rows: vec![landed_row("crates/settled", Some("111111111111"))],
            ..Default::default()
        };
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();
        let mut deliveries = HashMap::new();
        deliveries.insert(
            dispatch_task_id(&store.rows[0].identity, 0),
            "abcdef123456".to_string(),
        );

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            Some(&deliveries),
        )
        .await;

        assert!(
            store.deliveries().is_empty(),
            "a settled row is not walked, so its landing commit is written nowhere"
        );
        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(
            state.delivered,
            Some(0),
            "the pass measured: zero of the owed rows carries a landing commit"
        );
        assert_the_counts_add_up(&state);
    }

    /// A commit log that could not be read writes no delivery count at all.
    ///
    /// The unreadable branch is the whole reason the column is nullable: a pass
    /// that wrote zero would report "nothing was delivered" on exactly the pass
    /// that knew nothing, and the next reader would compare it with a pass that
    /// read the log and found the same number.
    #[tokio::test]
    async fn a_commit_log_that_could_not_be_read_writes_no_delivery_count() {
        let store = FakeLedger {
            rows: vec![pending_row("crates/unread")],
            ..Default::default()
        };
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            None,
        )
        .await;

        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(
            state.delivered, None,
            "unread is not zero: zero is a measurement this pass did not take"
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
            pool_size: 0,
            window_pace: None,
            measurable_pool_size: 0,
        };

        let read_back = dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            Some(&SilentPool),
            Some(paused),
            None,
            None,
            &[],
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
            None,
            &[],
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

    /// A pass names the intakes it could not read on its own row.
    ///
    /// A failed intake contributes no readings and the account keeps what it
    /// holds, which leaves exactly the rows of a pass that found nothing. The
    /// name is the only thing that separates them, and it has to outlive the
    /// process that wrote the warning.
    #[tokio::test]
    async fn a_pass_records_the_intakes_it_could_not_read() {
        let store = FakeLedger::default();
        let graph = Arc::new(FakeGraph::default());
        let control: Arc<dyn OrchestratorControl> = graph.clone();

        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[INTAKE_PRINCIPLES],
            None,
        )
        .await;
        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(
            state.sources_unread.as_deref(),
            Some("principles"),
            "a pass that lost its intake says so by name"
        );

        // Every source read is an empty string and not `None`: `None` is what a
        // row written before the column existed carries, and a reader that
        // conflated the two would report a read nothing performed.
        dispatch_pass(
            &store,
            &control,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            None,
        )
        .await;
        let state = store.written().pop().expect("the pass wrote its row");
        assert_eq!(state.sources_unread.as_deref(), Some(""));
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
            None,
            &[],
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
            None,
            &[],
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

        dispatch_pass(
            &store,
            &graph,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            None,
        )
        .await;

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

        dispatch_pass(
            &store,
            &graph,
            &HashMap::new(),
            None,
            None,
            None,
            None,
            &[],
            None,
        )
        .await;

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

    /// One cell of the reading face, selected the way a reader selects it.
    async fn published_cell(metrics: &Arc<dyn MetricsBackend>, readout: &str) -> f64 {
        let end = Utc::now() + chrono::Duration::seconds(1);
        let start = end - chrono::Duration::seconds(600);
        let samples = metrics
            .query_gauge_range(metric_names::CHANGE_DISPATCH_READOUTS.as_str(), start, end)
            .await
            .expect("the reading face is readable");
        samples
            .iter()
            .find(|sample| sample.labels.get("readout").map(String::as_str) == Some(readout))
            .unwrap_or_else(|| panic!("this pass published no cell for {readout}"))
            .value
    }

    /// One cell of the reading face, or `None` while no pass has published.
    ///
    /// [`published_cell`] insists on a cell, which is right for a pass the test
    /// drove itself; a test that starts the loop has to look before there is
    /// anything to look at.
    async fn readout_cell(metrics: &Arc<dyn MetricsBackend>, readout: &str) -> Option<f64> {
        let end = Utc::now() + chrono::Duration::seconds(1);
        let start = end - chrono::Duration::seconds(600);
        let samples = metrics
            .query_gauge_range(metric_names::CHANGE_DISPATCH_READOUTS.as_str(), start, end)
            .await
            .expect("the reading face is readable");
        samples
            .iter()
            .find(|sample| sample.labels.get("readout").map(String::as_str) == Some(readout))
            .map(|sample| sample.value)
    }

    /// The event store reaches the pass from the loop's own argument list.
    ///
    /// The test below starts at `dispatch_pass`, so it sees the store after the
    /// loop has already handed it over: turning that hand-off into `None` leaves
    /// it green while every deployment stops reading its handovers back, and the
    /// cells it publishes are zeros -- the same number a pass that read nothing
    /// writes. This one starts the loop and waits for the tick, so the value
    /// that arrives at the pass is the thing under test.
    #[tokio::test(start_paused = true)]
    async fn the_loop_hands_the_event_store_to_the_pass_that_reads_handovers_back() {
        let (dir, _first, _second) = two_revision_repo();
        let rows = vec![pending_row("crates/a"), pending_row("crates/b")];
        let ids: Vec<String> = rows
            .iter()
            .map(|row| dispatch_task_id(&row.identity, 0))
            .collect();
        let events = Arc::new(cog_storage::MemoryStateBackend::new());
        events
            .append_event(
                &ids[0],
                &handover_event(serde_json::json!([{
                    "name": CRITERION_ARTIFACT_NAME,
                    "content": "diff --git a/kept b/kept\n",
                    "artifact_type": "change",
                }])),
            )
            .await
            .unwrap();
        events
            .append_event(&ids[1], &handover_event(serde_json::json!([])))
            .await
            .unwrap();
        let store: Arc<dyn ChangeLedgerStore> = Arc::new(FakeLedger {
            rows,
            ..Default::default()
        });
        let metrics: Arc<dyn MetricsBackend> = Arc::new(cog_storage::MemoryMetricsBackend::new());
        let shutdown = ShutdownSignal::new();

        spawn_change_ledger_loop(
            store,
            None,
            Some(Arc::new(FakeGraph::default())),
            Some(Arc::new(SilentPool)),
            Some(Arc::clone(&metrics)),
            shutdown.clone(),
            CheckTree::new(dir.path().join("bare"), "main"),
            None,
            None,
            Some(events),
        );

        // The first tick is one pass interval away. Paused time makes that a
        // number of clock jumps rather than five minutes of waiting; the
        // deadline is on the loop's own clock, so a loop that never publishes
        // ends the test instead of hanging it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
        let value = loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if let Some(value) = readout_cell(&metrics, "handover_events").await {
                break value;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the loop ran an hour without publishing a pass; the wiring below it is not \
                 reaching the table"
            );
        };
        shutdown.trigger();

        // The same two numbers the driven pass reads, reached through the
        // loop's own wiring: both handovers on record, one of them with a
        // product.
        assert_eq!(value, 2.0);
        assert_eq!(readout_cell(&metrics, "handover_products").await, Some(1.0));
    }

    /// The pass reads the handovers back over the same set it hands over, and
    /// the reading face carries what it found.
    ///
    /// The publish test below would stay green if nothing ever called the
    /// reader: the census would be published as zeros, and zeros are exactly
    /// what a reader cannot tell from a pass that read nothing. This is the test
    /// that fails when the wiring is dropped rather than when the shape is
    /// wrong.
    #[tokio::test]
    async fn the_pass_reads_back_what_the_handovers_of_the_set_it_read_have_left() {
        let store = FakeLedger {
            rows: vec![pending_row("crates/a"), pending_row("crates/b")],
            ..Default::default()
        };
        let graph: Arc<dyn OrchestratorControl> = Arc::new(FakeGraph::default());
        let events = cog_storage::MemoryStateBackend::new();
        events
            .append_event(
                &dispatch_task_id(&store.rows[0].identity, 0),
                &handover_event(serde_json::json!([{
                    "name": CRITERION_ARTIFACT_NAME,
                    "content": "diff --git a/kept b/kept\n",
                    "artifact_type": "change",
                }])),
            )
            .await
            .unwrap();
        events
            .append_event(
                &dispatch_task_id(&store.rows[1].identity, 0),
                &handover_event(serde_json::json!([])),
            )
            .await
            .unwrap();
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        let metrics: Arc<dyn MetricsBackend> = metrics.clone();

        dispatch_pass(
            &store,
            &graph,
            &HashMap::new(),
            Some(&SilentPool),
            None,
            Some(&metrics),
            Some(&events),
            &[],
            None,
        )
        .await;

        // Both rows were handed over and both left events; one of them produced.
        // The two numbers are the reading: a pass that reported one of them
        // would be answering "did anything get produced" with "was anything
        // handed over".
        assert_eq!(published_cell(&metrics, "handover_events").await, 2.0);
        assert_eq!(published_cell(&metrics, "handover_products").await, 1.0);
        assert_eq!(published_cell(&metrics, "handover_unreadable").await, 0.0);
    }

    // ── Getting back to what a handover produced ──

    /// The id a change's handovers carry, and the one a re-arm moves.
    ///
    /// Read before anything is built on it, because everything is: the reader
    /// below, and the graph's own answer about a change, both ask for the id of a
    /// generation out of the identity alone. Generation zero is asserted to be
    /// the bare prefix and not a prefix with a zero in it -- the ids handed out
    /// before generations existed are the ones already in the stream, and a
    /// reader that recomputed them differently would find nothing and call it a
    /// product that was never kept.
    #[test]
    fn the_id_is_the_prefix_the_identity_fixes_and_a_rearm_appends_its_generation() {
        let identity = ChangeIdentity::new("grep", "crates/a", "fn thing");
        let prefix = handover_task_prefix(&identity);

        assert!(prefix.starts_with(DISPATCH_TASK_PREFIX), "{prefix}");
        assert_eq!(dispatch_task_id(&identity, 0), prefix);
        assert_eq!(dispatch_task_id(&identity, 2), format!("{prefix}-r2"));
        // Two changes that differ only in where a part ends are two changes:
        // the parts are terminated as they are hashed, so the split of the same
        // characters cannot land on one id.
        assert_ne!(
            handover_task_prefix(&ChangeIdentity::new("grep", "crates/a", "fn thing")),
            handover_task_prefix(&ChangeIdentity::new("grep", "crates/a:fn", "thing")),
            "an id shared by two changes is one of them never being dispatched"
        );
    }

    /// The deliverable's name, written out here rather than read from the
    /// constant the reader matches on.
    ///
    /// The name is one string that two things have to agree about -- the reader
    /// in this file, and the acceptance criterion that counts artifacts under it
    /// at the field layer -- and a test that took it from the reader's own
    /// constant could only ever show the reader agreeing with itself. Written
    /// out, a change to what the reader looks for fails here instead of passing
    /// twice.
    const CRITERION_ARTIFACT_NAME: &str = "changes.diff";

    /// The deliverable of a run, as the plan-generation segment writes it.
    fn handover_event(artifacts: serde_json::Value) -> cog_core::Event {
        cog_core::Event {
            offset: 0,
            task_id: String::new(),
            event_type: "ralph_iteration".to_string(),
            payload: serde_json::json!({
                "iteration": 1,
                "snapshot": {
                    "plan": { "steps": [] },
                    "generation": { "content": serde_json::Value::Null, "artifacts": artifacts },
                },
            }),
            timestamp: Utc.timestamp_opt(1_760_000_000, 0).unwrap(),
        }
    }

    /// The change's product is found from the identity, and what it produced is
    /// what comes back.
    ///
    /// The id is recomputed from the identity and the events are read back
    /// through it, with nothing stored to connect the two -- which is the whole
    /// of what the identity edge is for. The diff comes back verbatim: a reader
    /// that only counted the artifact would leave "it can be retrieved" and "the
    /// count went up" as the same assertion, and they are not.
    #[tokio::test]
    async fn a_changes_product_is_read_back_from_the_id_its_identity_fixes() {
        let identity = ChangeIdentity::new("grep", "crates/a", "fn thing");
        let events = cog_storage::MemoryStateBackend::new();
        events
            .append_event(
                &dispatch_task_id(&identity, 0),
                &handover_event(serde_json::json!([{
                    "name": CRITERION_ARTIFACT_NAME,
                    "content": "diff --git a/x b/x\n",
                    "artifact_type": "change",
                }])),
            )
            .await
            .unwrap();

        let readback = read_handover_products(&events, &identity).await.unwrap();

        assert_eq!(readback.generations, vec![0], "the events are still there");
        let product = readback.product.expect("the run recorded a deliverable");
        assert_eq!(product.generation, 0);
        assert_eq!(product.recorded_at, HANDOVER_GENERATION_ARTIFACTS);
        assert_eq!(product.diff, "diff --git a/x b/x\n");
        // And nothing of it is in another change's reading: the id is the
        // identity's, so a neighbour's events are not an answer about this one.
        let other = ChangeIdentity::new("grep", "crates/b", "fn thing");
        let readback = read_handover_products(&events, &other).await.unwrap();
        assert!(readback.generations.is_empty());
        assert!(readback.product.is_none());
    }

    /// A handover that ran and recorded nothing is not a handover that is gone,
    /// and the reading keeps the two apart.
    ///
    /// This is the shape production is in: the dispatched changes write their
    /// iterations and their generation's artifacts come back empty, because the
    /// pool refuses every one of them before the generator runs. Read as one
    /// number, that is the same zero as a change nothing was ever handed to --
    /// and a reader of the account would be told a product was lost.
    #[tokio::test]
    async fn a_handover_that_recorded_no_artifact_is_not_one_whose_events_are_gone() {
        let identity = ChangeIdentity::new("grep", "crates/a", "fn thing");
        let events = cog_storage::MemoryStateBackend::new();
        events
            .append_event(
                &dispatch_task_id(&identity, 0),
                &handover_event(serde_json::json!([])),
            )
            .await
            .unwrap();

        let ran_and_recorded_nothing = read_handover_products(&events, &identity).await.unwrap();
        let never_handed_over = read_handover_products(
            &events,
            &ChangeIdentity::new("grep", "crates/z", "fn thing"),
        )
        .await
        .unwrap();

        assert_eq!(ran_and_recorded_nothing.generations, vec![0]);
        assert!(ran_and_recorded_nothing.product.is_none());
        assert!(never_handed_over.generations.is_empty());
        assert!(never_handed_over.product.is_none());
        assert_ne!(
            HandoverCensus::default(),
            {
                let mut census = HandoverCensus::default();
                census.absorb(&ran_and_recorded_nothing);
                census
            },
            "a run that produced nothing still has to move the events count, or the two \
             readings are one number again"
        );
    }

    /// The other segment's shape is read too, and the reading says which one it
    /// came from.
    ///
    /// A run that goes through the roundtable records its deliverable per round
    /// rather than once, and a reader that knew only the plan-generation path
    /// would answer "produced nothing" about it. Which path it was found at is
    /// part of the answer rather than a detail: it is how a reader tells which
    /// segment of the run produced the change.
    #[tokio::test]
    async fn the_roundtables_own_shape_is_read_and_named_as_where_it_came_from() {
        let identity = ChangeIdentity::new("grep", "crates/a", "fn thing");
        let events = cog_storage::MemoryStateBackend::new();
        let mut event = handover_event(serde_json::json!([{
            "name": CRITERION_ARTIFACT_NAME,
            "content": "diff --git a/round-2 b/round-2\n",
            "artifact_type": "change",
        }]));
        // One round with nothing and a later one with the deliverable: the
        // newest round is the change's current attempt, so the later one is the
        // answer, and a reader that took the first match would report the round
        // that produced nothing.
        event.payload["snapshot"] = serde_json::json!({
            "roundtable": {
                "history": [
                    { "generation": { "content": "first", "artifacts": [] } },
                    { "generation": { "content": "second", "artifacts": [
                        { "name": CRITERION_ARTIFACT_NAME, "content": "diff --git a/r2 b/r2\n",
                          "artifact_type": "change" },
                    ] } },
                ],
            },
        });
        events
            .append_event(&dispatch_task_id(&identity, 0), &event)
            .await
            .unwrap();

        let readback = read_handover_products(&events, &identity).await.unwrap();

        let product = readback.product.expect("the roundtable recorded one");
        assert_eq!(product.recorded_at, HANDOVER_ROUNDTABLE_HISTORY);
        assert_eq!(product.diff, "diff --git a/r2 b/r2\n");
    }

    /// A deliverable the run recorded after more events than one page holds is
    /// still found.
    ///
    /// Measured against the live stream before it was written: the longest task
    /// holds 146 events and one of the deliverable-carrying events is the last of
    /// them, so a reader that read one page and stopped would answer "produced
    /// nothing" about the runs that took the longest -- the ones most likely to
    /// have produced something.
    #[tokio::test]
    async fn a_deliverable_past_the_first_page_is_still_found() {
        let identity = ChangeIdentity::new("grep", "crates/a", "fn thing");
        let events = cog_storage::MemoryStateBackend::new();
        let task_id = dispatch_task_id(&identity, 0);
        for _ in 0..HANDOVER_EVENTS_PER_PAGE {
            events
                .append_event(&task_id, &handover_event(serde_json::json!([])))
                .await
                .unwrap();
        }
        events
            .append_event(
                &task_id,
                &handover_event(serde_json::json!([{
                    "name": CRITERION_ARTIFACT_NAME,
                    "content": "diff --git a/late b/late\n",
                    "artifact_type": "change",
                }])),
            )
            .await
            .unwrap();

        let readback = read_handover_products(&events, &identity).await.unwrap();

        assert_eq!(readback.generations, vec![0]);
        assert_eq!(
            readback.product.expect("the last page carries it").diff,
            "diff --git a/late b/late\n"
        );

        // The whole stream was read, not just the two pages it happens to take:
        // a page loop that stopped early, or one that re-read the page it just
        // had, would be correct about this run and wrong about a longer one.
        for _ in 0..HANDOVER_EVENTS_PER_PAGE {
            events
                .append_event(
                    &dispatch_task_id(&identity, 1),
                    &handover_event(serde_json::json!([])),
                )
                .await
                .unwrap();
        }
        let readback = read_handover_products(&events, &identity).await.unwrap();
        assert_eq!(
            readback.generations,
            vec![0, 1],
            "every generation that left events is one of the answers, in order"
        );
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
    ///
    /// Both enumerations are published here because one metric carries both: the
    /// pass's own counts and what the handovers had left when it read them. The
    /// set asserted below is therefore their union, which is also what the panel
    /// selects over -- a cell written under this name and left out of both
    /// enumerations would be a series with no reader that nothing counts.
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
            delivered: None,
            unmeasurable: 5,
            resume_at_unix: 0,
            evidenced_recovery_unix: 0,
            quota_window_secs: 0,
            sources_unread: Some(String::new()),
            window_pace: None,
        };
        let readback = HandoverCensus {
            events: 40,
            products: 6,
            unreadable: 1,
        };
        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
        let metrics: Arc<dyn MetricsBackend> = metrics.clone();
        // The account's own counts, as its census and its band count would
        // answer them. The two partitions are of the same set, and that set is
        // larger than the pass's pending column on purpose: twelve rows are
        // `undecided`, so a reader of the pass's own columns alone would see
        // 148 owed and miss them.
        let owed = [
            (ChangePriority::P0, 4usize),
            (ChangePriority::P1, 2),
            (ChangePriority::P2, 0),
            (ChangePriority::P3, 1),
            (ChangePriority::Unstated, 153),
        ];
        let census = [
            (ChangeState::Conflict, 0u64),
            (ChangeState::Pending, 148),
            (ChangeState::Partial, 0),
            (ChangeState::Undecided, 12),
            (ChangeState::Landed, 900),
            (ChangeState::Retired, 40),
        ];
        // The demand side: what the documents asked for, and how much of it the
        // account holds as done. A pair, and the test carries a non-trivial pair
        // because a cell that only ever reads zero cannot tell a reading from an
        // absent one.
        let demand = DemandClosure {
            satisfied: 41,
            owed: 2085,
        };
        // What the account is holding in front of itself: eleven changes have
        // delivered a claim the tree has not confirmed, and 37 other rows are
        // measured at the revisions they delivered. Two conflicting faces as
        // well, because a cell that only ever reads zero cannot tell a reading
        // from an absent one.
        let blocking = BlockingSurface {
            holders: 11,
            held_landings: 37,
            block_conflicts: 2,
        };
        publish_dispatch_rows(
            Some(&metrics),
            &state,
            Some(&owed),
            Some(&census),
            Some(demand),
            Some(blocking),
        )
        .await;
        publish_dispatch_verdict(Some(&metrics), &state).await;
        publish_handover_readback(Some(&metrics), &readback).await;

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
                .map(|readout| readout.as_str())
                .chain(HandoverReadout::ALL.iter().map(|readout| readout.as_str()))
                .map(str::to_string)
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
        // The readback's own cells, and the difference between the two that is
        // the whole reason there are two: 40 changes still have handovers on
        // record and 6 of them left a deliverable, so the reading says 34 ran
        // and recorded nothing -- which is not the same statement as a change
        // nothing was ever handed to, and that one is the 0 in the first cell.
        assert_eq!(published["handover_events"], 40.0);
        assert_eq!(published["handover_products"], 6.0);
        assert_eq!(published["handover_unreadable"], 1.0);
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
        // The ledger's pending set as the account holds it: the rows this pass
        // could hand over plus the ones it could not settle. Read off the two
        // halves rather than recomputed from the rows, so the face's number and
        // the pass's own split cannot disagree.
        assert_eq!(published["change_ledger_pending"], 148.0);
        // The demand pair, and the reason both cells are published together:
        // 41 answered against 2085 still owed is a loop that closes slowly, and
        // the satisfied count alone would read exactly like a loop that closed
        // every demand it was ever given.
        assert_eq!(published["demand_satisfied"], 41.0);
        assert_eq!(published["demand_owed"], 2085.0);
        // What is holding the line, and how much is behind it: eleven changes
        // delivered and still owed, holding 37 rows measured at their
        // revisions, and two rows whose documents name different faces. The
        // three are written together, so a reader never sees a held count with
        // no holder count beside it.
        assert_eq!(published["blocking_rows"], 11.0);
        assert_eq!(published["held_landings"], 37.0);
        assert_eq!(published["block_conflicts"], 2.0);
        // The band split, and the relation that makes it a reading rather than
        // five numbers: the bands partition the owed set, so they have to add up
        // to the state cells' total. A band that stopped being published, or a
        // rank the count dropped, shows up here as a sum that no longer closes.
        assert_eq!(published["owed_p0"], 4.0);
        assert_eq!(published["owed_p1"], 2.0);
        assert_eq!(
            published["owed_p2"], 0.0,
            "a band with no rows is still a cell"
        );
        assert_eq!(published["owed_p3"], 1.0);
        assert_eq!(published["owed_unstated"], 153.0);
        // The census, one cell per outstanding state. The two empty states are
        // published as zeros rather than left out: an absent cell and a state
        // nobody counted read the same, and this is the face that has to say
        // which one it is.
        assert_eq!(published["state_conflict"], 0.0);
        assert_eq!(published["state_pending"], 148.0);
        assert_eq!(published["state_partial"], 0.0);
        assert_eq!(published["state_undecided"], 12.0);
        assert_eq!(
            published["owed_p0"]
                + published["owed_p1"]
                + published["owed_p2"]
                + published["owed_p3"]
                + published["owed_unstated"],
            published["state_conflict"]
                + published["state_pending"]
                + published["state_partial"]
                + published["state_undecided"],
            "the bands and the states are two partitions of one owed set"
        );
        // The pass's pending cell and the account's `pending` state are the
        // same population read two ways -- the rows this pass could hand over
        // plus the ones it could not settle -- and a disagreement between them
        // is the split losing a row.
        assert_eq!(
            published["change_ledger_pending"], published["state_pending"],
            "the pass's split and the account's census count the same rows"
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

    /// A pass that could not count the account publishes no band cell and no
    /// state cell, rather than publishing zeros.
    ///
    /// A zero is not a neutral placeholder here: `owed_p0 = 0` says the urgent
    /// work is done, and `state_undecided = 0` says every check can be settled;
    /// either is an answer a reader will act on. A skipped cell leaves the
    /// previous tick's count standing, and every write carries its own stamp, so
    /// "how old is this number" is readable. The pass says it could not count in
    /// its own log line.
    #[tokio::test]
    async fn a_pass_that_could_not_count_the_account_publishes_no_count_of_it() {
        let state = DispatchState {
            decision: DispatchDecision::Proceed,
            pool: PoolReading::Available,
            pending: 0,
            submitted: 0,
            already: 0,
            failed: 0,
            dead_environment: 0,
            held_back: 0,
            rearmed: 0,
            delivered: None,
            unmeasurable: 0,
            resume_at_unix: 0,
            evidenced_recovery_unix: 0,
            quota_window_secs: 0,
            sources_unread: Some(String::new()),
            window_pace: None,
        };
        let metrics: Arc<dyn MetricsBackend> = Arc::new(cog_storage::MemoryMetricsBackend::new());
        publish_dispatch_rows(Some(&metrics), &state, None, None, None, None).await;

        let end = Utc::now() + chrono::Duration::seconds(1);
        let start = end - chrono::Duration::seconds(600);
        let samples = metrics
            .query_gauge_range(metric_names::CHANGE_DISPATCH_READOUTS.as_str(), start, end)
            .await
            .expect("the reading face is readable");
        let names: Vec<String> = samples
            .iter()
            .filter_map(|sample| sample.labels.get("readout").cloned())
            .collect();
        assert!(
            names.iter().all(|name| !name.starts_with("owed_")
                && !name.starts_with("state_")
                && !name.starts_with("demand_")
                // Named rather than matched by prefix: `held_back` is a cell of
                // the pass's own counts, and a prefix rule would take it for one
                // of the blocking cells and pass on a cell that was written.
                && name != "blocking_rows"
                && name != "held_landings"
                && name != "block_conflicts"),
            "no band, census, demand or blocking cell is written when the count failed: \
             {names:?}"
        );
        assert!(
            names.iter().any(|name| name == "pending"),
            "the pass's own counts are still published: {names:?}"
        );
    }

    fn commit(rev: &str, body: &str) -> String {
        format!("{rev}\u{1f}{body}\u{1f}\u{1e}")
    }

    #[test]
    fn a_landing_trailer_names_the_change_and_the_commit_that_carried_it() {
        let log = [
            commit(
                "aaaa",
                "fix(x): something\n\nLanding change change-ledger-0123456789abcdef-abcd\n",
            ),
            commit("bbbb", "docs: nothing landed here\n"),
        ]
        .concat();
        let landed = landing_trailers(&log);
        assert_eq!(
            landed.get("change-ledger-0123456789abcdef-abcd"),
            Some(&"aaaa".to_string())
        );
        assert_eq!(landed.len(), 1, "a body with no trailer names nothing");
    }

    #[test]
    fn a_landing_from_before_the_trailer_existed_is_still_a_delivery() {
        // Every landing already on the tree was written by the generation that
        // named the change in a `Change-Id:` trailer and in the subject, not in
        // a `Landing change` line. A reader that knew only the current form
        // would read the entire history as never delivered.
        let log = commit(
            "ffff",
            "chore(cogneva): land change change-ledger-0123456789abcdef-abcd\n\n\
             Change-Id: change-ledger-0123456789abcdef-abcd\n\
             Signed-off-by: Luna#1822ab0e <luna-1822ab0e@cogneva.ai>\n",
        );
        assert_eq!(
            landing_trailers(&log)
                .get("change-ledger-0123456789abcdef-abcd")
                .map(String::as_str),
            Some("ffff")
        );
    }

    #[test]
    fn a_trailer_shaped_line_in_the_rationale_names_a_change_too() {
        // The rationale is free text. It is read like any other body line on
        // purpose: the id has to match a ledger row to matter, and a producer
        // quoting the trailer it was told to write is still naming that id.
        let log = commit(
            "gggg",
            "fix(x): something\n\n## Rationale\n\nLanding change change-ledger-0123456789abcdef-abcd\n",
        );
        assert_eq!(
            landing_trailers(&log)
                .get("change-ledger-0123456789abcdef-abcd")
                .map(String::as_str),
            Some("gggg")
        );
    }

    #[test]
    fn a_change_landed_twice_is_attributed_to_the_commit_the_log_names_first() {
        // The log is read newest first, so the first sighting is the later
        // commit; taking the last would date the delivery to a superseded try.
        let log = [
            commit(
                "newer",
                "Landing change change-ledger-0123456789abcdef-abcd\n",
            ),
            commit(
                "older",
                "Landing change change-ledger-0123456789abcdef-abcd\n",
            ),
        ]
        .concat();
        assert_eq!(
            landing_trailers(&log)
                .get("change-ledger-0123456789abcdef-abcd")
                .map(String::as_str),
            Some("newer")
        );
    }

    #[test]
    fn a_re_armed_handover_still_names_the_change_it_was_dispatched_under() {
        let identity = ChangeIdentity::new("grep", "crates/x", "fn thing");
        let mut deliveries = HashMap::new();
        deliveries.insert(dispatch_task_id(&identity, 1), "ccccc".to_string());
        assert_eq!(
            delivered_revision(&identity, &deliveries),
            Some("ccccc"),
            "the generation is appended to the id, so equality would miss every re-armed \
             handover and read a delivered change as undelivered"
        );
    }

    #[test]
    fn another_changes_delivery_does_not_name_this_row() {
        let identity = ChangeIdentity::new("grep", "crates/x", "fn thing");
        let other = ChangeIdentity::new("grep", "crates/x", "fn other");
        let mut deliveries = HashMap::new();
        deliveries.insert(dispatch_task_id(&other, 0), "ddddd".to_string());
        assert_eq!(delivered_revision(&identity, &deliveries), None);
    }

    /// A window row built from the fields the reading cares about, with the
    /// timestamps every test here keeps the same: the window was requested an
    /// hour ago, approved a minute ago, and expires in an hour unless the test
    /// says otherwise.
    fn window(writer: &str, approver: &str, scope: &str) -> WriteWindow {
        let now = Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap();
        WriteWindow {
            window_id: format!("{writer}:{scope}"),
            path_scope: scope.to_string(),
            writer: writer.to_string(),
            approver: approver.to_string(),
            requested_at: now - chrono::Duration::hours(1),
            approved_at: Some(now - chrono::Duration::minutes(1)),
            expires_at: Some(now + chrono::Duration::hours(1)),
            revoked_at: None,
        }
    }

    fn reading_at_noon(windows: &[WriteWindow]) -> WriteWindowReading {
        let now = Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap();
        write_window_reading(windows, now)
    }

    #[test]
    fn a_window_approved_by_its_writer_is_counted_apart_and_never_as_approved() {
        // The row says it was approved and the approver is the writer. It is
        // refused by the approval rule and still counted, because a row refused
        // and left invisible is a row nobody goes and corrects.
        let reading = reading_at_noon(&[window("operator", "operator", "crates/x")]);
        assert_eq!(
            reading.approved, 0,
            "a self-approval cannot be what the reading counts as an approved window"
        );
        assert_eq!(
            reading.self_approved, 1,
            "the refused row has to be nameable, or the refusal happens where nobody can see it"
        );
    }

    #[test]
    fn a_row_that_is_both_approved_and_revoked_is_counted_rather_than_read_as_revoked() {
        let mut row = window("worker", "operator", "crates/x");
        row.revoked_at = Some(Utc.with_ymd_and_hms(2026, 10, 20, 11, 0, 0).unwrap());
        let reading = reading_at_noon(&[row]);
        assert_eq!(
            reading.inconsistent, 1,
            "a window that stood and was then withdrawn is neither a window that stands nor \
             a request: it has a cell of its own so the withdrawal is not read as a request \
             that never had an approval"
        );
        assert_eq!(
            reading.approved, 0,
            "a row revoked at all is not a window that stands"
        );
    }

    #[test]
    fn two_writers_on_one_scope_are_an_overlap_and_two_scopes_are_not() {
        let one_scope = reading_at_noon(&[
            window("worker-a", "operator", "crates/x"),
            window("worker-b", "operator", "crates/x"),
        ]);
        assert_eq!(
            one_scope.overlap, 1,
            "two accounts writing under one scope is the overlap the face asks about, counted \
             once for the scope and not once per row"
        );
        assert_eq!(one_scope.approved, 2);

        let two_scopes = reading_at_noon(&[
            window("worker-a", "operator", "crates/x"),
            window("worker-b", "operator", "crates/y"),
        ]);
        assert_eq!(
            two_scopes.overlap, 0,
            "two windows on two scopes are two permissions, not one conflict"
        );
    }

    #[test]
    fn a_revoked_expired_or_unbounded_window_is_not_approved() {
        let now = Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap();

        let mut revoked = window("worker", "operator", "crates/x");
        revoked.revoked_at = Some(now - chrono::Duration::minutes(5));
        let mut expired = window("worker", "operator", "crates/y");
        expired.expires_at = Some(now - chrono::Duration::minutes(5));
        let mut unbounded = window("worker", "operator", "crates/z");
        unbounded.expires_at = None;
        let mut unnamed = window("worker", "", "crates/w");
        unnamed.approver = String::new();
        let mut unstamped = window("worker", "operator", "crates/v");
        unstamped.approved_at = None;

        let reading = reading_at_noon(&[revoked, expired, unbounded, unnamed, unstamped]);
        assert_eq!(
            reading.approved, 0,
            "each row is a way a permission can look approved without being one: revoked, \
             expired, unbounded, approved by nobody, and never approved at all"
        );
        assert_eq!(
            reading.inconsistent, 1,
            "only the revoked row carries both stamps; the other four were never withdrawn, \
             and a cell that counted them too would not be reading the record"
        );
        assert_eq!(reading.self_approved, 0);
        assert_eq!(
            reading.registered, 5,
            "five rows were handed over, so a zero on the approval cell is a \
             measurement of those five and not the shape of a table nobody writes"
        );
    }

    #[test]
    fn a_table_that_can_be_read_and_holds_nothing_is_a_reading_of_zero() {
        // Zero is a measurement -- no window is approved -- and it must not be
        // published as an absence. The absent case is `Ok(None)` and a failed
        // read, which produce no reading at all.
        let now = Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap();
        let empty = write_window_reading_from(&Ok(Some(Vec::new())), now)
            .expect("a readable empty table is a measurement, not an absent reading");
        assert_eq!(empty, WriteWindowReading::default());
        assert_eq!(empty.approved, 0);
        assert_eq!(
            empty.registered, 0,
            "a readable table with no rows is the one case where approved=0 does \
             mean nothing was ever recorded, and registered is the cell that says so"
        );

        assert_eq!(
            write_window_reading_from(&Ok(None), now),
            None,
            "a store with no window table has not measured anything, so publishing zero would \
             read as 'no window is approved'"
        );
        assert_eq!(
            write_window_reading_from(&Err(SFError::Database("relation missing".into())), now),
            None,
            "a failed read is not a table that holds nothing"
        );
    }
}
