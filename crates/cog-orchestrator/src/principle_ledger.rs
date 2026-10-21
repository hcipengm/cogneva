//! principle: feedback_an_ordering_key_written_when_a_class_was_unwritable_starves_it_once_one_can_be_written
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
//! Four things happen here, and the third is the only one that moves a row. The
//! loop folds the intake into the account, reporting by name any principle whose
//! carrier's bytes moved since it was last measured and holding those back from
//! measurement for the pass. It reads what is outstanding back out, with the
//! census of the whole account beside it, because "how much of the corpus has a
//! gate" is the reading this account exists to produce. It looks at the
//! outstanding set against both registries, through the one call that may move a
//! state. And it reports the reverse direction -- declarations in the platform's
//! own tree that name no row of the account -- by name, because a mistyped slug
//! and a principle the account never heard of are one count and two fixes.
//!
//! What this account is *about* is the corpus of principles: whether each one has
//! a gate, which is the reading it exists to produce. The work of *building* the
//! gates is not its subject. A principle with no gate is "no criterion has been
//! designed for this yet", a thing whose endpoint is a blank, so a task for it
//! could name only a slug -- and the rows that can name an endpoint are the
//! platform's own registries of owed work, which are read by the ledger that owns
//! those rows. Reading a registry here and handing it to a goal path of its own
//! would be a second answer to "who owes what" beside the account's.
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
    metric_names, MetricsBackend, OwnerLeaseBroker, PrincipleCensus, PrincipleCheckKind,
    PrincipleCheckKindCensus, PrincipleLedgerStore, PrincipleMeasurement, PrincipleOrphan,
    PrincipleRow, PrincipleState, ShutdownSignal,
};

use crate::change_measure::CheckTree;
use crate::principle_measure::{
    drift_annotation, judge, measurable, platform_declaration_pattern, Declarations, DeclaredTwice,
    UnsettledReason, Verdict, PLATFORM_REGISTRY_FILES,
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

/// How many refused rows one pass publishes.
///
/// The selector already bounds the population to the rows the account holds
/// with `refusals > 0`, which today number nine. The bound is deliberately
/// larger than that rather than tuned to it: a bound below the population would
/// truncate the reading by the ordering instead of by what the gates refused,
/// and the total a reader adds up would then disagree with the account for a
/// reason nothing on the surface states.
const REFUSAL_ROWS_PER_PASS: usize = 1000;

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

/// How many human-held rows one pass may look at for the first time.
///
/// A lane of its own. The judgement tier sorts behind every row a pass could
/// settle, and the debt is deeper than one pass takes, so these rows are not
/// reachable through the batch at all -- the account held more than five hundred
/// of them and no look had ever reached one. The bound is small enough that the
/// debt still gets most of a pass, and large enough that the first sweep clears
/// in hours rather than weeks.
const HUMAN_ONLY_SWEEP_PER_PASS: usize = 50;

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
/// `tree` is the project's own source, read as a git repository: the two
/// registries a look is taken against, and the declarations that name no row.
pub fn spawn_principle_ledger_loop(
    store: Arc<dyn PrincipleLedgerStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    shutdown: ShutdownSignal,
    tree: CheckTree,
    metrics: Option<Arc<dyn MetricsBackend>>,
) {
    info!(
        interval_secs = PRINCIPLE_LEDGER_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        "Principle ledger loop enabled"
    );

    drop(cog_core::loop_health::spawn(
        PRINCIPLE_LEDGER_LOOP,
        cog_core::loop_health::Cadence::Periodic(PRINCIPLE_LEDGER_PASS_INTERVAL),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            let tree = tree.clone();
            let metrics = metrics.clone();
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
                    read_pass(&*store, metrics.as_ref()).await;
                    measure_pass(&*store, &tree, &drift, &mut unsettled).await;
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
async fn read_pass(store: &dyn PrincipleLedgerStore, metrics: Option<&Arc<dyn MetricsBackend>>) {
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
        Ok(census) => {
            info!(account = %account_summary(&census), "principle ledger census");
            // The same numbers printed above, published: a log line is a reading
            // that dies with the pod, and the whole point of this account is a
            // figure somebody can watch without asking a human to run a query.
            // A failed write is not raised -- the next pass rewrites every cell
            // -- but it is not silent either, because a series that stopped
            // moving and a series nobody ever wrote look identical from outside.
            publish_census(metrics, &census).await;
        }
        Err(e) => warn!(error = %e, "principle ledger: the census could not be read"),
    }

    // What the gates refused, by the principle they refused it for. The account
    // has counted this per row since refusals were first reported, and until now
    // the count was only reachable by running a query: the census above answers
    // "how much of the corpus has a gate", never "which rule keeps stopping
    // work". Read every pass, like the census, so an absent series is a loop that
    // stopped publishing rather than a gate that stopped refusing.
    match store.refused(REFUSAL_ROWS_PER_PASS).await {
        Ok(rows) if rows.is_empty() => {
            debug!("principle ledger refusals: no principle has been refused");
        }
        Ok(rows) => {
            info!(
                rows = rows.len(),
                refusals = rows.iter().map(|row| row.refusals).sum::<i64>(),
                "principle ledger refusals"
            );
            publish_refusals(metrics, &rows).await;
        }
        Err(e) => warn!(error = %e, "principle ledger: the refusal counts could not be read"),
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

/// The census as one series per answer, the whole closed set written every pass.
///
/// `unmeasured` is written here even though it is not a value the state column
/// may take -- those are the rows whose state is NULL, and a fifth cell is
/// exactly what makes the four add up to the account. Writing only the three
/// states would leave "how much of the corpus has nothing looked at" as the one
/// number a reader has to derive by subtracting, and a derived number cannot
/// tell a zero from a read that never happened.
///
/// Zeroes are written for the same reason: the alternative is a series that
/// appears the first time a state is reached, so an operator watching a corpus
/// that is nearly all enforced would read an unwritten cell as "no data" when it
/// means "none right now".
async fn publish_census(metrics: Option<&Arc<dyn MetricsBackend>>, census: &PrincipleCensus) {
    let Some(metrics) = metrics else {
        return;
    };
    let mut cells: Vec<(&'static str, u64)> = Vec::with_capacity(PrincipleState::ALL.len() + 1);
    for state in PrincipleState::ALL {
        let count = match state {
            PrincipleState::Enforced => census.enforced,
            PrincipleState::Unenforced => census.unenforced,
            PrincipleState::Retired => census.retired,
        };
        cells.push((state.as_str(), count));
    }
    cells.push(("unmeasured", census.unmeasured));

    for (state, count) in cells {
        // Each cell also names itself a reading (`readout=principle_ledger_<state>`),
        // and that is not decoration: the face roster names **one** reading per face
        // and prints whatever comes back, so a bare metric name carrying four
        // labelled series arrives there as four unlabelled numbers in one cell. The
        // roster's reading lookup already resolves a name by this label, so the
        // per-state reading is addressable without teaching the roster a selector.
        let reading = ["principle_ledger_", state].concat();
        let labels = HashMap::from([
            ("state".to_string(), state.to_string()),
            ("readout".to_string(), reading),
        ]);
        if let Err(e) = metrics
            .record_gauge(metric_names::PRINCIPLE_LEDGER_ROWS, count as f64, labels)
            .await
        {
            warn!(
                error = %e,
                state,
                "principle ledger: the census could not be published"
            );
        }
    }
}

/// One series per refused principle, the account's cumulative count as the value.
///
/// Only the rows the account holds with `refusals > 0` are written, and that is
/// deliberate in the opposite direction from the census: the census writes a
/// zero for an answer the corpus holds none of, because the four cells have to
/// add up to the account. Here a zero is not a reading -- a slug with no series
/// has never been refused -- and writing zeroes for the whole corpus would make
/// the series population the corpus size rather than the number of principles a
/// gate has actually stopped something for.
///
/// Each series names itself a reading (`readout=principle_ledger_refusals`), the
/// same way the census cells do: a reader that takes one reading's name and
/// prints what comes back would otherwise get one cell per slug with nothing to
/// say which question they all answer.
async fn publish_refusals(metrics: Option<&Arc<dyn MetricsBackend>>, rows: &[PrincipleRow]) {
    let Some(metrics) = metrics else {
        return;
    };
    for row in rows {
        let labels = HashMap::from([
            ("slug".to_string(), row.slug.clone()),
            (
                "readout".to_string(),
                "principle_ledger_refusals".to_string(),
            ),
        ]);
        if let Err(e) = metrics
            .record_gauge(
                metric_names::PRINCIPLE_REFUSALS,
                row.refusals as f64,
                labels,
            )
            .await
        {
            warn!(
                error = %e,
                slug = %row.slug,
                "principle ledger: a refusal count could not be published"
            );
        }
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
            &platform_declaration_pattern(),
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
    // A retired row has no gate to record: the principle is no longer held, so a
    // declaration that still names it is about a row this account has stopped
    // asking about. Writing `None` clears what the last live measurement found
    // rather than carrying a gate beside a retirement.
    if state != cog_core::PrincipleState::Retired {
        measurement.platform_gate = declarations.gate_for(&row.slug).map(str::to_string);
    }
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
    let mut rows = match store.outstanding(take).await {
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

    // A row a look has settled leaves the outstanding set for good, and folding
    // a reading in cannot put it back: only a measurement writes `state`. So a
    // gate the platform gains after the row was settled would never be recorded
    // against it, and the account would go on reporting "the platform names no
    // gate here" through every pass for a principle the platform now declares.
    // That is the exact shape of moving a gate onto the platform -- the host's
    // side is already settled, so the move would be invisible on its own rows.
    //
    // The comparison is against what the row records, not against the tree's
    // revision, so a row is asked again once per change rather than once per
    // commit.
    let gates: HashMap<String, String> = declarations
        .slugs()
        .filter_map(|slug| {
            declarations
                .gate_for(slug)
                .map(|file| (slug.to_string(), file.to_string()))
        })
        .collect();
    match store.reopen_platform_gate_drift(&gates).await {
        Ok(reopened) if reopened.is_empty() => {}
        Ok(reopened) => {
            info!(
                reopened = reopened.len(),
                principles = %reopened.join(", "),
                "principle ledger: the registry names a gate for rows whose recorded gate is \
                 not that file, so they are asked again"
            );
            // The rows just re-opened were not in the set read before the
            // registry was, so this pass would otherwise not judge them until
            // the next one.
            rows = match store.outstanding(take).await {
                Ok(rows) => rows,
                Err(e) => {
                    warn!(
                        error = %e,
                        "principle ledger: the outstanding set could not be read after \
                         re-opening rows, so this pass judged none"
                    );
                    return;
                }
            };
        }
        Err(e) => warn!(
            error = %e,
            "principle ledger: rows whose recorded platform gate is not the registry's could \
             not be re-opened; they keep what they had"
        ),
    }

    // The batch is debt first, and the judgement tier sorts behind all of it.
    // That order was written when no judgement row could be settled: a row a
    // person holds writes nothing, so it would sit at the head of every pass
    // forever and take the bounded batch with it. A declaration is the exception
    // -- a judgement row the platform's registry names *is* settleable -- and the
    // ordering has no way to know which tier a file names, so such a row is
    // never in the batch while the debt runs deeper than one pass takes, which is
    // every pass: the declaration would be read every five minutes and recorded
    // never. So the declared slugs the batch did not offer are asked for by slug,
    // and the ones that are an unsettled judgement row go to the head of this
    // pass's work.
    let offered: HashSet<&str> = rows.iter().map(|row| row.slug.as_str()).collect();
    let declared: Vec<String> = declarations.slugs().map(str::to_string).collect();
    let mut rescued: Vec<PrincipleRow> = Vec::new();
    let mut declared_unread: Vec<String> = Vec::new();
    for slug in &declared {
        if offered.contains(slug.as_str()) {
            continue;
        }
        match store.get(slug).await {
            Ok(Some(row))
                if row.check_kind == Some(PrincipleCheckKind::Judgement)
                    && row.state != Some(PrincipleState::Enforced) =>
            {
                rescued.push(row);
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    slug = %slug,
                    error = %e,
                    "principle ledger: a principle the registry declares could not be read back"
                );
                declared_unread.push(slug.clone());
            }
        }
    }
    if !declared_unread.is_empty() {
        warn!(
            unread = declared_unread.len(),
            principles = %declared_unread.join(", "),
            "principle ledger: some declared principles could not be read back, so whether each \
             is one a person holds is unanswered this pass"
        );
    }
    let declared_judgement = rescued.len();

    // The human-held rows no look has reached are not in the batch either, for
    // the same reason: the tier sorts behind the debt and the debt is deeper
    // than one pass. Asking for them by their own query is what gives the
    // answer "no gate is meant to exist" a first look instead of leaving it
    // indistinguishable from a row nothing has ever seen. A row that was looked
    // at before the verdict learned to read its host column arrives the same
    // way: its stored gate is visible, the verdict it should now produce is not,
    // and only another look writes it.
    let sweep = match store.unsettled_judgement(HUMAN_ONLY_SWEEP_PER_PASS).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                error = %e,
                "principle ledger: the judgement rows their empty state has kept out of the \
                 batch could not be read, so this pass looks at none of them"
            );
            Vec::new()
        }
    };
    let sweep: Vec<PrincipleRow> = sweep
        .into_iter()
        .filter(|row| {
            !offered.contains(row.slug.as_str()) && !rescued.iter().any(|r| r.slug == row.slug)
        })
        .collect();
    let swept_offered = sweep.len();

    // In front rather than behind: the batch is bounded, and a row appended past
    // that bound would be read and never judged -- the same silence this is
    // here to end.
    rows.splice(0..0, rescued);
    rows.splice(0..0, sweep);

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

    // Two files claiming one slug is not a defect in either of them -- a
    // principle may be asserted in more than one place -- but the map keys on
    // the slug, so the account would record one carrier where the tree has two.
    // The pair is printed rather than folded: which file owns the claim is a
    // person's call, and a silent overwrite leaves them nothing to call it on.
    let duplicate_declarations = declarations.duplicate_count();
    if duplicate_declarations != 0 {
        warn!(
            duplicate_declarations,
            duplicates = %duplicate_summary(declarations.duplicates()),
            "principle ledger: more than one file declares the same principle, so the account \
             records only the last one in tree order"
        );
    }

    let now = Instant::now();
    let mut enforced = 0usize;
    let mut unenforced = 0usize;
    let mut retired = 0usize;
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
            // Stamped rather than skipped. A row a pass has looked at is not a
            // row nothing has looked at, and leaving `measured_at` empty made
            // those two read the same in every query. The state is left empty on
            // purpose: "no gate is meant to exist" is not one of the three.
            if let Err(e) = store.record_human_only(&row.slug, Some(&revision)).await {
                warn!(
                    slug = %row.slug,
                    error = %e,
                    "principle ledger: a human-held look could not be written back"
                );
            }
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
            // Only reachable when the host says the carrier left and a later
            // reading brought it back inside the same pass; counted so the line
            // adds up to the rows it judged.
            PrincipleState::Retired => retired += 1,
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
        retired,
        human_only,
        held_back_for_drift,
        unsettled = %reason_summary(&by_reason),
        already_unsettled = known_unsettled,
        orphan_declarations = ?orphan_declarations,
        declared_judgement,
        swept_offered,
        duplicate_declarations,
        revision = %revision,
        "principle ledger: judged the outstanding set against the two registries"
    );
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
        "enforced {} unenforced {} unmeasured {} retired {} total {} coverage {}",
        census.enforced,
        census.unenforced,
        census.unmeasured,
        census.retired,
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

/// The slugs two files claim, one per line, with both files named.
///
/// The kept file first: it is the one the account records, so a reader who
/// trusts that record can see which claim the tree actually carries and which
/// one the map dropped.
fn duplicate_summary(duplicates: &[DeclaredTwice]) -> String {
    duplicates
        .iter()
        .map(|duplicate| {
            format!(
                "{}:{}<-{}",
                duplicate.slug, duplicate.kept, duplicate.dropped
            )
        })
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
            where_red: None,
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
            retired: 0,
        };
        assert_eq!(
            account_summary(&census),
            "enforced 5 unenforced 556 unmeasured 0 retired 0 total 561 coverage 0.9%"
        );

        let partly = PrincipleCensus {
            enforced: 1,
            unenforced: 0,
            unmeasured: 2,
            retired: 0,
        };
        assert_eq!(
            account_summary(&partly),
            "enforced 1 unenforced 0 unmeasured 2 retired 0 total 3 coverage 33.3%"
        );

        // A retired row is in the account but not in its coverage: counting it
        // would lower coverage every time a principle left the roster.
        let with_retired = PrincipleCensus {
            enforced: 1,
            unenforced: 1,
            unmeasured: 1,
            retired: 7,
        };
        assert_eq!(
            account_summary(&with_retired),
            "enforced 1 unenforced 1 unmeasured 1 retired 7 total 3 coverage 33.3%"
        );
    }

    /// An account holding nothing has no coverage, and a zero would read as an
    /// account whose every principle is ungated.
    #[test]
    fn an_empty_account_has_no_coverage_rather_than_none() {
        assert_eq!(
            account_summary(&PrincipleCensus::default()),
            "enforced 0 unenforced 0 unmeasured 0 retired 0 total 0 coverage no rows"
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

    /// One slug claimed by two files reads as both files, kept first: a reader
    /// trusting the account's carrier must be able to see which claim the map
    /// dropped, or the overwrite is invisible exactly where it matters.
    #[test]
    fn the_duplicate_line_names_both_files_and_the_one_kept() {
        let duplicates = vec![DeclaredTwice {
            slug: "feedback_x".into(),
            kept: "crates/x/tests/b.rs".into(),
            dropped: "crates/x/tests/a.rs".into(),
        }];
        assert_eq!(
            duplicate_summary(&duplicates),
            "feedback_x:crates/x/tests/b.rs<-crates/x/tests/a.rs"
        );
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

    /// A retirement writes no gate, even when the platform's registry still
    /// names one: the row is no longer a question about gates, and carrying a
    /// gate beside it would keep answering "enforced somewhere" for a principle
    /// the host's roster has dropped.
    #[test]
    fn a_retirement_carries_no_gate() {
        let registry = declarations(&[("crates/cogneva/tests/a.rs", "//! principle: feedback_x")]);
        let judged = row("feedback_x", Some("retired:carrier_not_in_tree"), None);
        let verdict = judge(&judged, &registry);
        let retired =
            measurement_of(&judged, &verdict, &registry, "rev1").expect("a retirement is a state");
        assert_eq!(retired.state, PrincipleState::Retired);
        assert_eq!(retired.platform_gate, None);
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

    // ── The order inside a pass, not the store ──
    //
    // The store's half of the re-open is tested where it is written; what only
    // this file can get wrong is the sequence. The batch this pass will judge is
    // read *before* the registry is, so a row re-opened afterwards is not in the
    // batch already in hand. Re-opening a row and then judging the old batch
    // would leave it settled for another whole interval while the log says it was
    // asked again -- the reading and the work would part company, and no count on
    // the pass line would show it.
    //
    // Driven over a real repository, because the declaration the re-open turns on
    // is read out of one.

    /// A bare repository whose one Rust source declares `slug` at its head, plus
    /// a work tree to write it from: the shape the platform's registry is read
    /// out of.
    fn repo_declaring(slug: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let bare = dir.path().join("bare");
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).expect("the scratch worktree");
        let bare_s = bare.to_str().expect("a path this host can spell");
        let work_s = work.to_str().expect("a path this host can spell");
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
        git(&["init", "-q", "--bare", "-b", "main", bare_s]);
        git(&["--git-dir", bare_s, "config", "user.email", "t@example.com"]);
        git(&["--git-dir", bare_s, "config", "user.name", "t"]);
        std::fs::create_dir_all(work.join("crates/x/tests")).expect("the fixture directory");
        std::fs::write(
            work.join("crates/x/tests/gate_test.rs"),
            format!("//! principle: {slug}\n"),
        )
        .expect("the fixture file");
        git(&["--git-dir", bare_s, "--work-tree", work_s, "add", "--all"]);
        git(&[
            "--git-dir",
            bare_s,
            "--work-tree",
            work_s,
            "commit",
            "-q",
            "-m",
            "fixture",
        ]);
        dir
    }

    /// A store with exactly the two behaviours the re-open path reads: the row it
    /// holds is not in the first outstanding set (a settled row is not offered)
    /// and is in every set after the re-open that put it back.
    #[derive(Default)]
    struct ReopeningStore {
        /// How many times the outstanding set was asked for. The second read is
        /// the whole subject of the test, and a pass that skips it leaves
        /// everything else looking identical.
        outstanding_reads: std::sync::atomic::AtomicUsize,
        /// The registry the re-open was asked about, as `slug -> file`.
        gates: std::sync::Mutex<HashMap<String, String>>,
        measured: std::sync::Mutex<Vec<PrincipleMeasurement>>,
        /// The slugs a look settled as human-held. Kept apart from `measured`
        /// because the whole point of this write is that it moves no state.
        human_only: std::sync::Mutex<Vec<String>>,
        /// Whether the batch offers the held row on its first read.
        ///
        /// The re-opening case needs a row the batch never offers; the
        /// human-held case needs one a pass reads like any other. Modelling
        /// both with one store keeps which shape a test exercises written down
        /// where the store is built.
        batch_offers_row: bool,
        row: std::sync::Mutex<Option<PrincipleRow>>,
    }

    #[async_trait::async_trait]
    impl PrincipleLedgerStore for ReopeningStore {
        async fn record_readings(
            &self,
            _: &[cog_core::PrincipleReading],
        ) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn declare(&self, _: &[cog_core::PrincipleReading]) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn fold_declarations(&self, _: usize) -> cog_core::SFResult<cog_core::PrincipleFold> {
            unimplemented!()
        }
        async fn pending_declarations(&self) -> cog_core::SFResult<u64> {
            unimplemented!()
        }
        async fn apply_principle_measurement(
            &self,
            measurement: &PrincipleMeasurement,
        ) -> cog_core::SFResult<()> {
            self.measured.lock().unwrap().push(measurement.clone());
            Ok(())
        }
        async fn record_human_only(&self, slug: &str, _: Option<&str>) -> cog_core::SFResult<()> {
            self.human_only.lock().unwrap().push(slug.to_string());
            Ok(())
        }
        async fn unsettled_judgement(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            Ok(Vec::new())
        }
        async fn outstanding(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            use std::sync::atomic::Ordering;
            let read = self.outstanding_reads.fetch_add(1, Ordering::SeqCst) + 1;
            if read == 1 && !self.batch_offers_row {
                Ok(Vec::new())
            } else {
                Ok(self.row.lock().unwrap().clone().into_iter().collect())
            }
        }
        async fn reopen_platform_gate_drift(
            &self,
            gates: &HashMap<String, String>,
        ) -> cog_core::SFResult<Vec<String>> {
            *self.gates.lock().unwrap() = gates.clone();
            let row = self.row.lock().unwrap();
            Ok(match row.as_ref() {
                Some(r) if gates.get(&r.slug) != r.platform_gate.as_ref() => {
                    vec![r.slug.clone()]
                }
                _ => Vec::new(),
            })
        }
        async fn refused(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            unimplemented!()
        }
        async fn get(&self, slug: &str) -> cog_core::SFResult<Option<PrincipleRow>> {
            // The one row this fake holds is the one a declared slug can be asked
            // about; every other slug is a principle the account has no row for,
            // which is what a real store answers.
            Ok(self
                .row
                .lock()
                .unwrap()
                .clone()
                .filter(|row| row.slug == slug))
        }
        async fn slugs(&self) -> cog_core::SFResult<Vec<String>> {
            Ok(self
                .row
                .lock()
                .unwrap()
                .as_ref()
                .map(|r| vec![r.slug.clone()])
                .unwrap_or_default())
        }
        async fn census(&self) -> cog_core::SFResult<PrincipleCensus> {
            unimplemented!()
        }
        async fn check_kind_census(&self) -> cog_core::SFResult<PrincipleCheckKindCensus> {
            unimplemented!()
        }
        async fn report_refusals(
            &self,
            _: &[cog_core::PrincipleRefusal],
        ) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn fold_refusals(&self, _: usize) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn pending_refusals(&self) -> cog_core::SFResult<u64> {
            unimplemented!()
        }
    }

    /// A row the registry has moved onto the platform is judged in the pass that
    /// re-opened it. Without the second read the row would stay settled for
    /// another interval while the pass line reported it as asked again.
    #[tokio::test]
    async fn a_row_reopened_by_the_registry_is_judged_in_the_same_pass() {
        use std::sync::atomic::Ordering;

        let slug = "feedback_example_gate";
        let dir = repo_declaring(slug);
        let tree = CheckTree::new(dir.path().join("bare"), "main");

        let mut settled = row(
            slug,
            Some(crate::principle_measure::HOST_REGISTRY_READ_NO_GATE),
            Some(PrincipleState::Enforced),
        );
        settled.platform_gate = Some("crates/old/tests/gate_test.rs".to_string());
        let store = ReopeningStore {
            row: std::sync::Mutex::new(Some(settled)),
            ..Default::default()
        };

        let mut unsettled = HashMap::new();
        measure_pass(&store, &tree, &HashSet::new(), &mut unsettled).await;

        // The order this file owns: the set is read before the registry, and read
        // again after the re-open.
        assert_eq!(
            store.outstanding_reads.load(Ordering::SeqCst),
            2,
            "the pass did not read the outstanding set again after re-opening a row"
        );
        // The registry the re-open was asked about is the tree's own declaration,
        // not an empty map: a pass that asked about nothing would re-open nothing.
        assert_eq!(
            store.gates.lock().unwrap().get(slug).map(String::as_str),
            Some("crates/x/tests/gate_test.rs")
        );

        let measured = store.measured.lock().unwrap();
        assert_eq!(
            measured.len(),
            1,
            "the row re-opened this pass was not judged until the next one"
        );
        assert_eq!(measured[0].slug, slug);
        assert_eq!(measured[0].state, PrincipleState::Enforced);
        assert_eq!(
            measured[0].platform_gate.as_deref(),
            Some("crates/x/tests/gate_test.rs")
        );
    }

    /// A judgement row neither registry names a gate for is settled as
    /// human-held, and that look is written back.
    ///
    /// The stamp is the whole subject: without it the row a person holds and the
    /// row nothing has ever looked at come out of every query identical, which
    /// is the reading this account exists to be able to tell apart.
    #[tokio::test]
    async fn a_human_held_look_is_stamped_without_moving_the_state() {
        let slug = "feedback_example_judgement";
        let dir = repo_declaring("feedback_some_other_principle");
        let tree = CheckTree::new(dir.path().join("bare"), "main");

        let mut held = row(slug, Some("-"), None);
        held.check_kind = Some(PrincipleCheckKind::Judgement);
        let store = ReopeningStore {
            batch_offers_row: true,
            row: std::sync::Mutex::new(Some(held)),
            ..Default::default()
        };

        let mut unsettled = HashMap::new();
        measure_pass(&store, &tree, &HashSet::new(), &mut unsettled).await;

        assert_eq!(
            store.human_only.lock().unwrap().as_slice(),
            [slug],
            "the human-held look was not written back"
        );
        assert!(
            store.measured.lock().unwrap().is_empty(),
            "a human-held row moved a state it does not have"
        );
    }

    /// A store whose outstanding set is all debt, and whose judgement row is
    /// only reachable by slug -- which is what the real selector does: the whole
    /// judgement tier sorts behind every unsettled row, and there is always more
    /// debt than one pass takes.
    #[derive(Default)]
    struct StarvedStore {
        measured: std::sync::Mutex<Vec<PrincipleMeasurement>>,
        asked: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl PrincipleLedgerStore for StarvedStore {
        async fn record_readings(
            &self,
            _: &[cog_core::PrincipleReading],
        ) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn declare(&self, _: &[cog_core::PrincipleReading]) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn fold_declarations(&self, _: usize) -> cog_core::SFResult<cog_core::PrincipleFold> {
            unimplemented!()
        }
        async fn pending_declarations(&self) -> cog_core::SFResult<u64> {
            unimplemented!()
        }
        async fn apply_principle_measurement(
            &self,
            measurement: &PrincipleMeasurement,
        ) -> cog_core::SFResult<()> {
            self.measured.lock().unwrap().push(measurement.clone());
            Ok(())
        }
        async fn record_human_only(&self, _: &str, _: Option<&str>) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn unsettled_judgement(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            Ok(Vec::new())
        }
        async fn outstanding(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            Ok(vec![row("feedback_debt", Some("scripts/hook.sh"), None)])
        }
        async fn reopen_platform_gate_drift(
            &self,
            _: &HashMap<String, String>,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn refused(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            unimplemented!()
        }
        async fn get(&self, slug: &str) -> cog_core::SFResult<Option<PrincipleRow>> {
            self.asked.lock().unwrap().push(slug.to_string());
            if slug != "feedback_declared_judgement" {
                return Ok(None);
            }
            let mut held = row(slug, Some("-"), None);
            held.check_kind = Some(PrincipleCheckKind::Judgement);
            Ok(Some(held))
        }
        async fn slugs(&self) -> cog_core::SFResult<Vec<String>> {
            Ok(vec![
                "feedback_debt".to_string(),
                "feedback_declared_judgement".to_string(),
            ])
        }
        async fn census(&self) -> cog_core::SFResult<PrincipleCensus> {
            unimplemented!()
        }
        async fn check_kind_census(&self) -> cog_core::SFResult<PrincipleCheckKindCensus> {
            unimplemented!()
        }
        async fn report_refusals(
            &self,
            _: &[cog_core::PrincipleRefusal],
        ) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn fold_refusals(&self, _: usize) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn pending_refusals(&self) -> cog_core::SFResult<u64> {
            unimplemented!()
        }
    }

    /// A judgement row the platform declares is settled by the pass that reads
    /// the declaration, even though the outstanding set never offers it.
    ///
    /// The tier sorts behind all debt by design, and the debt outnumbers one
    /// pass on every pass, so the batch would leave such a row unjudged for as
    /// long as the debt exists -- the declaration read every five minutes and
    /// the account answering "no gate" forever.
    #[tokio::test]
    async fn a_declared_judgement_row_is_judged_even_though_the_batch_never_offers_it() {
        let slug = "feedback_declared_judgement";
        let dir = repo_declaring(slug);
        let tree = CheckTree::new(dir.path().join("bare"), "main");
        let store = StarvedStore::default();

        measure_pass(&store, &tree, &HashSet::new(), &mut HashMap::new()).await;

        // Asked for by slug, because the batch did not carry it.
        assert!(
            store.asked.lock().unwrap().iter().any(|s| s == slug),
            "the pass never asked for the declared judgement row"
        );
        let measured = store.measured.lock().unwrap();
        assert_eq!(
            measured.first().map(|m| m.slug.as_str()),
            Some(slug),
            "the rescued row was judged behind the debt it was rescued from"
        );
        assert_eq!(measured[0].state, PrincipleState::Enforced);
        assert_eq!(
            measured[0].platform_gate.as_deref(),
            Some("crates/x/tests/gate_test.rs")
        );
    }

    /// One gauge write: its name, its value, and the labels it carried.
    type GaugeWrite = (String, f64, HashMap<String, String>);

    /// A metrics backend that keeps every gauge write, in order.
    #[derive(Default)]
    struct RecordingMetrics {
        gauges: std::sync::Mutex<Vec<GaugeWrite>>,
    }

    #[async_trait::async_trait]
    impl MetricsBackend for RecordingMetrics {
        async fn record_gauge(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.gauges
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value, labels));
            Ok(())
        }
        async fn record_counter(
            &self,
            _: cog_core::MetricName,
            _: f64,
            _: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn record_histogram(
            &self,
            _: cog_core::MetricName,
            _: f64,
            _: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn query_gauge_range(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_counter_range(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_gauge_latest(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_counter_totals(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn query_histogram_totals(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            unimplemented!()
        }
        async fn query_histogram_range(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            unimplemented!()
        }
        async fn list_metric_names(
            &self,
            _: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// A store that answers the three reads one pass makes and holds a census
    /// whose cells differ from each other and include a zero.
    struct CensusStore {
        census: PrincipleCensus,
        /// What this store says the account holds with `refusals > 0`.
        refused: Vec<(String, i64)>,
    }

    #[async_trait::async_trait]
    impl PrincipleLedgerStore for CensusStore {
        async fn record_readings(
            &self,
            _: &[cog_core::PrincipleReading],
        ) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn declare(&self, _: &[cog_core::PrincipleReading]) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn fold_declarations(&self, _: usize) -> cog_core::SFResult<cog_core::PrincipleFold> {
            unimplemented!()
        }
        async fn pending_declarations(&self) -> cog_core::SFResult<u64> {
            unimplemented!()
        }
        async fn apply_principle_measurement(
            &self,
            _: &PrincipleMeasurement,
        ) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn record_human_only(&self, _: &str, _: Option<&str>) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn unsettled_judgement(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            unimplemented!()
        }
        async fn outstanding(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            Ok(Vec::new())
        }
        async fn reopen_platform_gate_drift(
            &self,
            _: &HashMap<String, String>,
        ) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn refused(&self, _: usize) -> cog_core::SFResult<Vec<PrincipleRow>> {
            Ok(self
                .refused
                .iter()
                .map(|(slug, refusals)| PrincipleRow {
                    slug: slug.clone(),
                    lead: None,
                    carrier_path: None,
                    carrier_sha256: None,
                    host_gate: None,
                    where_red: None,
                    platform_gate: None,
                    check_kind: None,
                    state: None,
                    refusals: *refusals,
                    last_refused_at: None,
                    first_seen_at: chrono::Utc::now(),
                    measured_at: None,
                    measured_rev: None,
                })
                .collect())
        }
        async fn get(&self, _: &str) -> cog_core::SFResult<Option<PrincipleRow>> {
            unimplemented!()
        }
        async fn slugs(&self) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn census(&self) -> cog_core::SFResult<PrincipleCensus> {
            Ok(PrincipleCensus {
                enforced: self.census.enforced,
                unenforced: self.census.unenforced,
                unmeasured: self.census.unmeasured,
                retired: self.census.retired,
            })
        }
        async fn check_kind_census(&self) -> cog_core::SFResult<PrincipleCheckKindCensus> {
            Ok(PrincipleCheckKindCensus {
                shape: 0,
                tree: 0,
                live: 0,
                judgement: 0,
                undeclared: 0,
            })
        }
        async fn report_refusals(
            &self,
            _: &[cog_core::PrincipleRefusal],
        ) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn fold_refusals(&self, _: usize) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn pending_refusals(&self) -> cog_core::SFResult<u64> {
            unimplemented!()
        }
    }

    /// The pass publishes every cell of the census, zero included, under the
    /// closed set of labels -- and it does so from the pass itself, not from a
    /// helper nobody calls.
    ///
    /// A reading that only reaches the log is a reading that dies with the pod,
    /// and a loop that computes a census every pass while publishing none of it
    /// looks exactly like a loop that is not running. The zero cell is the one
    /// that catches the tempting shortcut of writing only non-zero values: with
    /// it, a corpus where nothing has been retired republishes the same
    /// explicit zero, and an absent series means the loop stopped.
    #[tokio::test]
    async fn a_pass_publishes_every_census_cell_including_the_zero() {
        let metrics = Arc::new(RecordingMetrics::default());
        let store = CensusStore {
            census: PrincipleCensus {
                enforced: 232,
                unenforced: 1730,
                unmeasured: 518,
                retired: 0,
            },
            refused: Vec::new(),
        };

        read_pass(&store, Some(&(metrics.clone() as Arc<dyn MetricsBackend>))).await;

        let gauges = metrics.gauges.lock().unwrap();
        let series: HashMap<&str, f64> = gauges
            .iter()
            .filter(|(name, _, _)| name == metric_names::PRINCIPLE_LEDGER_ROWS.as_str())
            .map(|(_, value, labels)| {
                (
                    labels
                        .get("state")
                        .map(String::as_str)
                        .unwrap_or("<no label>"),
                    *value,
                )
            })
            .collect();
        assert_eq!(
            series.len(),
            4,
            "the pass published {} cells, not four: {:?}",
            series.len(),
            series.keys().collect::<Vec<_>>()
        );
        assert_eq!(series.get("enforced"), Some(&232.0));
        assert_eq!(series.get("unenforced"), Some(&1730.0));
        assert_eq!(series.get("unmeasured"), Some(&518.0));
        assert_eq!(
            series.get("retired"),
            Some(&0.0),
            "the zero cell was skipped; without it an absent series reads as \
             'not publishing' rather than 'none right now'"
        );

        // Every cell has to be addressable by its own reading name: the roster asks
        // for one reading and would otherwise take the first of four unlabelled
        // numbers. A cell without it resolves to nothing, which the roster prints
        // as `absent` -- the same word a series that was never written gets.
        let readings: HashMap<&str, f64> = gauges
            .iter()
            .filter(|(name, _, _)| name == metric_names::PRINCIPLE_LEDGER_ROWS.as_str())
            .map(|(_, value, labels)| {
                (
                    labels
                        .get("readout")
                        .map(String::as_str)
                        .unwrap_or("<no readout>"),
                    *value,
                )
            })
            .collect();
        assert_eq!(readings.get("principle_ledger_enforced"), Some(&232.0));
        assert_eq!(readings.get("principle_ledger_unenforced"), Some(&1730.0));
        assert_eq!(readings.get("principle_ledger_unmeasured"), Some(&518.0));
        assert_eq!(readings.get("principle_ledger_retired"), Some(&0.0));
        assert_eq!(
            readings.len(),
            4,
            "a cell carried no reading name: {readings:?}"
        );
    }

    /// The refusals are published as one series per slug the account holds with
    /// `refusals > 0`, and only for those.
    ///
    /// Unlike the census, no zero is written for the principles nothing has
    /// refused: the population is the answer to "which rules keep stopping
    /// work", and writing a zero for every row would turn it into the corpus
    /// size. The `readout` label is asserted for the same reason its sibling
    /// test does -- the roster resolves a reading by that label, and a family
    /// that only carries `slug` would arrive there as N unlabelled numbers.
    #[tokio::test]
    async fn a_pass_publishes_one_series_per_refused_principle() {
        let metrics = Arc::new(RecordingMetrics::default());
        let store = CensusStore {
            census: PrincipleCensus {
                enforced: 1,
                unenforced: 0,
                unmeasured: 0,
                retired: 0,
            },
            refused: vec![
                ("feedback_numbering_and_plain_language".to_string(), 887),
                ("principle-gate.sh:self-fault:drill".to_string(), 7),
            ],
        };

        read_pass(&store, Some(&(metrics.clone() as Arc<dyn MetricsBackend>))).await;

        let gauges = metrics.gauges.lock().unwrap();
        let by_slug: HashMap<&str, f64> = gauges
            .iter()
            .filter(|(name, _, _)| name == metric_names::PRINCIPLE_REFUSALS.as_str())
            .map(|(_, value, labels)| {
                (
                    labels
                        .get("slug")
                        .map(String::as_str)
                        .unwrap_or("<no slug>"),
                    *value,
                )
            })
            .collect();
        assert_eq!(
            by_slug.len(),
            2,
            "published {} series: {by_slug:?}",
            by_slug.len()
        );
        assert_eq!(
            by_slug.get("feedback_numbering_and_plain_language"),
            Some(&887.0)
        );
        assert_eq!(
            by_slug.get("principle-gate.sh:self-fault:drill"),
            Some(&7.0)
        );

        let unlabelled = gauges
            .iter()
            .filter(|(name, _, _)| name == metric_names::PRINCIPLE_REFUSALS.as_str())
            .filter(|(_, _, labels)| {
                labels.get("readout").map(String::as_str) != Some("principle_ledger_refusals")
            })
            .count();
        assert_eq!(
            unlabelled, 0,
            "a refusal series carried no readout label; the roster cannot name it"
        );
    }
}
