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
//! Two things happen here, and neither is a judgement. The loop folds the intake
//! into the account -- declarations queued by other parties become readings,
//! through the same reading path a direct write would use -- and reads the
//! outstanding set back out, saying plainly when a read failed. The fold is
//! mechanical: it carries what a document asked for and adds nothing of its own,
//! so this loop cannot make a change look done.
//!
//! The half that would hand the outstanding set to the goal/DAG path is still
//! missing. Nothing on this platform evaluates a change's own check yet, so a
//! state written here would be a verdict this loop made up. Until that path
//! exists the ledger is fed but not measured, and its rows stay `undecided` --
//! which is the honest reading of "read about, never looked at", and is why the
//! two halves are reported separately rather than as one progress number.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use cog_core::{ChangeLedgerRow, ChangeLedgerStore, ChangeState, OwnerLeaseBroker, ShutdownSignal};

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

/// Start the loop. Not started when there is no ledger store (see the module
/// header).
pub fn spawn_change_ledger_loop(
    store: Arc<dyn ChangeLedgerStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    shutdown: ShutdownSignal,
) {
    info!(
        interval_secs = CHANGE_LEDGER_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        "Change ledger loop enabled"
    );

    drop(cog_core::loop_health::spawn(
        CHANGE_LEDGER_LOOP,
        cog_core::loop_health::Cadence::Periodic(CHANGE_LEDGER_PASS_INTERVAL),
        shutdown.clone(),
        // Rebuilt per attempt, so everything the body consumes is cloned here.
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let shutdown = shutdown.clone();
            async move {
                // The role is renewed on the lease contract's own cadence rather
                // than once per pass: a pass can be long, and a term that ran out
                // mid-pass would hand the seat to a second process working the
                // same table. The loop still asks once per pass, because a role
                // lost has to stop the work in the pass it was lost.
                let hold =
                    cog_core::RoleHold::start(role, CHANGE_LEDGER_ROLE, beat.clone(), &shutdown);

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
}
