//!Contract for the backup account: one durable row per backup run, and the
//!answers a reader takes from it.
//!
//!A backup run's outcome used to be written once, inside the package the run
//!produced. That placement fails in three separate ways, and the contract is
//!shaped around them:
//!
//!  * a pruned package takes its own record with it, so the account of a run is
//!    deleted by the retention that deletes the run's output;
//!  * a week of successes and a week of failures look identical from the
//!    directory -- a failing run prunes nothing -- so the count of packages is
//!    the knob, not a fact about the runs;
//!  * the record cannot be queried, so no reading and no rule can be built on
//!    it.
//!
//!The contract keeps the run's *opening* and its *closing* apart as two calls,
//!because they are two events separated by the whole of the run. The opening is
//!written before any work happens, and that is deliberate: a process killed
//!mid-run -- OOM, eviction, node drain -- leaves the opening as the only witness
//!that it ever started. A contract that only accepted finished runs would be
//!silent in exactly the case where the record is all that survives.
//!
//!A closing is written at most once per run. The store refuses to overwrite a
//!row that already holds a terminal outcome, so a run that reports twice cannot
//!move a conclusion it already recorded -- the same rule the ledgers follow for
//!a settled row.
//!
//!The opening is also what lets the account settle a row no run can settle. A
//!killed process leaves its row open with nobody left to close it, and a row
//!that stays open is one every reader counts as a run still going. The opening
//!therefore carries the deadline the run was given, and past it the run is not
//!late but gone: the account concludes the row as `abandoned` and the reading
//!stops claiming something is running that is not. The conclusion is not a
//!guess -- it is the deadline the run's own scheduler enforces, written on the
//!row by the run itself, before anything could go wrong.
//!
//!Nothing here can make a run fail. The writer treats every call as an attempt
//!whose failure is reported and then ignored, because the alternative is worse
//!in the direction that matters: a backup that completed and was thrown away
//!because its record could not be written trades the thing being protected for
//!the note saying it was.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::SFResult;

/// The four states a run's row can hold.
///
/// `Running` is a state and not a gap: a row with no outcome yet and a row whose
/// outcome is "still going" are the same fact, and encoding it as a value is
/// what lets the row exist before the run has anything to conclude.
///
/// `Abandoned` is the state no run ever writes about itself. A process that was
/// killed cannot come back to correct its own row, so that row would hold
/// `running` forever and mean "still going" to every reader. The account
/// concludes it instead, once the run is past the deadline it was opened under,
/// and the row keeps that conclusion as a value rather than as a null in some
/// other column: three states would leave "never reported" and "reported a
/// failure" sharing a spelling, and a reader that selected by state would be
/// unable to name which of the two it was looking at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupOutcome {
    Running,
    Ok,
    Failed,
    Abandoned,
}

impl BackupOutcome {
    /// The closed set, for a reader that counts outcomes rather than naming one.
    pub const ALL: &'static [BackupOutcome] = &[
        BackupOutcome::Running,
        BackupOutcome::Ok,
        BackupOutcome::Failed,
        BackupOutcome::Abandoned,
    ];

    /// The value the row holds, which is also the value in the column's check
    /// constraint: one spelling of the set rather than one per carrier.
    pub fn as_str(self) -> &'static str {
        match self {
            BackupOutcome::Running => "running",
            BackupOutcome::Ok => "ok",
            BackupOutcome::Failed => "failed",
            BackupOutcome::Abandoned => "abandoned",
        }
    }

    /// Read one back. A value outside the set is not an outcome this build can
    /// name, and the caller decides what to do with a row written by a version
    /// that had more states -- the contract does not fold it into one of these.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "running" => Some(BackupOutcome::Running),
            "ok" => Some(BackupOutcome::Ok),
            "failed" => Some(BackupOutcome::Failed),
            "abandoned" => Some(BackupOutcome::Abandoned),
            _ => None,
        }
    }

    /// Whether a run is allowed to report this outcome about itself.
    ///
    /// Two are not: `Running` is the state a report leaves rather than one it
    /// writes, and `Abandoned` is the account's conclusion about a run that
    /// stopped reporting -- a run still holding the pen cannot have stopped.
    pub fn is_a_runs_own_report(self) -> bool {
        matches!(self, BackupOutcome::Ok | BackupOutcome::Failed)
    }
}

/// What a run leaves at its start: an identity, an instant, and the two bounds
/// it is running under.
///
/// `run_id` is the package stem -- the name the package will have if the run
/// finishes, derived from the same clock read as `started_at`. That is what
/// makes the row and the package name two views of one instant rather than two
/// measurements taken a moment apart, and it is why the row can be written
/// before the package exists.
///
/// `keep_target` is recorded per row rather than read from configuration: the
/// knob changes, and a history that does not carry the bound each run was under
/// makes every past row read as if it had been run under today's.
///
/// `run_deadline_seconds` is what makes an open row conclusive. It is the
/// longest this run may take -- the bound its own scheduler enforces by stopping
/// the run -- and past it the run is not late, it is gone. Written on the row
/// rather than read from configuration at the moment somebody asks, because the
/// question "has this run passed its deadline" is about *this* run: a knob that
/// was raised after the run started would make an abandoned row read as one that
/// still has time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupRunOpening {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub keep_target: i32,
    pub run_deadline_seconds: i32,
}

/// What a run leaves when it stops, whichever way it stopped.
///
/// The package fields are absent on a failed run and that absence is the fact,
/// not a missing measurement: a run that failed produced no package to name or
/// measure.
///
/// `kept_after` and `oldest_kept_at` are written together and read together.
/// They are the two halves of one claim -- "the newest `keep_target` packages
/// are kept" -- and a count on its own cannot falsify it, because a week of
/// successes and a week of failures leave the same count. The age of the oldest
/// kept package is the half that separates them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupRunClosing {
    /// `Ok` or `Failed`. A closing that says `Running` closes nothing, and the
    /// store rejects it rather than letting a row be written back to a state it
    /// can only leave; one that says `Abandoned` is refused for being the wrong
    /// author, since a run that can still report has not stopped reporting. See
    /// [`BackupOutcome::is_a_runs_own_report`].
    pub outcome: BackupOutcome,
    pub finished_at: DateTime<Utc>,
    /// The package left behind, as it is named on the volume.
    pub package: Option<String>,
    /// The package file's size in bytes.
    pub bytes: Option<i64>,
    /// How many packages the run's own retention pass left in the directory.
    pub kept_after: Option<i32>,
    /// The oldest package still in the directory after that pass, timed by the
    /// instant in its own name rather than by its mtime: the name is the same
    /// clock `started_at` came from, so the two are directly comparable, and a
    /// copy or an rsync that rewrites mtimes does not move it.
    pub oldest_kept_at: Option<DateTime<Utc>>,
    /// Why the run failed. Empty on a run that concluded `ok`.
    pub error: Option<String>,
}

/// The newest run that got as far as producing a package, with everything that
/// run left.
///
/// The columns travel as one record because they are one row's answers, and a
/// reader that took the count from one run and the bound from another would
/// compare two runs' numbers while reading as a statement about one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupPackageRecord {
    pub run_id: String,
    pub finished_at: DateTime<Utc>,
    /// The package's name on the volume. Not published as a label -- one value
    /// per run is a value per day, which is the shape a label must never have --
    /// but kept here because it is the name a restore is asked for by.
    pub package: Option<String>,
    pub bytes: Option<i64>,
    /// The bound this run was actually run under, and what its own pass left.
    pub keep_target: i32,
    pub kept_after: Option<i32>,
    pub oldest_kept_at: Option<DateTime<Utc>>,
}

/// What one look at the account answers.
///
/// The two instants that matter are kept apart. `latest_finish_at` is when a run
/// last concluded *anything*; `newest_package.finished_at` is when one last
/// succeeded. A reader that has only the second cannot say whether the backup
/// loop is running and failing or not running at all, and a reader that has only
/// the first cannot say whether anything was ever saved.
///
/// Every instant here is a point in the past and not an age. An age is computed
/// against the clock of whoever reads it, and a row that stored an age would be
/// a number that keeps going stale while claiming to be current.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackupAccountReading {
    pub running: i64,
    pub ok: i64,
    pub failed: i64,
    /// Runs the account concluded itself, because they were opened and never
    /// reported. Counted apart from the reported failures: the two answer "was
    /// the last run broken" and "has a run gone missing" respectively, and the
    /// reasons behind them are not the same set of repairs.
    pub abandoned: i64,
    /// When the newest run finished, whatever it concluded. A run that was
    /// abandoned is not in here -- it has no finish instant, and the newest
    /// reported finish is a different fact from the newest row.
    pub latest_finish_at: Option<DateTime<Utc>>,
    /// When the oldest still-open run started. A run that never came back is
    /// only visible here.
    pub oldest_running_at: Option<DateTime<Utc>>,
    /// The deadline that same run was opened under, in seconds.
    ///
    /// The two travel together because they are one row's answers: an age read
    /// against a bound that came from somewhere else would be a comparison
    /// between one run's duration and another's allowance, stated as if it were
    /// a statement about the first. Absent exactly when `oldest_running_at` is.
    pub oldest_running_deadline_seconds: Option<i32>,
    /// The newest run that produced a package, or `None` when no run ever has.
    /// Absent is a different fact from "an old one": a deployment that has never
    /// backed up successfully has no age to report, and a zero would say the
    /// opposite.
    pub newest_package: Option<BackupPackageRecord>,
}

/// Durable storage for the backup account.
///
/// Four calls, and the split between the first two is the run itself: one
/// writes down that a run has started, one writes down how it ended, and the
/// gap between them is where a run can be killed. `abandon_overdue` is the only
/// call that writes a conclusion no run asked for, and `read_account` is the
/// only read -- the accounting is done by the store, because summing rows in the
/// reader would be a second place the account's arithmetic lives.
///
/// There is no delete. A run's row is the record that it happened, and a record
/// that can be removed is one whose absence cannot be told from never having
/// been written.
#[async_trait]
pub trait BackupRunStore: Send + Sync {
    /// Open a run's row, before the run does any work.
    ///
    /// Answers whether the row was newly written. A run identity already in the
    /// table is left exactly as it was -- rewriting it would move the start
    /// instant of a run that has already been recorded -- so a second opening of
    /// the same identity answers `false` and changes nothing. The caller decides
    /// what to do with that; it is not an error, because the identity is derived
    /// from a clock and a collision is a fact about the clock rather than a
    /// fault in the caller.
    async fn open_run(&self, opening: &BackupRunOpening) -> SFResult<bool>;

    /// Close a run's row with its conclusion.
    ///
    /// Answers whether a row was moved. Only a row still holding `Running` is
    /// moved, so a terminal row is never rewritten and a run that reports twice
    /// keeps the conclusion it first recorded. `false` therefore means either
    /// that there is no such row or that it was already closed, and the caller
    /// reports which of the two it can -- a run whose opening is missing is a
    /// different problem from one that closed itself twice.
    ///
    /// Only `Ok` and `Failed` are accepted: a closing that says `Running` would
    /// be a transition to the state the row is already in, and one that says
    /// `Abandoned` would be a run reporting the conclusion that it stopped
    /// reporting. The second is the account's to write and not the run's.
    async fn close_run(&self, run_id: &str, closing: &BackupRunClosing) -> SFResult<bool>;

    /// Conclude every open run that is past the deadline it was opened under.
    ///
    /// This is not a repair and it is not a retry. A run that was killed cannot
    /// come back to close its own row, so without this call that row stays open
    /// for good and every reader that counts open runs is counting a run that
    /// ended. The bound is the row's own `run_deadline_seconds`, so what is
    /// concluded is never a run that could still be inside the time it was given.
    ///
    /// Answers how many rows this call concluded. `0` is the ordinary answer and
    /// says nothing is wrong; a positive one is a fact worth reporting, since
    /// each row it counts is a run that went missing.
    ///
    /// A concluded row keeps `Running`'s missing finish instant and gets an
    /// error text naming what happened, because there is no honest instant to put
    /// there: the whole of the fact is that this run's end was never seen.
    async fn abandon_overdue(&self, now: DateTime<Utc>) -> SFResult<u64>;

    /// Read the account: how many runs of each outcome, and the instants and the
    /// newest package that the counts cannot carry.
    async fn read_account(&self) -> SFResult<BackupAccountReading>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_outcome_set_round_trips_through_its_own_spelling() {
        for outcome in BackupOutcome::ALL {
            assert_eq!(BackupOutcome::parse(outcome.as_str()), Some(*outcome));
        }
        assert_eq!(BackupOutcome::ALL.len(), 4);
        assert_eq!(BackupOutcome::parse("aborted"), None);
        assert_eq!(BackupOutcome::parse(""), None);
        // The spelling the column's check constraint holds, so a row the store
        // wrote is one the column accepts.
        assert!(BackupOutcome::ALL
            .iter()
            .all(|o| o.as_str().chars().all(|c| c.is_ascii_lowercase())));
    }

    /// The two states a run cannot report about itself, and the two it can.
    ///
    /// This is the whole of what separates a closing from a conclusion somebody
    /// else reached: the store refuses the first pair, so a row holding
    /// `Abandoned` was written by the account and one holding `Failed` was
    /// written by the run. A reader selecting by state can therefore tell the
    /// two apart without a second column.
    #[test]
    fn only_two_outcomes_are_a_runs_own_report() {
        assert!(BackupOutcome::Ok.is_a_runs_own_report());
        assert!(BackupOutcome::Failed.is_a_runs_own_report());
        assert!(!BackupOutcome::Running.is_a_runs_own_report());
        assert!(!BackupOutcome::Abandoned.is_a_runs_own_report());
    }
}
