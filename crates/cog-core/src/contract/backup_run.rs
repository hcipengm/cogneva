//! principle: feedback_a_verdict_must_not_absorb_evidence_it_does_not_test
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
    /// How many packages in the retention window have no successful drill
    /// behind them -- their latest drill failed, or none ever ran on them.
    ///
    /// This is not a second reading of a drill's own verdict. A drill's row
    /// says whether the package it opened was restorable, and the newest row
    /// is one package; this counts the window. An installation whose nightly
    /// drill passes every night can still be holding six packages nobody has
    /// ever opened, and the two facts must be visible side by side rather than
    /// one standing in for the other.
    ///
    /// The window is the set of packages the last successful runs said they
    /// left, bounded by the newest run's own `keep_target`, and the drill
    /// history is matched to it by package name. Zero means every package the
    /// writer says it kept has been opened successfully at least once.
    pub drill_unproven_packages: i64,
}

/// One restore drill's answer: what a package was opened up and found to hold.
///
/// The account says a run produced a package; this says the package can be put
/// back. They are separate records because they are produced by separate
/// processes at separate times, and because the second one is allowed to fail
/// while the first is healthy -- that gap is exactly what a drill exists to
/// find.
///
/// The fields are the evidence rather than a verdict: the two counts the
/// manifest promised, the two the restored copy actually held, whether the
/// account table matched the source row for row, and the walk of the volume.
/// [`Self::ok`] is derived from them and not stored separately, so a row cannot
/// say "restorable" while carrying measurements that disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupDrillRecord {
    pub drill_id: String,
    pub started_at: DateTime<Utc>,
    /// When the drill finished. A drill writes its row once, at the end: there
    /// is no open state, because a drill that is killed leaves nothing behind
    /// and the age of the newest row is what tells a reader so.
    pub finished_at: DateTime<Utc>,
    /// The package that was restored, when the volume had one to offer.
    pub package: Option<String>,
    /// The instant in that package's name -- the clock of the run that wrote it.
    pub package_written_at: Option<DateTime<Utc>>,
    /// The size the drill itself found on disk, which is a measurement of the
    /// file rather than a repetition of what the run recorded about it.
    pub package_bytes: Option<i64>,
    /// How far apart the package's own segment stamps were, and the bound the
    /// package declared for that distance. Both `None` for a package with no
    /// segment table, and both `None` when no package was opened.
    ///
    /// The drill rejects a package whose segments are further apart than its
    /// declared bound, so an `ok` row always carries a skew at or below the
    /// bound. The pair is published anyway: that verdict is a cliff, and a
    /// reader watching only the verdict cannot see a package drifting toward
    /// the edge until the night it crosses. The two numbers come from the same
    /// manifest and are read together, because a skew is only "close" relative
    /// to the bound that same package declared.
    pub segment_skew_seconds: Option<i64>,
    pub segment_skew_bound_seconds: Option<i64>,
    /// How many packages the drill's own walk of the volume found, and the
    /// instant in the oldest one's name. These come from the directory and not
    /// from the writer's retention pass, which is the whole point of walking it.
    pub packages_on_volume: i32,
    pub oldest_package_at: Option<DateTime<Utc>>,
    /// The instant in the name of the newest package found outside the backup
    /// volume's directory -- the second copy. `None` when there is no second
    /// copy at all, which is a fact about the installation and not about this
    /// package's restorability: the drill does not need it to be ok.
    pub offsite_newest_at: Option<DateTime<Utc>>,
    /// Whether that second copy is the very file the drill restored: same name,
    /// same byte count. `None` when there was nothing to compare. This is a
    /// separate reading from the age of the copy -- a copy that is present but
    /// a day behind has an age, and this says the copy is the wrong day's.
    pub offsite_matches_newest: Option<bool>,
    /// How many packages on the volume have no same-named file in the offsite
    /// directory. `None` when the drill was given no offsite directory: the
    /// question does not apply, and a zero there would read as full coverage.
    ///
    /// This is a different subject from `offsite_matches_newest`, which asks
    /// about one file -- the newest one. The copy can be perfectly current and
    /// still cover only part of the retention window: it was switched on after
    /// the older packages were written, and nothing ever copied them. A window
    /// that exists in one place only reads green on the newest-file column.
    pub offsite_missing_packages: Option<i32>,
    /// What the offsite repository itself reported, as observed by the host
    /// timer that pushes to it: the instant of the newest snapshot in the
    /// repository, and when its newest encryption key was created.
    ///
    /// These are read off the repository (`restic snapshots`, `restic key
    /// list`), not off the writer's own account of "I just uploaded" and not off
    /// the age of a credential file.
    ///
    /// `offsite_observed_at` is when the host last looked at the repository.
    /// It is what separates "nobody observes this installation" (no value) from
    /// "somebody looked, and the repository held nothing to report" (a value
    /// here, with one or both repository times `None`). An empty repository and
    /// an absent observer are different repairs, so the record keeps them apart
    /// instead of folding both into NULL.
    pub offsite_observed_at: Option<DateTime<Utc>>,
    pub offsite_remote_newest_at: Option<DateTime<Utc>>,
    pub offsite_key_created_at: Option<DateTime<Utc>>,
    /// How many permission violations the drill found on each credential-bearing
    /// directory it was given: the backup volume, the offsite copy, and the host
    /// state directory.
    ///
    /// A violation is counted per entry, not per directory: a directory whose
    /// mode is not exactly 0700, or a top-level package file whose mode is not
    /// exactly 0600. `None` means the drill was not given that directory, which
    /// is a fact about the installation and not a clean reading -- a zero there
    /// would claim a surface was inspected when it was not.
    ///
    /// The drill is where this belongs because it is the one process that runs
    /// as root with all three directories mounted; a host-side timer would only
    /// ever see the one directory it happens to run next to.
    pub credential_violations_backup_pvc: Option<i32>,
    pub credential_violations_offsite: Option<i32>,
    pub credential_violations_state_dir: Option<i32>,
    /// Tables the scratch database held after the copy, against the tables the
    /// package's own manifest declared.
    pub tables_restored: Option<i32>,
    pub tables_expected: Option<i32>,
    pub rows_restored: Option<i64>,
    pub rows_expected: Option<i64>,
    /// Whether the account table came back identical, row for row, to the same
    /// rows in the live database. `None` when the comparison could not be made.
    pub source_runs_matched: Option<bool>,
    /// The `table.column` pairs whose values the package carried but the target
    /// recomputed instead of loading -- columns the schema derives from other
    /// columns of the same row.
    ///
    /// This is not part of [`Self::ok`]: a restore that leaves those columns to
    /// the target is still a restore. It is recorded because the alternative is
    /// silence. If a generated expression is ever changed, every older package
    /// comes back with values the package never held, while the table count, the
    /// row count and the account comparison all still agree -- the three
    /// measurements that make up the verdict cannot see it. The names are the
    /// only place that rewrite would show, so a row without them would say
    /// "restorable" about data that came back different.
    pub generated_columns_recomputed: Vec<String>,
    /// How many artifact-layer units the package's manifest promised, and how
    /// many the drill actually read back. A unit is one policy store (versions,
    /// head and active) or one `decision_stats.json` (groups and trials).
    ///
    /// Zeroes on a package written before the artifact section existed, which
    /// keeps older packages restorable: "that format had no such section" is
    /// not "the section was empty".
    pub artifacts_expected: i32,
    pub artifacts_restored: i32,
    /// One line per unit read back, carrying the numbers themselves
    /// (`evolution/policies/meta_learning.mode versions=1 head=1 active=1`),
    /// not just "unpacked". A byte-count increase in the package is not
    /// evidence that a store came back.
    pub artifact_readings: Vec<String>,
    /// Units the package promised but the drill could not read back, with the
    /// reason. Any entry here fails the drill.
    pub artifact_errors: Vec<String>,
    /// Why the drill did not prove the package restorable. Empty on a drill
    /// whose evidence agrees.
    pub error: Option<String>,
}

impl BackupDrillRecord {
    /// Whether this drill proved the package restorable.
    ///
    /// Every measurement has to be present and agreeing. A count that was never
    /// taken is not a count of zero, and a comparison that could not be made is
    /// not a match -- both leave the drill failed, which is the honest reading:
    /// nothing was proven.
    pub fn ok(&self) -> bool {
        self.error.is_none()
            && self.package.is_some()
            && self.package_written_at.is_some()
            && self.tables_restored.is_some()
            && self.tables_restored == self.tables_expected
            && self.rows_restored.is_some()
            && self.rows_restored == self.rows_expected
            && self.source_runs_matched == Some(true)
            && self.artifacts_restored == self.artifacts_expected
            && self.artifact_errors.is_empty()
    }
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

    /// Write down one drill's answer.
    ///
    /// The row is written once and never moved: a drill that runs twice is two
    /// rows, because the second run's evidence is the second run's, and a
    /// rewritten row would read as if the first had never disagreed.
    async fn record_drill(&self, drill: &BackupDrillRecord) -> SFResult<()>;

    /// The newest drill, whatever it concluded, or `None` when none ever ran.
    ///
    /// A failed drill is returned as readily as a successful one: "the last
    /// drill failed" and "no drill has run" are different facts, and a reader
    /// that only saw successes could not tell them apart.
    async fn read_latest_drill(&self) -> SFResult<Option<BackupDrillRecord>>;
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

    fn agreeing_drill() -> BackupDrillRecord {
        let at = Utc::now();
        BackupDrillRecord {
            drill_id: "d1".into(),
            started_at: at,
            finished_at: at,
            package: Some("cogneva-backup-20261018T031700Z.tar.zst".into()),
            package_written_at: Some(at),
            package_bytes: Some(1024),
            segment_skew_seconds: Some(38),
            segment_skew_bound_seconds: Some(300),
            packages_on_volume: 7,
            oldest_package_at: Some(at),
            offsite_newest_at: Some(at),
            offsite_matches_newest: Some(true),
            offsite_missing_packages: Some(0),
            offsite_observed_at: Some(at),
            offsite_remote_newest_at: Some(at),
            offsite_key_created_at: Some(at),
            credential_violations_backup_pvc: Some(0),
            credential_violations_offsite: Some(0),
            credential_violations_state_dir: Some(0),
            tables_restored: Some(46),
            tables_expected: Some(46),
            rows_restored: Some(1000),
            rows_expected: Some(1000),
            source_runs_matched: Some(true),
            generated_columns_recomputed: Vec::new(),
            artifacts_expected: 0,
            artifacts_restored: 0,
            artifact_readings: Vec::new(),
            artifact_errors: Vec::new(),
            error: None,
        }
    }

    /// A drill is ok only when every measurement was taken and they agree.
    ///
    /// The point of deriving the verdict from the evidence rather than storing
    /// a boolean is that no caller can write "ok" past a missing number. Each
    /// case below is one way of being unresolvably short of proof: a comparison
    /// that was never made (`None`) and a comparison that came out false are
    /// different, but neither is a success.
    #[test]
    fn a_drill_is_ok_only_with_all_of_its_evidence_agreeing() {
        assert!(agreeing_drill().ok());

        // 由目标重算的列不影响判决：它的值从同一行的其它列算出来，不搬不丢数据。
        // 这一格存在的理由是「表达式改了就没人看得见」，不是「恢复可疑」。
        let mut d = agreeing_drill();
        d.generated_columns_recomputed = vec!["raw_log_index.is_hot".into()];
        assert!(
            d.ok(),
            "a target-recomputed column is not a missing measurement"
        );

        let mut d = agreeing_drill();
        d.error = Some("restore failed".into());
        assert!(!d.ok(), "a drill that wrote an error proved nothing");

        let mut d = agreeing_drill();
        d.package = None;
        assert!(!d.ok(), "no package restored");

        let mut d = agreeing_drill();
        d.package_written_at = None;
        assert!(!d.ok(), "no instant to date the package by");

        let mut d = agreeing_drill();
        d.tables_restored = None;
        assert!(
            !d.ok(),
            "a count that was never taken is not a count of zero"
        );

        let mut d = agreeing_drill();
        d.tables_restored = Some(45);
        assert!(!d.ok(), "one table missing is a failed restore");

        let mut d = agreeing_drill();
        d.rows_expected = None;
        assert!(!d.ok());

        let mut d = agreeing_drill();
        d.rows_restored = Some(999);
        assert!(!d.ok());

        let mut d = agreeing_drill();
        d.source_runs_matched = None;
        assert!(
            !d.ok(),
            "a comparison that could not be made is not a match"
        );

        let mut d = agreeing_drill();
        d.source_runs_matched = Some(false);
        assert!(!d.ok());

        // The second copy is not part of this verdict. A package that restores
        // is restorable whether or not a copy exists elsewhere; a copy that
        // stopped being written is its own failure, reported by its own rules,
        // so folding it in here would make a missing copy read as a broken
        // backup.
        let mut d = agreeing_drill();
        d.offsite_newest_at = None;
        d.offsite_matches_newest = None;
        assert!(d.ok());

        // The repository's own observation is not part of the verdict either,
        // and its absence has to stay legal: an installation whose host timer
        // is not installed must still be able to write a drill row. The rules
        // read the absence as "nobody measures this", which is not the same
        // claim as "the copy is old".
        let mut d = agreeing_drill();
        d.offsite_observed_at = None;
        d.offsite_remote_newest_at = None;
        d.offsite_key_created_at = None;
        assert!(d.ok());

        // Observed, and the repository held nothing: a different state from
        // "nobody looked", so the observer time stays while the two repository
        // times are absent. It is still not a verdict about the install.
        let mut d = agreeing_drill();
        d.offsite_remote_newest_at = None;
        d.offsite_key_created_at = None;
        assert!(d.ok());

        // Artifact stores are measured, not inferred from the package growing.
        // A manifest that promised two stores and a drill that read one back
        // has not proven the second one came home.
        let mut d = agreeing_drill();
        d.artifacts_expected = 2;
        d.artifacts_restored = 1;
        assert!(!d.ok(), "one artifact unit promised but not read back");

        let mut d = agreeing_drill();
        d.artifacts_expected = 1;
        d.artifacts_restored = 1;
        d.artifact_errors = vec!["app/policies/x: ACTIVE points at a missing version".into()];
        assert!(
            !d.ok(),
            "a unit that was read back with an error is not restored"
        );

        // Old packages carry no artifact section at all. Zero promised and
        // zero read is agreement, not a missing measurement.
        let mut d = agreeing_drill();
        d.artifacts_expected = 0;
        d.artifacts_restored = 0;
        assert!(d.ok());
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
