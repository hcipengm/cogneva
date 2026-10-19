//!The backup account's reading loop: what the last runs did, folded onto the
//!metrics face.
//!
//!The rows are written by the backup runs themselves -- separate processes on a
//!schedule, each holding its own connection -- and this loop is the half that
//!reads them. Without it the account is a table only a person with a psql
//!session can see, and a table nobody reads is the same as no table at all.
//!
//!It is a loop rather than a line in the backup program because of the one case
//!that matters most: a run killed mid-flight (OOM, eviction, node drain) wrote
//!its opening row and nothing else, and the process that would have published an
//!age for it is the process that died. Only something outside the run can notice
//!that the run never came back, so the reader has to be resident.
//!
//!The loop carries no threshold. It publishes what the account holds -- counts
//!and instants and the newest package -- and where a bound is needed it belongs
//!to the rule that acts on it, so there is one place to change it and one place
//!to read why it has the value it has. A threshold baked in here would be
//!invisible to whoever reads the alert that fires on this series.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use cog_core::{
    metric_names, BackupAccountReading, BackupDrillRecord, BackupRunStore, MetricsBackend,
    OwnerLeaseBroker, ShutdownSignal,
};

/// This loop's name in the process liveness registry, and the value of
/// `cogneva_loop_tick_age_seconds{loop=...}`.
pub const BACKUP_RUNS_LOOP: &str = "backup_runs";

/// This loop's role name on the owner lease: who publishes the backup account's
/// readings.
///
/// The same value as the loop name, but the two read different surfaces: the
/// loop name labels the liveness and ownership readings (`{loop=...}`), the role
/// name is the row in `cogneva_owner_leases.role`. They agree now; changing one
/// is not changing the other.
pub const BACKUP_RUNS_ROLE: &str = "backup_runs";

/// How long one round waits for the next.
///
/// The account changes at most a few times a day -- a run opens, and it closes
/// minutes later -- so this period is not "how fast do backups arrive" but "how
/// long before a run that never came back is noticed". The bound that matters is
/// the next scheduled run, a day away, and a run that is still open when it is
/// due stops it: the backup Job does not start a second run while the first is
/// unfinished. Minutes rather than seconds keeps the steady state out of the log
/// while leaving hours of margin. The deployed liveness alert derives its
/// threshold from this period (`6 * period`), so changing it changes that
/// threshold.
pub const BACKUP_RUNS_PASS_INTERVAL: Duration = Duration::from_secs(300);

/// The readouts one look at the account produces.
///
/// A closed set on one series rather than one series each, because the values
/// answer one question between them and a reader forced to join families reads
/// some of the numbers without the others. The name of each carries its unit: a
/// value whose unit has to be inferred from the series name is a value that will
/// be read in the wrong one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BackupReadout {
    /// Runs that have opened and not closed. The count of what could still be
    /// going.
    RunsRunning,
    RunsOk,
    RunsFailed,
    /// Runs the account had to conclude itself, because they stopped reporting.
    RunsAbandoned,
    /// How long ago any run last concluded, whatever it concluded.
    LatestFinishAgeSeconds,
    /// How long ago a run last concluded `ok`.
    LatestSuccessAgeSeconds,
    /// How long the oldest still-open run has been open.
    OldestRunningAgeSeconds,
    /// The deadline that same run was opened under.
    OldestRunningDeadlineSeconds,
    /// What the newest successful run's own retention pass left.
    Kept,
    /// The bound that run was run under.
    KeepTarget,
    /// How long ago the oldest surviving package was written.
    OldestKeptAgeSeconds,
    /// The newest package's size on disk.
    NewestPackageBytes,
    /// How long ago the newest restore drill finished, whatever it concluded.
    DrillLatestAgeSeconds,
    /// Whether that drill proved its package restorable: 1 or 0.
    DrillLatestOk,
    /// How many packages the newest drill's own walk of the volume found.
    VolumePackages,
    /// How long ago the oldest of those was written, by the instant in its name.
    VolumeOldestPackageAgeSeconds,
    /// How long ago the newest package outside the backup volume was written.
    /// Absent when there is no second copy at all.
    OffsiteNewestAgeSeconds,
    /// Whether that second copy is the very package the drill restored: 1 or 0.
    OffsiteMatchesVolume,
    /// How many packages on the volume have no same-named copy outside it.
    /// Absent when the drill was given no offsite directory: the question does
    /// not apply, and a zero would read as full coverage.
    OffsiteMissingPackages,
    /// How long ago the host last looked at the offsite repository. This is
    /// the channel's own liveness, separate from what it found: absent means
    /// nobody observes, while a value here with the two repository ages absent
    /// means somebody looked and the repository held nothing to report.
    OffsiteObservationAgeSeconds,
    /// How long ago the offsite repository's newest snapshot was taken, as the
    /// repository reports it to the host timer that pushes to it. Absent until
    /// that timer has observed the repository once.
    OffsiteRemoteNewestAgeSeconds,
    /// How long ago the repository's newest encryption key was created -- how
    /// long the key has gone unrotated, from the repository's own key list.
    /// Absent until somebody observes it.
    OffsiteRepositoryKeyAgeSeconds,
    /// How far apart the newest opened package's own segments were, and the
    /// bound that same package declared. The drill refuses a package over its
    /// bound, so an ok drill always carries skew <= bound; the pair is
    /// published so a reader can see the distance to the edge before the night
    /// it crosses, which the verdict alone cannot show.
    SegmentSkewSeconds,
    SegmentSkewBoundSeconds,
    /// How many packages in the retention window have no successful drill
    /// behind them, counted by the store across the window rather than read off
    /// the newest drill's row.
    DrillWindowUnprovenPackages,
}

impl BackupReadout {
    const ALL: &'static [BackupReadout] = &[
        BackupReadout::RunsRunning,
        BackupReadout::RunsOk,
        BackupReadout::RunsFailed,
        BackupReadout::RunsAbandoned,
        BackupReadout::LatestFinishAgeSeconds,
        BackupReadout::LatestSuccessAgeSeconds,
        BackupReadout::OldestRunningAgeSeconds,
        BackupReadout::OldestRunningDeadlineSeconds,
        BackupReadout::Kept,
        BackupReadout::KeepTarget,
        BackupReadout::OldestKeptAgeSeconds,
        BackupReadout::NewestPackageBytes,
        BackupReadout::DrillLatestAgeSeconds,
        BackupReadout::DrillLatestOk,
        BackupReadout::VolumePackages,
        BackupReadout::VolumeOldestPackageAgeSeconds,
        BackupReadout::OffsiteNewestAgeSeconds,
        BackupReadout::OffsiteMatchesVolume,
        BackupReadout::OffsiteMissingPackages,
        BackupReadout::OffsiteObservationAgeSeconds,
        BackupReadout::OffsiteRemoteNewestAgeSeconds,
        BackupReadout::OffsiteRepositoryKeyAgeSeconds,
        BackupReadout::SegmentSkewSeconds,
        BackupReadout::SegmentSkewBoundSeconds,
        BackupReadout::DrillWindowUnprovenPackages,
    ];

    fn as_str(self) -> &'static str {
        match self {
            BackupReadout::RunsRunning => "runs_running",
            BackupReadout::RunsOk => "runs_ok",
            BackupReadout::RunsFailed => "runs_failed",
            BackupReadout::RunsAbandoned => "runs_abandoned",
            BackupReadout::LatestFinishAgeSeconds => "latest_finish_age_seconds",
            BackupReadout::LatestSuccessAgeSeconds => "latest_success_age_seconds",
            BackupReadout::OldestRunningAgeSeconds => "oldest_running_age_seconds",
            BackupReadout::OldestRunningDeadlineSeconds => "oldest_running_deadline_seconds",
            BackupReadout::Kept => "kept",
            BackupReadout::KeepTarget => "keep_target",
            BackupReadout::OldestKeptAgeSeconds => "oldest_kept_age_seconds",
            BackupReadout::NewestPackageBytes => "newest_package_bytes",
            BackupReadout::DrillLatestAgeSeconds => "drill_latest_age_seconds",
            BackupReadout::DrillLatestOk => "drill_latest_ok",
            BackupReadout::VolumePackages => "volume_packages",
            BackupReadout::VolumeOldestPackageAgeSeconds => "volume_oldest_package_age_seconds",
            BackupReadout::OffsiteNewestAgeSeconds => "offsite_newest_age_seconds",
            BackupReadout::OffsiteMatchesVolume => "offsite_matches_volume",
            BackupReadout::OffsiteMissingPackages => "offsite_missing_packages",
            BackupReadout::OffsiteObservationAgeSeconds => "offsite_observation_age_seconds",
            BackupReadout::OffsiteRemoteNewestAgeSeconds => "offsite_remote_newest_age_seconds",
            BackupReadout::OffsiteRepositoryKeyAgeSeconds => "offsite_repository_key_age_seconds",
            BackupReadout::SegmentSkewSeconds => "segment_skew_seconds",
            BackupReadout::SegmentSkewBoundSeconds => "segment_skew_bound_seconds",
            BackupReadout::DrillWindowUnprovenPackages => "drill_window_unproven_packages",
        }
    }
}

/// Who selects one of the account's values, or why nobody does.
///
/// A value with no reader is not the same thing as a value nobody needs: the
/// account answers several questions no rule asks, and it is right to publish
/// them -- but the difference has to be written down per value, because a
/// blanket "these are for whoever looks" is how a published cell stops having
/// anyone to be for while still costing a row on every scrape.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ReadoutReader {
    /// A shipped rule's expression selects this value by name.
    Rule(&'static str),
    /// No shipped rule selects it. `why` is why it is published anyway, and
    /// what would have to change before one did.
    Unread { why: &'static str },
}

/// What happens to each value the account publishes, keyed by the producer's
/// own roster.
///
/// This table is the other half of [`BackupReadout::ALL`] and not a second
/// roster: the keys are compared against `ALL` as a set (so a value added to the
/// exposition without a line here fails, and so does a name spelled twice), the
/// length is fixed by `ALL`, and every line is checked against the chart in the
/// direction it claims: a `Rule` line against that rule's own selector, an
/// `Unread` line against every shipped rule, and the whole chart against this
/// roster -- a rule reading a name the account cannot publish is a rule that can
/// never match, which no line here could show.
///
/// It is built only for the tests, because nothing in the loop reads it: the
/// account publishes the same values whether or not this table exists, and the
/// table's whole job is to be checked -- against the producer for which values
/// there are, and against the chart for who reads them.
#[cfg(test)]
const READOUT_READERS: [(BackupReadout, ReadoutReader); BackupReadout::ALL.len()] = [
    (
        // Read as a conjunction and not as a subject: the rule requires this
        // count to be zero, so what it keeps out is a run that is open right
        // now -- the one reading under which "nothing has ever been recorded"
        // is still true of an installation that is being reached. A rule
        // firing on this count rising above one was ruled out by a measurement
        // rather than by taste, and that reasoning has not changed; the reading
        // is here so its absence stays a fact the rule can state.
        BackupReadout::RunsRunning,
        ReadoutReader::Rule("backup_nothing_ever_recorded"),
    ),
    (
        BackupReadout::RunsOk,
        ReadoutReader::Rule("backup_never_succeeded"),
    ),
    (
        BackupReadout::RunsFailed,
        ReadoutReader::Rule("backup_never_succeeded"),
    ),
    (
        BackupReadout::RunsAbandoned,
        ReadoutReader::Rule("backup_run_abandoned"),
    ),
    (
        BackupReadout::LatestFinishAgeSeconds,
        ReadoutReader::Unread {
            why: "read with latest_success_age_seconds, which has a rule: the two together separate \
                  a loop that runs and fails from one that is not running, and only the success half \
                  is read today. A rule for this half has to say what its absence means -- it is \
                  absent until a run concludes anything, which is a different fact from an old \
                  finish.",
        },
    ),
    (
        BackupReadout::LatestSuccessAgeSeconds,
        ReadoutReader::Rule("backup_no_recent_success"),
    ),
    (
        BackupReadout::OldestRunningAgeSeconds,
        ReadoutReader::Rule("backup_run_over_its_own_deadline"),
    ),
    (
        BackupReadout::OldestRunningDeadlineSeconds,
        ReadoutReader::Rule("backup_run_over_its_own_deadline"),
    ),
    (
        BackupReadout::Kept,
        ReadoutReader::Rule("backup_retention_left_more_than_its_target"),
    ),
    (
        BackupReadout::KeepTarget,
        ReadoutReader::Rule("backup_retention_left_more_than_its_target"),
    ),
    (
        BackupReadout::OldestKeptAgeSeconds,
        ReadoutReader::Rule("backup_oldest_package_past_its_retention"),
    ),
    (
        // The size of the newest package, judged against this series' own
        // largest over the past week rather than a fixed floor: what the rule
        // has to see is a backup that stopped capturing while every other
        // reading stayed healthy, and only a proportion survives differing
        // database sizes.
        BackupReadout::NewestPackageBytes,
        ReadoutReader::Rule("backup_newest_package_shrank_below_its_own_recent_shape"),
    ),
    // The drill's four. A drill that never ran publishes none of them, so the
    // rule below fires on absence by a second expression that reads the same
    // selector -- `backup_restore_drill_never_ran` multiplies `absent()` of this
    // name by how long the CronJob has existed. The line names the age rule
    // because that is the one whose subject is this value.
    (
        BackupReadout::DrillLatestAgeSeconds,
        ReadoutReader::Rule("backup_restore_drill_is_stale"),
    ),
    (
        BackupReadout::DrillLatestOk,
        ReadoutReader::Rule("backup_restore_drill_failed"),
    ),
    // The writer's own count, read by a rule only where a second, independent
    // count exists: `kept` comes from the run's retention pass and this one from
    // the drill's walk, and the rule is the difference between them.
    (
        BackupReadout::VolumePackages,
        ReadoutReader::Rule("backup_volume_package_count_disagrees_with_the_last_run"),
    ),
    (
        // The oldest instant the walk found. It is the walk's own evidence that
        // the count means what it says -- seven packages whose oldest was written
        // yesterday is not the state seven with a week-old oldest describes --
        // and it is published for whoever looks. A rule over it would compare it
        // with `oldest_kept_age_seconds`, and the two agree by construction
        // whenever the walk and the run describe the same directory; when they
        // do not, the drill's own staleness is the fact, and the rule above this
        // one already reports it. A second rule would only say it twice.
        BackupReadout::VolumeOldestPackageAgeSeconds,
        ReadoutReader::Unread {
            why: "the walk's evidence for the count, read by whoever checks that the count is of \
                  the directory they think it is. Its disagreement with oldest_kept_age_seconds is \
                  the drill being old, which backup_restore_drill_is_stale reports already.",
        },
    ),
    (
        // The age of the second copy. Absent until one is written, so the rule
        // that asks whether one ever appeared reads the absence, and this one
        // only answers how old the newest is.
        BackupReadout::OffsiteNewestAgeSeconds,
        ReadoutReader::Rule("backup_offsite_copy_is_stale"),
    ),
    (
        BackupReadout::OffsiteMatchesVolume,
        ReadoutReader::Rule("backup_offsite_copy_is_not_the_latest_package"),
    ),
    // The other question about the same directory walk, and the one the
    // newest-file column cannot answer: how much of the window has no second
    // copy at all. Firing on it is ordinary after the copy is first enabled --
    // the backlog shrinks only as the window rotates -- so it names coverage,
    // not breakage.
    (
        BackupReadout::OffsiteMissingPackages,
        ReadoutReader::Rule("backup_offsite_copy_missing_from_window"),
    ),
    // The repository's own answer, as opposed to the directory walk above: the
    // walk sees a file on this machine, this sees a snapshot in the repository.
    // Absent when nobody observes, so the never-observed shape is a separate
    // rule reading the absence.
    (
        // The channel's own age: published even when the repository reported
        // nothing, so "nobody looked" and "looked and found none" stay apart.
        BackupReadout::OffsiteObservationAgeSeconds,
        ReadoutReader::Rule("backup_offsite_repository_is_never_observed"),
    ),
    (
        BackupReadout::OffsiteRemoteNewestAgeSeconds,
        ReadoutReader::Rule("backup_offsite_remote_copy_is_stale"),
    ),
    (
        BackupReadout::OffsiteRepositoryKeyAgeSeconds,
        ReadoutReader::Rule("backup_offsite_repository_key_is_old"),
    ),
    // The margin behind the drill's verdict. A package under its own declared
    // bound is not a fault, and the bound is the package's number rather than
    // the deployment's, so a rule would have to invent a fraction of "close"
    // for a distance the system already refuses at 100%. The verdict at the
    // bound is `drill_latest_ok`; this pair is for whoever wants to see the
    // drift before the cliff. Both are published together: a skew without its
    // bound cannot say whether it is near anything, and a bound without the
    // skew is not a reading at all.
    (
        BackupReadout::SegmentSkewSeconds,
        ReadoutReader::Unread {
            why: "the distance the drill judged against the package's own bound; a package under \
                  that bound is healthy by definition, so there is no threshold to alert on. \
                  Published so the drift toward the cliff is visible on a panel; the cliff itself \
                  is backup_restore_drill_failed.",
        },
    ),
    (
        BackupReadout::SegmentSkewBoundSeconds,
        ReadoutReader::Unread {
            why: "the other half of segment_skew_seconds: the bound that package declared. It is a \
                  per-package value, so a rule cannot compare it with a configured constant; it is \
                  meaningful only beside the skew from the same row.",
        },
    ),
    // The one drill value that is not a look at a row: the store counts the
    // window, because the loop has no directory to walk and no history of its
    // own. Its rule is the only one whose subject is the window rather than the
    // last drill, and that is why it has to exist beside `drill_latest_ok` --
    // a passing night cannot stand in for it.
    (
        BackupReadout::DrillWindowUnprovenPackages,
        ReadoutReader::Rule("backup_restore_drill_window_has_unproven_packages"),
    ),
];

/// How old an instant is, in whole seconds, floored at zero.
///
/// The floor is not cosmetic. The instant was written by the backup process and
/// the subtraction happens in this one, so a clock that moved backwards between
/// them would otherwise publish a negative age -- a number in a seconds field
/// that does not mean what every reader of that field will take it to mean.
/// Zero says "at least as recent as this can measure", which is true either way.
fn age_seconds(now: DateTime<Utc>, instant: DateTime<Utc>) -> f64 {
    (now - instant).num_seconds().max(0) as f64
}

/// The value of one readout, or `None` when the account has nothing to say for
/// it.
///
/// An absent readout is an absent fact and not a zero. No run has ever
/// succeeded, so there is no age for one; and no run is open, so there is no age
/// for the oldest open one. Writing zero for either would say the opposite of
/// the truth -- that a run just finished, or just started -- and the second of
/// the two is the value a reader watching for a killed run is looking at.
fn value_of(
    readout: BackupReadout,
    account: &BackupAccountReading,
    drill: Option<&BackupDrillRecord>,
    now: DateTime<Utc>,
) -> Option<f64> {
    match readout {
        BackupReadout::RunsRunning => Some(account.running as f64),
        BackupReadout::RunsOk => Some(account.ok as f64),
        BackupReadout::RunsFailed => Some(account.failed as f64),
        BackupReadout::RunsAbandoned => Some(account.abandoned as f64),
        BackupReadout::LatestFinishAgeSeconds => {
            account.latest_finish_at.map(|at| age_seconds(now, at))
        }
        BackupReadout::LatestSuccessAgeSeconds => account
            .newest_package
            .as_ref()
            .map(|pkg| age_seconds(now, pkg.finished_at)),
        BackupReadout::OldestRunningAgeSeconds => {
            account.oldest_running_at.map(|at| age_seconds(now, at))
        }
        // Published next to the age it belongs to, and not instead of a baked-in
        // bound: the rule that decides a run is gone compares these two numbers
        // to each other, so a run opened under a longer deadline is judged by its
        // own and not by whatever the deployment happens to run with today.
        BackupReadout::OldestRunningDeadlineSeconds => {
            account.oldest_running_deadline_seconds.map(f64::from)
        }
        // The retention pair and the package size are one run's answers, so
        // they are read from the same row. Taking the count from one run and the
        // bound from another would compare two runs' numbers while reading as a
        // statement about one.
        BackupReadout::Kept => account
            .newest_package
            .as_ref()
            .and_then(|pkg| pkg.kept_after)
            .map(f64::from),
        BackupReadout::KeepTarget => account
            .newest_package
            .as_ref()
            .map(|pkg| f64::from(pkg.keep_target)),
        BackupReadout::OldestKeptAgeSeconds => account
            .newest_package
            .as_ref()
            .and_then(|pkg| pkg.oldest_kept_at)
            .map(|at| age_seconds(now, at)),
        BackupReadout::NewestPackageBytes => account
            .newest_package
            .as_ref()
            .and_then(|pkg| pkg.bytes)
            .map(|bytes| bytes as f64),
        // The drill's values all come from one row and all disappear with it.
        // A drill that never ran publishes nothing here rather than a zero:
        // "no drill has ever finished" and "a drill just finished" are the same
        // number and opposite facts, and the rule that asks whether a drill is
        // owed reads the absence, not the zero.
        BackupReadout::DrillLatestAgeSeconds => drill.map(|d| age_seconds(now, d.finished_at)),
        BackupReadout::DrillLatestOk => drill.map(|d| if d.ok() { 1.0 } else { 0.0 }),
        BackupReadout::VolumePackages => drill.map(|d| d.packages_on_volume as f64),
        // Absent when the walk found nothing to take the oldest instant from,
        // which is the same state as "no package has ever been written" -- and
        // there is no age to publish for a package that is not there.
        BackupReadout::VolumeOldestPackageAgeSeconds => drill
            .and_then(|d| d.oldest_package_at)
            .map(|at| age_seconds(now, at)),
        // The second copy's two readings are absent together when there is no
        // copy, and that absence is not an error: the rule that asks whether a
        // second copy was ever written reads the absence itself, and the one
        // that asks whether the copy is current reads this age.
        BackupReadout::OffsiteNewestAgeSeconds => drill
            .and_then(|d| d.offsite_newest_at)
            .map(|at| age_seconds(now, at)),
        BackupReadout::OffsiteMatchesVolume => drill
            .and_then(|d| d.offsite_matches_newest)
            .map(|same| if same { 1.0 } else { 0.0 }),
        // The window, not one file: a copy that is current still leaves the
        // packages written before it was switched on without a second place to
        // be. Absent (no offsite directory) is not zero coverage.
        BackupReadout::OffsiteMissingPackages => drill
            .and_then(|d| d.offsite_missing_packages)
            .map(|missing| missing as f64),
        // Observed by the host timer on the repository itself, not by the
        // writer: the newest snapshot the repository reports, and when its
        // newest key was created. Both are absent when nobody observes, which
        // is a different fact from an observed value that has aged -- the rules
        // ask the two questions separately, and absence must not read as zero.
        BackupReadout::OffsiteObservationAgeSeconds => drill
            .and_then(|d| d.offsite_observed_at)
            .map(|at| age_seconds(now, at)),
        BackupReadout::OffsiteRemoteNewestAgeSeconds => drill
            .and_then(|d| d.offsite_remote_newest_at)
            .map(|at| age_seconds(now, at)),
        BackupReadout::OffsiteRepositoryKeyAgeSeconds => drill
            .and_then(|d| d.offsite_key_created_at)
            .map(|at| age_seconds(now, at)),
        // The package's own declaration, not an age: the distance between its
        // segment stamps and the bound it carried. Both come from one row and
        // disappear together -- a package with no segment table, or no package
        // at all, has no distance to report, and zero would read as a measured
        // perfect one.
        BackupReadout::SegmentSkewSeconds => drill
            .and_then(|d| d.segment_skew_seconds)
            .map(|skew| skew as f64),
        BackupReadout::SegmentSkewBoundSeconds => drill
            .and_then(|d| d.segment_skew_bound_seconds)
            .map(|bound| bound as f64),
        // Not read off the newest drill's row: that row answers "did the
        // package it opened restore", and this counts the window it did not
        // open. A green newest row and a window nobody has opened are both
        // ordinary, and only one of them is what a restore needs.
        BackupReadout::DrillWindowUnprovenPackages => Some(account.drill_unproven_packages as f64),
    }
}

/// Read the account once and publish what it holds.
///
/// The reconcile runs first and in the same round as the read, so a run that
/// went missing stops being counted as one that is going in the tick that
/// notices it rather than in whichever round comes after a conclusion written
/// somewhere else. Nothing here is published for a row that has just been
/// concluded: its age readout drops out of the exposition in the same round, and
/// a reader that selected it by name stops seeing it at all -- which is the
/// signal that pass is for.
///
/// A failure to publish is raised and not fatal, for the reason the ledger
/// loops give: the account still holds what it holds, and a loop that gave up
/// on the reading because the reading face was closed would trade the account
/// for the report of it.
async fn publish_account_once(
    store: &dyn BackupRunStore,
    metrics: Option<&Arc<dyn MetricsBackend>>,
) -> cog_core::SFResult<BackupAccountReading> {
    let now = Utc::now();

    // Whoever holds the seat is the one author of these conclusions, which is
    // what keeps two replicas from both writing them -- they would agree, but
    // "which process concluded this run" would stop having one answer.
    match store.abandon_overdue(now).await {
        Ok(0) => {}
        Ok(concluded) => warn!(
            concluded,
            "backup account: a run outlived the deadline it was opened under and never reported, \
             so the account concluded it -- the process that opened that row was killed or wedged \
             and nobody else was left to close it"
        ),
        Err(e) => warn!(
            error = %e,
            "backup account: this round could not conclude the runs that are past their deadline, \
             so an open row that no run is behind will keep being published as one that is going"
        ),
    }

    let account = store.read_account().await?;

    // A drill table that cannot be read is not a reason to withhold the account:
    // the two records answer different questions, and losing the older one
    // because the newer one is broken would take a working reading away. The
    // failure is loud in the log, and the drill's own rules go quiet -- which is
    // exactly the shape their freshness companions are there to make visible.
    let drill = match store.read_latest_drill().await {
        Ok(drill) => drill,
        Err(e) => {
            warn!(
                error = %e,
                "backup account: the newest restore drill could not be read this round, so the \
                 drill's readings are absent from the face even though the account's are not"
            );
            None
        }
    };

    if let Some(metrics) = metrics {
        for readout in BackupReadout::ALL {
            let Some(value) = value_of(*readout, &account, drill.as_ref(), now) else {
                continue;
            };
            let mut labels = HashMap::new();
            labels.insert("readout".to_string(), readout.as_str().to_string());
            if let Err(e) = metrics
                .record_gauge(metric_names::BACKUP_RUN_READINGS, value, labels)
                .await
            {
                warn!(
                    error = %e,
                    readout = readout.as_str(),
                    "backup account: this reading could not be published, so the reading face is \
                     missing a cell the account holds"
                );
            }
        }
    }

    debug!(
        runs_ok = account.ok,
        runs_failed = account.failed,
        runs_running = account.running,
        runs_abandoned = account.abandoned,
        latest_finish_at = ?account.latest_finish_at,
        oldest_running_at = ?account.oldest_running_at,
        oldest_running_deadline_seconds = ?account.oldest_running_deadline_seconds,
        newest_package = ?account.newest_package.as_ref().map(|p| p.run_id.as_str()),
        "backup account read"
    );

    Ok(account)
}

/// Start the loop that publishes the backup account's readings.
///
/// It is started only where the account's store was published, and not being
/// started is not a fault: a deployment without a shared database holds no
/// account to read, and the backup runner writes its rows to that same database
/// rather than to anything this process could see.
pub fn spawn_backup_runs_loop(
    store: Arc<dyn BackupRunStore>,
    role: Option<Arc<dyn OwnerLeaseBroker>>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    shutdown: ShutdownSignal,
) {
    info!(
        interval_secs = BACKUP_RUNS_PASS_INTERVAL.as_secs(),
        arbitrated = role.is_some(),
        readable = metrics.is_some(),
        "Backup account loop enabled"
    );
    if metrics.is_none() {
        warn!(
            "backup account: no metrics backend is published, so the account is read and nothing \
             is written down -- a run that never came back will be visible only to whoever opens \
             the table"
        );
    }

    drop(cog_core::loop_health::spawn(
        BACKUP_RUNS_LOOP,
        cog_core::loop_health::Cadence::Periodic(BACKUP_RUNS_PASS_INTERVAL),
        shutdown.clone(),
        move |beat| {
            let store = store.clone();
            let role = role.clone();
            let metrics = metrics.clone();
            let shutdown = shutdown.clone();
            async move {
                // One process at a time publishes the account. The readings are
                // a view of one table, so two replicas would agree; taking the
                // seat anyway is what makes "which process produced this
                // reading" a single answer, which is the question asked of a
                // reading that has stopped moving.
                let hold =
                    cog_core::RoleHold::start(role, BACKUP_RUNS_ROLE, beat.clone(), &shutdown);

                let mut ticker = tokio::time::interval(BACKUP_RUNS_PASS_INTERVAL);
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
                    if !hold.may_act().await {
                        continue;
                    }
                    if let Err(e) = publish_account_once(&*store, metrics.as_ref()).await {
                        warn!(
                            error = %e,
                            "backup account: the account could not be read this round, so the \
                             reading face keeps the last values it had -- which is the shape of a \
                             backup loop that has stopped, not of one that is running"
                        );
                    }
                }
            }
        },
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// The shipped rules, by name, as the chart holds them.
    ///
    /// Read from the chart's own file rather than from a rendered profile: the
    /// profiles are generated from it, so a rule renamed in the rendered copies
    /// and not here would be a rule the deployment does not have.
    fn shipped_rules() -> BTreeMap<String, String> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let path = root.join("deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&path).expect("read the chart's cogneva.json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("chart JSON parses");
        let rules = parsed["observability"]["infra_watch"]["rules"]
            .as_array()
            .expect("the chart ships an infra_watch rules array");
        assert!(
            !rules.is_empty(),
            "the path to the rules moved: the chart parsed but holds no rules"
        );
        rules
            .iter()
            .map(|rule| {
                (
                    rule["name"]
                        .as_str()
                        .expect("every rule carries a name")
                        .to_string(),
                    rule["promql"]
                        .as_str()
                        .expect("every rule carries an expression")
                        .to_string(),
                )
            })
            .collect()
    }

    /// The operators a `readout` selector can be written with, longest first so
    /// that `=~` is not read as `=`.
    const SELECTOR_OPERATORS: [&str; 4] = ["!~", "=~", "!=", "="];

    /// What one expression's `readout` selectors name.
    #[derive(Default)]
    struct ReadoutSelectors {
        /// Values the expression can return.
        selected: BTreeSet<String>,
        /// Values the expression excludes (`!=`, `!~`).
        excluded: BTreeSet<String>,
    }

    /// The series the account publishes its values on. The observed-timestamp
    /// companion next to it is part of the same family and carries the same
    /// selectors, so a name match is a family match.
    const ACCOUNT_SERIES: &str = "cogneva_backup_run_readings";

    /// Every value one expression's `readout` selectors name, on the account's
    /// own series.
    ///
    /// Both halves of the ledger check run on this, and neither can use a bare
    /// substring: `readout="kept"` is only the simplest form of the selector. An
    /// alternation names several values at once and names none of them literally
    /// -- read as text it leaves a line recorded as having no reader while a rule
    /// already reads it. A negation names everything except its operand, which is
    /// a different claim from selecting, so the two are reported apart.
    ///
    /// Only the selector blocks attached to the account's series count. The
    /// label name is not unique to this account -- the policy store publishes
    /// its own readings under `readout` too -- and a rule about another series
    /// is not a reader of this one, nor does its value have to appear in this
    /// account's roster. Reading every mention in every rule made the second
    /// flat for a rule that never selects the account.
    ///
    /// Alternatives have to be literal names: the roster is a closed set, so
    /// enumerating costs nothing, and a selector this cannot enumerate is one it
    /// cannot check in either direction.
    fn readout_selectors(promql: &str) -> ReadoutSelectors {
        let mut selectors = ReadoutSelectors::default();
        for block in account_selector_blocks(promql) {
            collect_readout_selectors(block, promql, &mut selectors);
        }
        selectors
    }

    /// The brace blocks attached to the account's series in one expression.
    ///
    /// The name is followed by the rest of its identifier (`_observed_...`),
    /// then by the selector; anything between the two means this occurrence is
    /// not a selector and is skipped rather than read across.
    fn account_selector_blocks(promql: &str) -> Vec<&str> {
        let mut blocks = Vec::new();
        let mut rest = promql;
        while let Some(at) = rest.find(ACCOUNT_SERIES) {
            rest = &rest[at + ACCOUNT_SERIES.len()..];
            let tail = rest.trim_start_matches(|c: char| c.is_ascii_alphanumeric() || c == '_');
            let Some(after) = tail.strip_prefix('{') else {
                continue;
            };
            let Some(end) = after.find('}') else {
                continue;
            };
            blocks.push(&after[..end]);
        }
        blocks
    }

    /// Read one selector block's `readout` selectors into `selectors`.
    fn collect_readout_selectors(block: &str, promql: &str, selectors: &mut ReadoutSelectors) {
        let mut rest = block;
        while let Some(at) = rest.find("readout") {
            rest = &rest[at + "readout".len()..];
            let trimmed = rest.trim_start();
            let Some((operator, after)) = SELECTOR_OPERATORS.iter().find_map(|operator| {
                trimmed
                    .strip_prefix(operator)
                    .map(|after| (*operator, after))
            }) else {
                // A mention that is not a selector -- `by (readout)` is the one
                // the rules actually use. Reading on past it keeps the scan
                // linear and makes such a mention cost nothing.
                continue;
            };
            let after = after.trim_start();
            let Some(after) = after.strip_prefix('"') else {
                continue;
            };
            let Some(end) = after.find('"') else {
                continue;
            };
            let named = &after[..end];
            rest = &after[end + 1..];

            let values: Vec<&str> = if operator.ends_with('~') {
                named.split('|').map(str::trim).collect()
            } else {
                vec![named]
            };
            for value in values {
                assert!(
                    !value.is_empty()
                        && value
                            .chars()
                            .all(|character| character.is_ascii_alphanumeric() || character == '_'),
                    "a readout selector this check cannot enumerate: {value:?} in {promql} -- name \
                     the values one at a time, so that both directions can read them"
                );
                if operator.starts_with('!') {
                    selectors.excluded.insert(value.to_string());
                } else {
                    selectors.selected.insert(value.to_string());
                }
            }
        }
    }

    /// The shipped rules whose expression names this value, by selecting it or by
    /// excluding it. Either way the rule is coupled to the value.
    fn rules_naming(rules: &BTreeMap<String, String>, value: &str) -> Vec<String> {
        rules
            .iter()
            .filter(|(_, promql)| {
                let selectors = readout_selectors(promql);
                selectors.selected.contains(value) || selectors.excluded.contains(value)
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Every value the loop can publish has exactly one line in
    /// [`READOUT_READERS`], and the table names nothing else.
    ///
    /// An equality rather than a bound in either direction: a missing line means
    /// a value on the exposition with nobody asked to read it, and a line for a
    /// value the account cannot publish means a reader waiting for a cell that
    /// never comes. The second is the one an upper bound would hide.
    #[test]
    fn every_published_readout_has_exactly_one_line_here() {
        let keyed: BTreeSet<String> = READOUT_READERS
            .iter()
            .map(|(readout, _)| readout.as_str().to_string())
            .collect();
        let published: BTreeSet<String> = BackupReadout::ALL
            .iter()
            .map(|readout| readout.as_str().to_string())
            .collect();

        assert_eq!(
            READOUT_READERS.len(),
            published.len(),
            "one line per published value, no value twice: {keyed:?}"
        );
        assert_eq!(
            keyed, published,
            "the values the account publishes and the values this table accounts for have to be \
             the same set"
        );
    }

    /// Each line is checked against the carrier it names, in both directions.
    ///
    /// The check is what keeps this table from being a second, self-consistent
    /// roster: a `Rule` line is verified against that rule's own expression, and
    /// an `Unread` line is verified against every shipped rule -- a value
    /// recorded as unread that some rule already selects would leave the rest of
    /// the lines worth nothing.
    #[test]
    fn the_readers_are_checked_against_the_rules_they_name() {
        let rules = shipped_rules();

        for (readout, reader) in READOUT_READERS {
            match reader {
                ReadoutReader::Rule(name) => {
                    let promql = rules.get(name).unwrap_or_else(|| {
                        panic!(
                            "readout {} names a rule the chart does not ship: {name}",
                            readout.as_str()
                        )
                    });
                    let selected = readout_selectors(promql).selected;
                    assert!(
                        selected.contains(readout.as_str()),
                        "readout {} is recorded as read by {name}, whose expression does not \
                         select it: {promql}",
                        readout.as_str()
                    );
                }
                ReadoutReader::Unread { why } => {
                    assert!(
                        !why.trim().is_empty(),
                        "readout {} has no reader and no reason it is still published",
                        readout.as_str()
                    );
                    let naming = rules_naming(&rules, readout.as_str());
                    assert!(
                        naming.is_empty(),
                        "readout {} is recorded as having no reader, but these rules read it: \
                         {naming:?}",
                        readout.as_str()
                    );
                }
            }
        }
    }

    /// No rule reads a value the account cannot publish.
    ///
    /// The table above is keyed by the producer's roster, so nothing in it can
    /// catch a name the *rules* have that the producer does not: a mistyped
    /// selector leaves a rule waiting on a cell that is never published, and a
    /// rule that cannot match anything never fires. Nothing else looks at that
    /// name, so the rule would stay dark with every gate green.
    #[test]
    fn every_value_a_rule_reads_is_one_the_account_publishes() {
        let rules = shipped_rules();
        let published: BTreeSet<&str> = BackupReadout::ALL
            .iter()
            .map(|readout| readout.as_str())
            .collect();

        let mut read = 0usize;
        for (name, promql) in &rules {
            let selectors = readout_selectors(promql);
            for value in selectors.selected.iter().chain(selectors.excluded.iter()) {
                read += 1;
                assert!(
                    published.contains(value.as_str()),
                    "rule {name} reads readout {value:?}, which the account never publishes; it \
                     can only be a mistyped selector, and a rule that matches nothing never fires. \
                     The account publishes {published:?}"
                );
            }
        }

        assert!(
            read > 0,
            "no rule reads the backup account at all -- either the rules moved, or the selectors \
             are spelled a way this check no longer recognises"
        );
    }

    /// The label name is shared: a `readout` selector on another series says
    /// nothing about this account, and its value has no reason to be published
    /// here.
    #[test]
    fn a_readout_selector_on_another_series_is_not_a_reader_of_this_account() {
        let foreign = "count(cogneva_policy_store_readings{readout=\"data_dir\"} and \
             ((time() - cogneva_policy_store_readings_observed_timestamp_seconds\
             {readout=\"data_dir\"}) < 1800)) > bool count(cogneva_policy_store_readings\
             {readout=\"active_version\"})";
        let selectors = readout_selectors(foreign);
        assert!(
            selectors.selected.is_empty() && selectors.excluded.is_empty(),
            "a rule about another series was read as a reader of this account: {selectors:?}",
            selectors = (selectors.selected, selectors.excluded)
        );

        let account = "max(cogneva_backup_run_readings{readout=\"runs_ok\"} and \
             ((time() - cogneva_backup_run_readings_observed_timestamp_seconds\
             {readout=\"runs_ok\"}) < 1800))";
        let selectors = readout_selectors(account);
        assert_eq!(
            selectors.selected,
            BTreeSet::from(["runs_ok".to_string()]),
            "the account's own selector still has to be read"
        );
    }
}
