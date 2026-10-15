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
    metric_names, BackupAccountReading, BackupRunStore, MetricsBackend, OwnerLeaseBroker,
    ShutdownSignal,
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
        }
    }
}

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

    if let Some(metrics) = metrics {
        for readout in BackupReadout::ALL {
            let Some(value) = value_of(*readout, &account, now) else {
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
