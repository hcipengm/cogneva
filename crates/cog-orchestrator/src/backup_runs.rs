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
/// length is fixed by `ALL`, and every `Rule` line is checked against the rule
/// it names -- a line claiming a rule reads a value that rule does not select is
/// the one failure this table could hide from itself.
///
/// It is built only for the tests, because nothing in the loop reads it: the
/// account publishes the same values whether or not this table exists, and the
/// table's whole job is to be checked -- against the producer for which values
/// there are, and against the chart for who reads them.
#[cfg(test)]
const READOUT_READERS: [(BackupReadout, ReadoutReader); BackupReadout::ALL.len()] = [
    (
        BackupReadout::RunsRunning,
        ReadoutReader::Unread {
            why: "the only positive reading that a run is open right now, and it is published even \
                  when the account holds nothing -- so its zero is a number a reader can act on \
                  rather than an absence. It is owed a rule, and until then an open run is visible \
                  only through its age, which is absent when there is none.",
        },
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
        ReadoutReader::Unread {
            why: "the only exit for a run the account had to conclude itself, and it is published \
                  even when the account holds nothing. The rule that reads runs_failed cannot see \
                  this state at all -- a run that went missing reported nothing -- so this is owed \
                  a rule of its own, and a rule added here has to say what it means when the value \
                  is zero, which is not the same as absent.",
        },
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
        ReadoutReader::Unread {
            why: "read with keep_target: a retention pass that keeps nothing is invisible to a \
                  count of runs, since the run still succeeded. Owed a rule; its absence means the \
                  newest successful run was older than any package age recorded, which the rule \
                  reading it would have to say.",
        },
    ),
    (
        BackupReadout::KeepTarget,
        ReadoutReader::Unread {
            why: "the bound that kept is measured against. Published so that the pair is read \
                  together rather than by comparing a count against a number baked into a rule; \
                  owed a rule that reads the two against each other.",
        },
    ),
    (
        BackupReadout::OldestKeptAgeSeconds,
        ReadoutReader::Unread {
            why: "the half a count cannot falsify: packages can stop being pruned while every run \
                  keeps succeeding, and then the surviving set ages without any other value on this \
                  series moving. Owed a rule.",
        },
    ),
    (
        BackupReadout::NewestPackageBytes,
        ReadoutReader::Unread {
            why: "a backup that stops capturing shrinks, and nothing else on this series moves when \
                  it does -- a run that wrote a smaller package still concluded ok. Owed a rule; \
                  being a size, its bound belongs to the rule rather than here.",
        },
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

    /// Every value one expression's `readout` selectors name.
    ///
    /// Both halves of the ledger check run on this, and neither can use a bare
    /// substring: `readout="kept"` is only the simplest form of the selector. An
    /// alternation names several values at once and names none of them literally
    /// -- read as text it leaves a line recorded as having no reader while a rule
    /// already reads it. A negation names everything except its operand, which is
    /// a different claim from selecting, so the two are reported apart.
    ///
    /// Alternatives have to be literal names: the roster is a closed set, so
    /// enumerating costs nothing, and a selector this cannot enumerate is one it
    /// cannot check in either direction.
    fn readout_selectors(promql: &str) -> ReadoutSelectors {
        let mut selectors = ReadoutSelectors::default();
        let mut rest = promql;
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
        selectors
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
}
