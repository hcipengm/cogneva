//!PostgreSQL-backed store for the backup account.
//!
//!The table is created by the storage migrations, not by an `init_schema` on
//!this type. Its columns are the contract's own record -- the package, the
//!instants, the retention pair -- and a second CREATE TABLE here would be a
//!second definition of that record: the two would drift, and the one that wins
//!is whichever ran last.
//!
//!Unlike the ledgers, this table has more than one writer by design: every
//!backup run writes its own row, and the runs are separate processes launched on
//!a schedule. What keeps that from being several accounts is the key -- one run
//!identity, derived from the clock, and a closing that is refused unless the row
//!is still open. Two writers therefore cannot both conclude the same run.
//!
//!There is a second author of conclusions, and it is the account's own pass
//!rather than a run: `abandon_overdue` closes rows whose run stopped reporting.
//!It cannot collide with a run either, for the same reason a run cannot collide
//!with itself -- both move a row out of `running`, and only one of them can find
//!the row still there. What separates their conclusions is the outcome they
//!write, so a reader selecting by state can tell which author it is looking at.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{
    BackupAccountReading, BackupDrillRecord, BackupPackageRecord, BackupRunClosing,
    BackupRunOpening, BackupRunStore, SFError, SFResult,
};

/// PostgreSQL backup account store.
#[derive(Clone)]
pub struct PostgresBackupRunStore {
    pool: PgPool,
}

impl PostgresBackupRunStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Open a run. The conflict clause writes nothing, so the statement's own row
/// count is the answer to "was this row newly written" rather than a second
/// query that could race with it.
///
/// The clause is what makes a repeated opening harmless rather than destructive:
/// without it a second run claiming an identity already in the table would move
/// a start instant that has already been recorded and read.
const OPEN_RUN_SQL: &str = "\
INSERT INTO backup_runs (run_id, started_at, outcome, keep_target, run_deadline_seconds) \
VALUES ($1, $2, 'running', $3, $4) \
ON CONFLICT (run_id) DO NOTHING";

/// Close a run, and only if it is still open. The guard makes the statement a
/// conclusion about this run rather than an assignment: a row that already holds
/// a terminal outcome is left where it is, so the first answer a run gave stands
/// and a second report cannot move it.
const CLOSE_RUN_SQL: &str = "\
UPDATE backup_runs \
SET outcome = $2, finished_at = $3, package = $4, bytes = $5, package_checksum = $6, \
    package_coverage = $7, kept_after = $8, oldest_kept_at = $9, error = $10 \
WHERE run_id = $1 AND outcome = 'running'";

/// Conclude every run that has outlived the deadline it was opened under.
///
/// The bound is read from each row rather than passed in, so the statement
/// cannot conclude a run that is still inside the time it was given -- which is
/// the one thing this pass must never do, since the row it would close is a run
/// that may still be working. The same clause is what makes the pass safe to run
/// at any moment and any number of times: a row leaves `running` once.
///
/// `finished_at` is left as it is, which for these rows is NULL. There is no
/// instant at which the run finished; inventing one would put a number in a
/// column that means "when this run ended" while the end is exactly what nobody
/// saw. The error text carries the number that decided it, so the row can be
/// read without knowing the configuration in force that day.
const ABANDON_OVERDUE_SQL: &str = "\
UPDATE backup_runs \
SET outcome = 'abandoned', \
    error = format('%s (the deadline it was opened under is %s s)', $1, run_deadline_seconds) \
WHERE outcome = 'running' \
  AND started_at + make_interval(secs => run_deadline_seconds) <= $2";

/// The whole account in one look.
///
/// The snapshot is materialized so that its counts, both halves of the open-run
/// pair and the newest package come from one instant. Read as separate
/// statements they could straddle a run finishing, and the reading would carry a
/// count from before that run and a package from after it -- two moments
/// presented as one, which is exactly the shape of error a reader cannot see.
///
/// The newest package is taken from the rows that concluded `ok` and not from
/// the newest row: the package fields are the ones a run that produced something
/// wrote, and a failed run has none to offer.
///
/// The open run is joined rather than aggregated twice. Its start instant and
/// the deadline it was opened under are one row's two columns, and two scalar
/// subqueries over the same predicate would be two lookups that a tie in
/// `started_at` could resolve to two different rows -- an age from one run
/// measured against another run's deadline, which is a comparison nobody asked
/// for and nobody could see.
const READ_ACCOUNT_SQL: &str = "\
WITH snapshot AS MATERIALIZED ( \
    SELECT run_id, started_at, finished_at, outcome, package, bytes, package_checksum, \
           package_coverage, keep_target, run_deadline_seconds, kept_after, oldest_kept_at \
    FROM backup_runs \
) \
SELECT \
    (SELECT count(*) FROM snapshot WHERE outcome = 'running')        AS running, \
    (SELECT count(*) FROM snapshot WHERE outcome = 'ok')             AS ok_count, \
    (SELECT count(*) FROM snapshot WHERE outcome = 'failed')         AS failed, \
    (SELECT count(*) FROM snapshot WHERE outcome = 'abandoned')      AS abandoned, \
    (SELECT max(finished_at) FROM snapshot)                          AS latest_finish_at, \
    open.started_at            AS open_started_at, \
    open.run_deadline_seconds  AS open_run_deadline_seconds, \
    newest.run_id          AS package_run_id, \
    newest.finished_at     AS package_finished_at, \
    newest.package         AS package, \
    newest.bytes           AS package_bytes, \
    newest.package_checksum AS package_checksum, \
    newest.package_coverage AS package_coverage, \
    newest.keep_target     AS package_keep_target, \
    newest.kept_after      AS package_kept_after, \
    newest.oldest_kept_at  AS package_oldest_kept_at \
FROM (SELECT 1) AS one \
LEFT JOIN LATERAL ( \
    SELECT * FROM snapshot WHERE outcome = 'running' ORDER BY started_at LIMIT 1 \
) AS open ON TRUE \
LEFT JOIN LATERAL ( \
    SELECT * FROM snapshot WHERE outcome = 'ok' ORDER BY started_at DESC LIMIT 1 \
) AS newest ON TRUE";

/// How many packages in the retention window have no successful drill.
///
/// The window is the packages the last successful runs recorded, cut at the
/// newest run's own `keep_target`; it is the writer's account of what it kept.
/// The drill side takes each package's **latest** row, so a package that was
/// proven a week ago and failed last night counts as unproven -- the question
/// is what is proven now, not what was proven once.
///
/// A package with no drill row at all joins to nulls and counts too: "never
/// opened" and "opened and failed" are one answer here, and the row the drill
/// writes is where the difference lives.
///
/// `LIMIT` cannot be a bare subquery over the table being read, so the bound
/// comes from the newest run; a null bound (no run ever succeeded) yields an
/// empty window and a count of zero, which is the state the backup rules
/// already speak to.
const UNPROVEN_PACKAGES_SQL: &str = "\
WITH kept AS ( \
    SELECT package FROM backup_runs \
    WHERE outcome = 'ok' AND package IS NOT NULL \
    ORDER BY started_at DESC \
    LIMIT coalesce((SELECT keep_target FROM backup_runs ORDER BY started_at DESC LIMIT 1), 0) \
), latest AS ( \
    SELECT DISTINCT ON (package) package, error FROM backup_drills \
    WHERE package IS NOT NULL \
    ORDER BY package, started_at DESC \
) \
SELECT count(*) FROM kept \
LEFT JOIN latest USING (package) \
WHERE latest.package IS NULL OR latest.error IS NOT NULL";

/// One drill's answer, written once.
///
/// The outcome is derived from the record rather than taken from a caller's
/// word -- `backup_drills_ok_carries_its_evidence` refuses an `ok` row whose
/// measurements are missing or disagreeing, and deriving the value here means
/// the writer cannot ask for one those measurements do not support.
///
/// A row for a drill identity that already exists is left alone: two drills of
/// the same package are two answers taken at two instants, and the second one
/// nobody can see is the second one that disagrees.
const RECORD_DRILL_SQL: &str = "\
INSERT INTO backup_drills ( \
    drill_id, started_at, finished_at, outcome, package, package_written_at, package_bytes, \
    packages_on_volume, oldest_package_at, offsite_newest_at, offsite_matches_newest, \
    tables_restored, tables_expected, \
    rows_restored, rows_expected, source_runs_matched, error, \
    generated_columns_recomputed, artifacts_expected, artifacts_restored, \
    artifact_readings, artifact_errors, offsite_observed_at, offsite_remote_newest_at, offsite_key_created_at, \
    segment_skew_seconds, segment_skew_bound_seconds, offsite_missing_packages, \
    credential_violations_backup_pvc, credential_violations_offsite, credential_violations_state_dir, \
    tables_without_a_home \
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, \
          $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32) \
ON CONFLICT (drill_id) DO NOTHING";

/// The newest drill, failed or not.
///
/// Ordered by the instant the drill finished and broken by its identity, so two
/// drills that finished within the same clock second still resolve to one row
/// and the same one on every read.
const READ_LATEST_DRILL_SQL: &str = "\
SELECT drill_id, started_at, finished_at, outcome, package, package_written_at, package_bytes, \
       packages_on_volume, oldest_package_at, offsite_newest_at, offsite_matches_newest, \
       tables_restored, tables_expected, \
       rows_restored, rows_expected, source_runs_matched, error, \
       generated_columns_recomputed, artifacts_expected, artifacts_restored, \
       artifact_readings, artifact_errors, offsite_observed_at, offsite_remote_newest_at, offsite_key_created_at, \
       segment_skew_seconds, segment_skew_bound_seconds, offsite_missing_packages, \
       credential_violations_backup_pvc, credential_violations_offsite, credential_violations_state_dir, \
       tables_without_a_home \
FROM backup_drills \
ORDER BY finished_at DESC, drill_id DESC \
LIMIT 1";

#[async_trait]
impl BackupRunStore for PostgresBackupRunStore {
    async fn open_run(&self, opening: &BackupRunOpening) -> SFResult<bool> {
        if opening.run_id.trim().is_empty() {
            return Err(SFError::Validation(
                "a backup run's identity cannot be empty: the row would be unreadable as a name \
                 for the run it records"
                    .into(),
            ));
        }
        let outcome = sqlx::query(OPEN_RUN_SQL)
            .bind(&opening.run_id)
            .bind(opening.started_at)
            .bind(opening.keep_target)
            .bind(opening.run_deadline_seconds)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("open backup run {}: {e}", opening.run_id)))?;
        Ok(outcome.rows_affected() > 0)
    }

    async fn close_run(&self, run_id: &str, closing: &BackupRunClosing) -> SFResult<bool> {
        if !closing.outcome.is_a_runs_own_report() {
            return Err(SFError::Validation(format!(
                "a backup run cannot close itself as {}: a run either reports how it ended or it \
                 stops being able to, and the second conclusion is the account's to write",
                closing.outcome.as_str()
            )));
        }
        let outcome = sqlx::query(CLOSE_RUN_SQL)
            .bind(run_id)
            .bind(closing.outcome.as_str())
            .bind(closing.finished_at)
            .bind(closing.package.as_deref())
            .bind(closing.bytes)
            .bind(closing.package_checksum.as_deref())
            .bind(closing.package_coverage.as_deref())
            .bind(closing.kept_after)
            .bind(closing.oldest_kept_at)
            .bind(closing.error.as_deref())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("close backup run {run_id}: {e}")))?;
        Ok(outcome.rows_affected() > 0)
    }

    async fn abandon_overdue(&self, now: DateTime<Utc>) -> SFResult<u64> {
        let outcome = sqlx::query(ABANDON_OVERDUE_SQL)
            .bind(
                "the run never reported and its deadline has passed, so the account concludes it \
                 here: the process that opened this row was killed or wedged and cannot close it",
            )
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("abandon overdue backup runs: {e}")))?;
        Ok(outcome.rows_affected())
    }

    async fn read_account(&self) -> SFResult<BackupAccountReading> {
        let row = sqlx::query(READ_ACCOUNT_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the backup account: {e}")))?;

        // The open run's two columns are absent together -- the lateral join
        // yields one row of nulls when nothing is open -- so the start instant
        // decides whether there is a pair to read. The deadline is NOT NULL in
        // the column, which makes a start without one unreadable rather than
        // silently paired with a zero.
        let oldest_running_at: Option<chrono::DateTime<chrono::Utc>> = row
            .try_get("open_started_at")
            .map_err(|e| SFError::Database(format!("open_started_at: {e}")))?;
        let oldest_running_deadline_seconds = match oldest_running_at {
            None => None,
            Some(_) => Some(
                row.try_get::<Option<i32>, _>("open_run_deadline_seconds")
                    .map_err(|e| SFError::Database(format!("open_run_deadline_seconds: {e}")))?
                    .ok_or_else(|| {
                        SFError::Database(
                            "a backup run is open without the deadline it was opened under, so \
                             nothing can decide whether it is late or gone"
                                .into(),
                        )
                    })?,
            ),
        };

        let package_run_id: Option<String> = row
            .try_get("package_run_id")
            .map_err(|e| SFError::Database(format!("package_run_id: {e}")))?;
        // The newest package's columns are all absent together -- the lateral
        // join yields one row of nulls when no run has ever concluded `ok` --
        // so the identity decides whether there is a record to read at all.
        let newest_package = match package_run_id {
            None => None,
            Some(run_id) => {
                let finished_at: Option<chrono::DateTime<chrono::Utc>> = row
                    .try_get("package_finished_at")
                    .map_err(|e| SFError::Database(format!("package_finished_at: {e}")))?;
                let finished_at = finished_at.ok_or_else(|| {
                    SFError::Database(format!(
                        "backup run {run_id} concluded ok without a finish instant, so the \
                         account cannot say when it finished"
                    ))
                })?;
                Some(BackupPackageRecord {
                    run_id,
                    finished_at,
                    package: row
                        .try_get("package")
                        .map_err(|e| SFError::Database(format!("package: {e}")))?,
                    bytes: row
                        .try_get("package_bytes")
                        .map_err(|e| SFError::Database(format!("package_bytes: {e}")))?,
                    package_checksum: row
                        .try_get("package_checksum")
                        .map_err(|e| SFError::Database(format!("package_checksum: {e}")))?,
                    package_coverage: row
                        .try_get("package_coverage")
                        .map_err(|e| SFError::Database(format!("package_coverage: {e}")))?,
                    keep_target: row
                        .try_get("package_keep_target")
                        .map_err(|e| SFError::Database(format!("package_keep_target: {e}")))?,
                    kept_after: row
                        .try_get("package_kept_after")
                        .map_err(|e| SFError::Database(format!("package_kept_after: {e}")))?,
                    oldest_kept_at: row
                        .try_get("package_oldest_kept_at")
                        .map_err(|e| SFError::Database(format!("package_oldest_kept_at: {e}")))?,
                })
            }
        };

        // The window is what the writer says it kept, not what the directory
        // holds: this reader has no directory to walk. The drill side is matched
        // by package name, and a package whose latest drill failed counts the
        // same as one no drill ever opened -- both leave the package unproven,
        // and the difference between them is the drill's own row, not this
        // count.
        let drill_unproven_packages: i64 = sqlx::query_scalar(UNPROVEN_PACKAGES_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count the window's unproven packages: {e}")))?;

        Ok(BackupAccountReading {
            running: row
                .try_get("running")
                .map_err(|e| SFError::Database(format!("running: {e}")))?,
            ok: row
                .try_get("ok_count")
                .map_err(|e| SFError::Database(format!("ok_count: {e}")))?,
            failed: row
                .try_get("failed")
                .map_err(|e| SFError::Database(format!("failed: {e}")))?,
            abandoned: row
                .try_get("abandoned")
                .map_err(|e| SFError::Database(format!("abandoned: {e}")))?,
            latest_finish_at: row
                .try_get("latest_finish_at")
                .map_err(|e| SFError::Database(format!("latest_finish_at: {e}")))?,
            oldest_running_at,
            oldest_running_deadline_seconds,
            newest_package,
            drill_unproven_packages,
        })
    }

    async fn record_drill(&self, drill: &BackupDrillRecord) -> SFResult<()> {
        if drill.drill_id.trim().is_empty() {
            return Err(SFError::Validation(
                "a drill's identity cannot be empty: the row would be unreadable as a name for \
                 the pass it records"
                    .into(),
            ));
        }
        // A drill that did not prove the package restorable has to say what it
        // found instead. The column's check holds the same rule, but reaching it
        // would report a constraint name where the caller can be told which
        // field is missing.
        if !drill.ok() && drill.error.as_deref().is_none_or(|e| e.trim().is_empty()) {
            return Err(SFError::Validation(format!(
                "drill {} did not prove its package restorable and carries no error text, so the \
                 row would record a failure without a reason",
                drill.drill_id
            )));
        }
        let outcome = if drill.ok() { "ok" } else { "failed" };
        sqlx::query(RECORD_DRILL_SQL)
            .bind(&drill.drill_id)
            .bind(drill.started_at)
            .bind(drill.finished_at)
            .bind(outcome)
            .bind(drill.package.as_deref())
            .bind(drill.package_written_at)
            .bind(drill.package_bytes)
            .bind(drill.packages_on_volume)
            .bind(drill.oldest_package_at)
            .bind(drill.offsite_newest_at)
            .bind(drill.offsite_matches_newest)
            .bind(drill.tables_restored)
            .bind(drill.tables_expected)
            .bind(drill.rows_restored)
            .bind(drill.rows_expected)
            .bind(drill.source_runs_matched)
            .bind(drill.error.as_deref())
            .bind(&drill.generated_columns_recomputed)
            .bind(drill.artifacts_expected)
            .bind(drill.artifacts_restored)
            .bind(&drill.artifact_readings)
            .bind(&drill.artifact_errors)
            .bind(drill.offsite_observed_at)
            .bind(drill.offsite_remote_newest_at)
            .bind(drill.offsite_key_created_at)
            .bind(drill.segment_skew_seconds)
            .bind(drill.segment_skew_bound_seconds)
            .bind(drill.offsite_missing_packages)
            .bind(drill.credential_violations_backup_pvc)
            .bind(drill.credential_violations_offsite)
            .bind(drill.credential_violations_state_dir)
            .bind(&drill.tables_without_a_home)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SFError::Database(format!("record backup drill {}: {e}", drill.drill_id))
            })?;
        Ok(())
    }

    async fn read_latest_drill(&self) -> SFResult<Option<BackupDrillRecord>> {
        let row = sqlx::query(READ_LATEST_DRILL_SQL)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the newest backup drill: {e}")))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let drill_id: String = row
            .try_get("drill_id")
            .map_err(|e| SFError::Database(format!("drill_id: {e}")))?;
        let started_at: DateTime<Utc> = row
            .try_get("started_at")
            .map_err(|e| SFError::Database(format!("drill started_at: {e}")))?;
        let finished_at: DateTime<Utc> = row
            .try_get("finished_at")
            .map_err(|e| SFError::Database(format!("drill finished_at: {e}")))?;
        Ok(Some(BackupDrillRecord {
            drill_id,
            started_at,
            finished_at,
            package: row
                .try_get("package")
                .map_err(|e| SFError::Database(format!("drill package: {e}")))?,
            package_written_at: row
                .try_get("package_written_at")
                .map_err(|e| SFError::Database(format!("drill package_written_at: {e}")))?,
            package_bytes: row
                .try_get("package_bytes")
                .map_err(|e| SFError::Database(format!("drill package_bytes: {e}")))?,
            packages_on_volume: row
                .try_get("packages_on_volume")
                .map_err(|e| SFError::Database(format!("drill packages_on_volume: {e}")))?,
            oldest_package_at: row
                .try_get("oldest_package_at")
                .map_err(|e| SFError::Database(format!("drill oldest_package_at: {e}")))?,
            offsite_newest_at: row
                .try_get("offsite_newest_at")
                .map_err(|e| SFError::Database(format!("drill offsite_newest_at: {e}")))?,
            offsite_matches_newest: row
                .try_get("offsite_matches_newest")
                .map_err(|e| SFError::Database(format!("drill offsite_matches_newest: {e}")))?,
            tables_restored: row
                .try_get("tables_restored")
                .map_err(|e| SFError::Database(format!("drill tables_restored: {e}")))?,
            tables_expected: row
                .try_get("tables_expected")
                .map_err(|e| SFError::Database(format!("drill tables_expected: {e}")))?,
            rows_restored: row
                .try_get("rows_restored")
                .map_err(|e| SFError::Database(format!("drill rows_restored: {e}")))?,
            rows_expected: row
                .try_get("rows_expected")
                .map_err(|e| SFError::Database(format!("drill rows_expected: {e}")))?,
            source_runs_matched: row
                .try_get("source_runs_matched")
                .map_err(|e| SFError::Database(format!("drill source_runs_matched: {e}")))?,
            generated_columns_recomputed: row.try_get("generated_columns_recomputed").map_err(
                |e| SFError::Database(format!("drill generated_columns_recomputed: {e}")),
            )?,
            tables_without_a_home: row
                .try_get("tables_without_a_home")
                .map_err(|e| SFError::Database(format!("drill tables_without_a_home: {e}")))?,
            artifacts_expected: row
                .try_get("artifacts_expected")
                .map_err(|e| SFError::Database(format!("drill artifacts_expected: {e}")))?,
            artifacts_restored: row
                .try_get("artifacts_restored")
                .map_err(|e| SFError::Database(format!("drill artifacts_restored: {e}")))?,
            artifact_readings: row
                .try_get("artifact_readings")
                .map_err(|e| SFError::Database(format!("drill artifact_readings: {e}")))?,
            artifact_errors: row
                .try_get("artifact_errors")
                .map_err(|e| SFError::Database(format!("drill artifact_errors: {e}")))?,
            offsite_observed_at: row
                .try_get("offsite_observed_at")
                .map_err(|e| SFError::Database(format!("drill offsite_observed_at: {e}")))?,
            offsite_remote_newest_at: row
                .try_get("offsite_remote_newest_at")
                .map_err(|e| SFError::Database(format!("drill offsite_remote_newest_at: {e}")))?,
            offsite_key_created_at: row
                .try_get("offsite_key_created_at")
                .map_err(|e| SFError::Database(format!("drill offsite_key_created_at: {e}")))?,
            segment_skew_seconds: row
                .try_get("segment_skew_seconds")
                .map_err(|e| SFError::Database(format!("drill segment_skew_seconds: {e}")))?,
            segment_skew_bound_seconds: row
                .try_get("segment_skew_bound_seconds")
                .map_err(|e| SFError::Database(format!("drill segment_skew_bound_seconds: {e}")))?,
            offsite_missing_packages: row
                .try_get("offsite_missing_packages")
                .map_err(|e| SFError::Database(format!("drill offsite_missing_packages: {e}")))?,
            credential_violations_backup_pvc: row
                .try_get("credential_violations_backup_pvc")
                .map_err(|e| {
                    SFError::Database(format!("drill credential_violations_backup_pvc: {e}"))
                })?,
            credential_violations_offsite: row.try_get("credential_violations_offsite").map_err(
                |e| SFError::Database(format!("drill credential_violations_offsite: {e}")),
            )?,
            credential_violations_state_dir: row
                .try_get("credential_violations_state_dir")
                .map_err(|e| {
                    SFError::Database(format!("drill credential_violations_state_dir: {e}"))
                })?,
            error: row
                .try_get("error")
                .map_err(|e| SFError::Database(format!("drill error: {e}")))?,
        }))
    }
}
