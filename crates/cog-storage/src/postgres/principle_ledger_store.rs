//!PostgreSQL-backed store for the principle ledger.
//!
//!One writer, and that writer is a lease-elected loop. The account's whole value
//!is that there is *one* account, so the write path here is built for a single
//!caller rather than arbitrating between several: what a second concurrent
//!writer would produce is two versions of the same account and no way to say
//!which one is the ledger.
//!
//!The tables are created by the storage migrations, not by an `init_schema` on
//!this type. The account's key is the principle's slug -- the identity the
//!registry, the memory list and the carrier's filename all spell the same way --
//!and a second CREATE TABLE here would be a second definition of it: the two
//!would drift, and the one that wins is whichever ran last.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{
    PrincipleCensus, PrincipleDrift, PrincipleFold, PrincipleLedgerStore, PrincipleMeasurement,
    PrincipleReading, PrincipleRefusal, PrincipleRow, PrincipleState, SFError, SFResult,
};

/// Every read returns these, in one place so no two readers can come back with
/// differently ordered columns.
const ROW_COLUMNS: &str = "slug, lead, carrier_path, carrier_sha256, host_gate, platform_gate, \
                           state, refusals, last_refused_at, first_seen_at, measured_at, \
                           measured_rev";

/// How wide each keyed column may be, taken from the columns that key them
/// rather than from what a caller would like to write.
///
/// The bounds are the narrower dialect's (MySQL cannot key an unbounded TEXT
/// column), so one set governs both. Widths are counted in characters, which is
/// what both dialects' VARCHAR lengths mean.
const SLUG_MAX: usize = 191;
const SOURCE_MAX: usize = 512;
const REFUSAL_SOURCE_MAX: usize = 256;
const SHA256_MAX: usize = 64;

/// PostgreSQL principle ledger store.
#[derive(Clone)]
pub struct PostgresPrincipleLedgerStore {
    pool: PgPool,
}

impl PostgresPrincipleLedgerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// A row as callers see it, or why it cannot be one.
    ///
    /// An unreadable row fails the read instead of being skipped. A dropped row
    /// is indistinguishable from a principle the account does not hold, and the
    /// reader acts on that difference: it would report a gated principle as a
    /// debt, or a debt as nothing at all.
    fn row_to_entry(row: &sqlx::postgres::PgRow) -> Result<PrincipleRow, String> {
        let state: Option<String> = row.try_get("state").map_err(|e| format!("state: {e}"))?;
        let state = match state {
            Some(text) => Some(
                PrincipleState::parse(&text)
                    .ok_or_else(|| format!("state column holds `{text}`, which is not a state"))?,
            ),
            None => None,
        };
        Ok(PrincipleRow {
            slug: row.try_get("slug").map_err(|e| format!("slug: {e}"))?,
            lead: row.try_get("lead").map_err(|e| format!("lead: {e}"))?,
            carrier_path: row
                .try_get("carrier_path")
                .map_err(|e| format!("carrier_path: {e}"))?,
            carrier_sha256: row
                .try_get("carrier_sha256")
                .map_err(|e| format!("carrier_sha256: {e}"))?,
            host_gate: row
                .try_get("host_gate")
                .map_err(|e| format!("host_gate: {e}"))?,
            platform_gate: row
                .try_get("platform_gate")
                .map_err(|e| format!("platform_gate: {e}"))?,
            state,
            refusals: row
                .try_get("refusals")
                .map_err(|e| format!("refusals: {e}"))?,
            last_refused_at: row
                .try_get("last_refused_at")
                .map_err(|e| format!("last_refused_at: {e}"))?,
            first_seen_at: row
                .try_get("first_seen_at")
                .map_err(|e| format!("first_seen_at: {e}"))?,
            measured_at: row
                .try_get("measured_at")
                .map_err(|e| format!("measured_at: {e}"))?,
            measured_rev: row
                .try_get("measured_rev")
                .map_err(|e| format!("measured_rev: {e}"))?,
        })
    }

    /// A queued reading as callers see it.
    fn row_to_reading(row: &sqlx::postgres::PgRow) -> Result<PrincipleReading, String> {
        Ok(PrincipleReading {
            slug: row.try_get("slug").map_err(|e| format!("slug: {e}"))?,
            source: row.try_get("source").map_err(|e| format!("source: {e}"))?,
            lead: row.try_get("lead").map_err(|e| format!("lead: {e}"))?,
            carrier_path: row
                .try_get("carrier_path")
                .map_err(|e| format!("carrier_path: {e}"))?,
            carrier_sha256: row
                .try_get("carrier_sha256")
                .map_err(|e| format!("carrier_sha256: {e}"))?,
            host_gate: row
                .try_get("host_gate")
                .map_err(|e| format!("host_gate: {e}"))?,
        })
    }

    /// Every row read here goes through the one conversion above, so no two
    /// readers can disagree about what a row means.
    fn entries(rows: Vec<sqlx::postgres::PgRow>) -> SFResult<Vec<PrincipleRow>> {
        rows.iter()
            .map(|row| Self::row_to_entry(row).map_err(SFError::Database))
            .collect()
    }
}

/// Write one reading into the account, on whatever executor the caller has.
///
/// Takes an executor rather than the pool because the fold writes inside a
/// transaction and this write is one statement of it: a version bound to the
/// pool could only be called outside one, and a fold whose steps run in
/// separate transactions is a fold that can stop between them.
async fn write_reading<'e, E>(exec: E, reading: &PrincipleReading) -> SFResult<()>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(RECORD_READING_SQL)
        .bind(&reading.slug)
        .bind(&reading.lead)
        .bind(&reading.carrier_path)
        .bind(&reading.carrier_sha256)
        .bind(&reading.host_gate)
        .execute(exec)
        .await
        .map_err(|e| SFError::Database(format!("record reading for {}: {e}", reading.slug)))?;
    Ok(())
}

#[async_trait]
impl PrincipleLedgerStore for PostgresPrincipleLedgerStore {
    async fn record_readings(&self, readings: &[PrincipleReading]) -> SFResult<usize> {
        let mut stored = 0usize;
        for reading in readings {
            check_reading(reading).map_err(SFError::Validation)?;
            write_reading(&self.pool, reading).await?;
            stored += 1;
        }
        Ok(stored)
    }

    async fn declare(&self, readings: &[PrincipleReading]) -> SFResult<usize> {
        let mut queued = 0usize;
        for reading in readings {
            check_reading(reading).map_err(SFError::Validation)?;
            let outcome = sqlx::query(DECLARE_SQL)
                .bind(&reading.slug)
                .bind(&reading.source)
                .bind(&reading.lead)
                .bind(&reading.carrier_path)
                .bind(&reading.carrier_sha256)
                .bind(&reading.host_gate)
                .execute(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("declare {}: {e}", reading.slug)))?;
            // The conflict clause writes nothing, so a reading already queued
            // contributes no row here. Counting the statement rather than the
            // batch is what makes "how many were newly queued" answerable.
            queued += outcome.rows_affected() as usize;
        }
        Ok(queued)
    }

    async fn fold_declarations(&self, limit: usize) -> SFResult<PrincipleFold> {
        let rows = sqlx::query(FOLD_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read readings to fold: {e}")))?;
        let readings: Vec<PrincipleReading> = rows
            .iter()
            .map(|row| Self::row_to_reading(row).map_err(SFError::Database))
            .collect::<SFResult<_>>()?;
        if readings.is_empty() {
            return Ok(PrincipleFold::default());
        }

        // Everything from reading the held hashes to marking the readings folded
        // rides in one transaction, and the reason is the drift reading rather
        // than the writes. The writes are idempotent, so repeating them costs
        // nothing; the *difference* is not, because this fold replaces the hash
        // it compares against. Run in separate transactions and interrupted in
        // between, the next pass reads a stored hash that the interrupted pass
        // already overwrote, finds no difference, and the fact that this rule's
        // text moved away from what its gate was measured against is never said
        // by anything. One commit covers the comparison and the overwrite it
        // depends on, so the two are either both in or both out.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("open the fold: {e}")))?;

        // What the account already holds for these slugs, read before anything
        // is written: the carrier hash whose difference from the pushed one is
        // the drift reading, and which the write below is about to replace.
        let slugs: Vec<String> = readings.iter().map(|r| r.slug.clone()).collect();
        let held: Vec<(String, Option<String>)> = sqlx::query(HELD_HASHES_SQL)
            .bind(&slugs)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("read carrier hashes: {e}")))?
            .iter()
            .map(|row| {
                Ok((
                    row.try_get("slug").map_err(|e| format!("slug: {e}"))?,
                    row.try_get("carrier_sha256")
                        .map_err(|e| format!("carrier_sha256: {e}"))?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(SFError::Database)?;
        let held: std::collections::HashMap<String, Option<String>> = held.into_iter().collect();

        let drifted: Vec<PrincipleDrift> = readings
            .iter()
            .filter_map(|reading| {
                let was = held.get(&reading.slug).cloned().flatten()?;
                let now = reading.carrier_sha256.clone()?;
                (was != now).then(|| PrincipleDrift {
                    slug: reading.slug.clone(),
                    was,
                    now,
                })
            })
            .collect();

        // The account first, the marks second: the other order would drop a
        // principle for as long as the marks live, and nothing would say so.
        for reading in &readings {
            write_reading(&mut *tx, reading).await?;
        }

        let mut folded = 0usize;
        for reading in &readings {
            let outcome = sqlx::query(FOLD_MARK_SQL)
                .bind(&reading.slug)
                .bind(&reading.source)
                .execute(&mut *tx)
                .await
                .map_err(|e| SFError::Database(format!("mark {} folded: {e}", reading.slug)))?;
            folded += outcome.rows_affected() as usize;
        }

        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("commit the fold: {e}")))?;
        Ok(PrincipleFold { folded, drifted })
    }

    async fn pending_declarations(&self) -> SFResult<u64> {
        let row = sqlx::query(PENDING_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count unfolded readings: {e}")))?;
        let count: i64 = row
            .try_get("n")
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(count.max(0) as u64)
    }

    async fn apply_principle_measurement(
        &self,
        measurement: &PrincipleMeasurement,
    ) -> SFResult<()> {
        check_fits("slug", &measurement.slug, SLUG_MAX).map_err(SFError::Validation)?;
        if let Some(rev) = &measurement.measured_rev {
            check_fits("measured revision", rev, SHA256_MAX).map_err(SFError::Validation)?;
        }
        sqlx::query(APPLY_MEASUREMENT_SQL)
            .bind(&measurement.slug)
            .bind(measurement.state.as_str())
            .bind(&measurement.platform_gate)
            .bind(&measurement.measured_rev)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SFError::Database(format!("apply measurement for {}: {e}", measurement.slug))
            })?;
        Ok(())
    }

    async fn outstanding(&self, limit: usize) -> SFResult<Vec<PrincipleRow>> {
        // Rows nothing has looked at sort first: an unmeasured row is the least
        // known, not the freshest, and ordering it by insertion would put every
        // newly read principle ahead of one that has waited for a look.
        let rows = sqlx::query(&format!(
            "SELECT {ROW_COLUMNS} FROM principle_ledger \
             WHERE state IS DISTINCT FROM 'enforced' \
             ORDER BY measured_at ASC NULLS FIRST, first_seen_at ASC \
             LIMIT $1"
        ))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Self::entries(rows)
    }

    async fn get(&self, slug: &str) -> SFResult<Option<PrincipleRow>> {
        let row = sqlx::query(&format!(
            "SELECT {ROW_COLUMNS} FROM principle_ledger WHERE slug = $1"
        ))
        .bind(slug)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        match row {
            Some(row) => Ok(Some(Self::row_to_entry(&row).map_err(SFError::Database)?)),
            None => Ok(None),
        }
    }

    async fn slugs(&self) -> SFResult<Vec<String>> {
        sqlx::query("SELECT slug FROM principle_ledger ORDER BY slug ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .iter()
            .map(|row| {
                row.try_get("slug")
                    .map_err(|e| SFError::Database(e.to_string()))
            })
            .collect()
    }

    async fn census(&self) -> SFResult<PrincipleCensus> {
        let rows = sqlx::query(CENSUS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        let mut census = PrincipleCensus::default();
        for row in &rows {
            // The query groups the unset state under its own key, so a row
            // reaching this loop with no state is the third count rather than an
            // error: `NULL` is a value this column is allowed to hold, and it is
            // the one that says no look has settled the row.
            let state: Option<String> = row
                .try_get("state")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let n: i64 = row
                .try_get("n")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let n = n.max(0) as u64;
            match state.as_deref() {
                Some(text) => match PrincipleState::parse(text) {
                    Some(PrincipleState::Enforced) => census.enforced += n,
                    Some(PrincipleState::Unenforced) => census.unenforced += n,
                    None => {
                        return Err(SFError::Database(format!(
                            "state column holds `{text}`, which is not a state"
                        )))
                    }
                },
                None => census.unmeasured += n,
            }
        }
        Ok(census)
    }

    async fn report_refusals(&self, events: &[PrincipleRefusal]) -> SFResult<usize> {
        let mut queued = 0usize;
        for event in events {
            check_refusal(event).map_err(SFError::Validation)?;
            let outcome = sqlx::query(REPORT_REFUSAL_SQL)
                .bind(&event.slug)
                .bind(event.refused_at)
                .bind(&event.source)
                .bind(event.count)
                .execute(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("report refusal of {}: {e}", event.slug)))?;
            queued += outcome.rows_affected() as usize;
        }
        Ok(queued)
    }

    async fn fold_refusals(&self, limit: usize) -> SFResult<usize> {
        let rows = sqlx::query(REFUSAL_FOLD_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read refusals to fold: {e}")))?;
        let events: Vec<PrincipleRefusal> = rows
            .iter()
            .map(|row| {
                Ok(PrincipleRefusal {
                    slug: row.try_get("slug").map_err(|e| format!("slug: {e}"))?,
                    refused_at: row
                        .try_get("refused_at")
                        .map_err(|e| format!("refused_at: {e}"))?,
                    source: row.try_get("source").map_err(|e| format!("source: {e}"))?,
                    count: row.try_get("count").map_err(|e| format!("count: {e}"))?,
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(SFError::Database)?;
        if events.is_empty() {
            return Ok(0);
        }

        // The counter first, the marks second -- and both in one transaction,
        // because this is the one fold here whose write is not an assignment.
        // `refusals` is added to, so repeating it is not idempotent: a run that
        // committed the additions and died before the marks would add the same
        // events again on the next pass, and the account would report more
        // refusals than were ever queued -- a count that grows every time the
        // writer is interrupted, with nothing in the reading to say so.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("open the refusal fold: {e}")))?;
        let mut folded = 0usize;
        for event in &events {
            let outcome = sqlx::query(REFUSAL_FOLD_APPLY_SQL)
                .bind(&event.slug)
                .bind(event.refused_at)
                .bind(event.count)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    SFError::Database(format!("count a refusal of {}: {e}", event.slug))
                })?;
            folded += outcome.rows_affected() as usize;
        }
        for event in &events {
            sqlx::query(REFUSAL_FOLD_MARK_SQL)
                .bind(&event.slug)
                .bind(event.refused_at)
                .bind(&event.source)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    SFError::Database(format!("mark a refusal of {} folded: {e}", event.slug))
                })?;
        }
        tx.commit()
            .await
            .map_err(|e| SFError::Database(format!("commit the refusal fold: {e}")))?;
        Ok(folded)
    }

    async fn pending_refusals(&self) -> SFResult<u64> {
        let row = sqlx::query(PENDING_REFUSALS_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count unfolded refusals: {e}")))?;
        let count: i64 = row
            .try_get("n")
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(count.max(0) as u64)
    }
}

/// Write one reading: the identity, the sentence, the carrier and its hash, and
/// the host's own gate.
///
/// Never touches `state`, `measured_at` or `measured_rev`, and never touches
/// `refusals`: a reading is not a measurement and not a refusal, and a principle
/// that could be described as enforced, or as violated, by being listed would
/// make every count on this table a claim rather than a finding.
///
/// The carrier hash moves to the pushed value. It has to: the row this reading
/// lands on is the one the next drift reading is computed against, and a write
/// that kept the old hash so the difference stayed visible would keep it visible
/// forever -- the drift would never clear, and the principle would never be
/// measured against its own current text again. The difference is read by the
/// fold, before this statement runs, and reported by name there.
///
/// `first_seen_at` is set by the column default on insert and not written on
/// conflict, so it stays the moment the row entered the account.
///
/// `host_gate` and the carrier fields keep what is already there when the
/// reading carries nothing for them: a reading from a source that does not know
/// about a field is not a reading that found the field empty.
const RECORD_READING_SQL: &str = "\
INSERT INTO principle_ledger (slug, lead, carrier_path, carrier_sha256, host_gate)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (slug) DO UPDATE SET
    lead = COALESCE(EXCLUDED.lead, principle_ledger.lead),
    carrier_path = COALESCE(EXCLUDED.carrier_path, principle_ledger.carrier_path),
    carrier_sha256 = COALESCE(EXCLUDED.carrier_sha256, principle_ledger.carrier_sha256),
    host_gate = COALESCE(EXCLUDED.host_gate, principle_ledger.host_gate)";

/// Queue a reading, or leave the one already queued for this principle and
/// source exactly as it is.
///
/// `DO NOTHING` rather than an update: a reading is one pass over one corpus, so
/// a source that wants to say something different pushes again with a new
/// source, or withdraws and re-declares. Updating in place would make "what the
/// host said about this principle" a value that changes without the host
/// changing, which is the same merge the account's own reading path refuses.
const DECLARE_SQL: &str = "\
INSERT INTO principle_ledger_declared (slug, source, lead, carrier_path, carrier_sha256, host_gate)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (slug, source) DO NOTHING";

/// The readings one fold takes, oldest first.
///
/// The slug breaks ties on `declared_at` because a batch queued in one statement
/// shares a timestamp, and without the tiebreak which of them a half-full fold
/// takes would follow whatever order the scan returned -- a different set on the
/// next run, and a different one again on the other dialect.
const FOLD_SELECT_SQL: &str = "\
SELECT slug, source, lead, carrier_path, carrier_sha256, host_gate
FROM principle_ledger_declared
WHERE folded_at IS NULL
ORDER BY declared_at ASC, slug ASC, source ASC
LIMIT $1";

/// What the account holds for the slugs one fold is about to write.
const HELD_HASHES_SQL: &str = "\
SELECT slug, carrier_sha256 FROM principle_ledger WHERE slug = ANY($1)";

/// Mark one reading folded, and only if it is still unfolded.
///
/// The `folded_at IS NULL` guard is what keeps the stamp a statement about this
/// fold rather than an overwrite: it can be run again without moving a time that
/// an earlier fold set.
const FOLD_MARK_SQL: &str = "\
UPDATE principle_ledger_declared SET folded_at = now()
WHERE folded_at IS NULL AND slug = $1 AND source = $2";

const PENDING_SQL: &str = "\
SELECT count(*) AS n FROM principle_ledger_declared WHERE folded_at IS NULL";

/// Write one measurement, with both times taken from the database clock.
///
/// The state and the platform-side gate column are written together, because the
/// verdict was drawn from that column: a row whose `platform_gate` names one file
/// while its state was decided by another is a row no reader can argue with.
///
/// The revision is the caller's and it takes the caller's `NULL` with it: a
/// measurement that named no revision must clear whatever revision was there
/// rather than leave a stale one beside a fresh state, which would read as a
/// state measured against a tree it was never measured against.
///
/// A measurement of a slug the account does not hold creates the row: the
/// account is defined by what has been looked at, not only by what has been
/// listed. The reading columns stay NULL until a reading fills them, which is
/// the honest reading of a principle whose contract test exists and whose
/// carrier the platform has not been told about.
///
/// No observation travels with the write. What the look saw is the platform-side
/// gate and the revision, and both ride in their own columns; the prose form of
/// the same thing is a rendered note nothing here stores, and a column holding it
/// would be a reading with no reader.
const APPLY_MEASUREMENT_SQL: &str = "\
INSERT INTO principle_ledger (slug, state, platform_gate, measured_at, measured_rev)
VALUES ($1, $2, $3, now(), $4)
ON CONFLICT (slug) DO UPDATE SET
    state = EXCLUDED.state,
    platform_gate = EXCLUDED.platform_gate,
    measured_at = now(),
    measured_rev = EXCLUDED.measured_rev";

/// The three counts in one read, the unset state included: `GROUP BY` over a
/// nullable column already forms a group for `NULL`, and filtering it out would
/// mean a second query to learn the same thing.
const CENSUS_SQL: &str = "\
SELECT state, count(*) AS n FROM principle_ledger
GROUP BY state";

/// Queue one refusal, or leave the one already queued for this principle,
/// instant and source exactly as it is.
/// Queue one refusal, keeping the largest report for its second.
///
/// Not `DO NOTHING`: the row's key is the second it happened in, so a second
/// queued twice must end up holding the larger of the two counts rather than
/// whichever arrived first -- a reporter that re-sends a batch because it could
/// not tell whether its last report landed is re-sending a fact that is already
/// there, but a later report that knows of more refusals in that same second is
/// new information, and dropping it is the silent under-count the key exists to
/// prevent. The `WHERE` is what keeps the row count honest: a repeat that says
/// nothing new affects no row, so the caller can still tell "queued" from
/// "already known".
const REPORT_REFUSAL_SQL: &str = "\
INSERT INTO principle_refusals (slug, refused_at, source, count)
VALUES ($1, $2, $3, $4)
ON CONFLICT (slug, refused_at, source) DO UPDATE
SET count = GREATEST(principle_refusals.count, EXCLUDED.count)
WHERE principle_refusals.count < EXCLUDED.count";

/// The refusals one fold takes, oldest first.
const REFUSAL_FOLD_SELECT_SQL: &str = "\
SELECT slug, refused_at, source, count
FROM principle_refusals
WHERE folded_at IS NULL
ORDER BY refused_at ASC, slug ASC, source ASC
LIMIT $1";

/// Move one refusal into its principle's count.
///
/// The count is added to rather than assigned, and the last-refusal time is the
/// later of the two: events are folded in the order they happened, but a pass
/// that was down while two of them were reported takes them in whatever order
/// the intake returns, and an assignment would then leave the older instant on
/// the row. The insert half is for a refusal the account has no row for -- a
/// gate refused work because of a principle the platform was never told about --
/// and it lands a row with a count and no state, which is exactly what is known
/// about it.
const REFUSAL_FOLD_APPLY_SQL: &str = "\
INSERT INTO principle_ledger (slug, refusals, last_refused_at)
VALUES ($1, $3, $2)
ON CONFLICT (slug) DO UPDATE SET
    refusals = principle_ledger.refusals + EXCLUDED.refusals,
    last_refused_at = GREATEST(principle_ledger.last_refused_at, EXCLUDED.last_refused_at)";

const REFUSAL_FOLD_MARK_SQL: &str = "\
UPDATE principle_refusals SET folded_at = now()
WHERE folded_at IS NULL AND slug = $1 AND refused_at = $2 AND source = $3";

const PENDING_REFUSALS_SQL: &str = "\
SELECT count(*) AS n FROM principle_refusals WHERE folded_at IS NULL";

/// Refuse a reading whose keys do not fit their columns.
///
/// Checked here rather than left to the database because the dialects disagree
/// about an over-long value: Postgres raises, and MySQL shortens it. A slug that
/// was silently shortened would land on a different principle's row, and the
/// collision is invisible in the stored row -- the two would simply be one row,
/// and one of them would never be looked at.
fn check_fits(what: &str, value: &str, max: usize) -> Result<(), String> {
    let width = value.chars().count();
    if width > max {
        return Err(format!(
            "{what} is {width} characters, over the {max} the column holds"
        ));
    }
    Ok(())
}

/// Every reading is checked before it is written, wherever it came from, so a
/// queued reading and a direct write are held to the same width.
fn check_reading(reading: &PrincipleReading) -> Result<(), String> {
    check_fits("slug", &reading.slug, SLUG_MAX)?;
    check_fits("source", &reading.source, SOURCE_MAX)?;
    if let Some(hash) = &reading.carrier_sha256 {
        check_fits("carrier hash", hash, SHA256_MAX)?;
    }
    Ok(())
}

/// A refusal is checked for the same reason a reading is, and for one more: its
/// count is what a reader takes as how often a principle was broken, so a count
/// of zero or less is refused rather than stored as a value that adds nothing
/// and looks like a data point.
fn check_refusal(event: &PrincipleRefusal) -> Result<(), String> {
    check_fits("slug", &event.slug, SLUG_MAX)?;
    check_fits("source", &event.source, REFUSAL_SOURCE_MAX)?;
    if event.count < 1 {
        return Err(format!(
            "a refusal of {} carries count {}, which is not a refusal",
            event.slug, event.count
        ));
    }
    Ok(())
}
