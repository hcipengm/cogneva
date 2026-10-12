//!PostgreSQL-backed store for the change ledger.
//!
//!One writer, and that writer is a lease-elected loop. The ledger's whole value
//!is that there is *one* account, so the write path here is built for a single
//!caller rather than arbitrating between several: what a second concurrent
//!writer would produce is two versions of the same account and no way to say
//!which one is the ledger.
//!
//!The table is created by the storage migrations, not by an `init_schema` on
//!this type. Its columns are the contract's own key -- kind, scope, literal --
//!and a second CREATE TABLE here would be a second definition of that key: the
//!two would drift, and the one that wins is whichever ran last.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{
    ChangeIdentity, ChangeLedgerRow, ChangeLedgerStore, ChangeMeasurement, ChangeReading,
    ChangeSource, ChangeState, SFError, SFResult,
};

/// Every read returns these, in one place so no two readers can come back with
/// differently ordered columns.
const ROW_COLUMNS: &str = "kind, scope, literal, state, title, assertion, sources, \
                           first_seen_at, measured_at, measured_rev, landed_at";

/// How wide each keyed identity part may be, taken from the columns that key
/// them rather than from what a caller would like to write.
///
/// The bounds are the narrower dialect's (MySQL cannot key an unbounded TEXT
/// column), so one set governs both. Widths are counted in characters, which is
/// what both dialects' VARCHAR lengths mean.
const KIND_MAX: usize = 32;
const SCOPE_MAX: usize = 191;
const LITERAL_MAX: usize = 512;
const DOC_MAX: usize = 512;

/// PostgreSQL change ledger store.
#[derive(Clone)]
pub struct PostgresChangeLedgerStore {
    pool: PgPool,
}

impl PostgresChangeLedgerStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// A row as callers see it, or why it cannot be one.
    ///
    /// An unreadable row fails the read instead of being skipped. A dropped row
    /// is indistinguishable from one that is not outstanding, and the reader
    /// acts on that difference: it would stop measuring a change that is still
    /// owed, and the ledger would look like progress.
    fn row_to_entry(row: &sqlx::postgres::PgRow) -> Result<ChangeLedgerRow, String> {
        let state: String = row.try_get("state").map_err(|e| format!("state: {e}"))?;
        let state = state
            .parse::<ChangeState>()
            .map_err(|e| format!("state column holds {e}"))?;
        let sources: serde_json::Value = row
            .try_get("sources")
            .map_err(|e| format!("sources: {e}"))?;
        let sources: Vec<ChangeSource> = serde_json::from_value(sources)
            .map_err(|e| format!("sources column unparsable: {e}"))?;
        Ok(ChangeLedgerRow {
            identity: ChangeIdentity::new(
                row.try_get::<String, _>("kind")
                    .map_err(|e| format!("kind: {e}"))?,
                row.try_get::<String, _>("scope")
                    .map_err(|e| format!("scope: {e}"))?,
                row.try_get::<String, _>("literal")
                    .map_err(|e| format!("literal: {e}"))?,
            ),
            state,
            title: row.try_get("title").map_err(|e| format!("title: {e}"))?,
            assertion: row
                .try_get("assertion")
                .map_err(|e| format!("assertion: {e}"))?,
            sources,
            first_seen_at: row
                .try_get("first_seen_at")
                .map_err(|e| format!("first_seen_at: {e}"))?,
            measured_at: row
                .try_get("measured_at")
                .map_err(|e| format!("measured_at: {e}"))?,
            measured_rev: row
                .try_get("measured_rev")
                .map_err(|e| format!("measured_rev: {e}"))?,
            landed_at: row
                .try_get("landed_at")
                .map_err(|e| format!("landed_at: {e}"))?,
        })
    }

    fn entries(
        rows: Result<Vec<sqlx::postgres::PgRow>, sqlx::Error>,
    ) -> SFResult<Vec<ChangeLedgerRow>> {
        let rows = rows.map_err(|e| SFError::Database(e.to_string()))?;
        rows.iter()
            .map(|row| Self::row_to_entry(row).map_err(SFError::Database))
            .collect()
    }

    /// A queued declaration as the reading it will be folded as.
    ///
    /// A row that cannot be read into a reading fails the fold rather than being
    /// skipped: a dropped declaration is indistinguishable from one that was
    /// never made, and the ledger would look caught up while a change it was
    /// told about sits queued forever.
    fn row_to_reading(row: &sqlx::postgres::PgRow) -> Result<ChangeReading, String> {
        Ok(ChangeReading {
            identity: ChangeIdentity::new(
                row.try_get::<String, _>("kind")
                    .map_err(|e| format!("kind: {e}"))?,
                row.try_get::<String, _>("scope")
                    .map_err(|e| format!("scope: {e}"))?,
                row.try_get::<String, _>("literal")
                    .map_err(|e| format!("literal: {e}"))?,
            ),
            title: row
                .try_get::<Option<String>, _>("title")
                .map_err(|e| format!("title: {e}"))?,
            assertion: row
                .try_get::<Option<String>, _>("assertion")
                .map_err(|e| format!("assertion: {e}"))?,
            source: ChangeSource {
                doc: row
                    .try_get::<String, _>("doc")
                    .map_err(|e| format!("doc: {e}"))?,
                verdict: row
                    .try_get::<Option<String>, _>("verdict")
                    .map_err(|e| format!("verdict: {e}"))?,
            },
        })
    }
}

/// Add a reading's source to a row that may already cite other documents.
///
/// Existing entries are left exactly as they are when the document is already
/// cited. A document whose verdict changed is applied by withdrawing that
/// document's reading and reading it again, which is what keeps "which document
/// said what" a statement about one reading of one document rather than a
/// merge of two.
const RECORD_READING_SQL: &str = "\
INSERT INTO change_ledger (kind, scope, literal, state, title, assertion, sources)
VALUES ($1, $2, $3, 'undecided', $4, $5, jsonb_build_array(jsonb_build_object('doc', $6, 'verdict', $7)))
ON CONFLICT (kind, scope, literal) DO UPDATE SET
    title = COALESCE(EXCLUDED.title, change_ledger.title),
    assertion = COALESCE(EXCLUDED.assertion, change_ledger.assertion),
    sources = CASE
        WHEN EXISTS (
            SELECT 1 FROM jsonb_array_elements(change_ledger.sources) AS cited(entry)
            WHERE cited.entry->>'doc' = $6
        ) THEN change_ledger.sources
        ELSE change_ledger.sources || EXCLUDED.sources
    END";

/// Write one measurement, with both times taken from the database clock.
///
/// `landed_at` is kept from the first measurement that found it landed and
/// cleared when a later one does not, so a row that stopped being satisfied
/// reads as outstanding rather than as done-and-forgotten. The times come from
/// `now()` in the same statement that writes the state: taken from the caller
/// instead, a completion time would be a value the measured side supplies.
///
/// The revision is the caller's, and it takes the caller's `NULL` with it: a
/// measurement that named no revision must clear whatever revision was there
/// rather than leave a stale one beside a fresh state, which would read as a
/// state measured against a tree it was never measured against.
const APPLY_MEASUREMENT_SQL: &str = "\
INSERT INTO change_ledger (kind, scope, literal, state, measured_rev, measured_at, landed_at)
VALUES ($1, $2, $3, $4, $5, now(), CASE WHEN $4 = 'landed' THEN now() ELSE NULL END)
ON CONFLICT (kind, scope, literal) DO UPDATE SET
    state = EXCLUDED.state,
    measured_rev = EXCLUDED.measured_rev,
    measured_at = now(),
    landed_at = CASE
        WHEN EXCLUDED.state = 'landed' THEN COALESCE(change_ledger.landed_at, now())
        ELSE NULL
    END";

/// Refuse a reading whose identity or document does not fit its column.
///
/// Checked here rather than left to the database because the dialects disagree
/// about an over-long value: Postgres raises, and MySQL shortens it. A key that
/// was silently shortened would land on a different change's row, and the
/// collision is invisible in the stored row -- the two would simply be one row,
/// and one of them would never be measured.
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
/// queued declaration and a direct write are held to the same width.
fn check_reading(reading: &ChangeReading) -> Result<(), String> {
    check_fits("kind", &reading.identity.kind, KIND_MAX)?;
    check_fits("scope", &reading.identity.scope, SCOPE_MAX)?;
    check_fits("literal", &reading.identity.literal, LITERAL_MAX)?;
    check_fits("source document", &reading.source.doc, DOC_MAX)
}

/// Queue a declaration, or leave the one already queued for this change and
/// document exactly as it is.
///
/// `DO NOTHING` rather than an update: a declaration is one reading of one
/// document, so a document that wants to say something different about a change
/// withdraws its declaration and makes a new one. Updating in place would make
/// "what this document asked for" a value that changes without the document
/// changing, which is the same merge the reading path refuses.
const DECLARE_SQL: &str = "\
INSERT INTO change_ledger_declared (kind, scope, literal, doc, title, assertion, verdict)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (kind, scope, literal, doc) DO NOTHING";

/// The declarations one fold takes, oldest first.
///
/// The identity parts break ties on `declared_at` because a batch queued in one
/// statement shares a timestamp, and without the tiebreak which of them a
/// half-full fold takes would follow whatever order the scan returned -- a
/// different set on the next run, and a different one again on the other
/// dialect.
const FOLD_SELECT_SQL: &str = "\
SELECT kind, scope, literal, doc, title, assertion, verdict
FROM change_ledger_declared
WHERE folded_at IS NULL
ORDER BY declared_at ASC, kind ASC, scope ASC, literal ASC, doc ASC
LIMIT $1";

/// Mark one declaration folded, and only if it is still unfolded.
///
/// The `folded_at IS NULL` guard is what keeps the stamp a statement about this
/// fold rather than an overwrite: it can be run again without moving a time that
/// an earlier fold set.
const FOLD_MARK_SQL: &str = "\
UPDATE change_ledger_declared SET folded_at = now()
WHERE folded_at IS NULL AND kind = $1 AND scope = $2 AND literal = $3 AND doc = $4";

const PENDING_SQL: &str =
    "SELECT count(*) AS n FROM change_ledger_declared WHERE folded_at IS NULL";

#[async_trait]
impl ChangeLedgerStore for PostgresChangeLedgerStore {
    async fn record_readings(&self, readings: &[ChangeReading]) -> SFResult<usize> {
        let mut stored = 0usize;
        for reading in readings {
            check_reading(reading).map_err(SFError::Validation)?;
            sqlx::query(RECORD_READING_SQL)
                .bind(&reading.identity.kind)
                .bind(&reading.identity.scope)
                .bind(&reading.identity.literal)
                .bind(&reading.title)
                .bind(&reading.assertion)
                .bind(&reading.source.doc)
                .bind(&reading.source.verdict)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!(
                        "record reading for {}: {e}",
                        reading.identity.as_key()
                    ))
                })?;
            stored += 1;
        }
        Ok(stored)
    }

    async fn declare(&self, readings: &[ChangeReading]) -> SFResult<usize> {
        let mut queued = 0usize;
        for reading in readings {
            check_reading(reading).map_err(SFError::Validation)?;
            let outcome = sqlx::query(DECLARE_SQL)
                .bind(&reading.identity.kind)
                .bind(&reading.identity.scope)
                .bind(&reading.identity.literal)
                .bind(&reading.source.doc)
                .bind(&reading.title)
                .bind(&reading.assertion)
                .bind(&reading.source.verdict)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!("declare {}: {e}", reading.identity.as_key()))
                })?;
            // The conflict clause writes nothing, so a declaration already queued
            // contributes no row here. Counting the statement rather than the
            // batch is what makes "how many were newly queued" answerable.
            queued += outcome.rows_affected() as usize;
        }
        Ok(queued)
    }

    async fn fold_declarations(&self, limit: usize) -> SFResult<usize> {
        let rows = sqlx::query(FOLD_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read declarations to fold: {e}")))?;
        let readings: Vec<ChangeReading> = rows
            .iter()
            .map(|row| Self::row_to_reading(row).map_err(SFError::Database))
            .collect::<SFResult<_>>()?;
        if readings.is_empty() {
            return Ok(0);
        }

        // The account first, the marks second. A fold that stops in between
        // leaves the declarations unfolded and repeats this work on the next
        // pass, which the reading path absorbs as an idempotent upsert; the
        // other order would drop the change instead, and nothing would say so.
        self.record_readings(&readings).await?;

        let mut folded = 0usize;
        for reading in &readings {
            sqlx::query(FOLD_MARK_SQL)
                .bind(&reading.identity.kind)
                .bind(&reading.identity.scope)
                .bind(&reading.identity.literal)
                .bind(&reading.source.doc)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    SFError::Database(format!(
                        "mark declaration folded for {}: {e}",
                        reading.identity.as_key()
                    ))
                })?;
            folded += 1;
        }
        Ok(folded)
    }

    async fn pending_declarations(&self) -> SFResult<u64> {
        let row = sqlx::query(PENDING_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count pending declarations: {e}")))?;
        let n: i64 = row
            .try_get("n")
            .map_err(|e| SFError::Database(format!("pending count: {e}")))?;
        Ok(n.max(0) as u64)
    }

    async fn apply_measurement(&self, measurement: &ChangeMeasurement) -> SFResult<()> {
        sqlx::query(APPLY_MEASUREMENT_SQL)
            .bind(&measurement.identity.kind)
            .bind(&measurement.identity.scope)
            .bind(&measurement.identity.literal)
            .bind(measurement.state.as_str())
            .bind(&measurement.measured_rev)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SFError::Database(format!(
                    "apply measurement for {}: {e}",
                    measurement.identity.as_key()
                ))
            })?;
        Ok(())
    }

    async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        // Rows nothing has measured sort first: an unmeasured row is the least
        // known, not the freshest, and ordering it by insertion would put every
        // newly read change ahead of one that has waited for a measurement.
        Self::entries(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM change_ledger \
                 WHERE state <> 'landed' \
                 ORDER BY measured_at ASC NULLS FIRST, first_seen_at ASC \
                 LIMIT $1"
            ))
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn get(&self, identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>> {
        let row = sqlx::query(&format!(
            "SELECT {ROW_COLUMNS} FROM change_ledger \
             WHERE kind = $1 AND scope = $2 AND literal = $3"
        ))
        .bind(&identity.kind)
        .bind(&identity.scope)
        .bind(&identity.literal)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        match row {
            Some(row) => Self::row_to_entry(&row)
                .map(Some)
                .map_err(SFError::Database),
            None => Ok(None),
        }
    }

    async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>> {
        let rows = sqlx::query("SELECT state, count(*) AS n FROM change_ledger GROUP BY state")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        let mut counted: Vec<(ChangeState, u64)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let state: String = row
                .try_get("state")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let state = state
                .parse::<ChangeState>()
                .map_err(|e| SFError::Database(format!("state column holds {e}")))?;
            let n: i64 = row
                .try_get("n")
                .map_err(|e| SFError::Database(e.to_string()))?;
            counted.push((state, n.max(0) as u64));
        }
        // Every state is answered, zeroes included: a reader comparing two
        // censuses has to see a state that emptied, and a missing key reads the
        // same as a state nobody thought to count.
        Ok(ChangeState::ALL
            .into_iter()
            .map(|state| {
                let n = counted
                    .iter()
                    .find(|(seen, _)| *seen == state)
                    .map(|(_, n)| *n)
                    .unwrap_or(0);
                (state, n)
            })
            .collect())
    }
}
