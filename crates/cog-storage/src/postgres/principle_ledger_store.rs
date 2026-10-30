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

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{
    ChangePriority, PrincipleCensus, PrincipleCheckKind, PrincipleCheckKindCensus, PrincipleDrift,
    PrincipleFold, PrincipleLedgerStore, PrincipleMeasurement, PrincipleReading, PrincipleRefusal,
    PrincipleRow, PrincipleState, SFError, SFResult,
};

/// Resolve the state and tier words a query template names.
///
/// A query that spells `enforced` is a second copy of a word
/// [`PrincipleState::as_str`] already decides, and the two agree only until one
/// is renamed. The failure is quiet: a predicate matching the old spelling
/// returns no rows, and no rows is what this ledger returns when every principle
/// is settled -- the debt reading answers "nothing is ungated" while the rows sit
/// right there. The tier column drifts the same way, and a lost tier turns every
/// principle a person holds into a debt no machine can ever clear.
///
/// The slot is named after the value it resolves, and the words come from each
/// enum's own `ALL`, so a variant added to either set is fillable without anyone
/// extending a second table here.
fn fill_vocabulary(template: &str) -> String {
    let mut slots: Vec<(&str, &str)> = Vec::new();
    for state in PrincipleState::ALL {
        slots.push(("state", state.as_str()));
    }
    for kind in PrincipleCheckKind::ALL {
        slots.push(("kind", kind.as_str()));
    }
    super::fill_slots(template, &slots)
}

/// A read over the ledger's own columns, with every state word filled in.
///
/// The column list is interpolated once here rather than in each statement, so a
/// reader cannot come back with a different column order -- and the statements
/// below stay plain string literals, which is what lets them name `{state:...}`
/// slots without doubling every brace for `format!`.
fn read_sql(rest: &str) -> String {
    fill_vocabulary(&format!("SELECT {ROW_COLUMNS} {rest}"))
}

/// Every read returns these, in one place so no two readers can come back with
/// differently ordered columns.
const ROW_COLUMNS: &str = "slug, lead, carrier_path, carrier_sha256, host_gate, where_red, \
                           platform_gate, check_kind, priority, state, refusals, last_refused_at, \
                           first_seen_at, measured_at, measured_rev";

/// How wide each keyed column may be, taken from the columns that key them
/// rather than from what a caller would like to write.
///
/// The bounds are the narrower dialect's (MySQL cannot key an unbounded TEXT
/// column), so one set governs both. Widths are counted in characters, which is
/// what both dialects' VARCHAR lengths mean.
// cap: bound — 取的是更窄那个方言的宽度（MySQL 不能给无界 TEXT 建键），一套管两边
const SLUG_MAX: usize = 191;
// cap: bound — the reading's source column width, a schema limit rather than a measurement
const SOURCE_MAX: usize = 512;
// cap: bound — the refusal row's source column width, a schema limit rather than a measurement
const REFUSAL_SOURCE_MAX: usize = 256;
/// The shape name a refusal carries: the gate's own key for what matched, not
/// the principle it counts against.
// cap: bound — 拒收携带的形状名：是门自己给它匹配到的东西起的名，不是它算在头上的那条原则
const REFUSAL_CRITERION_MAX: usize = 64;
/// The matched text. Wide enough to hold a phrase, narrow enough that the intake
/// stays a record of what happened rather than a copy of the call.
// cap: bound — 够装一个短语，又窄到入账仍是「发生过什么」的记录而不是那趟调用的拷贝
const REFUSAL_SPAN_MAX: usize = 512;
// cap: bound — the width of a SHA-256 digest in hex; a protocol figure
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
            where_red: row
                .try_get("where_red")
                .map_err(|e| format!("where_red: {e}"))?,
            platform_gate: row
                .try_get("platform_gate")
                .map_err(|e| format!("platform_gate: {e}"))?,
            check_kind: parse_check_kind(row)?,
            priority: parse_priority(row)?,
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
            where_red: row
                .try_get("where_red")
                .map_err(|e| format!("where_red: {e}"))?,
            check_kind: parse_check_kind(row)?,
            priority: parse_optional_priority(row)?,
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
        .bind(&reading.where_red)
        .bind(reading.check_kind.map(PrincipleCheckKind::as_str))
        .bind(reading.priority.map(ChangePriority::as_str))
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
                .bind(&reading.where_red)
                .bind(reading.check_kind.map(PrincipleCheckKind::as_str))
                .bind(reading.priority.map(ChangePriority::as_str))
                .execute(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("declare {}: {e}", reading.slug)))?;
            // A reading that says what the queued one already says matches the
            // conflict clause's `WHERE` and writes no row; one that says
            // something different writes its row and counts here. Counting the
            // statement rather than the batch is what makes "how many were
            // queued or re-queued" answerable, and the `WHERE` is what keeps
            // that number meaning something rather than counting every re-push.
            queued += outcome.rows_affected() as usize;
        }
        Ok(queued)
    }

    async fn fold_declarations(&self, limit: usize) -> SFResult<PrincipleFold> {
        // Everything from taking the batch to marking the readings folded rides
        // in one transaction, for two reasons. The drift reading: the writes are
        // idempotent, so repeating them costs nothing, but the *difference* is
        // not, because this fold replaces the hash it compares against -- run in
        // separate transactions and interrupted in between, the next pass reads a
        // stored hash that the interrupted pass already overwrote, finds no
        // difference, and the fact that this rule's text moved away from what its
        // gate was measured against is never said by anything. And the taking
        // itself: `folded_at IS NULL` is a look, not a reservation, so two
        // folders reading it at once match the same rows and the slower one can
        // write its reading over the newer one and mark it folded. `FOR UPDATE
        // SKIP LOCKED` in `FOLD_SELECT_SQL` turns the read into a claim, and a
        // claim is only worth anything inside the transaction that spends it.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(format!("open the fold: {e}")))?;

        let rows = sqlx::query(FOLD_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SFError::Database(format!("read readings to fold: {e}")))?;
        let readings: Vec<PrincipleReading> = rows
            .iter()
            .map(|row| Self::row_to_reading(row).map_err(SFError::Database))
            .collect::<SFResult<_>>()?;
        if readings.is_empty() {
            return Ok(PrincipleFold::default());
        }

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

    async fn record_human_only(&self, slug: &str, measured_rev: Option<&str>) -> SFResult<()> {
        check_fits("slug", slug, SLUG_MAX).map_err(SFError::Validation)?;
        if let Some(rev) = measured_rev {
            check_fits("measured revision", rev, SHA256_MAX).map_err(SFError::Validation)?;
        }
        // The state is cleared rather than left alone: a row this verdict reaches
        // may hold `enforced` from a platform gate the registry no longer names,
        // and keeping that while stamping a fresh `measured_at` would report the
        // old verdict as current. A row the account has never measured had no
        // state to clear, and lands here with only the two stamps.
        sqlx::query(RECORD_HUMAN_ONLY_SQL)
            .bind(slug)
            .bind(measured_rev)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("record human-only look for {slug}: {e}")))?;
        Ok(())
    }

    async fn unsettled_judgement(&self, limit: usize) -> SFResult<Vec<PrincipleRow>> {
        // Oldest first: the row that has waited longest for its first look goes
        // first, and the marker test keeps a row a retired carrier left behind
        // out of this lane -- its marker is what the fold clears, and stamping
        // it here would leave the row retired in everything but the query it
        // just fell out of.
        //
        // A stamped row is in this lane when its host column names a gate: the
        // verdict that reads that column is code, so a row looked at before that
        // verdict existed keeps an empty state while its own evidence says a
        // machine holds it, and nothing else asks the row again -- the batch
        // sorts the tier behind a debt deeper than one pass. The dashed and
        // `stale:` values are the host registry saying "no gate" and "I could
        // not read it"; neither is evidence to settle on, and listing them here
        // would re-stamp rows that have not changed. One look writes the verdict
        // and the row leaves the lane.
        //
        // `unenforced` is the third shape, and it is here for the opposite
        // reason: it is the one state this tier can never write. A row measured
        // before its carrier declared the tier keeps `unenforced`, which reads
        // on every surface as a debt, and no other path asks it again -- the
        // batch sorts the tier behind the debt, and an empty-state lane never
        // sees a row that has a state. So a stamped row holding it is asked even
        // with no gate named, because the state itself is the evidence that has
        // to be re-read. It sorts first: a wrong verdict is worse than a stamp
        // nobody has written yet.
        let rows = sqlx::query(&read_sql(
            "FROM principle_ledger \
             WHERE (state IS NULL OR state = '{state:unenforced}') \
               AND check_kind = '{kind:judgement}' \
               AND (host_gate IS NULL OR host_gate NOT LIKE 'retired:%') \
               AND (state = '{state:unenforced}' \
                    OR measured_at IS NULL \
                    OR (COALESCE(host_gate, '') NOT IN ('', '-') \
                        AND host_gate NOT LIKE 'stale:%')) \
             ORDER BY (state = '{state:unenforced}') DESC, \
                      (measured_at IS NOT NULL) ASC, first_seen_at ASC, slug ASC \
             LIMIT $1",
        ))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Self::entries(rows)
    }

    async fn outstanding(&self, limit: usize) -> SFResult<Vec<PrincipleRow>> {
        // Rows nothing has looked at sort first: an unmeasured row is the least
        // known, not the freshest, and ordering it by insertion would put every
        // newly read principle ahead of one that has waited for a look.
        //
        // A row a person holds sorts after every row a pass could settle. Its
        // verdict is the settled answer "no gate is meant to exist", and the
        // look stamps `measured_at` while leaving the state empty; the stamp is
        // what keeps it out of the never-measured head. The ordering below is
        // still needed on top of that: without it, a pass whose bound is smaller
        // than the number of human-held rows would spend its budget re-stamping
        // the same answers every five minutes and never reach a row it could
        // settle. They are kept in the set rather than filtered out so the line
        // still reports how many rows a person holds.
        //
        // A human-held row no pass has looked at yet is not reachable through
        // this query at all while the debt runs deeper than one pass: the tier
        // ordering above puts it behind all of it. That case is served by
        // `unsettled_judgement`, which asks for exactly those rows with its own
        // bound; the ordering here stays as it is so a settleable row is never
        // crowded out by a sweep that could take the whole pass.
        //
        // Retired rows leave the set, with one exception: a row whose stored host
        // value no longer carries the retire marker. That marker is what the fold
        // wrote from the host's last reading, so a carrier that returns to the
        // roster arrives as a changed value, the row comes back into the set, and
        // the next look writes whatever it finds -- the retirement is a state a
        // later measurement can move, not a tombstone. The comparison lives here
        // rather than in the fold because the fold is also the path that writes
        // the marker: one place would have to be read twice to see both halves.
        //
        // A row still carrying the marker sorts ahead of everything. The tier
        // ordering below would otherwise put a retirement reading for a
        // `judgement` row behind every unsettled row, and there is always more
        // unsettled work than one pass takes -- while the rescue path cannot
        // reach it either, because the carrier's removal is what removed the
        // declaration. That row would keep the marker and never reach the state
        // the marker stands for, which is the one thing the retirement reading
        // was written to prevent. Retirement readings are bounded by how many
        // carriers leave the roster and they clear as the marker is folded, so
        // spending the head of the pass on them costs nothing that was waiting.
        // A row recorded as enforced with neither registry's column naming a
        // gate is not a settled row: the verdict says a machine holds this
        // principle and the two readings say nothing does. That pair is only
        // reachable when a gate was later withdrawn, and no other path re-opens
        // it -- the reconciliation compares the platform side alone, so a host
        // gate that disappears moves no state. Asking the row again costs one
        // look and settles it, while leaving it out reports a gate for good. The
        // two columns empty together is the whole test: a row settled by either
        // side keeps that side's column, so a live verdict is never re-asked.
        //
        // These rows sort ahead of everything for the same reason retirement
        // readings do: the debt runs deeper than one pass takes, and a row that
        // was measured once sorts behind every row that was never measured. They
        // clear as the look is written, so the head is spent on work that ends.
        let rows = sqlx::query(&read_sql(
            "FROM principle_ledger \
             WHERE (state IS DISTINCT FROM '{state:enforced}' \
                    OR (COALESCE(host_gate, '-') IN ('-', '') \
                        AND COALESCE(platform_gate, '-') IN ('-', ''))) \
               AND (state IS DISTINCT FROM '{state:retired}' \
                    OR host_gate IS NULL \
                    OR host_gate NOT LIKE 'retired:%') \
             ORDER BY (COALESCE(host_gate, '') LIKE 'retired:%') DESC, \
                      (state IS NOT DISTINCT FROM '{state:enforced}') DESC, \
                      (check_kind IS NOT DISTINCT FROM '{kind:judgement}') ASC, \
                      measured_at ASC NULLS FIRST, first_seen_at ASC \
             LIMIT $1",
        ))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Self::entries(rows)
    }

    async fn reopen_platform_gate_drift(
        &self,
        gates: &HashMap<String, String>,
    ) -> SFResult<Vec<String>> {
        // Read the settled rows and compare here rather than in one statement:
        // the comparison is per slug, and expressing it in SQL would need the
        // registry as a relation. `state = 'enforced'` is a small set (the rows a
        // look has answered), so reading it whole costs less than carrying the
        // registry into the database.
        //
        // `judgement` rows are in that set too, once the platform declares a gate
        // for one: the tier stops a row being a debt when no machine names a
        // gate, it does not stop a declaration from settling one. Leaving them
        // out here would let a declaration that was later removed keep the row
        // reported as enforced for good.
        let settled: Vec<(String, Option<String>)> = sqlx::query(&fill_vocabulary(
            "SELECT slug, platform_gate FROM principle_ledger WHERE state = '{state:enforced}'",
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?
        .iter()
        .map(|row| {
            Ok((
                row.try_get("slug")
                    .map_err(|e| SFError::Database(format!("slug: {e}")))?,
                row.try_get("platform_gate")
                    .map_err(|e| SFError::Database(format!("platform_gate: {e}")))?,
            ))
        })
        .collect::<SFResult<_>>()?;

        let moved: Vec<String> = settled
            .into_iter()
            .filter(|(slug, held)| gates.get(slug) != held.as_ref())
            .map(|(slug, _)| slug)
            .collect();
        if moved.is_empty() {
            return Ok(moved);
        }

        // The state goes back to "nothing has looked", not to `unenforced`: the
        // row is one a look has to answer again, and writing an answer here would
        // be this statement deciding what the next pass is for. `measured_at`
        // clears with it so the row sorts with the other unmeasured ones rather
        // than looking freshly looked at.
        sqlx::query(&fill_vocabulary(
            "UPDATE principle_ledger SET state = NULL, measured_at = NULL \
                 WHERE slug = ANY($1) AND state = '{state:enforced}'",
        ))
        .bind(&moved)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(moved)
    }

    async fn refused(&self, limit: usize) -> SFResult<Vec<PrincipleRow>> {
        // Most recently refused first, so a bound cuts the rows whose evidence is
        // oldest rather than the live ones. A row may hold a count and no instant
        // -- events counted before that column was kept -- and it sorts last
        // rather than first: no instant is not the freshest evidence, it is the
        // absence of one.
        let rows = sqlx::query(&format!(
            "SELECT {ROW_COLUMNS} FROM principle_ledger \
             WHERE refusals > 0 \
             ORDER BY last_refused_at DESC NULLS LAST, slug ASC \
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
                    Some(PrincipleState::Retired) => census.retired += n,
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

    async fn check_kind_census(&self) -> SFResult<PrincipleCheckKindCensus> {
        let rows = sqlx::query(CHECK_KIND_CENSUS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        let mut census = PrincipleCheckKindCensus::default();
        for row in &rows {
            // The query groups the undeclared rows under their own key, so a row
            // reaching this loop with no tier is the fifth count rather than an
            // error: `NULL` is a value this column is allowed to hold, and it is
            // the one that says nobody has said how the row is checked.
            let kind: Option<String> = row
                .try_get("check_kind")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let n: i64 = row
                .try_get("n")
                .map_err(|e| SFError::Database(e.to_string()))?;
            let n = n.max(0) as u64;
            match kind.as_deref() {
                Some(text) => match PrincipleCheckKind::parse(text) {
                    Some(PrincipleCheckKind::Shape) => census.shape += n,
                    Some(PrincipleCheckKind::Tree) => census.tree += n,
                    Some(PrincipleCheckKind::Live) => census.live += n,
                    Some(PrincipleCheckKind::Judgement) => census.judgement += n,
                    None => {
                        return Err(SFError::Database(format!(
                            "check_kind column holds `{text}`, which is not a check kind"
                        )))
                    }
                },
                None => census.undeclared += n,
            }
        }
        Ok(census)
    }

    async fn report_refusals(&self, events: &[PrincipleRefusal]) -> SFResult<usize> {
        let mut queued = 0usize;
        for event in events {
            check_refusal(event).map_err(SFError::Validation)?;
            let span: String = event.span.chars().take(REFUSAL_SPAN_MAX).collect();
            let outcome = sqlx::query(REPORT_REFUSAL_SQL)
                .bind(&event.slug)
                .bind(event.refused_at)
                .bind(&event.source)
                .bind(&event.criterion)
                .bind(&span)
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
        let events: Vec<QueuedRefusal> = rows
            .iter()
            .map(|row| {
                Ok(QueuedRefusal {
                    slug: row.try_get("slug").map_err(|e| format!("slug: {e}"))?,
                    refused_at: row
                        .try_get("refused_at")
                        .map_err(|e| format!("refused_at: {e}"))?,
                    source: row.try_get("source").map_err(|e| format!("source: {e}"))?,
                    session_id: row
                        .try_get("session_id")
                        .map_err(|e| format!("session_id: {e}"))?,
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
                .bind(&event.session_id)
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
/// `host_gate` and the carrier fields keep what is already there when the reading
/// carries nothing for them: a reading from a source that does not know about a
/// field is not a reading that found the field empty.
///
/// `check_kind` is the exception, and it is not a looser rule but a stricter one:
/// its `NULL` is not silence, it is the account's own word for "no tier has been
/// declared for this principle". A merge that kept the stored tier would make
/// that word unwritable -- a tier could be declared and then never withdrawn,
/// while the column that records whether the tier was declared can only ever move
/// in one direction. A principle whose tier was declared by mistake would carry
/// it for good, and the fold that carries it here would keep it too: the queue
/// row is written from the declared row wholesale, so a `NULL` that reaches this
/// statement came from a source that pushed the column and left it empty.
/// `priority` needs both halves written out, because the two sides of the
/// statement want opposite things from the same `NULL`. The column is `NOT NULL`
/// and its default is `unstated`, so a **new** row has to be given that value --
/// binding the parameter straight into `VALUES` is a `NOT NULL` violation, and it
/// is the silent reading (the one that says nothing about the band) that trips it.
/// An **existing** row must keep the band it holds, so the same `NULL` may not
/// overwrite it. `COALESCE($8, 'unstated')` in `VALUES` alone would break the
/// second half: `EXCLUDED.priority` would never be `NULL` and silence would erase
/// a band. Hence the `SET` reads the parameter, not `EXCLUDED`.
///
/// The two columns the verdict is computed from are the only place this
/// statement writes more than the reading. When `host_gate` or `check_kind`
/// moves, the state and the stamp that a look wrote for the old values are
/// cleared, so the row goes back to the pass instead of waiting for it: the
/// verdict reads the host's column first and switches on the tier, so a reading
/// that changes either changes the answer the next look would give. Until
/// 2026-10-28 it moved neither `state` nor `measured_at`, and the row kept the
/// old verdict until the sweep came round to it -- one full scan, which on the
/// live account (about 3200 rows, 200 a pass, five minutes apart) is close to
/// eighty minutes. Measured 2026-10-23: a p1 principle was given its gate at
/// 03:47 and the account still read `unenforced` at 04:05, while the column
/// itself had moved in the second the reading arrived.
///
/// The other direction has to keep holding: a reading that says what is already
/// stored is a re-push, these are sent for every row every cycle, and a
/// condition that fired on them would send the whole corpus to the head of the
/// queue each time and no pass would ever get past it. That is why the test is
/// `IS DISTINCT FROM` on the effective value, not "a reading arrived".
const RECORD_READING_SQL: &str = "\
INSERT INTO principle_ledger (slug, lead, carrier_path, carrier_sha256, host_gate, where_red, \
check_kind, priority)
VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8::varchar, 'unstated'))
ON CONFLICT (slug) DO UPDATE SET
    lead = COALESCE(EXCLUDED.lead, principle_ledger.lead),
    carrier_path = COALESCE(EXCLUDED.carrier_path, principle_ledger.carrier_path),
    carrier_sha256 = COALESCE(EXCLUDED.carrier_sha256, principle_ledger.carrier_sha256),
    host_gate = COALESCE(EXCLUDED.host_gate, principle_ledger.host_gate),
    where_red = COALESCE(EXCLUDED.where_red, principle_ledger.where_red),
    check_kind = EXCLUDED.check_kind,
    priority = COALESCE($8::varchar, principle_ledger.priority), \
    state = CASE WHEN COALESCE(EXCLUDED.host_gate, principle_ledger.host_gate) \
            IS DISTINCT FROM principle_ledger.host_gate \
        OR EXCLUDED.check_kind IS DISTINCT FROM principle_ledger.check_kind \
        THEN NULL ELSE principle_ledger.state END, \
    measured_at = CASE WHEN COALESCE(EXCLUDED.host_gate, principle_ledger.host_gate) \
            IS DISTINCT FROM principle_ledger.host_gate \
        OR EXCLUDED.check_kind IS DISTINCT FROM principle_ledger.check_kind \
        THEN NULL ELSE principle_ledger.measured_at END";

/// Queue a reading, or re-queue the one already there when the reading would
/// actually change it.
///
/// Not `DO NOTHING`, which this used to be. The rule it was standing on -- "a
/// source that wants to say something different pushes again with a new source,
/// or withdraws and re-declares" -- described two moves this table has no way to
/// make. The source is a constant in the pusher, so a re-push cannot be a new
/// one; there is no withdrawal statement anywhere. What `DO NOTHING` did instead
/// was freeze each `(slug, source)` at whatever the **first** reading said, and a
/// reading taken before a column existed, or by a reader that could not yet see a
/// field, says `NULL` for it. A NULL is a value here, so the corrected reading
/// that arrived later matched the key, hit `DO NOTHING`, and was dropped while
/// the pass still reported success. Nothing in the account could ever say
/// otherwise: a row that cannot be re-read is a row that is frozen.
///
/// `WHERE ... IS DISTINCT FROM` is what keeps the re-queue from becoming a
/// livelock: without it every pass over the corpus would re-queue all of it,
/// resetting `folded_at` on rows whose values did not move, and the fold -- which
/// reads a bounded batch per tick -- would never catch up. It compares the values
/// the `SET` would produce, not the raw incoming ones, because a field the
/// reading carries nothing for keeps what is stored: comparing the incoming
/// `NULL` against a stored value would call that a change, re-queue it, write the
/// same row, and do the same thing again on the next pass.
///
/// The `COALESCE` half is not new and is what the account's own reading path
/// already does: a source that knows nothing about a field is not a source that
/// found the field empty, so it may not clear it. `check_kind` is the one column
/// that keeps no `COALESCE`, for the reason spelled out on
/// [`RECORD_READING_SQL`]: its empty value is a value the account has a meaning
/// for, so a merge that preserved the stored tier would leave "no tier declared"
/// unsayable in both copies of this rule.
const DECLARE_SQL: &str = "\
INSERT INTO principle_ledger_declared (slug, source, lead, carrier_path, carrier_sha256, host_gate, \
where_red, check_kind, priority)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (slug, source) DO UPDATE SET
    lead = COALESCE(EXCLUDED.lead, principle_ledger_declared.lead),
    carrier_path = COALESCE(EXCLUDED.carrier_path, principle_ledger_declared.carrier_path),
    carrier_sha256 = COALESCE(EXCLUDED.carrier_sha256, principle_ledger_declared.carrier_sha256),
    host_gate = COALESCE(EXCLUDED.host_gate, principle_ledger_declared.host_gate),
    where_red = COALESCE(EXCLUDED.where_red, principle_ledger_declared.where_red),
    check_kind = EXCLUDED.check_kind,
    priority = COALESCE(EXCLUDED.priority, principle_ledger_declared.priority),
    declared_at = now(),
    folded_at = NULL
WHERE (COALESCE(EXCLUDED.lead, principle_ledger_declared.lead),
       COALESCE(EXCLUDED.carrier_path, principle_ledger_declared.carrier_path),
       COALESCE(EXCLUDED.carrier_sha256, principle_ledger_declared.carrier_sha256),
       COALESCE(EXCLUDED.host_gate, principle_ledger_declared.host_gate),
       COALESCE(EXCLUDED.where_red, principle_ledger_declared.where_red),
       EXCLUDED.check_kind,
       COALESCE(EXCLUDED.priority, principle_ledger_declared.priority))
  IS DISTINCT FROM
      (principle_ledger_declared.lead, principle_ledger_declared.carrier_path,
       principle_ledger_declared.carrier_sha256, principle_ledger_declared.host_gate,
       principle_ledger_declared.where_red, principle_ledger_declared.check_kind,
       principle_ledger_declared.priority)";

/// The readings one fold takes, oldest first, claimed as they are read.
///
/// The slug breaks ties on `declared_at` because a batch queued in one statement
/// shares a timestamp, and without the tiebreak which of them a half-full fold
/// takes would follow whatever order the scan returned -- a different set on the
/// next run, and a different one again on the other dialect.
///
/// `FOR UPDATE SKIP LOCKED` is what makes this a claim rather than a look: two
/// folders reading `folded_at IS NULL` at once both match the same rows, and the
/// slower one can then write its older reading over the newer one and mark the
/// row folded, leaving the account citing text the carrier no longer holds with
/// every reading green. Locking the rows inside the transaction that writes and
/// marks them makes the second folder skip them instead.
const FOLD_SELECT_SQL: &str = "\
SELECT slug, source, lead, carrier_path, carrier_sha256, host_gate, where_red, check_kind, priority
FROM principle_ledger_declared
WHERE folded_at IS NULL
ORDER BY declared_at ASC, slug ASC, source ASC
LIMIT $1
FOR UPDATE SKIP LOCKED";

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
const RECORD_HUMAN_ONLY_SQL: &str = "\
UPDATE principle_ledger
SET state = NULL, platform_gate = NULL, measured_at = now(), measured_rev = $2
WHERE slug = $1";

const APPLY_MEASUREMENT_SQL: &str = "\
INSERT INTO principle_ledger (slug, state, platform_gate, measured_at, measured_rev)
VALUES ($1, $2, $3, now(), $4)
ON CONFLICT (slug) DO UPDATE SET
    state = EXCLUDED.state,
    platform_gate = EXCLUDED.platform_gate,
    measured_at = now(),
    measured_rev = EXCLUDED.measured_rev";

/// Which rows are *not* a principle, in the one place both censuses read.
///
/// The account holds a row per slug a gate refused work for, and a slug is not
/// always a principle: a gate that cannot read its own material records the
/// refusal under its own name (`principle-gate.sh:shape-table-unreadable`), and
/// so does the drill that proves that path works. Those rows belong in the
/// account -- the refusal counts are published by slug, and a counter that
/// drops the gate's own outage is the one failure the count exists to catch --
/// but they are not corpus, and the census says "how much of the corpus has a
/// gate". Counting them there is not a rounding error: those rows have no
/// state and no measurement by construction, so they land in `unmeasured`
/// forever and pin "principles nothing has ever looked at" at a number that can
/// never fall. Measured on 2026-10-23: that cell read 2, both of them gate
/// identities, and nothing in the reading could name which two.
///
/// Matched by shape rather than by a list of today's names, because the gate
/// gains refusal identities over time and a list would silently stop covering
/// them: a principle is named after the file that carries it, and a gate names
/// its own refusals `<script>.sh:<reason>`. The predicate is the shape, not the
/// substring -- a principle whose name happens to contain a colon is not
/// excluded by the colon, and the counterexample census on the live account
/// found exactly two colon-carrying slugs, both gate identities.
macro_rules! not_a_principle {
    () => {
        "slug !~ '^[A-Za-z0-9_.-]+\\.sh:'"
    };
}

/// The three counts in one read, the unset state included: `GROUP BY` over a
/// nullable column already forms a group for `NULL`, and filtering it out would
/// mean a second query to learn the same thing.
///
/// The gate's own refusal identities are left out for the reason on
/// [`not_a_principle`].
const CENSUS_SQL: &str = concat!(
    "SELECT state, count(*) AS n FROM principle_ledger WHERE ",
    not_a_principle!(),
    " GROUP BY state"
);

/// The same census by the tier that checks each row, the undeclared rows grouped
/// under `NULL`.
///
/// A separate statement from the state census rather than a `state, check_kind`
/// cross product: the two questions are asked by different readers at different
/// times, and a joint grouping would make each one pay for the other's cardinality.
///
/// Filtered by the same predicate as the state census: the two are read side by
/// side, and a tier census whose total disagreed with the state census by the
/// gate's own rows would make every reader ask which one is wrong.
const CHECK_KIND_CENSUS_SQL: &str = concat!(
    "SELECT check_kind, count(*) AS n FROM principle_ledger WHERE ",
    not_a_principle!(),
    " GROUP BY check_kind"
);

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
///
/// The session is written as the empty string, the value the column documents
/// for a side that has no session to name: this method is called by the
/// platform's own gate, which refuses inside a process that belongs to no
/// session, and the host gate does not come through here at all. That is the
/// empty string's one meaning, and the rows that predate the column do not share
/// it: they hold `-`, the marker for a session nobody recorded, so a reader
/// taking every blank for "there was no session" is not describing these rows.
/// Naming the column rather than letting the default apply is what it costs to
/// keep the conflict target equal to the primary key -- a target listing fewer columns
/// than the key matches no unique constraint, and the insert fails at run time
/// rather than merging. `hits` is left out for the opposite reason: a magnitude
/// is counted by the side that did the refusing, and this side counts none.
/// The key includes `criterion`, so two shapes of one principle refused in the
/// same second are two rows.
///
/// They are two facts: a call that both added widely and committed bare was
/// refused for two reasons, and a row that named whichever arrived first would
/// answer "which shape is doing this" with the wrong shape for good. The merge
/// below is the same merge as before, one column narrower: a re-report of the
/// same shape still collapses, and only a genuinely different shape takes a row
/// of its own.
///
/// `span` is written on insert and left alone on conflict, like `briefed`: it is
/// what the refusing side saw at that instant, and a re-report of the same event
/// is not a second observation of it.
const REPORT_REFUSAL_SQL: &str = "\
INSERT INTO principle_refusals (slug, refused_at, source, criterion, span, session_id, count) \
VALUES ($1, $2, $3, $4, $5, '', $6) \
ON CONFLICT (slug, refused_at, source, session_id, criterion) DO UPDATE \
SET count = GREATEST(principle_refusals.count, EXCLUDED.count) \
WHERE principle_refusals.count < EXCLUDED.count";

/// A refusal queued for the fold, carrying the whole of its key.
///
/// Not `PrincipleRefusal`: that type is the intake's payload, and a payload has
/// no session -- the side that reports through the contract's `report_refusals`
/// belongs to no session at all. A queued row does, and the fold has to carry it,
/// because a row is marked folded by its key and a mark that named less than the
/// key would stamp a row this pass never read.
struct QueuedRefusal {
    slug: String,
    refused_at: DateTime<Utc>,
    source: String,
    session_id: String,
    count: i64,
}

/// The refusals one fold takes, oldest first.
///
/// Ordered by the whole key rather than by the three columns that used to be one:
/// `LIMIT` cuts this list, and a tie broken differently between two passes would
/// leave a row unread on one pass and read twice on the next.
const REFUSAL_FOLD_SELECT_SQL: &str = "\
SELECT slug, refused_at, source, session_id, count
FROM principle_refusals
WHERE folded_at IS NULL
ORDER BY refused_at ASC, slug ASC, source ASC, session_id ASC
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

/// Mark one queued refusal as counted, by the whole of its key.
///
/// The batch this pass read is bounded, so two rows can share a slug, an instant
/// and a source and differ only in session. A mark that stopped at the source
/// would stamp the sibling this pass never read, and that sibling's count would
/// then never be added to the account -- the counter would fall behind the queue
/// with nothing in the reading to say so.
const REFUSAL_FOLD_MARK_SQL: &str = "\
UPDATE principle_refusals SET folded_at = now()
WHERE folded_at IS NULL AND slug = $1 AND refused_at = $2 AND source = $3
  AND session_id = $4";

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

/// The tier a row carries, or why it cannot be read.
///
/// A stored string this contract does not know fails the read instead of being
/// dropped or defaulted. Dropping it would report a classified principle as
/// unclassified -- the one thing the column exists to tell apart -- and defaulting
/// it would invent a tier nobody declared. Both are the same error the state
/// column refuses, applied to the column that says who is meant to check the
/// principle.
fn parse_check_kind(row: &sqlx::postgres::PgRow) -> Result<Option<PrincipleCheckKind>, String> {
    let raw: Option<String> = row
        .try_get("check_kind")
        .map_err(|e| format!("check_kind: {e}"))?;
    match raw {
        Some(text) => Ok(Some(PrincipleCheckKind::parse(&text).ok_or_else(|| {
            format!("check_kind column holds `{text}`, which is not a check kind")
        })?)),
        None => Ok(None),
    }
}

/// The band a row carries.
///
/// The column is not nullable on the ledger and the closed set has a member for
/// "no document said", so the only way this fails is a value outside the set --
/// which a foreign writer, not this store, would have put there. Reading it as
/// an error rather than folding it into `unstated` keeps a wrong word from
/// becoming a row that reads as evidence nobody prioritised the work.
fn parse_priority(row: &sqlx::postgres::PgRow) -> Result<ChangePriority, String> {
    let raw: String = row
        .try_get("priority")
        .map_err(|e| format!("priority: {e}"))?;
    raw.parse::<ChangePriority>()
        .map_err(|e| format!("priority column holds `{raw}`, which is not a band: {e}"))
}

/// The band a reading carried, when it carried one.
///
/// `None` is silence and not `unstated`: a reading written before this column
/// existed said nothing about urgency, and the merge is what keeps that silence
/// from clearing a band somebody did state.
fn parse_optional_priority(row: &sqlx::postgres::PgRow) -> Result<Option<ChangePriority>, String> {
    let raw: Option<String> = row
        .try_get("priority")
        .map_err(|e| format!("priority: {e}"))?;
    match raw {
        Some(text) => Ok(Some(text.parse::<ChangePriority>().map_err(|e| {
            format!("priority column holds `{text}`, which is not a band: {e}")
        })?)),
        None => Ok(None),
    }
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
    // A criterion longer than the column is refused, not cut: it is part of the
    // key, and two shape names that agree for the first sixty-four characters
    // would become one shape without anybody deciding that. The span, one line
    // down, is cut instead -- it is the evidence and not the identity, and
    // losing a refusal because its text ran long would trade the count for the
    // description of it.
    if event.criterion.chars().count() > REFUSAL_CRITERION_MAX {
        return Err(format!(
            "the criterion of a refusal of {} is longer than {REFUSAL_CRITERION_MAX} characters, \
             which is not a shape name",
            event.slug
        ));
    }
    if event.count < 1 {
        return Err(format!(
            "a refusal of {} carries count {}, which is not a refusal",
            event.slug, event.count
        ));
    }
    Ok(())
}
