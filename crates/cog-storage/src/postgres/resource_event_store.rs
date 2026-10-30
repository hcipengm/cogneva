//!PostgreSQL-backed store for the resource account's events.
//!
//!The table is created by the storage migrations, not by an `init_schema` here:
//!a second CREATE TABLE would be a second definition of the same table, and the
//!one that wins is whichever ran last.
//!
//!Only one column ever moves after a row is written: the fold marker. Everything
//!else about an event -- when it happened, who decided it, what moved -- is what
//!the row is *for*, and a store that could rewrite it would be a store whose
//!reader cannot tell what actually happened from what was last believed about
//!it.
//!
//!Two things here are easy to get subtly wrong and are therefore stated in the
//!code rather than left to the caller:
//!
//!  * [`append_events`] answers how many rows were *new*. `rows_affected` cannot
//!    answer that, because a repeated report updates the count on an existing row
//!    and counts as affected. The insert returns `xmax = 0` for a fresh row,
//!    which is the only thing that distinguishes the two cases.
//!  * [`fold_events`] selects and marks in one transaction with `FOR UPDATE SKIP
//!    LOCKED`, so two consumers cannot take the same row and a consumer cannot
//!    mark a row read that it never returned.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{
    ResourceEvent, ResourceEventKind, ResourceEventOutcome, ResourceEventRow, ResourceEventStore,
    SFError, SFResult,
};

/// The widths the keyed columns were created with, taken from the narrower
/// dialect's DDL. Checked here so a producer finds out at the write rather than
/// a database refusing a row halfway through a batch.
// cap: bound — 建键列当初的宽度，取自更窄那个方言的 DDL；在这里查是为了让生产者在写时就知道，而不是库在批量写到一半时拒一行
const SUBJECT_MAX: usize = 191;
// cap: bound — the actor column width, a schema limit rather than a measurement
const ACTOR_MAX: usize = 191;
// cap: bound — the reading-name column width, a schema limit rather than a measurement
const READING_MAX: usize = 191;

const ROW_COLUMNS: &str = "kind, occurred_at, subject, actor, reading, reading_value, \
                           from_value, to_value, outcome, detail, count, reported_at, folded_at";

/// Append one event; a repeated report of the same event adds to its count.
///
/// `xmax = 0` in `RETURNING` is true only for a row this statement inserted:
/// an upsert that found the row carries the updating transaction's id there.
const APPEND_SQL: &str = "\
    INSERT INTO resource_events \
        (kind, occurred_at, subject, actor, reading, reading_value, from_value, to_value, \
         outcome, detail, count) \
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
    ON CONFLICT (kind, occurred_at, subject, actor) DO UPDATE \
        SET count = resource_events.count + EXCLUDED.count \
    RETURNING (xmax = 0) AS inserted";

const FOLD_SELECT_SQL: &str = "\
    SELECT kind, occurred_at, subject, actor, reading, reading_value, from_value, to_value, \
           outcome, detail, count, reported_at, folded_at \
    FROM resource_events \
    WHERE folded_at IS NULL \
    ORDER BY occurred_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED";

const FOLD_MARK_SQL: &str = "\
    UPDATE resource_events SET folded_at = NOW() \
    WHERE kind = $1 AND occurred_at = $2 AND subject = $3 AND actor = $4";

const PENDING_SQL: &str = "SELECT count(*) AS n FROM resource_events WHERE folded_at IS NULL";

const CENSUS_SQL: &str = "\
    SELECT kind, outcome, count(*) AS n FROM resource_events GROUP BY kind, outcome";

const RECENT_SQL: &str = "\
    SELECT kind, occurred_at, subject, actor, reading, reading_value, from_value, to_value, \
           outcome, detail, count, reported_at, folded_at \
    FROM resource_events \
    ORDER BY occurred_at DESC \
    LIMIT $1";

fn db(e: sqlx::Error, what: &str) -> SFError {
    SFError::Database(format!("{what}: {e}"))
}

fn too_long(field: &str, value: &str, max: usize) -> Option<SFError> {
    (value.chars().count() > max).then(|| {
        SFError::Validation(format!(
            "resource event {field} is {} characters, over the {max} the column holds: {value}",
            value.chars().count()
        ))
    })
}

fn check(event: &ResourceEvent) -> SFResult<()> {
    for (field, value, max) in [
        ("subject", event.subject.as_str(), SUBJECT_MAX),
        ("actor", event.actor.as_str(), ACTOR_MAX),
        ("reading", event.reading.as_str(), READING_MAX),
    ] {
        if let Some(e) = too_long(field, value, max) {
            return Err(e);
        }
    }
    // A bound change without both ends is not the event this account exists to
    // record: "the bound moved" with neither end leaves a reader unable to say
    // what happened, and the row would be indistinguishable from a mistake.
    if event.kind == ResourceEventKind::BoundChange
        && (event.from_value.is_none() || event.to_value.is_none())
    {
        return Err(SFError::Validation(format!(
            "bound change of {} has no from/to: a move with one end is not a move",
            event.subject
        )));
    }
    if event.count < 1 {
        return Err(SFError::Validation(format!(
            "resource event count is {} for {}: an event reported zero times did not happen",
            event.count, event.subject
        )));
    }
    Ok(())
}

fn event_from_row(row: &sqlx::postgres::PgRow) -> Result<ResourceEvent, SFError> {
    let kind: String = row.try_get("kind").map_err(|e| db(e, "kind"))?;
    let outcome: String = row.try_get("outcome").map_err(|e| db(e, "outcome"))?;
    let kind = ResourceEventKind::parse(&kind).ok_or_else(|| {
        SFError::Database(format!(
            "resource_events holds kind `{kind}`, which this build does not know"
        ))
    })?;
    let outcome = ResourceEventOutcome::parse(&outcome).ok_or_else(|| {
        SFError::Database(format!(
            "resource_events holds outcome `{outcome}`, which this build does not know"
        ))
    })?;
    Ok(ResourceEvent {
        kind,
        occurred_at: row
            .try_get("occurred_at")
            .map_err(|e| db(e, "occurred_at"))?,
        subject: row.try_get("subject").map_err(|e| db(e, "subject"))?,
        actor: row.try_get("actor").map_err(|e| db(e, "actor"))?,
        reading: row.try_get("reading").map_err(|e| db(e, "reading"))?,
        reading_value: row
            .try_get("reading_value")
            .map_err(|e| db(e, "reading_value"))?,
        from_value: row.try_get("from_value").map_err(|e| db(e, "from_value"))?,
        to_value: row.try_get("to_value").map_err(|e| db(e, "to_value"))?,
        outcome,
        detail: row.try_get("detail").map_err(|e| db(e, "detail"))?,
        count: row.try_get("count").map_err(|e| db(e, "count"))?,
    })
}

fn row_from_row(row: &sqlx::postgres::PgRow) -> Result<ResourceEventRow, SFError> {
    Ok(ResourceEventRow {
        event: event_from_row(row)?,
        reported_at: row
            .try_get("reported_at")
            .map_err(|e| db(e, "reported_at"))?,
        folded_at: row.try_get("folded_at").map_err(|e| db(e, "folded_at"))?,
    })
}

/// The account's event half, over the platform's PostgreSQL.
pub struct PostgresResourceEventStore {
    pool: PgPool,
}

impl PostgresResourceEventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Every column this store reads, stated once: two readers that each wrote
    /// their own list would come back with differently ordered columns the day
    /// one of them is edited.
    pub fn row_columns() -> &'static str {
        ROW_COLUMNS
    }
}

#[async_trait]
impl ResourceEventStore for PostgresResourceEventStore {
    async fn append_events(&self, events: &[ResourceEvent]) -> SFResult<usize> {
        let mut inserted = 0usize;
        for event in events {
            check(event)?;
            let row = sqlx::query(APPEND_SQL)
                .bind(event.kind.as_str())
                .bind(event.occurred_at)
                .bind(&event.subject)
                .bind(&event.actor)
                .bind(&event.reading)
                .bind(event.reading_value)
                .bind(event.from_value)
                .bind(event.to_value)
                .bind(event.outcome.as_str())
                .bind(&event.detail)
                .bind(event.count)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| db(e, "append resource event"))?;
            let fresh: bool = row.try_get("inserted").map_err(|e| db(e, "inserted"))?;
            if fresh {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    async fn fold_events(&self, limit: usize) -> SFResult<Vec<ResourceEventRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db(e, "open the resource event fold"))?;
        let rows = sqlx::query(FOLD_SELECT_SQL)
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| db(e, "read resource events to fold"))?;
        let mut taken = Vec::with_capacity(rows.len());
        for row in &rows {
            let folded = row_from_row(row)?;
            sqlx::query(FOLD_MARK_SQL)
                .bind(folded.event.kind.as_str())
                .bind(folded.event.occurred_at)
                .bind(&folded.event.subject)
                .bind(&folded.event.actor)
                .execute(&mut *tx)
                .await
                .map_err(|e| db(e, "mark a resource event folded"))?;
            taken.push(folded);
        }
        tx.commit()
            .await
            .map_err(|e| db(e, "commit the resource event fold"))?;
        Ok(taken)
    }

    async fn pending_events(&self) -> SFResult<u64> {
        let row = sqlx::query(PENDING_SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| db(e, "count pending resource events"))?;
        let n: i64 = row.try_get("n").map_err(|e| db(e, "pending n"))?;
        Ok(n.max(0) as u64)
    }

    async fn oldest_unfolded_at(&self) -> SFResult<Option<DateTime<Utc>>> {
        let row = sqlx::query(
            "SELECT min(occurred_at) AS at FROM resource_events WHERE folded_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| db(e, "oldest unfolded event"))?;
        row.try_get("at")
            .map_err(|e| db(e, "oldest unfolded event"))
    }

    async fn census(&self) -> SFResult<Vec<(ResourceEventKind, ResourceEventOutcome, u64)>> {
        let rows = sqlx::query(CENSUS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| db(e, "census of resource events"))?;
        // Every closed-set pair, zero when nothing of that shape has happened:
        // a series that only appears after its first event cannot say "none
        // yet", and a reader summing the census would read an empty account as
        // an unreadable one.
        let mut out: Vec<(ResourceEventKind, ResourceEventOutcome, u64)> =
            Vec::with_capacity(ResourceEventKind::ALL.len() * ResourceEventOutcome::ALL.len());
        for kind in ResourceEventKind::ALL {
            for outcome in ResourceEventOutcome::ALL {
                out.push((kind, outcome, 0));
            }
        }
        for row in &rows {
            let kind: String = row.try_get("kind").map_err(|e| db(e, "kind"))?;
            let outcome: String = row.try_get("outcome").map_err(|e| db(e, "outcome"))?;
            let n: i64 = row.try_get("n").map_err(|e| db(e, "n"))?;
            let kind = ResourceEventKind::parse(&kind).ok_or_else(|| {
                SFError::Database(format!(
                    "resource_events holds kind `{kind}`, which this build does not know"
                ))
            })?;
            let outcome = ResourceEventOutcome::parse(&outcome).ok_or_else(|| {
                SFError::Database(format!(
                    "resource_events holds outcome `{outcome}`, which this build does not know"
                ))
            })?;
            if let Some(slot) = out.iter_mut().find(|(k, o, _)| *k == kind && *o == outcome) {
                slot.2 = n.max(0) as u64;
            }
        }
        Ok(out)
    }

    async fn recent(&self, limit: usize) -> SFResult<Vec<ResourceEventRow>> {
        let rows = sqlx::query(RECENT_SQL)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| db(e, "recent resource events"))?;
        rows.iter().map(row_from_row).collect()
    }
}
