//!PostgreSQL-backed store for the meta-learning engine's decision counters.
//!
//!The table is created by the storage migrations, not by an `init_schema` here:
//!a second CREATE TABLE would be a second definition of the same table, and the
//!one that wins is whichever ran last.
//!
//!One cell moves by addition and never by assignment. The statement is a single
//!`INSERT ... ON CONFLICT DO UPDATE` because the correctness argument is about
//!statements: two of them against the same key are serialized by the row lock,
//!and whichever runs second adds to what the first left, so the sum after both
//!is the sum the callers intended. Read-then-write would put an unbounded gap
//!between the two halves of that, which is the failure the file this replaces
//!had at its worst -- both processes alive, both counting, one increment gone.
//!
//!The column widths are checked here rather than left to the server: a
//!`VARCHAR(191)` refuses a longer value with a database error halfway through a
//!write, and the caller would have to read that error to learn which field was
//!too long. The check names the field.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use cog_core::{DecisionStatCell, DecisionStatsStore, SFError, SFResult};

/// The widths the keyed columns were created with.
// cap: bound — 建键列当初用的那个宽度
const CATEGORY_MAX: usize = 32;
// cap: bound — the group-key column width, a schema limit rather than a measurement
const GROUP_MAX: usize = 191;
// cap: bound — the decision column width, a schema limit rather than a measurement
const DECISION_MAX: usize = 191;

/// Add one cell's worth of evidence. The conflict clause adds rather than
/// assigns, and it touches the timestamp so a reader can tell a cell that is
/// accumulating from one that stopped.
const ACCUMULATE_SQL: &str = "\
INSERT INTO decision_stats_groups (category, group_key, decision, attempts, successes) \
VALUES ($1, $2, $3, $4, $5) \
ON CONFLICT (category, group_key, decision) DO UPDATE \
    SET attempts = decision_stats_groups.attempts + EXCLUDED.attempts, \
        successes = decision_stats_groups.successes + EXCLUDED.successes, \
        last_recorded_at = NOW()";

/// Raise the cell to at least these totals. `GREATEST` is what makes the
/// statement idempotent: running it any number of times leaves the cell at the
/// larger of the two, so a repeated start cannot add the same evidence twice
/// and a start by a second process cannot lower what the first recorded.
const HOLD_AT_LEAST_SQL: &str = "\
INSERT INTO decision_stats_groups (category, group_key, decision, attempts, successes) \
VALUES ($1, $2, $3, $4, $5) \
ON CONFLICT (category, group_key, decision) DO UPDATE \
    SET attempts = GREATEST(decision_stats_groups.attempts, EXCLUDED.attempts), \
        successes = GREATEST(decision_stats_groups.successes, EXCLUDED.successes), \
        last_recorded_at = NOW()";

const LOAD_ALL_SQL: &str = "\
SELECT category, group_key, decision, attempts, successes \
FROM decision_stats_groups \
ORDER BY category, group_key, decision";

/// PostgreSQL decision-stats store.
#[derive(Clone)]
pub struct PostgresDecisionStatsStore {
    pool: PgPool,
}

impl PostgresDecisionStatsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn check_width(what: &str, value: &str, max: usize) -> SFResult<()> {
    if value.is_empty() {
        return Err(SFError::Validation(format!(
            "decision stats: {what} is empty, and an empty key is a cell nothing can look up"
        )));
    }
    if value.len() > max {
        return Err(SFError::Validation(format!(
            "decision stats: {what} is {} bytes, over the column's {max}",
            value.len()
        )));
    }
    Ok(())
}

#[async_trait]
impl DecisionStatsStore for PostgresDecisionStatsStore {
    async fn accumulate(&self, delta: &DecisionStatCell) -> SFResult<()> {
        check_width("category", &delta.category, CATEGORY_MAX)?;
        check_width("group_key", &delta.group_key, GROUP_MAX)?;
        check_width("decision", &delta.decision, DECISION_MAX)?;
        if delta.attempts < 1 {
            return Err(SFError::Validation(format!(
                "decision stats: an accumulation of {} attempts records no decision",
                delta.attempts
            )));
        }
        if delta.successes < 0 || delta.successes > delta.attempts {
            return Err(SFError::Validation(format!(
                "decision stats: {} successes out of {} attempts is not an outcome",
                delta.successes, delta.attempts
            )));
        }
        sqlx::query(ACCUMULATE_SQL)
            .bind(&delta.category)
            .bind(&delta.group_key)
            .bind(&delta.decision)
            .bind(delta.attempts)
            .bind(delta.successes)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("decision stats accumulate: {e}")))?;
        Ok(())
    }

    async fn hold_at_least(&self, totals: &DecisionStatCell) -> SFResult<()> {
        check_width("category", &totals.category, CATEGORY_MAX)?;
        check_width("group_key", &totals.group_key, GROUP_MAX)?;
        check_width("decision", &totals.decision, DECISION_MAX)?;
        if totals.attempts < 0 || totals.successes < 0 {
            return Err(SFError::Validation(format!(
                "decision stats: ({}, {}) is not a pair of counts",
                totals.attempts, totals.successes
            )));
        }
        if totals.successes > totals.attempts {
            return Err(SFError::Validation(format!(
                "decision stats: {} successes out of {} attempts is not an outcome",
                totals.successes, totals.attempts
            )));
        }
        sqlx::query(HOLD_AT_LEAST_SQL)
            .bind(&totals.category)
            .bind(&totals.group_key)
            .bind(&totals.decision)
            .bind(totals.attempts)
            .bind(totals.successes)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("decision stats hold_at_least: {e}")))?;
        Ok(())
    }

    async fn load_all(&self) -> SFResult<Vec<DecisionStatCell>> {
        let rows = sqlx::query(LOAD_ALL_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("decision stats load_all: {e}")))?;
        rows.into_iter()
            .map(|row| {
                Ok(DecisionStatCell {
                    category: row.try_get("category").map_err(db_err)?,
                    group_key: row.try_get("group_key").map_err(db_err)?,
                    decision: row.try_get("decision").map_err(db_err)?,
                    attempts: row.try_get("attempts").map_err(db_err)?,
                    successes: row.try_get("successes").map_err(db_err)?,
                })
            })
            .collect()
    }
}

fn db_err(e: sqlx::Error) -> SFError {
    SFError::Database(format!("decision stats column: {e}"))
}
