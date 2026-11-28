//! The runtime principle account, in PostgreSQL.
//!
//! One writer per fact and one statement per transition. The two writes are
//! deliberately not "upsert whatever the caller has": a proposal and a decision
//! are different acts by different people, and a single upsert would let a
//! proposer's second call overwrite the sentence an owner has already approved.
//! What the statements below can and cannot do is the whole lifecycle, checked
//! by the server rather than by a reader remembering the rules.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{RuntimePrinciple, RuntimePrincipleState, RuntimePrincipleStore, SFError, SFResult};

/// PostgreSQL runtime principle account.
pub struct PostgresRuntimePrincipleStore {
    pool: PgPool,
}

impl PostgresRuntimePrincipleStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const COLUMNS: &str =
    "slug, trigger_condition, criterion, action, state, proposed_by, proposed_at, decided_at, used";

/// A proposal lands when the slug is new, and re-lands when the row it names was
/// rejected.
///
/// The `WHERE` clause is the guard, and it is on the update branch only: a row
/// that is proposed is someone else's pending proposal, and a row that is
/// approved or retired is a rule that people are working by. Overwriting either
/// would change what the platform tells its employees without a decision, which
/// is the one thing this account exists to prevent.
///
/// Every state word here is a parameter, read from the declaration that owns the
/// spelling: a query that spells one out keeps matching the old word after the
/// declaration is renamed, selects nothing, and reads as "there is no such row".
const PROPOSE_SQL: &str = "\
INSERT INTO runtime_principle \
    (slug, trigger_condition, criterion, action, state, proposed_by, proposed_at, decided_at) \
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL) \
ON CONFLICT (slug) DO UPDATE SET \
    trigger_condition = EXCLUDED.trigger_condition, \
    criterion = EXCLUDED.criterion, \
    action = EXCLUDED.action, \
    state = EXCLUDED.state, \
    proposed_by = EXCLUDED.proposed_by, \
    proposed_at = EXCLUDED.proposed_at, \
    decided_at = NULL \
WHERE runtime_principle.state = $8";

const STATE_OF_SQL: &str = "SELECT state FROM runtime_principle WHERE slug = $1";

/// A decision moves a row only out of the state it is a decision about: a
/// proposal is approved or rejected, a rule in force is retired. The `from`
/// state is the guard, and it is what makes the recorded `decided_at` the
/// moment the owner actually decided — without it a repeated call would move
/// the date forward and the row would answer "since when has this been in
/// force" with the last time somebody re-ran the command.
const DECIDE_SQL: &str = "\
UPDATE runtime_principle SET state = $2, decided_at = $3 \
WHERE slug = $1 AND state = $4";

const IN_FORCE_SQL: &str = "\
SELECT slug, trigger_condition, criterion, action, state, proposed_by, proposed_at, decided_at, used \
FROM runtime_principle \
WHERE state = $1 \
ORDER BY decided_at ASC, slug ASC";

const COUNT_SQL: &str = "UPDATE runtime_principle SET used = used + 1 WHERE slug = ANY($1)";

fn decode(row: &sqlx::postgres::PgRow) -> SFResult<RuntimePrinciple> {
    let raw: String = row.try_get("state").map_err(db_err)?;
    let state = RuntimePrincipleState::parse(&raw).ok_or_else(|| {
        SFError::Database(format!(
            "a runtime principle holds a state this build cannot name: {raw:?}"
        ))
    })?;
    Ok(RuntimePrinciple {
        slug: row.try_get("slug").map_err(db_err)?,
        trigger: row.try_get("trigger_condition").map_err(db_err)?,
        criterion: row.try_get("criterion").map_err(db_err)?,
        action: row.try_get("action").map_err(db_err)?,
        state,
        proposed_by: row.try_get("proposed_by").map_err(db_err)?,
        proposed_at: row.try_get("proposed_at").map_err(db_err)?,
        decided_at: row.try_get("decided_at").map_err(db_err)?,
        used: row.try_get::<i64, _>("used").map_err(db_err)?.max(0) as u64,
    })
}

fn db_err(e: sqlx::Error) -> SFError {
    SFError::Database(format!("decode a runtime principle row: {e}"))
}

#[async_trait]
impl RuntimePrincipleStore for PostgresRuntimePrincipleStore {
    async fn propose(&self, principle: &RuntimePrinciple) -> SFResult<()> {
        if principle.state != RuntimePrincipleState::Proposed {
            return Err(SFError::Validation(format!(
                "a proposal is a proposal: {} arrives as {:?}, and only a decision makes \
                 a rule in force",
                principle.slug, principle.state
            )));
        }
        let inserted = sqlx::query(PROPOSE_SQL)
            .bind(&principle.slug)
            .bind(&principle.trigger)
            .bind(&principle.criterion)
            .bind(&principle.action)
            .bind(RuntimePrincipleState::Proposed.as_str())
            .bind(&principle.proposed_by)
            .bind(principle.proposed_at)
            .bind(RuntimePrincipleState::Rejected.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("file a runtime principle: {e}")))?
            .rows_affected();
        if inserted == 0 {
            // The statement wrote nothing, and which state stopped it decides
            // what the caller should do: an approved row needs a replacement
            // proposal and a retirement, a pending one just needs to be decided
            // first. Both are answers the caller cannot get from "conflict".
            let held: Option<String> = sqlx::query(STATE_OF_SQL)
                .bind(&principle.slug)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| SFError::Database(format!("read the held principle row: {e}")))?
                .and_then(|row| row.try_get::<String, _>("state").ok());
            return Err(SFError::Conflict(format!(
                "runtime principle {:?} is already on the account as {:?}; an amendment is \
                 a new slug whose predecessor is retired after the replacement is approved",
                principle.slug, held
            )));
        }
        Ok(())
    }

    async fn decide(
        &self,
        slug: &str,
        decision: RuntimePrincipleState,
        at: DateTime<Utc>,
    ) -> SFResult<bool> {
        // Which state a decision moves a row out of is the whole difference
        // between the three: a proposal is approved or rejected, and only a
        // rule in force is retired.
        let from = match decision {
            RuntimePrincipleState::Approved | RuntimePrincipleState::Rejected => {
                RuntimePrincipleState::Proposed
            }
            RuntimePrincipleState::Retired => RuntimePrincipleState::Approved,
            RuntimePrincipleState::Proposed => {
                return Err(SFError::Validation(
                    "a decision is approved, rejected or retired: 'proposed' is what a row \
                     already is before anyone decides"
                        .into(),
                ))
            }
        };
        let changed = sqlx::query(DECIDE_SQL)
            .bind(slug)
            .bind(decision.as_str())
            .bind(at)
            .bind(from.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("decide runtime principle {slug:?}: {e}")))?;
        Ok(changed.rows_affected() > 0)
    }

    async fn in_force(&self) -> SFResult<Vec<RuntimePrinciple>> {
        let rows = sqlx::query(IN_FORCE_SQL)
            .bind(RuntimePrincipleState::Approved.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the runtime principles in force: {e}")))?;
        rows.iter().map(decode).collect()
    }

    async fn note_used(&self, slugs: &[String]) -> SFResult<()> {
        if slugs.is_empty() {
            return Ok(());
        }
        sqlx::query(COUNT_SQL)
            .bind(slugs)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("count a runtime principle's uses: {e}")))?;
        Ok(())
    }

    async fn list(&self, state: Option<RuntimePrincipleState>) -> SFResult<Vec<RuntimePrinciple>> {
        let sql = match state {
            Some(_) => format!(
                "SELECT {COLUMNS} FROM runtime_principle WHERE state = $1 \
                 ORDER BY decided_at NULLS LAST, proposed_at ASC, slug ASC"
            ),
            None => format!(
                "SELECT {COLUMNS} FROM runtime_principle \
                 ORDER BY decided_at NULLS LAST, proposed_at ASC, slug ASC"
            ),
        };
        let mut query = sqlx::query(&sql);
        if let Some(state) = state {
            query = query.bind(state.as_str().to_string());
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("list the runtime principles: {e}")))?;
        rows.iter().map(decode).collect()
    }
}
