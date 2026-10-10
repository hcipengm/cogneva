//! PostgreSQL-backed store for taste intent submissions.
//!
//! A submission has two jobs, and both of them need row-level state rather than
//! an append-only log. It is *evidence*: what a submitter said, kept so the work
//! their judgement produced can be read back against what was actually asked
//! for, including after the process that accepted it is gone. And it is *work
//! to pick up*: a row that nothing has claimed yet, then exactly one of two
//! endings — handed to the task layer, or kept as evidence because a judgement
//! about the same subject was already in hand.
//!
//! Both jobs are why this lives beside the other stateful stores instead of in a
//! file: the claim is a single-row update whose result the claimer has to see,
//! and a restart must not lose a row between "the submitter was answered" and
//! "something was filed for it".
//!
//! Status is the only column a reader joins on. `pending` is a row nothing has
//! claimed; the other two spellings are [`TasteDisposition`]'s, because that is
//! the vocabulary the rest of the system reads and it should not be written
//! twice.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use cog_core::{
    SFError, SFResult, StoredTasteIntent, TasteDisposition, TasteIntent, TasteIntentPayload,
    TasteIntentSink, TasteIntentSource,
};

/// A row no one has claimed. Not a [`TasteDisposition`]: a submission nothing
/// has happened to yet has no fate to record.
const STATUS_PENDING: &str = "pending";

/// The columns every read returns, in one place so the two readers cannot come
/// back with differently ordered rows.
const ROW_COLUMNS: &str = "id, subject, submitted_by, submitted_at, payload, task_id";

/// PostgreSQL taste intent store.
#[derive(Clone)]
pub struct PostgresTasteIntentStore {
    pool: PgPool,
}

impl PostgresTasteIntentStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the table and index if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cog_taste_intents (
                id           UUID        PRIMARY KEY,
                subject      TEXT        NOT NULL,
                submitted_by TEXT        NOT NULL,
                submitted_at TIMESTAMPTZ NOT NULL,
                payload      JSONB       NOT NULL,
                status       TEXT        NOT NULL,
                task_id      TEXT,
                disposed_at  TIMESTAMPTZ
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        // Readers ask for one status in submission order, which is the order
        // both of them act in: oldest first, so a submission cannot be starved
        // by newer ones arriving every round.
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_cog_taste_intents_status
                ON cog_taste_intents(status, submitted_at)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }

    /// Read one row into the shape callers see, or report why it cannot be one.
    ///
    /// A row whose payload no longer parses is named as such rather than
    /// skipped in silence: it is a submission somebody made and the rest of the
    /// system cannot tell its absence from "nobody submitted that".
    fn row_to_stored(row: &sqlx::postgres::PgRow) -> Result<StoredTasteIntent, String> {
        let id: Uuid = row
            .try_get("id")
            .map_err(|e| format!("id column unreadable: {e}"))?;
        let payload: serde_json::Value = row
            .try_get("payload")
            .map_err(|e| format!("payload column unreadable: {e}"))?;
        let payload: TasteIntentPayload =
            serde_json::from_value(payload).map_err(|e| format!("payload unparsable: {e}"))?;
        Ok(StoredTasteIntent {
            intent: TasteIntent {
                id,
                subject: row
                    .try_get("subject")
                    .map_err(|e| format!("subject column unreadable: {e}"))?,
                submitted_by: row
                    .try_get("submitted_by")
                    .map_err(|e| format!("submitted_by column unreadable: {e}"))?,
                submitted_at: row
                    .try_get("submitted_at")
                    .map_err(|e| format!("submitted_at column unreadable: {e}"))?,
                payload,
            },
            task_id: row
                .try_get("task_id")
                .map_err(|e| format!("task_id column unreadable: {e}"))?,
        })
    }

    /// What both readers do with the rows a query returned.
    ///
    /// `None` when the query itself failed. Rows that cannot be read are
    /// reported and left out of the result rather than failing it: the remaining
    /// submissions in the same batch are still work, and a reader that dropped
    /// the whole batch would file none of them.
    fn shape(
        rows: Result<Vec<sqlx::postgres::PgRow>, sqlx::Error>,
    ) -> Option<Vec<StoredTasteIntent>> {
        let rows = match rows {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "taste intent read failed");
                return None;
            }
        };
        Some(
            rows.iter()
                .filter_map(|row| match Self::row_to_stored(row) {
                    Ok(stored) => Some(stored),
                    Err(e) => {
                        tracing::warn!(error = %e, "taste intent row unreadable; left out of this batch");
                        None
                    }
                })
                .collect(),
        )
    }
}

#[async_trait]
impl TasteIntentSink for PostgresTasteIntentStore {
    async fn submit(&self, intent: &TasteIntent) -> SFResult<()> {
        let payload = serde_json::to_value(&intent.payload)?;
        let result = sqlx::query(
            "INSERT INTO cog_taste_intents \
                 (id, subject, submitted_by, submitted_at, payload, status) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(intent.id)
        .bind(&intent.subject)
        .bind(&intent.submitted_by)
        .bind(intent.submitted_at)
        .bind(payload)
        .bind(STATUS_PENDING)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        if result.rows_affected() == 0 {
            // Same id, already stored. Nothing is overwritten: the first
            // submission is the evidence, and a re-send must not replace the
            // words a later reader is reading it against.
            tracing::debug!(
                id = %intent.id,
                "taste intent already stored; submission left as it was"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl TasteIntentSource for PostgresTasteIntentStore {
    async fn pending_intents(&self, limit: i64) -> Option<Vec<StoredTasteIntent>> {
        // Nothing has happened to these rows, so there is no clock to window
        // them by and the whole queue is read: a submission that waited through
        // an outage is read again on the round the store answers.
        Self::shape(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM cog_taste_intents \
                 WHERE status = $1 ORDER BY submitted_at ASC LIMIT $2"
            ))
            .bind(STATUS_PENDING)
            .bind(limit)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn filed_intents(
        &self,
        filed_since: DateTime<Utc>,
        limit: i64,
    ) -> Option<Vec<StoredTasteIntent>> {
        // Windowed on when the work was handed over and ordered by it, so the
        // window and the order are one answer about one clock. Measured from
        // the submission instead, a row that sat unclaimed through an outage
        // would age out on the round its work was first read, and the failure
        // of work that had only just begun would be looked for never.
        Self::shape(
            sqlx::query(&format!(
                "SELECT {ROW_COLUMNS} FROM cog_taste_intents \
                 WHERE status = $1 AND disposed_at >= $2 \
                 ORDER BY disposed_at ASC LIMIT $3"
            ))
            .bind(TasteDisposition::Filed.as_str())
            .bind(filed_since)
            .bind(limit)
            .fetch_all(&self.pool)
            .await,
        )
    }

    async fn dispose(
        &self,
        id: Uuid,
        disposition: TasteDisposition,
        task_id: Option<&str>,
    ) -> SFResult<()> {
        // Only a row nobody has claimed may be disposed, so the first decision
        // stands: a caller that filed a submission and crashed before writing
        // this cannot come back and record a different fate for it.
        let result = sqlx::query(
            "UPDATE cog_taste_intents \
                SET status = $2, task_id = COALESCE($3, task_id), disposed_at = $4 \
              WHERE id = $1 AND status = $5",
        )
        .bind(id)
        .bind(disposition.as_str())
        .bind(task_id)
        .bind(Utc::now())
        .bind(STATUS_PENDING)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        if result.rows_affected() == 0 {
            tracing::debug!(
                id = %id,
                disposition = disposition.as_str(),
                "taste intent was already disposed; nothing rewritten"
            );
        }
        Ok(())
    }
}
