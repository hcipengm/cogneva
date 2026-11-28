//! PostgreSQL-backed notification store.
//!
//! The two ends of a notification do not live in one process: the producer is
//! whichever plugin has something to say (reflection when a sandbox is
//! downgraded, the approval gate when a human has to decide), and the reader is
//! the gateway serving `GET /api/v1/notifications`. An in-process store holds a
//! producer's rows in the producer's Pod, where the reader never looks, and
//! loses them on restart. This store is the face both ends share.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use cog_core::{
    Notification, NotificationFilter, NotificationList, NotificationStore, NotificationType,
    SFError, SFResult,
};

/// The `notifications` table, as this store needs it.
///
/// Spelled out here rather than executed from `migrations/postgres`, whose
/// `CREATE INDEX` statements are not idempotent and would have to be applied
/// every start. That leaves one shape written twice, so
/// `notification_contract_matches_its_schema` reads both this constant and the
/// migration file and asserts they still describe the same columns.
const CREATE_TABLE_SQL_TEMPLATE: &str = r#"
CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    ntype VARCHAR(32) NOT NULL CHECK (ntype IN ('{ntype:task_completed}', '{ntype:quota_warning}', '{ntype:mention}', '{ntype:system}')),
    title VARCHAR(256) NOT NULL,
    body TEXT,
    is_read BOOLEAN NOT NULL DEFAULT false,
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)"#;

/// The table statement, with the closed set of kinds filled from
/// [`NotificationType`].
///
/// The constraint and the enum are two spellings of one set, and a table whose
/// constraint no longer matches what the writer sends refuses the write rather
/// than mis-filing it -- the failure lands on the first notification after a
/// rename, which is a person's message going missing.
pub fn create_table_sql() -> String {
    let mut slots: Vec<(&str, &str)> = Vec::new();
    for ntype in NotificationType::ALL {
        slots.push(("ntype", ntype.as_str()));
    }
    super::fill_slots(CREATE_TABLE_SQL_TEMPLATE, &slots)
}

/// Indexes for the two queries this store runs: one user's list, newest first,
/// and that user's unread count.
pub const CREATE_INDEX_SQL: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_notifications_user_id \
     ON notifications(user_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_notifications_unread \
     ON notifications(user_id, is_read) WHERE is_read = false",
];

const NOTIFICATION_COLS: &str = "id, user_id, ntype, title, body, is_read, read_at, created_at";

type NotificationRow = (
    Uuid,
    Uuid,
    String,
    String,
    Option<String>,
    bool,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
);

fn to_sf(err: sqlx::Error) -> SFError {
    SFError::Database(err.to_string())
}

/// Parse a column that is a `uuid` in the table and a string in the contract.
///
/// An unparseable value is [`SFError::Validation`], not "no rows": the caller
/// handed over a value the column cannot hold, and answering `Ok(false)` would
/// read as "that notification does not exist" for a request that never reached
/// the database.
fn as_uuid(value: &str, what: &str) -> SFResult<Uuid> {
    Uuid::parse_str(value).map_err(|e| {
        SFError::Validation(format!(
            "{what} {value:?} is not a UUID, so no row can carry it: {e}"
        ))
    })
}

fn notification_from_row(row: NotificationRow) -> Notification {
    Notification {
        id: row.0.to_string(),
        user_id: row.1.to_string(),
        ntype: NotificationType::from_str_lossy(&row.2),
        title: row.3,
        // The column is nullable; the contract is not. A row without a body is
        // a notification with nothing to say, not a row to drop from the list.
        body: row.4.unwrap_or_default(),
        is_read: row.5,
        read_at: row.6,
        created_at: row.7,
    }
}

pub struct PostgresNotificationStore {
    pool: PgPool,
}

impl PostgresNotificationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create the table and its indexes if they are not there yet.
    ///
    /// Statements run one at a time: the driver prepares each, and a prepared
    /// statement is one statement.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(&create_table_sql())
            .execute(&self.pool)
            .await
            .map_err(to_sf)?;
        for sql in CREATE_INDEX_SQL {
            sqlx::query(sql).execute(&self.pool).await.map_err(to_sf)?;
        }
        Ok(())
    }

    /// How many read rows are older than `keep_days`. **Counted, not deleted.**
    ///
    /// The earlier shape of this pass was `DELETE … WHERE is_read AND
    /// read_at < now() - keep_days`. That is an age bound on a fact: a
    /// notification is something the platform told a person, and the row is
    /// the only copy of it. Rows past the window are the ones nobody will be
    /// shown again — which is why they are the ones an archive would take, not
    /// the ones to drop. The deletion path belongs behind the same three steps
    /// as the observability log's: write a readable copy, read it back and
    /// check it, then delete. Until that path exists this pass only answers
    /// "how many are due", and the answer is the reading that says the archive
    /// is owed.
    ///
    /// Unread rows are excluded on purpose: a notification nobody has opened
    /// yet is the only reason the table exists, and counting it as due would
    /// make the reading say "archive these" about rows a person still has to
    /// see.
    pub async fn count_read_past_retention(&self, keep_days: i64) -> SFResult<i64> {
        let due: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM notifications \
             WHERE is_read = true AND read_at < NOW() - make_interval(days => $1)",
        )
        .bind(keep_days as i32)
        .fetch_one(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(due)
    }

    async fn unread_count(&self, user_id: Uuid) -> SFResult<i64> {
        sqlx::query_scalar(
            "SELECT count(*) FROM notifications WHERE user_id = $1 AND is_read = false",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(to_sf)
    }

    /// Where the page after `cursor` starts.
    ///
    /// The cursor is a row id, and the row it names has to still be there to
    /// say where it sat: the ordering is `(created_at, id)` and an id alone
    /// carries no position. A cursor whose row is gone is therefore the same
    /// thing as no cursor — the list restarts rather than coming back empty,
    /// which is what "no rows" would mean to the caller.
    async fn cursor_position(
        &self,
        user_id: Uuid,
        cursor: &str,
    ) -> SFResult<Option<(DateTime<Utc>, Uuid)>> {
        let cursor_id = as_uuid(cursor, "notification cursor")?;
        let row: Option<(DateTime<Utc>, Uuid)> = sqlx::query_as(
            "SELECT created_at, id FROM notifications WHERE id = $1 AND user_id = $2",
        )
        .bind(cursor_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(row)
    }
}

#[async_trait]
impl NotificationStore for PostgresNotificationStore {
    async fn list(&self, user_id: &str, filter: NotificationFilter) -> SFResult<NotificationList> {
        let owner = as_uuid(user_id, "notification owner")?;
        let limit = filter.limit.min(1000) as i64;
        let position = match filter.cursor.as_deref() {
            Some(cursor) => self.cursor_position(owner, cursor).await?,
            None => None,
        };

        // One row more than asked for: whether another page exists is a fact
        // about the rows past this one, and counting them in a second query
        // gives an answer that can disagree with the page that was returned.
        let rows: Vec<NotificationRow> = sqlx::query_as(&format!(
            "SELECT {NOTIFICATION_COLS} FROM notifications \
             WHERE user_id = $1 \
               AND ($2 = false OR is_read = false) \
               AND ($3::timestamptz IS NULL OR (created_at, id) < ($3::timestamptz, $4::uuid)) \
             ORDER BY created_at DESC, id DESC \
             LIMIT $5"
        ))
        .bind(owner)
        .bind(filter.unread_only)
        .bind(position.as_ref().map(|(created_at, _)| *created_at))
        .bind(position.as_ref().map(|(_, id)| *id))
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(to_sf)?;

        let has_more = rows.len() as i64 > limit;
        let items: Vec<Notification> = rows
            .into_iter()
            .take(limit as usize)
            .map(notification_from_row)
            .collect();
        let next_cursor = if has_more {
            items.last().map(|n| n.id.clone())
        } else {
            None
        };

        Ok(NotificationList {
            items,
            // This user's unread total, not the page's: the badge counts what
            // is waiting, regardless of which page the caller is on.
            unread_count: self.unread_count(owner).await?.max(0) as u32,
            next_cursor,
            has_more,
        })
    }

    async fn mark_read(&self, user_id: &str, id: &str) -> SFResult<bool> {
        let owner = as_uuid(user_id, "notification owner")?;
        let id = as_uuid(id, "notification id")?;
        // Both halves in the predicate, so one user cannot reach another's row
        // by knowing its id. Re-marking an already-read notification is not an
        // error and refreshes `read_at`, matching the in-memory store.
        let result = sqlx::query(
            "UPDATE notifications SET is_read = true, read_at = NOW() \
             WHERE id = $1 AND user_id = $2",
        )
        .bind(id)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(result.rows_affected() > 0)
    }

    async fn mark_all_read(&self, user_id: &str) -> SFResult<usize> {
        let owner = as_uuid(user_id, "notification owner")?;
        // Already-read rows are left alone, so the count is of the rows this
        // call actually changed and `read_at` keeps the first reading time.
        let result = sqlx::query(
            "UPDATE notifications SET is_read = true, read_at = NOW() \
             WHERE user_id = $1 AND is_read = false",
        )
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(result.rows_affected() as usize)
    }

    async fn create(&self, notification: Notification) -> SFResult<()> {
        let id = as_uuid(&notification.id, "notification id")?;
        let user_id = as_uuid(&notification.user_id, "notification owner")?;
        sqlx::query(
            "INSERT INTO notifications \
             (id, user_id, ntype, title, body, is_read, read_at, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(id)
        .bind(user_id)
        .bind(notification.ntype.as_str())
        .bind(&notification.title)
        .bind(&notification.body)
        .bind(notification.is_read)
        .bind(notification.read_at)
        .bind(notification.created_at)
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(())
    }
}
