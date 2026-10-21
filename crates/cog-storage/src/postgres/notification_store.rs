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
pub const CREATE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    ntype VARCHAR(32) NOT NULL CHECK (ntype IN ('task_completed', 'quota_warning', 'mention', 'system')),
    title VARCHAR(256) NOT NULL,
    body TEXT,
    data JSONB NOT NULL DEFAULT '{}',
    is_read BOOLEAN NOT NULL DEFAULT false,
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)"#;

/// Indexes for the two queries this store runs: one user's list, newest first,
/// and that user's unread count.
pub const CREATE_INDEX_SQL: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_notifications_user_id \
     ON notifications(user_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_notifications_unread \
     ON notifications(user_id, is_read) WHERE is_read = false",
];

const NOTIFICATION_COLS: &str = "id, user_id, ntype, title, body, is_read, read_at, created_at";

// The table also carries a `data` column this build never writes or reads.
// Rows it did not write get the column's default (`{}`), which is why the
// insert below can leave it out.

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
        sqlx::query(CREATE_TABLE_SQL)
            .execute(&self.pool)
            .await
            .map_err(to_sf)?;
        for sql in CREATE_INDEX_SQL {
            sqlx::query(sql).execute(&self.pool).await.map_err(to_sf)?;
        }
        Ok(())
    }

    /// Drop read rows older than `keep_days`, leaving unread rows alone.
    ///
    /// Unread is the exception on purpose: a notification nobody has opened
    /// yet is the only reason the table exists, and expiring it by age would
    /// delete exactly the ones a person still has to see. Read rows are the
    /// history — useful for a while, unbounded otherwise.
    pub async fn prune_read(&self, keep_days: i64) -> SFResult<u64> {
        let result = sqlx::query(
            "DELETE FROM notifications \
             WHERE is_read = true AND read_at < NOW() - make_interval(days => $1)",
        )
        .bind(keep_days as i32)
        .execute(&self.pool)
        .await
        .map_err(to_sf)?;
        Ok(result.rows_affected())
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
