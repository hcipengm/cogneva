//! PostgreSQL-backed implementation of [`RawLogIndexStore`].

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;

use cog_core::{RawLogIndexEntry, RawLogIndexStore, RawLogQuery, SFError, SFResult, StorageTier};

/// PostgreSQL-backed raw log index store.
pub struct PostgresRawLogIndexStore {
    pool: PgPool,
}

impl PostgresRawLogIndexStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Auto-create the required table and indexes if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_log_index (
                id            BIGSERIAL,
                stream_name   VARCHAR(32)  NOT NULL,
                log_date      DATE         NOT NULL,
                file_path     TEXT         NOT NULL,
                tier          VARCHAR(8)   NOT NULL DEFAULT 'hot',
                size_bytes    BIGINT       NOT NULL DEFAULT 0,
                checksum      VARCHAR(128) NOT NULL,
                start_time    TIMESTAMPTZ  NOT NULL,
                end_time      TIMESTAMPTZ  NOT NULL,
                created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                PRIMARY KEY (stream_name, log_date)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS raw_log_index_stream_time_idx
                ON raw_log_index (stream_name, start_time, end_time)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl RawLogIndexStore for PostgresRawLogIndexStore {
    async fn upsert(&self, entry: RawLogIndexEntry) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO raw_log_index (
                stream_name, log_date, file_path, tier, size_bytes,
                checksum, start_time, end_time, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (stream_name, log_date) DO UPDATE SET
                file_path    = EXCLUDED.file_path,
                tier         = EXCLUDED.tier,
                size_bytes   = EXCLUDED.size_bytes,
                checksum     = EXCLUDED.checksum,
                start_time   = EXCLUDED.start_time,
                end_time     = EXCLUDED.end_time,
                created_at   = EXCLUDED.created_at
            "#,
        )
        .bind(&entry.stream_name)
        .bind(entry.log_date)
        .bind(&entry.file_path)
        .bind(entry.tier.as_str())
        .bind(entry.size_bytes as i64)
        .bind(&entry.checksum)
        .bind(entry.start_time)
        .bind(entry.end_time)
        .bind(entry.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }

    async fn query(&self, q: &RawLogQuery) -> SFResult<Vec<RawLogIndexEntry>> {
        let mut sql = String::from(
            "SELECT stream_name, log_date, file_path, tier, size_bytes, \
             checksum, start_time, end_time, created_at FROM raw_log_index WHERE 1=1",
        );

        if q.stream.is_some() {
            sql.push_str(" AND stream_name = $1");
        }
        if q.tier.is_some() {
            sql.push_str(" AND tier = $2");
        }
        if q.start.is_some() {
            sql.push_str(" AND end_time >= $3");
        }
        if q.end.is_some() {
            sql.push_str(" AND start_time <= $4");
        }

        sql.push_str(" ORDER BY start_time ASC");

        if q.limit.is_some() {
            sql.push_str(" LIMIT $5");
        }

        let mut query = sqlx::query_as::<_, RawLogIndexRow>(&sql);

        if let Some(ref stream) = q.stream {
            query = query.bind(stream);
        }
        if let Some(tier) = q.tier {
            query = query.bind(tier.as_str());
        }
        if let Some(start) = q.start {
            query = query.bind(start);
        }
        if let Some(end) = q.end {
            query = query.bind(end);
        }
        if let Some(limit) = q.limit {
            query = query.bind(limit as i64);
        }

        let rows: Vec<RawLogIndexRow> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(rows.into_iter().map(|r| r.into_entry()).collect())
    }
}

/// Internal row type for sqlx mapping.
#[derive(sqlx::FromRow)]
struct RawLogIndexRow {
    stream_name: String,
    log_date: NaiveDate,
    file_path: String,
    tier: String,
    size_bytes: i64,
    checksum: String,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl RawLogIndexRow {
    fn into_entry(self) -> RawLogIndexEntry {
        RawLogIndexEntry {
            hour: 0,
            stream_name: self.stream_name,
            log_date: self.log_date,
            file_path: self.file_path,
            tier: self.tier.parse().unwrap_or(StorageTier::Hot),
            size_bytes: self.size_bytes as u64,
            event_count: 0,
            checksum: self.checksum,
            start_time: self.start_time,
            end_time: self.end_time,
            created_at: self.created_at,
        }
    }
}
