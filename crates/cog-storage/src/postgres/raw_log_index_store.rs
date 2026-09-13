//! PostgreSQL-backed implementation of [`RawLogIndexStore`].
//!
//! The table is owned by the migrations, which declare it partitioned by
//! `log_date`. The store therefore never creates it: a store-local
//! `CREATE TABLE IF NOT EXISTS` would be a second definition of the same table
//! and the two would drift.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{PgPool, Row};

use cog_core::{RawLogIndexEntry, RawLogIndexStore, RawLogQuery, SFError, SFResult, StorageTier};

/// PostgreSQL-backed raw log index store.
pub struct PostgresRawLogIndexStore {
    pool: PgPool,
}

impl PostgresRawLogIndexStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RawLogIndexStore for PostgresRawLogIndexStore {
    async fn upsert(&self, entry: RawLogIndexEntry) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO raw_log_index (
                stream_name, log_date, hour, storage_path, size_bytes,
                event_count, checksum, first_at, last_at, tier, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (stream_name, log_date) DO UPDATE SET
                hour         = EXCLUDED.hour,
                storage_path = EXCLUDED.storage_path,
                size_bytes   = EXCLUDED.size_bytes,
                event_count  = EXCLUDED.event_count,
                checksum     = EXCLUDED.checksum,
                first_at     = EXCLUDED.first_at,
                last_at      = EXCLUDED.last_at,
                tier         = EXCLUDED.tier,
                created_at   = EXCLUDED.created_at
            "#,
        )
        .bind(&entry.stream_name)
        .bind(entry.log_date)
        .bind(entry.hour as i16)
        .bind(&entry.file_path)
        .bind(entry.size_bytes as i64)
        .bind(entry.event_count as i64)
        .bind(&entry.checksum)
        .bind(entry.start_time)
        .bind(entry.end_time)
        .bind(entry.tier.as_str())
        .bind(entry.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        Ok(())
    }

    async fn query(&self, q: &RawLogQuery) -> SFResult<Vec<RawLogIndexEntry>> {
        // Every filter is optional, so each one is written as "an absent
        // parameter matches everything" rather than assembled from string
        // fragments: placeholder numbering cannot drift out of step with the
        // bindings.
        let rows = sqlx::query(
            r#"
            SELECT stream_name, log_date, hour, storage_path, size_bytes,
                   event_count, checksum, first_at, last_at, tier, created_at
            FROM raw_log_index
            WHERE ($1::text IS NULL OR stream_name = $1)
              AND ($2::text IS NULL OR tier = $2)
              AND ($3::timestamptz IS NULL OR last_at >= $3)
              AND ($4::timestamptz IS NULL OR first_at <= $4)
              AND ($5::smallint IS NULL OR hour = $5)
            ORDER BY first_at ASC
            LIMIT $6
            "#,
        )
        .bind(q.stream.as_deref())
        .bind(q.tier.map(|t| t.as_str()))
        .bind(q.start)
        .bind(q.end)
        .bind(q.hour.map(|h| h as i16))
        .bind(q.limit.map(|l| l as i64).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        rows.iter().map(row_to_entry).collect()
    }
}

/// A malformed tier is reported rather than defaulted, so a schema mismatch
/// cannot masquerade as a tier decision.
fn row_to_entry(row: &sqlx::postgres::PgRow) -> SFResult<RawLogIndexEntry> {
    let column = |name: &str| SFError::Database(format!("raw_log_index.{name} unreadable"));
    let tier: String = row.try_get("tier").map_err(|_| column("tier"))?;
    let tier = tier
        .parse::<StorageTier>()
        .map_err(|_| SFError::Database(format!("unknown storage tier in raw_log_index: {tier}")))?;

    Ok(RawLogIndexEntry {
        hour: row.try_get::<i16, _>("hour").map_err(|_| column("hour"))? as u8,
        stream_name: row
            .try_get("stream_name")
            .map_err(|_| column("stream_name"))?,
        log_date: row
            .try_get::<NaiveDate, _>("log_date")
            .map_err(|_| column("log_date"))?,
        file_path: row
            .try_get("storage_path")
            .map_err(|_| column("storage_path"))?,
        tier,
        size_bytes: row
            .try_get::<i64, _>("size_bytes")
            .map_err(|_| column("size_bytes"))? as u64,
        event_count: row
            .try_get::<i64, _>("event_count")
            .map_err(|_| column("event_count"))? as u64,
        checksum: row.try_get("checksum").map_err(|_| column("checksum"))?,
        start_time: row
            .try_get::<DateTime<Utc>, _>("first_at")
            .map_err(|_| column("first_at"))?,
        end_time: row
            .try_get::<DateTime<Utc>, _>("last_at")
            .map_err(|_| column("last_at"))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|_| column("created_at"))?,
    })
}
