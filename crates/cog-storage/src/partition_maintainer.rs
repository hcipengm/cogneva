//! Keeps the monthly partitions of the time-series tables open ahead of the
//! clock.
//!
//! PostgreSQL sends a row to the partition whose range covers the row's key.
//! When no partition covers it and the table has no DEFAULT partition, there
//! is nowhere for the row to go and the insert fails — taking down whatever
//! carried it. The window is therefore kept open a few months ahead, and every
//! table also carries a DEFAULT partition so that an insert can never fail
//! even if this task stops running.
//!
//! Rows landing in DEFAULT mean the window fell behind, so they are counted
//! and reported rather than silently accumulated.

use std::time::Duration;

use chrono::{Datelike, NaiveDate, Utc};
use sqlx::PgPool;
use tracing::{info, warn};

use cog_core::{SFError, SFResult, ShutdownSignal};

/// A table partitioned by month.
#[derive(Debug, Clone)]
pub struct PartitionedTable {
    pub parent: String,
    /// Column the range is keyed on.
    pub key: String,
    /// Name prefix of this table's partitions. Not always the parent name:
    /// the explainability partitions predate the rename to `explainability_part`.
    pub prefix: String,
}

impl PartitionedTable {
    pub fn new(parent: &str, key: &str, prefix: &str) -> Self {
        Self {
            parent: parent.to_string(),
            key: key.to_string(),
            prefix: prefix.to_string(),
        }
    }
}

/// The time-series tables that carry an insert key, in the order maintenance
/// touches them.
pub fn time_series_tables() -> Vec<PartitionedTable> {
    vec![
        PartitionedTable::new("messages", "created_at", "messages"),
        PartitionedTable::new("audit_logs", "created_at", "audit_logs"),
        PartitionedTable::new("billing_records", "created_at", "billing_records"),
        PartitionedTable::new("raw_log_index", "log_date", "raw_log_index"),
        PartitionedTable::new("explainability_part", "timestamp", "explainability"),
    ]
}

/// How far ahead of the current month partitions are kept open.
const MONTHS_AHEAD: i32 = 2;

/// How far back maintenance reaches, so a recently dropped partition heals.
const MONTHS_BEHIND: i32 = 1;

/// Opens monthly partitions ahead of the clock and installs a DEFAULT
/// partition per table.
pub struct PartitionMaintainer {
    pool: PgPool,
    tables: Vec<PartitionedTable>,
}

impl PartitionMaintainer {
    pub fn new(pool: PgPool, tables: Vec<PartitionedTable>) -> Self {
        Self { pool, tables }
    }

    /// Run once immediately, then every `interval_secs` until shutdown.
    pub async fn run(&self, interval_secs: u64, shutdown: ShutdownSignal) {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.maintain().await {
                        warn!(error = %e, "Partition maintenance round failed");
                    }
                }
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Ensure every table has its window open and its DEFAULT partition.
    ///
    /// A table that fails is reported and skipped; the remaining tables are
    /// still reconciled, since the failure is more likely to be specific to
    /// one table than to the connection.
    pub async fn maintain(&self) -> SFResult<()> {
        let today = Utc::now().date_naive();
        let current = months_since_epoch(today);
        let from_month = current - MONTHS_BEHIND;
        let to_month = current + MONTHS_AHEAD;

        let mut failures = 0usize;
        for table in &self.tables {
            if let Err(e) = self.ensure_default(table).await {
                warn!(table = table.parent, error = %e, "DEFAULT partition missing");
                failures += 1;
                continue;
            }
            for month in from_month..=to_month {
                if let Err(e) = self.ensure_month(table, month).await {
                    warn!(table = table.parent, month, error = %e, "monthly partition missing");
                    failures += 1;
                }
            }
            if let Err(e) = self.report_default_backlog(table).await {
                warn!(table = table.parent, error = %e, "DEFAULT partition not inspected");
            }
        }

        if failures > 0 {
            return Err(SFError::Database(format!(
                "{failures} partition maintenance step(s) failed"
            )));
        }
        Ok(())
    }

    /// `CREATE TABLE ... PARTITION OF ... DEFAULT`, a no-op when it exists.
    async fn ensure_default(&self, table: &PartitionedTable) -> SFResult<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {default} PARTITION OF {parent} DEFAULT",
            default = quote_ident(&default_name(table)),
            parent = quote_ident(&table.parent),
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn ensure_month(&self, table: &PartitionedTable, month: i32) -> SFResult<()> {
        let name = month_partition_name(table, month);
        if self.partition_exists(&name).await? {
            return Ok(());
        }

        let (start, end) = month_bounds(month);
        if self.default_holds_range(table, start, end).await? {
            // The window fell behind and rows are already parked in DEFAULT.
            // They have to leave before the new partition can be attached, so
            // detach DEFAULT, attach the partition, move the rows across, and
            // put DEFAULT back.
            self.split_default(table, &name, start, end).await?;
            info!(
                table = table.parent,
                partition = name,
                "Attached backfilled partition and moved rows out of DEFAULT"
            );
        } else {
            self.attach_partition(table, &name, start, end).await?;
            info!(table = table.parent, partition = name, "Opened partition");
        }
        Ok(())
    }

    async fn attach_partition(
        &self,
        table: &PartitionedTable,
        name: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> SFResult<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {name} PARTITION OF {parent} FOR VALUES FROM ('{start}') TO ('{end}')",
            name = quote_ident(name),
            parent = quote_ident(&table.parent),
            start = start,
            end = end,
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    /// Attach `name` for the given range while relocating the DEFAULT rows
    /// that belong in it. All five statements are one transaction, so a
    /// failure leaves DEFAULT attached and untouched.
    async fn split_default(
        &self,
        table: &PartitionedTable,
        name: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> SFResult<()> {
        let default = default_name(table);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;

        for sql in [
            format!(
                "ALTER TABLE {parent} DETACH PARTITION {default}",
                parent = quote_ident(&table.parent),
                default = quote_ident(&default),
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {name} PARTITION OF {parent} FOR VALUES FROM ('{start}') TO ('{end}')",
                name = quote_ident(name),
                parent = quote_ident(&table.parent),
            ),
            // Re-inserting through the parent routes each row to the partition
            // that now covers it.
            format!(
                "INSERT INTO {parent} SELECT * FROM {default} WHERE {key} >= '{start}'::date AND {key} < '{end}'::date",
                parent = quote_ident(&table.parent),
                default = quote_ident(&default),
                key = quote_ident(&table.key),
            ),
            format!(
                "DELETE FROM {default} WHERE {key} >= '{start}'::date AND {key} < '{end}'::date",
                default = quote_ident(&default),
                key = quote_ident(&table.key),
            ),
            format!(
                "ALTER TABLE {parent} ATTACH PARTITION {default} DEFAULT",
                parent = quote_ident(&table.parent),
                default = quote_ident(&default),
            ),
        ] {
            sqlx::query(&sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }

    async fn partition_exists(&self, name: &str) -> SFResult<bool> {
        let row = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = $1 AND n.nspname = current_schema()
             )",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(row)
    }

    async fn default_holds_range(
        &self,
        table: &PartitionedTable,
        start: NaiveDate,
        end: NaiveDate,
    ) -> SFResult<bool> {
        let sql = format!(
            "SELECT EXISTS (SELECT 1 FROM ONLY {default} WHERE {key} >= $1::date AND {key} < $2::date)",
            default = quote_ident(&default_name(table)),
            key = quote_ident(&table.key),
        );
        let row = sqlx::query_scalar::<_, bool>(&sql)
            .bind(start)
            .bind(end)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(row)
    }

    /// Rows in DEFAULT mean the window fell behind: they are outside every
    /// monthly range this task maintains, so they will keep accumulating.
    async fn report_default_backlog(&self, table: &PartitionedTable) -> SFResult<()> {
        let sql = format!(
            "SELECT count(*) FROM ONLY {default}",
            default = quote_ident(&default_name(table)),
        );
        let backlog: i64 = sqlx::query_scalar(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        if backlog > 0 {
            warn!(
                table = table.parent,
                rows = backlog,
                "Rows are parked in the DEFAULT partition: their key falls outside every \
                 open monthly partition"
            );
        }
        Ok(())
    }
}

fn default_name(table: &PartitionedTable) -> String {
    format!("{}_default", table.prefix)
}

fn month_partition_name(table: &PartitionedTable, month: i32) -> String {
    let (year, month_of_year) = split_month(month);
    format!("{}_y{year}m{month_of_year:02}", table.prefix)
}

/// Months since 0000-01, so month arithmetic is plain integer arithmetic.
fn months_since_epoch(date: NaiveDate) -> i32 {
    date.year() * 12 + date.month0() as i32
}

fn split_month(month: i32) -> (i32, u32) {
    (month.div_euclid(12), month.rem_euclid(12) as u32 + 1)
}

/// `[start, end)` covering `month`.
fn month_bounds(month: i32) -> (NaiveDate, NaiveDate) {
    let (year, month_of_year) = split_month(month);
    let start = month_start(year, month_of_year);
    let end = if month_of_year == 12 {
        month_start(year + 1, 1)
    } else {
        month_start(year, month_of_year + 1)
    };
    (start, end)
}

fn month_start(year: i32, month_of_year: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month_of_year, 1)
        .expect("partition month falls inside the representable date range")
}

/// Quote an identifier for splicing into generated DDL. Identifiers here come
/// from the constants above and from integer month arithmetic, never from
/// input, but quoting keeps the generated SQL honest.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn months_round_trip_across_a_year_boundary() {
        let december = months_since_epoch(date(2026, 12, 31));
        let january = months_since_epoch(date(2027, 1, 1));
        assert_eq!(january, december + 1);

        assert_eq!(split_month(december), (2026, 12));
        assert_eq!(split_month(january), (2027, 1));
    }

    #[test]
    fn month_bounds_span_one_month_and_roll_over_the_year() {
        let (start, end) = month_bounds(months_since_epoch(date(2026, 7, 15)));
        assert_eq!(start, date(2026, 7, 1));
        assert_eq!(end, date(2026, 8, 1));

        let (start, end) = month_bounds(months_since_epoch(date(2026, 12, 1)));
        assert_eq!(start, date(2026, 12, 1));
        assert_eq!(end, date(2027, 1, 1));
    }

    #[test]
    fn partition_names_use_the_table_prefix_not_the_parent_name() {
        let tables = time_series_tables();
        let explainability = tables
            .iter()
            .find(|t| t.parent == "explainability_part")
            .unwrap();
        let month = months_since_epoch(date(2026, 7, 1));

        assert_eq!(
            month_partition_name(explainability, month),
            "explainability_y2026m07"
        );
        assert_eq!(default_name(explainability), "explainability_default");
    }
}
