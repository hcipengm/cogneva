//! The catalog that makes the bytes in an object store a dataset.
//!
//! An object store answers "is there something under this key". It cannot
//! answer "what is in this bucket, how large, and is it still the thing that
//! was written" -- and a store that only answers the first question is a place
//! data goes to be forgotten carefully. The row here is what a restore reads
//! before it trusts a byte: the digest is compared against the object, the size
//! against what the reader got, and the coverage against what the consumer
//! needed.
//!
//! One row per version, not per write. A version that denotes two different
//! byte strings is not a version: a restore told "restore v3" would have two
//! answers, and nothing downstream could tell which one the consumer meant.
//! Registering the same content again is therefore a no-op that returns the
//! existing row, and registering different content under a taken version is an
//! error rather than an overwrite.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{SFError, SFResult};

/// One dataset version, as the catalog holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEntry {
    pub name: String,
    pub version: String,
    pub object_key: String,
    pub content_sha256: String,
    pub bytes: i64,
    /// How many records the dataset holds, when it is tabular. Absent for a
    /// blob: zero would say "a table with no rows", which is a different fact.
    pub row_count: Option<i64>,
    /// The interval the dataset covers, when it has one. Both bounds are
    /// present together or not at all.
    pub coverage_from: Option<DateTime<Utc>>,
    pub coverage_to: Option<DateTime<Utc>>,
    /// Where the bytes came from, in whatever form would let a person fetch
    /// them again.
    pub source: String,
    pub media_type: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl DatasetEntry {
    /// Whether a byte string is the one this row describes.
    ///
    /// Compares the digest the row recorded against one computed from the bytes
    /// a reader actually has. Length is not a substitute: a truncated file and
    /// a file with a hole can both keep their length.
    pub fn describes(&self, bytes: &[u8]) -> bool {
        self.bytes == bytes.len() as i64 && self.content_sha256 == sha256_of(bytes)
    }
}

/// What a producer declares when it registers a version.
///
/// Separate from [`DatasetEntry`] because the catalog assigns the timestamp and
/// the caller has no business proposing one: a row dated by its writer could
/// say a version predates the one it replaced.
#[derive(Debug, Clone)]
pub struct NewDataset {
    pub name: String,
    pub version: String,
    pub object_key: String,
    pub content_sha256: String,
    pub bytes: i64,
    pub row_count: Option<i64>,
    pub coverage_from: Option<DateTime<Utc>>,
    pub coverage_to: Option<DateTime<Utc>>,
    pub source: String,
    pub media_type: Option<String>,
}

/// The catalog's operations.
///
/// A trait rather than the concrete store so a producer can be tested without a
/// server, and so a deployment with no database can record nothing rather than
/// record into a process-local imitation.
#[async_trait]
pub trait DatasetCatalog: Send + Sync {
    /// Record a version.
    ///
    /// Idempotent for identical content: registering the same bytes under a
    /// version that already holds them returns the existing row. Refused when
    /// the version holds different bytes.
    async fn register(&self, entry: NewDataset) -> SFResult<DatasetEntry>;

    /// One version, or `None`.
    async fn find(&self, name: &str, version: &str) -> SFResult<Option<DatasetEntry>>;

    /// The most recently registered version of a dataset, or `None`.
    async fn latest(&self, name: &str) -> SFResult<Option<DatasetEntry>>;

    /// The newest `limit` versions of a dataset, newest first. A limit of zero
    /// asks for nothing and answers nothing rather than for everything.
    async fn list(&self, name: &str, limit: usize) -> SFResult<Vec<DatasetEntry>>;
}

/// `sha256:<hex>` over the bytes. The algorithm name travels with the value so
/// a row written under a later scheme is readable as "not the one you know".
pub fn sha256_of(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

const INSERT_SQL: &str = "\
INSERT INTO datasets (name, version, object_key, content_sha256, bytes, row_count, \
    coverage_from, coverage_to, source, media_type) \
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
ON CONFLICT (name, version) DO NOTHING";

const SELECT_COLUMNS: &str = "\
name, version, object_key, content_sha256, bytes, row_count, \
    coverage_from, coverage_to, source, media_type, created_at";

/// PostgreSQL catalog.
pub struct PostgresDatasetCatalog {
    pool: PgPool,
}

impl PostgresDatasetCatalog {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn entry_from(row: &sqlx::postgres::PgRow) -> SFResult<DatasetEntry> {
        Ok(DatasetEntry {
            name: row.try_get("name").map_err(db_err)?,
            version: row.try_get("version").map_err(db_err)?,
            object_key: row.try_get("object_key").map_err(db_err)?,
            content_sha256: row.try_get("content_sha256").map_err(db_err)?,
            bytes: row.try_get("bytes").map_err(db_err)?,
            row_count: row.try_get("row_count").map_err(db_err)?,
            coverage_from: row.try_get("coverage_from").map_err(db_err)?,
            coverage_to: row.try_get("coverage_to").map_err(db_err)?,
            source: row.try_get("source").map_err(db_err)?,
            media_type: row.try_get("media_type").map_err(db_err)?,
            created_at: row.try_get("created_at").map_err(db_err)?,
        })
    }
}

fn db_err(e: sqlx::Error) -> SFError {
    SFError::Database(format!("read a dataset row: {e}"))
}

#[async_trait]
impl DatasetCatalog for PostgresDatasetCatalog {
    async fn register(&self, entry: NewDataset) -> SFResult<DatasetEntry> {
        if entry.name.trim().is_empty() || entry.version.trim().is_empty() {
            return Err(SFError::Validation(
                "a dataset needs a name and a version: a row without either is a row nothing can \
                 ask for"
                    .into(),
            ));
        }
        if (entry.coverage_from.is_none()) != (entry.coverage_to.is_none()) {
            return Err(SFError::Validation(
                "a dataset's coverage is an interval: one bound alone does not say when the data \
                 stops or starts"
                    .into(),
            ));
        }
        // `DO NOTHING` rather than `DO UPDATE`: the conflict is the interesting
        // case, and an update would silently replace the digest of a version a
        // consumer may already have restored.
        sqlx::query(INSERT_SQL)
            .bind(&entry.name)
            .bind(&entry.version)
            .bind(&entry.object_key)
            .bind(&entry.content_sha256)
            .bind(entry.bytes)
            .bind(entry.row_count)
            .bind(entry.coverage_from)
            .bind(entry.coverage_to)
            .bind(&entry.source)
            .bind(&entry.media_type)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("register a dataset version: {e}")))?;

        let held = self
            .find(&entry.name, &entry.version)
            .await?
            .ok_or_else(|| {
                SFError::Database(
                    "a dataset version vanished between its insert and its read".into(),
                )
            })?;
        if held.content_sha256 != entry.content_sha256 {
            return Err(SFError::Validation(format!(
                "dataset {} version {} already holds different bytes ({} vs {}): a version that \
                 means two things cannot be restored from",
                entry.name, entry.version, held.content_sha256, entry.content_sha256
            )));
        }
        Ok(held)
    }

    async fn find(&self, name: &str, version: &str) -> SFResult<Option<DatasetEntry>> {
        let sql = format!("SELECT {SELECT_COLUMNS} FROM datasets WHERE name = $1 AND version = $2");
        let row = sqlx::query(&sql)
            .bind(name)
            .bind(version)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read a dataset version: {e}")))?;
        row.as_ref().map(Self::entry_from).transpose()
    }

    async fn latest(&self, name: &str) -> SFResult<Option<DatasetEntry>> {
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM datasets WHERE name = $1 \
             ORDER BY created_at DESC, id DESC LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("read the newest dataset version: {e}")))?;
        row.as_ref().map(Self::entry_from).transpose()
    }

    async fn list(&self, name: &str, limit: usize) -> SFResult<Vec<DatasetEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM datasets WHERE name = $1 \
             ORDER BY created_at DESC, id DESC LIMIT $2"
        );
        let rows = sqlx::query(&sql)
            .bind(name)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("list a dataset's versions: {e}")))?;
        rows.iter().map(Self::entry_from).collect()
    }
}
