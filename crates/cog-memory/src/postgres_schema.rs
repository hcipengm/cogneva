//! PostgreSQL-backed implementation of [`SchemaBackend`].
//! This is the production-grade Layer 1 (Schema) backend.  It stores
//! [`SchemaEntry`] rows in a single `schema_entries` table with a JSONB
//! `properties` column for arbitrary structured data.
//! The table shape lives in [`SCHEMA_ENTRIES_DDL`] alone, so that the columns
//! a statement may bind and the columns the table actually has cannot drift
//! apart in two places.  Call [`PostgresSchemaBackend::init_table`] at startup
//! to apply it idempotently.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use std::time::Duration;

use cog_core::{merge_schema_observation, SchemaBackend};
use cog_core::{SFError, SFResult};
use cog_core::{SchemaEntry, SchemaKind, SchemaSearchResult, SourceRef};

/// SQL DDL applied by [`PostgresSchemaBackend::init_table`].
pub const SCHEMA_ENTRIES_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_entries (
    id                TEXT        PRIMARY KEY,
    namespace         TEXT        NOT NULL DEFAULT 'default',
    kind              TEXT        NOT NULL,
    name              TEXT        NOT NULL,
    key               TEXT        NOT NULL,
    properties        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    raw_uri           TEXT        NOT NULL,
    range_spec        TEXT,
    extractor_version TEXT        NOT NULL,
    extracted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confidence        REAL        NOT NULL DEFAULT 1.0,
    importance        REAL        NOT NULL DEFAULT 0.5,
    observed_by       TEXT[]      NOT NULL DEFAULT '{}',
    occurrences       BIGINT      NOT NULL DEFAULT 1,
    first_seen        TIMESTAMPTZ,
    last_seen         TIMESTAMPTZ
);
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS observed_by TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS occurrences BIGINT NOT NULL DEFAULT 1;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS first_seen  TIMESTAMPTZ;
ALTER TABLE schema_entries ADD COLUMN IF NOT EXISTS last_seen   TIMESTAMPTZ;

-- Rows written before the observer list existed name their one origin in
-- `raw_uri`. Seeding the list from that column is what keeps them answerable
-- by `schema_for_raw`: an entry whose list stays empty looks unextracted, and
-- the reconciliation pass would extract every one of them again on each sweep.
-- Idempotent, and repeated at every startup so a stale writer's rows are
-- picked up too.
UPDATE schema_entries SET observed_by = ARRAY[raw_uri] WHERE cardinality(observed_by) = 0;
UPDATE schema_entries SET first_seen = extracted_at WHERE first_seen IS NULL;
UPDATE schema_entries SET last_seen  = extracted_at WHERE last_seen  IS NULL;
ALTER TABLE schema_entries ALTER COLUMN first_seen SET NOT NULL;
ALTER TABLE schema_entries ALTER COLUMN last_seen  SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_schema_entries_namespace ON schema_entries(namespace);
CREATE INDEX IF NOT EXISTS idx_schema_entries_key       ON schema_entries(namespace, key);
CREATE INDEX IF NOT EXISTS idx_schema_entries_name_lower ON schema_entries(namespace, LOWER(name));
CREATE INDEX IF NOT EXISTS idx_schema_entries_raw_uri   ON schema_entries(namespace, raw_uri);
CREATE INDEX IF NOT EXISTS idx_schema_entries_kind      ON schema_entries(namespace, kind);
CREATE INDEX IF NOT EXISTS idx_schema_entries_observed_by ON schema_entries USING GIN (observed_by);
"#;

/// Columns every `SELECT` in this module returns, in the order
/// [`PostgresSchemaBackend::row_to_entry`] reads them.
const ENTRY_COLUMNS: &str = "namespace, id, kind, name, key, properties, raw_uri, range_spec, \
                             extractor_version, extracted_at, confidence, importance, \
                             observed_by, occurrences, first_seen, last_seen";

/// PostgreSQL-backed Schema layer.
/// Wraps a shared [`PgPool`] so callers can reuse an existing connection pool
/// (e.g. the one inside `cog_adapters::PostgresAdapter`).
pub struct PostgresSchemaBackend {
    pool: PgPool,
}

impl PostgresSchemaBackend {
    /// Build a backend on top of an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connect to a PostgreSQL DSN with sensible default pool sizing.
    pub async fn connect(dsn: impl AsRef<str>) -> SFResult<Self> {
        Self::connect_with_options(dsn, 16, 1, 10, 600).await
    }

    /// Connect with explicit pool options.
    pub async fn connect_with_options(
        dsn: impl AsRef<str>,
        max_connections: u32,
        min_connections: u32,
        acquire_timeout_secs: u64,
        idle_timeout_secs: u64,
    ) -> SFResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .idle_timeout(Some(Duration::from_secs(idle_timeout_secs)))
            .connect(dsn.as_ref())
            .await
            .map_err(|e| SFError::Agent(format!("PostgresSchemaBackend connect failed: {}", e)))?;
        Ok(Self { pool })
    }

    /// Borrow the underlying pool (useful for sharing it with other components).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the table + index DDL.  Safe to call repeatedly (uses
    /// `CREATE ... IF NOT EXISTS`).
    pub async fn init_table(&self) -> SFResult<()> {
        sqlx::raw_sql(SCHEMA_ENTRIES_DDL)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SFError::Agent(format!("PostgresSchemaBackend init_table failed: {}", e))
            })?;
        Ok(())
    }

    fn row_to_entry(row: &PgRow) -> SFResult<SchemaEntry> {
        let namespace: String = row
            .try_get("namespace")
            .map_err(|e| SFError::Agent(format!("decode namespace: {}", e)))?;
        let kind_str: String = row
            .try_get("kind")
            .map_err(|e| SFError::Agent(format!("decode kind: {}", e)))?;
        let kind = parse_schema_kind(&kind_str)?;

        let raw_uri: String = row
            .try_get("raw_uri")
            .map_err(|e| SFError::Agent(format!("decode raw_uri: {}", e)))?;
        let range_spec: Option<String> = row
            .try_get("range_spec")
            .map_err(|e| SFError::Agent(format!("decode range_spec: {}", e)))?;
        let extractor_version: String = row
            .try_get("extractor_version")
            .map_err(|e| SFError::Agent(format!("decode extractor_version: {}", e)))?;
        let extracted_at: DateTime<Utc> = row
            .try_get("extracted_at")
            .map_err(|e| SFError::Agent(format!("decode extracted_at: {}", e)))?;
        let confidence: f32 = row
            .try_get("confidence")
            .map_err(|e| SFError::Agent(format!("decode confidence: {}", e)))?;
        let importance: f32 = row
            .try_get("importance")
            .map_err(|e| SFError::Agent(format!("decode importance: {}", e)))?;
        let properties: serde_json::Value = row
            .try_get("properties")
            .map_err(|e| SFError::Agent(format!("decode properties: {}", e)))?;
        let id: String = row
            .try_get("id")
            .map_err(|e| SFError::Agent(format!("decode id: {}", e)))?;
        let name: String = row
            .try_get("name")
            .map_err(|e| SFError::Agent(format!("decode name: {}", e)))?;
        let key: String = row
            .try_get("key")
            .map_err(|e| SFError::Agent(format!("decode key: {}", e)))?;
        let observed_by: Vec<String> = row
            .try_get("observed_by")
            .map_err(|e| SFError::Agent(format!("decode observed_by: {}", e)))?;
        let occurrences: i64 = row
            .try_get("occurrences")
            .map_err(|e| SFError::Agent(format!("decode occurrences: {}", e)))?;
        let first_seen: DateTime<Utc> = row
            .try_get("first_seen")
            .map_err(|e| SFError::Agent(format!("decode first_seen: {}", e)))?;
        let last_seen: DateTime<Utc> = row
            .try_get("last_seen")
            .map_err(|e| SFError::Agent(format!("decode last_seen: {}", e)))?;

        Ok(SchemaEntry {
            id,
            namespace,
            kind,
            name,
            key,
            properties,
            source_ref: SourceRef {
                raw_uri,
                range: range_spec,
                extractor_version,
                extracted_at,
            },
            observed_by,
            occurrences: occurrences.max(0) as u64,
            first_seen,
            last_seen,
            confidence,
            importance,
            extracted_at,
        })
    }
}

/// Convert a [`SchemaKind`] to its canonical lowercase string form.
fn schema_kind_str(kind: SchemaKind) -> &'static str {
    kind.as_str()
}

/// Parse a [`SchemaKind`] from its canonical lowercase string form.
fn parse_schema_kind(s: &str) -> SFResult<SchemaKind> {
    match s {
        "entity" => Ok(SchemaKind::Entity),
        "relation" => Ok(SchemaKind::Relation),
        "event" => Ok(SchemaKind::Event),
        "sentiment" => Ok(SchemaKind::Sentiment),
        "learning" => Ok(SchemaKind::Learning),
        "error_pattern" => Ok(SchemaKind::ErrorPattern),
        "custom" => Ok(SchemaKind::Custom),
        "skill_effectiveness" => Ok(SchemaKind::SkillEffectiveness),
        "mode_decision" => Ok(SchemaKind::ModeDecision),
        "discovery_result" => Ok(SchemaKind::DiscoveryResult),
        other => Err(SFError::Agent(format!("unknown schema kind: {}", other))),
    }
}

#[async_trait]
impl SchemaBackend for PostgresSchemaBackend {
    /// Write one observation of a fact, merging it into the row that identity
    /// already names.
    ///
    /// The read and the write have to see the same row or a concurrent store
    /// of the same identity loses one of the two observations, so the merge
    /// runs inside a transaction on a locked row. The `DO NOTHING` insert is
    /// what puts a row there to lock when none existed: two inserters racing
    /// on a fresh identity both come out of it, one having inserted and one
    /// not, and the loser then merges into the winner's row instead of failing
    /// on the primary key. Merging the just-inserted row with itself is a
    /// no-op, which is what makes one code path serve both cases.
    async fn store_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Agent(format!("store_schema begin: {}", e)))?;

        sqlx::query(
            r#"
            INSERT INTO schema_entries
                (id, namespace, kind, name, key, properties, raw_uri, range_spec,
                 extractor_version, extracted_at, confidence, importance,
                 observed_by, occurrences, first_seen, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&entry.id)
        .bind(&entry.namespace)
        .bind(schema_kind_str(entry.kind))
        .bind(&entry.name)
        .bind(&entry.key)
        .bind(&entry.properties)
        .bind(&entry.source_ref.raw_uri)
        .bind(&entry.source_ref.range)
        .bind(&entry.source_ref.extractor_version)
        .bind(entry.extracted_at)
        .bind(entry.confidence)
        .bind(entry.importance)
        .bind(&entry.observed_by)
        .bind(entry.occurrences as i64)
        .bind(entry.first_seen)
        .bind(entry.last_seen)
        .execute(&mut *tx)
        .await
        .map_err(|e| SFError::Agent(format!("store_schema insert: {}", e)))?;

        let select = format!("SELECT {ENTRY_COLUMNS} FROM schema_entries WHERE id = $1 FOR UPDATE");
        let stored = sqlx::query(&select)
            .bind(&entry.id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| SFError::Agent(format!("store_schema lock: {}", e)))?;
        let merged = merge_schema_observation(&Self::row_to_entry(&stored)?, entry);

        sqlx::query(
            r#"
            UPDATE schema_entries SET
                namespace = $2, kind = $3, name = $4, key = $5, properties = $6,
                raw_uri = $7, range_spec = $8, extractor_version = $9,
                extracted_at = $10, confidence = $11, importance = $12,
                observed_by = $13, occurrences = $14, first_seen = $15, last_seen = $16
            WHERE id = $1
            "#,
        )
        .bind(&merged.id)
        .bind(&merged.namespace)
        .bind(schema_kind_str(merged.kind))
        .bind(&merged.name)
        .bind(&merged.key)
        .bind(&merged.properties)
        .bind(&merged.source_ref.raw_uri)
        .bind(&merged.source_ref.range)
        .bind(&merged.source_ref.extractor_version)
        .bind(merged.extracted_at)
        .bind(merged.confidence)
        .bind(merged.importance)
        .bind(&merged.observed_by)
        .bind(merged.occurrences as i64)
        .bind(merged.first_seen)
        .bind(merged.last_seen)
        .execute(&mut *tx)
        .await
        .map_err(|e| SFError::Agent(format!("store_schema update: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| SFError::Agent(format!("store_schema commit: {}", e)))?;
        Ok(())
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        let sql =
            format!("SELECT {ENTRY_COLUMNS} FROM schema_entries WHERE id = $1 AND namespace = $2");
        let row = sqlx::query(&sql)
            .bind(id)
            .bind(namespace)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("get_schema failed: {}", e)))?;

        match row {
            Some(r) => Ok(Some(Self::row_to_entry(&r)?)),
            None => Ok(None),
        }
    }

    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>> {
        let pattern = format!("%{}%", query);
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM schema_entries \
             WHERE namespace = $1 AND (name ILIKE $2 OR key ILIKE $2) \
             LIMIT $3"
        );
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .bind(&pattern)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("search_schema failed: {}", e)))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let entry = Self::row_to_entry(row)?;
            results.push(SchemaSearchResult { entry, score: 1.0 });
        }
        Ok(results)
    }

    /// Membership over the observer list, not equality against the single
    /// `raw_uri`: an entry merged from several sources belongs to each of
    /// them, and matching on the column would answer "nothing extracted" for
    /// every source but the one that wrote last — which is what drives the
    /// reconciliation pass to re-extract them over and over.
    ///
    /// The `raw_uri` arm is the same union [`SchemaEntry::observed_from`]
    /// applies, restated in SQL so a row and a decoded entry answer the same
    /// question the same way: `source_ref` names an origin whether or not the
    /// list still carries it.
    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        let raw_uri = format!("memory://{}", raw_id);
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM schema_entries \
             WHERE namespace = $1 AND ($2 = ANY(observed_by) OR raw_uri = $2)"
        );
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .bind(&raw_uri)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("schema_for_raw failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }
        Ok(entries)
    }

    /// Withdraw one origin from an entry, atomically, deleting the row only
    /// when that was the last origin.
    ///
    /// A read-modify-write in the caller would race another writer's merge on
    /// the same row: the merge would put the origin back and the deletion
    /// would be undone silently. Both outcomes are decided here from one
    /// snapshot of the row.
    async fn forget_schema_source(&self, namespace: &str, id: &str, raw_uri: &str) -> SFResult<()> {
        let sql = format!(
            "SELECT {ENTRY_COLUMNS} FROM schema_entries \
             WHERE namespace = $1 AND id = $2 FOR UPDATE"
        );
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| SFError::Agent(format!("forget_schema_source begin: {}", e)))?;
        let row = sqlx::query(&sql)
            .bind(namespace)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| SFError::Agent(format!("forget_schema_source lock: {}", e)))?;

        let Some(row) = row else {
            return Ok(());
        };
        match Self::row_to_entry(&row)?.without_observer(raw_uri) {
            Some(entry) => {
                sqlx::query(
                    "UPDATE schema_entries SET raw_uri = $2, observed_by = $3 WHERE id = $1",
                )
                .bind(id)
                .bind(&entry.source_ref.raw_uri)
                .bind(&entry.observed_by)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    SFError::Agent(format!("forget_schema_source update failed: {}", e))
                })?;
            }
            None => {
                sqlx::query("DELETE FROM schema_entries WHERE namespace = $1 AND id = $2")
                    .bind(namespace)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        SFError::Agent(format!("forget_schema_source delete failed: {}", e))
                    })?;
            }
        }
        tx.commit()
            .await
            .map_err(|e| SFError::Agent(format!("forget_schema_source commit: {}", e)))?;
        Ok(())
    }

    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>> {
        let sql = format!("SELECT {ENTRY_COLUMNS} FROM schema_entries WHERE namespace = $1");
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("list_schema failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }
        Ok(entries)
    }

    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()> {
        sqlx::query("DELETE FROM schema_entries WHERE namespace = $1 AND id = $2")
            .bind(namespace)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("delete_schema failed: {}", e)))?;
        Ok(())
    }

    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: cog_core::RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>> {
        let direction_str = match direction {
            cog_core::RelationDirection::From => "from",
            cog_core::RelationDirection::To => "to",
            cog_core::RelationDirection::Both => "both",
        };
        let rows = sqlx::query(
            r#"
            SELECT namespace, id, kind, name, key, properties, raw_uri, range_spec,
                   extractor_version, extracted_at, confidence, importance
            FROM schema_entries
            WHERE namespace = $1 AND kind = 'relation'
              AND (
                  ($2 = 'from' AND properties ->> 'from' = $3)
                  OR ($2 = 'to' AND properties ->> 'to' = $3)
                  OR ($2 = 'both' AND (properties ->> 'from' = $3 OR properties ->> 'to' = $3))
              )
              AND ($4::TEXT IS NULL OR properties ->> 'relation_type' = $4)
            "#,
        )
        .bind(namespace)
        .bind(direction_str)
        .bind(entity)
        .bind(relation_type)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("query_relations failed: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }
        Ok(entries)
    }

    /// Write the entry as given, merging only its properties.
    ///
    /// The conflict target is the primary key, which is the identity the
    /// caller selected by. A conflict target of `(namespace, key)` would need
    /// a unique index on those columns that does not exist — the statement
    /// would fail outright rather than update anything.
    async fn update_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO schema_entries
                (id, namespace, kind, name, key, properties, raw_uri, range_spec,
                 extractor_version, extracted_at, confidence, importance,
                 observed_by, occurrences, first_seen, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (id) DO UPDATE SET
                namespace         = EXCLUDED.namespace,
                kind              = EXCLUDED.kind,
                name              = EXCLUDED.name,
                key               = EXCLUDED.key,
                properties        = COALESCE(schema_entries.properties, '{}'::jsonb) || EXCLUDED.properties,
                raw_uri           = EXCLUDED.raw_uri,
                range_spec        = EXCLUDED.range_spec,
                extractor_version = EXCLUDED.extractor_version,
                extracted_at      = EXCLUDED.extracted_at,
                confidence        = EXCLUDED.confidence,
                importance        = EXCLUDED.importance,
                observed_by       = EXCLUDED.observed_by,
                occurrences       = EXCLUDED.occurrences,
                first_seen        = EXCLUDED.first_seen,
                last_seen         = EXCLUDED.last_seen
            "#,
        )
        .bind(&entry.id)
        .bind(&entry.namespace)
        .bind(schema_kind_str(entry.kind))
        .bind(&entry.name)
        .bind(&entry.key)
        .bind(&entry.properties)
        .bind(&entry.source_ref.raw_uri)
        .bind(&entry.source_ref.range)
        .bind(&entry.source_ref.extractor_version)
        .bind(entry.extracted_at)
        .bind(entry.confidence)
        .bind(entry.importance)
        .bind(&entry.observed_by)
        .bind(entry.occurrences as i64)
        .bind(entry.first_seen)
        .bind(entry.last_seen)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("update_schema failed: {}", e)))?;
        Ok(())
    }
}
