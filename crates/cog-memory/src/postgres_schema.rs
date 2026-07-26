//! PostgreSQL-backed implementation of [`SchemaBackend`].
//! This is the production-grade Layer 1 (Schema) backend.  It stores
//! [`SchemaEntry`] rows in a single `schema_entries` table with a JSONB
//! `properties` column for arbitrary structured data.
//! ## DDL
//! ```sql
//! CREATE TABLE IF NOT EXISTS schema_entries (
//!     id                TEXT        PRIMARY KEY,
//!     kind              TEXT        NOT NULL,
//!     name              TEXT        NOT NULL,
//!     key               TEXT        NOT NULL,
//!     properties        JSONB       NOT NULL DEFAULT '{}'::jsonb,
//!     raw_uri           TEXT        NOT NULL,
//!     range_spec        TEXT,
//!     extractor_version TEXT        NOT NULL,
//!     extracted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     confidence        REAL        NOT NULL DEFAULT 1.0
//! );
//! CREATE INDEX IF NOT EXISTS idx_schema_entries_key ON schema_entries(key);
//! CREATE INDEX IF NOT EXISTS idx_schema_entries_name_lower ON schema_entries(LOWER(name));
//! CREATE INDEX IF NOT EXISTS idx_schema_entries_raw_uri ON schema_entries(raw_uri);
//! CREATE INDEX IF NOT EXISTS idx_schema_entries_kind ON schema_entries(kind);
//! ```
//! Call [`PostgresSchemaBackend::init_table`] at startup to apply the DDL
//! idempotently.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use std::time::Duration;

use cog_core::SchemaBackend;
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
    importance        REAL        NOT NULL DEFAULT 0.5
);
CREATE INDEX IF NOT EXISTS idx_schema_entries_namespace ON schema_entries(namespace);
CREATE INDEX IF NOT EXISTS idx_schema_entries_key       ON schema_entries(namespace, key);
CREATE INDEX IF NOT EXISTS idx_schema_entries_name_lower ON schema_entries(namespace, LOWER(name));
CREATE INDEX IF NOT EXISTS idx_schema_entries_raw_uri   ON schema_entries(namespace, raw_uri);
CREATE INDEX IF NOT EXISTS idx_schema_entries_kind      ON schema_entries(namespace, kind);
"#;

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
            confidence,
            importance,
            extracted_at,
        })
    }
}

/// Convert a [`SchemaKind`] to its canonical lowercase string form.
fn schema_kind_str(kind: SchemaKind) -> &'static str {
    match kind {
        SchemaKind::Entity => "entity",
        SchemaKind::Relation => "relation",
        SchemaKind::Event => "event",
        SchemaKind::Sentiment => "sentiment",
        SchemaKind::Learning => "learning",
        SchemaKind::ErrorPattern => "error_pattern",
        SchemaKind::Custom => "custom",
        SchemaKind::SkillEffectiveness => "skill_effectiveness",
        SchemaKind::ModeDecision => "mode_decision",
        SchemaKind::DiscoveryResult => "discovery_result",
    }
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
    async fn store_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO schema_entries
                (id, namespace, kind, name, key, properties, raw_uri, range_spec,
                 extractor_version, extracted_at, confidence, importance)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE SET
                namespace         = EXCLUDED.namespace,
                kind              = EXCLUDED.kind,
                name              = EXCLUDED.name,
                key               = EXCLUDED.key,
                properties        = EXCLUDED.properties,
                raw_uri           = EXCLUDED.raw_uri,
                range_spec        = EXCLUDED.range_spec,
                extractor_version = EXCLUDED.extractor_version,
                extracted_at      = EXCLUDED.extracted_at,
                confidence        = EXCLUDED.confidence,
                importance        = EXCLUDED.importance
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
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("store_schema failed: {}", e)))?;
        Ok(())
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        let row = sqlx::query(
            "SELECT namespace, id, kind, name, key, properties, raw_uri, range_spec,
                    extractor_version, extracted_at, confidence, importance
             FROM schema_entries
             WHERE id = $1 AND namespace = $2",
        )
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
        let rows = sqlx::query(
            "SELECT namespace, id, kind, name, key, properties, raw_uri, range_spec,
                    extractor_version, extracted_at, confidence, importance
             FROM schema_entries
             WHERE namespace = $1 AND (name ILIKE $2 OR key ILIKE $2)
             LIMIT $3",
        )
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

    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        let raw_uri = format!("memory://{}", raw_id);
        let rows = sqlx::query(
            "SELECT namespace, id, kind, name, key, properties, raw_uri, range_spec,
                    extractor_version, extracted_at, confidence, importance
             FROM schema_entries
             WHERE namespace = $1 AND raw_uri = $2",
        )
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

    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>> {
        let rows = sqlx::query(
            "SELECT namespace, id, kind, name, key, properties, raw_uri, range_spec,
                    extractor_version, extracted_at, confidence, importance
             FROM schema_entries
             WHERE namespace = $1",
        )
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

    async fn update_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        sqlx::query(
            r#"
            INSERT INTO schema_entries
                (id, namespace, kind, name, key, properties, raw_uri, range_spec,
                 extractor_version, extracted_at, confidence, importance)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (namespace, key) DO UPDATE SET
                properties        = COALESCE(schema_entries.properties, '{}'::jsonb) || EXCLUDED.properties,
                raw_uri           = EXCLUDED.raw_uri,
                range_spec        = EXCLUDED.range_spec,
                extractor_version = EXCLUDED.extractor_version,
                extracted_at      = EXCLUDED.extracted_at,
                confidence        = EXCLUDED.confidence,
                importance        = EXCLUDED.importance
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
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("update_schema failed: {}", e)))?;
        Ok(())
    }
}
