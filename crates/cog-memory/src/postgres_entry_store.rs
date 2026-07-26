//! PostgreSQL-backed [`SummaryEntryStore`].
//! Stores the full typed [`SummaryEntry`] (including the embedding backup)
//! in PostgreSQL, but **does not perform vector search itself**.
//! Vector indexing and ANN retrieval are delegated to a separate
//! [`cog_core::VectorBackend`] (e.g. Qdrant) via [`VectorSummaryBackend`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgRow};
use sqlx::Row;

use crate::SummaryEntryStore;
use cog_core::{SFError, SFResult};
use cog_core::{SourceRef, SummaryEntry};

/// SQL DDL applied by [`PostgresEntryStore::init_table`].
pub const SUMMARY_ENTRIES_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS summary_entries (
    id                TEXT        PRIMARY KEY,
    namespace         TEXT        NOT NULL DEFAULT 'default',
    text              TEXT        NOT NULL,
    embedding         REAL[]      NOT NULL,
    sparse_embedding  JSONB,
    embedding_model   TEXT        NOT NULL DEFAULT '',
    raw_uri           TEXT        NOT NULL,
    range_spec        TEXT,
    extractor_version TEXT        NOT NULL,
    extracted_at      TIMESTAMPTZ NOT NULL,
    confidence        REAL        NOT NULL DEFAULT 1.0,
    importance        REAL        NOT NULL DEFAULT 0.5,
    related_schema_ids JSONB
);
CREATE INDEX IF NOT EXISTS idx_summary_entries_ns_raw_uri ON summary_entries(namespace, raw_uri);
"#;

/// PostgreSQL-backed entry store for the Summary layer.
/// Wraps a shared [`PgPool`].  Call [`init_table`] once at startup.
pub struct PostgresEntryStore {
    pool: PgPool,
}

impl PostgresEntryStore {
    /// Build a store on top of an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the table + index DDL idempotently.
    pub async fn init_table(&self) -> SFResult<()> {
        sqlx::raw_sql(SUMMARY_ENTRIES_DDL)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("PostgresEntryStore init_table failed: {}", e)))?;
        Ok(())
    }

    fn row_to_entry(row: &PgRow) -> SFResult<SummaryEntry> {
        let id: String = row
            .try_get("id")
            .map_err(|e| SFError::Agent(format!("decode id: {}", e)))?;
        let namespace: String = row
            .try_get("namespace")
            .map_err(|e| SFError::Agent(format!("decode namespace: {}", e)))?;
        let text: String = row
            .try_get("text")
            .map_err(|e| SFError::Agent(format!("decode text: {}", e)))?;
        let embedding: Vec<f32> = row
            .try_get("embedding")
            .map_err(|e| SFError::Agent(format!("decode embedding: {}", e)))?;
        let sparse_embedding: Option<cog_core::SparseEmbedding> = row
            .try_get::<Option<serde_json::Value>, _>("sparse_embedding")
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok());
        let embedding_model: String = row
            .try_get("embedding_model")
            .map_err(|e| SFError::Agent(format!("decode embedding_model: {}", e)))?;
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
        let related_schema_ids: Vec<String> = row
            .try_get::<Option<serde_json::Value>, _>("related_schema_ids")
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        Ok(SummaryEntry {
            id,
            namespace,
            text,
            embedding,
            sparse_embedding,
            embedding_model,
            source_ref: SourceRef {
                raw_uri,
                range: range_spec,
                extractor_version,
                extracted_at,
            },
            related_schema_ids,
            confidence,
            importance,
            generated_at: extracted_at,
        })
    }
}

#[async_trait]
impl SummaryEntryStore for PostgresEntryStore {
    fn is_durable(&self) -> bool {
        true
    }

    async fn get(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
        let row = sqlx::query(
            "SELECT id, namespace, text, embedding, sparse_embedding, embedding_model, \
                    raw_uri, range_spec, extractor_version, extracted_at, \
                    confidence, importance, related_schema_ids \
             FROM summary_entries \
             WHERE id = $1 AND namespace = $2",
        )
        .bind(id)
        .bind(namespace)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("entry_store get failed: {}", e)))?;

        match row {
            Some(r) => Ok(Some(Self::row_to_entry(&r)?)),
            None => Ok(None),
        }
    }

    async fn get_many(&self, namespace: &str, ids: &[String]) -> SFResult<Vec<SummaryEntry>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // sqlx 不支持 Vec<String> 直接 bind，用 unnest
        let rows = sqlx::query(
            "SELECT id, namespace, text, embedding, sparse_embedding, embedding_model, \
                    raw_uri, range_spec, extractor_version, extracted_at, \
                    confidence, importance, related_schema_ids \
             FROM summary_entries \
             WHERE namespace = $1 AND id = ANY($2)",
        )
        .bind(namespace)
        .bind(ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("entry_store get_many failed: {}", e)))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(Self::row_to_entry(row)?);
        }
        Ok(out)
    }

    async fn list(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
        let rows = sqlx::query(
            "SELECT id, namespace, text, embedding, sparse_embedding, embedding_model, \
                    raw_uri, range_spec, extractor_version, extracted_at, \
                    confidence, importance, related_schema_ids \
             FROM summary_entries \
             WHERE namespace = $1",
        )
        .bind(namespace)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("entry_store list failed: {}", e)))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(Self::row_to_entry(row)?);
        }
        Ok(out)
    }

    async fn list_by_raw_uri(&self, namespace: &str, raw_uri: &str) -> SFResult<Vec<SummaryEntry>> {
        let rows = sqlx::query(
            "SELECT id, namespace, text, embedding, sparse_embedding, embedding_model, \
                    raw_uri, range_spec, extractor_version, extracted_at, \
                    confidence, importance, related_schema_ids \
             FROM summary_entries \
             WHERE namespace = $1 AND raw_uri = $2",
        )
        .bind(namespace)
        .bind(raw_uri)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("entry_store list_by_raw_uri failed: {}", e)))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(Self::row_to_entry(row)?);
        }
        Ok(out)
    }

    async fn upsert(&self, entry: &SummaryEntry) -> SFResult<()> {
        let sparse_json = entry
            .sparse_embedding
            .as_ref()
            .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null));
        let related_json =
            serde_json::to_value(&entry.related_schema_ids).unwrap_or(serde_json::Value::Null);

        sqlx::query(
            r#"
            INSERT INTO summary_entries
                (id, namespace, text, embedding, sparse_embedding, embedding_model,
                 raw_uri, range_spec, extractor_version, extracted_at,
                 confidence, importance, related_schema_ids)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (id) DO UPDATE SET
                namespace         = EXCLUDED.namespace,
                text              = EXCLUDED.text,
                embedding         = EXCLUDED.embedding,
                sparse_embedding  = EXCLUDED.sparse_embedding,
                embedding_model   = EXCLUDED.embedding_model,
                raw_uri           = EXCLUDED.raw_uri,
                range_spec        = EXCLUDED.range_spec,
                extractor_version = EXCLUDED.extractor_version,
                extracted_at      = EXCLUDED.extracted_at,
                confidence        = EXCLUDED.confidence,
                importance        = EXCLUDED.importance,
                related_schema_ids = EXCLUDED.related_schema_ids
            "#,
        )
        .bind(&entry.id)
        .bind(&entry.namespace)
        .bind(&entry.text)
        .bind(&entry.embedding)
        .bind(sparse_json)
        .bind(&entry.embedding_model)
        .bind(&entry.source_ref.raw_uri)
        .bind(&entry.source_ref.range)
        .bind(&entry.source_ref.extractor_version)
        .bind(entry.source_ref.extracted_at)
        .bind(entry.confidence)
        .bind(entry.importance)
        .bind(related_json)
        .execute(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("entry_store upsert failed: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, namespace: &str, id: &str) -> SFResult<()> {
        sqlx::query("DELETE FROM summary_entries WHERE namespace = $1 AND id = $2")
            .bind(namespace)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Agent(format!("entry_store delete failed: {}", e)))?;
        Ok(())
    }

    async fn list_all(&self) -> SFResult<Vec<SummaryEntry>> {
        let rows = sqlx::query(
            "SELECT id, namespace, text, embedding, sparse_embedding, embedding_model, \
                    raw_uri, range_spec, extractor_version, extracted_at, \
                    confidence, importance, related_schema_ids \
             FROM summary_entries",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Agent(format!("entry_store list_all failed: {}", e)))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(Self::row_to_entry(row)?);
        }
        Ok(out)
    }
}
