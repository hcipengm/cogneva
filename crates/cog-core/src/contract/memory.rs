use crate::{storage::SparseEmbedding, SFResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ─── Types ───────────────────────────────────────────────────────────

/// A pointer back to the raw source that produced a schema or summary entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceRef {
    pub raw_uri: String,
    pub range: Option<String>,
    pub extractor_version: String,
    pub extracted_at: DateTime<Utc>,
}

impl SourceRef {
    pub fn new(raw_uri: impl Into<String>, extractor_version: impl Into<String>) -> Self {
        Self {
            raw_uri: raw_uri.into(),
            range: None,
            extractor_version: extractor_version.into(),
            extracted_at: Utc::now(),
        }
    }

    pub fn with_range(mut self, range: impl Into<String>) -> Self {
        self.range = Some(range.into());
        self
    }
}

/// Layer 0 — Raw Sources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawSource {
    pub id: String,
    pub namespace: String,
    pub content_type: String,
    pub payload: Vec<u8>,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub archived_at: DateTime<Utc>,
}

impl RawSource {
    pub fn new(
        id: impl Into<String>,
        namespace: impl Into<String>,
        content_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            namespace: namespace.into(),
            content_type: content_type.into(),
            payload,
            tags: Vec::new(),
            created_at: now,
            archived_at: now,
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = created_at;
        self
    }
}

/// Direction of a graph relationship traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationDirection {
    From,
    To,
    Both,
}

/// Types of structured entries in the Schema layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaKind {
    Entity,
    Relation,
    Event,
    Sentiment,
    Learning,
    ErrorPattern,
    Custom,
    SkillEffectiveness,
    ModeDecision,
    DiscoveryResult,
}

/// Layer 1 — Schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaEntry {
    pub id: String,
    pub namespace: String,
    pub kind: SchemaKind,
    pub name: String,
    pub key: String,
    pub properties: serde_json::Value,
    pub source_ref: SourceRef,
    pub confidence: f32,
    pub importance: f32,
    pub extracted_at: DateTime<Utc>,
}

impl SchemaEntry {
    pub fn new(
        id: impl Into<String>,
        namespace: impl Into<String>,
        kind: SchemaKind,
        name: impl Into<String>,
        key: impl Into<String>,
        source_ref: SourceRef,
    ) -> Self {
        Self {
            id: id.into(),
            namespace: namespace.into(),
            kind,
            name: name.into(),
            key: key.into(),
            properties: serde_json::Value::Object(Default::default()),
            source_ref,
            confidence: 1.0,
            importance: 0.5,
            extracted_at: Utc::now(),
        }
    }

    pub fn with_properties(mut self, properties: serde_json::Value) -> Self {
        self.properties = properties;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_importance(mut self, importance: f32) -> Self {
        self.importance = importance.clamp(0.0, 1.0);
        self
    }
}

/// Layer 2 — Summary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryEntry {
    pub id: String,
    pub namespace: String,
    pub text: String,
    pub embedding: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sparse_embedding: Option<SparseEmbedding>,
    pub embedding_model: String,
    pub source_ref: SourceRef,
    pub related_schema_ids: Vec<String>,
    pub confidence: f32,
    pub importance: f32,
    pub generated_at: DateTime<Utc>,
}

impl SummaryEntry {
    pub fn new(
        id: impl Into<String>,
        namespace: impl Into<String>,
        text: impl Into<String>,
        embedding: Vec<f32>,
        embedding_model: impl Into<String>,
        source_ref: SourceRef,
    ) -> Self {
        Self {
            id: id.into(),
            namespace: namespace.into(),
            text: text.into(),
            embedding,
            sparse_embedding: None,
            embedding_model: embedding_model.into(),
            source_ref,
            related_schema_ids: Vec::new(),
            confidence: 1.0,
            importance: 0.5,
            generated_at: Utc::now(),
        }
    }

    pub fn with_sparse_embedding(mut self, sparse: SparseEmbedding) -> Self {
        self.sparse_embedding = Some(sparse);
        self
    }

    pub fn with_related_schema_ids(mut self, ids: Vec<String>) -> Self {
        self.related_schema_ids = ids;
        self
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    pub fn with_importance(mut self, importance: f32) -> Self {
        self.importance = importance.clamp(0.0, 1.0);
        self
    }
}

/// The result of a schema search query.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaSearchResult {
    pub entry: SchemaEntry,
    pub score: f32,
}

/// The type of match that produced a search result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MatchType {
    #[default]
    Dense,
    Sparse,
    Hybrid,
    Rerank,
}

/// The result of a summary (semantic) search query.
#[derive(Debug, Clone, PartialEq)]
pub struct SummarySearchResult {
    pub entry: SummaryEntry,
    pub score: f32,
    pub match_type: MatchType,
    pub highlights: Vec<String>,
}

impl SummarySearchResult {
    pub fn new(entry: SummaryEntry, score: f32) -> Self {
        Self {
            entry,
            score,
            match_type: MatchType::default(),
            highlights: Vec::new(),
        }
    }

    pub fn with_match_type(mut self, match_type: MatchType) -> Self {
        self.match_type = match_type;
        self
    }

    pub fn with_highlights(mut self, highlights: Vec<String>) -> Self {
        self.highlights = highlights;
        self
    }
}

/// Report produced by a memory-decay pass.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DecayReport {
    pub namespace: String,
    pub entries_decayed: usize,
    pub entries_archived: usize,
}

/// A unified search result that can represent any memory layer entry.
#[derive(Debug, Clone, PartialEq)]
pub enum UnifiedSearchResult {
    Schema(SchemaSearchResult),
    Summary(SummarySearchResult),
}

/// Counters for memory-backend operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MemoryMetrics {
    pub raw_archived: u64,
    pub raw_retrieved: u64,
    pub schema_stored: u64,
    pub schema_updated: u64,
    pub schema_searched: u64,
    pub summary_stored: u64,
    pub summary_updated: u64,
    pub summary_searched: u64,
}

// ─── MemoryBackend trait ─────────────────────────────────────────────

/// Unified backend for the three-layer permanent memory architecture.
#[async_trait]
pub trait MemoryBackend: Send + Sync {
    // ── Layer 0: Raw Sources ────────────────────────────────────────────
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String>;
    async fn get_raw(&self, namespace: &str, id: &str) -> SFResult<Option<RawSource>>;
    async fn list_raw(
        &self,
        namespace: &str,
        content_type_prefix: Option<&str>,
    ) -> SFResult<Vec<String>>;
    async fn delete_raw(&self, namespace: &str, id: &str) -> SFResult<()>;

    // ── Layer 1: Schema ─────────────────────────────────────────────────
    async fn store_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;
    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>>;
    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>>;
    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>>;
    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>>;
    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()>;
    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>>;
    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;

    // ── Layer 2: Summary ────────────────────────────────────────────────
    async fn store_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;
    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>>;
    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>>;
    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let _ = query_sparse;
        self.search_summary(namespace, query_dense, top_k, time_range)
            .await
    }
    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>>;
    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>>;
    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()>;
    async fn update_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;
    fn metrics(&self) -> MemoryMetrics;
    async fn health_check(&self) -> SFResult<()>;
    async fn search_all(
        &self,
        namespace: &str,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<UnifiedSearchResult>>;
    async fn ingest_explicit(
        &self,
        namespace: &str,
        text: &str,
        importance: f32,
        tags: Vec<String>,
    ) -> SFResult<()>;
    async fn forget(&self, namespace: &str, id: &str) -> SFResult<()>;
    async fn decay(
        &self,
        namespace: &str,
        age_threshold_secs: u64,
        importance_threshold: f32,
    ) -> SFResult<DecayReport>;
}

/// Trait for ingesting raw sources into structured schema and summary layers.
/// Implementations may use rule-based extraction, LLM-assisted parsing,
/// or a hybrid approach. The gateway consumes this via `PluginContext`
/// so it never depends directly on `cog-memory` concrete types.
#[async_trait]
pub trait MemoryIngestor: Send + Sync {
    /// Run extraction against a raw source and return both layers.
    async fn ingest(&self, source: &RawSource) -> SFResult<(Vec<SchemaEntry>, SummaryEntry)>;
}

/// Trait for extracting structured schema and summary from raw sources.
/// This is the lower-level trait used by `MemoryIngestor` implementations.
/// The gateway consumes this via `PluginContext`
/// so it never depends directly on `cog-memory` concrete types.
#[async_trait]
pub trait MemoryExtractor: Send + Sync {
    /// Extract structured schema entries from a raw source.
    async fn extract_schema(&self, source: &RawSource) -> SFResult<Vec<SchemaEntry>>;

    /// Generate a semantic summary entry from a raw source.
    async fn generate_summary(&self, source: &RawSource) -> SFResult<SummaryEntry>;
}

// ─── SchemaBackend / SummaryBackend (migrated from cog-memory) ───────────

/// Pluggable backend for the Schema layer (Layer 1) of permanent memory.
/// Implementations may store entries in PostgreSQL, TDSQL-PG, an in-memory
/// HashMap, or any other relational/graph database.
#[async_trait]
pub trait SchemaBackend: Send + Sync {
    /// Store a schema entry.
    async fn store_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;

    /// Retrieve a schema entry by id.
    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>>;

    /// Search schema entries by name/key substring.
    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>>;

    /// Find schema entries that point to a given raw source id.
    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>>;

    /// List all schema entries.
    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>>;

    /// Delete a schema entry by id.
    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()>;

    /// Query relation entries by direction and optional relation type.
    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>>;

    /// Update a schema entry, merging properties if the key already exists.
    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()>;
}

/// Pluggable backend for the Summary layer (Layer 2) of permanent memory.
/// Implementations may store summaries (and their embeddings) in LanceDB,
/// Tencent VDB, or any [`crate::VectorBackend`] wrapped by a vector adapter.
#[async_trait]
pub trait SummaryBackend: Send + Sync {
    /// Store a summary entry, indexing its embedding for similarity search.
    async fn store_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;

    /// Retrieve a summary entry by id.
    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>>;

    /// Semantic search over summaries using a query embedding vector.
    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>>;

    /// Hybrid search over summaries using both dense and sparse vectors.
    /// Default implementation falls back to dense-only [`search_summary`].
    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let _ = query_sparse;
        self.search_summary(namespace, query_dense, top_k, time_range)
            .await
    }

    /// Find summary entries that point to a given raw source id.
    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>>;

    /// List all summary entries.
    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>>;

    /// Delete a summary entry by id.
    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()>;

    /// Update a summary entry, overwriting if the id already exists.
    async fn update_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()>;
}
