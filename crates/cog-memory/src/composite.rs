use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{MemorySchemaBackend, VectorSummaryBackend};
use chrono::{DateTime, Utc};
use cog_core::{
    DecayReport, MemoryBackend, MemoryMetrics, RawSource, SchemaEntry, SchemaSearchResult,
    SummaryEntry, SummarySearchResult, UnifiedSearchResult,
};
use cog_core::{ObjectBackend, SFError, SFResult};
use cog_core::{SchemaBackend, SummaryBackend};

/// A composite three-layer memory backend that delegates each layer to a
/// pluggable backend trait.
/// - Layer 0 (Raw) → [`ObjectBackend`] (e.g. FileObjectBackend, COS)
/// - Layer 1 (Schema) → [`SchemaBackend`] (e.g. [`MemorySchemaBackend`], PostgreSQL)
/// - Layer 2 (Summary) → [`SummaryBackend`] (e.g. [`VectorSummaryBackend`], LanceDB)
///
/// By default the schema and summary layers are backed by in-memory stores;
/// callers can swap them for production-grade implementations via
/// [`with_schema_backend`](Self::with_schema_backend) and
/// [`with_summary_backend`](Self::with_summary_backend).
pub struct CompositeMemoryBackend {
    raw: Arc<dyn ObjectBackend>,
    raw_ids: std::sync::RwLock<HashMap<String, Vec<String>>>,
    schema: Arc<dyn SchemaBackend>,
    summary: Arc<dyn SummaryBackend>,
    metrics: std::sync::RwLock<MemoryMetrics>,
    /// Concrete handle to the default schema backend, retained while the
    /// caller has not replaced it.  Used by [`set_persist_dir`](Self::set_persist_dir)
    /// and [`load`](Self::load) so the legacy persistence helpers keep working.
    default_schema: Option<Arc<MemorySchemaBackend>>,
    /// Concrete handle to the default summary backend; see `default_schema`.
    default_summary: Option<Arc<VectorSummaryBackend>>,
}

impl CompositeMemoryBackend {
    /// Create a composite backend wired to the given raw object backend, with
    /// in-memory defaults for the schema and summary layers.
    pub fn new(
        raw: Arc<dyn ObjectBackend>,
        vector: Arc<dyn cog_core::VectorBackend>,
        embedding_dim: usize,
    ) -> Self {
        let schema = Arc::new(MemorySchemaBackend::new());
        let summary = Arc::new(VectorSummaryBackend::new(vector, embedding_dim));
        Self {
            raw,
            raw_ids: std::sync::RwLock::new(HashMap::new()),
            schema: schema.clone(),
            summary: summary.clone(),
            metrics: std::sync::RwLock::new(MemoryMetrics::default()),
            default_schema: Some(schema),
            default_summary: Some(summary),
        }
    }

    /// Replace the schema-layer backend with a custom implementation
    /// (e.g. a PostgreSQL-backed `SchemaBackend`).
    pub fn with_schema_backend(mut self, schema: Arc<dyn SchemaBackend>) -> Self {
        self.schema = schema;
        self.default_schema = None;
        self
    }

    /// Replace the summary-layer backend with a custom implementation
    /// (e.g. a LanceDB-backed `SummaryBackend`).
    pub fn with_summary_backend(mut self, summary: Arc<dyn SummaryBackend>) -> Self {
        self.summary = summary;
        self.default_summary = None;
        self
    }

    /// Configure persistence for the default in-memory schema/summary backends.
    /// Has no effect on layers whose backend has been replaced via
    /// [`with_schema_backend`](Self::with_schema_backend) or
    /// [`with_summary_backend`](Self::with_summary_backend) — production
    /// backends manage their own persistence.
    pub fn set_persist_dir(&mut self, path: impl Into<PathBuf>) {
        let path = path.into();
        if let Some(schema) = self.default_schema.as_ref() {
            schema.set_persist_dir(path.clone());
        }
        if let Some(summary) = self.default_summary.as_ref() {
            summary.set_persist_dir(path);
        }
    }

    /// Load any previously persisted entries for the default in-memory
    /// schema/summary backends.
    /// Has no effect on layers whose backend has been replaced with a custom
    /// implementation.
    pub async fn load(&self) -> SFResult<()> {
        if let Some(schema) = self.default_schema.as_ref() {
            schema.load().await?;
        }
        if let Some(summary) = self.default_summary.as_ref() {
            summary.load().await?;
        }
        Ok(())
    }

    fn raw_key(namespace: &str, id: &str) -> String {
        format!("memory/raw/{}/{}", namespace, id)
    }
}

#[async_trait]
impl MemoryBackend for CompositeMemoryBackend {
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String> {
        let key = Self::raw_key(&source.namespace, &source.id);
        let uri = self.raw.put(&key, &source.payload).await?;
        let mut ids = self
            .raw_ids
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let ns_ids = ids.entry(source.namespace.clone()).or_default();
        if !ns_ids.contains(&source.id) {
            ns_ids.push(source.id.clone());
        }
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.raw_archived += 1;
        Ok(uri)
    }

    async fn get_raw(&self, namespace: &str, id: &str) -> SFResult<Option<RawSource>> {
        let key = Self::raw_key(namespace, id);
        match self.raw.get(&key).await? {
            Some(data) => {
                // Reconstruct a minimal RawSource from the payload.
                // In production the full metadata would be stored alongside.
                Ok(Some(RawSource::new(
                    id,
                    namespace,
                    "application/octet-stream",
                    data,
                )))
            }
            None => Ok(None),
        }
    }

    async fn list_raw(&self, namespace: &str, _prefix: Option<&str>) -> SFResult<Vec<String>> {
        let ids = self
            .raw_ids
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(ids.get(namespace).cloned().unwrap_or_default())
    }

    async fn delete_raw(&self, namespace: &str, id: &str) -> SFResult<()> {
        let key = Self::raw_key(namespace, id);
        self.raw.delete(&key).await?;
        let mut ids = self
            .raw_ids
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(ns_ids) = ids.get_mut(namespace) {
            ns_ids.retain(|x| x != id);
        }
        Ok(())
    }

    async fn store_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        self.schema.store_schema(namespace, entry).await?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.schema_stored += 1;
        Ok(())
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        self.schema.get_schema(namespace, id).await
    }

    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>> {
        self.schema.search_schema(namespace, query, limit).await
    }

    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        self.schema.schema_for_raw(namespace, raw_id).await
    }

    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>> {
        self.schema.list_schema(namespace).await
    }

    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()> {
        self.schema.delete_schema(namespace, id).await
    }

    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: cog_core::RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>> {
        self.schema
            .query_relations(namespace, entity, direction, relation_type)
            .await
    }

    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        self.schema.update_schema(namespace, entry).await?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.schema_updated += 1;
        Ok(())
    }

    async fn store_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        self.summary.store_summary(namespace, entry).await?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.summary_stored += 1;
        Ok(())
    }

    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
        self.summary.get_summary(namespace, id).await
    }

    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let results = self
            .summary
            .search_summary(namespace, query_embedding, top_k, time_range)
            .await?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.summary_searched += 1;
        Ok(results)
    }

    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&cog_core::SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let results = self
            .summary
            .search_summary_hybrid(namespace, query_dense, query_sparse, top_k, time_range)
            .await?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.summary_searched += 1;
        Ok(results)
    }

    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>> {
        self.summary.summary_for_raw(namespace, raw_id).await
    }

    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
        self.summary.list_summary(namespace).await
    }

    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()> {
        self.summary.delete_summary(namespace, id).await
    }

    async fn update_summary(&self, namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        self.summary.update_summary(namespace, entry).await?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        metrics.summary_updated += 1;
        Ok(())
    }

    fn metrics(&self) -> MemoryMetrics {
        self.metrics
            .read()
            .map(|m| m.clone())
            .unwrap_or_else(|_| MemoryMetrics::default())
    }

    async fn health_check(&self) -> SFResult<()> {
        // Exercise both layers so the check fails fast if either is broken.
        let _ = self.schema.list_schema("default").await?;
        let _ = self.summary.list_summary("default").await?;
        Ok(())
    }

    async fn search_all(
        &self,
        namespace: &str,
        query: &str,
        embedding: Option<&[f32]>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<UnifiedSearchResult>> {
        let mut results: Vec<UnifiedSearchResult> = self
            .schema
            .search_schema(namespace, query, top_k)
            .await?
            .into_iter()
            .map(UnifiedSearchResult::Schema)
            .collect();

        if let Some(emb) = embedding {
            let summaries = self
                .summary
                .search_summary(namespace, emb, top_k, time_range)
                .await?;
            results.extend(summaries.into_iter().map(UnifiedSearchResult::Summary));
        } else {
            let query_lower = query.to_lowercase();
            let summary_results: Vec<UnifiedSearchResult> = self
                .summary
                .list_summary(namespace)
                .await?
                .into_iter()
                .filter(|e| {
                    e.text.to_lowercase().contains(&query_lower)
                        && time_range.as_ref().is_none_or(|(start, end)| {
                            e.generated_at >= *start && e.generated_at <= *end
                        })
                })
                .map(|e| UnifiedSearchResult::Summary(SummarySearchResult::new(e, 1.0)))
                .collect();
            results.extend(summary_results);
        }

        results.truncate(top_k);
        Ok(results)
    }

    async fn ingest_explicit(
        &self,
        namespace: &str,
        text: &str,
        importance: f32,
        tags: Vec<String>,
    ) -> SFResult<()> {
        let id = format!("explicit-{}", uuid::Uuid::new_v4());
        let raw = RawSource::new(&id, namespace, "memory/explicit", text.as_bytes().to_vec())
            .with_tags(tags);

        let key = Self::raw_key(namespace, &id);
        self.raw.put(&key, &raw.payload).await?;

        {
            let mut ids = self
                .raw_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            let ns_ids = ids.entry(namespace.to_string()).or_default();
            if !ns_ids.contains(&id) {
                ns_ids.push(id.clone());
            }
        }

        {
            let mut metrics = self
                .metrics
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            metrics.raw_archived += 1;
        }

        let summary = SummaryEntry::new(
            &id,
            namespace,
            text,
            vec![0.0f32; 128],
            "explicit",
            cog_core::SourceRef::new(format!("memory://{}", id), "explicit/v1"),
        )
        .with_importance(importance);

        self.summary.store_summary(namespace, &summary).await?;

        {
            let mut metrics = self
                .metrics
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            metrics.summary_stored += 1;
        }

        Ok(())
    }

    async fn forget(&self, namespace: &str, id: &str) -> SFResult<()> {
        let key = Self::raw_key(namespace, id);
        self.raw.delete(&key).await?;

        {
            let mut ids = self
                .raw_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            if let Some(ns_ids) = ids.get_mut(namespace) {
                ns_ids.retain(|x| x != id);
            }
        }

        let schemas = self.schema.schema_for_raw(namespace, id).await?;
        for s in schemas {
            self.schema.delete_schema(namespace, &s.id).await?;
        }

        let summaries = self.summary.summary_for_raw(namespace, id).await?;
        for s in summaries {
            self.summary.delete_summary(namespace, &s.id).await?;
        }

        Ok(())
    }

    async fn decay(
        &self,
        namespace: &str,
        age_threshold_secs: u64,
        importance_threshold: f32,
    ) -> SFResult<DecayReport> {
        let summaries = self.summary.list_summary(namespace).await?;
        let now = Utc::now();
        let mut decayed = 0usize;

        for mut entry in summaries {
            let age_secs = (now - entry.generated_at).num_seconds() as u64;
            if age_secs > age_threshold_secs && entry.importance < importance_threshold {
                entry.embedding = entry
                    .embedding
                    .iter()
                    .map(|v| (v * 100.0).round() / 100.0)
                    .collect();
                self.summary.update_summary(namespace, &entry).await?;
                decayed += 1;
            }
        }

        Ok(DecayReport {
            namespace: namespace.to_string(),
            entries_decayed: decayed,
            entries_archived: 0,
        })
    }
}
