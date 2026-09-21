use async_trait::async_trait;
use base64::Engine;
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
    schema: Arc<dyn SchemaBackend>,
    summary: Arc<dyn SummaryBackend>,
    metrics: std::sync::RwLock<MemoryMetrics>,
    /// Dense embedder used when a caller stores an explicit memory. Without one
    /// the entry is stored with no vector at all, so it stays reachable through
    /// the text path and is not indexed as a point that ties with every other
    /// vector-less entry.
    embedder: Option<Arc<dyn cog_core::EmbeddingProvider>>,
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
            schema: schema.clone(),
            summary: summary.clone(),
            metrics: std::sync::RwLock::new(MemoryMetrics::default()),
            embedder: None,
            default_schema: Some(schema),
            default_summary: Some(summary),
        }
    }

    /// Attach a dense embedder. Explicit ingests then store a real embedding
    /// instead of none.
    pub fn with_embedder(mut self, embedder: Arc<dyn cog_core::EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
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

    fn raw_prefix(namespace: &str) -> String {
        format!("memory/raw/{}/", namespace)
    }

    /// Serialize a raw source into the object the Raw layer stores.
    ///
    /// The payload goes in as base64 rather than a JSON number array: a text
    /// payload stored that way costs several bytes per input byte, and these
    /// objects are written on every archived memory.
    fn encode_raw(source: &RawSource) -> SFResult<Vec<u8>> {
        let envelope = RawEnvelope {
            id: source.id.clone(),
            namespace: source.namespace.clone(),
            content_type: source.content_type.clone(),
            tags: source.tags.clone(),
            created_at: source.created_at,
            archived_at: source.archived_at,
            payload: base64::engine::general_purpose::STANDARD.encode(&source.payload),
        };
        serde_json::to_vec(&envelope)
            .map_err(|e| SFError::Agent(format!("encode raw source failed: {}", e)))
    }

    fn decode_raw(bytes: &[u8]) -> SFResult<RawSource> {
        let envelope: RawEnvelope = serde_json::from_slice(bytes)
            .map_err(|e| SFError::Agent(format!("decode raw source failed: {}", e)))?;
        let payload = base64::engine::general_purpose::STANDARD
            .decode(&envelope.payload)
            .map_err(|e| SFError::Agent(format!("decode raw payload failed: {}", e)))?;
        Ok(RawSource {
            id: envelope.id,
            namespace: envelope.namespace,
            content_type: envelope.content_type,
            payload,
            tags: envelope.tags,
            created_at: envelope.created_at,
            archived_at: envelope.archived_at,
        })
    }
}

/// On-object form of a [`RawSource`]. Storing the metadata alongside the
/// payload keeps a raw source self-describing: the content type survives a
/// round trip instead of being guessed back as `application/octet-stream`.
#[derive(serde::Serialize, serde::Deserialize)]
struct RawEnvelope {
    id: String,
    namespace: String,
    content_type: String,
    tags: Vec<String>,
    created_at: DateTime<Utc>,
    archived_at: DateTime<Utc>,
    payload: String,
}

#[async_trait]
impl MemoryBackend for CompositeMemoryBackend {
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String> {
        let key = Self::raw_key(&source.namespace, &source.id);
        let uri = self.raw.put(&key, &Self::encode_raw(source)?).await?;
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
            Some(data) => Ok(Some(Self::decode_raw(&data)?)),
            None => Ok(None),
        }
    }

    async fn list_raw(
        &self,
        namespace: &str,
        content_type_prefix: Option<&str>,
    ) -> SFResult<Vec<String>> {
        // The object store is the index: an in-process map is empty after a
        // restart and diverges between replicas, which is exactly the failure
        // this layer exists to avoid.
        let prefix = Self::raw_prefix(namespace);
        let mut ids: Vec<String> = self
            .raw
            .list(Some(&prefix))
            .await?
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_string))
            .filter(|id| !id.is_empty())
            .collect();

        if let Some(content_type) = content_type_prefix {
            // A listing carries keys only, and the content type lives inside
            // the stored envelope, so a type-filtered query reads its
            // candidates back. Unfiltered listings stay a single call.
            let mut matched = Vec::with_capacity(ids.len());
            for id in ids {
                let key = Self::raw_key(namespace, &id);
                if let Some(bytes) = self.raw.get(&key).await? {
                    if Self::decode_raw(&bytes)?
                        .content_type
                        .starts_with(content_type)
                    {
                        matched.push(id);
                    }
                }
            }
            ids = matched;
        }

        ids.sort();
        Ok(ids)
    }

    async fn delete_raw(&self, namespace: &str, id: &str) -> SFResult<()> {
        let key = Self::raw_key(namespace, id);
        self.raw.delete(&key).await
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
        // Exercise both layers so the check fails fast if either is broken, but
        // read one row at most: the `list_*` variants materialize the whole
        // namespace, which on the schema table is hundreds of megabytes of rows
        // and JSON pushed through the connection per call. This sits on the
        // readiness probe's path, so a corpus-sized read here grows with the
        // data until the probe exceeds its timeout and calls a healthy process
        // unready.
        self.schema.search_schema("default", "", 1).await?;
        self.summary.get_summary("default", "").await?;
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
        self.raw.put(&key, &Self::encode_raw(&raw)?).await?;

        {
            let mut metrics = self
                .metrics
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            metrics.raw_archived += 1;
        }

        // With no embedder this host has no vector layer, so the entry carries
        // no vector and the text path is the only way back to it. Writing a
        // zero vector instead would put a well-formed but information-free
        // point in the collection, where it ties at score 0.0 with every other
        // such point and answers searches with an arbitrary ranking.
        let embedding = match self.embedder.as_ref() {
            Some(embedder) => embedder
                .embed(vec![text.to_string()])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| SFError::Agent("embedder returned no vector".into()))?,
            None => Vec::new(),
        };
        let embedding_model = if embedding.is_empty() {
            cog_core::NO_EMBEDDING_MODEL
        } else {
            "explicit/v1"
        };

        let summary = SummaryEntry::new(
            &id,
            namespace,
            text,
            embedding,
            embedding_model,
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

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::RelationDirection;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Which read paths a layer was asked to serve. A health check may take the
    /// bounded one; the namespace-sized one is what a probe must never cost.
    #[derive(Default)]
    struct ReadTally {
        one_row: AtomicUsize,
        whole_namespace: AtomicUsize,
    }

    impl ReadTally {
        fn one_row(&self) -> usize {
            self.one_row.load(Ordering::SeqCst)
        }

        fn whole_namespace(&self) -> usize {
            self.whole_namespace.load(Ordering::SeqCst)
        }
    }

    struct CountingSchema {
        tally: Arc<ReadTally>,
    }

    #[async_trait]
    impl SchemaBackend for CountingSchema {
        async fn store_schema(&self, _namespace: &str, _entry: &SchemaEntry) -> SFResult<()> {
            Ok(())
        }

        async fn get_schema(&self, _namespace: &str, _id: &str) -> SFResult<Option<SchemaEntry>> {
            Ok(None)
        }

        async fn search_schema(
            &self,
            _namespace: &str,
            _query: &str,
            _limit: usize,
        ) -> SFResult<Vec<SchemaSearchResult>> {
            self.tally.one_row.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }

        async fn schema_for_raw(
            &self,
            _namespace: &str,
            _raw_id: &str,
        ) -> SFResult<Vec<SchemaEntry>> {
            Ok(Vec::new())
        }

        async fn list_schema(&self, _namespace: &str) -> SFResult<Vec<SchemaEntry>> {
            self.tally.whole_namespace.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }

        async fn delete_schema(&self, _namespace: &str, _id: &str) -> SFResult<()> {
            Ok(())
        }

        async fn query_relations(
            &self,
            _namespace: &str,
            _entity: &str,
            _direction: RelationDirection,
            _relation_type: Option<&str>,
        ) -> SFResult<Vec<SchemaEntry>> {
            Ok(Vec::new())
        }

        async fn update_schema(&self, _namespace: &str, _entry: &SchemaEntry) -> SFResult<()> {
            Ok(())
        }
    }

    struct CountingSummary {
        tally: Arc<ReadTally>,
    }

    #[async_trait]
    impl SummaryBackend for CountingSummary {
        async fn store_summary(&self, _namespace: &str, _entry: &SummaryEntry) -> SFResult<()> {
            Ok(())
        }

        async fn get_summary(&self, _namespace: &str, _id: &str) -> SFResult<Option<SummaryEntry>> {
            self.tally.one_row.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        async fn search_summary(
            &self,
            _namespace: &str,
            _query_embedding: &[f32],
            _top_k: usize,
            _time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
        ) -> SFResult<Vec<SummarySearchResult>> {
            Ok(Vec::new())
        }

        async fn summary_for_raw(
            &self,
            _namespace: &str,
            _raw_id: &str,
        ) -> SFResult<Vec<SummaryEntry>> {
            Ok(Vec::new())
        }

        async fn list_summary(&self, _namespace: &str) -> SFResult<Vec<SummaryEntry>> {
            self.tally.whole_namespace.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }

        async fn delete_summary(&self, _namespace: &str, _id: &str) -> SFResult<()> {
            Ok(())
        }

        async fn update_summary(&self, _namespace: &str, _entry: &SummaryEntry) -> SFResult<()> {
            Ok(())
        }
    }

    fn counting_composite(tally: Arc<ReadTally>) -> (CompositeMemoryBackend, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let backend = CompositeMemoryBackend::new(
            Arc::new(cog_storage::FileObjectBackend::new(tmp.path())),
            Arc::new(cog_storage::MemoryVectorBackend::new()),
            4,
        )
        .with_schema_backend(Arc::new(CountingSchema {
            tally: tally.clone(),
        }))
        .with_summary_backend(Arc::new(CountingSummary { tally }));
        (backend, tmp)
    }

    /// A readiness probe hits this several times a minute, so the health check
    /// has to stay bounded: reading the namespace makes the probe cost grow
    /// with the corpus until it overruns its timeout and a healthy process is
    /// called unready. Both layers must still be exercised, or the check would
    /// stop detecting a broken layer at all.
    #[tokio::test]
    async fn health_check_reads_one_row_per_layer_instead_of_the_namespace() {
        let tally = Arc::new(ReadTally::default());
        let (backend, _tmp) = counting_composite(tally.clone());

        backend.health_check().await.unwrap();

        assert_eq!(
            tally.one_row(),
            2,
            "health check must exercise both the schema and the summary layer"
        );
        assert_eq!(
            tally.whole_namespace(),
            0,
            "health check must not load a whole namespace through either layer"
        );
    }
}
