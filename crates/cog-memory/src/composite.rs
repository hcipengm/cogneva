use async_trait::async_trait;
use base64::Engine;
use std::path::PathBuf;
use std::sync::Arc;

use crate::maintenance::{DECAY_ARCHIVE_FLOOR, DECAY_IMPORTANCE_FACTOR};
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
/// - Layer 2 (Summary) → [`SummaryBackend`] (e.g. [`VectorSummaryBackend`], Qdrant)
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

    /// Attach an embedder. Explicit ingests then store a real embedding
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
    /// (e.g. a Qdrant-backed `SummaryBackend`).
    pub fn with_summary_backend(mut self, summary: Arc<dyn SummaryBackend>) -> Self {
        self.summary = summary;
        self.default_summary = None;
        self
    }

    /// Fill in the vectors an entry has to carry before the summary layer stores it.
    ///
    /// The summary layer indexes whatever vectors an entry arrives with and never calls a
    /// model itself, so this is the only place a stored text turns into vectors. Both
    /// producers reach it: an explicit ingest, and the auto-ingest pipeline, whose extractor
    /// hands its summaries to `store_summary` like any other caller.
    ///
    /// The dense half is computed only when the entry arrives without one — an extractor
    /// that already embedded its text has done that work, and the model's name goes on the
    /// row so a later reader can tell which model made the vector.
    ///
    /// The sparse half is computed only when the embedder can produce one at all. A
    /// provider whose sparse session failed to load still embeds densely, and failing the
    /// whole store over the missing half would take the working half down with it.
    ///
    /// With no embedder at all the entry keeps no vector and the text path is the only way
    /// back to it: a zero vector would be a well-formed but information-free point in the
    /// collection, tying at score 0.0 with every other such point and answering searches
    /// with an arbitrary ranking.
    async fn embed_entry(&self, entry: &SummaryEntry) -> SFResult<SummaryEntry> {
        let Some(embedder) = self.embedder.as_ref() else {
            return Ok(entry.clone());
        };
        let mut entry = entry.clone();

        if entry.embedding.is_empty() {
            let vector = embedder
                .embed(vec![entry.text.clone()])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| SFError::Agent("embedder returned no vector".into()))?;
            // An empty vector is what a provider returns for "no vector", so treating it as
            // one keeps the row's absent-vector state (and its name) rather than writing a
            // name for a vector that is not there.
            if !vector.is_empty() {
                entry.embedding = vector;
                entry.embedding_model = embedder.model_id().to_string();
            }
        }

        if entry.sparse_embedding.is_none() && embedder.supports_sparse() {
            match embedder.embed_sparse(vec![entry.text.clone()]).await {
                Ok(mut sparse) => {
                    if let Some(vector) = sparse.drain(..).next() {
                        entry = entry.with_sparse_embedding(vector);
                    }
                }
                Err(e) => tracing::warn!(
                    "sparse embedding failed for summary {}; storing it with its dense \
                     vector only: {e}",
                    entry.id
                ),
            }
        }

        Ok(entry)
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

    async fn presign_raw(
        &self,
        namespace: &str,
        id: &str,
        expiry_secs: u64,
    ) -> SFResult<Option<String>> {
        // The URL is signed against the same key `archive_raw` wrote, so it
        // resolves to the stored envelope the inline reader decodes -- one
        // stored object, two ways to reach it. The key stays private to this
        // layer: a caller naming the object by key would be re-deriving a
        // layout it does not own.
        let key = Self::raw_key(namespace, id);
        self.raw.presign_url(&key, expiry_secs).await.map(Some)
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
        let entry = self.embed_entry(entry).await?;
        self.summary.store_summary(namespace, &entry).await?;
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
            // The sparse half of the query is derived here rather than taken from the
            // caller: callers hold a dense vector -- one of them takes it straight from
            // a request body -- and no embedding provider, so a sparse query can only
            // come from this backend's own embedder. Absent or failing, the search
            // still runs on the dense half alone: a degraded ranking, not a failed one.
            let sparse_query = match self.embedder.as_ref() {
                Some(embedder) if embedder.supports_sparse() => {
                    match embedder.embed_sparse(vec![query.to_string()]).await {
                        Ok(mut vectors) => vectors.drain(..).next(),
                        Err(e) => {
                            tracing::warn!(
                                "sparse query embedding failed; ranking the summary layer by \
                                 its dense vector alone: {e}"
                            );
                            None
                        }
                    }
                }
                _ => None,
            };
            let summaries = self
                .summary
                .search_summary_hybrid(namespace, emb, sparse_query.as_ref(), top_k, time_range)
                .await?;
            results.extend(summaries.into_iter().map(UnifiedSearchResult::Summary));
        } else {
            let query_lower = query.to_lowercase();
            let mut summary_results: Vec<UnifiedSearchResult> = self
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
                .map(|e| {
                    let score = cog_core::importance_weighted_score(1.0, e.importance);
                    UnifiedSearchResult::Summary(SummarySearchResult::new(e, score))
                })
                .collect();
            // No embedder: every candidate is a substring hit, so importance is
            // the only ordering signal — the same prior the dense branch gets
            // from the weighted summary search. The id key is what actually makes
            // the order defined; equal scores alone would leave it to iteration.
            summary_results.sort_by(|a, b| {
                let key_a = match a {
                    UnifiedSearchResult::Summary(s) => {
                        cog_core::rank_key(1.0, s.entry.importance, &s.entry.id)
                    }
                    _ => (0.0, 0.0, ""),
                };
                let key_b = match b {
                    UnifiedSearchResult::Summary(s) => {
                        cog_core::rank_key(1.0, s.entry.importance, &s.entry.id)
                    }
                    _ => (0.0, 0.0, ""),
                };
                key_b
                    .partial_cmp(&key_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
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

        // Both vectors are filled in by the same helper the auto-ingest path goes
        // through, so an explicitly ingested memory and an extracted summary are
        // embedded one way and carry the model's own name rather than a label the
        // call site picked.
        let summary = SummaryEntry::new(
            &id,
            namespace,
            text,
            Vec::new(),
            cog_core::NO_EMBEDDING_MODEL,
            cog_core::SourceRef::new(format!("memory://{}", id), "explicit/v1"),
        )
        .with_importance(importance);
        let summary = self.embed_entry(&summary).await?;

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

        // A schema entry is identified by its content, so forgetting this raw
        // withdraws one origin rather than deleting the fact: an entity other
        // raws still mention has to outlive the one being forgotten.
        let schemas = self.schema.schema_for_raw(namespace, id).await?;
        let raw_uri = format!("memory://{}", id);
        for s in schemas {
            self.schema
                .forget_schema_source(namespace, &s.id, &raw_uri)
                .await?;
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
        let mut archived = 0usize;

        for mut entry in summaries {
            // `max(0)` guards a stored timestamp in the future: an entry that has
            // not aged yet must not read as ancient through a negative cast.
            let age_secs = (now - entry.generated_at).num_seconds().max(0) as u64;
            if age_secs <= age_threshold_secs || entry.importance >= importance_threshold {
                continue;
            }

            // Decay demotes, then archives: one pass lowers importance by
            // `DECAY_IMPORTANCE_FACTOR`, and an entry only leaves the searchable
            // layer once repeated passes have driven it to `DECAY_ARCHIVE_FLOOR`.
            // A single pass therefore cannot delete a freshly stored entry no
            // matter how low its starting importance was. The two counts are
            // disjoint: an entry is either demoted in place (still searchable)
            // or removed this pass.
            entry.importance = (entry.importance * DECAY_IMPORTANCE_FACTOR).max(0.0);
            if entry.importance <= DECAY_ARCHIVE_FLOOR {
                self.summary.delete_summary(namespace, &entry.id).await?;
                archived += 1;
            } else {
                self.summary.update_summary(namespace, &entry).await?;
                decayed += 1;
            }
        }

        Ok(DecayReport {
            namespace: namespace.to_string(),
            entries_decayed: decayed,
            entries_archived: archived,
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
