//! Vector-backed implementation of [`SummaryBackend`].
//! Wraps any [`cog_core::VectorBackend`] (e.g. `MemoryVectorBackend`,
//! `QdrantVectorBackend`, `LanceDbVectorBackend`) so the Summary layer
//! can be backed by a real vector database in production while still using
//! the typed [`SummaryEntry`] API.
//! Structured data (text, namespace, raw_uri, confidence, etc.) is delegated
//! to a [`SummaryEntryStore`] so callers can choose between in-memory (testing)
//! and PostgreSQL (production) persistence.  Vectors are indexed by the
//! [`VectorBackend`] independently.

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

use crate::SummaryEntryStore;
use chrono::{DateTime, Utc};
use cog_core::SummaryBackend;
use cog_core::{SFError, SFResult, VectorBackend};
use cog_core::{SummaryEntry, SummarySearchResult};

/// Default collection name when none is provided.
pub const DEFAULT_SUMMARY_COLLECTION: &str = "summaries";

/// What one repair pass did.
///
/// The counts are reported rather than inferred from the collection afterwards: an
/// entry store that could not be read and one where every row already carried its
/// vector leave the collection in the same shape, and only these tell them apart.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingRepairReport {
    /// Entries the store holds, and the pass therefore looked at.
    pub scanned: usize,
    /// Entries that had no vector and now have one.
    pub backfilled: usize,
    /// Entries that already carried a dense vector and were left untouched.
    pub already_embedded: usize,
    /// Entries still without a vector, because the model could not be reached for
    /// them or answered with nothing.
    pub failed: usize,
}

/// [`SummaryBackend`] that delegates similarity search to a
/// [`cog_core::VectorBackend`] and structured-data persistence to a
/// [`SummaryEntryStore`].
pub struct VectorSummaryBackend {
    vector: Arc<dyn VectorBackend>,
    store: Arc<dyn SummaryEntryStore>,
    collection: String,
    embedding_dim: usize,
    /// Map from `SummaryEntry::id` to the id returned by `VectorBackend::insert`.
    vec_ids: RwLock<HashMap<String, String>>,
    /// Guards lazy `create_collection` so we issue at most one DDL call.
    init_guard: Mutex<bool>,
    /// Optional directory for `<dir>/summary.json` persistence (fallback for
    /// in-memory entry stores).
    persist_dir: RwLock<Option<PathBuf>>,
}

impl VectorSummaryBackend {
    /// Create a new backend against `vector`, using
    /// [`DEFAULT_SUMMARY_COLLECTION`] as the collection name and an in-memory
    /// entry store.
    pub fn new(vector: Arc<dyn VectorBackend>, embedding_dim: usize) -> Self {
        Self::with_collection(vector, DEFAULT_SUMMARY_COLLECTION, embedding_dim)
    }

    /// Create a new backend with a custom collection name and an in-memory
    /// entry store.
    pub fn with_collection(
        vector: Arc<dyn VectorBackend>,
        collection: impl Into<String>,
        embedding_dim: usize,
    ) -> Self {
        Self {
            vector,
            store: Arc::new(crate::entry_store::MemoryEntryStore::new()),
            collection: collection.into(),
            embedding_dim,
            vec_ids: RwLock::new(HashMap::new()),
            init_guard: Mutex::new(false),
            persist_dir: RwLock::new(None),
        }
    }

    /// Builder-style helper that replaces the entry store.
    pub fn with_store(mut self, store: Arc<dyn SummaryEntryStore>) -> Self {
        self.store = store;
        self
    }

    /// Builder-style helper that sets the persistence directory.
    pub fn with_persist_dir(self, dir: impl Into<PathBuf>) -> Self {
        self.set_persist_dir(dir);
        self
    }

    /// Configure (or update) the persistence directory.
    pub fn set_persist_dir(&self, dir: impl Into<PathBuf>) {
        if let Ok(mut d) = self.persist_dir.write() {
            *d = Some(dir.into());
        }
    }

    fn current_persist_dir(&self) -> SFResult<Option<PathBuf>> {
        let d = self
            .persist_dir
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(d.clone())
    }

    /// Borrow the underlying vector backend.
    pub fn vector(&self) -> &Arc<dyn VectorBackend> {
        &self.vector
    }

    /// Collection name used for vector operations.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Lazily create the underlying collection on first write/search.
    async fn ensure_collection(&self) -> SFResult<()> {
        let mut initialized = self.init_guard.lock().await;
        if *initialized {
            return Ok(());
        }
        self.vector
            .create_collection(&self.collection, self.embedding_dim)
            .await?;
        *initialized = true;
        Ok(())
    }

    /// Load the persisted entries and rebuild the similarity index.
    ///
    /// The entry store is authoritative and the vector collection is derived
    /// from it, so the collection is dropped and rebuilt rather than appended
    /// to: re-inserting into a collection that already survived the restart
    /// would store a second vector per entry and skew similarity search.
    ///
    /// Entries come from `store.list_all()` when the store is durable (e.g.
    /// PostgreSQL) and from `<persist_dir>/summary.json` otherwise.  The JSON
    /// fallback loads the store itself, since an in-memory store is empty at
    /// this point.
    pub async fn load(&self) -> SFResult<()> {
        let entries: Vec<SummaryEntry> = if self.store.is_durable() {
            self.store.list_all().await?
        } else {
            let dir = match self.current_persist_dir()? {
                Some(d) => d,
                None => return Ok(()),
            };
            let path = dir.join("summary.json");
            if !path.exists() {
                return Ok(());
            }
            let data = tokio::fs::read(&path)
                .await
                .map_err(|e| SFError::Agent(format!("read summary.json failed: {}", e)))?;
            let entries: Vec<SummaryEntry> = serde_json::from_slice(&data)
                .map_err(|e| SFError::Agent(format!("parse summary.json failed: {}", e)))?;
            for entry in &entries {
                self.store.upsert(entry).await?;
            }
            entries
        };

        let _ = self.vector.delete_collection(&self.collection).await;
        self.vector
            .create_collection(&self.collection, self.embedding_dim)
            .await?;
        *self.init_guard.lock().await = true;

        let mut rebuilt = HashMap::new();
        for entry in entries {
            if let Some(vec_id) = self.index_entry(&entry).await {
                rebuilt.insert(entry.id.clone(), vec_id);
            }
        }
        let mut vec_ids = self
            .vec_ids
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        *vec_ids = rebuilt;
        Ok(())
    }

    /// Index one entry's dense and sparse vectors into the collection,
    /// returning the dense vector id when the backend accepted it.
    async fn index_entry(&self, entry: &SummaryEntry) -> Option<String> {
        let meta = serde_json::json!({
            "id": entry.id,
            "text": entry.text,
            "raw_uri": entry.source_ref.raw_uri,
        });
        // An entry with no embedding has nothing to index. Passing the empty
        // run through would either be rejected for its dimension or, worse, be
        // stored as a point that ties with every other unembedded entry.
        let vec_id = if entry.embedding.is_empty() {
            None
        } else {
            self.vector
                .insert(
                    &self.collection,
                    vec![entry.embedding.clone()],
                    vec![meta.clone()],
                )
                .await
                .ok()
                .and_then(|mut ids| ids.pop())
        };
        if let Some(ref sparse) = entry.sparse_embedding {
            let _ = self
                .vector
                .insert_sparse(&self.collection, vec![sparse.clone()], vec![meta])
                .await;
        }
        vec_id
    }

    /// Persist the current store to `<persist_dir>/summary.json`.
    /// Has no effect if the persistence directory is unset, or if the entry
    /// store itself is durable (e.g. PostgreSQL).
    pub async fn persist(&self) -> SFResult<()> {
        if self.store.is_durable() {
            return Ok(());
        }
        let dir = match self.current_persist_dir()? {
            Some(d) => d,
            None => return Ok(()),
        };
        let data = {
            let entries = self.store.list_all().await?;
            serde_json::to_vec_pretty(&entries)
                .map_err(|e| SFError::Agent(format!("serialize summary failed: {}", e)))?
        };
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| SFError::Agent(format!("create persist dir failed: {}", e)))?;
        tokio::fs::write(dir.join("summary.json"), data)
            .await
            .map_err(|e| SFError::Agent(format!("write summary.json failed: {}", e)))?;
        Ok(())
    }

    /// Give the entries that predate the embedder the vectors they never got.
    ///
    /// [`load`](Self::load) rebuilds the collection from the entry store and skips
    /// every entry with no vector, so a row stored before a model was configured
    /// stays unreachable by similarity search however long the process runs. Nothing
    /// in the readings says so: the collection and the store agree with each other,
    /// and a search simply returns less than the store holds. That is what this pass
    /// is for — it is the only thing that ever gives those rows a vector.
    ///
    /// Only entries with no dense vector are written. One that has a vector is left
    /// alone: its point in the collection is live, and re-storing it would delete that
    /// point and insert another in its place for no gain.
    ///
    /// A row that fails is counted and skipped, not fatal — one text the model cannot
    /// handle is not a reason to leave the other hundreds unembedded.
    pub async fn backfill_embeddings(
        &self,
        embedder: &dyn cog_core::EmbeddingProvider,
    ) -> SFResult<EmbeddingRepairReport> {
        let entries = self.store.list_all().await?;
        let mut report = EmbeddingRepairReport {
            scanned: entries.len(),
            ..Default::default()
        };

        for entry in entries {
            if !entry.embedding.is_empty() {
                report.already_embedded += 1;
                continue;
            }

            let embedded =
                match crate::embedding_provider::fill_missing_halves(embedder, &entry).await {
                    Ok(embedded) => embedded,
                    Err(e) => {
                        tracing::warn!(
                            "summary {} could not be embedded; it stays reachable by text \
                             only: {e}",
                            entry.id
                        );
                        report.failed += 1;
                        continue;
                    }
                };

            // A provider that answers with no vector leaves the row as it found it.
            // Storing it anyway would rewrite an identical row and count it as repaired.
            if embedded.embedding.is_empty() {
                report.failed += 1;
                continue;
            }

            // Through the store's own path, not the entry store's: the row and its
            // point move together, and a repair that wrote only one of them would be
            // a new instance of the very disagreement this pass exists to remove.
            if let Err(e) = self.store_summary(&entry.namespace, &embedded).await {
                tracing::warn!(
                    "summary {} was embedded but could not be stored back: {e}",
                    entry.id
                );
                report.failed += 1;
                continue;
            }
            report.backfilled += 1;
        }

        Ok(report)
    }
}

#[async_trait]
impl SummaryBackend for VectorSummaryBackend {
    async fn store_summary(&self, _namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        self.ensure_collection().await?;

        let meta = serde_json::json!({
            "id": entry.id,
            "text": entry.text,
            "raw_uri": entry.source_ref.raw_uri,
        });

        let vec_id =
            if entry.embedding.is_empty() {
                None
            } else {
                let mut returned_ids = self
                    .vector
                    .insert(
                        &self.collection,
                        vec![entry.embedding.clone()],
                        vec![meta.clone()],
                    )
                    .await?;
                Some(returned_ids.pop().ok_or_else(|| {
                    SFError::Agent("vector backend returned no id on insert".into())
                })?)
            };

        if let Some(ref sparse) = entry.sparse_embedding {
            let _ = self
                .vector
                .insert_sparse(&self.collection, vec![sparse.clone()], vec![meta.clone()])
                .await;
        }

        self.store.upsert(entry).await?;

        // An entry that lost its embedding must not keep the point it had: the
        // vector index is the store's derived copy, so a stale point would keep
        // answering searches for text the store no longer holds a vector for.
        let written_id = vec_id.clone();
        let stale_vec_id = {
            let mut vec_ids = self
                .vec_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            match vec_id {
                Some(id) => vec_ids.insert(entry.id.clone(), id),
                None => vec_ids.remove(&entry.id),
            }
        };
        if let Some(prev) = stale_vec_id {
            // A backend that names the point after the entry hands back that same
            // name on every insert, so what looks like the entry's previous point is
            // the one this insert just wrote — deleting it would drop the vector the
            // row still claims to hold. A backend that mints a fresh name per insert
            // hands back a different one, and that older point is the stale copy this
            // exists to remove.
            if written_id.as_deref() != Some(prev.as_str()) {
                let _ = self.vector.delete(&self.collection, &[prev]).await;
            }
        }
        self.persist().await?;
        Ok(())
    }

    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
        self.store.get(namespace, id).await
    }

    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        // No query vector means there is no similarity to rank by. Searching
        // anyway would rank against the collection's dimension, not the query,
        // and hand back whichever points the backend happened to order first.
        if query_embedding.is_empty() {
            return Ok(Vec::new());
        }
        if !self.vector.collection_exists(&self.collection).await? {
            return Ok(Vec::new());
        }
        let vector_results = self
            .vector
            .search(&self.collection, query_embedding, top_k)
            .await?;

        let ids: Vec<String> = vector_results
            .iter()
            .map(|vr| {
                vr.metadata
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| vr.id.clone())
            })
            .collect();

        let entries = self.store.get_many(namespace, &ids).await?;
        let entry_map: std::collections::HashMap<String, SummaryEntry> =
            entries.into_iter().map(|e| (e.id.clone(), e)).collect();

        let mut out = Vec::with_capacity(vector_results.len());
        for vr in vector_results {
            let entry_id = vr
                .metadata
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| vr.id.clone());
            if let Some(entry) = entry_map.get(&entry_id).filter(|e| {
                time_range
                    .as_ref()
                    .is_none_or(|(start, end)| e.generated_at >= *start && e.generated_at <= *end)
            }) {
                out.push((
                    cog_core::rank_key(vr.score, entry.importance, &entry.id),
                    SummarySearchResult::new(
                        entry.clone(),
                        cog_core::importance_weighted_score(vr.score, entry.importance),
                    ),
                ));
            }
        }
        // The vector store ranked this candidate set by raw similarity; the
        // importance weight can re-order it, so the recalled set is ranked again
        // before it is returned — by the full key, since the weighted score
        // flattens every non-positive similarity onto one value and would leave
        // their order to the order the store handed the candidates over in.
        out.sort_by(|(key_a, _), (key_b, _)| {
            key_b
                .partial_cmp(key_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(out.into_iter().map(|(_, result)| result).collect())
    }

    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>> {
        let raw_uri = format!("memory://{}", raw_id);
        self.store.list_by_raw_uri(namespace, &raw_uri).await
    }

    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
        self.store.list(namespace).await
    }

    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()> {
        let vec_id = {
            let mut vec_ids = self
                .vec_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            vec_ids.remove(id)
        };
        self.store.delete(namespace, id).await?;
        if let Some(vid) = vec_id {
            let _ = self.vector.delete(&self.collection, &[vid]).await;
        }
        self.persist().await?;
        Ok(())
    }

    async fn search_summary_hybrid(
        &self,
        namespace: &str,
        query_dense: &[f32],
        query_sparse: Option<&cog_core::SparseEmbedding>,
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        // Same reason as `search_summary`: the dense half is what the collection
        // is indexed by, and an empty one carries no similarity to rank with.
        if query_dense.is_empty() {
            return Ok(Vec::new());
        }
        if !self.vector.collection_exists(&self.collection).await? {
            return Ok(Vec::new());
        }

        let vector_results = self
            .vector
            .search_hybrid(&self.collection, query_dense, query_sparse, top_k)
            .await?;

        let ids: Vec<String> = vector_results
            .iter()
            .map(|vr| {
                vr.metadata
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| vr.id.clone())
            })
            .collect();

        let entries = self.store.get_many(namespace, &ids).await?;
        let entry_map: std::collections::HashMap<String, SummaryEntry> =
            entries.into_iter().map(|e| (e.id.clone(), e)).collect();

        let mut out = Vec::with_capacity(vector_results.len());
        for vr in vector_results {
            let entry_id = vr
                .metadata
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| vr.id.clone());
            if let Some(entry) = entry_map.get(&entry_id).filter(|e| {
                time_range
                    .as_ref()
                    .is_none_or(|(start, end)| e.generated_at >= *start && e.generated_at <= *end)
            }) {
                out.push((
                    cog_core::rank_key(vr.score, entry.importance, &entry.id),
                    SummarySearchResult::new(
                        entry.clone(),
                        cog_core::importance_weighted_score(vr.score, entry.importance),
                    ),
                ));
            }
        }
        // The vector store ranked this candidate set by raw similarity; the
        // importance weight can re-order it, so the recalled set is ranked again
        // before it is returned — by the full key, since the weighted score
        // flattens every non-positive similarity onto one value and would leave
        // their order to the order the store handed the candidates over in.
        out.sort_by(|(key_a, _), (key_b, _)| {
            key_b
                .partial_cmp(key_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(out.into_iter().map(|(_, result)| result).collect())
    }

    async fn update_summary(&self, _namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        self.ensure_collection().await?;

        let meta = serde_json::json!({
            "id": entry.id,
            "text": entry.text,
            "raw_uri": entry.source_ref.raw_uri,
        });

        let mut returned_ids = self
            .vector
            .insert(
                &self.collection,
                vec![entry.embedding.clone()],
                vec![meta.clone()],
            )
            .await?;
        let vec_id = returned_ids
            .pop()
            .ok_or_else(|| SFError::Agent("vector backend returned no id on insert".into()))?;

        if let Some(ref sparse) = entry.sparse_embedding {
            let _ = self
                .vector
                .insert_sparse(&self.collection, vec![sparse.clone()], vec![meta.clone()])
                .await;
        }

        self.store.upsert(entry).await?;

        let written_id = vec_id.clone();
        let stale_vec_id = {
            let mut vec_ids = self
                .vec_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            vec_ids.insert(entry.id.clone(), vec_id)
        };
        if let Some(prev) = stale_vec_id {
            // Same rule as `store_summary`: an id the insert just wrote is the entry's
            // current point, not a stale copy of it.
            if written_id != prev {
                let _ = self.vector.delete(&self.collection, &[prev]).await;
            }
        }
        self.persist().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry_store::MemoryEntryStore;
    use cog_core::{SourceRef, SummaryBackend};

    /// Durable stand-in for the PostgreSQL store: same entries, `is_durable`
    /// reporting true is what selects the authoritative-store load path.
    struct DurableStubStore {
        inner: MemoryEntryStore,
    }

    impl DurableStubStore {
        fn new() -> Self {
            Self {
                inner: MemoryEntryStore::new(),
            }
        }
    }

    #[async_trait]
    impl SummaryEntryStore for DurableStubStore {
        async fn get(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
            self.inner.get(namespace, id).await
        }
        async fn get_many(&self, namespace: &str, ids: &[String]) -> SFResult<Vec<SummaryEntry>> {
            self.inner.get_many(namespace, ids).await
        }
        async fn list(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
            self.inner.list(namespace).await
        }
        async fn list_by_raw_uri(
            &self,
            namespace: &str,
            raw_uri: &str,
        ) -> SFResult<Vec<SummaryEntry>> {
            self.inner.list_by_raw_uri(namespace, raw_uri).await
        }
        async fn upsert(&self, entry: &SummaryEntry) -> SFResult<()> {
            self.inner.upsert(entry).await
        }
        async fn delete(&self, namespace: &str, id: &str) -> SFResult<()> {
            self.inner.delete(namespace, id).await
        }
        async fn list_all(&self) -> SFResult<Vec<SummaryEntry>> {
            self.inner.list_all().await
        }
        fn is_durable(&self) -> bool {
            true
        }
    }

    fn entry(id: &str) -> SummaryEntry {
        SummaryEntry::new(
            id,
            "default",
            format!("text of {id}"),
            vec![1.0, 0.0, 0.0, 0.0],
            "test/v1",
            SourceRef::new(format!("memory://{id}"), "test/v1"),
        )
    }

    /// The vector collection is a derived index over the entry store. Loading
    /// twice must leave one vector per entry, not one per load.
    #[tokio::test]
    async fn load_rebuilds_index_without_duplicating_vectors() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&entry("s1")).await.unwrap();
        store.upsert(&entry("s2")).await.unwrap();

        let vector = Arc::new(cog_storage::MemoryVectorBackend::new());
        let backend = VectorSummaryBackend::new(vector, 4).with_store(store);

        backend.load().await.unwrap();
        backend.load().await.unwrap();

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2, "each entry must be indexed exactly once");

        let ids: Vec<&str> = hits.iter().map(|h| h.entry.id.as_str()).collect();
        assert!(ids.contains(&"s1") && ids.contains(&"s2"));
    }

    /// An entry written after a load must be searchable through the index the
    /// load rebuilt, and deleting it must drop the vector too.
    #[tokio::test]
    async fn entries_written_after_load_stay_searchable() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&entry("s1")).await.unwrap();

        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);

        backend.load().await.unwrap();
        backend
            .store_summary("default", &entry("s3"))
            .await
            .unwrap();

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);

        backend.delete_summary("default", "s3").await.unwrap();
        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.id, "s1");
    }

    /// The JSON fallback path must also rebuild the index rather than append.
    #[tokio::test]
    async fn load_from_json_rebuilds_index_without_duplicating_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::entry_store::MemoryEntryStore::new());
        store.upsert(&entry("s1")).await.unwrap();

        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store)
                .with_persist_dir(dir.path());
        backend.persist().await.unwrap();

        backend.load().await.unwrap();
        backend.load().await.unwrap();

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
    }

    /// No embedder means no vector, and an entry with no vector must not reach
    /// the index at all. Indexing a run of zeros instead would put a point in
    /// the collection that ties at score 0.0 with every other such point, so
    /// every search would answer with an arbitrary subset of them.
    #[tokio::test]
    async fn an_entry_without_an_embedding_is_stored_but_not_indexed() {
        let store = Arc::new(DurableStubStore::new());
        let mut no_vector = entry("s1");
        no_vector.embedding = Vec::new();
        no_vector.embedding_model = cog_core::NO_EMBEDDING_MODEL.to_string();
        store.upsert(&no_vector).await.unwrap();

        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);

        backend.load().await.unwrap();
        backend.store_summary("default", &no_vector).await.unwrap();

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert!(
            hits.is_empty(),
            "an entry with no vector must not be reachable through vector search"
        );

        let found = backend.get_summary("default", "s1").await.unwrap();
        assert!(
            found.is_some(),
            "the entry itself stays stored and reachable by id"
        );
    }

    /// The index is a derived copy of the store, so an entry that loses its
    /// embedding must lose the point it used to have. Leaving it would let the
    /// store answer with a vector it no longer holds.
    #[tokio::test]
    async fn an_entry_that_loses_its_embedding_drops_its_index_point() {
        let store = Arc::new(DurableStubStore::new());
        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);

        backend
            .store_summary("default", &entry("s1"))
            .await
            .unwrap();
        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);

        let mut stripped = entry("s1");
        stripped.embedding = Vec::new();
        backend.store_summary("default", &stripped).await.unwrap();

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert!(
            hits.is_empty(),
            "the stale point must be deleted, not merely left unindexed"
        );
    }

    /// A search with no query vector has no similarity to rank by, so it must
    /// return nothing rather than the collection's first points.
    #[tokio::test]
    async fn a_search_without_a_query_vector_returns_nothing() {
        let store = Arc::new(DurableStubStore::new());
        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);

        backend
            .store_summary("default", &entry("s1"))
            .await
            .unwrap();

        let hits = backend
            .search_summary("default", &[], 10, None)
            .await
            .unwrap();
        assert!(hits.is_empty());
    }

    /// A backend that names a point after the entry hands back that same name on
    /// every insert, so a re-store looks like it has a previous point to clean up
    /// when what it is looking at is the point it just wrote. Deleting that leaves
    /// the row claiming a vector the collection no longer holds: the entry silently
    /// stops being found, and nothing in the entry store says so.
    #[tokio::test]
    async fn restoring_an_entry_keeps_its_point() {
        let store = Arc::new(DurableStubStore::new());
        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);
        backend.load().await.unwrap();

        backend
            .store_summary("default", &entry("s1"))
            .await
            .unwrap();

        let mut changed = entry("s1");
        changed.importance = 0.3;
        backend.update_summary("default", &changed).await.unwrap();

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "the entry must still be found after being written again"
        );
        assert_eq!(hits[0].entry.importance, 0.3);
    }

    /// An entry as it looks when it was stored before any model was configured:
    /// text and metadata, no vector, and the name that says so.
    fn unembedded(id: &str) -> SummaryEntry {
        SummaryEntry::new(
            id,
            "default",
            format!("text of {id}"),
            Vec::new(),
            cog_core::NO_EMBEDDING_MODEL,
            SourceRef::new(format!("memory://{id}"), "test/v1"),
        )
    }

    /// Stands in for a loaded model: one non-zero dense vector and one sparse entry
    /// per text, so a repaired row can be told apart from one that never got a
    /// vector, and both halves of the hybrid vector can be checked. `refuses` names
    /// the texts it will not embed, so a pass can be handed a row it has to report
    /// as failed while the rest go through.
    struct StubEmbedder {
        dim: usize,
        refuses: Vec<String>,
    }

    impl StubEmbedder {
        fn new(dim: usize) -> Self {
            Self {
                dim,
                refuses: Vec::new(),
            }
        }

        fn refusing(dim: usize, text: &str) -> Self {
            Self {
                dim,
                refuses: vec![text.to_string()],
            }
        }
    }

    #[async_trait]
    impl cog_core::EmbeddingProvider for StubEmbedder {
        async fn embed(&self, texts: Vec<String>) -> SFResult<Vec<Vec<f32>>> {
            let mut out = Vec::with_capacity(texts.len());
            for text in &texts {
                if self.refuses.iter().any(|r| r == text) {
                    return Err(SFError::Agent(format!("stub refuses {text}")));
                }
                out.push(vec![0.5f32; self.dim]);
            }
            Ok(out)
        }

        async fn embed_sparse(
            &self,
            texts: Vec<String>,
        ) -> SFResult<Vec<cog_core::SparseEmbedding>> {
            Ok(texts
                .iter()
                .map(|_| cog_core::SparseEmbedding::new(vec![1], vec![1.0]))
                .collect())
        }

        fn supports_sparse(&self) -> bool {
            true
        }

        fn model_id(&self) -> &str {
            "stub/v1"
        }

        fn dimension(&self) -> usize {
            self.dim
        }
    }

    /// A row stored before a model was configured carries no vector, so the index
    /// `load` rebuilds skips it and similarity search can never return it. The pass
    /// is the only thing that ever gives it one.
    #[tokio::test]
    async fn the_pass_gives_a_vector_to_a_row_stored_without_one() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&unembedded("old")).await.unwrap();

        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);
        backend.load().await.unwrap();

        let before = backend
            .search_summary("default", &[0.5, 0.5, 0.5, 0.5], 10, None)
            .await
            .unwrap();
        assert!(
            before.is_empty(),
            "a row with no vector must not be reachable by similarity"
        );

        let report = backend
            .backfill_embeddings(&StubEmbedder::new(4))
            .await
            .unwrap();
        assert_eq!(
            report,
            EmbeddingRepairReport {
                scanned: 1,
                backfilled: 1,
                already_embedded: 0,
                failed: 0,
            }
        );

        let after = backend
            .search_summary("default", &[0.5, 0.5, 0.5, 0.5], 10, None)
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].entry.id, "old");
        assert_eq!(
            after[0].entry.embedding_model, "stub/v1",
            "the row records which model made the vector"
        );
        assert!(
            after[0].entry.sparse_embedding.is_some(),
            "the repair fills both halves of the hybrid vector, not just the dense one"
        );
    }

    /// Re-storing a row that already has a vector would delete its point from the
    /// collection and insert another in its place. The pass therefore writes only
    /// rows that have none, and a second run over the same store finds nothing left.
    #[tokio::test]
    async fn the_pass_leaves_an_embedded_row_alone() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&entry("s1")).await.unwrap();

        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);
        backend.load().await.unwrap();

        let report = backend
            .backfill_embeddings(&StubEmbedder::new(4))
            .await
            .unwrap();
        assert_eq!(
            report,
            EmbeddingRepairReport {
                scanned: 1,
                backfilled: 0,
                already_embedded: 1,
                failed: 0,
            }
        );

        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "the entry's own point must survive the pass");
        assert_eq!(
            hits[0].entry.embedding_model, "test/v1",
            "and stay the vector it already had"
        );
    }

    /// One text the model cannot handle must not hold back the others: the pass
    /// counts it and keeps going, so everything it could take ends up indexed and
    /// the count says what was left behind.
    #[tokio::test]
    async fn a_row_the_model_refuses_is_counted_and_does_not_stop_the_pass() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&unembedded("bad")).await.unwrap();
        store.upsert(&unembedded("good")).await.unwrap();

        let backend =
            VectorSummaryBackend::new(Arc::new(cog_storage::MemoryVectorBackend::new()), 4)
                .with_store(store);
        backend.load().await.unwrap();

        let report = backend
            .backfill_embeddings(&StubEmbedder::refusing(4, "text of bad"))
            .await
            .unwrap();
        assert_eq!(
            report,
            EmbeddingRepairReport {
                scanned: 2,
                backfilled: 1,
                already_embedded: 0,
                failed: 1,
            }
        );

        let hits = backend
            .search_summary("default", &[0.5, 0.5, 0.5, 0.5], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entry.id, "good");
    }
}
