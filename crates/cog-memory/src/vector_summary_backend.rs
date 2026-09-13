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
        let vec_id = self
            .vector
            .insert(
                &self.collection,
                vec![entry.embedding.clone()],
                vec![meta.clone()],
            )
            .await
            .ok()
            .and_then(|mut ids| ids.pop());
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

        let stale_vec_id = {
            let mut vec_ids = self
                .vec_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            vec_ids.insert(entry.id.clone(), vec_id)
        };
        if let Some(prev) = stale_vec_id {
            let _ = self.vector.delete(&self.collection, &[prev]).await;
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
                out.push(SummarySearchResult::new(entry.clone(), vr.score));
            }
        }
        Ok(out)
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
                out.push(SummarySearchResult::new(entry.clone(), vr.score));
            }
        }
        Ok(out)
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

        let stale_vec_id = {
            let mut vec_ids = self
                .vec_ids
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            vec_ids.insert(entry.id.clone(), vec_id)
        };
        if let Some(prev) = stale_vec_id {
            let _ = self.vector.delete(&self.collection, &[prev]).await;
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
}
