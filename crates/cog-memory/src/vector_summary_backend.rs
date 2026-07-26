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

    /// Load entries and re-index their embeddings for similarity search.
    /// If the entry store is durable (e.g. PostgreSQL), reads from `store.list_all()`
    /// and re-indexes into the vector backend.  Otherwise, falls back to
    /// `<persist_dir>/summary.json`.
    pub async fn load(&self) -> SFResult<()> {
        let _ = self.ensure_collection().await;

        if self.store.is_durable() {
            let entries = self.store.list_all().await?;
            for entry in entries {
                let meta = serde_json::json!({
                    "id": entry.id,
                    "text": entry.text,
                    "raw_uri": entry.source_ref.raw_uri,
                });
                let _ = self
                    .vector
                    .insert(
                        &self.collection,
                        vec![entry.embedding.clone()],
                        vec![meta.clone()],
                    )
                    .await;
                if let Some(ref sparse) = entry.sparse_embedding {
                    let _ = self
                        .vector
                        .insert_sparse(&self.collection, vec![sparse.clone()], vec![meta])
                        .await;
                }
            }
            return Ok(());
        }

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

        for entry in entries {
            self.store.upsert(&entry).await?;

            let meta = serde_json::json!({
                "id": entry.id,
                "text": entry.text,
                "raw_uri": entry.source_ref.raw_uri,
            });
            let _ = self
                .vector
                .insert(
                    &self.collection,
                    vec![entry.embedding.clone()],
                    vec![meta.clone()],
                )
                .await;
            if let Some(ref sparse) = entry.sparse_embedding {
                let _ = self
                    .vector
                    .insert_sparse(&self.collection, vec![sparse.clone()], vec![meta])
                    .await;
            }
        }
        Ok(())
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
