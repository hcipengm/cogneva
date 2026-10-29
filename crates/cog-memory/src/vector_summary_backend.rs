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

/// How long startup keeps trying to (re)create the summary collection before it
/// reports the failure.
///
/// A rollout starts every workload at once and each of them drops and rebuilds
/// this one collection, so the creates arrive as a storm and the live Qdrant
/// answers slower than the client's own six-second timeout. Waiting is the
/// cheap side of this trade: under `strict_persistence=true` the alternative is
/// exiting, which restarts the process into the same storm while the readiness
/// probe is the only thing keeping traffic off, and a restart costs more than
/// the wait. Two minutes is several times the window a live storm needed (about
/// half a minute of failed attempts took a process down on 2026-10-25) and it
/// stays well inside the 300s startup probe, so a store that is genuinely
/// broken still surfaces instead of hanging.
const CREATE_COLLECTION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(120);

/// Delay before the second attempt; doubled for each attempt after that, up to
/// [`CREATE_COLLECTION_BACKOFF_DOUBLINGS`], so a long wait keeps asking often
/// instead of sleeping one long block.
const CREATE_COLLECTION_BACKOFF_MS: u64 = 500;

/// How many times the backoff doubles before it stops growing: 500ms × 2⁴ = 8s
/// between attempts.
const CREATE_COLLECTION_BACKOFF_DOUBLINGS: u32 = 4;

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

/// The summary file's name under the persistence directory.
///
/// Read on start and written on every mutation: two spellings would make the
/// read side report "nothing summarised yet" while the other kept writing.
const SUMMARY_FILE: &str = "summary.json";

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
    /// How long a failed collection create is retried before the failure is
    /// reported; [`CREATE_COLLECTION_DEADLINE`] unless a test says otherwise.
    collection_retry_deadline: std::time::Duration,
    /// Base delay between attempts; [`CREATE_COLLECTION_BACKOFF_MS`] unless a
    /// test says otherwise.
    collection_retry_backoff_ms: u64,
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
            collection_retry_deadline: CREATE_COLLECTION_DEADLINE,
            collection_retry_backoff_ms: CREATE_COLLECTION_BACKOFF_MS,
        }
    }

    /// Shorten the collection-create retry window.
    ///
    /// Test-only: the production window is minutes long, and a test that sits
    /// through it proves nothing the constants do not already pin.
    #[cfg(test)]
    fn with_collection_retry(mut self, deadline: std::time::Duration, backoff_ms: u64) -> Self {
        self.collection_retry_deadline = deadline;
        self.collection_retry_backoff_ms = backoff_ms;
        self
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
        self.create_collection_with_retry().await?;
        *initialized = true;
        Ok(())
    }

    /// Create the collection, retrying until [`CREATE_COLLECTION_DEADLINE`] and
    /// stopping as soon as the collection turns out to be there for someone
    /// else's reason.
    ///
    /// Two failures reach this loop that are not the server saying no: the
    /// client's timeout is shorter than the create can take (the live Qdrant
    /// logged the delete and the create of `summaries` six seconds apart while
    /// the caller had already given up), and the create is refused because a
    /// second process sharing the collection got there first — a rollout starts
    /// the workloads at once, and every one of them rebuilds this collection.
    /// Both leave the collection present, and with `strict_persistence=true`
    /// reporting either one takes the whole process down and restarts it, so
    /// each failed attempt asks the server what it actually has instead of
    /// treating the refusal as final. The cost of being wrong in this direction
    /// is a slower start instead of a crash loop.
    ///
    /// The bound is a deadline rather than a count of attempts because the
    /// failures are slow: with the client's own timeout in front of it, five
    /// attempts are about half a minute, and a storm of deletes and recreates
    /// from the workloads starting together outlasts that.
    async fn create_collection_with_retry(&self) -> SFResult<()> {
        let started = std::time::Instant::now();
        let mut attempt: u32 = 1;
        loop {
            let failure = match self
                .vector
                .create_collection(&self.collection, self.embedding_dim)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            match self.vector.collection_exists(&self.collection).await {
                Ok(true) => {
                    tracing::info!(
                        collection = %self.collection,
                        attempt,
                        error = %failure,
                        "vector collection create failed but the collection is there; continuing"
                    );
                    return Ok(());
                }
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    collection = %self.collection,
                    attempt,
                    error = %e,
                    "could not read whether the collection exists after a failed create"
                ),
            }
            let left = match self
                .collection_retry_deadline
                .checked_sub(started.elapsed())
            {
                Some(left) => left,
                None => return Err(failure),
            };
            let delay_ms = self
                .collection_retry_backoff_ms
                .saturating_mul(1u64 << (attempt - 1).min(CREATE_COLLECTION_BACKOFF_DOUBLINGS));
            tracing::warn!(
                collection = %self.collection,
                attempt,
                delay_ms,
                waited_secs = started.elapsed().as_secs(),
                error = %failure,
                "vector collection create failed; retrying"
            );
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms).min(left)).await;
            attempt += 1;
        }
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
            let path = dir.join(SUMMARY_FILE);
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
        self.create_collection_with_retry().await?;
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
        tokio::fs::write(dir.join(SUMMARY_FILE), data)
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

    /// The counters live on the rows, so the write goes to the entry store. The
    /// vector index is a derived copy of those rows and carries no count of its
    /// own — writing it there would put the reading somewhere a rebuild drops.
    async fn record_summary_retrieval(&self, ids: &[String]) -> SFResult<()> {
        self.store.record_retrieval(ids).await
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
        async fn record_retrieval(&self, ids: &[String]) -> SFResult<()> {
            self.inner.record_retrieval(ids).await
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

        let vector = Arc::new(cog_test_support::MemoryVectorBackend::new());
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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
            VectorSummaryBackend::new(Arc::new(cog_test_support::MemoryVectorBackend::new()), 4)
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

    /// A create that times out while the server is still working it is the
    /// one failure worth repeating: the next attempt finds the collection the
    /// server did make, and startup continues instead of exiting.
    struct SlowCreate {
        inner: cog_test_support::MemoryVectorBackend,
        failures: std::sync::atomic::AtomicUsize,
        attempts: std::sync::atomic::AtomicUsize,
    }

    impl SlowCreate {
        fn failing(times: usize) -> Self {
            Self {
                inner: cog_test_support::MemoryVectorBackend::new(),
                failures: std::sync::atomic::AtomicUsize::new(times),
                attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl VectorBackend for SlowCreate {
        async fn create_collection(&self, collection: &str, dimension: usize) -> SFResult<()> {
            let seen = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if seen < self.failures.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SFError::Validation(
                    "qdrant create collection failed: Timeout expired".into(),
                ));
            }
            self.inner.create_collection(collection, dimension).await
        }
        async fn delete_collection(&self, collection: &str) -> SFResult<()> {
            self.inner.delete_collection(collection).await
        }
        async fn insert(
            &self,
            collection: &str,
            vectors: Vec<Vec<f32>>,
            metadata: Vec<serde_json::Value>,
        ) -> SFResult<Vec<String>> {
            self.inner.insert(collection, vectors, metadata).await
        }
        async fn insert_sparse(
            &self,
            collection: &str,
            sparse: Vec<cog_core::contract::storage::SparseEmbedding>,
            metadata: Vec<serde_json::Value>,
        ) -> SFResult<Vec<String>> {
            self.inner.insert_sparse(collection, sparse, metadata).await
        }
        async fn search(
            &self,
            collection: &str,
            vector: &[f32],
            top_k: usize,
        ) -> SFResult<Vec<cog_core::contract::storage::VectorSearchResult>> {
            self.inner.search(collection, vector, top_k).await
        }
        async fn delete(&self, collection: &str, ids: &[String]) -> SFResult<()> {
            self.inner.delete(collection, ids).await
        }
        async fn collection_exists(&self, collection: &str) -> SFResult<bool> {
            self.inner.collection_exists(collection).await
        }
    }

    /// A create another writer won. The collection is there — a second process
    /// sharing it, or the server still finishing a call whose client had given
    /// up — and this caller is told no anyway. That refusal is convergence.
    struct LostCreateRace {
        inner: cog_test_support::MemoryVectorBackend,
        seeded: std::sync::atomic::AtomicBool,
        attempts: std::sync::atomic::AtomicUsize,
    }

    impl LostCreateRace {
        fn new() -> Self {
            Self {
                inner: cog_test_support::MemoryVectorBackend::new(),
                seeded: std::sync::atomic::AtomicBool::new(false),
                attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl VectorBackend for LostCreateRace {
        async fn create_collection(&self, collection: &str, dimension: usize) -> SFResult<()> {
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if !self.seeded.swap(true, std::sync::atomic::Ordering::SeqCst) {
                self.inner.create_collection(collection, dimension).await?;
            }
            Err(SFError::Validation(format!(
                "qdrant create collection failed: Error in the response: \
                 Wrong input: Collection `{collection}` already exists!"
            )))
        }
        async fn delete_collection(&self, collection: &str) -> SFResult<()> {
            self.inner.delete_collection(collection).await
        }
        async fn insert(
            &self,
            collection: &str,
            vectors: Vec<Vec<f32>>,
            metadata: Vec<serde_json::Value>,
        ) -> SFResult<Vec<String>> {
            self.inner.insert(collection, vectors, metadata).await
        }
        async fn insert_sparse(
            &self,
            collection: &str,
            sparse: Vec<cog_core::contract::storage::SparseEmbedding>,
            metadata: Vec<serde_json::Value>,
        ) -> SFResult<Vec<String>> {
            self.inner.insert_sparse(collection, sparse, metadata).await
        }
        async fn search(
            &self,
            collection: &str,
            vector: &[f32],
            top_k: usize,
        ) -> SFResult<Vec<cog_core::contract::storage::VectorSearchResult>> {
            self.inner.search(collection, vector, top_k).await
        }
        async fn delete(&self, collection: &str, ids: &[String]) -> SFResult<()> {
            self.inner.delete(collection, ids).await
        }
        async fn collection_exists(&self, collection: &str) -> SFResult<bool> {
            self.inner.collection_exists(collection).await
        }
    }

    /// A create refused because the collection already exists is the server
    /// answering "it is already what you asked for". Under
    /// `strict_persistence=true` reading that refusal as a failure takes the
    /// process down and restarts it, next to a writer that had done nothing
    /// wrong.
    #[tokio::test]
    async fn a_create_lost_to_another_writer_continues_on_the_existing_collection() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&entry("s1")).await.unwrap();
        let vector = Arc::new(LostCreateRace::new());
        let backend = VectorSummaryBackend::new(vector.clone(), 4).with_store(store);

        backend
            .load()
            .await
            .expect("the collection is there, so startup continues");

        assert_eq!(
            vector.attempts(),
            1,
            "the answered question ends the retries: one create, then the collection was there"
        );
        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "the rebuilt index still holds the entry");
    }

    /// The window is what makes the retries worth anything: a window that ends
    /// before the server finishes is no retry at all. The failures are slow —
    /// each one costs the client's own timeout — so the window is measured in
    /// minutes, and it has to fit inside the startup probe that keeps the
    /// container alive while it waits.
    #[test]
    fn the_retry_window_outlasts_a_collection_storm_and_fits_the_startup_probe() {
        assert!(
            CREATE_COLLECTION_DEADLINE >= std::time::Duration::from_secs(60),
            "the retry window is {:?}; a storm of deletes and recreates from the workloads \
             starting together ran longer than half a minute on 2026-10-25",
            CREATE_COLLECTION_DEADLINE
        );
        assert!(
            CREATE_COLLECTION_DEADLINE < std::time::Duration::from_secs(300),
            "the retry window is {:?}; the startup probe kills the container after 300s, so a \
             window past that is a window nobody waits in",
            CREATE_COLLECTION_DEADLINE
        );
        assert_eq!(
            CREATE_COLLECTION_BACKOFF_MS * (1u64 << CREATE_COLLECTION_BACKOFF_DOUBLINGS),
            8_000,
            "the backoff tops out at 8s, so a long wait still asks often"
        );
    }

    #[tokio::test]
    async fn a_create_that_times_out_is_retried_before_startup_fails() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&entry("s1")).await.unwrap();
        let vector = Arc::new(SlowCreate::failing(2));
        let backend = VectorSummaryBackend::new(vector.clone(), 4).with_store(store);

        backend.load().await.expect("the third attempt succeeds");

        assert_eq!(vector.attempts(), 3, "each failed create is retried once");
        let hits = backend
            .search_summary("default", &[1.0, 0.0, 0.0, 0.0], 10, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "the rebuilt index still holds the entry");
    }

    /// A storm lasts longer than a handful of attempts. Every workload in a
    /// rollout drops and rebuilds this collection, and each of them is slower
    /// than the client's timeout, so a bound counted in attempts gives up in
    /// the middle of exactly the situation it exists for.
    #[tokio::test]
    async fn a_create_storm_outlasting_a_handful_of_attempts_is_waited_out() {
        let store = Arc::new(DurableStubStore::new());
        store.upsert(&entry("s1")).await.unwrap();
        // Nine creates fail before one gets through — more attempts than any
        // fixed count this loop ever had.
        let vector = Arc::new(SlowCreate::failing(8));
        let backend = VectorSummaryBackend::new(vector.clone(), 4)
            .with_store(store)
            .with_collection_retry(std::time::Duration::from_millis(200), 1);

        backend
            .load()
            .await
            .expect("the storm ends and startup continues");

        assert_eq!(vector.attempts(), 9, "the loop kept trying past the storm");
    }

    /// Retries are bounded: a collection that cannot be created at all still
    /// reports the failure rather than starting with a silently empty index.
    #[tokio::test]
    async fn a_create_that_keeps_failing_gives_up_after_the_bound() {
        let store = Arc::new(DurableStubStore::new());
        let vector = Arc::new(SlowCreate::failing(usize::MAX));
        let backend = VectorSummaryBackend::new(vector.clone(), 4)
            .with_store(store)
            .with_collection_retry(std::time::Duration::from_millis(50), 1);

        let err = backend.load().await.expect_err("the failure is reported");
        assert!(
            err.to_string().contains("Timeout expired"),
            "the last error survives the retries: {err}"
        );
        assert!(
            vector.attempts() > 1,
            "the bound is a wait, not the first failure: {} attempt(s)",
            vector.attempts()
        );
    }
}
