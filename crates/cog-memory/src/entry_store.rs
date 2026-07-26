//! Pluggable entry store for [`SummaryEntry`] typed data.
//! The [`VectorSummaryBackend`] delegates structured-data persistence to a
//! [`SummaryEntryStore`] so callers can choose between in-memory (testing)
//! and PostgreSQL (production) without changing the vector-backend layer.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use cog_core::SummaryEntry;
use cog_core::{SFError, SFResult};

/// Async store for [`SummaryEntry`] structured data.
/// Implementations handle CRUD only; vector indexing is the responsibility
/// of the separate [`cog_core::VectorBackend`] layer.
#[async_trait]
pub trait SummaryEntryStore: Send + Sync {
    /// Retrieve a single entry by namespace + id.
    async fn get(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>>;

    /// Retrieve multiple entries by namespace + ids.
    async fn get_many(&self, namespace: &str, ids: &[String]) -> SFResult<Vec<SummaryEntry>>;

    /// List all entries in a namespace.
    async fn list(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>>;

    /// Find entries whose `source_ref.raw_uri` matches.
    async fn list_by_raw_uri(&self, namespace: &str, raw_uri: &str) -> SFResult<Vec<SummaryEntry>>;

    /// Insert or overwrite an entry.
    async fn upsert(&self, entry: &SummaryEntry) -> SFResult<()>;

    /// Delete an entry by namespace + id.
    async fn delete(&self, namespace: &str, id: &str) -> SFResult<()>;

    /// List all entries across all namespaces (used for backup/export).
    async fn list_all(&self) -> SFResult<Vec<SummaryEntry>>;

    /// Whether this store persists data durably (e.g. PostgreSQL).
    /// When `true`, `VectorSummaryBackend` skips JSON file persistence
    /// and relies on the store itself for recovery.
    fn is_durable(&self) -> bool {
        false
    }
}

/// In-memory [`SummaryEntryStore`] backed by a [`HashMap`].
pub struct MemoryEntryStore {
    entries: RwLock<HashMap<(String, String), SummaryEntry>>,
}

impl MemoryEntryStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryEntryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SummaryEntryStore for MemoryEntryStore {
    async fn get(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
        let entries = self
            .entries
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(entries.get(&(namespace.into(), id.into())).cloned())
    }

    async fn get_many(&self, namespace: &str, ids: &[String]) -> SFResult<Vec<SummaryEntry>> {
        let entries = self
            .entries
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(e) = entries.get(&(namespace.into(), id.clone())) {
                out.push(e.clone());
            }
        }
        Ok(out)
    }

    async fn list(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
        let entries = self
            .entries
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(entries
            .values()
            .filter(|e| e.namespace == namespace)
            .cloned()
            .collect())
    }

    async fn list_by_raw_uri(&self, namespace: &str, raw_uri: &str) -> SFResult<Vec<SummaryEntry>> {
        let entries = self
            .entries
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(entries
            .values()
            .filter(|e| e.namespace == namespace && e.source_ref.raw_uri == raw_uri)
            .cloned()
            .collect())
    }

    async fn upsert(&self, entry: &SummaryEntry) -> SFResult<()> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        entries.insert((entry.namespace.clone(), entry.id.clone()), entry.clone());
        Ok(())
    }

    async fn delete(&self, namespace: &str, id: &str) -> SFResult<()> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        entries.remove(&(namespace.into(), id.into()));
        Ok(())
    }

    async fn list_all(&self) -> SFResult<Vec<SummaryEntry>> {
        let entries = self
            .entries
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(entries.values().cloned().collect())
    }
}
