//! In-memory stores for the tests of every crate that needs one.
//!
//! [`VectorBackend`] and [`ObjectBackend`] are core contracts, so any
//! conforming implementation is a legitimate stand-in when the subject under
//! test is a crate's logic over that contract. Using the store a *production*
//! crate ships instead would make the test a measurement of that crate -- a
//! change to its id rule or its scoring turns the test red, and the failure is
//! reported where nothing about that crate was being tested -- besides adding a
//! dependency edge from a crate to a sibling it has no business naming.
//!
//! What is deliberately missing: sparse vectors. The summary layer recalls on
//! dense vectors only, and a sparse implementation here would be a second
//! behaviour nobody asserts.

use std::collections::HashMap;
use std::sync::RwLock;

use cog_core::contract::storage::{SparseEmbedding, VectorBackend, VectorSearchResult};
use cog_core::{ObjectBackend, SFError, SFResult};

/// One stored point: the vector and the metadata the caller attached to it.
#[derive(Debug, Clone)]
struct Point {
    vector: Vec<f32>,
    metadata: serde_json::Value,
}

/// A collection of points, keyed by the id the caller knows them by.
#[derive(Debug, Default)]
pub struct MemoryVectorBackend {
    collections: RwLock<HashMap<String, HashMap<String, Point>>>,
}

impl MemoryVectorBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot / (norm_a * norm_b)
        }
    }
}

#[async_trait::async_trait]
impl VectorBackend for MemoryVectorBackend {
    async fn create_collection(&self, collection: &str, _dimension: usize) -> SFResult<()> {
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        cols.entry(collection.to_string()).or_default();
        Ok(())
    }

    async fn delete_collection(&self, collection: &str) -> SFResult<()> {
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        cols.remove(collection);
        Ok(())
    }

    async fn insert(
        &self,
        collection: &str,
        vectors: Vec<Vec<f32>>,
        metadata: Vec<serde_json::Value>,
    ) -> SFResult<Vec<String>> {
        if vectors.len() != metadata.len() {
            return Err(SFError::Agent(
                "vectors and metadata length mismatch".into(),
            ));
        }
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let store = cols.entry(collection.to_string()).or_default();
        let mut ids = Vec::with_capacity(vectors.len());
        for (vector, metadata) in vectors.into_iter().zip(metadata) {
            let id = match metadata.get("id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => {
                    let mut n = store.len();
                    while store.contains_key(&format!("vec-{n}")) {
                        n += 1;
                    }
                    format!("vec-{n}")
                }
            };
            store.insert(id.clone(), Point { vector, metadata });
            ids.push(id);
        }
        Ok(ids)
    }

    async fn insert_sparse(
        &self,
        _collection: &str,
        _sparse: Vec<SparseEmbedding>,
        _metadata: Vec<serde_json::Value>,
    ) -> SFResult<Vec<String>> {
        Err(SFError::Validation(
            "the test store keeps dense vectors only".into(),
        ))
    }

    async fn search(
        &self,
        collection: &str,
        vector: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let cols = self
            .collections
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let Some(store) = cols.get(collection) else {
            return Err(SFError::Agent(format!("collection {collection} not found")));
        };
        let mut results: Vec<VectorSearchResult> = store
            .iter()
            .map(|(id, point)| VectorSearchResult {
                id: id.clone(),
                score: Self::cosine(vector, &point.vector),
                metadata: point.metadata.clone(),
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        Ok(results)
    }

    async fn delete(&self, collection: &str, ids: &[String]) -> SFResult<()> {
        let mut cols = self
            .collections
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(store) = cols.get_mut(collection) {
            for id in ids {
                store.remove(id);
            }
        }
        Ok(())
    }

    async fn collection_exists(&self, collection: &str) -> SFResult<bool> {
        let cols = self
            .collections
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(cols.contains_key(collection))
    }
}

/// An in-memory object store, for the same reason as the vector one above:
/// the raw layer of composite memory needs an [`ObjectBackend`] to write to,
/// and what a test of another layer counts does not depend on where that
/// backend keeps its bytes.
#[derive(Debug, Default)]
pub struct MemoryObjectBackend {
    objects: RwLock<HashMap<String, Vec<u8>>>,
}

impl MemoryObjectBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ObjectBackend for MemoryObjectBackend {
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String> {
        let mut objects = self
            .objects
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        objects.insert(key.to_string(), data.to_vec());
        Ok(key.to_string())
    }

    async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
        let objects = self
            .objects
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(objects.get(key).cloned())
    }

    async fn delete(&self, key: &str) -> SFResult<()> {
        let mut objects = self
            .objects
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        objects.remove(key);
        Ok(())
    }

    /// Nothing here is reachable from outside the process, so there is no URL
    /// to hand out. Returning the key says that plainly: a caller that treats
    /// it as a link gets a name, not a silent 404 from somewhere else.
    async fn presign_url(&self, key: &str, _expiry_secs: u64) -> SFResult<String> {
        Ok(key.to_string())
    }

    async fn exists(&self, key: &str) -> SFResult<bool> {
        let objects = self
            .objects
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(objects.contains_key(key))
    }

    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let objects = self
            .objects
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut keys: Vec<String> = objects
            .keys()
            .filter(|key| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }
}
