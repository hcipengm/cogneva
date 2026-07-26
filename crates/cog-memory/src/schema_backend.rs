//! Pluggable Schema-layer backend.
//! The Schema layer (Layer 1) holds structured, queryable facts extracted from
//! raw sources.  In production this is typically backed by PostgreSQL or a
//! graph database; for tests and local development the [`MemorySchemaBackend`]
//! provided here keeps everything in a `HashMap` with optional JSON persistence.

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use cog_core::{SFError, SFResult, SchemaBackend};
use cog_core::{SchemaEntry, SchemaSearchResult};

/// In-memory schema backend with optional JSON persistence.
/// Stores all entries in a `HashMap`.  When a persistence directory is set
/// via [`MemorySchemaBackend::set_persist_dir`], the backend reads
/// `<dir>/schema.json` on [`load`] and writes it back on every mutation.
#[derive(Default)]
pub struct MemorySchemaBackend {
    store: RwLock<HashMap<String, SchemaEntry>>,
    persist_dir: RwLock<Option<PathBuf>>,
}

impl MemorySchemaBackend {
    /// Create an empty in-memory schema backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder-style helper that sets the persistence directory.
    pub fn with_persist_dir(self, dir: impl Into<PathBuf>) -> Self {
        self.set_persist_dir(dir);
        self
    }

    /// Configure (or update) the persistence directory.
    /// Subsequent calls to [`store_schema`](SchemaBackend::store_schema)
    /// and [`delete_schema`](SchemaBackend::delete_schema) will write the
    /// current store to `<dir>/schema.json`.
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

    /// Load any previously persisted entries from `<persist_dir>/schema.json`.
    /// Has no effect if the persistence directory is unset or the file does
    /// not exist.
    pub async fn load(&self) -> SFResult<()> {
        let dir = match self.current_persist_dir()? {
            Some(d) => d,
            None => return Ok(()),
        };

        let path = dir.join("schema.json");
        if !path.exists() {
            return Ok(());
        }

        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| SFError::Agent(format!("read schema.json failed: {}", e)))?;
        let entries: Vec<SchemaEntry> = serde_json::from_slice(&data)
            .map_err(|e| SFError::Agent(format!("parse schema.json failed: {}", e)))?;
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        for entry in entries {
            store.insert(entry.id.clone(), entry);
        }
        Ok(())
    }

    /// Persist the current store to `<persist_dir>/schema.json`.
    /// Has no effect if the persistence directory is unset.
    pub async fn persist(&self) -> SFResult<()> {
        let dir = match self.current_persist_dir()? {
            Some(d) => d,
            None => return Ok(()),
        };
        let data = {
            let store = self
                .store
                .read()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            let entries: Vec<SchemaEntry> = store.values().cloned().collect();
            serde_json::to_vec_pretty(&entries)
                .map_err(|e| SFError::Agent(format!("serialize schema failed: {}", e)))?
        };
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| SFError::Agent(format!("create persist dir failed: {}", e)))?;
        tokio::fs::write(dir.join("schema.json"), data)
            .await
            .map_err(|e| SFError::Agent(format!("write schema.json failed: {}", e)))?;
        Ok(())
    }
}

#[async_trait]
impl SchemaBackend for MemorySchemaBackend {
    async fn store_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        {
            let mut store = self
                .store
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            store.insert(entry.id.clone(), entry.clone());
        }
        self.persist().await?;
        Ok(())
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store.get(id).filter(|e| e.namespace == namespace).cloned())
    }

    async fn search_schema(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> SFResult<Vec<SchemaSearchResult>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let query_lower = query.to_lowercase();
        let mut results: Vec<SchemaSearchResult> = store
            .values()
            .filter(|e| {
                e.namespace == namespace
                    && (e.name.to_lowercase().contains(&query_lower)
                        || e.key.to_lowercase().contains(&query_lower))
            })
            .map(|e| SchemaSearchResult {
                entry: e.clone(),
                score: 1.0,
            })
            .collect();
        results.truncate(limit);
        Ok(results)
    }

    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        let raw_uri = format!("memory://{}", raw_id);
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .values()
            .filter(|e| e.namespace == namespace && e.source_ref.raw_uri == raw_uri)
            .cloned()
            .collect())
    }

    async fn list_schema(&self, namespace: &str) -> SFResult<Vec<SchemaEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .values()
            .filter(|e| e.namespace == namespace)
            .cloned()
            .collect())
    }

    async fn delete_schema(&self, _namespace: &str, id: &str) -> SFResult<()> {
        {
            let mut store = self
                .store
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            store.remove(id);
        }
        self.persist().await?;
        Ok(())
    }

    async fn query_relations(
        &self,
        namespace: &str,
        entity: &str,
        direction: cog_core::RelationDirection,
        relation_type: Option<&str>,
    ) -> SFResult<Vec<SchemaEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut results = Vec::new();
        for entry in store.values() {
            if entry.namespace != namespace || entry.kind != cog_core::SchemaKind::Relation {
                continue;
            }
            let from = entry.properties.get("from").and_then(|v| v.as_str());
            let to = entry.properties.get("to").and_then(|v| v.as_str());
            let rel_type = entry
                .properties
                .get("relation_type")
                .and_then(|v| v.as_str());

            let matches_direction = match direction {
                cog_core::RelationDirection::From => from == Some(entity),
                cog_core::RelationDirection::To => to == Some(entity),
                cog_core::RelationDirection::Both => from == Some(entity) || to == Some(entity),
            };

            let matches_type = relation_type.is_none_or(|rt| rel_type == Some(rt));

            if matches_direction && matches_type {
                results.push(entry.clone());
            }
        }
        Ok(results)
    }

    async fn update_schema(&self, namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        {
            let mut store = self
                .store
                .write()
                .map_err(|_| SFError::Agent("lock poisoned".into()))?;
            let existing = store
                .values()
                .find(|e| e.namespace == namespace && e.key == entry.key)
                .cloned();
            if let Some(mut existing) = existing {
                // Merge properties: new properties overwrite old ones at the top level.
                if let (Some(existing_map), Some(new_map)) = (
                    existing.properties.as_object_mut(),
                    entry.properties.as_object(),
                ) {
                    for (k, v) in new_map {
                        existing_map.insert(k.clone(), v.clone());
                    }
                } else {
                    existing.properties = entry.properties.clone();
                }
                existing.source_ref = entry.source_ref.clone();
                existing.extracted_at = entry.extracted_at;
                existing.confidence = entry.confidence;
                store.insert(existing.id.clone(), existing);
            } else {
                store.insert(entry.id.clone(), entry.clone());
            }
        }
        self.persist().await?;
        Ok(())
    }
}
