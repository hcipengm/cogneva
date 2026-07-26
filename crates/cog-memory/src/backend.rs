use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use cog_core::{
    DecayReport, MemoryMetrics, RawSource, SchemaEntry, SchemaSearchResult, SummaryEntry,
    SummarySearchResult, UnifiedSearchResult,
};
use cog_core::{SFError, SFResult};

// ─── In-memory implementation ──────────────────────────────────────────

#[derive(Debug, Default)]
struct MemoryStore {
    raw: HashMap<String, RawSource>,
    schema: HashMap<String, SchemaEntry>,
    summary: HashMap<String, SummaryEntry>,
}

/// In-memory three-layer memory backend for testing and local development.
/// All data is held in process memory and is lost on shutdown.
#[derive(Debug, Default)]
pub struct MemoryMemoryBackend {
    store: RwLock<MemoryStore>,
    metrics: RwLock<MemoryMetrics>,
}

impl MemoryMemoryBackend {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(MemoryStore::default()),
            metrics: RwLock::new(MemoryMetrics::default()),
        }
    }

    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
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

#[async_trait]
impl cog_core::MemoryBackend for MemoryMemoryBackend {
    async fn archive_raw(&self, source: &RawSource) -> SFResult<String> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let uri = format!("memory://{}", source.id);
        store.raw.insert(source.id.clone(), source.clone());
        metrics.raw_archived += 1;
        Ok(uri)
    }

    async fn get_raw(&self, namespace: &str, id: &str) -> SFResult<Option<RawSource>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let result = store
            .raw
            .get(id)
            .filter(|r| r.namespace == namespace)
            .cloned();
        if result.is_some() {
            metrics.raw_retrieved += 1;
        }
        Ok(result)
    }

    async fn list_raw(&self, namespace: &str, prefix: Option<&str>) -> SFResult<Vec<String>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut ids: Vec<String> = store
            .raw
            .values()
            .filter(|r| {
                r.namespace == namespace && prefix.is_none_or(|p| r.content_type.starts_with(p))
            })
            .map(|r| r.id.clone())
            .collect();
        ids.sort();
        Ok(ids)
    }

    async fn delete_raw(&self, namespace: &str, id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(r) = store.raw.get(id) {
            if r.namespace == namespace {
                store.raw.remove(id);
            }
        }
        Ok(())
    }

    async fn store_schema(&self, _namespace: &str, entry: &SchemaEntry) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.schema.insert(entry.id.clone(), entry.clone());
        metrics.schema_stored += 1;
        Ok(())
    }

    async fn get_schema(&self, namespace: &str, id: &str) -> SFResult<Option<SchemaEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .schema
            .get(id)
            .filter(|e| e.namespace == namespace)
            .cloned())
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
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let query_lower = query.to_lowercase();
        let mut results: Vec<SchemaSearchResult> = store
            .schema
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
        metrics.schema_searched += 1;
        Ok(results)
    }

    async fn schema_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SchemaEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let raw_uri = format!("memory://{}", raw_id);
        Ok(store
            .schema
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
            .schema
            .values()
            .filter(|e| e.namespace == namespace)
            .cloned()
            .collect())
    }

    async fn delete_schema(&self, namespace: &str, id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(e) = store.schema.get(id) {
            if e.namespace == namespace {
                store.schema.remove(id);
            }
        }
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
        for entry in store.schema.values() {
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
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let existing = store
            .schema
            .values()
            .find(|e| e.namespace == namespace && e.key == entry.key)
            .cloned();
        if let Some(mut existing) = existing {
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
            store.schema.insert(existing.id.clone(), existing);
        } else {
            store.schema.insert(entry.id.clone(), entry.clone());
        }
        metrics.schema_updated += 1;
        Ok(())
    }

    async fn store_summary(&self, _namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.summary.insert(entry.id.clone(), entry.clone());
        metrics.summary_stored += 1;
        Ok(())
    }

    async fn get_summary(&self, namespace: &str, id: &str) -> SFResult<Option<SummaryEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .summary
            .get(id)
            .filter(|e| e.namespace == namespace)
            .cloned())
    }

    async fn search_summary(
        &self,
        namespace: &str,
        query_embedding: &[f32],
        top_k: usize,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> SFResult<Vec<SummarySearchResult>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut results: Vec<SummarySearchResult> = store
            .summary
            .values()
            .filter(|e| {
                e.namespace == namespace
                    && time_range.as_ref().is_none_or(|(start, end)| {
                        e.generated_at >= *start && e.generated_at <= *end
                    })
            })
            .map(|e| {
                SummarySearchResult::new(
                    e.clone(),
                    Self::cosine_similarity(query_embedding, &e.embedding),
                )
            })
            .collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        metrics.summary_searched += 1;
        Ok(results)
    }

    async fn summary_for_raw(&self, namespace: &str, raw_id: &str) -> SFResult<Vec<SummaryEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let raw_uri = format!("memory://{}", raw_id);
        Ok(store
            .summary
            .values()
            .filter(|e| e.namespace == namespace && e.source_ref.raw_uri == raw_uri)
            .cloned()
            .collect())
    }

    async fn list_summary(&self, namespace: &str) -> SFResult<Vec<SummaryEntry>> {
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        Ok(store
            .summary
            .values()
            .filter(|e| e.namespace == namespace)
            .cloned()
            .collect())
    }

    async fn delete_summary(&self, namespace: &str, id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        if let Some(e) = store.summary.get(id) {
            if e.namespace == namespace {
                store.summary.remove(id);
            }
        }
        Ok(())
    }

    async fn update_summary(&self, _namespace: &str, entry: &SummaryEntry) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        store.summary.insert(entry.id.clone(), entry.clone());
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
        let _guard = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
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
        let store = self
            .store
            .read()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let query_lower = query.to_lowercase();

        // Schema search (text match)
        let mut results: Vec<UnifiedSearchResult> = store
            .schema
            .values()
            .filter(|e| {
                e.namespace == namespace
                    && (e.name.to_lowercase().contains(&query_lower)
                        || e.key.to_lowercase().contains(&query_lower))
            })
            .map(|e| {
                UnifiedSearchResult::Schema(SchemaSearchResult {
                    entry: e.clone(),
                    score: 1.0,
                })
            })
            .collect();

        // Summary search
        if let Some(emb) = embedding {
            let mut summaries: Vec<UnifiedSearchResult> = store
                .summary
                .values()
                .filter(|e| {
                    e.namespace == namespace
                        && time_range.as_ref().is_none_or(|(start, end)| {
                            e.generated_at >= *start && e.generated_at <= *end
                        })
                })
                .map(|e| {
                    let score = Self::cosine_similarity(emb, &e.embedding);
                    UnifiedSearchResult::Summary(SummarySearchResult::new(e.clone(), score))
                })
                .collect();
            summaries.sort_by(|a, b| {
                let score_a = match a {
                    UnifiedSearchResult::Summary(s) => s.score,
                    _ => 0.0,
                };
                let score_b = match b {
                    UnifiedSearchResult::Summary(s) => s.score,
                    _ => 0.0,
                };
                score_b
                    .partial_cmp(&score_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.extend(summaries);
        } else {
            let summaries: Vec<UnifiedSearchResult> = store
                .summary
                .values()
                .filter(|e| {
                    e.namespace == namespace
                        && e.text.to_lowercase().contains(&query_lower)
                        && time_range.as_ref().is_none_or(|(start, end)| {
                            e.generated_at >= *start && e.generated_at <= *end
                        })
                })
                .map(|e| UnifiedSearchResult::Summary(SummarySearchResult::new(e.clone(), 1.0)))
                .collect();
            results.extend(summaries);
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

        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let mut metrics = self
            .metrics
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;

        store.raw.insert(id.clone(), raw.clone());
        metrics.raw_archived += 1;

        let summary = SummaryEntry::new(
            &id,
            namespace,
            text,
            vec![0.0f32; 128],
            "explicit",
            cog_core::SourceRef::new(format!("memory://{}", id), "explicit/v1"),
        )
        .with_importance(importance);

        store.summary.insert(id.clone(), summary);
        metrics.summary_stored += 1;

        Ok(())
    }

    async fn forget(&self, namespace: &str, id: &str) -> SFResult<()> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;

        // Delete raw source
        if let Some(r) = store.raw.get(id) {
            if r.namespace == namespace {
                store.raw.remove(id);
            }
        }

        // Delete schema entries whose source_ref.raw_uri matches memory://{id}
        let raw_uri = format!("memory://{}", id);
        let schema_ids_to_remove: Vec<String> = store
            .schema
            .values()
            .filter(|e| e.namespace == namespace && e.source_ref.raw_uri == raw_uri)
            .map(|e| e.id.clone())
            .collect();
        for sid in schema_ids_to_remove {
            store.schema.remove(&sid);
        }

        // Delete summaries whose source_ref.raw_uri matches memory://{id}
        let summary_ids_to_remove: Vec<String> = store
            .summary
            .values()
            .filter(|e| e.namespace == namespace && e.source_ref.raw_uri == raw_uri)
            .map(|e| e.id.clone())
            .collect();
        for sid in summary_ids_to_remove {
            store.summary.remove(&sid);
        }

        Ok(())
    }

    async fn decay(
        &self,
        namespace: &str,
        age_threshold_secs: u64,
        importance_threshold: f32,
    ) -> SFResult<DecayReport> {
        let mut store = self
            .store
            .write()
            .map_err(|_| SFError::Agent("lock poisoned".into()))?;
        let now = Utc::now();
        let mut decayed = 0usize;

        for entry in store.summary.values_mut() {
            if entry.namespace != namespace {
                continue;
            }
            let age_secs = (now - entry.generated_at).num_seconds() as u64;
            if age_secs > age_threshold_secs && entry.importance < importance_threshold {
                entry.embedding = entry
                    .embedding
                    .iter()
                    .map(|v| (v * 100.0).round() / 100.0)
                    .collect();
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
