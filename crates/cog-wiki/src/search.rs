use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use cog_core::{SFResult, SkillRegistry, VectorBackend};

use crate::indexer::WikiIndexer;

/// A single search result from any tier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WikiSearchResult {
    pub source: cog_core::SearchTier,
    pub doc_id: String,
    pub title: String,
    pub path: String,
    pub score: f32,
    pub excerpt: String,
}

/// Three-tier search orchestrator.
/// Search strategy:
/// 1. **Index tier** reads `index.md` to locate relevant directories, then
///    BM25-ranks documents within those directories.
/// 2. **Vector tier** (optional) performs semantic similarity search via the
///    configured `VectorBackend`.  Document-level embeddings are used (no
///    chunking).
/// 3. **Skill tier** (optional) queries the `SkillRegistry` for structured
///    skill matches that map to wiki domains.
///
/// Results from all tiers are merged and deduplicated by `doc_id`, keeping
/// the highest score.
pub struct ThreeTierSearch<'a> {
    indexer: &'a WikiIndexer,
    vector_backend: Option<&'a dyn VectorBackend>,
    skill_registry: Option<&'a SkillRegistry>,
}

impl<'a> ThreeTierSearch<'a> {
    pub fn new(indexer: &'a WikiIndexer) -> Self {
        Self {
            indexer,
            vector_backend: None,
            skill_registry: None,
        }
    }

    pub fn set_vector_backend(&mut self, backend: Option<&'a dyn VectorBackend>) {
        self.vector_backend = backend;
    }

    pub fn set_skill_registry(&mut self, registry: Option<&'a SkillRegistry>) {
        self.skill_registry = registry;
    }

    /// Execute the full three-tier search.
    pub async fn execute(&self, query: &str, top_k: usize) -> SFResult<Vec<WikiSearchResult>> {
        let mut merged: HashMap<String, WikiSearchResult> = HashMap::new();

        // Tier 1: Index + BM25
        let index_results = self.search_index_tier(query, top_k).await?;
        for r in index_results {
            merged.insert(r.doc_id.clone(), r);
        }

        // Tier 2: Vector (optional)
        if self.vector_backend.is_some() {
            let vector_results = self.search_vector_tier(query, top_k).await?;
            for r in vector_results {
                merged
                    .entry(r.doc_id.clone())
                    .and_modify(|existing| {
                        if r.score > existing.score {
                            *existing = r.clone();
                        }
                    })
                    .or_insert(r);
            }
        }

        // Tier 3: Skill (optional)
        if self.skill_registry.is_some() {
            let skill_results = self.search_skill_tier(query, top_k).await?;
            for r in skill_results {
                merged
                    .entry(r.doc_id.clone())
                    .and_modify(|existing| {
                        if r.score > existing.score {
                            *existing = r.clone();
                        }
                    })
                    .or_insert(r);
            }
        }

        let mut results: Vec<WikiSearchResult> = merged.into_values().collect();
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        Ok(results)
    }

    /// Execute a single tier only (synchronous wrapper).
    pub async fn execute_single_tier(
        &self,
        query: &str,
        tier: cog_core::SearchTier,
        top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        match tier {
            cog_core::SearchTier::Index => self.search_index_tier(query, top_k).await,
            cog_core::SearchTier::Vector => self.search_vector_tier(query, top_k).await,
            cog_core::SearchTier::Skill => self.search_skill_tier(query, top_k).await,
            cog_core::SearchTier::All => self.execute(query, top_k).await,
        }
    }

    async fn search_index_tier(
        &self,
        query: &str,
        top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        let bm25_results = self.indexer.search_bm25(query, top_k * 2);
        let mut results = Vec::with_capacity(bm25_results.len());

        for (doc_id, score) in bm25_results {
            if let Some(path) = self.indexer.doc_path(&doc_id) {
                // Build a simple excerpt from the first non-empty paragraph
                let excerpt = "...".to_string();
                results.push(WikiSearchResult {
                    source: cog_core::SearchTier::Index,
                    doc_id,
                    title: path.split('/').next_back().unwrap_or(path).to_string(),
                    path: path.to_string(),
                    score,
                    excerpt,
                });
            }
        }

        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        Ok(results)
    }

    async fn search_vector_tier(
        &self,
        _query: &str,
        _top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        // Vector search requires an embedding for the query.
        // In a real implementation this would call an embedding service
        // to convert the query text to a vector, then search the backend.
        // For now we return empty results — the integration point is wired.
        tracing::debug!("Vector tier search called but no query embedding available");
        Ok(vec![])
    }

    async fn search_skill_tier(
        &self,
        query: &str,
        top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        let Some(registry) = self.skill_registry else {
            return Ok(vec![]);
        };

        let skills = registry.search_by_keyword(query);
        let mut results = Vec::with_capacity(skills.len().min(top_k));

        for (i, skill) in skills.iter().take(top_k).enumerate() {
            // Map skill id to a wiki document path: e.g. "db-migration" -> "skills/db-migration.md"
            let doc_id = format!("skill-{}", skill.id);
            let path = format!("skills/{}.md", skill.id);
            results.push(WikiSearchResult {
                source: cog_core::SearchTier::Skill,
                doc_id,
                title: skill.name.clone(),
                path,
                score: 1.0 - (i as f32 * 0.1), // Decaying score by rank
                excerpt: skill.description.clone(),
            });
        }

        Ok(results)
    }
}
