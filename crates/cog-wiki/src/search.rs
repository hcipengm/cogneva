use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use cog_core::{SFError, SFResult, SkillRegistry};

use crate::indexer::WikiIndexer;
use crate::unified_knowledge_backend::clip;
use crate::vector_index::WikiVectorIndex;

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
/// 2. **Vector tier** (optional) performs semantic similarity search through a
///    [`WikiVectorIndex`].  Document-level embeddings are used (no chunking).
/// 3. **Skill tier** (optional) queries the `SkillRegistry` for structured
///    skill matches that map to wiki domains.
///
/// Results from all tiers are merged and deduplicated by `doc_id`, keeping
/// the highest score.
///
/// The vector tier takes an index rather than a bare `VectorBackend` because a
/// query arrives as text and a backend cannot turn text into a vector on its
/// own: handed a backend with no embedder beside it, the tier has no way to
/// answer, which is how the tier came to be written as a stub that returned
/// nothing while the backend field made it look wired.
pub struct ThreeTierSearch<'a> {
    indexer: &'a WikiIndexer,
    vector_index: Option<&'a WikiVectorIndex>,
    skill_registry: Option<&'a SkillRegistry>,
}

impl<'a> ThreeTierSearch<'a> {
    pub fn new(indexer: &'a WikiIndexer) -> Self {
        Self {
            indexer,
            vector_index: None,
            skill_registry: None,
        }
    }

    pub fn set_vector_index(&mut self, index: Option<&'a WikiVectorIndex>) {
        self.vector_index = index;
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
        if self.vector_index.is_some() {
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
                // Not an excerpt, and deliberately not an empty string: a BM25
                // hit carries the terms it matched and the page they were in,
                // and this tier's index holds no text to draw one from. An
                // empty excerpt would read as a page with no words in it, so
                // the placeholder stays visible until the tier can be handed
                // the page it is describing. A reader that needs the words
                // reads the page from the backend by `path`.
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
        query: &str,
        top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        // Asking for a tier this search does not hold is a refusal, not an
        // empty answer: `execute_single_tier(Vector)` on a search built without
        // an index would otherwise be indistinguishable from a wiki in which
        // nothing is similar to the query.
        let Some(index) = self.vector_index else {
            return Err(SFError::Config(
                "the vector tier was asked for on a search with no vector index attached".into(),
            ));
        };

        let hits = index.search_text(query, top_k).await?;
        Ok(hits
            .into_iter()
            .map(|hit| WikiSearchResult {
                source: cog_core::SearchTier::Vector,
                doc_id: hit.document.id,
                title: hit.document.title,
                path: hit.document.path,
                score: hit.score,
                // The excerpt carries the page's text because the tier's own
                // result type has nowhere else to put it, and clipped to the
                // same bound the retrieval read side applies so that the two
                // do not hand one caller rows of two different sizes.
                excerpt: clip(&hit.document.content, crate::WIKI_PAGE_PROMPT_CHARS),
            })
            .collect())
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
