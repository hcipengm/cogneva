use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A typed representation of a wiki document returned by read/search operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WikiDocument {
    pub id: String,
    pub path: String,
    pub title: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

/// Typed search result returned by wiki search operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WikiSearchResult {
    pub document: WikiDocument,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_type: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub highlights: Vec<String>,
}

/// Tier used by the three-tier search architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchTier {
    /// BM25 / keyword index tier.
    Index,
    /// Vector / semantic similarity tier.
    Vector,
    /// Skill registry / structured lookup tier.
    Skill,
    /// All tiers combined (hybrid).
    All,
}

/// Wiki backend trait for document ingestion, retrieval, and knowledge-base search.
/// Implementations target local filesystem (markdown) or remote wiki stores.
#[async_trait]
pub trait WikiBackend: Send + Sync {
    async fn health_check(&self) -> bool;
    fn provider_name(&self) -> &str;

    /// Ingest a markdown document at the given relative path.
    ///
    /// An index write, not a reading of the document: this spends storage and a
    /// round trip, never a model call, so its cost does not move with how much
    /// text the caller has and it is safe to call on content nobody has looked
    /// at. That property is what callers of this trait rely on, and a backend
    /// that reached for a model here would spend money on a path whose name
    /// says "store" -- distilling a source is the maintainer trait's ingest,
    /// which is the one that costs a model call per source.
    async fn ingest_document(&self, relative_path: &str, content: &str) -> crate::SFResult<()>;

    /// Read a single document by its relative path.
    /// Default returns an error indicating the operation is not supported.
    async fn read_document(&self, relative_path: &str) -> crate::SFResult<WikiDocument> {
        let _ = relative_path;
        Err(crate::SFError::NotImplemented("read_document".into()))
    }

    /// Update an existing document. Creates if absent.
    /// Default returns an error indicating the operation is not supported.
    async fn update_document(&self, relative_path: &str, content: &str) -> crate::SFResult<()> {
        let _ = (relative_path, content);
        Err(crate::SFError::NotImplemented("update_document".into()))
    }

    /// Delete a document by its relative path.
    /// Default returns an error indicating the operation is not supported.
    async fn delete_document(&self, relative_path: &str) -> crate::SFResult<()> {
        let _ = relative_path;
        Err(crate::SFError::NotImplemented("delete_document".into()))
    }

    /// Search the wiki for documents matching the query.
    async fn search(&self, query: &str, top_k: usize) -> crate::SFResult<Vec<WikiSearchResult>> {
        let _ = (query, top_k);
        Err(crate::SFError::NotImplemented("search".into()))
    }

    /// Semantic search using an embedding vector.
    /// Default returns an error indicating the operation is not supported.
    async fn semantic_search(
        &self,
        query_embedding: &[f32],
        top_k: usize,
    ) -> crate::SFResult<Vec<WikiSearchResult>> {
        let _ = (query_embedding, top_k);
        Err(crate::SFError::NotImplemented("semantic_search".into()))
    }

    /// Search with explicit tier selection (Index / Vector / Skill / All).
    /// Default delegates to [`Self::search`] for Index tier and returns an error for others.
    async fn search_with_tier(
        &self,
        query: &str,
        top_k: usize,
        tier: SearchTier,
    ) -> crate::SFResult<Vec<WikiSearchResult>> {
        match tier {
            SearchTier::Index | SearchTier::All => self.search(query, top_k).await,
            _ => Err(crate::SFError::NotImplemented(format!(
                "search_with_tier: {:?}",
                tier
            ))),
        }
    }

    /// Search documents by tags.
    /// Default returns an error indicating the operation is not supported.
    async fn search_by_tags(
        &self,
        tags: &[String],
        top_k: usize,
    ) -> crate::SFResult<Vec<WikiSearchResult>> {
        let _ = (tags, top_k);
        Err(crate::SFError::NotImplemented("search_by_tags".into()))
    }

    /// List all documents in the wiki.
    async fn list_documents(&self) -> crate::SFResult<Vec<WikiDocument>> {
        Err(crate::SFError::NotImplemented("list_documents".into()))
    }
}

/// One source distilled into a page the wiki can serve.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistilledPage {
    /// What the page is called. This is what a reader sees in the index and in
    /// search results, so it is the source's subject rather than its file name.
    pub title: String,
    /// The page itself, in markdown.
    pub body: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Turns a raw source into a wiki page.
///
/// What a source is *about*, what is worth keeping from it and what to call the
/// page is a judgment, so it is handed to an employee rather than computed here
/// -- the same shape as any other work item on this platform, and the reason
/// this trait has no prompt, no model handle and no actor of its own. The
/// implementation carries the material and the answer's schema; the employee
/// brings the reading. What comes back is stored by the implementation, which is
/// the only place the page format lives.
///
/// [`WikiBackend::ingest_document`] is the neighbouring name and a different
/// thing: that one stores bytes, this one reads them first and costs a model
/// call for that reason.
#[async_trait]
pub trait WikiDistiller: Send + Sync {
    /// Distill the source at `source_path` and store the resulting page.
    ///
    /// Returns the wiki path the page landed on, so a caller can say where the
    /// answer went without listing the wiki to find out.
    async fn distill_source(&self, source_path: &str, content: &str) -> crate::SFResult<String>;
}
