//! Meilisearch-backed wiki backend.

use std::sync::Arc;

use cog_core::{SFResult, WikiBackend};
use serde::{Deserialize, Serialize};

use crate::vector_index::{IndexOutcome, WikiVectorIndex};

/// A document stored in the Meilisearch wiki index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiSearchDoc {
    pub id: String,
    pub path: String,
    pub title: String,
    pub content: String,
    pub tags: Vec<String>,
}

/// Wiki adapter backed by Meilisearch.
pub struct MeilisearchWikiBackend {
    client: meilisearch_sdk::client::Client,
    index_name: String,
    /// The semantic face, when this process holds one.
    ///
    /// The keyword store and the vector store hold the same pages and answer
    /// different questions, so a page enters and leaves both through the one
    /// call that changes it. A backend that wrote only to Meilisearch would
    /// leave the wiki's `semantic_search` reaching a store nobody had filled.
    vector_index: Option<Arc<WikiVectorIndex>>,
}

impl MeilisearchWikiBackend {
    /// Create a new Meilisearch wiki adapter.
    pub fn new(host: &str, api_key: Option<&str>, index_name: impl Into<String>) -> Self {
        let client = meilisearch_sdk::client::Client::new(host, api_key);
        Self {
            client,
            index_name: index_name.into(),
            vector_index: None,
        }
    }

    /// Attach the semantic face.
    pub fn with_vector_index(mut self, index: Arc<WikiVectorIndex>) -> Self {
        self.vector_index = Some(index);
        self
    }

    fn index(&self) -> meilisearch_sdk::indexes::Index {
        self.client.index(&self.index_name)
    }

    fn doc_id(path: &str) -> String {
        path.replace(['/', '\\', '.', '#', ':'], "-")
    }

    /// Describe one page the way both faces need it described.
    ///
    /// One builder for the two writes: the keyword store's id and the vector
    /// store's metadata have to name the same page, and two builders would be
    /// two places for the naming rule to drift apart.
    fn describe(relative_path: &str, content: &str) -> cog_core::WikiDocument {
        let id = Self::doc_id(relative_path);
        let title = content
            .lines()
            .find(|l| l.trim().starts_with("# "))
            .map(|l| l.trim()[2..].trim().to_string())
            .unwrap_or_else(|| {
                std::path::Path::new(relative_path)
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| id.clone())
            });
        cog_core::WikiDocument {
            id,
            path: relative_path.into(),
            title,
            content: content.into(),
            tags: Some(extract_tags(content)),
            created_at: None,
            updated_at: None,
        }
    }

    /// Put one document into the semantic face, reporting what happened.
    ///
    /// Never fails the write. The page is already in the keyword store by the
    /// time this runs, and turning a missing embedder into a write error would
    /// take the whole wiki down over a face that is meant to widen it.
    async fn index_for_semantic_search(&self, doc: &cog_core::WikiDocument) {
        let Some(index) = self.vector_index.as_ref() else {
            return;
        };
        match index.index_document(doc).await {
            Ok(IndexOutcome::Indexed) => {}
            Ok(IndexOutcome::NoEmbedder) => tracing::warn!(
                "wiki document {} is stored but not searchable by meaning: no embedding provider is published",
                doc.path
            ),
            Ok(IndexOutcome::EmptyEmbedding) => tracing::warn!(
                "wiki document {} is stored but not searchable by meaning: the embedding provider returned no vector",
                doc.path
            ),
            Err(e) => tracing::warn!(
                "wiki document {} is stored but its vector was refused: {}",
                doc.path,
                e
            ),
        }
    }
}

#[async_trait::async_trait]
impl WikiBackend for MeilisearchWikiBackend {
    async fn health_check(&self) -> bool {
        self.client.is_healthy().await
    }

    fn provider_name(&self) -> &str {
        "meilisearch"
    }

    async fn ingest_document(&self, relative_path: &str, content: &str) -> SFResult<()> {
        let doc = Self::describe(relative_path, content);

        let stored = WikiSearchDoc {
            id: doc.id.clone(),
            path: doc.path.clone(),
            title: doc.title.clone(),
            content: doc.content.clone(),
            tags: doc.tags.clone().unwrap_or_default(),
        };

        self.index()
            .add_or_replace(&[stored], Some("id"))
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch ingest failed: {e}")))?;

        self.index_for_semantic_search(&doc).await;
        Ok(())
    }

    /// Updating is storing again under the same id.
    ///
    /// The trait declares this and the wiki's own maintainer calls it on every
    /// page it rewrites; left to the default it returned `NotImplemented`, so a
    /// wiki served by this backend could not have a page changed at all.
    async fn update_document(&self, relative_path: &str, content: &str) -> SFResult<()> {
        self.ingest_document(relative_path, content).await
    }

    async fn delete_document(&self, relative_path: &str) -> SFResult<()> {
        let id = Self::doc_id(relative_path);
        self.index()
            .delete_document(&id)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch delete failed: {e}")))?;

        // The vector goes with the page, for the same reason it is written with
        // it: a point left behind is a page the wiki no longer holds that a
        // query can still be handed.
        if let Some(index) = self.vector_index.as_ref() {
            if let Err(e) = index.remove_document(&id).await {
                tracing::warn!(
                    "wiki document {} was removed but its vector was not: {}",
                    relative_path,
                    e
                );
            }
        }
        Ok(())
    }

    async fn semantic_search(
        &self,
        query_embedding: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<cog_core::WikiSearchResult>> {
        let Some(index) = self.vector_index.as_ref() else {
            return Err(cog_core::SFError::Config(
                "this wiki has no semantic face: no vector index is attached, so a query \
                 embedding cannot be answered here"
                    .into(),
            ));
        };
        index.semantic_search(query_embedding, top_k).await
    }

    async fn search(&self, query: &str, top_k: usize) -> SFResult<Vec<cog_core::WikiSearchResult>> {
        let index = self.index();
        let mut sq = index.search();
        sq.with_query(query);
        sq.with_limit(top_k);

        let results = sq
            .execute::<WikiSearchDoc>()
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch search failed: {e}")))?;

        let typed: Vec<cog_core::WikiSearchResult> = results
            .hits
            .into_iter()
            .map(|h| cog_core::WikiSearchResult {
                document: cog_core::WikiDocument {
                    id: h.result.id,
                    path: h.result.path,
                    title: h.result.title,
                    content: h.result.content,
                    tags: Some(h.result.tags),
                    created_at: None,
                    updated_at: None,
                },
                score: h.ranking_score.unwrap_or(0.0) as f32,
                match_type: Some("meilisearch".into()),
                highlights: vec![],
            })
            .collect();

        Ok(typed)
    }

    async fn list_documents(&self) -> SFResult<Vec<cog_core::WikiDocument>> {
        let docs = self
            .index()
            .get_documents::<WikiSearchDoc>()
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch list failed: {e}")))?;

        let typed: Vec<cog_core::WikiDocument> = docs
            .results
            .into_iter()
            .map(|d| cog_core::WikiDocument {
                id: d.id,
                path: d.path,
                title: d.title,
                content: d.content,
                tags: Some(d.tags),
                created_at: None,
                updated_at: None,
            })
            .collect();

        Ok(typed)
    }
}

fn extract_tags(content: &str) -> Vec<String> {
    let mut tags = Vec::new();
    if let Some(stripped) = content.strip_prefix("---") {
        if let Some(end) = stripped.find("---") {
            let fm = &stripped[..end];
            for line in fm.lines() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("tags:") {
                    let rest = rest.trim();
                    if rest.starts_with('[') && rest.ends_with(']') {
                        let inner = &rest[1..rest.len() - 1];
                        for t in inner.split(',') {
                            let tag = t.trim().trim_matches('"').trim_matches('\'');
                            if !tag.is_empty() {
                                tags.push(tag.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    tags
}
