//! Meilisearch-backed wiki backend.

use cog_core::{SFResult, WikiBackend};
use serde::{Deserialize, Serialize};

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
}

impl MeilisearchWikiBackend {
    /// Create a new Meilisearch wiki adapter.
    pub fn new(host: &str, api_key: Option<&str>, index_name: impl Into<String>) -> Self {
        let client = meilisearch_sdk::client::Client::new(host, api_key);
        Self {
            client,
            index_name: index_name.into(),
        }
    }

    fn index(&self) -> meilisearch_sdk::indexes::Index {
        self.client.index(&self.index_name)
    }

    fn doc_id(path: &str) -> String {
        path.replace(['/', '\\', '.', '#', ':'], "-")
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

        let tags = extract_tags(content);

        let doc = WikiSearchDoc {
            id,
            path: relative_path.into(),
            title,
            content: content.into(),
            tags,
        };

        self.index()
            .add_or_replace(&[doc], Some("id"))
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch ingest failed: {e}")))?;

        Ok(())
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
