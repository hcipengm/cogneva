use std::sync::Arc;

use cog_core::{ObjectBackend, SFError, SFResult, SkillRegistry};

use crate::indexer::WikiIndexer;
use crate::search::{ThreeTierSearch, WikiSearchResult};
use crate::vector_index::{IndexOutcome, WikiVectorIndex};

/// Build a [`cog_core::WikiDocument`] from a key and content.
fn wiki_doc_from_key(key: &str, content: &str) -> SFResult<cog_core::WikiDocument> {
    let rel = key.to_string();
    let id = rel.replace(['/', '\\'], "-");

    let title = content
        .lines()
        .find(|l| l.trim().starts_with("# "))
        .map(|l| l.trim()[2..].trim().to_string())
        .unwrap_or_else(|| {
            std::path::Path::new(key)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| id.clone())
        });

    let tags = extract_tags(content);

    Ok(cog_core::WikiDocument {
        id,
        path: rel,
        title,
        content: content.to_string(),
        tags: Some(tags),
        created_at: None,
        updated_at: None,
    })
}

fn extract_tags(content: &str) -> Vec<String> {
    let mut tags = Vec::new();
    if let Some(stripped) = content.strip_prefix("---") {
        if let Some(end) = stripped.find("---") {
            let fm = &stripped[..end];
            let mut in_tags = false;
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
                        in_tags = false;
                    }
                } else if in_tags && trimmed.starts_with("- ") {
                    let tag = trimmed[2..].trim().trim_matches('"').trim_matches('\'');
                    if !tag.is_empty() {
                        tags.push(tag.to_string());
                    }
                }
            }
        }
    }
    tags
}

/// Manages the wiki: ingestion, indexing, and querying.
pub struct WikiManager {
    object_backend: Arc<dyn ObjectBackend>,
    key_prefix: String,
    indexer: WikiIndexer,
    /// The semantic face, when this process holds one.
    ///
    /// Held here rather than passed per call because the object store is the
    /// only thing that makes a document, and a caller that had to hand in the
    /// index could hand in a different one -- or none, which is how this wiki
    /// came to answer every query with the keyword face alone while the type
    /// said it could search by meaning.
    vector_index: Option<Arc<WikiVectorIndex>>,
}

impl WikiManager {
    pub fn new(object_backend: Arc<dyn ObjectBackend>) -> Self {
        Self::with_prefix(object_backend, "wiki")
    }

    pub fn with_prefix(object_backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            object_backend: object_backend.clone(),
            key_prefix: prefix.clone(),
            indexer: WikiIndexer::with_prefix(object_backend, prefix),
            vector_index: None,
        }
    }

    /// Attach the semantic face.
    ///
    /// The index carries its own embedding provider, so a manager built without
    /// one is a manager whose `search` is the keyword tier and nothing else --
    /// a state it reports rather than hides.
    pub fn with_vector_index(mut self, index: Arc<WikiVectorIndex>) -> Self {
        self.vector_index = Some(index);
        self
    }

    fn make_key(&self, relative_path: &str) -> String {
        if self.key_prefix.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.key_prefix, relative_path)
        }
    }

    fn strip_prefix(&self, key: &str) -> String {
        if self.key_prefix.is_empty() {
            key.to_string()
        } else {
            let prefix = format!("{}/", self.key_prefix);
            key.strip_prefix(&prefix).unwrap_or(key).to_string()
        }
    }

    /// Ingest a markdown document into the wiki.
    ///
    /// Stores it and, when this manager holds a semantic face, indexes it in
    /// the same call. Indexing here rather than in a pass of its own is what
    /// makes "a document that is in the wiki can be found by meaning" hold: a
    /// face filled by a later sweep is empty until that sweep runs, and an
    /// empty face answers every query with nothing.
    pub async fn ingest_document(
        &self,
        relative_path: impl AsRef<std::path::Path>,
        content: impl Into<String>,
    ) -> SFResult<()> {
        let rel = relative_path.as_ref().to_string_lossy().replace('\\', "/");
        let content = content.into();
        let key = self.make_key(&rel);
        self.object_backend.put(&key, content.as_bytes()).await?;
        self.index_for_semantic_search(&rel, &content).await;
        Ok(())
    }

    /// Put one document into the semantic face, reporting what happened.
    ///
    /// Never fails the ingest. By the time this runs the document is already in
    /// the object store, and the keyword face already answers for it; turning a
    /// missing embedder or a refused vector write into an ingest error would
    /// take a working wiki down over a face that is meant to widen it. The
    /// outcome is logged instead, because the difference between "no embedder"
    /// and "indexed" is not otherwise readable from the wiki's own contents.
    async fn index_for_semantic_search(&self, rel: &str, content: &str) {
        let Some(index) = self.vector_index.as_ref() else {
            return;
        };
        let doc = match wiki_doc_from_key(rel, content) {
            Ok(doc) => doc,
            Err(e) => {
                tracing::warn!("wiki document {} could not be described for indexing: {}", rel, e);
                return;
            }
        };
        match index.index_document(&doc).await {
            Ok(IndexOutcome::Indexed) => {}
            Ok(IndexOutcome::NoEmbedder) => tracing::warn!(
                "wiki document {} is stored but not searchable by meaning: no embedding provider is published",
                rel
            ),
            Ok(IndexOutcome::EmptyEmbedding) => tracing::warn!(
                "wiki document {} is stored but not searchable by meaning: the embedding provider returned no vector",
                rel
            ),
            Err(e) => tracing::warn!(
                "wiki document {} is stored but its vector was refused: {}",
                rel,
                e
            ),
        }
    }


    /// Read a document from the wiki by relative path.
    pub async fn read_document(
        &self,
        relative_path: impl AsRef<std::path::Path>,
    ) -> SFResult<cog_core::WikiDocument> {
        let rel = relative_path.as_ref().to_string_lossy().replace('\\', "/");
        let key = self.make_key(&rel);
        match self.object_backend.get(&key).await? {
            Some(data) => {
                let content = String::from_utf8_lossy(&data);
                wiki_doc_from_key(&rel, &content)
            }
            None => Err(cog_core::SFError::IO(format!(
                "document not found: {}",
                rel
            ))),
        }
    }

    /// List all markdown documents in the wiki.
    pub async fn list_documents(&self) -> SFResult<Vec<cog_core::WikiDocument>> {
        let prefix = if self.key_prefix.is_empty() {
            None
        } else {
            Some(format!("{}/", self.key_prefix))
        };
        let keys = self.object_backend.list(prefix.as_deref()).await?;
        let mut docs = Vec::new();
        for key in keys {
            if key.ends_with(".md") && !key.ends_with("index.md") {
                if let Some(data) = self.object_backend.get(&key).await? {
                    let content = String::from_utf8_lossy(&data);
                    let rel = self.strip_prefix(&key);
                    docs.push(wiki_doc_from_key(&rel, &content)?);
                }
            }
        }
        Ok(docs)
    }

    /// Generate `index.md` files for all directories.
    pub async fn generate_indices(&self) -> SFResult<()> {
        self.indexer.generate_indices().await
    }

    /// Build the BM25 keyword index.
    pub async fn build_index(&mut self) -> SFResult<()> {
        self.indexer.build_bm25_index().await
    }

    /// Execute a three-tier search across the wiki.
    ///
    /// The vector tier runs whenever this manager holds a semantic face. It
    /// used to be reachable only through a `VectorBackend` argument, and since
    /// a backend cannot turn a query into a vector, a caller passing one still
    /// got nothing back.
    pub async fn search(
        &self,
        query: &str,
        top_k: usize,
        skill_registry: Option<&SkillRegistry>,
    ) -> SFResult<Vec<WikiSearchResult>> {
        let mut search = ThreeTierSearch::new(&self.indexer);
        search.set_vector_index(self.vector_index.as_deref());
        search.set_skill_registry(skill_registry);
        search.execute(query, top_k).await
    }

    /// Perform a single-tier search (e.g., BM25 only).
    pub async fn search_tier(
        &self,
        query: &str,
        tier: cog_core::SearchTier,
        top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        let mut search = ThreeTierSearch::new(&self.indexer);
        search.set_vector_index(self.vector_index.as_deref());
        search.execute_single_tier(query, tier, top_k).await
    }
}

#[async_trait::async_trait]
impl cog_core::WikiBackend for WikiManager {
    async fn health_check(&self) -> bool {
        self.list_documents().await.is_ok()
    }

    fn provider_name(&self) -> &str {
        "local-wiki"
    }

    async fn ingest_document(&self, relative_path: &str, content: &str) -> SFResult<()> {
        WikiManager::ingest_document(self, relative_path, content).await
    }

    async fn read_document(&self, relative_path: &str) -> SFResult<cog_core::WikiDocument> {
        WikiManager::read_document(self, relative_path).await
    }

    async fn update_document(&self, relative_path: &str, content: &str) -> SFResult<()> {
        WikiManager::ingest_document(self, relative_path, content).await
    }

    async fn delete_document(&self, relative_path: &str) -> SFResult<()> {
        let key = self.make_key(relative_path);
        self.object_backend.delete(&key).await?;
        // The vector goes with the document. A point left behind is a page the
        // wiki no longer holds that a query can still be handed, and the
        // keyword face cannot undo that: its own copy is gone, so the vector
        // face would be the only place still claiming the page exists.
        if let Some(index) = self.vector_index.as_ref() {
            let rel = relative_path.replace('\\', "/");
            let id = rel.replace(['/', '\\'], "-");
            if let Err(e) = index.remove_document(&id).await {
                tracing::warn!(
                    "wiki document {} was removed but its vector was not: {}",
                    rel,
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
            return Err(SFError::Config(
                "this wiki has no semantic face: no vector index is attached, so a query \
                 embedding cannot be answered here"
                    .into(),
            ));
        };
        index.semantic_search(query_embedding, top_k).await
    }

    async fn search(&self, query: &str, top_k: usize) -> SFResult<Vec<cog_core::WikiSearchResult>> {
        let results = WikiManager::search(self, query, top_k, None).await?;
        let mut typed = Vec::with_capacity(results.len());
        for r in results {
            typed.push(cog_core::WikiSearchResult {
                document: cog_core::WikiDocument {
                    id: r.doc_id,
                    path: r.path,
                    title: r.title,
                    content: r.excerpt,
                    tags: None,
                    created_at: None,
                    updated_at: None,
                },
                score: r.score,
                match_type: Some(format!("{:?}", r.source)),
                highlights: vec![],
            });
        }
        Ok(typed)
    }

    async fn search_with_tier(
        &self,
        query: &str,
        top_k: usize,
        tier: cog_core::SearchTier,
    ) -> SFResult<Vec<cog_core::WikiSearchResult>> {
        let results = WikiManager::search_tier(self, query, tier, top_k).await?;
        let mut typed = Vec::with_capacity(results.len());
        for r in results {
            typed.push(cog_core::WikiSearchResult {
                document: cog_core::WikiDocument {
                    id: r.doc_id,
                    path: r.path,
                    title: r.title,
                    content: r.excerpt,
                    tags: None,
                    created_at: None,
                    updated_at: None,
                },
                score: r.score,
                match_type: Some(format!("{:?}", r.source)),
                highlights: vec![],
            });
        }
        Ok(typed)
    }

    async fn search_by_tags(
        &self,
        tags: &[String],
        top_k: usize,
    ) -> SFResult<Vec<cog_core::WikiSearchResult>> {
        let docs = WikiManager::list_documents(self).await?;
        let mut results = Vec::new();
        for doc in docs {
            let doc_tags: std::collections::HashSet<_> = doc
                .tags
                .clone()
                .unwrap_or_default()
                .iter()
                .cloned()
                .collect();
            let query_tags: std::collections::HashSet<_> = tags.iter().cloned().collect();
            let intersection: Vec<_> = doc_tags.intersection(&query_tags).cloned().collect();
            if !intersection.is_empty() {
                let score = intersection.len() as f32 / query_tags.len().max(1) as f32;
                results.push(cog_core::WikiSearchResult {
                    document: doc,
                    score,
                    match_type: Some("tag_match".into()),
                    highlights: intersection,
                });
            }
            if results.len() >= top_k {
                break;
            }
        }
        Ok(results)
    }

    async fn list_documents(&self) -> SFResult<Vec<cog_core::WikiDocument>> {
        WikiManager::list_documents(self).await
    }
}
