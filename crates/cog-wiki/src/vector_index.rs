//! The wiki's vector face: documents findable by meaning, not only by keyword.
//!
//! A keyword index cannot answer a question whose words no page uses. That is
//! the whole reason this face exists, and it is why the vector face is not
//! derived from the keyword one: the two are written from the same bytes and
//! answer different questions, so one cannot be recomputed from the other's
//! results.
//!
//! **The write happens at ingest.** A vector face filled by a separate pass is
//! empty until that pass has run, and an empty vector face answers every query
//! with "no matches" — the same reading as a wiki that holds nothing. Indexing
//! inside `ingest_document` makes "a stored document is searchable by meaning"
//! hold without a second driver to keep alive.
//!
//! **A missing embedding provider is a state, not an empty result.** With no
//! provider published, this index cannot be written at all, and
//! [`WikiVectorIndex::search_text`] says so with an error rather than an empty
//! list: from the list alone, "no provider" and "nothing matches" are the same
//! answer.

use std::sync::Arc;

use cog_core::{
    EmbeddingProvider, SFError, SFResult, VectorBackend, WikiDocument, WikiSearchResult,
};

use crate::unified_knowledge_backend::MaybeService;

/// Collection the wiki's document vectors live in.
pub const DEFAULT_WIKI_COLLECTION: &str = "wiki";

/// Metadata keys a document is indexed under.
///
/// The backend derives its point id from [`META_ID`] when that key is present,
/// which is what makes re-ingesting a page replace its vector instead of
/// adding a second one, and what lets a delete name the point to drop without
/// a lookup table that a restart would empty.
const META_ID: &str = "id";
const META_PATH: &str = "path";
const META_TITLE: &str = "title";
const META_CONTENT: &str = "content";

/// What indexing one document did.
///
/// Three outcomes rather than a bool because they are three states of the
/// wiki, not two amounts of work: `NoEmbedder` is a process that cannot search
/// by meaning at all, `EmptyEmbedding` is a provider that answered and handed
/// back nothing to index, and `Indexed` is the one where the next query can
/// find this page. A caller that logged only "indexed / not indexed" would read
/// a misconfigured process as a quiet one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOutcome {
    /// The document's vector is in the collection.
    Indexed,
    /// No embedding provider is published; the keyword face holds the document
    /// and nothing else does.
    NoEmbedder,
    /// The provider answered with no vector for this document.
    EmptyEmbedding,
}

/// The wiki's dense-vector index, over any [`VectorBackend`].
pub struct WikiVectorIndex {
    vector: Arc<dyn VectorBackend>,
    embedding: MaybeService<dyn EmbeddingProvider>,
    collection: String,
}

impl WikiVectorIndex {
    /// Build an index over `vector`, embedding through whichever provider
    /// `embedding` resolves to.
    ///
    /// The provider arrives as a handle rather than a service because the wiki
    /// plugin builds this inside its own `init`, alongside the plugin that
    /// publishes the provider: a read there yields one only when that plugin's
    /// init happens to finish first, which would make whether the wiki can
    /// search by meaning depend on plugin order.
    pub fn new(
        vector: Arc<dyn VectorBackend>,
        collection: impl Into<String>,
        embedding: cog_core::LateService<dyn EmbeddingProvider>,
    ) -> Self {
        Self {
            vector,
            embedding: MaybeService::Late(embedding),
            collection: collection.into(),
        }
    }

    /// Build an index whose embedder is already in hand.
    ///
    /// The form a caller that resolved the provider itself uses, so that this
    /// index never has to be the one place that decides a service is missing.
    pub fn with_ready_embedding(
        vector: Arc<dyn VectorBackend>,
        collection: impl Into<String>,
        embedding: Arc<dyn EmbeddingProvider>,
    ) -> Self {
        Self {
            vector,
            embedding: MaybeService::Ready(embedding),
            collection: collection.into(),
        }
    }

    /// The collection this index reads and writes.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Whether an embedding provider is reachable right now.
    ///
    /// Asked before indexing a document rather than discovered per call: the
    /// answer cannot change while the process runs, and a wiki ingested without
    /// one is a wiki whose vector face is empty for a reason worth naming once.
    pub fn has_embedder(&self) -> bool {
        self.embedding.resolve().is_some()
    }

    /// Create the collection on first write.
    ///
    /// The dimension comes from the provider, which is why this runs at the
    /// first index rather than at construction: the provider may be resolved
    /// from the registry only after every plugin's `init` has returned.
    async fn ensure_collection(&self, dimension: usize) -> SFResult<()> {
        if self.vector.collection_exists(&self.collection).await? {
            return Ok(());
        }
        self.vector
            .create_collection(&self.collection, dimension)
            .await
    }

    /// Index one document so it can be found by meaning.
    ///
    /// Returns what happened rather than failing when the wiki cannot be
    /// embedded: `ingest_document` has already stored the document, and
    /// turning a missing embedder into an ingest failure would take the
    /// keyword face down with the vector one.
    pub async fn index_document(&self, doc: &WikiDocument) -> SFResult<IndexOutcome> {
        let Some(provider) = self.embedding.resolve() else {
            return Ok(IndexOutcome::NoEmbedder);
        };

        // Title and body in one string: the title is the page's own summary of
        // itself, and indexing the body alone makes a query that names the
        // subject in the title miss a page whose body never repeats it.
        let text = format!("{}\n\n{}", doc.title, doc.content);
        let mut vectors = provider.embed(vec![text]).await?;
        let Some(vector) = vectors.pop().filter(|v| !v.is_empty()) else {
            return Ok(IndexOutcome::EmptyEmbedding);
        };

        self.ensure_collection(vector.len()).await?;

        let metadata = serde_json::json!({
            META_ID: doc.id,
            META_PATH: doc.path,
            META_TITLE: doc.title,
            META_CONTENT: doc.content,
        });
        self.vector
            .insert(&self.collection, vec![vector], vec![metadata])
            .await?;
        Ok(IndexOutcome::Indexed)
    }

    /// Drop a document's vector.
    ///
    /// Best-effort by design: the caller's document is already gone from the
    /// authoritative store, and a failure here leaves a point that no longer
    /// matches anything — a stale hit at worst, never a missing one.
    pub async fn remove_document(&self, doc_id: &str) -> SFResult<()> {
        if !self.vector.collection_exists(&self.collection).await? {
            return Ok(());
        }
        self.vector
            .delete(&self.collection, &[doc_id.to_string()])
            .await
    }

    /// Find documents by similarity to an already-computed embedding.
    ///
    /// An empty query vector is refused by returning nothing rather than by
    /// searching: ranking against an empty vector ranks by the backend's own
    /// ordering, which is a result set that means nothing and looks normal.
    pub async fn semantic_search(
        &self,
        query_embedding: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<WikiSearchResult>> {
        if query_embedding.is_empty() || top_k == 0 {
            return Ok(Vec::new());
        }
        if !self.vector.collection_exists(&self.collection).await? {
            return Ok(Vec::new());
        }

        let hits = self
            .vector
            .search(&self.collection, query_embedding, top_k)
            .await?;

        Ok(hits
            .into_iter()
            .filter_map(|hit| {
                let path = hit.metadata.get(META_PATH)?.as_str()?.to_string();
                let title = hit
                    .metadata
                    .get(META_TITLE)
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let content = hit
                    .metadata
                    .get(META_CONTENT)
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let id = hit
                    .metadata
                    .get(META_ID)
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| hit.id.clone());
                Some(WikiSearchResult {
                    document: WikiDocument {
                        id,
                        path,
                        title,
                        content,
                        tags: None,
                        created_at: None,
                        updated_at: None,
                    },
                    score: hit.score,
                    match_type: Some(MATCH_TYPE.to_string()),
                    highlights: Vec::new(),
                })
            })
            .collect())
    }

    /// Find documents by similarity to a text query.
    ///
    /// Errors when no embedding provider is published. The alternative —
    /// answering with an empty list — is the one answer a caller cannot
    /// question, and it is indistinguishable from a query that genuinely
    /// matched nothing.
    pub async fn search_text(&self, query: &str, top_k: usize) -> SFResult<Vec<WikiSearchResult>> {
        let Some(provider) = self.embedding.resolve() else {
            return Err(SFError::Config(format!(
                "the wiki has no embedding provider: '{}' cannot be searched by meaning, \
                 and an empty result would read as a query that matched nothing",
                self.collection
            )));
        };
        let mut vectors = provider.embed(vec![query.to_string()]).await?;
        let Some(vector) = vectors.pop().filter(|v| !v.is_empty()) else {
            return Ok(Vec::new());
        };
        self.semantic_search(&vector, top_k).await
    }
}

/// How a vector hit names itself in [`WikiSearchResult::match_type`].
///
/// Distinct from the keyword backend's own wording on purpose: a caller
/// reading two results for one page has to be able to tell which face found
/// it, and the two faces answer different questions.
const MATCH_TYPE: &str = "vector";
