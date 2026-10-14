//! The wiki's semantic face, end to end over the real stores.
//!
//! A wiki that can only be searched by keyword carries every page that exists
//! and still answers nothing to a question phrased in other words, and that
//! failure is indistinguishable from an empty wiki. These tests drive pages in
//! through `WikiBackend::ingest_document` and read them back through the
//! semantic face, so a write the face cannot see, a delete that leaves a point
//! behind, and a re-ingest that doubles a page all fail here rather than showing
//! up as "the wiki had nothing relevant".

use std::sync::Arc;

use cog_core::{EmbeddingProvider, ObjectBackend, SearchTier, SFResult, WikiBackend};
use cog_storage::{MemoryObjectBackend, MemoryVectorBackend};
use cog_wiki::{WikiManager, WikiVectorIndex};

const DIM: usize = 32;

/// A bag-of-words embedder: offline, deterministic, and enough to place two
/// pages that share a word nearer each other than two that do not. What is
/// under test is the wiki's wiring, not a model's ordering, and a real model
/// would make this file depend on a download.
struct BagOfWords;

fn embed(text: &str) -> Vec<f32> {
    let mut vector = vec![0f32; DIM];
    for token in text
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
    {
        let mut hash: u64 = 1469598103934665603;
        for byte in token.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        vector[(hash % DIM as u64) as usize] += 1.0;
    }
    let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut vector {
            *x /= norm;
        }
    }
    vector
}

#[async_trait::async_trait]
impl EmbeddingProvider for BagOfWords {
    async fn embed(&self, texts: Vec<String>) -> SFResult<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| embed(t)).collect())
    }

    async fn embed_sparse(
        &self,
        _texts: Vec<String>,
    ) -> SFResult<Vec<cog_core::SparseEmbedding>> {
        Ok(Vec::new())
    }

    fn supports_sparse(&self) -> bool {
        false
    }

    fn model_id(&self) -> &str {
        "bag-of-words"
    }

    fn dimension(&self) -> usize {
        DIM
    }
}

/// A wiki over in-memory stores, with the semantic face wired the way the
/// plugin wires it: the index holds a handle that resolves against a context,
/// and the provider (when there is one) is published into that context. The
/// `None` arm is the plugin's "no embedding provider published" state.
fn wiki(embedding: Option<Arc<dyn EmbeddingProvider>>) -> WikiManager {
    let objects: Arc<dyn ObjectBackend> = Arc::new(MemoryObjectBackend::new());
    let vectors = Arc::new(MemoryVectorBackend::new());
    let ctx = cog_core::PluginContext::new(cog_core::Config::default());
    if let Some(provider) = embedding {
        ctx.publish_service::<dyn EmbeddingProvider>(provider);
    }
    let index = WikiVectorIndex::new(
        vectors,
        "wiki",
        cog_core::LateService::new(ctx.as_owner("wiki_vector_index_test")),
    );
    WikiManager::new(objects).with_vector_index(Arc::new(index))
}

const SESSION_PAGE: &str = "# Sessions\n\nA short lived credential is issued once the caller proves identity.";

#[tokio::test]
async fn a_stored_page_is_reachable_through_the_semantic_face() {
    let manager = wiki(Some(Arc::new(BagOfWords)));
    WikiBackend::ingest_document(&manager, "auth/sessions.md", SESSION_PAGE)
        .await
        .unwrap();

    // A question whose words no page uses: this is the query the keyword face
    // cannot answer and the reason the semantic face exists.
    let results = manager
        .search_tier("who are you really", SearchTier::Vector, 5)
        .await
        .unwrap();

    assert_eq!(results.len(), 1, "the stored page should be findable: {results:?}");
    assert_eq!(results[0].path, "auth/sessions.md");
    assert_eq!(results[0].source, SearchTier::Vector);
    assert!(
        results[0].excerpt.contains("short lived credential"),
        "a semantic hit carries the page's text, not a placeholder: {:?}",
        results[0].excerpt
    );
}

#[tokio::test]
async fn deleting_a_page_takes_its_vector_with_it() {
    let manager = wiki(Some(Arc::new(BagOfWords)));
    WikiBackend::ingest_document(&manager, "auth/sessions.md", SESSION_PAGE)
        .await
        .unwrap();
    WikiBackend::ingest_document(&manager, "billing/invoices.md", "# Invoices\n\nMonthly billing.")
        .await
        .unwrap();

    WikiBackend::delete_document(&manager, "auth/sessions.md")
        .await
        .unwrap();

    let results = manager
        .search_tier("credential identity", SearchTier::Vector, 10)
        .await
        .unwrap();
    let paths: Vec<&str> = results.iter().map(|r| r.path.as_str()).collect();
    assert!(
        !paths.contains(&"auth/sessions.md"),
        "a deleted page came back from the vector store: {paths:?}"
    );
    assert!(
        paths.contains(&"billing/invoices.md"),
        "deleting one page removed another: {paths:?}"
    );
}

#[tokio::test]
async fn reingesting_a_page_replaces_its_vector_instead_of_adding_one() {
    let manager = wiki(Some(Arc::new(BagOfWords)));
    WikiBackend::ingest_document(&manager, "auth/sessions.md", SESSION_PAGE)
        .await
        .unwrap();
    WikiBackend::update_document(
        &manager,
        "auth/sessions.md",
        "# Sessions\n\nA passkey replaces the password entirely.",
    )
    .await
    .unwrap();

    let results = manager
        .search_tier("passkey", SearchTier::Vector, 10)
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "the page was indexed twice instead of replaced: {results:?}"
    );
    assert!(
        results[0].excerpt.contains("passkey"),
        "the surviving vector is the old page's: {:?}",
        results[0].excerpt
    );
}

/// A wiki with no embedding provider cannot turn a query into a vector, and it
/// has to say so rather than answer with an empty list: "nothing here is
/// relevant" is the one reading a caller cannot tell apart from a face that was
/// never there.
#[tokio::test]
async fn a_wiki_without_an_embedder_refuses_to_search_by_meaning() {
    let manager = wiki(None);
    // The page still goes in: the keyword face holds it, and the ingest is not
    // the place to fail over a face that only widens what the wiki can answer.
    WikiBackend::ingest_document(&manager, "auth/sessions.md", SESSION_PAGE)
        .await
        .unwrap();
    assert!(
        WikiBackend::read_document(&manager, "auth/sessions.md")
            .await
            .is_ok(),
        "the document should be stored even when it cannot be embedded"
    );

    let err = manager
        .search_tier("credential", SearchTier::Vector, 5)
        .await
        .expect_err("a query cannot be embedded, so the tier cannot answer at all");
    assert!(
        err.to_string().contains("no embedding provider"),
        "the refusal has to name what is missing: {err}"
    );
}

/// The other absent state: a manager built without an index at all. Here a
/// caller supplies its own query embedding, so nothing is missing on the query
/// side -- the store the vector would be looked up in is what does not exist.
#[tokio::test]
async fn a_wiki_with_no_vector_index_refuses_a_query_embedding() {
    let objects: Arc<dyn ObjectBackend> = Arc::new(MemoryObjectBackend::new());
    let manager = WikiManager::new(objects);

    let err = manager
        .semantic_search(&embed("credential"), 5)
        .await
        .expect_err("a wiki with no semantic face must not answer with an empty list");
    assert!(
        err.to_string().contains("no semantic face"),
        "the refusal has to name what is missing: {err}"
    );
}
