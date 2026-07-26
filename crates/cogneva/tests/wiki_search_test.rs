use cog_core::{ObjectBackend, SearchTier};
use cog_storage::MemoryObjectBackend;
use cog_wiki::{ThreeTierSearch, WikiIndexer};
use std::sync::Arc;

#[tokio::test]
async fn test_three_tier_index_only() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend
        .put(
            "auth.md",
            b"# Authentication\n\nJWT and OAuth2 authentication guide.",
        )
        .await
        .unwrap();
    backend
        .put(
            "users.md",
            b"# User Management\n\nCRUD operations for users.",
        )
        .await
        .unwrap();

    let indexer = WikiIndexer::with_prefix(backend, "");
    let mut search = ThreeTierSearch::new(&indexer);
    search.set_vector_backend(None);
    search.set_skill_registry(None);

    // Before building BM25 index, search returns empty
    let results = search.execute("authentication", 2).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_three_tier_with_bm25() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend
        .put(
            "auth.md",
            b"# Authentication\n\nJWT and OAuth2 authentication guide.",
        )
        .await
        .unwrap();
    backend
        .put(
            "users.md",
            b"# User Management\n\nCRUD operations for users.",
        )
        .await
        .unwrap();

    let mut indexer = WikiIndexer::with_prefix(backend, "");
    indexer.build_bm25_index().await.unwrap();

    let search = ThreeTierSearch::new(&indexer);
    let results = search.execute("authentication", 2).await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].source, SearchTier::Index);
    assert!(results[0].path.contains("auth"));
}
