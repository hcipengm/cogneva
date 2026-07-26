use cog_core::ObjectBackend;
use cog_storage::MemoryObjectBackend;
use cog_wiki::WikiIndexer;
use std::sync::Arc;

#[test]
fn test_bm25_basic() {
    let mut idx = cog_wiki::indexer::Bm25Index::new();
    idx.add_document("doc1", "api/auth.md", "Authentication API using JWT tokens");
    idx.add_document("doc2", "api/users.md", "User management API endpoints");
    idx.add_document("doc3", "guides/setup.md", "Setup guide for beginners");

    let results = idx.search("api authentication", 2);
    assert!(!results.is_empty());
    assert_eq!(results[0].0, "doc1");
}

#[tokio::test]
async fn test_generate_index_md() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend
        .put("auth.md", b"# Authentication\n\nJWT auth guide.")
        .await
        .unwrap();
    backend
        .put("users.md", b"# Users\n\nUser management.")
        .await
        .unwrap();

    let indexer = WikiIndexer::with_prefix(backend.clone(), "");
    indexer.generate_indices().await.unwrap();

    let index = backend.get("index.md").await.unwrap().unwrap();
    let index_str = String::from_utf8(index).unwrap();
    assert!(index_str.contains("# Index"));
    assert!(index_str.contains("Authentication"));
    assert!(index_str.contains("Users"));
}

#[tokio::test]
async fn test_extract_tags_from_frontmatter() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend
        .put("doc.md", b"---\ntags: [auth, jwt, security]\n---\n# Auth\n")
        .await
        .unwrap();

    let tags = WikiIndexer::extract_tags(&*backend, "doc.md")
        .await
        .unwrap();
    assert_eq!(tags, vec!["auth", "jwt", "security"]);
}
