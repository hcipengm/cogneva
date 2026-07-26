use cog_storage::MemoryObjectBackend;
use cog_wiki::WikiManager;
use std::sync::Arc;

#[tokio::test]
async fn test_ingest_and_read() {
    let backend = Arc::new(MemoryObjectBackend::new());
    let mgr = WikiManager::new(backend);

    mgr.ingest_document("concepts/auth.md", "# Auth\n\nAuthentication guide.\n")
        .await
        .unwrap();

    let doc = mgr.read_document("concepts/auth.md").await.unwrap();
    assert_eq!(doc.title, "Auth");
    assert_eq!(doc.path, "concepts/auth.md");
    assert!(doc.content.contains("Authentication guide"));
}

#[tokio::test]
async fn test_list_documents() {
    let backend = Arc::new(MemoryObjectBackend::new());
    let mgr = WikiManager::new(backend);

    mgr.ingest_document("a.md", "# A\n").await.unwrap();
    mgr.ingest_document("b/c.md", "# C\n").await.unwrap();

    let docs = mgr.list_documents().await.unwrap();
    assert_eq!(docs.len(), 2);
}
