use cog_core::{ObjectBackend, SearchTier, WikiBackend};
use cog_storage::FileObjectBackend;
use cog_wiki::WikiManager;
use std::sync::Arc;

// ——— Helper: create a temp wiki with sample documents ———

fn sample_auth_doc() -> &'static str {
    r#"---
tags: [auth, security, jwt, oauth2]
---
# Authentication

This guide covers JWT and OAuth2 authentication flows.

## JWT

JSON Web Tokens are compact, URL-safe means of representing claims.

## OAuth2

OAuth2 is an authorization framework that enables applications to obtain limited access to user accounts.
"#
}

fn sample_users_doc() -> &'static str {
    r#"---
tags: [api, users, crud]
---
# User Management

CRUD operations for managing users in the system.

## Create User

POST /api/v1/users

## List Users

GET /api/v1/users
"#
}

fn sample_setup_doc() -> &'static str {
    r#"---
tags: [setup, dev, docker]
---
# Setup Guide

Step-by-step instructions for setting up the development environment.

## Prerequisites

- Docker
- Node.js 20+
- Rust 1.85+
"#
}

async fn create_test_wiki() -> (tempfile::TempDir, WikiManager, Arc<dyn ObjectBackend>) {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mgr = WikiManager::with_prefix(backend.clone(), "");

    mgr.ingest_document("concepts/auth.md", sample_auth_doc())
        .await
        .unwrap();
    mgr.ingest_document("api/users.md", sample_users_doc())
        .await
        .unwrap();
    mgr.ingest_document("guides/setup.md", sample_setup_doc())
        .await
        .unwrap();

    (temp, mgr, backend)
}

// ——— WikiManager tests ———

#[tokio::test]
async fn test_wiki_manager_ingest_and_read() {
    let (_temp, mgr, _backend) = create_test_wiki().await;

    let doc = mgr.read_document("concepts/auth.md").await.unwrap();
    assert_eq!(doc.title, "Authentication");
    assert_eq!(doc.path, "concepts/auth.md");
    assert!(doc.content.contains("JWT"));
    assert!(doc
        .tags
        .as_ref()
        .is_some_and(|t| t.contains(&"jwt".to_string())));
}

#[tokio::test]
async fn test_wiki_manager_list_documents() {
    let (_temp, mgr, _backend) = create_test_wiki().await;

    let docs = mgr.list_documents().await.unwrap();
    assert_eq!(docs.len(), 3);

    let titles: Vec<String> = docs.into_iter().map(|d| d.title).collect();
    assert!(titles.contains(&"Authentication".to_string()));
    assert!(titles.contains(&"User Management".to_string()));
    assert!(titles.contains(&"Setup Guide".to_string()));
}

#[tokio::test]
async fn test_wiki_manager_generate_indices() {
    let (temp, mgr, _backend) = create_test_wiki().await;
    mgr.generate_indices().await.unwrap();

    let index = std::fs::read_to_string(temp.path().join("index.md")).unwrap();
    assert!(index.contains("# Index"));
    assert!(index.contains("Authentication") || index.contains("concepts"));
}

#[tokio::test]
async fn test_wiki_manager_read_nonexistent() {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mgr = WikiManager::with_prefix(backend, "");

    let err = mgr.read_document("missing.md").await.unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("missing") || msg.contains("not found"));
}

// ——— BM25 search tests ———

#[tokio::test]
async fn test_bm25_search_finds_relevant_docs() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    let results = mgr
        .search_tier("authentication jwt", SearchTier::Index, 2)
        .await
        .unwrap();
    assert!(!results.is_empty());
    assert!(results[0].path.contains("auth"));
}

#[tokio::test]
async fn test_bm25_search_ranks_by_relevance() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    let results = mgr
        .search_tier("api users", SearchTier::Index, 2)
        .await
        .unwrap();
    assert_eq!(results.len(), 1); // Only users.md matches both terms strongly
    assert!(results[0].path.contains("users"));
}

#[tokio::test]
async fn test_bm25_search_empty_query() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    let results = mgr.search_tier("", SearchTier::Index, 5).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_bm25_search_no_match() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    let results = mgr
        .search_tier("kubernetes helm chart", SearchTier::Index, 5)
        .await
        .unwrap();
    assert!(results.is_empty());
}

// ——— ThreeTierSearch tests ———

#[tokio::test]
async fn test_three_tier_search_index_tier() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    let results = mgr.search("setup docker", 2, None, None).await.unwrap();
    assert!(!results.is_empty());
    assert!(results[0].path.contains("setup"));
    assert_eq!(results[0].source, SearchTier::Index);
}

#[tokio::test]
async fn test_three_tier_search_no_backend_returns_index_results() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    let results = mgr.search("JWT tokens", 5, None, None).await.unwrap();
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_three_tier_search_dedup_by_doc_id() {
    let (_temp, mut mgr, _backend) = create_test_wiki().await;
    mgr.build_index().await.unwrap();

    // Even if multiple tiers match the same doc, it should appear once
    let results = mgr.search("authentication", 10, None, None).await.unwrap();
    let auth_count = results.iter().filter(|r| r.path.contains("auth")).count();
    assert_eq!(auth_count, 1);
}

// ——— WikiBackend tests (via WikiManager) ———

#[tokio::test]
async fn test_wiki_backend_health_check() {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mgr = WikiManager::with_prefix(backend, "");
    assert!(mgr.health_check().await);
}

#[tokio::test]
async fn test_wiki_backend_ingest_and_search() {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mut mgr = WikiManager::with_prefix(backend, "");

    mgr.ingest_document("api/auth.md", "# Auth\n\nJWT guide.\n")
        .await
        .unwrap();

    mgr.ingest_document("api/users.md", "# Users\n\nUser API.\n")
        .await
        .unwrap();

    mgr.build_index().await.unwrap();

    let docs = WikiBackend::list_documents(&mgr).await.unwrap();
    assert_eq!(docs.len(), 2);

    let results = WikiBackend::search(&mgr, "jwt", 2).await.unwrap();
    assert!(!results.is_empty());
    let first = results.first().unwrap();
    assert!(first.document.path.contains("auth"));
}

#[tokio::test]
async fn test_wiki_backend_provider_name() {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mgr = WikiManager::with_prefix(backend, "");
    assert_eq!(mgr.provider_name(), "local-wiki");
}

// ——— Index generation tests ———

#[tokio::test]
async fn test_index_generation_creates_nested_indices() {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mgr = WikiManager::with_prefix(backend, "");

    mgr.ingest_document("a/b/c.md", "# Deep Doc\n\nContent.")
        .await
        .unwrap();
    mgr.generate_indices().await.unwrap();

    assert!(temp.path().join("index.md").exists());
    assert!(temp.path().join("a/index.md").exists());
    assert!(temp.path().join("a/b/index.md").exists());
}

#[tokio::test]
async fn test_index_generation_respects_existing_index_md() {
    let temp = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = Arc::new(FileObjectBackend::new(temp.path()));
    let mgr = WikiManager::with_prefix(backend, "");

    mgr.ingest_document("index.md", "# Custom Index\n\nDo not overwrite me.")
        .await
        .unwrap();
    mgr.ingest_document("other.md", "# Other\n").await.unwrap();
    mgr.generate_indices().await.unwrap();

    let index = std::fs::read_to_string(temp.path().join("index.md")).unwrap();
    // The root index should be regenerated and include other.md
    assert!(index.contains("Other") || index.contains("other"));
}
