/// Search index backend for full-text and hybrid search.
/// - Dual-write mechanism: writes go to both DB and search index
/// - Reads prefer search index for fast full-text queries
/// - Phase 1: Elasticsearch via REST API
/// - Phase 2: OpenSearch compatibility
///   **Machine layer**: Fast document retrieval for agent context building.
///   **Human layer**: Search API for messages, tasks, and wiki pages.
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;
use std::sync::Arc;

use cog_core::{HttpClient, HttpRequest};

/// A searchable document.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchDocument {
    pub doc_id: String,
    pub index: String,
    pub title: Option<String>,
    pub content: String,
    pub tags: Vec<String>,
    pub metadata: Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl SearchDocument {
    pub fn new(
        doc_id: impl Into<String>,
        index: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            doc_id: doc_id.into(),
            index: index.into(),
            title: None,
            content: content.into(),
            tags: Vec::new(),
            metadata: Value::Null,
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Search result with relevance score.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchResult {
    pub doc_id: String,
    pub index: String,
    pub score: f64,
    pub highlights: Vec<String>,
    pub source: Value,
}

/// Search backend trait.
#[async_trait::async_trait]
pub trait SearchBackend: Send + Sync {
    /// Index a single document.
    async fn index_document(&self, doc: SearchDocument) -> anyhow::Result<()>;

    /// Index a batch of documents.
    async fn index_batch(&self, docs: Vec<SearchDocument>) -> anyhow::Result<()>;

    /// Search across one or more indices.
    async fn search(
        &self,
        indices: &[String],
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<SearchResult>>;

    /// Delete a document by ID.
    async fn delete_document(&self, index: &str, doc_id: &str) -> anyhow::Result<()>;

    /// Check if an index exists.
    async fn index_exists(&self, index: &str) -> anyhow::Result<bool>;

    /// Create an index with optional mapping.
    async fn create_index(&self, index: &str, mapping: Option<Value>) -> anyhow::Result<()>;

    /// Delete an entire index.
    async fn delete_index(&self, index: &str) -> anyhow::Result<()>;

    /// Health check.
    async fn health_check(&self) -> bool;
}

// ─── Elasticsearch Implementation ─────────────────────────────────

/// Elasticsearch REST API backend.
/// Uses the Elasticsearch HTTP API (default port 9200).
/// Compatible with Elasticsearch 8.x and OpenSearch 2.x.
pub struct ElasticsearchBackend {
    client: Option<Arc<dyn HttpClient>>,
    base_url: String,
    username: Option<String>,
    password: Option<String>,
    api_key: Option<String>,
}

impl ElasticsearchBackend {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: None,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            username: None,
            password: None,
            api_key: None,
        }
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn with_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        if let Some(ref key) = self.api_key {
            headers.push(("Authorization".into(), format!("ApiKey {}", key)));
        } else if let (Some(ref u), Some(ref p)) = (&self.username, &self.password) {
            let creds = format!("{}:{}", u, p);
            let encoded = general_purpose::STANDARD.encode(creds);
            headers.push(("Authorization".into(), format!("Basic {}", encoded)));
        }
        headers
    }

    fn client(&self) -> anyhow::Result<&Arc<dyn HttpClient>> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ElasticsearchBackend has no HttpClient configured"))
    }

    fn build_request(&self, method: &str, path: &str) -> HttpRequest {
        let mut req = HttpRequest::new(method, self.url(path));
        for (k, v) in self.auth_headers() {
            req = req.header(k, v);
        }
        req
    }

    async fn handle_error(&self, resp: cog_core::HttpResponse) -> anyhow::Result<Value> {
        let status = resp.status;
        let text = resp
            .text()
            .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
        if !(200..300).contains(&status) {
            anyhow::bail!("Elasticsearch error ({}): {}", status, text);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }
}

#[async_trait::async_trait]
impl SearchBackend for ElasticsearchBackend {
    async fn index_document(&self, doc: SearchDocument) -> anyhow::Result<()> {
        self.index_batch(vec![doc]).await
    }

    async fn index_batch(&self, docs: Vec<SearchDocument>) -> anyhow::Result<()> {
        if docs.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for doc in docs {
            let action = serde_json::json!({
                "index": {
                    "_index": doc.index,
                    "_id": doc.doc_id,
                }
            });
            body.push_str(&serde_json::to_string(&action)?);
            body.push('\n');

            let source = serde_json::json!({
                "title": doc.title,
                "content": doc.content,
                "tags": doc.tags,
                "metadata": doc.metadata,
                "timestamp": doc.timestamp.to_rfc3339(),
            });
            body.push_str(&serde_json::to_string(&source)?);
            body.push('\n');
        }

        let req = self
            .build_request("POST", "/_bulk")
            .header("Content-Type", "application/x-ndjson")
            .body(body.into_bytes());

        let resp = self.client()?.execute(req).await?;
        let result: Value = self.handle_error(resp).await?;
        if result
            .get("errors")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let failed = result
                .get("items")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| item.get("index").and_then(|i| i.get("error")).is_some())
                        .count()
                })
                .unwrap_or(0);
            anyhow::bail!("Elasticsearch bulk indexing had {} failures", failed);
        }

        Ok(())
    }

    async fn search(
        &self,
        indices: &[String],
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<SearchResult>> {
        let index_path = if indices.is_empty() {
            "_all".to_string()
        } else {
            indices.join(",")
        };

        let search_body = serde_json::json!({
            "size": limit,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "content", "tags^2"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "title": {},
                    "content": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    }
                }
            },
            "sort": [
                { "_score": "desc" },
                { "timestamp": "desc" }
            ]
        });

        let req = self
            .build_request("POST", &format!("/{}/_search", index_path))
            .json(&search_body)
            .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?;

        let resp = self.client()?.execute(req).await?;
        let result: Value = self.handle_error(resp).await?;
        let hits = result
            .get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
            .cloned()
            .unwrap_or_default();

        let mut results = Vec::new();
        for hit in hits {
            let source = hit.get("_source").cloned().unwrap_or(Value::Null);
            let highlights = hit
                .get("highlight")
                .and_then(|h| h.as_object())
                .map(|obj| {
                    obj.values()
                        .filter_map(|v| v.as_array())
                        .flat_map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)))
                        .collect()
                })
                .unwrap_or_default();

            results.push(SearchResult {
                doc_id: hit["_id"].as_str().unwrap_or("").to_string(),
                index: hit["_index"].as_str().unwrap_or("").to_string(),
                score: hit["_score"].as_f64().unwrap_or(0.0),
                highlights,
                source,
            });
        }

        Ok(results)
    }

    async fn delete_document(&self, index: &str, doc_id: &str) -> anyhow::Result<()> {
        let req = self.build_request("DELETE", &format!("/{}/_doc/{}", index, doc_id));
        let resp = self.client()?.execute(req).await?;

        if resp.is_success() || resp.status == 404 {
            Ok(())
        } else {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("Elasticsearch delete_document failed: {}", text);
        }
    }

    async fn index_exists(&self, index: &str) -> anyhow::Result<bool> {
        let req = self.build_request("HEAD", &format!("/{}", index));
        let resp = self.client()?.execute(req).await?;
        Ok(resp.is_success())
    }

    async fn create_index(&self, index: &str, mapping: Option<Value>) -> anyhow::Result<()> {
        let body = if let Some(mapping) = mapping {
            serde_json::json!({
                "mappings": mapping
            })
        } else {
            serde_json::json!({
                "mappings": {
                    "dynamic_templates": [
                        {
                            "strings_as_keywords": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "text",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword",
                                            "ignore_above": 256
                                        }
                                    }
                                }
                            }
                        }
                    ],
                    "properties": {
                        "title": { "type": "text", "analyzer": "standard" },
                        "content": { "type": "text", "analyzer": "standard" },
                        "tags": { "type": "keyword" },
                        "timestamp": { "type": "date" },
                        "metadata": { "type": "object", "dynamic": true }
                    }
                }
            })
        };

        let req = self
            .build_request("PUT", &format!("/{}", index))
            .json(&body)
            .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?;

        let resp = self.client()?.execute(req).await?;
        if resp.is_success() || resp.status == 400 {
            Ok(())
        } else {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("Elasticsearch create_index failed: {}", text);
        }
    }

    async fn delete_index(&self, index: &str) -> anyhow::Result<()> {
        let req = self.build_request("DELETE", &format!("/{}", index));
        let resp = self.client()?.execute(req).await?;

        if resp.is_success() || resp.status == 404 {
            Ok(())
        } else {
            let text = resp
                .text()
                .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
            anyhow::bail!("Elasticsearch delete_index failed: {}", text);
        }
    }

    async fn health_check(&self) -> bool {
        let req = self.build_request("GET", "/_cluster/health");
        match self.client() {
            Ok(client) => match client.execute(req).await {
                Ok(resp) => resp.is_success(),
                Err(e) => {
                    tracing::warn!("Elasticsearch health check failed: {}", e);
                    false
                }
            },
            Err(_) => {
                tracing::warn!("Elasticsearch health check failed: no HttpClient configured");
                false
            }
        }
    }
}

// ─── cog-core trait bridge ────────────────────────────────────────────────

#[async_trait::async_trait]
impl cog_core::SearchBackend for ElasticsearchBackend {
    async fn search(
        &self,
        indices: &[String],
        query: &str,
        limit: usize,
    ) -> cog_core::SFResult<Vec<cog_core::SearchResult>> {
        let results =
            <Self as crate::search_index::SearchBackend>::search(self, indices, query, limit)
                .await
                .map_err(|e| cog_core::SFError::IO(e.to_string()))?;
        Ok(results
            .into_iter()
            .map(|r| cog_core::SearchResult {
                doc_id: r.doc_id,
                index: r.index,
                score: r.score,
                highlights: r.highlights,
                source: r.source,
            })
            .collect())
    }
}
