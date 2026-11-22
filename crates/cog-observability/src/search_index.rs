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

    /// 把一次 2xx 的正文读成 JSON。读不成就是这一份读数坏了，不是「空对象」。
    ///
    /// 代理挡在前面答一页 HTML、或者答一段空正文，状态码都是 200。把它折成
    /// `Value::Null`，两个调用点就都读作「成功且什么都没发生」：批量写读成已索引，
    /// 搜索读成一条都没命中——而真相是没人知道那边到底发生了什么。
    async fn handle_error(&self, resp: cog_core::HttpResponse) -> anyhow::Result<Value> {
        let status = resp.status;
        let text = resp
            .text()
            .map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))?;
        if !(200..300).contains(&status) {
            anyhow::bail!("Elasticsearch error ({}): {}", status, text);
        }
        serde_json::from_str(&text).map_err(|e| {
            let head: String = text.chars().take(200).collect();
            anyhow::anyhow!(
                "Elasticsearch answered {status} with a body that is not JSON ({e}): {head}"
            )
        })
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
        // 批量接口答 `errors` 是它这份回的判词；少了这一格，这次写是成是败没人
        // 知道，不能读成「没报错」。
        let errors = result
            .get("errors")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                anyhow::anyhow!("Elasticsearch bulk answer has no boolean `errors`: {result}")
            })?;
        if errors {
            // 有失败时条数只能从 `items` 数出来；没有 `items` 就说不知道，别印一个
            // 没量过的 0。
            let failed = result
                .get("items")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| item.get("index").and_then(|i| i.get("error")).is_some())
                        .count()
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Elasticsearch bulk answer says errors=true but carries no items array, \
                         so the failure count is unknown: {result}"
                    )
                })?;
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
        // 没有 `hits.hits` 的 200 不是「一条都没命中」：后者是一个读到的答案，
        // 前者是这份答案根本没读成。折在一起，检索面会把一次坏回话报成「没有文档」。
        let hits = result
            .get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                anyhow::anyhow!("Elasticsearch search answer has no hits.hits array: {result}")
            })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SFResult;

    /// 一份给定的应答：状态码与正文都由用例挑。
    #[derive(Debug)]
    struct CannedHttp {
        status: u16,
        body: String,
    }

    #[async_trait::async_trait]
    impl HttpClient for CannedHttp {
        async fn execute(&self, _req: HttpRequest) -> SFResult<cog_core::HttpResponse> {
            Ok(cog_core::HttpResponse {
                status: self.status,
                headers: Default::default(),
                body: self.body.clone().into_bytes(),
            })
        }
    }

    fn backend(status: u16, body: &str) -> ElasticsearchBackend {
        ElasticsearchBackend::new("http://es:9200").with_client(Arc::new(CannedHttp {
            status,
            body: body.to_string(),
        }))
    }

    fn doc() -> SearchDocument {
        SearchDocument::new("d1", "wiki", "hello")
    }

    /// 批量写回了 200 却没有 `errors` 判词：这次写是成是败没人知道。旧代码把它读成
    /// `false`，于是调用方记下「已索引」，而那边可能一条都没写进去。
    #[tokio::test]
    async fn a_bulk_answer_without_the_errors_flag_is_not_a_successful_write() {
        let error = backend(200, r#"{"took":3}"#)
            .index_batch(vec![doc()])
            .await
            .expect_err("没有 errors 判词的 200 不是「写成功了」");
        assert!(error.to_string().contains("errors"), "{error}");
    }

    /// 2xx 的正文不是 JSON（代理答的一页 HTML、一段空正文）：这是一份坏读数，不是
    /// 一个空对象。旧代码 `unwrap_or(Value::Null)` 把它读成成功。
    #[tokio::test]
    async fn a_bulk_answer_that_is_not_json_is_not_a_successful_write() {
        let error = backend(200, "<html>upstream says ok</html>")
            .index_batch(vec![doc()])
            .await
            .expect_err("不是 JSON 的 200 不是「写成功了」");
        assert!(error.to_string().contains("not JSON"), "{error}");
    }

    /// 反例：真的没有失败时批量写照常成功。
    #[tokio::test]
    async fn a_bulk_answer_with_no_failures_is_a_successful_write() {
        backend(200, r#"{"errors":false,"items":[{"index":{}}]}"#)
            .index_batch(vec![doc()])
            .await
            .expect("errors=false 是一份成功的回话");
    }

    /// 回话说有失败、却没带 `items`：条数只能从那里数出来，数不出就别说 0。
    #[tokio::test]
    async fn a_bulk_answer_that_says_errors_but_names_no_items_does_not_print_a_count() {
        let error = backend(200, r#"{"errors":true}"#)
            .index_batch(vec![doc()])
            .await
            .expect_err("errors=true 不是成功");
        let said = error.to_string();
        assert!(said.contains("unknown"), "{said}");
        assert!(
            !said.contains("0 failures"),
            "没量过的条数不许印成 0：{said}"
        );
    }

    /// 反例：带着 `items` 时条数照旧数得出来。
    #[tokio::test]
    async fn a_bulk_answer_with_one_failed_item_names_the_count() {
        let error = backend(
            200,
            r#"{"errors":true,"items":[{"index":{"error":{"type":"x"}}},{"index":{}}]}"#,
        )
        .index_batch(vec![doc()])
        .await
        .expect_err("errors=true 不是成功");
        assert!(error.to_string().contains("1 failures"), "{error}");
    }

    /// 搜索回了 200 却没有 `hits.hits`：这不是「一条都没命中」，是这份答案没读成。
    #[tokio::test]
    async fn a_search_answer_without_hits_hits_is_not_zero_results() {
        let error = backend(200, r#"{"took":1,"hits":{"total":{"value":0}}}"#)
            .search(&[], "q", 10)
            .await
            .expect_err("没有 hits.hits 的 200 不是「没有命中」");
        assert!(error.to_string().contains("hits.hits"), "{error}");
    }

    /// 反例：真的没有命中时，空数组是一个读到的答案，照常返回空表。
    #[tokio::test]
    async fn a_search_answer_with_no_matches_is_still_zero_results() {
        let hits = backend(200, r#"{"hits":{"total":{"value":0},"hits":[]}}"#)
            .search(&[], "q", 10)
            .await
            .expect("空数组是一份读到的答案");
        assert!(hits.is_empty(), "{hits:?}");
    }

    /// 反例：有命中时那条命中照常带出来。
    #[tokio::test]
    async fn a_search_answer_carries_its_hits() {
        let hits = backend(
            200,
            r#"{"hits":{"hits":[{"_id":"d1","_index":"wiki","_score":1.5,"_source":{"content":"hi"}}]}}"#,
        )
        .search(&[], "q", 10)
        .await
        .expect("一条命中是一份读到的答案");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, "d1");
        assert_eq!(hits[0].score, 1.5);
    }

    /// 非 2xx 的错误信息照旧带状态码与正文（这条行为没有变，钉住它免得改动把它
    /// 顺手折成别的东西）。
    #[tokio::test]
    async fn a_server_error_still_names_its_status_and_body() {
        let error = backend(503, "unavailable")
            .index_batch(vec![doc()])
            .await
            .expect_err("503 不是成功");
        let said = error.to_string();
        assert!(said.contains("503"), "{said}");
        assert!(said.contains("unavailable"), "{said}");
    }
}
