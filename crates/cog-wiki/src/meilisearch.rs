//! Meilisearch-backed wiki backend.

use std::sync::Arc;

use cog_core::{SFResult, WikiBackend};
use meilisearch_sdk::documents::DocumentsQuery;
use serde::{Deserialize, Serialize};

use crate::vector_index::{IndexOutcome, WikiVectorIndex};

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
    /// The semantic face, when this process holds one.
    ///
    /// The keyword store and the vector store hold the same pages and answer
    /// different questions, so a page enters and leaves both through the one
    /// call that changes it. A backend that wrote only to Meilisearch would
    /// leave the wiki's `semantic_search` reaching a store nobody had filled.
    vector_index: Option<Arc<WikiVectorIndex>>,
}

impl MeilisearchWikiBackend {
    /// Create a new Meilisearch wiki adapter.
    pub fn new(host: &str, api_key: Option<&str>, index_name: impl Into<String>) -> Self {
        let client = meilisearch_sdk::client::Client::new(host, api_key);
        Self {
            client,
            index_name: index_name.into(),
            vector_index: None,
        }
    }

    /// Attach the semantic face.
    pub fn with_vector_index(mut self, index: Arc<WikiVectorIndex>) -> Self {
        self.vector_index = Some(index);
        self
    }

    fn index(&self) -> meilisearch_sdk::indexes::Index {
        self.client.index(&self.index_name)
    }

    fn doc_id(path: &str) -> String {
        path.replace(['/', '\\', '.', '#', ':'], "-")
    }

    /// Describe one page the way both faces need it described.
    ///
    /// One builder for the two writes: the keyword store's id and the vector
    /// store's metadata have to name the same page, and two builders would be
    /// two places for the naming rule to drift apart.
    fn describe(relative_path: &str, content: &str) -> cog_core::WikiDocument {
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
        cog_core::WikiDocument {
            id,
            path: relative_path.into(),
            title,
            content: content.into(),
            tags: Some(extract_tags(content)),
            created_at: None,
            updated_at: None,
        }
    }

    /// 取全索引里的每一篇。
    ///
    /// `GET /indexes/{uid}/documents` 一次只回一页，而**不点名一页多大时它只回
    /// 20 条**（接口的默认值）。只问一次的读法，把「索引里第 21 篇起没人读过」
    /// 与「索引里只有这些」读成一个样子：列面少列，按 path 取单篇的那条路对它们
    /// 恒答 404——一篇在索引里、也搜得到的页面，读成「没有这篇」。
    ///
    /// 一页多大由服务端按它自己的默认给：要的是取全，不是把一页要多大钉进
    /// 代码——那个数没有出处，写进来就是一条没人量过的界。
    ///
    /// 应答带着 `total`，所以「取全了没有」有一句判词：凑不齐就把两个数一起报
    /// 出来，不把半份名单当全份交出去。
    async fn list_all_pages(&self) -> SFResult<Vec<WikiSearchDoc>> {
        let index = self.index();
        let mut all: Vec<WikiSearchDoc> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        // 由每一页的应答重新记一次；交出去的是最后一次读到的那份「一共几篇」。
        let mut total: usize;
        loop {
            let mut query = DocumentsQuery::new(&index);
            query.with_offset(all.len());
            let page = index
                .get_documents_with::<WikiSearchDoc>(&query)
                .await
                .map_err(|e| cog_core::SFError::IO(format!("meilisearch list failed: {e}")))?;
            total = page.total as usize;
            if page.results.is_empty() {
                // 服务端自己说没有下一批了。
                break;
            }
            let before = all.len();
            for doc in page.results {
                if seen.insert(doc.id.clone()) {
                    all.push(doc);
                }
            }
            // 这一页没有带来任何一篇新页面：`offset` 没被理（服务端把同一页
            // 又答了一遍）。再问下去只是把同一批再读一遍，到此为止，缺多少
            // 交给下面那句判词。
            if all.len() == before || all.len() >= total {
                break;
            }
        }
        if all.len() < total {
            return Err(cog_core::SFError::IO(format!(
                "meilisearch index holds {total} documents but this listing read {} of them",
                all.len()
            )));
        }
        Ok(all)
    }

    /// Put one document into the semantic face, reporting what happened.
    ///
    /// Never fails the write. The page is already in the keyword store by the
    /// time this runs, and turning a missing embedder into a write error would
    /// take the whole wiki down over a face that is meant to widen it.
    async fn index_for_semantic_search(&self, doc: &cog_core::WikiDocument) {
        let Some(index) = self.vector_index.as_ref() else {
            return;
        };
        match index.index_document(doc).await {
            Ok(IndexOutcome::Indexed) => {}
            Ok(IndexOutcome::NoEmbedder) => tracing::warn!(
                "wiki document {} is stored but not searchable by meaning: no embedding provider is published",
                doc.path
            ),
            Ok(IndexOutcome::EmptyEmbedding) => tracing::warn!(
                "wiki document {} is stored but not searchable by meaning: the embedding provider returned no vector",
                doc.path
            ),
            Err(e) => tracing::warn!(
                "wiki document {} is stored but its vector was refused: {}",
                doc.path,
                e
            ),
        }
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
        let doc = Self::describe(relative_path, content);

        let stored = WikiSearchDoc {
            id: doc.id.clone(),
            path: doc.path.clone(),
            title: doc.title.clone(),
            content: doc.content.clone(),
            tags: doc.tags.clone().unwrap_or_default(),
        };

        self.index()
            .add_or_replace(&[stored], Some("id"))
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch ingest failed: {e}")))?;

        self.index_for_semantic_search(&doc).await;
        Ok(())
    }

    /// Updating is storing again under the same id.
    ///
    /// The trait declares it and any writer that rewrites a page goes through
    /// it; left to the default it returned `NotImplemented`, so a wiki served by
    /// this backend could not have a page changed at all.
    async fn update_document(&self, relative_path: &str, content: &str) -> SFResult<()> {
        self.ingest_document(relative_path, content).await
    }

    async fn delete_document(&self, relative_path: &str) -> SFResult<()> {
        let id = Self::doc_id(relative_path);
        self.index()
            .delete_document(&id)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("meilisearch delete failed: {e}")))?;

        // The vector goes with the page, for the same reason it is written with
        // it: a point left behind is a page the wiki no longer holds that a
        // query can still be handed.
        if let Some(index) = self.vector_index.as_ref() {
            if let Err(e) = index.remove_document(&id).await {
                tracing::warn!(
                    "wiki document {} was removed but its vector was not: {}",
                    relative_path,
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
            return Err(cog_core::SFError::Config(
                "this wiki has no semantic face: no vector index is attached, so a query \
                 embedding cannot be answered here"
                    .into(),
            ));
        };
        index.semantic_search(query_embedding, top_k).await
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
        let docs = self.list_all_pages().await?;

        let typed: Vec<cog_core::WikiDocument> = docs
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn asked_target(head: &[u8]) -> String {
        String::from_utf8_lossy(head)
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("")
            .to_string()
    }

    fn query_param(target: &str, name: &str) -> Option<usize> {
        let (_, query) = target.split_once('?')?;
        query.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == name).then(|| v.parse().ok()).flatten()
        })
    }

    /// 一台只答 `GET /indexes/{uid}/documents` 的假 Meilisearch。
    ///
    /// 按真接口的规矩答：`limit` 缺省是 20 条（接口的默认值），`offset` 跳过前面
    /// 的条数，应答带 `total`。所以「只问一次」与「问到取全」在这台假服务上会拿
    /// 到不同长度的名单——测试量的正是那一步。
    ///
    /// `honours_offset == false` 的那一副是「总数报得对、却永远只答第一页」的
    /// 服务端：读的一方不许把那一页当全份。
    async fn serve_index(pages: usize, honours_offset: bool) -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let asked = Arc::new(AtomicUsize::new(0));
        let counter = asked.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 8192];
                let mut read = 0;
                loop {
                    match socket.read(&mut buf[read..]).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => read += n,
                    }
                    if buf[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let target = asked_target(&buf[..read]);
                counter.fetch_add(1, Ordering::SeqCst);
                let (offset, limit) = if honours_offset {
                    (
                        query_param(&target, "offset").unwrap_or(0),
                        query_param(&target, "limit").unwrap_or(20),
                    )
                } else {
                    (0, 20)
                };
                let end = (offset + limit).min(pages);
                let results: Vec<serde_json::Value> = (offset..end)
                    .map(|i| {
                        serde_json::json!({
                            "id": format!("pages-p{i}md"),
                            "path": format!("pages/p{i}.md"),
                            "title": format!("p{i}"),
                            "content": "c",
                            "tags": [],
                        })
                    })
                    .collect();
                let body = serde_json::json!({
                    "results": results,
                    "offset": offset,
                    "limit": limit,
                    "total": pages,
                })
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{}", addr), asked)
    }

    /// 索引里超过一页、没超过一页上限时，列出来的必须是全部：老读法不点名一页
    /// 多大，服务端就按默认的 20 条答，第 21 篇起在列面上与「没有这篇」同形。
    #[tokio::test]
    async fn a_listing_does_not_stop_at_the_servers_default_page_of_twenty() {
        let (url, _) = serve_index(45, true).await;
        let backend = MeilisearchWikiBackend::new(&url, None, "wiki");
        let docs = backend.list_documents().await.expect("45 篇都答得出来");
        let paths: Vec<&str> = docs.iter().map(|d| d.path.as_str()).collect();
        assert_eq!(paths.len(), 45, "第 21 篇起也必须在名单里");
        assert_eq!(paths.first().copied(), Some("pages/p0.md"));
        assert_eq!(paths.last().copied(), Some("pages/p44.md"));
    }

    /// 索引里的篇数远多于一页时，读的一方要接着问下一页，直到够上 `total`：
    /// 请求数证明了这一点——只问一次拿不回 2500 篇。
    #[tokio::test]
    async fn a_listing_larger_than_a_page_asks_again_until_the_total_is_read() {
        let (url, asked) = serve_index(2_500, true).await;
        let backend = MeilisearchWikiBackend::new(&url, None, "wiki");
        let docs = backend.list_documents().await.expect("2500 篇都答得出来");
        assert_eq!(docs.len(), 2_500, "越过一页的部分也得读全");
        assert_eq!(docs.last().map(|d| d.path.as_str()), Some("pages/p2499.md"));
        assert!(
            asked.load(Ordering::SeqCst) >= 3,
            "2500 篇按服务端一页 20 条要问一百多次，实际问了 {}",
            asked.load(Ordering::SeqCst)
        );
    }

    /// 服务端报的 `total` 与它交出来的篇数对不上时，这里不许把那半份名单当全份
    /// 交出去：两个数一起报出来，读面才知道自己少读了什么。
    #[tokio::test]
    async fn a_total_never_handed_out_is_not_a_complete_listing() {
        let (url, _) = serve_index(45, false).await;
        let backend = MeilisearchWikiBackend::new(&url, None, "wiki");
        let error = backend
            .list_documents()
            .await
            .expect_err("报了 45 篇却只给 20 篇，这不是一份完整的名单");
        let said = error.to_string();
        assert!(said.contains("45"), "{said}");
        assert!(said.contains("20"), "{said}");
    }
}
