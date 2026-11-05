//! Wiki REST API handlers.
//! Provides CRUD endpoints under `/api/v1/wiki/**` backed by a
//! [`cog_core::WikiBackend`] implementation.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

use crate::GatewayState;

/// 审计 3.4：知识库多租户隔离。认证用户的文档落在 `{workspace}/` 前缀下；
/// 未认证请求保持历史全局视图（无前缀），避免存量数据不可见。
fn ns_prefix(claims: Option<&cog_core::Claims>) -> Option<String> {
    claims
        .and_then(|c| c.workspace_ids.first())
        .map(|ws| format!("{ws}/"))
}

fn claims_ref(claims: &Option<axum::Extension<cog_core::Claims>>) -> Option<&cog_core::Claims> {
    claims.as_ref().map(|axum::Extension(c)| c)
}

/// 蒸馏器写出来的页面落在 `pages/` 段下（工作区来源在其前缀之后）。这既是不
/// 再把它读一遍的判据，也是「这一份已经是页面」的唯一标识。
fn is_distilled_page(path: &str) -> bool {
    path.split('/').any(|segment| segment == "pages")
}

/// 同时在读的来源上限：这里界的是活着的轮询，不是接到的请求。
static DISTILL_SLOTS: std::sync::LazyLock<Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(8)));

fn prefixed_path(prefix: &Option<String>, path: &str) -> String {
    match prefix {
        Some(p) => format!("{p}{path}"),
        None => path.to_string(),
    }
}

fn strip_ns(prefix: &Option<String>, doc: &mut cog_core::WikiDocument) {
    if let Some(p) = prefix {
        if let Some(stripped) = doc.path.strip_prefix(p.as_str()) {
            doc.path = stripped.to_string();
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateDocumentRequest {
    /// Relative path under the wiki root (e.g. "concepts/auth.md").
    pub path: String,
    /// Markdown content of the document.
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_top_k() -> usize {
    10
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Optional prefix filter (e.g. "concepts/").
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WikiInfoResponse {
    pub provider: String,
    pub healthy: bool,
}

/// `GET /api/v1/wiki/info` — Provider name + health.
pub async fn info_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let Some(adapter) = state.wiki_adapter.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "wiki adapter disabled"})),
        )
            .into_response();
    };

    let resp = WikiInfoResponse {
        provider: adapter.provider_name().to_string(),
        healthy: adapter.health_check().await,
    };

    (StatusCode::OK, Json(resp)).into_response()
}

/// `GET /api/v1/wiki` — List all documents (optionally filter by prefix).
pub async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(query): Query<ListQuery>,
) -> Response {
    let prefix = ns_prefix(claims_ref(&claims));
    let Some(adapter) = state.wiki_adapter.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "wiki adapter disabled"})),
        )
            .into_response();
    };

    match adapter.list_documents().await {
        Ok(mut docs) => {
            if let Some(p) = prefix.as_ref() {
                docs.retain(|d| d.path.starts_with(p.as_str()));
                for d in docs.iter_mut() {
                    strip_ns(&prefix, d);
                }
            }
            if let Some(prefix) = query.prefix.as_deref() {
                docs.retain(|d| d.path.starts_with(prefix));
            }
            (StatusCode::OK, Json(json!({"documents": docs}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("list failed: {}", e)})),
        )
            .into_response(),
    }
}

/// `POST /api/v1/wiki` — Create or update a wiki document.
pub async fn create_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<CreateDocumentRequest>,
) -> Response {
    let prefix = ns_prefix(claims_ref(&claims));
    let Some(adapter) = state.wiki_adapter.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "wiki adapter disabled"})),
        )
            .into_response();
    };

    if req.path.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "path is required"})),
        )
            .into_response();
    }
    if req.path.contains("..") || req.path.starts_with('/') {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "path must be a relative wiki path"})),
        )
            .into_response();
    }

    let full_path = prefixed_path(&prefix, &req.path);
    match adapter.ingest_document(&full_path, &req.content).await {
        Ok(()) => {
            // 存下来的只是字节：这页讲什么、该叫什么、留哪几条，是判断，交给名册
            // 上的员工。这里只把活递出去、不等它——一次投递不该把一个 HTTP 请求
            // 挂在模型的墙钟上。蒸馏失败也不回滚这份文档：字节已经存进去了，
            // 回滚会把「没读懂」变成「没收到」。
            let distillation = match state.wiki_distiller.as_ref() {
                // `pages/` 是蒸馏器自己的落点，再把它读一遍就是套娃。
                Some(distiller) if !is_distilled_page(&full_path) => {
                    let distiller = distiller.clone();
                    let source = full_path.clone();
                    let content = req.content.clone();
                    tokio::spawn(async move {
                        // 同时在读的来源有界：一件活最长等它自己的预算那么多秒，
                        // 而投递端点对任何人开放——不设界的话一次批量导入就能让
                        // 进程里堆满每半秒轮询一次的活。排队的那些不轮询，只是
                        // 等一个空位，所以界的是活着的轮询，不是收到的请求。
                        let _slot = DISTILL_SLOTS.clone().acquire_owned().await;
                        match distiller.distill_source(&source, &content).await {
                            Ok(page) => tracing::info!(
                                source = %source,
                                page = %page,
                                "posted source distilled into a wiki page"
                            ),
                            Err(e) => tracing::warn!(
                                source = %source,
                                error = %e,
                                "posted source was stored but not distilled"
                            ),
                        }
                    });
                    "queued"
                }
                _ => "not configured",
            };
            (
                StatusCode::CREATED,
                Json(json!({
                    "path": req.path,
                    "status": "created",
                    "distillation": distillation,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("create failed: {}", e)})),
        )
            .into_response(),
    }
}

/// `POST /api/v1/wiki/search` — Search for documents.
pub async fn search_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<SearchRequest>,
) -> Response {
    let prefix = ns_prefix(claims_ref(&claims));
    let Some(adapter) = state.wiki_adapter.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "wiki adapter disabled"})),
        )
            .into_response();
    };

    if req.query.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "query must not be empty"})),
        )
            .into_response();
    }

    let top_k = req.top_k.clamp(1, 100);
    match adapter.search(&req.query, top_k).await {
        Ok(mut results) => {
            if let Some(p) = prefix.as_ref() {
                results.retain(|r| r.document.path.starts_with(p.as_str()));
                for r in results.iter_mut() {
                    strip_ns(&prefix, &mut r.document);
                }
            }
            let count = results.len();
            (
                StatusCode::OK,
                Json(json!({"results": results, "count": count})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("search failed: {}", e)})),
        )
            .into_response(),
    }
}

/// `GET /api/v1/wiki/document` — Read a document by path (query param `path=...`).
/// Using a query parameter avoids issues with slashes and dots in path
/// segments that arise when using path-based routing for arbitrary wiki
/// paths.
#[derive(Debug, Deserialize)]
pub struct GetDocumentQuery {
    pub path: String,
}

pub async fn get_document_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(query): Query<GetDocumentQuery>,
) -> Response {
    let prefix = ns_prefix(claims_ref(&claims));
    let Some(adapter) = state.wiki_adapter.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "wiki adapter disabled"})),
        )
            .into_response();
    };

    if query.path.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "path is required"})),
        )
            .into_response();
    }
    if query.path.contains("..") || query.path.starts_with('/') {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "path must be a relative wiki path"})),
        )
            .into_response();
    }

    let full_path = prefixed_path(&prefix, &query.path);
    match adapter.list_documents().await {
        Ok(docs) => {
            if let Some(mut doc) = docs.into_iter().find(|d| d.path == full_path) {
                strip_ns(&prefix, &mut doc);
                (StatusCode::OK, Json(json!(doc))).into_response()
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "document not found", "path": query.path})),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("read failed: {}", e)})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::claims;

    fn doc(path: &str) -> cog_core::WikiDocument {
        cog_core::WikiDocument {
            id: path.into(),
            path: path.into(),
            title: "t".into(),
            content: "c".into(),
            tags: None,
            created_at: None,
            updated_at: None,
        }
    }

    /// 页面在任何命名空间下都认得出：工作区的页面在 `{workspace}/pages/`，全局
    /// 的在 `pages/`。只认前一种会让工作区的页面被再读一遍。
    #[test]
    fn a_distilled_page_is_recognised_in_any_namespace() {
        assert!(is_distilled_page("pages/auth-1a2b3c4d.md"));
        assert!(is_distilled_page("ws-alpha/pages/auth-1a2b3c4d.md"));
        assert!(!is_distilled_page("notes/auth.md"));
        assert!(!is_distilled_page("ws-alpha/notes/pages-of-history.md"));
    }

    #[test]
    fn unauthenticated_has_no_prefix() {
        assert_eq!(ns_prefix(None), None);
        let c = claims(&[]);
        assert_eq!(ns_prefix(Some(&c)), None);
    }

    #[test]
    fn authenticated_gets_workspace_prefix() {
        let c = claims(&["ws-alpha"]);
        assert_eq!(ns_prefix(Some(&c)), Some("ws-alpha/".to_string()));
    }

    #[test]
    fn strip_ns_removes_only_matching_prefix() {
        let prefix = Some("ws-alpha/".to_string());
        let mut d = doc("ws-alpha/concepts/auth.md");
        strip_ns(&prefix, &mut d);
        assert_eq!(d.path, "concepts/auth.md");

        let mut foreign = doc("ws-beta/concepts/auth.md");
        strip_ns(&prefix, &mut foreign);
        assert_eq!(foreign.path, "ws-beta/concepts/auth.md");

        let mut legacy = doc("concepts/auth.md");
        strip_ns(&None, &mut legacy);
        assert_eq!(legacy.path, "concepts/auth.md");
    }
}
