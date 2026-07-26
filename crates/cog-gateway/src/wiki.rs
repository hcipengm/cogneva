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
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({"path": req.path, "status": "created"})),
        )
            .into_response(),
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

    fn claims_with_workspaces(ws: Vec<String>) -> cog_core::Claims {
        cog_core::Claims {
            sub: "user-1".into(),
            iss: "test".into(),
            aud: "test".into(),
            exp: 0,
            iat: 0,
            jti: "jti".into(),
            preferred_username: "tester".into(),
            user_type: cog_core::UserType::Standard,
            workspace_ids: ws,
            permissions: Vec::new(),
            roles: Vec::new(),
        }
    }

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

    #[test]
    fn unauthenticated_has_no_prefix() {
        assert_eq!(ns_prefix(None), None);
        let c = claims_with_workspaces(Vec::new());
        assert_eq!(ns_prefix(Some(&c)), None);
    }

    #[test]
    fn authenticated_gets_workspace_prefix() {
        let c = claims_with_workspaces(vec!["ws-alpha".into()]);
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
