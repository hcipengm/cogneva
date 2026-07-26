use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;
use cog_core::MetricSample;
use cog_core::RawSource;

const DEFAULT_NS: &str = "default";

/// 审计 3.4：从 JWT Claims 派生有效命名空间（多租户记忆隔离）。
/// 取第一个 workspace 作为命名空间；未认证或无 workspace 时回退 default，
/// 保证历史行为不回退。
fn effective_ns(claims: Option<&cog_core::Claims>) -> String {
    claims
        .and_then(|c| c.workspace_ids.first().cloned())
        .unwrap_or_else(|| DEFAULT_NS.to_string())
}

fn claims_ref(claims: &Option<axum::Extension<cog_core::Claims>>) -> Option<&cog_core::Claims> {
    claims.as_ref().map(|axum::Extension(c)| c)
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub id: String,
    pub text: String,
    #[serde(default)]
    pub content_type: String,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub raw_uri: String,
    pub schema_count: usize,
    pub summary_id: String,
}

#[derive(Debug, Deserialize)]
pub struct BatchIngestItem {
    pub id: String,
    pub text: String,
    #[serde(default)]
    pub content_type: String,
}

#[derive(Debug, Deserialize)]
pub struct BatchIngestRequest {
    pub items: Vec<BatchIngestItem>,
}

#[derive(Debug, Serialize)]
pub struct BatchIngestResult {
    pub id: String,
    pub raw_uri: String,
    pub schema_count: usize,
    pub summary_id: String,
}

#[derive(Debug, Serialize)]
pub struct BatchIngestResponse {
    pub processed: usize,
    pub results: Vec<BatchIngestResult>,
    pub errors: Vec<BatchIngestError>,
}

#[derive(Debug, Serialize)]
pub struct BatchIngestError {
    pub id: String,
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct SchemaSearchQuery {
    pub query: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    10
}

#[derive(Debug, Serialize)]
pub struct SchemaSearchResponse {
    pub results: Vec<SchemaResult>,
}

#[derive(Debug, Serialize)]
pub struct SchemaResult {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub key: String,
    pub confidence: f32,
}

#[derive(Debug, Deserialize)]
pub struct SummarySearchRequest {
    pub embedding: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_top_k() -> usize {
    5
}

#[derive(Debug, Serialize)]
pub struct SummarySearchResponse {
    pub results: Vec<SummaryResult>,
}

#[derive(Debug, Deserialize)]
pub struct UnifiedSearchRequest {
    pub query: String,
    #[serde(default)]
    pub embedding: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", content = "data", rename_all = "lowercase")]
pub enum UnifiedResult {
    Schema(SchemaResult),
    Summary(SummaryResult),
}

#[derive(Debug, Serialize)]
pub struct UnifiedSearchResponse {
    pub results: Vec<UnifiedResult>,
}

#[derive(Debug, Serialize)]
pub struct SchemaListResponse {
    pub results: Vec<SchemaResult>,
}

#[derive(Debug, Serialize)]
pub struct SummaryListResponse {
    pub results: Vec<SummaryResult>,
}

#[derive(Debug, Serialize)]
pub struct SummaryResult {
    pub id: String,
    pub text: String,
    pub score: f32,
    pub confidence: f32,
}

pub async fn ingest_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<IngestRequest>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    let ingestor = match state.memory_ingestor.as_ref() {
        Some(i) => i.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory ingestor disabled"})),
            )
                .into_response();
        }
    };

    let content_type = if req.content_type.is_empty() {
        "text/plain"
    } else {
        &req.content_type
    };

    let raw = RawSource::new(&req.id, &ns, content_type, req.text.into_bytes());

    match ingestor.ingest(&raw).await {
        Ok((schema, summary)) => {
            let raw_uri = match backend.archive_raw(&raw).await {
                Ok(uri) => uri,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("archive failed: {}", e)})),
                    )
                        .into_response();
                }
            };

            for entry in &schema {
                if let Err(e) = backend.store_schema(&ns, entry).await {
                    tracing::warn!("Schema store failed: {}", e);
                }
            }

            if let Err(e) = backend.store_summary(&ns, &summary).await {
                tracing::warn!("Summary store failed: {}", e);
            }

            (
                StatusCode::OK,
                Json(IngestResponse {
                    raw_uri,
                    schema_count: schema.len(),
                    summary_id: summary.id,
                }),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("ingestion failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn batch_ingest_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<BatchIngestRequest>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    let ingestor = match state.memory_ingestor.as_ref() {
        Some(i) => i.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory ingestor disabled"})),
            )
                .into_response();
        }
    };

    let mut results = Vec::new();
    let mut errors = Vec::new();

    for item in req.items {
        let content_type = if item.content_type.is_empty() {
            "text/plain"
        } else {
            &item.content_type
        };

        let raw = RawSource::new(&item.id, &ns, content_type, item.text.into_bytes());

        match ingestor.ingest(&raw).await {
            Ok((schema, summary)) => {
                let raw_uri = match backend.archive_raw(&raw).await {
                    Ok(uri) => uri,
                    Err(e) => {
                        errors.push(BatchIngestError {
                            id: item.id,
                            error: format!("archive failed: {}", e),
                        });
                        continue;
                    }
                };

                for entry in &schema {
                    if let Err(e) = backend.store_schema(&ns, entry).await {
                        tracing::warn!("Schema store failed: {}", e);
                    }
                }

                if let Err(e) = backend.store_summary(&ns, &summary).await {
                    tracing::warn!("Summary store failed: {}", e);
                }

                results.push(BatchIngestResult {
                    id: item.id,
                    raw_uri,
                    schema_count: schema.len(),
                    summary_id: summary.id,
                });
            }
            Err(e) => {
                errors.push(BatchIngestError {
                    id: item.id,
                    error: format!("ingestion failed: {}", e),
                });
            }
        }
    }

    (
        StatusCode::OK,
        Json(BatchIngestResponse {
            processed: results.len(),
            results,
            errors,
        }),
    )
        .into_response()
}

pub async fn schema_search_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(params): Query<SchemaSearchQuery>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend
        .search_schema(&ns, &params.query, params.limit)
        .await
    {
        Ok(results) => {
            let items: Vec<SchemaResult> = results
                .into_iter()
                .map(|r| SchemaResult {
                    id: r.entry.id,
                    kind: format!("{:?}", r.entry.kind).to_lowercase(),
                    name: r.entry.name,
                    key: r.entry.key,
                    confidence: r.entry.confidence,
                })
                .collect();
            (
                StatusCode::OK,
                Json(SchemaSearchResponse { results: items }),
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

pub async fn summary_search_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<SummarySearchRequest>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend
        .search_summary(&ns, &req.embedding, req.top_k, None)
        .await
    {
        Ok(results) => {
            let items: Vec<SummaryResult> = results
                .into_iter()
                .map(|r| SummaryResult {
                    id: r.entry.id,
                    text: r.entry.text,
                    score: r.score,
                    confidence: r.entry.confidence,
                })
                .collect();
            (
                StatusCode::OK,
                Json(SummarySearchResponse { results: items }),
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

#[derive(Debug, Deserialize)]
pub struct ListRawQuery {
    pub prefix: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListRawResponse {
    pub ids: Vec<String>,
}

pub async fn unified_search_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Json(req): Json<UnifiedSearchRequest>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    let embedding = if req.embedding.is_empty() {
        None
    } else {
        Some(req.embedding.as_slice())
    };

    match backend
        .search_all(&ns, &req.query, embedding, req.top_k, None)
        .await
    {
        Ok(results) => {
            let items: Vec<UnifiedResult> = results
                .into_iter()
                .map(|r| match r {
                    cog_core::UnifiedSearchResult::Schema(s) => {
                        UnifiedResult::Schema(SchemaResult {
                            id: s.entry.id,
                            kind: format!("{:?}", s.entry.kind).to_lowercase(),
                            name: s.entry.name,
                            key: s.entry.key,
                            confidence: s.entry.confidence,
                        })
                    }
                    cog_core::UnifiedSearchResult::Summary(s) => {
                        UnifiedResult::Summary(SummaryResult {
                            id: s.entry.id,
                            text: s.entry.text,
                            score: s.score,
                            confidence: s.entry.confidence,
                        })
                    }
                })
                .collect();
            (
                StatusCode::OK,
                Json(UnifiedSearchResponse { results: items }),
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

pub async fn list_schema_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend.list_schema(&ns).await {
        Ok(entries) => {
            let items: Vec<SchemaResult> = entries
                .into_iter()
                .map(|e| SchemaResult {
                    id: e.id,
                    kind: format!("{:?}", e.kind).to_lowercase(),
                    name: e.name,
                    key: e.key,
                    confidence: e.confidence,
                })
                .collect();
            (StatusCode::OK, Json(SchemaListResponse { results: items })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("list failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn list_summary_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend.list_summary(&ns).await {
        Ok(entries) => {
            let items: Vec<SummaryResult> = entries
                .into_iter()
                .map(|e| SummaryResult {
                    id: e.id,
                    text: e.text,
                    score: 0.0,
                    confidence: e.confidence,
                })
                .collect();
            (StatusCode::OK, Json(SummaryListResponse { results: items })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("list failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn list_raw_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(params): Query<ListRawQuery>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    let prefix = params.prefix.as_deref();
    match backend.list_raw(&ns, prefix).await {
        Ok(ids) => (StatusCode::OK, Json(ListRawResponse { ids })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("list failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn get_raw_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend.get_raw(&ns, &id).await {
        Ok(Some(raw)) => (
            StatusCode::OK,
            Json(json!({
                "id": raw.id,
                "content_type": raw.content_type,
                "payload_length": raw.payload.len(),
                "created_at": raw.created_at,
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "raw source not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("retrieve failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn stats_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    let metrics = backend.metrics();
    (
        StatusCode::OK,
        Json(json!({
            "raw_archived": metrics.raw_archived,
            "raw_retrieved": metrics.raw_retrieved,
            "schema_stored": metrics.schema_stored,
            "schema_searched": metrics.schema_searched,
            "summary_stored": metrics.summary_stored,
            "summary_searched": metrics.summary_searched,
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    #[serde(default = "default_seconds")]
    pub seconds: u64,
}

fn default_seconds() -> u64 {
    60
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub counters: Vec<MetricSampleView>,
    pub histograms: Vec<MetricSampleView>,
}

#[derive(Debug, Serialize)]
pub struct MetricSampleView {
    pub timestamp: String,
    pub operation: String,
    pub value: f64,
}

fn to_views(samples: Vec<MetricSample>) -> Vec<MetricSampleView> {
    samples
        .into_iter()
        .map(|s| MetricSampleView {
            timestamp: s.timestamp.to_rfc3339(),
            operation: s.labels.get("operation").cloned().unwrap_or_default(),
            value: s.value,
        })
        .collect()
}

pub async fn delete_raw_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend.delete_raw(&ns, &id).await {
        Ok(()) => (StatusCode::NO_CONTENT, Json(json!({"deleted": id}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("delete failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn delete_schema_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend.delete_schema(&ns, &id).await {
        Ok(()) => (StatusCode::NO_CONTENT, Json(json!({"deleted": id}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("delete failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn delete_summary_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(id): Path<String>,
) -> Response {
    let ns = effective_ns(claims_ref(&claims));
    let backend = match state.memory_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "memory backend disabled"})),
            )
                .into_response();
        }
    };

    match backend.delete_summary(&ns, &id).await {
        Ok(()) => (StatusCode::NO_CONTENT, Json(json!({"deleted": id}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("delete failed: {}", e)})),
        )
            .into_response(),
    }
}

pub async fn metrics_handler(
    State(state): State<Arc<GatewayState>>,
    Query(params): Query<MetricsQuery>,
) -> Response {
    let mb = match state.metrics_backend.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "metrics backend disabled"})),
            )
                .into_response();
        }
    };

    let end = chrono::Utc::now();
    let start = end - chrono::Duration::seconds(params.seconds as i64);

    let counters = match mb
        .query_counter_range("memory_operations_total", start, end)
        .await
    {
        Ok(samples) => to_views(samples),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("query counters failed: {}", e)})),
            )
                .into_response();
        }
    };

    let histograms = match mb
        .query_histogram_range("memory_operation_latency_ms", start, end)
        .await
    {
        Ok(samples) => to_views(samples),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("query histograms failed: {}", e)})),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(MetricsResponse {
            counters,
            histograms,
        }),
    )
        .into_response()
}

use serde_json::json;

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

    #[test]
    fn ns_falls_back_to_default_without_claims() {
        assert_eq!(effective_ns(None), DEFAULT_NS);
    }

    #[test]
    fn ns_falls_back_to_default_with_empty_workspaces() {
        let c = claims_with_workspaces(Vec::new());
        assert_eq!(effective_ns(Some(&c)), DEFAULT_NS);
    }

    #[test]
    fn ns_derives_from_first_workspace() {
        let c = claims_with_workspaces(vec!["ws-alpha".into(), "ws-beta".into()]);
        assert_eq!(effective_ns(Some(&c)), "ws-alpha");
    }
}
