//! File HTTP handlers — real persistence via object storage backend.
//! Provides the upload / fetch / delete surface for chat attachments and
//! knowledge-base documents.
//! Routes wired in [`crate::create_router`]:
//! - `POST   /api/v1/files/upload` — upload (multipart/form-data)
//! - `GET    /api/v1/files/{id}`   — fetch metadata
//! - `DELETE /api/v1/files/{id}`   — delete

use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::GatewayState;

pub async fn upload_file_handler(
    State(state): State<Arc<GatewayState>>,
    mut multipart: Multipart,
) -> Response {
    // 上传要走带着目录的那一条路。只把字节写进桶、不写目录行，回答里那个「多大、
    // 什么时候传的、校验和是多少」就只活在这一个响应里，之后谁也答不上来——这正是
    // 目录表要修的那件事，所以宁可拒绝上传，也不写一份没人点得到名的数据。
    let store = match state.datasets {
        Some(ref s) => s.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "code": "service_unavailable",
                    "error": "dataset_catalog_not_configured",
                    "message": "File uploads need an object store and its catalog; one of them is missing"
                })),
            )
                .into_response();
        }
    };

    let mut file_data = Vec::new();
    let mut file_name = None;
    let mut content_type = None;

    while let Some(field) = match multipart.next_field().await {
        Ok(f) => f,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "code": "bad_request",
                    "error": "multipart_parse_failed",
                    "message": e.to_string()
                })),
            )
                .into_response();
        }
    } {
        if field.name() == Some("file") {
            file_name = field.file_name().map(|s| s.to_string());
            content_type = field.content_type().map(|s| s.to_string());
            match field.bytes().await {
                Ok(bytes) => file_data = bytes.to_vec(),
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "code": "bad_request",
                            "error": "file_read_failed",
                            "message": e.to_string()
                        })),
                    )
                        .into_response();
                }
            }
        }
    }

    if file_data.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "code": "bad_request",
                "error": "missing_file",
                "message": "No file field found in multipart upload"
            })),
        )
            .into_response();
    }

    let file_id = format!("file-{}", uuid::Uuid::new_v4());
    let key = format!("files/{}", file_id);
    // 名字取客户端给的那个；没给就写 upload，别拿 id 冒充名字——目录里那一列是给
    // 人读的，混进一个 uuid 就没人再认得出这是哪个文件。
    let name = file_name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .unwrap_or("upload")
        .to_string();

    let spec = cog_storage::DatasetWrite {
        name: name.clone(),
        version: file_id.clone(),
        source: "upload".to_string(),
        row_count: None,
        coverage: None,
        media_type: content_type.clone(),
    };

    match store.put_at(&key, spec, &file_data).await {
        Ok(entry) => {
            (
                StatusCode::CREATED,
                Json(json!({
                    "code": "ok",
                    "data": {
                        "file_id": file_id,
                        "uri": key,
                        "file_name": name,
                        "content_type": content_type.unwrap_or_else(|| "application/octet-stream".to_string()),
                        "size": entry.bytes,
                        "content_sha256": entry.content_sha256,
                        "uploaded_at": entry.created_at.to_rfc3339(),
                        "dataset": {
                            "name": entry.name,
                            "version": entry.version,
                        },
                    }
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!("Dataset write failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": "internal_error",
                    "error": "upload_failed",
                    "message": e.to_string()
                })),
            )
                .into_response()
        }
    }
}

pub async fn get_file_handler(
    State(state): State<Arc<GatewayState>>,
    Path(file_id): Path<String>,
) -> Response {
    let backend = match state.object_backend {
        Some(ref b) => b.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "code": "service_unavailable",
                    "error": "object_backend_not_configured",
                    "message": "File storage backend is not available"
                })),
            )
                .into_response();
        }
    };

    let key = format!("files/{}", file_id);

    match backend.exists(&key).await {
        Ok(true) => {
            // We can't get metadata directly from ObjectBackend, so return what we have
            (
                StatusCode::OK,
                Json(json!({
                    "code": "ok",
                    "data": {
                        "file_id": file_id,
                        "key": key,
                        "status": "available",
                    }
                })),
            )
                .into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": "not_found",
                "error": "file_not_found",
                "file_id": file_id,
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!("ObjectBackend exists failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": "internal_error",
                    "error": "check_failed",
                    "message": e.to_string()
                })),
            )
                .into_response()
        }
    }
}

pub async fn delete_file_handler(
    State(state): State<Arc<GatewayState>>,
    Path(file_id): Path<String>,
) -> Response {
    let backend = match state.object_backend {
        Some(ref b) => b.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "code": "service_unavailable",
                    "error": "object_backend_not_configured",
                    "message": "File storage backend is not available"
                })),
            )
                .into_response();
        }
    };

    let key = format!("files/{}", file_id);

    match backend.delete(&key).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "code": "ok",
                "data": {
                    "file_id": file_id,
                    "deleted_at": chrono::Utc::now().to_rfc3339(),
                }
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!("ObjectBackend delete failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": "internal_error",
                    "error": "delete_failed",
                    "message": e.to_string()
                })),
            )
                .into_response()
        }
    }
}
