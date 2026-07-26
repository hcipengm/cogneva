use axum::response::{Html, IntoResponse, Response};
use std::path::PathBuf;

fn dashboard_path() -> PathBuf {
    if let Ok(path) = std::env::var("COGNEVA_DASHBOARD_PATH") {
        PathBuf::from(path)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/web/index.html")
    }
}

pub async fn dashboard_handler() -> Response {
    let path = dashboard_path();
    match tokio::fs::read_to_string(&path).await {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!("failed to read dashboard from {}: {}", path.display(), e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("dashboard unavailable: {}", e),
            )
                .into_response()
        }
    }
}
