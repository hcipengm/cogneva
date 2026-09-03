use axum::response::{Html, IntoResponse, Response};
use std::path::PathBuf;

/// WebUI 静态资源目录（含 index.html 与 assets/）。
/// 优先级：COGNEVA_WEB_DIR > COGNEVA_DASHBOARD_PATH 的父目录
/// > $COGNEVA_APP_DIR/web（容器镜像内的标准位置，默认 /opt/cogneva/web）
/// > 源码树 web/dist（开发态从仓库根目录运行时命中）。
/// 注意：最后一档不能依赖编译期内嵌的构建机绝对路径——二进制发到任何别的
/// 机器上该路径都不存在或不可读。
pub fn web_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("COGNEVA_WEB_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(path) = std::env::var("COGNEVA_DASHBOARD_PATH") {
        if let Some(parent) = PathBuf::from(path).parent() {
            return parent.to_path_buf();
        }
    }
    let app_dir = std::env::var("COGNEVA_APP_DIR").unwrap_or_else(|_| "/opt/cogneva".to_string());
    let container_dir = PathBuf::from(app_dir).join("web");
    if container_dir.join("index.html").is_file() {
        return container_dir;
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../web/dist")
}

fn index_path() -> PathBuf {
    if let Ok(path) = std::env::var("COGNEVA_DASHBOARD_PATH") {
        return PathBuf::from(path);
    }
    web_dir().join("index.html")
}

pub async fn dashboard_handler() -> Response {
    let path = index_path();
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
