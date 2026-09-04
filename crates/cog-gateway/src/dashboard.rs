use axum::response::{Html, IntoResponse, Response};
use std::path::PathBuf;

/// WebUI 静态资源目录（含 index.html 与 assets/）。
/// 优先级：COGNEVA_WEB_DIR，其次 COGNEVA_DASHBOARD_PATH 的父目录，
/// 最后运行时自定位（从当前二进制位置向上逐级查找含 index.html 的
/// web 目录：容器内命中同级 web/，开发态从 target/debug 向上爬到
/// 仓库根的 web/dist/）。
///
/// 不内嵌任何编译期绝对路径：二进制随整个目录树搬迁到任何位置，只要
/// web 与二进制的相对布局不变就能找到；显式 env 仍是第一优先级。
pub fn web_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("COGNEVA_WEB_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(path) = std::env::var("COGNEVA_DASHBOARD_PATH") {
        if let Some(parent) = PathBuf::from(path).parent() {
            return parent.to_path_buf();
        }
    }
    if let Some(dir) = locate_web_dir() {
        return dir;
    }
    // 全部未命中：返回二进制同级 web 作为最后位置，让报错路径指向真实
    // 查找目标，而不是一个与运行环境无关的编译期路径。
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|p| p.join("web")))
        .unwrap_or_else(|| PathBuf::from("web"))
}

/// 从当前可执行文件位置出发，向上逐级查找含 index.html 的 web 目录。
/// 每一级先查 `<dir>/web`（容器布局），再查 `<dir>/web/dist`（源码树
/// 布局），命中即返回。
fn locate_web_dir() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let mut dir = exe.parent()?;
    loop {
        let container_layout = dir.join("web");
        if container_layout.join("index.html").is_file() {
            return Some(container_layout);
        }
        let source_layout = dir.join("web/dist");
        if source_layout.join("index.html").is_file() {
            return Some(source_layout);
        }
        dir = dir.parent()?;
    }
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
