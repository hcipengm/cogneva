//! WebUI 接管台（2.5.6）：引导器自毁后，运维人员通过浏览器接管自进化系统。
//!
//! 页面为自包含静态 HTML（无构建依赖），调用 admin API：
//! change 列表/审批/应用/部署/回滚、D5 指标、进化事件、审计事件。
//! HTML 本身公开；所有 API 调用仍需 admin Bearer Token（页面中输入）。

use axum::response::{Html, IntoResponse, Response};

const TAKEOVER_HTML: &str = include_str!("takeover.html");

pub async fn takeover_handler() -> Response {
    // no-cache：页面 JS 随版本演进，禁止浏览器启发式缓存旧页面
    // （OAuth 向导等新逻辑必须立即生效）
    (
        [(axum::http::header::CACHE_CONTROL, "no-cache")],
        Html(TAKEOVER_HTML),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #[test]
    fn embedded_page_references_admin_api() {
        let html = super::TAKEOVER_HTML;
        for needle in [
            "/api/v1/evolution/changes",
            "/api/v1/evolution/rollback",
            "/api/v1/evolution/metrics",
            "/api/v1/evolution/events",
            "/api/v1/evolution/promotion/switch",
            "/api/v1/evolution/promotions",
            "/api/v1/evolution/promotion/trend",
            "/api/v1/audit/events",
            "/approve",
        ] {
            assert!(html.contains(needle), "takeover page missing {needle}");
        }
    }
}
