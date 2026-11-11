//! 指标路径只声明一次，每个面都从那一处取。
//!
//! `/metrics` 由多个面回答：执行器、网关、安全网关的文本与 JSON 面，以及
//! `cogneva` 二进制里三条独立观测子命令（claim-store / host-ledger /
//! volume-walker）；`cog-observability` 的 Pushgateway 客户端也按同一基路径拼
//! `/metrics/job/...`。每个面各写一遍字面量时，改路径不报错：没改到的那个面继续
//! 在旧路径上答，抓取端从一个副本拿到 404，读数看起来只是「这个 Pod 很安静」。
//!
//! 本门普查的是**拼写**：生产源里除声明文件外不许再出现带引号的 `/metrics`，
//! 于是改声明值等于改所有站点——没有哪个站点保得住自己的副本。另一半数的是
//! **接线**：已知的主面里必须真的有路由用这两个常量注册，免得声明在、没人用。

use crate::common;

use common::root::repo_root;
use common::rust_production::{production_numbered_lines, production_sources, relative_name};

/// 唯一允许拼出该路径的地方。
const DECLARATION: &str = "crates/cog-core/src/contract/observability.rs";

#[test]
fn the_metrics_path_is_spelled_only_in_its_declaration() {
    let root = repo_root();
    let mut second_spellings = Vec::new();
    let mut text_routes = Vec::new();
    let mut json_routes = Vec::new();

    for path in production_sources(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let name = relative_name(&root, &path);
        for (number, line) in production_numbered_lines(&text) {
            if name != DECLARATION && line.contains("\"/metrics") {
                second_spellings.push(format!("{name}:{number}: {line}"));
            }
            if line.contains(".route(METRICS_PATH") {
                text_routes.push(name.clone());
            }
            if line.contains(".route(METRICS_JSON_PATH") {
                json_routes.push(name.clone());
            }
        }
    }

    assert!(
        second_spellings.is_empty(),
        "指标路径在声明之外还有第二份拼写；改声明值不会改到它们：\n{}",
        second_spellings.join("\n")
    );

    assert!(
        text_routes.len() >= 4,
        "只读到 {} 处用 `METRICS_PATH` 注册的路由（至少四处：执行器、网关、安全网关两处）；\
         读到的文件：{:?}",
        text_routes.len(),
        text_routes
    );
    assert!(
        text_routes
            .iter()
            .any(|name| name.ends_with("cog-extension/src/command_server.rs")),
        "执行器的指标路由没有接上声明：{text_routes:?}"
    );
    assert!(
        text_routes
            .iter()
            .any(|name| name.ends_with("cog-gateway/src/lib.rs")),
        "网关的指标路由没有接上声明：{text_routes:?}"
    );
    assert!(
        text_routes
            .iter()
            .filter(|name| name.ends_with("cog-gateway/src/security_gateway.rs"))
            .count()
            >= 2,
        "安全网关的两条路由没有都接上声明：{text_routes:?}"
    );
    assert!(
        json_routes
            .iter()
            .filter(|name| name.ends_with("cog-gateway/src/security_gateway.rs"))
            .count()
            >= 2,
        "安全网关的 JSON 指标路由没有接上声明：{json_routes:?}"
    );
}
