//! 远程走哪条路，由实测决定，不由「哪个环境变量设了」决定。
//!
//! 门判两件事：
//!
//! 1. 行为——配了网关基址只让网关成为一条候选；它上一次没走通就排到后面，走通过
//!    的两条按耗时排。没有哪条路因为配过就永远先走，也没有哪条路因为失败过一次就
//!    永远不再被试。
//! 2. 调用点——克隆与推送这两条真正走网络的路，选路都经过同一个排序函数；前一条
//!    改动之前 `select_remote_url_for` 是 first-match-wins，网关设了就永远先走。
//!
//! 没有这次改动本文件第 2 条会红（`select_remote_url_for` 还在），第 1 条会红
//! （网关在候选表里永远排第一）。

use cog_github::route::{self, Candidate, RouteBook};

fn keys(candidates: &[Candidate], book: &RouteBook) -> Vec<String> {
    route::ordered(candidates, book)
        .into_iter()
        .map(|candidate| candidate.route().to_string())
        .collect()
}

fn github_routes() -> Vec<Candidate> {
    route::candidates("github", "o/r", Some("http://gw"), true, Some("t"), true)
}

/// 配了网关基址只是让它进入候选；它没被量过、也没走通过，就不排在量过并走通的路前面。
#[test]
fn a_configured_gateway_participates_rather_than_preempts() {
    let routes = github_routes();
    let mut book = RouteBook::default();
    book.record_fail("github:proxy", 30, "connection refused");
    book.record_ok("github:token", 120);
    assert_eq!(
        keys(&routes, &book),
        vec!["token", "ssh", "anonymous", "proxy"]
    );
}

/// 走通过的路按自己的耗时排，快的那条在前。
#[test]
fn the_fastest_measured_route_goes_first() {
    let routes = github_routes();
    let mut book = RouteBook::default();
    book.record_ok("github:token", 120);
    book.record_ok("github:proxy", 40);
    assert_eq!(
        keys(&routes, &book),
        vec!["proxy", "token", "ssh", "anonymous"]
    );
}

/// 没试过的路排在失败过的路之前，所以一条失败过的路日后还有机会被重新量。
#[test]
fn an_untried_route_outranks_a_failed_one() {
    let routes = github_routes();
    let mut book = RouteBook::default();
    book.record_fail("github:proxy", 10, "timeout");
    let order = keys(&routes, &book);
    assert_eq!(order.first().map(String::as_str), Some("ssh"));
    assert_eq!(order.last().map(String::as_str), Some("proxy"));
}

/// 克隆与推送两条路都经同一个排序函数选路；改回 first-match-wins 会让这里红。
#[test]
fn both_network_paths_select_a_route_through_the_same_ordering() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let landing =
        std::fs::read_to_string(root.join("crates/cog-github/src/landing.rs")).expect("landing.rs");
    assert!(
        !landing.contains("select_remote_url_for"),
        "first-match-wins 的选路还在：设了网关基址就走网关，不问它此刻通不通"
    );
    assert_eq!(
        landing.matches("crate::route::ordered(").count(),
        3,
        "选路要在克隆、推送、镜像三处都经过同一个排序函数"
    );
}
