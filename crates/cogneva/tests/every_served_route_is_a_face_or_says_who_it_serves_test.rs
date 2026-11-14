//! A route nobody can reach from a page is a capability that does not exist.
//!
//! The WebUI is the one place a person meets this system. A gateway route with
//! no reader in `web/src` is therefore either a face somebody forgot to build,
//! or a route that serves something other than a person — another process, an
//! agent, a peer. Both are fine answers; what is not fine is not knowing which.
//!
//! The direction `web_resource_face_keys_are_served_routes_test` deliberately
//! does not judge ("a route with no face subscribing to it is not a defect --
//! many prefixes are read and written by other systems") is exactly the one
//! judged here, and the way it stays honest is by making the classification an
//! equality: every prefix is either mentioned in the client or listed below
//! with the system that does read it.
//!
//! A new route nobody classifies turns this red, which is the day a capability
//! grew and nobody decided whether a person should be able to see it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::common;

use common::root::repo_root;
use common::routes::{route_entries, walk};

/// Prefixes served to something other than the WebUI, with who reads them.
///
/// Every entry is a claim about another system, written where the route is
/// judged rather than where the route is registered: the registration says what
/// the process answers, not who asks.
const NOT_A_FACE: &[(&str, &str)] = &[
    (
        ".well-known",
        "A2A 对端：先取名片，再按名片声明的方式取凭证",
    ),
    ("git", "平台自己的 git 出站：工作树拉取与镜像推送经这条透传"),
    ("gitee", "平台自己的出站透传：Gitee API，凭证只在安全网关"),
    ("github", "平台自己的出站透传：GitHub API，凭证只在安全网关"),
    (
        "v1",
        "LLM 上游协议面：chat/completions 与 responses，调用方是 agent 与外部客户端",
    ),
    ("protocol", "A2A / MCP 对端：名片、工具列表、消息流、RPC"),
    (
        "webhooks",
        "GitHub / Gitee 的入站回调，发送方是代码托管平台",
    ),
    (
        "internal",
        "集群内进程：贡献面、LLM 状态、检索后备、任务预算、Secret 补丁",
    ),
    ("metrics", "Prometheus 抓取与自监控，读者是抓取端不是人"),
    (
        "search-fallback",
        "检索后备出口：给进程用的池子，不是给页面的搜索框",
    ),
    ("guard", "沙盒与执行侧的准入检查，调用方是执行器"),
    ("sandbox", "沙盒执行入口，调用方是编排器"),
    ("pricing", "上游计价的内部重算，调用方是 token 账"),
    ("proxy", "通用透传出口，调用方是进程而不是浏览器"),
    ("attach", "容器/会话附着代理，调用方是终端客户端"),
    ("takeover", "接管请求：由外部操作者发起的会话接管，不经页面"),
];

/// Prefixes with no reader anywhere, kept as counted debt.
///
/// These are the ones this census exists to surface: the route is served, no
/// page asks for it, and no other system does either. Each is a capability a
/// person cannot reach. Recording one here is a decision with a number, not a
/// pass — the ceiling only comes down, and a new one has to say why.
const NO_FACE_YET: &[(&str, &str)] = &[
    ("wiki", "知识面的读写接口齐了，WebUI 上还没有它的面"),
    ("knowledge", "检索接口在，页面走的是别的搜索框"),
    ("hooks", "钩子的增删查在，界面上只有状态展示、没有编辑面"),
    ("datasets", "数据集列表与校验在，界面上没有入口"),
    ("snapshots", "快照列表与下载地址在，界面上没有入口"),
    ("workspaces", "工作区列表与成员在，界面上没有入口"),
    ("squads", "编队状态在，界面上没有入口"),
    (
        "action-plan",
        "动作计划本该是判词面的一部分，今天只在 API 上",
    ),
    ("dashboard", "旧的仪表盘入口，界面已改走观测面的六块板"),
    ("plugins", "插件的上传与卸载在，界面上没有入口"),
    ("raw_logs", "原始日志接口在，日志板读的是另一条流"),
    ("dlq", "死信重投在，界面上没有入口"),
    ("search", "搜索接口在，页面走的是 knowledge/search"),
    ("files", "文件上传与下载在，界面上没有入口"),
];

/// How many prefixes are served with no reader anywhere (measured 2026-11-15).
const NO_FACE_AT_CENSUS: usize = 14;

/// The first path segment of a route shape, or `None` when the route is not
/// under a `/api/` or `/v1/` style root.
fn prefix_of(shape: &str) -> Option<String> {
    let mut segments = shape.split('/').filter(|s| !s.is_empty());
    let first = segments.next()?;
    // `/api/v1/<face>` and `/api/<face>` both name the face in a different
    // position; the tag between them is not the face.
    let face = if first == "api" {
        let second = segments.next()?;
        if second == "v1" || second == "v2" {
            segments.next()?
        } else {
            second
        }
    } else {
        first
    };
    (!face.starts_with('{') && !face.starts_with('*') && !face.starts_with(':'))
        .then(|| face.to_string())
}

/// Every prefix the client names, read off the whole `web/src` tree.
fn client_prefixes(root: &Path) -> BTreeSet<String> {
    let mut files = Vec::new();
    walk(&root.join("web/src"), &["ts", "tsx"], &mut files);
    let mut found = BTreeSet::new();
    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for shape in shapes_in_client(&text) {
            if let Some(prefix) = prefix_of(&shape) {
                found.insert(prefix);
            }
        }
    }
    found
}

/// The path shapes a client file mentions.
///
/// Read as text rather than by splitting on quotes: a client file mixes `'`,
/// `"` and backticks, and one unbalanced backtick anywhere shifts every later
/// piece into the "outside" half — which reads as "this path is never
/// requested", the exact answer this gate must not produce by accident. A path
/// is a run of path characters starting at a `/`, which needs no quoting model.
fn shapes_in_client(text: &str) -> Vec<String> {
    let bytes: Vec<char> = text.chars().collect();
    let mut shapes = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == '/' {
            let mut j = i;
            while j < bytes.len()
                && (bytes[j].is_ascii_alphanumeric()
                    || matches!(bytes[j], '/' | '_' | '-' | '.' | '$' | '{' | '}'))
            {
                j += 1;
            }
            let piece: String = bytes[i..j].iter().collect();
            if piece.starts_with("/api/")
                || piece.starts_with("/ws")
                || piece.starts_with("/grafana")
                || piece.starts_with("/files")
                || piece.starts_with("/attach")
                || piece.starts_with("/dashboard")
            {
                shapes.push(piece);
            }
            i = j.max(i + 1);
        } else {
            i += 1;
        }
    }
    shapes
}

#[test]
fn every_served_route_is_a_face_or_says_who_it_serves() {
    let root = repo_root();
    let served: BTreeMap<String, BTreeSet<String>> = {
        let mut map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for route in route_entries(&root) {
            let Some(prefix) = prefix_of(&route.shape) else {
                continue;
            };
            map.entry(prefix).or_default().extend(route.handlers);
        }
        map
    };
    assert!(
        served.len() > 10,
        "只读出 {} 个路由前缀，这个门没有题目",
        served.len()
    );

    let client = client_prefixes(&root);
    assert!(
        !client.is_empty(),
        "web/src 里一个 /api 形状都没读到，这个门会把每个前缀都当成没人用"
    );

    let classified: BTreeSet<&str> = NOT_A_FACE
        .iter()
        .chain(NO_FACE_YET.iter())
        .map(|(p, _)| *p)
        .collect();
    let unclassified: Vec<(&String, &BTreeSet<String>)> = served
        .iter()
        .filter(|(prefix, _)| !client.contains(*prefix) && !classified.contains(prefix.as_str()))
        .collect();

    assert!(
        unclassified.is_empty(),
        "这些路由前缀在 WebUI 里没有任何读者，也没有登记谁在用它们：{:?}。\
         要么给它一个面（WebUI 是人和系统交互的唯一渠道，能力在页面上点不到就等于没有），\
         要么在 NOT_A_FACE 里写明是哪个进程/对端在读它——「判不了」是一条要写下来的决定",
        unclassified
            .iter()
            .map(|(prefix, handlers)| format!("{prefix} <- {handlers:?}"))
            .collect::<Vec<_>>()
    );

    // 欠账只能降：一个前缀从界面上被补出来，这个数跟着降；涨的那一笔要说清为什么。
    assert_eq!(
        NO_FACE_YET.len(),
        NO_FACE_AT_CENSUS,
        "没有面的前缀数变了（现在 {} 条，常数 {NO_FACE_AT_CENSUS}）。多一条要说明为什么这个能力只能停在 API 上；少一条这个数跟着降",
        NO_FACE_YET.len()
    );

    // 表里每一行都得还被路由服务着：一条指向已经不存在的路由的登记，是替
    // 下一个同名前缀背书。
    let stale: Vec<&str> = NOT_A_FACE
        .iter()
        .chain(NO_FACE_YET.iter())
        .map(|(prefix, _)| *prefix)
        .filter(|prefix| !served.contains_key(*prefix) && !client.contains(*prefix))
        .collect();
    assert!(
        stale.is_empty(),
        "NOT_A_FACE 里这些前缀已经没有被服务了，登记过期：{stale:?}"
    );
}
