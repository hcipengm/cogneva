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
    ("dlq", "只有 POST /api/v1/dlq/retry：死信重投是动作，没有可供列出的队列接口"),
    ("squads", "编队状态由观测面的编队行读出，没有独立的列表接口"),
    (
        "datasets",
        "按名取：/api/v1/datasets/{name} 要先知道数据集名，而没有任何接口先列出这些名字——页面列不出来，这不是缺一个视图，是缺一个可枚举的入口",
    ),
    (
        "dashboard",
        "WebUI 自身的入口页（SPA 的 index.html）：人打开的就是它，不是被页面读取的接口。它没有「界面读者」是因为它本身就是界面",
    ),
];

/// Prefixes with no reader anywhere, kept as counted debt.
///
/// These are the ones this census exists to surface: the route is served, no
/// page asks for it, and no other system does either. Each is a capability a
/// person cannot reach. The table is **empty**, and that is the end state: a
/// count that may fall is a permission, and a permission is inherited. An entry
/// here would mean a capability that exists only in the API — the assertion
/// below refuses it.
const NO_FACE_YET: &[(&str, &str)] = &[];

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
    // 客户端的模板串会在段中间插值（`/api/v1/raw_logs${suffix}`）。只取到第一个
    // 不是标识符的字符为止——否则同一个前缀在两侧拼成两个不同的字符串，而它们指
    // 的是同一条路由。这一段在变基时丢过一次，表现是「界面明明读着，判据说没人读」。
    let face: String = face
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        .collect();
    (!face.is_empty() && !face.starts_with('*')).then_some(face)
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

    // 终态是**空**，不是一个还在降的数：计数是许可，许可会被继承。
    // 谁新增一个只有接口、没有界面的能力，这条立刻红——修法是给它一个面，
    // 不是在表里加一行；确实不该有界面的，写进 NOT_A_FACE 并说明谁在用。
    assert!(
        NO_FACE_YET.is_empty(),
        "这些能力只有接口、没有界面入口（WebUI 是唯一的人机面，点不到就等于没有）：{:?}",
        NO_FACE_YET
    );

    // 表里每一行还得**真的没人读**：单向检查（只问「路由还在不在」）会让补齐了
    // 界面的行永远留在表里，于是这张表从「欠账」变成「一份过时的自述」。
    // 实测过：`raw_logs` 与 `files` 都已经有了界面读者，却还挂在表上。
    let now_read: Vec<&str> = NO_FACE_YET
        .iter()
        .map(|(prefix, _)| *prefix)
        .filter(|prefix| client.contains(*prefix))
        .collect();
    assert!(
        now_read.is_empty(),
        "这些前缀已经有界面读者了，不能再算「没有入口」——把行删掉：{now_read:?}"
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
