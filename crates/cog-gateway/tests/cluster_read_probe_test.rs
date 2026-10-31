//! 探针要覆盖这一面读的每一条集合。
//!
//! 为什么这是一条门而不是一句注释：`cogneva_cluster_read_total` 的用处是「授权被收窄
//! 了」在没人看页面的时候也变成一条告警。它只有在**探针读的和面板读的是同一批**时才
//! 说得出这件事——面里新加一条读、探针没跟上，那条读的 403 就仍然是屏上一块空白，
//! 而读数面上一片安静。这正是 2026-10-27 的现场：`pods` 授过，两条刚改成集群范围的
//! 集合读没授，屏上是「读不到 Pod」，探针那时还不存在。
//!
//! 判据三面：
//!   1. 源码里每一条 `/api…` 读路径的 (分组, 资源)，要么在 `read_targets` 里，要么在
//!      下面那张**带理由**的豁免表里——豁免不许是空的，也不许留着一个源码里已经没有
//!      的形状；
//!   2. `read_targets` 的每个资源名在源码里都真的读得到（探针不许替一条不存在的读
//!      报健康）；
//!   3. 资源名不重复：同一个资源两条路径会合成一格，删掉一条探针也发现不了。

use std::collections::BTreeSet;

const SOURCE: &str = include_str!("../src/cluster.rs");

/// 读了却**不探**的路径形状，每条都要有理由。
///
/// 空着不是「暂时没想好」：一条读没人探，就是它在授权被收窄时不会告警。
const EXEMPT: &[(&str, &str, &str)] = &[
    (
        "",
        "version",
        "`/version` 任何已认证身份都读得到：它失败说明 apiserver 本身不在，而那一刻节点那一读已经红了",
    ),
    (
        "metrics.k8s.io",
        "pods",
        "可选用量来源：没装 metrics-server 的集群上它本来就该 403，把可选的缺席报成故障就是一张永远亮着的红条",
    ),
    (
        "",
        "nodes/stats",
        "同上的另一条可选用量来源（kubelet summary 经 apiserver 代理），403 是「没有这个来源」而不是故障",
    ),
];

/// 一条读路径的 (分组, 资源)。
fn classify(path: &str) -> Option<(String, String)> {
    let path = path.split('?').next().unwrap_or(path);
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let key = match segments.as_slice() {
        // /api/v1/namespaces/<ns>/<resource>
        ["api", "v1", "namespaces", _, resource] => ("", *resource),
        // /api/v1/<resource>
        ["api", "v1", resource] => ("", *resource),
        // /apis/<group>/<version>/namespaces/<ns>/<resource>
        ["apis", group, _, "namespaces", _, resource] => (*group, *resource),
        // /apis/<group>/<version>/<resource>
        ["apis", group, _, resource] => (*group, *resource),
        // /version：不在 /api 下面，但它是一条真读，豁免表要能对上它。
        ["version"] => ("", "version"),
        // kubelet summary 经 apiserver 代理：`/stats/*` 落到 `nodes/stats` 子资源。
        ["api", "v1", "nodes", _, "proxy", "stats", _] => ("", "nodes/stats"),
        _ => return None,
    };
    Some((key.0.to_string(), key.1.to_string()))
}

/// 源码里出现的每一条 apiserver 路径字面量。
fn literals() -> Vec<String> {
    let mut out = Vec::new();
    for piece in SOURCE.split('"').skip(1).step_by(2) {
        // `/version` 不在 `/api` 下面，但它是同一个客户端、同一条读面。
        if piece.starts_with("/api") || piece == "/version" {
            out.push(piece.to_string());
        }
    }
    out
}

/// 资源名落在哪个分组。探针表只带资源名，分组是 apiserver 的事。
fn group_of(resource: &str) -> &'static str {
    match resource {
        "deployments" | "statefulsets" | "daemonsets" => "apps",
        "cronjobs" | "jobs" => "batch",
        _ => "",
    }
}

#[test]
fn every_collection_the_face_reads_is_probed() {
    let targets = cog_gateway::cluster::read_targets();
    let probed: BTreeSet<(String, String)> = targets
        .iter()
        .map(|(resource, _)| (group_of(resource).to_string(), resource.to_string()))
        .collect();
    assert!(!probed.is_empty(), "探针一条读都不读——它替不了任何一条 403");
    assert_eq!(
        probed.len(),
        targets.len(),
        "探针表里有两条路径落在同一个 (分组, 资源) 上：它们合成一格，删掉一条也看不出来"
    );

    let exempt: BTreeSet<(String, String)> = EXEMPT
        .iter()
        .map(|(g, r, _)| (g.to_string(), r.to_string()))
        .collect();
    let mut unprobed = Vec::new();
    let mut source_shapes = BTreeSet::new();
    for literal in literals() {
        let Some(key) = classify(&literal) else {
            // 按名读（`…/<resource>/<name>`）与子资源在 classify 里落不到形状上；
            // 它们读的是同一批集合，集合那条已经被探针覆盖，代理那一条在豁免表里。
            continue;
        };
        source_shapes.insert(key.clone());
        if !probed.contains(&key) && !exempt.contains(&key) {
            unprobed.push(format!("{literal} → {key:?}"));
        }
    }
    assert!(
        unprobed.is_empty(),
        "这些读没有探针，它们 403 时屏幕上只是一块空白：\n  {}\n在 read_targets 里补一条，\
         或在 EXEMPT 里写下不探的理由",
        unprobed.join("\n  ")
    );

    for (group, resource, _) in EXEMPT {
        assert!(
            source_shapes.contains(&(group.to_string(), resource.to_string())),
            "EXEMPT 里的 {group}/{resource} 在源码里已经没有这条读了：豁免表留着一条读不到的形状，\
             下一次有人加回这条读时它会替它免检"
        );
    }

    for (group, resource) in &probed {
        assert!(
            source_shapes.contains(&(group.clone(), resource.clone())),
            "探针在读 {group}/{resource}，而这一面已经不读它了——探针替一条不存在的读报健康"
        );
    }
}

/// 探针的路径必须带 `limit=1`：探的是「这条读通不通」，不是「集群里有什么」。
///
/// 这条判的是 `probe_reads` 的写法，而不是 `read_targets`：预算路径留给面板用。
#[test]
fn the_probe_asks_for_one_row() {
    let probe = SOURCE
        .split("async fn probe_reads")
        .nth(1)
        .expect("找不到 probe_reads")
        .split("pub fn spawn_read_probe")
        .next()
        .unwrap();
    // 告警文案让读的人去看 Pod 日志里的状态行，所以失败那一支必须真的写一行：
    // 计数只说「没通」，说不清是 403（授权被收窄）、401（令牌没到）还是超时
    // （apiserver 忙），而这三种修法完全不同。这条判的是那行日志还在不在——
    // 文案可以改，承诺不能悄悄少一半。
    assert!(
        probe.contains("tracing::warn!"),
        "失败的那一支没有日志：告警文案点的那行状态行在日志里不存在"
    );
    assert!(
        probe.contains("?limit=1"),
        "探针没有把 limit=1 拼进路径：一分钟一次的探针会把整个集群的 Pod 读回来"
    );
}

/// 每一条读都要跟着写「是哪个进程写的」。
///
/// 这一面由两个 Deployment 发布——同一个二进制、两个 ServiceAccount。授权坏在
/// 绑定时只坏一边，而读数里没有进程名时，告警只说得出「nodes 读不通」，读者还得
/// 自己去抓取面翻是谁；两边同时失败时更是并成一行。部署名来自
/// `COGNEVA_DEPLOYMENT_NAME`（清单里两个工作负载都设了），不是抓取配置的 `job`：
/// `job` 是 Prometheus 起的名字，改一次 scrape config 就换了。
#[test]
fn every_read_says_which_deployment_wrote_it() {
    assert!(
        SOURCE.contains("cog_core::env::DEPLOYMENT_NAME_ENV"),
        "探针不再读 COGNEVA_DEPLOYMENT_NAME：读数里没有进程名，两个进程的失败就并成一条，\
         而告警说不出该去查哪一个 ServiceAccount 的绑定"
    );
    let probe = SOURCE
        .split("async fn probe_reads")
        .nth(1)
        .expect("找不到 probe_reads")
        .split("pub fn spawn_read_probe")
        .next()
        .unwrap();
    assert!(
        probe.contains("deployment.as_deref()"),
        "探针把部署名丢了：publish_read 收到的永远是 None，标签写不进去"
    );
    assert!(
        SOURCE.contains("crate::LABEL_DEPLOYMENT"),
        "publish_read 不再写 deployment 标签：同一条读数两个进程写出来是同一行"
    );
}
