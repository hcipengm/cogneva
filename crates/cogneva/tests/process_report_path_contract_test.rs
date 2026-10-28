//! The process self-report path: the address the reporter posts to, the door it
//! lands on, and who is allowed through it — three files that have to agree.
//!
//! A container pushes its own `/proc` table to the gateway every minute. Nothing
//! in that path is checked by a compiler: the reporter carries a URL written as a
//! constant, the gateway publishes that path on one of two ports, and the network
//! policy decides which pods may reach it. Every one of those can move without
//! the build noticing:
//!
//!   - the Service's internal port changes, and every reporter posts into a
//!     closed port for as long as nobody deploys and looks;
//!   - the route moves to the browser-facing router, and a write endpoint is now
//!     reachable from outside the cluster — the arm that was supposed to be the
//!     narrow one is still green;
//!   - the ingress stops admitting this platform's own pods, and the report is
//!     silently dropped by a NetworkPolicy: the request never reaches the
//!     handler, no error surfaces anywhere, and the screen shows the same "that
//!     container does not report" cell it shows for a third-party image.
//!
//! The failure shape is the same in all three: the feature looks implemented and
//! reads as absent, and no reading separates "nobody reports" from "reports are
//! being dropped".
//!
//! What this asserts is the **declared** path. Whether the loop actually runs in
//! a live pod is a different question, answered by the topology reading itself.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Deserialize as _;
use serde_yaml::Value;

mod common;

use common::routes::repo_root;

use common::rust_production::{production_sources, production_text};

fn read(rel: &str) -> String {
    std::fs::read_to_string(repo_root().join(rel)).unwrap_or_else(|e| panic!("读不到 {rel}：{e}"))
}

/// All documents in the static manifests of the k3s profile.
///
/// `deploy/k3s/` is the parity baseline the chart render is compared against, so
/// asking it rather than the templates means the answer is the shape that is
/// actually applied.
fn manifests() -> Vec<(String, Value)> {
    let dir = repo_root().join("deploy/k3s");
    let mut out = Vec::new();
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("deploy/k3s")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
        .collect();
    paths.sort();
    for path in paths {
        let text = std::fs::read_to_string(&path).expect("manifest");
        for document in serde_yaml::Deserializer::from_str(&text) {
            let Ok(value) = Value::deserialize(document) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            out.push((
                path.file_name().unwrap().to_string_lossy().to_string(),
                value,
            ));
        }
    }
    out
}

/// `dig(&doc, &["metadata", "name"])` — serde_yaml's `Value` has no JSON pointer.
fn dig<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(value, |node, key| node.get(*key))
}

/// `http://cogneva:8085/path` → ("cogneva", 8085, "/path").
fn parse_report_url(url: &str) -> (String, u16, String) {
    let rest = url
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("自报地址必须是集群内的明文 http：{url}"));
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = authority
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("自报地址要带端口：{url}"));
    (
        host.to_string(),
        port.parse().expect("端口是数字"),
        format!("/{path}"),
    )
}

#[test]
fn the_reporter_posts_to_the_service_the_manifests_publish() {
    let source = read("crates/cogneva/src/process_report.rs");
    let start = source.find("DEFAULT_REPORT_URL").expect("自报地址常量");
    let line = source[start..].lines().next().expect("一行");
    let url = line
        .split('"')
        .nth(1)
        .unwrap_or_else(|| panic!("这一行没有字面量：{line}"));
    let (service, port, _path) = parse_report_url(url);

    let documents = manifests();
    let service_doc = documents
        .iter()
        .find(|(_, doc)| {
            doc.get("kind").and_then(Value::as_str) == Some("Service")
                && dig(doc, &["metadata", "name"]).and_then(Value::as_str) == Some(service.as_str())
        })
        .unwrap_or_else(|| panic!("deploy/k3s 里没有名为 {service} 的 Service"));

    let ports = service_doc
        .1
        .get("spec")
        .and_then(|spec| spec.get("ports"))
        .and_then(Value::as_sequence)
        .expect("Service 要有 ports");
    let named = ports
        .iter()
        .find(|entry| entry.get("port").and_then(Value::as_u64) == Some(u64::from(port)));
    let Some(entry) = named else {
        panic!(
            "Service/{service} 没有 {port} 端口：自报会打到一个没人听的端口上，\
             而这件事只会在部署之后由一片安静的屏来体现"
        );
    };
    assert_eq!(
        entry.get("name").and_then(Value::as_str),
        Some("internal"),
        "自报要落在零凭证的集群内面：落在别的端口上就是把写入口放到了别的判据后面"
    );
}

#[test]
fn the_report_path_is_published_on_the_internal_router_and_nowhere_else() {
    let source = read("crates/cogneva/src/process_report.rs");
    let start = source.find("DEFAULT_REPORT_URL").expect("自报地址常量");
    let url = source[start..].lines().next().expect("一行");
    let (_service, _port, path) = parse_report_url(url.split('"').nth(1).expect("字面量"));

    let gateway = read("crates/cog-gateway/src/lib.rs");
    let internal_start = gateway
        .find("pub fn create_internal_router")
        .expect("内部面的路由器");
    let internal_end = gateway[internal_start..]
        .find("\n}\n")
        .map(|offset| internal_start + offset)
        .expect("函数体结束");
    let internal = &gateway[internal_start..internal_end];

    assert!(
        internal.contains(&path),
        "内部路由器里没有 {path}：自报地址指的是一条这条面不挂的路径"
    );

    let public_start = gateway.find("pub fn create_router").expect("主路由器");
    let public_end = gateway[public_start..]
        .find("let auth_layer")
        .map(|offset| public_start + offset)
        .expect("公开路由段结束");
    assert!(
        !gateway[public_start..public_end].contains(&path),
        "{path} 也挂在了浏览器能到的那一面上：这个端点是收写入的，不该有第二扇门"
    );
}

#[test]
fn the_ingress_admits_this_platforms_own_pods_on_the_internal_port() {
    let source = read("crates/cogneva/src/process_report.rs");
    let start = source.find("DEFAULT_REPORT_URL").expect("自报地址常量");
    let url = source[start..].lines().next().expect("一行");
    let (_service, port, _path) = parse_report_url(url.split('"').nth(1).expect("字面量"));

    let documents = manifests();
    let policy = documents
        .iter()
        .find(|(_, doc)| {
            doc.get("kind").and_then(Value::as_str) == Some("NetworkPolicy")
                && dig(doc, &["metadata", "name"]).and_then(Value::as_str)
                    == Some("cogneva-internal-ingress")
        })
        .expect("主应用 Pod 的入站策略")
        .1
        .clone();

    let admits = |selector_pairs: &[(&str, &str)]| -> bool {
        let rules = policy
            .get("spec")
            .and_then(|spec| spec.get("ingress"))
            .and_then(Value::as_sequence)
            .expect("ingress 规则");
        rules.iter().any(|rule| {
            let has_port = rule
                .get("ports")
                .and_then(Value::as_sequence)
                .map(|ports| {
                    ports.iter().any(|entry| {
                        entry.get("port").and_then(Value::as_u64) == Some(u64::from(port))
                    })
                })
                .unwrap_or(false);
            if !has_port {
                return false;
            }
            let from = rule.get("from");
            let Some(from) = from.and_then(Value::as_sequence) else {
                // 没写 from＝任何来源，当然也算放行。
                return true;
            };
            from.iter().any(|peer| {
                let labels = peer
                    .get("podSelector")
                    .and_then(|selector| selector.get("matchLabels"));
                let Some(labels) = labels.and_then(Value::as_mapping) else {
                    return false;
                };
                selector_pairs.iter().all(|(key, value)| {
                    labels
                        .get(Value::String((*key).to_string()))
                        .and_then(Value::as_str)
                        == Some(*value)
                })
            })
        })
    };

    assert!(
        admits(&[("app.kubernetes.io/name", "cogneva")]),
        "入站策略没有放行本平台自己的 Pod（name=cogneva）到 {port}：自报会被策略静默丢弃，\
         而屏上看到的与「这个容器不报」一模一样"
    );
    assert!(
        !admits(&[("app.kubernetes.io/name", "postgres")]),
        "入站策略把第三方数据面也放进了 {port}：这条面的判据是「谁能连这个端口」"
    );
}

/// The commands that start the reporter, read from `main.rs`.
///
/// The set is the arm of the `if matches!(` that guards the spawn call — that
/// guard is the one place that decides which workloads report.
fn reporting_commands() -> BTreeSet<String> {
    let source = read("crates/cogneva/src/main.rs");
    let start = source
        .find("process_report::spawn()")
        .expect("main.rs 里启动自报循环");
    let guard = source[..start]
        .rfind("if matches!(")
        .expect("启动点前面那圈 matches! 判定");
    let mut commands = BTreeSet::new();
    for piece in source[guard..start].split("Command::").skip(1) {
        let name: String = piece.chars().take_while(|c| c.is_alphanumeric()).collect();
        if !name.is_empty() {
            commands.insert(name);
        }
    }
    commands
}

#[test]
fn a_command_that_cannot_reach_the_internal_face_is_not_asked_to_report() {
    // 沙箱执行器的出站面是收紧过的：它够不到 8085。把它放进自报名单，那是一条
    // 每分钟都发、每分钟都被策略丢掉的请求——安静地空转，谁也不会红。
    let reporting = reporting_commands();
    assert!(
        reporting.contains("Run"),
        "主应用不报的话这条通道在主工作负载上就是空的"
    );

    let documents = manifests();
    let egress = documents.iter().find(|(_, doc)| {
        doc.get("kind").and_then(Value::as_str) == Some("NetworkPolicy")
            && dig(doc, &["metadata", "name"]).and_then(Value::as_str)
                == Some("cogneva-sandbox-executor-egress")
    });
    let Some((_, policy)) = egress else {
        return;
    };
    let admits_all = policy
        .get("spec")
        .and_then(|spec| spec.get("egress"))
        .and_then(Value::as_sequence)
        .map(|rules| {
            rules
                .iter()
                .any(|rule| rule.as_mapping().map(|m| m.is_empty()) == Some(true))
        })
        .unwrap_or(false);
    assert!(
        !admits_all,
        "沙箱执行器的出站面已经不是「全拒」了：这条断言的前提变了，先判它该不该报"
    );
    assert!(
        !reporting.contains("SandboxExecutor"),
        "沙箱执行器连不到 8085（它的出站策略只放行安全网关、DNS 与 443），\
         让它报就是每分钟发一次必被丢弃的请求"
    );
}

/// 自报地址只有一份写法：一个是写它的文件，其余都得引用它。
///
/// 采集器曾经在 `process_census.rs` 里把同一个 `http://…` 又写了一遍，两个常量谁也
/// 不知道谁。改端口时改一处不会报错，另一处照旧把读数发到一个没人听的地址上——而
/// 「发不过去」在读数面上与「这台节点没有进程」是同一种空白。
#[test]
fn the_report_address_is_spelled_in_one_production_file() {
    // 规范化过的根：read_dir 走出来的路径带着 `../..`，判词里点名一个这样的路径没人
    // 照着找得到文件。
    let root = repo_root().canonicalize().expect("仓库根");
    let report = read("crates/cogneva/src/process_report.rs");
    let line = report
        .find("DEFAULT_REPORT_URL")
        .and_then(|start| report[start..].lines().next())
        .expect("自报地址常量");
    let url = line
        .split('"')
        .nth(1)
        .expect("自报地址的字面量")
        .to_string();
    let literal = format!("\"{url}\"");

    let sources = production_sources(&root);
    assert!(
        report.contains(&literal),
        "自报模块自己没有写出 {literal}——读取器问错了文件"
    );

    let mut offenders = Vec::new();
    for path in &sources {
        if path.ends_with("crates/cogneva/src/process_report.rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        if production_text(&text).contains(&literal) {
            offenders.push(
                path.strip_prefix(&root)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .to_string(),
            );
        }
    }
    assert!(
        offenders.is_empty(),
        "这些生产源文件把自报地址又写了一遍：{offenders:?}——同一个端口与路径存两份，\
         改一处不会报错，另一处照旧把读数发到一个没人听的地址上"
    );
}

#[test]
fn the_census_reports_to_the_same_address_as_the_container_reporter() {
    assert_eq!(
        cogneva::process_census::DEFAULT_URL,
        cogneva::process_report::DEFAULT_REPORT_URL,
        "采集器的默认落点与容器自报不是同一个地址：采集器的读数会一直发到一个没人听的\
         端点上，而那在这条读数上与「这台节点没有进程」同形"
    );
}
