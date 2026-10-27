//! principle: feedback_no_hardcoding_config_surface
//!
//! 日志流的标签取值在两侧各写一遍：`cog-observability` 把 `job` / `service` 的
//! 取值（`"cogneva"`）写进每条 Loki 推送的 base labels，Grafana 面板里的 Loki
//! 查询按 `{job="cogneva"}` 选流。没有任何东西比较这两处。
//!
//! 漂移的后果是静默的：改代码侧的取值，面板照样渲染，只是永远选不中任何一条流——
//! 空的 trace 面板与「这段时间没有日志」是同一个读数。Prometheus 那侧有门管
//! （面板与规则对着代码声明过的标签与取值判），Loki 这侧此前没有：`observability_
//! dashboard_contract_test` 只读 `"expr"` 里有指标名的那一类。
//!
//! 判什么：
//!
//! * 代码侧：`LokiPushClient::new` 写进 `base_labels` 的 `job` / `service` 取值
//!   （经 `LABEL_JOB` / `LABEL_SERVICE` 常量，字面量读出来）；
//! * 部署侧：`deploy/k3s/observability/manifests/*.yaml` 里**以 `{` 开头**的查询
//!   （有指标名的是 PromQL，`up{job="cogneva-host-readings"}` 那类 scrape job 是
//!   Prometheus 配置给的，不归这条判据管），其中 `job=` / `service=` 匹配器的
//!   取值；
//! * 部署侧读到的每个取值，都必须是代码侧写过的那个。
//!
//! 判不了什么：
//!
//! * 面板里用模板变量选的流（`{job=~"$job"}`）——取值在运行时才知道；
//! * 别的进程推给 Loki 的流（网关、reflection 各有一处 push client）：它们的
//!   base labels 由各自的 `LokiPushClient::new` 写，本门只看主进程这处；
//! * 标签名与日志条目形状对不对——只比「那一侧是不是同一个词」。
//!
//! 普查要有下限：代码侧读不到 `job` / `service` 的取值，或部署侧一个 Loki 查询
//! 都没读到时按红处理——「没读到」与「没有漂移」在结果上分不开。
//!
//! 消融（真实树）：把 `deploy/k3s/observability/manifests/06-grafana-dashboard-
//! configmap.yaml` 第 343 行的 `{job="cogneva"}` 改成 `{job="cogneva-app"}`，本门
//! 红并点名该文件第 343 行、同时报出代码侧实际写的 `"cogneva"`；改回绿。代码侧
//! 缺 base labels 那一条由夹具 `a_client_that_writes_no_base_labels_is_not_green`
//! 守着。当前树上部署侧只有这一条 Loki 选择器，门照样要求它至少读到一条：下一条
//! 面板查询写错时，它不需要先有第二条才生效。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::routes::{repo_root, walk};

const CONSTANTS: &str = "crates/cog-observability/src/lib.rs";
const PUSH_CLIENT: &str = "crates/cog-observability/src/logs.rs";
const MANIFESTS: &str = "deploy/k3s/observability/manifests";

/// 代码侧写进 Loki base labels 的 (标签名 → 取值集合)。
///
/// 标签名取自常量声明（`LABEL_JOB` → `"job"`），取值取自 push 客户端里
/// `base_labels.insert(crate::LABEL_JOB.into(), "…".into())` 那一处。两边都从树上
/// 读，不在本文件里抄一份——抄一份就是第三处拼写。
fn written_labels(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut out = BTreeMap::new();
    let Ok(constants) = std::fs::read_to_string(root.join(CONSTANTS)) else {
        return out;
    };
    let Ok(client) = std::fs::read_to_string(root.join(PUSH_CLIENT)) else {
        return out;
    };
    for name in ["LABEL_JOB", "LABEL_SERVICE"] {
        let Some((_, word)) = const_value(&constants, name) else {
            continue;
        };
        let marker = format!(".insert(crate::{name}.into(), \"");
        let mut values = BTreeSet::new();
        let mut cursor = 0;
        while let Some(offset) = client[cursor..].find(&marker) {
            let at = cursor + offset + marker.len();
            let Some(end) = client[at..].find('"') else {
                break;
            };
            values.insert(client[at..at + end].to_string());
            cursor = at + end;
        }
        out.insert(word, values);
    }
    out
}

/// `pub const NAME: &str = "value";`
fn const_value(source: &str, name: &str) -> Option<(usize, String)> {
    let marker = format!("const {name}: &str = \"");
    let at = source.find(&marker)? + marker.len();
    let end = source[at..].find('"')?;
    Some((at, source[at..at + end].to_string()))
}

/// 部署侧每一处以 `{` 开头的查询里的 (标签名, 取值, 文件, 行号)。
fn deployed_selectors(root: &Path) -> Vec<(String, String, String, usize)> {
    let mut files = Vec::new();
    walk(&root.join(MANIFESTS), &["yaml", "yml"], &mut files);
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let where_ = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        for (line, query) in expressions(&text) {
            let trimmed = query.trim_start();
            // 只有以 `{` 开头的才是 LogQL 流选择器：前面挂着指标名的是 PromQL，
            // 那里的 `job` 由 Prometheus 的 scrape 配置给，和日志流是两个词汇。
            if !trimmed.starts_with('{') {
                continue;
            }
            for (label, value) in stream_matchers(trimmed) {
                out.push((label, value, where_.clone(), line));
            }
        }
    }
    out
}

/// 文件里每条 `expr` 的取值，连同它在文件里的行号。
///
/// 两种写法都读：Grafana 的 JSON 面板里是 `"expr": "{job=\"cogneva\"} …"`，
/// 规则清单里是 YAML 的 `expr: up{…}`。JSON 那一行的结尾是 `",`，去掉。
fn expressions(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for (marker, json) in [("\"expr\": \"", true), ("expr: ", false)] {
        let mut cursor = 0;
        while let Some(offset) = text[cursor..].find(marker) {
            let start = cursor + offset + marker.len();
            let line = text[..start].matches('\n').count() + 1;
            let rest = &text[start..];
            let end = rest.find('\n').unwrap_or(rest.len());
            let mut value = rest[..end].trim_end().to_string();
            if json {
                if let Some(stripped) = value.strip_suffix("\",") {
                    value = stripped.to_string();
                } else if let Some(stripped) = value.strip_suffix('"') {
                    value = stripped.to_string();
                }
                value = value.replace("\\\"", "\"");
            }
            out.push((line, value));
            cursor = start + end;
        }
    }
    out
}

/// 一个流选择器里的 `job=` / `service=` 取值。模板变量（`$job`）跳过：运行时的
/// 取值本门读不到，判它就是猜。
fn stream_matchers(query: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for marker in ["job=\"", "job=~\"", "service=\"", "service=~\""] {
        let label = marker.split('=').next().unwrap_or_default();
        let mut cursor = 0;
        while let Some(offset) = query[cursor..].find(marker) {
            let at = cursor + offset + marker.len();
            let Some(end) = query[at..].find('"') else {
                break;
            };
            let value = &query[at..at + end];
            if !value.starts_with('$') && !value.is_empty() {
                out.push((label.to_string(), value.to_string()));
            }
            cursor = at + end;
        }
    }
    out
}

fn complaints(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let written = written_labels(root);
    for required in ["job", "service"] {
        match written.get(required) {
            Some(values) if !values.is_empty() => {}
            _ => out.push(format!(
                "  代码侧读不到 `{required}` 这个流标签的取值（{PUSH_CLIENT} 里 \
                 `base_labels.insert(crate::LABEL_{} …)`）：读不出来按红处理",
                required.to_uppercase()
            )),
        }
    }

    let selectors = deployed_selectors(root);
    if selectors.is_empty() {
        out.push(format!(
            "  部署侧在 `{MANIFESTS}` 下一个以 `{{` 开头的查询都没读到：读不出来按红处理"
        ));
    }
    for (label, value, where_, line) in selectors {
        let known = written.get(&label);
        match known {
            Some(values) if values.contains(&value) => {}
            Some(values) => out.push(format!(
                "  `{where_}` 第 {line} 行的日志查询按 `{label}=\"{value}\"` 选流，\
                 而代码侧写的取值是 {}：面板不会报错，它只是永远选不中任何一条流，\
                 空的 trace 面板与「这段时间没有日志」是同一个读数",
                values
                    .iter()
                    .map(|v| format!("`\"{v}\"`"))
                    .collect::<Vec<_>>()
                    .join("、")
            )),
            None => out.push(format!(
                "  `{where_}` 第 {line} 行的日志查询按 `{label}=\"{value}\"` 选流，\
                 而代码侧根本没有写 `{label}` 这个流标签：这一侧有、那一侧没有，\
                 同样是永远选不中"
            )),
        }
    }
    out
}

#[test]
fn a_log_stream_selector_names_a_value_the_code_writes() {
    let found = complaints(&repo_root());
    assert!(
        found.is_empty(),
        "部署里的日志流选择器必须按代码写下的取值选流；这两处各自写一遍，编译器不比较，\
         改一处不会有任何报错。\n{}",
        found.join("\n")
    );
}

// ── 夹具：临时树里放一份常量、一个 push 客户端与一份清单，逐条验证红与绿 ──

fn fixture(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    complaints(dir.path())
}

const CONST_DECLS: &str = "pub const LABEL_JOB: &str = \"job\";\n\
pub const LABEL_SERVICE: &str = \"service\";\n";

const CLIENT: &str = "pub fn new() -> Self {\n    \
base_labels.insert(crate::LABEL_JOB.into(), \"cogneva\".into());\n    \
base_labels.insert(crate::LABEL_SERVICE.into(), \"cogneva\".into());\n}\n";

fn manifest(expr_line: &str) -> String {
    format!(
        "apiVersion: v1\nkind: ConfigMap\ndata:\n  dashboard.json: |\n    {{\n      \"panels\": [\n        {{\n          {expr_line}\n        }}\n      ]\n    }}\n"
    )
}

#[test]
fn the_fixture_is_green_when_both_sides_say_the_same_word() {
    let lod = manifest("\"expr\": \"{job=\\\"cogneva\\\"} |=\\\"$trace_id\\\"\",");
    assert_eq!(
        fixture(&[
            (CONSTANTS, CONST_DECLS),
            (PUSH_CLIENT, CLIENT),
            ("deploy/k3s/observability/manifests/dash.yaml", &lod),
        ]),
        Vec::<String>::new()
    );
}

#[test]
fn a_selector_no_stream_carries_is_reported() {
    let lod = manifest("\"expr\": \"{job=\\\"cogneva-app\\\"} |=\\\"$trace_id\\\"\",");
    let found = fixture(&[
        (CONSTANTS, CONST_DECLS),
        (PUSH_CLIENT, CLIENT),
        ("deploy/k3s/observability/manifests/dash.yaml", &lod),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("cogneva-app") && c.contains("dash.yaml")),
        "{found:?}"
    );
}

/// 前面挂着指标名的是 PromQL，`job` 由 scrape 配置给，和日志流是两个词汇：同一份
/// 清单里 `up{job="…"}` 不该被判红。
#[test]
fn a_prometheus_scrape_job_is_not_a_log_stream_label() {
    let rule = "apiVersion: v1\nkind: PrometheusRule\nspec:\n  groups:\n    - rules:\n        - expr: up{job=\"cogneva-host-readings\"} == 0\n";
    let lod = manifest("\"expr\": \"{job=\\\"cogneva\\\"}\",");
    let found = fixture(&[
        (CONSTANTS, CONST_DECLS),
        (PUSH_CLIENT, CLIENT),
        ("deploy/k3s/observability/manifests/rule.yaml", rule),
        ("deploy/k3s/observability/manifests/dash.yaml", &lod),
    ]);
    assert!(
        found.is_empty(),
        "scrape 的 job 不是日志流标签，不该判红：{found:?}"
    );
}

/// 读不到代码侧的取值时不能算绿：一个空集合会让每个选择器都「不在集合里」，也算
/// 不出「没读到」。
#[test]
fn a_client_that_writes_no_base_labels_is_not_green() {
    let lod = manifest("\"expr\": \"{job=\\\"cogneva\\\"}\",");
    let found = fixture(&[
        (CONSTANTS, CONST_DECLS),
        (PUSH_CLIENT, "pub fn new() -> Self {}\n"),
        ("deploy/k3s/observability/manifests/dash.yaml", &lod),
    ]);
    assert!(found.iter().any(|c| c.contains("读不到")), "{found:?}");
}

/// 一条 Loki 选择器都没有时也不能算绿：`deploy` 路径写错、清单改成别的格式，都会
/// 读成「没有选择器」，而「没有选择器」与「这道门没看见」在结果上分不开。
#[test]
fn a_deploy_tree_with_no_log_selector_is_not_green() {
    let found = fixture(&[
        (CONSTANTS, CONST_DECLS),
        (PUSH_CLIENT, CLIENT),
        (
            "deploy/k3s/observability/manifests/dash.yaml",
            "apiVersion: v1\nkind: ConfigMap\n",
        ),
    ]);
    assert!(found.iter().any(|c| c.contains("都没读到")), "{found:?}");
}
