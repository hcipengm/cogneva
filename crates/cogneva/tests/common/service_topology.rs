//! 部署面读一次：工作负载、容器、Service 端口，以及「这个容器起的是哪份代码」。
//!
//! 两条判据问的是同一个问题的两半：探针问「kubelet 访问的路径，这个容器起的那份
//! 代码注册过吗」，抓取目标问「Prometheus 访问的端口与路径，被选中的工作负载提供
//! 吗」。分成两份读就会分叉——一边把 `ports[].name` 读成端口身份，另一边把
//! `targetPort` 读成身份，两边对「这个 Service 指向谁」给出不同答案，而差异恰好
//! 出现在两处判据都读不到的地方。所以只留一份。
//!
//! 「这个容器起的是哪份代码」也不在这里另立一份词表：子命令从 `cli.rs` 的
//! `SUBCOMMANDS` 读（argv 词 → 枚举变体），变体 → 模块从 `main.rs` 的匹配臂读，
//! 模块路径按 Rust 的 `crate::mod::fn` 形状落到文件。

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use super::routes::{route_entries_in, walk};
use super::rust_production::production_text;

/// 唯一声明本项目镜像名的地方。
pub const CHART_VALUES: &str = "deploy/helm/cogneva/values.yaml";
/// 子命令的 argv 词表（词 → 枚举变体）。
pub const CLI: &str = "crates/cogneva/src/cli.rs";
/// 枚举变体 → 模块（子命令实际起的那份代码）。
pub const MAIN: &str = "crates/cogneva/src/main.rs";
/// 静态清单：apply 路径上真正被投递的那份字节。
pub const APPLY_TREE: &str = "deploy/k3s";
/// 预渲染产物：`helm install` 投递的那份字节。每个 profile 一个目录。
pub const RENDERED: &str = "deploy/rendered";

#[derive(Debug, Clone, Default)]
pub struct ContainerSpec {
    pub manifest: String,
    pub workload: String,
    pub name: String,
    pub image: String,
    /// `args` 里的第一个位置参数（子命令词）；`None` 表示走镜像入口、无参数。
    pub subcommand: Option<String>,
    /// `ports[]` 的 (名字, 容器端口)。
    pub ports: Vec<(String, u64)>,
    /// `(探针名, httpGet 路径)`。
    pub probes: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct Workload {
    pub manifest: String,
    pub kind: String,
    pub name: String,
    pub namespace: String,
    /// Pod 模板的标签：ServiceMonitor / PodMonitor 的选择器按它选。
    pub labels: BTreeMap<String, String>,
    pub containers: Vec<ContainerSpec>,
}

#[derive(Debug, Clone)]
pub struct ServicePort {
    pub name: String,
    /// `targetPort`：具名时是工作负载里的端口名，写成数字时是端口号。
    pub target: String,
}

#[derive(Debug, Clone)]
pub struct Service {
    pub manifest: String,
    pub name: String,
    pub namespace: String,
    /// Service 自身的标签：ServiceMonitor 的选择器按它选。
    pub labels: BTreeMap<String, String>,
    pub selector: BTreeMap<String, String>,
    pub ports: Vec<ServicePort>,
}

/// 一个抓取端点（ServiceMonitor 的 `endpoints[]`、PodMonitor 的 `podMetricsEndpoints[]`）。
#[derive(Debug, Clone, Default)]
pub struct MonitorEndpoint {
    /// 端口名。ServiceMonitor 里对 Service 的 `ports[].name`，PodMonitor 里对容器
    /// 端口名；写成数字时按端口号比。
    pub port: Option<String>,
    /// 缺省时 Prometheus Operator 用 `/metrics`。
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct Monitor {
    pub manifest: String,
    /// `ServiceMonitor` 或 `PodMonitor`。
    pub kind: String,
    pub name: String,
    pub namespace: String,
    /// `namespaceSelector.matchNames`；缺省时是监视器自己所在的那个命名空间。
    pub namespaces: Vec<String>,
    /// `namespaceSelector.any: true`：不按命名空间收窄。
    pub any_namespace: bool,
    pub selector: BTreeMap<String, String>,
    pub endpoints: Vec<MonitorEndpoint>,
}

fn text_at<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(|v| v.as_str())
}

/// 一条标量路径（`metadata.name`、`metadata.namespace`）。
fn scalar_at(value: &serde_yaml::Value, path: &[&str]) -> String {
    let mut cursor = value;
    for key in path {
        let Some(next) = cursor.get(*key) else {
            return String::new();
        };
        cursor = next;
    }
    cursor.as_str().unwrap_or_default().to_string()
}

fn labels_at(value: &serde_yaml::Value, path: &[&str]) -> BTreeMap<String, String> {
    let mut cursor = value;
    for key in path {
        let Some(next) = cursor.get(*key) else {
            return BTreeMap::new();
        };
        cursor = next;
    }
    let Some(map) = cursor.as_mapping() else {
        return BTreeMap::new();
    };
    map.iter()
        .filter_map(|(key, value)| {
            Some((
                key.as_str()?.to_string(),
                value.as_str().unwrap_or_default().to_string(),
            ))
        })
        .collect()
}

/// 会被投递出去的几棵树：静态清单加每个预渲染 profile。
///
/// 三条投递路径各有自己的字节（元启动 apply `deploy/k3s`，`helm install` 用
/// `deploy/rendered/<profile>`），而 chart 模板改完要重新渲染才会出现在后者里。
/// 只读其中一棵，另一条路径上的同一个拼写就没人比。profile 由目录枚举，新加一个
/// profile 不需要改任何判据。
pub fn manifest_trees(root: &Path) -> Vec<PathBuf> {
    let mut trees = vec![root.join(APPLY_TREE)];
    let mut profiles: Vec<PathBuf> = std::fs::read_dir(root.join(RENDERED))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect()
        })
        .unwrap_or_default();
    profiles.sort();
    trees.extend(profiles);
    trees
}

/// 这些树里的每一份 YAML 文档，连同它所在的相对路径。
fn documents(root: &Path) -> Vec<(String, serde_yaml::Value)> {
    let mut files = Vec::new();
    for tree in manifest_trees(root) {
        walk(&tree, &["yaml", "yml"], &mut files);
    }
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let manifest = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        for document in serde_yaml::Deserializer::from_str(&text) {
            if let Ok(value) = serde_yaml::Value::deserialize(document) {
                out.push((manifest.clone(), value));
            }
        }
    }
    out
}

/// 每个工作负载（Deployment / StatefulSet / DaemonSet）连同它的容器。
pub fn workloads(root: &Path) -> Vec<Workload> {
    let mut out = Vec::new();
    for (manifest, value) in documents(root) {
        let kind = text_at(&value, "kind").unwrap_or_default();
        if !matches!(kind, "Deployment" | "StatefulSet" | "DaemonSet") {
            continue;
        }
        let name = scalar_at(&value, &["metadata", "name"]);
        let namespace = scalar_at(&value, &["metadata", "namespace"]);
        let Some(spec) = value
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
        else {
            continue;
        };
        let labels = labels_at(&value, &["spec", "template", "metadata", "labels"]);
        let Some(list) = spec.get("containers").and_then(|c| c.as_sequence()) else {
            continue;
        };
        let mut containers = Vec::new();
        for container in list {
            let name = text_at(container, "name").unwrap_or_default().to_string();
            let image = text_at(container, "image").unwrap_or_default().to_string();
            let subcommand = container
                .get("args")
                .and_then(|a| a.as_sequence())
                .and_then(|args| args.first())
                .and_then(|a| a.as_str())
                .filter(|arg| !arg.starts_with('-'))
                .map(str::to_string);
            let mut ports = Vec::new();
            for port in container
                .get("ports")
                .and_then(|p| p.as_sequence())
                .into_iter()
                .flatten()
            {
                let Some(number) = port.get("containerPort").and_then(|n| n.as_u64()) else {
                    continue;
                };
                ports.push((
                    text_at(port, "name").unwrap_or_default().to_string(),
                    number,
                ));
            }
            let mut probes = Vec::new();
            for probe in ["livenessProbe", "readinessProbe", "startupProbe"] {
                let Some(path) = container
                    .get(probe)
                    .and_then(|p| p.get("httpGet"))
                    .and_then(|h| h.get("path"))
                    .and_then(|p| p.as_str())
                else {
                    continue;
                };
                probes.push((probe.to_string(), path.to_string()));
            }
            containers.push(ContainerSpec {
                manifest: manifest.clone(),
                workload: name.clone(),
                name,
                image,
                subcommand,
                ports,
                probes,
            });
        }
        out.push(Workload {
            manifest,
            kind: kind.to_string(),
            name,
            namespace,
            labels,
            containers,
        });
    }
    out
}

/// 这些树里的每个 Service，连同它的选择器与端口。
pub fn services(root: &Path) -> Vec<Service> {
    let mut out = Vec::new();
    for (manifest, value) in documents(root) {
        if text_at(&value, "kind") != Some("Service") {
            continue;
        }
        let selector = labels_at(&value, &["spec", "selector"]);
        let ports = value
            .get("spec")
            .and_then(|s| s.get("ports"))
            .and_then(|p| p.as_sequence())
            .into_iter()
            .flatten()
            .filter_map(|port| {
                Some(ServicePort {
                    name: text_at(port, "name")?.to_string(),
                    target: match port.get("targetPort") {
                        Some(value) if value.as_str().is_some() => value.as_str()?.to_string(),
                        Some(value) if value.as_u64().is_some() => value.as_u64()?.to_string(),
                        _ => value_as_u64_text(port.get("port")?)?,
                    },
                })
            })
            .collect();
        out.push(Service {
            manifest,
            name: scalar_at(&value, &["metadata", "name"]),
            namespace: scalar_at(&value, &["metadata", "namespace"]),
            labels: labels_at(&value, &["metadata", "labels"]),
            selector,
            ports,
        });
    }
    out
}

fn value_as_u64_text(value: &serde_yaml::Value) -> Option<String> {
    value.as_u64().map(|n| n.to_string())
}

/// 这些树里的每个抓取目标（ServiceMonitor / PodMonitor）。
pub fn monitors(root: &Path) -> Vec<Monitor> {
    let mut out = Vec::new();
    for (manifest, value) in documents(root) {
        let kind = text_at(&value, "kind").unwrap_or_default();
        if !matches!(kind, "ServiceMonitor" | "PodMonitor") {
            continue;
        }
        let Some(spec) = value.get("spec") else {
            continue;
        };
        let list_key = if kind == "ServiceMonitor" {
            "endpoints"
        } else {
            "podMetricsEndpoints"
        };
        let mut endpoints = Vec::new();
        for endpoint in spec
            .get(list_key)
            .and_then(|e| e.as_sequence())
            .into_iter()
            .flatten()
        {
            endpoints.push(MonitorEndpoint {
                port: endpoint.get("port").and_then(scalar_text),
                // 没写 `path` 时 Prometheus Operator 用 `/metrics`：把缺省读成
                // `/metrics`，否则「没写」会被读成「抓根路径」。
                path: endpoint
                    .get("path")
                    .and_then(|p| p.as_str())
                    .unwrap_or("/metrics")
                    .to_string(),
            });
        }
        let namespace_selector = spec.get("namespaceSelector");
        let namespaces = namespace_selector
            .and_then(|s| s.get("matchNames"))
            .and_then(|n| n.as_sequence())
            .map(|list| {
                list.iter()
                    .filter_map(|n| n.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        out.push(Monitor {
            manifest,
            kind: kind.to_string(),
            name: scalar_at(&value, &["metadata", "name"]),
            namespace: scalar_at(&value, &["metadata", "namespace"]),
            namespaces,
            any_namespace: namespace_selector
                .and_then(|s| s.get("any"))
                .and_then(|a| a.as_bool())
                .unwrap_or(false),
            selector: labels_at(spec, &["selector", "matchLabels"]),
            endpoints,
        });
    }
    out
}

/// 字符串或数字读成文本：端口名与端口号在清单里是两种写法。
fn scalar_text(value: &serde_yaml::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    value.as_u64().map(|n| n.to_string())
}

/// 这棵树自己声明了的主命名空间：工作负载或 Service 落在哪个命名空间，这个
/// 命名空间就算本树在管。第三方命名空间（`kube-system`、`ingress-nginx`）不
/// 会出现在这份清单里——抓它们的监视器由别人的栈负责。
pub fn declared_namespaces(root: &Path) -> BTreeSet<String> {
    workloads(root)
        .into_iter()
        .map(|workload| workload.namespace)
        .chain(services(root).into_iter().map(|service| service.namespace))
        .filter(|namespace| !namespace.is_empty())
        .collect()
}

/// 选择器是否命中一组标签：选择器里每一格都要取到同一个值。空格选择器按
/// Kubernetes 的语义命中全部，所以它是「命中」而不是「不命中」。
pub fn selector_matches(
    labels: &BTreeMap<String, String>,
    selector: &BTreeMap<String, String>,
) -> bool {
    selector
        .iter()
        .all(|(key, value)| labels.get(key) == Some(value))
}

/// 一个 NetworkPolicy 的 subject 选择器：这份策略管哪些 Pod。
pub struct NetworkPolicySubject {
    pub manifest: String,
    pub name: String,
    pub namespace: String,
    /// `matchLabels` 读成全等对；`matchExpressions` 这里不解释，只用
    /// `uses_expressions` 说明「这个选择器读不全」。
    pub selector: BTreeMap<String, String>,
    pub uses_expressions: bool,
}

/// 这些树里每份 NetworkPolicy 的 subject。只读 subject：对端选择器与端口由
/// `deploy/scripts/check-control-plane-reachability.sh` 判（它读的是同一个面，
/// 而且要在两棵交付树上各判一遍）。
pub fn network_policies(root: &Path) -> Vec<NetworkPolicySubject> {
    let mut out = Vec::new();
    for (manifest, value) in documents(root) {
        if text_at(&value, "kind") != Some("NetworkPolicy") {
            continue;
        }
        let Some(selector) = value
            .get("spec")
            .and_then(|s| s.get("podSelector"))
            .and_then(|s| s.as_mapping())
        else {
            continue;
        };
        let selector: BTreeMap<String, String> = selector
            .get("matchLabels")
            .and_then(|m| m.as_mapping())
            .map(|labels| {
                labels
                    .iter()
                    .filter_map(|(key, value)| {
                        Some((
                            key.as_str()?.to_string(),
                            value.as_str().unwrap_or_default().to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let uses_expressions = value
            .get("spec")
            .and_then(|s| s.get("podSelector"))
            .and_then(|s| s.get("matchExpressions"))
            .and_then(|e| e.as_sequence())
            .is_some_and(|list| !list.is_empty());
        out.push(NetworkPolicySubject {
            manifest,
            name: scalar_at(&value, &["metadata", "name"]),
            namespace: scalar_at(&value, &["metadata", "namespace"]),
            selector,
            uses_expressions,
        });
    }
    out
}

/// 一份清单文档里的 Pod 模板：工作负载、Job 与 CronJob 都算。策略与选择器选中的
/// 是「一组 Pod」，不是「一组 Deployment」——只看 Deployment 会让定时任务的 Pod
/// 在读选择器的那一侧不存在。
pub fn pod_template_of(value: &serde_yaml::Value) -> Option<&serde_yaml::Value> {
    let spec = value.get("spec")?;
    match text_at(value, "kind").unwrap_or_default() {
        "Deployment" | "StatefulSet" | "DaemonSet" | "Job" => spec.get("template"),
        "CronJob" => spec.get("jobTemplate")?.get("spec")?.get("template"),
        _ => None,
    }
}

/// 这棵树里每个 Pod 模板带的标签，连同它落在哪个命名空间。选择器命不命中，只能对着
/// 这一份名单说——而选择器只在同一个命名空间里比，所以命名空间要跟着一起读出来。
pub fn pod_labels(root: &Path) -> Vec<(String, BTreeMap<String, String>)> {
    let mut out = Vec::new();
    for (_, value) in documents(root) {
        let Some(template) = pod_template_of(&value) else {
            continue;
        };
        let labels = template
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_mapping())
            .map(|labels| {
                labels
                    .iter()
                    .filter_map(|(key, value)| {
                        Some((
                            key.as_str()?.to_string(),
                            value.as_str().unwrap_or_default().to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.push((scalar_at(&value, &["metadata", "namespace"]), labels));
    }
    out
}

/// 本项目镜像的仓库名（不含 tag），取自 chart values。
pub fn our_image(root: &Path) -> Option<String> {
    let text = std::fs::read_to_string(root.join(CHART_VALUES)).ok()?;
    let mut in_image = false;
    for line in text.lines() {
        if !line.starts_with(' ') && !line.trim().is_empty() {
            in_image = line.trim_end() == "image:";
            continue;
        }
        if !in_image {
            continue;
        }
        if let Some(rest) = line.trim_start().strip_prefix("repository:") {
            let value = rest.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// `cli.rs` 里 `SUBCOMMANDS` 声明的 argv 词 → 枚举变体。
pub fn subcommands(root: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(root.join(CLI)) else {
        return out;
    };
    let production = production_text(&text);
    let Some(start) = production.find("SUBCOMMANDS") else {
        return out;
    };
    let Some(end) = production[start..].find("];") else {
        return out;
    };
    let table = &production[start..start + end];
    let mut cursor = 0;
    while let Some(offset) = table[cursor..].find("(\"") {
        let word_at = cursor + offset + 2;
        let Some(word_end) = table[word_at..].find('"') else {
            break;
        };
        let word = &table[word_at..word_at + word_end];
        let after = table[word_at + word_end + 1..].trim_start();
        if let Some(rest) = after.strip_prefix(',') {
            if let Some(variant) = rest.trim_start().strip_prefix("Command::") {
                let name: String = variant
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    out.insert(word.to_string(), name);
                }
            }
        }
        cursor = word_at + word_end + 1;
    }
    out
}

/// `main.rs` 里 `Command::X => …` 每个匹配臂起的那几份代码（按 Rust 路径落到文件）。
pub fn dispatch(root: &Path) -> BTreeMap<String, Vec<PathBuf>> {
    let mut out: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(root.join(MAIN)) else {
        return out;
    };
    let production = production_text(&text);
    // 每个臂的开头：一行里只有空白之后跟 `Command::<Variant>` 再跟 `=>`。
    let mut arms: Vec<(usize, String, usize)> = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = production[cursor..].find("Command::") {
        let at = cursor + offset;
        cursor = at + "Command::".len();
        let line_start = production[..at].rfind('\n').map(|p| p + 1).unwrap_or(0);
        if !production[line_start..at].trim().is_empty() {
            continue;
        }
        let rest = &production[at + "Command::".len()..];
        let variant: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if variant.is_empty() {
            continue;
        }
        let after = &rest[variant.len()..];
        let trimmed = after.trim_start();
        let Some(body) = trimmed.strip_prefix("=>") else {
            continue;
        };
        let body_start = production.len() - body.len();
        arms.push((at, variant, body_start));
    }
    for (index, (_, variant, body_start)) in arms.iter().enumerate() {
        let body_end = arms
            .get(index + 1)
            .map(|(at, _, _)| *at)
            .unwrap_or(production.len());
        let body = &production[*body_start..body_end];
        let mut files = Vec::new();
        for path in rust_paths(body) {
            if let Some(file) = resolve(root, &path) {
                if !files.contains(&file) {
                    files.push(file);
                }
            }
        }
        out.insert(variant.clone(), files);
    }
    out
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// 一段代码里的 `crate::mod::fn` 形状（至少一段 `::`）。
fn rust_paths(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if !is_ident_start(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_ident_byte(bytes[i]) {
            i += 1;
        }
        let mut end = i;
        while text[end..].starts_with("::") {
            let segment = &text[end + 2..];
            let length = segment.bytes().take_while(|b| is_ident_byte(*b)).count();
            if length == 0 {
                break;
            }
            end += 2 + length;
            i = end;
        }
        let path = &text[start..end];
        if path.contains("::") {
            out.push(path.to_string());
        }
    }
    out
}

/// `cog_gateway::security_gateway::run` → `crates/cog-gateway/src/security_gateway.rs`。
///
/// 末尾那段是函数名，从后往前取能落到文件的最长模块前缀；`crate::f` 这种不带模块
/// 的落到 `src/lib.rs`。
fn resolve(root: &Path, path: &str) -> Option<PathBuf> {
    let mut parts = path.split("::");
    let krate = parts.next()?.replace('_', "-");
    let rest: Vec<&str> = parts.collect();
    let base = root.join("crates").join(&krate).join("src");
    if !base.is_dir() {
        return None;
    }
    let modules = &rest[..rest.len().saturating_sub(1)];
    if modules.is_empty() {
        let lib = base.join("lib.rs");
        return lib.is_file().then_some(lib);
    }
    for take in (1..=modules.len()).rev() {
        let fragment = modules[..take].join("/");
        let file = base.join(format!("{fragment}.rs"));
        if file.is_file() {
            return Some(file);
        }
        let module = base.join(&fragment).join("mod.rs");
        if module.is_file() {
            return Some(module);
        }
    }
    None
}

/// 一组源文件注册的路径（只读生产字节）。
pub fn routes_of(root: &Path, files: &[PathBuf]) -> BTreeSet<String> {
    route_entries_in(root, files, true)
        .into_iter()
        .map(|route| route.shape)
        .collect()
}

/// 整棵树注册的路径。主应用的路由由插件在运行时装配，静态读不到「谁挂了哪条」，
/// 所以对那份代码只问「这棵树里有没有」。
pub fn every_route(root: &Path) -> BTreeSet<String> {
    let mut files = Vec::new();
    walk(&root.join("crates"), &["rs"], &mut files);
    files.retain(|path| path.to_string_lossy().contains("/src/"));
    files.sort();
    routes_of(root, &files)
}

/// 一个容器起的那份代码注册的路径。
///
/// 不带子命令的那个容器是主应用，返回 [`every_route`]——它的路由由插件在运行时
/// 装配，静态读不到「哪个插件挂了哪条路由」，这一档比别的宽，宽在哪儿由调用方在
/// 自己的判据里写明。
pub fn container_routes(
    root: &Path,
    subcommand: Option<&str>,
    every: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    let Some(word) = subcommand else {
        return Ok(every.clone());
    };
    let subcommands = subcommands(root);
    let dispatch = dispatch(root);
    let Some(variant) = subcommands.get(word) else {
        return Err(format!(
            "以 `{word}` 为子命令启动，而 `{CLI}` 的 `SUBCOMMANDS` 里没有这个词：进程会\
             打印 `unrecognized argument` 并以退出码 2 结束，容器起不来"
        ));
    };
    let Some(files) = dispatch.get(variant) else {
        return Err(format!(
            "起的是子命令 `{word}`（`Command::{variant}`），而 `{MAIN}` 里没有 \
             `Command::{variant} =>` 这个匹配臂：读不到它起的是哪份代码"
        ));
    };
    if files.is_empty() {
        return Err(format!(
            "起的是子命令 `{word}`（`Command::{variant}`），而 `{MAIN}` 那个匹配臂里的 \
             Rust 路径一条都落不到文件：读不出它起的是哪份代码"
        ));
    }
    Ok(routes_of(root, files))
}
