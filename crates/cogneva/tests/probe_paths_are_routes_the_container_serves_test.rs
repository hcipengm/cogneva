//! principle: feedback_no_hardcoding_config_surface
//!
//! 探针路径在两侧各写一遍：进程用 `.route("/health/live", …)` 注册它，Deployment
//! 的 `livenessProbe.httpGet.path` 按字面量去访问它。没有任何东西比较这两处。
//!
//! 漂移的后果与「Pod 还没起来」是同一个读数：探针打到一个没人服务的路径，kubelet
//! 收到 404，容器一直不 ready，滚动端把这一轮判成启动失败。而两侧各自都「对」——
//! 代码编译通过，清单也 apply 成功。`deploy/scripts` 下那份控制面可达性门的头注里
//! 明写了这件事在它判据之外：「探针的路径对不对」不在它判的六族里，它判的是对象与
//! 对象之间的关系，不是路径与应用之间的关系。
//!
//! 判什么：
//!
//! * 每个跑本项目镜像的容器，它的 `httpGet` 探针路径必须是**这个容器起的那份代码**
//!   注册过的路由；
//! * 读的清单是**会被投递出去的每一棵树**：`deploy/k3s`（元启动 apply 的字节）与
//!   `deploy/rendered` 下的每个 profile（`helm install` 的字节）。profile 目录由
//!   目录枚举出来而不是在这里点名，新加一个 profile 不需要改这道门；
//! * 「这个容器起的是哪份代码」不在本文件里另写一份：子命令从 `crates/cogneva/src/
//!   cli.rs` 的 `SUBCOMMANDS` 读（argv 词 → 枚举变体），变体 → 模块从 `main.rs` 的
//!   匹配臂读，模块路径按 Rust 的 `crate::mod::fn` 形状落到文件；
//! * 「哪个镜像是本项目的」取 chart values 里的 `image.repository`——静态清单与
//!   chart 是 parity 锁着的一份拓扑，镜像名只有这一个出处。
//!
//! 判不了什么：
//!
//! * 第三方镜像容器的探针（crawl4ai 的 `/health`、registry 的 `/v2/`、nats 的
//!   `/healthz`）：它们的路由不在这棵树里，读不到就不判；
//! * 主应用（不带子命令、走镜像入口的那一个）注册了什么：它的路由由插件在运行时
//!   装配，静态读不到「哪个插件挂了哪条路由」，所以对这份代码只要求「这棵树里
//!   某处注册过它」——比旁的分支宽一档，宽在哪儿写在这里；
//! * 探针的端口对不对（那是 reachability 门第 4 族的事）、探针的时机与阈值；
//! * chart 模板本身（带 `{{ }}` 的不是合法 YAML）：模板里写错要到重新渲染之后才在
//!   这道门上出现，而「渲染产物是不是最新」是新鲜度门的事；parity 只比对
//!   env/卷/挂载/端口/SA，不比探针，所以两棵树之间的探针分叉同样由这道门接住。
//!
//! 普查要有下限：读不到 chart 的 `image.repository`、一个跑本项目镜像且带
//! `httpGet` 探针的容器都没找到、或这棵树里一条路由都没读到时，一律按红处理——
//! 「没读到」与「没有漂移」在结果上分不开。
//!
//! 消融（真实树）：把 `deploy/k3s/claim-store-daemonset.yaml` 的 readinessProbe
//! 路径 `/health/ready` 改成 `/health/readyz`，本门红，点名该文件、claim-store
//! 容器与 `claim_store.rs` 实际注册过的路径；改回绿。子命令解析、「只认生产字节」
//! 与预渲染 profile 这三个分支由夹具守着
//! （`a_subcommand_the_cli_does_not_declare_is_reported`、
//! `a_route_registered_only_in_a_test_block_is_not_served`、
//! `a_rendered_profile_with_a_stale_probe_path_is_reported`）——真实树上四棵树
//! 此刻彼此一致，把它们轮流改红只证明同一件事。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use common::routes::{repo_root, route_entries_in, walk};
use common::rust_production::production_text;

/// 唯一声明本项目镜像名的地方。
const CHART_VALUES: &str = "deploy/helm/cogneva/values.yaml";
/// 子命令的 argv 词表（词 → 枚举变体）。
const CLI: &str = "crates/cogneva/src/cli.rs";
/// 枚举变体 → 模块（子命令实际起的那份代码）。
const MAIN: &str = "crates/cogneva/src/main.rs";
/// 静态清单：apply 路径上真正被投递的那份字节。
const APPLY_TREE: &str = "deploy/k3s";
/// 预渲染产物：`helm install` 投递的那份字节。每个 profile 一个目录，由目录本身
/// 枚举，不在这里钉名字——新加一个 profile 不该需要改这道门。
const RENDERED: &str = "deploy/rendered";

#[derive(Debug, Clone)]
struct Container {
    manifest: String,
    workload: String,
    name: String,
    image: String,
    /// `args` 里的第一个位置参数（子命令词）；`None` 表示走镜像入口、无参数。
    subcommand: Option<String>,
    /// `(探针名, httpGet 路径)`。
    probes: Vec<(String, String)>,
}

/// 本项目镜像的仓库名（不含 tag），取自 chart values。
fn our_image(root: &Path) -> Option<String> {
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
fn subcommands(root: &Path) -> BTreeMap<String, String> {
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
fn dispatch(root: &Path) -> BTreeMap<String, Vec<PathBuf>> {
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
fn routes_of(root: &Path, files: &[PathBuf]) -> BTreeSet<String> {
    route_entries_in(root, files, true)
        .into_iter()
        .map(|route| route.shape)
        .collect()
}

/// 整棵树注册的路径。主应用的路由由插件在运行时装配，静态读不到「谁挂了哪条」，
/// 所以对那份代码只问「这棵树里有没有」。
fn every_route(root: &Path) -> BTreeSet<String> {
    let mut files = Vec::new();
    walk(&root.join("crates"), &["rs"], &mut files);
    files.retain(|path| path.to_string_lossy().contains("/src/"));
    files.sort();
    routes_of(root, &files)
}

/// 会被投递出去的几棵树：静态清单加每个预渲染 profile。
///
/// 三条投递路径各有自己的字节（元启动 apply `deploy/k3s`，`helm install` 用
/// `deploy/rendered/<profile>`），而 chart 模板改完要重新渲染才会出现在后者里。
/// 只读其中一棵，另一条路径上的同一个拼写就没人比。
fn manifest_trees(root: &Path) -> Vec<PathBuf> {
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

/// 清单里每一个容器，连同它的探针。
fn containers(root: &Path) -> Vec<Container> {
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
            let Ok(value) = serde_yaml::Value::deserialize(document) else {
                continue;
            };
            let kind = value.get("kind").and_then(|k| k.as_str()).unwrap_or("");
            if !matches!(kind, "Deployment" | "StatefulSet" | "DaemonSet") {
                continue;
            }
            let workload = value
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let Some(spec) = value
                .get("spec")
                .and_then(|s| s.get("template"))
                .and_then(|t| t.get("spec"))
            else {
                continue;
            };
            let Some(list) = spec.get("containers").and_then(|c| c.as_sequence()) else {
                continue;
            };
            for container in list {
                let name = container
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                let image = container
                    .get("image")
                    .and_then(|i| i.as_str())
                    .unwrap_or("")
                    .to_string();
                let subcommand = container
                    .get("args")
                    .and_then(|a| a.as_sequence())
                    .and_then(|args| args.first())
                    .and_then(|a| a.as_str())
                    .filter(|arg| !arg.starts_with('-'))
                    .map(str::to_string);
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
                out.push(Container {
                    manifest: manifest.clone(),
                    workload: workload.clone(),
                    name,
                    image,
                    subcommand,
                    probes,
                });
            }
        }
    }
    out
}

fn complaints(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Some(image) = our_image(root) else {
        out.push(format!(
            "  读不到 `{CHART_VALUES}` 里 `image.repository` 的取值：读不出来按红处理"
        ));
        return out;
    };
    let subcommands = subcommands(root);
    let dispatch = dispatch(root);
    let every = every_route(root);
    if every.is_empty() {
        out.push(
            "  这棵树的 `crates/*/src` 里一条 `.route(\"…\")` 都没读到：读不出来按红处理"
                .to_string(),
        );
    }

    let ours: Vec<Container> = containers(root)
        .into_iter()
        .filter(|container| container.image.starts_with(&image))
        .filter(|container| !container.probes.is_empty())
        .collect();
    if ours.is_empty() {
        out.push(format!(
            "  `{APPLY_TREE}` 与 `{RENDERED}` 下每个 profile 里，一个跑本项目镜像\
             （`{image}`）且带 `httpGet` 探针的容器都没读到：要么清单搬了家，要么探针\
             换了形状，两种都按红处理"
        ));
        return out;
    }

    for container in ours {
        let Container {
            manifest,
            workload,
            name,
            subcommand,
            probes,
            ..
        } = &container;
        let Some(word) = subcommand else {
            // 主应用：走镜像入口、无参数，路由由插件在运行时装配。
            for (probe, path) in probes {
                if !every.contains(path) {
                    out.push(format!(
                        "  `{manifest}` 的 `{workload}` 里容器 `{name}` 的 `{probe}` 访问 \
                         `{path}`，而这棵树的 `crates/*/src` 里没有任何 `.route(\"{path}\")`：\
                         探针会一直收到 404，容器永远不 ready，而两侧各自都是「对」的"
                    ));
                }
            }
            continue;
        };
        let Some(variant) = subcommands.get(word) else {
            out.push(format!(
                "  `{manifest}` 的 `{workload}` 里容器 `{name}` 以 `{word}` 为子命令启动，\
                 而 `{CLI}` 的 `SUBCOMMANDS` 里没有这个词：进程会打印 `unrecognized \
                 argument` 并以退出码 2 结束，容器起不来"
            ));
            continue;
        };
        let Some(files) = dispatch.get(variant) else {
            out.push(format!(
                "  `{manifest}` 的 `{workload}` 里容器 `{name}` 起的是子命令 `{word}`\
                 （`Command::{variant}`），而 `{MAIN}` 里没有 `Command::{variant} =>` 这个\
                 匹配臂：读不到它起的是哪份代码"
            ));
            continue;
        };
        if files.is_empty() {
            out.push(format!(
                "  `{manifest}` 的 `{workload}` 里容器 `{name}` 起的是子命令 `{word}`\
                 （`Command::{variant}`），而 `{MAIN}` 那个匹配臂里的 Rust 路径一条都\
                 落不到文件：读不出它起的是哪份代码"
            ));
            continue;
        }
        let routes = routes_of(root, files);
        for (probe, path) in probes {
            if routes.contains(path) {
                continue;
            }
            out.push(format!(
                "  `{manifest}` 的 `{workload}` 里容器 `{name}`（子命令 `{word}`，代码在 {}）\
                 的 `{probe}` 访问 `{path}`，而那份代码注册的路径是 {}：探针会一直收到 \
                 404，容器永远不 ready，而两侧各自都是「对」的——代码编译通过，清单也 \
                 apply 成功",
                files
                    .iter()
                    .map(|file| format!(
                        "`{}`",
                        file.strip_prefix(root).unwrap_or(file).to_string_lossy()
                    ))
                    .collect::<Vec<_>>()
                    .join("、"),
                if routes.is_empty() {
                    "（一条都没有）".to_string()
                } else {
                    routes
                        .iter()
                        .map(|route| format!("`{route}`"))
                        .collect::<Vec<_>>()
                        .join("、")
                }
            ));
        }
    }
    out
}

#[test]
fn a_probe_path_is_a_route_the_container_serves() {
    let found = complaints(&repo_root());
    assert!(
        found.is_empty(),
        "静态清单里每个探针路径都必须是那个容器起的代码注册过的路由；这两处各自写一遍，\
         编译器不比较。\n{}",
        found.join("\n")
    );
}

// ── 夹具：临时树里放一份 chart values、一张子命令表、一份分发表与一份清单 ──

fn fixture(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    complaints(dir.path())
}

const VALUES: &str = "image:\n  repository: registry.example/cogneva\n  tag: local\n";

const CLI_TABLE: &str = "const SUBCOMMANDS: [(&str, Command); 1] = [\n    \
(\"claim-store\", Command::ClaimStore),\n];\n";

const MAIN_ARMS: &str =
    "    match command {\n        Command::ClaimStore => cogneva::claim_store::run_from_env().await,\n    }\n";

const CLAIM_STORE: &str = "pub fn run_from_env() {\n    let app = Router::new()\n        \
.route(\"/health/live\", get(live))\n        .route(\"/health/ready\", get(ready));\n}\n";

fn workload(image: &str, args: &str, probes: &[(&str, &str)]) -> String {
    let mut body = format!(
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: claim-store\nspec:\n  \
         template:\n    spec:\n      containers:\n        - name: claim-store\n          \
         image: {image}\n"
    );
    if !args.is_empty() {
        body.push_str(&format!("          args: [\"{args}\"]\n"));
    }
    for (probe, path) in probes {
        body.push_str(&format!(
            "          {probe}:\n            httpGet:\n              path: {path}\n              \
             port: http\n"
        ));
    }
    body
}

#[test]
fn the_fixture_is_green_when_the_probe_asks_for_a_route_the_component_registers() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[
            ("livenessProbe", "/health/live"),
            ("readinessProbe", "/health/ready"),
        ],
    );
    assert_eq!(
        fixture(&[
            ("deploy/helm/cogneva/values.yaml", VALUES),
            ("crates/cogneva/src/cli.rs", CLI_TABLE),
            ("crates/cogneva/src/main.rs", MAIN_ARMS),
            ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
            ("deploy/k3s/claim-store.yaml", &manifest),
        ]),
        Vec::<String>::new()
    );
}

/// 判据落在**这个容器**起的那份代码上，不是「这棵树里有没有人服务它」：另一个
/// 组件的路由不该让这条探针变绿。
#[test]
fn a_probe_asking_for_another_components_route_is_reported() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/other")],
    );
    let elsewhere = "pub fn run() {\n    let app = Router::new().route(\"/health/other\", \
                     get(other));\n}\n";
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("crates/cogneva/src/other.rs", elsewhere),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("/health/other") && c.contains("claim-store")),
        "{found:?}"
    );
}

/// 预渲染 profile 是 `helm install` 投递的那份字节：它里面的探针路径同样要判。
/// 只读 `deploy/k3s` 时，改 chart 模板后重新渲染出来的那一份没人比。
#[test]
fn a_rendered_profile_with_a_stale_probe_path_is_reported() {
    let applied = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/ready")],
    );
    let rendered = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/readyz")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/claim-store-daemonset.yaml", &applied),
        ("deploy/rendered/k3s-single/claim-store.yaml", &rendered),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("/health/readyz") && c.contains("k3s-single")),
        "{found:?}"
    );
    assert!(
        !found.iter().any(|c| c.contains("claim-store-daemonset")),
        "apply 路径那一份是对的，不该被牵连：{found:?}"
    );
}

/// `#[cfg(test)]` 里注册的路由没有进程会起：读进来就是假绿。
#[test]
fn a_route_registered_only_in_a_test_block_is_not_served() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("readinessProbe", "/health/ready")],
    );
    let with_tests = "pub fn run_from_env() {\n    let app = Router::new()\n        \
                      .route(\"/health/live\", get(live));\n}\n\n#[cfg(test)]\nmod tests {\n    \
                      fn app() -> Router {\n        Router::new().route(\"/health/ready\", \
                      get(ready))\n    }\n}\n";
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", with_tests),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(
        found
            .iter()
            .any(|c| c.contains("/health/ready") && c.contains("claim-store")),
        "{found:?}"
    );
}

/// 第三方镜像的探针不判：它的路由不在这棵树里，按红处理只会逼出一条豁免表。
#[test]
fn a_container_running_another_image_is_not_judged() {
    let manifest = workload(
        "docker.io/library/nginx:1.27",
        "",
        &[("livenessProbe", "/health")],
    );
    let ours = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/nginx.yaml", &manifest),
        ("deploy/k3s/claim-store.yaml", &ours),
    ]);
    assert!(found.is_empty(), "{found:?}");
}

/// 一个本项目镜像的容器都没找到时不能算绿：清单搬了家、镜像名对不上，都会读成
/// 「没有容器」，而「没有容器」与「没有漂移」在结果上分不开。
#[test]
fn a_tree_with_no_container_running_the_project_image_is_not_green() {
    let manifest = workload(
        "docker.io/library/nginx:1.27",
        "",
        &[("livenessProbe", "/health")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/nginx.yaml", &manifest),
    ]);
    assert!(found.iter().any(|c| c.contains("都没读到")), "{found:?}");
}

/// 子命令词不在 `SUBCOMMANDS` 里：进程启动即 exit 2，容器起不来。
#[test]
fn a_subcommand_the_cli_does_not_declare_is_reported() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store-typo",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(found.iter().any(|c| c.contains("SUBCOMMANDS")), "{found:?}");
}

/// 读不到镜像名时按红：此时每个容器都会被判成「不是本项目的」，结果是空集。
#[test]
fn a_chart_that_names_no_image_is_not_green() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "claim-store",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/claim-store.yaml", &manifest),
    ]);
    assert!(
        found.iter().any(|c| c.contains("image.repository")),
        "{found:?}"
    );
}

/// 主应用没有子命令：判据放宽到「这棵树里某处注册过」，宽在明处。
#[test]
fn the_main_application_is_judged_against_every_route_in_the_tree() {
    let manifest = workload(
        "registry.example/cogneva:local",
        "",
        &[("livenessProbe", "/health/live")],
    );
    let found = fixture(&[
        ("deploy/helm/cogneva/values.yaml", VALUES),
        ("crates/cogneva/src/cli.rs", CLI_TABLE),
        ("crates/cogneva/src/main.rs", MAIN_ARMS),
        ("crates/cogneva/src/claim_store.rs", CLAIM_STORE),
        ("deploy/k3s/deployment.yaml", &manifest),
    ]);
    assert!(found.is_empty(), "{found:?}");
}
