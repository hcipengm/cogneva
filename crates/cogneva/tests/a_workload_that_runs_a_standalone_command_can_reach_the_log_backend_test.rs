//! Gate: a workload that runs one of the standalone subcommands can reach the
//! log backend, and the set of those subcommands is read off the tree rather
//! than kept here.
//!
//! The assembled application mirrors its tracing events into Loki from inside
//! the process. Every other mode — the node-side daemons, the one-shot Jobs,
//! the sandbox executor — installs its own subscriber, and it learns where to
//! push from `COGNEVA_LOKI_ENDPOINT`. A workload that runs one of them without
//! that variable is not broken in any way anything else notices: it starts,
//! does its work, and writes its lines to a stdout that the container runtime
//! deletes with the pod. What survives a failure is the exit code, and an exit
//! code cannot say which step the process stopped on.
//!
//! Nothing on either side of that wiring can see the other. The chart does not
//! know which argv tokens start a subscriber; the assembly's dispatch knows
//! nothing about manifests. So the two are joined here: the tokens come from
//! `standalone_log_service` in `main.rs`, and the workloads come from the
//! rendered trees that are actually delivered.
//!
//! Adding a subcommand to that function without giving its workload the
//! endpoint fails here. So does shipping a workload that runs one of the
//! already-wired tokens and forgetting the variable.

use crate::common;

use common::routes::repo_root;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// `("sandbox-executor", Command::SandboxExecutor)` rows from the argument
/// table: the argv spelling of every subcommand.
fn argv_tokens(root: &Path) -> BTreeMap<String, String> {
    let text = read(&root.join("crates/cogneva/src/cli.rs"));
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("(\"") else {
            continue;
        };
        let Some((token, tail)) = rest.split_once("\", Command::") else {
            continue;
        };
        let Some(variant) = tail.strip_suffix("),") else {
            continue;
        };
        out.insert(variant.trim().to_owned(), token.to_owned());
    }
    out
}

/// The variants `standalone_log_service` maps to a stream name, and the names
/// themselves.
///
/// Read by scanning for `(Command::` inside the table rather than by splitting
/// each line on `", "`: entries are allowed to wrap, and one of them does
/// (`Command::SandboxExecutor` puts its service name on the next line). A line
/// shaped parser quietly skipped that row — which is also the row a workload
/// shipped without the endpoint would have gone unnoticed on.
fn log_wired_variants(root: &Path) -> BTreeMap<String, String> {
    let text = read(&root.join("crates/cogneva/src/main.rs"));
    let start = text
        .find("const STANDALONE_LOG_SERVICES")
        .expect("STANDALONE_LOG_SERVICES is gone: this gate reads the wiring from it");
    let region = &text[start..];
    let region = &region[..region.find("\n];").unwrap_or(region.len())];
    // An entry starts either as a one-liner (`(Command::X, name),`) or as a bare
    // `(` with the variant on the next line — which is how the sandbox executor's
    // row is written, and the row a line-at-a-time parser silently loses.
    let entries = region
        .lines()
        .filter(|line| {
            let line = line.trim();
            line == "(" || line.starts_with("(Command::")
        })
        .count();
    let mut out = BTreeMap::new();
    let mut rest = region;
    while let Some(at) = rest.find("Command::") {
        rest = &rest[at + "Command::".len()..];
        let variant: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if variant.is_empty() {
            continue;
        }
        // The service name is either a string literal or a constant path; both
        // are only ever used in the failure message below.
        let tail = &rest[variant.len()..];
        let entry = &tail[..tail.find("(Command::").unwrap_or(tail.len())];
        let service = match entry.find('"') {
            Some(quote) => entry[quote + 1..]
                .split('"')
                .next()
                .unwrap_or("")
                .to_owned(),
            None => entry
                .trim_end_matches([',', ')', '\n', ' '])
                .trim()
                .to_owned(),
        };
        out.insert(variant, service);
    }
    assert_eq!(
        out.len(),
        entries,
        "STANDALONE_LOG_SERVICES 有 {entries} 行以 `(Command::` 开头的条目，却只解析出 {} \
         个（{:?}）：解析器认不出的那些子命令，其工作负载在这条判据里整条消失",
        out.len(),
        out
    );
    out
}

fn yaml_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            yaml_files(&path, out);
        } else if path.extension().map(|e| e == "yaml").unwrap_or(false) {
            out.push(path);
        }
    }
}

/// Every container object anywhere in the document, whatever workload kind
/// carries it. The paths differ per kind and per profile, and a walk that only
/// knew one of them would pass over the others silently.
fn containers(value: &serde_yaml::Value, out: &mut Vec<serde_yaml::Mapping>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                if key.as_str() == Some("containers") {
                    if let serde_yaml::Value::Sequence(items) = child {
                        for item in items {
                            if let serde_yaml::Value::Mapping(container) = item {
                                out.push(container.clone());
                            }
                        }
                    }
                }
                containers(child, out);
            }
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                containers(item, out);
            }
        }
        _ => {}
    }
}

fn strings_of(container: &serde_yaml::Mapping, key: &str) -> Vec<String> {
    container
        .get(serde_yaml::Value::String(key.to_owned()))
        .and_then(|v| v.as_sequence())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn env_names(container: &serde_yaml::Mapping) -> BTreeSet<String> {
    container
        .get(serde_yaml::Value::String("env".to_owned()))
        .and_then(|v| v.as_sequence())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("name")?.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn every_workload_that_runs_a_standalone_command_can_reach_the_log_backend() {
    let root = repo_root();
    let tokens = argv_tokens(&root);
    let wired = log_wired_variants(&root);
    assert!(
        wired.len() >= 5,
        "standalone_log_service maps only {} commands; either the function \
         stopped naming them, or this gate is reading the wrong text",
        wired.len()
    );

    // argv spelling -> stream name, for every command the assembly installs a
    // subscriber for.
    let mut watched: BTreeMap<String, String> = BTreeMap::new();
    for (variant, service) in &wired {
        let token = tokens.get(variant).unwrap_or_else(|| {
            panic!(
                "standalone_log_service names Command::{variant}, which the \
                 argument table in cli.rs does not spell: the service name \
                 (`{service}`) belongs to a command no workload can run"
            )
        });
        watched.insert(token.clone(), service.clone());
    }

    let mut files = Vec::new();
    yaml_files(&root.join("deploy/rendered"), &mut files);
    assert!(
        files.len() > 10,
        "read {} rendered manifests; the walk is not seeing the trees",
        files.len()
    );

    let mut checked = BTreeSet::new();
    let mut missing = Vec::new();
    for file in &files {
        let text = read(file);
        for document in text.split("\n---") {
            let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(document) else {
                continue;
            };
            let mut found = Vec::new();
            containers(&value, &mut found);
            for container in &found {
                let argv = strings_of(container, "args");
                let Some(token) = argv.iter().find(|arg| watched.contains_key(arg.as_str())) else {
                    continue;
                };
                let service = &watched[token];
                checked.insert(token.clone());
                let env = env_names(container);
                let name = container
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<unnamed>");
                for want in ["COGNEVA_LOKI_ENDPOINT", "COGNEVA_LOKI_ENABLED"] {
                    if !env.contains(want) {
                        missing.push(format!(
                            "{} [{}] 跑的是 `{}`（服务名 {service}），env 里没有 {want}",
                            file.strip_prefix(&root).unwrap_or(file).display(),
                            name,
                            token
                        ));
                    }
                }
            }
        }
    }

    assert!(
        missing.is_empty(),
        "这些工作负载跑的是独立子命令，但没有接上日志后端：\n  {}\n\
         它们的日志只写容器 stdout，容器一删就没了——而「这个 Job 为什么失败」\n\
         「这台节点的走查为什么起不来」正需要那段日志。补上 chart 模板里的\n\
         `{{{{- include \"cogneva.logEnv\" . }}}}`，并把同一对 env 抄进\n\
         `deploy/k3s/` 的静态基线（parity 门要求两边逐字段对齐）。",
        missing.join("\n  ")
    );

    assert!(
        checked.len() >= 4,
        "只在 {} 个工作负载里认出了独立子命令（{:?}）：这条判据今天几乎在空集上通过",
        checked.len(),
        checked
    );
}
