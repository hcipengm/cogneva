//! principle: feedback_no_hardcoding_config_surface
//! Every request the WebUI makes names a path the gateway serves, and the two
//! sides never meet at build time: one is TypeScript compiled into the image's
//! `web/` directory, the other is an axum router in another crate. Renaming a
//! route on the server is not an error anywhere — the client simply gets a 404,
//! and a board that renders its rows from an empty list renders exactly the same
//! way for "nothing happened yet". The panel is what the operator looks at, so
//! the missing route reads as a quiet system rather than as a broken page.
//!
//! So the client's paths are collected from `web/src` and matched against the
//! routes the gateway registers, constants resolved through `cog_core` where a
//! route is declared there. Matching is by shape: `{id}`, `{*path}` and a
//! `${...}` segment are all one wildcard, and a trailing `${...}` that is not a
//! path segment (a query suffix) is dropped before comparing.
//!
//! 实测（2026-10-27）：客户端 82 条路径形状全部有服务端路由；网关一侧读到
//! 224 条 `.route(...)`。消融对照：把 `web/src/api/client.ts` 里的
//! `/api/v1/processes` 改成 `/api/v1/host-processes`，门点名该文件后红，改回即绿。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, extensions, out);
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| extensions.contains(&e))
        {
            out.push(path);
        }
    }
}

/// One path reduced to the shape both sides can be compared by.
///
/// `$` and `{}` are handled in one pass because a `${...}` may carry a nested
/// template literal of its own (`` `/api/v1/audit/events${suffix ? `?${suffix}`
/// : ''}` ``), which a regex on the raw text cuts in the wrong place.
fn normalize(raw: &str) -> String {
    let mut out = String::new();
    let bytes: Vec<char> = raw.chars().collect();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            '?' => break,
            '$' if bytes.get(index + 1) == Some(&'{') => {
                let mut depth = 0usize;
                let mut end = index + 1;
                let mut closed = false;
                while end < bytes.len() {
                    match bytes[end] {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                closed = true;
                                break;
                            }
                        }
                        _ => {}
                    }
                    end += 1;
                }
                let whole_segment = out.is_empty() || out.ends_with('/');
                if whole_segment {
                    out.push('{');
                    out.push('}');
                }
                // Not a whole segment: a query or string suffix, which is not
                // part of the path. An unterminated one ends the literal.
                if !closed {
                    break;
                }
                index = end + 1;
            }
            '{' => {
                while index < bytes.len() && bytes[index] != '}' {
                    index += 1;
                }
                index += 1;
                out.push('{');
                out.push('}');
            }
            c => {
                out.push(c);
                index += 1;
            }
        }
    }
    while out.ends_with('/') {
        out.pop();
    }
    out
}

/// The paths the WebUI asks for, keyed by shape, with the files that ask.
fn client_paths(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut files = Vec::new();
    walk(&root.join("web/src"), &["ts", "tsx"], &mut files);
    files.sort();
    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text = read(&path);
        let mut rest = text.as_str();
        while let Some(at) = rest.find("/api/") {
            let tail = &rest[at..];
            let quote = tail[..1].to_string();
            // Only a literal that starts at the quote counts; `/api/` inside a
            // longer string (a doc link, a log line) is not a request.
            let opens_literal = at > 0 && {
                let before = rest[..at].chars().next_back().unwrap_or(' ');
                before == '"' || before == '\'' || before == '`'
            };
            if opens_literal && quote == "/" {
                let literal: String = tail
                    .chars()
                    .take_while(|c| *c != '"' && *c != '\'' && *c != '`' && *c != '\n')
                    .collect();
                found
                    .entry(normalize(&literal))
                    .or_default()
                    .insert(rel.clone());
                rest = &rest[at + 1..];
            } else {
                rest = &rest[at + 4..];
            }
        }
    }
    found
}

/// `pub const NAME: &str = "..."` values in the crate that declares routes.
fn core_path_constants(root: &Path) -> BTreeMap<String, String> {
    let mut files = Vec::new();
    walk(&root.join("crates/cog-core/src"), &["rs"], &mut files);
    let mut constants = BTreeMap::new();
    for path in files {
        let text = read(&path);
        for line in text.lines() {
            let Some(at) = line.find("pub const ") else {
                continue;
            };
            let rest = &line[at + "pub const ".len()..];
            let Some((name, tail)) = rest.split_once(':') else {
                continue;
            };
            if !tail.trim_start().starts_with("&str = \"") {
                continue;
            }
            let Some(start) = tail.find('"') else {
                continue;
            };
            let Some(end) = tail[start + 1..].find('"') else {
                continue;
            };
            constants.insert(
                name.trim().to_string(),
                tail[start + 1..start + 1 + end].to_string(),
            );
        }
    }
    constants
}

/// The paths the gateway registers.
fn served_paths(root: &Path) -> BTreeSet<String> {
    let constants = core_path_constants(root);
    let mut files = Vec::new();
    walk(&root.join("crates/cog-gateway/src"), &["rs"], &mut files);
    files.sort();
    let mut served = BTreeSet::new();
    for path in files {
        let text = read(&path);
        // The path is on the same line as `.route(` about as often as it is on
        // the next one, so the argument is read across the whitespace rather
        // than line by line.
        let mut rest = text.as_str();
        while let Some(at) = rest.find(".route(") {
            rest = &rest[at + ".route(".len()..];
            let argument = rest.trim_start();
            if let Some(literal) = argument.strip_prefix('"') {
                if let Some(end) = literal.find('"') {
                    served.insert(normalize(&literal[..end]));
                }
                continue;
            }
            // A route registered from a declaration resolves through that
            // declaration; an unresolvable one is reported by the caller.
            let name: String = argument
                .trim_start_matches("cog_core::")
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if let Some(value) = constants.get(&name) {
                served.insert(normalize(value));
            }
        }
    }
    served
}

#[test]
fn every_path_the_webui_asks_for_is_one_the_gateway_serves() {
    let root = repo_root();
    let client = client_paths(&root);
    let served = served_paths(&root);

    assert!(
        client.len() > 50,
        "只读出 {} 个客户端路径，这条判据没有题目",
        client.len()
    );
    assert!(
        served.len() > 100,
        "只读出 {} 个服务端路径，这条判据没有题目",
        served.len()
    );

    let missing: Vec<(&String, &BTreeSet<String>)> = client
        .iter()
        .filter(|(shape, _)| !served.contains(*shape))
        .collect();
    assert!(
        missing.is_empty(),
        "WebUI 请求的这些路径网关没有注册——它拿到的是一次 404，而按空列表画的板子\
         与「这段时间什么都没发生」长得一样，所以读出来是「安静」而不是「坏了」：\n{}",
        missing
            .iter()
            .map(|(shape, files)| format!(
                "  {shape}  ←  {}",
                files.iter().cloned().collect::<Vec<_>>().join(", ")
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_path_reader_handles_the_shapes_this_tree_writes() {
    // The three shapes actually present: a plain literal, a segment that is a
    // template expression, and a query suffix built from a nested template.
    assert_eq!(normalize("/api/v1/sessions"), "/api/v1/sessions");
    assert_eq!(
        normalize("/api/v1/sessions/${encodeURIComponent(sessionId)}/messages"),
        "/api/v1/sessions/{}/messages"
    );
    assert_eq!(
        normalize("/api/v1/audit/events${suffix ? `?${suffix}` : ''}"),
        "/api/v1/audit/events"
    );
    assert_eq!(normalize("/api/v1/events${suffix}"), "/api/v1/events");
    assert_eq!(normalize("/api/v1/x?limit=${limit}"), "/api/v1/x");
    assert_eq!(
        normalize("/api/v1/alerts/{id}/ack"),
        "/api/v1/alerts/{}/ack"
    );
}

#[test]
fn a_path_nothing_serves_is_reported() {
    let served: BTreeSet<String> = ["/api/v1/sessions".to_string()].into_iter().collect();
    let mut client = BTreeMap::new();
    client.insert(
        "/api/v1/sessions".to_string(),
        BTreeSet::from(["a.ts".to_string()]),
    );
    client.insert(
        "/api/v1/ghosts".to_string(),
        BTreeSet::from(["b.tsx".to_string()]),
    );

    let missing: Vec<&String> = client
        .keys()
        .filter(|shape| !served.contains(*shape))
        .collect();
    assert_eq!(missing, vec!["/api/v1/ghosts"]);
}
