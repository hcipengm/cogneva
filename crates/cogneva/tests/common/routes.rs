//! 网关注册了哪些路由，读一遍树就得出来。
//!
//! 这份读法有两家消费者，问的是同一个问题的两半：
//!
//!   - 客户端发出去的每个请求，落不落在一条真实的路由上（路径形状 ＋ 动词）；
//!   - 客户端订阅的每个 REST 面（`resourceKey('…')`），在不在服务端认得的前缀里。
//!
//! 分成两份写就会分叉：一边把 `.route(` 的参数读成形状，另一边从全树里刮
//! `/api/v1/<段>` 的字面量，两边对「网关服务什么」给出不同的答案，而差异恰好
//! 出现在两处判据都读不到的地方。所以只留一份。
//!
//! 这里只读**注册**（`.route(…)` 的参数），不读每一处提到 `/api/v1/` 的字符串：
//! 网关自己也当客户端（K8s 的 `/api/v1/namespaces/…`、Prometheus 的
//! `/api/v1/query`），把它们算成「网关照料的路径」，等于给判据开了个后门。

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// 这棵树的根。算法在 [`super::root`]——一份，全仓共用，别的 crate 的测试也编它。
///
/// `allow(unused_imports)` 是因为这一句要按「每个测试二进制」各判一次：任何一份
/// 测试文件写 `mod common;` 都会把这里编进去，而其中只有一部分真的会问根在哪。
/// 把它删掉让消费者改走 `common::root::repo_root`，等于让 75 个文件各写一条路径，
/// 而这里的一行恰恰是它们只写一条路径的原因。
#[allow(unused_imports)]
pub use super::root::repo_root;

pub fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

pub fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
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
pub fn normalize(raw: &str) -> String {
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

/// `pub const NAME: &str = "..."` values in the crate that declares routes.
pub fn core_path_constants(root: &Path) -> BTreeMap<String, String> {
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

/// One `.route(path, ...)` registration: the shape, the verbs it answers, and
/// the handler functions wired to each verb.
#[derive(Debug, Clone)]
pub struct Route {
    pub shape: String,
    pub verbs: BTreeSet<String>,
    pub handlers: BTreeSet<String>,
}

/// The verbs axum's combinators spell. A route whose handler expression is not
/// one of these shapes (`any(...)` is, `nest` is not) contributes no verb, and
/// the callers that compare verbs treat the route as unjudgeable rather than as
/// answering everything.
pub const VERBS: &[&str] = &[
    "get", "post", "put", "patch", "delete", "head", "options", "any",
];

/// The offset of the bracket closing the one at `open`.
pub fn matching_delimiter(bytes: &[u8], open: usize, opening: u8, closing: u8) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            c if c == opening => depth += 1,
            c if c == closing => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The offset of the paren closing the one at `open`.
pub fn matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    matching_delimiter(bytes, open, b'(', b')')
}

/// The offset of the brace closing the one at `open`.
pub fn matching_brace(bytes: &[u8], open: usize) -> Option<usize> {
    matching_delimiter(bytes, open, b'{', b'}')
}

/// Every route the gateway registers, read once here so the path reader, the
/// verb reader and the request-body reader cannot disagree about what a
/// `.route(…)` call means.
pub fn route_entries(root: &Path) -> Vec<Route> {
    let mut files = Vec::new();
    walk(&root.join("crates/cog-gateway/src"), &["rs"], &mut files);
    files.sort();
    route_entries_in(root, &files, false)
}

/// [`route_entries`] over an explicit file list.
///
/// `production_only` drops `#[cfg(test)]` bodies. A route registered inside a
/// test is served by no process, so a reader that answers "does the code this
/// container runs serve this path" must not count it. The gateway-wide reader
/// leaves them in: its question is whether *anything* serves the path a client
/// sends, where an extra served path only makes the answer more lenient, and
/// changing that would change what those gates judge.
pub fn route_entries_in(root: &Path, files: &[PathBuf], production_only: bool) -> Vec<Route> {
    let constants = core_path_constants(root);
    let mut routes = Vec::new();
    for path in files {
        let text = read(path);
        let text = if production_only {
            super::rust_production::production_text(&text)
        } else {
            text
        };
        let mut cursor = 0;
        while let Some(at) = text[cursor..].find(".route(") {
            let call = cursor + at;
            let open = call + ".route".len();
            cursor = open + 1;
            let argument = text[cursor..].trim_start();
            let shape = if let Some(literal) = argument.strip_prefix('"') {
                literal.find('"').map(|end| normalize(&literal[..end]))
            } else {
                // A route registered from a declaration resolves through that
                // declaration. Only a bare or module-qualified name is read; a
                // call of any other shape contributes no route, which shows up
                // as a client path with nothing serving it rather than as a
                // quiet pass.
                let qualified: String = argument
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
                    .collect();
                let name = qualified.rsplit("::").next().unwrap_or(qualified.as_str());
                constants.get(name).map(|value| normalize(value))
            };
            let Some(shape) = shape else {
                continue;
            };
            let Some(close) = matching_paren(text.as_bytes(), open) else {
                continue;
            };
            let expression = &text[open + 1..close];
            let mut verbs = BTreeSet::new();
            let mut handlers = BTreeSet::new();
            for verb in VERBS {
                let needle = format!("{verb}(");
                let mut from = 0;
                while let Some(found_at) = expression[from..].find(&needle) {
                    let hits = from + found_at;
                    from = hits + needle.len();
                    let boundary = expression[..hits]
                        .chars()
                        .next_back()
                        .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
                    if !boundary {
                        continue;
                    }
                    verbs.insert((*verb).to_string());
                    // The handler is the callee's last path segment. A closure
                    // or a nested expression names no function, and then the
                    // route simply carries no handler for this verb.
                    let callee: String = expression[from..]
                        .trim_start()
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
                        .collect();
                    let name = callee.rsplit("::").next().unwrap_or(callee.as_str());
                    if !name.is_empty() && !name.starts_with(|c: char| c.is_ascii_digit()) {
                        handlers.insert(name.to_string());
                    }
                }
            }
            routes.push(Route {
                shape,
                verbs,
                handlers,
            });
        }
    }
    routes
}

/// The paths the gateway registers.
pub fn served_paths(root: &Path) -> BTreeSet<String> {
    route_entries(root).into_iter().map(|r| r.shape).collect()
}

/// The verbs each registered path answers.
pub fn served_methods(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut served: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for route in route_entries(root) {
        served.entry(route.shape).or_default().extend(route.verbs);
    }
    served
}

/// The handler functions each `(verb, shape)` is wired to.
pub fn served_handlers(root: &Path) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut served: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for route in route_entries(root) {
        for verb in &route.verbs {
            served
                .entry((verb.to_uppercase(), route.shape.clone()))
                .or_default()
                .extend(route.handlers.iter().cloned());
        }
    }
    served
}

/// 服务端认得的前缀：`/api/v1/<段>` 里的第一段。
///
/// 一次写落在哪个前缀下，`resource_domain()` 就把「这个面过期了」发到
/// `resource:<段>` 上；前缀表就是这一族帧的地址空间。不在表里的面名，
/// 客户端再使劲订也等不到那一帧。
pub fn served_prefixes(root: &Path) -> BTreeSet<String> {
    let mut prefixes = BTreeSet::new();
    for path in served_paths(root) {
        let Some(rest) = path.strip_prefix("/api/v1/") else {
            continue;
        };
        let segment = rest.split('/').next().unwrap_or("");
        if !segment.is_empty() && segment != "{}" {
            prefixes.insert(segment.to_string());
        }
    }
    prefixes
}

/// 去掉注释，保留字符串（字符串里写着的路径仍然是路径）。
///
/// 路径的普查可以读注释：写在散文里的一条路径仍然是网关服务的路径，读它不要钱。
/// 动词的普查读不了——注释里点名的路径前面没有动词，最近的那个动词会是从旁边
/// 某个调用借来的。
pub fn strip_comments(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || c == '\'' || c == '`' {
            out.push(c);
            i += 1;
            while i < chars.len() {
                out.push(chars[i]);
                if chars[i] == '\\' {
                    i += 1;
                    if i < chars.len() {
                        out.push(chars[i]);
                    }
                } else if chars[i] == c {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}
