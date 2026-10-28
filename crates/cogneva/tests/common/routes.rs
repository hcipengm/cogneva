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

/// 读文件、走目录的那两条规则本身在 [`super::fsio`]——那里不 import 兄弟模块，
/// 所以 `src/` 的单测也能按 `#[path]` 编它。这里只是把它们原样再导一次：消费者
/// 写的是 `common::routes::read`，一处都不用改，而定义仍然只有一份。
#[allow(unused_imports)]
pub use super::fsio::{read, walk, walk_all, walk_files};

/// 仓库相对名的一条读取：`read` 收的是 `&Path`，而夹具手里大多只有一个表里写着
/// 相对名的字符串，于是十来份夹具各自写了一遍 `repo_root().join(rel)` 加同一条
/// panic——那正是「怎么读一个文件」这条规则的第二份，只是参数类型不同。
pub fn read_repo(rel: &str) -> String {
    read(&repo_root().join(rel))
}

/// 这棵树里每份源文件的相对名与全文，一次读齐。
///
/// 两份夹具各写过一遍同样的三行；两处都用 `ok()?` 把读失败丢掉了——于是读不到的
/// 那一份悄悄退出人口，剩下的那份普查照样断言「没有违规」，而「它有违规但没读到」
/// 与「它干净」在判词上同形。这里读不出来就 panic。
pub fn source_texts(root: &Path) -> Vec<(String, String)> {
    super::rust_production::src_and_test_sources(root)
        .into_iter()
        .map(|path| {
            let text = read(&path);
            (super::rust_production::relative_name(root, &path), text)
        })
        .collect()
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

/// `NAME: &str = "..."` declarations, by name, from every crate's production
/// source.
///
/// A `.route(SOME_PATH, …)` call names its path indirectly, so this table is
/// what turns the registration into a shape. A name the table does not carry
/// contributes no route at all -- the path drops out of the served set without
/// a word, and every reader downstream then answers about a tree that registers
/// fewer paths than it does.
///
/// The population is every crate rather than the one crate this reader was
/// first written against. The constant a route names may sit beside the route:
/// the gateway declares `pub(crate) const INTERNAL_…_PATH` in the very module
/// that registers it, and a reader walking one hardcoded crate path dropped
/// that registration silently. Walking `crates/` instead means the population
/// is the tree's own crate list, which is also why a fixture tree needs no
/// invented `cog-core`.
///
/// Two cuts keep that widening from making the table say something false:
/// `#[cfg(test)]` items are skipped (a fixture may declare a same-named
/// constant with a fake path), and a name two crates give different values is
/// dropped rather than resolved to whichever crate the walk reached last --
/// an ambiguous name is not a resolution, and a route that loses it shows up as
/// a path with nothing serving it, which is loud, rather than as a plausible
/// wrong shape, which is not.
pub fn path_constants(root: &Path) -> BTreeMap<String, String> {
    let crates = root.join("crates");
    let entries = std::fs::read_dir(&crates).unwrap_or_else(|e| {
        panic!(
            "{} 读不出来（{e}）：路由常量的普查面是这棵树的全部 crate，\
             少读一个 crate 就是一批 .route(CONST) 悄悄不再算作一条路由",
            crates.display()
        )
    });
    let mut files = Vec::new();
    for entry in entries.flatten() {
        let src = entry.path().join("src");
        // A crate without `src` declares no source, so it carries no constant.
        // This is a property of the tree, not a failed read: `crates/` itself
        // being unreadable is the panic above.
        if src.is_dir() {
            walk(&src, &["rs"], &mut files);
        }
    }
    let mut declared: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let text = read(&path);
        for line in super::rust_production::production_lines(&text) {
            if let Some((name, value)) = const_path(line) {
                declared.entry(name).or_default().insert(value);
            }
        }
    }
    declared
        .into_iter()
        .filter_map(|(name, mut values)| {
            if values.len() == 1 {
                values.pop_first().map(|value| (name, value))
            } else {
                None
            }
        })
        .collect()
}

/// `NAME` and the literal in `<vis> const NAME: &str = "…"`.
fn const_path(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim_start();
    let rest = trimmed
        .strip_prefix("pub(crate) const ")
        .or_else(|| trimmed.strip_prefix("pub(super) const "))
        .or_else(|| trimmed.strip_prefix("pub const "))
        .or_else(|| trimmed.strip_prefix("const "))?;
    let (name, tail) = rest.split_once(':')?;
    let value = tail.trim_start().strip_prefix("&str = \"")?;
    let end = value.find('"')?;
    Some((name.trim().to_string(), value[..end].to_string()))
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

/// The offset of the paren closing the one at `open`, skipping spans inside
/// `"…"` because a bracket in a string is not a bracket.
///
/// `open` and the answer are both **byte** offsets: the sources these readers
/// walk are full of Chinese, so a char index and a byte index are different
/// numbers and slicing by the wrong one cuts a character in half.
pub fn matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    matching_delimiter(bytes, open, b'(', b')')
}

/// The offset of the brace closing the one at `open`; byte offsets, as
/// [`matching_paren`].
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
    let constants = path_constants(root);
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
            // Rust 的块注释**可以嵌套**：`/* /* */ */` 的结尾在第二个 `*/`。只找
            // 第一个 `*/` 的写法会把后半截注释当成代码读——而这一半在判词上与
            // 「那里真写了这个名字」同形。
            let mut depth = 0usize;
            while i < chars.len() {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                    continue;
                }
                if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                    continue;
                }
                // 换行留着：按行读的消费者不该因为一段块注释被删就看见两行合成一行。
                if chars[i] == '\n' {
                    out.push('\n');
                }
                i += 1;
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, rel: &str, body: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().expect("a file has a parent")).expect("dir");
        std::fs::write(&path, body).expect("write");
    }

    fn shapes(root: &Path) -> Vec<String> {
        route_entries(root).into_iter().map(|r| r.shape).collect()
    }

    /// 常量声明在路由旁边时，那条路由仍然是一条路由。
    ///
    /// 这张表曾经只走一个写死的 crate 路径（`crates/cog-core/src`）。网关把几个
    /// 路径常量声明在自己的模块里（`pub(crate) const INTERNAL_…_PATH`），于是那些
    /// `.route(CONST)` 无声地从「服务过的路径」里消失——判据读到的是一棵比实际注册
    /// 得更少的树。普查面现在取这棵树自己的 crate 列表，夹具树也不必再凭空造一个
    /// 它没写过的 `cog-core`。
    #[test]
    fn a_constant_declared_beside_its_route_still_names_the_route() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        write(
            root,
            "crates/cog-gateway/src/lib.rs",
            "pub(crate) const INTERNAL_STATUS_PATH: &str = \"/internal/status\";\n\
             pub fn router() -> Router {\n\
                 Router::new().route(INTERNAL_STATUS_PATH, post(status))\n\
             }\n",
        );
        assert_eq!(shapes(root), vec!["/internal/status".to_string()]);
    }

    /// 两个 crate 给同一个名字两个取值时，那不是一次解析。名字整个不算，它那条
    /// 路由就落到「没有东西服务这个路径」里——红，且点得出是哪个路径——而不是
    /// 解析成遍历先后决定的那个值。
    #[test]
    fn a_name_two_crates_disagree_on_is_not_resolved() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        write(
            root,
            "crates/cog-one/src/lib.rs",
            "pub const SHARED_PATH: &str = \"/api/v1/one\";\n",
        );
        write(
            root,
            "crates/cog-two/src/lib.rs",
            "pub const SHARED_PATH: &str = \"/api/v1/two\";\n",
        );
        write(
            root,
            "crates/cog-gateway/src/lib.rs",
            "pub fn router() -> Router {\n\
                 Router::new().route(SHARED_PATH, get(shared))\n\
             }\n",
        );
        assert!(
            shapes(root).is_empty(),
            "重名常量被解析成了 {:?}——遍历顺序决定了路由的形状",
            shapes(root)
        );
    }

    /// `#[cfg(test)]` 里的常量是夹具自己的路径，不是程序注册的。放它进来，一条
    /// 真路由就会被解析成假路径，而那条假路径恰好也「服务过」。
    #[test]
    fn a_constant_inside_a_test_module_is_not_resolved() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        write(
            root,
            "crates/cog-gateway/src/lib.rs",
            "pub fn router() -> Router {\n\
                 Router::new().route(FIXTURE_PATH, get(fixture))\n\
             }\n\
             #[cfg(test)]\n\
             mod tests {\n\
                 pub const FIXTURE_PATH: &str = \"/fixture/only\";\n\
             }\n",
        );
        assert!(
            shapes(root).is_empty(),
            "夹具里的常量被当成了程序声明的路径: {:?}",
            shapes(root)
        );
    }

    /// `crates/` 读不出来时这张表必须炸：空表不会让任何断言变红，它只会让一批
    /// `.route(CONST)` 悄悄退出「服务过的路径」。
    #[test]
    fn a_crates_directory_that_cannot_be_read_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("no-crates-here");
        let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| path_constants(&root)));
        assert!(
            got.is_err(),
            "没有 crates/ 的树交出了一张空常量表，判据照旧往下走"
        );
    }
}
