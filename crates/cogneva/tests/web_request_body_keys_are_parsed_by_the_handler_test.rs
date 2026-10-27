//! principle: feedback_no_hardcoding_config_surface
//!
//! 客户端「发什么」也有两份写法：WebUI 里 `api.post('/api/v1/ide/rename', { from,
//! to })` 写的是请求体的键，网关那边 `#[derive(Deserialize)] struct RenameBody
//! { from, to }` 写的是同一份键的另一次拼写。中间没有任何东西比对两边。
//!
//! 改一边的代价不是报错，是**静默**：serde 默认不认的键直接丢掉（没有
//! `deny_unknown_fields`），于是 `from` 改名成 `source` 之后，服务端拿到的是
//! 一个缺字段的结构体，读到的是默认值；界面上这次重命名成功返回、文件纹丝不动。
//! 这类缺陷在读数上和「今天没有重命名」完全一样。
//!
//! 判一个方向：客户端发出的每一个请求体键，都必须是这条路由的处理函数用
//! `Json<T>` 解出来的那个结构体里的一个字段（按 serde 序列化后的名字判，`rename`
//! / `alias` / `rename_all` 都算进去）。反方向不判——服务端有而客户端不发的字段
//! 多半是可选的，判了只会一片假红。
//!
//! 读不到就跳过，并且数出来：请求体是变量而不是内联对象、对象里有展开、处理函数
//! 收的是 `Json<Value>`、结构体认不出——这些都不判，但普查里各有一格，所以
//! 「判了 0 条」和「一条都对」不会长得一样。
//!
//! 两个客户端都读：`web/src`（`api.post/put/patch(path, { … })`）和网关二进制里
//! 内嵌的运维页（`api(path, 'POST', { … })` 与 `fetch(path, { method, body })`）。
//!
//! 实测（2026-10-27）：两处合计 43 个内联请求体（WebUI 24、运维页 19），判到 36 个、
//! 60 个键，全部落在处理函数解出来的结构体里；6 处把请求体放在变量里（读不到键），
//! 7 处的处理函数没有可判的 `Json<T>`。
//!
//! 消融两条，客户端各一条，各自点名对应文件后红、改回即绿：把 `crates/cog-gateway/src/
//! ide.rs` 里 `RenameBody.from` 的 serde 名改成 `source`（报 `from ← web/src/api/
//! client.ts`）；把 `crates/cog-gateway/src/promotion_gate.rs` 里 `PromotionGateRequest
//! .enabled` 的 serde 名改成 `enable`（报 `enabled ← crates/cog-gateway/src/takeover.html`）。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

mod common;
use common::routes::{
    matching_brace, matching_paren, normalize, read, repo_root, route_entries, served_handlers,
    strip_comments, walk, VERBS,
};
use common::rust_production::production_text;

const WEB: &str = "web/src";
const CRATES: &str = "crates";
const GATEWAY: &str = "crates/cog-gateway/";
/// The verbs that carry a request body. A GET with a body is not a thing the
/// client does here, and judging one would only report axum's own quirks.
const BODY_METHODS: &[&str] = &["post", "put", "patch"];

/// One request the WebUI makes with a body it spelled out inline.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Body {
    method: String,
    shape: String,
    file: String,
    keys: BTreeSet<String>,
}

/// The top-level keys of an object literal body, or `None` when the object
/// cannot be enumerated: a spread (`...rest`) and a computed key (`[k]:`) both
/// stand for keys this reader never sees, and guessing the rest is how a ghost
/// key sails through.
fn top_level_keys(body: &str) -> Option<BTreeSet<String>> {
    let parts = split_top_level(body);
    let mut keys = BTreeSet::new();
    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if part.starts_with("...") || part.starts_with('[') {
            return None;
        }
        if let Some(rest) = part.strip_prefix('"').or_else(|| part.strip_prefix('\'')) {
            let quote = &part[..1];
            if let Some(end) = rest.find(quote) {
                let after = rest[end + 1..].trim_start();
                if after.starts_with(':') {
                    let key = &rest[..end];
                    if is_identifier(key) {
                        keys.insert(key.to_string());
                    }
                }
            }
            continue;
        }
        let name: String = part
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        if name.is_empty() || !is_identifier(&name) {
            continue;
        }
        let after = part[name.len()..].trim_start();
        // A shorthand (`{ path }`) and a spelled pair (`{ path: p }`) name the
        // same key; a bare expression (`{ ...}` already excluded, `fn()` here)
        // names none.
        if after.is_empty() || after.starts_with(':') {
            keys.insert(name);
        }
    }
    Some(keys)
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// A comma-separated list cut at the top level only: commas inside parentheses,
/// brackets, braces and string or template literals stay in their piece.
fn split_top_level(list: &str) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let chars: Vec<char> = list.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || c == '\'' || c == '`' {
            let quote = c;
            current.push(c);
            i += 1;
            while i < chars.len() {
                current.push(chars[i]);
                if chars[i] == '\\' {
                    i += 1;
                    if i < chars.len() {
                        current.push(chars[i]);
                    }
                } else if chars[i] == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(std::mem::take(&mut current));
                i += 1;
                continue;
            }
            _ => {}
        }
        current.push(c);
        i += 1;
    }
    parts.push(current);
    parts
}

/// Every `api.post/put/patch('<path>', { … })` in the WebUI.
///
/// Returns the calls it could read and, separately, the count of calls that
/// carry a literal path but whose body is a variable -- a body this reader
/// cannot open without following bindings, which it does not do.
fn client_bodies(text: &str, file: &str) -> (Vec<Body>, usize) {
    let mut bodies = Vec::new();
    let mut opaque = 0usize;
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("api.") {
        let after_api = cursor + at + "api.".len();
        let name: String = text[after_api..]
            .chars()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();
        cursor = after_api;
        if !BODY_METHODS.contains(&name.as_str()) {
            continue;
        }
        let mut rest = text[after_api + name.len()..].trim_start();
        if rest.starts_with('<') {
            let mut depth = 0i32;
            let mut end = None;
            for (index, c) in rest.char_indices() {
                match c {
                    '<' => depth += 1,
                    '>' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(index);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let Some(end) = end else { break };
            rest = rest[end + 1..].trim_start();
        }
        if !rest.starts_with('(') {
            continue;
        }
        let open = text.len() - rest.len();
        let Some(close) = matching_paren(text.as_bytes(), open) else {
            break;
        };
        cursor = close + 1;
        let arguments = &text[open + 1..close];
        let Some((literal, remainder)) = first_path_literal(arguments) else {
            continue;
        };
        let remainder = remainder.trim_start();
        let remainder = remainder
            .strip_prefix(',')
            .unwrap_or(remainder)
            .trim_start();
        if !remainder.starts_with('{') {
            opaque += 1;
            continue;
        }
        let Some(body_end) = matching_brace(remainder.as_bytes(), 0) else {
            continue;
        };
        let Some(keys) = top_level_keys(&remainder[1..body_end]) else {
            opaque += 1;
            continue;
        };
        bodies.push(Body {
            method: name.to_uppercase(),
            shape: normalize(&literal),
            file: file.to_string(),
            keys,
        });
    }
    (bodies, opaque)
}

/// The quotes around a quoted word, if the piece is one: `'POST'`, `"POST"`.
fn quoted_word(piece: &str) -> Option<String> {
    let piece = piece.trim();
    let quote = piece.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let end = piece[1..].find(quote)?;
    Some(piece[1..1 + end].to_string())
}

/// The body objects the operator page posts: `api(path, 'POST', { … })` and
/// `fetch(path, { method: 'POST', body: JSON.stringify({ … }) })`.
///
/// The page ships inside the gateway binary and calls the same router, so a
/// key it sends is that router's business in exactly the way a key from the
/// WebUI is.
fn page_bodies(text: &str, file: &str) -> Vec<Body> {
    let mut bodies = Vec::new();
    let mut scan = |opener: &str, verb_argument: &str| {
        let mut cursor = 0;
        while let Some(at) = text[cursor..].find(opener) {
            let start = cursor + at;
            cursor = start + opener.len();
            // `api(` must not be the tail of `myapi(` or `api.get(`; `fetch(`
            // must not be the tail of `prefetch(`.
            let before = text[..start].chars().next_back().unwrap_or(' ');
            if before.is_alphanumeric() || before == '_' || before == '.' {
                continue;
            }
            let open = start + opener.len() - 1;
            let Some(close) = matching_paren(text.as_bytes(), open) else {
                break;
            };
            cursor = close + 1;
            let arguments = split_top_level(&text[open + 1..close]);
            let Some(path) = arguments.first().and_then(|p| quoted_word(p)) else {
                continue;
            };
            if !path.starts_with("/api/") {
                continue;
            }
            let (method, body): (String, String) = match verb_argument {
                // `api(path, 'POST', { … })`
                "verb" => {
                    let Some(verb) = arguments.get(1).and_then(|p| quoted_word(p)) else {
                        continue;
                    };
                    let Some(body) = arguments.get(2) else {
                        continue;
                    };
                    (verb, body.trim().to_string())
                }
                // `fetch(path, { method: 'POST', body: JSON.stringify({ … }) })`
                _ => {
                    let Some(options) = arguments.get(1) else {
                        continue;
                    };
                    let options = options.trim();
                    let Some(inner) = options.strip_prefix('{') else {
                        continue;
                    };
                    let Some(inner) = inner.strip_suffix('}') else {
                        continue;
                    };
                    let mut method: Option<String> = None;
                    let mut body: Option<String> = None;
                    for option in split_top_level(inner) {
                        let option = option.trim();
                        if let Some(rest) = option.strip_prefix("method:") {
                            method = quoted_word(rest);
                        }
                        if let Some(rest) = option.strip_prefix("body:") {
                            let rest = rest.trim();
                            if let Some(at) = rest.find("JSON.stringify(") {
                                let inner_open = at + "JSON.stringify".len();
                                if let Some(end) = matching_paren(rest.as_bytes(), inner_open) {
                                    body = Some(rest[inner_open + 1..end].to_string());
                                }
                            }
                        }
                    }
                    let Some(method) = method else { continue };
                    let Some(body) = body else { continue };
                    (method, body)
                }
            };
            if !BODY_METHODS.iter().any(|m| m.eq_ignore_ascii_case(&method)) {
                continue;
            }
            let body = body.trim();
            let Some(inner) = body.strip_prefix('{') else {
                continue;
            };
            let Some(inner) = inner.strip_suffix('}') else {
                continue;
            };
            let Some(keys) = top_level_keys(inner) else {
                continue;
            };
            bodies.push(Body {
                method: method.to_uppercase(),
                shape: normalize(&path),
                file: file.to_string(),
                keys,
            });
        }
    };
    scan("api(", "verb");
    scan("fetch(", "options");
    bodies
}

/// The quoted `/api/…` literal a call starts with, and the rest of its
/// arguments. A template literal is a literal too: `${…}` becomes a wildcard.
fn first_path_literal(arguments: &str) -> Option<(String, &str)> {
    let trimmed = arguments.trim_start();
    let quote = trimmed.chars().next()?;
    if quote != '"' && quote != '\'' && quote != '`' {
        return None;
    }
    let rest = &trimmed[1..];
    let end = rest.find(quote)?;
    let literal = &rest[..end];
    if !literal.starts_with("/api/") {
        return None;
    }
    Some((literal.to_string(), &rest[end + 1..]))
}

/// A request struct's accepted key names.
#[derive(Debug, Clone, Default)]
struct Fields {
    names: BTreeSet<String>,
}

/// `camelCase` and friends, as serde spells them. A rule this does not know is
/// not a rule it guesses at: the struct then reads as unjudgeable.
fn convert(name: &str, rule: &str) -> Option<String> {
    let words: Vec<&str> = name.split('_').collect();
    let capitalize = |w: &str| {
        let mut chars = w.chars();
        match chars.next() {
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            None => String::new(),
        }
    };
    match rule {
        "snake_case" => Some(name.to_string()),
        "camelCase" => Some(
            words
                .iter()
                .enumerate()
                .map(|(i, w)| if i == 0 { w.to_string() } else { capitalize(w) })
                .collect(),
        ),
        "PascalCase" => Some(words.iter().map(|w| capitalize(w)).collect()),
        "kebab-case" => Some(words.join("-")),
        "SCREAMING_SNAKE_CASE" => Some(name.to_uppercase()),
        _ => None,
    }
}

/// The `serde` attributes on a stretch of text, as `(key, value)` pairs.
fn serde_option(attribute: &str, key: &str) -> Option<String> {
    let needle = format!("{key} = \"");
    let at = attribute.find(&needle)?;
    let rest = &attribute[at + needle.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn serde_key_in(attribute: &str, key: &str) -> bool {
    attribute
        .split([',', '(', ')'])
        .any(|piece| piece.trim() == key)
}

/// The structs a handler can deserialize, keyed by type name.
///
/// Read from production text only: a fixture struct with the same name would
/// answer for the real one, and the gate would then be judging the specimen.
fn request_structs(root: &Path) -> BTreeMap<String, Fields> {
    let mut files = Vec::new();
    walk(&root.join(CRATES), &["rs"], &mut files);
    files.sort();
    let mut structs: BTreeMap<String, Fields> = BTreeMap::new();
    for path in files {
        let label = path.to_string_lossy().replace('\\', "/");
        if !label.contains("/src/") {
            continue;
        }
        let text = production_text(&read(&path));
        let mut cursor = 0;
        while let Some(at) = text[cursor..].find("struct ") {
            let start = cursor + at;
            cursor = start + "struct ".len();
            let after = &text[cursor..];
            let name: String = after
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() || !name.starts_with(|c: char| c.is_uppercase()) {
                continue;
            }
            let rest = &after[name.len()..];
            let trimmed = rest.trim_start();
            // Optional generic parameters, then the body. A tuple struct and a
            // unit struct carry no named fields.
            let after_generics = if trimmed.starts_with('<') {
                let Some(end) = trimmed.find('>') else {
                    continue;
                };
                trimmed[end + 1..].trim_start()
            } else {
                trimmed
            };
            let Some(after_brace) = after_generics.strip_prefix('{') else {
                continue;
            };
            let body_open = text.len() - after_brace.len() - 1;
            let Some(body_close) = matching_brace(text.as_bytes(), body_open) else {
                continue;
            };
            let body = &text[body_open + 1..body_close];
            // The attribute block directly above the declaration may carry
            // `rename_all`. Only that block: an earlier item's attributes are
            // not this struct's rule.
            let mut header: Vec<&str> = Vec::new();
            let mut window_start = start.saturating_sub(600);
            while window_start < start && !text.is_char_boundary(window_start) {
                window_start += 1;
            }
            let mut first = true;
            for line in text[window_start..start].lines().rev() {
                let line = line.trim();
                if first {
                    first = false;
                    // The fragment of the declaration's own line: `pub`,
                    // `pub(crate)`, or nothing at all. Anything else is the
                    // tail of a previous item and ends the block.
                    let modifier = line.is_empty()
                        || line == "pub"
                        || (line.starts_with("pub(") && line.ends_with(')'));
                    if modifier {
                        continue;
                    }
                    if !line.starts_with("#[") {
                        break;
                    }
                }
                if line.is_empty() {
                    continue;
                }
                if line.starts_with("#[") {
                    header.push(line);
                    continue;
                }
                break;
            }
            let rename_all = serde_option(&header.join("\n"), "rename_all");
            let mut fields = BTreeSet::new();
            let mut readable = true;
            let mut pending: Vec<String> = Vec::new();
            for line in body.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line.starts_with("#[") {
                    pending.push(line.to_string());
                    continue;
                }
                let Some((field, _)) = line.split_once(':') else {
                    pending.clear();
                    continue;
                };
                let field = field.trim().trim_start_matches("pub ").trim();
                if !is_identifier(field) {
                    pending.clear();
                    continue;
                }
                let mut names = BTreeSet::new();
                let mut renamed = false;
                for attribute in &pending {
                    if let Some(rename) = serde_option(attribute, "rename") {
                        names.insert(rename);
                        renamed = true;
                    }
                    if let Some(alias) = serde_option(attribute, "alias") {
                        names.insert(alias);
                    }
                    if serde_key_in(attribute, "flatten") {
                        // A flattened field is a whole object of keys this
                        // reader cannot see; judging the struct would be
                        // judging it with a hole in it.
                        readable = false;
                    }
                }
                pending.clear();
                if !renamed {
                    match &rename_all {
                        Some(rule) => match convert(field, rule) {
                            Some(name) => {
                                names.insert(name);
                            }
                            None => readable = false,
                        },
                        None => {
                            names.insert(field.to_string());
                        }
                    }
                }
                fields.extend(names);
            }
            if readable && !fields.is_empty() {
                // Same-named structs in different crates merge rather than
                // override: the union is the lenient side of a one-directional
                // judgement, and a wrong merge can only let a key through, not
                // turn a real one red.
                structs.entry(name).or_default().names.extend(fields);
            }
        }
    }
    structs
}

/// The `Json<T>` type each gateway handler deserializes.
fn handler_types(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut files = Vec::new();
    walk(&root.join(GATEWAY).join("src"), &["rs"], &mut files);
    files.sort();
    let mut handlers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let text = production_text(&read(&path));
        let mut cursor = 0;
        while let Some(at) = text[cursor..].find("fn ") {
            let start = cursor + at;
            cursor = start + "fn ".len();
            let after = &text[cursor..];
            let name: String = after
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            let rest = &after[name.len()..];
            // Skip the generic parameters of a handler, if any.
            let rest = match rest.find('(') {
                Some(at) if !rest[..at].contains(';') && !rest[..at].contains('{') => &rest[at..],
                _ => continue,
            };
            let open = cursor + (after.len() - rest.len());
            let Some(close) = matching_paren(text.as_bytes(), open) else {
                continue;
            };
            cursor = close + 1;
            let arguments = &text[open + 1..close];
            let types = handlers.entry(name).or_default();
            let mut from = 0;
            while let Some(found) = arguments[from..].find("Json<") {
                let at = from + found;
                from = at + "Json<".len();
                let inner: String = arguments[from..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                    .collect();
                let last = inner.rsplit("::").next().unwrap_or(inner.as_str());
                if !last.is_empty() {
                    types.insert(last.to_string());
                }
            }
        }
    }
    handlers
}

/// What the reader could and could not judge. Printed by the census test so a
/// narrowing reader shows up as numbers rather than as a green gate.
#[derive(Debug, Default)]
struct Census {
    calls_with_body: usize,
    bodies_read: usize,
    bodies_variable: usize,
    keys_judged: usize,
    calls_without_json: usize,
    calls_with_unknown_struct: usize,
}

fn contradictions(root: &Path) -> (Vec<String>, Census) {
    let structs = request_structs(root);
    let handlers = handler_types(root);
    let served = served_handlers(root);
    let mut census = Census::default();
    let mut complaints = Vec::new();

    let mut files = Vec::new();
    walk(&root.join(WEB), &["ts", "tsx"], &mut files);
    walk(&root.join(GATEWAY).join("src"), &["html"], &mut files);
    files.sort();
    for path in files {
        let file = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text = strip_comments(&read(&path));
        let (bodies, opaque) = if file.ends_with(".html") {
            (page_bodies(&text, &file), 0)
        } else {
            client_bodies(&text, &file)
        };
        census.bodies_variable += opaque;
        for body in bodies {
            census.calls_with_body += 1;
            let Some(wired) = served.get(&(body.method.clone(), body.shape.clone())) else {
                census.calls_without_json += 1;
                continue;
            };
            let mut accepted: BTreeSet<String> = BTreeSet::new();
            let mut judgeable = !wired.is_empty();
            let mut named: BTreeSet<String> = BTreeSet::new();
            for handler in wired {
                let Some(types) = handlers.get(handler) else {
                    judgeable = false;
                    continue;
                };
                for name in types {
                    if name == "Value" {
                        judgeable = false;
                        continue;
                    }
                    match structs.get(name) {
                        Some(fields) => {
                            accepted.extend(fields.names.iter().cloned());
                            named.insert(name.clone());
                        }
                        None => judgeable = false,
                    }
                }
            }
            if !judgeable || accepted.is_empty() {
                census.calls_with_unknown_struct += 1;
                continue;
            }
            census.bodies_read += 1;
            census.keys_judged += body.keys.len();
            for key in &body.keys {
                if accepted.contains(key) {
                    continue;
                }
                complaints.push(format!(
                    "  {} {}  键 `{key}`  ←  {}（{} 里没有这个字段，serde 会把它丢掉）",
                    body.method,
                    body.shape,
                    body.file,
                    named.iter().cloned().collect::<Vec<_>>().join("/")
                ));
            }
        }
    }
    (complaints, census)
}

#[test]
fn every_key_a_client_sends_is_one_the_handler_parses() {
    let (complaints, census) = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "客户端发了处理函数不认的键。serde 不会为它报错，只会把它丢掉：\
         服务端拿到的是缺字段的结构体，读到的是默认值，界面上这次操作成功返回、\
         实际什么都没改——读起来和「今天没做过这件事」一样：\n{}\n\
         （普查：{census:?}）",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let (_, census) = contradictions(&repo_root());
    assert!(
        census.bodies_read >= 30,
        "只判到 {} 个请求体，读取器看错地方了：{census:?}",
        census.bodies_read
    );
    assert!(
        census.keys_judged >= 50,
        "只判到 {} 个请求体键，太少：{census:?}",
        census.keys_judged
    );
    // 变量请求体与 `Json<Value>` 处理函数是这类判据读不到的两种形状；它们各自
    // 有个数，所以读空与对得上不会长得一样。
    assert!(
        census.bodies_read > census.bodies_variable + census.calls_without_json,
        "判到的比读不到的都少，读取器退化了：{census:?}"
    );
}

#[test]
fn the_body_reader_sees_shorthand_pairs_and_spreads() {
    assert_eq!(
        top_level_keys(" from, to: target, 'message': msg, nested: { a: 1, b: 2 }"),
        Some(BTreeSet::from([
            "from".to_string(),
            "to".to_string(),
            "message".to_string(),
            "nested".to_string(),
        ]))
    );
    // 展开与计算键代表这个读取器看不见的键：整条对象都不判，而不是判剩下那些。
    assert_eq!(top_level_keys(" path, ...rest"), None);
    assert_eq!(top_level_keys(" [key]: 1"), None);
    assert_eq!(top_level_keys(""), Some(BTreeSet::new()));
}

#[test]
fn the_field_reader_honours_serde_renames_and_unreadable_structs() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("dir");
    std::fs::write(
        root.join("crates/cog-gateway/src/bodies.rs"),
        "\
#[derive(Deserialize)]
pub struct Renamed {
    #[serde(rename = \"taskId\")]
    pub task_id: String,
    #[serde(alias = \"old_name\")]
    pub name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = \"camelCase\")]
pub struct Camel {
    pub parent_task_id: Option<String>,
}

#[derive(Deserialize)]
pub struct Flattened {
    #[serde(flatten)]
    pub rest: serde_json::Value,
    pub path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = \"weird\")]
pub struct UnknownRule {
    pub a_b: String,
}

#[cfg(test)]
mod tests {
    #[derive(Deserialize)]
    pub struct Fixture {
        pub ghost: String,
    }
}
",
    )
    .expect("structs");
    let structs = request_structs(root);
    assert_eq!(
        structs.get("Renamed").map(|f| f.names.clone()),
        Some(BTreeSet::from([
            "taskId".to_string(),
            "name".to_string(),
            "old_name".to_string()
        ]))
    );
    assert_eq!(
        structs.get("Camel").map(|f| f.names.clone()),
        Some(BTreeSet::from(["parentTaskId".to_string()]))
    );
    // 展开的字段是一个看不见的洞；认不出的 rule 不猜。
    assert!(!structs.contains_key("Flattened"), "{structs:?}");
    assert!(!structs.contains_key("UnknownRule"), "{structs:?}");
    // 测试里的同名字段是标本：生产那一半没有它。
    assert!(!structs.contains_key("Fixture"), "{structs:?}");
}

#[test]
fn the_page_reader_sees_both_call_shapes_and_skips_reads() {
    let page = "\
async function a(enabled) { await api('/api/v1/admin/promotion-gate', 'POST', { enabled }); }
async function b(name) { await api('/api/v1/x/' + name, 'POST', { name }); }
async function c() { await api('/api/v1/y', 'GET'); }
async function d(u, p) {
  await fetch('/api/v1/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: u, password: p }),
  });
}
async function e() { await fetch('/api/v1/z', { method: 'GET' }); }
";
    let bodies = page_bodies(page, "page.html");
    let mut seen: Vec<(String, String, Vec<String>)> = bodies
        .iter()
        .map(|b| {
            (
                b.method.clone(),
                b.shape.clone(),
                b.keys.iter().cloned().collect(),
            )
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            (
                "POST".to_string(),
                "/api/v1/admin/promotion-gate".to_string(),
                vec!["enabled".to_string()]
            ),
            (
                "POST".to_string(),
                "/api/v1/auth/login".to_string(),
                vec!["password".to_string(), "username".to_string()]
            ),
            // 一条拼出来的路径只读到字面量那一截：形状对不上任何注册过的路由，
            // 于是它在判据里落进「读不到」，而不是判成一条假的。
            (
                "POST".to_string(),
                "/api/v1/x".to_string(),
                vec!["name".to_string()]
            ),
        ],
        "{seen:?}"
    );
}

#[test]
fn a_key_the_handler_does_not_parse_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("dir");
    std::fs::create_dir_all(root.join("web/src")).expect("dir");
    std::fs::write(
        root.join("crates/cog-gateway/src/ide.rs"),
        "\
#[derive(Deserialize)]
pub struct RenameBody {
    pub from: String,
    pub to: String,
}

pub async fn rename(Json(body): Json<RenameBody>) -> String { body.from }
",
    )
    .expect("handler");
    std::fs::write(
        root.join("crates/cog-gateway/src/lib.rs"),
        "fn r() -> Router { Router::new().route(\"/api/v1/ide/rename\", post(rename)) }\n",
    )
    .expect("router");
    std::fs::write(
        root.join("web/src/client.ts"),
        "export const rename = (from: string, to: string) =>\n  \
         api.post('/api/v1/ide/rename', { from, to });\n",
    )
    .expect("client");

    let (complaints, census) = contradictions(root);
    assert_eq!(census.bodies_read, 1, "{census:?}");
    assert_eq!(census.keys_judged, 2, "{census:?}");
    assert!(complaints.is_empty(), "{complaints:?}");

    std::fs::write(
        root.join("crates/cog-gateway/src/ide.rs"),
        "\
#[derive(Deserialize)]
pub struct RenameBody {
    pub source: String,
    pub to: String,
}

pub async fn rename(Json(body): Json<RenameBody>) -> String { body.source }
",
    )
    .expect("handler");
    let (complaints, census) = contradictions(root);
    assert_eq!(census.bodies_read, 1, "{census:?}");
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("`from`"), "{complaints:?}");
    assert!(
        complaints[0].contains("web/src/client.ts"),
        "{complaints:?}"
    );
}

/// 路由表与动词表是同一份读取：`.route(` 的第二种拼写（第二个参数是一个
/// 组合子式子）必须读出处理函数名，否则这道门只会得到一片「读不到」。
#[test]
fn the_route_reader_names_the_handler_behind_each_verb() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("dir");
    std::fs::write(
        root.join("crates/cog-gateway/src/lib.rs"),
        "fn r() -> Router {\n    Router::new()\n        .route(\"/api/v1/a\", \
         get(list).post(create))\n        .route(\n            \"/api/v1/b\",\n            \
         delete(drop_it),\n        )\n}\n",
    )
    .expect("router");
    let routes = route_entries(root);
    assert_eq!(routes.len(), 2, "{routes:?}");
    assert_eq!(
        routes[0].verbs,
        BTreeSet::from(["get".to_string(), "post".to_string()])
    );
    assert_eq!(
        routes[0].handlers,
        BTreeSet::from(["list".to_string(), "create".to_string()])
    );
    assert_eq!(routes[1].handlers, BTreeSet::from(["drop_it".to_string()]));
    assert!(VERBS.contains(&"patch"));
}
