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
//! Two clients ship and both are read: `web/src` (the WebUI) and the operator
//! page embedded in the gateway binary, which calls the same router from
//! JavaScript annotated nowhere.
//!
//! 实测（2026-10-27）：客户端路径形状全部有服务端路由（WebUI 82 条，内嵌运维页
//! 另 35 条）；网关一侧读到 224 条 `.route(...)`。消融对照两条，各自点名对应文件
//! 后红、改回即绿：`web/src/api/client.ts` 的 `/api/v1/processes` 改成
//! `/api/v1/host-processes`；`crates/cog-gateway/src/takeover.html` 的
//! `/api/v1/evolution/rollback` 改成 `/api/v1/evolution/rolled-back`。
//!
//! The verb is the other half of the same request and used to go unpaired: a path
//! that exists answers a *set* of methods, and a client asking with one that is
//! not in the set gets a 405. That is not a 404 — the client's error path turns
//! both into "操作没成功", so a wrong verb reads exactly like a validation
//! failure, and the panel never says "this route has no such action". The verbs
//! are read from the call the literal is an argument of (`api.get`, `api.post`,
//! the operator page's `api(path, 'POST')` helper, a bare `fetch` with a
//! `method:` option, or a helper that takes the verb first) and from the
//! combinators inside `.route(path, get(..).post(..))`.
//!
//! 实测（2026-10-27）：客户端 113 个「动词 + 路径」请求，网关 233 条路由形状，
//! 两侧动词全部相符，红的一栏为空。两个方向都做过消融，各自点名后红、改回即绿：
//! 把 `web/src/api/client.ts` 里 `listAgents` 的 `api.get` 改成 `api.post`，
//! 报 `POST /api/v1/agents ← web/src/api/client.ts（这条路由只答 get）`；把
//! `crates/cog-gateway/src/lib.rs` 里备份包那条路由的 `.delete(...)` 去掉，报
//! `DELETE /api/v1/admin/backup/packages/{} ← web/src/api/client.ts（只答 get）`。

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

/// The paths the shipped clients ask for, keyed by shape, with the files that
/// ask. Two clients ship in the image: the WebUI under `web/src`, and the
/// operator page embedded in the gateway binary -- both call the same router,
/// and neither is compiled against it.
fn client_paths(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut files = Vec::new();
    walk(&root.join("web/src"), &["ts", "tsx"], &mut files);
    walk(&root.join("crates/cog-gateway/src"), &["html"], &mut files);
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
            // declaration. Only a bare or module-qualified name is read; a call
            // of any other shape contributes no route, which shows up as a
            // client path with nothing serving it rather than as a quiet pass.
            let qualified: String = argument
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            let name = qualified.rsplit("::").next().unwrap_or(qualified.as_str());
            if let Some(value) = constants.get(name) {
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

/// The text with `//` and `/* */` comments removed, leaving string and template
/// literals untouched.
///
/// The path census can read comments: a path quoted in prose is still a path the
/// gateway serves, so it costs nothing. The method census cannot — a path named
/// in a comment has no verb in front of it, and the nearest verb in the file
/// would be borrowed from whatever call happens to be nearby.
fn strip_comments(text: &str) -> String {
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

/// The last quoted all-caps word in a stretch of text — how a helper that takes
/// the verb as its first argument (`act('POST', path)`) spells it.
fn last_quoted_method(text: &str) -> Option<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut found = None;
    let mut i = 0;
    while i < chars.len() {
        let quote = chars[i];
        if quote == '\'' || quote == '"' {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && chars[end] != quote {
                end += 1;
            }
            let word: String = chars[start..end].iter().collect();
            if (3..=7).contains(&word.len()) && word.chars().all(|c| c.is_ascii_uppercase()) {
                found = Some(word);
            }
            i = end;
        }
        i += 1;
    }
    found
}

/// The quoted verb in the first argument after a path literal, or after a
/// `{ method: ... }` option.
fn quoted_method(text: &str) -> Option<String> {
    // The call's own arguments, not the rest of the file: an unbounded search
    // for `method:` finds the next request's option and hands this one a verb
    // that belongs to a different call.
    let end = text
        .char_indices()
        .nth(120)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    let text = &text[..end];
    let rest = text.trim_start_matches(|c: char| c == ',' || c.is_whitespace());
    for quote in ['\'', '"'] {
        if let Some(tail) = rest.strip_prefix(quote) {
            let word: String = tail
                .chars()
                .take_while(|c| c.is_ascii_uppercase())
                .collect();
            if (3..=7).contains(&word.len()) {
                return Some(word);
            }
        }
    }
    let at = text.find("method:")?;
    let tail = text[at + "method:".len()..].trim_start();
    tail.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let word: String = tail[1..]
        .chars()
        .take_while(|c| c.is_ascii_uppercase())
        .collect();
    (3..=7).contains(&word.len()).then_some(word)
}

/// Every request the shipped clients make, as `(method, path shape)`.
///
/// The verb is read from the call the literal is an argument of: `api.get`,
/// `api.post` and friends carry it in the name; `api.sendBytes`, `api.request`,
/// the operator page's `api(...)` helper and a bare `fetch` carry it in an
/// argument, defaulting to GET. A literal with no verb in sight is left out
/// rather than guessed — guessing GET is how a POST would sail through.
fn client_requests(root: &Path) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut files = Vec::new();
    walk(&root.join("web/src"), &["ts", "tsx"], &mut files);
    walk(&root.join("crates/cog-gateway/src"), &["html"], &mut files);
    files.sort();
    let mut found: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text = strip_comments(&read(&path));
        let mut from = 0;
        while let Some(at) = text[from..].find("/api/") {
            let start = from + at;
            from = start + 1;
            let before = text[..start].chars().next_back().unwrap_or(' ');
            if before != '"' && before != '\'' && before != '`' {
                continue;
            }
            let literal: String = text[start..]
                .chars()
                .take_while(|c| *c != '"' && *c != '\'' && *c != '`' && *c != '\n')
                .collect();
            let close = start + literal.len();
            let window_start = text[..start]
                .char_indices()
                .rev()
                .nth(300)
                .map(|(i, _)| i)
                .unwrap_or(0);
            let window = &text[window_start..start];
            let marker = window.rfind("api.").map(|i| (i, "api")).or_else(|| {
                ["fetch(", "EventSource(", "api("]
                    .iter()
                    .filter_map(|needle| window.rfind(needle).map(|i| (i, *needle)))
                    .max_by_key(|(i, _)| *i)
            });
            let after = &text[close + 1..];
            let method = match marker {
                Some((i, "api")) => {
                    let name: String = window[i + 4..]
                        .chars()
                        .take_while(|c| c.is_ascii_alphabetic())
                        .collect();
                    match name.as_str() {
                        "get" | "getBytes" => Some("GET".to_string()),
                        "post" | "put" | "delete" | "patch" => Some(name.to_uppercase()),
                        "sendBytes" | "request" => quoted_method(after),
                        _ => None,
                    }
                }
                Some((_, "EventSource(")) => None,
                Some(_) => Some(quoted_method(after).unwrap_or_else(|| "GET".to_string())),
                // A helper that takes the verb first (`act('POST', path)`) puts
                // it before the literal rather than after.
                None => last_quoted_method(window),
            };
            let Some(method) = method else {
                continue;
            };
            found
                .entry((method, normalize(&literal)))
                .or_default()
                .insert(rel.clone());
        }
    }
    found
}

/// The verbs each registered path answers.
fn served_methods(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    const VERBS: &[&str] = &[
        "get", "post", "put", "patch", "delete", "head", "options", "any",
    ];
    let constants = core_path_constants(root);
    let mut files = Vec::new();
    walk(&root.join("crates/cog-gateway/src"), &["rs"], &mut files);
    files.sort();
    let mut served: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let text = read(&path);
        let mut cursor = 0;
        while let Some(at) = text[cursor..].find(".route(") {
            let call = cursor + at;
            cursor = call + ".route(".len();
            let argument = text[cursor..].trim_start();
            let shape = if let Some(literal) = argument.strip_prefix('"') {
                literal.find('"').map(|end| normalize(&literal[..end]))
            } else {
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
            // The handler expression runs from the opening paren of `.route(` to
            // its closing one; the verbs are whichever combinators appear in it.
            let open = cursor - 1;
            let Some(close) = matching_paren(text.as_bytes(), open) else {
                continue;
            };
            let expression = &text[open + 1..close];
            let verbs = served.entry(shape).or_default();
            for verb in VERBS {
                let needle = format!("{verb}(");
                let mut from = 0;
                while let Some(found_at) = expression[from..].find(&needle) {
                    let at = from + found_at;
                    from = at + needle.len();
                    let boundary = expression[..at]
                        .chars()
                        .next_back()
                        .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
                    if boundary {
                        verbs.insert((*verb).to_string());
                    }
                }
            }
        }
    }
    served
}

/// The offset of the paren closing the one at `open`.
fn matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
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

/// Every client request whose verb the route does not answer.
fn method_disagreements(
    requests: &BTreeMap<(String, String), BTreeSet<String>>,
    served: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    let mut out = Vec::new();
    for ((method, shape), files) in requests {
        let Some(verbs) = served.get(shape) else {
            continue;
        };
        if verbs.is_empty() || verbs.contains("any") || verbs.contains(&method.to_lowercase()) {
            continue;
        }
        out.push(format!(
            "  {method} {shape}  ←  {}（这条路由只答 {}）",
            files.iter().cloned().collect::<Vec<_>>().join(", "),
            verbs.iter().cloned().collect::<Vec<_>>().join("/")
        ));
    }
    out
}

#[test]
fn every_method_the_client_uses_is_one_its_route_answers() {
    let root = repo_root();
    let requests = client_requests(&root);
    let served = served_methods(&root);
    assert!(
        requests.len() > 80,
        "只读出 {} 个 (动词, 路径) 请求，这条判据没有题目",
        requests.len()
    );
    let found = method_disagreements(&requests, &served);
    assert!(
        found.is_empty(),
        "WebUI 用这些动词请求，网关的路由不答这个动词——它拿到的是一次 405，\
         客户端把非 2xx 一律当成一次失败，面板上读作「操作没成功」，\
         而真正的差别（这条路根本没有这个动作）不在任何读数里：\n{}",
        found.join("\n")
    );
}

/// 解析面自己也要有判据：四种调用形状都要认出动词，注释里的路径不能借走
/// 邻近调用的动词。
#[test]
fn the_method_reader_sees_the_call_shapes_this_tree_writes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("web/src")).expect("client dir");
    std::fs::write(
        root.join("web/src/calls.ts"),
        "// 名单、对话都走 `/api/v1/agents`\n\
         export const a = () => api.get<T>('/api/v1/agents');\n\
         export const b = (x: string) => api.post<T>('/api/v1/agents/x', x);\n\
         export const c = () => api.delete(`/api/v1/agents/${id}`);\n\
         export const d = () => api.sendBytes<T>(`/api/v1/ide/upload?p=${p}`, 'POST', blob);\n\
         export const e = () => api.request<T>('/api/v1/x', { method: 'PUT' });\n\
         export const f = () => fetch('/api/v1/y', { method: 'DELETE' });\n",
    )
    .expect("client");
    let requests = client_requests(root);
    let methods: Vec<(&String, &String)> = requests.keys().map(|(m, p)| (m, p)).collect();
    assert_eq!(
        methods,
        vec![
            (&"DELETE".to_string(), &"/api/v1/agents/{}".to_string()),
            (&"DELETE".to_string(), &"/api/v1/y".to_string()),
            (&"GET".to_string(), &"/api/v1/agents".to_string()),
            (&"POST".to_string(), &"/api/v1/agents/x".to_string()),
            (&"POST".to_string(), &"/api/v1/ide/upload".to_string()),
            (&"PUT".to_string(), &"/api/v1/x".to_string()),
        ],
        "{requests:?}"
    );
}

/// 判据自己也要有判据：一条答 GET 的路由，客户端拿 POST 去问必须点名。
#[test]
fn a_method_a_route_does_not_answer_is_reported() {
    let served: BTreeMap<String, BTreeSet<String>> = [(
        "/api/v1/agents".to_string(),
        BTreeSet::from(["get".to_string()]),
    )]
    .into_iter()
    .collect();
    let mut requests: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    requests.insert(
        ("GET".to_string(), "/api/v1/agents".to_string()),
        BTreeSet::from(["a.ts".to_string()]),
    );
    requests.insert(
        ("POST".to_string(), "/api/v1/agents".to_string()),
        BTreeSet::from(["b.ts".to_string()]),
    );
    requests.insert(
        ("POST".to_string(), "/api/v1/ghosts".to_string()),
        BTreeSet::from(["c.ts".to_string()]),
    );
    let found = method_disagreements(&requests, &served);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("POST /api/v1/agents"), "{found:?}");
    assert!(found[0].contains("b.ts"), "{found:?}");
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
