//! principle: feedback_no_hardcoding_config_surface
//!
//! A request is a path, a verb and a set of query names. The first two are
//! already paired by `web_client_routes_are_served_test`; this is the third, and
//! it is the one that fails quietly. A route with the wrong verb answers 405 and
//! a missing route answers 404 — both end up in the same "操作没成功" box, but at
//! least something answered. A query name the server does not read is answered
//! `200 OK` with the filter simply not applied: `?unreadOnly=true` against a
//! struct that spells it `unread_only` returns **every** row, and "all rows" is
//! indistinguishable from "no rows are unread" on a panel that renders a list.
//!
//! So the names are collected on both sides and compared.
//!
//! Client side, from `web/src` and the operator page embedded in the gateway
//! binary, three spellings of the same vocabulary:
//!
//! * `?name=` / `&name=` inside a string that starts with `/`, `?` or `&` (a
//!   route, a query suffix, or a fragment appended to one);
//! * keys of `new URLSearchParams({ name: … })`;
//! * `params.set('name', …)` / `.append('name', …)` on a `URLSearchParams`
//!   binding, counted only when that binding actually reaches a route literal —
//!   the same helper also builds Grafana links and provenance labels, and those
//!   names are not the gateway's to answer.
//!
//! Server side, from `crates/cog-gateway/src`:
//!
//! * every field of every struct used as `Query<T>`, with `#[serde(rename)]`
//!   and `#[serde(rename_all)]` applied, because the wire name is the serde
//!   name and not the Rust one;
//! * the names read by hand: `strip_prefix("name=")`, `query_params.get("name")`
//!   and `name.eq_ignore_ascii_case("…")`.
//!
//! 实测（2026-10-27）：客户端 23 个名字、网关 61 个可读名字，红的一栏为空。
//! 消融两条，各自点名后红、改回即绿：给 `crates/cog-gateway/src/projects.rs`
//! 的 `ExportQuery::view` 加一行 `#[serde(rename = "view_mode")]`，报
//! `` `view` ← web/src/api/client.ts ``；把 `client.ts` 的 `?view=` 写成
//! `?view_mode=`，同一个名字被反向点名。
//!
//! What it cannot judge:
//!
//! * names built from data rather than spelled: `listAuditEvents` loops over
//!   `Object.entries(query)` and calls `params.set(key, …)`, so `trigger_type`
//!   and `since` live in a TypeScript interface, not in a literal. A rename of
//!   those two still goes unnoticed;
//! * which endpoint reads a name. The comparison is name-level: a name accepted
//!   by some other route passes, and so does a rename masked by a second struct
//!   that happens to use the same wire name (there are two `SearchQuery`s, so
//!   renaming `q` in one of them stays green). Pairing a name to its route would
//!   need the client's request object, not just its literal;
//! * names the server reads that no client sends. Curl, the release scripts and
//!   the platform adapters are consumers too, so "unused" is not a defect;
//! * strings that start with `/` for another reason. A route-shaped literal that
//!   points somewhere else would be read as a gateway request and could redden
//!   on names the gateway never had — the direction that reports, not the
//!   direction that goes quiet.

mod common;

use common::routes::{read, walk};

use common::routes::repo_root;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Comments dropped, string literals kept. Both sides quote names in prose — a
/// doc comment saying "reads `?limit=`" is not a reader — and both sides spell
/// names inside strings, which are the only places that count.
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

/// The body of every string and template literal in the text.
fn string_bodies(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || c == '\'' || c == '`' {
            let mut body = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '\\' {
                    i += 1;
                    if i < chars.len() {
                        body.push(chars[i]);
                    }
                } else if chars[i] == c {
                    i += 1;
                    break;
                } else {
                    body.push(chars[i]);
                }
                i += 1;
            }
            out.push(body);
            continue;
        }
        i += 1;
    }
    out
}

fn ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `?name=` / `&name=` inside one string body. A bare `?flag` has no name the
/// server could read (a serde field of `bool` needs `=true` to deserialize), so
/// the `=` is required.
fn query_pairs(body: &str) -> Vec<String> {
    let chars: Vec<char> = body.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '?' && chars[i] != '&' {
            i += 1;
            continue;
        }
        i += 1;
        let start = i;
        while i < chars.len() && ident_char(chars[i]) {
            i += 1;
        }
        if i > start && chars.get(i) == Some(&'=') {
            out.push(chars[start..i].iter().collect());
        }
    }
    out
}

/// The identifier a `new URLSearchParams(…)` expression is assigned to.
fn binding_before(text: &str, at: usize) -> Option<String> {
    let head = text[..at].trim_end();
    let head = head.strip_suffix('=')?.trim_end();
    let start = head
        .char_indices()
        .rev()
        .find(|(_, c)| !ident_char(*c) && *c != '$')
        .map(|(i, _)| i)
        .map(|i| i + 1)
        .unwrap_or(0);
    let name = &head[start..];
    (!name.is_empty()).then(|| name.to_string())
}

/// The keys of an object literal starting at `{`, one identifier per item.
/// `open` is a byte index (the text has Chinese in it, so char and byte indices
/// differ).
fn object_keys(text: &str, open: usize) -> Vec<String> {
    let mut depth = 0usize;
    let mut end = None;
    for (index, c) in text.char_indices().skip_while(|(index, _)| *index < open) {
        match c {
            '{' => depth += 1,
            '}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    end = Some(index);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(end) = end else {
        return Vec::new();
    };
    let body = &text[open + 1..end];
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut item = String::new();
    for c in body.chars() {
        match c {
            '{' | '(' | '[' => depth += 1,
            '}' | ')' | ']' => depth -= 1,
            ',' if depth == 0 => {
                out.push(item.clone());
                item.clear();
                continue;
            }
            _ => {}
        }
        item.push(c);
    }
    out.push(item);
    out.into_iter()
        .filter_map(|item| {
            let item = item.trim();
            let name: String = item
                .chars()
                .take_while(|c| ident_char(*c) || *c == '$')
                .collect();
            (!name.is_empty()).then_some(name)
        })
        .collect()
}

/// One `URLSearchParams` binding: the name it is assigned to, every name it is
/// read back through (`query.toString()` stored in `suffix` is still `query`),
/// the stretch of text that is its own (up to the next binding), and the names
/// it sets.
struct Binding {
    var: String,
    aliases: Vec<String>,
    window: String,
    names: Vec<String>,
}

/// Identifiers that hold `<var>.toString()` — the tree usually spells the
/// request as `` `/api/v1/x${suffix}` `` rather than inlining the call.
fn aliases_of(text: &str, var: &str) -> Vec<String> {
    let needle = format!("{var}.toString()");
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(&needle) {
        let start = from + at;
        from = start + needle.len();
        if let Some(alias) = binding_before(text, start) {
            out.push(alias);
        }
    }
    out
}

/// Every `URLSearchParams` binding in one file.
fn search_params_of(text: &str) -> Vec<Binding> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find("new URLSearchParams(") {
        let start = from + at;
        let after = start + "new URLSearchParams(".len();
        from = after;
        let Some(var) = binding_before(text, start) else {
            continue;
        };
        let tail = &text[after..];
        let next = tail.find("new URLSearchParams(").unwrap_or(tail.len());
        let limit = tail.char_indices().nth(3000).map_or(tail.len(), |(i, _)| i);
        let window = tail[..next.min(limit)].to_string();

        let mut names = Vec::new();
        if let Some(open) = text[after..].find('{').map(|i| i + after) {
            // An object literal has to be the first thing inside the call.
            if text[after..open].trim().is_empty() {
                names = object_keys(text, open);
            }
        }
        if names.is_empty() {
            for op in [".set(", ".append("] {
                let needle = format!("{var}{op}");
                let mut cursor = 0;
                while let Some(hit) = window[cursor..].find(&needle) {
                    let arg = cursor + hit + needle.len();
                    cursor = arg;
                    let arg_text = window[arg..].trim_start();
                    let quote = match arg_text.chars().next() {
                        Some(q @ ('\'' | '"')) => q,
                        _ => continue,
                    };
                    let body = &arg_text[1..];
                    let end = body.find(quote).unwrap_or(0);
                    let name = &body[..end];
                    if !name.is_empty() && name.chars().all(ident_char) {
                        names.push(name.to_string());
                    }
                }
            }
        }
        let aliases = aliases_of(text, &var);
        out.push(Binding {
            var,
            aliases,
            window,
            names,
        });
    }
    out
}

/// Whether one binding reaches a route literal — `` `/api/v1/x${query}` ``.
/// The same helper builds `https://<grafana>/d/<uid>?from=…` and
/// `GET /api/v1/audit/events?…` labels, and neither is a gateway request, so
/// the question is asked of this binding's own stretch of text rather than of
/// the file: one file can hold a request helper and a dashboard link that both
/// call their params object `params`.
fn flows_into_a_route(window: &str, names: &[&str]) -> bool {
    names.iter().any(|name| {
        let marker = format!("${{{name}");
        string_bodies(window).iter().any(|body| {
            body.starts_with('/')
                && body.find(&marker).is_some_and(|at| {
                    !body[at + marker.len()..]
                        .chars()
                        .next()
                        .is_some_and(ident_char)
                })
        })
    })
}

/// Every query name the web surfaces send, with the files that send it.
fn client_query_names(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    const TAKEOVER: &str = "crates/cog-gateway/src/takeover.html";
    let mut files = Vec::new();
    walk(&root.join("web/src"), &["ts", "tsx"], &mut files);
    files.push(root.join(TAKEOVER));
    files.sort();

    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut record = |name: String, where_: &str| {
        out.entry(name).or_default().insert(where_.to_string());
    };
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text = strip_comments(&read(&path));
        for body in string_bodies(&text) {
            let starts_a_request = matches!(body.chars().next(), Some('/') | Some('?') | Some('&'));
            if !starts_a_request {
                continue;
            }
            for name in query_pairs(&body) {
                record(name, &rel);
            }
        }
        for binding in search_params_of(&text) {
            let mut names_it_is_read_through = vec![binding.var.as_str()];
            names_it_is_read_through.extend(binding.aliases.iter().map(String::as_str));
            if !flows_into_a_route(&binding.window, &names_it_is_read_through) {
                continue;
            }
            for name in binding.names {
                record(name, &format!("{rel}（{}）", binding.var));
            }
        }
    }
    out
}

/// `open` is a byte index into `text` (the gateway's sources are full of
/// Chinese, so a char index and a byte index are different numbers).
fn matching_brace(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for (index, c) in text.char_indices().skip_while(|(index, _)| *index < open) {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '"' | '\'' => quote = Some(c),
            '{' => depth += 1,
            '}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

/// The attribute lines immediately above a `struct` keyword: derive/serde/doc
/// lines, stopping at the first line that is none of those.
fn attribute_block(text: &str, struct_at: usize) -> String {
    // `struct_at` points at the keyword, which sits after `pub ` on its own
    // line; drop that partial line before walking upwards.
    let head = text[..struct_at]
        .rsplit_once('\n')
        .map_or("", |(head, _)| head);
    let mut lines: Vec<&str> = head.lines().collect();
    let mut kept = Vec::new();
    while let Some(line) = lines.pop() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if kept.is_empty() {
                continue;
            }
            break;
        }
        if trimmed.starts_with("#[") || trimmed.starts_with("///") || trimmed.starts_with("//!") {
            kept.push(trimmed);
            continue;
        }
        break;
    }
    kept.join("\n")
}

fn rename_all(style: &str, field: &str) -> String {
    let words: Vec<&str> = field.split('_').collect();
    match style {
        "camelCase" => {
            let mut out = words[0].to_string();
            for word in &words[1..] {
                let mut chars = word.chars();
                if let Some(first) = chars.next() {
                    out.push(first.to_ascii_uppercase());
                    out.push_str(chars.as_str());
                }
            }
            out
        }
        "PascalCase" => words
            .iter()
            .map(|word| {
                let mut chars = word.chars();
                match chars.next() {
                    Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                    None => String::new(),
                }
            })
            .collect(),
        "kebab-case" => field.replace('_', "-"),
        "SCREAMING_SNAKE_CASE" => field.to_ascii_uppercase(),
        _ => field.to_string(),
    }
}

/// The wire names of one `Query<T>` struct's fields.
fn struct_fields(text: &str, name: &str) -> Option<Vec<String>> {
    let needle = format!("struct {name}");
    let mut from = 0;
    let at = loop {
        let hit = text[from..].find(&needle)? + from;
        let after = text[hit + needle.len()..].chars().next();
        if after.is_none_or(|c| !ident_char(c)) {
            break hit;
        }
        from = hit + needle.len();
    };
    let open = text[at..].find('{').map(|i| i + at)?;
    let close = matching_brace(text, open)?;
    let body = &text[open + 1..close];
    let attrs = attribute_block(text, at);
    let style = attrs
        .split_once("rename_all")
        .and_then(|(_, rest)| rest.split_once('"'))
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(value, _)| value.to_string());

    let mut out = Vec::new();
    let mut pending: Option<String> = None;
    let mut skipped = false;
    for line in body.lines() {
        let line = line.trim();
        if line.starts_with("#[serde(") {
            if line.contains("skip") {
                skipped = true;
            }
            if let Some((_, rest)) = line.split_once("rename") {
                if let Some((_, rest)) = rest.split_once('"') {
                    if let Some((wire, _)) = rest.split_once('"') {
                        pending = Some(wire.to_string());
                    }
                }
            }
            continue;
        }
        if line.is_empty() || line.starts_with("#[") {
            continue;
        }
        let line = line.strip_prefix("pub(").map_or(line, |rest| {
            rest.split_once(')')
                .map(|(_, tail)| tail.trim_start())
                .unwrap_or(rest)
        });
        let line = line.strip_prefix("pub ").unwrap_or(line);
        let field: String = line.chars().take_while(|c| ident_char(*c)).collect();
        if field.is_empty() || !line[field.len()..].trim_start().starts_with(':') {
            continue;
        }
        if !skipped {
            out.push(pending.take().unwrap_or_else(|| match &style {
                Some(style) => rename_all(style, &field),
                None => field.clone(),
            }));
        }
        pending = None;
        skipped = false;
    }
    Some(out)
}

/// Every name the gateway can read out of a query string, with where it is read.
fn accepted_names(root: &Path) -> BTreeMap<String, String> {
    let mut files = Vec::new();
    walk(&root.join("crates/cog-gateway/src"), &["rs"], &mut files);
    files.sort();
    let texts: Vec<(String, String)> = files
        .iter()
        .map(|path| {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            (rel, strip_comments(&read(path)))
        })
        .collect();

    let mut out: BTreeMap<String, String> = BTreeMap::new();
    let mut insert = |name: String, where_: String| {
        out.entry(name).or_insert(where_);
    };

    // Structs the router deserializes a query string into.
    let mut query_structs = BTreeSet::new();
    for (_, text) in &texts {
        let mut from = 0;
        while let Some(at) = text[from..].find("Query<") {
            let start = from + at + "Query<".len();
            from = start;
            let name: String = text[start..]
                .trim_start()
                .chars()
                .take_while(|c| ident_char(*c))
                .collect();
            if !name.is_empty() {
                query_structs.insert(name);
            }
        }
    }
    for name in &query_structs {
        for (rel, text) in &texts {
            if let Some(fields) = struct_fields(text, name) {
                for field in fields {
                    insert(field, format!("{rel}::{name}"));
                }
            }
        }
    }

    // Names read straight out of the query string.
    for (rel, text) in &texts {
        for needle in ["strip_prefix(\"", "query_params.get(\"", "params.get(\""] {
            let mut from = 0;
            while let Some(at) = text[from..].find(needle) {
                let start = from + at + needle.len();
                from = start;
                let body: String = text[start..].chars().take_while(|c| *c != '"').collect();
                let body = body.strip_suffix('=').unwrap_or(&body);
                if !body.is_empty() && body.chars().all(|c| ident_char(c) || c == '-') {
                    insert(body.to_string(), format!("{rel}（手写解析）"));
                }
            }
        }
        for receiver in ["name", "key"] {
            let needle = format!("{receiver}.eq_ignore_ascii_case(\"");
            let mut from = 0;
            while let Some(at) = text[from..].find(&needle) {
                let start = from + at + needle.len();
                from = start;
                let body: String = text[start..].chars().take_while(|c| *c != '"').collect();
                if !body.is_empty() && body.chars().all(|c| ident_char(c) || c == '-') {
                    insert(body, format!("{rel}（大小写无关的比较）"));
                }
            }
        }
    }
    out
}

fn disagreements(root: &Path) -> Vec<String> {
    let client = client_query_names(root);
    let accepted = accepted_names(root);
    let mut out = Vec::new();
    for (name, where_) in &client {
        if accepted.contains_key(name) {
            continue;
        }
        let sites: Vec<&String> = where_.iter().collect();
        out.push(format!(
            "`{name}` ← {}（网关这一侧既没有 `Query<T>` 的字段读它，也没有手写解析读它）",
            sites
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("、")
        ));
    }
    out
}

#[test]
fn every_query_name_the_client_sends_is_one_the_gateway_reads() {
    let root = repo_root();

    // 抽取面自检：两侧都读到东西，判据才不是在空集上假装绿。
    let client = client_query_names(&root);
    let accepted = accepted_names(&root);
    assert!(
        client.len() >= 15,
        "客户端只读到 {} 个查询参数名，扫描器本身坏了",
        client.len()
    );
    assert!(
        accepted.len() >= 40,
        "网关只读到 {} 个可读的查询参数名，扫描器本身坏了",
        accepted.len()
    );
    for known in ["limit", "path", "q", "token", "agent_id", "unread_only"] {
        assert!(
            client.contains_key(known),
            "读不到客户端发送的 `{known}`：解析坏了或它真的不再发送"
        );
        assert!(
            accepted.contains_key(known),
            "读不到网关读取的 `{known}`：解析坏了或它真的不再读取"
        );
    }

    let found = disagreements(&root);
    assert!(
        found.is_empty(),
        "网页端发出的查询参数，网关这一侧没有人在读：\n{}\n\
         这种漂移不报错——服务端 200 返回，过滤条件没生效，\
         而「全部行」与「一条都没命中」在面板上长得一模一样",
        found.join("\n")
    );
}

/// 判据自己也要有判据：一棵小树里两边对齐时判空，客户端改一个字母、
/// 网关改一个字母，各自都要点名。
#[test]
fn a_query_name_the_gateway_does_not_read_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("web/src/api")).expect("web dir");
    std::fs::create_dir_all(root.join("crates/cog-gateway/src/auth")).expect("gateway dir");
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("gateway root");
    let client = root.join("web/src/api/client.ts");
    let server = root.join("crates/cog-gateway/src/things.rs");
    std::fs::write(root.join("crates/cog-gateway/src/takeover.html"), "").expect("takeover");

    std::fs::write(
        &client,
        "export async function listThings(id: string, limit = 20) {\n\
           return api.get(`/api/v1/things?widget_id=${id}&limit=${limit}`);\n\
         }\n\
         export async function paged(page: number) {\n\
           const query = new URLSearchParams({ pageSize: String(page) });\n\
           return api.get(`/api/v1/things/page?${query}`);\n\
         }\n\
         export async function viaHelper(id: string) {\n\
           const params = new URLSearchParams();\n\
           params.set('widget_id', id);\n\
           return api.get(`/api/v1/things/list${params ? `?${params}` : ''}`);\n\
         }\n\
         export function grafanaLink(uid: string, from: string) {\n\
           const params = new URLSearchParams();\n\
           params.set('from', from);\n\
           return `${grafana}/d/${uid}?${params}`;\n\
         }\n",
    )
    .expect("client");
    std::fs::write(
        &server,
        "#[derive(Deserialize)]\n\
         pub struct ThingsQuery {\n\
             #[serde(rename = \"widget_id\")]\n\
             pub widget: Option<String>,\n\
             pub limit: Option<usize>,\n\
         }\n\
         #[derive(Deserialize)]\n\
         #[serde(rename_all = \"camelCase\")]\n\
         pub struct PageQuery {\n\
             pub page_size: Option<usize>,\n\
         }\n\
         async fn list(Query(q): Query<ThingsQuery>) -> String { q.widget.unwrap_or_default() }\n\
         async fn page(Query(q): Query<PageQuery>) -> String { format!(\"{:?}\", q.page_size) }\n\
         fn mirror(req: Req) -> Option<&str> {\n\
             req.query.and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix(\"service=\")))\n\
         }\n",
    )
    .expect("server");

    // `widget_id`、`limit`、`page_size` 有字段读；`from` 只流向 Grafana 链接，不算。
    assert_eq!(disagreements(root), Vec::<String>::new());

    // 网关把 `widget_id` 改成 `widget`（Rust 名）却没改 serde 名：客户端那一侧没人读。
    std::fs::write(
        &server,
        "#[derive(Deserialize)]\n\
         pub struct ThingsQuery {\n\
             pub widget_id: Option<String>,\n\
             pub limit: Option<usize>,\n\
         }\n\
         #[derive(Deserialize)]\n\
         #[serde(rename_all = \"camelCase\")]\n\
         pub struct PageQuery {\n\
             pub page_size: Option<usize>,\n\
         }\n\
         async fn list(Query(q): Query<ThingsQuery>) -> String { q.widget_id.unwrap_or_default() }\n\
         async fn page(Query(q): Query<PageQuery>) -> String { format!(\"{:?}\", q.page_size) }\n",
    )
    .expect("server");
    assert_eq!(
        disagreements(root),
        Vec::<String>::new(),
        "改名后两侧仍然一致，不该红"
    );

    // 客户端写驼峰的 `widgetId`：`rename_all` 只管 `page_size` 那个结构体。
    std::fs::write(
        &client,
        "export async function listThings(id: string, limit = 20) {\n\
           return api.get(`/api/v1/things?widgetId=${id}&limit=${limit}`);\n\
         }\n",
    )
    .expect("client");
    let found = disagreements(root);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("widgetId"), "{found:?}");
    assert!(found[0].contains("client.ts"), "{found:?}");
}

/// 解析面自己也要有判据，否则「一个名字都没读到」与「没有人发查询参数」不可分。
#[test]
fn the_readers_see_the_shapes_this_tree_writes() {
    let js = "\
        const a = `/api/v1/x?hours=${h}`;\n\
        const b = hours === undefined ? '' : `?hours=${hours}`;\n\
        const c = source === 'workspace' ? '' : `&source=${source}`;\n\
        const d = 'https://github.com/settings/apps/new?manifest=' + m;\n\
        const e = 'GET /api/v1/audit/events?goal_id=' + id;\n\
        // const f = `/api/v1/x?commented=1`;\n\
        const query = new URLSearchParams({ kind: k, scope: s, limit: String(n) });\n\
        api.get(`/api/v1/changes/events?${query}`);\n\
        const params = new URLSearchParams();\n\
        params.set('from', window.from);\n\
        return `${grafana}/d/${uid}?${params}`;\n";
    let stripped = strip_comments(js);
    let mut names = Vec::new();
    for body in string_bodies(&stripped) {
        if matches!(body.chars().next(), Some('/') | Some('?') | Some('&')) {
            names.extend(query_pairs(&body));
        }
    }
    assert_eq!(names, vec!["hours", "hours", "source"], "{names:?}");

    let bindings = search_params_of(&stripped);
    assert_eq!(
        bindings.len(),
        2,
        "{:?}",
        bindings.iter().map(|b| &b.var).collect::<Vec<_>>()
    );
    assert_eq!(bindings[0].var, "query");
    assert_eq!(
        bindings[0].names,
        vec!["kind".to_string(), "scope".to_string(), "limit".to_string()]
    );
    assert_eq!(bindings[1].var, "params");
    assert_eq!(bindings[1].names, vec!["from".to_string()]);
    // 模板串里的绑定算走到路由；同一份文本里那个拼 Grafana 链接的没走到。
    assert!(flows_into_a_route(&bindings[0].window, &["query"]));
    assert!(!flows_into_a_route(&bindings[1].window, &["params"]));

    let rust = "\
        #[derive(Deserialize)]\n\
        #[serde(rename_all = \"camelCase\")]\n\
        pub struct PageQuery {\n\
            /// 每页几条。\n\
            pub page_size: Option<usize>,\n\
            #[serde(rename = \"q\")]\n\
            pub text: Option<String>,\n\
            #[serde(skip)]\n\
            pub internal: usize,\n\
        }\n\
        fn hand(q: &str) -> Option<&str> {\n\
            q.split('&').find_map(|kv| kv.strip_prefix(\"service=\"))\n\
        }\n";
    let fields = struct_fields(rust, "PageQuery").expect("struct");
    assert_eq!(fields, vec!["pageSize", "q"], "{fields:?}");
}
