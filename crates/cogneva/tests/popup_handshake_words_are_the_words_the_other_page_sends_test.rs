//! principle: feedback_no_hardcoding_config_surface
//!
//! 登录与授权是三个页面之间的一次握手，而握手的每个词都在两端各拼一遍：落地页把
//! 结果 `postMessage` 回打开它的窗口，运维页与 WebUI 各有一个 `message` 监听器拿
//! `type` 认这条消息。任何一处把词改掉，这一段就走进 `return`：弹窗自己关掉，
//! 向导那一头原地等——没有报错、没有日志，读数上和「用户还没授权」一模一样。
//! `state` 前缀（`ghapp:`）同理：它决定落地页把回调认成哪一种消息，写的一方与读的
//! 一方也各拼一遍，而拼错的结果是**另一种**消息被发出去，同样谁都不报错。
//!
//! 本门把这几条消息的每个词、消息体里的每个键、那个前缀当作一份词表，逐向验：
//!
//! * 发出去的类型词必须有人收，收的类型词必须有人发——单边多出一个词，就是一条
//!   永远不成立的分支；
//! * 每个词都必须在表里登记：新加一种消息而没登记，红在「没登记」上；
//! * 登记的每一端（发送点、接收点、payload 键）那段字节必须还在——改名而另一头
//!   没跟，红在登记行上；
//! * `state` 前缀两边解析出来的字面量必须相等；
//! * 登录 payload 里的 `auth.access_token` 这些键由 `AuthResponse` 的字段名序列化
//!   而来，所以直接读结构体：字段名对不上，或者给它挂上 serde 改名，都红。
//!
//! 判不了什么：不判 origin 校验（接收端有意不按 origin 过滤，防伪靠一次性 state），
//! 也不判收到之后怎么处理；`postMessage` 的类型既不是字面量、也不是能解析出三支的
//! 变量时按读不到处理，直接红；回调落地页的 URL（运维页里的 `GH_APP_LANDING` 与
//! Helm values 里那份默认值）属于部署配置面，不在本门。
//!
//! 实测（2026-10-27）：客户端面 2 处 `postMessage` 发出 3 种消息（登录落地页 1 种、
//! `docs/oauth-callback.html` 2 种），5 处 `message` 监听器收这 3 种（运维页 3、
//! WebUI 2），两向都空；`state` 前缀两边都是 `ghapp:`；消息体的键 7 个（`type`、
//! `data`、`code`、`state`、`auth`、`access_token`、`refresh_token`、`user`）。
//! 消融：把落地页三元里的 `'cogneva-app-manifest'` 改成 `'cogneva-app-manifest-v2'`，
//! 本门红，一边点名登记的那条消息再没人发、一边点名这个新词不在表里；改回即绿。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::js_literal::{is_identifier, split_top_level};
use common::routes::{matching_brace, matching_paren, read, repo_root, walk};
use common::rust_production::{production_text, relative_name};

const OPERATOR: &str = "crates/cog-gateway/src/takeover.html";
const CALLBACK: &str = "docs/oauth-callback.html";
const LOGIN_PAGE: &str = "crates/cog-gateway/src/auth/platform.rs";
const AUTH_RESPONSE: &str = "crates/cog-gateway/src/auth/mod.rs";
const WEB_AUTH: &str = "web/src/hooks/useAuth.ts";
const WEB_OAUTH: &str = "web/src/overlay/AuthOverlay.tsx";

/// 一条消息：寄出去的那个词、两端各自的拼写、消息体里的键。
struct Message {
    word: &'static str,
    senders: &'static [Site],
    receivers: &'static [Site],
    wires: &'static [Wire],
}

/// 一处拼写：哪个文件里那段必须还在的字节。
type Site = (&'static str, &'static str);

struct Wire {
    key: &'static str,
    sites: &'static [Site],
}

/// 用 `state` 前缀分辨回调类型的那一条握手：谁写、谁读。
struct Prefix {
    value: &'static str,
    writer: &'static str,
    reader: &'static str,
}

const LOGIN_POST: Site = (
    LOGIN_PAGE,
    "postMessage({{ type: 'cogneva-login', data: d }}",
);
const CALLBACK_POST: Site = (
    CALLBACK,
    "postMessage({ type: msgType, code: code, state: state }",
);

const MESSAGES: &[Message] = &[
    Message {
        word: "cogneva-login",
        senders: &[LOGIN_POST],
        receivers: &[
            (OPERATOR, "m.type !== 'cogneva-login'"),
            (WEB_AUTH, "data.type !== 'cogneva-login'"),
        ],
        wires: &[
            Wire {
                key: "type",
                sites: &[
                    LOGIN_POST,
                    (OPERATOR, "m.type !== 'cogneva-login'"),
                    (WEB_AUTH, "data.type !== 'cogneva-login'"),
                ],
            },
            Wire {
                key: "data",
                sites: &[
                    LOGIN_POST,
                    (OPERATOR, "adoptPlatformAuth(m.data)"),
                    (WEB_AUTH, "const token = data.data?.auth?.access_token;"),
                ],
            },
            Wire {
                key: "auth",
                sites: &[
                    (LOGIN_PAGE, "\"auth\": AuthResponse {"),
                    (WEB_AUTH, "const token = data.data?.auth?.access_token;"),
                ],
            },
            Wire {
                key: "access_token",
                sites: &[
                    (AUTH_RESPONSE, "pub access_token: String,"),
                    (WEB_AUTH, "const token = data.data?.auth?.access_token;"),
                ],
            },
            Wire {
                key: "refresh_token",
                sites: &[
                    (AUTH_RESPONSE, "pub refresh_token: String,"),
                    (
                        WEB_AUTH,
                        "refreshToken: data.data?.auth?.refresh_token ?? null",
                    ),
                ],
            },
            Wire {
                key: "user",
                sites: &[
                    (AUTH_RESPONSE, "pub user: UserResponse,"),
                    (WEB_AUTH, "user = data.data?.auth?.user"),
                ],
            },
        ],
    },
    Message {
        word: "cogneva-gitee-oauth",
        senders: &[CALLBACK_POST],
        receivers: &[
            (OPERATOR, "d.type !== 'cogneva-gitee-oauth'"),
            (WEB_OAUTH, "data.type !== 'cogneva-gitee-oauth'"),
        ],
        wires: &[
            Wire {
                key: "type",
                sites: &[
                    CALLBACK_POST,
                    (OPERATOR, "d.type !== 'cogneva-gitee-oauth'"),
                    (WEB_OAUTH, "data.type !== 'cogneva-gitee-oauth'"),
                ],
            },
            Wire {
                key: "code",
                sites: &[
                    CALLBACK_POST,
                    (
                        OPERATOR,
                        "if (!d || d.type !== 'cogneva-gitee-oauth' || !d.code) return;",
                    ),
                    (
                        WEB_OAUTH,
                        "if (!data.code || data.state !== pending.state) return;",
                    ),
                ],
            },
            Wire {
                key: "state",
                sites: &[
                    CALLBACK_POST,
                    (
                        OPERATOR,
                        "if (!giOAuthState || d.state !== giOAuthState) return;",
                    ),
                    (
                        WEB_OAUTH,
                        "if (!data.code || data.state !== pending.state) return;",
                    ),
                ],
            },
        ],
    },
    Message {
        word: "cogneva-app-manifest",
        senders: &[CALLBACK_POST],
        receivers: &[(OPERATOR, "d.type !== 'cogneva-app-manifest'")],
        wires: &[
            Wire {
                key: "type",
                sites: &[
                    CALLBACK_POST,
                    (OPERATOR, "d.type !== 'cogneva-app-manifest'"),
                ],
            },
            Wire {
                key: "code",
                sites: &[
                    CALLBACK_POST,
                    (
                        OPERATOR,
                        "if (!d || d.type !== 'cogneva-app-manifest' || !d.code) return;",
                    ),
                ],
            },
            Wire {
                key: "state",
                sites: &[
                    CALLBACK_POST,
                    (
                        OPERATOR,
                        "if (!ghAppState || d.state !== ghAppState) return;",
                    ),
                ],
            },
        ],
    },
];

const PREFIX: Prefix = Prefix {
    value: "ghapp:",
    writer: OPERATOR,
    reader: CALLBACK,
};

/// 紧跟着的一个带引号单词；模板串带插值时读不了，按没有处理。
fn quoted(piece: &str) -> Option<String> {
    let piece = piece.trim_start();
    let quote = piece.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let mut word = String::new();
    let mut escaped = false;
    for c in piece[1..].chars() {
        if escaped {
            escaped = false;
            word.push(c);
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == quote {
            return Some(word);
        }
        word.push(c);
    }
    None
}

/// 对象字面量里 `<key>:` 的值，切到下一个顶层逗号（或收尾的括号）。
fn value_of_key<'a>(object: &'a str, key: &str) -> Option<&'a str> {
    let bytes = object.as_bytes();
    let mut cursor = 0;
    while let Some(offset) = object[cursor..].find(key) {
        let at = cursor + offset;
        cursor = at + key.len();
        let boundary = at == 0 || {
            let c = bytes[at - 1];
            !(c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'$')
        };
        if !boundary {
            continue;
        }
        let mut i = cursor;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            continue;
        }
        i += 1;
        let start = i;
        let mut depth = 0i32;
        while i < bytes.len() {
            match bytes[i] {
                b'\'' | b'"' | b'`' => {
                    let quote = bytes[i];
                    i += 1;
                    while i < bytes.len() && bytes[i] != quote {
                        if bytes[i] == b'\\' {
                            i += 1;
                        }
                        i += 1;
                    }
                    if i >= bytes.len() {
                        return None;
                    }
                }
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                b',' if depth == 0 => break,
                _ => {}
            }
            i += 1;
        }
        return Some(&object[start..i]);
    }
    None
}

/// 三元表达式两支上写的字面量。`?`／`:` 判断的是紧挨着的分隔符，所以
/// `indexOf('ghapp:')` 里那个前缀不会被当成一个可能的消息类型。
fn ternary_branches(expression: &str) -> BTreeSet<String> {
    let bytes = expression.as_bytes();
    let mut words = BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        let quote = bytes[i];
        if quote != b'\'' && quote != b'"' {
            i += 1;
            continue;
        }
        let start = i;
        let mut j = i + 1;
        while j < bytes.len() && bytes[j] != quote {
            if bytes[j] == b'\\' {
                j += 1;
            }
            j += 1;
        }
        if j >= bytes.len() {
            break;
        }
        let word = &expression[start + 1..j];
        let mut before = start;
        while before > 0 && bytes[before - 1].is_ascii_whitespace() {
            before -= 1;
        }
        let marker = before.checked_sub(1).map(|k| bytes[k]);
        if matches!(marker, Some(b'?') | Some(b':')) {
            words.insert(word.to_string());
        }
        i = j + 1;
    }
    words
}

/// `postMessage({ type: … })` 发出的类型词。
fn posted_types(file: &str, source: &str) -> Result<BTreeSet<String>, String> {
    let bytes = source.as_bytes();
    let mut words = BTreeSet::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("postMessage(") {
        let at = cursor + offset;
        let open = at + "postMessage".len();
        let close = matching_paren(bytes, open).ok_or_else(|| {
            format!("  `{file}` 里的 `postMessage(` 括号不闭合：这条消息的类型读不出来")
        })?;
        cursor = close + 1;
        let call = &source[open + 1..close];
        let value = value_of_key(call, "type").ok_or_else(|| {
            format!("  `{file}` 里的 `postMessage(…)` 没有 `type:` 键：收的一方永远认不出这条消息")
        })?;
        let value = value.trim();
        if let Some(word) = quoted(value) {
            if value.starts_with('`') {
                return Err(format!(
                    "  `{file}` 里 `postMessage` 的 `type` 是模板串：拼出来的词读不出来"
                ));
            }
            words.insert(word);
            continue;
        }
        if !is_identifier(value) {
            return Err(format!(
                "  `{file}` 里 `postMessage` 的 `type` 是 `{value}`，既不是字面量也不是变量名"
            ));
        }
        // 变量：在同一个文件里找它的赋值，取三元两支上的字面量。
        let assignment = format!("{value} =");
        let at = source.find(&assignment).ok_or_else(|| {
            format!("  `{file}` 里 `postMessage` 的 `type` 是 `{value}`，而它的赋值找不到")
        })?;
        let from = at + assignment.len();
        let to = source[from..]
            .find(';')
            .map(|i| from + i)
            .ok_or_else(|| format!("  `{file}` 里 `{value}` 的赋值不闭合"))?;
        let branches = ternary_branches(&source[from..to]);
        if branches.is_empty() {
            return Err(format!(
                "  `{file}` 里 `{value}` 的赋值看不出三元的两支：`postMessage` 发什么类型读不出来"
            ));
        }
        words.extend(branches);
    }
    Ok(words)
}

/// `addEventListener('message', <handler>)` 的处理函数体。
fn handler_region(file: &str, source: &str, handler: &str) -> Result<String, String> {
    let handler = handler.trim();
    if handler.contains("=>") || handler.starts_with("function") {
        return Ok(handler.to_string());
    }
    if !is_identifier(handler) {
        return Err(format!(
            "  `{file}` 把 `message` 事件交给 `{handler}`，这不是一个能读的处理函数"
        ));
    }
    let definition = format!("{handler} =");
    let at = source
        .find(&definition)
        .or_else(|| source.find(&format!("function {handler}(")))
        .ok_or_else(|| {
            format!(
                "  `{file}` 把 `message` 事件交给 `{handler}`，而这个处理函数在同一个文件里找不到"
            )
        })?;
    let after = &source[at..];
    let body_start = match after.find("=>") {
        Some(arrow) => at + arrow + "=>".len(),
        None => {
            at + after
                .find(')')
                .ok_or_else(|| format!("  `{file}` 里 `{handler}` 的签名读不出来"))?
        }
    };
    let open = source[body_start..]
        .find('{')
        .map(|i| body_start + i)
        .ok_or_else(|| format!("  `{file}` 里 `{handler}` 的函数体读不出来"))?;
    let close = matching_brace(source.as_bytes(), open)
        .ok_or_else(|| format!("  `{file}` 里 `{handler}` 的函数体不闭合"))?;
    Ok(source[at..=close].to_string())
}

/// 处理函数里 `<x>.type === 'word'` 的那几个词。
fn collect_type_comparisons(region: &str, words: &mut BTreeSet<String>) {
    let bytes = region.as_bytes();
    let mut cursor = 0;
    while let Some(offset) = region[cursor..].find(".type") {
        let at = cursor + offset;
        cursor = at + ".type".len();
        let mut i = cursor;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let rest = &region[i..];
        let operator = if rest.starts_with("===") || rest.starts_with("!==") {
            3
        } else if rest.starts_with("==") || rest.starts_with("!=") {
            2
        } else {
            continue;
        };
        let mut j = i + operator;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if let Some(word) = quoted(&region[j..]) {
            words.insert(word);
        }
    }
}

/// `message` 监听器认的类型词。
fn listened_types(file: &str, source: &str) -> Result<BTreeSet<String>, String> {
    let bytes = source.as_bytes();
    let mut words = BTreeSet::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("addEventListener(") {
        let at = cursor + offset;
        let open = at + "addEventListener".len();
        let close = matching_paren(bytes, open).ok_or_else(|| {
            format!("  `{file}` 里的 `addEventListener(` 括号不闭合：监听器读不出来")
        })?;
        cursor = close + 1;
        let args = &source[open + 1..close];
        let parts = split_top_level(args);
        let Some(event) = parts.first() else { continue };
        let event = event.trim().trim_matches(|c| c == '\'' || c == '"');
        if event != "message" {
            continue;
        }
        let Some(handler) = parts.get(1) else {
            return Err(format!("  `{file}` 的 `message` 监听器没有处理函数"));
        };
        let region = handler_region(file, source, handler)?;
        collect_type_comparisons(&region, &mut words);
    }
    Ok(words)
}

/// 打开弹窗的一方写进 `state` 的前缀：`xState = 'ghapp:' + …`。
fn prefixes_written(source: &str) -> BTreeSet<String> {
    let bytes = source.as_bytes();
    // 从 `=` 出发找，而不是从头逐个引号配对：注释里一个落单的撇号就能让整串
    // 引号错位，而错位之后读到的是「一个前缀都没有」——那正是本门最怕的空读数。
    let mut words = BTreeSet::new();
    for (at, _) in source.match_indices('=') {
        let mut i = at + 1;
        if source[i..].starts_with('=') {
            continue;
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let Some(word) = quoted(&source[i..]) else {
            continue;
        };
        if !word.ends_with(':') {
            continue;
        }
        let quote = source.as_bytes()[i] as char;
        let Some(closed) = source[i + 1..].find(quote).map(|e| i + 1 + e) else {
            continue;
        };
        let mut after = closed + 1;
        while after < bytes.len() && bytes[after].is_ascii_whitespace() {
            after += 1;
        }
        if after < bytes.len() && bytes[after] == b'+' {
            words.insert(word);
        }
    }
    words
}

/// 落地页拿前缀分辨回调类型：`state.indexOf('ghapp:') === 0`。
fn prefixes_tested(source: &str) -> BTreeSet<String> {
    let mut words = BTreeSet::new();
    for call in ["indexOf(", "startsWith("] {
        let mut cursor = 0;
        while let Some(offset) = source[cursor..].find(call) {
            let at = cursor + offset;
            cursor = at + call.len();
            if let Some(word) = quoted(&source[cursor..]) {
                if word.ends_with(':') {
                    words.insert(word);
                }
            }
        }
    }
    words
}

/// `struct <name>` 的字段名，以及它有没有挂 serde 改名。
fn struct_fields(source: &str, name: &str) -> Option<(BTreeSet<String>, bool)> {
    let at = source.find(&format!("struct {name}"))?;
    let open = source[at..].find('{').map(|i| at + i)?;
    let close = matching_brace(source.as_bytes(), open)?;
    let body = &source[open + 1..close];
    let header_renames = source[..at]
        .lines()
        .rev()
        .take(6)
        .any(|line| line.contains("rename_all"));
    let renamed = header_renames || body.contains("#[serde(rename");
    let mut fields = BTreeSet::new();
    for line in body.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("pub ") else {
            continue;
        };
        let Some(field) = rest.split(':').next().map(str::trim) else {
            continue;
        };
        if !field.is_empty() {
            fields.insert(field.to_string());
        }
    }
    Some((fields, renamed))
}

/// 读哪些文件找发送点与接收点：两个内嵌/外置页面、WebUI、网关里的内嵌页面段。
fn client_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    walk(&root.join("web/src"), &["ts", "tsx"], &mut files);
    walk(&root.join("crates/cog-gateway/src"), &["rs"], &mut files);
    files.push(root.join(OPERATOR));
    files.push(root.join(CALLBACK));
    files.sort();
    files.dedup();
    files
}

fn join(files: &BTreeSet<String>) -> String {
    files.iter().cloned().collect::<Vec<_>>().join("、")
}

fn record(map: &mut BTreeMap<String, BTreeSet<String>>, file: &str, words: BTreeSet<String>) {
    for word in words {
        map.entry(word).or_default().insert(file.to_string());
    }
}

fn site(
    root: &Path,
    file: &str,
    needle: &str,
    word: &str,
    what: &str,
    complaints: &mut Vec<String>,
) {
    match std::fs::read_to_string(root.join(file)) {
        Ok(text) if text.contains(needle) => {}
        Ok(_) => complaints.push(format!(
            "  `{word}` 的{what} `{file}` 里找不到 `{needle}`：那一头改名了而这一头没跟上"
        )),
        Err(e) => complaints.push(format!(
            "  `{word}` 的{what} `{file}` 读不出来（{e}）：读不出来按红处理"
        )),
    }
}

fn contradictions(root: &Path, messages: &[Message], prefix: &Prefix) -> Vec<String> {
    let mut complaints = Vec::new();
    let mut sent: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut listened: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for path in client_files(root) {
        let relative = relative_name(root, &path);
        let raw = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) => {
                complaints.push(format!("  `{relative}` 读不出来（{e}）：读不出来按红处理"));
                continue;
            }
        };
        let source = if relative.ends_with(".rs") {
            production_text(&raw)
        } else {
            raw
        };
        match posted_types(&relative, &source) {
            Ok(words) => record(&mut sent, &relative, words),
            Err(complaint) => complaints.push(complaint),
        }
        match listened_types(&relative, &source) {
            Ok(words) => record(&mut listened, &relative, words),
            Err(complaint) => complaints.push(complaint),
        }
    }

    if sent.is_empty() || listened.is_empty() {
        complaints.push(format!(
            "一次握手只读出 {} 种发出的消息、{} 种收到的消息：读不出来的判据判不了任何东西，按红处理",
            sent.len(),
            listened.len()
        ));
    }

    let mut registered = BTreeSet::new();
    for message in messages {
        registered.insert(message.word);
        for (file, needle) in message.senders.iter().chain(message.receivers.iter()) {
            site(root, file, needle, message.word, "收发点", &mut complaints);
        }
        for wire in message.wires {
            for (file, needle) in wire.sites {
                site(
                    root,
                    file,
                    needle,
                    message.word,
                    "消息体键",
                    &mut complaints,
                );
                if !needle_names_key(needle, wire.key) {
                    complaints.push(format!(
                        "  `{word}` 的消息体键 `{}` 在 `{file}` 的 `{needle}` 里并没有出现：\
                         登记和那一处对不上",
                        wire.key,
                        word = message.word
                    ));
                }
            }
        }
        match (sent.get(message.word), listened.get(message.word)) {
            (Some(senders), Some(receivers)) => {
                if !receivers.iter().any(|r| !senders.contains(r)) {
                    complaints.push(format!(
                        "  `{}` 只有发没有收（发在 {}）：弹窗自己关掉，另一头原地等，没有报错",
                        message.word,
                        join(senders)
                    ));
                }
            }
            (None, _) => complaints.push(format!(
                "  表里登记的 `{}` 没有任何地方 `postMessage` 它：登记烂在这里就没人发现",
                message.word
            )),
            (_, None) => complaints.push(format!(
                "  表里登记的 `{}` 没有任何 `message` 监听器认它：这一支永远不成立",
                message.word
            )),
        }
    }
    for (word, files) in &sent {
        if !registered.contains(word.as_str()) {
            complaints.push(format!(
                "  `{word}` 被 `postMessage` 发出去，而它不在表里（{}）：新加的消息要么登记，\
                 要么删掉——没人收的类型就是一条永远不成立的分支",
                join(files)
            ));
        }
    }
    for (word, files) in &listened {
        if !registered.contains(word.as_str()) {
            complaints.push(format!(
                "  `{word}` 被 `message` 监听器认，而它不在表里（{}）：没人发的类型，这个监听器\
                 永远不会被叫到",
                join(files)
            ));
        }
    }

    for (side, file, what) in [("写", prefix.writer, "写"), ("读", prefix.reader, "读")] {
        let source = match std::fs::read_to_string(root.join(file)) {
            Ok(text) => text,
            Err(e) => {
                complaints.push(format!(
                    "  `state` 前缀的{what}方 `{file}` 读不出来（{e}）：读不出来按红处理"
                ));
                continue;
            }
        };
        let words = if side == "写" {
            prefixes_written(&source)
        } else {
            prefixes_tested(&source)
        };
        if !words.contains(prefix.value) {
            complaints.push(format!(
                "  `{file}` 里没有 `{}` 这个 `state` 前缀（读到的是 {words:?}）：前缀一变，\
                 落地页就把回调认成另一种消息，而两边都不报错",
                prefix.value
            ));
        }
    }

    complaints
}

/// 登记的那段字节里真的拼了这个键。否则表上写着「这里读 `code`」，而那段字节
/// 里根本没有 `code`——登记自己就是一处新的空话。
fn needle_names_key(needle: &str, key: &str) -> bool {
    needle.contains(key)
}

fn struct_complaints(source: &str) -> Vec<String> {
    let mut complaints = Vec::new();
    let Some((fields, renamed)) = struct_fields(source, "AuthResponse") else {
        complaints
            .push("  `AuthResponse` 读不出来：登录页递回的 payload 键判不了，按红处理".to_string());
        return complaints;
    };
    if renamed {
        complaints.push(
            "  `AuthResponse` 上挂了 serde 改名：线上名字与字段名分开了，而登录页读的是字段名"
                .to_string(),
        );
    }
    for key in ["access_token", "refresh_token", "user"] {
        if !fields.contains(key) {
            complaints.push(format!(
                "  `AuthResponse` 里没有 `{key}` 字段，而登录页读这个词（字段是 {fields:?}）"
            ));
        }
    }
    complaints
}

#[test]
fn every_word_in_the_handshake_is_a_word_the_other_side_writes() {
    let complaints = contradictions(&repo_root(), MESSAGES, &PREFIX);
    assert!(
        complaints.is_empty(),
        "登录/授权握手的每个词都必须在两端拼成同一个。拼错了没有报错，只有弹窗关掉、\
         向导原地等：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_login_payload_fields_are_the_fields_the_struct_serializes() {
    let source = read(&repo_root().join(AUTH_RESPONSE));
    let complaints = struct_complaints(&source);
    assert!(
        complaints.is_empty(),
        "登录 payload 的键就是 `AuthResponse` 的字段名：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let mut sent = BTreeSet::new();
    let mut listened = BTreeSet::new();
    let mut senders = BTreeSet::new();
    for path in client_files(&root) {
        let relative = relative_name(&root, &path);
        let raw = read(&path);
        let source = if relative.ends_with(".rs") {
            production_text(&raw)
        } else {
            raw
        };
        let posted = posted_types(&relative, &source).expect("发送点读不出来");
        if !posted.is_empty() {
            senders.insert(relative.clone());
        }
        sent.extend(posted);
        listened.extend(listened_types(&relative, &source).expect("监听器读不出来"));
    }
    assert!(sent.len() >= 3, "只读出 {} 种发出的消息", sent.len());
    assert!(
        listened.len() >= 3,
        "只读出 {} 种收到的消息",
        listened.len()
    );
    assert!(senders.len() >= 2, "发出点在 {} 个文件里", senders.len());
    let receivers: usize = MESSAGES.iter().map(|m| m.receivers.len()).sum();
    assert!(receivers >= 5, "登记的接收点只有 {receivers} 处");
    let wires: usize = MESSAGES.iter().flat_map(|m| m.wires.iter()).count();
    assert!(wires >= 7, "登记的消息体键只有 {wires} 个");
    assert!(
        !prefixes_written(&read(&root.join(PREFIX.writer))).is_empty(),
        "`state` 前缀一个都没读出来"
    );
    assert!(
        !prefixes_tested(&read(&root.join(PREFIX.reader))).is_empty(),
        "落地页测的 `state` 前缀一个都没读出来"
    );
}

#[test]
fn a_type_that_is_a_variable_is_read_from_its_ternary() {
    let source = "\
var kind = (state.indexOf('p:') === 0) ? 'one' : 'two';
window.opener.postMessage({ type: kind, code: code }, '*');
";
    let words = posted_types("fixture.js", source).expect("三元读不出两支");
    assert_eq!(
        words,
        ["one".to_string(), "two".to_string()].into_iter().collect()
    );
}

const FIXTURE_OPERATOR: &str = "\
window.addEventListener('message', (e) => {
  if (e.data.type !== 'demo-word') return;
});
appState = 'demo:' + Math.random().toString(36);
";

const FIXTURE_CALLBACK: &str = "\
var t = state.indexOf('demo:') === 0;
window.opener.postMessage({ type: 'demo-word', code: code, state: state }, '*');
";

const FIXTURE_LOGIN_PAGE: &str = "\
fn page() {
    let _ = \"<script>postMessage({{ type: 'login-word', data: d }}, origin)</script>\";
}
";

const FIXTURE_WEB_AUTH: &str = "\
const onMessage = (data) => {
  if (data.type !== 'login-word') return;
  const token = data.data;
};
window.addEventListener('message', onMessage);
";

fn fixture(root: &Path) {
    for (relative, body) in [
        (OPERATOR, FIXTURE_OPERATOR),
        (CALLBACK, FIXTURE_CALLBACK),
        (LOGIN_PAGE, FIXTURE_LOGIN_PAGE),
        (WEB_AUTH, FIXTURE_WEB_AUTH),
        (WEB_OAUTH, "// 这一棵树里没有第二种消息\n"),
    ] {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, body).expect("write");
    }
}

fn fixture_messages() -> Vec<Message> {
    vec![
        Message {
            word: "demo-word",
            senders: &[(
                CALLBACK,
                "postMessage({ type: 'demo-word', code: code, state: state }",
            )],
            receivers: &[(OPERATOR, "e.data.type !== 'demo-word'")],
            wires: &[
                Wire {
                    key: "code",
                    sites: &[(
                        CALLBACK,
                        "postMessage({ type: 'demo-word', code: code, state: state }",
                    )],
                },
                Wire {
                    key: "state",
                    sites: &[(
                        CALLBACK,
                        "postMessage({ type: 'demo-word', code: code, state: state }",
                    )],
                },
            ],
        },
        Message {
            word: "login-word",
            senders: &[(LOGIN_PAGE, "postMessage({{ type: 'login-word', data: d }}")],
            receivers: &[(WEB_AUTH, "data.type !== 'login-word'")],
            wires: &[Wire {
                key: "data",
                sites: &[
                    (LOGIN_PAGE, "postMessage({{ type: 'login-word', data: d }}"),
                    (WEB_AUTH, "const token = data.data;"),
                ],
            }],
        },
    ]
}

fn fixture_prefix() -> Prefix {
    Prefix {
        value: "demo:",
        writer: OPERATOR,
        reader: CALLBACK,
    }
}

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(complaints.is_empty(), "{complaints:?}");
}

#[test]
fn a_type_nobody_listens_for_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    std::fs::write(
        dir.path().join(CALLBACK),
        FIXTURE_CALLBACK.replace("'demo-word'", "'demo-word-v2'"),
    )
    .expect("write");
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(
        complaints.iter().any(|c| c.contains("demo-word-v2")),
        "{complaints:?}"
    );
}

#[test]
fn a_listener_nobody_posts_to_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    std::fs::write(
        dir.path().join(OPERATOR),
        FIXTURE_OPERATOR.replace("'demo-word'", "'demo-word-v2'"),
    )
    .expect("write");
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(
        complaints.iter().any(|c| c.contains("demo-word-v2")),
        "{complaints:?}"
    );
}

#[test]
fn a_registered_word_whose_sender_stopped_posting_it_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    std::fs::write(
        dir.path().join(LOGIN_PAGE),
        "fn page() { let _ = \"nothing\"; }\n",
    )
    .expect("write");
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(
        complaints.iter().any(|c| c.contains("login-word")),
        "{complaints:?}"
    );
}

#[test]
fn a_payload_key_the_sender_stopped_using_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    std::fs::write(
        dir.path().join(CALLBACK),
        FIXTURE_CALLBACK.replace("code: code", "value: code"),
    )
    .expect("write");
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("demo-word") && c.contains("找不到")),
        "{complaints:?}"
    );
}

#[test]
fn a_state_prefix_renamed_on_one_side_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    std::fs::write(
        dir.path().join(CALLBACK),
        FIXTURE_CALLBACK.replace("indexOf('demo:')", "indexOf('demo-v2:')"),
    )
    .expect("write");
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(
        complaints.iter().any(|c| c.contains("demo:")),
        "{complaints:?}"
    );
}

#[test]
fn a_page_that_cannot_be_read_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path());
    std::fs::remove_file(dir.path().join(CALLBACK)).expect("remove");
    let complaints = contradictions(dir.path(), &fixture_messages(), &fixture_prefix());
    assert!(
        complaints.iter().any(|c| c.contains("读不出来")),
        "{complaints:?}"
    );
}

#[test]
fn a_struct_that_renames_its_fields_is_reported() {
    let source = "\
#[derive(Debug, Serialize)]
#[serde(rename_all = \"camelCase\")]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub user: UserResponse,
}
";
    let complaints = struct_complaints(source);
    assert!(
        complaints.iter().any(|c| c.contains("serde 改名")),
        "{complaints:?}"
    );
}
