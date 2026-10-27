//! principle: feedback_no_hardcoding_config_surface
//!
//! A browser storage key is a word spelled in two places — where somebody writes
//! it and where somebody reads it — and nothing compares the spellings. When
//! they drift, both halves keep running: the write lands under a name nobody
//! reads, the read returns `null`, and `null` is exactly what "the user has
//! never signed in" looks like. The operator is shown the sign-in screen again
//! and told nothing.
//!
//! The surfaces here are the three that own storage: the web app
//! (`web/src`), the operator takeover page (`takeover.html`) and the OAuth
//! landing page the gateway renders (`auth/platform.rs`). The last two cannot
//! import the app's constants — they are separate documents — so the same name
//! has to be declared in each, and the only thing that can hold them together is
//! a reader that looks at all three.
//!
//! 实测（2026-10-27）：三处共 5 个键名。`TOKEN_KEY` / `REFRESH_KEY` /
//! `USER_KEY` 在网页端与落地页各声明一次、取值相同；`ADMIN_TOKEN_KEY` 与
//! `LANG_KEY` 只在接管页，各自独立。改之前有两处同名的键指的不是同一个东西
//! （接管页的 `TOKEN_KEY` 是管理员令牌），另有两处调用点在调用点直接写字面量。
//!
//! What it judges:
//!
//! * a key is never written as a literal at a call site: `setItem('…')` says the
//!   name twice and gives the second spelling nothing to check it against;
//! * every name used at a call site is declared in that surface;
//! * one name means one value everywhere. Two surfaces giving `TOKEN_KEY` two
//!   different values is a trap even when both work — the next copy-paste
//!   between them takes the wrong token;
//! * every declared key is actually used somewhere in its surface, so a name
//!   nobody reads is not left looking like part of the protocol.
//!
//! What it cannot judge:
//!
//! * whether the *value* is the right one. `cogneva_token` appears here because
//!   both surfaces agree on it, not because anything knows it is the token the
//!   server meant;
//! * the gateway's own reads. A key the server compares against (a cookie name,
//!   a header) is not a storage key and is out of scope;
//! * storage reached through an alias (`const s = localStorage; s.getItem(…)`).
//!   Nothing in the tree does that, and the scan would not follow it — the
//!   direction that goes quiet, not the direction that reddens.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Documents that own browser storage. `web/src` is walked; the other two are
/// read whole, because each is a single document that cannot import anything.
const WEB_ROOT: &str = "web/src";
const LANDING: &str = "crates/cog-gateway/src/auth/platform.rs";
const TAKEOVER: &str = "crates/cog-gateway/src/takeover.html";

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn web_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            web_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "ts" || e == "tsx") {
            out.push(path);
        }
    }
}

/// The text as the scan should see it: line comments dropped, so a call quoted
/// in a comment is not read as a call.
fn without_line_comments(text: &str) -> String {
    text.lines()
        .filter(|line| {
            let t = line.trim_start();
            !t.starts_with("//") && !t.starts_with('*')
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// One use of storage: the first argument of `localStorage.<op>(` — or
/// `sessionStorage.<op>(` — and whether it was a literal.
fn storage_calls(text: &str) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    for marker in ["localStorage.", "sessionStorage."] {
        let mut from = 0;
        while let Some(at) = text[from..].find(marker) {
            let start = from + at + marker.len();
            from = start;
            let rest = &text[start..];
            let op: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !["getItem", "setItem", "removeItem", "clear"].contains(&op.as_str()) {
                continue;
            }
            let after_op = &rest[op.len()..];
            let after_op = after_op.trim_start();
            let Some(args) = after_op.strip_prefix('(') else {
                continue;
            };
            let args = args.trim_start();
            let Some(first) = args.chars().next() else {
                continue;
            };
            if first == '\'' || first == '"' {
                let quote = first;
                let body = &args[1..];
                let end = body.find(quote).unwrap_or(0);
                out.push((body[..end].to_string(), true));
            } else if first.is_alphanumeric() || first == '_' || first == '$' {
                let name: String = args
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
                    .collect();
                out.push((name, false));
            }
        }
    }
    out
}

/// `NAME = 'value'` where the name ends in `KEY` — the shape every storage key
/// in this tree has. `_KEY_PREFIX`, `keyFor(…)` and friends are not keys.
/// The `===` guard keeps a comparison from reading as a declaration.
fn declarations(text: &str) -> Vec<(String, String)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if !(bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$') {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len()
            && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
        {
            i += 1;
        }
        let name = &text[start..i];
        let mut j = i;
        while j < bytes.len() && (bytes[j] as char).is_ascii_whitespace() {
            j += 1;
        }
        if bytes.get(j) != Some(&b'=') || bytes.get(j + 1) == Some(&b'=') {
            continue;
        }
        j += 1;
        while j < bytes.len() && (bytes[j] as char).is_ascii_whitespace() {
            j += 1;
        }
        let quote = match bytes.get(j) {
            Some(b'\'') => '\'',
            Some(b'"') => '"',
            _ => continue,
        };
        let body_start = j + 1;
        let Some(close) = text[body_start..].find(quote) else {
            continue;
        };
        if !name.ends_with("KEY") {
            continue;
        }
        out.push((
            name.to_string(),
            text[body_start..body_start + close].to_string(),
        ));
    }
    out
}

/// One document, as far as the scan cares: where it is and what it says.
struct SurfaceScan {
    file: String,
    calls: Vec<(String, bool)>,
    declared: BTreeMap<String, String>,
}

fn scan(file: String, text: &str) -> SurfaceScan {
    let mut declared = BTreeMap::new();
    for (name, value) in declarations(text) {
        declared.insert(name, value);
    }
    SurfaceScan {
        calls: storage_calls(text),
        declared,
        file,
    }
}

fn surfaces(root: &Path) -> Vec<SurfaceScan> {
    let mut out = Vec::new();
    let mut files = Vec::new();
    web_files(&root.join(WEB_ROOT), &mut files);
    files.sort();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        out.push(scan(rel, &without_line_comments(&read(&path))));
    }
    for doc in [LANDING, TAKEOVER] {
        out.push(scan(doc.to_string(), &read(&root.join(doc))));
    }
    out
}

fn disagreements(root: &Path) -> Vec<String> {
    let scans = surfaces(root);
    let mut out = Vec::new();

    // The app's declarations are shared by every file in it; a document declares
    // its own, because it cannot import.
    let mut in_web: BTreeMap<String, String> = BTreeMap::new();
    for s in scans.iter().filter(|s| s.file.starts_with(WEB_ROOT)) {
        in_web.extend(s.declared.clone());
    }

    // One name, one value — across every place the name is declared.
    let mut values: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for s in &scans {
        for (name, value) in &s.declared {
            values
                .entry(name.clone())
                .or_default()
                .push((s.file.clone(), value.clone()));
        }
    }
    for (name, sites) in &values {
        let distinct: Vec<&String> = {
            let mut v: Vec<&String> = sites.iter().map(|(_, value)| value).collect();
            v.sort();
            v.dedup();
            v
        };
        if distinct.len() > 1 {
            let spelled: Vec<String> = sites
                .iter()
                .map(|(file, value)| format!("{file} = {value:?}"))
                .collect();
            out.push(format!(
                "同一个键名 `{name}` 在不同地方指的不是同一格存储：{}。\
                 名字相同取值不同，两个页面各自都能跑，下一次在它们之间复制粘贴就会读写错那一格",
                spelled.join("；")
            ));
        }
    }

    // Every declared name is used, and every use is declared.
    for s in &scans {
        let scope: &BTreeMap<String, String> = if s.file.starts_with(WEB_ROOT) {
            &in_web
        } else {
            &s.declared
        };
        for (arg, literal) in &s.calls {
            if *literal {
                out.push(format!(
                    "{} 在调用点上直接写死了键 {arg:?}：这个名字在别处还有一份声明，\
                     改一处不会改到另一处，而读写对不上时两边都不报错",
                    s.file
                ));
                continue;
            }
            if arg == "clear" {
                continue;
            }
            if !scope.contains_key(arg) {
                out.push(format!(
                    "{} 用 `{arg}` 当存储键，但这个文件所在的文档里没有声明它",
                    s.file
                ));
            }
        }
        for name in s.declared.keys() {
            if !s.calls.iter().any(|(arg, _)| arg == name) {
                out.push(format!(
                    "{} 声明了 `{name}` 却没有用它：一格没人读写的存储键，\
                     读起来像是协议的一部分",
                    s.file
                ));
            }
        }
    }

    out
}

#[test]
fn every_storage_key_is_declared_once_and_used() {
    let root = repo_root();
    let scans = surfaces(&root);
    // 抽取面自检：一个调用点都没读到、或三处文档里一处键声明都没有时，下面的
    // 判据会在空集上假装绿。
    let calls: usize = scans.iter().map(|s| s.calls.len()).sum();
    assert!(
        calls >= 10,
        "只在三处文档里读到 {calls} 个存储调用点，扫描器本身坏了"
    );
    let mut names: Vec<&String> = scans.iter().flat_map(|s| s.declared.keys()).collect();
    names.sort();
    names.dedup();
    for known in ["TOKEN_KEY", "REFRESH_KEY", "LANG_KEY"] {
        assert!(
            names.iter().any(|n| n.as_str() == known),
            "读不到键声明 `{known}`：解析坏了或它真的被删了"
        );
    }

    let found = disagreements(&root);
    assert!(
        found.is_empty(),
        "浏览器存储的键在几处文档里各写一份，没有任何东西把它们对上：\n{}\n\
         对不上的时候两边都不报错：写落在一个名字下，读从另一个名字取回 null，\
         而 null 正是「这个人还没登录过」的样子",
        found.join("\n")
    );
}

/// 判据自己也要有判据：一棵小树里三处文档，取值一致时判空；把落地页的取值
/// 改掉、把调用点换成字面量、声明一个没人用的键，各自都要点名。
#[test]
fn a_key_spelled_two_ways_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("web/src/auth")).expect("web dir");
    std::fs::create_dir_all(root.join("crates/cog-gateway/src/auth")).expect("gateway dir");
    let web = root.join("web/src/auth/session.ts");
    let landing = root.join(LANDING);
    let takeover = root.join(TAKEOVER);

    std::fs::write(
        &web,
        "export const TOKEN_KEY = 'cogneva_token';\n\
         export const THEME_KEY = 'cogneva.theme';\n\
         export function store(t: string) {\n\
           localStorage.setItem(TOKEN_KEY, t);\n\
         }\n\
         export function tone() {\n\
           return localStorage.getItem(THEME_KEY);\n\
         }\n",
    )
    .expect("web");
    std::fs::write(
        &landing,
        "fn page() { format!(\"<script>const TOKEN_KEY = 'cogneva_token';\\n\\\n\
         localStorage.setItem(TOKEN_KEY, d.token);</script>\") }\n",
    )
    .expect("landing");
    std::fs::write(
        &takeover,
        "<script>\nconst ADMIN_TOKEN_KEY = 'cogneva_admin_token';\n\
         localStorage.setItem(ADMIN_TOKEN_KEY, $('token').value);\n</script>\n",
    )
    .expect("takeover");

    assert_eq!(disagreements(root), Vec::<String>::new());

    // 落地页把取值写成另一个：两个名字相同、存储不同。
    std::fs::write(
        &landing,
        "fn page() { format!(\"<script>const TOKEN_KEY = 'cogneva_tok';\\n\\\n\
         localStorage.setItem(TOKEN_KEY, d.token);</script>\") }\n",
    )
    .expect("landing");
    let drifted = disagreements(root);
    assert_eq!(drifted.len(), 1, "{drifted:?}");
    assert!(drifted[0].contains("TOKEN_KEY"), "{drifted:?}");

    // 调用点直接写字面量。
    std::fs::write(
        &landing,
        "fn page() { format!(\"<script>const TOKEN_KEY = 'cogneva_token';\\n\\\n\
         localStorage.setItem(TOKEN_KEY, d.token);</script>\") }\n",
    )
    .expect("landing");
    std::fs::write(
        &web,
        "export const TOKEN_KEY = 'cogneva_token';\n\
         export const THEME_KEY = 'cogneva.theme';\n\
         export function store(t: string) {\n\
           localStorage.setItem(TOKEN_KEY, t);\n\
         }\n\
         export function tone() {\n\
           return localStorage.getItem(THEME_KEY);\n\
         }\n\
         export function legacy(t: string) {\n\
           localStorage.setItem('cogneva_token', t);\n\
         }\n",
    )
    .expect("web");
    let literal = disagreements(root);
    assert_eq!(literal.len(), 1, "{literal:?}");
    assert!(
        literal[0].contains("字面量") || literal[0].contains("写死"),
        "{literal:?}"
    );

    // 声明了一个没人读写的键。
    std::fs::write(
        &web,
        "export const TOKEN_KEY = 'cogneva_token';\n\
         export const THEME_KEY = 'cogneva.theme';\n\
         const ORPHAN_KEY = 'cogneva_orphan';\n\
         export function store(t: string) {\n\
           localStorage.setItem(TOKEN_KEY, t);\n\
         }\n\
         export function tone() {\n\
           return localStorage.getItem(THEME_KEY);\n\
         }\n",
    )
    .expect("web");
    let orphan = disagreements(root);
    assert_eq!(orphan.len(), 1, "{orphan:?}");
    assert!(orphan[0].contains("ORPHAN_KEY"), "{orphan:?}");
}

/// 解析面自己也要有判据，否则「读到零个键」与「没有人用存储」不可分。
#[test]
fn the_reader_sees_keys_and_ignores_comparisons() {
    let text = "const TOKEN_KEY = 'cogneva_token';\n\
                const LANG_KEY = 'cogneva_lang';\n\
                localStorage.setItem(TOKEN_KEY, a);\n\
                window.localStorage.removeItem( TOKEN_KEY );\n\
                localStorage.setItem('inline_key', b);\n\
                if (LANG_KEY === 'cogneva_lang') {}\n\
                // localStorage.setItem('commented', c);\n";
    let stripped = without_line_comments(text);
    let calls = storage_calls(&stripped);
    assert_eq!(
        calls,
        vec![
            ("TOKEN_KEY".to_string(), false),
            ("TOKEN_KEY".to_string(), false),
            ("inline_key".to_string(), true),
        ]
    );
    let declared = declarations(&stripped);
    // `===` 右边那个不是声明：读成声明会让「比较里出现的键值」变成第二份定义。
    assert_eq!(
        declared,
        vec![
            ("TOKEN_KEY".to_string(), "cogneva_token".to_string()),
            ("LANG_KEY".to_string(), "cogneva_lang".to_string()),
        ]
    );
}
