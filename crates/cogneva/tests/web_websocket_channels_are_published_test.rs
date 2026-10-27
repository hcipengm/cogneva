//! principle: feedback_no_hardcoding_config_surface
//!
//! 频道名是两边各写一次的又一份词表：服务端在 `event_channels` 里拼
//! （`agent:<id>`、`task:<id>`、`hooks`、`resource:<domain>`），客户端在订阅时
//! 写一串带星号的前缀。中间没有任何东西比对，订阅错一个前缀的结果不是报错——
//! 是那一族帧**根本不会送到**这条连接上：套接字接通了，心跳也通，界面上只是
//! 永远没有那类事件。
//!
//! 实测（2026-10-27）落地时抓到一次：网关把「一次 REST 写落地」发在
//! `resource:<domain>` 上，而 WebUI 只订了 `agent:*`、`task:*`、`hooks`。于是
//! `resource_changed` 这一族帧一帧都到不了浏览器——界面上十几个面各自的
//! `resourceKey('settings')` 这类刷新键，从写下来那天起就没有被触发过一次，
//! 那些面只能靠各自的轮询。订阅里补上 `resource:*` 之后两边才接上。
//!
//! 两个方向都判：订了却没人发的频道族是白等，发了却没人订的频道族是白发。
//! 「发」只看 `event_channels`：那是唯一决定一帧落到哪条通道上的地方。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const WEB: &str = "web/src";
const TAKEOVER: &str = "crates/cog-gateway/src/takeover.html";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn strip_comments(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == quote {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&text[start..i.min(bytes.len())]);
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            _ => {
                let ch = text[i..].chars().next().expect("in bounds");
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

/// 服务端会发的频道族：前缀（`agent:`）或整名（`hooks`）。
///
/// 来源只有 `event_channels` 的函数体：那里拼什么，一帧就落在什么通道上。
/// 体里出现的字符串常量也要解析——`resource:` 是经 `RESOURCE_CHANNEL_PREFIX`
/// 拼进去的，只读 `format!` 的字面量会漏掉整整一族。
fn published_families(text: &str) -> BTreeSet<String> {
    let start = text
        .find("pub fn event_channels")
        .unwrap_or_else(|| panic!("{PROTOCOL} 里没有 event_channels，这条判据没有题目"));
    let end = text[start..]
        .find("\n}\n")
        .map_or(text.len(), |n| start + n + 1);
    let body = &text[start..end];

    let mut families = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = body[cursor..].find("format!(\"") {
        let lit_start = cursor + at + "format!(\"".len();
        let Some(lit_end) = body[lit_start..].find('"') else {
            break;
        };
        let literal = &body[lit_start..lit_start + lit_end];
        cursor = lit_start + lit_end;
        let prefix = literal.split('{').next().unwrap_or("");
        if !prefix.is_empty() {
            families.insert(prefix.to_string());
        }
    }

    // 体里点名的常量：`HOOKS_CHANNEL`、`RESOURCE_CHANNEL_PREFIX`。
    let mut words = BTreeSet::new();
    for word in body.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
        if word.len() > 3 && word.starts_with(|c: char| c.is_ascii_uppercase()) {
            words.insert(word.to_string());
        }
    }
    for word in words {
        let needle = format!("pub const {word}: &str = \"");
        if let Some(at) = text.find(&needle) {
            let value_start = at + needle.len();
            if let Some(len) = text[value_start..].find('"') {
                families.insert(text[value_start..value_start + len].to_string());
            }
        }
    }
    families
}

/// 客户端订阅的频道：`channels: [ … ]` 里那一串字面量。
fn subscribed(text: &str) -> BTreeSet<String> {
    let clean = strip_comments(text);
    let mut names = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find("channels: [") {
        let open = cursor + at + "channels: [".len();
        let Some(len) = clean[open..].find(']') else {
            break;
        };
        for (index, part) in clean[open..open + len].split('\'').enumerate() {
            if index % 2 == 1 && !part.trim().is_empty() {
                names.insert(part.trim().to_string());
            }
        }
        cursor = open + len;
    }
    names
}

/// 订阅项匹配不匹配某一族：`agent:*` 对 `agent:`，`hooks` 对 `hooks`。
fn matches(subscription: &str, family: &str) -> bool {
    match subscription.strip_suffix('*') {
        Some(prefix) => prefix == family && family.ends_with(':'),
        None => subscription == family,
    }
}

fn all_subscribed(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut files = Vec::new();
    collect(&root.join(WEB), &mut files);
    let page = root.join(TAKEOVER);
    if page.exists() {
        files.push(page);
    }
    let mut out = BTreeMap::new();
    for path in files {
        let names = subscribed(&read(&path));
        if !names.is_empty() {
            let label = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            out.insert(label, names);
        }
    }
    out
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("ts") | Some("tsx")
        ) {
            out.push(path);
        }
    }
}

fn contradictions(root: &Path) -> (Vec<String>, usize) {
    let families = published_families(&read(&root.join(PROTOCOL)));
    let subscriptions = all_subscribed(root);
    let mut complaints = Vec::new();

    for (file, names) in &subscriptions {
        for name in names {
            if !families.iter().any(|family| matches(name, family)) {
                complaints.push(format!(
                    "{file} 订了 '{name}'，而 event_channels 只发 {families:?}：这条订阅上\
                     永远不会有帧，连接是通的，界面上只是从来没有这一类事件"
                ));
            }
        }
    }
    let mut judged = 0;
    for family in &families {
        judged += 1;
        if !subscriptions
            .values()
            .any(|names| names.iter().any(|name| matches(name, family)))
        {
            complaints.push(format!(
                "event_channels 会往 '{family}' 上发帧，而没有一个客户端订它：这一族帧\
                 发出去就没有收件人。要么在客户端的订阅列表里加上它，要么把发它的代码删掉"
            ));
        }
    }
    (complaints, judged)
}

#[test]
fn every_channel_the_gateway_publishes_has_a_subscriber() {
    let (complaints, judged) = contradictions(&repo_root());
    assert!(judged > 0, "一个频道族都没判到，这条判据没有题目");
    assert!(
        complaints.is_empty(),
        "频道名两边各写一次，写出来的不是同一份：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let families = published_families(&read(&root.join(PROTOCOL)));
    assert_eq!(
        families,
        BTreeSet::from([
            "agent:".to_string(),
            "hooks".to_string(),
            "resource:".to_string(),
            "task:".to_string(),
        ]),
        "event_channels 发出去的频道族变了：读取器与那边的写法对不上，或者真少了一族"
    );
    let subscriptions = all_subscribed(&root);
    assert!(
        subscriptions.len() >= 2,
        "只读到 {} 处订阅点，太少：订阅的写法变了，这条判据会靠「找不到题目」通过",
        subscriptions.len()
    );
}

#[test]
fn the_reader_sees_families_consts_and_subscriptions() {
    let server = "\
pub const HOOKS_CHANNEL: &str = \"hooks\";
pub const RESOURCE_CHANNEL_PREFIX: &str = \"resource:\";

pub fn event_channels(event: &AgentEvent) -> Vec<String> {
    match event {
        AgentEvent::TaskStatusChange { task_id, .. } => vec![format!(\"task:{}\", task_id)],
        AgentEvent::Heartbeat { .. } => vec![HOOKS_CHANNEL.to_string()],
        AgentEvent::ResourceChanged { domain, .. } => {
            vec![format!(\"{}{}\", RESOURCE_CHANNEL_PREFIX, domain)]
        }
    }
}
";
    assert_eq!(
        published_families(server),
        BTreeSet::from([
            "hooks".to_string(),
            "resource:".to_string(),
            "task:".to_string()
        ])
    );

    let client = "\
const options = {
  channels: ['agent:*', 'hooks'],
};
// 注释里的 channels: ['ghost'] 不算。
";
    assert_eq!(
        subscribed(client),
        BTreeSet::from(["agent:*".to_string(), "hooks".to_string()])
    );

    assert!(matches("agent:*", "agent:"));
    assert!(!matches("agent:*", "task:"));
    assert!(matches("hooks", "hooks"));
    assert!(!matches("hooks", "task:"));
}

#[test]
fn a_subscription_nothing_publishes_and_a_family_nobody_hears_are_both_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-channels-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");
    std::fs::create_dir_all(root.join(WEB)).expect("temp web");

    std::fs::write(
        root.join(PROTOCOL),
        "pub const HOOKS_CHANNEL: &str = \"hooks\";\npub fn event_channels(e: &AgentEvent) -> \
         Vec<String> {\n    vec![format!(\"agent:{}\", id), format!(\"resource:{}\", d)]\n}\n",
    )
    .expect("protocol");
    std::fs::write(
        root.join(WEB).join("app.ts"),
        "const c = { channels: ['agent:*', 'task:*'] };\n",
    )
    .expect("app");

    let (complaints, judged) = contradictions(&root);
    assert_eq!(judged, 2, "{complaints:?}");
    let joined = complaints.join("\n");
    assert!(joined.contains("task:*"), "{joined}");
    assert!(joined.contains("resource:"), "{joined}");

    // 补上订阅就都成立：判据盯的是两边接没接上，不是订阅长什么样。
    std::fs::write(
        root.join(WEB).join("app.ts"),
        "const c = { channels: ['agent:*', 'resource:*'] };\n",
    )
    .expect("app");
    let (complaints, _) = contradictions(&root);
    assert!(complaints.is_empty(), "{complaints:?}");

    let _ = std::fs::remove_dir_all(&root);
}
