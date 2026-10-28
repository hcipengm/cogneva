//! principle: feedback_no_hardcoding_config_surface
//!
//! 套接字上的信封类型名是两边各写一次的：服务端在 `ServerMessage` 上用
//! `#[serde(rename = "…")]` 写一次，客户端在 `message.type === '…'` 上再写一次。
//! 中间没有任何东西编译两边，所以改了一边，另一边只是从此再也匹配不上——帧照常
//! 到达，分支一次也不进，界面上是「什么都没发生」。
//!
//! 这里按两个方向各判一次，并且判「声明了有没有人写 / 有没有人读」：
//! 1. 客户端等的每一个名字，都必须是服务端真会写的名字。
//! 2. 客户端等的那个变体，服务端必须真有一处构造它的代码——否则客户端这一支是死的。
//! 3. 服务端能发的每一个变体，客户端要么分派它，要么在 `IGNORED` 里写明为什么
//!    可以不理；登记表本身也要自洽（名字还在、且确实没被分派）。
//!
//! 实测（2026-10-27）落地前，`ServerMessage` 有 11 个变体：`TaskUpdate` /
//! `QuotaWarning` / `Kick` / `Error` 全树没有一处构造，而客户端却在等 `task_update`
//! ——那一支从写下来那天起就没进过；`Unsubscribed` / `Acknowledged` 的回执只在
//! 「客户端退订 / 批量确认」时发，而两个客户端都没有发过那两条上行消息（`ack` 的
//! 处理器只是把 id 原样回显，服务端从不使用它们）。这六个变体连同它们唯一的上行
//! 消息 `unsubscribe` / `ack` 一起删了。留下五个真会发的，其中两个是握手回执，
//! 客户端有意不理，登记在 `IGNORED` 里。
//!
//! 上行方向同样判：两个客户端发出去的每一个 `type` 都必须是 `ClientMessage` 真认的
//! 名字，而 `ClientMessage` 的每个变体都必须有发送者——删完之后剩 ping / subscribe /
//! typing / chat_message 四条，两个客户端各自发其中几条，四条都有人发。
//!
//! 名字对上了，带的键也要对上：一个字段名写成另一个（`seq` 写成 `sequence`），
//! serde 把不认识的键丢掉、用默认值顶上，请求照发，缺的那块像没填——回执上什么
//! 异常都没有。所以每个上行对象顶层那些键，也必须是那个变体声明的字段。实测
//! （2026-10-27）：两个客户端的上行对象全部对得上；真树消融把 `ping` 的 `seq`
//! 改成 `sequence`，本门红并点名它，改回即绿。
//!
//! 判据不判断「该不该有这个名字」：真需要 `error` 帧，就先把发它的代码写上，
//! 变体和客户端的读法一起加回来——那时本门自然认得。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::routes::read;

use common::routes::repo_root;

use common::rust_production::{all_crate_files, crate_files_of};

/// 服务端→客户端的信封。
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";

/// 客户端的收帧处。
const CLIENT: &str = "web/src/transport/websocket.ts";

/// 网关内嵌的那份兜底客户端。它是第二个客户端，同一套名字再拼一遍。
const TAKEOVER: &str = "crates/cog-gateway/src/takeover.html";

/// 聊天帧的名字从这里写出来。
const CHAT: &str = "crates/cog-gateway/src/chat.rs";

/// `AgentEvent` 变体的线上名字从这里写出来。
const EVENT_WIRE: &str = "crates/cog-storage/src/event_filter.rs";

/// 服务端会发、客户端有意不理的变体，以及为什么可以不理解。
///
/// 四个都是回执或握手：它们不带数据，客户端的行为不因它们改变。少写一条，本门会
/// 要求客户端分派它；写了一条其实会分派的，本门也会红——这张表不是豁免，是说明。
const IGNORED: &[(&str, &str)] = &[
    (
        "Connected",
        "握手回执：带连接 id 与服务端时间，客户端拿到连接就已经在工作，没有一格要读它",
    ),
    (
        "Subscribed",
        "订阅回执：客户端订阅之后不等确认就开始收帧，缺这个回执只说明订阅发丢了，而那时收不到帧的现象本身就是信号",
    ),
];

/// 去掉 `//` 与 `/* */` 注释，保留字符串。
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

/// `ServerMessage` 的每个变体与它的线上名字：变体名 → 线上名字。
fn enum_variants(text: &str, header: &str) -> BTreeMap<String, String> {
    let start = text
        .find(header)
        .unwrap_or_else(|| panic!("{PROTOCOL} 里没有 {header}，这条判据没有题目"));
    let mut variants = BTreeMap::new();
    let mut pending: Option<String> = None;
    for line in text[start..].lines().skip(1) {
        if line.starts_with('}') {
            break;
        }
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[serde(rename") {
            let after = &trimmed[trimmed.find('"').expect("rename 里没有名字") + 1..];
            pending = Some(after[..after.find('"').expect("rename 没有收尾")].to_string());
            continue;
        }
        // 变体头在四格缩进上；字段在八格，别的都不是变体。
        let head = line
            .strip_prefix("    ")
            .is_some_and(|rest| !rest.starts_with(' '));
        if !head {
            continue;
        }
        let name: String = trimmed
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if name.is_empty() || !name.starts_with(|c: char| c.is_ascii_uppercase()) {
            continue;
        }
        let wire = pending.take().unwrap_or_else(|| {
            panic!("ServerMessage::{name} 没有 #[serde(rename)]：线上名字会变成变体名本身，而客户端认的是小写下划线")
        });
        variants.insert(name, wire);
    }
    variants
}

/// 客户端拿信封上的 `type` 比对的那些名字。
fn dispatched(text: &str) -> BTreeSet<String> {
    let clean = strip_comments(text);
    let mut names = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find(".type === '") {
        let start = cursor + at + ".type === '".len();
        let Some(len) = clean[start..].find('\'') else {
            break;
        };
        names.insert(clean[start..start + len].to_string());
        cursor = start + len;
    }
    names
}

/// 生产 Rust 里出现过 `ServerMessage::<变体>` 的那些变体。
///
/// 人口是整棵工作树的 crate 代码（`crates/` 下每个 `.rs`，夹具在内）：这一条问的
/// 是「这棵树里还有没有第二处构造」，不是「发出去的镜像里有没有」——夹具里构造一份
/// 同样是这个判据要认的现场。
fn constructed(files: &[PathBuf]) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for path in files {
        let text = read(path);
        let mut cursor = 0;
        while let Some(at) = text[cursor..].find("ServerMessage::") {
            let start = cursor + at + "ServerMessage::".len();
            let name: String = text[start..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            cursor = start + name.len().max(1);
            if !name.is_empty() {
                found.insert(name);
            }
        }
    }
    found
}

/// 一份树上的全部判词。`judged` 是判过的变体数，用来防「一条都没判」。
fn contradictions(root: &Path, files: &[PathBuf]) -> (Vec<String>, usize) {
    let variants = enum_variants(&read(&root.join(PROTOCOL)), "pub enum ServerMessage");
    let mut complaints = Vec::new();
    let client_path = root.join(CLIENT);
    if !client_path.exists() {
        return (complaints, 0);
    }
    let client = read(&client_path);
    let waits = dispatched(&client);
    let builds = constructed(files);

    for wire in &waits {
        if !variants.values().any(|name| name == wire) {
            complaints.push(format!(
                "{CLIENT} 在等 type === '{wire}'，而 ServerMessage 里没有这个线上名字（它有 \
                 {:?}）：这一支永远不会进，帧到了也只是一条认不出的消息",
                variants.values().collect::<Vec<_>>()
            ));
        }
    }

    let mut judged = 0;
    for (variant, wire) in &variants {
        judged += 1;
        let wanted = waits.contains(wire);
        let excused = IGNORED.iter().find(|(name, _)| name == variant);
        match (wanted, excused) {
            (true, Some(_)) => complaints.push(format!(
                "{variant} 既在客户端分派（'{wire}'），又登记在 IGNORED 里说可以不理：\
                 两者只能有一个是真的"
            )),
            (true, None) => {
                if !builds.contains(variant) {
                    complaints.push(format!(
                        "{CLIENT} 在等 '{wire}'（{variant}），而整棵树里没有一处构造 \
                         ServerMessage::{variant}：客户端这一支等的是一个永远不会来的帧"
                    ));
                }
            }
            (false, None) => complaints.push(format!(
                "ServerMessage::{variant}（'{wire}'）会发到客户端，而客户端既不分派它，\
                 也没在 IGNORED 里写明为什么可以不理解。不理一个真实存在的帧，与不认识\
                 它是同一种表现：画面什么都没变"
            )),
            (false, Some(_)) => {}
        }
    }

    for (variant, _) in IGNORED {
        if !variants.contains_key(*variant) {
            complaints.push(format!(
                "IGNORED 里登记着 {variant}，而 ServerMessage 已经没有这个变体：\
                 登记表在替一个不存在的东西说话"
            ));
        }
    }
    (complaints, judged)
}

/// `AgentEvent` 的线上名字集合：`AgentEvent::X { .. } => "name"` 一行一条。
fn agent_wire_names(text: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for line in text.lines() {
        let Some(at) = line.find("AgentEvent::") else {
            continue;
        };
        let Some(arrow) = line[at..].find("=>") else {
            continue;
        };
        let tail = &line[at + arrow..];
        let Some(open) = tail.find('"') else { continue };
        let Some(close) = tail[open + 1..].find('"') else {
            continue;
        };
        names.insert(tail[open + 1..open + 1 + close].to_string());
    }
    names
}

/// 聊天帧的名字：`chat.rs` 生产半里所有 `"message.…"` 字面量。
fn chat_frames(text: &str) -> BTreeSet<String> {
    let clean = strip_comments(text);
    let mut names = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find("\"message.") {
        let start = cursor + at + 1;
        let Some(len) = clean[start..].find('"') else {
            break;
        };
        names.insert(clean[start..start + len].to_string());
        cursor = start + len;
    }
    names
}

/// `['a', 'b'].includes(msg.type)` 里那一串字面量。
///
/// `needle` 要一直写到 `.includes(…)` 的被判对象：页面上另有一张三字母时区的
/// 名单也用 `.includes(` 判定，只认 `].includes(` 就会读错那一张。
fn literal_list(text: &str, needle: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let Some(at) = text.find(needle) else {
        return names;
    };
    let Some(open) = text[..at].rfind('[') else {
        return names;
    };
    for (index, part) in text[open..at].split('\'').enumerate() {
        if index % 2 == 1 && !part.trim().is_empty() {
            names.insert(part.trim().to_string());
        }
    }
    names
}

/// 页面脚本里所有 `case '…':` 的名字。
fn case_labels(text: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("case '") {
        let start = cursor + at + "case '".len();
        let Some(len) = text[start..].find('\'') else {
            break;
        };
        names.insert(text[start..start + len].to_string());
        cursor = start + len;
    }
    names
}

#[test]
fn every_field_a_client_sends_is_one_its_variant_parses() {
    let root = repo_root();
    let protocol = read(&root.join(PROTOCOL));
    let variants = enum_variants(&protocol, "pub enum ClientMessage");
    let fields = variant_fields(&protocol, "pub enum ClientMessage");
    let by_wire: BTreeMap<&String, &String> = variants.iter().map(|(v, w)| (w, v)).collect();

    assert_eq!(
        fields.len(),
        variants.len(),
        "变体与字段两张表读出来的变体数不一样：读取器有一条读残了"
    );

    let sent = all_sent_objects(&root);
    assert!(
        sent.len() >= 4,
        "只读到 {} 条上行消息对象，太少：发送点的写法变了，这条判据会靠「找不到题目」通过",
        sent.len()
    );

    let mut complaints = Vec::new();
    let mut judged = 0;
    for (wire, keys) in &sent {
        // 名字对不上由另一条判据报，这里不重复。
        let Some(variant) = by_wire.get(wire) else {
            continue;
        };
        let Some(carried) = fields.get(*variant) else {
            continue;
        };
        judged += 1;
        let extra: Vec<&String> = keys
            .difference(carried)
            .filter(|key| key.as_str() != "type")
            .collect();
        if !extra.is_empty() {
            let mut names: Vec<&str> = extra.iter().map(|s| s.as_str()).collect();
            names.sort_unstable();
            complaints.push(format!(
                "客户端发 '{wire}' 时带上 {names:?}，而 ClientMessage::{variant} 只认 {carried:?}：\
                 serde 不认识的键被丢掉，服务端读到的是默认值——请求发出去了，缺的那块像没填"
            ));
        }
    }
    assert!(
        judged >= 4,
        "只判到 {judged} 条上行消息，太少：读取器没认出客户端发的是什么"
    );
    assert!(
        complaints.is_empty(),
        "上行消息的字段名两边写的不是同一份：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_object_reader_sees_flat_keys_and_ignores_nested_ones() {
    let source = "\
ws.send(JSON.stringify({ type: 'subscribe', channels: ['agent:*', 'task:*'], timestamp: at }));

ws.send(JSON.stringify({
  type: 'chat_message',
  session_id: 'default',
  content: v,
}));

ws.send(JSON.stringify(payload));
";
    let found = sent_objects(source);
    assert_eq!(found.len(), 2, "{found:#?}");
    assert_eq!(found[0].0, "subscribe");
    assert_eq!(
        found[0].1,
        BTreeSet::from([
            "type".to_string(),
            "channels".to_string(),
            "timestamp".to_string()
        ])
    );
    assert_eq!(found[1].0, "chat_message");
    assert_eq!(
        found[1].1,
        BTreeSet::from([
            "type".to_string(),
            "session_id".to_string(),
            "content".to_string()
        ])
    );
}

#[test]
fn a_sent_key_its_variant_does_not_parse_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-sent-keys-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");
    std::fs::create_dir_all(root.join("web/src")).expect("temp web");

    std::fs::write(
        root.join(PROTOCOL),
        "#[derive(Debug, Deserialize)]\n#[serde(tag = \"type\")]\npub enum ClientMessage {\n    \
         #[serde(rename = \"ping\")]\n    Ping {\n        #[serde(default)]\n        timestamp: \
         Option<String>,\n        seq: u64,\n    },\n}\n",
    )
    .expect("protocol");
    std::fs::write(
        root.join("web/src/socket.ts"),
        "ws.send(JSON.stringify({ type: 'ping', seq: 1, gost: 2 }));\n",
    )
    .expect("socket");

    let protocol = read(&root.join(PROTOCOL));
    let fields = variant_fields(&protocol, "pub enum ClientMessage");
    assert_eq!(
        fields.get("Ping").cloned().unwrap_or_default(),
        BTreeSet::from(["timestamp".to_string(), "seq".to_string()])
    );

    let sent = all_sent_objects(&root);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].0, "ping");
    let extra: Vec<&String> = sent[0]
        .1
        .difference(fields.get("Ping").expect("ping"))
        .filter(|key| key.as_str() != "type")
        .collect();
    assert_eq!(extra, vec!["gost"], "{extra:?}");

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn the_envelope_vocabulary_is_the_same_on_both_ends() {
    let root = repo_root();
    let (complaints, judged) = contradictions(&root, &all_crate_files(&root));
    assert!(judged > 0, "一个变体都没判到，这条判据没有题目");
    assert!(
        complaints.is_empty(),
        "套接字信封的名字两边对不上，或者有一边根本没有写者：\n{}",
        complaints.join("\n")
    );
}

/// `<x>.send( … )` 括号里出现的 `type: '<name>'`。
///
/// 只认 `send(` 的实参：同一个仓里 `type: '…'` 还有别的意思（合成一帧事件、
/// 画一条拓扑线、发一条 REST 提示），那些字面量与套接字上行无关，收进来只会
/// 让判据报假的。
fn sent_types(text: &str) -> BTreeSet<String> {
    let clean = strip_comments(text);
    let bytes = clean.as_bytes();
    let mut names = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find(".send(") {
        let open = cursor + at + ".send(".len();
        let mut depth = 1usize;
        let mut i = open;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        let args = &clean[open..i.saturating_sub(1).max(open)];
        cursor = i.max(open + 1);
        for prefix in ["type: '", "type: \""] {
            let mut inner = 0;
            while let Some(found) = args[inner..].find(prefix) {
                let start = inner + found + prefix.len();
                let Some(len) = args[start..].find(['\'', '"']) else {
                    break;
                };
                names.insert(args[start..start + len].to_string());
                inner = start + len;
            }
        }
    }
    names
}

/// `ClientMessage` 每个变体在线上认哪些字段名。
///
/// 与 `enum_variants` 同一种行读法：变体头在四格缩进上，字段在八格；`#[serde(...)]`
/// 属性行跳过。字段名没有改名的——上行对象的键就是字段名。
fn variant_fields(text: &str, header: &str) -> BTreeMap<String, BTreeSet<String>> {
    let start = text
        .find(header)
        .unwrap_or_else(|| panic!("{PROTOCOL} 里没有 {header}，这条判据没有题目"));
    let mut fields: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in text[start..].lines().skip(1) {
        if line.starts_with('}') {
            break;
        }
        let trimmed = line.trim_start();
        let head = line
            .strip_prefix("    ")
            .is_some_and(|rest| !rest.starts_with(' '));
        if head {
            let name: String = trimmed
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            if name.starts_with(|c: char| c.is_ascii_uppercase()) {
                fields.insert(name.clone(), BTreeSet::new());
                current = Some(name);
            } else {
                current = None;
            }
            continue;
        }
        let Some(variant) = current.as_ref() else {
            continue;
        };
        let field: String = trimmed
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !field.is_empty()
            && field.starts_with(|c: char| c.is_ascii_lowercase())
            && trimmed[field.len()..].trim_start().starts_with(':')
        {
            fields
                .get_mut(variant)
                .expect("just inserted")
                .insert(field);
        }
    }
    fields
}

/// 一个对象字面量体（两个花括号之间）在**顶层**的那些键。
///
/// 按顶层逗号切开，每段取开头那个 `名字:`。嵌套的对象、数组、模板串都在深度里，
/// 不会漏进来——`channels: ['agent:*']` 里的引号内容不是一个键。
fn top_level_keys(body: &str) -> BTreeSet<String> {
    let bytes = body.as_bytes();
    let mut keys = BTreeSet::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut i = 0usize;
    let mut parts: Vec<&str> = Vec::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
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
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(&body[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&body[start.min(bytes.len())..]);
    for part in parts {
        let trimmed = part.trim_start();
        let key: String = trimmed
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !key.is_empty()
            && key.starts_with(|c: char| c.is_ascii_alphabetic())
            && trimmed[key.len()..].trim_start().starts_with(':')
        {
            keys.insert(key);
        }
    }
    keys
}

/// 一个 `send( … )` 实参里带 `type: '<名字>'` 的那些对象字面量：名字与它的键。
///
/// 只认真正带 `type` 的那一层：`send(payload)` 这种把整条消息交给上层拼的写法
/// 在这里读不出名字，也就不判——跳过而不是猜。
fn sent_objects_in(args: &str) -> Vec<(String, BTreeSet<String>)> {
    let bytes = args.as_bytes();
    let mut out = Vec::new();
    let mut stack: Vec<usize> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[i];
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
                continue;
            }
            b'{' => stack.push(i),
            b'}' => {
                if let Some(open) = stack.pop() {
                    let inner = &args[open + 1..i];
                    let keys = top_level_keys(inner);
                    if let Some(wire) = quoted_type(inner) {
                        out.push((wire, keys));
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    out
}

/// 对象体里顶层 `type: '<名字>'` 的那个名字。
fn quoted_type(body: &str) -> Option<String> {
    let at = body.find("type:")?;
    let rest = body[at + "type:".len()..].trim_start();
    let quote = rest.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let after = &rest[1..];
    let end = after.find(quote)?;
    Some(after[..end].to_string())
}

/// 两个客户端发出去的全部上行消息：名字 → 它带的键。
fn all_sent_objects(root: &Path) -> Vec<(String, BTreeSet<String>)> {
    let mut found = Vec::new();
    let mut files = Vec::new();
    collect_ts(&root.join("web/src"), &mut files);
    for path in files {
        found.extend(sent_objects(&read(&path)));
    }
    let page = root.join(TAKEOVER);
    if page.exists() {
        found.extend(sent_objects(&read(&page)));
    }
    found
}

/// 每个 `send(` 实参里带 `type` 的对象字面量。
fn sent_objects(text: &str) -> Vec<(String, BTreeSet<String>)> {
    let clean = strip_comments(text);
    let bytes = clean.as_bytes();
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(at) = clean[cursor..].find(".send(") {
        let open = cursor + at + ".send(".len();
        let mut depth = 1usize;
        let mut i = open;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        let args = &clean[open..i.saturating_sub(1).max(open)];
        cursor = i.max(open + 1);
        out.extend(sent_objects_in(args));
    }
    out
}

/// 两个客户端发出去的全部上行消息名字。
fn all_sent(root: &Path) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut files = Vec::new();
    collect_ts(&root.join("web/src"), &mut files);
    for path in files {
        names.extend(sent_types(&read(&path)));
    }
    let page = root.join(TAKEOVER);
    if page.exists() {
        names.extend(sent_types(&read(&page)));
    }
    names
}

fn collect_ts(dir: &Path, out: &mut Vec<PathBuf>) {
    // 读不出来就 panic：递归遍历里跳过一个子目录不会让任何断言变红，而剩下的那份
    // 普查往往仍高于下限——「少读了一块」与「那一块都合规」在判词上分不开。
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!(
            "{} 读不出来（{e}）：这棵树的普查面少了一块，而用剩下的那份判出来的\
             结论与「整片区都合规」同形",
            dir.display()
        )
    });
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_ts(&path, out);
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("ts") | Some("tsx")
        ) {
            out.push(path);
        }
    }
}

#[test]
fn every_message_a_client_sends_is_one_the_gateway_parses() {
    let root = repo_root();
    let variants = enum_variants(&read(&root.join(PROTOCOL)), "pub enum ClientMessage");
    let wires: BTreeSet<&String> = variants.values().collect();
    let sent = all_sent(&root);

    assert!(
        variants.len() >= 4,
        "只读出 {} 个 ClientMessage 变体，读取器掉线了",
        variants.len()
    );
    assert!(
        sent.len() >= 4,
        "只读出 {} 个客户端上行名字（{sent:?}）：上行读取器没在看发送点",
        sent.len()
    );

    let mut complaints = Vec::new();
    for name in &sent {
        if !wires.contains(name) {
            complaints.push(format!(
                "客户端发 type: '{name}'，而 ClientMessage 不认识这个名字（它认 {wires:?}）：\
                 服务端解析失败，这一条请求消失在没有回执的地方"
            ));
        }
    }
    for (variant, wire) in &variants {
        if !sent.contains(wire) {
            complaints.push(format!(
                "ClientMessage::{variant}（'{wire}'）没有任何客户端发：解析它的那条路、\
                 它带来的回执，都是只为一条不存在的消息写的"
            ));
        }
    }
    assert!(
        complaints.is_empty(),
        "上行消息的名字两边对不上，或者有一边根本没有发送者：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_takeover_page_speaks_only_names_the_gateway_writes() {
    let root = repo_root();
    let page = read(&root.join(TAKEOVER));
    let envelopes: BTreeSet<String> =
        enum_variants(&read(&root.join(PROTOCOL)), "pub enum ServerMessage")
            .into_values()
            .collect();
    let frames = chat_frames(&production(&root.join(CHAT)));
    let events = agent_wire_names(&read(&root.join(EVENT_WIRE)));

    assert_eq!(
        frames.len(),
        7,
        "只读出 {} 个聊天帧名字（{frames:?}）：写者是 chat.rs 的 envelope 调用，读不出七个\
         说明读取器与那边的写法已经对不上",
        frames.len()
    );
    assert!(
        events.len() >= 20,
        "只读出 {} 个 agent 事件的线上名字，读取器掉线了",
        events.len()
    );

    let mut complaints = Vec::new();
    let envelope_list = literal_list(&page, "].includes(msg.type)");
    assert!(
        envelope_list.len() >= 2,
        "只从 {TAKEOVER} 里读出 {} 个信封名字（{envelope_list:?}）：页面换了判法，\
         这条判据会靠「找不到题目」通过",
        envelope_list.len()
    );
    for wire in envelope_list {
        if !envelopes.contains(&wire) {
            complaints.push(format!(
                "{TAKEOVER} 把 '{wire}' 当成信封收，而 ServerMessage 没有这个线上名字\
                 （它有 {envelopes:?}）：这一条永远不会命中"
            ));
        }
    }
    let labels = case_labels(&page);
    assert!(
        labels.len() >= 15,
        "只读出 {} 个 case 标签，太少：页面脚本改了写法，这条判据会靠\
         「找不到题目」通过",
        labels.len()
    );
    for label in &labels {
        if label.contains('.') {
            // 带点的是聊天帧：`message.start` 这一族由 chat.rs 发。
            if !frames.contains(label) {
                complaints.push(format!(
                    "{TAKEOVER} 有 case '{label}'，而 chat.rs 不发这个帧名字（它发 {frames:?}）"
                ));
            }
        } else if !events.contains(label) && !envelopes.contains(label) {
            complaints.push(format!(
                "{TAKEOVER} 有 case '{label}'，而它既不是 agent 事件的线上名字，也不是信封名：\
                 这一支永远不会进"
            ));
        }
    }
    assert!(
        complaints.is_empty(),
        "接管页与网关各拼了一遍词表，拼出来的不是同一份：\n{}",
        complaints.join("\n")
    );
}

/// 生产半：`#[cfg(test)]` 之后的一半是标本，不是部署事实。
fn production(path: &Path) -> String {
    common::rust_production::production_text(&read(path))
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let variants = enum_variants(&read(&root.join(PROTOCOL)), "pub enum ServerMessage");
    assert!(
        variants.len() >= 5,
        "只读出 {} 个 ServerMessage 变体，读取器掉线了",
        variants.len()
    );
    for known in ["Pong", "AgentEvent", "Notification"] {
        assert!(
            variants.contains_key(known),
            "{known} 不在读出来的变体里：读取器与 {PROTOCOL} 的写法已经对不上"
        );
    }
    let waits = dispatched(&read(&root.join(CLIENT)));
    assert!(
        waits.len() >= 3,
        "只读出 {} 个客户端在等的信封名字，太少：客户端换了写法（比如改成 switch），\
         这条判据就会靠「找不到题目」通过",
        waits.len()
    );
    let builds = constructed(&all_crate_files(&root));
    assert!(
        builds.len() >= 5,
        "只找到 {} 处 ServerMessage 构造，太少：构造普查没在看生产代码",
        builds.len()
    );
}

#[test]
fn the_reader_sees_variants_renames_and_client_waits() {
    let server = "\
#[derive(Debug, Serialize)]
#[serde(tag = \"type\")]
pub enum ServerMessage {
    #[serde(rename = \"pong\")]
    Pong {
        seq: u64,
    },
    /// 一个注释不影响读取。
    #[serde(rename = \"agent_event\")]
    AgentEvent {
        event_id: String,
        payload: serde_json::Value,
    },
}
";
    let variants = enum_variants(server, "pub enum ServerMessage");
    assert_eq!(variants["Pong"], "pong");
    assert_eq!(variants["AgentEvent"], "agent_event");

    let client = "\
if (message.type === 'pong') return;
// 注释里的 type === 'ghost' 不算。
if (message.type === 'agent_event') return;
";
    assert_eq!(
        dispatched(client),
        BTreeSet::from(["agent_event".to_string(), "pong".to_string()])
    );
}

#[test]
fn a_wait_nothing_sends_and_a_frame_nobody_reads_are_both_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-envelope-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");
    std::fs::create_dir_all(root.join("crates/demo/src")).expect("temp crate");
    std::fs::create_dir_all(root.join("web/src/transport")).expect("temp web");

    std::fs::write(
        root.join(PROTOCOL),
        "#[derive(Debug, Serialize)]\n#[serde(tag = \"type\")]\npub enum ServerMessage {\n    \
         #[serde(rename = \"pong\")]\n    Pong {\n        seq: u64,\n    },\n    \
         #[serde(rename = \"agent_event\")]\n    AgentEvent {\n        payload: serde_json::Value,\n    \
         },\n    #[serde(rename = \"kick\")]\n    Kick {\n        reason: String,\n    },\n}\n",
    )
    .expect("protocol");
    std::fs::write(
        root.join("crates/demo/src/lib.rs"),
        "pub fn a() -> ServerMessage {\n    ServerMessage::Pong { seq: 1 }\n}\n\
         pub fn b() -> ServerMessage {\n    ServerMessage::AgentEvent { payload: Default::default() }\n}\n",
    )
    .expect("lib");
    std::fs::write(
        root.join(CLIENT),
        "export function f(message: { type: string }) {\n  if (message.type === 'pong') return;\n  \
         if (message.type === 'agent_event') return;\n  if (message.type === 'task_update') \
         return;\n}\n",
    )
    .expect("client");

    let (complaints, judged) = contradictions(&root, &crate_files_of(&root));
    assert_eq!(judged, 3, "{complaints:?}");
    let joined = complaints.join("\n");
    // 客户端在等一个不存在的名字。
    assert!(joined.contains("task_update"), "{joined}");
    // 服务端会发、客户端不理、也没登记。
    assert!(joined.contains("ServerMessage::Kick"), "{joined}");

    // 登记之后只剩一条：登记表与分派不能同时成立。
    std::fs::write(
        root.join(CLIENT),
        "export function f(message: { type: string }) {\n  if (message.type === 'pong') return;\n  \
         if (message.type === 'agent_event') return;\n  if (message.type === 'kick') return;\n}\n",
    )
    .expect("client");
    let (complaints, _) = contradictions(&root, &crate_files_of(&root));
    let joined = complaints.join("\n");
    assert!(!joined.contains("task_update"), "{joined}");
    assert!(
        joined.contains("ServerMessage::Kick") && joined.contains("IGNORED"),
        "{joined}"
    );

    // 名字对上了但没人构造：客户端那一支等的是一个不会来的帧。
    std::fs::write(
        root.join("crates/demo/src/lib.rs"),
        "pub fn a() -> ServerMessage {\n    ServerMessage::Pong { seq: 1 }\n}\n\
         pub fn c() -> ServerMessage {\n    ServerMessage::Kick { reason: String::new() }\n}\n",
    )
    .expect("lib");
    let (complaints, _) = contradictions(&root, &crate_files_of(&root));
    assert!(
        complaints.join("\n").contains("永远不会来的帧"),
        "{complaints:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}
