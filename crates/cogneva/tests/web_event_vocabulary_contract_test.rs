//! principle: feedback_no_hardcoding_config_surface
//! The event stream is a closed vocabulary written on one side and filtered on
//! the other. The server names each `AgentEvent` variant once, on the `type`
//! tag; the WebUI keeps the same names in a union and in a set, and
//! `isEventType` drops anything outside that set. A name the server starts
//! writing and the client has not learned is therefore not an error anywhere:
//! the frame arrives, the guard says no, and the panel renders exactly what it
//! renders when nothing happened. The reverse is the same fault wearing the
//! other face — a name the client still keeps after the server stopped writing
//! it is a filter that can never match, which reads as a feature that has gone
//! quiet rather than as a stale copy.
//!
//! The Rust half of this pairing is already pinned at run time: every variant
//! is serialized and compared with the stored name. What has no counterpart is
//! the cross-language hop, which is why it is read out of the source here — the
//! client is TypeScript and no compiler in this tree sees both sides.
//!
//! The chat stream carries a second, different table: the replies inside an
//! `agent_event` envelope name themselves `message.start` / `message.text_delta`
//! / `message.end`, and the client keeps them in a map of its own precisely
//! because they are not `AgentEvent` variants. That family was once dropped
//! whole — the reply was on the bus the entire time and the screen showed
//! nothing — so both tables are read here, each against the file that writes it.
//!
//! 实测（2026-10-27）：agent 事件两侧各 20 个名字、聊天帧两侧各 7 个，逐一相符。
//! 消融对照两条：把客户端 `'resource_changed'` 改成 `'resource_change'`，
//! 正向红；把服务端 `resource_changed` 那一臂改掉，反向红。

use std::collections::BTreeSet;

use crate::common;

use common::routes::read_repo;

use common::rust_production::production_lines;

/// The names the server writes on the `type` tag.
///
/// Read from `event_type_name` rather than from the serde attributes: the two
/// are pinned to each other by a run-time test in the same file, so one reader
/// of the pair is enough here, and this one is a plain match whose arms are
/// names.
const SERVER: &str = "crates/cog-storage/src/event_filter.rs";

/// The names the client keeps.
const CLIENT: &str = "web/src/types/events.ts";

/// The string literals in a stretch of text.
fn literals(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('\'') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('\'') else {
            break;
        };
        found.push(after[..end].to_string());
        rest = &after[end + 1..];
    }
    found
}

/// The lines of the stretch starting at `from`, up to and including the first
/// line that contains `until`.
fn span<'a>(text: &'a str, from: &str, until: &str) -> Option<&'a str> {
    let start = text.find(from)? + from.len();
    let rest = &text[start..];
    let end = rest.find(until)? + until.len();
    Some(&rest[..end])
}

/// The names in `event_type_name`'s match, and whether it ends with a wildcard
/// arm — a wildcard would make the census see only the named arms and call the
/// remainder an empty set, which is exactly the quiet direction.
fn server_names(text: &str) -> (BTreeSet<String>, bool) {
    let Some(start) = text.find("pub fn event_type_name") else {
        panic!("{SERVER} 里没有 event_type_name，这条判据没有题目");
    };
    let mut names = BTreeSet::new();
    let mut wildcard = false;
    for line in text[start..].lines() {
        // The function closes with a brace in the first column; the match's own
        // closing brace is indented.
        if line.starts_with('}') {
            break;
        }
        let Some(at) = line.find("=>") else {
            continue;
        };
        // 臂的左边是变体路径；不是路径的那一支（`_` 或一个绑定名）会把没写到的
        // 变体收进一个看不见的默认分支，名单就读不全了。
        if !line[..at].contains("::") {
            wildcard = true;
        }
        let arm = line[at + 2..].trim();
        if let Some(rest) = arm.strip_prefix('"') {
            if let Some(end) = rest.find('"') {
                names.insert(rest[..end].to_string());
            }
        }
    }
    (names, wildcard)
}

/// The names in the client's union type.
fn client_union(text: &str) -> BTreeSet<String> {
    let body = span(text, "export type EventType =", ";")
        .unwrap_or_else(|| panic!("{CLIENT} 里没有 export type EventType，这条判据没有题目"));
    literals(body).into_iter().collect()
}

/// The names in the client's `KNOWN_TYPES` set — the one `isEventType` filters
/// by, which is what actually decides whether a frame reaches a panel.
fn client_known(text: &str) -> BTreeSet<String> {
    let body = span(text, "const KNOWN_TYPES", "]);")
        .unwrap_or_else(|| panic!("{CLIENT} 里没有 KNOWN_TYPES，这条判据没有题目"));
    literals(body).into_iter().collect()
}

/// The names one side carries and the other does not.
fn only_in(left: &BTreeSet<String>, right: &BTreeSet<String>) -> Vec<String> {
    left.difference(right).cloned().collect()
}

#[test]
fn every_event_the_server_writes_is_one_the_client_keeps() {
    let (server, wildcard) = server_names(&read_repo(SERVER));
    let text = read_repo(CLIENT);
    let union = client_union(&text);
    let known = client_known(&text);

    assert!(
        !wildcard,
        "{SERVER} 的 event_type_name 有了通配臂：没写到的变体不会再被这条判据看见，\
         而它们正是会被客户端默默丢掉的那些。"
    );
    assert!(
        server.len() > 15,
        "只读出 {} 个服务端事件名，这条判据没有题目",
        server.len()
    );

    let untyped = only_in(&server, &union);
    assert!(
        untyped.is_empty(),
        "服务端会写这些事件名，客户端的 EventType 里没有：{untyped:?}。\
         这类帧到了浏览器、被 isEventType 判假、然后丢掉——界面既不报错也不留痕，\
         画出来与「这段时间没有事件」一模一样。"
    );

    let undropped = only_in(&server, &known);
    assert!(
        undropped.is_empty(),
        "服务端会写这些事件名，客户端 KNOWN_TYPES 里没有：{undropped:?}。\
         联合类型里有、过滤集合里没有，等于声明了却永远放不进去——同一条帧照样被丢。"
    );
}

#[test]
fn every_event_the_client_keeps_is_one_the_server_writes() {
    let server = server_names(&read_repo(SERVER)).0;
    let text = read_repo(CLIENT);
    let union = client_union(&text);
    let known = client_known(&text);

    assert!(
        union.len() > 15,
        "只读出 {} 个客户端事件名，这条判据没有题目",
        union.len()
    );

    let stale = only_in(&union, &server);
    assert!(
        stale.is_empty(),
        "客户端还认这些事件名，服务端已经一个都不写了：{stale:?}。\
         这两条都是永远匹配不到的分支：某块面板等着一种不会再来的帧，\
         读出来是「这块功能没动静」，而不是「这条名字已经作废」。"
    );

    let stale_known = only_in(&known, &server);
    assert!(
        stale_known.is_empty(),
        "客户端 KNOWN_TYPES 里还有这些服务端不再写的事件名：{stale_known:?}"
    );
}

#[test]
fn the_client_does_not_keep_the_vocabulary_twice() {
    let text = read_repo(CLIENT);
    let union = client_union(&text);
    let known = client_known(&text);
    assert_eq!(
        union, known,
        "客户端的联合类型与 KNOWN_TYPES 不是同一份名字。过滤用的是 KNOWN_TYPES，\
         所以只在联合类型里出现过的名字读起来是「支持了这个事件」，而运行时它仍会被丢掉。"
    );
}

#[test]
fn the_readers_handle_the_shapes_this_tree_writes() {
    let server = "\
/// Return the snake_case type name.
pub fn event_type_name(event: &AgentEvent) -> &str {
    match event {
        AgentEvent::AgentStart { .. } => \"agent_start\",
        AgentEvent::ReActStepStart { .. } => \"react_step_start\",
    }
}

#[cfg(test)]
mod tests { }";
    let (names, wildcard) = server_names(server);
    assert_eq!(
        names,
        BTreeSet::from(["agent_start".to_string(), "react_step_start".to_string()])
    );
    assert!(!wildcard, "这一段没有通配臂");

    let client = "\
export type EventType =
  | 'agent_start'
  | 'react_step_start';

const KNOWN_TYPES: ReadonlySet<string> = new Set<EventType>([
  'agent_start',
]);
";
    assert_eq!(
        client_union(client),
        BTreeSet::from(["agent_start".to_string(), "react_step_start".to_string()])
    );
    assert_eq!(
        client_known(client),
        BTreeSet::from(["agent_start".to_string()])
    );
}

#[test]
fn a_name_missing_on_either_side_is_reported() {
    let server = BTreeSet::from(["agent_start".to_string(), "resource_changed".to_string()]);
    let union = BTreeSet::from(["agent_start".to_string(), "resource_change".to_string()]);

    assert_eq!(
        only_in(&server, &union),
        vec!["resource_changed".to_string()]
    );
    assert_eq!(
        only_in(&union, &server),
        vec!["resource_change".to_string()]
    );

    // 通配臂认出来才算数：它会把没写到的变体藏进一个看不见的默认分支里。
    let (_, wildcard) = server_names(
        "pub fn event_type_name(e: &E) -> &str {\n    match e {\n        E::A => \"a\",\n        _ => \"other\",\n    }\n}\n",
    );
    assert!(wildcard, "通配臂必须被读出来");
}

/// The chat stream's own frame family, as the gateway writes it.
const CHAT_SERVER: &str = "crates/cog-gateway/src/chat.rs";

/// The prefix that makes a literal a chat frame name. The client's map is keyed
/// by exactly these strings.
const CHAT_PREFIX: &str = "message.";

/// The double-quoted literals in a stretch of text — the Rust side's spelling.
fn double_quoted(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find('"') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('"') else {
            break;
        };
        found.push(after[..end].to_string());
        rest = &after[end + 1..];
    }
    found
}

/// The frame names the gateway sends. Comments and test code are not senders,
/// which is why this reads through the shared line filter rather than the raw
/// text: the module doc explains the family in prose full of these very names.
fn server_chat_frames(text: &str) -> BTreeSet<String> {
    production_lines(text)
        .into_iter()
        .flat_map(double_quoted)
        .filter(|literal| literal.starts_with(CHAT_PREFIX))
        .collect()
}

/// The frame names the client maps to a kind.
fn client_chat_frames(text: &str) -> BTreeSet<String> {
    let body = span(text, "const CHAT_FRAME_KINDS", "};")
        .unwrap_or_else(|| panic!("{CLIENT} 里没有 CHAT_FRAME_KINDS，这条判据没有题目"));
    literals(body)
        .into_iter()
        .filter(|literal| literal.starts_with(CHAT_PREFIX))
        .collect()
}

#[test]
fn every_chat_frame_the_gateway_sends_is_one_the_client_maps() {
    let server = server_chat_frames(&read_repo(CHAT_SERVER));
    let client = client_chat_frames(&read_repo(CLIENT));

    assert!(
        server.len() >= 5,
        "只读出 {} 个聊天帧名，这条判据没有题目",
        server.len()
    );
    assert!(
        client.len() >= 5,
        "只读出 {} 个客户端聊天帧，这条判据没有题目",
        client.len()
    );

    let unmapped = only_in(&server, &client);
    assert!(
        unmapped.is_empty(),
        "网关会发这些聊天帧，客户端的 CHAT_FRAME_KINDS 里没有：{unmapped:?}。\
         normalizeChatFrame 认不出就返回 null，整帧丢掉——回复一直在总线上，\
         屏幕上却一条也没有，而这与「对方还没说话」长得一模一样。"
    );

    let unmapped_reverse = only_in(&client, &server);
    assert!(
        unmapped_reverse.is_empty(),
        "客户端还认得这些聊天帧，网关已经不发了：{unmapped_reverse:?}。\
         它们对应的分支永远匹配不到，读出来是「这条功能没动静」。"
    );
}

#[test]
fn the_chat_frame_reader_ignores_prose_and_test_code() {
    let text = "\
//! 说 `message.end` 会把整段聊完。
fn send() { envelope(session, \"message.end\", inner); }
#[cfg(test)]
mod tests { fn t() { envelope(session, \"message.start\", inner); } }";
    assert_eq!(
        server_chat_frames(text),
        BTreeSet::from(["message.end".to_string()]),
        "说明文字与测试代码里的帧名都不是发送点"
    );

    let client = "\
const CHAT_FRAME_KINDS: Record<string, ChatFrameKind> = {
  'message.end': 'end',
  'message.goal': 'goal',
};";
    assert_eq!(
        client_chat_frames(client),
        BTreeSet::from(["message.end".to_string(), "message.goal".to_string()])
    );
}
