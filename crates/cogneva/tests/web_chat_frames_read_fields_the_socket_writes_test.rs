//! principle: feedback_no_hardcoding_config_surface
//!
//! 聊天流是第三种帧：外层仍是 `agent_event` 信封，内层才是
//! `{"type": "message.text_delta", "message_id": …, "delta": …}` 这一族。写它的是
//! `crates/cog-gateway/src/chat.rs` 的 `emit(…, "<帧名>", json!({ … }))`，读它的有两
//! 个客户端：WebUI 的 `web/src/types/events.ts` 和网关二进制里内嵌的运维页。三方各
//! 拼一遍字段名，中间没有东西比对。
//!
//! 帧名那一半已经有判据（运维页的 `case` 标签对上 `chat.rs` 发的那几个名字）。
//! 这份判的是**字段**那一半：客户端从帧里读的每一个字段，都必须是某个聊天帧真的
//! 写出来的键。读一个没人写的字段，拿到的是 `undefined`——界面上那一格永远空着，
//! 和「这一轮没有内容」长得一模一样。
//!
//! 判一个方向，且是钝的：字段只要被**任意一个**帧写过就算过。`role` 只有
//! `message.start` 写，一段 `message.text_delta` 的臂里读 `role` 也能过——这一层
//! 判据不区分是哪一帧，它只保证「这个词在聊天面上真实存在过」。要逐帧精确，得先把
//! `switch` 的分支与 `emit` 的调用对上，那是另一条判据。
//!
//! 实测（2026-10-27）：写者一侧读到 7 个帧名（start / text_delta / end / goal /
//! thinking_start / thinking_delta / thinking_end），键合并后 4 个（`message_id`、
//! `role`、`delta`、`goal_id`）；两个客户端读的字段全部在这 4 个里。消融：把
//! `chat.rs` 七个 `emit` 载荷里的键 `message_id` 全改成 `mid`，本门红并同时点名
//! `web/src/types/events.ts` 与 `crates/cog-gateway/src/takeover.html`；改回即绿。
//! 只改其中一个帧不会红——那正是上面说的「钝」：`delta` 被两个帧写着，改掉一个
//! 另一个还在写。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

mod common;
use common::js_literal::{quoted_word, split_top_level, top_level_keys};
use common::routes::{matching_brace, matching_paren, read, repo_root, strip_comments};
use common::rust_production::production_text;

const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const GATEWAY: &str = "crates/cog-gateway/src";
const WEB: &str = "web/src/types/events.ts";
const TAKEOVER: &str = "crates/cog-gateway/src/takeover.html";
/// Both clients name the frame body `inner`, and both read it as a field bag.
const READER_VAR: &str = "inner";

/// Every chat frame the gateway emits, as name → the keys it carries.
///
/// The writer is one helper call: `emit(out, session_id, "<name>",
/// serde_json::json!({ … }))`. A frame whose payload is built some other way
/// contributes no keys, and the client reads it would show up as fields nobody
/// writes rather than as a quiet pass.
fn emitted_frames(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut frames: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("emit(") {
        let start = cursor + at;
        cursor = start + "emit".len();
        // `emit(` must not be the tail of `emit_delta(`, `emitted(` or a
        // method called `.emit(`: only the bare helper writes a frame name and
        // a payload object side by side.
        let before = text[..start].chars().next_back().unwrap_or(' ');
        // `-` counts as part of the name too: `.bar-inner.ok` in the page's
        // stylesheet is a class selector, not a read off the frame body.
        if before.is_alphanumeric() || before == '_' || before == '.' || before == '-' {
            continue;
        }
        let open = start + "emit".len();
        let Some(close) = matching_paren(text.as_bytes(), open) else {
            break;
        };
        cursor = close + 1;
        let arguments = split_top_level(&text[open + 1..close]);
        let Some(name) = arguments.get(2).and_then(|a| quoted_word(a)) else {
            continue;
        };
        let Some(payload) = arguments.get(3) else {
            continue;
        };
        let keys = frames.entry(name).or_default();
        if let Some(at) = payload.find("json!(") {
            let brace = match payload[at..].find('{') {
                Some(offset) => at + offset,
                None => continue,
            };
            let Some(end) = matching_brace(payload.as_bytes(), brace) else {
                continue;
            };
            if let Some(keys_in) = top_level_keys(&payload[brace + 1..end]) {
                keys.extend(keys_in);
            }
        }
    }
    frames
}

/// The fields a client reads off the frame body: `inner.<name>`.
fn read_fields(text: &str) -> BTreeSet<String> {
    let needle = format!("{READER_VAR}.");
    let mut fields = BTreeSet::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find(&needle) {
        let start = cursor + at;
        cursor = start + needle.len();
        let before = text[..start].chars().next_back().unwrap_or(' ');
        // `-` counts as part of the name too: `.bar-inner.ok` in the page's
        // stylesheet is a class selector, not a read off the frame body.
        if before.is_alphanumeric() || before == '_' || before == '.' || before == '-' {
            continue;
        }
        let name: String = text[cursor..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        if !name.is_empty() {
            fields.insert(name);
        }
    }
    fields
}

/// The fields each client reads, keyed by file.
fn client_reads(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut reads = BTreeMap::new();
    for relative in [WEB, TAKEOVER] {
        let path = root.join(relative);
        let text = strip_comments(&read(&path));
        let fields = read_fields(&text);
        if !fields.is_empty() {
            reads.insert(relative.to_string(), fields);
        }
    }
    reads
}

fn contradictions(root: &Path) -> (Vec<String>, BTreeMap<String, BTreeSet<String>>) {
    let frames = emitted_frames(&production_text(&read(&root.join(CHAT))));
    let written: BTreeSet<String> = frames.values().flatten().cloned().collect();
    let reads = client_reads(root);
    let mut complaints = Vec::new();
    for (file, fields) in &reads {
        for field in fields {
            if written.contains(field) {
                continue;
            }
            complaints.push(format!(
                "  {file} 读帧里的 `{field}`，而聊天帧只写 {written:?}：读到的是 \
                 undefined，那一格永远空着——与「这一轮没有内容」同一种读数"
            ));
        }
    }
    (complaints, frames)
}

#[test]
fn every_field_a_client_reads_off_a_chat_frame_is_one_the_socket_writes() {
    let (complaints, frames) = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "客户端从聊天帧里读的字段，发帧的那一侧没有写。读一个不存在的字段不会报错，\
         只会一直读到 undefined：\n{}\n（帧与键：{frames:?}）",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let (_, frames) = contradictions(&root);
    assert!(
        frames.len() >= 7,
        "只读出 {} 个聊天帧，读取器看错地方了：{frames:?}",
        frames.len()
    );
    let written: BTreeSet<String> = frames.values().flatten().cloned().collect();
    assert!(
        written.len() >= 4,
        "帧里的键只读出 {} 个，太少：{written:?}",
        written.len()
    );
    let reads = client_reads(&root);
    assert!(
        reads.len() >= 2,
        "只读到 {} 个客户端读帧的地方：读取器掉线了",
        reads.len()
    );
    let total: usize = reads.values().map(BTreeSet::len).sum();
    assert!(total >= 6, "两个客户端合计只读到 {total} 个字段，太少");
}

#[test]
fn the_writer_reader_sees_names_payloads_and_ignores_the_other_helpers() {
    let text = "\
async fn a(out: &Sender) {
    emit(out, &sid, \"message.start\", serde_json::json!({ \"message_id\": id, \"role\": role })).await;
    emit(out, &sid, \"message.end\", serde_json::json!({ \"message_id\": id })).await;
    emit_delta(out, &sid, &mid, \"text\").await;
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn fixture() {
        emit(out, &sid, \"message.ghost\", serde_json::json!({ \"ghost\": 1 })).await;
    }
}
";
    let frames = emitted_frames(&production_text(text));
    assert_eq!(
        frames.keys().cloned().collect::<Vec<_>>(),
        vec!["message.end".to_string(), "message.start".to_string()]
    );
    assert_eq!(
        frames["message.start"],
        BTreeSet::from(["message_id".to_string(), "role".to_string()])
    );
}

#[test]
fn a_field_no_frame_writes_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    std::fs::create_dir_all(root.join(GATEWAY)).expect("gateway dir");
    std::fs::create_dir_all(root.join("web/src/types")).expect("web dir");
    std::fs::write(
        root.join(CHAT),
        "\
async fn a(out: &Sender) {
    emit(out, &sid, \"message.text_delta\", serde_json::json!({ \"message_id\": id, \"delta\": d })).await;
}
",
    )
    .expect("chat");
    std::fs::write(
        root.join(WEB),
        "const messageId = inner.message_id;\nconst text = inner.delta;\n",
    )
    .expect("web");
    std::fs::write(
        root.join(TAKEOVER),
        "case 'message.text_delta':\n  if (inner.message_id) append(inner.ghost_field);\n",
    )
    .expect("page");

    let (complaints, frames) = contradictions(root);
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("ghost_field"), "{complaints:?}");
    assert!(complaints[0].contains(TAKEOVER), "{complaints:?}");

    std::fs::write(
        root.join(TAKEOVER),
        "case 'message.text_delta':\n  if (inner.message_id) append(inner.delta);\n",
    )
    .expect("page");
    let (complaints, _) = contradictions(root);
    assert!(complaints.is_empty(), "{complaints:?}");
}
