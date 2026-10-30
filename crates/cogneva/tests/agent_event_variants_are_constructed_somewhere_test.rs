//! principle: feedback_no_hardcoding_config_surface
//!
//! A wire enum variant is a word spelled twice: once where it is declared, and
//! once where somebody builds one. Everything between those two places — the
//! bus filter, the WAL round trip, the websocket mapping, the WebUI panel —
//! reads the variant and assumes it arrives. Nothing anywhere compares "we
//! declare this" with "we emit this", so a variant that nobody constructs is a
//! whole panel that renders exactly what it renders when nothing happens.
//!
//! The enums read here are the four the event module declares: `AgentEvent`
//! (the bus), `AssistantMessageEvent` (the chat stream, folded by `apply`),
//! `TaskEvent` (task lifecycle) and the legacy `StreamEvent` the SSE parser
//! still answers with.
//!
//! 实测（2026-10-27）：四张词表共 47 个变体，42 个在 `crates/*/src` 的生产
//! 半边有构造点，剩下 5 个都在下面的登记里。其中 `AgentEvent::SelfReview` 与
//! `AgentEvent::ToolExecutionUpdate` 的缺口最要命，因为两侧的消费端都在等着：
//! `web/src/store/streamStore.ts` 有一支 `case 'self_review'`，
//! `crates/cog-reflection/src/detector.rs` 按它的 `status` 派生学习信号，
//! `cog-observability` 拿它的 `agent_id` 归属；后者被
//! `cog-gateway/src/websocket_protocol.rs`、`cog-storage` 的过滤与快照分类。
//! 面板空白与「没有自我复盘发生」读起来一模一样——这正是这条线一直在打的形状。
//! `StreamEvent::Start` 与 `ToolCall` 同样没有构造点，且这一对的缺口还会改变
//! 词的意思：解析器认不出的分片一律落到兜底那一支变成 `TextDelta`，所以
//! 一个工具调用分片读出来是一段文本（见登记）。`AssistantMessageEvent::Usage`
//! 是唯一一个「没有产出端也不骗人」的：折叠时它被有意跳过。
//!
//! What it judges:
//!
//! * every variant declared in one of the four enums is constructed at least
//!   once in the production half of some `crates/*/src` file, unless it is
//!   registered below with the reason its producer is not in this tree;
//! * each registration is still true: the day a producer appears, the row has to
//!   go, or the registry becomes a place where a fixed gap keeps being announced.
//!
//! What it cannot judge:
//!
//! * whether the event is actually published. A construction that is dropped on
//!   the floor counts here; reaching the bus is `emit_event`'s problem, not this
//!   one's;
//! * a construction written inside a macro this scanner does not follow. The
//!   scanner skips `matches!` arguments (those are patterns) and knows nothing
//!   about other macros, so a `macro_rules!` that builds events would read as no
//!   producer at all — the direction that reddens rather than the direction that
//!   goes quiet;
//! * whether a construction is *reached*. A variant built on a branch nothing
//!   takes counts as a producer here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::common;

use common::lex::{split_top_level, strip, Dialect};

use common::routes::{matching_brace, matching_paren, repo_root};

use common::rust_production::{production_sources, production_text, sources_of_tree};

struct WireEnum {
    file: &'static str,
    name: &'static str,
}

/// The enums whose variants are words on a wire. Only the ones that really
/// leave this tree: a `#[cfg(test)]`-only enum has no consumer to deceive.
const WIRE_ENUMS: &[WireEnum] = &[
    WireEnum {
        file: "crates/cog-core/src/types/event.rs",
        name: "AgentEvent",
    },
    WireEnum {
        file: "crates/cog-core/src/types/event.rs",
        name: "AssistantMessageEvent",
    },
    WireEnum {
        file: "crates/cog-core/src/types/event.rs",
        name: "TaskEvent",
    },
    WireEnum {
        file: "crates/cog-core/src/types/event.rs",
        name: "StreamEvent",
    },
];

/// Variants with no producer in this tree, and why that is not a hole this gate
/// can close.
///
/// None of these is a fixture that was left lying around: the two agent events
/// have live consumers that read them and behave differently when they arrive.
/// They are registered rather than fixed because the missing half is the
/// agent's own runtime — the thing that would have to decide to emit a
/// self-review or a partial tool result — and that is a product change, not a
/// vocabulary one. Registering them here is what makes the gap readable on
/// every run instead of only in the `else` branch of a renderer.
///
/// The two `StreamEvent` rows are a different kind of gap: that enum has no
/// consumer in this tree at all (`LLMStream` is declared and re-exported, never
/// awaited), and its one producer — the SSE parser — emits neither of them. So
/// whoever wires the parser up will wait for a start frame and a tool-call
/// frame that never come, and the tool-call chunk will arrive as `TextDelta`
/// because the parser's fallback turns every unrecognised chunk into text.
///
/// `AssistantMessageEvent::Usage` is the one row whose consumer is *deliberately*
/// a no-op, so its absence deceives nobody today — the row is here so that the
/// word cannot quietly stop being absent.
const NO_PRODUCER_HERE: &[(&str, &str, &str)] = &[
    (
        "AgentEvent",
        "SelfReview",
        "本树没有构造点：自我复盘跑完后没有人把这一帧发上总线。消费端在等它——\
         WebUI 的 `case 'self_review'` 面板、`detector` 的 NEED_REVISION 分支、\
         观测网关的 agent 归属。树外 agent 可以经 gRPC `event_json` 推这种帧，\
         但本树的 agent 运行时自己从不发",
    ),
    (
        "AgentEvent",
        "ToolExecutionUpdate",
        "本树没有构造点：`runtime` 只发 `ToolExecutionStart` 与 `ToolExecutionEnd`，\
         工具的部分结果从不经总线发出。消费端在等它——`websocket_protocol` 的映射、\
         `event_filter` 的分类、快照的归属",
    ),
    (
        "AssistantMessageEvent",
        "Usage",
        "没有任何 provider 发这一帧，且 `apply` 把它当空操作跳过——注释写明 token 记账跟着 \
         `Done` 携带的终态消息走，token 数现在经 agent 的 run usage 与观测指标入账。\
         登记它而不是删它，是因为删改动的是跨 crate 的公开枚举；而这行登记会在有产出端\
         那天变红，逼着重新决定留还是删",
    ),
    (
        "StreamEvent",
        "Start",
        "SSE 解析器不产出这一帧，且本树没有任何消费端读 `StreamEvent`（`LLMStream` \
         只声明与再导出，没有人 await）。接上消费端时这一帧永远等不来——那时要么让\
         解析器在流开头补一帧，要么把它从词表里删掉",
    ),
    (
        "StreamEvent",
        "ToolCall",
        "SSE 解析器不产出这一帧：认不出的分片一律落到兜底支变成 `TextDelta`，\
         所以一个工具调用分片读出来是一段文本。消费端同上——本树还没有人读这个枚举",
    ),
];

/// What may follow a `{ ... }` and still mean the braces were a pattern rather
/// than a value: a match arm, an alternation, a guard, a `let` binding, an
/// `else` clause.
fn following_is_pattern(after: &str) -> bool {
    let after = after.trim_start();
    after.starts_with("=>")
        || after.starts_with('|')
        || after.starts_with("if ")
        || after.starts_with("else")
        // `let AgentEvent::X { .. } = event else` binds; `AgentEvent::X { .. } == y`
        // would be a value, and there is no such comparison today.
        || (after.starts_with('=') && !after.starts_with("=="))
}

/// The spans of every `matches!(...)` argument list. A struct-shaped pattern
/// there is followed by `)`, which on its own would read as a value.
fn macro_argument_spans(text: &str, needle: &str) -> Vec<(usize, usize)> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(needle) {
        let mut i = from + at + needle.len();
        from = i;
        while i < b.len() && (b[i] as char).is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() || b[i] != b'(' {
            continue;
        }
        if let Some(close) = matching_paren(text, i, Dialect::Rust) {
            out.push((i, close));
        }
    }
    out
}

/// Whether one of the fields is a bare `..`.
///
/// Only a field of its own marks a rest pattern. `..Default::default()` is a
/// struct update and belongs to a value being built — reading any `..` as the
/// rest marker would call that construction a pattern, which is the wrong
/// direction: it takes a producer away and reddens a correct tree.
fn has_rest_pattern(body: &str) -> bool {
    top_level_fields(body).contains(&"..")
}

/// The comma-separated fields of a struct literal or pattern, at depth zero.
fn top_level_fields(body: &str) -> Vec<&str> {
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut item_start = 0usize;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'{' || c == b'(' || c == b'[' {
            depth += 1;
        } else if c == b'}' || c == b')' || c == b']' {
            depth -= 1;
        } else if c == b'"' {
            i += 1;
            while i < b.len() {
                if b[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if b[i] == b'"' {
                    break;
                }
                i += 1;
            }
        } else if c == b',' && depth == 0 {
            out.push(body[item_start..i].trim());
            item_start = i + 1;
        }
        i += 1;
    }
    let tail = body[item_start..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    out.into_iter().filter(|field| !field.is_empty()).collect()
}

/// Every place a variant of `enum_name` is named with a brace, and whether that
/// brace holds a constructed value or a pattern.
fn event_sites(text: &str, enum_name: &str) -> Vec<(String, bool)> {
    let needle = format!("{enum_name}::");
    let needle = needle.as_str();
    let b = text.as_bytes();
    let macros = macro_argument_spans(text, "matches!");
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(needle) {
        let start = from + at;
        from = start + needle.len();
        let id_start = from;
        let mut i = id_start;
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        if i == id_start {
            continue;
        }
        let variant = text[id_start..i].to_string();
        let mut j = i;
        while j < b.len() && (b[j] as char).is_ascii_whitespace() {
            j += 1;
        }
        if j >= b.len() || b[j] != b'{' {
            continue;
        }
        let Some(close) = matching_brace(text, j, Dialect::Rust) else {
            continue;
        };
        let body = &text[j + 1..close];
        let after = &text[close + 1..];
        let inside_macro = macros.iter().any(|(s, e)| start >= *s && close <= *e);
        let pattern = has_rest_pattern(body) || inside_macro || following_is_pattern(after);
        out.push((variant, !pattern));
        from = close + 1;
    }
    out
}

/// The variants declared inside `pub enum {name}`, in declaration order.
fn declared_variants(root: &Path, file: &str, name: &str) -> Vec<String> {
    let path = root.join(file);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let needle = format!("pub enum {name} {{");
    let Some(at) = text.find(&needle) else {
        panic!("{file} 里没有 `{needle}`：枚举搬了家或改了名，这条门会在空集上假装绿");
    };
    let open = at + needle.len() - 1;
    let close = matching_brace(&text, open, Dialect::Rust).expect("enum body is not closed");
    // 文档注释里的散文与 `{` 都不是声明：先剥注释再按顶层逗号切，否则
    // 「一句带 `{` 的注释」会把后面的散文读成变体。
    let body = strip(&text[open + 1..close], Dialect::Rust);

    let mut out = Vec::new();
    for part in split_top_level(&body, Dialect::Rust) {
        let name: String = part
            .trim()
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if name.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
            out.push(name);
        }
    }
    out
}

/// Each enum's name and the variants it declares, in declaration order.
type Declared = Vec<(String, Vec<String>)>;

/// How many production sites construct a given variant of a given enum.
type Constructed = BTreeMap<(String, String), usize>;

/// Every variant declared in each of `enums`, in declaration order, and how
/// many production sites construct one.
fn census_of(root: &Path, enums: &[(&str, &str)], sources: &[PathBuf]) -> (Declared, Constructed) {
    let declared: Declared = enums
        .iter()
        .map(|(file, name)| (name.to_string(), declared_variants(root, file, name)))
        .collect();
    let mut counts: Constructed = BTreeMap::new();
    for (name, variants) in &declared {
        for variant in variants {
            counts.insert((name.clone(), variant.clone()), 0);
        }
    }
    for path in sources {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let production = production_text(&text);
        for (_, name) in enums {
            for (variant, constructed) in event_sites(&production, name) {
                if !constructed {
                    continue;
                }
                if let Some(count) = counts.get_mut(&(name.to_string(), variant)) {
                    *count += 1;
                }
            }
        }
    }
    (declared, counts)
}

fn census(root: &Path) -> (Declared, Constructed) {
    let enums: Vec<(&str, &str)> = WIRE_ENUMS.iter().map(|e| (e.file, e.name)).collect();
    census_of(root, &enums, &production_sources(root))
}

fn holes(counts: &Constructed, registered: &[(&str, &str, &str)]) -> Vec<String> {
    let mut out = Vec::new();
    for ((enum_name, variant), count) in counts {
        let entry = registered
            .iter()
            .find(|(e, name, _)| e == enum_name && name == variant);
        match (count, entry) {
            (0, None) => out.push(format!(
                "`{enum_name}::{variant}` 声明在词表里、消费端在读它，但本树没有任何一处构造它：\
                 这一帧永远不上总线，读它的面板与分支永远落在「没有发生」那一支"
            )),
            (0, Some(_)) => {}
            (_, None) => {}
            (_, Some(_)) => out.push(format!(
                "`{enum_name}::{variant}` 已经有 {count} 个生产构造点了，\
                 `NO_PRODUCER_HERE` 里那一行过期，删掉它"
            )),
        }
    }
    for (enum_name, variant, _) in registered {
        if !counts.contains_key(&(enum_name.to_string(), variant.to_string())) {
            out.push(format!(
                "`NO_PRODUCER_HERE` 里登记的 `{enum_name}::{variant}` 已经不在枚举里了，这条登记过期"
            ));
        }
    }
    out
}

#[test]
fn every_declared_event_is_constructed_somewhere() {
    let root = repo_root();
    let (declared, counts) = census(&root);

    // 抽取面自检：枚举读空、或扫描器一个构造点都看不见时，下面那条判据会在
    // 空集上假装绿。
    let total: usize = declared.iter().map(|(_, v)| v.len()).sum();
    assert!(
        total >= 40,
        "只从 event.rs 里读到 {total} 个变体，枚举解析本身坏了：{declared:?}"
    );
    for (name, variants) in &declared {
        assert!(
            variants.len() >= 5,
            "`{name}` 只读出 {} 个变体：解析坏了，或者这个枚举真的瘦成这样了——两条都要看一眼",
            variants.len()
        );
    }
    for known in [
        ("AgentEvent", "SelfReview"),
        ("AgentEvent", "ToolExecutionUpdate"),
        ("AssistantMessageEvent", "TextDelta"),
        ("TaskEvent", "TaskLeaseExpired"),
        ("StreamEvent", "Usage"),
    ] {
        assert!(
            counts.contains_key(&(known.0.to_string(), known.1.to_string())),
            "读不到 `{}::{}`：解析坏了或它真的被删了",
            known.0,
            known.1
        );
    }
    let constructed = counts.values().filter(|n| **n > 0).count();
    assert!(
        constructed >= 35,
        "生产半边只认出 {constructed} 个变体的构造点，扫描器本身坏了"
    );

    let found = holes(&counts, NO_PRODUCER_HERE);
    assert!(
        found.is_empty(),
        "事件词表里有点了名却没有产出端的词：\n{}\n\
         声明、消费、过滤三处都在，产出那一处不在——而这不会报错，\
         读它的面板与分支安静地落在 else 上",
        found.join("\n")
    );
}

/// 判据自己也要有判据：一棵小树里造一个、只当模式用一个、登记一个，判出来的
/// 必须正是没造的那个；给登记的那个补上构造点，它必须改口说过期。
#[test]
fn a_variant_nobody_constructs_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    let enum_dir = root.join("crates/cog-core/src/types");
    std::fs::create_dir_all(&enum_dir).expect("enum dir");
    std::fs::write(
        enum_dir.join("event.rs"),
        "pub enum AgentEvent {\n\
             Produced { agent_id: String },\n\
             OnlyMatched { agent_id: String },\n\
             FromTheWire { agent_id: String },\n\
         }\n",
    )
    .expect("enum");
    let src = root.join("crates/cog-example/src");
    std::fs::create_dir_all(&src).expect("src dir");
    std::fs::write(
        src.join("lib.rs"),
        "fn emit(agent_id: String) -> AgentEvent {\n\
             AgentEvent::Produced { agent_id }\n\
         }\n\
         \n\
         fn classify(event: &AgentEvent) {\n\
             match event {\n\
                 AgentEvent::Produced { agent_id } | AgentEvent::OnlyMatched { agent_id } => drop(agent_id),\n\
                 _ => {}\n\
             }\n\
             if let AgentEvent::OnlyMatched { agent_id } = event {\n\
                 drop(agent_id);\n\
             }\n\
             if matches!(event, AgentEvent::FromTheWire { agent_id }) {\n\
                 drop(agent_id);\n\
             }\n\
         }\n\
         \n\
         #[cfg(test)]\n\
         mod tests {\n\
             fn fixture() -> AgentEvent {\n\
                 AgentEvent::OnlyMatched { agent_id: \"a\".into() }\n\
             }\n\
         }\n",
    )
    .expect("source");

    let enums = [("crates/cog-core/src/types/event.rs", "AgentEvent")];
    let (declared, counts) = census_of(root, &enums, &sources_of_tree(root));
    assert_eq!(declared.len(), 1, "{declared:?}");
    assert_eq!(declared[0].1, ["Produced", "OnlyMatched", "FromTheWire"]);
    let key = |name: &str| ("AgentEvent".to_string(), name.to_string());
    assert_eq!(counts[&key("Produced")], 1, "{counts:?}");
    // 只在测试模块里造的、只在模式位置出现的、只在 matches! 里出现的，都不算产出端。
    assert_eq!(counts[&key("OnlyMatched")], 0, "{counts:?}");
    assert_eq!(counts[&key("FromTheWire")], 0, "{counts:?}");

    let registered = [("AgentEvent", "FromTheWire", "树外生产")];
    let found = holes(&counts, &registered);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("OnlyMatched"), "{found:?}");

    // 登记的那个真的有了产出端：这条登记必须过期，否则一个补好的洞会被一直念。
    std::fs::write(
        src.join("lib.rs"),
        "fn emit(agent_id: String) -> AgentEvent {\n\
             AgentEvent::Produced { agent_id: agent_id.clone() }\n\
         }\n\
         fn wire(agent_id: String) -> AgentEvent {\n\
             AgentEvent::FromTheWire { agent_id }\n\
         }\n\
         fn only_matched(agent_id: String) -> AgentEvent {\n\
             AgentEvent::OnlyMatched { agent_id }\n\
         }\n",
    )
    .expect("source");
    let (_, counts) = census_of(root, &enums, &sources_of_tree(root));
    let found = holes(&counts, &registered);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("过期"), "{found:?}");
}

/// 扫描面自己也要有判据，否则「读到零个构造点」与「真的没人造」不可分。
#[test]
fn the_scanner_reads_values_and_skips_patterns() {
    let sites = event_sites(
        "let a = AgentEvent::Made { agent_id };\n\
         let b = AgentEvent::Made { agent_id: x.clone(), ..Default::default() };\n\
         match e { AgentEvent::Armed { id } => 1, _ => 0 }\n\
         let AgentEvent::Armed { id } = e else { return };\n\
         if matches!(e, AgentEvent::Armed { id }) {}\n\
         match e { AgentEvent::Armed { id } if ok => 1, _ => 0 }\n\
         let c = |AgentEvent::Armed { id }| id;\n\
         let d = AgentEvent::Made { agent_id: \"quoted } brace\".into() };\n",
        "AgentEvent",
    );
    let made = sites.iter().filter(|(v, c)| v == "Made" && *c).count();
    let armed = sites.iter().filter(|(v, c)| v == "Armed" && *c).count();
    assert_eq!(made, 3, "{sites:?}");
    assert_eq!(armed, 0, "{sites:?}");
}
