//! principle: feedback_no_hardcoding_config_surface
//! The payload half of the same hop: the client reads fields off an event by
//! name, and the server writes them by the variant's field names. A field the
//! server renames leaves the client's read as `undefined`, and TypeScript
//! cannot see it — the interface was written by hand and nothing compiles the
//! two halves together. What the operator then sees is a blank cell in a panel
//! that is otherwise fine, which is the same picture as an event that carried
//! no data.
//!
//! The direction checked here is one-way on purpose: a field the server writes
//! and the client ignores is fine (the client shows what it shows), so only the
//! client's own reads have to land somewhere. The pairing between an interface
//! and a variant is by name — `ToolExecutionStartFields` is
//! `AgentEvent::ToolExecutionStart` — which is the convention the file already
//! follows.
//!
//! 实测（2026-10-27）：10 个客户端载荷形状，每一个的每个字段都在对应变体上；
//! 服务端 20 个变体。反向留空是刻意的：客户端不读的字段不算漂移。

mod common;

use common::routes::repo_root;

use std::collections::{BTreeMap, BTreeSet};

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Where the variants and their fields are written.
const SERVER: &str = "crates/cog-core/src/types/event.rs";

/// Where the client's shapes are written.
const CLIENT: &str = "web/src/types/events.ts";

/// One variant's fields.
#[derive(Debug, Default)]
struct Variant {
    fields: BTreeSet<String>,
    /// A field whose wire name is pinned by `#[serde(rename = ...)]` is not the
    /// name written in the source, so a census over source names would read the
    /// wrong string. None carries one today; one appearing has to be looked at
    /// rather than silently believed.
    renamed: bool,
}

/// The variants of `AgentEvent` and the fields each carries.
fn server_variants(text: &str) -> BTreeMap<String, Variant> {
    let start = text
        .find("pub enum AgentEvent")
        .unwrap_or_else(|| panic!("{SERVER} 里没有 AgentEvent，这条判据没有题目"));

    let mut variants = BTreeMap::new();
    let mut current: Option<(String, Variant)> = None;
    for line in text[start..].lines().skip(1) {
        // 枚举在列首的右括号处结束。
        if line.starts_with('}') {
            break;
        }
        // 变体头与变体自己的右括号都在四格缩进上，字段在八格；先按缩进分开，
        // 免得一个变体的字段被算进上一个变体。
        let variant_indent = line
            .strip_prefix("    ")
            .is_some_and(|rest| !rest.starts_with(' '));
        if variant_indent {
            if let Some((name, variant)) = current.take() {
                variants.insert(name, variant);
            }
            if let Some(name) = variant_head(line.trim_start()) {
                current = Some((name, Variant::default()));
            }
            continue;
        }
        let Some((_, variant)) = current.as_mut() else {
            continue;
        };
        if line.trim_start().starts_with("#[serde(rename") {
            variant.renamed = true;
        }
        if let Some(field) = field_name(line) {
            variant.fields.insert(field);
        }
    }
    if let Some((name, variant)) = current.take() {
        variants.insert(name, variant);
    }
    variants
}

/// `AgentStart {` at variant indentation -> `AgentStart`.
fn variant_head(line: &str) -> Option<String> {
    let name: String = line
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric())
        .collect();
    if name.is_empty() || !name.starts_with(|c: char| c.is_ascii_uppercase()) {
        return None;
    }
    line[name.len()..]
        .trim_start()
        .starts_with('{')
        .then_some(name)
}

/// `        agent_id: String,` -> `agent_id`. Attributes and the closing brace
/// name nothing.
fn field_name(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('#') || trimmed.starts_with('}') || trimmed.starts_with("//") {
        return None;
    }
    let name: String = trimmed
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let rest = trimmed[name.len()..].trim_start();
    (!name.is_empty()
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && rest.starts_with(':'))
    .then_some(name)
}

/// The client's payload shapes, keyed by the variant they describe.
fn client_shapes(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut shapes = BTreeMap::new();
    let mut rest = text;
    while let Some(at) = rest.find("export interface ") {
        rest = &rest[at + "export interface ".len()..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        let Some(variant) = name.strip_suffix("Fields") else {
            continue;
        };
        let body_start = rest.find('{').unwrap_or(0);
        let body_end = rest[body_start..]
            .find("\n}")
            .unwrap_or_else(|| panic!("客户端接口 {name} 没有闭合"));
        let body = &rest[body_start..body_start + body_end];
        let fields = body
            .lines()
            .filter_map(|line| {
                let field: String = line
                    .trim_start()
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                let after = line.trim_start()[field.len()..].trim_start();
                let after = after.strip_prefix('?').unwrap_or(after).trim_start();
                (!field.is_empty() && after.starts_with(':')).then_some(field)
            })
            .collect();
        shapes.insert(variant.to_string(), fields);
    }
    shapes
}

/// The fields a shape reads that its variant does not carry.
fn unknown_fields(shape: &BTreeSet<String>, variant: &Variant) -> Vec<String> {
    shape.difference(&variant.fields).cloned().collect()
}

#[test]
fn every_field_the_client_reads_is_one_the_server_writes() {
    let server = server_variants(&read(SERVER));
    let shapes = client_shapes(&read(CLIENT));

    assert!(
        server.len() > 15,
        "只读出 {} 个事件变体，这条判据没有题目",
        server.len()
    );
    assert!(
        shapes.len() >= 8,
        "只读出 {} 个客户端载荷形状，这条判据没有题目",
        shapes.len()
    );

    let mut complaints = Vec::new();
    for (variant_name, shape) in &shapes {
        let Some(variant) = server.get(variant_name) else {
            complaints.push(format!(
                "{variant_name}Fields 描述的变体在 {SERVER} 里不存在：这组读法读到的是一个\
                 永远不会来的载荷，每个字段都是 undefined"
            ));
            continue;
        };
        if variant.renamed {
            complaints.push(format!(
                "{variant_name} 的字段带了 #[serde(rename)]：线上名字与源码名字不同，\
                 这条判据读的是源码名字，结论不可信"
            ));
        }
        let unknown = unknown_fields(shape, variant);
        if !unknown.is_empty() {
            complaints.push(format!(
                "{variant_name}Fields 读了 {unknown:?}，而 {variant_name} 上没有这些字段\
                 （它有 {:?}）：读出来是 undefined，面板上是一格空白，不是一次报错",
                variant.fields
            ));
        }
    }
    assert!(
        complaints.is_empty(),
        "客户端按名字读事件载荷，服务端已经不写这些名字了：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_reader_handles_the_shapes_this_tree_writes() {
    let server = "\
pub enum AgentEvent {
    /// Agent run started.
    AgentStart {
        agent_id: String,
        #[serde(skip_serializing_if = \"Option::is_none\")]
        crew_id: Option<String>,
        timestamp: DateTime<Utc>,
    },
    ToolExecutionEnd {
        agent_id: String,
        tool_call_id: String,
        result: Value,
        is_error: bool,
    },
}";
    let variants = server_variants(server);
    assert_eq!(
        variants["AgentStart"].fields,
        BTreeSet::from([
            "agent_id".to_string(),
            "crew_id".to_string(),
            "timestamp".to_string()
        ])
    );
    assert!(!variants["AgentStart"].renamed);
    assert_eq!(
        variants["ToolExecutionEnd"].fields,
        BTreeSet::from([
            "agent_id".to_string(),
            "tool_call_id".to_string(),
            "result".to_string(),
            "is_error".to_string()
        ])
    );

    let client = "\
export interface ToolExecutionEndFields {
  agent_id: string;
  tool_call_id: string;
  result: unknown;
  is_error: boolean;
}

export interface ChatFrame {
  type: string;
}";
    let shapes = client_shapes(client);
    assert_eq!(shapes.len(), 1, "只有 *Fields 是载荷形状：{shapes:?}");
    assert_eq!(
        shapes["ToolExecutionEnd"],
        BTreeSet::from([
            "agent_id".to_string(),
            "tool_call_id".to_string(),
            "result".to_string(),
            "is_error".to_string()
        ])
    );
}

#[test]
fn a_renamed_field_is_reported() {
    let server = "\
pub enum AgentEvent {
    ToolExecutionEnd {
        agent_id: String,
        tool_name: String,
    },
}";
    let variants = server_variants(server);
    let shape = BTreeSet::from(["agent_id".to_string(), "tool_call_id".to_string()]);
    assert_eq!(
        unknown_fields(&shape, &variants["ToolExecutionEnd"]),
        vec!["tool_call_id".to_string()],
        "服务端把 tool_call_id 改名成 tool_name 之后，客户端读 tool_call_id 必须被报出来"
    );

    // 字段级 rename 会让这条判据读到错的名字，必须显式拒判。
    let renamed = "\
pub enum AgentEvent {
    ToolExecutionEnd {
        #[serde(rename = \"toolCallId\")]
        tool_call_id: String,
    },
}";
    assert!(server_variants(renamed)["ToolExecutionEnd"].renamed);
}
