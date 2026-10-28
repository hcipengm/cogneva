//! principle: feedback_no_hardcoding_config_surface
//!
//! 客户端读事件载荷有两种写法：`switch (event.type)` 的每一臂，和「不是这个类型
//! 就提前返回」的守卫。两种写法都把一段读法**限定在一个变体上**——那段代码只会
//! 收到那个变体的帧。既然限定住了，每一臂能读到的字段就是那个变体的字段，不多一个。
//!
//! 读多出来的字段不会报错：`p.tool_name` 在 `tool_execution_end` 上就是 `undefined`，
//! 而 `undefined` 落进模板串是空串，落进 `??` 链是「继续找下一个」。界面上表现为
//! 一格空白或一句缺了主语的话，和「这一帧本来就没带这个信息」长得一模一样。
//!
//! 实测（2026-10-27）落地时抓到四处，两个病：
//! - `tool_execution_update` 那一臂读 `chunk` / `delta` / `output`，而
//!   `ToolExecutionUpdate` 只带 `partial_result`——三个备选名字从来没有一个存在过。
//!   同一个文件里早写着「按 chunk/delta/output 去读，读到的一直是空」，改的时候
//!   只在前面补了 `partial_result`，三个死名字留在后面。
//! - `tool_execution_update` 与 `tool_execution_end` 两臂都读 `tool_name`，而这两个
//!   变体不带它（名字只在 `tool_execution_start` 上）。于是活动流里每一次工具返回
//!   都是一句 `工具 call-1 返回`：显示的是调用 id，而代码读起来像是在显示工具名。
//!
//! 判据只往前走一步：它不判断「该不该有这个字段」，只判断「这一臂读的名字是不是
//! 它收到的那个变体真带着的」。多出来的字段一律要求写者先写上——真需要，就把字段
//! 加到变体上（那是跨 crate 的契约改动），而不是在客户端读一个不存在的名字。
//!
//! 消融对照：在真树 `streamStore.ts` 的 `state_change` 一臂里临时加一个
//! `p.ghost_field`，本门红并点名它；还原后绿。临时树上另做了三条：多读一个字段要
//! 红、把字段写进变体后要绿、`case` 写一个不存在的线上名字也要红（那一臂根本进不去）。
//!
//! 第二个客户端是嵌在网关二进制里的运维页（`takeover.html`）：它拿事件对象本身
//! 去 `switch (msg.type)`，没有 `payload` 那一层绑定。它 12 条臂里的读法全部成立
//! （实测：`state_change` 读 `to`、`react_step_start` 读 `iteration`、
//! `message_update` 读 `assistant_event`、`tool_execution_end` 读 `is_error`，都对得上
//! 变体）。真树消融：在那一页的 `state_change` 一臂里加 `msg.ghost_field`，本门红并
//! 点名；还原后绿。同一个函数里还有一处 `switch (p.type)`——那是信封里聊天流的类型
//! （`message.start` 之类），判到它只会得到一片假红，所以「被 switch 的变量自己是
//! 从 `.payload` 解出来的」那一处不判。
//!
//! 认不出的地方一律跳过，不猜：变体名与线上名字的对应关系从
//! `crates/cog-storage/src/event_filter.rs` 的 `event_type_name` 里读，不自己把
//! `react_step_end` 拼成 `ReactStepEnd`（变体其实叫 `ReActStepEnd`）；文件里找不到
//! `payload` 绑定的 switch 或守卫不判。

mod common;

use common::routes::read;

use common::routes::repo_root;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// 变体与字段。
const EVENT: &str = "crates/cog-core/src/types/event.rs";

/// 线上名字 → 变体名的唯一出处。
const WIRE: &str = "crates/cog-storage/src/event_filter.rs";

/// 客户端。
const WEB: &str = "web/src";

/// 第二个客户端：嵌在网关二进制里的运维页。
///
/// 它把事件对象本身拿去 `switch (msg.type)`，每一臂直接读 `msg.<field>`，没有
/// `const p = event.payload` 那一层。同一个函数里还有一处 `switch (p.type)`——那
/// 是信封里那一层聊天流的类型（`message.start` 之类），不是 `AgentEvent` 的线上
/// 名字，读的也不是变体字段，判到它只会得到一片假红。两者的区别写在代码里：被
/// switch 的那个变量若自己就是从 `.payload` 里解出来的，这一处不判。
const PAGE: &str = "crates/cog-gateway/src/takeover.html";

/// 去掉 `//` 与 `/* */` 注释，保留字符串。
///
/// 判据读的 `case` 标签与线上名字都是字符串字面量，必须留着；而被注释掉的那一臂
/// 不该算。整棵树里有中文，所以按字节走、只在字符边界上推进。
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

/// `AgentEvent::ToolExecutionEnd { .. } => "tool_execution_end"` 两列。
fn wire_names(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("AgentEvent::") {
        let start = cursor + at + "AgentEvent::".len();
        let variant: String = text[start..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        cursor = start + variant.len();
        if variant.is_empty() {
            continue;
        }
        // 同一行必须同时有 `{` 与 `=> "name"`，否则这不是一张对应表的行。
        let line_end = text[cursor..].find('\n').map_or(text.len(), |n| cursor + n);
        let line = &text[cursor..line_end];
        if !line.contains('{') {
            continue;
        }
        let Some(arrow) = line.find("=>") else {
            continue;
        };
        let Some(open) = line[arrow..].find('"') else {
            continue;
        };
        let rest = &line[arrow + open + 1..];
        let Some(close) = rest.find('"') else {
            continue;
        };
        map.insert(rest[..close].to_string(), variant);
    }
    map
}

/// `AgentEvent` 的每个变体带着哪些字段。
fn variants(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let start = text
        .find("pub enum AgentEvent")
        .unwrap_or_else(|| panic!("{EVENT} 里没有 AgentEvent，这条判据没有题目"));
    let mut variants = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in text[start..].lines().skip(1) {
        if line.starts_with('}') {
            break;
        }
        let head_indent = line
            .strip_prefix("    ")
            .is_some_and(|rest| !rest.starts_with(' '));
        if head_indent {
            let name: String = line
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            current = (!name.is_empty()
                && name.starts_with(|c: char| c.is_ascii_uppercase())
                && line.trim_start()[name.len()..]
                    .trim_start()
                    .starts_with('{'))
            .then(|| {
                variants.insert(name.clone(), BTreeSet::new());
                name
            });
            continue;
        }
        let Some(variant) = current.as_ref() else {
            continue;
        };
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') || trimmed.starts_with('}') || trimmed.starts_with("//") {
            continue;
        }
        let field: String = trimmed
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !field.is_empty()
            && field.starts_with(|c: char| c.is_ascii_lowercase())
            && trimmed[field.len()..].trim_start().starts_with(':')
        {
            variants
                .get_mut(variant)
                .expect("just inserted")
                .insert(field);
        }
    }
    variants
}

/// 包含 `at` 的那个顶层函数，返回它的字节区间。
///
/// 这些文件里的判据都写在 `export function …` 里，函数体在列首的 `}` 收尾；
/// 找不到收尾就取到文件末尾（多读不会误报，只会少判——`payload` 绑定仍须在区间里）。
fn function_span(text: &str, at: usize) -> (usize, usize) {
    let mut start = 0;
    for needle in [
        "\nfunction ",
        "\nexport function ",
        "\nexport async function ",
    ] {
        let mut cursor = 0;
        while let Some(found) = text[cursor..at.min(text.len())].find(needle) {
            let position = cursor + found + 1;
            if position < at {
                start = start.max(position);
            }
            cursor += found + needle.len();
        }
    }
    let end = text[at..].find("\n}\n").map_or(text.len(), |n| at + n + 1);
    (start, end)
}

/// 函数区间里 `const <var> = <base>.payload` 的那个 `<var>`。
fn payload_var(span: &str, base: &str) -> Option<String> {
    let needle = ".payload";
    let mut cursor = 0;
    while let Some(at) = span[cursor..].find(needle) {
        let position = cursor + at;
        cursor = position + needle.len();
        let before = &span[..position];
        let Some(assign) = before.rfind('=') else {
            continue;
        };
        // `= <base>.payload`：等号与点之间只许有 `base` 本身与空白。
        let between = before[assign + 1..].trim();
        if between != base {
            continue;
        }
        let head = &before[..assign];
        let Some(const_at) = head.rfind("const ") else {
            continue;
        };
        let name: String = head[const_at + "const ".len()..]
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// 一段代码里从 `<var>` 上读的字段名（`<var>.field`），不含判据本身读的 `type`。
fn fields_read(snippet: &str, var: &str) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let needle = format!("{var}.");
    let mut cursor = 0;
    while let Some(at) = snippet[cursor..].find(&needle) {
        let start = cursor + at;
        // `p.foo` 与 `api.p.foo` 不是一回事：前面紧挨着标识符字符的跳过。
        let boundary = start == 0
            || !(snippet.as_bytes()[start - 1].is_ascii_alphanumeric()
                || snippet.as_bytes()[start - 1] == b'_'
                || snippet.as_bytes()[start - 1] == b'.');
        let name_start = start + needle.len();
        let field: String = snippet[name_start..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '$')
            .collect();
        cursor = name_start + field.len();
        if boundary && !field.is_empty() && field != "type" {
            fields.insert(field);
        }
    }
    fields
}

/// 一处「这一臂收到的变体」：线上名字、以及这一臂从载荷上读的字段。
#[derive(Debug)]
struct Arm {
    file: String,
    wire: String,
    fields: BTreeSet<String>,
}

/// 一个文件里所有能判的臂：switch 的每个 case，以及「不是这个类型就返回」的守卫。
fn arms(file: &str, source: &str) -> Vec<Arm> {
    let text = strip_comments(source);
    let mut out = Vec::new();

    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("switch (") {
        let open = cursor + at + "switch (".len();
        cursor = open;
        let Some(close) = text[open..].find(')') else {
            break;
        };
        let scrutinee = text[open..open + close].trim();
        let Some(base) = scrutinee.strip_suffix(".type") else {
            continue;
        };
        let (start, end) = function_span(&text, open);
        let Some(var) = payload_var(&text[start..end], base.trim()) else {
            continue;
        };
        let Some(brace) = text[open + close..].find('{') else {
            continue;
        };
        let body_start = open + close + brace + 1;
        let body = &text[body_start..end];
        let mut labels: Vec<(usize, String)> = Vec::new();
        let mut label_cursor = 0;
        while let Some(found) = body[label_cursor..].find("case '") {
            let position = label_cursor + found;
            let line_start = body[..position].rfind('\n').map_or(0, |n| n + 1);
            if !body[line_start..position].trim().is_empty() {
                label_cursor = position + "case '".len();
                continue;
            }
            let name_start = position + "case '".len();
            let Some(name_end) = body[name_start..].find('\'') else {
                break;
            };
            labels.push((
                position,
                body[name_start..name_start + name_end].to_string(),
            ));
            label_cursor = name_start + name_end;
        }
        for (index, (position, wire)) in labels.iter().enumerate() {
            let stop = labels.get(index + 1).map_or(body.len(), |(next, _)| *next);
            out.push(Arm {
                file: file.to_string(),
                wire: wire.clone(),
                fields: fields_read(&body[*position..stop], &var),
            });
        }
    }

    // `if (event.type !== 'x') return …;` —— 其余整段都只会收到 `x` 的帧。
    let mut guard_cursor = 0;
    while let Some(at) = text[guard_cursor..].find("if (") {
        let open = guard_cursor + at + "if (".len();
        guard_cursor = open;
        let Some(close) = text[open..].find(')') else {
            break;
        };
        let condition = text[open..open + close].trim();
        let Some(rest) = condition.strip_suffix('\'') else {
            continue;
        };
        let Some(split) = rest.find(" !== '") else {
            continue;
        };
        let scrutinee = rest[..split].trim();
        let Some(base) = scrutinee.strip_suffix(".type") else {
            continue;
        };
        let wire = &rest[split + " !== '".len()..];
        let (start, end) = function_span(&text, open);
        let Some(var) = payload_var(&text[start..end], base.trim()) else {
            continue;
        };
        out.push(Arm {
            file: file.to_string(),
            wire: wire.to_string(),
            fields: fields_read(&text[open..end], &var),
        });
    }

    out
}

/// 运维页那种写法：`switch (msg.type)`，臂里直接读 `msg.<field>`。
///
/// 只认「被 switch 的变量就是事件对象」这一种：从 `.payload` 里解出来的变量
/// （`switch (p.type)`）不判——那一层是信封里的聊天流类型，不是 `AgentEvent`。
/// 这个变量是不是自己就从 `.payload` 里解出来的（`const p = msg.payload;`）。
///
/// 是的话，`switch (p.type)` 判的是信封里那一层聊天流的类型，不是 `AgentEvent`。
fn bound_from_payload(span: &str, var: &str) -> bool {
    let needle = format!("const {var} =");
    let mut cursor = 0;
    while let Some(at) = span[cursor..].find(&needle) {
        let start = cursor + at + needle.len();
        let end = span[start..].find(';').map_or(span.len(), |n| start + n);
        let rest = &span[start..end];
        cursor = start;
        if rest.contains(".payload") {
            return true;
        }
    }
    false
}

fn page_arms(file: &str, source: &str) -> Vec<Arm> {
    let text = strip_comments(source);
    let mut out = Vec::new();

    let mut cursor = 0;
    while let Some(at) = text[cursor..].find("switch (") {
        let open = cursor + at + "switch (".len();
        cursor = open;
        let Some(close) = text[open..].find(')') else {
            break;
        };
        let scrutinee = text[open..open + close].trim();
        let Some(base) = scrutinee.strip_suffix(".type") else {
            continue;
        };
        let base = base.trim();
        let (start, end) = function_span(&text, open);
        if bound_from_payload(&text[start..end], base) {
            continue;
        }
        let Some(brace) = text[open + close..].find('{') else {
            continue;
        };
        let body_start = open + close + brace + 1;
        let body = &text[body_start..end];
        let mut labels: Vec<(usize, String)> = Vec::new();
        let mut label_cursor = 0;
        while let Some(found) = body[label_cursor..].find("case '") {
            let position = label_cursor + found;
            let line_start = body[..position].rfind('\n').map_or(0, |n| n + 1);
            if !body[line_start..position].trim().is_empty() {
                label_cursor = position + "case '".len();
                continue;
            }
            let name_start = position + "case '".len();
            let Some(name_end) = body[name_start..].find('\'') else {
                break;
            };
            labels.push((
                position,
                body[name_start..name_start + name_end].to_string(),
            ));
            label_cursor = name_start + name_end;
        }
        for (index, (position, wire)) in labels.iter().enumerate() {
            let stop = labels.get(index + 1).map_or(body.len(), |(next, _)| *next);
            out.push(Arm {
                file: file.to_string(),
                wire: wire.clone(),
                fields: fields_read(&body[*position..stop], base),
            });
        }
    }

    out
}

/// 每一条读法都要落在它那一臂收到的变体上。
fn contradictions(root: &Path) -> (Vec<String>, usize) {
    let wire = wire_names(&read(&root.join(WIRE)));
    let variants = variants(&read(&root.join(EVENT)));
    let mut complaints = Vec::new();
    let mut judged = 0;
    let mut files = Vec::new();
    collect_ts(&root.join(WEB), &mut files);
    files.sort();
    let mut found = Vec::new();
    for path in files {
        let name = path
            .strip_prefix(root.join(WEB))
            .unwrap_or(&path)
            .display()
            .to_string();
        found.extend(arms(&name, &read(&path)));
    }
    let page = root.join(PAGE);
    if page.exists() {
        found.extend(page_arms(PAGE, &read(&page)));
    }
    for arm in found {
        let Some(variant) = wire.get(&arm.wire) else {
            complaints.push(format!(
                "{}: case '{}' 不是任何事件变体的线上名字（{} 里没有它）：这一臂永远不会进，\
                 写在这里的每一句都是死的",
                arm.file, arm.wire, WIRE
            ));
            continue;
        };
        let Some(carried) = variants.get(variant) else {
            complaints.push(format!(
                "{} 的线上名字 {} 对到变体 {variant}，而 {EVENT} 里没有这个变体",
                arm.wire, variant
            ));
            continue;
        };
        judged += 1;
        let extra: Vec<&String> = arm.fields.difference(carried).collect();
        if !extra.is_empty() {
            let mut names: Vec<&str> = extra.iter().map(|s| s.as_str()).collect();
            names.sort_unstable();
            complaints.push(format!(
                "{}: case '{}' 读 {names:?}，而 {variant} 只带 {:?}：那一帧没有这些字段，\
                 读出来是 undefined——读法本身在编一个不存在的名字",
                arm.file, arm.wire, carried
            ));
        }
    }
    (complaints, judged)
}

/// 客户端源码文件数下限。`judged > 0` 只保证「判到了几条读法」，而运维页
/// （`takeover.html`）单独就能交十几条——整片 `web/src` 读不出来时它照旧成立。
/// 所以题目要先按文件数数一遍。
const MIN_CLIENT_FILES: usize = 100;

fn collect_ts(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!(
            "{} 读不出来（{e}）：客户端的读法普查面少了一块，而少读的文件与\
             「那些文件都没有越界读法」在判词上分不开",
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
fn every_payload_field_is_one_the_variant_under_that_case_carries() {
    let (complaints, judged) = contradictions(&repo_root());
    assert!(judged > 0, "一条读法都没判到，这条判据没有题目");
    assert!(
        complaints.is_empty(),
        "客户端的某些臂读了它收到的那个变体根本没有的字段：\n{}\n\
         读一个不存在的名字不报错，只是得到 undefined——界面上是空格，不是故障。\
         要那个信息，先让写者把它写进变体；不要的话，把读法删掉。",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let wire = wire_names(&read(&root.join(WIRE)));
    assert_eq!(
        wire.len(),
        20,
        "只读出 {} 条线上名字，而 AgentEvent 有 20 个变体：对应表读残了，这条判据会把\
         整臂整臂地跳过",
        wire.len()
    );
    let variants = variants(&read(&root.join(EVENT)));
    assert_eq!(variants.len(), 20, "只读出 {} 个变体", variants.len());

    let mut files = Vec::new();
    collect_ts(&root.join(WEB), &mut files);
    assert!(
        files.len() >= MIN_CLIENT_FILES,
        "{WEB} 下只读到 {} 个 .ts/.tsx（下限 {MIN_CLIENT_FILES}）：普查面缩了，\
         后面的臂数与字段数都是在残表上量的",
        files.len()
    );
    let mut total_arms = 0;
    let mut total_fields = BTreeSet::new();
    for path in files {
        let name = path.display().to_string();
        for arm in arms(&name, &read(&path)) {
            total_arms += 1;
            total_fields.extend(arm.fields);
        }
    }
    assert!(
        total_arms >= 30,
        "只判到 {total_arms} 条臂——读取器掉线了，臂数少到这条判据靠「找不到题目」通过"
    );
    assert!(
        total_fields.len() >= 25,
        "只读到 {} 个不同的字段名，太少：判据没在看载荷读法",
        total_fields.len()
    );

    let page = root.join(PAGE);
    assert!(
        page.exists(),
        "{PAGE} 不在了：第二个客户端会一声不响地不再被判"
    );
    let page_judged = page_arms(PAGE, &read(&page)).len();
    assert!(
        page_judged >= 10,
        "运维页只判到 {page_judged} 条臂，太少了：那一处的 switch 写法变了"
    );
}

#[test]
fn the_reader_follows_the_page_that_switches_on_the_event_itself() {
    let source = "\
function handleLiveMessage(data) {
  let msg;
  try { msg = JSON.parse(data); } catch { return; }
  if (['agent_event', 'notification'].includes(msg.type) && msg.payload) {
    const p = msg.payload;
    switch (p.type) {
      case 'message.start':
        if (p.message_id) startMessage(p.message_id);
        break;
    }
    return;
  }
  switch (msg.type) {
    case 'state_change':
      if (msg.agent_id) setStatus(msg.to);
      break;
    case 'agent_error':
    case 'resource_alert':
      if (msg.agent_id) setStatus('error');
      break;
  }
}
";
    let found = page_arms("page.html", source);
    // 信封里那一层 `switch (p.type)` 不判：它的词是聊天流的，不是 AgentEvent 的。
    assert_eq!(found.len(), 3, "{found:#?}");
    assert_eq!(found[0].wire, "state_change");
    assert_eq!(
        found[0].fields,
        BTreeSet::from(["agent_id".to_string(), "to".to_string()])
    );
    // 叠在一起的两个 case 标签：前一条臂只到第二个标签为止，字段由后一条承接。
    assert_eq!(found[1].wire, "agent_error");
    assert!(found[1].fields.is_empty(), "{found:#?}");
    assert_eq!(found[2].wire, "resource_alert");
    assert_eq!(found[2].fields, BTreeSet::from(["agent_id".to_string()]));
}

#[test]
fn a_page_read_of_a_field_its_variant_lacks_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-page-fields-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join(WEB)).expect("temp web");
    std::fs::create_dir_all(root.join("crates/cog-core/src/types")).expect("temp core");
    std::fs::create_dir_all(root.join("crates/cog-storage/src")).expect("temp storage");
    std::fs::create_dir_all(root.join("crates/cog-gateway/src")).expect("temp gateway");

    std::fs::write(
        root.join(EVENT),
        "pub enum AgentEvent {\n    ToolExecutionEnd {\n        agent_id: String,\n        \
         is_error: bool,\n    },\n}\n",
    )
    .expect("event");
    std::fs::write(
        root.join(WIRE),
        "pub fn event_type_name(event: &AgentEvent) -> &str {\n    match event {\n        \
         AgentEvent::ToolExecutionEnd { .. } => \"tool_execution_end\",\n    }\n}\n",
    )
    .expect("wire");
    std::fs::write(
        root.join(PAGE),
        "function f(data) {\n  const msg = JSON.parse(data);\n  switch (msg.type) {\n    \
         case 'tool_execution_end':\n      use(msg.agent_id, msg.tool_name);\n      break;\n  }\n}\n",
    )
    .expect("page");

    let (complaints, judged) = contradictions(&root);
    assert_eq!(judged, 1, "{complaints:?}");
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(
        complaints[0].contains("读 [\"tool_name\"]"),
        "{complaints:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn the_reader_sees_both_ways_a_file_names_its_frame_type() {
    let source = "\
export function fromSwitch(event: AgentEvent): unknown {
  const p = event.payload as Record<string, unknown>;
  switch (event.type) {
    case 'self_review':
      return { score: p.score, summary: p.summary };
    case 'resource_alert':
      return { metric: p.metric };
  }
}

export function fromGuard(event: AgentEvent): unknown {
  if (event.type !== 'state_change') return null;
  const p = event.payload ?? {};
  return { from: p.from, to: p.to };
}
";
    let found = arms("fixture.ts", source);
    assert_eq!(found.len(), 3, "{found:#?}");
    assert_eq!(found[0].wire, "self_review");
    assert_eq!(
        found[0].fields,
        BTreeSet::from(["score".to_string(), "summary".to_string()])
    );
    assert_eq!(found[1].wire, "resource_alert");
    assert_eq!(found[1].fields, BTreeSet::from(["metric".to_string()]));
    // 守卫那一条：载荷绑定写在守卫**之后**，读取器必须往前找到它。
    assert_eq!(found[2].wire, "state_change");
    assert_eq!(
        found[2].fields,
        BTreeSet::from(["from".to_string(), "to".to_string()])
    );
}

#[test]
fn a_field_outside_the_case_variant_is_reported() {
    let root = std::env::temp_dir().join(format!("cogneva-arm-fields-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join(WEB)).expect("temp web tree");
    std::fs::create_dir_all(root.join("crates/cog-core/src/types")).expect("temp core");
    std::fs::create_dir_all(root.join("crates/cog-storage/src")).expect("temp storage");

    let event = |extra: &str| {
        format!(
            "pub enum AgentEvent {{\n    /// A.\n    ToolExecutionEnd {{\n        agent_id: \
             String,\n        tool_call_id: String,\n{extra}        is_error: bool,\n    }},\n    \
             StateChange {{\n        from: String,\n        to: String,\n    }},\n}}\n"
        )
    };
    std::fs::write(root.join(EVENT), event("")).expect("event");
    std::fs::write(
        root.join(WIRE),
        "pub fn event_type_name(event: &AgentEvent) -> &str {\n    match event {\n        \
         AgentEvent::ToolExecutionEnd { .. } => \"tool_execution_end\",\n        \
         AgentEvent::StateChange { .. } => \"state_change\",\n    }\n}\n",
    )
    .expect("wire");

    std::fs::write(
        root.join(WEB).join("bad.ts"),
        "export function f(event: AgentEvent) {\n  const p = event.payload;\n  \
         switch (event.type) {\n    case 'tool_execution_end':\n      \
         return [p.tool_call_id, p.tool_name];\n  }\n}\n",
    )
    .expect("bad");
    let (complaints, judged) = contradictions(&root);
    assert_eq!(judged, 1, "{complaints:?}");
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(
        complaints[0].contains("读 [\"tool_name\"]"),
        "要报告的正是那一个多出来的名字，一个不多一个不少：{}",
        complaints[0]
    );

    // 把名字写进变体，这一臂就成立了：判据盯的是「写者写没写」，不是「读法像不像」。
    std::fs::write(root.join(EVENT), event("        tool_name: String,\n")).expect("event");
    assert!(contradictions(&root).0.is_empty());

    // 一条臂都不认得的线上名字要说出来，而不是静悄悄跳过。
    std::fs::write(
        root.join(WEB).join("bad.ts"),
        "export function f(event: AgentEvent) {\n  const p = event.payload;\n  \
         switch (event.type) {\n    case 'tool_execution_gone':\n      return p.tool_call_id;\n  \
         }\n}\n",
    )
    .expect("bad");
    let (complaints, _) = contradictions(&root);
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(
        complaints[0].contains("tool_execution_gone"),
        "{complaints:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// 普查面本身也要有判据：读法目录读不出来时必须炸。旧写法读不出来就 `return`，
/// 运维页那十几条臂足以让 `judged > 0` 照旧成立——题目少了一整片，判词不变。
#[test]
fn a_reading_directory_that_cannot_be_read_is_refused() {
    let missing =
        std::env::temp_dir().join(format!("cogneva-web-payload-absent-{}", std::process::id()));
    let mut files = Vec::new();
    let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        collect_ts(&missing, &mut files)
    }));
    assert!(
        got.is_err(),
        "web/src 读不出来时读法普查面为空，而这一跑照旧会印「每一臂读的字段\
         都是那个变体带着的」"
    );
}
