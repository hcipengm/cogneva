//! principle: feedback_no_hardcoding_config_surface
//!
//! 模型吐字那一路事件的线上名字只有一个写者：`AssistantMessageEvent` 用
//! `#[serde(tag = "type", rename_all = "snake_case")]` 把变体名写成标签。两个客户端
//! 各自拿一个标签做分支，决定这段增量是不是「要给用户看的正文」：
//!
//! - WebUI 的 `extractDelta` 只把 `text_delta` 当正文，其余（含 `thinking_delta`，
//!   模型的内部草稿）一律丢掉；
//! - 嵌在网关里的运维页把 `thinking_delta` 认成「正在思考」，别的都画成「正在写」。
//!
//! 两边各拼一遍这个标签。拼错时不会报错，只会一直走另一支：
//!
//! - `extractDelta` 拼错的那天，聊天面板的正文**永远不会出现**——回复一直在总线上，
//!   屏幕上是一条空消息，和「这一轮模型什么也没说」同一种读数。
//! - 运维页拼错的那天，活动流永远显示「正在写」，和「模型从不思考」同一种读数。
//!
//! 所以判据是：客户端比较的每一个标签都必须是这个枚举真写出来的；枚举读不出
//! （没有 `snake_case`、变体解析不了）或者某个客户端一个比较都读不到，都按红处理
//! ——读不出来的判据判不了任何东西，绿是假绿。
//!
//! 实测（2026-10-27）：枚举 13 个标签；客户端比较 2 处（`useEventStream.ts` 的
//! `text_delta`、运维页的 `thinking_delta`），两处都成立。消融：把运维页的
//! `thinking_delta` 改成 `thinking_delta_x`，本门红并点名那一处；改回即绿。

use std::collections::BTreeSet;
use std::path::Path;

use crate::common;

use common::rust_text::snake_case;

use common::js_literal::quoted_word;
use common::lex::{split_top_level, Dialect};
use common::routes::{matching_brace, read, repo_root, strip_comments};
use common::rust_production::production_text;

const EVENT: &str = "crates/cog-core/src/types/event.rs";
const ENUM: &str = "AssistantMessageEvent";

/// 比较这些标签的客户端。两个都是发出去的产品：WebUI 与嵌在网关二进制里的运维页。
const CLIENTS: &[&str] = &[
    "web/src/hooks/useEventStream.ts",
    "crates/cog-gateway/src/takeover.html",
];

/// 客户端给「从载荷里解出来的那个事件对象」起的名字，都直接从这些词旁边绑出来。
const CARRIERS: &[&str] = &["assistant_event", "assistantEvent"];

/// 枚举序列化出去的标签，或 `None`：没有 `snake_case` 重命名、有变体解析不了、或者
/// 根本没有这个枚举。`None` 是红，不是空词表——空词表会让每个标签都像缺陷，
/// 「有什么算什么」又会让一个都不像。
fn enum_tags(source: &str, name: &str) -> Option<BTreeSet<String>> {
    let source = strip_comments(source);
    let at = source.find(&format!("enum {name}"))?;
    // 属性写在声明上面，往回取 240 字节可能落在多字节字符中间，先走到边界上。
    let mut from = at.saturating_sub(240);
    while from > 0 && !source.is_char_boundary(from) {
        from -= 1;
    }
    if !source[from..at].contains("rename_all = \"snake_case\"") {
        return None;
    }
    let brace = at + source[at..].find('{')?;
    let end = matching_brace(&source, brace, Dialect::Rust)?;
    let mut tags = BTreeSet::new();
    // 按顶层逗号切：变体自己的结构体字段、属性里的括号都留在同一片里。
    for part in split_top_level(&source[brace + 1..end], Dialect::Rust) {
        let mut part = part.trim();
        if part.is_empty() {
            continue;
        }
        // 变体可以带自己的属性；名字在开头那串 `#[…]` 之后。反过来找最后一个 `]`
        // 是不行的：结构体变体的字段上也能挂属性，那个 `]` 在名字后面很远。
        while part.starts_with("#[") {
            let close = part.find(']')?;
            part = part[close + 1..].trim_start();
        }
        let variant: String = part
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            return None;
        }
        tags.insert(snake_case(&variant));
    }
    if tags.is_empty() {
        return None;
    }
    Some(tags)
}

/// 客户端里「从载荷解出来的那个事件对象」被 `const` 绑到了哪些名字上。
///
/// 只认同一行里 `const <名字> = …<carrier>…` 这一种：比较写在别处（比如直接把参数
/// 拿来比）就认不出名字，而认不出名字的那一处比较不会被判——所以一个名字都绑不出来
/// 时整份文件按红处理。
fn carriers(text: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for carrier in CARRIERS {
        let mut cursor = 0;
        while let Some(offset) = text[cursor..].find(carrier) {
            let at = cursor + offset;
            cursor = at + carrier.len();
            let line_start = text[..at].rfind('\n').map_or(0, |i| i + 1);
            let Some(bind) = text[line_start..at].rfind("const ") else {
                continue;
            };
            let rest = text[line_start + bind + "const ".len()..at].trim_start();
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
                .collect();
            if !name.is_empty() && rest[name.len()..].trim_start().starts_with('=') {
                names.insert(name);
            }
        }
    }
    names
}

/// `<名字>.type` 与某个词比较时写的词，包括 `switch (<名字>.type)` 上的 `case` 标签。
///
/// 两种写法都算：直接比较是这一路最常见的样子，`case` 是把同一个词表铺开写的样子。
fn compared_tags(text: &str, names: &BTreeSet<String>) -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    for name in names {
        let subject = format!("{name}.type");
        let mut cursor = 0;
        while let Some(offset) = text[cursor..].find(&subject) {
            cursor += offset + subject.len();
            let rest = text[cursor..].trim_start();
            // `===` / `!==` / `==` / `!=` 后面才是词；`=` 是赋值，跳过。
            let op = ["===", "!==", "==", "!="]
                .into_iter()
                .find(|op| rest.starts_with(op));
            if let Some(op) = op {
                if let Some(word) = quoted_word(&rest[op.len()..]) {
                    tags.insert(word);
                }
            }
        }
        // `switch (<名字>.type) { case '…': … }`
        for form in [format!("switch ({subject})"), format!("switch({subject})")] {
            let mut cursor = 0;
            while let Some(offset) = text[cursor..].find(&form) {
                cursor += offset + form.len();
                let Some(brace_off) = text[cursor..].find('{') else {
                    continue;
                };
                let brace = cursor + brace_off;
                let Some(end) = matching_brace(text, brace, Dialect::JavaScript) else {
                    continue;
                };
                let body = &text[brace + 1..end];
                let mut inner = 0;
                while let Some(offset) = body[inner..].find("case ") {
                    let at = inner + offset;
                    inner = at + "case ".len();
                    // `lowercase ` 里也有 `case `；`case` 前面必须是开头或分隔符。
                    let before = body[..at].chars().next_back();
                    if before.is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$') {
                        continue;
                    }
                    if let Some(word) = quoted_word(&body[inner..]) {
                        tags.insert(word);
                    }
                }
            }
        }
    }
    tags
}

/// 一条罪状一种写法，方便用例点名。
fn contradictions(root: &Path) -> Vec<String> {
    let mut complaints = Vec::new();
    let Some(tags) = enum_tags(&production_text(&read(&root.join(EVENT))), ENUM) else {
        complaints.push(format!(
            "读不出 `{ENUM}` 的标签：要么枚举改了名，要么它不再用 `snake_case` 重命名。\
             读不出来的判据判不了任何东西，所以按红处理"
        ));
        return complaints;
    };
    for client in CLIENTS {
        let text = strip_comments(&read(&root.join(client)));
        let names = carriers(&text);
        if names.is_empty() {
            complaints.push(format!(
                "读不出 `{client}` 从哪里解出事件对象：没有一行 `const <名字> = …` 绑到\
                 `assistant_event`。找不到绑定的比较不会被判，所以按红处理"
            ));
            continue;
        }
        let compared = compared_tags(&text, &names);
        if compared.is_empty() {
            complaints.push(format!(
                "读不出 `{client}` 比较了哪些标签（绑定名 {names:?}）：它可能不再看这一路\
                 事件，也可能换了一种写法。读不出来按红处理"
            ));
            continue;
        }
        for word in &compared {
            if tags.contains(word) {
                continue;
            }
            complaints.push(format!(
                "  `{client}` 拿 `{word}` 和事件标签比，而 `{ENUM}` 从不写这个词\
                 （它只有 {tags:?}）：这一支永远不成立——正文永远不出现、或者永远显示\
                 「正在写」，与「这一轮没有内容」同一种读数"
            ));
        }
    }
    complaints
}

#[test]
fn every_tag_a_client_compares_is_a_tag_the_enum_writes() {
    let complaints = contradictions(&repo_root());
    assert!(
        complaints.is_empty(),
        "客户端按模型事件的标签分支，而它拼的词不是枚举写出来的。拼错的那一支不会\
         报错，只是永远不成立：\n{}",
        complaints.join("\n")
    );
}

#[test]
fn the_census_is_wide_enough_to_mean_something() {
    let root = repo_root();
    let tags = enum_tags(&production_text(&read(&root.join(EVENT))), ENUM)
        .expect("读不出 AssistantMessageEvent 的标签");
    assert!(
        tags.len() >= 10,
        "枚举只读出 {} 个标签：{tags:?}",
        tags.len()
    );
    for client in CLIENTS {
        let text = strip_comments(&read(&root.join(client)));
        let names = carriers(&text);
        let compared = compared_tags(&text, &names);
        assert!(
            !compared.is_empty(),
            "`{client}` 一处比较也没读出来（绑定名 {names:?}）"
        );
    }
}

const FIXTURE_EVENT: &str = "\
#[derive(Serialize)]
#[serde(tag = \"type\", rename_all = \"snake_case\")]
pub enum AssistantMessageEvent {
    Start {
        #[serde(default = \"Utc::now\")]
        timestamp: DateTime<Utc>,
    },
    TextDelta {
        content_index: usize,
        delta: String,
    },
    ThinkingDelta {
        content_index: usize,
        delta: String,
    },
    Done,
}
";

const FIXTURE_WEB: &str = "\
function extractDelta(assistantEvent: unknown): string | null {
  const e = assistantEvent as { type?: string; delta?: string } | null;
  if (!e) return null;
  return e.type === 'text_delta' ? e.delta ?? null : null;
}
";

const FIXTURE_PAGE: &str = "\
function feed(msg) {
  const ae = msg.assistant_event;
  const phase = ae.type === 'thinking_delta' ? 'thinking' : 'writing';
  return phase;
}
";

fn fixture(root: &Path, event: &str, web: &str, page: &str) {
    for (relative, body) in [
        (EVENT, event),
        ("web/src/hooks/useEventStream.ts", web),
        ("crates/cog-gateway/src/takeover.html", page),
    ] {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
        std::fs::write(path, body).expect("write");
    }
}

#[test]
fn the_fixture_is_green_before_any_tag_is_changed() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path(), FIXTURE_EVENT, FIXTURE_WEB, FIXTURE_PAGE);
    let complaints = contradictions(dir.path());
    assert!(complaints.is_empty(), "{complaints:?}");
}

#[test]
fn a_tag_the_enum_never_writes_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let page = FIXTURE_PAGE.replace("'thinking_delta'", "'thinking_delta_x'");
    assert_ne!(page, FIXTURE_PAGE);
    fixture(dir.path(), FIXTURE_EVENT, FIXTURE_WEB, &page);
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("thinking_delta_x"), "{complaints:?}");
}

#[test]
fn a_case_label_the_enum_never_writes_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let web = "\
function fold(assistantEvent) {
  const e = assistantEvent;
  switch (e.type) {
    case 'text_delta':
      return 'text';
    case 'ghost_delta':
      return 'ghost';
  }
  return null;
}
";
    fixture(dir.path(), FIXTURE_EVENT, web, FIXTURE_PAGE);
    let complaints = contradictions(dir.path());
    assert!(
        complaints.iter().any(|c| c.contains("ghost_delta")),
        "{complaints:?}"
    );
}

#[test]
fn an_enum_without_the_snake_case_rename_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    let event = FIXTURE_EVENT.replace("rename_all = \"snake_case\"", "rename_all = \"camelCase\"");
    assert_ne!(event, FIXTURE_EVENT);
    fixture(dir.path(), &event, FIXTURE_WEB, FIXTURE_PAGE);
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("按红处理"), "{complaints:?}");
}

#[test]
fn a_client_that_binds_nothing_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    fixture(dir.path(), FIXTURE_EVENT, FIXTURE_WEB, "const x = 1;\n");
    let complaints = contradictions(dir.path());
    assert_eq!(complaints.len(), 1, "{complaints:?}");
    assert!(complaints[0].contains("按红处理"), "{complaints:?}");
}
