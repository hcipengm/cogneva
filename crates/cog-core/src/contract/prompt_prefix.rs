//! 请求的**稳定前缀**：同一会话的下一轮，是从这一串字节开始的。
//!
//! 缓存命中只由一件事决定：上一轮发出去的字节，这一轮是不是还以它们开头。所以「前缀」
//! 不是一句愿望，它得是**能算出来的字节**——而这个模块就是那次计算。它只回答一个问题：
//! 一次请求里哪些字节在下一轮里不会变。
//!
//! 三条口径，都是这条判据逼出来的：
//!
//! - **键序固定**：`serde_json::Value` 的 `Map` 默认按键排序，所以同一份对象两次序列化
//!   得到同一串字节——「同一个意思」不等于「同一串字节」，而缓存认的是后者。
//! - **只取稳定字段**：`model`、`system`/`instructions`、`tools`。其余字段（温度、上限、
//!   各种开关）不参与前缀：它们一变，整条前缀就得重算，而那正是「为什么命中率是 1%」
//!   最容易被忽略的一半。
//! - **`messages` 按顺序整段参与**，包括最后一条：下一轮把它当作历史原样重发，所以这一轮
//!   发出去的每一条都会出现在下一轮的开头。空会话（没有 messages）时前缀就是那三个字段。
//!   （这段原写作「取到倒数一条之前」，与实现对不上；实现是对的——漏掉最后一条会把
//!   「上一轮的字节」算短一截，而缓存认的正是那串字节。）
//!
//! 这条判据今天不存在，所以「同会话两轮、第二轮不以第一轮开头」这件事只能靠人读日志
//! 发现——而它读起来只是「命中率低」，看不出是哪一层断的。

use serde_json::{Map, Value};

/// 参与前缀的字段：它们的值在下一轮里不变。
pub const STABLE_FIELDS: [&str; 3] = ["model", "system", "tools"];

/// 一次请求的稳定前缀，按固定字节形状给出。
///
/// 同一个会话的连续两轮，第二轮的 [`prefix`] 必须以第一轮的那一串开头——这是缓存
/// 命中的机械前提，也是这条判据唯一断言的事。
pub fn prefix(body: &Map<String, Value>) -> Vec<u8> {
    let mut stable = Map::new();
    for field in STABLE_FIELDS {
        if let Some(value) = body.get(field) {
            stable.insert(field.to_string(), value.clone());
        }
    }
    // 前缀是**一串按顺序拼起来的字节**：先是那几个不动的字段，然后历史里的每一条
    // 逐条拼接。不能把 `messages` 当一个 JSON 数组整体序列化——数组的 `]` 一旦出现，
    // 上一轮的字节就不可能是这一轮的开头，而缓存认的正是「上一轮那串字节是这一轮的
    // 开头」。逐条拼，追加第 N+1 条时前面的字节一个都没变。
    let mut out = serde_json::to_vec(&Value::Object(stable)).unwrap_or(Default::default());
    if let Some(Value::Array(messages)) = body.get("messages") {
        for message in messages {
            out.extend_from_slice(&serde_json::to_vec(message).unwrap_or_default());
        }
    }
    out
}

/// 前缀断了的时候，是哪一类稳定字段先变的。
///
/// 命中率低只是一个数；「为什么低」要能分开：工具定义变了、system 变了、还是根本没有
/// 前缀（模型换了）。三者要做的活完全不同——工具定义变通常是装配顺序问题，system 变是
/// 有人在里面注入了每轮都会变的东西，模型换则前缀本来就该重来。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixBreak {
    /// 接得上，没有断。
    None,
    /// `tools` 变了。
    Tools,
    /// `system` 变了。
    System,
    /// 模型变了：前缀从这一格起本来就不同。
    Model,
    /// 只有历史变了（或历史被改写）：前一条要重发，而后一条会整段作废。
    Messages,
}

/// 分类一次前缀断裂。
pub fn classify_break(previous: &Map<String, Value>, next: &Map<String, Value>) -> PrefixBreak {
    if extends(previous, next) {
        return PrefixBreak::None;
    }
    for (field, which) in [
        ("model", PrefixBreak::Model),
        ("system", PrefixBreak::System),
        ("tools", PrefixBreak::Tools),
    ] {
        if previous.get(field) != next.get(field) {
            return which;
        }
    }
    PrefixBreak::Messages
}

/// 前缀是不是**逐字节**接得上：上一轮那一串是否就是这一轮的开头。
///
/// 用它而不是比较两份 `Value`：缓存认字节，不认「意思相同」。
pub fn extends(previous: &Map<String, Value>, next: &Map<String, Value>) -> bool {
    let before = prefix(previous);
    let after = prefix(next);
    after.starts_with(&before)
}

impl PrefixBreak {
    /// 写进读数的那一格词。
    pub fn as_str(self) -> &'static str {
        match self {
            PrefixBreak::None => "none",
            PrefixBreak::Tools => "tools",
            PrefixBreak::System => "system",
            PrefixBreak::Model => "model",
            PrefixBreak::Messages => "messages",
        }
    }

    /// 从读数那一格词读回来；认不出的词是 `None`（不替它编一个类别）。
    pub fn parse(word: &str) -> Option<Self> {
        Some(match word {
            "none" => PrefixBreak::None,
            "tools" => PrefixBreak::Tools,
            "system" => PrefixBreak::System,
            "model" => PrefixBreak::Model,
            "messages" => PrefixBreak::Messages,
            _ => return None,
        })
    }
}

/// 一次请求的前缀清单：稳定三段各自的哈希、历史每一条的哈希、整条前缀的哈希。
///
/// 为什么要分开留三段的哈希：会话里连着两次调用，问「这一次为什么没有命中」只要
/// 比这三格——工具定义变了、system 里注入了每轮都变的东西、还是换了模型。只留整条
/// 前缀的哈希，能答的只有「没命中」，答不出该修哪一层。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrefixManifest {
    /// `model` 的哈希（没有这个字段就没有）。
    pub model: Option<String>,
    /// `system`／`instructions` 的哈希。
    pub system: Option<String>,
    /// `tools` 的哈希。
    pub tools: Option<String>,
    /// 每一条 message 的哈希，按发送顺序。
    pub segments: Vec<String>,
    /// 整条前缀的哈希：两次调用相同，前缀就是同一串字节。
    pub prefix: String,
}

fn field_hash(body: &Map<String, Value>, field: &str) -> Option<String> {
    use sha2::{Digest, Sha256};
    let value = body.get(field)?;
    Some(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).unwrap_or_default())
    ))
}

/// 一次请求的前缀清单，从请求体算出来。
///
/// 这是**唯一**算这份清单的地方：写的人（网关落地那一刻）与读的人（同会话相邻两次
/// 调用的比较）用的是同一段代码，两处各算一次时，「前缀没断」与「读的人算短了一截」
/// 在读数上同形。
pub fn manifest(body: &Map<String, Value>) -> PrefixManifest {
    use sha2::{Digest, Sha256};
    let segments: Vec<String> = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .map(|messages| {
            messages
                .iter()
                .map(|message| {
                    format!(
                        "{:x}",
                        Sha256::digest(serde_json::to_vec(message).unwrap_or_default())
                    )
                })
                .collect()
        })
        .unwrap_or(Default::default());
    PrefixManifest {
        model: field_hash(body, "model"),
        system: field_hash(body, "system").or_else(|| field_hash(body, "instructions")),
        tools: field_hash(body, "tools"),
        segments,
        prefix: format!("{:x}", Sha256::digest(prefix(body))),
    }
}

/// 清单的 JSON 形状，落在调用事实的 `prompt_manifest` 那一格里。
pub fn manifest_json(body: &Map<String, Value>) -> String {
    let manifest = manifest(body);
    serde_json::json!({
        "model": manifest.model,
        "system": manifest.system,
        "tools": manifest.tools,
        "segments": manifest.segments,
        "prefix": manifest.prefix,
    })
    .to_string()
}

/// 把落下来的那一格读回清单；读不回来是 `None`，不猜。
pub fn parse_manifest(json: &str) -> Option<PrefixManifest> {
    let value: Value = serde_json::from_str(json).ok()?;
    let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_string);
    let segments = value
        .get("segments")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    Some(PrefixManifest {
        model: text("model"),
        system: text("system"),
        tools: text("tools"),
        segments,
        prefix: text("prefix")?,
    })
}

/// 同会话相邻两次调用之间，前缀断在哪一格——**只看两份清单**，不再回看请求体。
///
/// 先比三段稳定字段，再比历史：历史必须以上一次的整段开头，否则是「改写」而不是
/// 「追加」，两者要做的活不同（前者是重发整段，后者只是这一次命中不了）。
pub fn classify_manifest_break(previous: &PrefixManifest, next: &PrefixManifest) -> PrefixBreak {
    if previous.prefix == next.prefix {
        return PrefixBreak::None;
    }
    if previous.model != next.model {
        return PrefixBreak::Model;
    }
    if previous.system != next.system {
        return PrefixBreak::System;
    }
    if previous.tools != next.tools {
        return PrefixBreak::Tools;
    }
    if next.segments.starts_with(&previous.segments) {
        return PrefixBreak::None;
    }
    PrefixBreak::Messages
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn body(messages: Value) -> Map<String, Value> {
        json!({
            "model": "m-1",
            "system": "you are a careful worker",
            "tools": [{"name": "read_file", "description": "read"}],
            "messages": messages,
            "temperature": 0.2,
        })
        .as_object()
        .expect("an object")
        .clone()
    }

    /// 同一会话连到第三轮：每一轮的前缀都以前一轮的**全部历史**开头。
    #[test]
    fn the_next_turn_starts_with_the_bytes_the_previous_one_sent() {
        let turn1 = body(json!([{"role": "user", "content": "第一问"}]));
        let turn2 = body(json!([
            {"role": "user", "content": "第一问"},
            {"role": "assistant", "content": "第一答"},
            {"role": "user", "content": "第二问"},
        ]));
        let turn3 = body(json!([
            {"role": "user", "content": "第一问"},
            {"role": "assistant", "content": "第一答"},
            {"role": "user", "content": "第二问"},
            {"role": "assistant", "content": "第二答"},
            {"role": "user", "content": "第三问"},
        ]));
        assert!(extends(&turn1, &turn2), "第二轮没有以第一轮为前缀");
        assert!(extends(&turn2, &turn3), "第三轮没有以第二轮为前缀");
    }

    /// 会变的字段不进前缀：`temperature` 改一下不该让整条历史失效。
    #[test]
    fn a_field_that_changes_between_turns_is_not_part_of_the_prefix() {
        let a = body(json!([{"role": "user", "content": "问"}]));
        let mut b = body(json!([
            {"role": "user", "content": "问"},
            {"role": "assistant", "content": "答"},
            {"role": "user", "content": "再问"},
        ]));
        b.insert("temperature".to_string(), json!(0.9));
        assert!(extends(&a, &b), "一个每轮都变的开关进了前缀");
    }

    /// 工具定义进前缀：它一变，整条历史确实失效——这是对的，而它必须看得见。
    #[test]
    fn a_changed_tool_definition_breaks_the_prefix() {
        let a = body(json!([{"role": "user", "content": "问"}]));
        let mut b = body(json!([
            {"role": "user", "content": "问"},
            {"role": "assistant", "content": "答"},
            {"role": "user", "content": "再问"},
        ]));
        b.insert(
            "tools".to_string(),
            json!([{"name": "read_file", "description": "read a different way"}]),
        );
        assert!(!extends(&a, &b), "工具定义变了，前缀却读作没变");
    }

    /// 断在哪一格要说得出来：工具 / system / 模型 / 历史，四种分开。
    #[test]
    fn a_broken_prefix_names_the_field_that_broke_it() {
        let base = body(json!([{"role": "user", "content": "问"}]));
        let grown = body(json!([
            {"role": "user", "content": "问"},
            {"role": "assistant", "content": "答"},
            {"role": "user", "content": "再问"},
        ]));
        assert_eq!(classify_break(&base, &grown), PrefixBreak::None);

        let mut system_moved = grown.clone();
        system_moved.insert("system".into(), json!("a different instruction"));
        assert_eq!(classify_break(&base, &system_moved), PrefixBreak::System);

        let mut model_moved = grown.clone();
        model_moved.insert("model".into(), json!("m-2"));
        assert_eq!(classify_break(&base, &model_moved), PrefixBreak::Model);

        // 历史被改写（第一条变了）不是「接不上」，是「整段作废」——两者要分的活不同。
        let mut rewritten = grown.clone();
        rewritten.insert(
            "messages".into(),
            json!([
                {"role": "user", "content": "改过的第一问"},
                {"role": "assistant", "content": "答"},
                {"role": "user", "content": "再问"},
            ]),
        );
        assert_eq!(classify_break(&base, &rewritten), PrefixBreak::Messages);
    }

    /// 连着两轮的清单：第二轮的历史以第一轮的整段开头，所以是 `None`。
    #[test]
    fn adjacent_turns_are_an_extension_from_their_own_manifests() {
        let turn1 = body(json!([{"role": "user", "content": "第一问"}]));
        let turn2 = body(json!([
            {"role": "user", "content": "第一问"},
            {"role": "assistant", "content": "第一答"},
            {"role": "user", "content": "第二问"},
        ]));
        let m1 = manifest(&turn1);
        let m2 = manifest(&turn2);
        assert_eq!(classify_manifest_break(&m1, &m2), PrefixBreak::None);
        assert_eq!(m2.segments.len(), 3);
        assert_eq!(&m2.segments[..1], &m1.segments[..1]);
    }

    /// 断在哪一格，从两份清单里就答得出来——不需要回看请求体。
    #[test]
    fn a_manifest_names_the_field_that_broke() {
        let base = body(json!([{"role": "user", "content": "问"}]));
        let grown = body(json!([
            {"role": "user", "content": "问"},
            {"role": "assistant", "content": "答"},
        ]));
        let m_base = manifest(&base);
        let m_grown = manifest(&grown);
        assert_eq!(
            classify_manifest_break(&m_base, &m_grown),
            PrefixBreak::None
        );

        let mut system_moved = grown.clone();
        system_moved.insert("system".into(), json!("a different instruction"));
        assert_eq!(
            classify_manifest_break(&m_base, &manifest(&system_moved)),
            PrefixBreak::System
        );

        let mut tools_moved = grown.clone();
        tools_moved.insert("tools".into(), json!([{"name": "write_file"}]));
        assert_eq!(
            classify_manifest_break(&m_base, &manifest(&tools_moved)),
            PrefixBreak::Tools
        );

        let mut model_moved = grown.clone();
        model_moved.insert("model".into(), json!("m-2"));
        assert_eq!(
            classify_manifest_break(&m_base, &manifest(&model_moved)),
            PrefixBreak::Model
        );

        let mut rewritten = grown.clone();
        rewritten.insert(
            "messages".into(),
            json!([{"role": "user", "content": "改过的第一问"}]),
        );
        assert_eq!(
            classify_manifest_break(&m_base, &manifest(&rewritten)),
            PrefixBreak::Messages
        );
    }

    /// 清单落成 JSON 再读回来，分类结论不变；读不回来的不是「没断」。
    #[test]
    fn a_manifest_reads_back_from_its_json() {
        let turn1 = body(json!([{"role": "user", "content": "问"}]));
        let turn2 = body(json!([
            {"role": "user", "content": "问"},
            {"role": "assistant", "content": "答"},
        ]));
        let json = manifest_json(&turn1);
        let parsed = parse_manifest(&json).expect("a manifest this module wrote must read back");
        assert_eq!(parsed, manifest(&turn1));
        assert_eq!(
            classify_manifest_break(&parsed, &manifest(&turn2)),
            PrefixBreak::None
        );
        assert_eq!(parse_manifest("not json"), None);
        assert_eq!(parse_manifest("{\"segments\":[]}"), None);
    }

    /// 同一份请求算两次是同一串字节：键序固定是这条判据的前提，不是巧合。
    #[test]
    fn the_same_request_yields_the_same_bytes_twice() {
        let a = body(json!([{"role": "user", "content": "问"}]));
        assert_eq!(prefix(&a), prefix(&a));
    }
}
