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
//! - **`messages` 取到倒数一条之前**：最后一条是这一轮新说的话，它不属于前缀；前面那些
//!   在下一轮里原样重发。空会话（没有 messages）时前缀就是那三个字段。
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
    // **全部历史都进前缀**：上一轮发出去的每一段字节都在缓存里，包括它最后一条。
    // 把最后一条当「这一轮新说的」剔掉，上一轮的前缀就成不了这一轮的开头——
    // 而那正是缓存唯一的判据。
    if let Some(messages) = body.get("messages") {
        stable.insert("messages".to_string(), messages.clone());
    }
    // `to_vec` 走的是同一套键序规则，所以这串字节两次算出来一样。
    serde_json::to_vec(&Value::Object(stable)).unwrap_or_default()
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

    /// 同一份请求算两次是同一串字节：键序固定是这条判据的前提，不是巧合。
    #[test]
    fn the_same_request_yields_the_same_bytes_twice() {
        let a = body(json!([{"role": "user", "content": "问"}]));
        assert_eq!(prefix(&a), prefix(&a));
    }
}
