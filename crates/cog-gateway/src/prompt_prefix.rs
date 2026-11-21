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
    // 倒数一条消息是这一轮新说的，不属于前缀；空会话时这一格是空数组而不是缺席——
    // 「没有历史」与「字段没带」在缓存看来是两串不同的字节。
    if let Some(Value::Array(messages)) = body.get("messages") {
        let history = if messages.is_empty() {
            Vec::new()
        } else {
            messages[..messages.len() - 1].to_vec()
        };
        stable.insert("messages".to_string(), Value::Array(history));
    }
    // `to_vec` 走的是同一套键序规则，所以这串字节两次算出来一样。
    serde_json::to_vec(&Value::Object(stable)).unwrap_or_default()
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

    /// 同一份请求算两次是同一串字节：键序固定是这条判据的前提，不是巧合。
    #[test]
    fn the_same_request_yields_the_same_bytes_twice() {
        let a = body(json!([{"role": "user", "content": "问"}]));
        assert_eq!(prefix(&a), prefix(&a));
    }
}
