//! 部署里写的 planner 答案形状，与树里那条待办的类型是同一组字段。
//!
//! 写计划的那次调用要交出「每一条待办的三格」（终态／现在／差距）＋序号与状态；这
//! 一形状写在 `deploy/helm/cogneva/files/cogneva.json` 的 `pge.schemas.planner` 里，
//! 而读它的那一侧是 `cog_core::contract::ledger_item::LedgerItem`。两处各写一份字段名
//! 时，改名只会让一边悄悄不认——模型照着一边写，另一边读出来是空的，而两边都不报错。
//!
//! 判据：配置里 `items.items.required` 的字段集，与 `LedgerItem` 生成的 schema 的
//! required 相同；状态词的闭集也相同。

use std::collections::BTreeSet;

fn config() -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("deploy/helm/cogneva/files/cogneva.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("配置读不到：{}（{e}）", path.display()));
    serde_json::from_str(&text).expect("配置不是 JSON")
}

#[test]
fn the_configured_answer_shape_names_the_same_fields_as_the_ledger_item() {
    let doc = config();
    let item = doc
        .pointer("/pge/schemas/planner/properties/items/items")
        .expect("部署里没有 planner 的 items 答案形状（pge.schemas.planner）");

    let configured: BTreeSet<&str> = item
        .get("required")
        .and_then(|v| v.as_array())
        .expect("planner 的 items 没写 required")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();

    // 用**序列化出去的**字段名比，而不是编译期的字段名：改名、改 serde 的写法都会让
    // 接口那一侧变，而读的人看到的正是序列化后的那一份。
    use cog_core::contract::ledger_item::{LedgerItem, LedgerState};
    let sample = LedgerItem {
        id: String::new(),
        terminal: String::new(),
        now: String::new(),
        gap: String::new(),
        state: LedgerState::Open,
    };
    let value = serde_json::to_value(&sample).expect("这一条序列化不了");
    let expected: BTreeSet<&str> = value
        .as_object()
        .expect("一个对象")
        .keys()
        .map(|s| s.as_str())
        .collect();

    assert_eq!(
        configured, expected,
        "部署里 planner 的答案形状与 LedgerItem 的字段对不上：配置 {configured:?}，类型 {expected:?}"
    );

    // 状态是闭集两词，两边同一对。
    let states: Vec<&str> = item
        .pointer("/properties/state/enum")
        .and_then(|v| v.as_array())
        .expect("planner 的 items 没写状态闭集")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert_eq!(
        states,
        vec!["未落", "已落"],
        "状态闭集与台账那一面不是同一对词"
    );
}
