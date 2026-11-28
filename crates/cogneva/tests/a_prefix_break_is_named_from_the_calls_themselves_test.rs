//! 同会话连着的两次调用「为什么没命中」，从两次调用**自己记下的清单**里答得出来。
//!
//! 缓存命中只由一件事决定：上一轮发出去的字节，这一轮是不是还以它们开头。所以每一
//! 次调用落下的那一格里要有三段稳定字段（`model`／`system`／`tools`）各自的哈希、历史
//! 每一条的哈希、以及整条前缀的哈希；同会话相邻两条一比，就答得出来断在哪一格——工具
//! 定义变了、system 里注入了每轮都变的东西、换了模型，还是历史被改写。三样要做的活
//! 完全不同，而只留一个「命中率」的读数把它们糊成一件事。
//!
//! 这条门没有这次改动会红：`prompt_prefix::manifest` 不存在（清单只有历史那一段），
//! 而 `prefix_breaks_in_session` 也不存在（清单写了没人读，等于没写）。

use serde_json::{json, Map, Value};

use cog_core::contract::prompt_prefix::{
    classify_manifest_break, manifest, manifest_json, parse_manifest, PrefixBreak,
};

fn body(messages: Value) -> Map<String, Value> {
    json!({
        "model": "m-1",
        "system": "you are a careful worker",
        "tools": [{"name": "read_file"}],
        "messages": messages,
    })
    .as_object()
    .expect("an object")
    .clone()
}

#[test]
fn the_second_turn_reads_as_an_extension_of_the_first() {
    let turn1 = body(json!([{"role": "user", "content": "第一问"}]));
    let turn2 = body(json!([
        {"role": "user", "content": "第一问"},
        {"role": "assistant", "content": "第一答"},
        {"role": "user", "content": "第二问"},
    ]));
    assert_eq!(
        classify_manifest_break(&manifest(&turn1), &manifest(&turn2)),
        PrefixBreak::None
    );
}

#[test]
fn the_break_names_the_layer_that_broke_it() {
    let base = body(json!([{"role": "user", "content": "问"}]));
    let grown = body(json!([
        {"role": "user", "content": "问"},
        {"role": "assistant", "content": "答"},
    ]));

    let mut system = grown.clone();
    system.insert("system".into(), json!("injected every turn"));
    assert_eq!(
        classify_manifest_break(&manifest(&base), &manifest(&system)),
        PrefixBreak::System
    );

    let mut tools = grown.clone();
    tools.insert("tools".into(), json!([{"name": "write_file"}]));
    assert_eq!(
        classify_manifest_break(&manifest(&base), &manifest(&tools)),
        PrefixBreak::Tools
    );

    let mut model = grown.clone();
    model.insert("model".into(), json!("m-2"));
    assert_eq!(
        classify_manifest_break(&manifest(&base), &manifest(&model)),
        PrefixBreak::Model
    );

    let mut rewritten = grown.clone();
    rewritten.insert(
        "messages".into(),
        json!([{"role": "user", "content": "改过的第一问"}]),
    );
    assert_eq!(
        classify_manifest_break(&manifest(&base), &manifest(&rewritten)),
        PrefixBreak::Messages
    );
}

/// 清单的算法只有一处：网关落账那一刻用它写，读的人用它比。两处各算一次时，
/// 「前缀没断」与「读的人算短了一截」在读数上同形。
#[test]
fn one_place_computes_the_manifest_and_one_reader_compares_them() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let contract =
        std::fs::read_to_string(root.join("crates/cog-core/src/contract/llm_call_event.rs"))
            .expect("llm_call_event.rs");
    assert!(
        contract.contains("super::prompt_prefix::manifest_json"),
        "落账那一刻没有走共用的清单算法：两份清单迟早各算各的"
    );
    let reader =
        std::fs::read_to_string(root.join("crates/cog-storage/src/postgres/llm_call_events.rs"))
            .expect("llm_call_events.rs");
    assert!(
        reader.contains("prefix_breaks_in_session"),
        "清单写了没人读：命中率低仍然只能读出「低」，读不出断在哪一格"
    );
    // 没有清单的那一次调用不能被跳过：跨过它去比前后两条，报出来的「接得上」是关于
    // 两条彼此没见过的请求说的。
    assert!(
        reader.contains("previous = None"),
        "读的人跨过了没有清单的调用：比较链在那一条上应当断开，而不是接起来"
    );
    let _ = manifest_json(&body(json!([])));
    let _ = parse_manifest;
}
