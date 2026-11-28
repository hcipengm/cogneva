//! 流上的每一类事件都要说自己回答的是哪一种问题：领域事实，还是遥测。
//!
//! 两者进同一条流，读的人问的却不是同一个问题——折叠当前值时遥测不参与，按花费
//! 读时领域事实不参与。分类若是「每一处读面各自按名字认」，新加一个变体就会有一处
//! 认不出；所以分类写在 `EventPayload::class()` 里，并且是**穷尽匹配**：新加变体
//! 不给它分类，编译就红，而不是在这里再补一条名单。
//!
//! 没有这次改动这条会红：`EventClass` 与 `class()` 都不存在。

use cog_core::contract::event_fact::{class_of_kind, EventClass, EventPayload};

#[test]
fn a_domain_fact_and_a_telemetry_fact_are_told_apart() {
    let retired = EventPayload::EmployeeRetired {
        agent_id: "employee-1".into(),
    };
    assert_eq!(retired.class(), EventClass::Domain);
}

#[test]
fn the_classifier_is_exhaustive_and_names_the_two_call_kinds_as_telemetry() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join("crates/cog-core/src/contract/event_fact.rs"),
    )
    .expect("event_fact.rs 读不到");
    let start = source
        .find("pub fn class(&self) -> EventClass")
        .expect("class() 不在 event_fact.rs 里");
    let body = &source[start..];
    let end = body.find("\n    }\n").expect("class() 没有结尾");
    let body = &body[..end];
    assert!(
        body.contains("Self::LlmCall(..) | Self::ToolCall(..) => EventClass::Telemetry"),
        "两条调用没有被归成遥测"
    );
    assert!(
        !body.contains("_ =>"),
        "分类用了通配臂：新加一个变体会被静默归到某一类，而不是编译红"
    );
    // 遥测那一臂只点这两条；领域那一臂不许再出现它们——同一条事件不能同时落在两类。
    let telemetry_at = body.find("EventClass::Telemetry").expect("遥测臂");
    let telemetry_arm = body[..telemetry_at]
        .lines()
        .next_back()
        .unwrap_or(Default::default());
    assert!(
        telemetry_arm.contains("LlmCall") && telemetry_arm.contains("ToolCall"),
        "遥测臂没有恰当地点名两条调用：{telemetry_arm}"
    );
    let domain_side = &body[telemetry_at..];
    assert!(
        !domain_side.contains("Self::LlmCall") && !domain_side.contains("Self::ToolCall"),
        "调用事件同时落在了领域臂里"
    );
}

#[test]
fn every_kind_in_the_closed_set_has_a_class_and_only_the_calls_are_telemetry() {
    use cog_core::contract::event_envelope::KINDS;
    for kind in KINDS {
        assert!(
            class_of_kind(kind).is_some(),
            "闭集里的 {kind} 没有类别：它属于领域还是遥测，读的人只能猜"
        );
    }
    let telemetry: Vec<&str> = KINDS
        .iter()
        .copied()
        .filter(|kind| class_of_kind(kind) == Some(EventClass::Telemetry))
        .collect();
    assert_eq!(
        telemetry,
        vec!["llm_call", "tool_call"],
        "遥测那一侧只能有两条调用"
    );
    assert_eq!(class_of_kind("a_kind_this_build_never_wrote"), None);
}

#[test]
fn the_stream_census_carries_the_class_beside_the_kind() {
    let source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join("crates/cog-storage/src/event_stream_census.rs"),
    )
    .expect("event_stream_census.rs");
    assert!(
        source.contains("LABEL_CLASS") && source.contains("class_of_kind(kind)"),
        "事件流普查没有带上类别：分类没有消费者，读了也白读"
    );
}
