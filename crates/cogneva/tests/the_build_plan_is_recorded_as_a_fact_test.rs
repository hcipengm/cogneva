//! 一次工作区测试计划怎么决定，要进账本，不能只活在钩子的 stderr 里。
//!
//! 门判三段：闭集里有这个词、载荷目录点名它写出去的四个键、`cell-plan` 在印出
//! 结果之前把它写进事件写口。缺任何一段，「复用与真跑的条数」就还是只读得到一次。
//!
//! 没有这次改动这条会红：`build_planned` 不在闭集里，`cell-plan` 也不写任何事实。

use crate::common::routes::repo_root;

#[test]
fn the_kind_and_its_payload_are_in_the_contract() {
    assert!(
        cog_core::KINDS.contains(&"build_planned"),
        "闭集里没有 build_planned"
    );
    let fields =
        cog_core::contract::event_payload::payload_fields_for("build_planned", "planned", 1)
            .expect("目录里没有 build_planned");
    for name in ["rerun", "reused", "changed", "source"] {
        assert!(
            fields.iter().any(|(field, _)| *field == name),
            "目录没有点名 {name}：写出去而读的人按名字取不到"
        );
    }
    assert_eq!(
        cog_core::contract::event_fact::class_of_kind("build_planned"),
        Some(cog_core::contract::event_fact::EventClass::Domain),
        "构建计划是领域事实，不是遥测"
    );
}

#[test]
fn the_plan_is_written_before_it_is_printed() {
    let source = std::fs::read_to_string(repo_root().join("crates/cogneva/src/cell_plan.rs"))
        .expect("cell_plan.rs");
    let record = source
        .find("record_plan_blocking")
        .expect("cell-plan 没有写这条事实");
    let print = source
        .find("println!(\"rerun")
        .expect("cell-plan 没有印结果");
    assert!(record < print, "先印后写：下一次读不到这一次的账");
    assert!(
        source.contains("EventPayload::BuildPlanned"),
        "写进去的不是这条事实"
    );
}

#[test]
fn the_counts_are_readable_without_the_stderr_of_that_run() {
    let source =
        std::fs::read_to_string(repo_root().join("crates/cogneva/src/build_plan_readings.rs"))
            .expect("build_plan_readings.rs");
    assert!(
        source.contains("kind = 'build_planned'"),
        "读者没有按这条事实取数：条数又只活在 stderr 里"
    );
    // 人口普查里要有读者，并且指向这个文件。
    let row = cog_core::contract::event_kinds::EVENT_CLASS_CENSUS
        .iter()
        .find(|row| row.kind == "build_planned")
        .expect("census 没有 build_planned");
    assert_eq!(
        row.reader,
        Some("crates/cogneva/src/build_plan_readings.rs")
    );
}
