//! 落地账不再是只有磁盘的那一份：每次落盘同时落一条 `change_landed` 事实，文件退成缓存。
//! 从流上折得出「这条变更落到哪、CI 怎么看、为什么退」——文件丢了、机器换了也答得出。
//!
//! 没有这次改动这几条会红：事实类型不存在、落盘不写事实、没有从流上读的入口。

use cog_core::contract::event_payload::payload_fields_for;

#[test]
fn the_landing_fact_is_catalogued_and_its_class_names_both_ends() {
    let fields = payload_fields_for("change_landed", "landed", 1)
        .expect("change_landed 没有登记在载荷清单里");
    let names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
    for needed in [
        "change_id",
        "base",
        "landed_rev",
        "state",
        "retired_reason",
        "record",
        "closed",
    ] {
        assert!(
            names.contains(&needed),
            "载荷清单里没有 {needed}：{names:?}"
        );
    }
    let class = cog_core::EVENT_CLASS_CENSUS
        .iter()
        .find(|row| row.kind == "change_landed")
        .expect("普查里没有这一类");
    assert!(class.writer.is_some() && class.reader.is_some());
}

#[test]
fn saving_a_record_lands_the_fact_before_the_file() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let landing =
        std::fs::read_to_string(root.join("crates/cog-github/src/landing.rs")).expect("landing.rs");
    let fact_at = landing
        .find("EventPayload::ChangeLanded")
        .expect("落盘没有写事实：文件仍然是唯一的一份");
    let file_at = landing
        .find("tokio::fs::write(record_path")
        .expect("写文件的路径不见了");
    assert!(
        fact_at < file_at,
        "事实写在文件之后：中途断掉时留下的那一份状态只剩磁盘"
    );
    assert!(landing.contains("pub fn attach_events"));
    let plugin =
        std::fs::read_to_string(root.join("crates/cog-github/src/plugin.rs")).expect("plugin.rs");
    assert!(
        plugin.contains("attach_events"),
        "装配处没有把事件写口接上：事实一条也不会落"
    );
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cli.rs")).expect("cli.rs");
    assert!(cli.contains("landing-states"), "流上读落地状态的入口不存在");
    let census = std::fs::read_to_string(root.join("crates/cog-storage/src/table_shapes.rs"))
        .expect("table_shapes.rs");
    assert!(
        census.contains("function: \"landing_states\""),
        "读口不在 STREAM_READERS 里：普查看不见这个读面"
    );
}

/// 读面也是流：通道的热读走事实，盘上那份文件只在读不到流时兜底。
///
/// 没有这一条，写口写了一年的事实也可以没有任何人读——「有正本」与「读正本的是
/// 谁」在两棵树上同形，而差的正是这一句。
#[test]
fn the_channel_reads_its_records_from_the_facts() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let landing =
        std::fs::read_to_string(root.join("crates/cog-github/src/landing.rs")).expect("landing.rs");
    assert!(
        landing.contains("pub fn attach_facts") && landing.contains("facts_are_the_source"),
        "落地账没有接事实读口：它只写事实、不读事实"
    );
    assert!(
        landing.contains("latest_landing_attributes"),
        "load_records 没有从事实里取记录：盘上那份文件仍是通道的记忆"
    );
    let github_plugin =
        std::fs::read_to_string(root.join("crates/cog-github/src/plugin.rs")).expect("plugin.rs");
    assert!(
        github_plugin.contains("LandingFactReader") && github_plugin.contains("attach_facts"),
        "装配处没有把事实读口接给落地账"
    );
    let storage_plugin = std::fs::read_to_string(root.join("crates/cog-storage/src/plugin.rs"))
        .expect("storage plugin.rs");
    assert!(
        storage_plugin.contains("LandingFactReader"),
        "存储插件没有发布事实读口：装配处无从接上"
    );
}
