//! 一行变更账的**全部**格子都要能从流上折回来：判定与交付已经在了，但「它当时说的是
//! 什么」（title／assertion）与「谁说的、要求的档与面」只在表里——删掉表，那一行就只剩
//! 一个状态，而读的人会把它读成「没人说过它该怎么做」。
//!
//! 所以每一次 reading 都落一条 `change_declared`（身份三格＋title／assertion＋一条引用
//! ＋first_seen_rev），重建时把 title／assertion 补回行、把引用按 (identity, doc) 写回
//! `change_ledger_sources` 并重算两列排序值。
//!
//! 没有这次改动这几条会红：闭集里没有 `change_declared`、载荷清单没有它、写路径不落
//! 这条事实、重建也不读它。

use cog_core::contract::event_payload::payload_fields_for;

#[test]
fn the_declaration_is_a_catalogued_fact_in_the_change_family() {
    assert!(
        cog_core::KINDS.contains(&"change_declared"),
        "闭集里没有 change_declared：写下去的行不在闭集里"
    );
    assert_eq!(
        cog_core::aggregate_of("change_declared"),
        "change",
        "声明不属于 change 这一族：按族读的人读不到它"
    );
    let fields = payload_fields_for("change_declared", "declared", 1)
        .expect("change_declared 没有登记在载荷清单里");
    let names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
    for needed in [
        "kind",
        "scope",
        "literal",
        "title",
        "assertion",
        "doc",
        "verdict",
        "priority",
        "block",
        "first_seen_rev",
    ] {
        assert!(
            names.contains(&needed),
            "载荷清单里没有 {needed}：{names:?}"
        );
    }
}

/// 写与折两头都接上了：两条写路径各落一条事实，重建读它并把它补回行与引用。
#[test]
fn the_writer_lands_the_fact_and_the_rebuild_folds_it_back() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let store = std::fs::read_to_string(
        root.join("crates/cog-storage/src/postgres/change_ledger_store.rs"),
    )
    .expect("change_ledger_store.rs");
    assert!(
        store.contains("append_declared"),
        "reading 的写路径没有落声明事实：删掉表，title／assertion 就没了"
    );
    assert_eq!(
        store.matches("self.append_declared(").count(),
        2,
        "两条写路径（直接记 reading 与折声明）都要落这一条事实"
    );
    assert!(
        store.contains("kind = 'change_declared'"),
        "重建没有读声明事实：它折回来的行缺 title／assertion"
    );
    assert!(
        store.contains("INSERT INTO change_ledger_sources"),
        "重建没有把引用写回去：重建出来的行读作「没人说过它该怎么做」"
    );
    assert!(
        store.contains("RECOUNT_SQL") && store.matches("RECOUNT_SQL").count() >= 3,
        "重建写回引用之后没有重算两列排序值：先档后块取批次的顺序读的是旧值"
    );
}
