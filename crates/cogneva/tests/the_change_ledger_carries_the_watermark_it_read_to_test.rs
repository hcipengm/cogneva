//! 一张投影表要说得出「我读到哪儿了」。变更账的每一格都从流上折回来，水位列
//! `as_of_seq` 记的是这一行读到的最深一条事实；0 是「还没有人折过它」，与「折到了
//! 开头」在增量重折时判得不一样。
//!
//! 没有这次改动这几条会红：迁移里没有这一列、形状普查仍把它登记成文档、重建不写水位。

use cog_storage::table_shapes::{TableShape, MUTABLE_TABLES};

#[test]
fn the_ledger_declares_the_watermark_column_its_ddl_carries() {
    let declaration = MUTABLE_TABLES
        .iter()
        .find(|table| table.table == "change_ledger")
        .expect("change_ledger 不在形状普查里");
    match declaration.shape {
        TableShape::Projection {
            watermark,
            rebuild,
            rebuilt_from,
            rebuild_test,
            ..
        } => {
            assert_eq!(
                watermark,
                Some("as_of_seq"),
                "水位列名与 DDL 里的那一列对不上"
            );
            assert!(!rebuild.trim().is_empty() && !rebuilt_from.trim().is_empty());
            assert!(
                !rebuild_test.trim().is_empty(),
                "投影没有一条重算测试作证据"
            );
        }
        TableShape::Document { why } => {
            panic!("变更账仍按文档登记（{why}）：它的每一格都从流上折得回来")
        }
    }
    assert!(
        cog_storage::projection_readers::PROJECTION_READERS
            .iter()
            .any(|reader| reader.table == "change_ledger"),
        "投影登记里没有 change_ledger：读者与「为什么不用真源」都写不出来"
    );
}

/// 迁移与重建两头都接上了：列在迁移里，重建把这一行读到的最深一条事实写进去。
#[test]
fn the_rebuild_records_how_far_it_read() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let migration = std::fs::read_to_string(
        root.join("crates/cog-storage/migrations/postgres/136_change_ledger_as_of_seq.up.sql"),
    )
    .expect("watermark migration");
    assert!(
        migration.contains("as_of_seq") && migration.contains("change_ledger"),
        "迁移没有给 change_ledger 加水位列"
    );
    let store = std::fs::read_to_string(
        root.join("crates/cog-storage/src/postgres/change_ledger_store.rs"),
    )
    .expect("change_ledger_store.rs");
    assert!(
        store.contains("as_of_seq = GREATEST(change_ledger.as_of_seq, EXCLUDED.as_of_seq)"),
        "重建不写水位：水位停在 0，增量重折会把整本账重折一遍"
    );
    assert!(
        store.contains("entry.as_of_seq = entry.as_of_seq.max(seq)"),
        "折的时候没有记下读到哪一条：水位只能靠猜"
    );
    // 增量重折只折水位之后的事实，水位由表带进折叠；没有新事实时一行都不写
    // （空跑推进水位会让「读到哪儿了」追上还没发生的事）。
    assert!(
        store.contains("project_change_ledger_incremental"),
        "水位没有任何人用：它只是表上的一列"
    );
    assert!(
        store.contains("fold_change_account_from(pool, &since, &table)"),
        "增量重折没有把水位带进折叠：一次增量会把整本账重折一遍"
    );
    assert!(
        store.contains("if changed.is_empty()"),
        "空跑没有提前返回：水位会被推到一个还没有事实的位置"
    );
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/change_ledger_parity.rs"))
        .expect("change_ledger_parity.rs");
    assert!(
        cli.contains("--project"),
        "增量重折没有入口：只有库里的一个函数，没有人跑得到"
    );
}
