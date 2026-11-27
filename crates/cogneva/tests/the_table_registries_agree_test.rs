//! 三本表登记（`FACT_TABLES`、`MUTABLE_TABLES`、`TABLE_CENSUS`）说的是同一批表的同一件事。
//!
//! 它们各自只回答一问：`TABLE_CENSUS` 说这张表是哪一类（流／投影／配置／旧主存），
//! `MUTABLE_TABLES` 说被 UPDATE 的表是投影还是文档，`FACT_TABLES` 说哪些表受
//! 「定时删除前先归档」那条门管。三本各写一遍表名，一份改了另一份没跟上时，每道门
//! 单看都绿——2026-11-27 实测就是这么发现的：`chat_turns` 在普查里是投影、在事实表里
//! 也算一份「事实」；`notifications`／`summary_entries`／`schema_entries` 同时是事实表
//! 与会被 UPDATE 的文档。
//!
//! 判据（每条都从常量现算，不抄名单）：
//!   * 事实表 ⇔ 普查里的 `Stream`；
//!   * 事实表与被 UPDATE 的表不相交；
//!   * 被声明成投影的表，普查里也是 `Projection`；
//!   * 被声明成文档的表，普查里是 `Legacy`（还没有迁到流上的当前值）；
//!   * 普查里的 `Config` 不出现在事实表或被更新表里——配置不是事实。

use cog_storage::table_shapes::{
    CensusClass, TableShape, FACT_TABLES, MUTABLE_TABLES, TABLE_CENSUS,
};

use std::collections::BTreeSet;

fn class_of(table: &str) -> Option<&'static CensusClass> {
    TABLE_CENSUS
        .iter()
        .find(|row| row.table == table)
        .map(|row| &row.class)
}

#[test]
fn the_three_registries_tell_one_story_about_every_table() {
    let facts: BTreeSet<&str> = FACT_TABLES.iter().copied().collect();
    let mutable: BTreeSet<&str> = MUTABLE_TABLES
        .iter()
        .map(|declaration| declaration.table)
        .collect();
    let streams: BTreeSet<&str> = TABLE_CENSUS
        .iter()
        .filter(|row| matches!(row.class, CensusClass::Stream))
        .map(|row| row.table)
        .collect();

    let mut problems = Vec::new();

    for table in facts.difference(&streams) {
        problems.push(format!("{table}: 在 FACT_TABLES 里，普查里却不是 Stream"));
    }
    for table in streams.difference(&facts) {
        problems.push(format!("{table}: 普查里是 Stream，却不在 FACT_TABLES 里"));
    }
    for table in facts.intersection(&mutable) {
        problems.push(format!(
            "{table}: 同时是「只追加的事实」与「会被 UPDATE 的表」——两者互斥"
        ));
    }
    for declaration in MUTABLE_TABLES {
        match declaration.shape {
            TableShape::Projection { .. } => {
                if !matches!(
                    class_of(declaration.table),
                    Some(CensusClass::Projection { .. })
                ) {
                    problems.push(format!(
                        "{}: 形状声明是投影，普查里是 {:?}",
                        declaration.table,
                        class_of(declaration.table).map(|class| format!("{class:?}"))
                    ));
                }
            }
            TableShape::Document { .. } => {
                if !matches!(
                    class_of(declaration.table),
                    Some(CensusClass::Legacy { .. })
                ) {
                    problems.push(format!(
                        "{}: 形状声明是文档（当前值），普查里必须是 Legacy——还不是由流重算出来的",
                        declaration.table
                    ));
                }
            }
        }
    }
    for row in TABLE_CENSUS {
        if matches!(row.class, CensusClass::Config { .. })
            && (facts.contains(row.table) || mutable.contains(row.table))
        {
            problems.push(format!(
                "{}: 普查里是 Config（配置不是事实），却出现在事实表或被更新表里",
                row.table
            ));
        }
    }

    assert!(
        problems.is_empty(),
        "三本表登记说的不是同一件事：\n  {}",
        problems.join("\n  ")
    );
}
