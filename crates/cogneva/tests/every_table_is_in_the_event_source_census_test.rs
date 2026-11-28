//! principle: feedback_the_four_kinds_cover_the_domain_is_a_census_not_a_theorem
//!
//! 「本表的每一张表都是事件流本身、能从事件重建的投影，或纯传输配置」不是一句
//! 论证，是一条**普查**：库里每一张表都要在 `cog_storage::table_shapes::TABLE_CENSUS`
//! 里有一行，说明它属于哪一类；归不进去的，明确标成 `Legacy`（旧主存，尚未迁移），
//! 而这一格的数字只许减不许增。
//!
//! 判据两头都判：DDL 里建的每一张表都要在名单里（新表无处可藏），名单里点名的
//! 每一张表也都要在 DDL 里（名单不跟着表漂）。投影那一格还要指得出重建路线，而且
//! 那条路线在树里找得到——说不出按什么重算的表，就是一份副本。

use std::collections::BTreeSet;

use crate::common;
use common::migrations::{tables_in, up_migrations};
use common::routes::repo_root;
use common::rust_production::production_texts;

use cog_storage::table_shapes::{CensusClass, TABLE_CENSUS};

/// 旧主存的张数上限（只能减，不能增）。
///
/// 这不是「允许的第四种形状」：它是一张债务表。新加一张旧主存会让它超上限，
/// 于是新表必须先回答「它属于三类里的哪一类」。
const LEGACY_BUDGET: usize = 78;

/// 迁移与 store 的 DDL 里建的每一张表。
fn ddl_tables() -> BTreeSet<String> {
    let root = repo_root();
    let mut found = BTreeSet::new();
    for path in up_migrations(&root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        found.extend(tables_in(&text).into_keys());
    }
    for (_, text) in production_texts(&root) {
        // 只读产出那一半：`#[cfg(test)]` 之后的夹具建表不是产品建的表。
        found.extend(tables_in(&common::rust_production::production_text(&text)).into_keys());
    }
    found
}

#[test]
fn every_table_in_the_ddl_is_classified_in_the_census() {
    let found = ddl_tables();
    assert!(
        found.len() >= 80,
        "只读到 {} 张表：这份普查没有一个题目，任何「每一张都归类了」的断言在\
         空集上都成立",
        found.len()
    );
    let listed: BTreeSet<String> = TABLE_CENSUS
        .iter()
        .map(|row| row.table.to_string())
        .collect();
    assert_eq!(listed.len(), TABLE_CENSUS.len(), "普查名单里有重复的表名");

    let unclassified: Vec<&String> = found.difference(&listed).collect();
    assert!(
        unclassified.is_empty(),
        "这几张表建在库里，而普查里没有它们的归属。加一行 \
         `cog_storage::table_shapes::TABLE_CENSUS`，说明它是事件流本身、可重建的投影\
         （点名重建路线）、还是纯传输配置；三者都不是就先标 `Legacy` 并在台账记一笔：\n{}",
        unclassified
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let stale: Vec<&String> = listed.difference(&found).collect();
    assert!(
        stale.is_empty(),
        "普查里点名的这几张表在 DDL 里已经不存在了，名单没跟上表：\n{}",
        stale
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn a_projection_names_a_rebuild_route_that_exists_and_a_config_row_says_why() {
    let root = repo_root();
    let production: String = production_texts(&root)
        .into_iter()
        .map(|(_, text)| text)
        .collect::<Vec<_>>()
        .join("\n");
    let mut unexplained = Vec::new();
    for row in TABLE_CENSUS {
        match &row.class {
            CensusClass::Projection {
                rebuild,
                rebuilt_from,
            } => {
                if rebuild.trim().is_empty() || rebuilt_from.trim().is_empty() {
                    unexplained.push(format!("{}: 投影没写重建路线或「由什么重算」", row.table));
                } else if !production.contains(rebuild) {
                    unexplained.push(format!(
                        "{}: 点名的重建路线 `{rebuild}` 在生产代码里找不到",
                        row.table
                    ));
                }
            }
            CensusClass::Config { why } => {
                if why.trim().is_empty() {
                    unexplained.push(format!(
                        "{}: 标成配置/读数，却没说为什么它不存事实",
                        row.table
                    ));
                }
            }
            CensusClass::Stream | CensusClass::Legacy { .. } => {}
        }
    }
    assert!(
        unexplained.is_empty(),
        "普查里的这几行没有说清自己：\n{}",
        unexplained.join("\n")
    );
}

#[test]
fn every_legacy_row_names_its_successor_or_says_what_is_missing() {
    use cog_storage::table_shapes::{CensusClass, TABLE_CENSUS};
    let mut unexplained = Vec::new();
    for row in TABLE_CENSUS {
        let CensusClass::Legacy { successor, why } = row.class else {
            continue;
        };
        if why.trim().is_empty() {
            unexplained.push(format!("{}: 没说为什么留着", row.table));
            continue;
        }
        if let Some(kind) = successor {
            if !cog_core::KINDS.contains(&kind) {
                unexplained.push(format!("{}: 点名的后继 `{kind}` 不在事件闭集里", row.table));
            }
        }
    }
    assert!(
        unexplained.is_empty(),
        "旧主存的行要么点名它已经在流上的那一类事实，要么说清为什么还留着：\n{}",
        unexplained.join("\n")
    );
}

#[test]
fn the_legacy_backlog_only_shrinks() {
    let legacy = TABLE_CENSUS
        .iter()
        .filter(|row| matches!(row.class, CensusClass::Legacy { .. }))
        .count();
    assert!(
        legacy <= LEGACY_BUDGET,
        "旧主存从 {LEGACY_BUDGET} 涨到了 {legacy}：新加的表必须先回答它属于事件流、\
         可重建投影、还是纯传输配置；三者都不是就不该建这张表"
    );
}
