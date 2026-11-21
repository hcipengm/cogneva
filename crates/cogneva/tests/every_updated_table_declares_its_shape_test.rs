//! principle: feedback_no_hardcoding_config_surface
//!
//! 每一张会被 `UPDATE` 的表，都要说清自己是哪一种形状。
//!
//! 存储终态的第一条不变式问的是「这张表怎么改」：追加表每一行写下就不动，当前值由
//! 读者折；投影表从别处重算，所以要水位、重建命令与重建测试；而**权威文档**的行就是
//! 那段事实的当前值，改它就是写新描述。三种形状的差别不是风格，是「这一行的真源在
//! 哪里」——答不出这句话的表，坏起来只有一种样子：某天两个写者各改一半，谁都看不出。
//!
//! 判据从两边量，不读手写的名单：
//!
//! * 表集合从 DDL 里算（迁移的 `.up.sql` 与 store 的建表常量）；
//! * 会被 `UPDATE` 的表从**生产代码**的 SQL 字面量里算——`#[cfg(test)]` 之后的不算，
//!   那里的 `UPDATE` 是夹具；
//! * 两边比：被更新的表必须在 `cog_storage::table_shapes::MUTABLE_TABLES` 里声明形状，
//!   声明里的表也必须在树上（表改名或没人再更新它，那一行就该删）。
//!
//! 投影的声明还要自证：读者与重建命令点的路径在树上，水位列在 DDL 里，并且它在
//! `projection_readers` 里也有一行——两张声明表说的是同一件事，不能一处有一处没有。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::common;

use cog_storage::table_shapes::{TableShape, MUTABLE_TABLES};

use common::migrations::{tables_in, up_migrations};
use common::routes::repo_root;
use common::rust_production::{production_sources, production_text};

#[test]
fn every_updated_table_declares_its_shape() {
    let root = repo_root();
    let tables = ddl_tables(&root);
    let updated = updated_tables(&root);
    let declared: BTreeSet<String> = MUTABLE_TABLES
        .iter()
        .map(|declaration| declaration.table.to_string())
        .collect();

    let undeclared: Vec<&String> = updated.difference(&declared).collect();
    assert!(
        undeclared.is_empty(),
        "这几张表在生产代码里会被 `UPDATE`，而 `cog_storage::table_shapes` 里没有它们的\
         形状声明。加一行，并在注释里回答「这一行的真源在哪里」：\n{}",
        undeclared
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let stale: Vec<&String> = declared.difference(&updated).collect();
    assert!(
        stale.is_empty(),
        "这几张表声明成「会被更新」，而生产代码里已经没有一条 `UPDATE` 点它们——\
         要么它已经变成追加表（删掉这一行），要么那条 UPDATE 在别处：\n{}",
        stale
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let known: BTreeSet<String> = tables.keys().cloned().collect();
    let unknown: Vec<&String> = declared.difference(&known).collect();
    assert!(
        unknown.is_empty(),
        "声明里的这几张表在 DDL 里找不到：表改名或删掉的那一笔里顺手改掉声明：\n{}",
        unknown
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_declaration_answers_where_the_truth_is() {
    let root = repo_root();
    let tables = ddl_tables(&root);
    let readers: BTreeSet<String> = cog_storage::projection_readers::PROJECTION_READERS
        .iter()
        .map(|declaration| declaration.table.to_string())
        .collect();
    let mut problems = Vec::new();
    for declaration in MUTABLE_TABLES {
        match declaration.shape {
            TableShape::Projection {
                reader,
                watermark,
                rebuild,
                rebuilt_from,
                rebuild_test,
            } => {
                // 「由什么重算」写不出空话：一张说不出自己跟谁一致的投影表，与一份
                // 没人核对过的副本在库里同形。
                if rebuilt_from.trim().is_empty() {
                    problems.push(format!("{}: 没写它由哪些事实重算", declaration.table));
                }
                for (label, path) in [
                    ("读者", reader),
                    ("重建命令", rebuild),
                    ("重算测试", rebuild_test),
                ] {
                    if !root.join(path).is_file() {
                        problems.push(format!(
                            "{}: {label} `{path}` 在树里不存在——点一个不存在的路径等于没有它",
                            declaration.table
                        ));
                    }
                }
                let has_watermark = tables
                    .get(declaration.table)
                    .is_some_and(|columns| columns.contains(watermark));
                if !has_watermark {
                    problems.push(format!(
                        "{}: 声明的水位列 `{watermark}` 不在它的 DDL 里",
                        declaration.table
                    ));
                }
                if !readers.contains(declaration.table) {
                    problems.push(format!(
                        "{}: 投影在 `table_shapes` 里有形状，却在 `projection_readers` 里没有\
                         读者声明——两张表说的是同一件事",
                        declaration.table
                    ));
                }
            }
            TableShape::Document { why } => {
                if why.trim().chars().count() < 40 {
                    problems.push(format!(
                        "{}: 「为什么它是真源而不是派生」只有 {} 个字，那不是一个理由",
                        declaration.table,
                        why.trim().chars().count()
                    ));
                }
            }
        }
    }
    assert!(
        problems.is_empty(),
        "形状声明要自证：\n{}",
        problems.join("\n")
    );
}

/// DDL 里声明过的每一张表，以及它们的列。
fn ddl_tables(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut texts: Vec<String> = Vec::new();
    for path in production_sources(root) {
        if let Ok(text) = std::fs::read_to_string(&path) {
            texts.push(text);
        }
    }
    for path in up_migrations(root) {
        if let Ok(text) = std::fs::read_to_string(&path) {
            texts.push(text);
        }
    }
    for text in texts {
        for (table, columns) in tables_in(&text) {
            tables.entry(table).or_default().extend(columns);
        }
    }
    tables
}

/// 生产代码里写着 `UPDATE <表> SET` 的那些表。
///
/// `#[cfg(test)]` 之后的部分切掉：那里的 SQL 是夹具，夹具更新一张表不说明生产会。
fn updated_tables(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        // 产出面的边界只有一份（`common::rust_production`）：自己按
        // `#[cfg(test)]` 切会把文件中间 test-only 项之后的产出整段丢掉，而
        // 「那里没有」与「那里没读」在判词上是同一句话。
        let lower = production_text(&text).to_ascii_lowercase();
        let mut rest = lower.as_str();
        while let Some(at) = rest.find("update ") {
            rest = &rest[at + "update ".len()..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            let after = rest.get(name.len()..).unwrap_or_default().trim_start();
            if after.starts_with("set ") {
                out.insert(name);
            }
        }
        // 一次 upsert 也是原地改：`INSERT … ON CONFLICT … DO UPDATE SET` 改的是已经
        // 在库里的那一行。只认 `UPDATE <t> SET` 会把这类表读成「从不更新」——而它们
        // 恰恰是最需要说清形状的那一批（累加表就是这么写的）。
        let mut rest = lower.as_str();
        while let Some(at) = rest.find("insert into ") {
            rest = &rest[at + "insert into ".len()..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            let rest_of_statement = rest.split(';').next().unwrap_or_default();
            if rest_of_statement.contains("do update set") {
                out.insert(name);
            }
        }
    }
    out
}
