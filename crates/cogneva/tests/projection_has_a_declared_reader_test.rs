//! principle: feedback_no_hardcoding_config_surface
//!
//! 投影表必须自证有读者。
//!
//! 一张投影表是「从别处重算出来的表」：真源在别的地方，它存在是因为那个问题用真源
//! 回答太贵。这句话必须被写下来，否则一张投影表与一份重复的副本在库里长得一模一样
//! ——而副本的失败方式是没有失败：没人读它，它也不会报错，只会一直跟着真源重算，
//! 还让读的人以为那份事实在这里。
//!
//! 「投影表」由形状认，不由名字认：水位列（`as_of` 开头的那一列）是「我读到哪了」
//! 的自述，只有重建得出的表才需要说这句话。判据两头都判——树上有水位的表必须在声明
//! 里，声明里点名的表也必须在树上：少一条是「新投影没有声明」，多一条是「声明没跟
//! 上表」。声明住在本仓的契约里（`cog_storage::projection_readers::PROJECTION_READERS`），因为它是一份
//! 数据，不是这段测试的私有名单。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::common;

use common::routes::repo_root;
use common::rust_production::production_sources;

/// 水位列的名字形状：`as_of` 或 `as_of_*`。
const WATERMARK_PREFIX: &str = "as_of";

#[test]
fn every_projection_table_declares_its_reader_and_why_the_source_would_not_do() {
    let root = repo_root();
    let found = projection_tables(&root);
    let declared: BTreeSet<String> = cog_storage::projection_readers::PROJECTION_READERS
        .iter()
        .map(|declaration| declaration.table.to_string())
        .collect();

    let undeclared: Vec<&String> = found.difference(&declared).collect();
    assert!(
        undeclared.is_empty(),
        "这几张表带水位列，也就是自称是从别处重算出来的投影，而契约里没有它们的读者\
         声明。加一行 `cog_storage::projection_readers::PROJECTION_READERS`：谁在读它，以及那个问题为什么\
         不能用真源回答。写不出这两句话的表是纯重复，删掉它：\n{}",
        undeclared
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let stale: Vec<&String> = declared.difference(&found).collect();
    assert!(
        stale.is_empty(),
        "契约声明了这几张投影表，而树上的 DDL 里已经没有带水位的表叫这个名字：\
         声明没跟上表。表改名或删掉的那一笔里顺手改掉声明：\n{}",
        stale
            .iter()
            .map(|table| format!("  {table}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_declaration_names_a_reader_in_the_tree_and_says_why() {
    let root = repo_root();
    let mut problems = Vec::new();
    for declaration in cog_storage::projection_readers::PROJECTION_READERS {
        let reader = Path::new(declaration.reader);
        if !root.join(reader).is_file() {
            problems.push(format!(
                "{}: 读者 `{}` 在树里不存在——点一个不存在的路径等于没有读者",
                declaration.table, declaration.reader
            ));
        }
        let why = declaration.why_not_the_source.trim();
        if why.chars().count() < 40 {
            problems.push(format!(
                "{}: 「为什么不能用真源回答」只有 {} 个字，那不是一个理由",
                declaration.table,
                why.chars().count()
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "投影表的声明要自证：\n{}",
        problems.join("\n")
    );
}

/// 树上有水位列的那些表。
///
/// 口径是声明点：迁移的 `.up.sql`（重放时建表）与 store 自己的 `CREATE TABLE IF
/// NOT EXISTS` 常量（首启时建表）都算，与「库只存列」那条普查同一套口径。
fn projection_tables(root: &Path) -> BTreeSet<String> {
    let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        collect_tables(&text, &mut tables);
    }
    for path in common::migrations::up_migrations(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        collect_tables(&text, &mut tables);
    }
    tables
        .into_iter()
        .filter(|(_, columns)| {
            columns.iter().any(|column| {
                column == WATERMARK_PREFIX || column.starts_with(&format!("{WATERMARK_PREFIX}_"))
            })
        })
        .map(|(table, _)| table)
        .collect()
}

/// 一份 DDL/源码文本里的 `CREATE TABLE` 与它们的列名。
fn collect_tables(text: &str, out: &mut BTreeMap<String, BTreeSet<String>>) {
    let mut in_table: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        let lower = trimmed.to_ascii_lowercase();
        if in_table.is_none() {
            for opening in ["create table if not exists ", "create table "] {
                if let Some(rest) = lower.strip_prefix(opening) {
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect();
                    if !name.is_empty() {
                        out.entry(name.clone()).or_default();
                        in_table = Some(name);
                    }
                    break;
                }
            }
            continue;
        }
        if trimmed.starts_with(')') {
            in_table = None;
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        // 约束行不是列：`PRIMARY KEY (...)`、`UNIQUE (...)`、`FOREIGN KEY` 等。
        let first = trimmed
            .split(|c: char| c.is_whitespace() || c == '(')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        if matches!(
            first.as_str(),
            "primary" | "unique" | "foreign" | "check" | "constraint" | "exclude" | "like"
        ) {
            continue;
        }
        if let Some(table) = &in_table {
            out.entry(table.clone()).or_default().insert(first);
        }
    }
}
