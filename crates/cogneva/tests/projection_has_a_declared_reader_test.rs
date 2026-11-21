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

    // 投影这件事在同一 crate 里说了两遍：`PROJECTION_READERS` 说「谁读它、为什么不用
    // 真源、由什么重算」，`table_shapes::MUTABLE_TABLES` 说「它是投影还是权威文档」。
    // 两份名单说的是同一批表，而其中一份改了名、另一份没跟上时，两边都读得通——直到
    // 有人按错的那一份去找重建路线。判据：投影那一侧的两份必须逐字一致。
    let mut disputed = Vec::new();
    for table in cog_storage::table_shapes::MUTABLE_TABLES {
        let cog_storage::table_shapes::TableShape::Projection {
            reader,
            rebuild,
            rebuilt_from,
            rebuild_test,
            ..
        } = table.shape
        else {
            continue;
        };
        match cog_storage::projection_readers::PROJECTION_READERS
            .iter()
            .find(|declaration| declaration.table == table.table)
        {
            None => disputed.push(format!(
                "{}: 形状声明说它是投影，读者声明里没有这一条",
                table.table
            )),
            Some(declaration) if declaration.reader != reader => disputed.push(format!(
                "{}: 两份声明点名的读者不同（{} / {}）",
                table.table, declaration.reader, reader
            )),
            Some(declaration) if declaration.rebuild != rebuild => disputed.push(format!(
                "{}: 两份声明点名的重建路线不同（{} / {}）",
                table.table, declaration.rebuild, rebuild
            )),
            Some(declaration) if declaration.rebuild_test != rebuild_test => {
                disputed.push(format!(
                    "{}: 两份声明点名的重算测试不同（{} / {}）",
                    table.table, declaration.rebuild_test, rebuild_test
                ))
            }
            Some(declaration) if declaration.rebuilt_from != rebuilt_from => {
                disputed.push(format!(
                    "{}: 两份声明点名的「由什么重算」不同（{} / {}）",
                    table.table, declaration.rebuilt_from, rebuilt_from
                ))
            }
            Some(_) => {}
        }
    }
    assert!(
        disputed.is_empty(),
        "同一批投影表在同一 crate 里有两份声明，而它们对不上：\n{}",
        disputed.join("\n")
    );

    // 「由哪些事实重算」与「按什么重算一遍」两句都要写得出，而且后者要点得到树里的东西：
    // 一张说不出自己跟谁一致的投影表，与一份没人核对过的副本在库里同形。
    let mut unexplained = Vec::new();
    for declaration in cog_storage::projection_readers::PROJECTION_READERS {
        if declaration.rebuilt_from.trim().is_empty() {
            unexplained.push(format!("{}: 没写「由哪些事实重算」", declaration.table));
            continue;
        }
        if declaration.rebuild.trim().is_empty() {
            unexplained.push(format!("{}: 没写重写它的是哪处代码", declaration.table));
            continue;
        }
        if !root.join(declaration.rebuild).exists() {
            unexplained.push(format!(
                "{}: 点名的重写路线 {} 在树里不存在",
                declaration.table, declaration.rebuild
            ));
            continue;
        }
        // 「能重算」的**证据**是一条真的重算测试：它在树里，而且它跑的是一个真库用例
        // （`#[ignore]` 后面那一条，需要 COGNEVA_TEST_DATABASE_URL）。只写路径不写
        // 测试，与不写这一格是一样的——声明没有证据。
        let test_path = root.join(declaration.rebuild_test);
        if !test_path.is_file() {
            unexplained.push(format!(
                "{}: 点名的重算测试 {} 在树里不存在",
                declaration.table, declaration.rebuild_test
            ));
            continue;
        }
        let text = std::fs::read_to_string(&test_path).unwrap_or_default();
        if !text.contains("#[ignore") || !text.contains("async fn") {
            unexplained.push(format!(
                "{}: {} 里没有一条需要真库的重算用例（`#[ignore]` + `async fn`）",
                declaration.table, declaration.rebuild_test
            ));
        }
    }
    assert!(
        unexplained.is_empty(),
        "这些投影表的声明答不全「它由什么重算、按什么重算一遍」：\n{}",
        unexplained.join("\n")
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
        for (table, columns) in common::migrations::tables_in(&text) {
            tables.entry(table).or_default().extend(columns);
        }
    }
    for path in common::migrations::up_migrations(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (table, columns) in common::migrations::tables_in(&text) {
            tables.entry(table).or_default().extend(columns);
        }
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
