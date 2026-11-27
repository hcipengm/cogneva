//! principle: feedback_no_hardcoding_config_surface
//!
//! 库只存列：`json` / `jsonb` 列的棘轮。
//!
//! 存储终态的第二条不变式是「全库没有 `json`/`jsonb` 列」：开集载荷存 `text`，
//! 语法上不可查询；要按某个键查，就给它加列。理由不是风格——jsonb 列上的读法
//! 是 `task->>'goal_id'` 这类字符串，键名打错返回 NULL 而不报错，判据用的字段
//! 建不了索引，改不同字段的两个写者照样互相覆盖；派生量被冻进文档之后，再没有
//! 任何东西重算它。
//!
//! 那份方案是一段讨论。讨论不会自己变红，也不会自己变短：一张表今天还在，明天
//! 又长出一列，读的人无从知道总数是在降还是在涨。这条门把它变成一个数。
//!
//! 2026-11-09 那份普查读到 66 行，一路清到 2026-11-16 的零行：整块读的载荷转
//! `text`，按键读的升成真列（`change_ledger.sources` 拆成一张引用表，指标标签集
//! 转 `text` 并把名册读的那个键升成列），没有任何代码念到的表连同它们的列一起
//! 删掉。**清到零之后基线删了**——基数下限也跟着删了，因为它挡的是「走错树」而
//! 不是「清得太多」，而那个作用由下面第二条判词接手。
//!
//! 两个判词：
//!
//! * 树上出现一列 `json`/`jsonb` → 红。它不是一个可以顺手加的东西，是一份存储
//!   决定：要按它查就给它加列，只是搬运就存 `text`，没有人读就删掉；
//! * 普查真的读了东西：读空与「全清完了」在判词上同形，而读空只可能来自走错了
//!   树。判据因此不数「还剩几列」（清到零时那个数就是零），而是数**输入**：几百
//!   份生产源文件、上百份迁移，一份都没读到才是走错了树。
//!
//! 口径是声明点，不是运行中的库：迁移的 `.up.sql`（重放时建表）与 store 自己的
//! `DDL` 常量（首启时建表）都算；`.down.sql` 不算，它描述的是回滚到旧形状，不是
//! 这棵树想去的终态。

use std::collections::BTreeSet;
use std::path::Path;

use crate::common;

use common::routes::repo_root;
use common::rust_production::production_sources;

/// 这张账的终态：一列都没有。
///
/// 这一段曾经是一份逐列点名的基线，一行一列，只能减不能增。2026-11-16 清到零，
/// 基线随之删掉——留一个空切片，读的人会以为「还剩一些，只是暂时没写下来」。
/// 判据本身留着，而且只剩一个方向：树上出现任何一列 json/jsonb 就是红。
#[test]
fn a_json_column_in_the_tree_is_a_storage_decision() {
    let root = repo_root();
    let found = census(&root);
    assert!(
        found.is_empty(),
        "这几列是 json/jsonb，而库的终态是一列都没有——加一列 jsonb 不是一次普通\
         改动，是把一段「可查询但没有 schema」的状态又放回库里。要按它查就给它加\
         列；只是搬运就存 text；没有人读就把它删掉：\n{}",
        found
            .iter()
            .map(|r| format!("  {r}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// 这一遍普查读了多少份输入：`(生产源文件, 迁移文件)`。
///
/// 「一列都没读到」有两种原因，而它们在判词上同形：库里真的一列都不剩了，或者
/// 这份普查走错了树。这个数把两者分开——读空只可能来自后者，因为树里无论何时都
/// 有几百份源文件与上百份迁移。清到零的那天这条门仍然该绿，而且绿得有意义：
/// 走错树会让它红，而不是让「全清完了」替我红一次。
fn inputs_read(root: &Path) -> (usize, usize) {
    (
        production_sources(root).len(),
        common::migrations::up_migrations(root).len(),
    )
}

#[test]
fn the_census_reads_the_tree_and_not_an_empty_set() {
    let root = repo_root();
    let (sources, migrations) = inputs_read(&root);
    assert!(
        sources > 0 && migrations > 0,
        "这份普查读到了 {sources} 份生产源文件与 {migrations} 份迁移：读空与「全清完了」\
         在判词上同形，而读空通常只意味着它走错了树"
    );
}

/// 树上声明的每一个 json/jsonb 列，键是 `文件:表.列`。
fn census(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        scan(&rel(root, &path), &text, &mut out);
    }
    for path in common::migrations::up_migrations(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        scan(&rel(root, &path), &text, &mut out);
    }
    // 迁移是一条时间线：后面的 `DROP COLUMN` 之后，那一列已经不是 jsonb 了；
    // `ALTER COLUMN … TYPE` 之后它已经是别的类型；整张表被 `DROP TABLE` 之后，
    // 它下面每一列都不在了。三种写法折的是同一件事，不折就只能靠改写历史迁移来
    // 减少——而历史是被应用到库上的，不该为了判据好看去改。
    let dropped = dropped_columns(root);
    let gone = dropped_tables(root);
    let retyped = retyped_columns(root);
    out.retain(|entry| {
        let Some((_, table_col)) = entry.split_once(':') else {
            return true;
        };
        let table = table_col.split('.').next().unwrap_or(table_col);
        !dropped.contains(table_col) && !gone.contains(table) && !retyped.contains(table_col)
    });
    out
}

/// 迁移里被改成别的类型的那些列，`表.列`。
///
/// `jsonb` → `text` 不必换列，也不必丢数据：`ALTER COLUMN ... TYPE TEXT USING
/// c::text` 就是这一步。判据只认 `DROP COLUMN` 的时候，这种改法在账上看起来
/// 什么也没发生——清到零的那天，账还压着一堆早就不是 jsonb 的列。改成别的类型
/// 的列与删掉的列在「它已经不是 jsonb 了」这件事上是同一件事。
fn retyped_columns(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in common::migrations::up_migrations(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for statement in common::migrations::statements(&text) {
            let Some(rest) = statement
                .to_ascii_lowercase()
                .strip_prefix("alter table ")
                .map(str::to_string)
            else {
                continue;
            };
            let mut words = rest.split_whitespace();
            let Some(table) = words.next() else {
                continue;
            };
            let table = table.trim_matches(|c: char| c == '"' || c == '`');
            let body: String = words.collect::<Vec<_>>().join(" ");
            // 两种写法都认：PostgreSQL 的 `ALTER COLUMN c TYPE t` 与 MySQL 的
            // `MODIFY COLUMN c t`。只看「改成了什么」——改成 json/jsonb 的
            // (`down` 迁移里那一种) 不算清账。
            for marker in ["alter column ", "modify column "] {
                let Some(at) = body.find(marker) else {
                    continue;
                };
                let after = body[at + marker.len()..].trim_start();
                let column: String = after
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if column.is_empty() {
                    continue;
                }
                let Some(kind_text) = after.get(column.len()..) else {
                    continue;
                };
                let kind_text = kind_text.trim_start();
                let kind_text = kind_text.strip_prefix("type ").unwrap_or(kind_text);
                let kind: String = kind_text
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if kind != "json" && kind != "jsonb" {
                    out.insert(format!("{table}.{column}"));
                }
            }
        }
    }
    out
}

/// 迁移里被 `ALTER TABLE <t> DROP COLUMN [IF EXISTS] <c>` 掉的那些列，`表.列`。
fn dropped_columns(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in common::migrations::up_migrations(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        // 一条语句一个分号：不先按 `;` 断开，第一条不带 `DROP COLUMN` 的
        // `ALTER TABLE` 会认领文件里后面任意一张表的 `DROP COLUMN`。
        for statement in common::migrations::statements(&text) {
            let Some(rest) = statement
                .to_ascii_lowercase()
                .strip_prefix("alter table ")
                .map(str::to_string)
            else {
                continue;
            };
            let mut words = rest.split_whitespace();
            let Some(table) = words.next() else {
                continue;
            };
            let body: String = words.collect::<Vec<_>>().join(" ");
            let Some(at) = body.find("drop column ") else {
                continue;
            };
            let after = body[at + "drop column ".len()..].trim_start();
            let after = after.strip_prefix("if exists ").unwrap_or(after);
            let column = after
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .unwrap_or("");
            if !column.is_empty() {
                out.insert(format!(
                    "{}.{column}",
                    table.trim_matches(|c: char| c == '"' || c == '`')
                ));
            }
        }
    }
    out
}

/// 迁移里被 `DROP TABLE [IF EXISTS] <t>` 掉的表。
///
/// 一张表没了，它下面每一列都不在库里了——这与 `DROP COLUMN` 是同一件事的
/// 两种写法，判据不该只认得窄的那一种。认不得的时候，`085` 那批「没有任何代码
/// 念到的表」会永远压在账上，读的人以为还有一列要清，其实那张表早就不是库里的
/// 形状了。
fn dropped_tables(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in common::migrations::up_migrations(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for statement in common::migrations::statements(&text) {
            let Some(rest) = statement
                .to_ascii_lowercase()
                .strip_prefix("drop table ")
                .map(str::to_string)
            else {
                continue;
            };
            let rest = rest
                .trim_start()
                .strip_prefix("if exists ")
                .unwrap_or(&rest);
            let table = rest
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .unwrap_or("");
            if !table.is_empty() {
                out.insert(table.to_string());
            }
        }
    }
    out
}

fn rel(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

/// 一份 DDL 文本里的 json/jsonb 列。
///
/// 只认表体里的列声明：`CREATE TABLE … (` 之后、配平的右括号之前，以及
/// `ALTER TABLE … ADD COLUMN`。整行匹配而不是全文本正则，是因为 `FORMAT JSON`
/// 这类语句尾巴会与「列名 类型」同形——`analytics.rs` 里 ClickHouse 的建表语句
/// 就有一行只写着 `FORMAT JSON`，它把自己的名字读成了一列。
fn scan(file: &str, text: &str, out: &mut BTreeSet<String>) {
    if !text.to_ascii_uppercase().contains("JSON") {
        return;
    }
    let mut table: Option<String> = None;
    let mut inside = false;
    // 一条 `ALTER TABLE` 可以跨很多行：第一行只有表名，列名在后续的行上。
    // 只读第一行时，「同一行里有没有 add column」把多行写法整段漏掉——而迁移
    // 里多行写法正是常态（`124` 的 `round_batch JSONB` 就是这样漏进树里的）。
    let mut alter_cont = false;
    let mut depth: i32 = 0;
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower
            .trim_start()
            .strip_prefix("create table if not exists ")
        {
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"');
            table = Some(name.to_string());
            depth = parens(line);
            inside = depth > 0;
            continue;
        }
        if let Some(rest) = lower.trim_start().strip_prefix("alter table ") {
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"');
            table = Some(name.to_string());
            inside = false;
            // 语句没在这一行收尾，后面的续行仍属于这张表。
            alter_cont = !line.trim_end().ends_with(';');
            let line_lower = line.to_ascii_lowercase();
            if let Some(rest) = line_lower
                .split_once("add column if not exists ")
                .map(|(_, r)| r)
            {
                if let Some(column) = column_type(rest) {
                    out.insert(format!("{file}:{}.{column}", name.trim_matches('"')));
                }
            }
            continue;
        }
        // `ALTER TABLE` 的续行：同样是这张表上的一列，只是换了一行写。
        if alter_cont {
            if let Some(name) = &table {
                let lower = line.to_ascii_lowercase();
                let rest = lower.trim_start();
                let rest = rest
                    .strip_prefix("add column if not exists ")
                    .unwrap_or(rest);
                if let Some(column) = column_type(rest) {
                    out.insert(format!("{file}:{}.{column}", name.trim_matches('"')));
                }
            }
            if line.trim_end().ends_with(';') {
                alter_cont = false;
            }
            continue;
        }
        if inside {
            if let Some(column) = column_type(line) {
                if let Some(name) = &table {
                    out.insert(format!("{file}:{name}.{column}"));
                }
            }
            depth += parens(line);
            if depth <= 0 {
                inside = false;
            }
        }
    }
}

fn parens(line: &str) -> i32 {
    line.matches('(').count() as i32 - line.matches(')').count() as i32
}

/// `<列名> JSON(B)` 的那一行，返回列名。
fn column_type(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let name_end = trimmed
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(trimmed.len());
    if name_end == 0 {
        return None;
    }
    let name = &trimmed[..name_end];
    let rest = trimmed[name_end..].trim_start();
    let rest_lower = rest.to_ascii_lowercase();
    for kind in ["jsonb", "json"] {
        if let Some(after) = rest_lower.strip_prefix(kind) {
            let boundary = after
                .chars()
                .next()
                .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
            if boundary {
                return Some(name.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod multi_line_alter_tests {
    use super::scan;
    use std::collections::BTreeSet;

    /// 多行 `ALTER TABLE` 的续行上新增的 jsonb 列要被读到。
    ///
    /// 这条普查原先只看 `ALTER TABLE` 那一行里有没有 `add column`；迁移里的
    /// 常态写法是表名一行、列定义在随后几行，于是 `124` 的 `round_batch JSONB`
    /// 整段漏过，门是绿的。红的必须是被读到的那一列，不是「文件里有 JSON 这个词」。
    #[test]
    fn a_json_column_added_on_a_continuation_line_is_counted() {
        let sql = "\
ALTER TABLE cogneva_event
    ADD COLUMN IF NOT EXISTS ledger_rev TEXT,
    ADD COLUMN IF NOT EXISTS round_batch JSONB GENERATED ALWAYS AS (attributes::jsonb -> 'batch') STORED;
";
        let mut out = BTreeSet::new();
        scan("m.sql", sql, &mut out);
        assert!(
            out.iter().any(|row| row.contains("round_batch")),
            "续行上的 jsonb 列没被读到：{out:?}"
        );
        assert!(
            !out.iter().any(|row| row.contains("ledger_rev")),
            "text 列被读成了 json：{out:?}"
        );
    }

    /// 一条 `ALTER TABLE` 收尾之后，后面的语句不该被当成本表的续行。
    #[test]
    fn a_statement_after_the_alter_does_not_become_its_rows() {
        let sql = "\
ALTER TABLE t
    ADD COLUMN IF NOT EXISTS a TEXT;
CREATE TABLE other (b TEXT);
ALTER TABLE other
    ADD COLUMN IF NOT EXISTS c JSON;
";
        let mut out = BTreeSet::new();
        scan("m.sql", sql, &mut out);
        assert!(
            out.iter().any(|row| row.contains("other.c")),
            "第二条 ALTER 的列没读到：{out:?}"
        );
        assert!(
            !out.iter().any(|row| row.contains("t.c")),
            "第二条 ALTER 的列被记到了上一张表上：{out:?}"
        );
    }
}
