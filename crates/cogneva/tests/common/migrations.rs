//! `crates/*/migrations/**/*.up.sql`：重放时会建表的那些。
//!
//! 两条普查读同一批文件（「库只存列」与「投影表自证有读者」），而它们先前各自抄了
//! 一份同样的走树代码。抄件之间不会打架，只会分开长：一处开始收 `mysql/`、另一处
//! 只收 `postgres/`，两份判词就关于两棵不同的树，而两份都绿。
//!
//! 判据在 `the_fixture_reading_steps_are_written_once_test`。

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::routes::walk;

/// 树里每一份迁移的 `.up.sql`。
///
/// 只收 `.up.sql`：`.down.sql` 描述的是回滚到旧形状，不是这棵树想去的终态。
pub fn up_migrations(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let crates = root.join("crates");
    let Ok(entries) = std::fs::read_dir(&crates) else {
        return out;
    };
    for entry in entries.flatten() {
        let migrations = entry.path().join("migrations");
        if !migrations.is_dir() {
            continue;
        }
        // 递归只该有一份（`common::fsio` 里那一份）。`walk` 按 `Path::extension` 收，
        // 而这里要的是 `*.up.sql`——扩展名是 `sql`，`.up` 是名字的一部分，所以先按
        // `sql` 收齐再按后缀过滤，而不是把 `"up.sql"` 当扩展名传进去（那样一份也收
        // 不到，空着与「全清完了」同形）。
        let mut sql = Vec::new();
        walk(&migrations, &["sql"], &mut sql);
        out.extend(sql.into_iter().filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".up.sql"))
        }));
    }
    out
}

/// 一份迁移文本里的一条条语句。
///
/// 先去掉整行注释再按 `;` 断：迁移文件的头是一段说明，第一行从来不是 SQL，
/// 直接 `trim_start().strip_prefix("drop table ")` 会一条也认不出来——账就会
/// 停在历史迁移那一行上，看起来像「这张表还在」。行内字符串里出现 `--` 的情形
/// 在迁移里没有用到；真用到的那天，这里会被读成一条更短的语句，而不是被读错。
pub fn statements(text: &str) -> Vec<String> {
    let cleaned: String = text
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    cleaned
        .split(';')
        .map(|statement| statement.trim().to_string())
        .collect()
}

/// 一份 DDL/源码文本里的 `CREATE TABLE` 与它们的列名。
///
/// 口径是声明点：迁移的 `.up.sql`（重放时建表）与 store 自己的 `CREATE TABLE IF
/// NOT EXISTS` 常量（首启时建表）都算。两条普查读同一张表就必须用同一个扫描器——
/// 两份扫描器分开长的时候，一份认 `if not exists`、另一份不认，两份判词就关于两棵
/// 不同的树，而两份都绿。
pub fn tables_in(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut in_table: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        let lower = trimmed.to_ascii_lowercase();
        if in_table.is_none() {
            // 注释里提到一句 `CREATE TABLE named inline` 不是建表：注释没有形状，
            // 但这条扫描器只认文本，所以先在这里把它排除。
            // 注释里提到一句 `CREATE TABLE named inline` 不是建表：迁移用 `--`、
            // 源码用 `//`，两种都排除。
            if trimmed.starts_with("--") || trimmed.starts_with("//") {
                continue;
            }
            // 一个 `CREATE TABLE` 不必从行首开始：`pub const DDL: &str = "CREATE TABLE
            // IF NOT EXISTS cogneva_owner_leases (` 就是这样写的。只认行首会把这类
            // 表读成「树上没有」，而那条判据的方向是「没有声明就红」——它不红，它
            // 一句也不说。
            // 后加的列从 `ALTER TABLE ... ADD COLUMN` 来：水位列几乎都是这样加上的，
            // 只认 `CREATE TABLE` 会把「这张表带水位」读成「没有」，而形状普查按水位
            // 认投影——那一条判据会对一张真有水位的表说它没有。
            if let Some(at) = lower.find("alter table ") {
                let rest = &lower[at + "alter table ".len()..];
                // `ALTER TABLE IF EXISTS <name>`：不剥掉这两个词，表名会被读成 `if`，
                // 于是普查里多出一张不存在的表、真表那一列的来路也丢了。
                let rest = rest.strip_prefix("if exists ").unwrap_or(rest);
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                if let Some(add) = lower.find(" add column ") {
                    let tail = &lower[add + " add column ".len()..];
                    let tail = tail.strip_prefix("if not exists ").unwrap_or(tail);
                    let column: String = tail
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect();
                    if !name.is_empty() && !column.is_empty() {
                        out.entry(name).or_default().insert(column);
                    }
                }
            }
            for opening in ["create table if not exists ", "create table "] {
                if let Some(at) = lower.find(opening) {
                    let rest = &lower[at + opening.len()..];
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
    out
}
