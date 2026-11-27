//! principle: feedback_no_hardcoding_config_surface
//!
//! 迁移链必须能在**空库**上从头跑完。
//!
//! 这是恢复演练的第一步，也是唯一一条「镜像能不能建出自己的形状」的判据：演练把
//! 最新一个包装进一个跑完迁移的空库，再建 store 表、灌数据、逐表对数。链上有一条
//! 语句在空库上失败，整条演练就红——而它红的时候，人读到的是「这个包不可恢复」，
//! 不是「迁移 113 写到一张还没建的表上」。
//!
//! 两种失败长得很像，判据要分开：
//!
//! * 写到一张**这条链自己会建**的表：只要那一步在 `CREATE TABLE` 之后，就没问题；
//! * 写到一张**store 首启时才建**的表（`cog_metrics_samples` 这类）：链在空库上跑
//!   到它时它还不存在。这种语句必须写成 `ALTER TABLE IF EXISTS`，或者整段包在
//!   `to_regclass('<表>')` 的守卫里——新库那边由 store 的 `CREATE TABLE` 直接建成
//!   最终形状，迁移这一步什么也不做才对。
//!
//! 这条判据是量出来的：2026-11-16 第一次把整条链在一个空库上重放，它红在三条语句
//! 上（`messages` 已被 081 删掉、`alerts` 与四张指标表由 store 建）。当时链上没有任何
//! 一条判据看这件事，红只在演练里出现——也就是只在一个已经装不下的时候出现。

use std::collections::BTreeSet;
use std::path::Path;

use crate::common;

use common::migrations::{statements, up_migrations};
use common::routes::repo_root;

#[test]
fn every_migration_statement_survives_a_database_that_starts_empty() {
    let root = repo_root();
    let violations = violations(&root);
    assert!(
        violations.is_empty(),
        "这些语句在空库上会失败，或者会被当成「表在这儿」而去改一张 store 建的表：\n{}\n\
         改成 `ALTER TABLE IF EXISTS`，或者把整段包进 `to_regclass('<表>')` 的守卫——\
         空库那边由 store 的 `CREATE TABLE` 直接建成最终形状，迁移这一步什么也不做才对。",
        violations
            .iter()
            .map(|violation| format!("  {violation}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// 逐份文件、逐条语句走一遍，记下那些点名一张**这条链从来不建**的表、又没有写
/// `IF EXISTS`（或没有守卫）的语句。
///
/// 先收齐「链建过的表」，再判语句，而不是边读边判：建表语句也会出现在 `DO` 块里
/// （025 的分区生成本身就是一条 `CREATE TABLE … PARTITION OF`），按语句前缀收会漏掉
/// 它们，那些表就会被误判成「没人建」。顺序本身不判——空库重放已经证明顺序成立，
/// 这一步只回答「这张表是不是从头到尾都不归迁移管」。
fn violations(root: &Path) -> Vec<String> {
    // 一种方言一条链：MySQL 的 `.up.sql` 与 PostgreSQL 的**不是**同一条链的两个
    // 片段，它们各建各的表。混在一起判，会把 MySQL 那一侧每一条写 store 表的 ALTER
    // 都算到 PostgreSQL 头上——而这一条判据只对 PostgreSQL 有实测（空库重放跑的是
    // 它）。MySQL 那一侧的容错写法是它自己的方言问题（它没有 `ALTER TABLE IF
    // EXISTS`），要用它的时候得按它自己的规则再判一次。
    let mut files: Vec<(String, String)> = Vec::new();
    for path in up_migrations(root) {
        let dialect = path
            .parent()
            .and_then(|dir| dir.file_name())
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        if dialect != "postgres" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap_or(Default::default());
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        files.push((name, text));
    }

    let mut created: BTreeSet<String> = BTreeSet::new();
    for (_, text) in &files {
        let lower = text.to_ascii_lowercase();
        let mut rest = lower.as_str();
        while let Some(at) = rest.find("create table ") {
            rest = &rest[at + "create table ".len()..];
            let rest = rest.strip_prefix("if not exists ").unwrap_or(rest);
            if let Some(table) = identifier(rest) {
                created.insert(table);
            }
        }
    }

    let mut out = Vec::new();
    for (name, text) in &files {
        let lower = text.to_ascii_lowercase();
        let mut gone: BTreeSet<String> = BTreeSet::new();
        let mut rest = lower.as_str();
        while let Some(at) = rest.find("drop table ") {
            rest = &rest[at + "drop table ".len()..];
            let rest = rest.strip_prefix("if exists ").unwrap_or(rest);
            if let Some(table) = identifier(rest) {
                gone.insert(table);
            }
        }
        let guards = guarded_tables(text);
        for statement in statements(text) {
            let statement_lower = statement.to_ascii_lowercase();
            let Some((table, tolerant)) = touched_table(&statement_lower) else {
                continue;
            };
            if !created.contains(&table) || gone.contains(&table) {
                // 链不管这张表：它归 store 建，空库上此刻还不存在。
                if tolerant || guards.contains(&table) {
                    continue;
                }
            } else {
                continue;
            }
            out.push(format!("{name}: {}", one_line(&statement)));
        }
    }
    out
}

/// 一条语句点名的表，以及它是否自带容错写法。
fn touched_table(lower: &str) -> Option<(String, bool)> {
    if let Some(rest) = lower.strip_prefix("alter table ") {
        return match rest.strip_prefix("if exists ") {
            Some(after) => Some((identifier(after)?, true)),
            None => Some((identifier(rest)?, false)),
        };
    }
    if let Some(rest) = lower.strip_prefix("update ") {
        return Some((identifier(rest)?, false));
    }
    if let Some(rest) = lower.strip_prefix("delete from ") {
        return Some((identifier(rest)?, false));
    }
    if let Some(rest) = lower.strip_prefix("insert into ") {
        return Some((identifier(rest)?, false));
    }
    if lower.starts_with("create index") || lower.starts_with("create unique index") {
        // `CREATE INDEX [IF NOT EXISTS] <idx> ON <t>`
        let at = lower.find(" on ")?;
        return Some((identifier(&lower[at + 4..])?, false));
    }
    // `SELECT`/`DO`/函数体与判据无关：它们不改形状。
    None
}

/// 这份文本里用 `to_regclass('t')` 守住的表。
fn guarded_tables(text: &str) -> BTreeSet<String> {
    let lower = text.to_ascii_lowercase();
    let mut out = BTreeSet::new();
    let mut rest = lower.as_str();
    while let Some(at) = rest.find("to_regclass('") {
        rest = &rest[at + "to_regclass('".len()..];
        if let Some(table) = identifier(rest) {
            out.insert(table);
        }
    }
    out
}

/// 开头的那个标识符（大小写已折成小写），引号与限定名都算「叫不准」，返回 `None`。
fn identifier(text: &str) -> Option<String> {
    let text = text.trim_start();
    if text.starts_with('"') {
        return None;
    }
    let name: String = text
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn one_line(statement: &str) -> String {
    statement.split_whitespace().collect::<Vec<_>>().join(" ")
}
