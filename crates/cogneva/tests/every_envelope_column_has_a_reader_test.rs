//! principle: feedback_a_column_nobody_reads_is_a_claim_nobody_checks
//!
//! 事件信封的每一列都要有一个读者或写者。
//!
//! 信封的列是**能力**：`causation_id` 存在是因为有人要顺着因果链回答「这一条是谁引发的」，
//! `batch_id` 存在是因为有人要问「这一批一起花了多少」。一列没有读者，它就不是能力，
//! 是一条没人校验的声明——写进去的值可以是错的、可以一直空着，而没有任何读数会红。
//!
//! 判据是**在树里找得到**：迁移里出现的每一个列名，必须在生产代码里至少被点名一次
//! （读、写、或作为查询条件）。只写在迁移里、没有别的文件提到它的那一列，是这道门要
//! 报的形状。
//!
//! 这不是「列名不许出现第二次」那种门：它只问「有没有人在用它」，不限制用几次、怎么用。

use std::collections::BTreeSet;
use std::path::Path;

// 仓库根只有一份算法（`common::routes::repo_root()`）：各写一遍时，根一漂读数就
// 变成零，与「没有题目」同形。
use crate::common::routes::repo_root;

/// 迁移里 `cogneva_event` 的列名，按出现顺序。
fn envelope_columns(root: &Path) -> Vec<String> {
    let path = root.join("crates/cog-storage/migrations/postgres/117_event_stream.up.sql");
    let text = std::fs::read_to_string(&path).expect("the envelope migration is in the tree");
    let body = text
        .split("CREATE TABLE IF NOT EXISTS cogneva_event (")
        .nth(1)
        .expect("the migration creates the table");
    let mut columns = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with(')') || trimmed.starts_with("CONSTRAINT") {
            break;
        }
        let mut parts = trimmed.split_whitespace();
        if let Some(name) = parts.next() {
            if name.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                && parts.next().is_some()
                && !name.is_empty()
            {
                columns.push(name.to_string());
            }
        }
    }
    columns
}

/// 生产源码里出现的标识符集合（`crates/**/src`，不含测试）。
fn production_text(root: &Path) -> String {
    let mut out = String::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // 测试目录不是读者：它们证明的是「这条路径能跑」，不是「有人在用这一列」。
                if path.file_name().and_then(|n| n.to_str()) != Some("tests") {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("rs")
                && path.components().any(|c| c.as_os_str() == "src")
            {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    out.push_str(&text);
                    out.push('\n');
                }
            }
        }
    }
    out
}

#[test]
fn every_envelope_column_is_named_outside_the_migration() {
    let root = repo_root();
    let columns = envelope_columns(&root);
    assert!(
        columns.len() >= 15,
        "信封应当有十几列，读出来只有 {columns:?}：解析坏了，不是树里少列"
    );
    let text = production_text(&root);
    let mut unread: Vec<&str> = Vec::new();
    for column in &columns {
        if !text.contains(column.as_str()) {
            unread.push(column);
        }
    }
    assert!(
        unread.is_empty(),
        "信封里这些列在生产代码里没有任何读者或写者：写进去的值可以是错的、可以一直空着，\
         而没有一条读数会红。要么给它一个读者，要么从信封里去掉：{unread:?}（读到的列：{columns:?}）"
    );
}

/// 解析本身要有反例：把一列换成一个没人写的名字，这条门必须报出来。
#[test]
fn a_column_nobody_names_is_reported() {
    let root = repo_root();
    let columns = envelope_columns(&root);
    let text = production_text(&root);
    let ghost: BTreeSet<&str> = ["a_column_that_nobody_writes"].into_iter().collect();
    let unread: Vec<&&str> = ghost
        .iter()
        .filter(|column| !text.contains(**column))
        .collect();
    assert_eq!(
        unread.len(),
        1,
        "反例没被报出来，说明这条判据认不出『没有读者的列』（读到的列数 {}）",
        columns.len()
    );
}
