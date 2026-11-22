//! principle: feedback_an_unmeasured_value_written_as_zero_is_indistinguishable_from_a_measured_zero
//!
//! 留底表的那一句 `INSERT` 点名的每一列，都必须由这个 store 自己建出来。
//!
//! 这张表没有第二份 DDL：它由 `usage_store.rs` 里的 DDL 模板在进程启动时建/补齐，
//! `cog-storage` 的迁移目录里没有它。于是「写入口加了列、建表那一侧没加」不会在编译
//! 期被发现，也不会在启动期被发现——它只会让每一次 `INSERT` 在运行时失败，然后被那句
//! `tracing::warn!` 吞掉：**留底表安静地什么都不写**，而用量台账照常，读数上一切正常。
//!
//! 2026-11-20 实测过一次：正文两列（`request_body` / `response_body`）就是这样被写进
//! `INSERT` 的，模板里没有它们，所以那一版在真库上一条都写不进去。
//!
//! 判据是树判据：从源码里读出 `INSERT` 的列名集合与 DDL 的列名集合，前者必须是后者的
//! 子集。反方向不判——老行留在表里的列比写入口用到的多，那是允许的（删列会销毁正文）。

// 仓库根只有一份算法（`common::routes::repo_root()`）：各写一遍时，根一漂读数就
// 变成零，与「没有题目」同形。
use crate::common::routes::repo_root;

fn usage_store_source() -> String {
    let path = repo_root().join("crates/cog-observability/src/usage_store.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读不到 {}: {e}", path.display()))
}

/// `INSERT INTO gateway_llm_exchange ( … )` 里点名的列。
fn inserted_columns(src: &str) -> Vec<String> {
    let at = src
        .find("INSERT INTO gateway_llm_exchange")
        .expect("源码里没有这条 INSERT：这张表没有第二处写入口，找不到就是写入口被挪走了");
    let rest = &src[at..];
    let open = rest.find('(').expect("INSERT 后面没有列清单");
    let close = rest[open..].find(')').expect("列清单没有右上括号") + open;
    rest[open + 1..close]
        .split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect()
}

/// DDL 模板替这张表建出来的列：`CREATE TABLE` 的列，加上后来补的 `ADD COLUMN`。
fn created_columns(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(at) = src.find("CREATE TABLE IF NOT EXISTS gateway_llm_exchange") {
        let rest = &src[at..];
        let open = rest.find('(').expect("建表语句后面没有列清单");
        let close = rest[open..].find("\n);").expect("建表语句没有结尾") + open;
        for line in rest[open + 1..close].lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") {
                continue;
            }
            if let Some(name) = line.split_whitespace().next() {
                out.push(name.to_string());
            }
        }
    }
    // `ALTER TABLE gateway_llm_exchange [ADD COLUMN [IF NOT EXISTS]] <列名>`
    let mut rest = src;
    while let Some(at) = rest.find("ALTER TABLE gateway_llm_exchange") {
        let tail = &rest[at + "ALTER TABLE gateway_llm_exchange".len()..];
        let end = tail.find(';').unwrap_or(tail.len());
        let stmt = &tail[..end];
        rest = &tail[end..];
        let Some(add) = stmt.find("ADD COLUMN") else {
            continue;
        };
        let after = stmt[add + "ADD COLUMN".len()..].trim_start();
        let after = after
            .strip_prefix("IF NOT EXISTS")
            .map(str::trim_start)
            .unwrap_or(after);
        if let Some(name) = after.split_whitespace().next() {
            out.push(name.to_string());
        }
    }
    out
}

/// 写入口点名的每一列，都必须建得出来。
///
/// 这条判据在 2026-11-20 之前是红的：`request_body` / `response_body` 在 INSERT 里，
/// 模板里没有它们。
#[test]
fn every_bound_column_is_created_by_the_store() {
    let src = usage_store_source();
    let inserted = inserted_columns(&src);
    let created = created_columns(&src);
    assert!(
        !inserted.is_empty(),
        "一条列都读不出来：解析没跟上源码，这道门现在是空的"
    );
    let missing: Vec<&String> = inserted.iter().filter(|c| !created.contains(c)).collect();
    assert!(
        missing.is_empty(),
        "这条 INSERT 点名了 store 自己不建的列：{missing:?}。它们在真库上会让每一次写都失败，\
         而失败被 `tracing::warn!` 吞掉——留底表什么都不写，读数一切正常。建出来的列：{created:?}"
    );
}
