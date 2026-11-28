//! principle: feedback_a_table_name_built_at_a_call_site_is_invisible_to_every_census
//!
//! 表名拼在语句里的写入，任何「按字面找 INSERT INTO <表>」的普查都读不见。
//!
//! 这不是说那种写法错了：分区维护要在父表与默认分区之间搬行（名字由维护器算），
//! 账本那条按表名分派（名字来自常量）。但**普查要看得见它们**，否则「谁写了这张表」
//! 的答案会少掉一整块，而那一块恰恰是最容易漏的那一类。所以这里要的是一张**名单**：
//! 每一处「表名是拼出来的」写入都登记在这里，并写清名字从哪儿来。
//!
//! 判据两条：
//!   1. 树上每一处形如 `INSERT INTO {…}` 的写入，都必须在这张名单里点名（文件 + 行内容）；
//!   2. 名单里的每一条都必须在树上还找得到——搬走或删掉之后要跟着划掉，不留死指针。

use std::path::Path;

// 仓库根只有一份算法（`common::routes::repo_root()`）。
use crate::common::routes::repo_root;

/// 生产源码里每一处「表名是拼出来的」写入：`(相对路径, 语句片段)`。
fn dynamic_inserts(root: &Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|n| n.to_str()) != Some("tests") {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.contains("INSERT INTO {") {
                    out.push((
                        path.strip_prefix(root)
                            .unwrap_or(&path)
                            .display()
                            .to_string(),
                        trimmed.to_string(),
                    ));
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// 登记过的「表名拼出来的」写入，以及名字从哪儿来。
///
/// 每一条都要回答「这个名字是谁给的」：常量、维护器算的、调用方传的。答不出处
/// 的那一处，就是下一次普查会漏掉的那一处。
#[test]
fn every_dynamic_insert_is_named_in_the_register() {
    let root = repo_root();
    let found = dynamic_inserts(&root);
    assert!(
        !found.is_empty(),
        "一处都没扫到：要么树上真的没有了（那就把这条门删掉），要么扫描坏了"
    );
    for (file, statement) in &found {
        assert!(file.starts_with("crates/"), "路径不在 crates/ 下：{file}");
        // 名单以「文件 + 语句」为键，登记在下面的常量里。
        assert!(
            REGISTER
                .iter()
                .any(|(known_file, known)| { *known_file == file && statement.contains(*known) }),
            "这一处 `INSERT INTO {{…}}` 没有登记：{file} — `{statement}`。\n\
             它是哪张表、名字从哪儿来？写进本文件顶部的 REGISTER，普查才看得见它。"
        );
    }
}

/// 名单不许留下树里已经没有的指针。
#[test]
fn the_register_holds_no_dead_entry() {
    let root = repo_root();
    let found = dynamic_inserts(&root);
    for (file, known) in REGISTER {
        assert!(
            found
                .iter()
                .any(|(found_file, statement)| found_file == file && statement.contains(known)),
            "名单里的这一条在树上已经找不到了：{file} — `{known}`。删掉它，或者说明它搬去了哪。"
        );
    }
}

/// 表名从哪儿来。
const REGISTER: &[(&str, &str)] = &[
    (
        "crates/cog-storage/src/partition_maintainer.rs",
        "INSERT INTO {parent} SELECT * FROM {default}",
    ),
    (
        "crates/cog-observability/src/skip_store.rs",
        "INSERT INTO {SKIP_TABLE}",
    ),
    (
        "crates/cog-observability/src/spend_verdict.rs",
        "INSERT INTO {VERDICT_TABLE}",
    ),
    (
        "crates/cog-observability/src/analytics.rs",
        "INSERT INTO {}",
    ),
    (
        "crates/cog-observability/src/usage_store.rs",
        "INSERT INTO {WINDOW_TABLE}",
    ),
    (
        "crates/cog-observability/src/usage_store.rs",
        "INSERT INTO {ROLLUP_TABLE}",
    ),
];
