//! 网关与反射这一层不自己读表：它们的答案来自下一层的存储读口（`STREAM_READERS` 与
//! `PROJECTION_READERS` 两份普查）。这一层一旦自己写 SQL，那个读口就绕过普查——「这个
//! 答案从哪条流算出来」在这一层变成没人回答的问题。
//!
//! 判据只认出现形式：`FROM`／`INTO`／`UPDATE`／`JOIN` 后面跟着普查里的一张表。今天这两个
//! crate 的生产代码里一条都没有；新加一条就红，加的人要么改用存储读口，要么把那个读口
//! 登记进普查。
//!
//! 没有这次改动这条会红：它本身不存在，而这一层的读法没有任何东西守着。

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;

#[test]
fn neither_the_gateway_nor_reflection_reads_a_censused_table_directly() {
    let root = repo_root();
    let censused: Vec<&str> = cog_storage::table_shapes::TABLE_CENSUS
        .iter()
        .map(|row| row.table)
        .collect();
    let mut offenders = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if !(relative.starts_with("crates/cog-gateway/src/")
            || relative.starts_with("crates/cog-reflection/src/"))
        {
            continue;
        }
        for table in &censused {
            for verb in ["FROM", "INTO", "UPDATE", "JOIN"] {
                let needle = format!("{verb} {table}");
                if text.contains(&needle) {
                    offenders.push(format!("{relative}: `{needle}`"));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "这一层自己读了表（应当走存储读口，并把那个读口登记进普查）：\n  {}",
        offenders.join("\n  ")
    );
}
