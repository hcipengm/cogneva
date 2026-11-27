//! 每一个从流上直接读答案的入口，都要在普查里点名：函数在哪、读哪几类事实。
//!
//! 读口是「任何查询的答案都能从事件重算」这条不变式的执行面。新加一个读风格的
//! `pub async fn` 而不登记，它读的那几类事实就没人对过账。

use crate::common::routes::repo_root;
use crate::common::rust_production::production_texts;
use cog_storage::table_shapes::STREAM_READERS;

const READ_PREFIXES: &[&str] = &[
    "read",
    "list",
    "get",
    "load",
    "pending",
    "current",
    "dialogue",
    "calls",
    "keys",
    "rebuild",
    "redactions",
    "lease",
    "item_",
    "principle_",
];

#[test]
fn every_read_shaped_function_in_storage_is_in_the_census() {
    let root = repo_root();
    let mut found = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if !relative.starts_with("crates/cog-storage/src/postgres/") {
            continue;
        }
        // 测试段落不参与：夹具不是读口。
        let production = crate::common::rust_production::production_text(&text);
        for line in production.lines() {
            let line = line.trim_start();
            let Some(rest) = line.strip_prefix("pub async fn ") else {
                continue;
            };
            let name = rest.split(['(', '<']).next().unwrap_or("").trim();
            if READ_PREFIXES.iter().any(|prefix| name.starts_with(prefix)) {
                found.push((name.to_string(), relative.clone()));
            }
        }
    }
    let listed: std::collections::BTreeSet<&str> = STREAM_READERS
        .iter()
        .map(|reader| reader.function)
        .collect();
    let mut missing = Vec::new();
    for (name, path) in &found {
        if !listed.contains(name.as_str()) {
            missing.push(format!("{path}: {name}"));
        }
    }
    assert!(
        missing.is_empty(),
        "这些读风格的入口不在 `STREAM_READERS` 里：\n{}",
        missing.join("\n")
    );
    for reader in STREAM_READERS {
        assert!(
            root.join(reader.path).exists(),
            "{}: 点名的文件不存在",
            reader.function
        );
        assert!(
            !reader.reads.trim().is_empty(),
            "{}: 没写它读哪几类事实",
            reader.function
        );
    }
}
