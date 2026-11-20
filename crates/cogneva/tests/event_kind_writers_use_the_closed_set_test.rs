//! principle: feedback_a_closed_set_is_only_closed_where_the_writer_reads_it
//!
//! 每个写事件的地方，写下去的 kind 都必须在 `cog_core::event_envelope::KINDS` 里。
//!
//! 闭集只有被**写入口**读才算闭：一处写死的第三十个拼写照样能落库，而「有哪些 kind」
//! 的答案会多出一族没人登记的事件。这条判据在树里找：生产代码里每个写成字面量的
//! `kind`（`kind = "..."`、`'...'` 出现在 `cogneva_event` 的 INSERT 附近、以及
//! `const ..._KIND: &str = "..."`），都必须在闭集里。
//!
//! 只判字面量：拼接出来的 kind 属于上一族（表名/名字拼在语句里），由它自己的登记表管。

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("two levels below the root")
        .to_path_buf()
}

/// 生产源码里出现的 kind 字面量。
fn kind_literals(root: &Path) -> Vec<(String, String)> {
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
                let Some(rest) = trimmed.strip_prefix("const ") else {
                    continue;
                };
                if !rest.contains("_KIND: &str = \"") {
                    continue;
                }
                if let Some(start) = rest.find('"') {
                    if let Some(end) = rest[start + 1..].find('"') {
                        out.push((
                            path.strip_prefix(root)
                                .unwrap_or(&path)
                                .display()
                                .to_string(),
                            rest[start + 1..start + 1 + end].to_string(),
                        ));
                    }
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// 每个 `*_KIND` 常量写的词，都必须在闭集里。
#[test]
fn every_kind_constant_writes_a_word_the_closed_set_holds() {
    let root = repo_root();
    let found = kind_literals(&root);
    assert!(
        found.len() >= 5,
        "扫到的 kind 常量太少（{}），要么扫描坏了，要么常量改了形状：{found:?}",
        found.len()
    );
    let known: Vec<&str> = cog_core::event_envelope::KINDS.to_vec();
    let mut unknown: Vec<String> = Vec::new();
    for (file, kind) in &found {
        if !known.contains(&kind.as_str()) {
            unknown.push(format!("{file}: {kind}"));
        }
    }
    assert!(
        unknown.is_empty(),
        "这些常量写下的 kind 不在闭集里：{unknown:?}。加上它（同时改 `KINDS`），\
         或者把那一族并进已有的拼写——两个词就是两族事件。"
    );
}
