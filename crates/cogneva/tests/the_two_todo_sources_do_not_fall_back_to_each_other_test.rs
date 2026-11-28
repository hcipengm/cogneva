//! 两本台账各有各的读者：干活的人读自己那一本（文件），平台读变更账（库）。两边不会
//! 互相顶替——一本读不到时报「读不到」，不回落成另一本，也不读成「没有待办」。
//!
//! 没有这次改动这几条会红：解析器开始回落共享清单、两边互相引用、读不到被读成空。

use std::path::PathBuf;

use cog_collaboration::actors::ledger::resolve_ledger_path;

#[test]
fn the_worker_ledger_is_resolved_or_refused_never_defaulted() {
    // 什么都没给：报错，不回落成一份共享清单。
    assert!(resolve_ledger_path(None, None, None).is_err());
    // 显式地址优先，`md:` 前缀是同一个地址的另一种写法。
    let declared = PathBuf::from("/declared/ledger.md");
    assert_eq!(
        resolve_ledger_path(Some("md:/a/one.md"), Some("/b/two.md"), Some(&declared)).unwrap(),
        PathBuf::from("/a/one.md")
    );
    assert_eq!(
        resolve_ledger_path(None, Some("/b/two.md"), Some(&declared)).unwrap(),
        PathBuf::from("/b/two.md")
    );
    // 有一个声明文件但它读不到 / 是空的：报错，而不是「没有待办」。
    assert!(resolve_ledger_path(None, None, Some(&declared)).is_err());
}

#[test]
fn the_two_readers_are_separate_paths_through_the_tree() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let ledger =
        std::fs::read_to_string(root.join("crates/cog-collaboration/src/actors/ledger.rs"))
            .expect("ledger.rs");
    // 带外台账这一侧只认文件；它不去查变更账。
    assert!(
        ledger.contains("pub fn resolve_ledger_path"),
        "带外台账的解析点不见了"
    );
    assert!(
        !ledger.contains("FROM change_ledger"),
        "带外台账那一侧查起了变更账：两边的待办会互相顶替"
    );
    let change = std::fs::read_to_string(
        root.join("crates/cog-storage/src/postgres/change_ledger_store.rs"),
    )
    .expect("change_ledger_store.rs");
    assert!(
        !change.contains("COGNEVA_TODO_LEDGER") && !change.contains("todo-ledger"),
        "平台那一侧去读带外台账了：它读的应当是库里的变更账"
    );
    let item = std::fs::read_to_string(root.join("crates/cog-core/src/contract/ledger_item.rs"))
        .expect("ledger_item.rs");
    assert!(
        !item.contains("read_to_string"),
        "批的选择读起了文件：它应当只对传进来的条目做判断"
    );
}
