//! 同一件事只写一遍：批怎么选、判词对着什么说、派生的条目从哪来——每一件在树里只有
//! 一处实现，别处都是调用它。
//!
//! 这不是风格要求。三处各写一遍的后果是它们只在刚写完的那天一致：一次改了批的选择而
//! 另一处没改，两条路径给出的批次不同，而两边的日志都看起来正常。
//!
//! 没有这次改动这几条会红：`select_batch` 还没被开轮与钉轮共用、完成判能把空批读成
//! 「全部通过」、派生条目会把没有验收的片也当成待办。

use std::path::Path;

fn read(root: &Path, relative: &str) -> String {
    std::fs::read_to_string(root.join(relative)).unwrap_or_else(|e| panic!("{relative}: {e}"))
}

#[test]
fn one_function_decides_what_the_batch_is() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let contract = read(root, "crates/cog-core/src/contract/ledger_item.rs");
    assert_eq!(
        contract.matches("pub fn select_batch").count(),
        1,
        "批的选择有两份实现：它们相等的时机只有刚写完的那一天"
    );
    let ledger = read(root, "crates/cog-collaboration/src/actors/ledger.rs");
    assert!(
        ledger.matches("select_batch(").count() >= 2,
        "开轮与钉轮没有都走同一个选择函数"
    );
}

#[test]
fn a_completion_verdict_has_nothing_to_say_about_an_empty_batch() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let evaluator = read(root, "crates/cog-collaboration/src/actors/evaluator.rs");
    assert!(
        evaluator.contains("items.is_empty()"),
        "完成判没有挡住空批：空批会被读成「全部通过」"
    );
}

#[test]
fn derived_items_come_only_from_pieces_that_declared_an_acceptance() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let types = read(root, "crates/cog-collaboration/src/squad/pge/types.rs");
    assert!(
        types.contains(".filter(|t| !t.acceptance.is_empty())"),
        "派生条目把没有验收的片也当成待办：那是判据对着自己的影子下判断"
    );
}
