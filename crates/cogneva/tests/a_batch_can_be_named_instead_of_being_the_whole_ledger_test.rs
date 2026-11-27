//! 一轮做哪几条，可以由定批的那一侧点名，默认仍是台账上全部未完成的。
//!
//! 点名只做两件事：**筛**（顺序仍按台账——按点名顺序会让同一批在两次执行里换一个
//! 次序）与**查**（点了一个台账上没有的标记不是更小的批，是点错了；静默丢掉它，
//! 这一轮就会带着一个缺条目的批走完全部验证，而缺的那条没有谁会去看）。
//!
//! 没有这次改动这两条会红：`select_batch` 不存在，`ledger open --batch` 也不存在
//! （开轮只认「账上全部未完成」，一本八十几条的账因此永远关不掉）。

use cog_core::contract::ledger_item::{select_batch, LedgerItem, LedgerState};

fn item(id: &str) -> LedgerItem {
    LedgerItem {
        id: id.into(),
        terminal: "到终态的样子".into(),
        now: "现在".into(),
        gap: "差什么".into(),
        state: LedgerState::Open,
    }
}

#[test]
fn a_named_batch_is_a_filter_over_the_ledgers_own_order() {
    let items = vec![item("S1"), item("S2"), item("S3")];
    let asked = vec!["S3".to_string(), "S1".to_string()];
    let selected: Vec<String> = select_batch(&items, Some(&asked))
        .expect("both marks exist")
        .into_iter()
        .map(|item| item.id.clone())
        .collect();
    assert_eq!(selected, vec!["S1", "S3"], "点名的顺序不该改变批的顺序");
}

#[test]
fn a_mark_the_ledger_does_not_carry_refuses_the_batch() {
    let items = vec![item("S1")];
    let asked = vec!["S1".to_string(), "S29".to_string()];
    let err = select_batch(&items, Some(&asked)).expect_err("a typo must not shrink the batch");
    assert!(err.contains("S29"), "{err}");
    // 不点名就是全量：默认全量仍是默认。
    assert_eq!(select_batch(&items, None).unwrap().len(), 1);
}

/// 开轮这条路确实把点名传下去了：CLI 的 `--batch` 解出来之后，批与差距都从选中的
/// 那些条目来（不是先算全量再在旁边标注）。
#[test]
fn opening_a_round_carries_the_named_batch_into_the_book() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let ledger =
        std::fs::read_to_string(root.join("crates/cog-collaboration/src/actors/ledger.rs"))
            .expect("ledger.rs");
    assert!(
        ledger.contains("fn asked_batch"),
        "开轮没有 `--batch` 这个入口：一轮只能做账上全部未完成"
    );
    assert!(
        ledger.contains("select_batch(&items, asked.as_deref())"),
        "点名的批没有经过 `select_batch`：点错的标记会被静默丢掉"
    );
    assert!(
        ledger.contains("fn pin_round_selected"),
        "把计划钉进书里的那一步没有收这一批：轮里记的仍是全量"
    );
}
