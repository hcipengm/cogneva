//! 换表三段的路与那道门：双写 → 读切新侧 → 旧侧只读 → 删掉。往下一段走的唯一依据是
//! 这一刻量出来的逐行对照——只在旧侧、只在新侧都是零。差一行就停在当前段，不往回退，
//! 也不往前推。
//!
//! 没有这次改动这几条会红：命令不存在、判定不是从逐行对照来的、段可以跳。

use cog_storage::swap_stage::{advance, Comparison, SwapStage};

#[test]
fn the_gate_reads_the_comparison_not_a_schedule() {
    let equal = Comparison {
        only_old: 0,
        only_new: 0,
    };
    assert_eq!(
        advance(SwapStage::DoubleWrite, &equal),
        Ok(SwapStage::ReadNew)
    );
    let differ = Comparison {
        only_old: 0,
        only_new: 2,
    };
    let stop = advance(SwapStage::ReadNew, &differ).expect_err("a difference stops the walk");
    assert!(stop.contains("只在新侧 2 行"), "{stop}");
    // 最后一段没有下一段，哪怕两侧相等。
    assert!(advance(SwapStage::Dropped, &equal).is_err());
}

#[test]
fn the_command_measures_both_sides_before_it_answers() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let cli = std::fs::read_to_string(root.join("crates/cogneva/src/cli.rs")).expect("cli.rs");
    assert!(cli.contains("table-swap"), "没有这个子命令：门没有入口");
    let command = std::fs::read_to_string(root.join("crates/cogneva/src/table_swap.rs"))
        .expect("table_swap.rs");
    assert!(
        command.contains("diff_counts"),
        "判定不是从这一刻的逐行对照来的：那它只能来自日程或人的判断"
    );
    assert!(
        command.contains("SwapStage::parse"),
        "段词没有按闭集读：写错一个词会被当成另一段"
    );
}
