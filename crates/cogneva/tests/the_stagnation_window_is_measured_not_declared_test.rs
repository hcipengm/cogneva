//! 「连续多少轮不买进展算停滞」不是一个人拍的轮数：它是这套部署见过的「平了 K 轮之后
//! 才买到一次进展」里最大的 K。没有样本时它不判——把还没跑够读成已经停滞，处置正好相反。
//!
//! 没有这次改动这几条会红：窗口常量还在（人拍的 5）、Ralph 的默认值不读读数、
//! 买到进展时没有人把平局轮数记成样本。

use cog_core::stagnation::{record_flat_run_before_improvement, window_rounds};

#[test]
fn the_window_follows_what_was_measured() {
    // 样本只增不减：一条更长的平局把窗口抬高，短平局不把它缩回去。
    let before = window_rounds();
    record_flat_run_before_improvement(7);
    assert!(window_rounds() >= 7);
    record_flat_run_before_improvement(2);
    assert!(window_rounds() >= 7, "短平局把窗口收窄了");
    assert!(window_rounds() >= before);
}

#[test]
fn ralph_reads_the_window_and_feeds_it() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let ralph = std::fs::read_to_string(
        root.join("crates/cog-collaboration/src/squad/ralph/ralph_loop.rs"),
    )
    .expect("ralph_loop.rs");
    assert!(
        !ralph.contains("RALPH_STAGNATION_WINDOW"),
        "人拍的窗口常量还在：默认值又会变成一个数字"
    );
    assert!(
        ralph.contains("cog_core::stagnation::window_rounds()"),
        "默认窗口不读实测"
    );
    assert!(
        ralph.contains("record_flat_run_before_improvement(flat_streak)"),
        "买到进展时没有把「平了多少轮才买到」记成样本：窗口永远停在 0"
    );
    assert!(
        ralph.contains("if window == 0"),
        "没有样本时（窗口 0）仍在判停滞：还没跑够会被读成已经停滞"
    );
}
