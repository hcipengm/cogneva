//! 原始事实留多久，不由一个拍出来的天数决定：体积算出来的天数与读者实际问过的窗口，
//! 两者取大。读者问过四十五天而账本只留三十天，下一次问同一段过去就会读成一个看起来
//! 像「那段时间没有发生任何事」的缺口——缺口与安静读数上同形。
//!
//! 没有这次改动这几条会红：`kept_days_against_reads` 不存在、花销屏不记它问过的窗口、
//! 用量清扫不看那个读数。

use cog_core::retention::kept_days_against_reads;

#[test]
fn the_floor_is_the_larger_of_volume_days_and_the_farthest_reader() {
    // 没有读者问得更远：体积算出来的天数就是地板。
    assert_eq!(kept_days_against_reads(30, 0), 30);
    // 读者问过四十五天：地板抬到四十五天。
    assert_eq!(kept_days_against_reads(30, 45 * 86_400), 45);
    // 不足一天按一天算（留半天与留零天在缺口上同形）。
    assert_eq!(kept_days_against_reads(1, 1), 1);
    // 读者问得比现有地板近：不缩短。
    assert_eq!(kept_days_against_reads(90, 10 * 86_400), 90);
}

#[test]
fn the_spend_screen_records_its_window_and_the_sweep_reads_it() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let gateway = std::fs::read_to_string(root.join("crates/cog-gateway/src/observability.rs"))
        .expect("observability.rs");
    assert!(
        gateway.contains("LLM_USAGE_READ_WINDOW_KNOB"),
        "花销屏没有记下它问过的窗口：清扫那一侧永远不知道有人在读更远"
    );
    assert!(
        gateway.contains("record_read_span("),
        "窗口没有落到读数里：knob 定义了也没有人写"
    );
    let sweep =
        std::fs::read_to_string(root.join("crates/cog-observability/src/usage_retention.rs"))
            .expect("usage_retention.rs");
    assert!(
        sweep.contains("kept_days_against_reads"),
        "清扫只看体积、不看读者能读多远"
    );
    assert!(
        sweep.contains("longest_read_span"),
        "清扫没有读「问过的最远窗口」：地板不会跟着读者抬"
    );
}
