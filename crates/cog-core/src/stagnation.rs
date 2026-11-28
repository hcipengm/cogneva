//! 「连续多少轮没买到进展算停滞」这个窗口从哪来。
//!
//! 它与编排侧的静默窗口同一条形状：窗**由实测导出**——这套部署见过的、「连续平了 K 轮
//! 之后才终于买到一次进展」里最大的那个 K。没有样本时它不判（窗口 0）：一个拍出来的
//! 轮数会把「还没跑够」读成「已经停滞」，而两者的处置相反。
//!
//! 只增不减：一条被实测到的长平局是「这条路可以平这么久还活着」的证据，后来没人再跑出
//! 那么长的平局，不构成把它缩回去的理由——缩回去的窗口会误杀下一次正常的慢收敛。

use std::sync::atomic::{AtomicU32, Ordering};

static LONGEST_FLAT_RUN: AtomicU32 = AtomicU32::new(0);

/// 记一次「平了 `flat_rounds` 轮之后买到了进展」。0 不是一个样本（它说的是上一轮就
/// 有进展），不记。
pub fn record_flat_run_before_improvement(flat_rounds: u32) {
    if flat_rounds == 0 {
        return;
    }
    LONGEST_FLAT_RUN.fetch_max(flat_rounds, Ordering::Relaxed);
}

/// 这套部署见过的最大平局轮数；还没有样本时是 0（「量不到」）。
pub fn window_rounds() -> u32 {
    LONGEST_FLAT_RUN.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 窗口跟着样本走：记一条更长的，窗口就跟着变长；记短的不会缩回去。
    #[test]
    fn the_window_follows_the_longest_flat_run_it_has_seen() {
        // 这里是一个纯函数面的读数：测试进程里这个静态量只被本用例写。
        let before = window_rounds();
        record_flat_run_before_improvement(3);
        assert!(window_rounds() >= 3);
        record_flat_run_before_improvement(1);
        assert!(window_rounds() >= 3, "短平局不把窗口缩回去");
        record_flat_run_before_improvement(0);
        assert!(window_rounds() >= 3, "0 不是一个样本");
        assert!(window_rounds() >= before);
    }
}
