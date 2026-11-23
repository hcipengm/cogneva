//! MCP 工具被点了几次：本进程窗口内、按名字累计。
//!
//! 为什么要有这一格：能力格的使用一行把「有计数器」当成「量过」，把「没有计数器」
//! 当成「这一层没人记」。插件与 MCP 两层此前都落在后者——于是它们的有项被读成
//! 「不知道有没有人用」，而不是「量过，没人用」。两个结论要的动作不同：前者去接
//! 计数器，后者去看用的人为什么不来。
//!
//! 两层的答案不一样，所以只给一层装：
//!
//! - MCP 有真实派发点（网关的 `/api/v1/protocol/mcp/tools/call`），每次调用都经过
//!   这里，能数。
//! - 插件没有派发点：它的行是 [`crate::capabilities::Reach::DeclaredOnly`]，点了会
//!   落空。给一个没人走得到的路口装车流表，读数会永远是 0，而 0 在读数面上读作
//!   「量过，没人用」——那比不装更误导。它的「没有计数器」由 `reach` 那一栏说清，
//!   不靠这一格。
//!
//! 口径与内建那一层一致：进程内、累计、重启清零。跨进程与跨重启的持久读数是另一件
//! 事（工作台产出格那一格专门谈这个口径），不能拿这一格的数当成它的答案。

use std::collections::HashMap;
use std::sync::Mutex;

/// 一次派发记一格。锁中毒（别的线程 panic 在临界区里）时读数为空、写被丢掉：
/// 丢一次计数比让网关在计数上 panic 便宜，而「计数丢了」与「没人用」在读数上
/// 靠 `Some(0)` 与 `None` 分开——这里保证有计数器时至少答 `Some(0)`。
#[derive(Debug, Default)]
pub struct McpCallCounts {
    calls: Mutex<HashMap<String, u64>>,
}

impl McpCallCounts {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记一次派发。名字是工具在对端 `tools/list` 里报的那个。
    pub fn record(&self, tool: &str) {
        if let Ok(mut calls) = self.calls.lock() {
            *calls.entry(tool.to_string()).or_insert(0) += 1;
        }
    }

    /// 这个工具被点过几次。**有计数器**时没点过答 `Some(0)`；锁中毒答 `None`
    /// （这一刻量不到，不是量到 0）。
    pub fn count(&self, tool: &str) -> Option<u64> {
        self.calls
            .lock()
            .ok()
            .map(|calls| calls.get(tool).copied().unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_recorded_call_is_one_and_an_unrecorded_one_is_a_measured_zero() {
        let counts = McpCallCounts::new();
        assert_eq!(
            counts.count("search"),
            Some(0),
            "有计数器、没点过 = 量到的 0"
        );
        counts.record("search");
        counts.record("search");
        counts.record("fetch");
        assert_eq!(counts.count("search"), Some(2));
        assert_eq!(counts.count("fetch"), Some(1));
        assert_eq!(
            counts.count("never-called"),
            Some(0),
            "别的名字不影响这一行"
        );
    }
}
