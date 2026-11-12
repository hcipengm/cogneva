//! 排空闸：进程被要求停下时，让手上的运行走到**回合边界**再停，而不是从
//! 任意一个 await 点被掐断。
//!
//! 与 [`cog_core::attempt_journal`] 是一对，各管一头：
//!
//! - 日志保证「被硬杀时，下一次从最后一条有结果的调用之后接着跑」——至多丢
//!   一步，且丢的那一步可判定；
//! - 排空保证「被**通知**要停时，一步也不丢」——手上的工具批次跑完、结果落盘，
//!   然后不再向模型发起新一轮。停下来的位置就是日志里那一条有结果的调用之后，
//!   下一次派发原样接上，不重跑任何东西。
//!
//! 两件事都不能独立成立：只有日志、没有排空，每次滚动部署都会制造一次「不确定」
//! 的调用；只有排空、没有日志，SIGKILL 与 OOM 仍然把整段对话重买一遍。
//!
//! 闸的位置在**模型调用之前**，不在工具批次中间：在批次中间退出会留下一个带
//! 调用却没有结果的助手回合，那正是续跑要处置的「不确定」——排空存在的意义
//! 就是不去制造它。所以内核只在两个动作之间看一眼这道闸。
//!
//! 这不是幂等。幂等说的是「同一个操作做几次效果相同」；排空＋续跑说的是
//! 「每个动作最多发生一次，且下一次从最后一个已完成的动作之后接上」。写文件的
//! 工具之所以能重跑，靠的是工具自己的幂等；有外部效果的工具做不到，于是它们
//! 的策略是**不重放**。合起来的上限是「至多一次」，不是「恰好一次」。
//!
//! 它住在 `cog-agent` 而不是 `cog-core`：这把闸只在跑 agent 回合的那个 crate 里
//! 有意义——装信号、进闸、等归零的三处调用点全在这里，`cog-core` 的另一半是
//! 纯数据契约。放在底座里会让一个只被一个 crate 用到的「动作」留在那一层，而
//! 底座的规矩是**只放两个以上 crate 共享的东西**（`core_boundary_contract_test`
//! 判的就是这条）。
use cog_core::contract::shutdown::ShutdownSignal;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// 本进程的停机信号。装了才可能为真；没装（测试、内嵌运行）恒为假。
static SIGNAL: OnceLock<Mutex<Option<ShutdownSignal>>> = OnceLock::new();

/// 此刻正在跑的运行数。排空等的是它归零。
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

fn slot() -> &'static Mutex<Option<ShutdownSignal>> {
    SIGNAL.get_or_init(|| Mutex::new(None))
}

/// 把本进程的停机信号交给闸。重复调用以最后一次为准。
pub fn install(signal: ShutdownSignal) {
    *slot().lock().unwrap_or_else(|e| e.into_inner()) = Some(signal);
}

/// 是否已经被要求停下。
///
/// 没有装信号就是「没有」：一个不认识停机信号的嵌入进程，把运行掐断只会白扔
/// 手上已经做出来的东西。
pub fn requested() -> bool {
    slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(ShutdownSignal::is_triggered)
}

/// 一次正在跑的运行。落盘的对账（`leave`）在 `Drop` 里，panic 与提前返回都
/// 会把它减回去——否则一次失败的运行会让排空永远等不到零。
pub struct RunGuard;

impl Drop for RunGuard {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 记一次运行开始。返回的守卫活着期间它都算在飞的。
pub fn enter_run() -> RunGuard {
    IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    RunGuard
}

/// 此刻在飞的运行数。
pub fn in_flight() -> usize {
    IN_FLIGHT.load(Ordering::SeqCst)
}

/// 等「在飞数归零」时两次读数之间的间隔。
///
/// 它在飞的数量只在一头一尾各动一次，等的时长由调用方给的 `limit` 决定，这个
/// 间隔只决定收尾快多少。
// cap: bound — 一次收尾的等待，量的是「多久看一次」，不是「等多久」；等多久由调用方给
const DRAIN_POLL: Duration = Duration::from_millis(50);

/// 等到手上的运行都结束，最多等 `limit`。返回是否等到了零。
///
/// 轮询而不是条件变量：在飞的数量本来就只在一头一尾各动一次，而一个条件变量
/// 要多一套唤醒路径，多出来的东西比它省下的那点唤醒更贵。
pub async fn wait_drained(limit: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if in_flight() == 0 {
            return true;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        tokio::time::sleep(DRAIN_POLL.min(deadline - now)).await;
    }
}

/// 测试用：清掉信号与计数。
///
/// 只在**同一个进程里没有别的运行在飞**时调用：它把计数归零，而一个还活着的
/// 守卫在 `Drop` 里照样会减一次，那会把已经归零的计数器减成天文数字。集成测试
/// 在自己的进程里用它；进程内的单元测试要用 [`clear_signal_for_tests`]。
pub fn reset_for_tests() {
    clear_signal_for_tests();
    IN_FLIGHT.store(0, Ordering::SeqCst);
}

/// 测试用：只清信号，不动在飞计数。
pub fn clear_signal_for_tests() {
    *slot().lock().unwrap_or_else(|e| e.into_inner()) = None;
}

// 这两条读数判据（一进一出、装没装信号）在 `tests/drain_window.rs`：它们读的是
// 进程级的在飞计数，而本 crate 的单元测试里别的用例也会进闸，同一个进程里断言
// 只会读到别人的一进一出。那边各是一个进程。
