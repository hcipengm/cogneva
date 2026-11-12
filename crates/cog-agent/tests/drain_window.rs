//! 排空闸的两条读数判据，跑在**自己的进程**里。
//!
//! 这两条读的是进程级的「在飞」计数，而 `cog-agent` 的单元测试套件里别的用例
//! 也会调 `enter_run`（`run_scoped` 一进闸就走这条）。同一个进程里，断言「守卫
//! 落下，计数回到进闸之前」读到的是别人的一进一出：2026-11-12 的预推送门上实测
//! `left: 4, right: 3`——被测代码一字未改，红的是并行邻居。
//!
//! 集成测试各是一个进程，`reset_for_tests()` 在这里是安全的（它的文档写明只许
//! 在进程里没有别的运行在飞时调用），于是「一进一出」的差可以照实断言。
//!
//! 但**同一个测试二进制里的两条用例仍然并行**，用的又是同一个计数器：一条开局
//! `reset_for_tests()` 把计数清零时，另一条手里的守卫还在飞，于是它读到「排空
//! 完成」。2026-11-12 整门上实测红在这一格（`drain_window.rs:26`），被测代码一字
//! 未改——搬进程只隔离了二进制，没隔离二进制里的两条用例。所以这里串行：两条都
//! 先拿 [`async_test_lock`]，谁拿着计数器谁说话。

use std::time::Duration;

use cog_agent::drain::{
    clear_signal_for_tests, enter_run, in_flight, install, requested, reset_for_tests, wait_drained,
};
use cog_core::contract::shutdown::ShutdownSignal;
use cog_core::progress::async_test_lock;

#[tokio::test]
async fn a_run_in_flight_keeps_the_drain_waiting_until_it_ends() {
    let _serial = async_test_lock().await;
    reset_for_tests();
    let before = in_flight();
    assert_eq!(before, 0, "新进程里没有别的运行在飞");

    let guard = enter_run();
    assert_eq!(in_flight(), 1, "守卫活着就是在飞的那一个");
    assert!(
        !wait_drained(Duration::from_millis(120)).await,
        "有运行没结束时，排空不能读成已完成"
    );

    drop(guard);
    assert_eq!(in_flight(), 0, "守卫落下，计数回到进闸之前");
    assert!(
        wait_drained(Duration::from_millis(120)).await,
        "守卫落下之后排空完成"
    );
}

#[tokio::test]
async fn no_signal_installed_means_never_asked_to_stop() {
    let _serial = async_test_lock().await;
    reset_for_tests();
    assert!(
        !requested(),
        "an ordinary embedded run is never told to stop"
    );
    let signal = ShutdownSignal::new();
    install(signal.clone());
    assert!(!requested());
    signal.trigger();
    assert!(requested(), "the installed signal is what the gate reads");
    clear_signal_for_tests();
}
