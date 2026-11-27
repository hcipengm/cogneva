//! 内核 futex 那一类等待，`/proc` 里点不出持锁者——这不是没做，是内核不记这个名字。
//! 能做的第二条读数是：这个进程自己有几条线程、其中几条还在跑。有线程在跑，等的是
//! 内部工作；一条都不跑，才是所有线程都睡在一把锁上。两种处置不同，而「在等锁」
//! 一格分不出来。
//!
//! 没有这次改动这两条会红：`thread_census` 不存在、`describe` 在持锁者空时不带线程数。

use cog_core::progress::{classify_silence, thread_census, SilenceAction, SilenceCause};

#[test]
fn the_process_thread_census_is_readable_and_bounded() {
    let pid = std::process::id();
    let (threads, runnable) = thread_census(pid);
    assert!(
        threads >= 1,
        "读自己的进程应当至少有一条线程：({threads}, {runnable})"
    );
    assert!(
        runnable <= threads,
        "可运行线程数不可能多过线程数：({threads}, {runnable})"
    );
}

#[test]
fn a_futex_wait_with_no_nameable_holder_carries_the_thread_census() {
    let cause = SilenceCause {
        state: "State: S (sleeping)".into(),
        wchan: "futex_wait_queue".into(),
        action: classify_silence("State: S (sleeping)", "futex_wait_queue"),
        holders: Vec::new(),
        threads: 4,
        runnable_threads: 1,
    };
    assert_eq!(cause.action, SilenceAction::WaitOnLock);
    let described = cause.describe();
    assert!(
        described.contains("4 threads, 1 of them runnable"),
        "持锁者点不出名字时没有说这个进程自己的样子：{described}"
    );

    // 读不到线程数时不编：照旧只说「持锁者读不出」，不印一个 0。
    let unknown = SilenceCause {
        threads: 0,
        runnable_threads: 0,
        ..cause
    };
    assert!(!unknown.describe().contains("threads"));
}
