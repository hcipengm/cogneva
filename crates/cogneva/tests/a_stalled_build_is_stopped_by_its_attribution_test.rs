//! 判停之后动不动手，由**归因**回答，不由时钟回答；动了手要留下「停住」这个结局。
//!
//! 一次构建没有第二个观察者：判停之后没有人会再看一眼，等下去就是永远等下去。而
//! 「等下去」与「收掉」之间不是一秒数，是它此刻停在哪：
//!
//! - 在等 I/O、在等一把锁、`/proc` 读不到：三种都会自己结束（或判不了），继续等；
//! - 热的、却没有对象可等：空转，等下去不会结束，收掉。
//!
//! 收掉之后结局必须是 `BuildEnding::Stalled`：它没有跑完、也没有结论，与「跑完了而
//! 结果不好」是两件事。这条门没有这次改动会红——`action_for` 不存在，而
//! `BuildEnding::Stalled` 在生产里没有生产者。

use cog_core::progress::{
    action_for, classify_silence, SilenceAction, SilenceCause, StallDecision,
};

fn cause(state: &str, wchan: &str) -> SilenceCause {
    SilenceCause {
        state: state.to_string(),
        wchan: wchan.to_string(),
        action: classify_silence(state, wchan),
        holders: Vec::new(),
        threads: 0,
        runnable_threads: 0,
    }
}

#[test]
fn the_three_cells_of_attribution_decide_the_action() {
    // 热的、没有可等的对象：收。
    let spin = cause("State: R (running)", "0");
    assert_eq!(spin.action, SilenceAction::Spin);
    assert_eq!(action_for(&spin), StallDecision::Kill);

    // 在等 I/O：继续等。
    let wait = cause("State: S (sleeping)", "pipe_read");
    assert_eq!(wait.action, SilenceAction::Wait);
    assert_eq!(action_for(&wait), StallDecision::KeepRunning);

    // 在等锁：继续等，并把持锁者点名（点名在 describe 里）。
    let lock = cause("State: S (sleeping)", "futex_wait");
    assert_eq!(lock.action, SilenceAction::WaitOnLock);
    assert_eq!(action_for(&lock), StallDecision::KeepRunning);

    // 读不到：判不了，不猜；三种落法里只有「收」不可逆。
    let unknown = cause("State: unreadable", "unreadable");
    assert_eq!(unknown.action, SilenceAction::Unknown);
    assert_eq!(action_for(&unknown), StallDecision::KeepRunning);
}

/// 生产里确实接上了：构建这一条单步动作把回答者交给看门狗，并把「被收掉」记成
/// `BuildEnding::Stalled`。两处少任何一处，上面那条判据就只是库里的一个函数。
#[test]
fn the_build_hands_the_watcher_a_decision_and_records_the_stall() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let deployer =
        std::fs::read_to_string(root.join("crates/cog-reflection/src/evolution_deployer.rs"))
            .expect("evolution_deployer.rs");
    assert!(
        deployer.contains("run_child_watched_with_pid_decided"),
        "构建仍然只归因不动手：判停之后没有第二个观察者，等下去就是永远等下去"
    );
    assert!(
        deployer.contains("BuildEnding::Stalled"),
        "被按归因收掉的构建没有它自己的结局：读的人会把它读成一次失败"
    );
    assert!(
        deployer.contains("stall.cause.describe()"),
        "收掉构建的那一刻没有把归因写进错误里：事后只剩「被判停」一条读数"
    );
}
