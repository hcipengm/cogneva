//! 晋升侧 A/B 那一行，派发器会不会开工。
//!
//! 2026-11-02 实测：`cogneva_task_held_unstated_rows{kind="promotion_eval"}` 连续两小时非零，
//! 图里那条 `promotion-eval-change-ledger-…` 自 11-01 起停在 pending，验收判据长度 0。写它的
//! 地方没带判据，而派发器只开「说了怎样算完」的行（`ready_queue::is_dispatchable_work`），
//! 于是这次晋升 A/B 从来没跑过——它的结局读作 `Unavailable`，与「这道门根本没问」是同一格。
//!
//! 这条测试把生产者自己造出来的那一行送到派发器的判据前面，而不是在旁边另写一份「判据
//! 非空」：判据在 `ready_queue` 里，那边改了这条才跟着改。

use std::path::Path;

use cog_orchestrator::ready_queue::is_dispatchable_work;
use cog_reflection::promotion_eval::promotion_eval_task;

/// 一条待晋升变更的 A/B 活包，按生产代码的样子造出来。
fn filed_row() -> cog_core::Task {
    promotion_eval_task(
        "change-ledger-abc",
        "deadbeef",
        Path::new("/workspace/cogneva"),
        900,
    )
}

#[test]
fn a_promotion_eval_row_is_work_the_dispatcher_will_start() {
    let task = filed_row();
    assert!(
        is_dispatchable_work(&task),
        "按现在的样子交出去，这一行会因为缺判据被留住：A/B 永远不开工，而这道门会一直读成 \
         Unavailable。acceptance={:?}",
        task.acceptance
    );
}

/// 判据要说的是**这一条变更**怎样算完，不是一句放之四海皆准的话：一句通用的话能用，
/// 但它在账上答不出「这条活包在证明哪件事」，而账本来就是按变更读的。
#[test]
fn the_criterion_names_the_change_it_belongs_to() {
    let task = filed_row();
    assert!(
        task.acceptance
            .iter()
            .any(|criterion| criterion.contains("change-ledger-abc")),
        "判据没有点名这条变更：{:?}",
        task.acceptance
    );
}

/// 反例：把判据摘掉，同一行就正好是派发器要留住的那一种。少了这一条，一个对什么都回答
/// 「能派发」的判据也能让上面那条通过。
#[test]
fn the_same_row_without_its_criterion_is_held() {
    let mut task = filed_row();
    task.acceptance.clear();
    assert!(
        !is_dispatchable_work(&task),
        "没有判据的行被判成可派发：那这一棵树里没有任何一行会被留住"
    );
}
