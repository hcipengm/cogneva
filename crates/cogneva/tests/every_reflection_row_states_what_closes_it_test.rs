//! 反思侧把活打包成一行交给主流程的每一处，那一行都要过派发器的判据。
//!
//! `ready_queue::is_dispatchable_work` 只开工「说了怎样算完」的行（`acceptance` 非空），
//! 而 `Task::new` 写的是空列表。生产者少写一句判据的后果不是报错，是那一行安静地停在
//! `Pending` 上：派发器从不开工，也没有哪一趟会重看它——安全的那半边，同时也是
//! 「这道门从来没跑过」的那半边。2026-11-02 晋升侧 A/B 那一行就是这样停在图上二十小时，
//! 而它的结局读作 `Unavailable`，与「这道门根本没被问过」逐字同形。
//!
//! 这里把每个生产者自己造出来的那一行直接送到派发器前面，不在旁边另写一份「判据非空」：
//! 判据在 `ready_queue` 里，那边改了这些用例才跟着改。每一族都配一条反例（把判据摘掉，
//! 同一行正好是派发器要留住的那一种），否则一个对什么都回答「能派发」的判据也能全绿。

use cog_orchestrator::ready_queue::is_dispatchable_work;
use cog_reflection::baseline_port::port_rework_task;
use cog_reflection::plugin::discovery_probe_task;
use cog_reflection::promotion_eval::promotion_eval_task;
use cog_reflection::types::{DiscoveryStatus, DiscoveryStrategy, DiscoveryTask};

/// 一条发现探针：工具组合策略，带一条判据。
fn probe(criteria: &[&str]) -> DiscoveryTask {
    DiscoveryTask {
        id: "disc-0001".into(),
        explored_key: "tool:a+b".into(),
        hypothesis: "把 a 与 b 串起来能一次答完这一类问题".into(),
        strategy: DiscoveryStrategy::ToolCombination,
        tools: vec!["a".into(), "b".into()],
        prompt_variants: vec!["direct".into()],
        evaluation_criteria: criteria.iter().map(|c| (*c).to_string()).collect(),
        status: DiscoveryStatus::Pending,
        created_at: chrono::Utc::now(),
    }
}

#[test]
fn a_discovery_probe_is_work_the_dispatcher_will_start() {
    let (_, task) = discovery_probe_task(&probe(&["两次调用足够，不要第三次"]));
    assert!(
        is_dispatchable_work(&task),
        "按现在的样子交出去，这条探针会因为缺判据被留住：发现那一环永远收不到结局，\
         而它在账上与「这一轮什么都没生成」同形。acceptance={:?}",
        task.acceptance
    );
}

/// 探针的判据来自它自己的 `evaluation_criteria`：一句「任务完成」对每条探针都成立，
/// 账上却答不出这一条在验证哪件事。
#[test]
fn a_probe_criterion_carries_the_criteria_the_probe_was_generated_with() {
    let (_, task) = discovery_probe_task(&probe(&["两次调用足够", "不留半截答案"]));
    let written = task.acceptance.join(" ");
    assert!(
        written.contains("两次调用足够") && written.contains("不留半截答案"),
        "判据没有带上探针自己的评价标准：{:?}",
        task.acceptance
    );
}

/// 不带评价标准的探针是一种合法形状（只问「这个组合能不能用」），不是缺一项：
/// 它照样要说清怎样算完，而不是因此退回成一条没有判据的行。
#[test]
fn a_probe_without_criteria_still_states_what_closes_it() {
    let (_, task) = discovery_probe_task(&probe(&[]));
    assert!(
        is_dispatchable_work(&task),
        "没有评价标准的探针被留住了：这一类探针从此一条都跑不起来。acceptance={:?}",
        task.acceptance
    );
    assert!(
        task.acceptance.iter().any(|c| c.contains("tool:a+b")),
        "判据没有点名这条探针在试哪个组合：{:?}",
        task.acceptance
    );
}

#[test]
fn a_probe_without_its_criterion_is_held() {
    let (_, mut task) = discovery_probe_task(&probe(&["两次调用足够"]));
    task.acceptance.clear();
    assert!(
        !is_dispatchable_work(&task),
        "没有判据的探针被判成可派发：那这一棵树里没有任何一行会被留住"
    );
}

#[test]
fn a_port_rework_row_is_work_the_dispatcher_will_start() {
    let (_, task) = port_rework_task(
        "change-ledger-abc",
        "v1.2.3",
        "把 X 做出来",
        "三轮自动移植都编译不过",
    );
    assert!(
        is_dispatchable_work(&task),
        "按现在的样子交出去，这份回流单会因为缺判据被留住：移植最后一条兜底路径不跑，\
         一份移植不过的变更既没有移植、也没有人重新实现。acceptance={:?}",
        task.acceptance
    );
}

/// 回流单的判据点名这条变更与新基线：它答的是「这条变更在新基线上怎样算完」，
/// 不是「这条任务跑完了」。
#[test]
fn the_rework_criterion_names_the_change_and_the_baseline() {
    let (_, task) = port_rework_task("change-ledger-abc", "v1.2.3", "把 X 做出来", "编译不过");
    let written = task.acceptance.join(" ");
    assert!(
        written.contains("change-ledger-abc") && written.contains("v1.2.3"),
        "判据没有点名变更与基线：{:?}",
        task.acceptance
    );
}

#[test]
fn a_port_rework_row_without_its_criterion_is_held() {
    let (_, mut task) = port_rework_task("change-ledger-abc", "v1.2.3", "把 X 做出来", "编译不过");
    task.acceptance.clear();
    assert!(
        !is_dispatchable_work(&task),
        "没有判据的回流单被判成可派发：那这一棵树里没有任何一行会被留住"
    );
}

/// 晋升侧 A/B 那一行与上面两条走同一条判据。它 2026-11-02 之前正是缺这一句。
#[test]
fn a_promotion_eval_row_is_work_the_dispatcher_will_start() {
    let task = promotion_eval_task(
        "change-ledger-abc",
        "deadbeef",
        std::path::Path::new("/workspace/cogneva"),
        900,
    );
    assert!(
        is_dispatchable_work(&task),
        "晋升 A/B 那一行会缺判据被留住：acceptance={:?}",
        task.acceptance
    );
}
