//! principle: feedback_absent_reading_fix_must_walk_to_its_input
//! Gate: 每个按任务图作答的面都走带报错的那条读，读失败留成错误而不是空答案。
//!
//! 这一族病灶同一个形状：处理函数的读口是折叠过的那条（`get_all_tasks()` 之类，
//! 失败时 `unwrap_or_default()`），于是库坏掉的时候，工作台画出一页零、总览画
//! 出一块空流程、依赖列表画成「这个任务没有依赖」、对外的 `ListTasks` 答「你有
//! 0 条任务」。这些画面与一个「真的什么都没有」的部署逐字同形，读的人无从分辨，
//! 而分辨正是这一面存在的理由。
//!
//! 修法在上游：读口旁挂一条带报错的那条（`get_all_tasks_checked`），处理函数
//! 把 `Err` 映射成 503。行为那一半在 `cog_gateway::tasks::tests` 与
//! `cog_gateway::progress::tests` 里各自钉着；这里钉的是**接线**——哪几个处理
//! 函数、走的是哪一条读。整个处理函数要一个完整的 `GatewayState` 才跑得起来，
//! 所以只能在源码这一面钉，而引脚落在函数体上而不是整个文件：同一个文件里只要
//! 还有一处走对了，文件级的正例就会替走错的那一处作证。
//!
//! 「产出面在哪里」由 `common::rust_production` 算，不在这里再切一次。

use crate::common::routes::read_repo;
use crate::common::rust_production::production_text;

const TASKS: &str = "crates/cog-gateway/src/tasks.rs";
const A2A: &str = "crates/cog-gateway/src/a2a.rs";
const TASK_LEDGER: &str = "crates/cog-gateway/src/task_ledger.rs";
const PROJECTS: &str = "crates/cog-gateway/src/projects.rs";
const PROJECT_CONTROL: &str = "crates/cog-gateway/src/project_control.rs";
const PROGRESS: &str = "crates/cog-gateway/src/progress.rs";
const PROCESSES: &str = "crates/cog-gateway/src/processes.rs";
const LIB: &str = "crates/cog-gateway/src/lib.rs";
const EXECUTOR: &str = "crates/cog-gateway/src/executor.rs";
const REBALANCER: &str = "crates/cog-supervisor/src/task_rebalancer.rs";
const DAG_EXECUTOR: &str = "crates/cog-orchestrator/src/dag_executor/orchestrator.rs";
const DAG_CONSUMER: &str = "crates/cog-orchestrator/src/dag_executor/mod.rs";
const ACTION_PLANNER: &str =
    "crates/cog-orchestrator/src/action_planner/action_plan_orchestrator.rs";
const SIGNAL_WATCHER: &str = "crates/cog-reflection/src/signal_watcher.rs";
const CHANGE_LEDGER: &str = "crates/cog-orchestrator/src/change_ledger.rs";
const DISCOVERY_LOOP: &str = "crates/cog-github/src/discovery_loop.rs";
const PROMOTION_EVAL: &str = "crates/cog-reflection/src/promotion_eval.rs";

/// 折叠过的那几条读。某一段正文里出现它们，就是那一面在替没读到的图作证。
const FOLDED: &[&str] = &[
    "get_all_tasks(",
    "get_ready_tasks(",
    "get_graph(",
    "get_dependents(",
    "get_dependencies(",
    "get_task(",
];

/// (文件, 函数锚点, 它该走的那条读, 折叠之后那一面会说什么)。
const SURFACES: &[(&str, &str, &str, &str)] = &[
    (
        TASKS,
        "pub async fn list_tasks_handler",
        "get_all_tasks_checked",
        "工作台画出一页零条任务",
    ),
    (
        TASKS,
        "pub async fn get_ready_tasks_handler",
        "get_ready_tasks_checked",
        "待办队列答成「没有可派的话」",
    ),
    (
        TASKS,
        "pub async fn task_summary_handler",
        "get_all_tasks_checked",
        "汇总把每一格都读成 0",
    ),
    (
        TASKS,
        "pub async fn purge_tasks_handler",
        "get_all_tasks_checked",
        "清理按一张空名单一样删不掉任何东西，还答成功",
    ),
    (
        TASKS,
        "pub async fn update_task_handler",
        "get_all_tasks_checked",
        "改一行答成「这行不存在」",
    ),
    (
        TASKS,
        "pub async fn save_task_plan_handler",
        "get_all_tasks_checked",
        "存计划答成「这行不存在」",
    ),
    (
        TASKS,
        "pub async fn move_task_handler",
        "get_all_tasks_checked",
        "挪一行答成「这行不存在」",
    ),
    (
        TASKS,
        "pub async fn get_task_dependents_handler",
        "get_dependents_checked",
        "依赖答成「这个任务谁也不等它」",
    ),
    (
        TASKS,
        "pub async fn get_task_dependencies_handler",
        "get_dependencies_checked",
        "前置答成「这个任务没有前置」",
    ),
    (
        TASKS,
        "pub async fn get_task_handler",
        "get_task_checked",
        "一条还在跑的活被答成「已经被删了」",
    ),
    (
        TASKS,
        "pub async fn get_task_graph_handler",
        "get_graph_checked",
        "任务图画成一张空图",
    ),
    (
        TASK_LEDGER,
        "pub async fn task_ledger_handler",
        "get_all_tasks_checked",
        "这一面账答成「在册 0 条、无事可做」",
    ),
    (
        A2A,
        "async fn rpc_tasks_get",
        "get_task_checked",
        "对端据 -32001 去换 id，而那一行可能还在",
    ),
    (
        A2A,
        "async fn rpc_list_tasks",
        "get_all_tasks_checked",
        "对端唯一的视野答成「你有 0 条任务」",
    ),
    (
        A2A,
        "async fn rpc_tasks_cancel",
        "get_task_checked",
        "对端据 -32001 去换 id，而那一行可能还在",
    ),
    (
        PROJECT_CONTROL,
        "async fn coverage_this_pass",
        "get_all_tasks_checked",
        "没量出来的那一轮盖出一份「0 行、覆盖 0 行」",
    ),
    (
        PROJECTS,
        "async fn visible_tasks",
        "get_all_tasks_checked",
        "整个项目面答成「一个项目都没有」，导出写成一张空表",
    ),
    (
        PROGRESS,
        "async fn merged_from",
        "get_all_tasks_checked",
        "进度页画成一块空板",
    ),
    (
        PROCESSES,
        "pub async fn process_overview_handler",
        "get_all_tasks_checked",
        "流程总览画成「还没有派过活」",
    ),
    (
        PROCESSES,
        "pub async fn process_overview_handler",
        "dlq_len_from_reading(",
        "死信队列画成一只干净的队列",
    ),
    (
        PROCESSES,
        "pub async fn process_overview_handler",
        "registry_from_reading(",
        "agent 一栏答成「一个 agent 都没注册」",
    ),
    (
        PROCESSES,
        "pub async fn process_overview_handler",
        "health_report_from_reading(",
        "健康四格一起落到 0，读成「四个都没病」",
    ),
    (
        LIB,
        "async fn supervisor_queue_stats_handler",
        "get_ready_tasks_checked",
        "待办队列答成「早就排空了」",
    ),
    (
        LIB,
        "async fn supervisor_queue_stats_handler",
        "dlq_len_from_reading(",
        "死信队列画成一只干净的队列",
    ),
    (
        LIB,
        "async fn supervisor_status_handler",
        "health_report_from_reading(",
        "健康四格一起落到 0，读成「四个都没病」",
    ),
    (
        LIB,
        "async fn task_timeline_handler",
        "get_task_checked",
        "一条还在跑的活的时间线被答成「已经被删了」",
    ),
    (
        EXECUTOR,
        "async fn owner_from_graph",
        "get_graph_checked",
        "交接边的「谁干的」一列留空，读成「没人经手」",
    ),
    (
        EXECUTOR,
        "pub fn spawn_collaboration_listener",
        "get_dependents_checked",
        "完成的任务没有交接边，读成「它没有下游」",
    ),
    (
        EXECUTOR,
        "pub fn spawn_collaboration_listener",
        "get_graph_checked",
        "一次重启没有留下任何被打断的边，读成「没打断任何在跑的活」",
    ),
    (
        REBALANCER,
        "pub async fn plan",
        "get_ready_tasks_checked",
        "待派队列答成「没有可派的任务」，每一趟调度都空转",
    ),
    (
        REBALANCER,
        "async fn count_running_tasks_for_agent",
        "get_all_tasks_checked",
        "每个 agent 都读成 0 条在跑，负载永远平衡，谁也不让谁",
    ),
    (
        REBALANCER,
        "pub async fn recovery_plan",
        "get_all_tasks_checked",
        "读失败让每个检查点都读成「任务已不在图里」，恢复计划悄悄空掉",
    ),
    (
        DAG_EXECUTOR,
        "pub async fn find_stalled_scheduled_tasks",
        "all_tasks_checked",
        "清扫读成「没有停住的行」，一整轮什么都没收也算完",
    ),
    (
        DAG_EXECUTOR,
        "async fn reclaim_one_stalled",
        "get_task_checked",
        "逐行那一读失败被读成「它已经走开了」，清扫静默跳过这一行",
    ),
    (
        DAG_EXECUTOR,
        "async fn on_task_terminal",
        "get_task_checked",
        "读不到被读成「这一行不在了」，目标行永远停在 Pending 而没人知道为什么",
    ),
    (
        DAG_CONSUMER,
        "async fn result_names_another_owner",
        "get_task_checked",
        "读不到被读成「不是别的单元的」，别人的结果被照常应用",
    ),
    (
        DAG_CONSUMER,
        "async fn orphan_reconcile_tick",
        "get_task_checked",
        "读不到被读成「任务已经不在了」，一条正在响的告警被收掉",
    ),
    (
        DAG_CONSUMER,
        "async fn wbs_verdict_no_longer_holds",
        "all_tasks_checked",
        "读不到被读成「没有行」，一条仍然成立的判词被撤掉",
    ),
    (
        ACTION_PLANNER,
        "pub async fn process_goal_impl",
        "get_task_checked",
        "读不到那一行被读成「还没计划过」，同一条目标每次重投都重付一次完整分解",
    ),
    (
        ACTION_PLANNER,
        "async fn stranded_placeholders",
        "get_task_checked",
        "读不到子任务被读成「占位没有挂上谁」，一条健康的行被删掉再改写 Failed",
    ),
    (
        ACTION_PLANNER,
        "async fn held_status",
        "get_task_checked",
        "日志把「读不到」与「没有这一行」印成同一个值",
    ),
    (
        ACTION_PLANNER,
        "async fn fail_submission",
        "held_status(",
        "判退那一行被顶掉时，日志把「读不到」与「没有这一行」印成同一个值",
    ),
    (
        SIGNAL_WATCHER,
        "async fn tick",
        "get_all_tasks_checked",
        "读不到图的一轮被读成一个没事的系统：没有失败、没有积压、没有待补的行",
    ),
    (
        SIGNAL_WATCHER,
        "async fn fill_signal_criteria",
        "get_task_checked",
        "读不到那一行被读成「这行不在了」，一条候选从回填里悄悄消失",
    ),
    (
        SIGNAL_WATCHER,
        "async fn fill_rework_criteria",
        "get_task_checked",
        "读不到那一行被读成「这行不在了」，一条回流行永远补不上判据",
    ),
    (
        SIGNAL_WATCHER,
        "async fn submit_intent",
        "get_task_checked",
        "读不到在手的行被读成「库里没有」，同一 id 每拍重投一次、每拍报重复",
    ),
    (
        CHANGE_LEDGER,
        "async fn dispatch_pass",
        "get_all_tasks_checked",
        "读不到图的一趟把每条欠账按 generation 0 再投一次，重复 id 每拍报错",
    ),
    (
        CHANGE_LEDGER,
        "async fn settle_dispatch_parents",
        "get_all_tasks_checked",
        "空图被读成「已经补完了」，一趟回填此后一辈子不再跑",
    ),
    (
        CHANGE_LEDGER,
        "async fn fill_dispatch_criteria",
        "get_task_checked",
        "读不到那一行被读成「这行不在了」，一条变更的判据永远空着",
    ),
    (
        CHANGE_LEDGER,
        "async fn owed_row",
        "get_task_checked",
        "读不到那一代被读成「没人占着」，欠账按一个可能还在库里的 id 投出去，每趟报重复",
    ),
    (
        CHANGE_LEDGER,
        "async fn fold_handover",
        "get_task_checked",
        "读不到那条交接被读成「从没开始」，一条可能已经在跑的任务被取消掉",
    ),
    (
        CHANGE_LEDGER,
        "async fn carried_batches",
        "get_all_tasks_checked",
        "读不到图被读成「一条批次都没有」，被批次带过的每条交付都读成「从没交付」，留着欠账、再交一次",
    ),
    (
        DISCOVERY_LOOP,
        "async fn intent_decision",
        "get_task_checked",
        "读不到被读成「没有这一行」，同一条意图照同一个 id 再交一次",
    ),
    (
        DISCOVERY_LOOP,
        "async fn run_assess_task",
        "get_task_checked",
        "读不到被读成「还没跑完」，等到超时再把「判定没回来」当成答案",
    ),
    (
        DISCOVERY_LOOP,
        "async fn poll_cross_validation",
        "get_task_checked",
        "读不到被读成「这一行不在了」，一次读失败静默地把这张 PR 的判词吞掉",
    ),
    (
        DISCOVERY_LOOP,
        "pub(crate) async fn settle_filed_criteria",
        "get_task_checked",
        "读不到那一行被读成「它不在了」，remaining 少算一条，回填以「补完了」退场",
    ),
    (
        PROMOTION_EVAL,
        "pub(crate) async fn withdraw_abandoned",
        "get_all_tasks_checked",
        "清扫读成「图上没有孤儿」，答「收走 0 条」，与真的没有孤儿逐字同形",
    ),
    (
        PROMOTION_EVAL,
        "pub(crate) async fn submit_and_wait",
        "get_task_checked",
        "提交前那一读读成「没有旧行挡着」，这一次提交被当重投静默跳过；等待里那一读读成「还在跑」，无上限地等下去",
    ),
];

#[test]
fn every_surface_that_draws_the_graph_keeps_the_failure() {
    let mut productions: std::collections::HashMap<&str, String> = Default::default();
    for file in [
        TASKS,
        A2A,
        TASK_LEDGER,
        PROJECTS,
        PROJECT_CONTROL,
        PROGRESS,
        PROCESSES,
        LIB,
        EXECUTOR,
        REBALANCER,
        DAG_EXECUTOR,
        DAG_CONSUMER,
        ACTION_PLANNER,
        SIGNAL_WATCHER,
        CHANGE_LEDGER,
        DISCOVERY_LOOP,
        PROMOTION_EVAL,
    ] {
        productions.insert(file, production_text(&read_repo(file)));
    }
    for (file, anchor, checked, folded_page) in SURFACES {
        let production = productions
            .get(file)
            .unwrap_or_else(|| panic!("{file} 没有读进来"));
        let body = body_after(production, anchor);
        assert!(
            body.contains(checked),
            "{file} 的 {anchor} 没有走 {checked}：读失败会{folded_page}"
        );
        for folded in FOLDED {
            assert!(
                !body.contains(folded),
                "{file} 的 {anchor} 换回了折叠的那条读（{folded}）：读失败会{folded_page}，\
                 与真的什么都没有逐字同形"
            );
        }
    }
}

/// 锚点那一段的正文：从函数名之后第一个 `{` 起，到括号回到零的那一行止。
///
/// 以前切在「第一个顶格收尾的花括号」上，理由是「这些函数都在顶格收尾」——住进
/// `impl` 里的方法不是，它们的收尾是四个空格。于是那一段一直量到 impl 结束，把同一
/// 个文件里后面所有方法都算进来：`impl DagExecutor` 里的三个引脚，量的是它自己加上
/// 后面几千行，`get_all_tasks(` 这样的**定义**也会触发「换回了折叠的那条读」
/// （2026-11-23 实测：改对了也红，理由是隔壁方法的签名）。括号按行数，字符串与注释
/// 里的括号由 `lex` 剔掉——`&'a str` 那个撇号也算对。
fn body_after<'a>(production: &'a str, anchor: &str) -> &'a str {
    use crate::common::lex::{scan, Dialect};
    let rest = production
        .split_once(anchor)
        .unwrap_or_else(|| panic!("{anchor} 不在了：这一面对应的引脚要跟着改"))
        .1;
    let body = &rest[rest
        .find('{')
        .unwrap_or_else(|| panic!("{anchor} 之后没有函数体"))..];
    let scan = scan(body, Dialect::Rust);
    let mut depth = 0i32;
    let mut end = body.len();
    let mut at = 0usize;
    for (line, shape) in body.split_inclusive('\n').zip(scan.lines.iter()) {
        depth += shape.delta;
        at += line.len();
        if depth == 0 {
            end = at;
            break;
        }
    }
    &body[..end]
}
