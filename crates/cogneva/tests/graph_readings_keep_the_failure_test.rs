//! principle: feedback_a_reading_folded_into_an_empty_value_answers_for_a_read_it_never_made
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

/// 锚点之后到第一个顶格收尾的花括号为止——这个仓里这些函数都在顶格收尾。
fn body_after<'a>(production: &'a str, anchor: &str) -> &'a str {
    let rest = production
        .split_once(anchor)
        .unwrap_or_else(|| panic!("{anchor} 不在了：这一面对应的引脚要跟着改"))
        .1;
    rest.split("\n}\n").next().unwrap_or(rest)
}
