//! 共享任务列表在对话里看得见，而且与任务图同源。
//!
//! 从前的形状是「账本共享、对话内无面板」：智能体在这一轮挂上任务图一行，人要看
//! 「谁在做、做到哪」，得离开这段对话去工作台。两处读的本来就是同一张表
//! （`OrchestratorControl::get_all_tasks`），没有第二个出处可言。
//!
//! 与 E8 的单轮待办分开：那一份是「这一轮自己要做的几步」，这一份是「这一段对话
//! 挂上了任务图上的哪几行」。两样东西的寿命不同，画在一起会让读的人以为它们一起
//! 结束。
//!
//! 这里钉四段：
//!
//! 1. **观测点**：这一轮跑过的 `start_task` 在 `after_tool_call` 上交出来，装配层
//!    在那里认它——工具自己不知道「这一段对话」这个概念；
//! 2. **只写那一刻已确定的事**：哪一行、什么活、派给谁、属于哪个 goal。状态不抄
//!    进对话——那是任务图随时间的读数，抄一份就成了永远停在入队的副本；
//! 3. **同一张表**：列表拿 id 去 `/api/v1/tasks/list?ids=` 读回，与工作台同一个
//!    `get_all_tasks`，不是另一份账；
//! 4. **页面上画成一片**，叠上总线上已经到过的状态变迁。
//!
//! 行为那一半在 `cog_gateway::tasks::tests::an_id_filter_keeps_exactly_the_rows_it_names`；
//! 这里钉接线。

use crate::common::routes::read_repo;

const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const TASKS: &str = "crates/cog-gateway/src/tasks.rs";
const WEB_EVENTS: &str = "web/src/types/events.ts";
const WEB_CHAT: &str = "web/src/store/chatStore.ts";
const WEB_PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 装配这一侧：这一轮挂上图的活被认出来，随交付物回去。
#[test]
fn the_turn_collects_the_work_it_filed_on_the_shared_graph() {
    let assembly = read_repo(ASSEMBLY);
    for (needle, why) in [
        ("if tool == \"start_task\"", "观测点不认派活那只工具"),
        ("fn task_list(", "没有把回执折成列表那一行要的事实"),
        (
            "\"tasks\".to_string()",
            "不把这一轮派过的活带回去：界面拿不到这一格",
        ),
        (
            "let title = arguments",
            "没有从 goal 取第一行当活名：整段说明挤不进列表的一行",
        ),
    ] {
        assert!(assembly.contains(needle), "{ASSEMBLY} 缺 {needle:?}：{why}");
    }
    // 状态不抄进对话：那一格只该由任务图那一次读回提供。
    let helper = assembly
        .split("fn task_list(")
        .nth(1)
        .expect("task_list 改了形状");
    let body = helper.split("\n}\n").next().unwrap_or(helper);
    assert!(
        !body.contains("\"status\""),
        "{ASSEMBLY} 把派活那一刻的状态抄进了对话：任务图从此有两份状态，过期的那份永远不会变"
    );
}

/// 网关：一帧带整份列表，排在收尾帧之前。
#[test]
fn the_whole_list_is_one_frame_before_the_turn_closes() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("async fn emit_turn_tasks("),
        "{CHAT} 没有发这一帧：派了活之后页面上什么都没有"
    );
    assert!(
        chat.contains("\"message.tasks\""),
        "{CHAT} 的帧名与页面认的名字对不上"
    );
    let run = chat
        .split_once("async fn run_chat_turn")
        .expect("run_chat_turn 改了名字")
        .1;
    let emit_at = run
        .find("emit_turn_tasks(")
        .expect("定义了发帧的函数却没有调用");
    // 最后一个才是收尾：前面几处是失败与被停那几条路各发一次，这一帧要排在它们
    // 之后、收尾之前，才算落在这一条还活着的时候。
    let end_at = run
        .rfind("emit_message_end(")
        .expect("收尾帧那一段改了形状");
    assert!(
        emit_at < end_at,
        "{CHAT} 在收尾帧之后才发这一帧：那一条已经定稿，这一行没有落点"
    );
}

/// 读面：同一张表，只读点名的几行。
#[test]
fn the_list_reads_the_same_graph_the_workbench_reads() {
    let tasks = read_repo(TASKS);
    for (needle, why) in [
        (
            "pub ids: Option<String>",
            "读口不收 id 一组：列表只能读整张表再自己截",
        ),
        ("fn in_ids(", "id 这一组没有判据"),
    ] {
        assert!(tasks.contains(needle), "{TASKS} 缺 {needle:?}：{why}");
    }
    // 同一张表：id 过滤挂在 `matches_query` 里，而它服务的还是 `get_all_tasks`。
    let filter = tasks
        .split("fn matches_query(")
        .nth(1)
        .expect("matches_query 改了形状");
    assert!(
        filter
            .split("\n}\n")
            .next()
            .unwrap_or(filter)
            .contains("in_ids("),
        "{TASKS} 的 id 过滤没有挂进读口的判据：`?ids=` 会被静默忽略"
    );
    assert!(
        tasks.contains("state.orchestrator.get_all_tasks().await"),
        "{TASKS} 换了一条读源：对话里的列表与工作台从此不是同一份账"
    );
}

/// 页面：帧表、消息上的那一格、以及画出来的那一片。
#[test]
fn the_page_draws_the_rows_and_their_live_state() {
    for (file, needle, why) in [
        (
            WEB_EVENTS,
            "'message.tasks': 'tasks'",
            "聊天帧表里没有这一帧",
        ),
        (
            WEB_CHAT,
            "case 'tasks':",
            "消息上不记这一格，那一片画不出来",
        ),
        (WEB_PANE, "TurnTasks(", "任务列表那一片没有画出来"),
        (
            WEB_PANE,
            "useStreamStore((s) => s.liveTasks)",
            "没有叠上总线上的状态变迁：列表会停在打开那一刻",
        ),
        (
            WEB_PANE,
            "状态没读到",
            "读不到与还没读到被写成同一句话：前者要人去查读口",
        ),
    ] {
        assert!(
            read_repo(file).contains(needle),
            "{file} 缺 {needle:?}：{why}"
        );
    }
    // 与 E8 的单轮待办是两片，不是一片。
    let pane = read_repo(WEB_PANE);
    assert!(
        pane.contains("TodoPanel") && pane.contains("TurnTasks("),
        "{WEB_PANE} 把这一轮的待办与任务图上的活并成了一片：两样东西的寿命不同"
    );
}
