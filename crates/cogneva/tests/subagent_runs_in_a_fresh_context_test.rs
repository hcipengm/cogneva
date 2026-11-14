//! 子智能体：一条独立上下文里跑完一件活，结果以这一次调用的结果回到父会话。
//!
//! 与另外两条路分开：`send_to_agent` 是给平级同事发消息（对方是另一个人），
//! `start_task` 是往共享任务图上挂活（那一行从此属于所有人）。子任务是**这一次
//! 调用的内部分解**：子任务只看见交给它的那一句，父会话的历史一条也不进去，
//! 它读了几十万 token 的中间过程不会挤占父会话的窗口。
//!
//! 这里钉四段：
//!
//! 1. **工具在**：`spawn_subagent` 是内置工具，参数是那一句话本身；
//! 2. **隔离**：交给子任务的是 `vec![Message::user(task)]`，不是父会话的历史；
//! 3. **一条消息回来**：子任务的最终产出就是这次工具调用的结果，不再转述一层；
//! 4. **深度是一**：子任务的工具表是在装上它自己**之前**复制的快照——子任务手上
//!    没有 `spawn_subagent`。没有这一条，一层写错的递归能把整台机器的预算吃光，
//!    而失败的那一侧只会看见「很慢」。
//!
//! 行为那一半在 `cog_agent::tools::builtins::tests` 的
//! `a_subagent_sees_only_its_task_and_answers_once` 与
//! `a_subagent_without_a_task_is_refused_before_anything_starts`；这里钉接线。

use crate::common::routes::read_repo;

const TOOLS: &str = "crates/cog-agent/src/tools.rs";
const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";

/// 工具这一侧：起一条新循环、只给它那一句话、把它的答案当结果交回去。
#[test]
fn the_child_runs_in_a_fresh_context_and_its_answer_is_the_tool_result() {
    let tools = read_repo(TOOLS);
    for (needle, why) in [
        ("pub fn spawn_subagent(", "没有子智能体这只工具"),
        (
            "name: \"spawn_subagent\"",
            "工具的线上名字与规格里的名字对不上",
        ),
        (
            "run_conversation(vec![cog_core::Message::user(task)], llm.as_ref())",
            "交给子任务的不只是那一句话：父会话的历史进了它的上下文，隔离就没了",
        ),
        (
            "\"subagent\": true",
            "结果里读不出这是子任务的产出，父会话分不清它与自己这一轮",
        ),
    ] {
        assert!(tools.contains(needle), "{TOOLS} 缺 {needle:?}：{why}");
    }
    // 子任务的事件这一侧不读，但必须有人排空：循环往一条没人读的管道里写，
    // 写满之后会停在那里，而屏幕上只会显示「很慢」。
    assert!(
        tools.contains("received.recv().await.is_some()"),
        "{TOOLS} 没有排空子任务的事件管道：它会在写满之后停住"
    );
}

/// 装配这一侧：快照先复制、工具后装进父表——父子深度就是一。
#[test]
fn the_child_gets_the_parent_table_as_it_stood_before_the_spawn_tool_went_in() {
    let assembly = read_repo(ASSEMBLY);
    let fork_at = assembly
        .find("let child_tools = tools.fork();")
        .expect("没有给子任务复制工具表：它拿到的会是父表的实时那把，子任务自己也在里面");
    let register_at = assembly
        .find("builtins::spawn_subagent(")
        .expect("没有把子智能体装进这一轮的工具表");
    assert!(
        fork_at < register_at,
        "{ASSEMBLY} 先装工具再复制：子任务手上也有 `spawn_subagent`，父子深度不再是一"
    );
    // 权限跟着父这一轮走：子任务动手之前问的是同一个人、同一段对话的规则。
    let registration = &assembly[register_at..];
    let head = registration.split("));").next().expect("登记那一段还在");
    assert!(
        head.contains("decider.clone()") && head.contains("permission_mode"),
        "{ASSEMBLY} 没把判定者与模式交给子任务：一次分解就绕开了人的决定"
    );
    // 判定者必须先于工具表建好（工具要在 `with_tools` 之前进表）。
    let decider_at = assembly
        .find("let decider: Option<Arc<dyn cog_agent::permission::PermissionDecider>> =")
        .expect("判定者的构造改了形状");
    assert!(
        decider_at < register_at,
        "{ASSEMBLY} 判定者在子智能体之后才建：那条路上只能自己造一个「不问」的判定者"
    );
}
