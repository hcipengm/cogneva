//! 界面上那段对话不是一次补全，而是系统唯一的那条智能体循环上的一轮。
//!
//! 属主 2026-11-04 定的能力口径：它要能像操作者手边的编程智能体一样工作——先看清
//! 事实，再拆、再派、再盯、再回报——但它**自己不干活**，活交给编排器分解出的任务，
//! 由 squad 执行。两半都要，缺一半都不是这个能力：
//!
//! - 只答文字（没有工具）：它只能凭印象说「进展怎么样」，而平台的账它一个字都读
//!   不到。这就是 2026-11-04 之前的样子。
//! - 给它执行工具：界面上就多出一条执行路径，它做的事不进任务图、不进账、不经过
//!   任何门，而屏幕上看起来与编排器派出去的活一模一样——那正是「旁路」。
//! - **在网关里为它另写一条循环**：哪怕那条循环只读台账、只派活，它也是同一件事的
//!   另一种形状。工具守卫、预算、token 记账、总线事件、清点都只在系统那条循环里
//!   发生一次；第二条循环做的事上面这些一样都不过。
//!
//! 所以这里判三件事：网关里没有循环，只有交接；控制面工具装在那条唯一的循环上；
//! 句柄在插件起来前发布、起来后填上——空着的时候界面读到的是一句「还没接上」，
//! 不是一条自作主张的旁路。工具集本身是读还是执行，由装配层的单元测试按闭集判。

use crate::common::{fsio, root};

const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const BOOT: &str = "crates/cogneva/src/lib.rs";

fn source(path: &str) -> String {
    fsio::read(&root::repo_root().join(path))
}

#[test]
fn the_gateway_hands_the_turn_over_instead_of_running_one() {
    let text = source(CHAT);

    // 交接点在，而且只有这一个入口：跑到这里说明这一轮真的交给了那条循环。
    assert!(
        text.contains("state.conversation.get()"),
        "网关不再从装配层填的句柄里取循环：它要么没有循环可用，要么自己另起了一条"
    );
    assert!(
        text.contains(".turn("),
        "这一轮没有交给 `ConversationTurn`：界面上的对话不在系统那条循环上跑"
    );

    // 网关里不该再有循环的形状。两条路径并存时，屏幕上分不出这一句是哪条跑出来的，
    // 而只有一条会过守卫、记账、清点。
    for shape in [
        "TurnScope",
        "TOOL_ROUNDS",
        "round_options.tools",
        "Message::ToolResult",
        "for round in 0..",
    ] {
        assert!(
            !text.contains(shape),
            "网关里还有 `{shape}`：这就是第二条执行路径。循环只有一条，在智能体内核里"
        );
    }
}

#[test]
fn the_control_plane_tools_ride_on_the_system_s_own_loop() {
    let text = source(ASSEMBLY);

    assert!(
        text.contains("cog_agent::AgentRuntime::new"),
        "装配层没有用系统的那条循环：它得自己拼一条，而那正是旁路"
    );
    assert!(
        text.contains("run_conversation"),
        "装配层取到循环后没有跑对话那一轮：工具与历史都不会经过守卫与记账"
    );
    assert!(
        text.contains("with_tools(tools)"),
        "工具集没有装到循环上：定义了却从不传出去的工具集与没有工具是同一种能力"
    );

    for name in [
        "dispatch_goal",
        "list_tasks",
        "get_task",
        "retry_task",
        "cancel_task",
    ] {
        assert!(
            text.contains(&format!("\"{name}\"")),
            "缺 {name}：这段对话读得到系统或改得动系统的能力缺了一半"
        );
    }

    // 身份与归属：按人记账，并且记在界面那一格上，而不是混进 agent 池。
    assert!(
        text.contains("with_call_identity"),
        "这一轮没有声明调用身份：界面花的钱会落在没有人的那一格"
    );
    assert!(
        text.contains("with_actor_label"),
        "这一轮没有钉 actor：按角色名记会把界面花的钱混进 agent 池那一族"
    );
}

#[test]
fn the_slot_is_published_empty_before_plugins_start_and_filled_after() {
    let text = source(BOOT);

    let publish = text
        .find("ctx.publish(conversation.clone())")
        .expect("装配层没有把会话句柄发布出去：网关取不到，界面永远报「还没接上」");
    let init = text
        .find("plugin_runner.init_all(&ctx)")
        .expect("找不到插件初始化点，这条判据失去锚");
    let fill = text
        .find("PlatformConversation::new(")
        .expect("装配层没有在插件起来后把循环填进句柄：界面永远报「还没接上」");

    assert!(
        publish < init,
        "句柄必须在插件起来前发布：网关在 `init_all` 里就要取它"
    );
    assert!(
        init < fill,
        "填句柄必须在插件起来后：它要的编排器由插件在 `init` 里发布"
    );
}

#[test]
fn the_one_tool_that_changes_the_system_is_dispatch() {
    let text = source(ASSEMBLY);
    assert!(
        text.contains("\"dispatch_goal\""),
        "没有派活工具：这段对话看得见系统的账，却改不动它——用户说「去做」时它只能回答"
    );
    // 派活那一条必须点名目标号是唯一权威：模型引用的 id 只能来自工具返回。
    assert!(
        text.contains("照抄它"),
        "派活的返回里没有说「id 照抄」，模型就会在回复里自己编一个目标号"
    );
}
