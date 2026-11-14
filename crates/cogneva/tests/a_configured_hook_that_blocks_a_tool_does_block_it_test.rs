//! 属主配一条阻断钩子，那一轮的工具就真的被拒；配了的钩子在读数上看得见跑没跑。
//!
//! 这条链上曾经断在两处：配置表在（YAML 加载器、管理面 REST、变更目录），但
//! 聊天 runtime 从来没挂过它；而引擎的动作表只有通知，没有能拒绝一次调用的
//! 那一类。于是「配了钩子」在对话里的效果是**什么都没有**，且没有任何读数能
//! 告诉属主它跑过。
//!
//! 这里钉五段：
//!
//! 1. **装配**：装配层拿到的是平台那一份钩子表（智能体插件发布的服务），不是
//!    另抄一份；
//! 2. **判定发生在动手之前**：阻断与改写挂在 `before_tool_call` 上，工具没跑；
//! 3. **一个动作一个执行者**：发布引擎跳过判定型动作，同一次调用不会被拒两次；
//! 4. **读数**：配置清单与这一轮跑过的记录一起随交付物回网关，网关发一帧；
//! 5. **页面上**：「配了 N 条、这一轮一条都没跑」与「跑过哪几条」是两句不同的话。
//!
//! 行为那一半在 `cog_agent::hooks::guard::tests`（阻断、改写、模式、无范围不
//! 参与判定、报告取一次清空）与 `cog_agent::agent_kernel::runtime::tests`
//! （`a_blocking_hook_stops_the_call_before_it_runs`、
//! `a_rewriting_hook_changes_what_the_tool_and_the_bus_see`）；这里钉接线。

use crate::common::routes::read_repo;

const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const BOOT: &str = "crates/cogneva/src/lib.rs";
const GUARD: &str = "crates/cog-agent/src/hooks/guard.rs";
const ENGINE: &str = "crates/cog-agent/src/hooks/engine.rs";
const RUNTIME: &str = "crates/cog-agent/src/agent_kernel/runtime.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const WEB_EVENTS: &str = "web/src/types/events.ts";
const WEB_CHAT: &str = "web/src/store/chatStore.ts";
const WEB_PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 一块源码：从某个标记到下一次同缩进的 `}` 之后（够判据读一个函数体）。
fn block<'a>(source: &'a str, open: &str) -> &'a str {
    let Some((_, after)) = source.split_once(open) else {
        panic!("找不到 {open:?}：这一处改了名字或搬了家，判据要跟着改");
    };
    after
}

/// 装配：拿到的是平台那本钩子表，判定挂在这一轮的工具调用之前。
#[test]
fn the_assembly_reads_the_platform_hook_table_and_guards_the_turn() {
    let boot = read_repo(BOOT);
    assert!(
        boot.contains("ctx.consume_service::<dyn cog_core::HookEngine>()"),
        "{BOOT} 不从进程里拿钩子表：属主配的钩子到不了对话这一侧"
    );

    let assembly = read_repo(ASSEMBLY);
    assert!(
        assembly.contains("hooks: Option<Arc<dyn cog_core::HookEngine>>"),
        "{ASSEMBLY} 不持有钩子表：这一轮没有地方读「配了什么」"
    );
    assert!(
        assembly.contains("engine.list_hooks().await"),
        "{ASSEMBLY} 不读配置表：钩子写了也进不了这一轮"
    );
    assert!(
        assembly.contains("cog_agent::hooks::guard::before_tool_call("),
        "{ASSEMBLY} 不把判定规则挂到工具调用之前：阻断钩子只能看着调用发生"
    );
    let hooks_block = block(&assembly, "let hook_defs: Vec<cog_core::HookDef>");
    assert!(
        hooks_block.contains("before_tool_call"),
        "{ASSEMBLY} 读了配置却没挂 before_tool_call：这一轮一次判定都不会发生"
    );
    assert!(
        hooks_block.contains("HookTrigger::OnToolAfter"),
        "{ASSEMBLY} 不在工具结束后发事件：通知型钩子听不到工具前后"
    );
    assert!(
        hooks_block.contains("HookTrigger::OnAgentEnd"),
        "{ASSEMBLY} 不在这一轮结束时发事件：「停止」那个触发点永远不会响"
    );
}

/// 判定发生在动手之前：签名拿到可变参数，工具没跑就没有结果。
#[test]
fn the_guard_decides_before_the_tool_runs() {
    let runtime = read_repo(RUNTIME);
    assert!(
        runtime.contains("&mut serde_json::Value) -> SFResult<()>"),
        "{RUNTIME} 的 before_tool_call 不拿可变参数：改写只能改一份没人读的副本"
    );
    let loop_block = block(
        &runtime,
        "for (frame, tc) in tool_calls.iter_mut().enumerate()",
    );
    let before_start = loop_block
        .split("self.emit_event(AgentEvent::ToolExecutionStart")
        .next()
        .unwrap_or(loop_block);
    assert!(
        before_start.contains("before_tool_call"),
        "{RUNTIME} 先发开始帧再问钩子：总线上那个调用带着一份没跑过的参数"
    );
    assert!(
        before_start.contains("hook(&tc.name, &mut tc.arguments)"),
        "{RUNTIME} 问钩子时不把它换过的参数写回去：改写不生效"
    );

    let guard = read_repo(GUARD);
    assert!(
        guard.contains("GuardAction::Block"),
        "{GUARD} 没有阻断这一类动作：判定型钩子只剩改写"
    );
    assert!(
        guard.contains("*arguments = replacement.clone()"),
        "{GUARD} 改写不落到这次调用上：钩子说改了，工具收到的还是原来的"
    );
    // 没写 `tool:` 的判定钩子不参与：无差别拒绝会盖住每一只工具。
    assert!(
        guard.contains("let Some(pattern) = rule.tool.as_deref() else"),
        "{GUARD} 对没写范围的判定照做：一条写错配置的钩子会拒掉所有工具"
    );
}

/// 一个动作一个执行者：发布引擎跳过判定型动作。
#[test]
fn a_deciding_hook_is_not_published_as_a_notification() {
    let engine = read_repo(ENGINE);
    assert!(
        engine.contains(".filter(|h| !h.action.is_guard())"),
        "{ENGINE} 把判定型动作也当通知发布：同一次调用会被拒两次，读数里出现两条记录"
    );
}

/// 读数：配置与运行记录一起回到网关，网关发一帧。
#[test]
fn the_turn_reports_what_was_configured_and_what_ran() {
    let assembly = read_repo(ASSEMBLY);
    assert!(
        assembly.contains("\"hooks\".to_string()"),
        "{ASSEMBLY} 的交付物里没有这一格：配了钩子却没跑过，界面上看不出来"
    );
    assert!(
        assembly.contains("cog_agent::hooks::guard::report("),
        "{ASSEMBLY} 不把配置清单带回去：只报运行记录时，一条模式写错的钩子看起来像没配"
    );

    let chat = read_repo(CHAT);
    assert!(
        chat.contains("async fn emit_turn_hooks("),
        "{CHAT} 没有发这一帧：钩子跑没跑过到不了对话上"
    );
    assert!(
        chat.contains("\"message.hooks\""),
        "{CHAT} 的帧名与页面认的名字对不上"
    );
    let run = block(&chat, "async fn run_chat_turn");
    assert!(
        run.contains("emit_turn_hooks("),
        "{CHAT} 定义了发帧的函数却没有调用：这一帧从写下那天起不会出现"
    );
}

/// 页面上：跑过哪几条、以及「配了但一条都没跑」。
#[test]
fn the_page_tells_configured_apart_from_ran() {
    for (file, needle, why) in [
        (
            WEB_EVENTS,
            "'message.hooks': 'hooks'",
            "聊天帧表里没有这一帧，到了也落不进消息",
        ),
        (
            WEB_CHAT,
            "case 'hooks':",
            "消息上不记这一格，那一行永远画不出来",
        ),
        (WEB_PANE, "message.hooks", "配了哪些、跑过哪些没有画出来"),
        (
            WEB_PANE,
            "一条都没跑",
            "「配了但不生效」没有自己的句子：读的人以为没配",
        ),
    ] {
        assert!(
            read_repo(file).contains(needle),
            "{file} 缺 {needle:?}：{why}"
        );
    }
}
