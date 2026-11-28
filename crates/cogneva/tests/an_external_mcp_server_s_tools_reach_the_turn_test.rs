//! 挂一个外部 MCP 服务，它的工具在这一轮里调得到；连不上时明说。
//!
//! 这条链上曾经断在最后一米：客户端按端点建好了、REST 面读得到它，而它报出的
//! 工具从来没有进过任何一张 `ToolRegistry`——模型能听懂「有一个外部服务」，却
//! 一只也调不到。这里钉的就是那一米：
//!
//! 1. **装配**：`PlatformConversation` 拿着客户端，每轮问一次工具表并装进这一轮
//!    的工具表；
//! 2. **命名**：外部工具带 `mcp__` 前缀，盖不掉平台自己的同名工具，调用转回上游
//!    原名；
//! 3. **读数**：挂没挂、连上没有、问到几只，随交付物回网关并发一帧；
//! 4. **页面**：那一帧画成一行，连不上时带原因。
//!
//! 转换与调用行为在 `cogneva::mcp_tools::tests`（4 条：列出即注册且转回原名、
//! 名字不盖平台工具、连不上没有工具且有原因、没 schema 也能无参调用）。

use crate::common::routes::read_repo;

const ADAPTER: &str = "crates/cogneva/src/mcp_tools.rs";
const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const BOOT: &str = "crates/cogneva/src/lib.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const WEB_EVENTS: &str = "web/src/types/events.ts";
const WEB_CHAT: &str = "web/src/store/chatStore.ts";
const WEB_PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 装配这一侧：客户端进了构造，每轮问一次，工具装进这一轮的表。
#[test]
fn the_turn_asks_the_server_and_registers_what_it_offers() {
    let assembly = read_repo(ASSEMBLY);
    assert!(
        assembly.contains("mcp: Option<Arc<dyn cog_core::McpClient>>"),
        "{ASSEMBLY} 不持有 MCP 客户端：这一轮没有地方问「它有哪些工具」"
    );
    assert!(
        assembly.contains("crate::mcp_tools::tools("),
        "{ASSEMBLY} 不把外部的工具转成注册表认得的那一只：客户端有了，工具还是没有"
    );
    assert!(
        assembly.contains("tools.register(tool.clone())"),
        "{ASSEMBLY} 没把外部工具装进这一轮的表：它只被列出来，调不到"
    );
    assert!(
        assembly.contains("object.insert(\"mcp\".to_string(), report)"),
        "{ASSEMBLY} 不把连没连上、问到几只带回去：界面上只剩「调不到」一个结论"
    );

    let boot = read_repo(BOOT);
    assert!(
        boot.contains("ctx.consume_service::<dyn cog_core::McpClient>()"),
        "{BOOT} 不从进程里拿客户端：协议插件发布了它，装配这一侧却要一个空的"
    );
}

/// 命名：前缀让两只同名工具在一张表里都活着。
#[test]
fn an_external_tool_cannot_take_a_platform_tool_s_name() {
    let adapter = read_repo(ADAPTER);
    assert!(
        adapter.contains("pub const MCP_TOOL_PREFIX: &str = \"mcp__\""),
        "{ADAPTER} 的外部工具不带前缀：一只叫 read_file 的外部工具会盖掉平台那只"
    );
    assert!(
        adapter.contains("client.call_tool(&name, arguments)"),
        "{ADAPTER} 的处理器不转回上游：装进来的是一只调不通的工具"
    );
    assert!(
        adapter.contains("registered_name(&remote)"),
        "{ADAPTER} 注册时用上游原名：前缀只写在常量里，没有真的用上"
    );
}

/// 读数：交付物带回结果，网关发一帧，页面画成一行。
#[test]
fn what_the_server_answered_is_reported_to_the_page() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("async fn emit_turn_mcp("),
        "{CHAT} 没有发这一帧：连不上时页面上什么都没有"
    );
    assert!(
        chat.contains("\"message.mcp\""),
        "{CHAT} 的帧名与页面认的名字对不上"
    );
    let run = chat
        .split_once("async fn run_chat_turn")
        .expect("run_chat_turn 改了名字")
        .1;
    assert!(
        run.contains("emit_turn_mcp("),
        "{CHAT} 定义了发帧的函数却没有调用"
    );

    for (file, needle, why) in [
        (WEB_EVENTS, "'message.mcp': 'mcp'", "聊天帧表里没有这一帧"),
        (WEB_CHAT, "case 'mcp':", "消息上不记这一格，那一行画不出来"),
        (WEB_PANE, "message.mcp", "外部服务那一行没有画出来"),
        (
            WEB_PANE,
            "连不上",
            "断开时没有一句人话：读的人分不出没挂与断了",
        ),
    ] {
        assert!(
            read_repo(file).contains(needle),
            "{file} 缺 {needle:?}：{why}"
        );
    }
}
