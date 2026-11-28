//! plan 模式的一整条链：只读调研 → 把计划递出去 → 人批准 → 同一个回合动手。
//!
//! 这条链任何一环缺失，屏幕上的样子都是「它一直在查，然后停了」：
//!
//! 1. plan 模式下写工具不问就拒（内核的闭集）；
//! 2. 唯一问人的那一句是把计划递上去——`exit_plan_mode`，只在 plan 模式下注册，
//!    别的模式里它没有可批准的东西；
//! 3. 计划正文原样送到界面，不被摘要的 240 字界截掉；
//! 4. 批准之后这一轮换成 default，后面每一步动手照旧问人；
//! 5. 拒绝则留在 plan，理由回到模型手里。
//!
//! 行为本身由 `cog_agent::permission` 与内核的单元测试钉住，这里钉的是两侧接得上。

use crate::common::routes::read_repo;

const PERMISSION: &str = "crates/cog-agent/src/permission.rs";
const RUNTIME: &str = "crates/cog-agent/src/agent_kernel/runtime.rs";
const CONVERSATION: &str = "crates/cogneva/src/conversation.rs";
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const TRANSPORT: &str = "web/src/transport/websocket.ts";

/// 计划模式里唯一问人的一句必须存在，且别的模式里不许放它过去。
#[test]
fn the_plan_gate_exists_and_belongs_to_one_mode_only() {
    let permission = read_repo(PERMISSION);
    assert!(
        permission.contains("pub const EXIT_PLAN_TOOL"),
        "{PERMISSION} 里没有那扇门：plan 模式下模型连「请你批准」都说不出口"
    );
    assert!(
        permission.contains("PermissionMode::Plan => None"),
        "{PERMISSION} 在 plan 模式下不把批准这一问交给人：它会被当成写工具直接拒绝"
    );
}

/// 批准换的是**模式**，不是给这一个工具开后门。
#[test]
fn an_approval_switches_the_turn_instead_of_waving_one_tool_through() {
    let runtime = read_repo(RUNTIME);
    assert!(
        runtime.contains("tc.name == crate::permission::EXIT_PLAN_TOOL"),
        "{RUNTIME} 不认「批准计划」那一次调用：批完了这一轮还停在只读上"
    );
    assert!(
        runtime.contains("self.permission_mode = crate::permission::PermissionMode::Default;"),
        "{RUNTIME} 批准之后没有换模式：下一步照样撞在只读墙上"
    );
}

/// 计划正文要原样到界面，而不是塞进一行 240 字的摘要。
#[test]
fn the_plan_reaches_the_page_whole() {
    let conversation = read_repo(CONVERSATION);
    assert!(
        conversation.contains("EXIT_PLAN_TOOL") && conversation.contains(".then(||"),
        "{CONVERSATION} 不把计划正文递出去：界面上只剩一行摘要"
    );
    assert!(
        conversation.contains("builtins::exit_plan_mode()"),
        "{CONVERSATION} 不注册那只工具：plan 模式下模型没有出口"
    );

    let protocol = read_repo(PROTOCOL);
    assert!(
        protocol.contains("plan: Option<String>"),
        "{PROTOCOL} 的询问帧里没有计划这一格"
    );

    let transport = read_repo(TRANSPORT);
    assert!(
        transport.contains("plan?: string"),
        "{TRANSPORT} 不读计划那一格：正文到了这一层就被丢掉"
    );
}

/// 界面把「批准」与「继续改」画成两个不同的答案，而不是复用「允许 / 拒绝」。
#[test]
fn the_page_asks_for_approval_and_offers_to_keep_planning() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("ask.plan"),
        "{PANE} 不认这一问是「计划待批准」：它会把一份计划画成一次文件写入"
    );
    assert!(
        pane.contains("批准并执行"),
        "{PANE} 上没有批准计划的落点：人读完了却没法放行"
    );
    assert!(
        pane.contains("继续只读调研"),
        "{PANE} 上「不批准」不给模型下一步的话：它会当作一次失败重试同一个调用"
    );
}
