//! 对话面那三件事各是一条走到底的链，不是四个各写一半的零件。
//!
//! 1. **停止**：按钮 → `chat_cancel`（按会话号）→ 网关按会话找到正在跑的轮并放下
//!    那一轮的 future → 回 `message.cancelled` 再回 `message.end` → 界面把这一轮
//!    画成「已中止」。任何一环缺失，屏幕上都只是一个按下去不动的按钮：没有上行，
//!    服务端那一边继续生成；没有回帧，界面永远是「正在说」。
//! 2. **同时开几段**：拨号条 ↔ store 的多会话，以及那段名单存在浏览器里。没有界面
//!    入口的那份 store 不是一个功能——它从来没被点过，也就从来没被验证过。
//! 3. **点进账**：`message.goal` / `message.change` 帧带着身份，对话面把它交给
//!    `focusRecord`，账那一面按身份落位。名字对不上不会报错，只会静默地什么都不
//!    发生，所以这里逐字钉住接口。
//!
//! 判据读的是两侧的源文件：前端没有测试运行器，Rust 这一侧的门是这套界面唯一
//! 会红的契约（同 `chat_user_echo_is_rendered_once_test.rs`）。

use crate::common::routes::read_repo;

const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const DOCK: &str = "web/src/conversation/SessionDock.tsx";
const STORE: &str = "web/src/store/chatStore.ts";
const EVENTS: &str = "web/src/types/events.ts";
const PEOPLE: &str = "web/src/views/PeopleView.tsx";
const PROJECTS: &str = "web/src/views/ProjectsView.tsx";
const CHANGES: &str = "web/src/views/ChangesView.tsx";
const SHELL: &str = "web/src/shell/store.ts";

#[test]
fn a_stop_travels_from_the_button_to_the_turn_and_back() {
    let protocol = read_repo(PROTOCOL);
    assert!(
        protocol.contains("#[serde(rename = \"chat_cancel\")]") && protocol.contains("ChatCancel"),
        "{PROTOCOL} 里没有 `chat_cancel`：界面发出去的那条上行消息在网关上不存在"
    );

    let pane = read_repo(PANE);
    assert!(
        pane.contains("type: 'chat_cancel'"),
        "{PANE} 没有发 `chat_cancel` 的地方：停止按钮不发东西，服务端那一轮继续跑"
    );

    let chat = read_repo(CHAT);
    assert!(
        chat.contains("message.cancelled"),
        "{CHAT} 不回「停了」这一帧：界面分不出「被停掉」与「一个字都没说」"
    );
    assert!(
        chat.contains("run_until_stopped"),
        "{CHAT} 不再把这一轮跑到「跑完或者被叫停」为止：停止只剩一个结果没人要"
    );

    let events = read_repo(EVENTS);
    assert!(
        events.contains("'message.cancelled': 'cancelled'"),
        "{EVENTS} 认不出 `message.cancelled`：这一帧会在解析那一步被整条丢掉"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("case 'cancelled':") && store.contains("已中止"),
        "{STORE} 收到停止之后没有把这一轮记成「已中止」"
    );
}

/// Esc 是「■ 停止」的第二个入口，不是第二个停止实现。
///
/// 判据分三件：这个键确实挂上了（`keydown` + `Escape`），它只在有轮在跑时挂
/// （否则空白对话里一按 Esc 就发一条不该有的上行），以及它走的是同一个发点——
/// 全文件只有一处 `chat_cancel`。第三件是这一条的主旨：两个入口各写一份发送逻辑
/// 时，停止的语义（按会话、不本地拦截）要记两遍，改一处就有一处漏。
#[test]
fn a_stop_also_comes_from_the_escape_key() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("addEventListener('keydown'") && pane.contains("event.key !== 'Escape'"),
        "{PANE} 没有挂在 Esc 上的停止：按钮能用，手在键盘上的人停不下来"
    );
    assert!(
        pane.contains("if (!streaming) return;"),
        "{PANE} 的 Esc 监听不是在「有轮在跑」时才挂的：空白对话里按一下也会发停止"
    );
    assert_eq!(
        pane.matches("type: 'chat_cancel'").count(),
        1,
        "{PANE} 里有多处发 `chat_cancel` 的地方：Esc 与按钮各写了一份停止语义，\
         迟早只改一处"
    );
}

/// 一轮花掉的量，从循环量出来到画在屏幕上是一条链：循环挂在交付物上 → 网关
/// 发 `message.usage` → 客户端认得这个名字 → store 记在说那句话的那一条上 →
/// 对话面画一行、会话头画上下文占用。任何一环断掉，屏幕上都是「这一轮花了多少
/// 不知道」，而它看起来与「这一轮没花什么」一模一样。
#[test]
fn what_a_turn_spent_is_read_off_the_loop_and_drawn_under_the_turn() {
    let agent = read_repo("crates/cog-agent/src/agent_kernel/runtime.rs");
    assert!(
        agent.contains("\"usage\"") && agent.contains("\"context_tokens\""),
        "循环不再把这一轮的用量挂到交付物上：后面每一环都无从读起"
    );

    let chat = read_repo(CHAT);
    assert!(
        chat.contains("\"message.usage\"") && chat.contains("emit_turn_usage"),
        "{CHAT} 不再发 `message.usage`：界面收不到这一轮花了多少"
    );
    assert!(
        chat.contains(".get(\"usage\")"),
        "{CHAT} 没去交付物里读用量：那一帧即便发了，装的也不是循环量出来的数"
    );

    let events = read_repo(EVENTS);
    assert!(
        events.contains("'message.usage': 'usage'"),
        "{EVENTS} 认不出 `message.usage`：这一帧会在解析那一步被整条丢掉"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("case 'usage'"),
        "{STORE} 不把用量记在任何一条消息上：屏幕上那一行没有落点"
    );

    let pane = read_repo(PANE);
    assert!(
        pane.contains("usageLine(message.usage)"),
        "{PANE} 不在这一条消息下面画花销：服务端量到了、屏幕上没有"
    );
    assert!(
        pane.contains("上下文 {compactCount(contextTokens)}"),
        "{PANE} 的会话头不显示上下文占用：背着多重的历史读不出来"
    );
}

#[test]
fn several_conversations_can_be_open_at_once_and_survive_a_reload() {
    let dock = read_repo(DOCK);
    assert!(
        dock.contains("createSession") && dock.contains("closeSession"),
        "{DOCK} 不再是「开一段、切一段、关一段」的那条拨号条：多开的那几段没有入口"
    );
    assert!(
        dock.contains("chat_cancel"),
        "{DOCK} 关掉一段之前不先停：那一格消失，服务端那一边还在生成、还在记账"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("cogneva.chatSessions"),
        "{STORE} 不把开着的几段记在本机：刷新一次，多开的对话全回到默认那一段"
    );
    assert!(
        store.contains("setActive"),
        "{STORE} 没有切换当前那段的能力：几段同时开着只能看第一段"
    );

    let people = read_repo(PEOPLE);
    assert!(
        people.contains("<SessionDock"),
        "{PEOPLE} 不画拨号条：那几段开在 store 里，屏幕上只有一段"
    );
}

#[test]
fn what_the_conversation_touches_opens_the_row_it_names() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("message.goal") && chat.contains("message.change"),
        "{CHAT} 不再为派出去的目标与翻开过的变更发身份帧：对话里只剩正文里的几个字"
    );

    let events = read_repo(EVENTS);
    assert!(
        events.contains("'message.goal': 'goal'") && events.contains("'message.change': 'change'"),
        "{EVENTS} 认不出这两帧：它们会在解析那一步被丢掉，点进去无从谈起"
    );
    assert!(
        events.contains("changeId"),
        "{EVENTS} 的聊天帧不带变更身份：`message.change` 那一帧没有落点"
    );

    let pane = read_repo(PANE);
    assert!(
        pane.contains("focusRecord('goal'") && pane.contains("focusRecord('change'"),
        "{PANE} 里的目标与变更不是入口：它们只是几行字，点不进去"
    );

    let shell = read_repo(SHELL);
    assert!(
        shell.contains("focusRecord"),
        "{SHELL} 没有「打开账上这一行」的请求：上面那几处点了也没有落点"
    );

    let projects = read_repo(PROJECTS);
    assert!(
        projects.contains("useRecordFocus('goal'"),
        "{PROJECTS} 不认领目标落点：点进来仍然停在之前选中的那个项目上"
    );

    let changes = read_repo(CHANGES);
    assert!(
        changes.contains("useRecordFocus('change'"),
        "{CHANGES} 不认领变更落点：点进来仍然看不到那一行的来路"
    );
}
