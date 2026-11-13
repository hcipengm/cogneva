//! 待办这一整条链：工具把清单写进答案 → 总线把答案原样带回来 → 界面把多次重写折
//! 成一块面板。
//!
//! 这条链断在哪儿，屏幕上的样子都不一样，但没有一个看起来像坏了：
//!
//! 1. 答案里没有整张清单（只回一句「已记录」）——界面就无从画起，只能照旧把一次
//!    工具调用原文摆出来；
//! 2. 答案不是结构化的（拼成一段给人读的文本）——界面只能猜，猜错的项画成什么
//!    状态都不报错；
//! 3. 界面把每次 `todo` 调用都画成一行——同一张单子被抄了五遍，最新的那份要自己找；
//! 4. 完成的项画得和待办的一样——「还剩几步」这句话就读不出来了。
//!
//! 1 与 2 由 `tools.rs` 钉住，3 与 4 由前端两个文件钉住（WebUI 没有 JS 测试跑道，
//! 这一族门都是读源码的；见 `web_workbench_shows_people_venues_tasks_test.rs`）。

use crate::common::routes::read_repo;

const TOOLS: &str = "crates/cog-agent/src/tools.rs";
const EVENT: &str = "crates/cog-core/src/types/event.rs";
const PANEL: &str = "web/src/conversation/TodoPanel.tsx";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const STORE: &str = "web/src/store/streamStore.ts";

/// 工具回答的是整张清单与三个计数，不是一句「已记录」。
#[test]
fn the_tool_answers_with_the_list_it_wrote() {
    let tools = read_repo(TOOLS);
    assert!(
        tools.contains(r#""items": parsed"#),
        "{TOOLS} 的 todo 不把整张清单回给调用者：界面拿不到当下这一版，只能显示一次\
         调用原文"
    );
    assert!(
        tools.contains(r#""pending": counts("pending")"#),
        "{TOOLS} 的 todo 不回报计数：面板上的「还剩几步」得自己数，而数漏的那一项\
         不会有任何报错"
    );
    assert!(
        tools.contains(r#"matches!(status, "pending" | "in_progress" | "done")"#),
        "{TOOLS} 不再校验这三种状态：清单里会出现面板不认识的字"
    );
}

/// 结构化是这一整条链的前提：答案以 JSON 值的形式上总线，而不是一段文本。
#[test]
fn the_answer_travels_as_a_value_not_as_text() {
    let event = read_repo(EVENT);
    assert!(
        event.contains("result: serde_json::Value") || event.contains("result: serde_json::Value,"),
        "{EVENT} 的 `tool_execution_end` 不再带结构化的结果：清单到了前端已经是一段\
         字符串，面板只能按自己的想象去解析"
    );
    let store = read_repo(STORE);
    assert!(
        store.contains("result: string"),
        "{STORE} 的工具记录这一格应当保持「原文」语义：改它之前先想清楚面板要从哪里\
         拿到结构"
    );
}

/// 同一位写者的多次调用折成一块面板；调用本身不再各占时间线一行。
#[test]
fn many_updates_of_one_list_become_one_panel() {
    let panel = read_repo(PANEL);
    assert!(
        panel.contains("export function todoListsIn"),
        "{PANEL} 里没有把调用折成清单的那一步：每一次 `todo` 都会各画一块"
    );
    assert!(
        panel.contains("call.name !== TODO_TOOL"),
        "{PANEL} 不认哪些调用是待办"
    );
    assert!(
        panel.contains("const byWriter = new Map<string, ToolCallRecord[]>()"),
        "{PANEL} 不按写者分组：服务端按调用者分账的几张单子会在这里合成一张、互相覆盖"
    );
    assert!(
        panel.contains("if (call.status !== 'ok') continue;"),
        "{PANEL} 把一次还在跑或者失败了的写入也当成已落地：画出来的是模型想写的那一版，\
         不是服务端手上的那一版"
    );

    let pane = read_repo(PANE);
    assert!(
        pane.contains("todoListsIn(tools)"),
        "{PANE} 没把这一段调过的工具交给面板"
    );
    assert!(
        pane.contains("if (call.name === 'todo') continue;"),
        "{PANE} 仍然把每次 `todo` 调用画进时间线：同一份清单在流里被抄了好几遍"
    );
    assert!(pane.contains("<TodoPanel"), "{PANE} 上没有面板的落点");
    // 读不出来的那一版要留一句字：一块凭空消失的面板与「它没写过待办」在屏幕上
    // 是同一件事。
    assert!(
        panel.contains("这一版读不全"),
        "{PANEL} 在原文被截断时悄悄不画：读者会以为模型根本没写过待办"
    );
}

/// 一块面板里，三种状态画得不一样；完成的项划掉而不是消失。
#[test]
fn a_done_item_is_drawn_done() {
    let panel = read_repo(PANEL);
    for glyph in ["'✓'", "'▸'", "'·'"] {
        assert!(
            panel.contains(glyph),
            "{PANEL} 里没有 {glyph} 这个记号：三种状态在屏幕上分不出来"
        );
    }
    assert!(
        panel.contains("line-through"),
        "{PANEL} 不把完成的项划掉：做完的与没做的长得一样，「还剩几步」读不出来"
    );
    // 认不出来的状态要原字写出来，而不是折进某一支画成「还没开始」。
    assert!(
        panel.contains("return { glyph: '?', color: 'var(--warn)', note: status || '没有状态' };"),
        "{PANEL} 把没登记过的状态折进了别的分支：服务端哪天加一种，屏幕上会把它画成\
         一种正常状态"
    );
}

/// 面板贴在输入框上方，不跟着历史滚走。
#[test]
fn the_panel_stays_put_while_the_history_scrolls() {
    let pane = read_repo(PANE);
    let at = pane
        .find("{todoLists.length > 0 && (")
        .expect("对话面上没有待办面板这一块");
    let block = &pane[at..];
    let end = block.find("</div>").unwrap_or(block.len());
    assert!(
        block[..end.min(200)].contains("shrink-0"),
        "{PANE} 的待办面板没有固定高度约束：它会被卷进滚动区，翻历史时跟着走"
    );
}
