//! 打一个 `@`，列的是仓库里的文件；挑中的那条路径真的把文件带进这一轮。
//!
//! 两段合起来才算「提及」：界面上打得进去（列表、键盘挑选），以及服务端真的读了
//! 那份文件。只做前半段，`@src/main.rs` 在模型那边只是一串字；只做后半段，没人
//! 知道路径该怎么打。
//!
//! 这一面没有前端测试跑道（`web/package.json` 里只有构建工具），界面那一半读源码；
//! 服务端那一半的真行为在 `cog-gateway` 的单元测试里（`chat::tests`），这里只钉
//! 「两截是同一件事」（同一个 `@` 形状、同一份路径、接在句子里）。

use crate::common::routes::read_repo;

const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";

/// 打 `@` 时列的是项目那棵树，不是用户上传目录：员工读的是同一份仓库。
#[test]
fn the_page_lists_the_project_tree_while_typing_an_at_name() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("mentionAt("),
        "{PANE} 不认 `@` 打头的那一段：补全没有起点"
    );
    assert!(
        pane.contains("getWorkspaceTree("),
        "{PANE} 不问服务端要目录：列表只能靠猜"
    );
    assert!(
        pane.contains("'project'"),
        "{PANE} 列的不是项目仓库：员工读的那份文件不会出现在列表里"
    );
    assert!(
        pane.contains("mentionCandidates.map"),
        "{PANE} 不把候选画出来：键盘挑不到看不见的东西"
    );
    assert!(
        pane.contains("MENTION_ROWS"),
        "{PANE} 不量列表高度：一屏外的候选把输入框挤出屏幕"
    );
    // 目录补到 `/` 上继续往下打，文件补一个空格收尾——没有空格，下一句会连成
    // 一条不存在的路径。
    assert!(
        pane.contains("mentionedPath("),
        "{PANE} 拼出来的不是一条能接着打的路径"
    );
}

/// 列表开着时，上下键与回车归列表：回车挑路径，不是把半句话发出去。
#[test]
fn the_keyboard_chooses_from_the_list_instead_of_sending() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("e.key === 'ArrowDown' || e.key === 'ArrowUp'"),
        "{PANE} 的上下键不在列表里走：只能拿鼠标点"
    );
    assert!(
        pane.contains("acceptMention(pick)"),
        "{PANE} 的回车不挑候选：这一下会把 `@src/ma` 当成一句话发出去"
    );
    assert!(pane.contains("e.key === 'Escape'"), "{PANE} 收不掉这份列表");
    assert!(
        pane.contains("e.nativeEvent.isComposing") && pane.contains("keyCode === 229"),
        "{PANE} 挑候选时没避开输入法选词的那一次回车"
    );
}

/// 服务端真的读了点名的那份文件，并把它排在正文之后。
#[test]
fn the_gateway_reads_what_the_sentence_named() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("&mentioned_paths(&content)"),
        "{CHAT} 不认这句话里点名的文件：路径在模型那边只是一串字"
    );
    assert!(
        chat.contains("user_message_with_mentions(&text, &mentions, &attachments)"),
        "{CHAT} 拼这一句时没把点名的那几份带进去"
    );
    assert!(
        chat.contains("cog_core::SandboxPayload::ReadFile"),
        "{CHAT} 不是从员工读文件的同一个后端读的：写进去的和读回来的不是一份字节"
    );
}
