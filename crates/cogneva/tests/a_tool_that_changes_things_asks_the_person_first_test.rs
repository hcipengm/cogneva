//! 会改东西的工具在动手之前问坐在屏幕前的人——这条链路上没有一处是「接上就行」的默认值。
//!
//! 三段各自都能单独跑绿，而它们中间任意一段被摘掉，屏幕上会安静地退回「不问」：
//!
//! - 内核那一侧（`PermissionDecider`）在没人答时拒绝；
//! - 网关那一侧把问句送出去、把答案带回来；
//! - **装配那一侧**把两者接起来：`request.permissions` 有没有被递给运行时，只在
//!   `conversation.rs` 那一行里。
//!
//! 第三段是这里判的。它坏起来不会编译失败：`with_permissions` 少调一次，内核照样跑，
//! 只是判词永远来自「没人可以问」那一支——而那一支是拒绝，于是界面上所有写盘与命令都
//! 变成一次没有解释的失败。这正是本仓反复记过的形状：装配根写 `None` 时，功能整体在，
//! 每次都说「这个进程没接到」。

use crate::common::{fsio, root};

const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const GATEWAY: &str = "crates/cog-gateway/src/chat.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";

fn source(path: &str) -> String {
    fsio::read(&root::repo_root().join(path))
}

#[test]
fn the_turn_hands_the_person_s_answer_to_the_loop_it_runs() {
    let text = source(ASSEMBLY);
    assert!(
        text.contains("request.permissions"),
        "装配层没有读 `request.permissions`：这条连接上的问答永远到不了内核"
    );
    assert!(
        text.contains(".with_permissions("),
        "运行时没有挂上判词来源：内核每一次执行前都会读成「没有人可以问」，于是拒绝"
    );
    // 模式也是这条链路上的一格：默认走 `default`，但客户端发来的那一格要能盖过它。
    assert!(
        text.contains("PermissionMode::parse"),
        "连接上报的模式没有被解析：`plan` 与 `bypass` 会退成同一格"
    );
}

#[test]
fn the_gateway_asks_over_the_connection_it_was_given() {
    let text = source(GATEWAY);
    assert!(
        text.contains("\"type\": \"permission_request\"")
            || text.contains("\"permission_request\""),
        "问句没有自己的帧名：界面无从分辨「要你点头」与一条普通消息"
    );
    assert!(
        text.contains("pub trait PermissionBridge"),
        "没有把「问谁」抽成可替换的一条：内核会直接依赖这条 socket"
    );
    assert!(
        text.contains("self.out.closed()"),
        "出站管道断了时这一问还在原地等：连接没了，答案不会再来，这一轮会一直挂着"
    );
}

#[test]
fn the_socket_takes_the_answer_back_to_the_question_that_asked() {
    let text = source(SOCKET);
    assert!(
        text.contains("PermissionResponse"),
        "上行没有答案这一族消息：人点了允许，这条连接上没有人接"
    );
    assert!(
        text.contains("PermissionMode"),
        "模式切换没有上行的入口：会话上的模式只能靠改代码"
    );
    assert!(
        text.contains("permissions.answer("),
        "答案没有被交回等待那一问"
    );
    assert!(
        text.contains("permissions.mode("),
        "这一轮没有读这一段对话当前的模式：每一轮都按缺省那一格问"
    );
}
