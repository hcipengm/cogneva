//! E24：一次代码改动在对话里以 diff 呈现，人能接受或拒绝；拒绝后改动不落盘。
//!
//! 这一面原本只有「要不要动手：edit_file」一行字，人点头时并不知道自己在批准哪几
//! 行——diff 的渲染件（`web/src/conversation/toolDiff.ts`）一直都有，但它只画在
//! 那次调用落地之后的流里，落在问句之前的那一步是盲的。
//!
//! 判的是四件事：
//!
//! 1. **问句带着改动**：改动类工具的那一问里有一份 diff，不是一句工具名；
//! 2. **两层同一个形状**：内核算、网关搬、页面画用的是同一个名字（
//!    `cog_core::contract::permission::PermissionDiff`），不是各写一份；
//! 3. **两个答案分得开**：接受放行这一次写工具，拒绝把理由交回模型；
//! 4. **拒绝之后树没变**：这一条的行为判据在
//!    `cog_agent::agent_kernel::runtime::tests::a_refused_change_leaves_every_byte_as_it_was`
//!    ——工具真的握着一次写盘的机会，拒绝之后文件里仍是原来那几个字节。这里钉的是
//!    界面与线路上的那一半。
//!
//! 规格里那一行 check 是 `grep web/src/conversation/ConversationPane.tsx :: diff_accept`：
//! 那个词就是下面这一对动作名（接受 / 拒绝），页面按它们把答案发回去。

use crate::common::routes::read_repo;
use cog_core::contract::permission::{PermissionDiff, PermissionDiffKind};
use cog_gateway::chat::{
    PermissionAnswer, PermissionAskView, PermissionBridge, PermissionChannel,
    SocketPermissionBridge,
};

fn a_change() -> PermissionDiff {
    PermissionDiff {
        path: "crates/x.rs".into(),
        kind: PermissionDiffKind::Edit,
        label: "这一处替换".into(),
        before: "let a = 1;".into(),
        after: "let a = 2;".into(),
        truncated: false,
    }
}

/// 问句带着那次改动，三层用的是同一个形状。
///
/// 这一条钉的是接线：内核折出它（`edit_preview`）、装配层把它填进那一问、网关把它
/// 原样搬上线、页面按它画。任一处少写一格，「这个工具动手」那一步就又变回盲的。
#[test]
fn the_question_carries_the_change_it_would_make() {
    let kernel = read_repo("crates/cog-agent/src/permission.rs");
    assert!(
        kernel.contains("pub fn edit_preview("),
        "内核不折那次改动了：问句里只剩工具名"
    );
    // 认的是工具名单里的那三只，不是按「看起来像写」猜的。
    assert!(
        kernel.contains("EDIT_TOOLS"),
        "改动类工具的名单要与「哪些工具要问」那张名单同源"
    );

    let shape = read_repo("crates/cog-core/src/contract/permission.rs");
    assert!(
        shape.contains("pub struct PermissionDiff"),
        "三层共用的形状要在 core 里只有一份"
    );

    let assembly = read_repo("crates/cogneva/src/conversation.rs");
    assert!(
        assembly.contains("cog_agent::permission::edit_preview(&ask.tool, &ask.arguments)"),
        "装配层不往问句里填那次改动：界面上收不到 diff"
    );

    let gateway = read_repo("crates/cog-gateway/src/chat.rs");
    assert!(
        gateway.contains("pub diff: Option<cog_core::contract::permission::PermissionDiff>"),
        "问句视图里没有这一格：改动到不了连接上"
    );
    assert!(
        gateway.contains("diff: view.diff.clone()"),
        "问句出门时不带改动：界面上收不到 diff"
    );
    let protocol = read_repo("crates/cog-gateway/src/websocket_protocol.rs");
    assert!(
        protocol.contains("diff: Option<cog_core::contract::permission::PermissionDiff>"),
        "线路上没有这一格"
    );

    let wire = read_repo("web/src/transport/websocket.ts");
    assert!(wire.contains("diff?: PermissionDiff | null"));
    let store = read_repo("web/src/store/permissionStore.ts");
    assert!(
        store.contains("diff?: PermissionDiff | null"),
        "等答案的那一页不存改动：画不出那份 diff"
    );
    let types = read_repo("web/src/types/events.ts");
    assert!(types.contains("export interface PermissionDiff"));
}

/// 页面上画的是改动本身，两个答案各自有名：接受放行，拒绝不动树。
#[test]
fn the_page_draws_the_change_and_names_the_two_answers() {
    let pane = read_repo("web/src/conversation/ConversationPane.tsx");
    assert!(
        pane.contains("diffForPreview(ask.diff)"),
        "问句有改动却不折它：弹窗里还是那行工具名"
    );
    assert!(pane.contains("<DiffBlock"), "弹窗里要画同一份 diff");
    assert!(
        pane.contains("'diff_accept'"),
        "接受这一步的动作名不在页面上（规格的 check 就找这个词）"
    );
    assert!(
        pane.contains("'diff_refuse'"),
        "拒绝这一步的动作名不在页面上"
    );
    assert!(
        pane.contains("拒绝这份改动：工作树一个字节都不动"),
        "拒绝按钮要说出后果：树不动"
    );
    let diff = read_repo("web/src/conversation/toolDiff.ts");
    assert!(
        diff.contains("export function diffForPreview("),
        "问句与流里要共用同一套画法：两套算法会画出两种笔迹"
    );
}

/// 两个答案回到那一问：接受是放行，拒绝是带理由的拒绝。
#[tokio::test]
async fn the_two_answers_go_back_to_the_question_that_asked() {
    let view = |request_id: &str| PermissionAskView {
        request_id: request_id.to_string(),
        session_id: "s-1".into(),
        tool: "edit_file".into(),
        summary: "path=crates/x.rs".into(),
        mode: "default".into(),
        plan: None,
        diff: Some(a_change()),
    };

    // 拒绝：理由交回模型，没有执行。
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(8);
    let channel = std::sync::Arc::new(PermissionChannel::new());
    let bridge = SocketPermissionBridge::new("s-1".into(), out_tx, std::sync::Arc::clone(&channel));
    let asking = tokio::spawn(async move { bridge.ask(view("perm-refuse")).await });
    let frame = out_rx.recv().await.expect("问句必须出门");
    let parsed: serde_json::Value = serde_json::from_str(&frame).expect("问句是 JSON");
    assert_eq!(parsed["type"], "permission_request");
    assert_eq!(parsed["diff"]["path"], "crates/x.rs");
    assert_eq!(parsed["diff"]["kind"], "edit");
    assert_eq!(parsed["diff"]["before"], "let a = 1;");
    assert_eq!(parsed["diff"]["after"], "let a = 2;");
    assert!(
        channel.answer(
            "perm-refuse",
            PermissionAnswer::Deny {
                reason: "这份改动先别落盘".into(),
                always: false,
            }
        ),
        "拒绝要落到正等着的那一问上"
    );
    match asking.await.expect("问的那一头不该崩") {
        PermissionAnswer::Deny { reason, .. } => {
            assert_eq!(
                reason, "这份改动先别落盘",
                "理由被吃掉了：模型收到的会是一句没有来由的失败"
            )
        }
        PermissionAnswer::Allow { .. } => panic!("人拒绝了，内核却拿到放行"),
    }

    // 接受：放行这一次写工具。
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(8);
    let channel = std::sync::Arc::new(PermissionChannel::new());
    let bridge = SocketPermissionBridge::new("s-1".into(), out_tx, std::sync::Arc::clone(&channel));
    let asking = tokio::spawn(async move { bridge.ask(view("perm-accept")).await });
    let frame = out_rx.recv().await.expect("问句必须出门");
    let parsed: serde_json::Value = serde_json::from_str(&frame).expect("问句是 JSON");
    assert_eq!(parsed["request_id"], "perm-accept");
    assert!(
        channel.answer("perm-accept", PermissionAnswer::allow()),
        "接受要落到正等着的那一问上"
    );
    assert!(
        matches!(
            asking.await.expect("问的那一头不该崩"),
            PermissionAnswer::Allow { always: false }
        ),
        "人接受了，内核却拿到拒绝"
    );
}
