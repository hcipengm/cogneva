//! E20：放到后台的那条命令跑完时，自己回到当初起它的那一段对话里说一声。
//!
//! 一个人把一条长命令放到后台，是为了继续说话，不是为了记得回来轮询——而它跑完
//! 的那一刻，这一轮对话里没有任何一步会再碰它：模型可能正在说别的话，也可能已经
//! 答完了。所以「跑完了」这件事得由命令自己说出来，说到当初把它放到后台的那一段
//! 对话上。
//!
//! 判的是四件看得见的事：
//!
//! 1. **恰好一条**：同一条命令只结束一次，完成消息也只有一条——把结局换成第二次
//!    的，屏幕上就会多出第二条，而它只跑完过一次；
//! 2. **跑完之前不说**：还在跑的命令没有完成通知，句柄在交出去的那一刻就能读；
//! 3. **说得出为什么**：没跑成的命令，通知里是原因与它最后印的那几行，不是一句
//!    「失败了」；
//! 4. **落回它自己那一段对话**：通知带的是起命令的那段会话号，装配层把它变成那
//!    一段上的一条消息加一张卡片；没有会话号的命令不替它猜一段。

use std::time::Duration;

use crate::common::routes::read_repo;
use cog_agent::background::{self, Ending, Finished};
use cog_core::CommandEvent;
use cog_gateway::chat::publish_background_finished;
use cog_gateway::chat_stream::ChatStream;
use tokio::sync::broadcast;

/// 收下属于 `handle` 的那几条通知，直到安静满 `quiet`。
///
/// 订阅是进程级的：同一条广播还会被这个进程里同时跑着的用例看见。按句柄认领，
/// 别的句柄原样放过——「这条命令说了几次」问的是它自己的次数。
async fn notices_for(
    mut rx: broadcast::Receiver<Finished>,
    handle: &str,
    quiet: Duration,
) -> Vec<Finished> {
    let mut seen = Vec::new();
    loop {
        match tokio::time::timeout(quiet, rx.recv()).await {
            Ok(Ok(done)) if done.notice["handle"] == handle => seen.push(done),
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => break,
        }
    }
    seen
}

/// 恰好一条：先到的结局算数，第二遍收尾不再说一次。
///
/// 这一条是「完成消息恰好一条」的判据本身：把 `finish` 里的「空槽才写」去掉，
/// 再收尾一次就会多播一条，两次说法的结局还不一样。
#[tokio::test]
async fn a_finished_command_says_so_exactly_once() {
    let rx = background::subscribe();
    let command = background::open();
    let handle = command.handle().to_string();

    // 句柄交给调用方的那一刻就能读，而那时它还在跑：没有完成通知。
    assert_eq!(
        background::get(&handle).expect("句柄立即可读").snapshot(0)["state"],
        "running"
    );
    command.push(CommandEvent::Stdout {
        data: "building\n".into(),
    });
    assert!(
        command.completion_notice().is_none(),
        "还在跑的命令不该有完成通知"
    );

    command.finish(Ending::Exited(0));
    command.finish(Ending::Failed("第二遍收尾".into()));

    let seen = notices_for(rx, &handle, Duration::from_millis(400)).await;
    assert_eq!(seen.len(), 1, "同一条命令只该说一次跑完了：{seen:?}");
    assert_eq!(
        command.ending(),
        Some(Ending::Exited(0)),
        "先到的结局算数，第二遍收尾不改变它"
    );
    let notice = &seen[0].notice;
    assert_eq!(notice["state"], "exited");
    assert_eq!(notice["exit_code"], 0);
    assert!(
        notice["summary"]
            .as_str()
            .unwrap_or_default()
            .contains(&handle),
        "摘要要点得出是哪条命令：{notice:?}"
    );
}

/// 没跑成的命令，通知里说得出为什么，以及它最后印的那几行。
#[tokio::test]
async fn a_command_that_failed_says_why() {
    let rx = background::subscribe();
    let command = background::open();
    let handle = command.handle().to_string();
    command.push(CommandEvent::Stderr {
        data: "error[E0308]: mismatched types\n".into(),
    });
    command.finish(Ending::Failed("退出码 101".into()));

    let seen = notices_for(rx, &handle, Duration::from_millis(400)).await;
    assert_eq!(seen.len(), 1, "跑完这件事只该说一次：{seen:?}");
    let notice = &seen[0].notice;
    assert_eq!(notice["state"], "failed");
    assert_eq!(notice["error"], "退出码 101");
    assert!(
        notice["summary"]
            .as_str()
            .unwrap_or_default()
            .contains("没能跑完"),
        "摘要要直说没跑成：{notice:?}"
    );
    assert!(
        notice["stderr_tail"]
            .as_str()
            .unwrap_or_default()
            .contains("E0308"),
        "失败时最后印的那几行要一起带出来：{notice:?}"
    );
    assert_eq!(notice["exit_code"], serde_json::Value::Null);
}

/// 命令记得当初是哪一段对话把它放到后台的；没人说过时就不替它猜。
#[tokio::test]
async fn the_command_remembers_which_conversation_asked() {
    let rx = background::subscribe();
    let command = cog_agent::tools::CALLER_SESSION
        .scope("s-20".to_string(), async { background::open() })
        .await;
    let handle = command.handle().to_string();
    command.finish(Ending::Exited(0));

    let seen = notices_for(rx, &handle, Duration::from_millis(400)).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].session.as_deref(),
        Some("s-20"),
        "通知要回到起这条命令的那一段对话"
    );

    // 不带会话号的运行（嵌入、或不在任何一段对话里）：`None`，而不是随便挑一段。
    let rx = background::subscribe();
    let unnamed = background::open();
    let handle = unnamed.handle().to_string();
    unnamed.finish(Ending::Exited(0));
    let seen = notices_for(rx, &handle, Duration::from_millis(400)).await;
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].session, None, "没人说过是哪一段，就不该替它猜");
}

/// 通知落进它自己那一段：一条 assistant 消息（正文就是那句摘要），外加一张同号的卡片。
///
/// 落成一条消息而不是另起一种角色：这是这一段里的执行者在回话（「你放到后台的那
/// 件事，我回来告诉你结果」），不是第三个人插进来的系统提示。消息号由句柄推出，
/// 重连拿到的快照里它就还在——不需要客户端再记一份「谁在跑」。
#[tokio::test]
async fn the_notice_lands_in_its_own_conversation() {
    let out = ChatStream::new(16);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(64);
    out.watch("s-20", tx);

    let summary = "后台命令 cmd-abc 结束（用时 1.2 秒，输出 1 行）。用 command_output 读全文。";
    let notice = serde_json::json!({
        "handle": "cmd-abc",
        "state": "exited",
        "exit_code": 0,
        "error": serde_json::Value::Null,
        "elapsed_ms": 1200,
        "stdout_bytes": 9,
        "tail": "building\n",
        "tail_truncated": false,
        "stderr_tail": "",
        "summary": summary,
    });
    publish_background_finished(&out, "s-20", &notice).await;

    // 重连的人拿到的是「这一段现在说到哪儿」：那条消息带着摘要，已经收尾。
    let snapshot = out.snapshot("s-20");
    let said = snapshot
        .iter()
        .find(|message| message.message_id == "background-cmd-abc")
        .unwrap_or_else(|| panic!("快照里没有那条通知：{snapshot:?}"));
    assert_eq!(said.role, "assistant");
    assert_eq!(said.text, summary);
    assert!(said.ended, "通知是一句话，不是一段还在流的回答");
    assert!(
        out.snapshot("s-21").is_empty(),
        "通知只落在它自己那一段对话"
    );

    // 此刻在看的人先收到快照，再收到那一句的三帧和一张卡片。
    let mut kinds = Vec::new();
    let mut card = None;
    while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        let value: serde_json::Value = serde_json::from_str(&frame).expect("frame is json");
        if value["type"] != "agent_event" {
            continue;
        }
        let kind = value["payload"]["type"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if kind == "message.background" {
            card = Some(value["payload"]["payload"].clone());
        }
        kinds.push(kind);
    }
    assert_eq!(
        kinds,
        vec![
            "message.start",
            "message.text_delta",
            "message.end",
            "message.background"
        ],
        "一句消息三帧，再一张卡片"
    );
    let card = card.expect("卡片一帧");
    assert_eq!(card["message_id"], "background-cmd-abc");
    assert_eq!(card["handle"], "cmd-abc");
    assert_eq!(card["state"], "exited");
}

/// 页面与装配层各自把这件事接到位：起命令时就有一张「在跑」的卡片，跑完时那条
/// 通知不让流式光标以为新一轮开始了，也没有谁把卡片画成一条用户消息。
#[test]
fn the_page_shows_the_command_coming_back() {
    let pane = read_repo("web/src/conversation/ConversationPane.tsx");
    assert!(pane.contains("BackgroundCardView"), "页面上要有这张卡片");
    assert!(
        pane.contains("message.background && <BackgroundCardView"),
        "卡片挂在它那一句消息上"
    );
    let types = read_repo("web/src/types/events.ts");
    assert!(types.contains("export interface BackgroundCard"));
    assert!(
        types.contains("'message.background': 'background'"),
        "帧的类型名要在客户端的普查表里"
    );
    assert!(types.contains("NOTICE_MESSAGE_ID_PREFIX"));
    let store = read_repo("web/src/store/chatStore.ts");
    assert!(store.contains("case 'background'"), "store 要认得这一帧");
    assert!(
        store.contains("startsWith(NOTICE_MESSAGE_ID_PREFIX)"),
        "通知的那条消息不该抢走正在流的那一句的光标"
    );

    let gateway = read_repo("crates/cog-gateway/src/chat.rs");
    assert!(
        gateway.contains("Some(\"run_command_background\") if !is_error"),
        "句柄一交出去就该有一张「在跑」的卡片"
    );
    assert!(gateway.contains("pub async fn publish_background_finished"));
    assert!(read_repo("crates/cog-agent/src/background.rs").contains("pub fn completion_notice"));
    let assembly = read_repo("crates/cogneva/src/lib.rs");
    assert!(
        assembly.contains("cog_agent::background::subscribe()"),
        "装配层是这本书与那段对话之间唯一的持有者"
    );
    assert!(assembly.contains("publish_background_finished"));
}
