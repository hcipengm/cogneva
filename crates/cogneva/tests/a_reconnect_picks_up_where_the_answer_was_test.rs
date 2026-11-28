//! 断线续流：这一轮已经说出来的字，接上之后还在，而且只有一份。
//!
//! 从前聊天帧只走这一条连接：网一抖、页面重开，回答仍在生成、字仍在流，而屏幕
//! 上一个字都收不到。要的是把投递从「某一条 socket」改成「这一段对话」——连接走
//! 了，帧还在；新连接接上来先看一份快照，接着往下写。
//!
//! 这条链有三处，任何一处退回旧样子，这一条判据就红：
//!
//! 1. **帧挂在这一段对话上**：套接字不再为每条连接造一份正在跑的名册，`run_chat_turn`
//!    发进部署级的那一条流；停止也从那里按会话找。
//! 2. **接得上**：`chat_resume` 走到 `watch_session`，先交快照再转后续帧；快照是
//!    「替换」语义，同一条连接重复接、断两次再接，都不会把一句话叠两遍。
//! 3. **页面真的会接**：连上的那一刻、以及换到另一段对话时都发 `chat_resume`；收到
//!    的快照按整段替换而不是追加。
//!
//! 第 2 条在本文件里跑真的流；第 1、3 条读源码——WebUI 没有 JS 测试跑道，这一族
//! 的路由判据历来读源码（同 `web_chat_works_like_a_workspace_test`）。

use std::time::Duration;

use tokio::sync::mpsc;

use crate::common::routes::read_repo;

const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";
const STREAM: &str = "crates/cog-gateway/src/chat_stream.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const HOOK: &str = "web/src/hooks/useEventStream.ts";
const TRANSPORT: &str = "web/src/transport/websocket.ts";
const STORE: &str = "web/src/store/chatStore.ts";

/// 一段代码里从 `from` 开始的那一块：用来判「谁写在谁前面」。
fn from(source: &str, from: &str) -> String {
    source
        .split_once(from)
        .unwrap_or_else(|| panic!("找不到 `{from}`：这条判据读的那一段改了名字或搬了家"))
        .1
        .to_string()
}

/// 一条帧在流上留下什么：测试帧就是它自己那一段 JSON。
fn publish(
    stream: &cog_gateway::chat_stream::ChatStream,
    session: &str,
    event: &str,
    inner: serde_json::Value,
) {
    let json = inner.to_string();
    stream.publish(session, event, &inner, json);
}

async fn next_frame(out: &mut mpsc::Receiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(2), out.recv())
        .await
        .expect("接上的那条连接要在两秒内收到一帧")
        .expect("转发任务不该在测试结束前退出")
}

/// 说了一句话的开头：三帧增量，然后没人再看。
fn say_a_sentence(stream: &cog_gateway::chat_stream::ChatStream, session: &str) {
    publish(
        stream,
        session,
        "message.start",
        serde_json::json!({ "message_id": "m-1", "role": "assistant", "attachment": null }),
    );
    for piece in ["先", "说", "一句"] {
        publish(
            stream,
            session,
            "message.text_delta",
            serde_json::json!({ "message_id": "m-1", "delta": piece }),
        );
    }
}

/// 断线期间说的话不丢：新连接先拿到「说到哪儿了」，而且正文逐字拼得起来。
#[tokio::test]
async fn a_socket_that_connects_mid_turn_gets_what_was_already_said() {
    use cog_gateway::chat_stream::ChatStream;

    let stream = ChatStream::new(64);
    // 没有人在看：这一轮照样往下说。
    say_a_sentence(&stream, "s-1");

    let (out_tx, mut out_rx) = mpsc::channel(16);
    stream.watch("s-1", out_tx);
    let resume: serde_json::Value =
        serde_json::from_str(&next_frame(&mut out_rx).await).expect("快照是一帧 JSON");
    assert_eq!(resume["type"], "chat_resume", "{resume}");
    assert_eq!(resume["session_id"], "s-1");
    assert_eq!(resume["messages"][0]["text"], "先说一句");
    assert_eq!(resume["messages"][0]["role"], "assistant");
    assert_eq!(resume["messages"][0]["ended"], false);

    // 接上之后说的，一句不落地到这条连接上。
    publish(
        &stream,
        "s-1",
        "message.text_delta",
        serde_json::json!({ "message_id": "m-1", "delta": "，再说一句" }),
    );
    let frame = next_frame(&mut out_rx).await;
    assert!(frame.contains("，再说一句"), "{frame}");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), out_rx.recv())
            .await
            .is_err(),
        "快照里已经有的字不该再发一遍"
    );
}

/// 连两次、断两次再接：同一句话仍然只有一份字。
#[tokio::test]
async fn reconnecting_does_not_say_the_sentence_twice() {
    use cog_gateway::chat_stream::ChatStream;

    let stream = ChatStream::new(64);
    say_a_sentence(&stream, "s-1");

    let first = stream.snapshot("s-1");
    let second = stream.snapshot("s-1");
    assert_eq!(first, second, "两次重连拿到的是同一份快照");
    assert_eq!(first.len(), 1, "一句消息一次，不是每次重连多一格");

    // 第二段连接进来时这一句还在说：它拿到的仍是整段，而不是叠在上一份上。
    publish(
        &stream,
        "s-1",
        "message.text_delta",
        serde_json::json!({ "message_id": "m-1", "delta": "又一句" }),
    );
    let (out_tx, mut out_rx) = mpsc::channel(16);
    stream.watch("s-1", out_tx);
    let resume: serde_json::Value =
        serde_json::from_str(&next_frame(&mut out_rx).await).expect("JSON");
    assert_eq!(resume["messages"][0]["text"], "先说一句又一句");
}

/// 两段对话各有一条流：另一段说的字不会出现在这一段的快照里。
#[tokio::test]
async fn one_conversation_does_not_pick_up_another_s() {
    use cog_gateway::chat_stream::ChatStream;

    let stream = ChatStream::new(64);
    say_a_sentence(&stream, "s-1");
    say_a_sentence(&stream, "s-2");
    publish(
        &stream,
        "s-2",
        "message.text_delta",
        serde_json::json!({ "message_id": "m-1", "delta": "（另一段）" }),
    );
    assert_eq!(stream.snapshot("s-1")[0].text, "先说一句");
    assert!(stream.snapshot("s-2")[0].text.ends_with("（另一段）"));
}

/// 帧挂在部署上，不挂在一条连接上：套接字不再自己造一份名册。
#[test]
fn the_stream_belongs_to_the_deployment_not_to_one_socket() {
    let socket = read_repo(SOCKET);
    let root = read_repo("crates/cog-gateway/src/lib.rs");

    assert!(
        root.contains("pub chat_stream: chat_stream::ChatStream"),
        "部署状态上没有那一条流：重连上来的新连接接不到任何人"
    );
    assert!(
        !socket.contains("RunningTurns::new()"),
        "{SOCKET} 又为一条连接造了一份正在跑的名册：换个连接就找不到那一轮，停止也按不到了"
    );
    assert!(
        socket.contains("let mut watched: std::collections::HashSet<String>"),
        "{SOCKET} 要记住这条连接接在哪几段对话上（重复接同一段会收到两份帧）"
    );
    let start = from(&socket, "fn start_chat_turn(");
    assert!(
        start.contains("state.chat_stream.clone()"),
        "{SOCKET} 起一轮时没把部署级的流交出去：帧又只发回起这一轮的那条 socket"
    );

    let chat = read_repo(CHAT);
    assert!(
        chat.contains("out: crate::chat_stream::ChatStream"),
        "{CHAT} 的这一轮还在往一个裸发送端里写：连接一走，字就没了"
    );
    let emit = from(&chat, "async fn emit(");
    assert!(
        emit.contains("out.publish("),
        "{CHAT} 的帧没有进那一条流：重连拿不到它"
    );
    let cancel = from(&socket, "ClientMessage::ChatCancel {");
    assert!(
        cancel.contains("state.chat_stream.running().cancel("),
        "停止没有按会话去部署级的名册里找：另一个连接起的那一轮停不掉"
    );
}

/// 续流要问一句「这一段能不能给你看」，再问「先交快照」。
#[test]
fn a_resume_checks_the_conversation_before_it_hands_over_the_stream() {
    let socket = read_repo(SOCKET);
    let resume = from(&socket, "ClientMessage::ChatResume {");
    let check = resume
        .find("crate::sessions::speaks_here(")
        .unwrap_or_else(|| {
            panic!("{SOCKET} 的 `chat_resume` 没判归属：猜一个会话号就能读到别人的回答")
        });
    let attach = resume
        .find("watch_session(")
        .unwrap_or_else(|| panic!("{SOCKET} 的 `chat_resume` 没有接上流"));
    assert!(check < attach, "先判归属，再接上流");

    let watch = from(&socket, "fn watch_session(");
    assert!(
        watch.contains("if watched.insert("),
        "同一条连接重复接同一段会多一条转发，每一帧都到两次"
    );
    assert!(
        watch.contains("state.chat_stream.watch("),
        "接上的不是部署级的那条流"
    );

    let protocol = read_repo(PROTOCOL);
    assert!(
        protocol.contains("ChatResume"),
        "{PROTOCOL} 没有续流这条上行帧：页面没有东西可发"
    );
    let stream = read_repo(STREAM);
    let forwarder = from(&stream, "pub fn watch(");
    assert!(
        forwarder.contains("send_snapshot(&out, &session, &snapshot)"),
        "接上时没有先交一份快照：断线那一段只有之后的增量，前面缺一段"
    );
    assert!(
        forwarder.contains("if frame.seq < upto"),
        "快照之前的帧又发了一遍：同一句话会在屏幕上出现两次"
    );
}

/// 页面真的会接：连上时、换到另一段时都发 `chat_resume`，快照是替换不是追加。
#[test]
fn the_page_asks_for_the_turn_again_when_it_connects_and_when_it_switches() {
    let hook = read_repo(HOOK);
    assert!(
        hook.contains("type: 'chat_resume'"),
        "{HOOK} 从不发续流帧：后端接得上，页面也不接"
    );
    assert!(
        hook.contains("onOpen: resumeOpenSession"),
        "连上的那一刻不接：重连之后那一轮的下文仍然收不到"
    );
    assert!(
        hook.contains("useChatStore.subscribe("),
        "换到另一段对话时不接：切回来看到的是切换前的旧字"
    );

    let transport = read_repo(TRANSPORT);
    let branch = from(&transport, "'chat_resume'");
    assert!(
        branch.contains("onChatResume"),
        "{TRANSPORT} 收到了 `chat_resume` 却没有交给页面"
    );

    let store = read_repo(STORE);
    let apply = from(&store, "applyResume: (sessionId, messages) => {");
    assert!(
        apply.contains("text: line.text"),
        "{STORE} 的 `applyResume` 不是整段替换：重连会把同一句话拼两遍"
    );
    assert!(
        !apply.contains("+= line.text") && !apply.contains("+= line.thinking"),
        "快照被当成增量追加了"
    );
}
