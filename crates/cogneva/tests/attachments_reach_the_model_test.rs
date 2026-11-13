//! 贴进来的一张图，真的走到了模型面前。
//!
//! 这条链有五段，任何一段断掉，屏幕上都只是「模型答得奇怪」：
//!
//! 1. **客户端**把文件读成 base64，并把类型按文件本身带上（不按扩展名猜）；
//! 2. **上行协议**把 `attachments` 与正文带在同一帧里；
//! 3. **网关**把它们原样交给这一轮，不在中途筛一遍（收不收是模型的协议边缘的事，
//!    那里会降级并留下说明——在这里筛掉，等于把「看不见这张图」变成静默丢弃）；
//! 4. **交给循环的消息**里它们是媒体块，且排在正文之后；
//! 5. **界面**把随这一句发过的附件写在消息下面——两样都在时，事后才分得清
//!    「问了这么一句」与「问了这么一句、还带了一张图」。
//!
//! 前四段的判据在这棵树上跑得动（纯函数与协议解析），第五段读源码：这一面没有
//! 前端测试跑道。

use crate::common::routes::read_repo;

const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";
const PROTOCOL: &str = "crates/cog-gateway/src/websocket_protocol.rs";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const SESSIONS: &str = "crates/cog-gateway/src/sessions.rs";
const CLIENT: &str = "web/src/api/client.ts";
const STORE: &str = "web/src/store/chatStore.ts";

fn shot() -> cog_core::ChatAttachment {
    cog_core::ChatAttachment {
        name: Some("shot.png".into()),
        mime_type: "image/png".into(),
        data: "QUJD".into(),
    }
}

/// 正文在前、图在后：图排在前面时，模型会先描述图再回答那句话。
#[test]
fn a_message_with_an_attachment_carries_it_as_a_media_block() {
    let message = cog_gateway::chat::user_message("这里错了", &[shot()]);
    let cog_core::Message::User { content, .. } = message else {
        panic!("交给循环的不是一条用户消息");
    };
    assert_eq!(content.len(), 2, "正文与附件没有各占一格：{content:?}");
    assert_eq!(
        content[0].as_text().as_deref(),
        Some("这里错了"),
        "第一格不是正文：模型先看到的是图，不是那句话"
    );
    assert!(
        matches!(
            &content[1],
            cog_core::ContentBlock::Media { data, mime_type }
                if data == "QUJD" && mime_type == "image/png"
        ),
        "第二格不是这张图：{content:?}"
    );
}

/// 一张截图配一句「这里错了」，图本身就是那半句话——没有正文也要发得出去。
#[test]
fn an_attachment_without_words_is_still_a_message() {
    let message = cog_gateway::chat::user_message("", &[shot()]);
    let cog_core::Message::User { content, .. } = message else {
        panic!("交给循环的不是一条用户消息");
    };
    assert_eq!(content.len(), 1, "空正文也占了一格：{content:?}");
    assert!(
        matches!(&content[0], cog_core::ContentBlock::Media { .. }),
        "只有附件时交上去的不是媒体块：{content:?}"
    );
}

/// 上行那一帧认得 `attachments`，类型按文件上报的那个词收。
#[test]
fn the_wire_accepts_attachments_on_the_chat_frame() {
    let frame: cog_gateway::websocket_protocol::ClientMessage = serde_json::from_str(
        r#"{"type":"chat_message","content":"","attachments":[
              {"name":"shot.png","mime_type":"image/png","data":"QUJD"}
           ]}"#,
    )
    .expect("带附件的一帧解析不了：客户端发什么都是白说");
    let cog_gateway::websocket_protocol::ClientMessage::ChatMessage { attachments, .. } = frame
    else {
        panic!("解析成了别的消息");
    };
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].mime_type, "image/png");
    assert_eq!(attachments[0].data, "QUJD");
}

/// 网关把这一帧里的附件交给这一轮，不在中途筛。
#[test]
fn the_socket_hands_the_attachments_to_the_turn() {
    let socket = read_repo(SOCKET);
    let at = socket
        .find("crate::chat::run_chat_turn(")
        .expect("套接字不再调这一轮");
    let call = &socket[at..(at + 900).min(socket.len())];
    assert!(
        call.contains("attachments"),
        "{SOCKET} 把附件留在了自己手里：这一轮收到的只有文字"
    );
    assert!(
        socket.contains("content.is_empty() && attachments.is_empty()"),
        "{SOCKET} 仍然按「正文为空」拒发：只带一张截图的那一句永远发不出去"
    );

    let chat = read_repo(CHAT);
    assert!(
        chat.contains("user_message_with_mentions(&text, &mentions, &attachments)"),
        "{CHAT} 没把附件交给消息装配：图片在网关里就丢了"
    );
    let protocol = read_repo(PROTOCOL);
    assert!(
        protocol.contains("attachments: Vec<cog_core::ChatAttachment>"),
        "{PROTOCOL} 的这一帧还是只有正文：客户端带的附件在解析时就被丢掉了"
    );
}

/// 界面上：读得进（选文件 / 粘贴）、带得出去（同一帧）、事后看得见。
#[test]
fn the_page_carries_what_was_pasted_and_shows_it_afterwards() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("readAsDataURL"),
        "{PANE} 不读文件内容：附件只是一个文件名，模型那边什么都没有"
    );
    assert!(
        pane.contains("onPaste"),
        "{PANE} 接不了粘贴：截图要么先存盘再选，要么根本进不来"
    );
    assert!(
        pane.contains("mime_type: file.mimeType"),
        "{PANE} 上行时换了字段名：服务端那一帧读到的类型是空的"
    );
    assert!(
        pane.contains("const attachments = files.map") && pane.contains("attachments,\n"),
        "{PANE} 发出去的那一帧不带附件：读进来的东西没有出门"
    );
    assert!(
        pane.contains("message.files.map"),
        "{PANE} 不把随消息发过的附件画出来：事后分不清带没带图"
    );
    assert!(
        pane.contains("MAX_ATTACHMENT_BYTES"),
        "{PANE} 不量附件大小：一张大到送不进一帧的图会被默默留下"
    );
}

/// 重开页面时，说过一张图的那一句仍然认得出那张图，而图本身不必再下载一遍。
///
/// 这一条量的是两头的形状对得上：记录接口带出去的是名字与类型（`ChatAttachmentInfo`），
/// 不是附件体；页面读回来的这一行把名字与类型装回 `files`，字节数留空。字节数在
/// 那一格里写个 0 的话，界面会把一张图显示成 0 B——「没量到」与「量到是零」被画成
/// 了同一个东西。
#[test]
fn the_reopened_page_still_names_what_came_with_each_line() {
    let sessions = read_repo(SESSIONS);
    assert!(
        sessions.contains("\"attachments\": turn.attachments"),
        "{SESSIONS} 的历史接口不带附件：重开页面之后那句话看起来没带过图"
    );
    assert!(
        !sessions.contains("attachment.data"),
        "{SESSIONS} 把附件体也发出去了：每一次重开都要把整段对话的图重新下载一遍"
    );

    let client = read_repo(CLIENT);
    assert!(
        client.contains("attachments?: ChatAttachmentInfo[]"),
        "{CLIENT} 读回的历史里没有附件那一格：服务端带了，页面读不到"
    );

    let store = read_repo(STORE);
    assert!(
        store.contains("line.attachments ?? []"),
        "{STORE} 不把历史里的附件装回消息：刷新之后这一行又不见了"
    );
    assert!(
        store.contains("bytes: null"),
        "{STORE} 给没量到的字节数填了一个值：「没量到」被画成「量到是零」"
    );
    assert!(
        read_repo(PANE).contains("file.bytes !== null"),
        "{PANE} 读回的历史里没有大小，界面却照样写一个数"
    );
}
