//! 一条太长的消息：原文逐字落成文件，循环拿到的是引用与开头，界面画成附件。
//!
//! 这条链有四段，断在哪一段屏幕上都不像「截断了」：
//!
//! 1. **阈值**。它由两条读数导出——这一段对话真正跑在多大的窗口里，和一帧最
//!    多带多少——取最紧的那一条。写成常量的话，换模型、换窗口，落文件的那条线
//!    还停在原处，而屏上没有任何东西会因此变红。
//! 2. **落盘**。原文写的必须是原文，一个字节都不能少；写去的地方必须是模型自己
//!    `read_file` 读得到的那个执行器，否则「落成文件」只是本地多了一份没人读的
//!    副本。
//! 3. **交给循环的那一句**。它要说清「原文在哪、它有多长、这一段只是开头」；
//!    否则模型读到的半句话会当成全文，答得越认真，错得越远。
//! 4. **界面**。这一条要画成附件（长度写的是全文），而不是把开头铺成正文。
//!
//! 判据一半是纯函数（阈值、路径、参考文、写盘），一半写在源码上：聊天这一轮的
//! 编排在 `run_chat_turn` 里，跨执行体调不起来，读源码是这一层能做到的实证。

use std::sync::{Arc, Mutex};

use crate::common::routes::read_repo;

const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const EVENTS: &str = "web/src/types/events.ts";
const PANE: &str = "web/src/conversation/ConversationPane.tsx";
const STORE: &str = "web/src/store/chatStore.ts";

/// 这一段对话的窗口（token），取内核默认的那一档——与实现读的是同一个出处。
fn window() -> usize {
    cog_core::RuntimeConfig::default().context_window_size
}

/// 一帧的上限（字节），取网关钉下的那一个。
fn frame() -> usize {
    cog_gateway::chat::MAX_INBOUND_FRAME_BYTES
}

/// 阈值是算出来的：换窗口、换帧上限，它跟着动；两条里更紧的那条说了算。
#[test]
fn the_threshold_follows_the_window_and_the_frame() {
    let base = cog_gateway::chat::oversized_threshold_tokens(window(), frame());
    assert!(base > 0, "阈值不能是 0：任何一句都会被判成长消息");

    // 窗口翻倍，阈值跟着翻倍：窗口大的部署能把更长的消息直接读进来。
    assert_eq!(
        cog_gateway::chat::oversized_threshold_tokens(window() * 2, frame()),
        base * 2,
        "窗口换了，落文件的那条线没跟着换"
    );

    // 帧比窗口更紧时，阈值降到帧那一条上，而且是按帧的比例走。
    let tighter = cog_gateway::chat::oversized_threshold_tokens(window(), 4 * 1024);
    assert!(
        tighter < base,
        "帧收紧了，阈值却还按窗口算：这条消息根本送不进一帧"
    );
    assert_eq!(
        cog_gateway::chat::oversized_threshold_tokens(window(), 2 * 1024) * 2,
        tighter,
        "帧这一项不是按比例读的"
    );

    // 这一版两句读数里窗口是更紧的那一条：它必须真的被算进去了。
    assert_eq!(
        base,
        window() / 8,
        "阈值不是「窗口的八分之一」：窗口那一项没有被读进来"
    );
}

/// 写进执行器的就是原文本身，路径与参考文说的也是同一份。
#[tokio::test]
async fn the_original_is_written_to_the_executor_byte_for_byte() {
    let backend = Arc::new(Recording::default());
    let dyn_backend: Arc<dyn cog_core::SandboxBackend> = backend.clone();
    let content = "第一段。\n第二段有标点、换行与 emoji 🙂。\n".repeat(500);

    let archived =
        cog_gateway::chat::archive_oversized(Some(&dyn_backend), "s-1", &content, window() / 8)
            .await
            .expect("原文必须写得进去");

    let writes = backend.writes.lock().unwrap().clone();
    assert_eq!(writes.len(), 1, "一次归档写一份：{writes:?}");
    assert_eq!(writes[0].1, content, "写进去的不是原文：字节被动过了");
    assert_eq!(
        writes[0].0,
        cog_gateway::chat::attachment_path("s-1", &content),
        "写去的路径与参考文里说的不是同一个"
    );
    assert_eq!(
        archived.path, writes[0].0,
        "界面被告知的路径与真正写下的不是同一个"
    );
    assert_eq!(archived.chars, content.chars().count());
}

/// 没有执行器可写时不假装写成了：参考文不会凭空出现，调用方拿到的是原因。
#[tokio::test]
async fn a_deployment_with_nowhere_to_write_says_so() {
    let error = cog_gateway::chat::archive_oversized(None, "s-1", "很长的一段话", 16)
        .await
        .expect_err("没有沙箱后端时说成了写成功");
    assert!(
        error.contains("沙箱后端"),
        "失败原因没有说清是「没地方写」：{error}"
    );
}

/// 同一段原文永远落在同一个路径上：下一轮读历史时，路径由内容算得回来。
#[test]
fn the_path_is_the_same_for_the_same_message() {
    let a = cog_gateway::chat::attachment_path("s-1", "同样的原文");
    assert_eq!(a, cog_gateway::chat::attachment_path("s-1", "同样的原文"));
    assert_ne!(
        a,
        cog_gateway::chat::attachment_path("s-1", "另一段原文"),
        "两段不同的原文写去了同一个文件：后写的把先写的盖了"
    );
    assert_ne!(
        a,
        cog_gateway::chat::attachment_path("s-2", "同样的原文"),
        "两段对话共用一个文件：一段的原文会出现在另一段的引用里"
    );

    // 会话号是客户端给的：带路径分隔符的那一种不能走出根目录。
    let escaped = cog_gateway::chat::attachment_path("../../etc", "x");
    assert!(
        escaped.starts_with(cog_gateway::chat::ATTACHMENT_ROOT),
        "会话号能走出附件根目录：{escaped}"
    );
    assert!(!escaped.contains(".."), "会话号里的 `..` 被原样拼进了路径");
}

/// 参考文说清了三件事：原文多长、在哪、这一段只是开头；它自己不带全文。
#[test]
fn the_reference_says_what_is_missing_and_where_the_rest_is() {
    let content = "字".repeat(50_000);
    let path = "/tmp/cogneva-chat-attachments/s/a.txt";
    let reference = cog_gateway::chat::oversized_reference(&content, path, window() / 8);

    assert!(reference.contains(path), "参考文没有说原文在哪");
    assert!(reference.contains("50000"), "参考文没有说原文有多长");
    assert!(
        reference.contains("不是全文"),
        "参考文没有说明这一段只是开头：模型会把它当成全文"
    );
    assert!(
        reference.chars().count() < content.chars().count(),
        "参考文与原文一样长：它自己就把窗口再撑破一次"
    );
    assert!(
        !reference.contains(&content),
        "参考文里带着全文：落文件这件事没有发生"
    );
}

/// 循环读到的是引用；回声与记录拿的是原文。
///
/// 两边不能对调：循环要的是一句话，记录与界面要的是「用户说过的那段东西」。
#[test]
fn the_loop_reads_the_reference_while_the_record_keeps_the_body() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("archive_if_oversized(&state, &session_id, &content)"),
        "{CHAT} 不再在回声之前归档：长消息照旧整条进上下文"
    );
    assert!(
        chat.contains("Some(archived) => archived.reference.clone()"),
        "{CHAT} 给循环的不是引用：原文又整条进了上下文"
    );
    assert!(
        chat.contains("emit_delta(&out, &session_id, &user_mid, &content)"),
        "{CHAT} 的回声不是原文：界面读回的不是用户说过的那段东西"
    );
    let at = chat
        .find("record_turn(\n                    &state.chat_turns,")
        .unwrap_or_else(|| panic!("{CHAT} 里找不到写成功那一轮的那个调用"));
    let call = &chat[at..(at + 260).min(chat.len())];
    assert!(
        call.contains("&content,") && call.contains("&attachments,") && call.contains("&reply,"),
        "{CHAT} 的记录不是原文与随它的附件：刷新之后这条消息只剩开头：{call}"
    );
    assert!(
        chat.contains("estimate_tokens(&text) <= threshold_tokens"),
        "{CHAT} 读历史时不再折一次：上一轮的长消息下一轮整条回来"
    );
    assert!(
        chat.contains("turn.context_window_tokens()"),
        "{CHAT} 的阈值没有问执行体自己的窗口：它读的是另一个数"
    );
}

/// 回声那一帧带着附件：界面靠它把这一条画成附件，而不是把开头当成全文。
#[test]
fn the_attachment_travels_on_the_start_frame() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("\"attachment\": attachment"),
        "{CHAT} 的 message.start 不带附件：界面上这条长消息与普通正文长得一样"
    );

    let events = read_repo(EVENTS);
    assert!(
        events.contains("attachment: chatAttachment(inner.attachment)"),
        "{EVENTS} 不读附件那一格：帧上有、界面上没有"
    );
    let store = read_repo(STORE);
    assert!(
        store.contains("attachment: frame.attachment"),
        "{STORE} 不把附件收进消息：画的时候没有可用的那一格"
    );
}

/// 界面上这一条画成附件：长度、原文位置、展开全文。
#[test]
fn the_page_draws_a_long_message_as_an_attachment() {
    let pane = read_repo(PANE);
    assert!(
        pane.contains("长消息 ·"),
        "{PANE} 上没有一个「长消息」的附件格：它仍然是一段正文"
    );
    assert!(
        pane.contains("原文已存成文件"),
        "{PANE} 不说原文落在哪：读者以为屏幕上这段就是全部"
    );
    assert!(
        pane.contains("展开全文"),
        "{PANE} 没有展开全文的入口：折起来之后就读不到了"
    );
    assert!(
        pane.contains("message.attachment ||"),
        "{PANE} 只看正文长度，不认帧上的附件那一格"
    );
}

/// 一个只记下写入的沙箱后端：这一层量的是「写没写、写了什么」，不是执行器本身。
#[derive(Default)]
struct Recording {
    writes: Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
impl cog_core::SandboxBackend for Recording {
    async fn execute(
        &self,
        req: &cog_core::SandboxRequest,
    ) -> cog_core::SFResult<cog_core::SandboxResult> {
        if let cog_core::SandboxPayload::WriteFile { path, content } = &req.payload {
            self.writes
                .lock()
                .unwrap()
                .push((path.clone(), content.clone()));
        }
        Ok(cog_core::SandboxResult {
            exit_code: 0,
            ..Default::default()
        })
    }

    async fn precompile(&self, _bytes: &[u8]) -> cog_core::SFResult<String> {
        Err(cog_core::SFError::Agent(
            "a recording backend compiles nothing".into(),
        ))
    }
}
