//! 同事之间的消息在发起方这一段对话里看得见：谁发给谁、说了什么、结局如何。
//!
//! 从前的形状是「数据面有、可视面没有」：`send_to_agent` 把话发出去，发起方这
//! 一侧只剩一行 `tracing::info`；人要看「我刚才让它办的那件事回没回」，只能去翻
//! 通信面板或日志，而通信面板画的是任务交接的边，不是消息内容。
//!
//! 这里钉四段：
//!
//! 1. **观测点**：这一轮真的跑过的工具在 `after_tool_call` 上交出来，装配层在那里
//!    认 `send_to_agent`——工具自己不知道「这一段对话」这个概念，只知道收件人地址；
//! 2. **一条一帧**：网关按条发 `message.colleague`，不折成一行汇总，因为合计数
//!    回答不了「发给他那条到底回没回」；
//! 3. **结局分三种**：等到了回信、发出去了但没等到（`reason` 说明为什么）、根本
//!    没发出去（`error`）。把缺席读成假值会把「没问」画成「问了没答」；
//! 4. **页面上画成一行**，超时那一句写的是「没在时限内答」，不是「对方没做完」。
//!
//! 行为那一半在 `cogneva::conversation::tests` 的
//! `a_message_sent_to_a_colleague_carries_its_outcome` 与
//! `cog_gateway::chat::tests::{each_message_to_a_colleague_is_one_frame_with_its_own_outcome,
//! a_turn_with_no_message_to_a_colleague_sends_no_frame}`；这里钉接线。

use crate::common::routes::read_repo;

const ASSEMBLY: &str = "crates/cogneva/src/conversation.rs";
const CHAT: &str = "crates/cog-gateway/src/chat.rs";
const WEB_EVENTS: &str = "web/src/types/events.ts";
const WEB_CHAT: &str = "web/src/store/chatStore.ts";
const WEB_PANE: &str = "web/src/conversation/ConversationPane.tsx";

/// 装配这一侧：这一轮跑过的工具在观测点上被认出来，集合随交付物回去。
#[test]
fn the_turn_watches_what_it_sent_to_its_colleagues() {
    let assembly = read_repo(ASSEMBLY);
    assert!(
        assembly.contains("after_tool_call: Some("),
        "{ASSEMBLY} 没有装调用之后的观测点：发出去的话在这一侧没有任何落点"
    );
    assert!(
        assembly.contains("if tool == \"send_to_agent\""),
        "{ASSEMBLY} 的观测点不认发消息那只工具：别的工具调用也会被记成同事消息"
    );
    assert!(
        assembly.contains("fn colleague_message("),
        "{ASSEMBLY} 没有把回执折成那一行要的事实"
    );
    assert!(
        assembly.contains("\"colleagues\".to_string()"),
        "{ASSEMBLY} 不把这一轮发过的消息带回去：界面拿不到这一格"
    );
    // 观测点不能只装在「配了钩子」的分支里：勾子表是可选能力，同事消息不是。
    let hooks = assembly
        .split_once("let hook_runs = Arc::new(cog_agent::hooks::guard::Runs::default())")
        .expect("钩子读数那一段改了形状")
        .1;
    assert!(
        hooks.contains("runtime = runtime.with_hooks("),
        "{ASSEMBLY} 的观测点只在某种配置下才装：不配钩子的部署看不到同事消息"
    );
}

/// 网关：一条消息一帧，帧名与页面认的名字对得上，且排在收尾帧之前。
#[test]
fn every_sent_message_becomes_its_own_frame() {
    let chat = read_repo(CHAT);
    assert!(
        chat.contains("async fn emit_turn_colleagues("),
        "{CHAT} 没有发这一帧：消息发出去之后页面上什么都没有"
    );
    assert!(
        chat.contains("\"message.colleague\""),
        "{CHAT} 的帧名与页面认的名字对不上"
    );
    let run = chat
        .split_once("async fn run_chat_turn")
        .expect("run_chat_turn 改了名字")
        .1;
    let emit_at = run
        .find("emit_turn_colleagues(")
        .expect("定义了发帧的函数却没有调用");
    // 最后一个才是收尾：前面几处是失败与被停那几条路各发一次，这一帧要排在它们
    // 之后、收尾之前，才算落在这一条还活着的时候。
    let end_at = run
        .rfind("emit_message_end(")
        .expect("收尾帧那一段改了形状");
    assert!(
        emit_at < end_at,
        "{CHAT} 在收尾帧之后才发这一帧：那一条已经定稿，这一行没有落点"
    );
}

/// 页面：帧表、消息上的那一格、以及画出来的那一行。
#[test]
fn the_page_draws_who_was_told_what_and_how_it_ended() {
    for (file, needle, why) in [
        (
            WEB_EVENTS,
            "'message.colleague': 'colleague'",
            "聊天帧表里没有这一帧",
        ),
        (
            WEB_CHAT,
            "case 'colleague':",
            "消息上不记这一格，那一行画不出来",
        ),
        (WEB_PANE, "colleaguesLine(", "同事消息那一行没有画出来"),
        (
            WEB_PANE,
            "没在时限内答",
            "超时被写成了「没做完」：这两件事在屏幕上必须分开",
        ),
    ] {
        assert!(
            read_repo(file).contains(needle),
            "{file} 缺 {needle:?}：{why}"
        );
    }
}
