//! 一轮对话的记录必须落在 `message.end` 之前。
//!
//! 界面把 `message.end` 当作「这一轮结束了」，收到它就把下一句发上来；而网关对
//! 每一轮是各自 `tokio::spawn` 的，下一轮读历史的那次查询于是可以和上一轮的落库
//! 赛跑。记录写在这一帧之后时，用户一口气追问，上一轮就不在上下文里——模型只能
//! 说「我看不到刚才那轮」，而这不是它记性差，是那一行还没提交。
//!
//! 实测（2026-11-05，带外客户端）：同一会话连问四轮，每一轮都丢掉上一轮；隔几秒
//! 用同一个会话号再问，它把上一轮最后一句原样复述了出来。差别只在两轮之间有没有
//! 那几秒。
//!
//! 判据读的是 `run_chat_turn` 里的先后：最后一处写记录（`record_turn` /
//! `record_dispatched_turn` / `record_failed_turn`）早于最后一处收尾帧。

use crate::common::routes::read_repo;

const SERVER: &str = "crates/cog-gateway/src/chat.rs";

/// `run_chat_turn` 的函数体：从签名之后到测试模块之前。
fn the_turn_body(source: &str) -> &str {
    let Some((_, after)) = source.split_once("pub async fn run_chat_turn(") else {
        panic!("{SERVER} 里找不到 run_chat_turn：这条判据读的函数改了名字或搬了家");
    };
    after.split("\n#[cfg(test)]").next().unwrap_or(after)
}

#[test]
fn the_turn_is_written_down_before_the_interface_is_told_it_ended() {
    let source = read_repo(SERVER);
    let body = the_turn_body(&source);

    let recorded_at = [
        "record_turn(",
        "record_dispatched_turn(",
        "record_failed_turn(",
    ]
    .iter()
    .filter_map(|call| body.rfind(call))
    .max()
    .unwrap_or_else(|| {
        panic!("{SERVER} 的 run_chat_turn 里一处写记录都没有：这一轮不会留下任何痕迹")
    });

    let ended_at = body
        .rfind("emit_message_end(&out, &session_id, &assistant_mid)")
        .unwrap_or_else(|| {
            panic!("{SERVER} 里找不到助手那一轮的 message.end：界面等的收尾帧不见了")
        });

    assert!(
        recorded_at < ended_at,
        "这一轮先说了「结束了」（message.end），才把记录写下去。\
         界面收到 end 就把下一句发上来，下一轮读历史时会跑在这次提交之前——\
         上一轮在上下文里不存在，模型只能说「我看不到刚才那轮」。\
         把写记录挪到收尾帧之前。"
    );
}
