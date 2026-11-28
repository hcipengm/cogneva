//! 一段对话的帧发到哪儿：此刻在看的每一条连接，以及「刚才没在看」的那一段。
//!
//! 聊天帧原本只走一条路：这一轮的出站管道 → 起这一轮的那条 socket。连接一断，
//! 管道那一头就没了，回答仍在生成、字仍在流，而屏幕上一个字都拿不到——重连之后
//! 那一段回答要么消失，要么从零开始。人看到的是「网一抖，回答没了」，与
//! Claude Code 里网络恢复后那一轮接着往下写，是两种体验。
//!
//! 这里把帧的投递从「某一条连接」改成「这一段对话」：
//!
//! - **每一段一条广播**。帧发进这一段的广播通道，谁在看谁收；没人在看时它也没有
//!   收件人，但帧照样记下来了。广播的容量就是「离开过的连接能补回多少帧」，写满
//!   之后最老的那一帧从环上掉出去——这正是慢收件人该付出的代价，而不是把这一轮
//!   卡住等它。所以这里没有一个 `await`：投递不进锁、不阻塞生成。
//! - **每一段一份快照**。重连时不必重放每一帧，交一份「这一段现在说到哪儿了」：
//!   `chat_resume` 收到的是到目前为止的正文与思考。客户端拿快照整段替换，再接
//!   后续增量——重连几次都不会把同一句话叠两遍。
//! - **快照与订阅在同一把锁里**，快照带一个序号上界 `upto`：登记那一刻之前的帧
//!   都已经在快照里，登记之后才产生的一定带着更大的序号。转发的那一头按这个界
//!   过滤，所以「快照 + 后续帧」拼起来恰好是这一段的字，不多一个也不少一个。
//!   慢到被广播挤掉的收件人（`Lagged`）走同一条路：重新取一份快照，从那里接着看。
//!
//! 只记会说给人看的那几族帧：正文、思考、附件那一行。工具调用、用量、目标身份
//! 这些不进快照——它们在记录里有，重开页面按 REST 读回来；在一段正在流的回答里
//! 叠上它们，反而要在重连时把两个来源拼一遍。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{broadcast, mpsc};

use crate::chat::RunningTurns;

/// 一句话在重连时交还给客户端的样子。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MessageSnapshot {
    pub message_id: String,
    pub role: String,
    /// 到目前为止收到的正文，逐字拼起来的——与在线时看到的完全一致。
    pub text: String,
    /// 到目前为止的思考内容。非空而没 `ended`，就是「它正在想」。
    pub thinking: String,
    /// 这条消息是不是落在文件里的长消息（`message.start` 上带的那一格）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachment: Option<serde_json::Value>,
    /// 这一句已经收尾（收到了 `message.end`）。
    pub ended: bool,
}

/// 广播上的一帧：序号 + 要发出去的 JSON。
#[derive(Debug, Clone)]
struct Frame {
    seq: u64,
    json: String,
}

/// 部署级的聊天流：正在跑的轮 + 每一段的在说内容。
///
/// 装进 [`crate::GatewayState`] 而不是每条连接一份：一条连接断开之后，重连上来的
/// 是一条新连接，它要能接着看同一段对话；正在跑的那几轮也要还在名册上，它的停止
/// 按钮才按得动。
#[derive(Clone)]
pub struct ChatStream {
    running: RunningTurns,
    inner: Arc<Inner>,
}

struct Inner {
    /// 每一段留多少句、广播环上留多少帧（见 [`ChatStream::new`]）。
    keep: usize,
    sessions: Mutex<HashMap<String, Session>>,
}

struct Session {
    next_seq: u64,
    /// 这一段从现在起发出的每一帧。容量即「离开过的连接能补回多少帧」。
    frames: broadcast::Sender<Frame>,
    /// 会说给人看的那几句，最老的先掉出去。
    messages: VecDeque<MessageSnapshot>,
}

impl Default for ChatStream {
    fn default() -> Self {
        // 默认值与 websocket 的补帧缓存同一个来源：两者回答的是同一个问题——
        // 「一条离开过的连接能补回多少」。
        Self::new(cog_core::SystemConfig::default().websocket_event_cache_capacity)
    }
}

impl ChatStream {
    /// `keep` 与 websocket 的补帧缓存同一个旋钮（装配层按
    /// `system.websocket_event_cache_capacity` 传进来）。
    pub fn new(keep: usize) -> Self {
        Self {
            running: RunningTurns::new(),
            inner: Arc::new(Inner {
                keep: keep.max(1),
                sessions: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// 正在跑的轮。停止按会话寻址，名册是整台部署共用的。
    pub fn running(&self) -> &RunningTurns {
        &self.running
    }

    /// 发一帧给这一段对话，并把它记进快照。
    ///
    /// 不 `await`：投递是环上的写入，写满就挤掉最老的一帧（见模块注释）。所以
    /// 生成慢不慢与谁在看无关，一条不读的 socket 也卡不住这一段。
    pub fn publish(
        &self,
        session_id: &str,
        event_type: &str,
        inner: &serde_json::Value,
        json: String,
    ) {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let keep = self.inner.keep;
        let session = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| Session::new(keep));
        let seq = session.next_seq;
        session.next_seq += 1;
        note(&mut session.messages, event_type, inner);
        while session.messages.len() > keep {
            session.messages.pop_front();
        }
        // 没有收件人时 `send` 报错，那不是错：这一段的帧已经记在快照里了。
        cog_core::ignore_lost!(session.frames.send(Frame { seq, json }));
        // 一句都不剩、也没人在看：这一段在这个进程里没有可留的东西了。
        if session.messages.is_empty() && session.frames.receiver_count() == 0 {
            sessions.remove(session_id);
        }
    }

    /// 把这条连接接上这一段的广播，先把「说到哪儿了」交给它，再一帧一帧往后转。
    ///
    /// 转发是一个独立的任务：它只往这条连接的出站管道写，管道那一头没了
    /// （`send` 失败）它自己就退出。所以断开连接不需要谁去撤销它。
    pub fn watch(&self, session_id: &str, out: mpsc::Sender<String>) {
        // 订阅与快照在同一把锁里取：两者之间的帧不存在，见模块注释。
        let (mut frames, upto, snapshot) = {
            let mut sessions = self
                .inner
                .sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let keep = self.inner.keep;
            let session = sessions
                .entry(session_id.to_string())
                .or_insert_with(|| Session::new(keep));
            (
                session.frames.subscribe(),
                session.next_seq,
                session.messages.iter().cloned().collect::<Vec<_>>(),
            )
        };
        let inner = Arc::clone(&self.inner);
        let session = session_id.to_string();
        tokio::spawn(async move {
            let mut upto = upto;
            if !send_snapshot(&out, &session, &snapshot).await {
                return;
            }
            loop {
                match frames.recv().await {
                    Ok(frame) => {
                        // 比快照更旧的帧不重发：它的字已经在快照里了，再发一遍就是
                        // 同一句话在屏幕上出现两次。
                        if frame.seq < upto {
                            continue;
                        }
                        if out.send(frame.json).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        // 慢到被广播挤掉：接着往下发就是一段缺字的回答。重新要一份
                        // 快照，让客户端整段替换——这正是重连走的那条路。
                        tracing::debug!(
                            session = %session,
                            missed,
                            "chat stream fell behind; resending the snapshot"
                        );
                        let (next, snapshot) = snapshot_of(&inner, &session);
                        upto = next;
                        if !send_snapshot(&out, &session, &snapshot).await {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
    }

    /// 这一段现在说到哪儿了。给重连用（也给自己人看的那一面）。
    pub fn snapshot(&self, session_id: &str) -> Vec<MessageSnapshot> {
        let sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        sessions
            .get(session_id)
            .map(|session| session.messages.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Session {
    fn new(keep: usize) -> Self {
        let (frames, _) = broadcast::channel(keep);
        Self {
            next_seq: 0,
            frames,
            messages: VecDeque::new(),
        }
    }
}

/// 这一段的此刻：最后一帧的序号（含）与要说的话。登记之后产生的帧序号一定更大。
fn snapshot_of(inner: &Inner, session_id: &str) -> (u64, Vec<MessageSnapshot>) {
    let mut sessions = inner.sessions.lock().unwrap_or_else(|e| e.into_inner());
    let session = sessions
        .entry(session_id.to_string())
        .or_insert_with(|| Session::new(inner.keep));
    (
        session.next_seq,
        session.messages.iter().cloned().collect::<Vec<_>>(),
    )
}

/// 把「说到哪儿了」交给这条连接。`false` 是这条连接已经走了。
async fn send_snapshot(
    out: &mpsc::Sender<String>,
    session_id: &str,
    messages: &[MessageSnapshot],
) -> bool {
    let Ok(frame) = serde_json::to_string(&crate::websocket_protocol::ServerMessage::ChatResume {
        session_id: session_id.to_string(),
        messages: messages.to_vec(),
    }) else {
        return true;
    };
    out.send(frame).await.is_ok()
}

/// 一条帧对快照的影响。
///
/// 只认会说给人看的那几族（见模块注释）；其余帧原样发出去、不进快照。
fn note(messages: &mut VecDeque<MessageSnapshot>, event_type: &str, inner: &serde_json::Value) {
    let message_id = inner
        .get("message_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if message_id.is_empty() {
        return;
    }
    let at = |messages: &mut VecDeque<MessageSnapshot>| {
        messages.iter().position(|m| m.message_id == message_id)
    };
    match event_type {
        "message.start" => {
            let message = MessageSnapshot {
                message_id: message_id.to_string(),
                role: inner
                    .get("role")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("assistant")
                    .to_string(),
                text: String::new(),
                thinking: String::new(),
                attachment: inner.get("attachment").filter(|a| !a.is_null()).cloned(),
                ended: false,
            };
            // 同一个号又说了一遍「开始」= 这一句重新从头说，旧的那条整条替换。
            match at(messages) {
                Some(index) => messages[index] = message,
                None => messages.push_back(message),
            }
        }
        "message.text_delta" => {
            if let Some(index) = at(messages) {
                if let Some(delta) = inner.get("delta").and_then(serde_json::Value::as_str) {
                    messages[index].text.push_str(delta);
                }
            }
        }
        "message.thinking_delta" => {
            if let Some(index) = at(messages) {
                if let Some(delta) = inner.get("delta").and_then(serde_json::Value::as_str) {
                    messages[index].thinking.push_str(delta);
                }
            }
        }
        "message.end" => {
            if let Some(index) = at(messages) {
                messages[index].ended = true;
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::ChatStream;
    use serde_json::json;
    use tokio::sync::mpsc;

    // 测试帧就是它自己那一段 JSON：真帧是套在信封里发的，这里只关心转发出去了什么。
    fn start(live: &ChatStream, session: &str, message: &str, role: &str) {
        let inner = json!({ "message_id": message, "role": role, "attachment": null });
        live.publish(session, "message.start", &inner, inner.to_string());
    }

    fn delta(live: &ChatStream, session: &str, message: &str, text: &str) {
        let inner = json!({ "message_id": message, "delta": text });
        live.publish(session, "message.text_delta", &inner, inner.to_string());
    }

    /// 断线时那一轮说的话不丢：连接走了，字照样记着，重连拿到的快照里有。
    #[tokio::test]
    async fn a_turn_that_kept_talking_while_nobody_listened_is_still_there() {
        let live = ChatStream::new(16);
        start(&live, "s1", "m-1", "assistant");
        for piece in ["先", "说", "一句"] {
            delta(&live, "s1", "m-1", piece);
        }
        let snapshot = live.snapshot("s1");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].message_id, "m-1");
        assert_eq!(snapshot[0].text, "先说一句");
        assert!(!snapshot[0].ended);
    }

    /// 重连拿到的快照是替换而不是追加：同一句话接两次也只有一份字。
    #[tokio::test]
    async fn reconnecting_twice_does_not_double_the_text() {
        let live = ChatStream::new(16);
        start(&live, "s1", "m-1", "assistant");
        delta(&live, "s1", "m-1", "前半");
        let first = live.snapshot("s1")[0].text.clone();
        delta(&live, "s1", "m-1", "后半");
        let second = live.snapshot("s1")[0].text.clone();
        assert_eq!(first, "前半");
        assert_eq!(second, "前半后半");
        // 快照里每一条消息只出现一次，序号推进不会让它变成两格。
        assert_eq!(live.snapshot("s1").len(), 1);
    }

    /// 接上之后先收到快照，再收到之后的每一帧；快照之前的帧不重发。
    #[tokio::test]
    async fn a_watcher_gets_the_past_once_and_every_later_frame() {
        let live = ChatStream::new(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        start(&live, "s1", "m-1", "assistant");
        delta(&live, "s1", "m-1", "已经说过的");
        live.watch("s1", out_tx);
        // 快照：一段话，正文是断线前说到的那个字。
        let snapshot = out_rx.recv().await.expect("snapshot frame");
        let value: serde_json::Value = serde_json::from_str(&snapshot).expect("JSON");
        assert_eq!(value["type"], "chat_resume");
        assert_eq!(value["messages"][0]["text"], "已经说过的");
        // 之后说的每一句都来，且只有一次。
        delta(&live, "s1", "m-1", "接着说的");
        let frame = out_rx.recv().await.expect("delta frame");
        assert!(frame.contains("接着说的"), "{frame}");
        assert!(out_rx.try_recv().is_err(), "快照里已经有的帧不该再发一遍");
    }

    /// 每一段各有一份快照：另一段对话的句子不会跑进来。
    #[tokio::test]
    async fn one_conversation_does_not_leak_into_another() {
        let live = ChatStream::new(16);
        start(&live, "s1", "m-1", "assistant");
        delta(&live, "s1", "m-1", "这一段");
        start(&live, "s2", "m-2", "assistant");
        delta(&live, "s2", "m-2", "那一段");
        assert_eq!(live.snapshot("s1")[0].text, "这一段");
        assert_eq!(live.snapshot("s2")[0].text, "那一段");
    }

    /// 一句话说完之后再重连，看到的是完整的、收尾了的那一句。
    #[tokio::test]
    async fn a_finished_sentence_reads_as_finished() {
        let live = ChatStream::new(16);
        start(&live, "s1", "m-1", "assistant");
        delta(&live, "s1", "m-1", "答完了");
        live.publish(
            "s1",
            "message.end",
            &json!({ "message_id": "m-1" }),
            "{}".into(),
        );
        let snapshot = live.snapshot("s1");
        assert_eq!(snapshot[0].text, "答完了");
        assert!(snapshot[0].ended);
    }
}
