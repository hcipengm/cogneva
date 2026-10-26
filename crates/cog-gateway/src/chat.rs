//! Web UI chat turn — bridges the WebSocket `chat_message` client message to
//! the LLM and streams the reply back to the requesting connection as
//! `agent_event` envelopes (message.start / message.text_delta / message.end),
//! matching what the SPA's `useEventStream` store renders.

use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::websocket_protocol::ServerMessage;
use crate::GatewayState;
use cog_observability::usage_store::LlmOrigin;

/// 一次请求里最多带多少条历史（用户与助手合计）。
///
/// 这是「装得进一次请求」的界，不是「存得下多少」的界：记录本身留在库里，按会话
/// 自己的上界裁剪，见存储面。
const SESSION_HISTORY_CAP: usize = 40;
/// Connecting to the LLM backend should be fast; the failover pool handles
/// slow backends by switching.
const CONNECT_TIMEOUT_SECS: u64 = 30;
/// A reasoning model may think for a long while between deltas; only an idle
/// gap beyond this aborts the turn. Matches the provider's own in-stream cap.
const STREAM_IDLE_TIMEOUT_SECS: u64 = 180;
/// Intent classification is a one-word answer; anything slower means the
/// backend is struggling and we fall back to the plain chat path.
const CLASSIFY_TIMEOUT_SECS: u64 = 20;

/// Intent router prompt. The Web UI chat is an entry point into the system,
/// not a side channel: actionable messages must reach the orchestrator so
/// they flow through decompose → DAG → squad like every other intent, with
/// progress coming back on the event stream. Pure Q&A keeps the direct
/// streaming path.
const INTENT_CLASSIFY_PROMPT: &str =
    "判断用户消息属于哪一类。\
ACT = 可执行意图：要求系统做事情（构建、修改、修复、实现、运行、部署、排查、分析、优化等会产生实际动作或代码变更的请求）。\
CHAT = 纯问答：聊天、知识提问、解释概念、询问系统状态等只需要文字回答的消息。\
只回答一个词：ACT 或 CHAT，不要任何其他内容。";

/// Persona for the Web UI chat. Identity follows the project definition:
/// Cogneva is a distributed AI multi-agent autonomous system, not a Q&A
/// assistant — the configured upstream model only supplies the language
/// capability. Raw history + user text without a system prompt makes
/// coding-tuned endpoints answer terse, generic, and clueless about the
/// platform.
const CHAT_SYSTEM_PROMPT: &str =
    "你是 Cogneva——分布式 AI 多智能体自治系统（元启动·真自治·全进化），一个能自己站起来、自己运转、自己成长的数字生命体。用户正通过系统的 Live 面板与你对话。\
身份口径：你就是 Cogneva 系统本身，不是客服也不是问答助手；语言模型由管理员在设置向导中配置的上游提供，那只是你的语言能力来源，不是你的身份。被问及身份时按此口径回答，可简要介绍三大支柱（元启动解决系统起源、真自治解决系统意志、全进化解决系统成长）。\
用用户的语言回答（中文提问用中文，英文提问用英文）。回答要具体、有实质内容，适当展开但不要啰嗦；不知道就直说不知道，不要编造。";

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Route a chat message: ACT (system should do something) or CHAT (answer
/// directly). Classification failure defaults to CHAT — answering is always
/// safe, silently dropping an intent into a goal is not.
async fn is_actionable_intent(llm: &Arc<dyn cog_core::LlmClient>, content: &str) -> bool {
    let messages = [
        cog_core::Message::system(INTENT_CLASSIFY_PROMPT),
        cog_core::Message::user(content),
    ];
    let options = cog_core::ChatOptions::default()
        .with_actor("intent_gateway")
        // A person is waiting on this classification, so its spend belongs to
        // the interactive environment even though the process it runs in is
        // the one driving the platform's own pool.
        .with_origin(LlmOrigin::Interactive.as_str());
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(CLASSIFY_TIMEOUT_SECS),
        llm.chat(&messages, &options),
    )
    .await;
    match res {
        Ok(Ok(resp)) => resp
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<String>()
            .trim()
            .to_ascii_uppercase()
            .starts_with("ACT"),
        _ => false,
    }
}

fn envelope(session_id: &str, event_type: &str, inner: serde_json::Value) -> String {
    let msg = ServerMessage::AgentEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        session_id: Some(session_id.to_string()),
        task_id: None,
        payload: serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": event_type,
            "timestamp": now_iso(),
            "payload": inner,
        }),
        server_time: now_iso(),
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

async fn emit(
    out: &mpsc::Sender<String>,
    session_id: &str,
    event_type: &str,
    inner: serde_json::Value,
) {
    // Receiver only disappears when the socket loop ended — nothing to do then.
    let _ = out.send(envelope(session_id, event_type, inner)).await;
}

async fn emit_message_start(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    role: &str,
) {
    emit(
        out,
        session_id,
        "message.start",
        serde_json::json!({ "message_id": message_id, "role": role }),
    )
    .await;
}

async fn emit_delta(out: &mpsc::Sender<String>, session_id: &str, message_id: &str, delta: &str) {
    emit(
        out,
        session_id,
        "message.text_delta",
        serde_json::json!({ "message_id": message_id, "delta": delta }),
    )
    .await;
}

async fn emit_message_end(out: &mpsc::Sender<String>, session_id: &str, message_id: &str) {
    emit(
        out,
        session_id,
        "message.end",
        serde_json::json!({ "message_id": message_id }),
    )
    .await;
}

/// 聊天对一次目标提交的回报：只报承载这道目标的那一**行**的实况。
///
/// 不按 `submit_goal_auto` 的返回值判成败：分解判退也是 `Ok`（判词落在目标行的
/// `Failed` 上），按返回值判会把失败报成成功。目标行是读面认这一道目标的那一行，
/// 这里报的就是它——用户在这条消息里看到的与流水线里查到的是同一份事实。
fn goal_report(goal_id: &str, row: Option<&cog_core::Task>) -> String {
    match row {
        Some(row) if row.status == cog_core::TaskStatus::Failed => format!(
            "目标 {goal_id} 分解失败：{}。可以换个更具体的描述再试一次。",
            row.error.as_deref().unwrap_or("编排器没有记下原因")
        ),
        // 分解成功时这一行还没跑完（终态与答案是收口任务跑完后抄回来的），但它上面
        // 有「答案落在哪一条」的声明——没有这条声明就说明没有任何东西会收口。
        Some(row) if row.sink_task_id.is_none() => format!(
            "目标 {goal_id} 已受理，但没有任何一条任务承载它的答案（这次提交没点出收口任务），\
             它不会自己跑完；请重新提交，或检查编排器的分解器是否就绪。"
        ),
        Some(_) => format!(
            "目标 {goal_id} 已受理：分解出的任务已进入执行图，进度与最终答案见右侧目标流水线\
             （按 goal_id={goal_id} 可查）。"
        ),
        None => format!("目标 {goal_id} 没有被记下：这次提交没有留下可查的那一行。"),
    }
}

/// 这一轮给模型看的历史：最近 [`SESSION_HISTORY_CAP`] 条，旧的最先。
///
/// 读不到就按「没有历史」回答，不按「历史是空的」记一笔：这两种在读数上要分得开，
/// 而这一趟能做的只有回答——拿一个读不到的库把聊天停掉，是把「不知道」当成「没有」。
async fn load_history(store: &crate::ChatTurnHandle, session_id: &str) -> Vec<cog_core::Message> {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮没有历史上下文"
        );
        return Vec::new();
    };
    match store.history(session_id, SESSION_HISTORY_CAP).await {
        Ok(turns) => turns_to_messages(turns),
        Err(e) => {
            tracing::warn!(
                error = %e,
                session = %session_id,
                "聊天：历史读不出来，这一轮按没有上下文回答"
            );
            Vec::new()
        }
    }
}

/// 记录里的一行变成模型看得懂的一条。角色的闭集在存储面，这里只做搬运。
fn turns_to_messages(turns: Vec<cog_storage::ChatTurn>) -> Vec<cog_core::Message> {
    turns
        .into_iter()
        .map(|turn| match turn.role {
            cog_storage::ChatTurnRole::User => cog_core::Message::user(turn.content),
            cog_storage::ChatTurnRole::Assistant => cog_core::Message::assistant_text(turn.content),
        })
        .collect()
}

/// 把这一轮写进记录。写不进去只留一条 warn：用户已经在等一个回答，拿「记不下来」
/// 把回答吞掉，比丢一行历史更糟。
async fn record_turn(store: &crate::ChatTurnHandle, session_id: &str, user: &str, assistant: &str) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store.record_turn(session_id, user, assistant).await {
        tracing::warn!(
            error = %e,
            session = %session_id,
            "聊天：这一轮没写进记录"
        );
    }
}

/// 记下这一轮没答出来的那句话，以及没答出来的原因。
///
/// 答不出来的那一轮同样是用户对这个平台说过话的一轮：什么都不写，用户说的那句
/// 就只活在他刚收到的那个响应里，下一次提问的上下文里也没有它——一段对话在最
/// 需要留下证据的时候（上游坏了）反而是空的。原因写在同一行上，读者才知道这一
/// 轮为什么有问无答；将来构建提示词时它不会当成模型说过的话被回放。
async fn record_failed_turn(
    store: &crate::ChatTurnHandle,
    session_id: &str,
    user: &str,
    failure: &str,
) {
    let Some(store) = store else {
        tracing::warn!(
            session = %session_id,
            "聊天：这套部署没有聊天记录库，这一轮不会留下记录"
        );
        return;
    };
    if let Err(e) = store.record_failed_turn(session_id, user, failure).await {
        tracing::warn!(
            error = %e,
            session = %session_id,
            "聊天：这一轮没写进记录"
        );
    }
}

/// Run one chat turn: echo the user message, stream the LLM reply delta by
/// delta (typewriter style — the routing layer still fails over transparently
/// if a backend errors before the first content delta), and record the turn in
/// the session history.
pub async fn run_chat_turn(
    state: Arc<GatewayState>,
    out: mpsc::Sender<String>,
    session_id: String,
    content: String,
) {
    // 1. Echo the user message so the UI renders it (the real transport has
    //    no local echo, unlike the mock client).
    let user_mid = format!("user-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &user_mid, "user").await;
    emit_delta(&out, &session_id, &user_mid, &content).await;
    emit_message_end(&out, &session_id, &user_mid).await;

    let assistant_mid = format!("assistant-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &assistant_mid, "assistant").await;

    let llm = state
        .llm_client
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();

    let Some(llm) = llm else {
        emit_delta(
            &out,
            &session_id,
            &assistant_mid,
            "LLM 未配置或未就绪，无法处理聊天消息。请先在右上角完成 LLM 设置。",
        )
        .await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        return;
    };

    // 2. Route the intent. Actionable messages go to the orchestrator as a
    //    goal (decompose → DAG → squad); progress returns on the event
    //    stream, which the UI renders live. Decomposition takes 50-90s of
    //    LLM planning, so it runs detached and reports back on this
    //    connection when it lands.
    if is_actionable_intent(&llm, &content).await {
        // 这道目标的身份在受理它的这一层定：承载行的 id 就是它，分解出的子任务都挂在
        // 这一行下面。当场定下来才报得出「是哪个目标」，用户也才有得可查——空 `tasks`
        // 那条提交形态同样合法，但那样身份的诞生就落在了分解里面，读的人拿不到。
        let goal_id = uuid::Uuid::new_v4().to_string();
        let ack = format!(
            "收到，这是可执行意图。已作为目标 {goal_id} 提交给编排器：分解为任务后由 squad 接管执行，\
             进度会实时出现在右侧目标流水线（按 goal_id={goal_id} 可查）；分解完成或失败我都会在这里回报。"
        );
        emit_delta(&out, &session_id, &assistant_mid, &ack).await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        // 这一轮已经不再是「一句问答」：它交给编排器了，后面由任务与监督接管。
        // 目标身份单独发一帧——界面要拿它去接「这个目标下的执行者在做什么」。
        // 只写在回复正文里的话，界面只能靠解析一句话去认它，而那句话用户随时
        // 可以让模型改口径。
        emit(
            &out,
            &session_id,
            "message.goal",
            serde_json::json!({ "message_id": assistant_mid, "goal_id": goal_id }),
        )
        .await;
        // 可执行意图这一支也要留痕：下一次追问「刚才那个目标怎么样了」时，历史里
        // 得有这次提交，否则模型只看见一句没头没尾的追问。
        record_turn(&state.chat_turns, &session_id, &content, &ack).await;

        let orchestrator = state.orchestrator.clone();
        let out2 = out.clone();
        let sid = session_id.clone();
        let goal = content.clone();
        tokio::spawn(async move {
            // message.start is emitted only when there is content — the
            // decomposition takes minutes, and an empty bubble that whole
            // time reads as broken.
            let row = cog_core::Task::goal_row(goal_id.clone(), &goal);
            let text = match orchestrator.submit_goal_auto(&goal, vec![row]).await {
                Ok(_) => {
                    // 成败读目标行本身，不读返回值：判退也是 Ok。
                    let row = orchestrator.get_task(&goal_id).await;
                    goal_report(&goal_id, row.as_ref())
                }
                Err(e) => format!("目标 {goal_id} 分解失败：{e}。可以换个更具体的描述再试一次。"),
            };
            let mid = format!("assistant-{}", uuid::Uuid::new_v4());
            emit_message_start(&out2, &sid, &mid, "assistant").await;
            emit_delta(&out2, &sid, &mid, &text).await;
            emit_message_end(&out2, &sid, &mid).await;
        });
        return;
    }

    // 3. Build the request from session history + the new user message.
    let mut messages = load_history(&state.chat_turns, &session_id).await;
    messages.insert(0, cog_core::Message::system(CHAT_SYSTEM_PROMPT));
    messages.push(cog_core::Message::user(content.clone()));

    let options = cog_core::ChatOptions {
        response_format: cog_core::ResponseFormat::Text,
        ..Default::default()
    }
    .with_actor("web_chat")
    // Both chat faces answer a person who is waiting, so both are interactive
    // spend — unlike the pool the same process drives on its own.
    .with_origin(LlmOrigin::Interactive.as_str());

    // 4. Stream the reply: deltas are forwarded as they arrive.
    let mut stream = match tokio::time::timeout(
        std::time::Duration::from_secs(CONNECT_TIMEOUT_SECS),
        llm.chat_stream(&messages, &options),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            emit_delta(
                &out,
                &session_id,
                &assistant_mid,
                &format!("LLM 调用失败：{e}"),
            )
            .await;
            emit_message_end(&out, &session_id, &assistant_mid).await;
            return;
        }
        Err(_) => {
            emit_delta(
                &out,
                &session_id,
                &assistant_mid,
                &format!("LLM 连接超时（>{CONNECT_TIMEOUT_SECS}s），请稍后重试。"),
            )
            .await;
            emit_message_end(&out, &session_id, &assistant_mid).await;
            return;
        }
    };

    let mut reply = String::new();
    let mut failure: Option<String> = None;
    loop {
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
            stream.next(),
        )
        .await;
        match next {
            Ok(Some(cog_core::AssistantMessageEvent::TextDelta { delta, .. })) => {
                reply.push_str(&delta);
                emit_delta(&out, &session_id, &assistant_mid, &delta).await;
            }
            // Reasoning models (thinking forced on at some endpoints) can
            // think for tens of seconds before the first text delta. Forward
            // the thinking stream so the UI shows progress instead of an
            // empty bubble for the whole reasoning window.
            Ok(Some(cog_core::AssistantMessageEvent::ThinkingStart { .. })) => {
                emit(
                    &out,
                    &session_id,
                    "message.thinking_start",
                    serde_json::json!({ "message_id": assistant_mid }),
                )
                .await;
            }
            Ok(Some(cog_core::AssistantMessageEvent::ThinkingDelta { delta, .. })) => {
                emit(
                    &out,
                    &session_id,
                    "message.thinking_delta",
                    serde_json::json!({ "message_id": assistant_mid, "delta": delta }),
                )
                .await;
            }
            Ok(Some(cog_core::AssistantMessageEvent::ThinkingEnd { .. })) => {
                emit(
                    &out,
                    &session_id,
                    "message.thinking_end",
                    serde_json::json!({ "message_id": assistant_mid }),
                )
                .await;
            }
            Ok(Some(cog_core::AssistantMessageEvent::Error { error, .. })) => {
                failure = Some(error.content());
                break;
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                failure = Some(format!(
                    "LLM 响应超时（>{STREAM_IDLE_TIMEOUT_SECS}s 无输出）"
                ));
                break;
            }
        }
    }

    // Reasoning-only replies can arrive with no text deltas at all; fall back
    // to the final response payload so the user never sees an empty bubble.
    if failure.is_none() && reply.is_empty() {
        let final_resp = stream.result().await;
        if let Some(err) = final_resp.error_message {
            failure = Some(err);
        } else {
            reply = final_resp
                .content
                .iter()
                .filter_map(|b| b.as_text())
                .collect::<Vec<_>>()
                .join("");
            if !reply.is_empty() {
                emit_delta(&out, &session_id, &assistant_mid, &reply).await;
            }
        }
    }

    if let Some(err) = &failure {
        emit_delta(
            &out,
            &session_id,
            &assistant_mid,
            &format!("LLM 调用失败：{err}"),
        )
        .await;
    }
    emit_message_end(&out, &session_id, &assistant_mid).await;

    // 5. Record the turn. 失败的那一轮也写：用户说的这句话是对这个平台说过的，
    //    模型没答出来是这一轮的一个事实，不是把它忘掉的许可。空回复按失败写，
    //    因为它对用户而言和一个错误没有分别，而「什么都不写」是最坏的那种差别。
    match (&failure, reply.is_empty()) {
        (Some(err), _) => {
            record_failed_turn(&state.chat_turns, &session_id, &content, err).await;
        }
        (None, true) => {
            record_failed_turn(
                &state.chat_turns,
                &session_id,
                &content,
                "模型没有返回任何内容",
            )
            .await;
        }
        (None, false) => record_turn(&state.chat_turns, &session_id, &content, &reply).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_storage::ChatTurnStore;

    /// 判退落在目标行的 `Failed` 上、`submit_goal_auto` 照样返回 `Ok`——所以回报必须
    /// 读那一行，否则失败会被报成「已受理，进度见右侧」，用户去右侧只会等一个不会来的
    /// 结果。
    #[test]
    fn a_refused_decomposition_is_reported_as_the_failure_the_row_records() {
        let mut row = cog_core::Task::goal_row("goal-1", "fix the thing");
        row.status = cog_core::TaskStatus::Failed;
        row.error = Some("decomposition produced no executable tasks after 3 attempt(s)".into());
        let text = goal_report("goal-1", Some(&row));
        assert!(text.contains("分解失败"), "{text}");
        assert!(text.contains("no executable tasks"), "{text}");
        assert!(!text.contains("已受理"), "{text}");
    }

    /// 目标身份必须走自己的一帧。
    ///
    /// 它现在只写在回复正文里也能被读到——但那是让人去解析一句话，而这句话的措辞
    /// 随提示词改。界面靠名字认这一帧：名字改了，界面不会报错，只是从此再也接不上
    /// 「这道目标下的执行者在做什么」。所以名字与两个字段一起钉在这里。
    #[test]
    fn a_goal_submission_carries_its_identity_in_its_own_frame() {
        let raw = envelope(
            "s1",
            "message.goal",
            serde_json::json!({ "message_id": "m-1", "goal_id": "goal-9" }),
        );
        let value: serde_json::Value = serde_json::from_str(&raw).expect("frame is JSON");
        let event = &value["payload"];
        assert_eq!(event["type"], "message.goal");
        assert_eq!(event["payload"]["goal_id"], "goal-9");
        assert_eq!(event["payload"]["message_id"], "m-1");
        // 会话身份在信封上，不在帧体里：界面按它把这一帧落回哪一段对话。
        assert_eq!(value["session_id"], "s1");
    }

    /// 分解成功：行还是 Pending（答案要等收口任务跑完才抄回来），但声明已经在了，
    /// 回报要给出可查的 goal_id 而不是一句「已注入 N 个任务」。
    #[test]
    fn an_accepted_goal_is_reported_with_its_id_and_the_pipeline_it_lands_in() {
        let mut row = cog_core::Task::goal_row("goal-2", "fix the thing");
        row.sink_task_id = Some("child-7".into());
        let text = goal_report("goal-2", Some(&row));
        assert!(text.contains("goal-2"), "{text}");
        assert!(text.contains("已受理"), "{text}");
        assert!(!text.contains("分解失败"), "{text}");
    }

    /// 没有任何东西会收口时必须说出来：受理了却一条承载答案的任务都没有，用户等的是
    /// 一件不会发生的事。
    #[test]
    fn a_goal_no_task_will_close_says_so_instead_of_pointing_at_the_pipeline() {
        let row = cog_core::Task::goal_row("goal-3", "fix the thing");
        assert!(row.sink_task_id.is_none());
        let text = goal_report("goal-3", Some(&row));
        assert!(text.contains("不会自己跑完"), "{text}");
        assert!(
            !text.contains("进度与最终答案"),
            "it must not point the user at a pipeline for an answer nothing will produce: {text}"
        );
    }

    /// 一行都没留下（没有 DagExecutor 之类）：报「没被记下」，不报「已受理」。
    #[test]
    fn a_submission_that_left_no_row_is_not_reported_as_accepted() {
        let text = goal_report("goal-4", None);
        assert!(text.contains("没有被记下"), "{text}");
        assert!(!text.contains("已受理"), "{text}");
    }

    /// 历史从记录里来，不从这个进程的内存里来。
    ///
    /// 这一格替换掉的是那个 `HashMap`：它在同一个进程里读自己刚写的东西，所以
    /// 「历史还在」这件事在重启之前永远为真——而重启正是要证明的那一件事。
    #[tokio::test]
    async fn the_history_the_model_sees_comes_from_the_store() {
        let store = std::sync::Arc::new(FakeTurns::default());
        store
            .record_turn("s1", "上一问", "上一答")
            .await
            .expect("seed the fake transcript");
        let handle: crate::ChatTurnHandle = Some(store.clone());

        let messages = load_history(&handle, "s1").await;
        assert_eq!(
            messages.iter().map(|m| m.role()).collect::<Vec<_>>(),
            vec!["user", "assistant"]
        );
        assert_eq!(messages[0].content(), "上一问");
        assert_eq!(messages[1].content(), "上一答");
        // 上下文窗口的界要有人传给存储面，否则「最近 40 条」只是这里的一句注释。
        assert_eq!(*store.last_limit.lock().await, Some(SESSION_HISTORY_CAP));
    }

    /// 回答出来的这一轮必须写进记录，且是作为一问一答一起写。
    #[tokio::test]
    async fn an_answered_turn_is_written_down_as_a_pair() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_turn(&handle, "s1", "问", "答").await;
        let held = store.turns.lock().await.clone();
        assert_eq!(
            held,
            vec![
                cog_storage::ChatTurn {
                    role: cog_storage::ChatTurnRole::User,
                    content: "问".into()
                },
                cog_storage::ChatTurn {
                    role: cog_storage::ChatTurnRole::Assistant,
                    content: "答".into()
                },
            ]
        );
    }

    /// 没答出来的那一轮同样是用户说过的一句话，必须留下。
    ///
    /// 上游不可用的那段时间，这个平台收到的每一句话原本都会消失：记录只在
    /// 回答送达后才写。这里要的正是反面——问句和原因都进记录，而构建上下文时
    /// 它不冒充模型说过的话。
    #[tokio::test]
    async fn an_unanswered_turn_still_keeps_what_the_user_said() {
        let store = std::sync::Arc::new(FakeTurns::default());
        let handle: crate::ChatTurnHandle = Some(store.clone());
        record_failed_turn(&handle, "s1", "问", "upstream 503").await;

        let held = store.unanswered.lock().await.clone();
        assert_eq!(
            held,
            vec![("问".to_string(), "upstream 503".to_string())],
            "问句与原因都要落在记录里"
        );
        // 它不进上下文：失败通知不是模型说过的话。
        assert!(store.turns.lock().await.is_empty());
    }

    /// 没有记录库的部署：照样回答，不假装有历史，也不悄悄记在别处。
    #[tokio::test]
    async fn a_deployment_without_a_store_answers_without_history() {
        let handle: crate::ChatTurnHandle = None;
        assert!(load_history(&handle, "s1").await.is_empty());
        // 不发散：这里只要求「不写进任何东西」——没有第二个家可写。
        record_turn(&handle, "s1", "问", "答").await;
        record_failed_turn(&handle, "s1", "问", "答不出来").await;
    }

    /// 读记录失败按「没有历史」回答，而不是按「历史是空的」记一笔。
    #[tokio::test]
    async fn a_transcript_that_cannot_be_read_answers_without_context() {
        let store = std::sync::Arc::new(FakeTurns::default());
        store
            .record_turn("s1", "问", "答")
            .await
            .expect("seed the fake transcript");
        store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        let handle: crate::ChatTurnHandle = Some(store.clone());
        assert!(load_history(&handle, "s1").await.is_empty());
        // 写也失败时不许把这一轮的回答吞掉：record_turn 只留 warn。
        record_turn(&handle, "s1", "问", "答").await;
    }

    /// 一份内存里的记录，只作测试替身。生产里没有它的对应物——这次改动拆掉的
    /// 正是那样一张表。
    #[derive(Default)]
    struct FakeTurns {
        turns: tokio::sync::Mutex<Vec<cog_storage::ChatTurn>>,
        /// 有问无答的那几轮：留在记录里，但不进上下文（`history` 不返回它们）。
        unanswered: tokio::sync::Mutex<Vec<(String, String)>>,
        last_limit: tokio::sync::Mutex<Option<usize>>,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl cog_storage::ChatTurnStore for FakeTurns {
        async fn record_turn(
            &self,
            session_id: &str,
            user: &str,
            assistant: &str,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            let mut held = self.turns.lock().await;
            if session_id != "s1" {
                return Ok(());
            }
            held.push(cog_storage::ChatTurn {
                role: cog_storage::ChatTurnRole::User,
                content: user.to_string(),
            });
            held.push(cog_storage::ChatTurn {
                role: cog_storage::ChatTurnRole::Assistant,
                content: assistant.to_string(),
            });
            Ok(())
        }

        async fn record_failed_turn(
            &self,
            session_id: &str,
            user: &str,
            failure: &str,
        ) -> cog_core::SFResult<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            if session_id != "s1" {
                return Ok(());
            }
            self.unanswered
                .lock()
                .await
                .push((user.to_string(), failure.to_string()));
            Ok(())
        }

        async fn history(
            &self,
            _session_id: &str,
            limit: usize,
        ) -> cog_core::SFResult<Vec<cog_storage::ChatTurn>> {
            *self.last_limit.lock().await = Some(limit);
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            Ok(self.turns.lock().await.clone())
        }

        /// 读回整段会话（含失败轮），按时间正序截取最后 `limit` 条。
        ///
        /// 与 `history` 的差别只在「失败轮算不算」：上下文里不放它们，屏幕上要放。
        async fn transcript(
            &self,
            _session_id: &str,
            limit: usize,
        ) -> cog_core::SFResult<Vec<cog_storage::ChatTurnRecord>> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(cog_core::SFError::Database("unreachable".into()));
            }
            if limit == 0 {
                return Ok(Vec::new());
            }
            let turns = self.turns.lock().await;
            let start = turns.len().saturating_sub(limit);
            Ok(turns[start..]
                .iter()
                .enumerate()
                .map(|(offset, turn)| cog_storage::ChatTurnRecord {
                    id: (start + offset) as i64 + 1,
                    role: turn.role,
                    content: turn.content.clone(),
                    failure: None,
                    created_at: chrono::Utc::now(),
                })
                .collect())
        }
    }

    #[test]
    fn envelope_matches_frontend_contract() {
        let raw = envelope(
            "s1",
            "message.start",
            serde_json::json!({"message_id": "m1", "role": "assistant"}),
        );
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["type"], "agent_event");
        assert_eq!(v["session_id"], "s1");
        assert_eq!(v["payload"]["type"], "message.start");
        assert_eq!(v["payload"]["payload"]["message_id"], "m1");
        assert_eq!(v["payload"]["payload"]["role"], "assistant");
    }
}
