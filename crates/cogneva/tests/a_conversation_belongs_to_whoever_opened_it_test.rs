//! 会话归属：一段对话只交给写下它的那个人，别人拿同一个会话号什么也读不到。
//!
//! 会话号是猜得出来的（它就是一段文本），所以判据只有一条：这一段**记下的那个人**
//! 是不是现在说话的人。判的是记录，不是请求里带了什么。
//!
//! 五种结局各有各的话，混成两句就会把人往错的方向引：
//!
//! - **自己的**：读得到，返回里标明不是共享的；
//! - **没有属主**（写它的那条连接没有登录身份）：谁都读得到，而返回里写明它是共享的
//!   ——不把「没有主人」画成「属于你」；
//! - **别人的**：只有一句 `session_not_found`，与另一段别人的对话一字不差。多一种
//!   文案，就是给猜会话号的人一条「这一段存在」的读数；
//! - **一个谁都没写过字的名字**：200 + 空记录。它没有主人，所以是公共场所的入口；
//!   新开一段会话时页面读的就是这条路，它必须是一张空记录，而不是一句错误。它与
//!   上一行的区别只说「这个名字下面有没有别人的字」，不说是谁的；
//! - **判不了**（存储抖了）：拒（503）。把「判不了」画成「没有主人」，等于在那一瞬
//!   把别人的对话交给猜中号的人。
//!
//! 读、说、停、换模型、续流五条路共用同一把尺子；任何一条漏了，冒用会话号就有一个
//! 入口。读面在本文件里跑真的路由；说、停、换、续的接线读源码——套接字路径没有在
//! 测试里起服务器（同 `web_chat_works_like_a_workspace_test` 那一族的判法）。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{ChatTurn, ChatTurnRecord, ChatTurnRole, ChatTurnStore, SFResult};
use cog_gateway::sessions::SessionAccess;
use cog_gateway::{create_router, GatewayState};

use crate::common::routes::read_repo;
use crate::router_state;

const SESSIONS: &str = "crates/cog-gateway/src/sessions.rs";
const SOCKET: &str = "crates/cog-gateway/src/websocket.rs";

/// 一份只有 `owner_of` 与 `transcript` 是真的记录：这两问就是这条判据的判据。
#[derive(Default)]
struct Transcript {
    owners: Mutex<std::collections::HashMap<String, Result<Option<String>, String>>>,
    rows: Mutex<std::collections::HashMap<String, Vec<ChatTurnRecord>>>,
}

impl Transcript {
    fn owned_by(&self, session: &str, owner: &str) -> &Self {
        self.owners
            .lock()
            .unwrap()
            .insert(session.into(), Ok(Some(owner.into())));
        self
    }

    fn unowned(&self, session: &str) -> &Self {
        self.owners.lock().unwrap().insert(session.into(), Ok(None));
        self
    }

    fn unreadable(&self, session: &str) -> &Self {
        self.owners
            .lock()
            .unwrap()
            .insert(session.into(), Err("connection reset".into()));
        self
    }

    fn answered(&self, session: &str, question: &str, answer: &str) -> &Self {
        let at = chrono::Utc::now();
        let rows = vec![
            ChatTurnRecord {
                id: 1,
                role: ChatTurnRole::User,
                content: question.into(),
                failure: None,
                goal: None,
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                agent: None,
                created_at: at,
            },
            ChatTurnRecord {
                id: 2,
                role: ChatTurnRole::Assistant,
                content: answer.into(),
                failure: None,
                goal: None,
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                agent: None,
                created_at: at,
            },
        ];
        self.rows.lock().unwrap().insert(session.into(), rows);
        self
    }
}

#[async_trait]
impl ChatTurnStore for Transcript {
    async fn record_turn(
        &self,
        _session_id: &str,
        _owner: &str,
        _user: &str,
        _model_content: Option<&str>,
        _attachments: &[cog_core::ChatAttachment],
        _assistant: &str,
        _tool_calls: &[cog_core::ChatToolCall],
        _agent: Option<&str>,
    ) -> SFResult<()> {
        unreachable!("这条判据只读记录")
    }

    async fn record_dispatched_turn(
        &self,
        _session_id: &str,
        _owner: &str,
        _user: &str,
        _model_content: Option<&str>,
        _attachments: &[cog_core::ChatAttachment],
        _assistant: &str,
        _goal_id: &str,
        _tool_calls: &[cog_core::ChatToolCall],
        _agent: Option<&str>,
    ) -> SFResult<()> {
        unreachable!("这条判据只读记录")
    }

    async fn record_failed_turn(
        &self,
        _session_id: &str,
        _owner: &str,
        _user: &str,
        _attachments: &[cog_core::ChatAttachment],
        _failure: &str,
        _tool_calls: &[cog_core::ChatToolCall],
        _agent: Option<&str>,
    ) -> SFResult<()> {
        unreachable!("这条判据只读记录")
    }

    async fn history(&self, _session_id: &str, _limit: usize) -> SFResult<Vec<ChatTurn>> {
        unreachable!("读面走的是 transcript")
    }

    async fn transcript(&self, session_id: &str, _limit: usize) -> SFResult<Vec<ChatTurnRecord>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn owner_of(&self, session_id: &str) -> SFResult<Option<String>> {
        match self.owners.lock().unwrap().get(session_id) {
            Some(Ok(owner)) => Ok(owner.clone()),
            Some(Err(error)) => Err(cog_core::SFError::Internal(error.clone())),
            // 一行都没记过的那一段：没有人名，也就是共享的。
            None => Ok(None),
        }
    }

    async fn sessions(
        &self,
        _owner: &str,
        _limit: usize,
        _search: Option<&str>,
    ) -> SFResult<Vec<cog_core::ChatSessionSummary>> {
        unreachable!("名单不在这一条判据里")
    }
}

fn state(transcript: Arc<Transcript>) -> Arc<GatewayState> {
    router_state::with_chat_turns(Some(transcript))
}

/// 令牌里 `sub` 那一格才是说话人的身份：记录里存下的属主也是它，不是用户名。
/// （套接字的 `conn.user_id` 与读面的 `Claims.sub` 是同一格。）
fn subject_of(token: &str) -> String {
    use base64::Engine as _;
    let payload = token.split('.').nth(1).expect("JWT 是三段");
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .expect("JWT 的载荷是 base64url");
    let claims: serde_json::Value = serde_json::from_slice(&bytes).expect("JWT 的载荷是 JSON");
    claims["sub"].as_str().expect("登录令牌里有 sub").into()
}

/// GET 一段对话的记录，带上（或带上一个空）Bearer。
async fn read(
    state: &Arc<GatewayState>,
    session: &str,
    token: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method("GET")
        .uri(format!("/api/v1/sessions/{session}/messages"));
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let response = create_router(state.clone())
        .oneshot(request.body(Body::empty()).expect("well formed"))
        .await
        .expect("the router answers");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body reads");
    let body = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// 自己的那一段读得到，而且返回里不冒充「共享」。
#[tokio::test]
async fn the_owner_reads_her_own_conversation() {
    let transcript = Arc::new(Transcript::default());
    let state = state(transcript.clone());
    let token = router_state::member_bearer(&state, "alice").await;
    transcript
        .owned_by("s-alice", &subject_of(&token))
        .answered("s-alice", "这一段是谁的", "你的");

    let (status, body) = read(&state, "s-alice", Some(&token)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"]["shared"], false,
        "自己的对话不该被标成共享：{body}"
    );
    let items = body["data"]["items"].as_array().expect("items is a list");
    assert_eq!(items.len(), 2, "{body}");
    assert_eq!(items[0]["content"], "这一段是谁的");
    assert_eq!(items[1]["content"], "你的");
}

/// 别人的那一段对读的人就是「不存在」：只有一句 `session_not_found`，正文里不留
/// 一个字、也不多一句「存在，但不是你的」。
///
/// 从来没有人在下面写过字的名字走另一条路：200 + 空记录。那不是「别人的对话」，
/// 是一个还没有人写过字的公共场所——新开一段会话时页面读的就是这条路，它必须是
/// 一张空记录而不是一句错误。两条路的区别只在于「这个名字下面有没有别人的话」，
/// 谁也不告诉读的人那是谁的话。
#[tokio::test]
async fn somebody_elses_conversation_answers_as_if_it_did_not_exist() {
    let transcript = Arc::new(Transcript::default());
    let state = state(transcript.clone());
    let alices = router_state::member_bearer(&state, "alice").await;
    transcript
        .owned_by("s-alice", &subject_of(&alices))
        .answered("s-alice", "这一段是谁的", "不是你的")
        // 有主、但一个字也没说过的一段：404 的答法不能被「有没有内容」左右。
        .owned_by("s-silent", &subject_of(&alices));
    let token = router_state::member_bearer(&state, "bob").await;
    assert_ne!(
        subject_of(&token),
        subject_of(&alices),
        "两次登录拿到了同一个身份：这一条判据证不了任何事"
    );

    let (theirs, their_body) = read(&state, "s-alice", Some(&token)).await;
    let (silent, silent_body) = read(&state, "s-silent", Some(&token)).await;

    assert_eq!(theirs, StatusCode::NOT_FOUND, "{their_body}");
    assert_eq!(silent, StatusCode::NOT_FOUND, "{silent_body}");
    assert_eq!(their_body["error"], "session_not_found");
    assert!(
        their_body.get("data").is_none(),
        "404 的正文里带了一份记录：{their_body}"
    );
    // 正文里唯一允许不同的是被回显的会话号——那是问她自己的输入。把这一格抹掉之后，
    // 两段别人的对话答得必须一模一样：多一种写法，就是一条「这一段长什么样」的读数。
    let normalised = |body: &serde_json::Value| {
        let mut body = body.clone();
        body["session_id"] = serde_json::Value::Null;
        body
    };
    assert_eq!(
        normalised(&their_body),
        normalised(&silent_body),
        "两段别人的对话答出了不同的正文"
    );

    let (unused, unused_body) = read(&state, "s-nobody-ever-wrote", Some(&token)).await;
    assert_eq!(unused, StatusCode::OK, "{unused_body}");
    assert_eq!(
        unused_body["data"]["shared"], true,
        "一个没人写过字的名字被标成了「属于你」：{unused_body}"
    );
    assert_eq!(
        unused_body["data"]["items"].as_array().map(Vec::len),
        Some(0),
        "{unused_body}"
    );
}

/// 没有属主的那一段谁都读得到，但返回里必须写明它是共享的。
#[tokio::test]
async fn a_conversation_nobody_owns_says_it_is_shared() {
    let transcript = Arc::new(Transcript::default());
    let state = state(transcript.clone());
    transcript
        .unowned("s-open")
        .answered("s-open", "谁都能看的一段", "对");
    let token = router_state::member_bearer(&state, "alice").await;

    let (status, body) = read(&state, "s-open", Some(&token)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["data"]["shared"], true,
        "没有属主的对话被画成「属于你」：读的人不知道这一段是公开的"
    );
}

/// 没有登录身份就没有「只属于你」可判：拒，而不是给一张空记录。
#[tokio::test]
async fn nobody_reads_a_conversation_without_a_login() {
    let transcript = Arc::new(Transcript::default());
    let state = state(transcript.clone());
    transcript.owned_by("s-alice", "somebody");

    let (status, body) = read(&state, "s-alice", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_ne!(
        body["data"]["items"].as_array().map(Vec::len),
        Some(0),
        "匿名读到了一份空记录：「这一段没有说过话」与「不知道你是谁」是两句不同的话"
    );
}

/// 判不了属主就拒：存储抖一下不该变成「没有主人」。
#[tokio::test]
async fn a_store_that_cannot_say_who_owns_it_refuses() {
    let transcript = Arc::new(Transcript::default());
    let state = state(transcript.clone());
    transcript.unreadable("s-alice");

    let token = router_state::member_bearer(&state, "alice").await;
    let (status, body) = read(&state, "s-alice", Some(&token)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("判不了"),
        "拒的时候要说清是「判不了」：{body}"
    );
}

/// 说、停、换模型共用同一把尺子，读的也是记录。
#[tokio::test]
async fn speaking_in_someone_elses_conversation_is_refused() {
    let transcript = Arc::new(Transcript::default());
    let state = state(transcript.clone());
    transcript
        .owned_by("s-alice", "alice")
        .unowned("s-open")
        .unreadable("s-broken");

    assert!(
        cog_gateway::sessions::speaks_here(&state, "s-alice", "alice")
            .await
            .is_ok(),
        "自己的对话说不了话"
    );
    assert!(
        cog_gateway::sessions::speaks_here(&state, "s-open", "bob")
            .await
            .is_ok(),
        "没有属主的对话谁都能说"
    );
    let refused = cog_gateway::sessions::speaks_here(&state, "s-alice", "bob").await;
    assert!(
        refused.is_err(),
        "别人能在一段有主的对话里说话：归属在这一条路上没有生效"
    );
    assert!(
        refused.unwrap_err().contains("属于另一个人"),
        "拒的那句话要说清为什么"
    );
    assert!(
        cog_gateway::sessions::speaks_here(&state, "s-alice", "")
            .await
            .is_err(),
        "没有身份的人在一段有主的对话里有话语权"
    );
    assert!(
        cog_gateway::sessions::speaks_here(&state, "s-broken", "alice")
            .await
            .is_err(),
        "判不了属主时放行了：存储抖一下，猜中号的人就拿到了写权限"
    );

    // 没有记录库的部署：这一段不会留下任何人的话，也就没有别人可保护的东西。
    let bare = router_state::stateless();
    assert!(
        cog_gateway::sessions::speaks_here(&bare, "s-anyone", "alice")
            .await
            .is_ok(),
        "没有记录库的部署连话都不能说了"
    );
}

/// 三格是闭集，判的只有「记录里的人是不是说话的人」。
#[test]
fn who_may_read_is_decided_by_the_name_on_the_row() {
    assert_eq!(
        cog_gateway::sessions::access(Some("alice"), "alice"),
        SessionAccess::Own
    );
    assert_eq!(
        cog_gateway::sessions::access(Some("alice"), "bob"),
        SessionAccess::SomebodyElses
    );
    assert_eq!(
        cog_gateway::sessions::access(None, "bob"),
        SessionAccess::Shared
    );
    assert_eq!(
        cog_gateway::sessions::access(Some("   "), "bob"),
        SessionAccess::Shared,
        "空白名字是「没记下人」，不是「属于一个叫空格的人」"
    );
    assert_eq!(
        cog_gateway::sessions::access(Some("alice"), "  "),
        SessionAccess::SomebodyElses,
        "没有名字的连接对一段有主的对话没有话语权"
    );
}

/// 读面在把记录交出去之前先判归属；说、停、换、续也各判一次。
#[test]
fn every_way_into_a_conversation_checks_the_owner_first() {
    let sessions = read_repo(SESSIONS);
    let handler = sessions
        .split_once("pub async fn list_messages_handler(")
        .unwrap_or_else(|| panic!("{SESSIONS} 里找不到 list_messages_handler"))
        .1;
    let owned = handler
        .find("store.owner_of(&session_id)")
        .unwrap_or_else(|| panic!("读面没有问「这一段属于谁」就把记录交出去了"));
    let reads = handler
        .find("store.transcript(&session_id, limit)")
        .unwrap_or_else(|| panic!("{SESSIONS} 的读面没读记录"));
    assert!(owned < reads, "先判归属，再读记录");
    assert!(
        handler.contains("SessionAccess::SomebodyElses"),
        "别人的对话没有单独的一支：它会顺着共享那条路被交出去"
    );
    assert!(
        handler.contains("shared_marking(access)"),
        "返回里没有标明这一段是不是共享的"
    );

    let socket = read_repo(SOCKET);
    // 说话：`refuses_session` 判完才起这一轮。
    let chat = socket
        .split_once("ClientMessage::ChatMessage {")
        .unwrap_or_else(|| panic!("{SOCKET} 里没有 chat_message 这一支"))
        .1;
    let check = chat
        .find("refuses_session(")
        .unwrap_or_else(|| panic!("说话之前没判归属：猜一个会话号就能往别人的对话里写"));
    let start = chat
        .find("start_chat_turn(")
        .unwrap_or_else(|| panic!("{SOCKET} 的这一支没起过一轮？"));
    assert!(check < start, "先判归属，再开口");
    let refuses = socket
        .split_once("async fn refuses_session(")
        .unwrap_or_else(|| panic!("{SOCKET} 里找不到 refuses_session"))
        .1;
    assert!(
        refuses.contains("crate::sessions::speaks_here("),
        "refuses_session 不是用归属那把尺子判的"
    );

    // 停：别人的对话停不了（名册是整台部署共用的）。
    let cancel = socket
        .split_once("ClientMessage::ChatCancel {")
        .unwrap_or_else(|| panic!("{SOCKET} 里没有 chat_cancel 这一支"))
        .1;
    let gate = cancel
        .find("crate::sessions::speaks_here(")
        .unwrap_or_else(|| panic!("停止没判归属：猜一个会话号就能掐掉别人正在跑的那一轮"));
    let stop = cancel
        .find(".running().cancel(")
        .unwrap_or_else(|| panic!("{SOCKET} 的停止没找到名册"));
    assert!(gate < stop, "先判归属，再停");

    // 换模型：换的是这一段对话的属性。
    let model = socket
        .split_once("ClientMessage::ChatModel {")
        .unwrap_or_else(|| panic!("{SOCKET} 里没有 chat_model 这一支"))
        .1;
    let model_gate = model
        .find("crate::sessions::speaks_here(")
        .unwrap_or_else(|| panic!("换模型没判归属：猜一个会话号就能改别人那一段的模型"));
    let write = model
        .find(".choose_model(")
        .unwrap_or_else(|| panic!("{SOCKET} 的换模型从没写下去过"));
    assert!(model_gate < write, "先判归属，再换");

    // 续流：别人的对话里在说什么，与他无关。
    let resume = socket
        .split_once("ClientMessage::ChatResume {")
        .unwrap_or_else(|| panic!("{SOCKET} 里没有 chat_resume 这一支"))
        .1;
    let resume_gate = resume
        .find("crate::sessions::speaks_here(")
        .unwrap_or_else(|| panic!("续流没判归属：猜一个会话号就能读到别人正在写的回答"));
    let attach = resume
        .find("watch_session(")
        .unwrap_or_else(|| panic!("{SOCKET} 的续流没有接上流"));
    assert!(resume_gate < attach, "先判归属，再接上流");
}
