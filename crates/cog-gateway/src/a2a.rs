//! A2A 服务端：`message/stream` —— 一条 message 进来，任务状态作为 SSE 流出去。
//!
//! 这张名片一直在对外的 Agent Card 上宣称 `streaming: true` 与
//! `state_transition_history: true`，而这两个承诺指名的那个端点从来不存在：
//! 名片是网关唯一一条 A2A 路由，`message/stream` 一次都没被服务过。一个挂着
//! 「能流式」的旗、旗后什么都没有的名片，是平台对外的**假承诺**——对端按旗子
//! 去开流，只会撞上 404。
//!
//! 修法是把旗子兑现，不是把旗子摘掉：摘掉等于宣告 cogneva 不能作为流式的 A2A
//! 对端参与协作，那是把能力削掉来对齐文档。所以这里补的是旗子指名的那个方法。
//!
//! 状态从哪来：cogneva 里一次被委派的工作就是**一个 goal**，而 `message/stream`
//! 的一条 message 落成的正是 `POST /api/v1/tasks` 会建的那一行（同一个
//! `Task::goal_row` 构造、同一个 id）。于是流上报的状态与 HTTP 面读到的状态是
//! 同一行，不存在第二套「A2A 视图里的任务」——一个平台里两套任务状态，迟早各自
//! 漂。goal 分解出来的那张 DAG 是实现细节，不属于 A2A 任务。
//!
//! 状态只是这条流的一半。对端在另一个进程里，读不到本平台的任务表：流若只报
//! 「完成」，它能知道的只有跑完了，不知道做成了什么。所以终态那一帧同时带上这一行
//! 的 `result` 与 `error`，各占一件产物——合成一件之后，「跑完了但没有产出」与
//! 「没跑完」在客户端读成同一个形状。

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::stream::Stream;
use serde_json::{json, Value};

use cog_core::{A2aTaskStatus, MessagePart, OrchestratorControl, Task, TaskArtifact, TaskStatus};

use crate::GatewayState;

// ─── 端点、旗子，与那张「哪杆旗指着哪张端点」的表 ──────────────────────────

/// 本模块服务的那张 A2A 端点。路由注册与名片上的旗子都读这一处。
///
/// 旗与端点分开写，就是这张名片原来的样子：旗写着 `streaming: true`，而全仓
/// 没有一条路径含 `message/stream`。把路径提成一个常量并不神奇，它买到的是
/// 「改端点的人与改旗的人看的是同一行」——而这张名片从没被兑现过，正是因为这
/// 两件事当时分别落在两个文件的两个字面量上。
pub const MESSAGE_STREAM: &str = "/api/v1/protocol/a2a/message/stream";

/// 名片上那个 `url` 指向的端点：**一个地址，方法写在 JSON-RPC 的 `method` 里**。
///
/// A2A 的对端拿到名片后只认这一个地址，按 `method` 分派。只给一条
/// `/message/stream` 路径，等于只服务会流式的对端：不流式的那一类要用的是
/// `message/send` 与 `tasks/get`，两条都不在路径表上，它照名片打过来得到的是 404。
/// 端点补上之后，名片上的 `url` 也指到这里，不再指一个没有路由的根。
pub const A2A_RPC: &str = "/api/v1/protocol/a2a";

/// A2A 的流式方法名。一杆旗成立的条件是「表里有一行，它指名的端点以这个名字
/// 结尾」，不是「那一行非空」：端点换个前缀照旧成立，指到别的路由上旗子就灭。
const A2A_STREAM_METHOD: &str = "message/stream";

/// 名片上的一杆旗，以及这杆旗指名的端点。
pub struct Advertised {
    /// 名片上的字段名，与 [`cog_core::AgentCapabilities`] 的字段一一对应。
    pub capability: &'static str,
    /// 这杆旗指名的路径，与路由注册读的是同一个常量。
    pub endpoint: &'static str,
}

/// 名片宣称了哪几杆旗、各指着哪张端点。**没有对应端点的能力不进这张表**。
///
/// 表本身买到的是「旗由端点算出」：一杆旗的真假不再是函数体里那个字面量，
/// 而是「这里有没有一行」。于是「旗亮着、端点不存在」这件事有了一份待查清单——
/// 对端就是照这张清单动作的：它读名片上 `streaming: true`，然后请求旗子指名的
/// 那张路径。清单在，才有东西可查；只读常量与旗子查不出「路由被删了」，那两处
/// 会一起留在原处。
///
/// 如实报关的能力，形状是**不写这一行**。`push_notifications` 就是这样报 false
/// 的：A2A 的 push 要服务端拿到客户端给的 webhook 地址后回调任务更新，而本平台
/// 既没有接收那个地址的端点、也没有回调器；报 true 会让对端配一个永远收不到推送
/// 的地址，它那边看到的是「通知注册成功了但什么都没有」。写在函数体里的 `false`
/// 与这里的「缺一行」读起来一样，但后者在端点补上那天会自己变亮。
pub const ADVERTISED: &[Advertised] = &[
    Advertised {
        capability: "streaming",
        endpoint: MESSAGE_STREAM,
    },
    Advertised {
        capability: "state_transition_history",
        endpoint: MESSAGE_STREAM,
    },
];

/// 名片宣称的能力，逐杆从上面那张表算出来。
pub fn capabilities() -> cog_core::AgentCapabilities {
    cog_core::AgentCapabilities {
        streaming: advertises("streaming"),
        push_notifications: advertises("push_notifications"),
        state_transition_history: advertises("state_transition_history"),
    }
}

/// 这杆旗在表里有没有一行指着 A2A 的流式方法。
fn advertises(capability: &str) -> bool {
    ADVERTISED
        .iter()
        .any(|a| a.capability == capability && a.endpoint.ends_with(A2A_STREAM_METHOD))
}

// ─── 两个旋钮 ──────────────────────────────────────────────────────────────
//
// 两个都是环境可覆盖的，默认值也都写了来源，不是随手一个数：
//
// `POLL` —— 流读的是任务行，而行只在编排器推进时变。这个值只决定「变了之后
// 客户端最多晚多久听到」，不决定推进节奏（推进节奏不归这里定）。1s 落在
// 「人看得出是流、又不至于把一行读成热路径」的区间里。
//
// `MAX_STREAM` —— 一条流的寿命不该长过它承载的任务。「还在流」必须意味着
// 「任务还在动」；没有上限的话，一个永远不动的 goal 会把连接挂到天荒地老，
// 而流上什么都不发、看起来和「任务正在跑」一模一样。30 分钟是这类长任务的上限量级，
// 环境覆盖位留给部署按实际任务时长调。

fn poll_interval() -> Duration {
    let ms: u64 = std::env::var("COGNEVA_A2A_STREAM_POLL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    Duration::from_millis(ms.max(1))
}

fn max_stream() -> Duration {
    let secs: u64 = std::env::var("COGNEVA_A2A_STREAM_MAX_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30 * 60);
    Duration::from_secs(secs.max(1))
}

// ─── 读一行的那一个口 ─────────────────────────────────────────────────────

/// 这个流对编排器的全部需求只有一个：按 id 把那一行读回来。
///
/// 收成一个只有这一个方法的口，是因为 `OrchestratorControl` 有五十来个方法，
/// 一个只读一行状态的流按它实现，测试就得先把另外四十九个都桩上——那种测试里
/// 的信号会被桩埋掉，而且每次给编排器加方法都要回来改一遍测试。规格要的是
/// 「读得到这一行」，接口就只该说这件事。
#[async_trait::async_trait]
pub trait GoalReader: Send + Sync {
    async fn goal(&self, goal_id: &str) -> Option<Task>;
}

#[async_trait::async_trait]
impl GoalReader for Arc<dyn OrchestratorControl> {
    async fn goal(&self, goal_id: &str) -> Option<Task> {
        self.get_task(goal_id).await
    }
}

// ─── 状态映射 ─────────────────────────────────────────────────────────────

/// cogneva 调度器那一套状态 → A2A 线上那一套。
///
/// 两套状态不是同一件事：调度器要区分「排队等资源」（Pending）与「已排进依赖图」
/// （Scheduled），而 A2A 的线只有 submitted/working 这种粗档。映射刻意是**满**的：
/// 两个来源态落到同一个线上态是允许的，一个来源态落到两个线上态则会在读面上
/// 造出**同一个任务两种说法**。
///
/// `InputRequired` 目前没有来源：cogneva 的任务不问人（需要人拍板的是变更流程，
/// 不是被委派的任务）。这里不硬凑一个来源态去映射它——线上那一档宁可空着，
/// 也不要一个靠猜填出来的值。
fn a2a_state(status: TaskStatus) -> A2aTaskStatus {
    match status {
        TaskStatus::Pending | TaskStatus::Scheduled => A2aTaskStatus::Submitted,
        TaskStatus::Running => A2aTaskStatus::Working,
        TaskStatus::Completed => A2aTaskStatus::Completed,
        TaskStatus::Failed => A2aTaskStatus::Failed,
        TaskStatus::Cancelled => A2aTaskStatus::Canceled,
    }
}

/// 终态：到了这里流就该收，因为这一行不会再动。
///
/// 与 [`a2a_state`] 同源读同一套枚举，但这里是**判据**不是映射：新增一个线上态
/// 而忘了在这里归类，流会永远开着——所以它对着 A2aTaskStatus 穷举，不留 `_`。
fn is_terminal(state: A2aTaskStatus) -> bool {
    match state {
        A2aTaskStatus::Completed | A2aTaskStatus::Failed | A2aTaskStatus::Canceled => true,
        A2aTaskStatus::Submitted | A2aTaskStatus::Working | A2aTaskStatus::InputRequired => false,
    }
}

// ─── 请求解析 ─────────────────────────────────────────────────────────────

/// 一条 `message/stream` 请求里这个端点用得上的两样东西。
struct StreamRequest {
    /// JSON-RPC 的 `id`，原样回填进每一帧。没有 id 的请求也照收（回 `null`）：
    /// 拒了它只会让一个少写一个字段的客户端什么都读不到，而它要问的事是清楚的。
    rpc_id: Value,
    /// 拿 message 的文本 part 拼成的 goal。
    goal: String,
}

/// 从请求体里取出 message 的文本，作为 goal。
///
/// 形态上既收 JSON-RPC 信封（`{"id":…,"params":{"message":…}}`）也收裸 message
/// （`{"message":…}`）：前者是 A2A 规范的形状，后者是同族端点（MCP 那侧、以及本
/// 平台自己的 HTTP 面）已经在用的直接形状。两种都收的成本是几行，而只收一种会把
/// 「用另一种写的客户端」挡在门外，且是静默地挡——它拿到的是一个 400，说不清是
/// 自己写错了还是这个平台不认它。
fn parse_message_stream(body: &Value) -> Result<StreamRequest, String> {
    let rpc_id = body.get("id").cloned().unwrap_or(Value::Null);
    let message = body
        .get("params")
        .and_then(|params| params.get("message"))
        .or_else(|| body.get("message"))
        .ok_or_else(|| "the request carries no `message`".to_string())?;

    let parts = message
        .get("parts")
        .and_then(Value::as_array)
        .ok_or_else(|| "the message carries no `parts` array".to_string())?;

    let text = parts
        .iter()
        .filter_map(|part| {
            // A2A 0.2 用 `kind` 标 part 类型，早期 draft 用 `type`。两个都认：
            // 一个字段名的差异不该让一份照规范写的 message 变成「没有文本」。
            let is_text = part.get("kind").and_then(Value::as_str) == Some("text")
                || part.get("type").and_then(Value::as_str) == Some("text");
            if !is_text {
                return None;
            }
            part.get("text").and_then(Value::as_str)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let goal = text.trim().to_string();
    if goal.is_empty() {
        // 空 goal 不放过：它会建出一行没有任何内容的 goal，分解器拿到它只会
        // 造出一堆无意义的子任务，而调用方在流上看不出是自己发空了。
        return Err("the message has no text part to use as a goal".to_string());
    }
    Ok(StreamRequest { rpc_id, goal })
}

// ─── SSE 帧 ───────────────────────────────────────────────────────────────

/// 一帧 SSE。
///
/// 事件名固定 `message`，因为这里发的每一帧都是一条 JSON-RPC 消息；用不同事件名
/// 分「快照帧 / 状态帧」会让客户端必须按事件名分支，而它按 `result.kind` 分支更
/// 直接——事件名换了就断的是传输层，kind 换了断的才是语义层。
fn sse_frame(data: &Value) -> Bytes {
    Bytes::from(format!("event: message\ndata: {data}\n\n"))
}

fn jsonrpc_result(rpc_id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": rpc_id, "result": result})
}

// ─── 流本体 ───────────────────────────────────────────────────────────────

struct StreamState {
    reader: Arc<dyn GoalReader>,
    goal_id: String,
    rpc_id: Value,
    /// 上一次发出去的线上态。与本次相同就不发——重复帧不是「更多信息」，
    /// 而是把「变了」这个信号淹掉的噪声。
    last: Option<A2aTaskStatus>,
    /// 有没有一行被读到过。
    seen_row: bool,
    /// 流已经结束（发了终态，或到点报了错），下一次不再产生帧。
    done: bool,
    deadline: tokio::time::Instant,
    interval: Duration,
}

impl StreamState {
    /// 第一帧：整条任务快照。
    ///
    /// 快照与后续的状态帧用 `kind` 分开，而不是靠「第一帧特殊」这个位置约定：
    /// 位置约定在客户端重连或从中间接入时会立刻失效，字段则一直成立。
    fn snapshot(&self, task: &Task, state: A2aTaskStatus) -> Value {
        let mut a2a = serde_json::to_value(cog_core::A2aTask {
            id: self.goal_id.clone(),
            status: state,
            messages: Vec::new(),
            artifacts: artifacts_of(task),
            created_at: task.created_at,
            updated_at: task.updated_at,
        })
        .unwrap_or_else(|_| json!({}));
        if let Some(obj) = a2a.as_object_mut() {
            obj.insert("kind".into(), json!("task"));
        }
        jsonrpc_result(&self.rpc_id, a2a)
    }

    /// 后续帧：状态变了才发，带 `final` 标出这是不是最后一条。
    ///
    /// `final` 与终态判定同源（[`is_terminal`]），不是各写一遍：两处各判一次，
    /// 「流什么时候收」与「帧里说没收」迟早会不一致，而客户端信的正是帧里那个标记。
    ///
    /// 终态那一帧带上产物：调用方在别的进程里，读不到本平台的任务表，流是它
    /// 唯一看得到这次运行的地方，只报「完成了」而不给结果，等于让它再猜一次
    /// 到底做成了什么。非终态不带——那时候还没有结果，带着一个空数组只是让
    /// 「没有产物」与「还没产出」在客户端读成同一件事。
    fn status_update(&self, task: &Task, state: A2aTaskStatus) -> Value {
        let mut frame = json!({
            "kind": "status-update",
            "id": self.goal_id,
            "status": state,
            "final": is_terminal(state),
        });
        if let Some(obj) = frame.as_object_mut().filter(|_| is_terminal(state)) {
            obj.insert("artifacts".into(), json!(artifacts_of(task)));
        }
        jsonrpc_result(&self.rpc_id, frame)
    }
}

/// 一行任务留在 A2A 线上的产物。
///
/// 平台的任务行只有两个可交付的字段：`result` 是运行产出，`error` 是失败原因。
/// 两者各占一件产物而不是合成一件，因为客户端要能把「跑完了但没有产出」与
/// 「没跑完」分开——并成一件之后，读到的都是「有产物」。
///
/// 文本里放 JSON 而不是把 `result` 摊成结构化片段：A2A 的 `Part` 只有文本与文件
/// 两种，摊开需要发明一套字段映射，而那份映射一旦写错，客户端看到的是**残缺的**
/// 结果，不是一条读不懂的结果。
fn artifacts_of(task: &Task) -> Vec<TaskArtifact> {
    let mut artifacts = Vec::new();
    if let Some(result) = task.result.as_ref() {
        artifacts.push(TaskArtifact {
            name: "result".into(),
            parts: vec![MessagePart::Text {
                text: result.to_string(),
            }],
            index: artifacts.len() as u32,
        });
    }
    if let Some(error) = task.error.as_ref() {
        artifacts.push(TaskArtifact {
            name: "error".into(),
            parts: vec![MessagePart::Text {
                text: error.clone(),
            }],
            index: artifacts.len() as u32,
        });
    }
    artifacts
}

/// 一行任务在 A2A 线上的样子。
///
/// 与流里第一帧的快照同源（同一套状态映射、同一份产物），差别只在流那侧要额外
/// 打一个 `kind` 标记。两处若各写一遍，「流里看到的结果」与「`tasks/get` 拿到的
/// 结果」就会各自漂——而它们说的是同一行。
fn a2a_task(task: &Task) -> cog_core::A2aTask {
    cog_core::A2aTask {
        id: task.id.clone(),
        status: a2a_state(task.status),
        messages: Vec::new(),
        artifacts: artifacts_of(task),
        created_at: task.created_at,
        updated_at: task.updated_at,
    }
}

/// 把一条 goal 的推进做成 SSE 帧序列。
///
/// 形状是 `unfold` 而不是通道 + 后台任务：帧的产生只依赖「读一行、比对上一次」，
/// 没有别的事件源要合流，多一个中间通道就多一处要在客户端断开时记得关掉的东西
/// （流被丢弃时通道那头没人收，写端就成了没有人读的常驻任务）。
fn goal_stream(state: StreamState) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    futures::stream::unfold(state, |mut s| async move {
        loop {
            if s.done {
                return None;
            }
            if tokio::time::Instant::now() >= s.deadline {
                if !s.seen_row {
                    // 到点了这一行从没出现过。**必须报一帧错再收**：直接收流
                    // 会让调用方看到「流正常结束、零帧」，而那和「任务一步都没
                    // 动」在读面上是同一个形状——分不出是提交没落地还是任务没
                    // 推进，等于把一次失败伪装成一次正常但无事发生。
                    s.done = true;
                    let frame = sse_frame(&json!({
                        "jsonrpc": "2.0",
                        "id": s.rpc_id,
                        "error": {
                            "code": -32001,
                            "message": format!(
                                "no goal row for '{}' appeared within {:?}",
                                s.goal_id, s.interval
                            ),
                        },
                    }));
                    return Some((Ok(frame), s));
                }
                return None;
            }

            let reader = s.reader.clone();
            let row = reader.goal(&s.goal_id).await;
            if let Some(task) = row {
                s.seen_row = true;
                let state = a2a_state(task.status);
                if s.last != Some(state) {
                    let frame = if s.last.is_none() {
                        s.snapshot(&task, state)
                    } else {
                        s.status_update(&task, state)
                    };
                    s.last = Some(state);
                    s.done = is_terminal(state);
                    return Some((Ok(sse_frame(&frame)), s));
                }
            }
            tokio::time::sleep(s.interval).await;
        }
    })
}

// ─── handler ─────────────────────────────────────────────────────────────

/// `POST /api/v1/protocol/a2a/message/stream`
///
/// 立即回 `200` + `text/event-stream`，不等任务跑完：流本身就是「慢」的表达方式，
/// 一个先把活儿干完再开流的实现，等于把长任务又塞回一次请求-响应里，客户端该读
/// 到的中间态全丢了。
pub async fn message_stream_handler(
    State(state): State<Arc<GatewayState>>,
    Json(body): Json<Value>,
) -> Response {
    let request = match parse_message_stream(&body) {
        Ok(request) => request,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_request", "message": message})),
            )
                .into_response();
        }
    };

    // goal id 由这里生成并回给客户端，而不是等编排器分配：客户端要拿它去订阅、
    // 去查，而这个 id 只有在第一帧之前就定下来，才可能出现在第一帧里。
    let goal_id = uuid::Uuid::new_v4().to_string();

    // 与 HTTP 面同一个受理路径（同一行构造、同一次 spawn）：两条路各自写一遍
    // 提交逻辑，就会出现「经 A2A 提的 goal 少了某个字段」这类只在一条路上
    // 存在的缺陷。
    let mut goal_row = Task::goal_row(goal_id.clone(), &request.goal);
    goal_row.workspace_id = None;
    let orchestrator = state.orchestrator.clone();
    let goal = request.goal.clone();
    let submit_id = goal_id.clone();
    tokio::spawn(async move {
        match orchestrator.submit_goal_auto(&goal, vec![goal_row]).await {
            Ok(ids) => tracing::info!(
                goal_id = %submit_id,
                task_count = ids.len(),
                "a2a message/stream goal accepted"
            ),
            Err(e) => {
                tracing::error!(goal_id = %submit_id, "a2a message/stream submit failed: {e}")
            }
        }
    });

    let stream = goal_stream(StreamState {
        reader: Arc::new(state.orchestrator.clone()),
        goal_id,
        rpc_id: request.rpc_id,
        last: None,
        seen_row: false,
        done: false,
        deadline: tokio::time::Instant::now() + max_stream(),
        interval: poll_interval(),
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        // 反向代理默认会缓冲响应体，缓冲住了的「流」等于一次请求-响应；这条头
        // 是让中间那层别把帧攒起来一起发。
        .header("x-accel-buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "stream_unavailable"})),
            )
                .into_response()
        })
}

// ─── JSON-RPC 端点 ────────────────────────────────────────────────────────
//
// 四条方法共用一个地址：`message/send`、`message/stream`、`tasks/get`、
// `tasks/cancel`。`message/stream` 另有自己的路径（上文的 `MESSAGE_STREAM`，
// 名片上的旗子指着它），这里再按 `method` 收一次：规范给对端的是一张地址，
// 照规范写的对端会把流也发到这里；只认路径不认方法，它拿到的就是「不认识这个方法」。
// 另外两条是每个对端都会用的，缺了它们，一个按名片打过来的客户端能做的只有开流。

/// 一帧 JSON-RPC 错误。
///
/// 协议的错写在 `error` 里、HTTP 状态仍是 200：一次「方法不认识」与一次「这一行
/// 不存在」都是对端读得懂的答复，用 404 表达会逼它去猜一份非 JSON 的响应是不是
/// 这个平台在说「没有」。
fn rpc_error(rpc_id: &Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": rpc_id,
        "error": {"code": code, "message": message.into()},
    })
}

/// `POST /api/v1/protocol/a2a` —— 名片上那个地址，按 `method` 分派。
pub async fn rpc_handler(
    State(state): State<Arc<GatewayState>>,
    Json(body): Json<Value>,
) -> Response {
    let rpc_id = body.get("id").cloned().unwrap_or(Value::Null);
    match body.get("method").and_then(Value::as_str) {
        Some("message/send") => rpc_message_send(&state, &body, &rpc_id).await,
        Some("message/stream") => message_stream_handler(State(state), Json(body)).await,
        Some("tasks/get") => rpc_tasks_get(&state, &body, &rpc_id).await,
        Some("tasks/cancel") => rpc_tasks_cancel(&state, &body, &rpc_id).await,
        other => (
            StatusCode::OK,
            Json(rpc_error(
                &rpc_id,
                -32601,
                format!(
                    "unknown method '{}'; this endpoint serves message/send, \
                     message/stream, tasks/get and tasks/cancel",
                    other.unwrap_or("<missing>")
                ),
            )),
        )
            .into_response(),
    }
}

/// `tasks/get` 与 `tasks/cancel` 共用的 id 取法。
fn rpc_task_id(body: &Value) -> Option<&str> {
    let params = body.get("params")?;
    // `id` 是 A2A 0.2 的字段名，`taskId` 是更早的草稿：两个都认，一个字段名的
    // 差异不该让一份照旧版规范写的请求变成「没有任务」。
    params
        .get("id")
        .or_else(|| params.get("taskId"))
        .and_then(Value::as_str)
}

/// `message/send`：受理一条 message，立刻回这一行的当前样子。
///
/// 不等它跑完：A2A 允许这里回一个还在跑的任务，对端用 `tasks/get` 追。等它跑完
/// 再回，就把这个方法变成一次长请求——而「长」正是 `message/stream` 存在的理由，
/// 一个不流式的对端选了 `message/send`，就是接受「先拿到受理、再来问结果」。
async fn rpc_message_send(state: &Arc<GatewayState>, body: &Value, rpc_id: &Value) -> Response {
    let request = match parse_message_stream(body) {
        Ok(request) => request,
        Err(message) => {
            return (StatusCode::OK, Json(rpc_error(rpc_id, -32602, message))).into_response()
        }
    };

    // 与 `POST /api/v1/tasks` 和 `message/stream` 走同一条受理路径：三条路各自
    // 写一遍提交逻辑，就会出现「经某一条路提的 goal 少了某个字段」这种只在一条
    // 路上存在的缺陷。
    let goal_id = uuid::Uuid::new_v4().to_string();
    let mut goal_row = Task::goal_row(goal_id.clone(), &request.goal);
    goal_row.workspace_id = None;
    let accepted = goal_row.clone();
    let orchestrator = state.orchestrator.clone();
    let goal = request.goal.clone();
    let submit_id = goal_id.clone();
    tokio::spawn(async move {
        match orchestrator.submit_goal_auto(&goal, vec![goal_row]).await {
            Ok(ids) => tracing::info!(
                goal_id = %submit_id,
                task_count = ids.len(),
                "a2a message/send goal accepted"
            ),
            Err(e) => tracing::error!(goal_id = %submit_id, "a2a message/send submit failed: {e}"),
        }
    });

    (
        StatusCode::OK,
        Json(jsonrpc_result(rpc_id, json!(a2a_task(&accepted)))),
    )
        .into_response()
}

/// `tasks/get`：按 id 读一行，带上它已有的结果与失败原因。
///
/// 这也是不流式的对端拿结果的唯一一条路——`message/send` 回的是受理那一刻的
/// 样子，而结果是在之后写进这一行的。产物与流里终态帧同源（[`artifacts_of`]）。
async fn rpc_tasks_get(state: &Arc<GatewayState>, body: &Value, rpc_id: &Value) -> Response {
    let Some(task_id) = rpc_task_id(body) else {
        return (
            StatusCode::OK,
            Json(rpc_error(rpc_id, -32602, "tasks/get takes params.id")),
        )
            .into_response();
    };

    match state.orchestrator.get_task(task_id).await {
        Some(task) => (
            StatusCode::OK,
            Json(jsonrpc_result(rpc_id, json!(a2a_task(&task)))),
        )
            .into_response(),
        None => (
            StatusCode::OK,
            Json(rpc_error(rpc_id, -32001, format!("no task '{task_id}'"))),
        )
            .into_response(),
    }
}

/// `tasks/cancel`：喊停一行及其下游，回停完之后的那一行。
///
/// 先读一次再喊停，是因为「这个 id 不存在」与「停了但读不回来」要对端看到不同
/// 的东西：前者是它问错了 id（`-32001`），后者是平台这边的内部故障。两个都压成
/// 一条错误文本，对端就只剩「取消失败」一个读数。
///
/// 返回的是**停完之后**的行，不是「已受理」：取消在这个平台上是同步做的
/// （cascade 的 id 也随这次调用返回），所以对端拿到 `canceled` 就能收尾，
/// 不必再问一次 `tasks/get` 猜它停没停。
async fn rpc_tasks_cancel(state: &Arc<GatewayState>, body: &Value, rpc_id: &Value) -> Response {
    let Some(task_id) = rpc_task_id(body) else {
        return (
            StatusCode::OK,
            Json(rpc_error(rpc_id, -32602, "tasks/cancel takes params.id")),
        )
            .into_response();
    };

    if state.orchestrator.get_task(task_id).await.is_none() {
        return (
            StatusCode::OK,
            Json(rpc_error(rpc_id, -32001, format!("no task '{task_id}'"))),
        )
            .into_response();
    }

    match state.orchestrator.cancel_task(task_id).await {
        Ok(cascaded) => {
            tracing::info!(task_id, cascaded = cascaded.len(), "a2a tasks/cancel");
            match state.orchestrator.get_task(task_id).await {
                Some(task) => (
                    StatusCode::OK,
                    Json(jsonrpc_result(rpc_id, json!(a2a_task(&task)))),
                )
                    .into_response(),
                None => (
                    StatusCode::OK,
                    Json(rpc_error(
                        rpc_id,
                        -32603,
                        format!("task '{task_id}' was cancelled but can no longer be read"),
                    )),
                )
                    .into_response(),
            }
        }
        Err(e) => (
            StatusCode::OK,
            Json(rpc_error(
                rpc_id,
                -32603,
                format!("task '{task_id}' could not be cancelled: {e}"),
            )),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Mutex;

    /// 一个会说「第 n 次被问到时报什么状态」的读端。
    ///
    /// 它不实现 `OrchestratorControl`（五十来个方法），只实现流真正要的那一个口
    /// ——这正是那个窄口存在的意义：测试里出现的东西全是本用例的读数，没有桩。
    struct ScriptedReader {
        /// 依次给出的行状态；用完之后一直停在最后一个。
        script: Mutex<Vec<Option<TaskStatus>>>,
        asks: Mutex<usize>,
        /// 这一行带回来的结果与失败原因，两者都是任务行自己的字段。
        result: Option<Value>,
        error: Option<String>,
    }

    impl ScriptedReader {
        fn new(script: Vec<Option<TaskStatus>>) -> Arc<Self> {
            Self::with_outcome(script, None, None)
        }

        fn with_outcome(
            script: Vec<Option<TaskStatus>>,
            result: Option<Value>,
            error: Option<&str>,
        ) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                asks: Mutex::new(0),
                result,
                error: error.map(str::to_string),
            })
        }
    }

    #[async_trait::async_trait]
    impl GoalReader for ScriptedReader {
        async fn goal(&self, goal_id: &str) -> Option<Task> {
            let mut asks = self.asks.lock().unwrap();
            let nth = *asks;
            *asks += 1;
            let script = self.script.lock().unwrap();
            let step = script
                .get(nth)
                .or_else(|| script.last())
                .cloned()
                .flatten()?;
            let mut row = Task::goal_row(goal_id.to_string(), "a goal");
            row.status = step;
            row.result = self.result.clone();
            row.error = self.error.clone();
            Some(row)
        }
    }

    fn state_of(reader: Arc<dyn GoalReader>) -> StreamState {
        StreamState {
            reader,
            goal_id: "g-1".into(),
            rpc_id: json!(7),
            last: None,
            seen_row: false,
            done: false,
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            interval: Duration::from_millis(5),
        }
    }

    fn parse_frame(bytes: &Bytes) -> Value {
        let text = std::str::from_utf8(bytes).expect("an SSE frame is UTF-8");
        let line = text
            .lines()
            .find_map(|l| l.strip_prefix("data: "))
            .expect("every frame carries a data line");
        serde_json::from_str(line).expect("the data line is a JSON-RPC message")
    }

    /// 名片上每一杆旗，都得是表里那一行算出来的；指名的端点必须是那张 A2A 方法。
    ///
    /// 这里判的是**旗由端点算出**：某一杆写成硬编码的 `true`、或表里那一行被指到
    /// 别的路由上，这里就红。判不了「路由被删、常量与表都留着」——axum 没有枚举
    /// 路由的接口，旗子与表读的又是同一个常量，三处会一起留在原处。那一半只有
    /// 对**真正在跑的路由**发一次请求才看得见：旗指名而路由上没有，对端拿到的是
    /// 404，本仓的集成用例照这张清单打这一枪。
    #[test]
    fn every_capability_flag_is_derived_from_a_row_naming_the_stream_method() {
        let caps = capabilities();
        let flags = [
            ("streaming", caps.streaming),
            ("push_notifications", caps.push_notifications),
            ("state_transition_history", caps.state_transition_history),
        ];
        for (name, on) in flags {
            let row = ADVERTISED.iter().find(|a| a.capability == name);
            assert_eq!(
                on,
                row.is_some(),
                "`{name}` 的真假只能来自表里有没有那一行，不能另有出处"
            );
            if let Some(a) = row {
                assert!(
                    a.endpoint.ends_with(A2A_STREAM_METHOD),
                    "`{name}` 指名的端点必须还是 A2A 的那张流式方法，指到别处旗子就该灭"
                );
            }
        }
        assert!(
            caps.streaming,
            "流式是这张名片对外的承诺，也是设计里要落地的那张端点；旗灭等于宣告 cogneva 不做流式对端"
        );
        assert!(
            caps.state_transition_history,
            "状态按变化逐帧发出，这条历史就是流本身"
        );
        assert!(
            !caps.push_notifications,
            "本平台没有接收 push 回调地址的端点，这杆旗必须如实报 false"
        );
    }

    /// 一次 `message/stream` 的请求照 JSON-RPC 信封写时，取到的是那条 message
    /// 的文本，且 `id` 原样带出来。
    #[test]
    fn a_jsonrpc_envelope_and_a_bare_message_both_parse() {
        let envelope = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "message/stream",
            "params": {"message": {"role": "user", "parts": [{"kind": "text", "text": "do it"}]}},
        });
        let parsed = parse_message_stream(&envelope).expect("an envelope parses");
        assert_eq!(parsed.goal, "do it");
        assert_eq!(parsed.rpc_id, json!(3));

        // 同一件事，写成裸 message，也该认。
        let bare = json!({"message": {"parts": [{"type": "text", "text": "do it"}]}});
        let parsed = parse_message_stream(&bare).expect("a bare message parses");
        assert_eq!(parsed.goal, "do it");
        assert_eq!(parsed.rpc_id, Value::Null);
    }

    /// 没有文本可当 goal 的请求被拒，不是被当成一个空 goal 受理。
    #[test]
    fn a_message_with_no_text_is_refused_rather_than_submitted_empty() {
        for body in [
            json!({}),
            json!({"message": {"parts": []}}),
            json!({"message": {"parts": [{"kind": "file", "name": "a.bin"}]}}),
            json!({"message": {"parts": [{"kind": "text", "text": "   "}]}}),
        ] {
            assert!(
                parse_message_stream(&body).is_err(),
                "this request has no goal in it: {body}"
            );
        }
    }

    /// 状态一路走下来，帧跟着状态变，到终态那一帧收流。
    ///
    /// 这条用例挡的是三种读起来都正常的写法：每次读都发一帧（同一状态重复发）、
    /// 只在状态改动的那一步发（首帧丢掉）、以及终态之后继续开着（流不结束）。
    #[tokio::test]
    async fn the_stream_follows_the_status_and_ends_at_the_terminal_one() {
        let reader = ScriptedReader::new(vec![
            Some(TaskStatus::Pending),
            Some(TaskStatus::Running),
            Some(TaskStatus::Running),
            Some(TaskStatus::Completed),
        ]);
        let mut stream = Box::pin(goal_stream(state_of(reader)));

        let first = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(first["id"], json!(7), "the frame carries the request's id");
        assert_eq!(first["result"]["kind"], json!("task"));
        assert_eq!(first["result"]["status"], json!("submitted"));
        assert_eq!(first["result"]["id"], json!("g-1"));

        let second = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(second["result"]["kind"], json!("status-update"));
        assert_eq!(second["result"]["status"], json!("working"));
        assert_eq!(
            second["result"]["final"],
            json!(false),
            "a running task is not a final frame"
        );

        let third = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(
            third["result"]["status"],
            json!("completed"),
            "the repeat of running produced no frame of its own"
        );
        assert_eq!(third["result"]["final"], json!(true));

        assert!(
            stream.next().await.is_none(),
            "a terminal state ends the stream"
        );
    }

    /// 一行都没出现时，流以一条**错**收尾，不是以「零帧的正常结束」收尾。
    ///
    /// 这是本文件里最容易被写没的一条：让 unfold 到点直接返回 None 是最自然的写法，
    /// 读起来也正常——而它把「提交没落地」和「任务一步都没动」变成同一个形状。
    #[tokio::test]
    async fn a_goal_row_that_never_appears_ends_the_stream_with_an_error_frame() {
        let reader = ScriptedReader::new(vec![None]);
        let mut state = state_of(reader);
        state.deadline = tokio::time::Instant::now() + Duration::from_millis(30);
        let mut stream = Box::pin(goal_stream(state));

        let frame = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(
            frame["error"]["code"],
            json!(-32001),
            "a stream that saw nothing says so instead of ending silently: {frame}"
        );
        assert!(
            stream.next().await.is_none(),
            "and it ends right after saying so"
        );
    }

    /// 终态那一帧把这次运行的结果带给调用方，中间帧不带。
    ///
    /// 流是进程外调用方唯一看得到这次运行的地方：只报「completed」而不带产物，
    /// 它能知道的只有「跑完了」，不知道做成了什么——那正是这条端点在商用闭环里
    /// 缺的那一半。反过来，中间帧带一个空数组会让「还没产出」与「没有产出」在
    /// 客户端读成同一件事，所以只有终态带。
    #[tokio::test]
    async fn the_terminal_frame_carries_what_the_run_produced_and_the_earlier_ones_do_not() {
        let reader = ScriptedReader::with_outcome(
            vec![
                Some(TaskStatus::Pending),
                Some(TaskStatus::Running),
                Some(TaskStatus::Completed),
            ],
            Some(json!({"answer": 42})),
            None,
        );
        let mut stream = Box::pin(goal_stream(state_of(reader)));

        let first = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(
            first["result"]["kind"],
            json!("task"),
            "the first frame is the row's snapshot; whatever the row holds is reported as it stands"
        );

        let second = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(second["result"]["status"], json!("working"));
        assert!(
            second["result"].get("artifacts").is_none(),
            "a status update before the end does not carry an artifact list: {second}"
        );

        let third = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(third["result"]["status"], json!("completed"));
        assert_eq!(
            third["result"]["artifacts"][0]["name"],
            json!("result"),
            "the outcome travels in an artifact named as such: {third}"
        );
        assert_eq!(
            third["result"]["artifacts"][0]["parts"][0]["text"],
            json!("{\"answer\":42}"),
            "the artifact carries the task's own result rather than a rendering of it"
        );
    }

    /// 一行既没有结果也没有错误时，产物是空的——不是一件空产物。
    ///
    /// 空片段在客户端读起来与「跑完了但没有产出」一样，而那句话该由一个空列表
    /// 说，不该由一件内容为空的产物说。
    #[test]
    fn a_row_with_neither_result_nor_error_has_no_artifact() {
        let row = Task::goal_row(String::from("g-1"), "a goal");
        assert!(artifacts_of(&row).is_empty());
    }

    /// 失败的那一次带的是失败原因，而且与结果分两件产物。
    ///
    /// 合成一件之后，读到的都是「有产物」——客户端分不出「跑完了但没有产出」与
    /// 「没跑完」。
    #[tokio::test]
    async fn a_failed_run_reports_its_reason_apart_from_a_result() {
        let reader = ScriptedReader::with_outcome(
            vec![Some(TaskStatus::Running), Some(TaskStatus::Failed)],
            None,
            Some("upstream refused the request"),
        );
        let mut stream = Box::pin(goal_stream(state_of(reader)));

        let _running = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        let failed = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(failed["result"]["status"], json!("failed"));
        assert_eq!(
            failed["result"]["artifacts"][0]["name"],
            json!("error"),
            "a failure names itself instead of borrowing the result's artifact: {failed}"
        );
        assert_eq!(
            failed["result"]["artifacts"][0]["parts"][0]["text"],
            json!("upstream refused the request")
        );
    }
}
