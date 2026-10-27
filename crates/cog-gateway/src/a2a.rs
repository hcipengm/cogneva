//! principle: feedback_a_field_named_error_is_not_a_failure_until_the_row_says_it_is_terminal
//! principle: feedback_an_acceptance_is_a_promise_the_id_it_hands_out_must_read_back
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
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::stream::Stream;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use cog_core::{
    A2aTask, A2aTaskStatus, A2aVersion, MessagePart, OrchestratorControl, Task, TaskArtifact,
    TaskMessage, TaskStatus,
};

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

// ─── 版本 ─────────────────────────────────────────────────────────────────
//
// 服务两版：0.3 与 1.0。
//
// 判据只有一条：**线上格式是这套系统里唯一不能单方面改的东西**。代码、部署、
// 数据模型都是我们自己的，随时能改；而对面那个 agent 是别人的进程，它的解析器
// 不会因为我们改了版本就跟着改。所以版本的选择标准不是「我们现网在跑哪版」
// （那随时可以换），而是「按某个版本写的对端，我们能不能接得住」。
//
// - 1.0（最新已发布版，1.0.1 是补丁）必须服务：新对端、各语言 SDK 都照它写，
//   不接就是把自己排除在新增的协作面之外。
// - 0.3 必须服务：规范自己规定**空的 `A2A-Version` 按 0.3 解释**——不接它，
//   等于把「没写版本头的对端」这一类按规范合法的请求判成错。而且 0.3 的已装
//   客户端基数最大，两者线上形状又不是两套模型（同一份中立任务模型带两个编解码），
//   增量成本是可控的。
// - 0.2 及更早不服务：方法名与 part/状态编码都不同（`tasks/send` 一族），是第三
//   套编解码；而两版名片都没有字段能宣告它们，一个 0.2 的对端连发现都发现不了。
//   加它的触发条件是「真有一个按 `tasks/send` 打的调用方」——那会以一条具名的
//   `-32601`（拒绝时点名我们服务的方法）出现在日志里，不是靠猜。
// - 1.2 不存在：已发布版本止于 v1.0.1。等真发布了新的 major.minor，现有代码会
//   用 `-32009` 点名「只说 0.3 和 1.0」把它挡住——那条拒绝就是回来加支持的信号。
//
// 入站按规范允许的兼容期两版的写法都收；出站只按请求头声明的版本渲染一份。

/// 请求声明的协议版本：`A2A-Version` 头优先，其次查询参数，都没有（或为空）
/// 按规范解释成 0.3。
///
/// 认不出的版本是**错误**，不是回落到 0.3：一个 1.2 的客户端按 1.2 的语义发请求，
/// 拿 0.3 的答复去读，读出来的是「对端行为怪异」而不是「版本不支持」。
fn requested_version(headers: &HeaderMap, uri: &Uri) -> Result<A2aVersion, String> {
    requested_version_or(headers, uri, A2aVersion::V03)
}

/// 同一条协商规则，只换「没写版本时按哪一版」。缺省值不是口味问题：
/// 0.3 规范自己规定空的 `A2A-Version` 按 0.3 解释，所以 RPC 路径的缺省是 0.3；
/// 而 `/.well-known/agent-card.json` 是 1.0 规范定义的发现入口（0.3 也把名片
/// 放在这个路径，但它的名片自带 `protocolVersion` 与 `url`），所以发现路径的
/// 缺省是 1.0——一个照 1.0 写的对端先取名片、再按 `supportedInterfaces` 选传输，
/// 拿不到 `supportedInterfaces` 的卡它连传输都选不出来。显式声明的那一版永远优先。
fn requested_version_or(
    headers: &HeaderMap,
    uri: &Uri,
    default: A2aVersion,
) -> Result<A2aVersion, String> {
    let from_header = headers
        .get("a2a-version")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|raw| !raw.is_empty());
    let from_query = uri.query().and_then(|query| {
        query.split('&').find_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            name.eq_ignore_ascii_case("a2a-version")
                .then(|| value.trim())
                .filter(|value| !value.is_empty())
        })
    });
    match from_header.or(from_query) {
        None => Ok(default),
        Some(raw) => A2aVersion::parse(raw).ok_or_else(|| {
            format!("A2A version '{raw}' is not served; this endpoint speaks 0.3 and 1.0")
        }),
    }
}

// ─── 名片 ─────────────────────────────────────────────────────────────────

/// 名片 `url` 的基址：**调用方刚刚够得着的那一个**，从这次请求的 `Host` 与
/// 反代的 `X-Forwarded-Proto` 取，不是本进程的绑定地址。`0.0.0.0` 的意思是
/// 「监听所有接口」，不是任何一个对端能拨号的目标。
fn advertised_base(headers: &HeaderMap, port: u16) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|scheme| !scheme.is_empty())
        .unwrap_or("http");
    match headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|host| !host.is_empty())
    {
        Some(host) => format!("{scheme}://{host}"),
        None => format!("http://0.0.0.0:{port}"),
    }
}

/// 两版名片共用的技能表。
fn skills() -> Value {
    json!([
        {
            "id": "task_orchestration",
            "name": "Task Orchestration",
            "description": "DAG-based task scheduling and execution",
            "tags": ["orchestration", "dag"],
            "examples": ["Split this migration into stages and run them in order"],
        },
        {
            "id": "multi_agent_collaboration",
            "name": "Multi-Agent Collaboration",
            "description": "Crew-based multi-agent problem solving",
            "tags": ["collaboration", "crew"],
            "examples": ["Have a crew review this design and report by concern"],
        },
    ])
}

/// 能力旗的线上形状。两版都是 camelCase；值仍由 [`ADVERTISED`] 推出。
fn capabilities_wire() -> Value {
    let caps = capabilities();
    json!({
        "streaming": caps.streaming,
        "pushNotifications": caps.push_notifications,
        "stateTransitionHistory": caps.state_transition_history,
    })
}

/// 0.3 的名片：必填字段按 0.3 的 schema——`protocolVersion`、`defaultInputModes`、
/// `defaultOutputModes`、`url` 都在。鉴权只用 `securitySchemes`/`security`，
/// 不再发更早草稿的 `authentication`。
fn card_v03(base: &str) -> Value {
    json!({
        "protocolVersion": "0.3",
        "name": "cogneva",
        "description": "Cogneva autonomous multi-agent collaboration platform",
        "url": format!("{base}{A2A_RPC}"),
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": capabilities_wire(),
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": skills(),
        "securitySchemes": {
            "jwt": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"},
        },
        "security": [{"jwt": []}],
    })
}

/// 1.0 的名片：没有顶层 `url`，端点挪进 `supportedInterfaces`；安全方案是
/// 判别联合（`httpAuthSecurityScheme`）。列表里两条指向同一个地址，一条 1.0
/// 一条 0.3——一个 1.0 的客户端因此看得出这个对端也听得懂 0.3。
fn card_v10(base: &str) -> Value {
    let endpoint = format!("{base}{A2A_RPC}");
    json!({
        "name": "cogneva",
        "description": "Cogneva autonomous multi-agent collaboration platform",
        "supportedInterfaces": [
            {"url": endpoint, "protocolBinding": "JSONRPC", "protocolVersion": "1.0"},
            {"url": endpoint, "protocolBinding": "JSONRPC", "protocolVersion": "0.3"},
        ],
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": capabilities_wire(),
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": skills(),
        "securitySchemes": {
            "jwt": {"httpAuthSecurityScheme": {"scheme": "Bearer", "bearerFormat": "JWT"}},
        },
    })
}

/// `GET /api/v1/protocol/a2a/agent-card`
///
/// 名片是公开的（对端的第一件事实在是「该用什么凭证」），且是只读发现文档：
/// 短 TTL 让重复发现不必每次落到进程里重算，又短到地址或能力变更几分钟内就能
/// 被看到。公开缓存是安全的——名片里没有任何按调用方不同的内容。
pub async fn agent_card_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    card_response(http_port(&state), &headers, &uri, A2aVersion::V03)
}

/// `GET /.well-known/agent-card.json`
///
/// 两个版本的规范都把发现入口钉在这个 well-known 路径上：对端找到一个 agent
/// 的第一件事就是 GET 它，没有「先配置一个我们自己的路径」这一步。
/// 只把名片放在自定义路径上，照规范写的对端与官方 TCK 都发现不了我们——那是
/// 对端面唯一一类「我们全对、对面就是接不上」的缺陷。
///
/// 缺省给 1.0：这个路径由 1.0 规范定义，而 1.0 的对端要先从 `supportedInterfaces`
/// 选出传输，拿不到就无路可走。显式要 0.3 的对端在这里照旧拿到带 `url` 的 0.3 名片。
pub async fn well_known_card_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    card_response(http_port(&state), &headers, &uri, A2aVersion::V10)
}

/// 本进程监听的那个口：名片要能给出「调用方够得着的基址」，缺 `Host` 时退回它。
fn http_port(state: &GatewayState) -> u16 {
    state
        .config
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .http_port
}

fn card_response(port: u16, headers: &HeaderMap, uri: &Uri, default: A2aVersion) -> Response {
    let version = match requested_version_or(headers, uri, default) {
        Ok(version) => version,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "version_not_supported", "message": message})),
            )
                .into_response();
        }
    };
    let base = advertised_base(headers, port);
    let card = match version {
        A2aVersion::V03 => card_v03(&base),
        A2aVersion::V10 => card_v10(&base),
    };
    let body = serde_json::to_vec(&card).unwrap_or_else(|_| b"{}".to_vec());
    // 名片要带一个由内容（或 `version` 字段）导出的 `ETag`，对端过期后应当用
    // 条件请求而不是重下。这里取内容哈希：`version` 只在发版时变，
    // 而名片还会随 `Host`（地址）变，哈希把这两件事都覆盖住。
    let digest = Sha256::digest(&body);
    let etag = format!("\"{}\"", hex16(&digest[..]));
    let cache_control = header::HeaderValue::from_static("public, max-age=300");

    // 条件请求命中就回 304：对端每拍都来发现一次，内容没变时不该再传一遍正文。
    let matched = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .any(|candidate| candidate == etag || candidate == "*")
        })
        .unwrap_or(false);
    let mut response = if matched {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, cog_core::CONTENT_TYPE_JSON)],
            body,
        )
            .into_response()
    };
    let headers_mut = response.headers_mut();
    headers_mut.insert(header::CACHE_CONTROL, cache_control);
    headers_mut.insert(
        header::ETAG,
        header::HeaderValue::from_str(&etag)
            .unwrap_or_else(|_| header::HeaderValue::from_static("\"\"")),
    );
    headers_mut.insert(header::LAST_MODIFIED, card_last_modified());
    response
}

/// 名片这个表示「上一次可能改动」的时刻，按 HTTP 的日期格式给出去。
///
/// 取本进程第一次渲染名片的时刻：正文是发版版本、进程配置与调用方 `Host` 的函数，
/// 前两者随进程启动而定，第三者随请求变、已经由内容 `ETag` 覆盖。所以对一个固定的
/// 调用方，正文在这个进程里不会再变，而**报早一点是安全的**——缓存只是多来问一次；
/// 报晚一点则会让对端拿着一张过期名片继续用。强 `ETag` 优先于这个弱校验器。
fn card_last_modified() -> header::HeaderValue {
    use chrono::{DateTime, Utc};
    use std::sync::OnceLock;
    use std::time::SystemTime;

    static FIRST_RENDER: OnceLock<SystemTime> = OnceLock::new();
    let at: DateTime<Utc> = (*FIRST_RENDER.get_or_init(SystemTime::now)).into();
    header::HeaderValue::from_str(&at.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|_| header::HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"))
}

/// 哈希的前 16 个十六进制位：足够区分名片，又不至于把响应头撑长。
fn hex16(bytes: &[u8]) -> String {
    bytes.iter().take(8).map(|b| format!("{b:02x}")).collect()
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

/// 两个受理入口要的那一个口：把这一行递进去，再把它读回来。
///
/// 与 [`GoalReader`] 分开一层，是为了让「受理」这件事本身有一个能测的接线。
/// 判据 [`accepted`] 只判回执；处理函数用没用它、读回来的那一行是不是库里
/// 那一行，都在这一层才看得见——而直接对 `OrchestratorControl` 写测试要么桩
/// 五十个方法，要么什么也没钉住。
#[async_trait::async_trait]
pub trait GoalSink: GoalReader {
    async fn submit(&self, goal: &str, row: Task) -> cog_core::SFResult<Vec<String>>;
}

#[async_trait::async_trait]
impl GoalSink for Arc<dyn OrchestratorControl> {
    async fn submit(&self, goal: &str, row: Task) -> cog_core::SFResult<Vec<String>> {
        self.submit_goal_auto(goal, vec![row]).await
    }
}

/// 受理一条 goal：递进去、只认图回执、再把库里那一行读回来。
///
/// 三步是一件事的三个必要条件，所以收在一个函数里而不是留给每条入口各写一遍：
/// 递进去不算收下（批量写入会跳过已在册的行，返回的是它收下的那些），收下不算
/// 读得回来（读的是库里的当前行，不是递进去的那一份副本）。
async fn accept_goal(sink: &dyn GoalSink, goal: &str, row: Task) -> Result<Task, String> {
    let goal_id = row.id.clone();
    let submitted = sink.submit(goal, row).await;
    accepted(&goal_id, submitted)?;
    sink.goal(&goal_id)
        .await
        .ok_or_else(|| format!("the goal '{goal_id}' was submitted but cannot be read back"))
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
/// 与 [`a2a_state`] 同源读同一套枚举，判定收在 `A2aTaskStatus::is_terminal` 一处：
/// 流什么时候收、产物什么时候发、名片上的 `stateTransitionHistory` 意味着什么，
/// 都读同一个答案；两处各判一次，迟早有一处把「完成的」读成「还在跑」。
fn is_terminal(state: A2aTaskStatus) -> bool {
    state.is_terminal()
}

// ─── 请求解析 ─────────────────────────────────────────────────────────────

/// 一条 `message/stream` 请求里这个端点用得上的两样东西。
struct StreamRequest {
    /// JSON-RPC 的 `id`，原样回填进每一帧。没有 id 的请求也照收（回 `null`）：
    /// 拒了它只会让一个少写一个字段的客户端什么都读不到，而它要问的事是清楚的。
    rpc_id: Value,
    /// 拿 message 的文本 part 拼成的 goal。
    goal: String,
    /// 对端给这条消息的 id（两版都叫 `messageId`）。它决定重发能不能落到同一行
    /// 上：见 [`goal_id_for`]。
    message_id: Option<String>,
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
            // 文本 part 三种写法都收：0.3 是 `{"kind":"text","text":…}`，早期草稿
            // 是 `{"type":"text",…}`，1.0 的 Part 是成员名判别的 `{"text":…}`（没有
            // kind 字段）。一个判别字段的写法差异不该让一份照规范写的 message
            // 变成「没有文本」。
            let is_text = part.get("kind").and_then(Value::as_str) == Some("text")
                || part.get("type").and_then(Value::as_str) == Some("text")
                || (part.get("kind").is_none()
                    && part.get("type").is_none()
                    && part.get("text").is_some());
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
    // 两版的 `Message` 都把这个字段写在同一个位置上（0.3 与 1.0 的
    // `TaskMessage::to_wire` 都是 `messageId`），所以只读这一种拼法。
    let message_id = message
        .get("messageId")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(StreamRequest {
        rpc_id,
        goal,
        message_id,
    })
}

/// 这一条消息该落到哪一行上。
///
/// 对端重发是通信面上的常态，不是例外：它自己的超时比我们的分解短、连接在
/// 半路断了、代理重试了一次——这些都会把同一条 message 再发一遍。每次新开一
/// 条 goal 意味着同一件事被分解、执行、计费两遍，两份结果还各自回流，对端最终
/// 拿到哪一份取决于它问的是哪个 id。
///
/// 所以 id 由 message 自己的 `messageId` 现算，而不是每次现抽一个：同一个
/// `messageId` 派生出同一个 id，于是「这一行已经在库里」就是重发的全部证据——
/// 读得到就返回它，读不到才受理。不查表、不新增存储，重启之后依然成立，因为
/// 映射不是被记住的，是被算出来的。
///
/// 没有 `messageId` 的请求仍然是每次新抽一个：没有幂等键就没有幂等的依据，
/// 替它编一个（比如按文本哈希）会把两个不同对端说的同一句话并成一个任务。
fn goal_id_for(message_id: Option<&str>) -> String {
    match message_id.map(str::trim).filter(|id| !id.is_empty()) {
        Some(id) => {
            let digest = Sha256::digest(id.as_bytes());
            let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
            format!("message-{hex}")
        }
        None => uuid::Uuid::new_v4().to_string(),
    }
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
    /// 请求声明的协议版本：帧按它渲染。
    version: A2aVersion,
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
    /// 快照与后续的状态帧靠**对象类型**分开（0.3 是 `kind:"task"`，1.0 是
    /// `{"task": …}` 这层包装），而不是靠「第一帧特殊」这个位置约定：位置约定
    /// 在客户端重连或从中间接入时会立刻失效，字段则一直成立。
    fn snapshot(&self, task: &Task) -> Vec<Value> {
        let body = a2a_task(task).to_wire(self.version);
        let result = match self.version {
            A2aVersion::V03 => body,
            A2aVersion::V10 => json!({"task": body}),
        };
        vec![jsonrpc_result(&self.rpc_id, result)]
    }

    /// 后续帧：状态变了才发。终态时先发产物事件，再发状态帧。
    ///
    /// 产物走规范自己的 `TaskArtifactUpdateEvent`（两版都有），不塞进状态事件：
    /// 状态事件说的是「到哪一步了」，产物是「做成了什么」。合成一帧之后，
    /// 「跑完了但没有产出」与「跑完并且有产出」在客户端读成同一个形状。
    fn status_update(&self, task: &Task, state: A2aTaskStatus) -> Vec<Value> {
        let view = a2a_task(task);
        let mut frames = Vec::new();
        if is_terminal(state) {
            for artifact in &view.artifacts {
                frames.push(jsonrpc_result(&self.rpc_id, self.artifact_frame(artifact)));
            }
        }
        let status = view.to_wire(self.version)["status"].clone();
        let frame = match self.version {
            // 0.3 的状态事件自带 `final`：客户端据此收流。1.0 取消了 `final`，
            // 终态由状态值本身表达，所以那一版不拼这个字段。
            A2aVersion::V03 => json!({
                "kind": "status-update",
                "taskId": self.goal_id,
                "contextId": self.goal_id,
                "status": status,
                "final": is_terminal(state),
            }),
            A2aVersion::V10 => json!({
                "statusUpdate": {
                    "taskId": self.goal_id,
                    "contextId": self.goal_id,
                    "status": status,
                }
            }),
        };
        frames.push(jsonrpc_result(&self.rpc_id, frame));
        frames
    }

    fn artifact_frame(&self, artifact: &TaskArtifact) -> Value {
        match self.version {
            A2aVersion::V03 => json!({
                "kind": "artifact-update",
                "taskId": self.goal_id,
                "contextId": self.goal_id,
                "artifact": artifact.to_wire(A2aVersion::V03),
            }),
            A2aVersion::V10 => json!({
                "artifactUpdate": {
                    "taskId": self.goal_id,
                    "contextId": self.goal_id,
                    "artifact": artifact.to_wire(A2aVersion::V10),
                }
            }),
        }
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
///
/// 产物只属于终态行。`error` 字段在活着的行上不是失败原因：编排器把「排队太久、
/// 放回队列、不计一次重试」这类**进展**也写在这里，而那一行仍是 Scheduled（现网
/// 2026-10-22 读到 `change-ledger-a16efa97104ac2dd` 就是这样）。不按状态分，
/// 排队中的任务会被报成「submitted + 一件叫 error 的产物」，对端只能读成失败。
fn artifacts_of(task: &Task) -> Vec<TaskArtifact> {
    if !is_terminal(a2a_state(task.status)) {
        return Vec::new();
    }
    let mut artifacts = Vec::new();
    if let Some(result) = task.result.as_ref() {
        artifacts.push(TaskArtifact {
            artifact_id: "result".into(),
            name: Some("result".into()),
            description: Some("the run's output".into()),
            parts: vec![MessagePart::Text {
                text: result.to_string(),
            }],
        });
    }
    if let Some(error) = task.error.as_ref() {
        artifacts.push(TaskArtifact {
            artifact_id: "error".into(),
            name: Some("error".into()),
            description: Some("why the run did not produce a result".into()),
            parts: vec![MessagePart::Text {
                text: error.clone(),
            }],
        });
    }
    artifacts
}

/// 一行任务在 A2A 线上、版本无关的样子。
///
/// 与流里第一帧的快照同源（同一套状态映射、同一份产物、同一句状态说明），差别只在
/// 流那侧要额外包一层事件。两处若各写一遍，「流里看到的结果」与「`tasks/get` 拿到
/// 的结果」就会各自漂——而它们说的是同一行。
fn a2a_task(task: &Task) -> A2aTask {
    A2aTask {
        id: task.id.clone(),
        // 一条 message 建一个任务，没有跨任务会话：上下文就是任务自己。
        context_id: task.id.clone(),
        status: a2a_state(task.status),
        // 行上的 `error` 在活着的行上不是失败原因，是编排器写的推进笔记
        // （排队太久、放回队列）。把它放进 `status.message` 是如实的——它在
        // 说「现在怎么着了」；失败由 `state` 说，产物里另有一件。
        status_message: task.error.clone(),
        updated_at: task.updated_at,
        artifacts: artifacts_of(task),
        history: Vec::new(),
    }
}

/// 调用方要几条历史。
///
/// 两版把这一格放在不同的地方：0.3 的 `tasks/get` 是 `params.historyLength`，
/// `message/send` 是 `params.configuration.historyLength`；1.0 的 `TaskQueryParams`
/// 也把它平铺在 `params` 上。两处都认，是因为**认错一格不会报错，只会静默地
/// 什么都不给**——而「对端要了历史、我们回了空」正是这次要修的那件事。
/// 缺省 0：没要就不给，历史不是白送的。
fn requested_history(body: &Value) -> usize {
    let params = body.get("params");
    params
        .and_then(|p| p.get("historyLength"))
        .or_else(|| {
            params
                .and_then(|p| p.get("configuration"))
                .and_then(|c| c.get("historyLength"))
        })
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize
}

/// 这条任务交换过的 message，最多 `wanted` 条（规范说的是**最近** N 条）。
///
/// 名片一直宣称 `stateTransitionHistory: true`，而 `tasks/get` 一直回 `history: []`
/// ——旗子说「我留着交换过的消息」，读回来一条也没有。这与「旗后没有端点」是同一
/// 类假承诺，只是更难看见：端点答了 200，形状也对，空的那一格没人会去核对。
///
/// 本平台不落 message 表，但这条任务**是由一条 message 建出来的**，那句话就在
/// `input.goal` 上（`Task::goal_row` 的唯一形状）。所以能给的恰好就是一条：
/// 建出这个任务的那条用户消息。给不出时回空，而不是编一条。
fn history_of(task: &Task, wanted: usize) -> Vec<TaskMessage> {
    if wanted == 0 {
        return Vec::new();
    }
    let Some(goal) = task.input.get("goal").and_then(Value::as_str) else {
        return Vec::new();
    };
    // 一次提交一条 goal，历史最多一条，所以「最近 N 条」的截断在这里没有可截的；
    // 取 `wanted` 只为把「要了才给」写清楚，别让它变成一个恒真的形状。
    let _ = wanted;
    vec![TaskMessage {
        // 同一个任务上的同一条消息，两次读必须是同一个 id：对端会拿它去重。
        message_id: format!("{}-goal", task.id),
        role: String::from("user"),
        parts: vec![MessagePart::Text {
            text: goal.to_string(),
        }],
    }]
}

/// 与 [`a2a_task`] 同源，只多带调用方要的历史。
fn a2a_task_with_history(task: &Task, wanted: usize) -> A2aTask {
    let mut view = a2a_task(task);
    view.history = history_of(task, wanted);
    view
}

// ─── `ListTasks`：翻一页对端建过的任务 ────────────────────────────────────

/// 一页默认几条、最多几条。两数出自规范对 `page_size` 的说明：不写就按 50，
/// 值域 1..=100。
const LIST_PAGE_DEFAULT: usize = 50;
const LIST_PAGE_MAX: usize = 100;

/// `ListTasks` 的过滤与分页条件（解析之后的样子）。
///
/// 解析与取页分成两半，是因为这一半全是判断：哪个值是过滤、哪个值是分页、
/// 认不出的值报不报错、写错一格的代价是「多一条」还是「一条都没有」。这半截
/// 没有库、没有状态，能单独钉住；另一半只剩「读全量、渲染」。
#[derive(Debug, Clone, PartialEq, Eq)]
struct ListQuery {
    context_id: Option<String>,
    status: Option<A2aTaskStatus>,
    page_size: usize,
    offset: usize,
    history: usize,
    include_artifacts: bool,
    updated_after: Option<chrono::DateTime<chrono::Utc>>,
}

/// 取一格可选的字符串。
///
/// 写了但不是字符串（`"status": 5`）要报错，不能当成「没写」：当成没写，这个
/// 过滤就静默地不生效，对端拿到的是全部任务——它读不出「我的过滤被丢了」，
/// 只会读出「这些任务都符合条件」。
fn optional_str<'a>(params: &'a Value, key: &str) -> Result<Option<&'a str>, String> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.as_str())),
        Some(other) => Err(format!("{key} takes a string, not {other}")),
    }
}

fn parse_list_query(body: &Value) -> Result<ListQuery, String> {
    let params = body.get("params").cloned().unwrap_or_else(|| json!({}));

    let page_size = match params.get("pageSize") {
        None | Some(Value::Null) => LIST_PAGE_DEFAULT,
        Some(value) => match value.as_i64() {
            Some(n) if n >= 1 => (n as usize).min(LIST_PAGE_MAX),
            Some(n) => return Err(format!("pageSize is at least 1, not {n}")),
            None => return Err(format!("pageSize takes an integer, not {value}")),
        },
    };

    // 空串不是错：那正是我们自己写在 `nextPageToken` 里表示「没有下一页」的值，
    // 对端把它原样带回来时应当读成第一页。
    let offset = match optional_str(&params, "pageToken")? {
        None | Some("") => 0,
        Some(token) => token.parse::<usize>().map_err(|_| {
            format!("pageToken '{token}' was not issued by this endpoint: it is the decimal offset a previous page ended after")
        })?,
    };

    let status = match optional_str(&params, "status")? {
        None | Some("") => None,
        Some(raw) => {
            let parsed = A2aTaskStatus::parse(raw);
            // `parse` 认不出的值退成 `Unknown`，而 `Unknown` 自己也是合法的线上值
            // （`unknown` / `TASK_STATE_UNSPECIFIED`）。所以判「认不认得」不看它落
            // 在哪个变体上，看这个值能不能原样拼回来：能，它就是规范里的一个状态。
            if parsed.wire(A2aVersion::V03) == raw || parsed.wire(A2aVersion::V10) == raw {
                Some(parsed)
            } else {
                return Err(format!(
                    "'{raw}' is not a task state; use one of {} / {}",
                    A2aTaskStatus::Submitted.wire(A2aVersion::V03),
                    A2aTaskStatus::Completed.wire(A2aVersion::V03),
                ));
            }
        }
    };

    let updated_after = match optional_str(&params, "statusTimestampAfter")? {
        None | Some("") => None,
        Some(raw) => Some(
            chrono::DateTime::parse_from_rfc3339(raw)
                .map_err(|e| format!("statusTimestampAfter is not an RFC 3339 instant: {e}"))?
                .with_timezone(&chrono::Utc),
        ),
    };

    let include_artifacts = match params.get("includeArtifacts") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(flag)) => *flag,
        Some(other) => return Err(format!("includeArtifacts takes a boolean, not {other}")),
    };

    Ok(ListQuery {
        // 本平台一条 message 建一个任务，没有跨任务会话：上下文就是任务自己。
        // 所以按 `contextId` 过滤就是按 id 过滤，不另立一张会话表。
        context_id: optional_str(&params, "contextId")?
            .filter(|id| !id.is_empty())
            .map(str::to_string),
        status,
        page_size,
        offset,
        history: requested_history(body),
        include_artifacts,
        updated_after,
    })
}

/// 按条件筛一遍、排定、切一页，回（这一页，下一页游标，筛完的总数）。
///
/// 顺序是契约的一部分：游标是一个位次，位次每次读都不同就翻不了页。新→旧，
/// 同一时刻按 id 兜底——两行同一毫秒更新时不能互相换位，否则第二页会重复
/// 或漏掉其中一行。
///
/// 位次是**偏移量**而不是「上一页最后一条的 id」：偏移量对同一批行稳定，
/// 而跨页期间有人提交了新任务时，新行落在队头会把后面整体推后一格，翻页
/// 可能重复一行——读端拿 `id` 去重，这是分页游标的固有代价，不是缺陷；
/// 换成 id 游标在同一场景下则可能**漏掉**一行，漏比重复更难发现。
fn list_page(tasks: Vec<Task>, query: &ListQuery) -> (Vec<Task>, String, usize) {
    let mut matched: Vec<Task> = tasks
        .into_iter()
        // 只列承载行：A2A 的任务就是一次被委派的工作，分解出来的 DAG 是实现
        // 细节。`tasks/get` 读得到子任务，是为了排障，不是它们也成了对端建的
        // 任务——列出来会让对端看见一堆它从没发过的 id。
        .filter(Task::is_goal_row)
        .filter(|task| {
            query
                .context_id
                .as_deref()
                .is_none_or(|want| task.id == want)
        })
        .filter(|task| {
            query
                .status
                .is_none_or(|want| a2a_state(task.status) == want)
        })
        .filter(|task| {
            query
                .updated_after
                .is_none_or(|after| task.updated_at >= after)
        })
        .collect();

    matched.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });

    let total = matched.len();
    let start = query.offset.min(total);
    let page: Vec<Task> = matched
        .into_iter()
        .skip(start)
        .take(query.page_size)
        .collect();
    let next = if start + page.len() < total {
        (start + page.len()).to_string()
    } else {
        String::new()
    };
    (page, next, total)
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
                    let frames = if s.last.is_none() {
                        s.snapshot(&task)
                    } else {
                        s.status_update(&task, state)
                    };
                    s.last = Some(state);
                    s.done = is_terminal(state);
                    // 一次推进可能产出多帧（终态是「产物事件 + 状态事件」）：
                    // 逐帧交给客户端，但整组作为一个 stream item 返回，因为
                    // 它们描述的是同一次观测，拆开就要在两次 poll 之间重新
                    // 读一遍库、可能读到同一个状态发两轮。
                    let body = frames
                        .iter()
                        .map(|frame| String::from_utf8_lossy(&sse_frame(frame)).into_owned())
                        .collect::<String>();
                    return Some((Ok(Bytes::from(body)), s));
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
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<Value>,
) -> Response {
    let version = match requested_version(&headers, &uri) {
        Ok(version) => version,
        Err(message) => {
            // 请求都没解释成我们服务的某一版，就不该按某一版的形状回帧：
            // 报一条 JSON-RPC 的版本错，对端按错误码就能分辨「版本不对」与
            // 「请求写错了」，而回一份 0.3 形状的流只会让它误读。
            let rpc_id = body.get("id").cloned().unwrap_or(Value::Null);
            return (StatusCode::OK, Json(rpc_error(&rpc_id, -32009, message))).into_response();
        }
    };
    message_stream_for(state, body, version).await
}

/// 版本已经定下来之后的流式受理。
///
/// 与 handler 分两截，是因为同一个方法有两个入口：名片上的统一地址（按
/// `method` 分派）和旗子指名的 `/message/stream` 路径。版本只在入口取一次，
/// 两条入口共用同一段受理逻辑，版本渲染不会各写一遍。
async fn message_stream_for(
    state: Arc<GatewayState>,
    body: Value,
    version: A2aVersion,
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
    let goal_id = goal_id_for(request.message_id.as_deref());

    // 与 HTTP 面同一个受理路径（同一行构造、同一次 spawn）：两条路各自写一遍
    // 提交逻辑，就会出现「经 A2A 提的 goal 少了某个字段」这类只在一条路上
    // 存在的缺陷。重发的判据也与 `message/send` 同一个：这一行已经在，就订阅
    // 它，不再分解一次。
    let already = if request.message_id.is_some() {
        GoalReader::goal(&state.orchestrator, &goal_id).await
    } else {
        None
    };
    if already.is_some() {
        tracing::info!(goal_id = %goal_id, "a2a message/stream repeated a message already accepted");
    } else {
        let mut goal_row = Task::goal_row(goal_id.clone(), &request.goal);
        goal_row.workspace_id = None;
        // 提交在开流之前等它落地，理由与 `message/send` 是同一个：这一行要读不回来，
        // 流能给出的只有「这一行从没出现过」——而那与「提交被拒」是两件事，对端要等
        // 满一个截止时间才能听到前者。拒了就当场报一帧，理由照实说。
        if let Err(reason) = accept_goal(&state.orchestrator, &request.goal, goal_row).await {
            // 同一个 `messageId` 的两份请求可以同时到：先到的插了行，后到的在批量
            // 注入里被跳过。库里有这一行就照样订阅，不把一次已受理读成拒收。
            let raced = request.message_id.is_some()
                && GoalReader::goal(&state.orchestrator, &goal_id)
                    .await
                    .is_some();
            if !raced {
                tracing::error!(goal_id = %goal_id, reason = %reason, "a2a message/stream was not accepted");
                let frame = sse_frame(&json!({
                    "jsonrpc": "2.0",
                    "id": request.rpc_id,
                    "error": {"code": -32603, "message": reason},
                }));
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .header(header::CACHE_CONTROL, "no-cache")
                    .header("x-accel-buffering", "no")
                    .body(Body::from(frame))
                    .unwrap_or_else(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": "stream_unavailable"})),
                        )
                            .into_response()
                    });
            }
            tracing::info!(goal_id = %goal_id, "a2a message/stream lost the race to the same message and subscribes to the row it found");
        }
        tracing::info!(goal_id = %goal_id, "a2a message/stream goal accepted");
    }

    goal_stream_response(&state, goal_id, request.rpc_id, version)
}

/// 按版本把一行任务跟到底的 SSE 响应。
///
/// 开流（收到一条新消息）与续流（请求重新订阅已有的一行）在这里汇合：两条入口
/// 之后看到的是同一个任务、同一套帧，各写一遍只会有一边忘了某条头、或者忘了
/// 终态之后收流。
fn goal_stream_response(
    state: &Arc<GatewayState>,
    goal_id: String,
    rpc_id: Value,
    version: A2aVersion,
) -> Response {
    let stream = goal_stream(StreamState {
        reader: Arc::new(state.orchestrator.clone()),
        goal_id,
        rpc_id,
        version,
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

/// 这个端点服务哪些方法，写给对端看。
///
/// 一次拒答本身不解决问题，但它得让对端知道该改用什么——所以两条拒答（不认识的
/// 方法、认识但没做的方法）读同一个常量：手抄两份，加一条方法时只会更新其中一份。
const SERVED_METHODS: &str = "this endpoint serves message/send, message/stream, tasks/get, tasks/cancel, tasks/resubscribe and ListTasks";

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

/// A2A 定义了、但这个平台不提供的方法。
///
/// 这些名字回 `-32004 UnsupportedOperation`，而不是和拼错的字符串一起回
/// `-32601 MethodNotFound`：对端要知道的差别是「你的规范我认，这个能力我没有」
/// 还是「你写的这个方法名不存在」。压成同一个码，一份照规范写了推送配置的
/// 客户端会去翻自己的拼写，而它拼得没错。
fn defined_but_unsupported(method: &str) -> bool {
    matches!(
        method,
        // 0.3 名字
        "tasks/pushNotificationConfig/set"
            | "tasks/pushNotificationConfig/get"
            | "tasks/pushNotificationConfig/list"
            | "tasks/pushNotificationConfig/delete"
            | "agent/getAuthenticatedExtendedCard"
            | "agent/authenticatedExtendedCard"
            // 1.0 名字
            | "CreateTaskPushNotificationConfig"
            | "GetTaskPushNotificationConfig"
            | "ListTaskPushNotificationConfigs"
            | "DeleteTaskPushNotificationConfig"
            | "GetExtendedAgentCard"
    )
}

/// `POST /api/v1/protocol/a2a` —— 名片上那个地址，按 `method` 分派。
///
/// 方法名两版都收（0.3 的 `message/send` 与 1.0 的 `SendMessage`），因为它们
/// 指的同一次调用；版本只决定**回执按哪一版的形状渲染**。入站宽、出站严：
/// 一份 0.3 客户端不会因为服务端也认得 PascalCase 而受害，而一份 1.0 客户端
/// 拿到 0.3 形状的回执会解析失败。
pub async fn rpc_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    uri: Uri,
    Json(body): Json<Value>,
) -> Response {
    let rpc_id = body.get("id").cloned().unwrap_or(Value::Null);
    let version = match requested_version(&headers, &uri) {
        Ok(version) => version,
        Err(message) => {
            return (StatusCode::OK, Json(rpc_error(&rpc_id, -32009, message))).into_response()
        }
    };
    match body.get("method").and_then(Value::as_str) {
        Some("message/send") | Some("SendMessage") => {
            rpc_message_send(&state, &body, &rpc_id, version).await
        }
        Some("message/stream") | Some("SendStreamingMessage") => {
            message_stream_for(state, body, version).await
        }
        Some("tasks/get") | Some("GetTask") => rpc_tasks_get(&state, &body, &rpc_id, version).await,
        Some("tasks/cancel") | Some("CancelTask") => {
            rpc_tasks_cancel(&state, &body, &rpc_id, version).await
        }
        Some("tasks/resubscribe") | Some("SubscribeToTask") => {
            rpc_tasks_resubscribe(&state, &body, &rpc_id, version).await
        }
        Some("ListTasks") => rpc_list_tasks(&state, &body, &rpc_id, version).await,
        Some(method) if defined_but_unsupported(method) => (
            StatusCode::OK,
            Json(rpc_error(
                &rpc_id,
                -32004,
                format!("'{method}' is defined by A2A but not served here; {SERVED_METHODS}"),
            )),
        )
            .into_response(),
        other => (
            StatusCode::OK,
            Json(rpc_error(
                &rpc_id,
                -32601,
                format!(
                    "unknown method '{}'; {SERVED_METHODS}",
                    other.unwrap_or("<missing>")
                ),
            )),
        )
            .into_response(),
    }
}

/// 1.0 的 `SendMessage` 回 `SendMessageResponse`，它是 `{task}|{message}` 的
/// oneof：ProtoJSON 把 oneof 渲染成「成员名 → 值」。0.3 的 `message/send` 直接
/// 回 Task 本身。这层包装只差一个键，但少了它，1.0 的客户端解出来的是一份没有
/// `task` 字段的响应——和「服务端回了个它不认识的类型」同形。
///
/// 只有这一条方法有这个包装：1.0 的 `GetTask`/`CancelTask` 直接回 `Task`。
fn send_message_result(version: A2aVersion, task: &A2aTask) -> Value {
    let body = task.to_wire(version);
    match version {
        A2aVersion::V03 => body,
        A2aVersion::V10 => json!({"task": body}),
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

/// 受理的判据：图把它收下了，而且收下的就是这张 id。
///
/// `submit_goal_auto` 返回的是**图真正收下的 id**，不是递进去的那些：批量插入会
/// 跳过已经在册的行，于是「递进去了」与「被收下了」是两件事。拿递进去的那份当
/// 回执，对端会拿到一个它以后读不回来的 id——而「没有这个任务」与「你抄错了 id」
/// 在它那里逐字同形。
fn accepted(goal_id: &str, submitted: cog_core::SFResult<Vec<String>>) -> Result<(), String> {
    match submitted {
        Ok(ids) if ids.iter().any(|id| id == goal_id) => Ok(()),
        Ok(ids) => Err(format!(
            "the goal was not taken: the graph returned {ids:?}, not '{goal_id}'"
        )),
        Err(e) => Err(format!("the goal could not be submitted: {e}")),
    }
}

/// `message/send`：受理一条 message，回**从库里读回来的**那一行。
///
/// 不等它跑完：A2A 允许这里回一个还在跑的任务，对端用 `tasks/get` 追。等它跑完
/// 再回，就把这个方法变成一次长请求——而「长」正是 `message/stream` 存在的理由，
/// 一个不流式的对端选了 `message/send`，就是接受「先拿到受理、再来问结果」。
///
/// 但「不等它跑完」不等于「不等它落库」：受理是对端手里的一个承诺——这个 id 以后
/// 一定读得回来。提交放在后台跑、失败只写一行日志，就会出现对端拿到 id、随后
/// `tasks/get` 永远回「没有这个任务」的形状，与它抄错 id 完全同形。所以这里等的是
/// **提交本身**（一次入图／落库），不是这一次运行。
///
/// 这条等待今天不是「一次入图」那么短：生产进程的编排器接了规划器
/// （`OrchestratorPlugin` 把 `action_planner` 与 skill registry 都装上），
/// 一条没有已验证子任务的 goal 会走分解——那是一次上游调用，重试次数由
/// `decomposition_max_attempts` 定。所以等的是**分解的受理结论**，不是一次
/// 数据库写入：
///
/// - 上游不可用时分解被拒，这里当场回一条带原因的拒答，不留下一个读不回来的 id；
/// - 上游可用时回的是分解之后的图回执，对端拿到就能追。
///
/// 这条路今天的代价是延迟跟着分解走。若哪天分解慢到不可接受，改法不是把等待
/// 删掉（那会退回「先答应、再失败」），而是把受理拆成两步：先落行、再由分解
/// 写结论，并让这一行在分解完成前读作 `submitted`。
///
/// **1.0 的 `SendMessage` 还有一层**：那一版规定不带 `configuration.returnImmediately`
/// 时默认**阻塞**到任务进入终态或被打断（`INPUT_REQUIRED`/`AUTH_REQUIRED`）。这不是
/// 可选的风格差异，是同一份请求在两版里承诺不同的东西：0.3 的 `message/send` 回的是
/// 「受理了」，1.0 回的是「跑完了」。按 0.3 的样子回给 1.0 的对端，它会拿一个
/// `submitted` 当最终答复收工——比报错更糟，因为它不报错。
async fn rpc_message_send(
    state: &Arc<GatewayState>,
    body: &Value,
    rpc_id: &Value,
    version: A2aVersion,
) -> Response {
    let request = match parse_message_stream(body) {
        Ok(request) => request,
        Err(message) => {
            return (StatusCode::OK, Json(rpc_error(rpc_id, -32602, message))).into_response()
        }
    };

    // 与 `POST /api/v1/tasks` 和 `message/stream` 走同一条受理路径：三条路各自
    // 写一遍提交逻辑，就会出现「经某一条路提的 goal 少了某个字段」这种只在一条
    // 路上存在的缺陷。
    let goal_id = goal_id_for(request.message_id.as_deref());
    let orchestrator = state.orchestrator.clone();

    // 重发先读、后提交：提交会经过分解，那是一次要花配额的上游调用——先读一行
    // 比再分解一次便宜得多，而这一行在，就说明这条消息已经被受理过。
    if request.message_id.is_some() {
        if let Some(existing) = GoalReader::goal(&orchestrator, &goal_id).await {
            tracing::info!(goal_id = %goal_id, "a2a message/send repeated a message already accepted");
            return send_result(rpc_id, version, &orchestrator, &goal_id, existing, body).await;
        }
    }

    let mut goal_row = Task::goal_row(goal_id.clone(), &request.goal);
    goal_row.workspace_id = None;
    // 回执读的是库里的那一行，不是本地构造的这一份：受理之后状态可能已经被派工
    // 改过，对端要的是它现在是什么样。
    let row = match accept_goal(&orchestrator, &request.goal, goal_row).await {
        Ok(row) => row,
        // 同一个 `messageId` 的两份请求可以同时到：两份都没读到行，先到的那份
        // 把行插进去，后到的那份在批量注入里按 id 被跳过、回执里没有这个 id。
        // 这不等于「没受理」——库里有这一行就是受理了，回它。
        Err(reason) => match GoalReader::goal(&orchestrator, &goal_id).await {
            Some(existing) if request.message_id.is_some() => {
                tracing::info!(
                    goal_id = %goal_id,
                    "a2a message/send lost the race to the same message and answers the row it found"
                );
                return send_result(rpc_id, version, &orchestrator, &goal_id, existing, body).await;
            }
            _ => {
                tracing::error!(goal_id = %goal_id, reason = %reason, "a2a message/send was not accepted");
                return (StatusCode::OK, Json(rpc_error(rpc_id, -32603, reason))).into_response();
            }
        },
    };
    tracing::info!(goal_id = %goal_id, "a2a message/send goal accepted");

    send_result(rpc_id, version, &orchestrator, &goal_id, row, body).await
}

/// 把一行任务按对端要的版本与形状答回去（必要时先等到终态）。
///
/// 受理成功与「这条消息已经受理过、直接答」两条路要答的是同一件事，所以收在
/// 一处：两份写法迟早会有一份忘了按版本渲染，或者忘了读 `historyLength`。
async fn send_result(
    rpc_id: &Value,
    version: A2aVersion,
    orchestrator: &Arc<dyn OrchestratorControl>,
    goal_id: &str,
    row: Task,
    body: &Value,
) -> Response {
    let row = if blocks_until_done(body, version) {
        wait_until_done(orchestrator, goal_id, &row).await
    } else {
        row
    };

    (
        StatusCode::OK,
        Json(jsonrpc_result(
            rpc_id,
            send_message_result(
                version,
                &a2a_task_with_history(&row, requested_history(body)),
            ),
        )),
    )
        .into_response()
}

/// 这一次 `SendMessage` 要不要阻塞到任务结束。
///
/// 只看 1.0：0.3 没有 `configuration.returnImmediately` 这个字段，它的
/// `message/send` 语义就是先回受理。缺省值是 `false`（阻塞）——规范把
/// 「不回就等」定为默认，读成「默认不等」会让对端以为拿到的是结果。
fn blocks_until_done(body: &Value, version: A2aVersion) -> bool {
    if version != A2aVersion::V10 {
        return false;
    }
    !body
        .get("params")
        .and_then(|params| params.get("configuration"))
        .and_then(|config| config.get("returnImmediately"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// 等到这一行进入终态（或被打断），等不到就回当前这一行。
///
/// 上限与流用同一个旋钮：两者承载的是同一类长任务，给两套上限只会让「流挂得住、
/// 阻塞请求挂不住」这种差别出现在没有理由的地方。到点仍没结束不是错误——回一行
/// 仍在跑的、带 id 的任务，对端可以接着 `tasks/get`；比回一个「超时」错误有用，
/// 因为那会把「还在跑」说成「失败了」。
async fn wait_until_done(
    orchestrator: &Arc<dyn OrchestratorControl>,
    goal_id: &str,
    first: &Task,
) -> Task {
    let deadline = tokio::time::Instant::now() + max_stream();
    let mut current = first.clone();
    // 停止点是「终态，或被打断」：规范把 `INPUT_REQUIRED`/`AUTH_REQUIRED` 也算作
    // 「可以回来了」。本平台今天没有产出这两档的来源，写在这里是为了让判据跟着
    // 规范走——哪天接上问人/Auth 的流程，这一行不用回来改。
    let stopped = |state: A2aTaskStatus| {
        state.is_terminal()
            || matches!(
                state,
                A2aTaskStatus::InputRequired | A2aTaskStatus::AuthRequired
            )
    };
    while !stopped(a2a_state(current.status)) {
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                goal_id,
                "a blocking SendMessage hit its wait ceiling; answering with the row as it stands"
            );
            return current;
        }
        tokio::time::sleep(poll_interval()).await;
        match orchestrator.get_task(goal_id).await {
            Some(row) => current = row,
            None => return current,
        }
    }
    current
}

/// `tasks/get`：按 id 读一行，带上它已有的结果与失败原因。
///
/// 这也是不流式的对端拿结果的唯一一条路——`message/send` 回的是受理那一刻的
/// 样子，而结果是在之后写进这一行的。产物与流里终态帧同源（[`artifacts_of`]）。
async fn rpc_tasks_get(
    state: &Arc<GatewayState>,
    body: &Value,
    rpc_id: &Value,
    version: A2aVersion,
) -> Response {
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
            Json(jsonrpc_result(
                rpc_id,
                a2a_task_with_history(&task, requested_history(body)).to_wire(version),
            )),
        )
            .into_response(),
        None => (
            StatusCode::OK,
            Json(rpc_error(rpc_id, -32001, format!("no task '{task_id}'"))),
        )
            .into_response(),
    }
}

/// `ListTasks`（1.0）：翻一页对端能看见的任务。
///
/// 没有这条方法时，对端对它建过的任务只有一个视野：`message/send` 当时回给它的
/// 那些 id。进程重启一次，这个视野就整个消失——任务还在跑、结果还读得回来，
/// 但已经没有那条路。`tasks/get` 答的是「我知道 id」，这条答的是「我不知道」，
/// 两者不是同一件事，前者替代不了后者。
///
/// `includeArtifacts` 缺省**不带产物**（规范如此，为的是压住整页的体积）；要
/// 结果的对端要么在请求里写 `true`，要么拿 id 去 `tasks/get`。
async fn rpc_list_tasks(
    state: &Arc<GatewayState>,
    body: &Value,
    rpc_id: &Value,
    version: A2aVersion,
) -> Response {
    let query = match parse_list_query(body) {
        Ok(query) => query,
        Err(message) => {
            return (StatusCode::OK, Json(rpc_error(rpc_id, -32602, message))).into_response()
        }
    };

    let (page, next, total) = list_page(state.orchestrator.get_all_tasks().await, &query);
    let tasks: Vec<Value> = page
        .iter()
        .map(|task| {
            let mut view = a2a_task_with_history(task, query.history);
            if !query.include_artifacts {
                view.artifacts.clear();
            }
            view.to_wire(version)
        })
        .collect();
    tracing::debug!(
        offset = query.offset,
        returned = tasks.len(),
        total,
        "a2a ListTasks"
    );
    (
        StatusCode::OK,
        Json(jsonrpc_result(
            rpc_id,
            json!({
                "tasks": tasks,
                // 空串＝没有下一页，不是「再来取一次」。用回同一个词是因为
                // 规范把它定义成「空即到底」，对端读的是这个约定。
                "nextPageToken": next,
                "pageSize": query.page_size,
                // 分页之前的总数：对端据此知道还有多远，而不是靠一页一页
                // 试到底。
                "totalSize": total,
            }),
        )),
    )
        .into_response()
}

/// `tasks/resubscribe`（0.3）／`SubscribeToTask`（1.0）：接回一条正在跑的任务。
///
/// 流会断，而任务不会因为断线停下——对端重连时拿的是它手里的那个 id，不是重新
/// 发一遍消息。没有这条方法，一个跑了二十分钟的任务在客户端掉线一次之后就只剩
/// 轮询；而轮询与流看到的不是同一份东西（流里有中间态，`tasks/get` 只有当下）。
///
/// id 不存在时**回信封错误而不是开一条流**：这一行在不在，开流之前就知道，没有
/// 理由让对端先收一条永远是空的流、再从一个错误帧里读结论。
async fn rpc_tasks_resubscribe(
    state: &Arc<GatewayState>,
    body: &Value,
    rpc_id: &Value,
    version: A2aVersion,
) -> Response {
    let Some(task_id) = rpc_task_id(body) else {
        return (
            StatusCode::OK,
            Json(rpc_error(
                rpc_id,
                -32602,
                "tasks/resubscribe takes params.id",
            )),
        )
            .into_response();
    };

    match GoalReader::goal(&state.orchestrator, task_id).await {
        Some(_) => goal_stream_response(state, task_id.to_string(), rpc_id.clone(), version),
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
async fn rpc_tasks_cancel(
    state: &Arc<GatewayState>,
    body: &Value,
    rpc_id: &Value,
    version: A2aVersion,
) -> Response {
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
                    Json(jsonrpc_result(
                        rpc_id,
                        a2a_task_with_history(&task, requested_history(body)).to_wire(version),
                    )),
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
    use axum::http::HeaderValue;
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
            version: A2aVersion::V03,
            last: None,
            seen_row: false,
            done: false,
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            interval: Duration::from_millis(5),
        }
    }

    /// 一次 stream item 里的**全部** JSON-RPC 消息。
    ///
    /// 一组帧是「同一次观测」的产物（终态是产物事件＋状态事件），所以按组返回：
    /// 只读第一帧的辅助函数会让「终态那帧带产物」这件事看起来成立，实际读到的是
    /// 产物帧，状态帧被扔掉。
    fn parse_frames(bytes: &Bytes) -> Vec<Value> {
        let text = std::str::from_utf8(bytes).expect("an SSE frame is UTF-8");
        text.split("\n\n")
            .filter(|chunk| !chunk.trim().is_empty())
            .map(|chunk| {
                let line = chunk
                    .lines()
                    .find_map(|l| l.strip_prefix("data: "))
                    .expect("every frame carries a data line");
                serde_json::from_str(line).expect("the data line is a JSON-RPC message")
            })
            .collect()
    }

    fn parse_frame(bytes: &Bytes) -> Value {
        parse_frames(bytes)
            .into_iter()
            .next()
            .expect("at least one frame")
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
        assert_eq!(first["result"]["status"]["state"], json!("submitted"));
        assert_eq!(first["result"]["id"], json!("g-1"));

        let second = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(second["result"]["kind"], json!("status-update"));
        assert_eq!(second["result"]["taskId"], json!("g-1"));
        assert_eq!(second["result"]["contextId"], json!("g-1"));
        assert_eq!(second["result"]["status"]["state"], json!("working"));
        assert_eq!(
            second["result"]["final"],
            json!(false),
            "a running task is not a final frame"
        );

        let third = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(
            third["result"]["status"]["state"],
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
        assert_eq!(second["result"]["status"]["state"], json!("working"));
        assert!(
            second["result"].get("artifacts").is_none(),
            "a status update before the end does not carry an artifact list: {second}"
        );

        // 终态那一组：产物事件在前，状态事件在后。
        let group = parse_frames(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(
            group.len(),
            2,
            "the terminal observation is an artifact frame plus a status frame: {group:?}"
        );
        assert_eq!(group[0]["result"]["kind"], json!("artifact-update"));
        assert_eq!(group[0]["result"]["taskId"], json!("g-1"));
        assert_eq!(
            group[0]["result"]["artifact"]["name"],
            json!("result"),
            "the outcome travels in an artifact named as such: {:?}",
            group[0]
        );
        assert_eq!(
            group[0]["result"]["artifact"]["parts"][0]["text"],
            json!("{\"answer\":42}"),
            "the artifact carries the task's own result rather than a rendering of it"
        );
        assert_eq!(group[1]["result"]["kind"], json!("status-update"));
        assert_eq!(group[1]["result"]["status"]["state"], json!("completed"));
        assert_eq!(group[1]["result"]["final"], json!(true));
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

    /// 还没有终态的行即使挂着一条 `error` 文本也不是失败。
    ///
    /// 编排器把「排队太久、放回队列、不计一次重试」这类进展写在 `error` 里，行仍
    /// 留在 Scheduled。把这段文本当失败原因发出去，对端拿到的是「submitted 外加
    /// 一件叫 error 的产物」——同一条回答说两件事，读哪一句都能自圆其说。终态之后
    /// 同一段文本才成为失败原因。
    #[test]
    fn a_note_on_a_row_that_is_still_queued_is_not_a_failure_artifact() {
        let note = "never started: the task sat in Scheduled and was put back in line";
        let mut row = Task::goal_row(String::from("g-2"), "a goal");
        row.error = Some(note.into());
        assert!(
            artifacts_of(&row).is_empty(),
            "a queued row has no outcome yet: {:?}",
            artifacts_of(&row)
        );

        row.status = TaskStatus::Failed;
        assert_eq!(
            artifacts_of(&row)[0].name.as_deref(),
            Some("error"),
            "once the row is terminal the same note is the failure reason"
        );
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
        let group = parse_frames(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(group.len(), 2, "{group:?}");
        assert_eq!(
            group[0]["result"]["artifact"]["name"],
            json!("error"),
            "a failure names itself instead of borrowing the result's artifact: {:?}",
            group[0]
        );
        assert_eq!(
            group[0]["result"]["artifact"]["parts"][0]["text"],
            json!("upstream refused the request")
        );
        assert_eq!(group[1]["result"]["status"]["state"], json!("failed"));
    }

    /// 受理只认图回执里那一个 id。
    ///
    /// `submit_goal_auto` 返回的是图收下的 id，可能不含递进去的那个（批量插入会
    /// 跳过在册的行）。拿递进去的那份当回执，对端会拿到一个永远读不回来的 id，
    /// 而「没有这个任务」与「你抄错了 id」在它那里同形。
    #[test]
    fn acceptance_is_the_id_the_graph_handed_back_not_the_one_we_offered() {
        assert!(
            accepted("g-1", Ok(vec!["g-1".into()])).is_ok(),
            "the graph took exactly the row we offered"
        );

        let refused = accepted("g-1", Ok(vec!["g-2".into()]))
            .expect_err("a different id is not our row: the peer would read it as never existing");
        assert!(
            refused.contains("g-2") && refused.contains("g-1"),
            "the refusal has to name both ids: {refused}"
        );

        let failed = accepted("g-1", Err(cog_core::SFError::Agent("store is down".into())))
            .expect_err("a submission that failed is not an acceptance");
        assert!(
            failed.contains("store is down"),
            "the peer's answer has to carry the reason the platform had: {failed}"
        );
    }

    /// 图什么都不收（空回执）也算没受理——不是「受理了一条没有任务可跑的 goal」。
    #[test]
    fn an_empty_receipt_is_not_an_acceptance() {
        assert!(accepted("g-1", Ok(Vec::new())).is_err());
    }

    /// 受理这一步自己要走完三步：递进去、只认回执、把库里那一行读回来。
    ///
    /// 这钉的是**接线**，不是判据：把 `accept_goal` 换回「spawn 之后回本地构造
    /// 的那一行」，判据函数仍全绿，这里会红在「读回来的不是库里那一行」。
    enum Receipt {
        Takes,
        TakesAnother(String),
        Empty,
        Fails(String),
    }

    struct ScriptedSink {
        receipt: Receipt,
        row: Option<Task>,
    }

    #[async_trait::async_trait]
    impl GoalReader for ScriptedSink {
        async fn goal(&self, _goal_id: &str) -> Option<Task> {
            self.row.clone()
        }
    }

    #[async_trait::async_trait]
    impl GoalSink for ScriptedSink {
        async fn submit(&self, _goal: &str, row: Task) -> cog_core::SFResult<Vec<String>> {
            match &self.receipt {
                Receipt::Takes => Ok(vec![row.id]),
                Receipt::TakesAnother(other) => Ok(vec![other.clone()]),
                Receipt::Empty => Ok(Vec::new()),
                Receipt::Fails(reason) => Err(cog_core::SFError::Agent(reason.clone())),
            }
        }
    }

    fn offered() -> Task {
        let mut row = Task::goal_row("g-1".to_string(), "a goal");
        row.workspace_id = None;
        row
    }

    #[tokio::test]
    async fn a_submission_the_graph_refuses_is_refused_before_any_row_is_promised() {
        for (label, receipt) in [
            ("another id", Receipt::TakesAnother("g-2".into())),
            ("nothing", Receipt::Empty),
            ("store down", Receipt::Fails("store is down".into())),
        ] {
            let sink = ScriptedSink {
                receipt,
                // A row that is readable must not rescue a submission the graph
                // did not take: the promise is about this id, not about the store.
                row: Some(offered()),
            };
            let refused = accept_goal(&sink, "a goal", offered())
                .await
                .expect_err(&format!("{label} is not an acceptance"));
            assert!(!refused.is_empty(), "{label}: the refusal has to say why");
        }
    }

    #[tokio::test]
    async fn an_accepted_goal_answers_with_the_row_the_store_holds_now() {
        let mut current = offered();
        current.status = TaskStatus::Running;
        let sink = ScriptedSink {
            receipt: Receipt::Takes,
            row: Some(current),
        };
        let row = accept_goal(&sink, "a goal", offered())
            .await
            .expect("the graph took this id and the store can read it back");
        assert_eq!(
            row.status,
            TaskStatus::Running,
            "the answer is the row read back from the store, not the copy we offered"
        );
    }

    // ─── 版本 ─────────────────────────────────────────────────────────────

    fn negotiation() -> (HeaderMap, Uri) {
        let headers = HeaderMap::new();
        let uri: Uri = "/api/v1/protocol/a2a".parse().expect("a valid path");
        (headers, uri)
    }

    /// 空/缺省的版本按 0.3 解释，认不出的版本是错误而不是回落。
    ///
    /// 回落看着「更宽容」，实际是把一份按 1.2 语义写的请求用 0.3 的形状回过去：
    /// 对端读到的会是「这个对端行为怪异」，而不是「它不说我这个版本」。
    #[test]
    fn an_absent_version_means_0_3_and_an_unknown_one_is_refused() {
        let (mut headers, uri) = negotiation();
        assert_eq!(requested_version(&headers, &uri), Ok(A2aVersion::V03));

        headers.insert("a2a-version", HeaderValue::from_static("  "));
        assert_eq!(
            requested_version(&headers, &uri),
            Ok(A2aVersion::V03),
            "an empty header is the same as no header"
        );

        headers.insert("a2a-version", HeaderValue::from_static("1.0"));
        assert_eq!(requested_version(&headers, &uri), Ok(A2aVersion::V10));
        headers.insert("a2a-version", HeaderValue::from_static("0.3"));
        assert_eq!(requested_version(&headers, &uri), Ok(A2aVersion::V03));

        for raw in ["0.1", "1.2", "latest"] {
            headers.insert("a2a-version", HeaderValue::from_str(raw).unwrap());
            let refused = requested_version(&headers, &uri)
                .expect_err("an unserved version is refused, not rounded down");
            assert!(
                refused.contains("0.3") && refused.contains("1.0"),
                "{refused}"
            );
        }
    }

    /// 查询参数是版本的第二入口（规范允许），且头优先。
    #[test]
    fn the_query_parameter_declares_the_version_and_the_header_wins() {
        let (mut headers, _) = negotiation();
        let uri: Uri = "/api/v1/protocol/a2a?a2a-version=1.0".parse().unwrap();
        assert_eq!(requested_version(&headers, &uri), Ok(A2aVersion::V10));

        headers.insert("a2a-version", HeaderValue::from_static("0.3"));
        assert_eq!(
            requested_version(&headers, &uri),
            Ok(A2aVersion::V03),
            "the header is the declared version; the query is the fallback"
        );
    }

    /// 1.0 的帧形状：快照包在 `{"task": …}` 里，状态事件叫 `statusUpdate`，
    /// 没有 `final` 字段，状态值是 `TASK_STATE_*`。
    ///
    /// 这三处与 0.3 各不相同，而且**都不是可选的**：少一层包装、多一个 `final`、
    /// 或者用 0.3 的小写状态值，1.0 的客户端都会解析失败或读错状态。
    #[tokio::test]
    async fn a_1_0_stream_renders_the_oneof_wrappers_and_the_enum_spellings() {
        let reader = ScriptedReader::with_outcome(
            vec![Some(TaskStatus::Running), Some(TaskStatus::Completed)],
            Some(json!({"answer": 42})),
            None,
        );
        let mut state = state_of(reader);
        state.version = A2aVersion::V10;
        let mut stream = Box::pin(goal_stream(state));

        let first = parse_frame(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(
            first["result"]["task"]["status"]["state"],
            json!("TASK_STATE_WORKING")
        );
        assert!(
            first["result"].get("kind").is_none(),
            "1.0 has no `kind` discriminator: {first}"
        );
        assert_eq!(first["result"]["task"]["id"], json!("g-1"));

        let group = parse_frames(&stream.next().await.expect("a frame").expect("a frame"));
        assert_eq!(group.len(), 2, "{group:?}");
        assert_eq!(
            group[0]["result"]["artifactUpdate"]["artifact"]["parts"][0]["text"],
            json!("{\"answer\":42}")
        );
        assert_eq!(
            group[1]["result"]["statusUpdate"]["status"]["state"],
            json!("TASK_STATE_COMPLETED")
        );
        assert!(
            group[1]["result"]["statusUpdate"].get("final").is_none(),
            "1.0 dropped `final`: the terminal state itself ends the stream: {:?}",
            group[1]
        );
        assert!(stream.next().await.is_none(), "the stream still ends");
    }

    /// 名片宣称 `stateTransitionHistory: true`：对端要几条历史就得真拿到，
    /// 而不是一个永远空的格子。
    #[test]
    fn a_task_returns_the_message_that_created_it_when_history_is_asked_for() {
        let row = Task::goal_row(String::from("g-1"), "刷新接口未区分访问令牌");

        // 没要就不给：空历史在 `to_wire` 里连这个键都不写。
        let none = a2a_task_with_history(&row, 0).to_wire(A2aVersion::V03);
        assert!(none.get("history").is_none(), "{none}");

        let v03 = a2a_task_with_history(&row, 1).to_wire(A2aVersion::V03);
        let history = v03["history"].as_array().expect("history is an array");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0]["kind"], json!("message"));
        assert_eq!(history[0]["role"], json!("user"));
        assert_eq!(
            history[0]["parts"][0]["text"],
            json!("刷新接口未区分访问令牌")
        );
        assert_eq!(history[0]["messageId"], json!("g-1-goal"));

        // 1.0 换了拼法：没有 `kind`，role 是 proto 枚举名。
        let v10 = a2a_task_with_history(&row, 1).to_wire(A2aVersion::V10);
        let history = v10["history"].as_array().expect("history is an array");
        assert!(history[0].get("kind").is_none(), "{}", history[0]);
        assert_eq!(history[0]["role"], json!("ROLE_USER"));
        // 同一个任务读两次必须是同一个 messageId：对端拿它去重。
        assert_eq!(history[0]["messageId"], v03["history"][0]["messageId"]);
    }

    /// `historyLength` 两版放在不同的位置，而认错一格**不会报错**——只会静默地
    /// 什么都不给，正好复现要修的那件事。
    #[test]
    fn the_history_length_is_read_from_both_shapes() {
        assert_eq!(
            requested_history(&json!({"params": {"id": "g", "historyLength": 3}})),
            3
        );
        assert_eq!(
            requested_history(&json!({"params": {"message": {},
                "configuration": {"historyLength": 2}}})),
            2
        );
        assert_eq!(requested_history(&json!({"params": {"id": "g"}})), 0);
        assert_eq!(
            requested_history(&json!({"params": {"id": "g", "historyLength": "3"}})),
            0,
            "a string is not a count"
        );
    }

    /// 同一个 `messageId` 必须派生出同一个 goal id，否则「重发读到同一行」这条
    /// 判据根本无从成立：id 每次都不一样，读的永远是空。
    #[test]
    fn the_same_message_id_always_derives_the_same_goal_id() {
        let first = goal_id_for(Some("7c9e6679-7425-40de-944b-e07fc1f90ae7"));
        let again = goal_id_for(Some("7c9e6679-7425-40de-944b-e07fc1f90ae7"));
        assert_eq!(first, again);
        assert_ne!(first, goal_id_for(Some("another-message")));
        assert!(
            first.starts_with("message-"),
            "a derived id says where it came from: {first}"
        );

        // 没有幂等键的请求不是「所有无键请求都是同一条」，而是每次都是新的：
        // 按文本之类的代理键合并，会把两个对端说的同一句话并成一个任务。
        let anon = goal_id_for(None);
        assert_ne!(anon, goal_id_for(None));
        assert_eq!(anon.len(), 36, "still one id shape for a task row: {anon}");
        let blank_a = goal_id_for(Some("   "));
        let blank_b = goal_id_for(Some("   "));
        assert_eq!(
            blank_a.len(),
            36,
            "a blank messageId is no key at all: {blank_a}"
        );
        assert_ne!(
            blank_a, blank_b,
            "with no key every send is a new goal, not one shared one"
        );
    }

    /// 1.0 的 `SendMessage` 回 `SendMessageResponse`，任务包在 `task` 键里；
    /// 0.3 直接回任务本身。
    #[test]
    fn send_message_wraps_the_task_only_in_1_0() {
        let row = Task::goal_row(String::from("g-1"), "a goal");
        let task = a2a_task(&row);
        let v03 = send_message_result(A2aVersion::V03, &task);
        assert_eq!(v03["id"], json!("g-1"));
        assert!(v03.get("task").is_none());

        let v10 = send_message_result(A2aVersion::V10, &task);
        assert_eq!(v10["task"]["id"], json!("g-1"));
        assert_eq!(
            v10["task"]["status"]["state"],
            json!("TASK_STATE_SUBMITTED")
        );
    }

    /// 一条 goal 承载行，时间与状态由用例给定。
    fn goal_row_at(id: &str, status: TaskStatus, minutes: i64) -> Task {
        let mut row = Task::goal_row(String::from(id), "a goal");
        row.status = status;
        row.updated_at = chrono::DateTime::parse_from_rfc3339("2026-10-23T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
            + chrono::Duration::minutes(minutes);
        row
    }

    /// 一条分解出来的子任务：不是承载行，列任务时不该出现。
    fn sub_task(id: &str, minutes: i64) -> Task {
        let mut row = Task::new(
            String::from(id),
            cog_core::TaskType::Planner,
            json!({"description": "a step"}),
        );
        row.goal_id = Some(String::from("g-1"));
        row.is_executable = true;
        row.updated_at = chrono::DateTime::parse_from_rfc3339("2026-10-23T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
            + chrono::Duration::minutes(minutes);
        row
    }

    /// 一页按新→旧取、只取承载行，游标接着上一页结束的位置。
    /// 判据：把 `.filter(Task::is_goal_row)` 摘掉，这条会因为多出一条子任务而红。
    #[test]
    fn a_page_lists_the_goal_rows_newest_first_and_the_cursor_continues_where_it_stopped() {
        let tasks = vec![
            goal_row_at("g-old", TaskStatus::Completed, 0),
            sub_task("sub-1", 5),
            goal_row_at("g-new", TaskStatus::Running, 10),
            goal_row_at("g-mid", TaskStatus::Pending, 5),
        ];

        let first = parse_list_query(&json!({"params": {"pageSize": 2}})).expect("plain query");
        let (page, next, total) = list_page(tasks.clone(), &first);
        assert_eq!(
            page.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec!["g-new", "g-mid"],
            "newest first, and the decomposition step is not a task the peer created"
        );
        assert_eq!(total, 3);
        assert_eq!(next, "2");

        let second = parse_list_query(&json!({"params": {"pageSize": 2, "pageToken": next}}))
            .expect("the token we just issued");
        let (page, next, total) = list_page(tasks, &second);
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, "g-old");
        assert_eq!(next, "", "the last page says there is no next one");
        assert_eq!(total, 3, "the total counts the whole filter, not the page");
    }

    /// 过滤是过滤，写错的东西要报错，不能静默变成「没有过滤」。
    /// 判据：把 `status: 5` 当成「没写」那条分支改回去，这条会红。
    #[test]
    fn a_filter_is_either_applied_or_refused_never_silently_dropped() {
        let by_state = parse_list_query(&json!({"params": {"status": "TASK_STATE_WORKING"}}))
            .expect("1.0 spelling");
        assert_eq!(by_state.status, Some(A2aTaskStatus::Working));
        let by_state_v03 =
            parse_list_query(&json!({"params": {"status": "working"}})).expect("0.3 spelling");
        assert_eq!(by_state_v03.status, Some(A2aTaskStatus::Working));

        for (params, expected) in [
            (json!({"status": 5}), "takes a string"),
            (json!({"status": "busy"}), "is not a task state"),
            (json!({"pageSize": 0}), "at least 1"),
            (json!({"pageSize": "many"}), "takes an integer"),
            (json!({"pageToken": "abc"}), "was not issued"),
            (json!({"includeArtifacts": "yes"}), "takes a boolean"),
            (json!({"statusTimestampAfter": "yesterday"}), "RFC 3339"),
        ] {
            let error = parse_list_query(&json!({"params": params}))
                .expect_err("a malformed filter is refused, not ignored");
            assert!(error.contains(expected), "{params}: {error}");
        }

        let clamped = parse_list_query(&json!({"params": {"pageSize": 500}})).expect("too many");
        assert_eq!(clamped.page_size, LIST_PAGE_MAX);
        let defaulted = parse_list_query(&json!({"params": {}})).expect("nothing asked");
        assert_eq!(defaulted.page_size, LIST_PAGE_DEFAULT);
        assert!(!defaulted.include_artifacts, "the spec defaults to false");
    }

    /// 状态、上下文、时间三个过滤各管各的；空 `pageToken` 是「第一页」。
    #[test]
    fn the_state_context_and_time_filters_each_narrow_the_page() {
        let tasks = vec![
            goal_row_at("g-1", TaskStatus::Completed, 0),
            goal_row_at("g-2", TaskStatus::Running, 5),
            goal_row_at("g-3", TaskStatus::Failed, 10),
        ];

        let running = parse_list_query(&json!({"params": {"status": "TASK_STATE_WORKING"}}))
            .expect("a state filter");
        let (page, _, total) = list_page(tasks.clone(), &running);
        assert_eq!(
            page.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec!["g-2"]
        );
        assert_eq!(total, 1);

        let one_context =
            parse_list_query(&json!({"params": {"contextId": "g-3"}})).expect("a context filter");
        let (page, _, _) = list_page(tasks.clone(), &one_context);
        assert_eq!(
            page.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec!["g-3"]
        );

        let recent =
            parse_list_query(&json!({"params": {"statusTimestampAfter": "2026-10-23T10:05:00Z"}}))
                .expect("a time filter");
        let (page, _, total) = list_page(tasks, &recent);
        assert_eq!(total, 2, "the boundary is inclusive");
        assert_eq!(
            page.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec!["g-3", "g-2"],
            "the task updated exactly at the boundary is in, the older one is out"
        );

        let echoed_empty_token =
            parse_list_query(&json!({"params": {"pageToken": ""}})).expect("our own empty token");
        assert_eq!(echoed_empty_token.offset, 0);
    }

    #[tokio::test]
    async fn an_accepted_goal_that_cannot_be_read_back_is_not_an_acceptance() {
        let sink = ScriptedSink {
            receipt: Receipt::Takes,
            row: None,
        };
        let refused = accept_goal(&sink, "a goal", offered())
            .await
            .expect_err("an id that reads back as nothing is the never-existing case again");
        assert!(refused.contains("read back"), "{refused}");
    }
}
