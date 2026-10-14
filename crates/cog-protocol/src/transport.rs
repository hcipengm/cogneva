//! MCP 传输层：HTTP+SSE 与 stdio。
//!
//! 两者的共同形状是 **JSON-RPC 2.0 上的请求—应答**：请求带 `id` 出去，应答按同一个
//! `id` 回来，中间可能插着与本次无关的通知。差别只在承载：
//!
//! - **SSE**（MCP 的 HTTP 传输）：客户端 GET 一条 `text/event-stream`，服务端先以
//!   `endpoint` 事件告知消息端点，之后请求 POST 到那个地址，**应答从事件流上回来**
//!   ——不是 POST 的响应体。这一点是这个传输层最容易做错的地方：把 POST 的响应体当
//!   应答读，会读到一个 202 空体，而那与服务端「稍后从流上给你」并不矛盾，只是读错
//!   了地方；反过来，把 GET 的响应体当一份 JSON 读，读到的是一串事件帧的一部分。
//!   所以这里真的解析事件流，且应答按 `id` 匹配。
//! - **stdio**：子进程的标准输入输出，一行一条 JSON-RPC。没有 HTTP、没有事件帧，
//!   读的是行。这一条上有两件事与 HTTP 那边不同源，都是「进程」带来的：子进程会把
//!   诊断信息写到**标准错误**上（不读它，管道写满，子进程就停在那里），以及子进程
//!   会**退出**（那时标准输出到尽头，还在等的调用必须当场失败，而不是等下去）。
//!
//! 另一头也在这个文件里：[`serve`] 是**服务端**方向——把 cogneva 的工具按同一份协议
//! 回答出去。两个方向共用一套 JSON-RPC 形状（`mcp.rs` 里的类型与 `McpServer::handle`
//! 的应答规则），差别只在谁拉起了谁：客户端这边是它去拉一个子进程，服务端这边是它
//! 被一个客户端拉起，所以服务端那条上没有「子进程会退出」的问题，多出来的是**输出
//! 就是线**——服务端往标准输出写诊断信息，等于给对面发了一条读不懂的消息。

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, warn};

use crate::mcp::{McpTool, McpToolCall, McpToolResult, McpTransport};

/// 本传输层实现的是 MCP 的 HTTP+SSE 修订版（2024-11-05）：GET 一条事件流、
/// 由 `endpoint` 事件点名消息端点、请求走 POST、应答走事件流。
///
/// 这个串会随 `initialize` 发出去，服务端据此决定用哪一版的语义回话；它是**协议
/// 版本号**，不是可以随手改的配置——换掉它等于换了对面该按哪份规范答话。
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// 一条会话上按 `id` 等着的应答方。
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<serde_json::Value, String>>>>>;

/// 把应答交还给等待者；没有人在等（超时后已摘掉、或 id 从未发出）就丢掉。
async fn deliver(pending: &Pending, id: u64, value: Result<serde_json::Value, String>) {
    let waiter = pending.lock().await.remove(&id);
    if let Some(tx) = waiter {
        let _ = tx.send(value);
    } else {
        debug!(id, "mcp: a reply arrived with nobody waiting for it");
    }
}

/// 会话结束（流断、子进程退出）时，把还在等的请求全部叫醒。
///
/// 不做这一步的失败形状很隐蔽：等待者不会报错，它会一直等下去，于是调用方看到的
/// 是「慢」，而真实情况是「不会来了」。
async fn fail_all_pending(pending: &Pending, reason: &str) {
    let waiters: Vec<_> = pending.lock().await.drain().map(|(_, tx)| tx).collect();
    for tx in waiters {
        let _ = tx.send(Err(reason.to_string()));
    }
}

/// 把一份 JSON-RPC 应答拆成 `result`：错误信封要在这里变成 `Err`，不能当成功往下走
/// ——`{"jsonrpc":"2.0","id":1,"error":{...}}` 里没有 `result`，按 `result` 取会得到
/// `null`，而 `null` 会被下游读成「服务端答了一个空结果」。
fn unwrap_jsonrpc(method: &str, value: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    if let Some(err) = value.get("error") {
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        let message = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("no message");
        anyhow::bail!("mcp {method} failed: {message} (code {code})");
    }
    Ok(value
        .get("result")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

fn jsonrpc_request(id: u64, method: &str, params: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn jsonrpc_notification(method: &str, params: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"jsonrpc": "2.0", "method": method, "params": params})
}

/// 工具名与参数，`tools/call` 的 params 形状（MCP 定义在 request params 上，
/// 不在 JSON-RPC 的顶层）。
fn tool_call_params(call: &McpToolCall) -> serde_json::Value {
    serde_json::json!({"name": call.name, "arguments": call.arguments})
}

/// `initialize` 的 params：客户端自报的协议版本与身份。
///
/// 两条传输共用这一份，不是省几行：对面据此决定按哪一版规范回话，两份副本就是两版
/// 可能对不上的话。
fn initialize_params() -> serde_json::Value {
    serde_json::json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {
            "name": "cogneva",
            "version": env!("CARGO_PKG_VERSION"),
        },
    })
}

/// `tools/list` 的应答体 → 工具表。缺 `tools` 是空表，不是错误：服务端可以有零个工具。
fn parse_tools(result: serde_json::Value) -> anyhow::Result<Vec<McpTool>> {
    let tools = result
        .get("tools")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    Ok(serde_json::from_value(tools)?)
}

/// `tools/call` 的应答体 → 调用结果。
fn parse_tool_result(result: serde_json::Value) -> anyhow::Result<McpToolResult> {
    Ok(serde_json::from_value(result)?)
}

// ==========================================================================
// SSE
// ==========================================================================

/// 一条活着的 SSE 会话。
struct SseSession {
    /// 服务端在 `endpoint` 事件里点名的消息端点（已按流地址解析成绝对地址）。
    message_url: String,
    pending: Pending,
    next_id: AtomicU64,
    /// 读流那一头已结束。置位后这条会话不能再发请求：应答不会来了。
    closed: Arc<AtomicBool>,
}

/// MCP 的 HTTP+SSE 传输。真的按事件流解析（见本文件头部）。
pub struct SseTransport {
    endpoint: String,
    /// 由 `net` 插件发布的那一份**流式**客户端：整体请求期限不在它身上，事件流才
    /// 不会在短交互的期限上被切断。请求—应答自己的期限由 `request_timeout` 逐次
    /// 给（POST 走 [`cog_core::HttpRequest::timeout`]，等应答与等 `endpoint` 事件
    /// 各包一层 `tokio::time::timeout`）。
    http: Arc<dyn cog_core::HttpClient>,
    request_timeout: Duration,
    /// 会话按需建立：第一次请求时才连流、握手，之后复用。流断掉之后下一次请求会
    /// 重新建一条，而不是在这一条上继续等。
    session: Mutex<Option<Arc<SseSession>>>,
}

impl SseTransport {
    /// 用调用方交进来的客户端与请求期限构造。
    ///
    /// 客户端与期限都来自 `net` 插件发布的那两份服务，传输层不自己读配置、不另立
    /// 一个数字：这里的等待不在 HTTP 调用本身上——应答是**另一条连接**上回来的，
    /// 客户端的整体期限管不到它——但两个期限是同一件事的两面，各写一份迟早会一个
    /// 先到时另一个还在等。
    ///
    /// 地址在这里就解析一次：写错的端点是一条**启动时**就该报的配置错误，而拖到第一次
    /// 工具调用才报，找到它的时候那个配变量的人早已不在现场。
    pub fn new(
        endpoint: impl Into<String>,
        http: Arc<dyn cog_core::HttpClient>,
        request_timeout: Duration,
    ) -> cog_core::SFResult<Self> {
        let endpoint = endpoint.into();
        reqwest::Url::parse(&endpoint).map_err(|e| {
            cog_core::SFError::Config(format!("mcp sse endpoint {endpoint:?} is not a URL: {e}"))
        })?;
        Ok(Self {
            endpoint,
            http,
            request_timeout,
            session: Mutex::new(None),
        })
    }

    /// 取一条可用会话：没有、或上一条的流已经断了，就重连一次。
    async fn session(&self) -> anyhow::Result<Arc<SseSession>> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            if !session.closed.load(Ordering::Acquire) {
                return Ok(session.clone());
            }
            debug!("mcp sse: previous stream ended, reconnecting");
        }
        let session = self.connect().await?;
        *guard = Some(session.clone());
        Ok(session)
    }

    /// 连流、读 `endpoint`、握手。返回一条可以发请求的会话。
    async fn connect(&self) -> anyhow::Result<Arc<SseSession>> {
        let url = reqwest::Url::parse(&self.endpoint).map_err(|e| {
            anyhow::anyhow!("mcp sse endpoint {:?} is not a URL: {e}", self.endpoint)
        })?;
        // 连流这一笔**不带**请求期限：它会一直开着，期限由客户端持有（流式那一份
        // 没有整体期限）。
        let resp = self
            .http
            .execute_stream(
                cog_core::HttpRequest::get(url.as_str()).header("Accept", "text/event-stream"),
            )
            .await
            .map_err(|e| anyhow::anyhow!("mcp sse connect to {url} failed: {e}"))?;
        if !(200..300).contains(&resp.status) {
            anyhow::bail!("mcp sse endpoint {url} -> HTTP {}", resp.status);
        }
        // 内容类型不对就当场报：把一份 HTML 错误页当事件流读，会读成「一个事件都没
        // 有」；而服务端说的其实是别的意思。
        let content_type = resp
            .headers
            .get("content-type")
            .cloned()
            .unwrap_or_default();
        if !content_type.starts_with("text/event-stream") {
            anyhow::bail!(
                "mcp sse endpoint {url} answered Content-Type {content_type:?}, not text/event-stream"
            );
        }

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let (endpoint_tx, endpoint_rx) = oneshot::channel::<Result<String, String>>();
        tokio::spawn(read_sse_stream(
            resp.stream,
            url.clone(),
            pending.clone(),
            closed.clone(),
            endpoint_tx,
        ));

        let message_url = match tokio::time::timeout(self.request_timeout, endpoint_rx).await {
            Ok(Ok(Ok(url))) => url,
            Ok(Ok(Err(e))) => anyhow::bail!("mcp sse endpoint {url}: {e}"),
            // 发送端被丢下＝读流那一头已经结束，且没说端点在哪。
            Ok(Err(_)) => {
                anyhow::bail!("mcp sse stream from {url} ended before it named a message endpoint")
            }
            Err(_) => anyhow::bail!(
                "mcp sse endpoint {url} did not send an `endpoint` event within {:?}",
                self.request_timeout
            ),
        };

        let session = Arc::new(SseSession {
            message_url,
            pending,
            next_id: AtomicU64::new(1),
            closed,
        });
        self.initialize(&session).await?;
        Ok(session)
    }

    /// `initialize` 握手：先问能力（这一步有应答），再发 `initialized` 通知
    /// （这一步按规范没有应答，发出去就往下走）。
    async fn initialize(&self, session: &Arc<SseSession>) -> anyhow::Result<()> {
        // 应答体里带回服务端自己的 capabilities/serverInfo，本传输层不读它：不发它
        // 的话服务端通常直接拒绝后续请求，收下它则要在这里存一份没人用的副本。
        let _ = self
            .request_on(session, "initialize", initialize_params())
            .await?;
        self.notify_on(session, "notifications/initialized", serde_json::json!({}))
            .await?;
        Ok(())
    }

    async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let session = self.session().await?;
        self.request_on(&session, method, params).await
    }

    /// 在给定会话上发一次请求。应答从事件流上回来，按 `id` 认领。
    async fn request_on(
        &self,
        session: &Arc<SseSession>,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        if session.closed.load(Ordering::Acquire) {
            anyhow::bail!("mcp sse stream is closed; {method} cannot be answered");
        }
        let id = session.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // 先登记再 POST：反过来的话，答得快的服务端会在登记之前把应答送到，于是
        // 那条应答被当成「没人在等」丢掉，而这次调用会一直等到超时。
        session.pending.lock().await.insert(id, tx);

        let body = jsonrpc_request(id, method, params);
        let post = cog_core::HttpRequest::post(&session.message_url)
            .json(&body)
            .map(|req| req.timeout(self.request_timeout.as_secs().max(1)));
        let posted = match post {
            Ok(req) => self.http.execute(req).await,
            Err(e) => Err(e),
        };
        match posted {
            Ok(resp) if resp.is_success() => {}
            Ok(resp) => {
                session.pending.lock().await.remove(&id);
                anyhow::bail!(
                    "mcp {method}: POST to {} -> HTTP {}",
                    session.message_url,
                    resp.status
                );
            }
            Err(e) => {
                session.pending.lock().await.remove(&id);
                anyhow::bail!("mcp {method}: POST to {} failed: {e}", session.message_url);
            }
        }

        match tokio::time::timeout(self.request_timeout, rx).await {
            Ok(Ok(Ok(value))) => unwrap_jsonrpc(method, value),
            Ok(Ok(Err(e))) => anyhow::bail!("mcp {method}: {e}"),
            // 发送端被丢：登记被清掉了（会话收尾），或这条流那边出了问题。
            Ok(Err(_)) => anyhow::bail!("mcp {method}: the reply channel was dropped"),
            Err(_) => {
                session.pending.lock().await.remove(&id);
                anyhow::bail!(
                    "mcp {method}: no reply on the event stream within {:?}",
                    self.request_timeout
                )
            }
        }
    }

    /// 发一条没有应答的 JSON-RPC 通知。
    async fn notify_on(
        &self,
        session: &Arc<SseSession>,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<()> {
        let body = jsonrpc_notification(method, params);
        let req = cog_core::HttpRequest::post(&session.message_url)
            .json(&body)?
            .timeout(self.request_timeout.as_secs().max(1));
        let resp = self.http.execute(req).await.map_err(|e| {
            anyhow::anyhow!("mcp {method}: POST to {} failed: {e}", session.message_url)
        })?;
        if !resp.is_success() {
            anyhow::bail!(
                "mcp {method}: POST to {} -> HTTP {}",
                session.message_url,
                resp.status
            );
        }
        Ok(())
    }
}

/// 读事件流那一头：把 `endpoint` 事件交给连接方，把 `message` 事件里的 JSON-RPC
/// 应答按 `id` 交给等着的人。
async fn read_sse_stream(
    body: cog_core::HttpBodyStream,
    stream_url: reqwest::Url,
    pending: Pending,
    closed: Arc<AtomicBool>,
    endpoint_tx: oneshot::Sender<Result<String, String>>,
) {
    let mut events = body.eventsource();
    let mut endpoint_tx = Some(endpoint_tx);
    while let Some(event) = events.next().await {
        let event = match event {
            Ok(e) => e,
            Err(e) => {
                warn!("mcp sse stream from {stream_url} failed: {e}");
                break;
            }
        };
        match event.event.as_str() {
            "endpoint" => {
                let Some(tx) = endpoint_tx.take() else {
                    // 规范里端点只报一次；再来一次是服务端的问题，不是重连信号。
                    debug!("mcp sse: ignoring a second `endpoint` event");
                    continue;
                };
                let resolved = stream_url.join(event.data.trim()).map(|u| u.to_string());
                let _ = tx.send(resolved.map_err(|e| {
                    format!("its `endpoint` event {:?} is not a URL: {e}", event.data)
                }));
            }
            "message" => {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data) else {
                    warn!(
                        "mcp sse: an event on the stream is not JSON, dropping it: {:?}",
                        event.data
                    );
                    continue;
                };
                match value.get("id").and_then(|v| v.as_u64()) {
                    Some(id) => deliver(&pending, id, Ok(value)).await,
                    // 没有 id ＝ 通知（或服务端的请求，本传输层不回话）：它不是本次
                    // 任何一次调用的应答，认领不了。
                    None => debug!("mcp sse: notification on the stream: {value}"),
                }
            }
            other => debug!("mcp sse: ignoring event type {other:?}"),
        }
    }

    // 收尾。顺序要紧：先置位（新的调用会去重连，而不会往这条死流上发），再把还在
    // 等的叫醒。
    closed.store(true, Ordering::Release);
    if let Some(tx) = endpoint_tx.take() {
        let _ = tx.send(Err(
            "the event stream ended before it named a message endpoint".into(),
        ));
    }
    fail_all_pending(
        &pending,
        "the MCP event stream closed before the reply arrived",
    )
    .await;
}

#[async_trait::async_trait]
impl McpTransport for SseTransport {
    async fn list_tools(&self) -> anyhow::Result<Vec<McpTool>> {
        parse_tools(self.request("tools/list", serde_json::json!({})).await?)
    }

    async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
        parse_tool_result(self.request("tools/call", tool_call_params(&call)).await?)
    }
}

// ==========================================================================
// stdio
// ==========================================================================

/// 一条活着的 stdio 会话。
struct StdioSession {
    /// 子进程。留着它不是为了读它的退出码，是为了让它的命跟着这条会话走
    /// （`kill_on_drop`）：这条会话被换掉、或传输层被丢掉时，进程一起走，不留一条
    /// 既没人读、也没人收的进程在跑。
    _child: Child,
    stdin: Mutex<ChildStdin>,
    pending: Pending,
    next_id: AtomicU64,
    /// 子进程的标准输出已到尽头。置位后这条会话不能再发请求：应答不会来了。
    closed: Arc<AtomicBool>,
}

/// MCP 的 stdio 传输：起一个子进程，一行一条 JSON-RPC（见本文件头部）。
pub struct StdioTransport {
    program: String,
    args: Vec<String>,
    /// 子进程自己的环境变量。MCP 服务端常靠环境拿它要读的路径或令牌，所以这是配置
    /// 的一部分，不是给测试开的口子。
    env: Vec<(String, String)>,
    request_timeout: Duration,
    /// 会话按需建立：第一次请求时才起进程、握手，之后复用。子进程的标准输出到尽头
    /// 之后下一次请求会另起一个，而不是在这一条上继续等。
    session: Mutex<Option<Arc<StdioSession>>>,
}

impl StdioTransport {
    /// 起 `program`、带上 `args`。
    ///
    /// 空命令在这里就拒：这是一条**配置错误**，而拖到第一次工具调用才报，找到它的
    /// 时候配它的那个人早已不在现场。（子进程起不起得来要等真去起的时候才知道，
    /// 与 SSE 那条一样属于连接期的事。）
    pub fn new(
        program: impl Into<String>,
        args: Vec<String>,
        request_timeout: Duration,
    ) -> cog_core::SFResult<Self> {
        let program = program.into();
        if program.trim().is_empty() {
            return Err(cog_core::SFError::Config(
                "mcp stdio: the command to start is empty".into(),
            ));
        }
        Ok(Self {
            program,
            args,
            env: Vec::new(),
            request_timeout,
            session: Mutex::new(None),
        })
    }

    /// 给子进程的额外环境变量（父进程的环境照常继承）。
    pub fn with_env(mut self, env: Vec<(String, String)>) -> Self {
        self.env = env;
        self
    }

    /// 这句话用于日志与报错：读的人要靠它认出是哪个子进程。
    fn describe(&self) -> String {
        if self.args.is_empty() {
            self.program.clone()
        } else {
            format!("{} {}", self.program, self.args.join(" "))
        }
    }

    /// 取一条可用会话：没有、或上一条的子进程已经不再答话，就另起一个。
    async fn session(&self) -> anyhow::Result<Arc<StdioSession>> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            if !session.closed.load(Ordering::Acquire) {
                return Ok(session.clone());
            }
            debug!("mcp stdio: the previous child stopped answering, starting another");
        }
        let session = self.start().await?;
        *guard = Some(session.clone());
        Ok(session)
    }

    /// 起进程、接上三个管道、握手。返回一条可以发请求的会话。
    async fn start(&self) -> anyhow::Result<Arc<StdioSession>> {
        let describe = self.describe();
        let mut command = tokio::process::Command::new(&self.program);
        command
            .args(&self.args)
            .envs(self.env.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|e| anyhow::anyhow!("mcp stdio: cannot start {describe}: {e}"))?;
        // 三个管道都要拿走：少拿一个，那个管道就没有读者，子进程写满它就会停在那里
        // —— 一个该答的都答完了的服务端，看上去就是「卡住」。
        let stdin = child.stdin.take().expect("the child's stdin is piped");
        let stdout = child.stdout.take().expect("the child's stdout is piped");
        let stderr = child.stderr.take().expect("the child's stderr is piped");

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        tokio::spawn(read_child_output(
            stdout,
            pending.clone(),
            closed.clone(),
            describe.clone(),
        ));
        tokio::spawn(drain_child_errors(stderr, describe));

        let session = Arc::new(StdioSession {
            _child: child,
            stdin: Mutex::new(stdin),
            pending,
            next_id: AtomicU64::new(1),
            closed,
        });
        self.initialize(&session).await?;
        Ok(session)
    }

    /// `initialize` 握手：先问能力（这一步有应答），再发 `initialized` 通知
    /// （这一步按规范没有应答，写出去就往下走）。
    async fn initialize(&self, session: &Arc<StdioSession>) -> anyhow::Result<()> {
        let _ = self
            .request_on(session, "initialize", initialize_params())
            .await?;
        self.notify_on(session, "notifications/initialized", serde_json::json!({}))
            .await?;
        Ok(())
    }

    async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let session = self.session().await?;
        self.request_on(&session, method, params).await
    }

    /// 在给定会话上发一次请求。应答从子进程的标准输出上回来，按 `id` 认领。
    async fn request_on(
        &self,
        session: &Arc<StdioSession>,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        if session.closed.load(Ordering::Acquire) {
            anyhow::bail!("the MCP stdio child has closed its output; {method} cannot be answered");
        }
        let id = session.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        // 先登记再写：反过来的话，答得快的服务端会在登记之前把应答送到（本机管道上
        // 就是快），那条应答被当成「没人在等」丢掉，而这次调用会一直等到超时。
        session.pending.lock().await.insert(id, tx);

        let mut frame = serde_json::to_string(&jsonrpc_request(id, method, params))?;
        frame.push('\n');
        if let Err(e) = write_frame(&session.stdin, frame.as_bytes()).await {
            session.pending.lock().await.remove(&id);
            anyhow::bail!("mcp {method}: writing to the stdio child failed: {e}");
        }

        match tokio::time::timeout(self.request_timeout, rx).await {
            Ok(Ok(Ok(value))) => unwrap_jsonrpc(method, value),
            Ok(Ok(Err(e))) => anyhow::bail!("mcp {method}: {e}"),
            Ok(Err(_)) => anyhow::bail!("mcp {method}: the reply channel was dropped"),
            Err(_) => {
                session.pending.lock().await.remove(&id);
                anyhow::bail!(
                    "mcp {method}: the stdio child sent no reply within {:?}",
                    self.request_timeout
                )
            }
        }
    }

    /// 发一条没有应答的 JSON-RPC 通知。
    async fn notify_on(
        &self,
        session: &Arc<StdioSession>,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<()> {
        let mut frame = serde_json::to_string(&jsonrpc_notification(method, params))?;
        frame.push('\n');
        write_frame(&session.stdin, frame.as_bytes())
            .await
            .map_err(|e| anyhow::anyhow!("mcp {method}: writing to the stdio child failed: {e}"))
    }
}

/// 一行一帧地写进子进程的标准输入，写完就冲：管道不是行缓冲的，留在缓冲里的尾巴
/// 正是服务端在等的那一条。
async fn write_frame(stdin: &Mutex<ChildStdin>, frame: &[u8]) -> std::io::Result<()> {
    let mut stdin = stdin.lock().await;
    stdin.write_all(frame).await?;
    stdin.flush().await
}

/// 读子进程的标准输出那一头：一行一条 JSON-RPC，应答按 `id` 交给等着的人。
///
/// 两种「输出不对」要分开：一行不是 JSON 只丢那一行（服务端往标准输出上打日志是
/// 常事，为此把整条会话判死，会让一次工具调用失败在一个与它无关的打印上），而输出
/// **到尽头是另一件事** —— 那意味着这条会话不会再有应答。
async fn read_child_output(
    stdout: ChildStdout,
    pending: Pending,
    closed: Arc<AtomicBool>,
    describe: String,
) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(e) => {
                warn!("mcp stdio: reading the output of {describe} failed: {e}");
                break;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            warn!("mcp stdio: a line from {describe} is not JSON, dropping it: {line:?}");
            continue;
        };
        match value.get("id").and_then(|v| v.as_u64()) {
            Some(id) => deliver(&pending, id, Ok(value)).await,
            // 没有 id ＝ 通知（或服务端自己的请求，本传输层不回话）：它不是本次任何
            // 一次调用的应答，认领不了。
            None => debug!("mcp stdio: a message from {describe} with no id: {value}"),
        }
    }

    // 收尾。顺序与 SSE 那边一样：先置位（新的调用会去另起一个进程，而不会往这条死
    // 会话上发），再把还在等的叫醒。
    closed.store(true, Ordering::Release);
    fail_all_pending(
        &pending,
        &format!("the MCP stdio child {describe} closed its output before the reply arrived"),
    )
    .await;
}

/// 把子进程的标准错误读走。
///
/// 这不是为了记日志（尽管它顺手记了）：管道写满，子进程就停在那里写。一个把诊断
/// 信息写到标准错误上的服务端，会在它自己以为答完之后再也不动，从这一头看是「这次
/// 调用没有应答」，真因是没人读它的标准错误。行里的诊断留在 debug，不往上层报。
async fn drain_child_errors(stderr: ChildStderr, describe: String) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        debug!("mcp stdio {describe}: {line}");
    }
}

#[async_trait::async_trait]
impl McpTransport for StdioTransport {
    async fn list_tools(&self) -> anyhow::Result<Vec<McpTool>> {
        parse_tools(self.request("tools/list", serde_json::json!({})).await?)
    }

    async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
        parse_tool_result(self.request("tools/call", tool_call_params(&call)).await?)
    }
}

// ==========================================================================
// 服务端方向：把 cogneva 的工具当 MCP tools 回答出去
// ==========================================================================

/// 服务一个 MCP 客户端：输入一行一条 JSON-RPC，输出一行一条应答。
///
/// 读的方向反了，但**要求一样是「输出就是线」**：既然是行分隔的 JSON，那么任何
/// 一行不是 JSON-RPC 的东西都会被对面当成一条读不懂的消息——诊断信息必须走标准错误。
/// 这条在这里不是提醒，`serve` 只往 `output` 写应答。
///
/// 一条读不懂的行要回 `-32700`（`id: null`）：`id` 就藏在解析失败的那段文本里，
/// 拿不到它，协议给的形状就是空 id。丢掉不回在这里是错的选择——调用方会一直等一条
/// 永远不会来的应答。
///
/// 输入读到尽头（客户端关了自己那一头）就正常收工：那是「问完了」，不是故障。
pub async fn serve<R, W>(server: &crate::McpServer, input: R, mut output: W) -> anyhow::Result<()>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut lines = input.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(e) => {
                warn!("mcp server: reading a line from the client failed: {e}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let message = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(message) => message,
            Err(e) => {
                warn!("mcp server: a line from the client is not JSON: {e}");
                let reply = crate::mcp::error_reply(
                    serde_json::Value::Null,
                    -32700,
                    format!("Parse error: {e}"),
                );
                write_message(&mut output, &reply).await?;
                continue;
            }
        };
        // 通知（没有 id）没有应答，`handle` 用 `None` 说了这件事。
        if let Some(reply) = server.handle(&message).await {
            write_message(&mut output, &reply).await?;
        }
    }
    Ok(())
}

/// 在本进程的标准输入输出上服务一个 MCP 客户端。
///
/// 写一行就冲一次：对面按行读，攒着不发等于对面的调用一直不返回。
async fn write_message<W: tokio::io::AsyncWrite + Unpin>(
    output: &mut W,
    message: &serde_json::Value,
) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(message)?;
    line.push(b'\n');
    output.write_all(&line).await?;
    output.flush().await?;
    Ok(())
}

/// 用本进程的标准输入输出跑一个 MCP 服务端（stdio 是**由客户端拉起**的那条传输：
/// 谁来拉起它，谁就是它的信任边界）。
pub async fn serve_stdio(server: &crate::McpServer) -> anyhow::Result<()> {
    let input = tokio::io::BufReader::new(tokio::io::stdin());
    serve(server, input, tokio::io::stdout()).await
}

// ==========================================================================
// 从配置选一条
// ==========================================================================

/// 按配置选一条 MCP 传输，返回传输与「哪条配置选出了它」（给日志和报错用）。
///
/// 两条配置各对应一条传输：`COGNEVA_MCP_ENDPOINT` 是 HTTP+SSE 的地址，
/// `COGNEVA_MCP_COMMAND` 是要起的子进程（stdio，参数见 `COGNEVA_MCP_ARGS`）。
///
/// 两样都配、或只配了参数没有命令，一律**拒绝启动**，而不是挑一个能用的：挑一个会把
/// 另一条配置变成一句谁也读不到的谎 —— 写它的人以为它生效了，系统却照另一条走。两样
/// 都没配是正常状态（没有外部 MCP 服务端）。
pub fn transport_from_config(
    endpoint: Option<String>,
    command: Option<String>,
    args: Option<String>,
    http: Arc<dyn cog_core::HttpClient>,
    request_timeout: Duration,
) -> cog_core::SFResult<Option<(Box<dyn McpTransport>, String)>> {
    match (endpoint, command) {
        (Some(_), Some(_)) => Err(cog_core::SFError::Config(
            "both COGNEVA_MCP_ENDPOINT (HTTP+SSE) and COGNEVA_MCP_COMMAND (stdio) are set: \
             an MCP client talks to one server over one transport, and picking one of them \
             here would leave the other looking configured while nothing reads it"
                .into(),
        )),
        (Some(endpoint), None) => {
            let transport = SseTransport::new(endpoint.clone(), http, request_timeout)?;
            Ok(Some((Box::new(transport), format!("sse {endpoint}"))))
        }
        (None, Some(command)) => {
            let args = parse_command_args(args.as_deref())?;
            let transport = StdioTransport::new(command.clone(), args, request_timeout)?;
            Ok(Some((Box::new(transport), format!("stdio {command}"))))
        }
        (None, None) => match args {
            Some(_) => Err(cog_core::SFError::Config(
                "COGNEVA_MCP_ARGS is set but COGNEVA_MCP_COMMAND is not: there is no command \
                 for those arguments to belong to"
                    .into(),
            )),
            None => Ok(None),
        },
    }
}

/// `COGNEVA_MCP_ARGS`：要传给子进程的参数，写成 JSON 数组。
///
/// 不按空白切：一个带空格的路径会被切成两条参数，而服务端收到的是另一条没人配过的
/// 命令 —— 那不是「配置写得不规范」，是另一件事。JSON 数组没有这个歧义，写错了也
/// 当场报出来，报错里带上正确的写法。
fn parse_command_args(raw: Option<&str>) -> cog_core::SFResult<Vec<String>> {
    let Some(raw) = raw.map(str::trim).filter(|r| !r.is_empty()) else {
        return Ok(Vec::new());
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| {
        cog_core::SFError::Config(format!(
            "COGNEVA_MCP_ARGS is not JSON: {e}; it holds the arguments as a JSON array of \
             strings, e.g. [\"-y\", \"some-mcp-server\", \"/data\"]"
        ))
    })?;
    let serde_json::Value::Array(items) = value else {
        return Err(cog_core::SFError::Config(format!(
            "COGNEVA_MCP_ARGS is {value}, not a JSON array of strings"
        )));
    };
    items
        .into_iter()
        .map(|item| match item {
            serde_json::Value::String(s) => Ok(s),
            other => Err(cog_core::SFError::Config(format!(
                "COGNEVA_MCP_ARGS holds {other}, which is not a string"
            ))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The selection tests never send a request — they ask which configuration
    /// wins — so a client that says so is the honest one: a stand-in that
    /// answered anything would hide a construction path that tried to use it.
    #[derive(Debug)]
    struct UnusedClient;

    #[async_trait::async_trait]
    impl cog_core::HttpClient for UnusedClient {
        async fn execute(
            &self,
            _req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            unimplemented!("the selection tests never send a request")
        }
    }

    fn unused_http() -> Arc<dyn cog_core::HttpClient> {
        Arc::new(UnusedClient)
    }

    /// 选中的是哪条配置。把传输层丢掉：测试只问「选了谁」，起进程是另一条路的事
    /// （在 `tests/mcp_stdio.rs` 里问）。
    fn selected(
        endpoint: Option<&str>,
        command: Option<&str>,
        args: Option<&str>,
    ) -> cog_core::SFResult<Option<String>> {
        transport_from_config(
            endpoint.map(str::to_string),
            command.map(str::to_string),
            args.map(str::to_string),
            unused_http(),
            Duration::from_secs(30),
        )
        .map(|selection| selection.map(|(_, what)| what))
    }

    fn refused(endpoint: Option<&str>, command: Option<&str>, args: Option<&str>) -> String {
        match selected(endpoint, command, args) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("this configuration has to be refused, not resolved"),
        }
    }

    #[test]
    fn the_transport_named_is_the_one_its_configuration_asks_for() {
        assert_eq!(
            selected(Some("http://127.0.0.1:9/sse"), None, None).unwrap(),
            Some("sse http://127.0.0.1:9/sse".into())
        );
        assert_eq!(
            selected(None, Some("some-mcp-server"), Some("[\"-y\", \"pkg\"]")).unwrap(),
            Some("stdio some-mcp-server".into())
        );
        assert_eq!(
            selected(None, None, None).unwrap(),
            None,
            "no configuration is not an error: it is an MCP client that was not asked for"
        );
    }

    #[test]
    fn configuring_both_transports_is_refused_rather_than_resolved() {
        let message = refused(
            Some("http://127.0.0.1:9/sse"),
            Some("some-mcp-server"),
            None,
        );
        assert!(
            message.contains("COGNEVA_MCP_ENDPOINT") && message.contains("COGNEVA_MCP_COMMAND"),
            "the refusal has to name both configurations, so whoever wrote them can tell \
             which two it is looking at: {message}"
        );
    }

    #[test]
    fn arguments_without_a_command_are_refused() {
        let message = refused(None, None, Some("[\"--flag\"]"));
        assert!(
            message.contains("COGNEVA_MCP_COMMAND"),
            "the refusal has to name the configuration that is missing: {message}"
        );
    }

    #[test]
    fn argument_lists_that_are_not_a_json_array_of_strings_are_refused() {
        for raw in ["--flag", "{}-o", "[\"ok\", 3]", "[\"ok\"] trailing"] {
            let message = refused(None, Some("some-mcp-server"), Some(raw));
            assert!(
                message.contains("COGNEVA_MCP_ARGS"),
                "{raw:?} has to be refused by name: {message}"
            );
        }
        assert_eq!(
            selected(None, Some("some-mcp-server"), Some("   ")).unwrap(),
            Some("stdio some-mcp-server".into()),
            "an empty value is the same as not setting it"
        );
    }

    #[test]
    fn an_empty_command_is_refused() {
        let message = refused(None, Some("   "), None);
        assert!(message.contains("empty"), "{message}");
    }
}
