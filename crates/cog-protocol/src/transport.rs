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
//!   读的是行。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, warn};

use crate::mcp::{McpTool, McpToolCall, McpToolResult, McpTransport};

/// 本传输层实现的是 MCP 的 HTTP+SSE 修订版（2024-11-05）：GET 一条事件流、
/// 由 `endpoint` 事件点名消息端点、请求走 POST、应答走事件流。
///
/// 这个串会随 `initialize` 发出去，服务端据此决定用哪一版的语义回话；它是**协议
/// 版本号**，不是可以随手改的配置——换掉它等于换了对面该按哪份规范答话。
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// 一次请求—应答的期限，以及握手（连上到拿到 `endpoint`）的期限。
///
/// 默认取系统 `http_client.timeout_secs`：这是本仓里「一次请求能等多久」的那一处
/// 定义，传输层不另立一个。
fn default_request_timeout() -> Duration {
    Duration::from_secs(cog_net::HttpClientConfig::default().timeout_secs)
}

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
    http: reqwest::Client,
    request_timeout: Duration,
    /// 会话按需建立：第一次请求时才连流、握手，之后复用。流断掉之后下一次请求会
    /// 重新建一条，而不是在这一条上继续等。
    session: Mutex<Option<Arc<SseSession>>>,
}

impl SseTransport {
    /// 用系统 `http_client` 段构造（流式客户端：没有总超时，见
    /// [`cog_net::build_streaming_client`]）。
    ///
    /// 地址在这里就解析一次：写错的端点是一条**启动时**就该报的配置错误，而拖到第一次
    /// 工具调用才报，找到它的时候那个配变量的人早已不在现场。
    pub fn new(endpoint: impl Into<String>) -> cog_core::SFResult<Self> {
        let endpoint = endpoint.into();
        reqwest::Url::parse(&endpoint).map_err(|e| {
            cog_core::SFError::Config(format!("mcp sse endpoint {endpoint:?} is not a URL: {e}"))
        })?;
        let config = cog_net::HttpClientConfig::load()?;
        let http = cog_net::build_streaming_client(&config)?;
        Ok(Self::with_client_and_timeout(
            endpoint,
            http,
            Duration::from_secs(config.timeout_secs),
        ))
    }

    /// 用给定的客户端构造，期限取系统默认。测试注入用，生产走 [`Self::new`]。
    pub fn with_client(endpoint: impl Into<String>, http: reqwest::Client) -> Self {
        Self::with_client_and_timeout(endpoint, http, default_request_timeout())
    }

    fn with_client_and_timeout(
        endpoint: impl Into<String>,
        http: reqwest::Client,
        request_timeout: Duration,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            http,
            request_timeout,
            session: Mutex::new(None),
        }
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
        let resp = self
            .http
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("mcp sse connect to {url} failed: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("mcp sse endpoint {url} -> HTTP {status}");
        }
        // 内容类型不对就当场报：把一份 HTML 错误页当事件流读，会读成「一个事件都没
        // 有」；而服务端说的其实是别的意思。
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !content_type.starts_with("text/event-stream") {
            anyhow::bail!(
                "mcp sse endpoint {url} answered Content-Type {content_type:?}, not text/event-stream"
            );
        }

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let (endpoint_tx, endpoint_rx) = oneshot::channel::<Result<String, String>>();
        tokio::spawn(read_sse_stream(
            resp,
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
            .request_on(
                session,
                "initialize",
                serde_json::json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "cogneva",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                }),
            )
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
        let posted = self
            .http
            .post(&session.message_url)
            .json(&body)
            .send()
            .await;
        match posted {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => {
                session.pending.lock().await.remove(&id);
                anyhow::bail!(
                    "mcp {method}: POST to {} -> HTTP {}",
                    session.message_url,
                    resp.status()
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
        let resp = self
            .http
            .post(&session.message_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!("mcp {method}: POST to {} failed: {e}", session.message_url)
            })?;
        if !resp.status().is_success() {
            anyhow::bail!(
                "mcp {method}: POST to {} -> HTTP {}",
                session.message_url,
                resp.status()
            );
        }
        Ok(())
    }
}

/// 读事件流那一头：把 `endpoint` 事件交给连接方，把 `message` 事件里的 JSON-RPC
/// 应答按 `id` 交给等着的人。
async fn read_sse_stream(
    resp: reqwest::Response,
    stream_url: reqwest::Url,
    pending: Pending,
    closed: Arc<AtomicBool>,
    endpoint_tx: oneshot::Sender<Result<String, String>>,
) {
    let mut events = resp.bytes_stream().eventsource();
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
        let result = self.request("tools/list", serde_json::json!({})).await?;
        let tools = result
            .get("tools")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
        Ok(serde_json::from_value(tools)?)
    }

    async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
        let result = self.request("tools/call", tool_call_params(&call)).await?;
        Ok(serde_json::from_value(result)?)
    }
}
