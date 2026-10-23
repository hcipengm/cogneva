//! A2A (Agent-to-Agent Protocol) 支持。
//! Google 推出的 Agent 间通信标准。
//! - Agent Card: 标准 Agent 描述（类型住在 `cog-core`，见下）
//! - Task: 任务生命周期管理
//! - Client: 向外部 Agent 发送任务

// ==========================================================================
// Agent Card
// ==========================================================================

// 名片类型只有一套，定义在 `cog-core`。这里曾经各自定义了一份字段相同、但
// `credentials` 不一样的副本（本 crate 是 `HashMap<String,String>`，core 是
// `serde_json::Value`），而 gateway 按 core 版构造名片对外服务、`A2aClient`
// 按本 crate 版返回拉到的名片——两边是**不同的类型，接不上**，契约 doc 许诺的
// 「gateway 永不依赖 cog-protocol 具体类型」于是只能靠再写一层转换才成立，等于
// 空话。同一件事只有一处定义、其余是它的别名，这一处就是 core；留着副本只会让
// 两份定义各自漂移。
//
// 顺带收掉一个潜伏的解析缺陷：A2A 的 credentials 是任意形状的对象，`Value` 收得下
// 嵌套值，旧的 `HashMap<String,String>` 遇到 `{"token": {"nested": ...}}` 会解析失败。
pub use cog_core::{AgentAuthentication, AgentCapabilities, AgentCard, AgentSkill};

// ==========================================================================
// A2A Task
// ==========================================================================

// 同名片那一族：类型住在 `cog-core`——A2A 的 HTTP 面由 gateway 服务，而 gateway 不许
// 依赖本 crate 的具体类型。这里只做别名。
//
// `TaskStatus` 在 core 里叫 `A2aTaskStatus`：core 根上已经有一个给自家任务存储用的
// `TaskStatus`，两者是两套状态（调度器的 vs A2A 线上的），同名会让人读错。
pub use cog_core::A2aTaskStatus as TaskStatus;
pub use cog_core::{A2aTask, A2aVersion, MessagePart, TaskArtifact, TaskMessage};

// ==========================================================================
// A2A Client
// ==========================================================================

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::Value;

/// JSON-RPC 的请求 id。对端原样回填，本客户端不靠它匹配回包（一次调用一次
/// 往返），但它必须是每条请求自己的数——对端若在日志里按 id 找请求，全用 `1`
/// 会让它的追溯面失明。
static RPC_ID: AtomicU64 = AtomicU64::new(1);

/// 向外部 Agent 发请求时声明哪一版。
///
/// 默认 1.0：它是当前已发布版本，新对端照它写，能力面也最全。但**用哪一版不是
/// 这个客户端单方面说了算的**——对端可能只服务 0.3，所以每次调用都带
/// `A2A-Version` 头，遇到「不认这一版」或「不认这个方法名」时退回 0.3 再试一次
/// （见 [`A2aClient::rpc`]）。版本是线上契约里唯一不能自己改的那一部分，客户端的
/// 责任是把自己会说的版本声明清楚、并按对端的答复收窄，而不是假设。
pub struct A2aClient {
    client: Arc<dyn cog_core::HttpClient>,
    /// 对端要的 bearer 令牌。
    ///
    /// 名片里的 `authentication.schemes` 写着 `jwt` 的平台（本平台自己就是）在鉴权
    /// 中间件后面：不带它，每条请求都回 401，而一个只会在 401 上失败的客户端与
    /// 「不会说话」是同一件事。没配令牌时请求原样发出——对公开对端，401 不会发生。
    bearer: Option<String>,
    /// 出站声明的协议版本。
    version: A2aVersion,
}

impl A2aClient {
    /// 必须交出 HttpClient 才能构造。
    ///
    /// 这里曾经可以空着手造（`new()` + `Default` + `with_client`），代价是
    /// 一个没有 client 的实例每个调用都返回「没有配置 HttpClient」——它和能
    /// 干活的实例在类型上完全一样，谁取到都看不出来。构造器要求交出 client，
    /// 就把这件事挪到编译期：没有 client 的 `A2aClient` 造不出来，也就没有
    /// 「发布了却在第一次调用失败」这条路径。
    pub fn new(client: Arc<dyn cog_core::HttpClient>) -> Self {
        Self {
            client,
            bearer: None,
            version: A2aVersion::V10,
        }
    }

    /// 给这个客户端配上对端要的 bearer 令牌。
    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    /// 固定用某一版说话（不自动收窄）。
    ///
    /// 只在调用方**已经知道**对端说哪一版时用——比如从名片里读到
    /// `supportedInterfaces` 之后。给一个不懂 1.0 的对端钉 1.0，每次调用都会先
    /// 撞一次版本错再退回来，那是可以省掉的一跳。
    pub fn with_version(mut self, version: A2aVersion) -> Self {
        self.version = version;
        self
    }

    /// 把令牌与版本声明挂到请求上。所有出站请求都经这一处，不各写一遍。
    ///
    /// `A2A-Version` 是规范定义的服务参数，HTTP 绑定下走同名请求头；缺省（不发）
    /// 按 0.3 解释，所以 0.3 的请求也要显式发——否则「我们说的是 0.3」这件事
    /// 只能靠对端猜。
    fn prepared(&self, req: cog_core::HttpRequest, version: A2aVersion) -> cog_core::HttpRequest {
        let req = match self.bearer.as_deref() {
            Some(token) => req.header("Authorization", format!("Bearer {token}")),
            None => req,
        };
        req.header("A2A-Version", version.as_str())
    }

    /// 获取目标 Agent 的 Agent Card。
    ///
    /// 名片端点也要说清版本：1.0 与 0.3 的名片形状不同（`supportedInterfaces`
    /// 对 `url`），对端按声明回一份。若对端回 400 说版本不支持，去掉版本头再取
    /// 一次——不带版本按规范就是 0.3，那正是「最宽的那一版」。
    pub async fn fetch_agent_card(&self, url: &str) -> anyhow::Result<AgentCard> {
        let req = self.prepared(cog_core::HttpRequest::get(url), self.version);
        let resp = self.client.execute(req).await?;
        let resp = if resp.status == 400 {
            self.client.execute(cog_core::HttpRequest::get(url)).await?
        } else {
            resp
        };
        let card: AgentCard = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        Ok(card)
    }

    /// 发一条 message 给对端，拿回它为这条 message 建出来的任务。
    ///
    /// 这是 A2A 的 `message/send`：**一个地址，方法在 JSON-RPC 信封里**。这里曾经是
    /// `POST {agent_url}/tasks/send`——早期草稿的形状，今天没有对端服务那条路径，
    /// 包括本平台自己。一个「客户端在、但打到谁头上都是 404」的调用，与没有客户端
    /// 是同一件事。
    ///
    /// 回来的是**受理时**的那一行（通常是 `submitted` / `working`）：要结果得再问
    /// [`Self::get_task`]。不在这里等它跑完，是因为等待的长度不该由一次 HTTP 往返
    /// 承担；要边跑边读的对端用 `message/stream`。
    pub async fn send_message(&self, agent_url: &str, text: &str) -> anyhow::Result<A2aTask> {
        let result = self
            .rpc(agent_url, |version| {
                let message = TaskMessage {
                    message_id: next_rpc_id().to_string(),
                    role: "user".into(),
                    parts: vec![MessagePart::Text { text: text.into() }],
                }
                .to_wire(version);
                let mut params = serde_json::json!({"message": message});
                if version == A2aVersion::V10 {
                    // 1.0 的 `SendMessage` 默认阻塞到终态。这个客户端的方法语义是
                    // 「拿回受理、再问 get_task」，两版要一致，所以显式关掉阻塞。
                    params["configuration"] = serde_json::json!({"returnImmediately": true});
                }
                (method_name(version, "SendMessage", "message/send"), params)
            })
            .await?;
        task_from_result(&result)
    }

    /// 发送任务到目标 Agent——把这条任务里的文本 part 作为一条 message 发出去。
    ///
    /// A2A 的 `message/send` 收的是 message，不是任务：任务的 id 由对端分配，
    /// 调用方这边那一行只是「要说什么」的载体。所以这里取的是它带的文本，
    /// 不是把它整个序列化过去。
    pub async fn send_task(&self, agent_url: &str, task: A2aTask) -> anyhow::Result<A2aTask> {
        let text = task
            .history
            .iter()
            .flat_map(|message| message.parts.iter())
            .filter_map(|part| match part {
                MessagePart::Text { text } => Some(text.as_str()),
                MessagePart::File { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        if text.trim().is_empty() {
            anyhow::bail!("the task carries no text part to send as a message");
        }
        self.send_message(agent_url, &text).await
    }

    /// 查询任务状态（含它已经产出的结果与失败原因）。
    pub async fn get_task(&self, agent_url: &str, task_id: &str) -> anyhow::Result<A2aTask> {
        let result = self
            .rpc(agent_url, |version| {
                (
                    method_name(version, "GetTask", "tasks/get"),
                    serde_json::json!({"id": task_id}),
                )
            })
            .await?;
        task_from_result(&result)
    }

    /// 一次 JSON-RPC 往返，按对端的答复收窄版本。
    ///
    /// 版本是线上契约里唯一不能由我们单方面决定的那一半：默认说 1.0，可对端可能
    /// 只服务 0.3。两种答复说明「它不说这一版」，而且都发生在**执行之前**，所以
    /// 退回 0.3 重发一次没有副作用：
    ///
    /// - `-32009`：对端明说这个版本不支持；
    /// - `-32601`：对端不认识这个**方法名**——1.0 的 PascalCase 名字在只认 0.3
    ///   的对端那里就是这样，比 `-32009` 更常见（很多 0.3 的对端根本不看版本头）。
    ///
    /// 只对 1.0 收窄一次，不来回试：0.3 是对端最宽的公共口径，它也不认就是真的
    /// 不会说。收回来的错误仍按原样抛，调用方看得到对端到底说了什么。
    async fn rpc<F>(&self, agent_url: &str, make: F) -> anyhow::Result<Value>
    where
        F: Fn(A2aVersion) -> (String, Value),
    {
        let (method, params) = make(self.version);
        match self.rpc_at(agent_url, self.version, &method, params).await {
            Ok(result) => Ok(result),
            Err(RpcFailure::Refused { code, message })
                if self.version == A2aVersion::V10 && (code == -32009 || code == -32601) =>
            {
                tracing::warn!(
                    peer = agent_url,
                    code,
                    refusal = %message,
                    "peer refused the 1.0 name; falling back to 0.3 for this call"
                );
                let (method, params) = make(A2aVersion::V03);
                self.rpc_at(agent_url, A2aVersion::V03, &method, params)
                    .await
                    .map_err(anyhow::Error::from)
            }
            Err(failure) => Err(failure.into()),
        }
    }

    /// 一次往返，只报事实：对端说了什么。
    ///
    /// 信封里的 `error` 与 HTTP 层的失败分开报：对端「认识这个方法但这一行不存在」
    /// 与「这个地址根本没有服务」是两种要处置的情形，前者重试同一行没用，后者要
    /// 查网络与地址。把它们压成同一个 `anyhow` 字符串，读日志的人就只剩一次调用
    /// 失败这件事。JSON-RPC 的 `error` 单独成一档，是因为版本收窄要读它的错误码。
    async fn rpc_at(
        &self,
        agent_url: &str,
        version: A2aVersion,
        method: &str,
        params: Value,
    ) -> Result<Value, RpcFailure> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": next_rpc_id(),
            "method": method,
            "params": params,
        });
        let req = self.prepared(
            cog_core::HttpRequest::post(agent_url)
                .json(&body)
                .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                .timeout(30),
            version,
        );
        let resp = self
            .client
            .execute(req)
            .await
            .map_err(|e| RpcFailure::Failed(anyhow::anyhow!("{e}")))?;
        if !resp.is_success() {
            return Err(RpcFailure::Failed(anyhow::anyhow!(
                "{} answered HTTP {}: {}",
                method,
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        let envelope: Value = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        if let Some(error) = envelope.get("error") {
            return Err(RpcFailure::Refused {
                code: error.get("code").and_then(Value::as_i64).unwrap_or(0),
                message: format!(
                    "{} was refused: {} (code {})",
                    method,
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("no message"),
                    error.get("code").and_then(Value::as_i64).unwrap_or(0)
                ),
            });
        }
        envelope.get("result").cloned().ok_or_else(|| {
            RpcFailure::Failed(anyhow::anyhow!(
                "{} answered without a result or an error",
                method
            ))
        })
    }
}

/// 一次调用的两种失败：对端有答复的拒（带错误码），与本地/传输层的失败。
enum RpcFailure {
    Refused { code: i64, message: String },
    Failed(anyhow::Error),
}

impl From<RpcFailure> for anyhow::Error {
    fn from(failure: RpcFailure) -> Self {
        match failure {
            RpcFailure::Refused { message, .. } => anyhow::anyhow!(message),
            RpcFailure::Failed(error) => error,
        }
    }
}

impl From<anyhow::Error> for RpcFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

/// 同一个方法在两版里的名字：1.0 是 PascalCase，0.3 是 `namespace/name`。
fn method_name(version: A2aVersion, v10: &'static str, v03: &'static str) -> String {
    match version {
        A2aVersion::V10 => v10.to_string(),
        A2aVersion::V03 => v03.to_string(),
    }
}

/// 从 `result` 里取任务。
///
/// 1.0 的 `SendMessage` 回 `SendMessageResponse`——一个 `{task}|{message}` 的
/// oneof，ProtoJSON 把它渲染成「成员名 → 值」；0.3 与 1.0 的 `GetTask`/`CancelTask`
/// 直接回任务本身。两种都读得动，调用方才不必按方法记两套解包。
fn task_from_result(result: &Value) -> anyhow::Result<A2aTask> {
    let task = result.get("task").unwrap_or(result);
    A2aTask::from_wire(task).map_err(|e| anyhow::anyhow!("the task did not parse: {}", e))
}

fn next_rpc_id() -> u64 {
    RPC_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    /// 一个记下请求、按脚本回话的对端。
    #[derive(Debug)]
    struct RecordingClient {
        seen: Mutex<Vec<cog_core::HttpRequest>>,
        answer: Value,
    }

    impl RecordingClient {
        fn answering(answer: Value) -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                answer,
            })
        }

        fn only_request(&self) -> cog_core::HttpRequest {
            let seen = self.seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "exactly one request was sent");
            seen[0].clone()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for RecordingClient {
        async fn execute(
            &self,
            req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            self.seen.lock().unwrap().push(req);
            Ok(cog_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: serde_json::to_vec(&self.answer).expect("the script is JSON"),
            })
        }
    }

    fn a_task(id: &str, state: &str) -> Value {
        json!({
            "id": id,
            "contextId": id,
            "status": {"state": state, "timestamp": "2026-10-22T00:00:00Z"},
            "history": [],
            "artifacts": [],
        })
    }

    fn body_of(req: &cog_core::HttpRequest) -> Value {
        serde_json::from_slice(req.body.as_deref().expect("a body")).expect("the body is JSON")
    }

    /// 客户端打到对端纸上的是**端点地址本身**与信封里的 `method`，不是一个
    /// 路径后缀。默认说 1.0：方法名是 PascalCase，请求头声明版本，响应包在
    /// `{"task": …}` 里（ProtoJSON 的 oneof 形状）。
    #[tokio::test]
    async fn a_message_is_sent_as_send_message_wrapped_as_a_oneof() {
        let peer = RecordingClient::answering(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"task": a_task("t-1", "TASK_STATE_SUBMITTED")},
        }));
        let client = A2aClient::new(peer.clone());

        let accepted = client
            .send_message("http://peer.example/api/v1/protocol/a2a", "hello")
            .await
            .expect("the peer accepted the message");

        let req = peer.only_request();
        assert_eq!(
            req.url, "http://peer.example/api/v1/protocol/a2a",
            "the request goes to the endpoint the card names, not to a suffix of it"
        );
        assert_eq!(req.method, "POST");
        assert_eq!(
            req.headers.get("A2A-Version").map(String::as_str),
            Some("1.0"),
            "the client declares the version it is speaking"
        );
        let body = body_of(&req);
        assert_eq!(body["method"], "SendMessage");
        assert_eq!(body["params"]["message"]["parts"][0]["text"], "hello");
        assert_eq!(
            body["params"]["message"]["role"], "ROLE_USER",
            "1.0 spells roles as proto enum names"
        );
        assert_eq!(
            body["params"]["configuration"]["returnImmediately"],
            json!(true),
            "this client returns on acceptance in both versions; it must not be made blocking by 1.0's default"
        );
        assert_eq!(accepted.id, "t-1");
        assert_eq!(accepted.status, TaskStatus::Submitted);
    }

    /// 只服务 0.3 的对端用 `-32601` 说明「不认识这个方法名」：客户端退回 0.3
    /// 重发一次，并且真的按 0.3 的形状发。
    ///
    /// 这是最常见的真实情形：大量 0.3 对端根本不看 `A2A-Version` 头，它们只是
    /// 不认识 `SendMessage` 这个名字。只认 `-32009` 的客户端在这一类对端上每次
    /// 都失败，而且失败原因看上去像是「对端坏了」。
    #[tokio::test]
    async fn an_unknown_10_method_falls_back_to_the_03_name_and_shape() {
        // 第一次答 -32601，第二次答成功——用脚本化的客户端。
        #[derive(Debug)]
        struct Scripted {
            seen: Mutex<Vec<cog_core::HttpRequest>>,
        }
        #[async_trait::async_trait]
        impl cog_core::HttpClient for Scripted {
            async fn execute(
                &self,
                req: cog_core::HttpRequest,
            ) -> cog_core::SFResult<cog_core::HttpResponse> {
                let mut seen = self.seen.lock().unwrap();
                let first = seen.is_empty();
                seen.push(req);
                let answer = if first {
                    json!({"jsonrpc": "2.0", "id": 1,
                           "error": {"code": -32601, "message": "unknown method 'SendMessage'"}})
                } else {
                    json!({"jsonrpc": "2.0", "id": 2,
                           "result": a_task("t-2", "submitted")})
                };
                Ok(cog_core::HttpResponse {
                    status: 200,
                    headers: Default::default(),
                    body: serde_json::to_vec(&answer).unwrap(),
                })
            }
        }
        let peer = Arc::new(Scripted {
            seen: Mutex::new(Vec::new()),
        });

        let accepted = A2aClient::new(peer.clone())
            .send_message("http://peer.example/a2a", "hello")
            .await
            .expect("the 0.3 retry succeeds");

        let seen = peer.seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "one refused attempt, one retry");
        assert_eq!(
            seen[1].headers.get("A2A-Version").map(String::as_str),
            Some("0.3"),
            "the retry declares the version it switched to"
        );
        let retry = body_of(&seen[1]);
        assert_eq!(retry["method"], "message/send");
        assert_eq!(
            retry["params"]["message"]["parts"][0]["kind"], "text",
            "0.3 parts carry a `kind` discriminator"
        );
        assert!(
            retry["params"].get("configuration").is_none(),
            "0.3 has no returnImmediately switch"
        );
        assert_eq!(accepted.id, "t-2");
    }

    /// `tasks/get` 也是同一个端点上的一个方法。
    #[tokio::test]
    async fn a_task_is_read_by_method_not_by_path() {
        let peer = RecordingClient::answering(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": a_task("t-1", "TASK_STATE_COMPLETED"),
        }));
        let client = A2aClient::new(peer.clone());

        let task = client
            .get_task("http://peer.example/api/v1/protocol/a2a", "t-1")
            .await
            .expect("the peer answered");

        let req = peer.only_request();
        assert_eq!(req.url, "http://peer.example/api/v1/protocol/a2a");
        let body = body_of(&req);
        assert_eq!(body["method"], "GetTask");
        assert_eq!(body["params"]["id"], "t-1");
        assert_eq!(task.status, TaskStatus::Completed);
    }

    /// 信封里的 `error` 是拒答，不是「没有结果」：它必须成为一个错误，
    /// 而不是一条解析失败。钉住 0.3 的对端上用 0.3 的名字问。
    #[tokio::test]
    async fn a_refusal_in_the_envelope_is_an_error_not_a_parse_failure() {
        let peer = RecordingClient::answering(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32001, "message": "no task 't-9'"},
        }));
        let client = A2aClient::new(peer).with_version(A2aVersion::V03);

        let err = client
            .get_task("http://peer.example/api/v1/protocol/a2a", "t-9")
            .await
            .expect_err("a refusal is an error");
        let text = err.to_string();
        assert!(
            text.contains("no task 't-9'") && text.contains("-32001"),
            "the refusal has to keep what the peer said and its code: {text}"
        );
    }

    /// 名片只有一套类型：`A2aClient` 从远端拉回来的那张，与 gateway 对外服务的
    /// 那张，是同一个 `cog_core::AgentCard`。这条断言在**编译期**成立——哪天有人
    /// 再在本 crate 里定义一份自己的名片，`let same: AgentCard = card;` 立刻不编译。
    /// 这正是「两套 DTO 接不上」那个缺陷的判据：不是运行时读出来的，是类型系统挡的。
    #[test]
    fn the_card_the_client_parses_is_the_card_the_gateway_serves() {
        // A2A 的 credentials 是任意形状的对象；旧的那份副本把它收成
        // `HashMap<String,String>`，下面这个嵌套值在它那里解析不出来。
        let wire = serde_json::json!({
            "name": "remote",
            "description": "a remote agent",
            "url": "https://remote.example/a2a",
            "version": "1.0.0",
            "protocolVersion": "0.3",
            "capabilities": {
                "streaming": true,
                "pushNotifications": false,
                "stateTransitionHistory": true,
            },
            "skills": [],
            "authentication": {
                "schemes": ["bearer"],
                "credentials": {"token": {"nested": "kept"}},
            },
        });

        let card: cog_core::AgentCard = serde_json::from_value(wire).expect("an agent card parses");
        assert_eq!(
            card.authentication
                .as_ref()
                .expect("the old-style card carries authentication")
                .credentials
                .as_ref()
                .unwrap()["token"]["nested"],
            serde_json::json!("kept"),
            "a credential object keeps its shape across the wire"
        );
        assert_eq!(card.protocol_version(), Some(A2aVersion::V03));

        // 1.0 的名片没有 `url`，端点与版本都在 `supportedInterfaces` 里。
        let v10 = serde_json::json!({
            "name": "remote",
            "description": "a remote agent",
            "version": "1.0.0",
            "supportedInterfaces": [
                {"url": "https://remote.example/a2a", "protocolBinding": "JSONRPC",
                 "protocolVersion": "1.0"},
                {"url": "https://remote.example/a2a", "protocolBinding": "JSONRPC",
                 "protocolVersion": "0.3"},
            ],
            "capabilities": {"streaming": true},
            "skills": [],
        });
        let card: cog_core::AgentCard =
            serde_json::from_value(v10).expect("a 1.0 agent card parses");
        assert_eq!(card.endpoint(), Some("https://remote.example/a2a"));
        assert_eq!(card.protocol_version(), Some(A2aVersion::V10));
        assert!(
            !card.capabilities.state_transition_history,
            "a capability the card does not mention reads as false, not as a parse failure"
        );

        // 就是同一个类型，换成本 crate 的名字也认。
        let same: AgentCard = card;
        assert_eq!(same.name, "remote");
    }
}
