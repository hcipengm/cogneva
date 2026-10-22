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
pub use cog_core::{A2aTask, MessagePart, TaskArtifact, TaskMessage};

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

/// A2A Client — 向外部 Agent 发送任务。
pub struct A2aClient {
    client: Arc<dyn cog_core::HttpClient>,
    /// 对端要的 bearer 令牌。
    ///
    /// 名片里的 `authentication.schemes` 写着 `jwt` 的平台（本平台自己就是）在鉴权
    /// 中间件后面：不带它，每条请求都回 401，而一个只会在 401 上失败的客户端与
    /// 「不会说话」是同一件事。没配令牌时请求原样发出——对公开对端，401 不会发生。
    bearer: Option<String>,
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
        }
    }

    /// 给这个客户端配上对端要的 bearer 令牌。
    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    /// 把令牌挂到请求上。所有出站请求都经这一处，不各写一遍。
    fn authorized(&self, req: cog_core::HttpRequest) -> cog_core::HttpRequest {
        match self.bearer.as_deref() {
            Some(token) => req.header("Authorization", format!("Bearer {token}")),
            None => req,
        }
    }

    /// 获取目标 Agent 的 Agent Card。
    pub async fn fetch_agent_card(&self, url: &str) -> anyhow::Result<AgentCard> {
        let req = self.authorized(cog_core::HttpRequest::get(url));
        let resp = self.client.execute(req).await?;
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
        let message = serde_json::json!({
            "kind": "message",
            "messageId": next_rpc_id().to_string(),
            "role": "user",
            // `kind` 是 A2A 0.2 的 part 类型字段，`type` 是更早草稿的名字：两个都带上，
            // 一份照任一版本写的对端都能认出这是一段文本。
            "parts": [{"kind": "text", "type": "text", "text": text}],
        });
        self.rpc(
            agent_url,
            "message/send",
            serde_json::json!({"message": message}),
        )
        .await
    }

    /// 发送任务到目标 Agent——把这条任务里的文本 part 作为一条 message 发出去。
    ///
    /// A2A 的 `message/send` 收的是 message，不是任务：任务的 id 由对端分配，
    /// 调用方这边那一行只是「要说什么」的载体。所以这里取的是它带的文本，
    /// 不是把它整个序列化过去。
    pub async fn send_task(&self, agent_url: &str, task: A2aTask) -> anyhow::Result<A2aTask> {
        let text = task
            .messages
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
        self.rpc(agent_url, "tasks/get", serde_json::json!({"id": task_id}))
            .await
    }

    /// 一次 JSON-RPC 往返。
    ///
    /// 信封里的 `error` 与 HTTP 层的失败分开报：对端「认识这个方法但这一行不存在」
    /// 与「这个地址根本没有服务」是两种要处置的情形，前者重试同一行没用，后者要
    /// 查网络与地址。把它们压成同一个 `anyhow` 字符串，读日志的人就只剩一次调用
    /// 失败这件事。
    async fn rpc(&self, agent_url: &str, method: &str, params: Value) -> anyhow::Result<A2aTask> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": next_rpc_id(),
            "method": method,
            "params": params,
        });
        let req = self.authorized(
            cog_core::HttpRequest::post(agent_url)
                .json(&body)
                .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
                .timeout(30),
        );
        let resp = self.client.execute(req).await?;
        if !resp.is_success() {
            anyhow::bail!(
                "{} answered HTTP {}: {}",
                method,
                resp.status,
                String::from_utf8_lossy(&resp.body)
            );
        }
        let envelope: Value = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        if let Some(error) = envelope.get("error") {
            anyhow::bail!(
                "{} was refused: {} (code {})",
                method,
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("no message"),
                error.get("code").and_then(Value::as_i64).unwrap_or(0)
            );
        }
        let result = envelope
            .get("result")
            .ok_or_else(|| anyhow::anyhow!("{} answered without a result or an error", method))?;
        serde_json::from_value(result.clone())
            .map_err(|e| anyhow::anyhow!("the task did not parse: {}", e))
    }
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

    fn a_task(id: &str, status: &str) -> Value {
        json!({
            "id": id,
            "status": status,
            "messages": [],
            "artifacts": [],
            "created_at": "2026-10-22T00:00:00Z",
            "updated_at": "2026-10-22T00:00:00Z",
        })
    }

    /// 客户端打到对端纸上的是**端点地址本身**与信封里的 `method`，不是一个
    /// 路径后缀。这条断言在两种写法之间二选一时是判据：`{url}/tasks/send` 那个
    /// 形状今天没有对端服务，包括本平台自己。
    #[tokio::test]
    async fn a_message_is_sent_to_the_endpoint_itself_as_message_send() {
        let peer = RecordingClient::answering(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": a_task("t-1", "submitted"),
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
        let body: Value =
            serde_json::from_slice(req.body.as_deref().expect("a body")).expect("the body is JSON");
        assert_eq!(body["method"], "message/send");
        assert_eq!(body["params"]["message"]["parts"][0]["text"], "hello");
        assert_eq!(accepted.id, "t-1");
        assert_eq!(accepted.status, TaskStatus::Submitted);
    }

    /// `tasks/get` 也是同一个端点上的一个方法。
    #[tokio::test]
    async fn a_task_is_read_by_method_not_by_path() {
        let peer = RecordingClient::answering(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": a_task("t-1", "completed"),
        }));
        let client = A2aClient::new(peer.clone());

        let task = client
            .get_task("http://peer.example/api/v1/protocol/a2a", "t-1")
            .await
            .expect("the peer answered");

        let req = peer.only_request();
        assert_eq!(req.url, "http://peer.example/api/v1/protocol/a2a");
        let body: Value =
            serde_json::from_slice(req.body.as_deref().expect("a body")).expect("the body is JSON");
        assert_eq!(body["method"], "tasks/get");
        assert_eq!(body["params"]["id"], "t-1");
        assert_eq!(task.status, TaskStatus::Completed);
    }

    /// 信封里的 `error` 是拒答，不是「没有结果」：它必须成为一个错误，
    /// 而不是一条解析失败。
    #[tokio::test]
    async fn a_refusal_in_the_envelope_is_an_error_not_a_parse_failure() {
        let peer = RecordingClient::answering(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32001, "message": "no task 't-9'"},
        }));
        let client = A2aClient::new(peer);

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
            "capabilities": {
                "streaming": true,
                "push_notifications": false,
                "state_transition_history": true,
            },
            "skills": [],
            "authentication": {
                "schemes": ["bearer"],
                "credentials": {"token": {"nested": "kept"}},
            },
        });

        let card: cog_core::AgentCard = serde_json::from_value(wire).expect("an agent card parses");
        assert_eq!(
            card.authentication.credentials.as_ref().unwrap()["token"]["nested"],
            serde_json::json!("kept"),
            "a credential object keeps its shape across the wire"
        );

        // 就是同一个类型，换成本 crate 的名字也认。
        let same: AgentCard = card;
        assert_eq!(same.name, "remote");
    }
}
