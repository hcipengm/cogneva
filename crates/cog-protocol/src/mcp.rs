//! MCP (Model Context Protocol) 支持。
//! - Server: 将 cogneva 的 ToolRegistry 以 MCP 协议暴露
//! - Client: 调用外部 MCP servers 的 tools
//! - Transport: 见 [`crate::transport`]（HTTP+SSE），以及 stdio

use cog_core::{ToolCall, ToolDefinition, ToolExecutor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ==========================================================================
// MCP Server
// ==========================================================================

// ─── 服务端应答哪些方法，以及对外声明了哪些能力 ──────────────────────────────
//
// 这两件事过去各写各的：派发是一个 `match` 字面量，`initialize` 的应答体里
// `"capabilities": {"tools": {}}` 是另一个字面量。于是「对外声明」与「实际应答」
// 可以各自漂移——补一个 `resources/list` 的应答臂，名片照旧只报 tools；把两个
// tools 臂删掉，名片照旧报 tools。对端照着名片动作，看到的是「能力列表」与
// 「真能调用的东西」不符。
//
// 修法与 A2A 名片那一处同形：**声明由端点算出**，不由函数体里的字面量给出。
// 方法名只有这一份拼写，派发与声明都引它；能力表列出每项能力由哪些方法支撑，
// `initialize` 的 `capabilities` 逐项从表算出。如实报关的能力（本服务端不支持的）
// 形状是**不写这一行**，与写在函数体里的 `false`/`{}` 读起来一样，但补上端点
// 那天会自己变亮。

/// `initialize` —— 建连握手。MCP 规定每个服务端都答它。
pub const METHOD_INITIALIZE: &str = "initialize";
/// `ping` —— 活性探测。同上，协议级方法，不是客户端可选的能力。
pub const METHOD_PING: &str = "ping";
/// `tools/list`。
pub const METHOD_TOOLS_LIST: &str = "tools/list";
/// `tools/call`。
pub const METHOD_TOOLS_CALL: &str = "tools/call";

/// 服务端对外声明的一项能力：名字，以及支撑它的那些方法。
///
/// **没有方法支撑的能力不进这张表**：写进来就等于代表一个从未实现的端点宣布它
/// 做成了。
pub struct AdvertisedCapability {
    pub capability: &'static str,
    pub methods: &'static [&'static str],
}

/// 名片上宣称的能力。今天只有 `tools` 一项，它由 `tools/list` 与 `tools/call`
/// 两个方法支撑——两个都在，这一项才亮。
pub const ADVERTISED_CAPABILITIES: &[AdvertisedCapability] = &[AdvertisedCapability {
    capability: "tools",
    methods: &[METHOD_TOOLS_LIST, METHOD_TOOLS_CALL],
}];

/// 服务端应答、但**不是**「客户端可选的能力」的那几个方法：协议本身要求每个
/// 服务端都答的握手与探活。它们进不了能力表（没有任何客户端会去开启它们），但
/// 它们是应答集的一部分，所以要有名字，否则「应答集＝声明集」这条不变式会把它
/// 们算成没声明过的方法。
pub const NON_CAPABILITY_METHODS: &[&str] = &[METHOD_INITIALIZE, METHOD_PING];

/// 服务端应答的方法全集：声明出来的能力所支撑的方法，加协议级的那些。
///
/// 派发只许应答案集里的名字，而这个名字集由上面两处声明拼出——于是一边加应答臂
/// 一边忘声明这件事，在集合上就已经不成立。
pub fn served_methods() -> Vec<&'static str> {
    let mut methods: Vec<&'static str> = ADVERTISED_CAPABILITIES
        .iter()
        .flat_map(|capability| capability.methods.iter().copied())
        .chain(NON_CAPABILITY_METHODS.iter().copied())
        .collect();
    methods.sort_unstable();
    methods.dedup();
    methods
}

/// `initialize` 应答里的 `capabilities` 对象，逐项从 [`ADVERTISED_CAPABILITIES`]
/// 算出。
///
/// 每项的值是空对象：MCP 里子字段（如 `listChanged`）都是可选的，本服务端一个
/// 都不提供；写 `{}` 是「这项能力在，但它没有可选项」，与「这项能力不在」是
/// 两件事——后者是键根本不出现。
pub fn capabilities_json() -> serde_json::Value {
    let mut declared = serde_json::Map::new();
    for capability in ADVERTISED_CAPABILITIES {
        declared.insert(capability.capability.to_string(), serde_json::json!({}));
    }
    serde_json::Value::Object(declared)
}

/// MCP Server 实现 — 暴露 cogneva tools 为 MCP tools。
pub struct McpServer {
    tools: Vec<ToolDefinition>,
    executor: Option<Arc<dyn ToolExecutor>>,
}

impl McpServer {
    pub fn new(tools: Vec<ToolDefinition>) -> Self {
        Self {
            tools,
            executor: None,
        }
    }

    /// 绑定一个真实的工具执行器。
    pub fn with_executor(mut self, executor: Arc<dyn ToolExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// 生成 MCP tool 列表。
    pub fn list_tools(&self) -> Vec<McpTool> {
        self.tools
            .iter()
            .map(|t| McpTool {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.parameters.clone(),
            })
            .collect()
    }

    /// 执行 MCP tool 调用。
    pub async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
        let tool = self
            .tools
            .iter()
            .find(|t| t.name == call.name)
            .ok_or_else(|| anyhow::anyhow!("Tool not found: {}", call.name))?;

        let tool_call = ToolCall {
            id: uuid::Uuid::new_v4().to_string(),
            name: call.name.clone(),
            arguments: serde_json::to_value(&call.arguments)?,
        };

        match &self.executor {
            Some(executor) => match executor.execute(&call.name, tool_call.arguments).await {
                Ok(result) => Ok(McpToolResult {
                    content: vec![McpContent::text(result.to_string())],
                    is_error: false,
                }),
                Err(e) => Ok(McpToolResult {
                    content: vec![McpContent::text(format!("Tool execution error: {}", e))],
                    is_error: true,
                }),
            },
            None => {
                // 未绑定执行器时返回描述性 mock（便于独立测试）
                let _ = tool_call;
                Ok(McpToolResult {
                    content: vec![McpContent::text(format!(
                        "Tool '{}' is registered but no executor is bound. Pass Arc<dyn ToolExecutor> via with_executor() to enable real execution.",
                        tool.name
                    ))],
                    is_error: true,
                })
            }
        }
    }

    /// 一条 JSON-RPC 消息的应答；`None` 表示不必答。
    ///
    /// `None` 只有一种来由，且是协议规定的：**通知**（不带 `id` 的消息）没有应答。
    /// 给它编一条应答，对面拿不到任何东西去配对——它没有 id。
    ///
    /// 三种失败分得开（这一层是协议面，不是业务面）：
    /// - **协议错误**：方法不存在（`-32601`）、参数不是 `tools/call` 要的形状
    ///   （`-32602`）。这类错误的正文是给调用方看的，它必须能据此知道自己发错了什么。
    /// - **工具错误**：工具跑了但失败了 —— 那不是协议错误，是一条 `isError: true` 的
    ///   **正常应答**。把它写成协议错误，等于告诉调用方「你请求的方法不存在」。
    /// - **解析错误**：整个消息不是 JSON —— 那时连 `id` 都不存在，只能按协议的
    ///   `-32700` 用 `id: null` 回，见 [`crate::transport::serve`]。
    ///
    /// 下面 `match` 的每个模式都取 [`served_methods`] 里的那个常量，不写第二遍
    /// 方法名——应答集与声明集共用一个拼写，是「对外声明的能力与实际应答一致」
    /// 这条不变式的落点：多一个应答臂就多一个名字，而这个名字必须先在某处声明过。
    pub async fn handle(&self, message: &serde_json::Value) -> Option<serde_json::Value> {
        let Some(id) = message.get("id").cloned() else {
            // 通知：`notifications/initialized` 就在这一类里，它本就无应答。
            return None;
        };
        let Some(method) = message.get("method").and_then(|m| m.as_str()) else {
            return Some(error_reply(id, -32600, "Invalid Request: no method"));
        };

        match method {
            METHOD_INITIALIZE => Some(result_reply(
                id,
                serde_json::json!({
                    "protocolVersion": crate::transport::MCP_PROTOCOL_VERSION,
                    "capabilities": capabilities_json(),
                    "serverInfo": {
                        "name": "cogneva",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                }),
            )),
            METHOD_PING => Some(result_reply(id, serde_json::json!({}))),
            METHOD_TOOLS_LIST => Some(result_reply(
                id,
                serde_json::json!({"tools": self.list_tools()}),
            )),
            METHOD_TOOLS_CALL => Some(self.call_reply(id, message).await),
            other => Some(error_reply(
                id,
                -32601,
                format!("Method not found: {other}"),
            )),
        }
    }

    /// `tools/call` 一路：参数先判形状，工具不存在按协议错误回。
    async fn call_reply(
        &self,
        id: serde_json::Value,
        message: &serde_json::Value,
    ) -> serde_json::Value {
        let params = message.get("params");
        let Some(name) = params.and_then(|p| p.get("name")).and_then(|n| n.as_str()) else {
            return error_reply(id, -32602, "Invalid params: tools/call needs a tool name");
        };
        let arguments = params
            .and_then(|p| p.get("arguments"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let serde_json::Value::Object(arguments) = arguments else {
            // 参数是工具自己的 inputSchema 决定的对象；不是对象就是调用方给错了，
            // 在这里就断，别把它猜成 `{}`——那样一次参数错误的调用会看起来像一次
            // 不带参数的调用。
            return error_reply(
                id,
                -32602,
                "Invalid params: tools/call arguments must be a JSON object",
            );
        };

        match self
            .call_tool(McpToolCall {
                name: name.to_string(),
                arguments: arguments.into_iter().collect(),
            })
            .await
        {
            // 工具跑了但失败：这是**结果**，不是协议错误（见 `handle` 的说明）。
            Ok(result) => result_reply(id, serde_json::to_value(result).unwrap_or_else(|e| {
                serde_json::json!({
                    "content": [{"type": "text", "text": format!("unserializable tool result: {e}")}],
                    "isError": true,
                })
            })),
            // 工具不在表里（或参数序列化不了）：请求本身不成立。
            Err(e) => error_reply(id, -32602, format!("Invalid params: {e}")),
        }
    }
}

/// 一条应答信封：`id` 原样回声——它是调用方唯一的配对凭据，数字与字符串不能互换。
fn result_reply(id: serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub(crate) fn error_reply(
    id: serde_json::Value,
    code: i64,
    message: impl std::fmt::Display,
) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message.to_string()},
    })
}

// ==========================================================================
// MCP Client
// ==========================================================================

/// MCP Client — 调用外部 MCP servers。
pub struct McpClient {
    transport: Box<dyn McpTransport>,
}

impl McpClient {
    pub fn new(transport: Box<dyn McpTransport>) -> Self {
        Self { transport }
    }

    pub async fn list_tools(&self) -> anyhow::Result<Vec<McpTool>> {
        self.transport.list_tools().await
    }

    pub async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
        self.transport.call_tool(call).await
    }
}

#[async_trait::async_trait]
impl cog_core::McpClient for McpClient {
    async fn list_tools(&self) -> cog_core::SFResult<Vec<serde_json::Value>> {
        let tools = self
            .list_tools()
            .await
            .map_err(|e| cog_core::SFError::Agent(format!("mcp list_tools failed: {}", e)))?;
        let mut out = Vec::new();
        for t in tools {
            out.push(serde_json::to_value(t).map_err(|e| {
                cog_core::SFError::Agent(format!("mcp tool serialization failed: {}", e))
            })?);
        }
        Ok(out)
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> cog_core::SFResult<serde_json::Value> {
        // 参数是工具自己的 input_schema 决定的对象；不是对象就是调用方给错了，
        // 在这里就断，别把它猜成 `{}`——那会让一次参数错误的调用看起来像一次
        // 不带参数的调用，而两者的失败面完全不同。
        let serde_json::Value::Object(arguments) = arguments else {
            return Err(cog_core::SFError::Validation(format!(
                "mcp tool '{name}' arguments must be a JSON object"
            )));
        };
        let call = McpToolCall {
            name: name.to_string(),
            arguments: arguments.into_iter().collect(),
        };
        let result = self.call_tool(call).await.map_err(|e| {
            cog_core::SFError::Agent(format!("mcp call_tool('{name}') failed: {e}"))
        })?;
        serde_json::to_value(result).map_err(|e| {
            cog_core::SFError::Agent(format!("mcp tool result serialization failed: {}", e))
        })
    }
}

// ==========================================================================
// MCP Transport
// ==========================================================================

#[async_trait::async_trait]
pub trait McpTransport: Send + Sync {
    async fn list_tools(&self) -> anyhow::Result<Vec<McpTool>>;
    async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult>;
}

// ==========================================================================
// MCP Types
// ==========================================================================

/// One entry of a `tools/list` reply.
///
/// The field names below are the protocol's, not this crate's: the wire spells
/// them camelCase, so the serde attribute is what makes this type readable by
/// and writable to anything else that speaks MCP. Without it the struct
/// round-trips with itself and with nothing else -- every real server's
/// `tools/list` fails to parse as a missing `input_schema`, and everything this
/// side emits is unreadable to a client that follows the schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolCall {
    pub name: String,
    pub arguments: HashMap<String, serde_json::Value>,
}

/// A `tools/call` reply. `isError` is the protocol's spelling; it is absent from
/// a successful call, hence the default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolResult {
    pub content: Vec<McpContent>,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum McpContent {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
    },
}

impl McpContent {
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text { text: s.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定答复的传输层：这些测试要证的是**经 trait 对象**这一路通不通（契约里有
    /// 没有那个方法、分发的对象拿不拿得到），不是某个具体传输层对不对——那由它自己
    /// 的测试证。
    struct StubTransport {
        fail: bool,
    }

    #[async_trait::async_trait]
    impl McpTransport for StubTransport {
        async fn list_tools(&self) -> anyhow::Result<Vec<McpTool>> {
            Ok(vec![McpTool {
                name: "echo".into(),
                description: "echoes its argument back".into(),
                input_schema: serde_json::json!({"type": "object"}),
            }])
        }

        async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
            if self.fail {
                return Err(anyhow::anyhow!("transport refused"));
            }
            let q = call
                .arguments
                .get("q")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Ok(McpToolResult {
                content: vec![McpContent::text(format!("echo:{q}"))],
                is_error: false,
            })
        }
    }

    fn as_trait_object(fail: bool) -> Arc<dyn cog_core::McpClient> {
        Arc::new(McpClient::new(Box::new(StubTransport { fail })))
    }

    #[tokio::test]
    async fn a_trait_object_can_call_a_tool_and_not_only_list_one() {
        let client = as_trait_object(false);
        assert_eq!(client.list_tools().await.unwrap().len(), 1);

        let result = client
            .call_tool("echo", serde_json::json!({"q": "hi"}))
            .await
            .unwrap();
        assert_eq!(result["isError"], serde_json::json!(false));
        assert_eq!(result["content"][0]["type"], serde_json::json!("text"));
        assert_eq!(result["content"][0]["text"], serde_json::json!("echo:hi"));
    }

    #[tokio::test]
    async fn arguments_that_are_not_an_object_are_refused_before_the_server_sees_them() {
        let err = as_trait_object(false)
            .call_tool("echo", serde_json::json!("hi"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be a JSON object"), "{err}");
    }

    #[tokio::test]
    async fn a_transport_failure_is_an_error_and_not_a_result() {
        assert!(as_trait_object(true)
            .call_tool("echo", serde_json::json!({}))
            .await
            .is_err());
    }

    /// The writing direction, which the transport tests cannot cover: they read
    /// spec-shaped replies, so they catch this side failing to *parse* the
    /// protocol, while nothing there notices this side failing to *emit* it. A
    /// tool list goes out to callers from here (the gateway hands one straight
    /// to whoever asked), and a client that follows the schema reads
    /// `inputSchema` there -- a crate that spells it otherwise is talking to
    /// nobody.
    #[test]
    fn what_this_side_writes_is_spelled_the_way_the_protocol_spells_it() {
        let tool = McpTool {
            name: "echo".into(),
            description: "echoes its argument back".into(),
            input_schema: serde_json::json!({"type": "object"}),
        };
        let wire = serde_json::to_value(&tool).unwrap();
        assert!(wire.get("inputSchema").is_some(), "{wire}");
        assert!(wire.get("input_schema").is_none(), "{wire}");

        let result = McpToolResult {
            content: vec![McpContent::Image {
                data: "AA==".into(),
                mime_type: "image/png".into(),
            }],
            is_error: true,
        };
        let wire = serde_json::to_value(&result).unwrap();
        assert_eq!(wire["isError"], serde_json::json!(true), "{wire}");
        assert_eq!(
            wire["content"][0]["mimeType"],
            serde_json::json!("image/png"),
            "{wire}"
        );
    }
}
