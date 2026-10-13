//! MCP (Model Context Protocol) 支持。
//! - Server: 将 cogneva 的 ToolRegistry 以 MCP 协议暴露
//! - Client: 调用外部 MCP servers 的 tools
//! - Transport: SSE / stdio

use cog_core::{ToolCall, ToolDefinition, ToolExecutor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ==========================================================================
// MCP Server
// ==========================================================================

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

/// SSE 传输层。
pub struct SseTransport {
    endpoint: String,
    client: Option<Arc<dyn cog_core::HttpClient>>,
}

impl SseTransport {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: None,
        }
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn client(&self) -> anyhow::Result<&Arc<dyn cog_core::HttpClient>> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("SseTransport has no HttpClient configured"))
    }
}

#[async_trait::async_trait]
impl McpTransport for SseTransport {
    async fn list_tools(&self) -> anyhow::Result<Vec<McpTool>> {
        let req = cog_core::HttpRequest::get(format!("{}/tools/list", self.endpoint)).timeout(30);
        let resp = self.client()?.execute(req).await?;
        let tools: Vec<McpTool> = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        Ok(tools)
    }

    async fn call_tool(&self, call: McpToolCall) -> anyhow::Result<McpToolResult> {
        let req = cog_core::HttpRequest::post(format!("{}/tools/call", self.endpoint))
            .json(&call)
            .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
            .timeout(30);
        let resp = self.client()?.execute(req).await?;
        let result: McpToolResult = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        Ok(result)
    }
}

// ==========================================================================
// MCP Types
// ==========================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    Image { data: String, mime_type: String },
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
        assert_eq!(result["is_error"], serde_json::json!(false));
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
}
