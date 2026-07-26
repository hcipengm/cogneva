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
