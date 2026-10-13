//!Protocol DTOs and traits — A2A / MCP abstractions so the gateway never
//!depends on `cog-protocol` concrete types.

use serde::{Deserialize, Serialize};

// ─── A2A Agent Card DTOs ──────────────────────────────────────────────────

/// A2A Agent Card — describes an agent's capabilities and endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    pub url: String,
    pub version: String,
    pub capabilities: AgentCapabilities,
    pub skills: Vec<AgentSkill>,
    pub authentication: AgentAuthentication,
}

/// Capabilities advertised by an A2A agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
    pub state_transition_history: bool,
}

/// A skill exposed by an A2A agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub examples: Vec<String>,
}

/// Authentication schemes supported by an A2A agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAuthentication {
    pub schemes: Vec<String>,
    pub credentials: Option<serde_json::Value>,
}

// ─── A2A Task DTOs ────────────────────────────────────────────────────────

// 这一族与上面的名片同源、同理由：A2A 的 HTTP 面由 gateway 服务，而 gateway 不许
// 依赖 `cog-protocol` 的具体类型。名片那一族先搬到了这里，任务这一族留在了原地，
// 于是「客户端拿到的任务」与「服务端要吐出的任务」在有人真把它们接起来的那天会是
// 两个不同的类型——正是名片那一族刚修过的同一个坑。
//
// `A2aTaskStatus` 带前缀，是因为本 crate 根上已经有一个给自家任务存储用的
// `TaskStatus`，而两者不是同一套状态：那个是调度器的，这个是 A2A 线上的。同一个名字
// 挂两套状态，就是让读者把一套读成另一套。

/// A2A Task — 从一条 message 到终态的生命周期。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct A2aTask {
    pub id: String,
    pub status: A2aTaskStatus,
    pub messages: Vec<TaskMessage>,
    pub artifacts: Vec<TaskArtifact>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// A2A 线上的一条任务状态。闭集与 A2A 规范逐字一致，不是本平台调度器那一套。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum A2aTaskStatus {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Canceled,
    Failed,
}

/// 一条 message：角色 + 若干 part。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMessage {
    pub role: String,
    pub parts: Vec<MessagePart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MessagePart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "file")]
    File {
        name: String,
        mime_type: String,
        bytes: String,
    },
}

/// 一条产物：若干 part 加它在产出序列里的位置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskArtifact {
    pub name: String,
    pub parts: Vec<MessagePart>,
    pub index: u32,
}

// ─── MCP Client trait ─────────────────────────────────────────────────────

/// Trait abstracting an MCP (Model Context Protocol) client.
#[async_trait::async_trait]
pub trait McpClient: Send + Sync {
    /// List tools exposed by the MCP server.
    async fn list_tools(&self) -> crate::SFResult<Vec<serde_json::Value>>;

    /// Call a tool exposed by the MCP server and return its result.
    ///
    /// Both ends of this call are untyped JSON, for the same reason
    /// [`Self::list_tools`] is: the concrete request/result types belong to the
    /// protocol crate, and a caller holding this trait object must be able to
    /// make the call without depending on that crate. A trait that stops at
    /// listing tools makes the call unreachable for exactly those callers —
    /// they can see which tools exist and cannot run one.
    ///
    /// `arguments` is the tool's argument object; an implementation is expected
    /// to reject a non-object rather than guess.
    async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> crate::SFResult<serde_json::Value>;
}
