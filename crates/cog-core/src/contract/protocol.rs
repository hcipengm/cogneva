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
