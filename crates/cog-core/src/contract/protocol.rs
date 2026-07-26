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
}
