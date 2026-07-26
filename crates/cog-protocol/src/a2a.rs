//! A2A (Agent-to-Agent Protocol) 支持。
//! Google 推出的 Agent 间通信标准。
//! - Agent Card: 标准 Agent 描述
//! - Task: 任务生命周期管理
//! - Client: 向外部 Agent 发送任务

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ==========================================================================
// Agent Card
// ==========================================================================

/// A2A Agent Card — 标准 Agent 描述。
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
    pub state_transition_history: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAuthentication {
    pub schemes: Vec<String>,
    pub credentials: Option<HashMap<String, String>>,
}

// ==========================================================================
// A2A Task
// ==========================================================================

/// A2A Task 生命周期。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct A2aTask {
    pub id: String,
    pub status: TaskStatus,
    pub messages: Vec<TaskMessage>,
    pub artifacts: Vec<TaskArtifact>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Canceled,
    Failed,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskArtifact {
    pub name: String,
    pub parts: Vec<MessagePart>,
    pub index: u32,
}

// ==========================================================================
// A2A Client
// ==========================================================================

use std::sync::Arc;

/// A2A Client — 向外部 Agent 发送任务。
pub struct A2aClient {
    client: Option<Arc<dyn cog_core::HttpClient>>,
}

impl Default for A2aClient {
    fn default() -> Self {
        Self::new()
    }
}

impl A2aClient {
    pub fn new() -> Self {
        Self { client: None }
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn client(&self) -> anyhow::Result<&Arc<dyn cog_core::HttpClient>> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("A2aClient has no HttpClient configured"))
    }

    /// 获取目标 Agent 的 Agent Card。
    pub async fn fetch_agent_card(&self, url: &str) -> anyhow::Result<AgentCard> {
        let req = cog_core::HttpRequest::get(url);
        let resp = self.client()?.execute(req).await?;
        let card: AgentCard = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        Ok(card)
    }

    /// 发送任务到目标 Agent。
    pub async fn send_task(&self, agent_url: &str, task: A2aTask) -> anyhow::Result<A2aTask> {
        let req = cog_core::HttpRequest::post(format!("{}/tasks/send", agent_url))
            .json(&task)
            .map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))?
            .timeout(30);
        let resp = self.client()?.execute(req).await?;
        let result: A2aTask = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        Ok(result)
    }

    /// 查询任务状态。
    pub async fn get_task(&self, agent_url: &str, task_id: &str) -> anyhow::Result<A2aTask> {
        let req =
            cog_core::HttpRequest::get(format!("{}/tasks/{}", agent_url, task_id)).timeout(30);
        let resp = self.client()?.execute(req).await?;
        let task: A2aTask = resp
            .json()
            .map_err(|e| anyhow::anyhow!("JSON parse failed: {}", e))?;
        Ok(task)
    }
}
