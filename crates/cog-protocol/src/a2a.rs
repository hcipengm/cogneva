//! A2A (Agent-to-Agent Protocol) 支持。
//! Google 推出的 Agent 间通信标准。
//! - Agent Card: 标准 Agent 描述（类型住在 `cog-core`，见下）
//! - Task: 任务生命周期管理
//! - Client: 向外部 Agent 发送任务

use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;

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
