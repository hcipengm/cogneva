//! 示例共用的 MockAgent：不依赖外部 LLM，返回固定 JSON，
//! 让示例在任何环境都能 `cargo run --example <name>` 直接运行。

use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{Agent, AgentState, InboxMessage};

pub struct MockAgent {
    pub response: serde_json::Value,
}

impl MockAgent {
    pub fn json(response: serde_json::Value) -> Arc<dyn Agent> {
        Arc::new(Self { response })
    }
}

#[async_trait]
impl Agent for MockAgent {
    async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }
    async fn start(&self) {}
    async fn snapshot(&self, _task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        Ok(cog_core::AgentCheckpoint {
            checkpoint_id: String::new(),
            task_id: String::new(),
            agent_state: serde_json::Value::Null,
            context_window: Vec::new(),
            event_offset: 0,
            timestamp: chrono::Utc::now(),
        })
    }
    async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn continue_(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }
    async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn abort(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn reset(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn state(&self) -> cog_core::SFResult<AgentState> {
        Ok(AgentState::Idle)
    }
    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
        producer.end(cog_core::ChatResponse::default());
        Ok(stream)
    }
    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(&[], &cog_core::ChatOptions::default())
            .await
    }
    async fn read_board(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        Ok(None)
    }
    async fn write_board(
        &self,
        _task_id: &str,
        _field: &str,
        _value: &str,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
    async fn receive_message(&self, _msg: InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }
}
