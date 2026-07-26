use async_trait::async_trait;
use cog_core::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, LlmClient, Message,
    SFResult,
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct HotSwappableLlmClient {
    current: RwLock<Arc<dyn LlmClient>>,
}

impl HotSwappableLlmClient {
    pub fn new(initial: Arc<dyn LlmClient>) -> Self {
        Self {
            current: RwLock::new(initial),
        }
    }

    pub async fn swap(&self, next: Arc<dyn LlmClient>) {
        *self.current.write().await = next;
    }

    pub async fn current(&self) -> Arc<dyn LlmClient> {
        self.current.read().await.clone()
    }
}

#[async_trait]
impl LlmClient for HotSwappableLlmClient {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let current = self.current().await;
        current.chat_stream(messages, options).await
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let current = self.current().await;
        current.complete_stream(prompt, options).await
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let current = self.current().await;
        current.chat(messages, options).await
    }

    async fn health_check(&self) -> bool {
        let current = self.current().await;
        current.health_check().await
    }
}
