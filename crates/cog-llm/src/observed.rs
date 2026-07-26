use async_trait::async_trait;
use cog_core::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, LlmClient, Message,
    SFResult,
};
use std::sync::Arc;

pub struct ObservedLlmClient {
    inner: Arc<dyn LlmClient>,
}

impl ObservedLlmClient {
    pub fn new(inner: Arc<dyn LlmClient>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> Arc<dyn LlmClient> {
        self.inner.clone()
    }
}

#[async_trait]
impl LlmClient for ObservedLlmClient {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.inner.chat_stream(messages, options).await
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.inner.complete_stream(prompt, options).await
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let start = std::time::Instant::now();
        let response = self.inner.chat(messages, options).await?;
        let latency_ms = start.elapsed().as_millis() as u64;

        let obs = crate::observable::global_observable();
        if response.error_message.is_some() {
            obs.record_error();
        } else {
            obs.record_call(
                response.usage.input as u64,
                response.usage.output as u64,
                latency_ms,
            );
        }

        Ok(response)
    }

    async fn health_check(&self) -> bool {
        self.inner.health_check().await
    }
}
