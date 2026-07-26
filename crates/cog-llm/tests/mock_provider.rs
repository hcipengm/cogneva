use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, LlmClient as LLMProvider, Message, SFResult};
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};

/// Mock provider for testing.
pub struct MockProvider {
    pub response: String,
}

#[async_trait]
impl LLMProvider for MockProvider {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        Ok(ChatResponse {
            content: vec![ContentBlock::text(self.response.clone())],
            api: "mock".into(),
            provider: "mock".into(),
            model: "mock".into(),
            response_id: None,
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_llm::DEFAULT_STREAM_CAPACITY);
        let response_text = self.response.clone();

        tokio::spawn(async move {
            let mut response = ChatResponse {
                content: Vec::new(),
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };

            let _ = producer
                .push(AssistantMessageEvent::Start {
                    partial: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            let idx = 0;
            response.content.push(ContentBlock::text(""));
            let _ = producer
                .push(AssistantMessageEvent::TextStart {
                    content_index: idx,
                    partial: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            response.content[0] = ContentBlock::text(response_text.clone());
            let _ = producer
                .push(AssistantMessageEvent::TextDelta {
                    content_index: idx,
                    delta: response_text.clone(),
                    partial: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: idx,
                    content: response_text,
                    partial: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason: StopReason::Stop,
                    message: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            producer.end(response);
        });

        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.chat_stream(&[Message::user(_prompt)], &ChatOptions::default())
            .await
    }

    async fn health_check(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn test_mock_provider_chat() {
    let provider = MockProvider {
        response: "Hello, world!".into(),
    };
    let response = provider
        .chat(&[Message::user("hi")], &ChatOptions::default())
        .await
        .unwrap();
    assert_eq!(response.content.len(), 1);
    assert_eq!(response.content[0].as_text().unwrap(), "Hello, world!");
}

#[tokio::test]
async fn test_mock_provider_health_check() {
    let provider = MockProvider {
        response: "ok".into(),
    };
    assert!(provider.health_check().await);
}
