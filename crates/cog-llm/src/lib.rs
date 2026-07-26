pub mod hot_swap;
pub mod model;
pub mod observable;
pub mod observed;
pub mod provider;
pub mod registry;
pub mod resilience;
pub mod routing;
pub mod stream;
pub mod utils;

pub use cog_core::event_stream::{
    EventStream, EventStreamProducer, ResultFuture, DEFAULT_STREAM_CAPACITY,
};
pub use cog_core::resilience::{
    BackpressureConfig, BackpressureError, DEFAULT_HIGH_WATERMARK, DEFAULT_LOW_WATERMARK,
};

pub use cog_core::{
    AssistantMessageEventProducer, AssistantMessageEventStream, CacheRetention, ChatOptions,
    ChatResponse, CompleteOptions, Cost, LlmClient, LlmModelInfo, ResponseFormat, SFError,
    SFResult, StopReason, ThinkingLevel, ToolCall, Transport, Usage,
};
pub use hot_swap::HotSwappableLlmClient;
pub use model::{ApiType, Model, ModelCost, Provider};
pub use observable::LlmObservable;
pub use observed::ObservedLlmClient;
pub use provider::{anthropic, google, ollama, openai};
pub use registry::ProviderRegistry;
pub use resilience::{BackoffStrategy, ResilientProvider, RetryPolicy};
pub use routing::{is_rate_limit_or_quota_error, RoutingProvider};
pub use stream::{parse_sse_stream, LLMStream};

pub mod plugin;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::LlmClient as LLMProvider;
    use cog_core::{execute_structured, AssistantMessageEvent, ContentBlock, Message};

    /// A mock provider that always returns a hard-coded JSON string.
    struct MockProvider {
        response_text: String,
    }

    #[async_trait]
    impl LLMProvider for MockProvider {
        async fn chat(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            Ok(ChatResponse {
                content: vec![ContentBlock::text(self.response_text.clone())],
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
            let content = vec![ContentBlock::Text {
                text: self.response_text.clone(),
                text_signature: None,
            }];
            let response = ChatResponse {
                content: content.clone(),
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };
            let (stream, mut producer) = AssistantMessageEventStream::with_capacity(10);
            let _ = producer
                .push(AssistantMessageEvent::Start {
                    partial: Message::assistant(content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: 0,
                    content: self.response_text.clone(),
                    partial: Message::assistant(content),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(response);
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            prompt: &str,
            _options: &CompleteOptions,
        ) -> SFResult<AssistantMessageEventStream> {
            let content = vec![ContentBlock::Text {
                text: self.response_text.clone(),
                text_signature: None,
            }];
            let response = ChatResponse {
                content: content.clone(),
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };
            let (stream, mut producer) = AssistantMessageEventStream::with_capacity(10);

            let _ = producer
                .push(AssistantMessageEvent::Start {
                    partial: Message::assistant(content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            // Stream the prompt text in chunks to simulate streaming.
            let chunk_size = 4;
            for (i, chunk) in prompt
                .chars()
                .collect::<Vec<_>>()
                .chunks(chunk_size)
                .enumerate()
            {
                let text: String = chunk.iter().collect();
                if i == 0 {
                    let _ = producer
                        .push(AssistantMessageEvent::TextStart {
                            content_index: 0,
                            partial: Message::assistant(content.clone()),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }
                let _ = producer
                    .push(AssistantMessageEvent::TextDelta {
                        content_index: 0,
                        delta: text,
                        partial: Message::assistant(content.clone()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            }

            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: 0,
                    content: self.response_text.clone(),
                    partial: Message::assistant(content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason: StopReason::Stop,
                    message: Message::assistant(content),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            producer.end(response);
            Ok(stream)
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
    struct Person {
        name: String,
        age: u32,
    }

    #[tokio::test]
    async fn test_execute_structured_validation_error() {
        // Missing required field `age`, and `name` is the wrong type (number instead of string)
        let bad_json = r#"{"name": 42}"#;
        let provider = MockProvider {
            response_text: bad_json.to_string(),
        };

        let result: SFResult<Person> = execute_structured(
            &provider,
            &[Message::user("give me a person")],
            &ChatOptions::default(),
        )
        .await;

        assert!(
            result.is_err(),
            "expected validation error for invalid JSON"
        );
        let err = result.unwrap_err();
        match err {
            SFError::Validation(msg) => {
                assert!(
                    msg.contains("validation failed"),
                    "error message should mention validation: {msg}"
                );
            }
            other => panic!("expected SFError::Validation, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_execute_structured_success() {
        let good_json = r#"{"name": "Alice", "age": 30}"#;
        let provider = MockProvider {
            response_text: good_json.to_string(),
        };

        let result: SFResult<Person> = execute_structured(
            &provider,
            &[Message::user("give me a person")],
            &ChatOptions::default(),
        )
        .await;

        assert!(result.is_ok(), "expected success for valid JSON");
        let person = result.unwrap();
        assert_eq!(person.name, "Alice");
        assert_eq!(person.age, 30);
    }
}
