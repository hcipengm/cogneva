use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, LlmClient as LLMProvider, Message, SFResult};
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};
use std::collections::VecDeque;
use std::sync::Mutex;

/// A mock LLM provider for testing that supports queued responses and failures.
/// Responses are returned in FIFO order.  Both `chat()` and `chat_stream()`
/// consume the queue.  All calls are recorded for later assertion.
pub struct MockLLMProvider {
    queue: Mutex<VecDeque<MockResponse>>,
    calls: Mutex<Vec<MockCall>>,
}

enum MockResponse {
    Ok(String),
    Err(String),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MockCall {
    pub method: String,
    pub messages: Vec<Message>,
    pub prompt: Option<String>,
    pub options: serde_json::Value,
}

#[allow(dead_code)]
impl MockLLMProvider {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a successful text response.
    pub fn queue_response(&self, response: impl Into<String>) {
        self.queue
            .lock()
            .unwrap()
            .push_back(MockResponse::Ok(response.into()));
    }

    /// Enqueue a failure.
    pub fn queue_failure(&self, error: impl Into<String>) {
        self.queue
            .lock()
            .unwrap()
            .push_back(MockResponse::Err(error.into()));
    }

    /// Return all recorded calls for assertion.
    pub fn recorded_calls(&self) -> Vec<MockCall> {
        self.calls.lock().unwrap().clone()
    }

    fn pop_response(&self) -> MockResponse {
        self.queue
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| MockResponse::Ok(String::new()))
    }

    fn record_call(
        &self,
        method: &str,
        messages: Vec<Message>,
        prompt: Option<String>,
        options: &serde_json::Value,
    ) {
        self.calls.lock().unwrap().push(MockCall {
            method: method.into(),
            messages,
            prompt,
            options: options.clone(),
        });
    }

    async fn build_stream(&self, response_text: String) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_llm::DEFAULT_STREAM_CAPACITY);

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
}

impl Default for MockLLMProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LLMProvider for MockLLMProvider {
    async fn chat(&self, messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        self.record_call("chat", messages.to_vec(), None, &serde_json::Value::Null);
        match self.pop_response() {
            MockResponse::Ok(text) => Ok(ChatResponse {
                content: vec![ContentBlock::text(text)],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            }),
            MockResponse::Err(msg) => Err(cog_core::SFError::LLM(msg)),
        }
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.record_call(
            "chat_stream",
            messages.to_vec(),
            None,
            &serde_json::Value::Null,
        );
        match self.pop_response() {
            MockResponse::Ok(text) => self.build_stream(text).await,
            MockResponse::Err(msg) => Err(cog_core::SFError::LLM(msg)),
        }
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.record_call(
            "complete_stream",
            vec![],
            Some(prompt.into()),
            &serde_json::Value::Null,
        );
        match self.pop_response() {
            MockResponse::Ok(text) => self.build_stream(text).await,
            MockResponse::Err(msg) => Err(cog_core::SFError::LLM(msg)),
        }
    }

    async fn health_check(&self) -> bool {
        true
    }
}
