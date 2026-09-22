use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, LlmClient as LLMProvider, Message, SFResult};
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Simple mock provider for cog-agents tests.
/// Supports a single fixed response or a queue of responses.
pub struct MockProvider {
    pub response: String,
    pub with_tool_call: Option<cog_core::ToolCall>,
    /// Optional queue of responses returned in FIFO order.
    /// When present, each call pops the next response.
    response_queue: Option<Mutex<VecDeque<String>>>,
    /// Emit the configured tool call on the first `n` `chat_stream` calls only.
    /// A mock that offers a tool call on every call cannot tell the loop's own
    /// turn apart from the forced ask that follows it, and what makes that ask
    /// final is precisely that no tool is on the table.
    tool_call_turns: Option<usize>,
    /// Tools offered on each `chat_stream` call, in call order: `None` when the
    /// request carried no tool definitions at all.
    pub offered_tools: Mutex<Vec<Option<usize>>>,
    stream_calls: AtomicUsize,
}

impl MockProvider {
    pub fn new(response: impl Into<String>) -> Self {
        Self {
            response: response.into(),
            with_tool_call: None,
            response_queue: None,
            tool_call_turns: None,
            offered_tools: Mutex::new(Vec::new()),
            stream_calls: AtomicUsize::new(0),
        }
    }

    #[allow(dead_code)]
    pub fn with_tool_call(mut self, tc: cog_core::ToolCall) -> Self {
        self.with_tool_call = Some(tc);
        self
    }

    /// Stop emitting the configured tool call after the first `n` calls.
    #[allow(dead_code)]
    pub fn with_tool_call_turns(mut self, n: usize) -> Self {
        self.tool_call_turns = Some(n);
        self
    }

    /// Create a provider that returns responses from a queue in order.
    #[allow(dead_code)]
    pub fn with_responses(responses: Vec<impl Into<String>>) -> Self {
        let queue: VecDeque<String> = responses.into_iter().map(|s| s.into()).collect();
        Self {
            response: queue.front().cloned().unwrap_or_default(),
            with_tool_call: None,
            response_queue: Some(Mutex::new(queue)),
            tool_call_turns: None,
            offered_tools: Mutex::new(Vec::new()),
            stream_calls: AtomicUsize::new(0),
        }
    }

    fn pop_response(&self) -> String {
        if let Some(ref queue) = self.response_queue {
            let mut guard = queue.lock().unwrap();
            if let Some(r) = guard.pop_front() {
                return r;
            }
        }
        self.response.clone()
    }
}

#[async_trait]
impl LLMProvider for MockProvider {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        let response_text = self.pop_response();
        let mut content = vec![ContentBlock::text(response_text.clone())];
        if let Some(tc) = self.with_tool_call.clone() {
            content.push(ContentBlock::tool_call(
                &tc.id,
                &tc.name,
                tc.arguments.clone(),
            ));
        }
        Ok(ChatResponse {
            content,
            api: "mock".into(),
            provider: "mock".into(),
            model: "mock".into(),
            response_id: None,
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_llm::DEFAULT_STREAM_CAPACITY);
        let call_index = self.stream_calls.fetch_add(1, Ordering::Relaxed);
        self.offered_tools
            .lock()
            .unwrap()
            .push(options.tools.as_ref().map(|t| t.len()));
        let response_text = self.pop_response();
        let tool_call = match self.tool_call_turns {
            Some(n) if call_index >= n => None,
            _ => self.with_tool_call.clone(),
        };

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
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            };

            let _ = producer
                .push(AssistantMessageEvent::Start {
                    timestamp: chrono::Utc::now(),
                })
                .await;

            let idx = 0;
            response.content.push(ContentBlock::text(""));
            let _ = producer
                .push(AssistantMessageEvent::TextStart {
                    content_index: idx,
                    timestamp: chrono::Utc::now(),
                })
                .await;

            response.content[0] = ContentBlock::text(response_text.clone());
            let _ = producer
                .push(AssistantMessageEvent::TextDelta {
                    content_index: idx,
                    delta: response_text.clone(),
                    timestamp: chrono::Utc::now(),
                })
                .await;

            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: idx,
                    content: response_text,
                    timestamp: chrono::Utc::now(),
                })
                .await;

            // Emit tool call if configured
            if let Some(tc) = tool_call {
                response.content.push(ContentBlock::tool_call(
                    &tc.id,
                    &tc.name,
                    tc.arguments.clone(),
                ));
                let _ = producer
                    .push(AssistantMessageEvent::ToolCallEnd {
                        content_index: 1,
                        tool_call: tc,
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            }

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
