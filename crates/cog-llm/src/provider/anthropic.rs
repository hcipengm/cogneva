use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, StopReason};
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::Arc;

use super::LLMProvider;
use crate::{
    model::Model, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, Usage,
};

pub struct AnthropicProvider {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    model: Model,
    api_key: String,
    stream_capacity: usize,
    default_max_tokens: u32,
}

impl AnthropicProvider {
    pub fn new(model: Model, api_key: impl Into<String>) -> Self {
        Self {
            client: None,
            model,
            api_key: api_key.into(),
            stream_capacity: crate::DEFAULT_STREAM_CAPACITY,
            default_max_tokens: 4096,
        }
    }

    pub fn with_stream_capacity(mut self, capacity: usize) -> Self {
        self.stream_capacity = capacity;
        self
    }

    pub fn with_default_max_tokens(mut self, tokens: u32) -> Self {
        self.default_max_tokens = tokens;
        self
    }

    pub fn with_client(mut self, client: Arc<dyn cog_core::HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    fn build_request_body(&self, messages: &[Message], options: &ChatOptions) -> serde_json::Value {
        let system = messages.iter().find_map(|m| match m {
            Message::System { content, .. } => Some(content.clone()),
            _ => None,
        });

        let msgs: Vec<serde_json::Value> = messages
            .iter()
            .filter(|m| !matches!(m, Message::System { .. }))
            .map(|msg| match msg {
                Message::User { content, .. } => json!({"role": "user", "content": content}),
                Message::Assistant { content, .. } => {
                    let mut anthropic_content: Vec<serde_json::Value> = Vec::new();

                    // Text blocks
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    if !text.is_empty() {
                        anthropic_content.push(json!({"type": "text", "text": text}));
                    }

                    // Tool use blocks for tool calls in conversation history
                    for block in content {
                        if let ContentBlock::ToolCall {
                            id,
                            name,
                            arguments,
                            ..
                        } = block
                        {
                            anthropic_content.push(json!({
                                "type": "tool_use",
                                "id": id,
                                "name": name,
                                "input": arguments,
                            }));
                        }
                    }

                    if anthropic_content.is_empty() {
                        anthropic_content.push(json!({"type": "text", "text": ""}));
                    }

                    json!({"role": "assistant", "content": anthropic_content})
                }
                Message::ToolResult {
                    tool_call_id,
                    content,
                    is_error,
                    ..
                } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    let mut tool_result = json!({
                        "type": "tool_result",
                        "tool_use_id": tool_call_id,
                        "content": text,
                    });
                    if *is_error {
                        tool_result["is_error"] = json!(true);
                    }
                    json!({
                        "role": "user",
                        "content": [tool_result]
                    })
                }
                _ => json!({"role": "user", "content": ""}),
            })
            .collect();

        let mut body = json!({
            "model": options.model.as_ref().unwrap_or(&self.model.id),
            "messages": msgs,
            "max_tokens": options.max_tokens.unwrap_or(self.default_max_tokens),
            "stream": true,
        });

        if let Some(sys) = system {
            body["system"] = json!(sys);
        }
        if let Some(temp) = options.temperature {
            body["temperature"] = json!(temp);
        }
        if let Some(tools) = &options.tools {
            let tool_defs: Vec<_> = tools.iter().map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": {
                        "type": "object",
                        "properties": t.parameters.get("properties").unwrap_or(&serde_json::Value::Null),
                        "required": t.parameters.get("required").unwrap_or(&serde_json::Value::Array(vec![])),
                    }
                })
            }).collect();
            body["tools"] = json!(tool_defs);
        }

        // Tool choice (auto/none/any) from metadata
        if let Some(tool_choice) = options.metadata.get("tool_choice") {
            match tool_choice.as_str() {
                "none" => {
                    body["tool_choice"] = json!({"type": "none"});
                }
                "any" => {
                    body["tool_choice"] = json!({"type": "any"});
                }
                _ => {
                    body["tool_choice"] = json!({"type": "auto"});
                }
            }
        }

        // Thinking / reasoning budget
        if let Some(level) = options.reasoning {
            let budget = match level {
                crate::ThinkingLevel::Minimal => 1024,
                crate::ThinkingLevel::Low => 2048,
                crate::ThinkingLevel::Medium => 8192,
                crate::ThinkingLevel::High | crate::ThinkingLevel::Xhigh => 16384,
            };
            body["thinking"] = json!({
                "type": "enabled",
                "budget_tokens": budget,
            });
        }

        body
    }
}

#[async_trait]
impl LLMProvider for AnthropicProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(self.stream_capacity);
        let model = self.model.clone();
        let api_key = self.api_key.clone();
        let client = self
            .client
            .clone()
            .ok_or_else(|| SFError::Agent("AnthropicProvider: no HttpClient configured".into()))?;
        let headers = options.headers.clone();
        let body = self.build_request_body(messages, options);
        let abort_signal = options.abort_signal.clone();

        let url = format!("{}/messages", model.base_url);
        let raw_logger = options.raw_logger.clone();

        tokio::spawn(async move {
            // Log outbound request
            if let Some(ref logger) = raw_logger {
                let record = cog_core::RawRecord {
                    meta: cog_core::RawMeta {
                        version: "1.0".into(),
                        stream: "llm_raw".into(),
                        recorded_at: chrono::Utc::now(),
                        recorded_by: "sf-llm".into(),
                        sequence: 0,
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: None,
                    },
                    context: cog_core::RawContext::default(),
                    payload: cog_core::RawPayload {
                        direction: "outbound".into(),
                        transport: "anthropic".into(),
                        format: Some("json".into()),
                        raw: body.clone(),
                    },
                };
                if let Err(e) = logger.write(record).await {
                    tracing::warn!("RawLogger write failed (llm_raw outbound): {}", e);
                }
            }

            let mut response = ChatResponse {
                content: Vec::new(),
                api: "anthropic-messages".into(),
                provider: model.provider.to_string(),
                model: model.id.clone(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };

            if producer
                .push(AssistantMessageEvent::Start {
                    partial: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await
                .is_err()
            {
                return;
            };

            let mut req = cog_core::HttpRequest::post(&url)
                .header("x-api-key", &api_key)
                .header("anthropic-version", "2023-06-01")
                .header("Content-Type", "application/json")
                .json(&body)
                .map_err(|e| SFError::Agent(format!("JSON serialization failed: {}", e)))
                .unwrap()
                .timeout(120);

            for (key, value) in &headers {
                req = req.header(key, value);
            }

            let http_response = match client.execute_stream(req).await {
                Ok(r) => r,
                Err(e) => {
                    response.stop_reason = StopReason::Error;
                    response.error_message = Some(format!("HTTP error: {}", e));
                    if producer
                        .push(AssistantMessageEvent::Error {
                            reason: StopReason::Error,
                            error: Message::assistant_text(
                                response.error_message.clone().unwrap_or_default(),
                            ),
                            timestamp: chrono::Utc::now(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    };
                    producer.end(response);
                    return;
                }
            };

            if !http_response.is_success() {
                let text = http_response.drain_text().await;
                response.stop_reason = StopReason::Error;
                response.error_message = Some(format!("API error: {}", text));
                if producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Error,
                        error: Message::assistant_text(
                            response.error_message.clone().unwrap_or_default(),
                        ),
                        timestamp: chrono::Utc::now(),
                    })
                    .await
                    .is_err()
                {
                    return;
                };
                producer.end(response);
                return;
            }

            let bytes_stream = http_response.stream;
            let lines = bytes_stream
                .map_err(std::io::Error::other)
                .into_async_read();
            let lines = futures::io::BufReader::new(lines).lines();
            futures::pin_mut!(lines);

            let mut current_block: Option<ContentBlock> = None;
            let mut block_index: usize = 0;
            let mut current_tool_args_buffer: Option<String> = None;

            while let Ok(Some(line)) = lines.try_next().await {
                let line = line.trim();
                // SSE 规范里 data 后的空格可选；kimi 的 anthropic 端点发 `data:{`
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim_start();

                let json: serde_json::Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let event_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("");

                match event_type {
                    "message_start" => {
                        if let Some(msg) = json.get("message") {
                            if let Some(id) = msg.get("id").and_then(|v| v.as_str()) {
                                response.response_id = Some(id.to_string());
                            }
                            if let Some(usage) = msg.get("usage") {
                                response.usage.input = usage
                                    .get("input_tokens")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0)
                                    as u32;
                                response.usage.output = usage
                                    .get("output_tokens")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0)
                                    as u32;
                            }
                        }
                    }
                    "content_block_start" => {
                        block_index =
                            json.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                        if let Some(block) = json.get("content_block") {
                            let block_type =
                                block.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            match block_type {
                                "text" => {
                                    current_block = Some(ContentBlock::text(""));
                                    response.content.push(current_block.clone().unwrap());
                                    if producer
                                        .push(AssistantMessageEvent::TextStart {
                                            content_index: block_index,
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    };
                                }
                                "thinking" => {
                                    current_block = Some(ContentBlock::thinking(""));
                                    response.content.push(current_block.clone().unwrap());
                                    if producer
                                        .push(AssistantMessageEvent::ThinkingStart {
                                            content_index: block_index,
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    };
                                }
                                "redacted_thinking" => {
                                    let sig = block
                                        .get("data")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    current_block = Some(ContentBlock::Thinking {
                                        thinking: "[Reasoning redacted]".into(),
                                        thinking_signature: Some(sig),
                                        redacted: true,
                                    });
                                    response.content.push(current_block.clone().unwrap());
                                    if producer
                                        .push(AssistantMessageEvent::ThinkingStart {
                                            content_index: block_index,
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    };
                                }
                                "tool_use" => {
                                    let id = block
                                        .get("id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    let name = block
                                        .get("name")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    current_block = Some(ContentBlock::tool_call(
                                        id,
                                        name,
                                        serde_json::Value::Object(Default::default()),
                                    ));
                                    current_tool_args_buffer = Some(String::new());
                                    response.content.push(current_block.clone().unwrap());
                                    if producer
                                        .push(AssistantMessageEvent::ToolCallStart {
                                            content_index: block_index,
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    };
                                }
                                _ => {}
                            }
                        }
                    }
                    "content_block_delta" => {
                        block_index =
                            json.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                        if let Some(delta) = json.get("delta") {
                            if let Some(text) = delta.get("text").and_then(|v| v.as_str()) {
                                if let Some(ref mut block) = current_block {
                                    if block.is_text() {
                                        block.append_text(text);
                                        if let Some(last) = response.content.last_mut() {
                                            *last = block.clone();
                                        }
                                        if producer
                                            .push(AssistantMessageEvent::TextDelta {
                                                content_index: block_index,
                                                delta: text.to_string(),
                                                partial: Message::assistant(
                                                    response.content.clone(),
                                                ),
                                                timestamp: chrono::Utc::now(),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        };
                                    }
                                }
                            }
                            if let Some(thinking) = delta.get("thinking").and_then(|v| v.as_str()) {
                                if let Some(ref mut block) = current_block {
                                    if block.is_thinking() {
                                        block.append_thinking(thinking);
                                        if let Some(last) = response.content.last_mut() {
                                            *last = block.clone();
                                        }
                                        if producer
                                            .push(AssistantMessageEvent::ThinkingDelta {
                                                content_index: block_index,
                                                delta: thinking.to_string(),
                                                partial: Message::assistant(
                                                    response.content.clone(),
                                                ),
                                                timestamp: chrono::Utc::now(),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        };
                                    }
                                }
                            }
                            if let Some(partial_json) =
                                delta.get("partial_json").and_then(|v| v.as_str())
                            {
                                if let Some(ref mut block) = current_block {
                                    if let ContentBlock::ToolCall { .. } = block {
                                        // Accumulate into buffer; do NOT parse mid-stream.
                                        // arguments remains an empty object placeholder until finalized.
                                        if let Some(ref mut buffer) = current_tool_args_buffer {
                                            buffer.push_str(partial_json);
                                        }
                                        if let Some(last) = response.content.last_mut() {
                                            *last = block.clone();
                                        }
                                        if producer
                                            .push(AssistantMessageEvent::ToolCallDelta {
                                                content_index: block_index,
                                                delta: partial_json.to_string(),
                                                partial: Message::assistant(
                                                    response.content.clone(),
                                                ),
                                                timestamp: chrono::Utc::now(),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        };
                                    }
                                }
                            }
                        }
                    }
                    "content_block_stop" => {
                        block_index =
                            json.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                        if let Some(ref mut block) = current_block {
                            // Finalize tool call arguments before emitting end event
                            if let ContentBlock::ToolCall { .. } = block {
                                if let Some(ref buf) = current_tool_args_buffer {
                                    if !buf.is_empty() {
                                        if let Ok(parsed) = serde_json::from_str(buf) {
                                            if let ContentBlock::ToolCall {
                                                ref mut arguments,
                                                ..
                                            } = block
                                            {
                                                *arguments = parsed;
                                            }
                                        } else {
                                            if let ContentBlock::ToolCall {
                                                ref mut arguments,
                                                ..
                                            } = block
                                            {
                                                *arguments = serde_json::Value::String(buf.clone());
                                            }
                                        }
                                        if let Some(last) = response.content.last_mut() {
                                            *last = block.clone();
                                        }
                                    }
                                }
                            }
                            finish_block(block, block_index, &producer, &response.content).await;
                        }
                        current_block = None;
                        current_tool_args_buffer = None;
                    }
                    "message_delta" => {
                        if let Some(delta) = json.get("delta") {
                            if let Some(stop) = delta.get("stop_reason").and_then(|v| v.as_str()) {
                                response.stop_reason = map_stop_reason(stop);
                            }
                        }
                        if let Some(usage) = json.get("usage") {
                            response.usage.input = usage
                                .get("input_tokens")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                                as u32;
                            response.usage.output = usage
                                .get("output_tokens")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                                as u32;
                        }
                    }
                    "message_stop" => {
                        break;
                    }
                    "error" => {
                        let err_msg = json
                            .get("error")
                            .and_then(|e| e.get("message"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown error");
                        response.stop_reason = StopReason::Error;
                        response.error_message = Some(err_msg.to_string());
                        break;
                    }
                    _ => {}
                }

                // Check abort signal
                if let Some(ref signal) = abort_signal {
                    if signal.load(std::sync::atomic::Ordering::Relaxed) {
                        response.stop_reason = StopReason::Aborted;
                        break;
                    }
                }
            }

            // Finish any remaining block
            if let Some(ref mut block) = current_block {
                if let ContentBlock::ToolCall { .. } = block {
                    if let Some(ref buf) = current_tool_args_buffer {
                        if !buf.is_empty() {
                            if let Ok(parsed) = serde_json::from_str(buf) {
                                if let ContentBlock::ToolCall {
                                    ref mut arguments, ..
                                } = block
                                {
                                    *arguments = parsed;
                                }
                            } else {
                                if let ContentBlock::ToolCall {
                                    ref mut arguments, ..
                                } = block
                                {
                                    *arguments = serde_json::Value::String(buf.clone());
                                }
                            }
                            if let Some(last) = response.content.last_mut() {
                                *last = block.clone();
                            }
                        }
                    }
                }
                finish_block(block, block_index, &producer, &response.content).await;
            }

            // Calculate cost from usage and model cost metadata
            if response.usage.total_tokens > 0
                || response.usage.input > 0
                || response.usage.output > 0
            {
                response.usage.cost = crate::model::calculate_cost(&model, &response.usage);
            }

            if response.stop_reason == StopReason::Aborted {
                if producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Aborted,
                        error: Message::assistant_text("Request was aborted"),
                        timestamp: chrono::Utc::now(),
                    })
                    .await
                    .is_err()
                {
                    return;
                };
            } else if response.stop_reason == StopReason::Error {
                if producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Error,
                        error: Message::assistant_text(
                            response.error_message.clone().unwrap_or_default(),
                        ),
                        timestamp: chrono::Utc::now(),
                    })
                    .await
                    .is_err()
                {
                    return;
                };
            } else {
                if producer
                    .push(AssistantMessageEvent::Done {
                        reason: response.stop_reason,
                        message: Message::assistant(response.content.clone()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await
                    .is_err()
                {
                    return;
                };
            }

            // Log inbound response
            if let Some(ref logger) = raw_logger {
                let record = cog_core::RawRecord {
                    meta: cog_core::RawMeta {
                        version: "1.0".into(),
                        stream: "llm_raw".into(),
                        recorded_at: chrono::Utc::now(),
                        recorded_by: "sf-llm".into(),
                        sequence: 0,
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: None,
                    },
                    context: cog_core::RawContext::default(),
                    payload: cog_core::RawPayload {
                        direction: "inbound".into(),
                        transport: "anthropic".into(),
                        format: Some("json".into()),
                        raw: match serde_json::to_value(&response) {
                            Ok(v) => v,
                            Err(_) => {
                                serde_json::json!({"stop_reason": format!("{:?}", response.stop_reason)})
                            }
                        },
                    },
                };
                if let Err(e) = logger.write(record).await {
                    tracing::warn!("RawLogger write failed (llm_raw inbound): {}", e);
                }
            }

            producer.end(response);
        });

        Ok(stream)
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let chat_options = ChatOptions {
            model: options.model.clone(),
            temperature: options.temperature,
            max_tokens: options.max_tokens,
            api_key: options.api_key.clone(),
            ..Default::default()
        };
        self.chat_stream(&[Message::user(prompt)], &chat_options)
            .await
    }

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse> {
        let mut stream = self.chat_stream(messages, options).await?;
        while stream.next().await.is_some() {}
        Ok(stream.result().await)
    }

    async fn health_check(&self) -> bool {
        match self
            .chat(
                &[Message::user("hi")],
                &ChatOptions {
                    max_tokens: Some(1),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!("Anthropic health check failed: {}", e);
                false
            }
        }
    }
}

async fn finish_block(
    block: &ContentBlock,
    idx: usize,
    producer: &crate::AssistantMessageEventProducer,
    content: &[ContentBlock],
) {
    match block {
        ContentBlock::Text { text, .. } => {
            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: idx,
                    content: text.clone(),
                    partial: Message::assistant(content.to_vec()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
        }
        ContentBlock::Thinking { thinking, .. } => {
            let _ = producer
                .push(AssistantMessageEvent::ThinkingEnd {
                    content_index: idx,
                    content: thinking.clone(),
                    partial: Message::assistant(content.to_vec()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
        }
        ContentBlock::ToolCall {
            id,
            name,
            arguments,
            ..
        } => {
            let _ = producer
                .push(AssistantMessageEvent::ToolCallEnd {
                    content_index: idx,
                    tool_call: cog_core::ToolCall {
                        id: id.clone(),
                        name: name.clone(),
                        arguments: arguments.clone(),
                    },
                    partial: Message::assistant(content.to_vec()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
        }
        _ => {}
    }
}

fn map_stop_reason(reason: &str) -> StopReason {
    match reason {
        "end_turn" => StopReason::Stop,
        "max_tokens" => StopReason::Length,
        "tool_use" => StopReason::ToolUse,
        "stop_sequence" => StopReason::Stop,
        _ => StopReason::Error,
    }
}
