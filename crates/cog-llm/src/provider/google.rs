use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, StopReason};
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::Arc;

use super::LLMProvider;
use crate::{
    model::Model, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, Usage,
};

pub struct GoogleProvider {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    model: Model,
    api_key: String,
    stream_capacity: usize,
}

impl GoogleProvider {
    pub fn new(model: Model, api_key: impl Into<String>) -> Self {
        Self {
            client: None,
            model,
            api_key: api_key.into(),
            stream_capacity: crate::DEFAULT_STREAM_CAPACITY,
        }
    }

    pub fn with_stream_capacity(mut self, capacity: usize) -> Self {
        self.stream_capacity = capacity;
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

        let contents: Vec<serde_json::Value> = messages
            .iter()
            .filter(|m| !matches!(m, Message::System { .. }))
            .map(|msg| match msg {
                Message::User { content, .. } => {
                    json!({"role": "user", "parts": [{"text": content}]})
                }
                Message::Assistant { content, .. } => {
                    let mut parts: Vec<serde_json::Value> = Vec::new();

                    // Text parts
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    if !text.is_empty() {
                        parts.push(json!({"text": text}));
                    }

                    // Function call parts for tool calls in conversation history
                    for block in content {
                        if let ContentBlock::ToolCall {
                            name, arguments, ..
                        } = block
                        {
                            parts.push(json!({
                                "functionCall": {
                                    "name": name,
                                    "args": arguments,
                                }
                            }));
                        }
                    }

                    if parts.is_empty() {
                        parts.push(json!({"text": ""}));
                    }

                    json!({"role": "model", "parts": parts})
                }
                Message::ToolResult {
                    tool_name, content, ..
                } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    json!({
                        "role": "user",
                        "parts": [{
                            "functionResponse": {
                                "name": tool_name,
                                "response": { "result": text }
                            }
                        }]
                    })
                }
                _ => json!({"role": "user", "parts": [{"text": ""}]}),
            })
            .collect();

        let mut body = json!({
            "contents": contents,
        });

        if let Some(sys) = system {
            body["systemInstruction"] = json!({"parts": [{"text": sys}]});
        }

        let mut generation_config = serde_json::Map::new();
        if let Some(temp) = options.temperature {
            generation_config.insert("temperature".into(), json!(temp));
        }
        if let Some(max) = options.max_tokens {
            generation_config.insert("maxOutputTokens".into(), json!(max));
        }
        if !generation_config.is_empty() {
            body["generationConfig"] = generation_config.into();
        }

        if let Some(tools) = &options.tools {
            let func_decls: Vec<_> = tools
                .iter()
                .map(|t| {
                    json!({
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    })
                })
                .collect();
            body["tools"] = json!([{"functionDeclarations": func_decls}]);
        }

        // Tool config for tool choice (auto/none/any)
        if let Some(tool_choice) = options.metadata.get("tool_choice") {
            match tool_choice.as_str() {
                "none" => {
                    body["toolConfig"] = json!({
                        "functionCallingConfig": { "mode": "NONE" }
                    });
                }
                "any" => {
                    body["toolConfig"] = json!({
                        "functionCallingConfig": { "mode": "ANY" }
                    });
                }
                _ => {
                    // Default to AUTO
                    body["toolConfig"] = json!({
                        "functionCallingConfig": { "mode": "AUTO" }
                    });
                }
            }
        }

        body
    }
}

#[async_trait]
impl LLMProvider for GoogleProvider {
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
            .ok_or_else(|| SFError::Agent("GoogleProvider: no HttpClient configured".into()))?;
        let headers = options.headers.clone();
        let body = self.build_request_body(messages, options);
        let abort_signal = options.abort_signal.clone();

        let model_id = options.model.as_ref().unwrap_or(&model.id);
        let url = format!(
            "{}/models/{}:streamGenerateContent?key={}",
            model.base_url, model_id, api_key
        );

        tokio::spawn(async move {
            let mut response = ChatResponse {
                content: Vec::new(),
                api: "google-generative-ai".into(),
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
            }

            let mut req = cog_core::HttpRequest::post(&url)
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
                    }
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
                }
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
            let mut tool_call_counter = 0u64;

            'stream: while let Ok(Some(line)) = lines.try_next().await {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let json: serde_json::Value = match serde_json::from_str(line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Extract response ID
                if response.response_id.is_none() {
                    if let Some(id) = json.get("responseId").and_then(|v| v.as_str()) {
                        response.response_id = Some(id.to_string());
                    }
                }

                // Extract usage
                if let Some(usage) = json.get("usageMetadata") {
                    let prompt = usage
                        .get("promptTokenCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let candidates = usage
                        .get("candidatesTokenCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let thoughts = usage
                        .get("thoughtsTokenCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let cached = usage
                        .get("cachedContentTokenCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let total = usage
                        .get("totalTokenCount")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    response.usage.input = prompt.saturating_sub(cached);
                    response.usage.output = candidates + thoughts;
                    response.usage.cache_read = cached;
                    response.usage.total_tokens = total;
                }

                // Extract candidates
                if let Some(candidates) = json.get("candidates").and_then(|v| v.as_array()) {
                    if let Some(candidate) = candidates.first() {
                        // Handle finish reason
                        if let Some(finish) = candidate.get("finishReason").and_then(|v| v.as_str())
                        {
                            response.stop_reason = map_stop_reason(finish);
                        }

                        if let Some(parts) = candidate
                            .get("content")
                            .and_then(|c| c.get("parts"))
                            .and_then(|v| v.as_array())
                        {
                            for part in parts {
                                // Text content
                                if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                                    let is_thinking = part
                                        .get("thought")
                                        .and_then(|v| v.as_bool())
                                        .unwrap_or(false);

                                    if is_thinking {
                                        let idx = response.content.len();
                                        if current_block.as_ref().is_none_or(|b| !b.is_thinking()) {
                                            if let Some(ref block) = current_block {
                                                if let Some(t) = block.as_text() {
                                                    if producer
                                                        .push(AssistantMessageEvent::TextEnd {
                                                            content_index: idx.saturating_sub(1),
                                                            content: t.to_string(),
                                                            partial: Message::assistant(
                                                                response.content.clone(),
                                                            ),
                                                            timestamp: chrono::Utc::now(),
                                                        })
                                                        .await
                                                        .is_err()
                                                    {
                                                        break 'stream;
                                                    }
                                                }
                                            }
                                            current_block = Some(ContentBlock::thinking(text));
                                            response.content.push(current_block.clone().unwrap());
                                            if producer
                                                .push(AssistantMessageEvent::ThinkingStart {
                                                    content_index: idx,
                                                    partial: Message::assistant(
                                                        response.content.clone(),
                                                    ),
                                                    timestamp: chrono::Utc::now(),
                                                })
                                                .await
                                                .is_err()
                                            {
                                                break 'stream;
                                            }
                                        } else if let Some(ref mut block) = current_block {
                                            block.append_thinking(text);
                                            if let Some(last) = response.content.last_mut() {
                                                *last = block.clone();
                                            }
                                        }
                                        if producer
                                            .push(AssistantMessageEvent::ThinkingDelta {
                                                content_index: idx,
                                                delta: text.to_string(),
                                                partial: Message::assistant(
                                                    response.content.clone(),
                                                ),
                                                timestamp: chrono::Utc::now(),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            break 'stream;
                                        }
                                    } else {
                                        let idx = response.content.len();
                                        if current_block.as_ref().is_none_or(|b| !b.is_text()) {
                                            if let Some(ref block) = current_block {
                                                if let Some(t) = block.as_thinking() {
                                                    if producer
                                                        .push(AssistantMessageEvent::ThinkingEnd {
                                                            content_index: idx.saturating_sub(1),
                                                            content: t.to_string(),
                                                            partial: Message::assistant(
                                                                response.content.clone(),
                                                            ),
                                                            timestamp: chrono::Utc::now(),
                                                        })
                                                        .await
                                                        .is_err()
                                                    {
                                                        break 'stream;
                                                    }
                                                }
                                            }
                                            current_block = Some(ContentBlock::text(text));
                                            response.content.push(current_block.clone().unwrap());
                                            if producer
                                                .push(AssistantMessageEvent::TextStart {
                                                    content_index: idx,
                                                    partial: Message::assistant(
                                                        response.content.clone(),
                                                    ),
                                                    timestamp: chrono::Utc::now(),
                                                })
                                                .await
                                                .is_err()
                                            {
                                                break 'stream;
                                            }
                                        } else if let Some(ref mut block) = current_block {
                                            block.append_text(text);
                                            if let Some(last) = response.content.last_mut() {
                                                *last = block.clone();
                                            }
                                        }
                                        if producer
                                            .push(AssistantMessageEvent::TextDelta {
                                                content_index: idx,
                                                delta: text.to_string(),
                                                partial: Message::assistant(
                                                    response.content.clone(),
                                                ),
                                                timestamp: chrono::Utc::now(),
                                            })
                                            .await
                                            .is_err()
                                        {
                                            break 'stream;
                                        }
                                    }
                                }

                                // Function calls (tool calls)
                                if let Some(func_call) = part.get("functionCall") {
                                    let name = func_call
                                        .get("name")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    let args = func_call
                                        .get("args")
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Object(Default::default()));
                                    let provided_id = func_call.get("id").and_then(|v| v.as_str());
                                    let needs_new_id = provided_id.is_none()
                                        || response.content.iter().any(|b| {
                                            if let ContentBlock::ToolCall { id, .. } = b {
                                                id == provided_id.unwrap()
                                            } else {
                                                false
                                            }
                                        });
                                    let id = if needs_new_id {
                                        format!(
                                            "{}_{}_{}",
                                            name,
                                            chrono::Utc::now().timestamp_millis(),
                                            {
                                                tool_call_counter += 1;
                                                tool_call_counter
                                            }
                                        )
                                    } else {
                                        provided_id.unwrap().to_string()
                                    };

                                    let idx = response.content.len();
                                    if let Some(ref block) = current_block {
                                        finish_block(
                                            block,
                                            idx.saturating_sub(1),
                                            &producer,
                                            &response.content,
                                        )
                                        .await;
                                    }
                                    current_block = None;

                                    let tool_call_block =
                                        ContentBlock::tool_call(id, name, args.clone());
                                    response.content.push(tool_call_block.clone());
                                    if producer
                                        .push(AssistantMessageEvent::ToolCallStart {
                                            content_index: idx,
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break 'stream;
                                    }
                                    if producer
                                        .push(AssistantMessageEvent::ToolCallDelta {
                                            content_index: idx,
                                            delta: args.to_string(),
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break 'stream;
                                    }
                                    if producer
                                        .push(AssistantMessageEvent::ToolCallEnd {
                                            content_index: idx,
                                            tool_call: cog_core::ToolCall {
                                                id: if let ContentBlock::ToolCall { id, .. } =
                                                    &tool_call_block
                                                {
                                                    id.clone()
                                                } else {
                                                    String::new()
                                                },
                                                name: if let ContentBlock::ToolCall {
                                                    name, ..
                                                } = &tool_call_block
                                                {
                                                    name.clone()
                                                } else {
                                                    String::new()
                                                },
                                                arguments: if let ContentBlock::ToolCall {
                                                    arguments,
                                                    ..
                                                } = &tool_call_block
                                                {
                                                    arguments.clone()
                                                } else {
                                                    serde_json::Value::Null
                                                },
                                            },
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break 'stream;
                                    }
                                }
                            }
                        }
                    }
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
            if let Some(ref block) = current_block {
                let idx = response.content.len().saturating_sub(1);
                finish_block(block, idx, &producer, &response.content).await;
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
                }
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
                }
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
                tracing::warn!("Google health check failed: {}", e);
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
        _ => {}
    }
}

fn map_stop_reason(reason: &str) -> StopReason {
    match reason {
        "STOP" => StopReason::Stop,
        "MAX_TOKENS" => StopReason::Length,
        _ => StopReason::Error,
    }
}
