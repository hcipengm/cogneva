use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, StopReason};
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

use super::LLMProvider;
use crate::{
    model::Model,
    utils::compat::{
        apply_openrouter_routing, apply_reasoning_effort, apply_vercel_routing,
        compat_from_options, MaxTokensField,
    },
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, Usage,
};

/// Finalize a tool call block by parsing accumulated JSON arguments.
/// If parse fails, keeps the raw string as `Value::String`.
fn finalize_tool_call(block: &mut ContentBlock, buffer: &str) {
    if let ContentBlock::ToolCall {
        ref mut arguments, ..
    } = block
    {
        if !buffer.is_empty() {
            *arguments = serde_json::from_str(buffer)
                .unwrap_or_else(|_| serde_json::Value::String(buffer.to_string()));
        }
    }
}

pub struct OpenAIProvider {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    model: Model,
    api_key: String,
    stream_capacity: usize,
}

impl OpenAIProvider {
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
        let compat = compat_from_options(options, &self.model.base_url);

        let mut msgs: Vec<serde_json::Value> = Vec::new();
        let mut prev_was_tool_result = false;

        for msg in messages {
            // If previous message was a tool result and this is a user/system message,
            // some providers require an assistant message in between.
            if compat.requires_assistant_after_tool_result
                && prev_was_tool_result
                && matches!(msg, Message::User { .. } | Message::System { .. })
            {
                msgs.push(json!({"role": "assistant", "content": ""}));
            }
            prev_was_tool_result = matches!(msg, Message::ToolResult { .. });

            let json_msg = match msg {
                Message::System { content, .. } => {
                    let role = if compat.supports_developer_role {
                        "developer"
                    } else {
                        "system"
                    };
                    json!({"role": role, "content": content})
                }
                Message::User { content, .. } => json!({"role": "user", "content": content}),
                Message::Assistant { content, .. } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| {
                            if b.is_text() {
                                b.as_text()
                            } else if b.is_thinking() && compat.requires_thinking_as_text {
                                b.as_thinking()
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("");
                    let mut m = json!({"role": "assistant", "content": text});
                    // Tool calls must accompany the assistant message on the wire:
                    // strict providers (Kimi/OpenAI) reject any later role:"tool"
                    // message whose tool_call_id was never declared here.
                    let calls = msg.tool_calls();
                    if !calls.is_empty() {
                        m["tool_calls"] = calls
                            .iter()
                            .map(|c| {
                                json!({
                                    "id": c.id,
                                    "type": "function",
                                    "function": {
                                        "name": c.name,
                                        "arguments": c.arguments.to_string(),
                                    }
                                })
                            })
                            .collect();
                    }
                    m
                }
                Message::ToolResult {
                    tool_call_id,
                    tool_name,
                    content,
                    ..
                } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    let mut m = json!({
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": text,
                    });
                    if compat.requires_tool_result_name {
                        m["name"] = json!(tool_name);
                    }
                    m
                }
            };
            msgs.push(json_msg);
        }

        let mut body = json!({
            "model": options.model.as_ref().unwrap_or(&self.model.id),
            "messages": msgs,
            "stream": true,
        });

        if compat.supports_usage_in_streaming {
            body["stream_options"] = json!({ "include_usage": true });
        }

        if let Some(temp) = options.temperature {
            // Some reasoning models (e.g. Kimi k2.6) only accept temperature=1.0.
            let effective_temp = if compat.requires_temperature_one {
                1.0
            } else {
                temp
            };
            body["temperature"] = json!(effective_temp);
        }
        if let Some(max) = options.max_tokens {
            match compat.max_tokens_field {
                MaxTokensField::MaxCompletionTokens => {
                    body["max_completion_tokens"] = json!(max);
                }
                MaxTokensField::MaxTokens => {
                    body["max_tokens"] = json!(max);
                }
            }
        }
        if let Some(tools) = &options.tools {
            let tool_defs: Vec<_> = tools
                .iter()
                .map(|t| {
                    let mut func = json!({
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    });
                    if compat.supports_strict_mode {
                        func["strict"] = json!(true);
                    }
                    json!({
                        "type": "function",
                        "function": func,
                    })
                })
                .collect();
            body["tools"] = json!(tool_defs);
        }
        if options.response_format == crate::ResponseFormat::Json {
            body["response_format"] = json!({"type": "json_object"});
        }

        if compat.supports_reasoning_effort {
            apply_reasoning_effort(
                &mut body,
                options.reasoning,
                compat.thinking_format,
                &compat.reasoning_effort_map,
            );
        }

        if let Some(ref routing) = compat.openrouter_routing {
            apply_openrouter_routing(&mut body, routing);
        }
        if let Some(ref routing) = compat.vercel_gateway_routing {
            apply_vercel_routing(&mut body, routing);
        }

        if compat.zai_tool_stream {
            body["tool_stream"] = json!(true);
        }

        body
    }
}

#[async_trait]
impl LLMProvider for OpenAIProvider {
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
            .ok_or_else(|| SFError::Agent("OpenAIProvider: no HttpClient configured".into()))?;
        let headers = options.headers.clone();
        let body = self.build_request_body(messages, options);
        let abort_signal = options.abort_signal.clone();

        // Apply on_payload hook if provided
        let body = if let Some(ref on_payload) = options.on_payload {
            let model_info = cog_core::LlmModelInfo {
                id: model.id.clone(),
                name: model.name.clone(),
                provider: model.provider.to_string(),
                base_url: model.base_url.clone(),
            };
            match on_payload(body, &model_info) {
                Ok(modified) => modified,
                Err(e) => {
                    let response = ChatResponse {
                        stop_reason: StopReason::Error,
                        error_message: Some(format!("on_payload hook error: {}", e)),
                        ..Default::default()
                    };
                    producer.end(response);
                    return Ok(stream);
                }
            }
        } else {
            body
        };

        let url = format!("{}/chat/completions", model.base_url);
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
                        transport: "openai".into(),
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
                api: "openai-completions".into(),
                provider: model.provider.to_string(),
                model: model.id.clone(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };
            let start = std::time::Instant::now();

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

            // kimi-k2.6 and other reasoning models can stream for 90-120 s per
            // turn; use a generous per-request timeout so the pipeline is not
            // cut off mid-stream.
            let mut req = cog_core::HttpRequest::post(&url)
                .header("Authorization", format!("Bearer {}", api_key))
                .header("Content-Type", "application/json")
                .json(&body)
                .map_err(|e| SFError::Agent(format!("JSON serialization failed: {}", e)))
                .unwrap()
                .timeout(600);

            for (key, value) in &headers {
                req = req.header(key, value);
            }

            tracing::info!(provider = "openai", url = %url, model = %model.id, "OpenAIProvider executing stream request");
            let http_response = match client.execute_stream(req).await {
                Ok(r) => {
                    tracing::info!(
                        provider = "openai",
                        status = r.status,
                        "OpenAIProvider stream request succeeded"
                    );
                    r
                }
                Err(e) => {
                    tracing::warn!(provider = "openai", error = %e, "OpenAIProvider stream request failed");
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
            let mut current_tool_args_buffer: Option<String> = None;

            // Some providers (e.g. kimi-k2.6 under high load) open the SSE stream
            // but then stall without sending data. A per-event timeout lets us fail
            // fast and gives the RoutingProvider a chance to failover, while a hard
            // total duration cap (matching the HTTP request timeout) prevents a
            // single slow reasoning stream from blocking the whole squad execution
            // indefinitely. The per-event timeout resets after the first event so
            // that long initial reasoning windows are tolerated without masking
            // mid-stream stalls.
            let total_timeout = Duration::from_secs(600);
            // Before the first event, tolerate long reasoning windows up to the
            // total cap; tightened to 60s once bytes start flowing.
            let mut event_timeout = total_timeout;
            let stream_start = std::time::Instant::now();
            let mut first_event_logged = false;

            'stream: loop {
                let elapsed = stream_start.elapsed();
                if elapsed >= total_timeout {
                    tracing::warn!(
                        provider = "openai",
                        model = %model.id,
                        total_secs = total_timeout.as_secs(),
                        "SSE stream exceeded total duration limit"
                    );
                    response.stop_reason = StopReason::Error;
                    response.error_message = Some(format!(
                        "SSE stream exceeded total duration limit of {}s",
                        total_timeout.as_secs()
                    ));
                    break;
                }

                let remaining = total_timeout - elapsed;
                let timeout = event_timeout.min(remaining).max(Duration::from_secs(1));

                tracing::debug!(
                    provider = "openai",
                    model = %model.id,
                    timeout_secs = timeout.as_secs(),
                    "awaiting next SSE event"
                );

                let line = match tokio::time::timeout(timeout, lines.try_next()).await {
                    Ok(Ok(Some(line))) => line,
                    Ok(Ok(None)) => {
                        tracing::debug!(provider = "openai", model = %model.id, "SSE stream closed by provider");
                        break;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(provider = "openai", model = %model.id, error = %e, "SSE stream read error");
                        response.stop_reason = StopReason::Error;
                        response.error_message = Some(format!("SSE stream read error: {}", e));
                        break;
                    }
                    Err(_) => {
                        tracing::warn!(
                            provider = "openai",
                            model = %model.id,
                            timeout_secs = timeout.as_secs(),
                            "SSE stream timed out waiting for next event"
                        );
                        response.stop_reason = StopReason::Error;
                        response.error_message = Some(format!(
                            "SSE stream timed out after {}s with no events",
                            timeout.as_secs()
                        ));
                        break;
                    }
                };

                if !first_event_logged {
                    tracing::info!(
                        provider = "openai",
                        model = %model.id,
                        "OpenAIProvider SSE stream received first event"
                    );
                    first_event_logged = true;
                    // After the first event we can use a stricter per-event
                    // timeout to detect mid-stream stalls.
                    event_timeout = Duration::from_secs(60);
                }

                let line = line.trim();
                // SSE 规范里 data 后的空格可选（有实现发 `data:{`）
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim_start();
                if data == "[DONE]" {
                    break;
                }

                let json: serde_json::Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Extract response ID
                if response.response_id.is_none() {
                    if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                        response.response_id = Some(id.to_string());
                    }
                }

                // Extract usage
                if let Some(usage) = json.get("usage") {
                    let prompt = usage
                        .get("prompt_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let completion = usage
                        .get("completion_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    let total = usage
                        .get("total_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    response.usage.input = prompt;
                    response.usage.output = completion;
                    response.usage.total_tokens = total;
                }

                // Extract choice delta
                let choice = json
                    .get("choices")
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.first());
                if let Some(choice) = choice {
                    // Handle finish_reason
                    if let Some(finish) = choice.get("finish_reason").and_then(|v| v.as_str()) {
                        let (stop_reason, error_msg) = map_stop_reason(finish);
                        response.stop_reason = stop_reason;
                        if let Some(msg) = error_msg {
                            response.error_message = Some(msg);
                        }
                    }

                    if let Some(delta) = choice.get("delta") {
                        // Text content
                        if let Some(content) = delta.get("content").and_then(|v| v.as_str()) {
                            if !content.is_empty() {
                                let idx = response.content.len();
                                if current_block.as_ref().is_none_or(|b| !b.is_text()) {
                                    // Finish previous block if exists
                                    if let Some(ref mut block) = current_block {
                                        if block.is_tool_call() {
                                            if let Some(ref buf) = current_tool_args_buffer {
                                                finalize_tool_call(block, buf);
                                                if let Some(last) = response.content.last_mut() {
                                                    *last = block.clone();
                                                }
                                            }
                                        }
                                        let prev_idx = idx.saturating_sub(1);
                                        finish_block(block, prev_idx, &producer, &response.content)
                                            .await;
                                    }
                                    current_tool_args_buffer = None;
                                    current_block = Some(ContentBlock::text(content));
                                    response.content.push(current_block.clone().unwrap());
                                    if producer
                                        .push(AssistantMessageEvent::TextStart {
                                            content_index: idx,
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                } else if let Some(ref mut block) = current_block {
                                    block.append_text(content);
                                    if let Some(last) = response.content.last_mut() {
                                        *last = block.clone();
                                    }
                                }
                                if producer
                                    .push(AssistantMessageEvent::TextDelta {
                                        content_index: idx,
                                        delta: content.to_string(),
                                        partial: Message::assistant(response.content.clone()),
                                        timestamp: chrono::Utc::now(),
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }

                        // Reasoning/thinking content
                        let reasoning_fields = ["reasoning_content", "reasoning", "reasoning_text"];
                        for field in &reasoning_fields {
                            if let Some(reasoning) = delta.get(field).and_then(|v| v.as_str()) {
                                if !reasoning.is_empty() {
                                    let idx = response.content.len();
                                    if current_block.as_ref().is_none_or(|b| !b.is_thinking()) {
                                        if let Some(ref mut block) = current_block {
                                            if block.is_tool_call() {
                                                if let Some(ref buf) = current_tool_args_buffer {
                                                    finalize_tool_call(block, buf);
                                                    if let Some(last) = response.content.last_mut()
                                                    {
                                                        *last = block.clone();
                                                    }
                                                }
                                            }
                                            let prev_idx = idx.saturating_sub(1);
                                            finish_block(
                                                block,
                                                prev_idx,
                                                &producer,
                                                &response.content,
                                            )
                                            .await;
                                        }
                                        current_tool_args_buffer = None;
                                        current_block = Some(ContentBlock::thinking(reasoning));
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
                                            break;
                                        }
                                    } else if let Some(ref mut block) = current_block {
                                        block.append_thinking(reasoning);
                                        if let Some(last) = response.content.last_mut() {
                                            *last = block.clone();
                                        }
                                    }
                                    if producer
                                        .push(AssistantMessageEvent::ThinkingDelta {
                                            content_index: idx,
                                            delta: reasoning.to_string(),
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                    break; // Only use first non-empty reasoning field
                                }
                            }
                        }

                        // Tool calls
                        if let Some(tool_calls) = delta.get("tool_calls").and_then(|v| v.as_array())
                        {
                            for tool_call in tool_calls {
                                let id = tool_call
                                    .get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let name = tool_call
                                    .get("function")
                                    .and_then(|f| f.get("name"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let args_chunk = tool_call
                                    .get("function")
                                    .and_then(|f| f.get("arguments"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");

                                let idx = response.content.len();
                                if current_block.as_ref().is_none_or(|b| !b.is_tool_call()) {
                                    if let Some(ref mut block) = current_block {
                                        if block.is_tool_call() {
                                            if let Some(ref buf) = current_tool_args_buffer {
                                                finalize_tool_call(block, buf);
                                                if let Some(last) = response.content.last_mut() {
                                                    *last = block.clone();
                                                }
                                            }
                                        }
                                        finish_block(
                                            block,
                                            idx.saturating_sub(1),
                                            &producer,
                                            &response.content,
                                        )
                                        .await;
                                    }
                                    let mut buffer = String::new();
                                    buffer.push_str(args_chunk);
                                    current_block = Some(ContentBlock::tool_call(
                                        if id.is_empty() {
                                            "pending".to_string()
                                        } else {
                                            id.clone()
                                        },
                                        if name.is_empty() {
                                            "pending".to_string()
                                        } else {
                                            name.clone()
                                        },
                                        serde_json::Value::Object(Default::default()),
                                    ));
                                    current_tool_args_buffer = Some(buffer);
                                    response.content.push(current_block.clone().unwrap());
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
                                } else if let Some(ref mut buffer) = current_tool_args_buffer {
                                    buffer.push_str(args_chunk);
                                    // Do NOT update ContentBlock::ToolCall.arguments during deltas.
                                    // arguments remains an empty object placeholder until the block is finalized.
                                }
                                if producer
                                    .push(AssistantMessageEvent::ToolCallDelta {
                                        content_index: idx,
                                        delta: args_chunk.to_string(),
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
                if block.is_tool_call() {
                    if let Some(ref buf) = current_tool_args_buffer {
                        finalize_tool_call(block, buf);
                    }
                }
                if let Some(last) = response.content.last_mut() {
                    *last = block.clone();
                }
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

            tracing::info!(
                provider = "openai",
                model = %model.id,
                elapsed_ms = %start.elapsed().as_millis(),
                tokens_in = %response.usage.input,
                tokens_out = %response.usage.output,
                "OpenAIProvider stream finished"
            );

            if response.stop_reason == StopReason::Aborted {
                let _ = producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Aborted,
                        error: Message::assistant_text("Request was aborted"),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            } else if response.stop_reason == StopReason::Error {
                let _ = producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Error,
                        error: Message::assistant_text(
                            response.error_message.clone().unwrap_or_default(),
                        ),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            } else {
                let _ = producer
                    .push(AssistantMessageEvent::Done {
                        reason: response.stop_reason,
                        message: Message::assistant(response.content.clone()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
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
                        transport: "openai".into(),
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
        // The stream result future can race with the producer task and return a
        // default ChatResponse when the producer is dropped before calling end().
        // To make chat() reliable we therefore reconstruct the final response
        // from the Done event, which always carries the full assistant message.
        //
        // A hard timeout protects the caller from providers (e.g. kimi-k2.6)
        // that open the SSE stream and then stall without closing it.
        let chat_timeout = Duration::from_secs(180);
        let consume_result = tokio::time::timeout(chat_timeout, async {
            let mut final_message: Option<Message> = None;
            while let Some(event) = stream.next().await {
                if let AssistantMessageEvent::Done { message, .. } = event {
                    final_message = Some(message);
                }
            }
            final_message
        })
        .await;

        let final_message = match consume_result {
            Ok(msg) => msg,
            Err(_) => {
                return Err(SFError::LLM(format!(
                    "OpenAIProvider chat() timed out after {}s waiting for stream",
                    chat_timeout.as_secs()
                )));
            }
        };

        let mut response = stream.result().await;
        if let Some(Message::Assistant { content, .. }) = final_message {
            response.content = content;
        }
        Ok(response)
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
                tracing::warn!("OpenAI health check failed: {}", e);
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

fn map_stop_reason(reason: &str) -> (StopReason, Option<String>) {
    match reason {
        "stop" | "end" => (StopReason::Stop, None),
        "length" => (StopReason::Length, None),
        "function_call" | "tool_calls" => (StopReason::ToolUse, None),
        "content_filter" => (
            StopReason::Error,
            Some("Provider finish_reason: content_filter".to_string()),
        ),
        "network_error" => (
            StopReason::Error,
            Some("Provider finish_reason: network_error".to_string()),
        ),
        _ => (
            StopReason::Error,
            Some(format!("Provider finish_reason: {}", reason)),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ApiType, ModelCost, Provider};
    use std::collections::HashMap;

    fn test_model() -> Model {
        Model {
            id: "k3".into(),
            name: "k3".into(),
            api: ApiType::OpenAICompletions,
            provider: Provider::OpenAI,
            base_url: "http://gateway-internal:8081/v1".into(),
            context_window: 128_000,
            max_tokens: 8192,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: false,
            supports_reasoning: true,
            cost: ModelCost {
                input: 0.0,
                output: 0.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: HashMap::new(),
        }
    }

    #[test]
    fn assistant_tool_calls_are_serialized() {
        let provider = OpenAIProvider::new(test_model(), "key");
        let assistant = Message::assistant(vec![
            ContentBlock::text("我先请求一下"),
            ContentBlock::tool_call(
                "call_123",
                "http_request",
                json!({"url": "https://example.com"}),
            ),
        ]);
        let tool_result = Message::tool_result_text("call_123", "http_request", "{\"status\":200}");
        let messages = vec![Message::system("sys"), assistant, tool_result];

        let body = provider.build_request_body(&messages, &ChatOptions::default());
        let msgs = body["messages"].as_array().unwrap();
        let wire_calls = msgs[1]["tool_calls"].as_array().unwrap();
        assert_eq!(wire_calls.len(), 1);
        assert_eq!(wire_calls[0]["id"], "call_123");
        assert_eq!(wire_calls[0]["type"], "function");
        assert_eq!(wire_calls[0]["function"]["name"], "http_request");
        assert_eq!(
            wire_calls[0]["function"]["arguments"],
            serde_json::Value::String("{\"url\":\"https://example.com\"}".into())
        );
        assert_eq!(msgs[2]["role"], "tool");
        assert_eq!(msgs[2]["tool_call_id"], "call_123");
    }

    #[test]
    fn assistant_without_tool_calls_has_no_tool_calls_field() {
        let provider = OpenAIProvider::new(test_model(), "key");
        let messages = vec![Message::system("sys"), Message::assistant_text("plain")];
        let body = provider.build_request_body(&messages, &ChatOptions::default());
        let msgs = body["messages"].as_array().unwrap();
        assert!(msgs[1].get("tool_calls").is_none());
    }
}
