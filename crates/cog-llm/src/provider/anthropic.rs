use async_trait::async_trait;
use cog_core::{
    AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, StopReason, UpstreamFailure,
};
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::Arc;

use super::LLMProvider;
use crate::{
    model::Model, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, Usage,
};

/// The `api_style` word for the Anthropic messages face, and the endpoint a base
/// URL names on it.
///
/// Same routing key as [`crate::openai::API_STYLE`]: compared as a word, never
/// parsed from the shape of a response. The version segment belongs to the base
/// URL, because this is the only path the platform appends: every base carries
/// whatever precedes `/messages` in its vendor's documented endpoint
/// (`https://api.anthropic.com/v1`, `https://api.deepseek.com/anthropic/v1`),
/// and a base that leaves it out composes an address that never existed. Both
/// the word and that rule live in the contract layer so the gateway, which
/// composes the same address when it probes an upstream, reads one copy.
pub use cog_core::{messages_url, ANTHROPIC_API_STYLE as API_STYLE};

/// 这个协议面上「要不要让上游缓存前缀」的落点。
///
/// 不标断点就没有缓存：上游按 `cache_control` 逐块记下可复用的前缀，返回里的
/// `cache_creation_input_tokens` 只在请求标了断点时才会涨。2026-10-26 实测 372 条
/// 成功响应里这个计数**全是 0**，`cache_read_input_tokens` 只有 3.6% 的输入落在
/// 上面——同一批提示词在同一天的 OpenAI 兼容面上是 50%～67%。差的不是提示词的写法，
/// 是请求里没有这一句。
///
/// 保留期：`Short` 是上游默认的短窗（分钟级，够一段对话内复读）；`Long` 要一小时，
/// 写缓存按 2 倍价、读按 0.1 倍——只有「同一段前缀会在十分钟以上被反复重发」时才划算。
fn cache_marker(retention: cog_core::CacheRetention) -> Option<serde_json::Value> {
    match retention {
        cog_core::CacheRetention::None => None,
        cog_core::CacheRetention::Short => Some(json!({"type": "ephemeral"})),
        cog_core::CacheRetention::Long => Some(json!({"type": "ephemeral", "ttl": "1h"})),
    }
}

/// 把断点挂到一条消息内容的末尾。
///
/// 内容有两种线上形状：纯文本是裸字符串（没有位置挂断点），多媒体是块数组。
/// 字符串要就地折成单块数组——这不是改变语义，`[{"type":"text","text":s}]` 与 `s`
/// 在协议里是同一件事，只是一个能挂断点。
fn mark_content_end(content: &mut serde_json::Value, marker: &serde_json::Value) {
    match content {
        serde_json::Value::String(text) => {
            let text = text.clone();
            *content = json!([{ "type": "text", "text": text, "cache_control": marker }]);
        }
        serde_json::Value::Array(blocks) => {
            let marked = match blocks.last_mut().and_then(|b| b.as_object_mut()) {
                Some(last) => {
                    last.insert("cache_control".to_string(), marker.clone());
                    true
                }
                None => false,
            };
            if !marked {
                // 空内容也得有个位置：上游拒收空的 content 数组。
                *content = json!([{ "type": "text", "text": "", "cache_control": marker }]);
            }
        }
        _ => {}
    }
}

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
                Message::User { content, .. } => json!({
                    "role": "user",
                    "content": super::media::anthropic_content(content, &self.model)
                }),
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

        // 缓存断点：这个协议面不开自动缓存，不标就是全价重发（见 `cache_marker`）。
        let marker = cache_marker(options.cache_retention);

        if let Some(sys) = system {
            body["system"] = match &marker {
                // 标了断点的 system 只能是块数组：字符串形式没有地方挂断点。
                Some(m) => json!([{ "type": "text", "text": sys, "cache_control": m }]),
                None => json!(sys),
            };
        }
        if let Some(temp) = options.temperature {
            body["temperature"] = json!(temp);
        }
        if let Some(tools) = &options.tools {
            let mut tool_defs: Vec<_> = tools.iter().map(|t| {
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
            // 断点落在最后一个工具上：协议面的前缀顺序是 工具 → system → messages，
            // 标在末尾那一个上，整段工具定义连同它后面的 system 一起进缓存。
            if let Some(m) = marker.as_ref() {
                if let Some(last) = tool_defs.last_mut() {
                    last["cache_control"] = m.clone();
                }
            }
            body["tools"] = json!(tool_defs);
        }
        // 最后一个断点落在最后一条消息的末尾：下一次调用是同一段对话又长了一轮，
        // 前面那段逐字节没变，于是整段历史按缓存价读，只有新增的那一轮按原价。
        if let Some(m) = &marker {
            if let Some(last) = body["messages"]
                .as_array_mut()
                .and_then(|arr| arr.last_mut())
            {
                mark_content_end(&mut last["content"], m);
            }
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
        let headers = crate::origin::outbound_headers(options);
        let body = self.build_request_body(messages, options);
        let abort_signal = options.abort_signal.clone();

        let url = messages_url(&model.base_url);
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
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            };

            if producer
                .push(AssistantMessageEvent::Start {
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
                .header("Content-Type", cog_core::CONTENT_TYPE_JSON)
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
                let status = http_response.status;
                // 上游明说的等待时长要在读掉响应体之前取出来：头与体一起被
                // 消费，先取则留着，后取就只剩空表。
                response.retry_after_secs = cog_core::retry_after_hint(&http_response.headers);
                let declared_cause = cog_core::declared_upstream_cause(&http_response.headers);
                let text = http_response.drain_text().await;
                response.stop_reason = StopReason::Error;
                // 状态码是这次失败的**类型**，文本只是它的措辞。留在这里，
                // 下游的故障转移与退避就不必再去猜上游用了哪些词。
                response.upstream_failure =
                    Some(declared_cause.unwrap_or_else(|| UpstreamFailure::from_status(status)));
                response.error_message = Some(format!("API error (HTTP {status}): {text}"));
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
                            finish_block(block, block_index, &producer).await;
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
                finish_block(block, block_index, &producer).await;
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
        let chat_options = crate::utils::simple_options::chat_options_from(options);
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
) {
    match block {
        ContentBlock::Text { text, .. } => {
            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: idx,
                    content: text.clone(),
                    timestamp: chrono::Utc::now(),
                })
                .await;
        }
        ContentBlock::Thinking { thinking, .. } => {
            let _ = producer
                .push(AssistantMessageEvent::ThinkingEnd {
                    content_index: idx,
                    content: thinking.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ApiType, ModelCost, Provider};
    use cog_core::{CacheRetention, ToolDefinition};
    use std::collections::HashMap;

    fn test_model() -> Model {
        Model {
            id: "k3".into(),
            name: "k3".into(),
            api: ApiType::AnthropicMessages,
            provider: Provider::Anthropic,
            base_url: "https://api.example.com/anthropic/v1".into(),
            context_window: 200_000,
            max_tokens: 8192,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: false,
            supports_reasoning: true,
            cost: ModelCost::default(),
            headers: HashMap::new(),
        }
    }

    fn one_tool() -> ToolDefinition {
        ToolDefinition {
            name: "read_file".into(),
            description: "read one file".into(),
            parameters: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
        }
    }

    fn conversation() -> Vec<Message> {
        vec![
            Message::system("You are the planner."),
            Message::user("fix the alert"),
            Message::Assistant {
                content: vec![ContentBlock::text("looking")],
                tool_calls: None,
                usage: None,
                timestamp: chrono::Utc::now(),
            },
            Message::ToolResult {
                tool_call_id: "call_1".into(),
                tool_name: "read_file".into(),
                content: vec![ContentBlock::text("fn main() {}")],
                is_error: false,
                timestamp: chrono::Utc::now(),
            },
        ]
    }

    fn body_with(retention: CacheRetention, msgs: &[Message]) -> serde_json::Value {
        let provider = AnthropicProvider::new(test_model(), "key");
        let options = ChatOptions {
            tools: Some(vec![one_tool(), one_tool()]),
            cache_retention: retention,
            ..Default::default()
        };
        provider.build_request_body(msgs, &options)
    }

    /// 不要求缓存时，请求体与改动之前逐字节一致：这一支是**回归**的判据。
    #[test]
    fn retention_none_sends_no_breakpoint_at_all() {
        let body = body_with(CacheRetention::None, &conversation());
        assert!(
            !body.to_string().contains("cache_control"),
            "没要缓存却标了断点：上游会照标写缓存，按写价计费"
        );
        assert!(body["system"].is_string(), "system 仍是字符串形状");
    }

    /// 要了缓存：三条断点，落在模型实际读到的顺序上（工具 → system → 最后一条消息）。
    #[test]
    fn a_short_retention_marks_the_last_tool_the_system_and_the_tail() {
        let body = body_with(CacheRetention::Short, &conversation());
        let tools = body["tools"].as_array().expect("tools");
        assert!(tools.last().unwrap()["cache_control"].is_object());
        assert!(
            tools[0].get("cache_control").is_none(),
            "断点只标最后一个工具：标在中间会把后面的工具挤出可缓存前缀"
        );

        let system = body["system"].as_array().expect("system 必须是块数组");
        assert_eq!(
            system.last().unwrap()["cache_control"],
            json!({"type": "ephemeral"})
        );

        let msgs = body["messages"].as_array().expect("messages");
        let tail = msgs.last().unwrap()["content"].as_array().expect("content");
        assert_eq!(
            tail.last().unwrap()["cache_control"],
            json!({"type": "ephemeral"})
        );
    }

    /// 一小时写缓存按 2 倍价：只有明说要长留时才写这个 ttl。
    #[test]
    fn a_long_retention_asks_for_the_hour_and_a_short_one_does_not() {
        let long = body_with(CacheRetention::Long, &conversation());
        assert_eq!(long["system"][0]["cache_control"]["ttl"], json!("1h"));
        let short = body_with(CacheRetention::Short, &conversation());
        assert!(short["system"][0]["cache_control"].get("ttl").is_none());
    }

    /// 协议面允许的断点上限是四个；多标一个就整条请求被拒。
    #[test]
    fn the_breakpoints_stay_within_the_four_the_protocol_allows() {
        let body = body_with(CacheRetention::Long, &conversation());
        let count = body.to_string().matches("cache_control").count();
        assert!(count <= 4, "标了 {count} 个断点，上游只收 4 个");
    }

    /// 纯文本消息的内容是裸字符串，断点没有位置可挂——它必须就地折成块数组，
    /// 否则最后一条消息那一条断点会被静默丢掉，命中率只吃到前两条。
    #[test]
    fn a_plain_text_message_still_carries_the_tail_breakpoint() {
        let body = body_with(CacheRetention::Short, &[Message::user("hello")]);
        let content = &body["messages"][0]["content"];
        assert_eq!(
            content,
            &json!([{ "type": "text", "text": "hello",
                      "cache_control": {"type": "ephemeral"} }])
        );
    }

    /// 同一段前缀在两次调用之间必须逐字节相同，否则命中的开销白付。
    #[test]
    fn two_turns_of_one_conversation_send_the_same_head() {
        let mut msgs = conversation();
        let first = body_with(CacheRetention::Short, &msgs);
        msgs.push(Message::user("and now check the tests"));
        let second = body_with(CacheRetention::Short, &msgs);
        assert_eq!(first["system"], second["system"], "system 跨轮变了");
        assert_eq!(first["tools"], second["tools"], "工具定义跨轮变了");
        let head = first["messages"][0].clone();
        assert_eq!(head, second["messages"][0], "第一条消息跨轮变了");
    }
}
