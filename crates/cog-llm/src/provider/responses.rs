//! The native Responses face of the OpenAI protocol family.
//!
//! The OpenAI-compatible vendors speak two shapes: the chat-completions shape
//! (`messages`, `choices`, `tool_calls`) and the Responses shape (`input`,
//! `output` items, `function_call`, `response.*` stream events). The Responses
//! shape is the one the vendor documents as current, and it is the only one
//! that carries a reasoning item and a tool call as separate items of one
//! output; the chat shape flattens both into a single message. A caller that
//! can speak it should, and this module is what makes the platform able to.
//!
//! Everything below the wire is the platform's own model of a turn: the same
//! [`Message`] chain in, the same [`ContentBlock`]s out, so a provider swapped
//! between the two shapes changes nothing above the transport.

use async_trait::async_trait;
use cog_core::{
    AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, StopReason, UpstreamFailure,
};
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::openai::{finalize_tool_call, finish_block, sanitize_tool_arguments};
use super::LLMProvider;
use crate::model::Model;
use crate::utils::compat::compat_from_options;
use crate::{AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, Usage};

/// The `api_style` string that names this protocol face.
pub const API_STYLE: &str = "openai-responses";

/// Whether a backend's declared style belongs to the OpenAI protocol family.
///
/// The family is what keeps a vendor that answers `/responses` from dropping
/// out of the chat-completions face: serving the newer shape says nothing about
/// having stopped serving the older one, and a pool entry is listed under the
/// face it was *verified* on, not the only face it can serve.
pub fn is_openai_family(style: &str) -> bool {
    style == "openai" || style == API_STYLE
}

/// The endpoint a base URL names on this face.
///
/// Same rule as the other two faces: the platform appends exactly one path
/// element, so the base carries everything before it, version segment
/// included (`https://api.openai.com/v1` composes
/// `https://api.openai.com/v1/responses`).
pub fn responses_url(base_url: &str) -> String {
    format!("{}/responses", base_url.trim_end_matches('/'))
}

/// Per-item accumulator for a streamed function call. Responses names each
/// item (`item_id`) rather than indexing a parallel array, and one turn can
/// carry several calls, so chunks are keyed by that name.
struct ToolAcc {
    id: String,
    name: String,
    args: String,
    content_pos: usize,
}

/// One user turn's content in the Responses shape.
///
/// Text-only turns go out as a plain string, which every implementation
/// accepts; a turn with media goes out as parts, where the part type is
/// `input_text`/`input_image` rather than the chat shape's `text`/`image_url`.
/// Media the model cannot take is dropped with a note, the same rule the other
/// faces follow, so a downgraded image does not silently vanish from the turn.
fn input_content(blocks: &[ContentBlock], model: &Model) -> serde_json::Value {
    let text: String = blocks
        .iter()
        .filter_map(|b| b.as_text().map(str::to_string))
        .collect::<Vec<_>>()
        .join("");
    let mut parts: Vec<serde_json::Value> = Vec::new();
    let mut dropped = false;
    for block in blocks {
        match block {
            ContentBlock::Media { data, mime_type } if model.supports_vision => {
                parts.push(json!({
                    "type": "input_image",
                    "image_url": format!("data:{mime_type};base64,{data}"),
                }));
            }
            ContentBlock::Media { .. } => dropped = true,
            _ => {}
        }
    }
    if parts.is_empty() {
        if dropped {
            return json!(format!(
                "{text}\n[image omitted: this model has no vision support]"
            ));
        }
        return json!(text);
    }
    let mut content = vec![json!({"type": "input_text", "text": text})];
    content.extend(parts);
    if dropped {
        content.push(json!({
            "type": "input_text",
            "text": "[unsupported media omitted]",
        }));
    }
    json!(content)
}

pub struct OpenAIResponsesProvider {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    model: Model,
    api_key: String,
    stream_capacity: usize,
}

impl OpenAIResponsesProvider {
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

    /// The conversation as Responses input items.
    ///
    /// An assistant turn splits into two kinds of item, which is the shape's
    /// whole point: what the model said is a `message`, and each call it made
    /// is a `function_call` carrying the `call_id` the tool result has to name.
    /// The result comes back as a `function_call_output` item, not as a
    /// message with a `tool_call_id` field.
    fn input_items(&self, messages: &[Message]) -> Vec<serde_json::Value> {
        // Same wire-validity guard as the chat face: context trimming can leave
        // an orphan call or result, and strict vendors reject the whole request
        // for it. Repaired here so the producing bug stays visible in the log.
        let (messages, repair) = cog_core::enforce_tool_chain_validity(messages);
        if !repair.is_clean() {
            tracing::warn!(
                stripped_assistants = repair.stripped_assistants,
                dropped_tool_results = repair.dropped_tool_results,
                "outgoing LLM request had orphan tool calls/results; repaired at protocol edge"
            );
        }

        let mut items: Vec<serde_json::Value> = Vec::new();
        for msg in &messages {
            match msg {
                Message::System { content, .. } => items.push(json!({
                    "role": "system",
                    "content": [{"type": "input_text", "text": content}],
                })),
                Message::User { content, .. } => items.push(json!({
                    "role": "user",
                    "content": input_content(content, &self.model),
                })),
                Message::Assistant { content, .. } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    if !text.is_empty() {
                        items.push(json!({
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": text}],
                        }));
                    }
                    for call in msg.tool_calls() {
                        items.push(json!({
                            "type": "function_call",
                            "call_id": call.id,
                            "name": call.name,
                            "arguments": sanitize_tool_arguments(&call.arguments).to_string(),
                        }));
                    }
                }
                Message::ToolResult {
                    tool_call_id,
                    content,
                    ..
                } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    items.push(json!({
                        "type": "function_call_output",
                        "call_id": tool_call_id,
                        "output": text,
                    }));
                }
            }
        }
        items
    }

    fn build_request_body(&self, messages: &[Message], options: &ChatOptions) -> serde_json::Value {
        let compat = compat_from_options(options, &self.model.base_url);
        let mut body = json!({
            "model": options.model.as_ref().unwrap_or(&self.model.id),
            "input": self.input_items(messages),
            "stream": true,
        });

        if let Some(temp) = options.temperature {
            // The same reasoning models the chat face clamps are reached
            // through this one too.
            body["temperature"] = json!(if compat.requires_temperature_one {
                1.0
            } else {
                temp
            });
        }
        if let Some(max) = options.max_tokens {
            // The chat face's `max_tokens` / `max_completion_tokens` pair does
            // not exist here: this shape has one name for the cap.
            body["max_output_tokens"] = json!(max);
        }
        if let Some(tools) = &options.tools {
            // No `function` wrapper: the tool definition *is* the item.
            let defs: Vec<_> = tools
                .iter()
                .map(|t| {
                    let mut def = json!({
                        "type": "function",
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    });
                    if compat.supports_strict_mode {
                        def["strict"] = json!(true);
                    }
                    def
                })
                .collect();
            body["tools"] = json!(defs);
        }
        if options.response_format == crate::ResponseFormat::Json {
            body["text"] = json!({"format": {"type": "json_object"}});
        }
        body
    }
}

#[async_trait]
impl LLMProvider for OpenAIResponsesProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(self.stream_capacity);
        let model = self.model.clone();
        let api_key = self.api_key.clone();
        let client = self.client.clone().ok_or_else(|| {
            SFError::Agent("OpenAIResponsesProvider: no HttpClient configured".into())
        })?;
        let headers = crate::origin::outbound_headers(options);
        let body = self.build_request_body(messages, options);
        let abort_signal = options.abort_signal.clone();

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

        let url = responses_url(&model.base_url);
        let raw_logger = options.raw_logger.clone();

        tokio::spawn(async move {
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
                        transport: "openai-responses".into(),
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
                api: "openai-responses".into(),
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
            let start = std::time::Instant::now();

            if producer
                .push(AssistantMessageEvent::Start {
                    timestamp: chrono::Utc::now(),
                })
                .await
                .is_err()
            {
                return;
            }

            let mut req = cog_core::HttpRequest::post(&url)
                .header("Authorization", format!("Bearer {}", api_key))
                .header("Content-Type", cog_core::CONTENT_TYPE_JSON)
                .json(&body)
                .map_err(|e| SFError::Agent(format!("JSON serialization failed: {}", e)))
                .unwrap()
                .timeout(600);
            for (key, value) in &headers {
                req = req.header(key, value);
            }

            tracing::info!(
                provider = "openai-responses",
                url = %url,
                model = %model.id,
                "OpenAIResponsesProvider executing stream request"
            );
            let http_response = match client.execute_stream(req).await {
                Ok(r) => {
                    tracing::info!(
                        provider = "openai-responses",
                        status = r.status,
                        "OpenAIResponsesProvider received stream response"
                    );
                    r
                }
                Err(e) => {
                    tracing::warn!(provider = "openai-responses", error = %e, "stream request failed");
                    response.stop_reason = StopReason::Error;
                    response.upstream_failure = Some(UpstreamFailure::Transport);
                    response.error_message = Some(format!("HTTP error: {}", e));
                    let _ = producer
                        .push(AssistantMessageEvent::Error {
                            reason: StopReason::Error,
                            error: Message::assistant_text(
                                response.error_message.clone().unwrap_or_default(),
                            ),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                    producer.end(response);
                    return;
                }
            };

            if !http_response.is_success() {
                let status = http_response.status;
                response.retry_after_secs = cog_core::retry_after_hint(&http_response.headers);
                let declared_cause = cog_core::declared_upstream_cause(&http_response.headers);
                let text = http_response.drain_text().await;
                response.stop_reason = StopReason::Error;
                response.upstream_failure =
                    Some(declared_cause.unwrap_or_else(|| UpstreamFailure::from_status(status)));
                response.error_message = Some(format!("API error (HTTP {status}): {text}"));
                let _ = producer
                    .push(AssistantMessageEvent::Error {
                        reason: StopReason::Error,
                        error: Message::assistant_text(
                            response.error_message.clone().unwrap_or_default(),
                        ),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(response);
                return;
            }

            let bytes_stream = http_response.stream;
            let lines = bytes_stream
                .map_err(std::io::Error::other)
                .into_async_read();
            let lines = futures::io::BufReader::new(lines).lines();
            futures::pin_mut!(lines);

            let mut text_pos: Option<usize> = None;
            let mut thinking_pos: Option<usize> = None;
            let mut tool_accs: BTreeMap<String, ToolAcc> = BTreeMap::new();

            let total_timeout = crate::provider::STREAM_TOTAL_TIMEOUT;
            let mut event_timeout = total_timeout;
            let stream_start = std::time::Instant::now();
            let mut first_event_logged = false;

            'stream: loop {
                let elapsed = stream_start.elapsed();
                if elapsed >= total_timeout {
                    response.stop_reason = StopReason::Error;
                    response.error_message = Some(format!(
                        "SSE stream exceeded total duration limit of {}s",
                        total_timeout.as_secs()
                    ));
                    break;
                }
                let remaining = total_timeout - elapsed;
                let timeout = event_timeout.min(remaining).max(Duration::from_secs(1));

                let line = match tokio::time::timeout(timeout, lines.try_next()).await {
                    Ok(Ok(Some(line))) => line,
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => {
                        response.stop_reason = StopReason::Error;
                        response.error_message = Some(format!("SSE stream read error: {}", e));
                        break;
                    }
                    Err(_) => {
                        response.stop_reason = StopReason::Error;
                        response.error_message = Some(format!(
                            "SSE stream timed out after {}s with no events",
                            timeout.as_secs()
                        ));
                        break;
                    }
                };

                if !first_event_logged {
                    first_event_logged = true;
                    event_timeout = crate::provider::STREAM_EVENT_TIMEOUT;
                }

                let line = line.trim();
                // The `event:` line names the same type the payload carries;
                // only the payload is read so the two can never disagree.
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim_start();
                if data.is_empty() || data == "[DONE]" {
                    if data == "[DONE]" {
                        break;
                    }
                    continue;
                }
                let event: serde_json::Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let kind = event.get("type").and_then(|t| t.as_str()).unwrap_or("");

                if response.response_id.is_none() {
                    if let Some(id) = event.pointer("/response/id").and_then(|v| v.as_str()) {
                        response.response_id = Some(id.to_string());
                    }
                }

                match kind {
                    "response.output_text.delta" => {
                        let delta = event.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                        if !delta.is_empty() {
                            if let Some(pos) = thinking_pos.take() {
                                let block = response.content[pos].clone();
                                finish_block(&block, pos, &producer).await;
                            }
                            let idx = match text_pos {
                                Some(pos) => pos,
                                None => {
                                    let pos = response.content.len();
                                    response.content.push(ContentBlock::text(""));
                                    text_pos = Some(pos);
                                    if producer
                                        .push(AssistantMessageEvent::TextStart {
                                            content_index: pos,
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break 'stream;
                                    }
                                    pos
                                }
                            };
                            if let ContentBlock::Text { text, .. } = &mut response.content[idx] {
                                text.push_str(delta);
                            }
                            if producer
                                .push(AssistantMessageEvent::TextDelta {
                                    content_index: idx,
                                    delta: delta.to_string(),
                                    timestamp: chrono::Utc::now(),
                                })
                                .await
                                .is_err()
                            {
                                break 'stream;
                            }
                        }
                    }
                    "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                        let delta = event.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                        if !delta.is_empty() {
                            let idx = match thinking_pos {
                                Some(pos) => pos,
                                None => {
                                    let pos = response.content.len();
                                    response.content.push(ContentBlock::thinking(String::new()));
                                    thinking_pos = Some(pos);
                                    if producer
                                        .push(AssistantMessageEvent::ThinkingStart {
                                            content_index: pos,
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break 'stream;
                                    }
                                    pos
                                }
                            };
                            if let ContentBlock::Thinking { thinking, .. } =
                                &mut response.content[idx]
                            {
                                thinking.push_str(delta);
                            }
                            if producer
                                .push(AssistantMessageEvent::ThinkingDelta {
                                    content_index: idx,
                                    delta: delta.to_string(),
                                    timestamp: chrono::Utc::now(),
                                })
                                .await
                                .is_err()
                            {
                                break 'stream;
                            }
                        }
                    }
                    "response.output_item.added" => {
                        let item = event.get("item").cloned().unwrap_or_default();
                        if item.get("type").and_then(|t| t.as_str()) != Some("function_call") {
                            continue;
                        }
                        let item_id = item
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        // The id the caller must echo back on the result is
                        // `call_id`; `id` names the item inside the reply.
                        let call_id = item
                            .get("call_id")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                            .unwrap_or_else(|| {
                                if item_id.is_empty() {
                                    "pending".to_string()
                                } else {
                                    item_id.clone()
                                }
                            });
                        let name = item
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("pending")
                            .to_string();
                        if let Some(pos) = text_pos.take() {
                            let block = response.content[pos].clone();
                            finish_block(&block, pos, &producer).await;
                        }
                        if let Some(pos) = thinking_pos.take() {
                            let block = response.content[pos].clone();
                            finish_block(&block, pos, &producer).await;
                        }
                        let content_pos = response.content.len();
                        response.content.push(ContentBlock::tool_call(
                            call_id.clone(),
                            name.clone(),
                            serde_json::Value::Object(Default::default()),
                        ));
                        let key = if item_id.is_empty() {
                            call_id.clone()
                        } else {
                            item_id
                        };
                        tool_accs.insert(
                            key,
                            ToolAcc {
                                id: call_id,
                                name,
                                args: String::new(),
                                content_pos,
                            },
                        );
                        if producer
                            .push(AssistantMessageEvent::ToolCallStart {
                                content_index: content_pos,
                                timestamp: chrono::Utc::now(),
                            })
                            .await
                            .is_err()
                        {
                            break 'stream;
                        }
                    }
                    "response.function_call_arguments.delta" => {
                        let item_id = event.get("item_id").and_then(|v| v.as_str()).unwrap_or("");
                        let delta = event.get("delta").and_then(|d| d.as_str()).unwrap_or("");
                        if let Some(acc) = tool_accs.get_mut(item_id) {
                            acc.args.push_str(delta);
                            if producer
                                .push(AssistantMessageEvent::ToolCallDelta {
                                    content_index: acc.content_pos,
                                    delta: delta.to_string(),
                                    timestamp: chrono::Utc::now(),
                                })
                                .await
                                .is_err()
                            {
                                break 'stream;
                            }
                        }
                    }
                    "response.function_call_arguments.done" => {
                        let item_id = event.get("item_id").and_then(|v| v.as_str()).unwrap_or("");
                        if let (Some(acc), Some(args)) = (
                            tool_accs.get_mut(item_id),
                            event.get("arguments").and_then(|v| v.as_str()),
                        ) {
                            // The done event carries the whole string; take it
                            // verbatim rather than trusting the deltas to have
                            // arrived complete.
                            acc.args = args.to_string();
                        }
                    }
                    "response.output_item.done" => {
                        let item = event.get("item").cloned().unwrap_or_default();
                        if item.get("type").and_then(|t| t.as_str()) != Some("function_call") {
                            continue;
                        }
                        let item_id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        if let Some(acc) = tool_accs.get_mut(item_id) {
                            if let Some(args) = item.get("arguments").and_then(|v| v.as_str()) {
                                if !args.is_empty() {
                                    acc.args = args.to_string();
                                }
                            }
                        }
                    }
                    "response.completed" | "response.incomplete" => {
                        if let Some(usage) = event.pointer("/response/usage") {
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
                            response.usage.cache_read = usage
                                .pointer("/input_tokens_details/cached_tokens")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                                as u32;
                            response.usage.total_tokens = usage
                                .get("total_tokens")
                                .and_then(|v| v.as_u64())
                                .map(|t| t as u32)
                                .unwrap_or(response.usage.input + response.usage.output);
                        }
                        if kind == "response.incomplete" {
                            let reason = event
                                .pointer("/response/incomplete_details/reason")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            response.stop_reason = if reason == "content_filter" {
                                response.error_message =
                                    Some("Provider stopped on content_filter".into());
                                StopReason::Error
                            } else {
                                StopReason::Length
                            };
                        }
                        break;
                    }
                    "response.failed" | "error" => {
                        response.stop_reason = StopReason::Error;
                        response.error_message = Some(
                            event
                                .pointer("/response/error/message")
                                .or_else(|| event.pointer("/error/message"))
                                .and_then(|v| v.as_str())
                                .or_else(|| event.pointer("/message").and_then(|v| v.as_str()))
                                .unwrap_or("upstream reported a failed response")
                                .to_string(),
                        );
                        break;
                    }
                    _ => {}
                }

                if let Some(ref signal) = abort_signal {
                    if signal.load(std::sync::atomic::Ordering::Relaxed) {
                        response.stop_reason = StopReason::Aborted;
                        break;
                    }
                }
            }

            // Materialize every streamed call and close the still-open block.
            for acc in tool_accs.values() {
                if let Some(block) = response.content.get_mut(acc.content_pos) {
                    finalize_tool_call(block, &acc.args);
                    if let ContentBlock::ToolCall {
                        ref mut id,
                        ref mut name,
                        ..
                    } = block
                    {
                        *id = acc.id.clone();
                        *name = acc.name.clone();
                    }
                    let block = block.clone();
                    finish_block(&block, acc.content_pos, &producer).await;
                }
            }
            if let Some(pos) = text_pos.take() {
                let block = response.content[pos].clone();
                finish_block(&block, pos, &producer).await;
            }
            if let Some(pos) = thinking_pos.take() {
                let block = response.content[pos].clone();
                finish_block(&block, pos, &producer).await;
            }

            // A turn that called tools is a tool-use turn even when the
            // upstream's completion event does not say so.
            if response.stop_reason == StopReason::Stop
                && response.content.iter().any(|b| b.is_tool_call())
            {
                response.stop_reason = StopReason::ToolUse;
            }

            if response.usage.total_tokens > 0
                || response.usage.input > 0
                || response.usage.output > 0
            {
                response.usage.cost = crate::model::calculate_cost(&model, &response.usage);
            }

            tracing::info!(
                provider = "openai-responses",
                model = %model.id,
                elapsed_ms = %start.elapsed().as_millis(),
                tokens_in = %response.usage.input,
                tokens_out = %response.usage.output,
                "OpenAIResponsesProvider stream finished"
            );

            match response.stop_reason {
                StopReason::Aborted => {
                    let _ = producer
                        .push(AssistantMessageEvent::Error {
                            reason: StopReason::Aborted,
                            error: Message::assistant_text("Request was aborted"),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }
                StopReason::Error => {
                    let _ = producer
                        .push(AssistantMessageEvent::Error {
                            reason: StopReason::Error,
                            error: Message::assistant_text(
                                response.error_message.clone().unwrap_or_default(),
                            ),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }
                _ => {
                    let _ = producer
                        .push(AssistantMessageEvent::Done {
                            reason: response.stop_reason,
                            message: Message::assistant(response.content.clone()),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }
            }

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
                        transport: "openai-responses".into(),
                        format: Some("json".into()),
                        raw: match serde_json::to_value(&response) {
                            Ok(v) => v,
                            Err(_) => serde_json::json!({
                                "stop_reason": format!("{:?}", response.stop_reason)
                            }),
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
        let chat_timeout = crate::provider::CHAT_CONSUME_TIMEOUT;
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
                    "OpenAIResponsesProvider chat() timed out after {}s waiting for stream",
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
                tracing::warn!("OpenAI responses health check failed: {}", e);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ApiType, ModelCost, Provider};
    use std::collections::HashMap;

    fn test_model() -> Model {
        Model {
            id: "gpt-6".into(),
            name: "gpt-6".into(),
            api: ApiType::OpenAIResponses,
            provider: Provider::OpenAI,
            base_url: "https://api.openai.com/v1".into(),
            context_window: 200_000,
            max_tokens: 8192,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: true,
            cost: ModelCost::default(),
            headers: HashMap::new(),
        }
    }

    #[derive(Debug)]
    struct SseClient {
        body: Vec<u8>,
        seen_url: std::sync::Mutex<Option<String>>,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for SseClient {
        async fn execute(&self, _req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            unreachable!("stream path only")
        }
        async fn execute_stream(
            &self,
            req: cog_core::HttpRequest,
        ) -> SFResult<cog_core::HttpStreamResponse> {
            *self.seen_url.lock().unwrap() = Some(req.url.clone());
            let chunk: Result<bytes::Bytes, SFError> = Ok(bytes::Bytes::from(self.body.clone()));
            let stream: cog_core::HttpBodyStream = Box::pin(futures::stream::iter(vec![chunk]));
            Ok(cog_core::HttpStreamResponse {
                status: 200,
                headers: std::collections::HashMap::new(),
                stream,
            })
        }
    }

    /// The base names the version segment and this face appends one element to
    /// it. A base that stops short composes an address the vendor does not
    /// serve, and the 404 reads like a bad key.
    #[test]
    fn the_endpoint_is_the_base_plus_one_segment() {
        assert_eq!(
            responses_url("https://api.openai.com/v1"),
            "https://api.openai.com/v1/responses"
        );
        assert_eq!(
            responses_url("https://api.openai.com/v1/"),
            "https://api.openai.com/v1/responses"
        );
    }

    /// The request body is the other wire shape: input items, a function call
    /// as its own item, a result named by `call_id`, and one name for the cap.
    /// A body built in the chat shape sent here is rejected by every vendor on
    /// this face, and the rejection says nothing about which field was wrong.
    #[test]
    fn the_request_body_speaks_the_responses_shape() {
        let provider = OpenAIResponsesProvider::new(test_model(), "key");
        let messages = vec![
            Message::user("what is the weather"),
            Message::assistant(vec![ContentBlock::tool_call(
                "call_7",
                "get_weather",
                json!({"city": "Paris"}),
            )]),
            Message::tool_result_text("call_7", "get_weather", "sunny"),
        ];
        let options = ChatOptions {
            max_tokens: Some(64),
            ..Default::default()
        };
        let body = provider.build_request_body(&messages, &options);

        assert!(body.get("messages").is_none(), "{body}");
        assert_eq!(body["max_output_tokens"], json!(64));
        let items = body["input"].as_array().expect("input items");
        assert_eq!(items[0]["role"], json!("user"));
        assert_eq!(items[1]["type"], json!("function_call"));
        assert_eq!(items[1]["call_id"], json!("call_7"));
        assert_eq!(items[2]["type"], json!("function_call_output"));
        assert_eq!(items[2]["call_id"], json!("call_7"));
        assert_eq!(items[2]["output"], json!("sunny"));
    }

    /// A streamed turn on this face arrives as items: text deltas, a call that
    /// announces itself with its `call_id`, argument deltas keyed by the item,
    /// and one completion event that carries the usage. Every one of those
    /// names differs from the chat shape, so a parser written for the other
    /// face returns an empty turn and no error.
    #[tokio::test]
    async fn a_streamed_turn_becomes_text_a_call_and_its_usage() {
        let mut sse = String::new();
        let mut evt = |obj: serde_json::Value| sse.push_str(&format!("data: {obj}\n\n"));
        evt(json!({"type": "response.created", "response": {"id": "resp_1"}}));
        evt(json!({"type": "response.output_text.delta", "delta": "Looking "}));
        evt(json!({"type": "response.output_text.delta", "delta": "it up."}));
        evt(json!({"type": "response.output_text.done", "text": "Looking it up."}));
        evt(json!({"type": "response.output_item.added", "item": {
            "type": "function_call", "id": "item_1", "call_id": "call_9",
            "name": "get_weather", "arguments": ""
        }}));
        evt(
            json!({"type": "response.function_call_arguments.delta", "item_id": "item_1",
                   "delta": "{\"city\": "}),
        );
        evt(
            json!({"type": "response.function_call_arguments.delta", "item_id": "item_1",
                   "delta": "\"Paris\"}"}),
        );
        evt(
            json!({"type": "response.function_call_arguments.done", "item_id": "item_1",
                   "arguments": "{\"city\": \"Paris\"}"}),
        );
        evt(json!({"type": "response.output_item.done", "item": {
            "type": "function_call", "id": "item_1", "call_id": "call_9",
            "name": "get_weather", "arguments": "{\"city\": \"Paris\"}"
        }}));
        evt(
            json!({"type": "response.completed", "response": {"id": "resp_1", "usage": {
                "input_tokens": 120, "output_tokens": 30, "total_tokens": 150,
                "input_tokens_details": {"cached_tokens": 100}
            }}}),
        );

        let client = Arc::new(SseClient {
            body: sse.into_bytes(),
            seen_url: std::sync::Mutex::new(None),
        });
        let provider =
            OpenAIResponsesProvider::new(test_model(), "key").with_client(client.clone());
        let mut stream = provider
            .chat_stream(&[Message::user("weather?")], &ChatOptions::default())
            .await
            .expect("stream starts");
        while stream.next().await.is_some() {}
        let response = stream.result().await;

        assert_eq!(
            client.seen_url.lock().unwrap().as_deref(),
            Some("https://api.openai.com/v1/responses"),
            "the stream must go to this face's endpoint"
        );
        assert_eq!(response.response_id.as_deref(), Some("resp_1"));
        assert_eq!(response.stop_reason, StopReason::ToolUse, "{response:?}");
        let text: String = response
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect();
        assert_eq!(text, "Looking it up.");
        let calls: Vec<_> = response
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolCall {
                    id,
                    name,
                    arguments,
                    ..
                } => Some((id.clone(), name.clone(), arguments.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            calls,
            vec![(
                "call_9".to_string(),
                "get_weather".to_string(),
                json!({"city": "Paris"})
            )],
            "the call_id is what the result will have to name: {response:?}"
        );
        assert_eq!(response.usage.input, 120);
        assert_eq!(response.usage.output, 30);
        assert_eq!(response.usage.total_tokens, 150);
        assert_eq!(response.usage.cache_read, 100);
    }
}
