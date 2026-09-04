use async_trait::async_trait;
use cog_core::{AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, StopReason};
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt};
use serde_json::json;
use std::sync::Arc;

use super::LLMProvider;
use crate::{
    model::Model, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, Usage,
};

pub struct OllamaProvider {
    client: Option<Arc<dyn cog_core::HttpClient>>,
    model: Model,
    stream_capacity: usize,
}

impl OllamaProvider {
    pub fn new(model: Model) -> Self {
        Self {
            client: None,
            model,
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
        let msgs: Vec<serde_json::Value> = messages
            .iter()
            .map(|msg| match msg {
                Message::System { content, .. } => json!({"role": "system", "content": content}),
                Message::User { content, .. } => {
                    let (text, images) = super::media::ollama_content(content, &self.model);
                    let mut m = json!({"role": "user", "content": text});
                    if !images.is_empty() {
                        m["images"] = json!(images);
                    }
                    m
                }
                Message::Assistant { content, .. } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    json!({"role": "assistant", "content": text})
                }
                Message::ToolResult { content, .. } => {
                    let text: String = content
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    json!({"role": "user", "content": text})
                }
            })
            .collect();

        let mut body = json!({
            "model": options.model.as_ref().unwrap_or(&self.model.id),
            "messages": msgs,
            "stream": true,
        });

        let mut opts = serde_json::Map::new();
        if let Some(temp) = options.temperature {
            opts.insert("temperature".into(), json!(temp));
        }
        if let Some(max) = options.max_tokens {
            opts.insert("num_predict".into(), json!(max as i64));
        }
        if !opts.is_empty() {
            body["options"] = opts.into();
        }

        if let Some(tools) = &options.tools {
            let tool_defs: Vec<_> = tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "function": {
                            "name": t.name,
                            "description": t.description,
                            "parameters": t.parameters,
                        }
                    })
                })
                .collect();
            body["tools"] = json!(tool_defs);
        }

        body
    }
}

#[async_trait]
impl LLMProvider for OllamaProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(self.stream_capacity);
        let model = self.model.clone();
        let client = self
            .client
            .clone()
            .ok_or_else(|| SFError::Agent("OllamaProvider: no HttpClient configured".into()))?;
        let body = self.build_request_body(messages, options);
        let abort_signal = options.abort_signal.clone();

        let url = format!("{}/api/chat", model.base_url);

        tokio::spawn(async move {
            let mut response = ChatResponse {
                content: Vec::new(),
                api: "ollama-chat".into(),
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

            let req = cog_core::HttpRequest::post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .map_err(|e| SFError::Agent(format!("JSON serialization failed: {}", e)))
                .unwrap()
                .timeout(120);

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

            while let Ok(Some(line)) = lines.try_next().await {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let json: serde_json::Value = match serde_json::from_str(line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Extract usage from final chunk
                if let Some(prompt_eval) = json.get("prompt_eval_count").and_then(|v| v.as_u64()) {
                    response.usage.input = prompt_eval as u32;
                }
                if let Some(eval_count) = json.get("eval_count").and_then(|v| v.as_u64()) {
                    response.usage.output = eval_count as u32;
                }
                if response.usage.input > 0 || response.usage.output > 0 {
                    response.usage.total_tokens = response.usage.input + response.usage.output;
                }

                // Check if done
                let done = json.get("done").and_then(|v| v.as_bool()).unwrap_or(false);

                if let Some(message) = json.get("message") {
                    // Text content
                    if let Some(content) = message.get("content").and_then(|v| v.as_str()) {
                        if !content.is_empty() {
                            let idx = response.content.len();
                            if current_block.as_ref().is_none_or(|b| !b.is_text()) {
                                if let Some(ref block) = current_block {
                                    if let Some(text) = block.as_text() {
                                        if producer
                                            .push(AssistantMessageEvent::TextEnd {
                                                content_index: idx.saturating_sub(1),
                                                content: text.to_string(),
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
                                    }
                                }
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

                    // Tool calls (usually appear in final message)
                    if done {
                        if let Some(tool_calls) =
                            message.get("tool_calls").and_then(|v| v.as_array())
                        {
                            // Finish any current text block first
                            if let Some(ref block) = current_block {
                                let idx = response.content.len().saturating_sub(1);
                                if block.is_text()
                                    && producer
                                        .push(AssistantMessageEvent::TextEnd {
                                            content_index: idx,
                                            content: block.as_text().unwrap_or("").to_string(),
                                            partial: Message::assistant(response.content.clone()),
                                            timestamp: chrono::Utc::now(),
                                        })
                                        .await
                                        .is_err()
                                {
                                    break;
                                }
                                current_block = None;
                            }

                            for tool_call in tool_calls {
                                let name = tool_call
                                    .get("function")
                                    .and_then(|f| f.get("name"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let args = tool_call
                                    .get("function")
                                    .and_then(|f| f.get("arguments"))
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        serde_json::Value::Object(Default::default())
                                    });
                                let id = format!(
                                    "ollama_{}_{}",
                                    name,
                                    chrono::Utc::now().timestamp_millis()
                                );

                                let idx = response.content.len();
                                let tool_block = ContentBlock::tool_call(id, name, args);
                                response.content.push(tool_block.clone());
                                if producer
                                    .push(AssistantMessageEvent::ToolCallStart {
                                        content_index: idx,
                                        partial: Message::assistant(response.content.clone()),
                                        timestamp: chrono::Utc::now(),
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                if producer
                                    .push(AssistantMessageEvent::ToolCallEnd {
                                        content_index: idx,
                                        tool_call: cog_core::ToolCall {
                                            id: if let ContentBlock::ToolCall { id, .. } =
                                                &tool_block
                                            {
                                                id.clone()
                                            } else {
                                                String::new()
                                            },
                                            name: if let ContentBlock::ToolCall { name, .. } =
                                                &tool_block
                                            {
                                                name.clone()
                                            } else {
                                                String::new()
                                            },
                                            arguments: if let ContentBlock::ToolCall {
                                                arguments,
                                                ..
                                            } = &tool_block
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
                                    break;
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

                if done {
                    break;
                }
            }

            // Finish any remaining block
            if let Some(ref block) = current_block {
                let idx = response.content.len().saturating_sub(1);
                if block.is_text()
                    && producer
                        .push(AssistantMessageEvent::TextEnd {
                            content_index: idx,
                            content: block.as_text().unwrap_or("").to_string(),
                            partial: Message::assistant(response.content.clone()),
                            timestamp: chrono::Utc::now(),
                        })
                        .await
                        .is_err()
                {
                    return;
                }
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
        let client = match self.client.as_ref() {
            Some(c) => c,
            None => return false,
        };
        let req =
            cog_core::HttpRequest::get(format!("{}/api/tags", self.model.base_url)).timeout(10);
        match client.execute(req).await {
            Ok(r) => r.is_success(),
            Err(e) => {
                tracing::warn!("Ollama health check failed: {}", e);
                false
            }
        }
    }
}
