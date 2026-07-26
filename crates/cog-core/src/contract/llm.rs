use crate::{
    AssistantMessageEvent, ContentBlock, EventStream, EventStreamProducer, Message, RawLogger,
    SFError, SFResult, StopReason, ToolDefinition,
};
use async_trait::async_trait;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub type AssistantMessageEventStream = EventStream<AssistantMessageEvent, ChatResponse>;
pub type AssistantMessageEventProducer = EventStreamProducer<AssistantMessageEvent, ChatResponse>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    #[default]
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheRetention {
    #[default]
    None,
    Short,
    Long,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Transport {
    #[default]
    Auto,
    Sse,
    Websocket,
}

#[derive(Debug, Clone, Default)]
pub struct LlmModelInfo {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub base_url: String,
}

#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct ChatOptions {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub tools: Option<Vec<ToolDefinition>>,
    pub response_format: ResponseFormat,
    pub reasoning: Option<ThinkingLevel>,
    pub api_key: Option<String>,
    pub session_id: Option<String>,
    pub headers: HashMap<String, String>,
    pub on_payload: Option<
        Arc<dyn Fn(serde_json::Value, &LlmModelInfo) -> SFResult<serde_json::Value> + Send + Sync>,
    >,
    pub transport: Transport,
    pub cache_retention: CacheRetention,
    pub max_retry_delay_ms: Option<u64>,
    pub metadata: HashMap<String, String>,
    pub abort_signal: Option<Arc<AtomicBool>>,
    pub raw_logger: Option<Arc<dyn RawLogger>>,
}

impl std::fmt::Debug for ChatOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatOptions")
            .field("model", &self.model)
            .field("temperature", &self.temperature)
            .field("max_tokens", &self.max_tokens)
            .field("tools", &self.tools.as_ref().map(|t| t.len()))
            .field("response_format", &self.response_format)
            .field("reasoning", &self.reasoning)
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("session_id", &self.session_id)
            .field("headers", &self.headers)
            .field("on_payload", &self.on_payload.as_ref().map(|_| "[closure]"))
            .field("transport", &self.transport)
            .field("cache_retention", &self.cache_retention)
            .field("max_retry_delay_ms", &self.max_retry_delay_ms)
            .field("metadata", &self.metadata)
            .field("abort_signal", &self.abort_signal)
            .field("raw_logger", &self.raw_logger.as_ref().map(|_| "[logger]"))
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum ResponseFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, Default)]
pub struct CompleteOptions {
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ChatResponse {
    pub content: Vec<ContentBlock>,
    pub api: String,
    pub provider: String,
    pub model: String,
    pub response_id: Option<String>,
    pub usage: Usage,
    pub stop_reason: StopReason,
    pub error_message: Option<String>,
    pub timestamp: DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Usage {
    pub input: u32,
    pub output: u32,
    pub cache_read: u32,
    pub cache_write: u32,
    pub total_tokens: u32,
    pub cost: crate::message::Cost,
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn chat_stream(
        &self,
        messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream>;

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream>;

    async fn chat(&self, messages: &[Message], options: &ChatOptions) -> SFResult<ChatResponse>;

    async fn health_check(&self) -> bool;
}

/// Execute an LLM request that expects a structured JSON response matching
/// the given JSON Schema.
/// The function injects a system prompt instructing the model to produce valid
/// JSON conforming to the schema, validates the response, and returns a parsed
/// value.
/// # Example
/// ```
/// use serde::Deserialize;
/// use schemars::JsonSchema;
/// use cog_core::{LlmClient, SFResult, Message, ChatOptions, execute_structured};
/// #[derive(Debug, Deserialize, JsonSchema)]
/// struct DecomposeResult {
///     tasks: Vec<String>,
/// }
/// # async fn example(provider: &dyn LlmClient) -> SFResult<()> {
/// let result: DecomposeResult = execute_structured(
///     provider,
///     &[Message::user("Break down: deploy a service")],
///     &ChatOptions::default(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn execute_structured<T>(
    provider: &dyn LlmClient,
    messages: &[Message],
    options: &ChatOptions,
) -> SFResult<T>
where
    T: schemars::JsonSchema + serde::de::DeserializeOwned,
{
    let root_schema = schemars::gen::SchemaGenerator::default().root_schema_for::<T>();
    let schema_json = serde_json::to_string_pretty(&root_schema).map_err(SFError::Serialization)?;

    let system_msg = Message::system(format!(
        "Respond with valid JSON conforming to this JSON Schema. \
         Do not output markdown fences or explanatory text.\n\n{schema_json}"
    ));

    let mut msgs = vec![system_msg];
    msgs.extend_from_slice(messages);

    let mut opts = options.clone();
    opts.response_format = ResponseFormat::Json;

    let response = provider.chat(&msgs, &opts).await?;
    let text = response
        .content
        .iter()
        .filter_map(|b| b.as_text())
        .collect::<String>();

    let value: serde_json::Value = serde_json::from_str(&text).map_err(SFError::Serialization)?;

    let schema_value = serde_json::to_value(&root_schema).map_err(SFError::Serialization)?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|e| SFError::Validation(format!("invalid schema: {e}")))?;

    let errors: Vec<String> = validator
        .iter_errors(&value)
        .map(|e| e.to_string())
        .collect();
    if !errors.is_empty() {
        return Err(SFError::Validation(format!(
            "JSON schema validation failed: {}",
            errors.join("; ")
        )));
    }

    let parsed: T = serde_json::from_value(value).map_err(SFError::Serialization)?;
    Ok(parsed)
}
