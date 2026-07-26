use serde_json::json;
use std::collections::HashMap;

use crate::{ChatOptions, ThinkingLevel};

/// Compatibility settings for OpenAI-compatible completions APIs.
/// Aligns with pi-ai's OpenAICompletionsCompat.
#[derive(Debug, Clone, Default)]
pub struct OpenAICompat {
    /// Whether the provider supports the `store` field.
    pub supports_store: bool,
    /// Whether the provider supports the `developer` role (vs `system`).
    pub supports_developer_role: bool,
    /// Whether the provider supports `reasoning_effort`.
    pub supports_reasoning_effort: bool,
    /// Mapping from reasoning levels to provider-specific values.
    pub reasoning_effort_map: HashMap<ThinkingLevel, String>,
    /// Whether the provider supports `stream_options: { include_usage: true }`.
    pub supports_usage_in_streaming: bool,
    /// Which field to use for max tokens.
    pub max_tokens_field: MaxTokensField,
    /// Whether tool results require the `name` field.
    pub requires_tool_result_name: bool,
    /// Whether a user message after tool results requires an assistant message in between.
    pub requires_assistant_after_tool_result: bool,
    /// Whether the provider only accepts temperature=1.0 (e.g. Kimi k2.6).
    pub requires_temperature_one: bool,
    /// Whether thinking blocks must be converted to text blocks with `<thinking>` delimiters.
    pub requires_thinking_as_text: bool,
    /// Format for reasoning/thinking parameter.
    pub thinking_format: ThinkingFormat,
    /// OpenRouter-specific routing preferences.
    pub openrouter_routing: Option<OpenRouterRouting>,
    /// Vercel AI Gateway routing preferences.
    pub vercel_gateway_routing: Option<VercelGatewayRouting>,
    /// Whether z.ai supports top-level `tool_stream: true`.
    pub zai_tool_stream: bool,
    /// Whether the provider supports the `strict` field in tool definitions.
    pub supports_strict_mode: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxTokensField {
    #[default]
    MaxCompletionTokens,
    MaxTokens,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ThinkingFormat {
    #[default]
    OpenAI,
    OpenRouter,
    Zai,
    Qwen,
    QwenChatTemplate,
}

/// OpenRouter provider routing preferences.
#[derive(Debug, Clone, Default)]
pub struct OpenRouterRouting {
    pub allow_fallbacks: Option<bool>,
    pub require_parameters: Option<bool>,
    pub data_collection: Option<DataCollection>,
    pub order: Option<Vec<String>>,
    pub only: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataCollection {
    Allow,
    Deny,
}

/// Vercel AI Gateway routing preferences.
#[derive(Debug, Clone, Default)]
pub struct VercelGatewayRouting {
    pub only: Option<Vec<String>>,
    pub order: Option<Vec<String>>,
}

/// Auto-detect compatibility settings from a provider's base URL.
pub fn detect_compat(base_url: &str) -> OpenAICompat {
    let lower = base_url.to_lowercase();

    if lower.contains("openrouter.ai") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: true,
            thinking_format: ThinkingFormat::OpenRouter,
            supports_usage_in_streaming: true,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: true,
            ..Default::default()
        };
    }

    if lower.contains("gateway.ai.cloudflare.com") || lower.contains("ai-gateway") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: true,
            max_tokens_field: MaxTokensField::MaxCompletionTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: true,
            ..Default::default()
        };
    }

    if lower.contains("api.groq.com") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: false,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("api.cerebras.ai") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: false,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("api.x.ai") || lower.contains("api.xai.com") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: true,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("api.mistral.ai") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: false,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("api.minimax.chat") || lower.contains("minimax") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: false,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("api.kimi.com") || lower.contains("kimi") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: false,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            requires_temperature_one: true,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("githubcopilot") || lower.contains("copilot") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: true,
            max_tokens_field: MaxTokensField::MaxCompletionTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    if lower.contains("localhost:11434") || lower.contains("ollama") {
        return OpenAICompat {
            supports_store: false,
            supports_developer_role: false,
            supports_reasoning_effort: false,
            thinking_format: ThinkingFormat::OpenAI,
            supports_usage_in_streaming: false,
            max_tokens_field: MaxTokensField::MaxTokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            supports_strict_mode: false,
            ..Default::default()
        };
    }

    // Default: assume official OpenAI or OpenAI-compatible with latest features
    OpenAICompat {
        supports_store: true,
        supports_developer_role: true,
        supports_reasoning_effort: true,
        thinking_format: ThinkingFormat::OpenAI,
        supports_usage_in_streaming: true,
        max_tokens_field: MaxTokensField::MaxCompletionTokens,
        requires_tool_result_name: false,
        requires_assistant_after_tool_result: false,
        supports_strict_mode: true,
        ..Default::default()
    }
}

/// Extract compat overrides from ChatOptions metadata.
/// Keys are prefixed with `compat_` in metadata.
pub fn compat_from_options(options: &ChatOptions, base_url: &str) -> OpenAICompat {
    let mut compat = detect_compat(base_url);

    let meta = &options.metadata;
    if let Some(v) = meta.get("compat_supports_store") {
        compat.supports_store = v.parse().unwrap_or(compat.supports_store);
    }
    if let Some(v) = meta.get("compat_supports_developer_role") {
        compat.supports_developer_role = v.parse().unwrap_or(compat.supports_developer_role);
    }
    if let Some(v) = meta.get("compat_supports_reasoning_effort") {
        compat.supports_reasoning_effort = v.parse().unwrap_or(compat.supports_reasoning_effort);
    }
    if let Some(v) = meta.get("compat_supports_usage_in_streaming") {
        compat.supports_usage_in_streaming =
            v.parse().unwrap_or(compat.supports_usage_in_streaming);
    }
    if let Some(v) = meta.get("compat_requires_tool_result_name") {
        compat.requires_tool_result_name = v.parse().unwrap_or(compat.requires_tool_result_name);
    }
    if let Some(v) = meta.get("compat_requires_assistant_after_tool_result") {
        compat.requires_assistant_after_tool_result = v
            .parse()
            .unwrap_or(compat.requires_assistant_after_tool_result);
    }
    if let Some(v) = meta.get("compat_requires_thinking_as_text") {
        compat.requires_thinking_as_text = v.parse().unwrap_or(compat.requires_thinking_as_text);
    }
    if let Some(v) = meta.get("compat_supports_strict_mode") {
        compat.supports_strict_mode = v.parse().unwrap_or(compat.supports_strict_mode);
    }
    if let Some(v) = meta.get("compat_max_tokens_field") {
        compat.max_tokens_field = match v.as_str() {
            "max_tokens" => MaxTokensField::MaxTokens,
            _ => MaxTokensField::MaxCompletionTokens,
        };
    }
    if let Some(v) = meta.get("compat_thinking_format") {
        compat.thinking_format = match v.as_str() {
            "openrouter" => ThinkingFormat::OpenRouter,
            "zai" => ThinkingFormat::Zai,
            "qwen" => ThinkingFormat::Qwen,
            "qwen_chat_template" | "qwen-chat-template" => ThinkingFormat::QwenChatTemplate,
            _ => ThinkingFormat::OpenAI,
        };
    }

    compat
}

/// Apply reasoning effort to the request body based on thinking format.
pub fn apply_reasoning_effort(
    body: &mut serde_json::Value,
    level: Option<ThinkingLevel>,
    format: ThinkingFormat,
    effort_map: &HashMap<ThinkingLevel, String>,
) {
    let level = match level {
        Some(ThinkingLevel::Xhigh) => Some(ThinkingLevel::High),
        other => other,
    };
    let level = match level {
        Some(l) => l,
        None => return,
    };

    let effort_str = effort_map
        .get(&level)
        .cloned()
        .unwrap_or_else(|| match level {
            ThinkingLevel::Minimal | ThinkingLevel::Low => "low".into(),
            ThinkingLevel::Medium => "medium".into(),
            ThinkingLevel::High => "high".into(),
            ThinkingLevel::Xhigh => "high".into(),
        });

    match format {
        ThinkingFormat::OpenAI => {
            body["reasoning_effort"] = json!(effort_str);
        }
        ThinkingFormat::OpenRouter => {
            body["reasoning"] = json!({ "effort": effort_str });
        }
        ThinkingFormat::Zai | ThinkingFormat::Qwen => {
            body["enable_thinking"] = json!(true);
        }
        ThinkingFormat::QwenChatTemplate => {
            body["chat_template_kwargs"] = json!({ "enable_thinking": true });
        }
    }
}

/// Apply OpenRouter routing preferences to the request body.
pub fn apply_openrouter_routing(body: &mut serde_json::Value, routing: &OpenRouterRouting) {
    let mut provider = serde_json::Map::new();

    if let Some(v) = routing.allow_fallbacks {
        provider.insert("allow_fallbacks".into(), json!(v));
    }
    if let Some(v) = routing.require_parameters {
        provider.insert("require_parameters".into(), json!(v));
    }
    if let Some(v) = routing.data_collection {
        provider.insert(
            "data_collection".into(),
            json!(match v {
                DataCollection::Allow => "allow",
                DataCollection::Deny => "deny",
            }),
        );
    }
    if let Some(ref v) = routing.order {
        provider.insert("order".into(), json!(v));
    }
    if let Some(ref v) = routing.only {
        provider.insert("only".into(), json!(v));
    }
    if let Some(ref v) = routing.ignore {
        provider.insert("ignore".into(), json!(v));
    }

    if !provider.is_empty() {
        body["provider"] = provider.into();
    }
}

/// Apply Vercel AI Gateway routing preferences to the request body.
pub fn apply_vercel_routing(body: &mut serde_json::Value, routing: &VercelGatewayRouting) {
    let mut provider = serde_json::Map::new();

    if let Some(ref v) = routing.only {
        provider.insert("only".into(), json!(v));
    }
    if let Some(ref v) = routing.order {
        provider.insert("order".into(), json!(v));
    }

    if !provider.is_empty() {
        body["provider"] = provider.into();
    }
}
