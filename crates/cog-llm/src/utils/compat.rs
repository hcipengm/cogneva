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
    /// Whether the provider only accepts temperature=1.0. A vendor branch sets
    /// it, and no model name is named beside it: this profile is keyed on the
    /// base URL, so a model written here is a claim nothing re-checks -- the one
    /// that used to stand here had already gone stale against the model the pool
    /// actually ran. The admission probe settles this per upstream anyway, and
    /// this value is only the fallback for an upstream it has not reached.
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

    // Volcengine Ark (`*.volces.com`). The coding surface (`/api/coding/v3`)
    // answered requests with 400 before the first byte while the pool served
    // through its peers -- the request-shape counter moved on this upstream
    // alone, which names this profile, not the shared request builder. It ran
    // on the fallback below, the one that assumes the latest OpenAI shape, so
    // the `store` / `reasoning_effort` / tool `strict` fields callers send
    // went through untouched and the endpoint rejected the body over a field
    // it does not know. The conservative field set keeps those fields off the
    // wire; the gateway's admission probes settle the capabilities a host
    // name cannot tell.
    if lower.contains("volces.com") || lower.contains("volcengine") {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The profile the ark.cn-beijing.volces.com alert named: a request-shape
    /// rejection that moved on this upstream alone, because the fallback
    /// profile forwarded `store` / `reasoning_effort` / tool `strict` to an
    /// endpoint that answers what it does not know with 400 before its first
    /// byte.
    #[test]
    fn volcengine_ark_strips_the_fields_it_rejects() {
        let compat = detect_compat("https://ark.cn-beijing.volces.com/api/coding/v3");
        assert!(!compat.supports_store);
        assert!(!compat.supports_reasoning_effort);
        assert!(!compat.supports_strict_mode);
        assert!(!compat.supports_developer_role);
        // Ark reports usage in streaming and spells the output cap the new
        // way, as doubao on the same vendor does.
        assert!(compat.supports_usage_in_streaming);
        assert_eq!(compat.max_tokens_field, MaxTokensField::MaxCompletionTokens);
    }

    #[test]
    fn unknown_hosts_keep_the_latest_openai_shape() {
        let compat = detect_compat("https://llm.example.com/v1");
        assert!(compat.supports_store);
        assert!(compat.supports_reasoning_effort);
        assert!(compat.supports_strict_mode);
    }

    /// 网关据画像决定的六个字段里，只有两个能被实测推翻——`requires_temperature_one`
    /// 有池条目的准入判定、`supports_usage_in_streaming` 有 `effective_usage_verdict`。
    /// 另外四个（`supports_store`、`supports_reasoning_effort`、`supports_strict_mode`、
    /// `max_tokens_field`）**画像写什么就是什么**：池里没有任何读数能把它们判错，
    /// 记下来的只是「改写发生过」，不是「改写是对的」。所以它们唯一会遇到读者的地方
    /// 就是这张表——逐分支钉住，改动因此必然是一次有意的改动。
    ///
    /// 这张表记的是**画像说的话**，不是上游的能力：`kimi` 那条
    /// `requires_temperature_one: true` 的出处不在这里（准入探测能把这台问清楚，
    /// 现役池上还没问过），这里只是不让它被默默改掉。
    ///
    /// 分母取自源码本身：本文件里 `if lower` + `.contains(` 的条数就是分支数
    /// （needle 拆开拼，免得把这条判据自己数进去），漏一行或新加分支没配行都会红。
    #[test]
    fn every_vendor_branch_pins_the_decisions_no_measurement_can_overturn() {
        // (字面量, store, reasoning_effort, strict, max_tokens 拼写, 流里报用量, 只收 temperature=1)
        #[rustfmt::skip]
        let branches: &[(&str, bool, bool, bool, MaxTokensField, bool, bool)] = &[
            ("openrouter.ai",     false, true,  true,  MaxTokensField::MaxTokens,           true,  false),
            ("gateway.ai.cloudflare.com", false, false, true, MaxTokensField::MaxCompletionTokens, true, false),
            ("ai-gateway",        false, false, true,  MaxTokensField::MaxCompletionTokens, true,  false),
            ("api.groq.com",      false, false, false, MaxTokensField::MaxTokens,           false, false),
            ("api.cerebras.ai",   false, false, false, MaxTokensField::MaxTokens,           false, false),
            ("api.x.ai",          false, false, false, MaxTokensField::MaxTokens,           true,  false),
            ("api.xai.com",       false, false, false, MaxTokensField::MaxTokens,           true,  false),
            ("api.mistral.ai",    false, false, false, MaxTokensField::MaxTokens,           false, false),
            ("api.minimax.chat",  false, false, false, MaxTokensField::MaxTokens,           false, false),
            ("minimax",           false, false, false, MaxTokensField::MaxTokens,           false, false),
            ("api.kimi.com",      false, false, false, MaxTokensField::MaxTokens,           false, true),
            ("kimi",              false, false, false, MaxTokensField::MaxTokens,           false, true),
            ("githubcopilot",     false, false, false, MaxTokensField::MaxCompletionTokens, true,  false),
            ("copilot",           false, false, false, MaxTokensField::MaxCompletionTokens, true,  false),
            ("volces.com",        false, false, false, MaxTokensField::MaxCompletionTokens, true,  false),
            ("volcengine",        false, false, false, MaxTokensField::MaxCompletionTokens, true,  false),
            ("localhost:11434",   false, false, false, MaxTokensField::MaxTokens,           false, false),
            ("ollama",            false, false, false, MaxTokensField::MaxTokens,           false, false),
        ];
        for (literal, store, reasoning, strict, spelling, usage, temperature) in branches {
            // 厂商身份放进路径：真实上游的 URL 也常常是带路径的，画像正是按子串认厂。
            let compat = detect_compat(&format!("https://upstream.example.test/v1/{literal}"));
            let at = format!("字面量 `{literal}`");
            assert_eq!(compat.supports_store, *store, "{at}: supports_store");
            assert_eq!(
                compat.supports_reasoning_effort, *reasoning,
                "{at}: supports_reasoning_effort"
            );
            assert_eq!(
                compat.supports_strict_mode, *strict,
                "{at}: supports_strict_mode"
            );
            assert_eq!(compat.max_tokens_field, *spelling, "{at}: max_tokens_field");
            assert_eq!(
                compat.supports_usage_in_streaming, *usage,
                "{at}: supports_usage_in_streaming"
            );
            assert_eq!(
                compat.requires_temperature_one, *temperature,
                "{at}: requires_temperature_one"
            );
        }

        let src =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/utils/compat.rs"))
                .expect("本文件要能被读到");
        let branch_count = src.matches(concat!("if lower", ".contains(")).count();
        assert_eq!(
            branch_count, 11,
            "厂商分支数变了（{branch_count} 条）：新分支要在这张表里配齐行，\
             删分支也要把行删掉——否则这条判据自己会静默缩水"
        );
    }
}
