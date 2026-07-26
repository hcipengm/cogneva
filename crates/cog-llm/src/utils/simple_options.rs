use crate::{ChatOptions, Model, ThinkingLevel};

/// Build base options from simple options and model defaults.
pub fn build_base_options(options: &ChatOptions, model: &Model) -> ChatOptions {
    ChatOptions {
        model: options.model.clone().or_else(|| Some(model.id.clone())),
        temperature: options.temperature,
        max_tokens: options.max_tokens.or(Some(model.max_tokens)),
        api_key: options.api_key.clone(),
        cache_retention: options.cache_retention,
        session_id: options.session_id.clone(),
        headers: options.headers.clone(),
        on_payload: options.on_payload.clone(),
        max_retry_delay_ms: options.max_retry_delay_ms,
        metadata: options.metadata.clone(),
        ..Default::default()
    }
}

/// Clamp "xhigh" reasoning to "high" for providers that don't support it.
pub fn clamp_reasoning(level: Option<ThinkingLevel>) -> Option<ThinkingLevel> {
    match level {
        Some(ThinkingLevel::Xhigh) => Some(ThinkingLevel::High),
        other => other,
    }
}

/// Token budgets for each thinking level.
#[derive(Debug, Clone)]
pub struct ThinkingBudgets {
    pub minimal: u32,
    pub low: u32,
    pub medium: u32,
    pub high: u32,
}

impl Default for ThinkingBudgets {
    fn default() -> Self {
        Self {
            minimal: 1024,
            low: 2048,
            medium: 8192,
            high: 16384,
        }
    }
}

/// Adjust max_tokens to reserve budget for thinking.
pub fn adjust_max_tokens_for_thinking(
    base_max_tokens: u32,
    model_max_tokens: u32,
    reasoning_level: Option<ThinkingLevel>,
    custom_budgets: Option<&ThinkingBudgets>,
) -> (u32, u32) {
    let budgets = custom_budgets.cloned().unwrap_or_default();
    let level = clamp_reasoning(reasoning_level).unwrap_or(ThinkingLevel::High);
    let thinking_budget = match level {
        ThinkingLevel::Minimal => budgets.minimal,
        ThinkingLevel::Low => budgets.low,
        ThinkingLevel::Medium => budgets.medium,
        ThinkingLevel::High | ThinkingLevel::Xhigh => budgets.high,
    };

    let min_output_tokens = 1024;
    let max_tokens = (base_max_tokens + thinking_budget).min(model_max_tokens);

    let thinking_budget = if max_tokens <= thinking_budget {
        max_tokens.saturating_sub(min_output_tokens)
    } else {
        thinking_budget
    };

    (max_tokens, thinking_budget)
}
