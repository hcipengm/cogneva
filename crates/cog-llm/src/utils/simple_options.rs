use crate::{ChatOptions, Model, ThinkingLevel};

/// `CompleteOptions` → `ChatOptions`：四个字段在这里映射一次。
///
/// 六个 provider 各抄过一遍这张表（routing 与五个 provider 的
/// `complete_stream`）。抄的好处是每处都看得见自己在传什么；代价是给
/// `CompleteOptions` 加一个字段不会有任何一处编译失败——它们都带
/// `..Default::default()`，新字段在**每一个** provider 上静默变成默认值，
/// 调用方却以为自己传进去了。加到这里，六处一起动。
pub fn chat_options_from(options: &crate::CompleteOptions) -> ChatOptions {
    ChatOptions {
        model: options.model.clone(),
        temperature: options.temperature,
        max_tokens: options.max_tokens,
        api_key: options.api_key.clone(),
        ..Default::default()
    }
}

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

/// 思考预算的一档：模型扩展思考能分配的最小单位。
///
/// 不是本仓挑的数——供应商的 `budget_tokens` 从 1024 起算，其余各档都是它的
/// 整数倍。写成 1024 的四份拷贝会让「最小一档」这个说法在四处慢慢走散。
// cap: fixed — 供应商扩展思考 `budget_tokens` 的最小值：1024
pub const THINKING_BUDGET_UNIT: u32 = 1024;

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
            minimal: THINKING_BUDGET_UNIT,
            low: THINKING_BUDGET_UNIT * 2,
            medium: THINKING_BUDGET_UNIT * 8,
            high: THINKING_BUDGET_UNIT * 16,
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
        // Nothing to reserve: the request asks the model not to think, so the
        // caller's cap is the whole cap.
        ThinkingLevel::Off => 0,
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
