use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::Usage;

/// API 类型。参考 pi-ai 的 KnownApi，但去掉不常用的。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ApiType {
    #[serde(rename = "openai-completions")]
    OpenAICompletions,
    #[serde(rename = "anthropic-messages")]
    AnthropicMessages,
    #[serde(rename = "google-generative-ai")]
    GoogleGenerativeAI,
    #[serde(rename = "ollama-chat")]
    OllamaChat,
}

/// 提供商枚举。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    OpenAI,
    Anthropic,
    Google,
    Ollama,
}

impl std::fmt::Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Provider::OpenAI => write!(f, "openai"),
            Provider::Anthropic => write!(f, "anthropic"),
            Provider::Google => write!(f, "google"),
            Provider::Ollama => write!(f, "ollama"),
        }
    }
}

/// 推理级别。参考 pi-ai 的 ThinkingLevel。
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

/// Cost metadata per model, in $/million tokens.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelCost {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
}

/// 模型元数据。参考 pi-ai 的 Model。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub name: String,
    pub api: ApiType,
    pub provider: Provider,
    pub base_url: String,
    pub context_window: u32,
    pub max_tokens: u32,
    pub supports_tools: bool,
    pub supports_streaming: bool,
    pub supports_vision: bool,
    pub supports_reasoning: bool,
    pub cost: ModelCost,
    pub headers: HashMap<String, String>,
}

/// Model registry: Map<provider, Map<id, Model>>.
#[derive(Debug, Clone, Default)]
pub struct ModelRegistry {
    models: HashMap<Provider, HashMap<String, Model>>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    pub fn register(&mut self, model: Model) {
        self.models
            .entry(model.provider)
            .or_default()
            .insert(model.id.clone(), model);
    }

    pub fn get(&self, provider: Provider, id: &str) -> Option<&Model> {
        self.models.get(&provider)?.get(id)
    }

    pub fn get_by_api(&self, api: ApiType, id: &str) -> Option<&Model> {
        for provider_models in self.models.values() {
            if let Some(model) = provider_models.get(id) {
                if model.api == api {
                    return Some(model);
                }
            }
        }
        None
    }

    pub fn list_by_provider(&self, provider: Provider) -> Vec<&Model> {
        self.models
            .get(&provider)
            .map(|m| m.values().collect())
            .unwrap_or_default()
    }

    pub fn all_models(&self) -> Vec<&Model> {
        self.models.values().flat_map(|m| m.values()).collect()
    }
}

/// Calculate cost from usage and model cost metadata.
/// Prices are in $/million tokens.
pub fn calculate_cost(model: &Model, usage: &Usage) -> crate::Cost {
    let input_cost = (model.cost.input / 1_000_000.0) * usage.input as f64;
    let output_cost = (model.cost.output / 1_000_000.0) * usage.output as f64;
    let cache_read_cost = (model.cost.cache_read / 1_000_000.0) * usage.cache_read as f64;
    let cache_write_cost = (model.cost.cache_write / 1_000_000.0) * usage.cache_write as f64;
    let total = input_cost + output_cost + cache_read_cost + cache_write_cost;

    crate::Cost {
        input: input_cost,
        output: output_cost,
        cache_read: cache_read_cost,
        cache_write: cache_write_cost,
        total,
    }
}

/// Check if a model supports the "xhigh" reasoning effort level.
pub fn supports_xhigh(model_id: &str) -> bool {
    let lower = model_id.to_lowercase();
    lower.contains("gpt-5.2")
        || lower.contains("gpt-5.3")
        || lower.contains("gpt-5.4")
        || lower.contains("opus-4-6")
        || lower.contains("opus-4.6")
}

/// Check if two models are the same (same id and provider).
/// Aligns with pi-ai's modelsAreEqual.
pub fn models_are_equal(a: Option<&Model>, b: Option<&Model>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.id == b.id && a.provider == b.provider,
        _ => false,
    }
}
