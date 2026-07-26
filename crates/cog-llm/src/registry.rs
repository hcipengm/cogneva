use std::collections::HashMap;
use std::sync::Arc;

use crate::model::{ApiType, Model, Provider};
use crate::provider::{
    anthropic::AnthropicProvider, google::GoogleProvider, ollama::OllamaProvider,
    openai::OpenAIProvider,
};
use cog_core::LlmClient as LLMProvider;

/// Provider 注册表。
/// 参考 pi-ai 的 api-registry，但用 Rust HashMap 替代函数式注册。
pub struct ProviderRegistry {
    providers: HashMap<ApiType, Arc<dyn LLMProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    /// 注册内置 provider。
    /// `model_resolver` 负责根据 provider 返回对应的默认 [`Model`] 元数据，
    /// 使调用方（assembly 层）控制模型参数，避免在 `cog-llm` 内硬编码。
    pub fn register_builtin(
        &mut self,
        api_key_resolver: impl Fn(Provider) -> Option<String>,
        model_resolver: impl Fn(Provider) -> Model,
        stream_capacity: usize,
    ) {
        // OpenAI
        if let Some(key) = api_key_resolver(Provider::OpenAI) {
            let model = model_resolver(Provider::OpenAI);
            self.register(
                ApiType::OpenAICompletions,
                Arc::new(OpenAIProvider::new(model, key).with_stream_capacity(stream_capacity)),
            );
        }

        // Anthropic
        if let Some(key) = api_key_resolver(Provider::Anthropic) {
            let model = model_resolver(Provider::Anthropic);
            self.register(
                ApiType::AnthropicMessages,
                Arc::new(AnthropicProvider::new(model, key).with_stream_capacity(stream_capacity)),
            );
        }

        // Google
        if let Some(key) = api_key_resolver(Provider::Google) {
            let model = model_resolver(Provider::Google);
            self.register(
                ApiType::GoogleGenerativeAI,
                Arc::new(GoogleProvider::new(model, key).with_stream_capacity(stream_capacity)),
            );
        }

        // Ollama (no API key needed)
        {
            let model = model_resolver(Provider::Ollama);
            self.register(
                ApiType::OllamaChat,
                Arc::new(OllamaProvider::new(model).with_stream_capacity(stream_capacity)),
            );
        }
    }

    pub fn register(&mut self, api: ApiType, provider: Arc<dyn LLMProvider>) {
        self.providers.insert(api, provider);
    }

    pub fn get(&self, api: &ApiType) -> Option<Arc<dyn LLMProvider>> {
        self.providers.get(api).cloned()
    }

    pub fn health_check_all(&self) -> HashMap<ApiType, bool> {
        let mut results = HashMap::new();
        for api in self.providers.keys() {
            // Note: In async context, use async health check
            results.insert(*api, true); // Placeholder
        }
        results
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}
