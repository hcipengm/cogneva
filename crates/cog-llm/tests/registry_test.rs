use cog_llm::{ApiType, Model, ModelCost, Provider, ProviderRegistry};

#[test]
fn test_registry_new() {
    let registry = ProviderRegistry::new();
    // Empty registry should have no providers
    assert!(registry.get(&cog_llm::ApiType::OpenAICompletions).is_none());
}

fn test_model_for(provider: Provider) -> cog_llm::Model {
    match provider {
        Provider::OpenAI => Model {
            id: "gpt-4o".into(),
            name: "GPT-4o".into(),
            api: ApiType::OpenAICompletions,
            provider,
            base_url: "https://api.openai.com/v1".into(),
            context_window: 128_000,
            max_tokens: 4096,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: false,
            cost: ModelCost {
                input: 2.5,
                output: 10.0,
                cache_read: 1.25,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
        Provider::Anthropic => Model {
            id: "claude-sonnet-4-7".into(),
            name: "Claude Sonnet 4.7".into(),
            api: ApiType::AnthropicMessages,
            provider,
            base_url: "https://api.anthropic.com".into(),
            context_window: 200_000,
            max_tokens: 4096,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: true,
            cost: ModelCost {
                input: 3.0,
                output: 15.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
        Provider::Google => Model {
            id: "gemini-2.5-pro".into(),
            name: "Gemini 2.5 Pro".into(),
            api: ApiType::GoogleGenerativeAI,
            provider,
            base_url: "https://generativelanguage.googleapis.com/v1beta".into(),
            context_window: 1_000_000,
            max_tokens: 8192,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: true,
            supports_reasoning: true,
            cost: ModelCost {
                input: 1.25,
                output: 10.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
        Provider::Ollama => Model {
            id: "llama3.1".into(),
            name: "Llama 3.1".into(),
            api: ApiType::OllamaChat,
            provider,
            base_url: "http://localhost:11434".into(),
            context_window: 128_000,
            max_tokens: 4096,
            supports_tools: true,
            supports_streaming: true,
            supports_vision: false,
            supports_reasoning: false,
            cost: ModelCost {
                input: 0.0,
                output: 0.0,
                cache_read: 0.0,
                cache_write: 0.0,
            },
            headers: Default::default(),
        },
    }
}

#[test]
fn test_registry_register_builtin() {
    let mut registry = ProviderRegistry::new();
    registry.register_builtin(
        |provider| match provider {
            Provider::OpenAI => Some("sk-test".into()),
            Provider::Anthropic => Some("sk-ant".into()),
            Provider::Google => Some("sk-goo".into()),
            Provider::Ollama => None,
        },
        test_model_for,
        256,
    );

    assert!(registry.get(&cog_llm::ApiType::OpenAICompletions).is_some());
    assert!(registry.get(&cog_llm::ApiType::AnthropicMessages).is_some());
    assert!(registry
        .get(&cog_llm::ApiType::GoogleGenerativeAI)
        .is_some());
    assert!(registry.get(&cog_llm::ApiType::OllamaChat).is_some());
}

#[test]
fn test_registry_get_nonexistent() {
    let registry = ProviderRegistry::new();
    assert!(registry.get(&cog_llm::ApiType::OpenAICompletions).is_none());
}
