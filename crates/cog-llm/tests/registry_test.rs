mod common;

use cog_llm::{Provider, ProviderRegistry};

#[test]
fn test_registry_new() {
    let registry = ProviderRegistry::new();
    // Empty registry should have no providers
    assert!(registry.get(&cog_llm::ApiType::OpenAICompletions).is_none());
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
        common::test_model_for,
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
