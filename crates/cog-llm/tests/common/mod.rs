// 每个集成测试文件各自编一次这份夹具，只用得到其中几个的文件不该为此报警。
#![allow(dead_code)]

//! A model the gateway can be asked about, with the four providers' shapes as
//! the suite writes them.
//!
//! Both suites that need one had their own copy. Neither copy is a statement
//! about the crate: it is the input to statements about cost and about the
//! registry, and the two copies drifting apart would have made them statements
//! about different inputs while both stayed green.

use cog_llm::{
    model::{Model, ModelCost, Provider},
    ApiType,
};

pub fn test_model_for(provider: Provider) -> Model {
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
