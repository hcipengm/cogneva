//! LLM plugin — implements [`cog_core::SystemPlugin`].

use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// LLM plugin that builds and publishes the LLM provider from config.
pub struct LlmPlugin {
    initialized: bool,
}

impl LlmPlugin {
    /// Create a plugin that will build the LLM client during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for LlmPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for LlmPlugin {
    fn name(&self) -> &'static str {
        "llm"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let config = ctx.config();
        // tuning / llm_routing 是 cog-llm 自有配置段，自读 cogneva.json。
        let stream_capacity = crate::TuningConfig::load()?.stream_capacity;
        let anthropic_default_max_tokens = config.system.anthropic_default_max_tokens;
        let llm_routing = crate::LLMRoutingConfig::load()?;
        let llm = config.llm.clone();
        // Drop config borrow before publishing
        let _ = config;

        let provider = build_llm_provider(
            stream_capacity,
            anthropic_default_max_tokens,
            &llm_routing,
            &llm,
            ctx.consume_service::<dyn cog_core::HttpClient>(),
        )?;
        let hot_swap = Arc::new(crate::HotSwappableLlmClient::new(provider));
        ctx.publish(hot_swap.clone());
        info!("LlmPlugin LLM provider published");

        // Also publish as dyn LlmClient so consumers can use either type.
        ctx.publish_service::<dyn cog_core::LlmClient>(hot_swap.clone());

        // Observable publish (pin-style)
        ctx.publish_service(crate::observable::global_observable());
        info!("LlmPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("LlmPlugin shutdown");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal helpers (moved from cogneva::assembly::llm)
// ---------------------------------------------------------------------------

pub fn default_model_for(provider: crate::Provider) -> crate::Model {
    use crate::{ApiType, Model, ModelCost};
    match provider {
        crate::Provider::OpenAI => Model {
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
            headers: HashMap::new(),
        },
        crate::Provider::Anthropic => Model {
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
            headers: HashMap::new(),
        },
        crate::Provider::Google => Model {
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
            headers: HashMap::new(),
        },
        crate::Provider::Ollama => Model {
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
            headers: HashMap::new(),
        },
    }
}

pub fn build_single_provider(
    backend: &crate::LLMBackendConfig,
    stream_capacity: usize,
    anthropic_default_max_tokens: u32,
    http_client: Option<Arc<dyn cog_core::HttpClient>>,
) -> Arc<dyn cog_core::LlmClient> {
    let mut model = match backend.api_style.as_str() {
        "anthropic" => default_model_for(crate::Provider::Anthropic),
        "google" => default_model_for(crate::Provider::Google),
        "ollama" => default_model_for(crate::Provider::Ollama),
        _ => default_model_for(crate::Provider::OpenAI),
    };
    model.id = backend.model.clone();
    model.name = backend.model.clone();
    if let Some(ref url) = backend.base_url {
        model.base_url = url.clone();
    }

    let provider: Arc<dyn cog_core::LlmClient> = match backend.api_style.as_str() {
        "anthropic" => {
            let mut p = crate::anthropic::AnthropicProvider::new(model, &backend.api_key)
                .with_stream_capacity(stream_capacity)
                .with_default_max_tokens(anthropic_default_max_tokens);
            if let Some(ref c) = http_client {
                p = p.with_client(c.clone());
            }
            Arc::new(p)
        }
        "google" => {
            let mut p = crate::google::GoogleProvider::new(model, &backend.api_key)
                .with_stream_capacity(stream_capacity);
            if let Some(ref c) = http_client {
                p = p.with_client(c.clone());
            }
            Arc::new(p)
        }
        "ollama" => {
            let mut p =
                crate::ollama::OllamaProvider::new(model).with_stream_capacity(stream_capacity);
            if let Some(ref c) = http_client {
                p = p.with_client(c.clone());
            }
            Arc::new(p)
        }
        _ => {
            let mut p = crate::openai::OpenAIProvider::new(model, &backend.api_key)
                .with_stream_capacity(stream_capacity);
            if let Some(ref c) = http_client {
                p = p.with_client(c.clone());
            }
            Arc::new(p)
        }
    };
    provider
}

pub fn build_llm_provider(
    stream_capacity: usize,
    anthropic_default_max_tokens: u32,
    llm_routing: &crate::LLMRoutingConfig,
    llm: &cog_core::LLMConfig,
    http_client: Option<Arc<dyn cog_core::HttpClient>>,
) -> cog_core::SFResult<Arc<dyn cog_core::LlmClient>> {
    let backends: Vec<Arc<dyn cog_core::LlmClient>> = llm_routing
        .backends
        .iter()
        .filter(|b| b.enabled)
        .map(|b| {
            build_single_provider(
                b,
                stream_capacity,
                anthropic_default_max_tokens,
                http_client.clone(),
            )
        })
        .collect();

    let provider: Arc<dyn cog_core::LlmClient> = if !backends.is_empty() {
        info!(
            "LLM RoutingProvider created with {} backend(s)",
            backends.len()
        );
        Arc::new(crate::RoutingProvider::new(
            backends,
            llm_routing.max_failover_attempts,
            llm_routing.retry_on_429,
            llm_routing.retry_on_402,
        ))
    } else if !llm.provider.is_empty() {
        info!(
            "LLM fallback: using legacy single-provider config (provider={})",
            llm.provider
        );
        let fallback = crate::LLMBackendConfig {
            provider: llm.provider.clone(),
            api_key: llm.api_key.clone(),
            base_url: llm.base_url.clone(),
            model: llm.model.clone(),
            api_style: "openai".into(),
            weight: 1,
            enabled: true,
        };
        build_single_provider(
            &fallback,
            stream_capacity,
            anthropic_default_max_tokens,
            http_client,
        )
    } else {
        return Err(cog_core::SFError::Config(
            "No LLM backends configured. Please set llm_routing.backends or llm in cogneva.json"
                .into(),
        ));
    };

    Ok(Arc::new(crate::ObservedLlmClient::new(provider)))
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "llm",
    requires: &["net"],
    optional_requires: &[],
    provides: &["HotSwappableLlmClient", "LlmClient", "Observable"],
    consumes: &[cog_core::ConsumeSpec {
        type_name: "HttpClient",
        required: false,
    }],
    factory: || Box::new(LlmPlugin::new()),
};
