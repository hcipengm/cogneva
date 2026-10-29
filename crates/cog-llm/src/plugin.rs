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
        // Drop config borrow before publishing
        let _ = config;

        let provider = build_llm_provider(
            stream_capacity,
            anthropic_default_max_tokens,
            &llm_routing,
            ctx.consume_service::<dyn cog_core::HttpClient>(),
        )?;
        let hot_swap = Arc::new(crate::HotSwappableLlmClient::new(provider));
        ctx.publish(hot_swap.clone());
        info!("LlmPlugin LLM provider published");

        // Also publish as dyn LlmClient so consumers can use either type.
        ctx.publish_service::<dyn cog_core::LlmClient>(hot_swap.clone());

        // What we believe about an upstream shapes the request body, and the
        // gateway rewrites that body on its way out. It asks through the
        // contract instead of linking this crate: the table is vendor
        // knowledge and stays here, the question and the answer's shape are
        // the contract's.
        ctx.publish_service::<dyn cog_core::ProviderCompatTable>(Arc::new(
            crate::utils::compat::CompatImages,
        ));

        // Observable publish (pin-style)
        ctx.publish_observable(crate::observable::global_observable());
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
            // 版本段在 base 里：`AnthropicProvider` 只往后接 `/messages`，
            // 少一段就是 `https://api.anthropic.com/messages`——那个地址不存在。
            base_url: "https://api.anthropic.com/v1".into(),
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
        crate::anthropic::API_STYLE => default_model_for(crate::Provider::Anthropic),
        "google" => default_model_for(crate::Provider::Google),
        "ollama" => default_model_for(crate::Provider::Ollama),
        _ => default_model_for(crate::Provider::OpenAI),
    };
    // The style names a wire shape, and the OpenAI family has two. The
    // Responses shape is the vendor's current one for the same endpoint, so a
    // backend that says so gets the provider that speaks it; anything else in
    // the family keeps the chat-completions provider.
    if backend.api_style == crate::responses::API_STYLE {
        model.api = crate::ApiType::OpenAIResponses;
    }
    model.id = backend.model.clone();
    model.name = backend.model.clone();
    if let Some(ref url) = backend.base_url {
        model.base_url = url.clone();
    }

    let provider: Arc<dyn cog_core::LlmClient> = match backend.api_style.as_str() {
        crate::anthropic::API_STYLE => {
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
        crate::responses::API_STYLE => {
            let mut p = crate::responses::OpenAIResponsesProvider::new(model, &backend.api_key)
                .with_stream_capacity(stream_capacity);
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

    if backends.is_empty() {
        return Err(cog_core::SFError::Config(
            "No LLM backends configured. Please set llm_routing.backends in cogneva.json".into(),
        ));
    }

    info!(
        "LLM RoutingProvider created with {} backend(s)",
        backends.len()
    );
    let provider: Arc<dyn cog_core::LlmClient> = Arc::new(crate::RoutingProvider::new(
        backends,
        llm_routing.max_failover_attempts,
        llm_routing.retry_on_429,
        llm_routing.retry_on_402,
    ));

    Ok(Arc::new(crate::ObservedLlmClient::new(provider)))
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "llm",
    requires: &["net"],
    optional_requires: &[],
    factory: || Box::new(LlmPlugin::new()),
};

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{ChatOptions, Message, SFError, SFResult};
    use futures::StreamExt;
    use std::sync::Mutex;

    /// Records the address the provider chose, then answers an empty stream.
    #[derive(Debug)]
    struct AddressRecorder {
        seen: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl cog_core::HttpClient for AddressRecorder {
        async fn execute(&self, _req: cog_core::HttpRequest) -> SFResult<cog_core::HttpResponse> {
            unreachable!("stream path only")
        }
        async fn execute_stream(
            &self,
            req: cog_core::HttpRequest,
        ) -> SFResult<cog_core::HttpStreamResponse> {
            self.seen.lock().unwrap().push(req.url.clone());
            let chunk: Result<bytes::Bytes, SFError> = Ok(bytes::Bytes::from_static(
                b"data: {\"type\": \"response.completed\", \"response\": {\"id\": \"r\", \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}}\n\n",
            ));
            let stream: cog_core::HttpBodyStream = Box::pin(futures::stream::iter(vec![chunk]));
            Ok(cog_core::HttpStreamResponse {
                status: 200,
                headers: std::collections::HashMap::new(),
                stream,
            })
        }
    }

    fn backend(style: &str) -> crate::LLMBackendConfig {
        crate::LLMBackendConfig {
            provider: "gateway".into(),
            api_key: "placeholder".into(),
            base_url: Some("http://cogneva-security-gateway:8081/v1".into()),
            model: "via-security-gateway".into(),
            api_style: style.into(),
            weight: 1,
            enabled: true,
        }
    }

    /// The style is what chooses the wire shape, and the two shapes are served
    /// at different addresses on the same host. A backend that declares the
    /// Responses face and is answered on `/chat/completions` would be talking
    /// to a face it did not choose — and the reply would be parsed as the
    /// wrong shape rather than refused.
    #[tokio::test]
    async fn each_style_name_reaches_its_own_endpoint() {
        for (style, want) in [
            (
                "openai",
                "http://cogneva-security-gateway:8081/v1/chat/completions",
            ),
            (
                "openai-responses",
                "http://cogneva-security-gateway:8081/v1/responses",
            ),
        ] {
            let recorder = Arc::new(AddressRecorder {
                seen: Mutex::new(Vec::new()),
            });
            let provider = build_single_provider(&backend(style), 8, 4096, Some(recorder.clone()));
            let mut stream = provider
                .chat_stream(&[Message::user("hi")], &ChatOptions::default())
                .await
                .expect("the provider builds a stream");
            while stream.next().await.is_some() {}
            assert_eq!(
                recorder.seen.lock().unwrap().as_slice(),
                [want.to_string()],
                "style {style}"
            );
        }
    }
}
