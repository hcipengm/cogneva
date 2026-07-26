//! Wiki plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Wiki plugin that self-assembles and publishes the wiki backend.
pub struct WikiPlugin {
    initialized: bool,
}

impl WikiPlugin {
    /// Create a plugin that will build the wiki backend during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for WikiPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for WikiPlugin {
    fn name(&self) -> &'static str {
        "wiki"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let config = ctx.config();
        let wiki_adapter = build_wiki_adapter(config, ctx).await;

        if let Some(ref backend) = wiki_adapter {
            ctx.publish_service(backend.clone());
            info!("WikiPlugin wiki backend published");
        } else {
            info!("WikiPlugin no wiki backend configured");
        }

        // Build and publish UnifiedKnowledgeBackend when possible.
        if let Some(ref wiki) = wiki_adapter {
            let mut unified = crate::UnifiedKnowledgeBackend::new().with_wiki(wiki.clone());
            if let Some(memory) = ctx.consume_service::<dyn cog_core::MemoryBackend>() {
                unified = unified.with_memory(memory);
            }
            if let Some(embedding) = ctx.consume_service::<dyn cog_core::EmbeddingProvider>() {
                unified = unified.with_embedding(embedding);
            }
            ctx.publish_service::<dyn cog_core::KnowledgeBackend>(Arc::new(unified));
            info!("WikiPlugin KnowledgeBackend published");
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("WikiPlugin shutdown");
        Ok(())
    }
}

async fn build_wiki_adapter(
    config: &cog_core::Config,
    ctx: &cog_core::PluginContext,
) -> Option<Arc<dyn cog_core::WikiBackend>> {
    if let Some(ref wiki_cfg) = config.providers.wiki {
        if wiki_cfg.enabled && wiki_cfg.provider == "meilisearch" {
            let host = wiki_cfg
                .options
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("http://localhost:7700");
            let api_key = wiki_cfg.options.get("api_key").and_then(|v| v.as_str());
            let index = wiki_cfg
                .options
                .get("index")
                .and_then(|v| v.as_str())
                .unwrap_or("wiki");
            let adapter = crate::meilisearch::MeilisearchWikiBackend::new(host, api_key, index);
            tracing::info!(
                "WikiBackend initialized: provider=meilisearch host={} index={}",
                host,
                index
            );
            return Some(Arc::new(adapter));
        }
    }

    let object_backend = ctx.consume_service::<dyn cog_core::ObjectBackend>()?;
    let adapter = crate::WikiManager::new(object_backend);
    tracing::info!("WikiBackend initialized: provider=local-wiki prefix=wiki");
    Some(Arc::new(adapter))
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "wiki",
    requires: &[],
    optional_requires: &[],
    provides: &["WikiBackend", "KnowledgeBackend"],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "ObjectBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MemoryBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "EmbeddingProvider",
            required: false,
        },
    ],
    factory: || Box::new(WikiPlugin::new()),
};
