//! Wiki plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

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
        let wiki_adapter = build_wiki_adapter(config, ctx).await?;

        if let Some(ref backend) = wiki_adapter {
            ctx.publish_service(backend.clone());
            info!("WikiPlugin wiki backend published");
        } else {
            info!("WikiPlugin no wiki backend configured");
        }

        // The retrieval series is seeded before the backend is built, and
        // whether or not a wiki layer was found: a process that reaches no wiki
        // is exactly the one that will write no retrieval cell, so its zeros
        // are what has to stay readable.
        let metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        match metrics {
            Some(ref backend) => {
                cog_core::contract::knowledge::seed_retrieval_cells(backend).await;
            }
            None => warn!(
                "no metrics backend published; the knowledge retrieval cells \
                 will neither be seeded nor recorded"
            ),
        }

        // Build and publish UnifiedKnowledgeBackend when possible.
        if let Some(ref wiki) = wiki_adapter {
            // All three services are wired as handles rather than read here. The
            // memory plugin publishes them and it initialises in this plugin's
            // own layer, where plugins initialise concurrently -- so a read
            // here yields a service only when that plugin's init happens to
            // finish first. The handles resolve on first use instead, which is
            // after every init has returned.
            let mut unified = crate::UnifiedKnowledgeBackend::new()
                .with_wiki(wiki.clone())
                .with_memory_late(cog_core::LateService::new(ctx.clone()))
                .with_embedding_late(cog_core::LateService::new(ctx.clone()))
                .with_reranker_late(cog_core::LateService::new(ctx.clone()));
            // The retrieval outcome series is the only place that separates a
            // layer this process does not hold from a layer that answered with
            // nothing, and the plugin context is in hand only here.
            if let Some(backend) = metrics {
                unified = unified.with_metrics(backend);
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

/// Build the wiki's semantic face over whatever this process holds.
///
/// Returns `None` when no vector store is published, which is a wiki that
/// answers by keyword only. That is a state worth a warning at wiring time and
/// not a silence: every query whose words no page uses comes back empty from
/// such a wiki, and nothing downstream can tell that from a wiki that holds
/// nothing relevant.
fn build_vector_index(ctx: &cog_core::PluginContext) -> Option<Arc<crate::WikiVectorIndex>> {
    let Some(vector) = ctx.consume_service::<dyn cog_core::VectorBackend>() else {
        warn!(
            "no VectorBackend published; the wiki will answer by keyword only and a query \
             whose words no page uses will find nothing"
        );
        return None;
    };
    Some(Arc::new(crate::WikiVectorIndex::new(
        vector,
        crate::vector_index::DEFAULT_WIKI_COLLECTION,
        cog_core::LateService::new(ctx.clone()),
    )))
}

async fn build_wiki_adapter(
    config: &cog_core::Config,
    ctx: &cog_core::PluginContext,
) -> cog_core::SFResult<Option<Arc<dyn cog_core::WikiBackend>>> {
    let strict = config.system.strict_persistence;
    let vector_index = build_vector_index(ctx);
    // Read once here, before either branch moves the index into the backend:
    // whether the wiki can search by meaning is the one fact about this wiring
    // that no later reading of the wiki's own contents can recover.
    let has_semantic_face = vector_index.is_some();

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
            let mut adapter = crate::meilisearch::MeilisearchWikiBackend::new(host, api_key, index);
            if let Some(index) = vector_index.clone() {
                adapter = adapter.with_vector_index(index);
            }
            if cog_core::WikiBackend::health_check(&adapter).await {
                tracing::info!(
                    "WikiBackend initialized: provider=meilisearch host={} index={} semantic={}",
                    host,
                    index,
                    has_semantic_face
                );
                return Ok(Some(Arc::new(adapter)));
            }
            if strict {
                return Err(cog_core::SFError::Config(format!(
                    "Meilisearch wiki backend unreachable at {}; the configured provider would silently fall back to the local wiki (strict_persistence=true)",
                    host
                )));
            }
            tracing::warn!(
                "Meilisearch wiki backend unreachable at {}; falling back to local wiki",
                host
            );
        }
    }

    let Some(object_backend) = ctx.consume_service::<dyn cog_core::ObjectBackend>() else {
        if strict {
            return Err(cog_core::SFError::Config(
                "No ObjectBackend available for the wiki backend (strict_persistence=true)".into(),
            ));
        }
        return Ok(None);
    };
    let mut adapter = crate::WikiManager::new(object_backend);
    if let Some(index) = vector_index {
        adapter = adapter.with_vector_index(index);
    }
    tracing::info!(
        "WikiBackend initialized: provider=local-wiki prefix=wiki semantic={}",
        has_semantic_face
    );
    Ok(Some(Arc::new(adapter)))
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "wiki",
    // Same reasoning as cog-memory: the ObjectBackend is consumed during init,
    // and same-layer plugins init in parallel, so the edge must be declared or
    // the consume races with storage's init.
    requires: &["storage"],
    optional_requires: &[],
    factory: || Box::new(WikiPlugin::new()),
};
