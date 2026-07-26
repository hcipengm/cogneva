//! Memory plugin — implements [`cog_core::SystemPlugin`].

use cog_core::EmbeddingProvider;
use std::sync::Arc;
use tracing::{info, warn};

/// Memory plugin that self-assembles and publishes memory backend,
/// metrics backend, embedding provider, and reranker provider.
pub struct MemoryPlugin {
    initialized: bool,
}

impl MemoryPlugin {
    /// Create a plugin that will build all memory services during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for MemoryPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for MemoryPlugin {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Snapshot config values to drop immutable borrow before publishing.
        let (
            memory_enabled,
            memory_backend_type,
            memory_base_dir,
            memory_embedding_dimension,
            strict_persistence,
        ) = {
            let config = ctx.config();
            (
                config.memory.enabled,
                config.memory.backend_type.clone(),
                config.memory.base_dir.clone(),
                config.memory.embedding_dimension,
                config.system.strict_persistence,
            )
        };

        // Consume PostgreSQL explain pool (published by StoragePlugin).
        let pg_pool_explain = ctx
            .consume::<cog_core::storage::ExplainPool>()
            .and_then(|p| p.0.clone());

        // ── Metrics backend ──
        let metrics_backend = ctx
            .consume_service::<dyn cog_core::MetricsBackend>()
            .unwrap_or_else(|| {
                warn!("No MetricsBackend published by StoragePlugin; using no-op fallback");
                Arc::new(crate::NoopMetricsBackend::new())
            });

        // ── Vector backend ──
        let vector_backend = ctx
            .consume_service::<dyn cog_core::VectorBackend>()
            .unwrap_or_else(|| {
                warn!("No VectorBackend published by StoragePlugin; using no-op fallback");
                Arc::new(crate::NoopVectorBackend::new())
            });

        // ── Memory backend ──
        if !memory_enabled {
            info!("Memory backend disabled; skipping embedding/reranker initialization");
            self.initialized = true;
            return Ok(());
        }

        let memory_backend: Option<Arc<dyn cog_core::MemoryBackend>> = {
            let backend: Arc<dyn cog_core::MemoryBackend> = match memory_backend_type.as_str() {
                "composite" => {
                    let object_backend = match ctx.consume_service::<dyn cog_core::ObjectBackend>()
                    {
                        Some(b) => b,
                        None => {
                            return Err(cog_core::SFError::Config(
                                "No ObjectBackend available for MemoryPlugin".into(),
                            ));
                        }
                    };
                    let mut composite = crate::CompositeMemoryBackend::new(
                        object_backend,
                        vector_backend.clone(),
                        memory_embedding_dimension,
                    );
                    composite.set_persist_dir(&memory_base_dir);
                    if let Err(e) = composite.load().await {
                        warn!("Failed to load persisted memory data: {}", e);
                    } else {
                        info!("CompositeMemoryBackend loaded persisted data");
                    }
                    let summary_backend = crate::VectorSummaryBackend::new(
                        vector_backend.clone(),
                        memory_embedding_dimension,
                    );
                    composite = composite.with_summary_backend(Arc::new(summary_backend));
                    if let Some(ref pool) = pg_pool_explain {
                        let schema_backend =
                            Arc::new(crate::PostgresSchemaBackend::from_pool(pool.clone()));
                        match schema_backend.init_table().await {
                            Ok(()) => {
                                info!("PostgresSchemaBackend initialized for schema layer");
                                composite = composite.with_schema_backend(schema_backend);
                            }
                            Err(e) => {
                                if strict_persistence {
                                    return Err(cog_core::SFError::Config(
                                        format!("PostgresSchemaBackend init_table failed (strict_persistence=true): {}", e)
                                    ));
                                }
                                warn!("PostgresSchemaBackend init_table failed: {}. Schema layer will use memory fallback.", e);
                            }
                        }
                    }
                    info!("CompositeMemoryBackend enabled");
                    Arc::new(crate::MetricsInstrumentedMemoryBackend::new(
                        Arc::new(composite),
                        metrics_backend.clone(),
                    ))
                }
                _ => {
                    info!("MemoryMemoryBackend enabled");
                    Arc::new(crate::MetricsInstrumentedMemoryBackend::new(
                        Arc::new(crate::MemoryMemoryBackend::new()),
                        metrics_backend.clone(),
                    ))
                }
            };
            Some(backend)
        };

        // ── Embedding provider ──
        let embed_provider: Option<Arc<dyn cog_core::EmbeddingProvider>> =
            match crate::FastEmbedProvider::try_new() {
                Ok(p) => {
                    info!("FastEmbed BGE-M3 loaded: {} dim", p.dimension());
                    Some(Arc::new(p))
                }
                Err(e) => {
                    warn!("Failed to load BGE-M3 embedding model: {}", e);
                    None
                }
            };

        // ── Reranker provider ──
        let reranker_provider: Option<Arc<dyn crate::RerankerProvider>> =
            match crate::FastEmbedRerankerProvider::try_new() {
                Ok(p) => {
                    info!("FastEmbed BGE-Reranker-V2-M3 loaded");
                    Some(Arc::new(p))
                }
                Err(e) => {
                    warn!("Failed to load BGE-Reranker-V2-M3: {}", e);
                    None
                }
            };

        // Publish all services.
        if let Some(ref b) = memory_backend {
            ctx.publish_service(b.clone());
            info!("MemoryPlugin memory backend published");
        }
        let default_ingestor: Arc<dyn cog_core::MemoryIngestor> = Arc::new(
            crate::IngestionPipeline::new(crate::RuleBasedExtractor::new()),
        );
        ctx.publish_service(default_ingestor);
        info!("MemoryPlugin default ingestor published");
        ctx.publish_service(metrics_backend);
        info!("MemoryPlugin metrics backend published");
        if let Some(ref p) = embed_provider {
            ctx.publish_service(p.clone());
            info!("MemoryPlugin embed provider published");
        }
        if let Some(ref p) = reranker_provider {
            ctx.publish(Arc::new(RerankerProviderHolder(p.clone())));
            info!("MemoryPlugin reranker provider published");
        }

        // Observable publish (pin-style)
        ctx.publish_service(crate::observable::global_observable());
        info!("MemoryPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let config = ctx.config();
        if !config.memory.enabled || !config.memory.auto_ingest {
            return Ok(());
        }

        let memory_backend = ctx.consume_service::<dyn cog_core::MemoryBackend>();
        let embed_provider = ctx.consume_service::<dyn cog_core::EmbeddingProvider>();
        let llm_provider = ctx.consume_service::<dyn cog_core::LlmClient>();
        let event_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            .map(|h| (*h).clone());

        if let Some(backend) = memory_backend {
            let extractor: Arc<dyn cog_core::MemoryExtractor> =
                if let Some(ref provider) = llm_provider {
                    let mut extractor = crate::LlmMemoryExtractor::new(
                        provider.clone(),
                        config.memory.embedding_dimension,
                    );
                    if let Some(ref embedder) = embed_provider {
                        extractor = extractor.with_embedder(embedder.clone());
                    }
                    Arc::new(extractor)
                } else {
                    Arc::new(crate::RuleBasedExtractor::new())
                };
            let ingestor = crate::MemoryIngestor::new(backend, extractor);
            info!("Memory auto-ingest enabled");
            if let Some(tx) = event_tx {
                let _ = ingestor.spawn(tx.subscribe());
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("MemoryPlugin shutdown");
        Ok(())
    }
}

/// Wrapper so `dyn RerankerProvider` can be stored in [`cog_core::PluginContext`].
pub struct RerankerProviderHolder(pub Arc<dyn crate::RerankerProvider>);

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "memory",
    requires: &[],
    optional_requires: &[],
    provides: &[
        "MemoryBackend",
        "MemoryIngestor",
        "EmbeddingProvider",
        "RerankerProvider",
        "Observable",
    ],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "ExplainPool",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MetricsBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "VectorBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ObjectBackend",
            required: false,
        },
    ],
    factory: || Box::new(MemoryPlugin::new()),
};
