//! Memory plugin — implements [`cog_core::SystemPlugin`].

use cog_core::EmbeddingProvider;
use std::sync::Arc;
use tracing::{info, warn};

/// Memory plugin that self-assembles and publishes memory backend,
/// metrics backend, embedding provider, and reranker provider.
pub struct MemoryPlugin {
    initialized: bool,
    /// Stop handle for the auto-ingest background task. Must be held for the
    /// whole plugin lifetime: the ingestor stops on an explicit signal or
    /// when the event broadcast closes, and `shutdown` uses this handle to
    /// stop it cleanly.
    ingestor_stop: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<()>>>,
}

impl MemoryPlugin {
    /// Create a plugin that will build all memory services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            ingestor_stop: std::sync::Mutex::new(None),
        }
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
            memory_embedding_dimension,
            strict_persistence,
            load_embedding_model,
            load_reranker_model,
        ) = {
            let config = ctx.config();
            // memory 是 cog-memory 自有配置段，自读 cogneva.json。
            let memory = crate::MemoryConfig::load()?;
            (
                memory.enabled,
                memory.backend_type.clone(),
                memory.embedding_dimension,
                config.system.strict_persistence,
                memory.load_embedding_model,
                memory.load_reranker_model,
            )
        };

        // Consume PostgreSQL explain pool (published by StoragePlugin).
        let pg_pool_explain = ctx
            .consume::<cog_storage::ExplainPool>()
            .and_then(|p| p.0.clone());

        // ── Metrics backend ──
        let metrics_backend = ctx
            .consume_service::<dyn cog_core::MetricsBackend>()
            .unwrap_or_else(|| {
                warn!("No MetricsBackend published by StoragePlugin; using no-op fallback");
                Arc::new(crate::NoopMetricsBackend::new())
            });

        // ── Vector backend ──
        let vector_backend = ctx.consume_service::<dyn cog_core::VectorBackend>();

        // ── Memory backend ──
        if !memory_enabled {
            info!("Memory backend disabled; skipping embedding/reranker initialization");
            self.initialized = true;
            return Ok(());
        }

        let vector_backend: Arc<dyn cog_core::VectorBackend> = match vector_backend {
            Some(b) => b,
            None => {
                if strict_persistence {
                    return Err(cog_core::SFError::Config(
                        "No VectorBackend published by StoragePlugin (strict_persistence=true)"
                            .into(),
                    ));
                }
                warn!("No VectorBackend published by StoragePlugin; using no-op fallback");
                Arc::new(crate::NoopVectorBackend::new())
            }
        };

        // ── Embedding provider ──
        // Built before the memory backend so an explicit ingest can embed with
        // the real model. Absent, entries are stored with no vector at all.
        let embed_provider: Option<Arc<dyn cog_core::EmbeddingProvider>> = if load_embedding_model {
            match crate::FastEmbedProvider::try_new() {
                Ok(p) => {
                    info!("FastEmbed BGE-M3 loaded: {} dim", p.dimension());
                    Some(Arc::new(p))
                }
                Err(e) => {
                    warn!("Failed to load BGE-M3 embedding model: {}", e);
                    None
                }
            }
        } else {
            // 关掉是因为这台机器吃不下：权重约 2.1GiB 常驻（dense 与 sparse 各开一个
            // session，等于同一份权重占两遍），且本地无缓存时会向 HuggingFace 发一次
            // 没有超时的拉取，离线集群里会把 init 挂死。权重不进镜像，由部署侧以
            // 只读卷/共享目录提供；确认权重就位且内存足够再打开即恢复向量能力。
            info!(
                "Embedding model loading disabled (memory.load_embedding_model=false); \
                 published vectors stay unavailable"
            );
            None
        };

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
                    if let Some(ref embedder) = embed_provider {
                        composite = composite.with_embedder(embedder.clone());
                    }

                    // Each layer's own backend loads its own state. The composite's
                    // `set_persist_dir`/`load` helpers only drive the default
                    // in-memory backends, and both are replaced right here, so
                    // calling them would do nothing but look like recovery.
                    let mut summary_backend = crate::VectorSummaryBackend::new(
                        vector_backend.clone(),
                        memory_embedding_dimension,
                    );
                    match pg_pool_explain {
                        Some(ref pool) => {
                            let entry_store = crate::PostgresEntryStore::from_pool(pool.clone());
                            match entry_store.init_table().await {
                                Ok(()) => {
                                    info!("PostgresEntryStore initialized for summary layer");
                                    summary_backend =
                                        summary_backend.with_store(Arc::new(entry_store));
                                }
                                Err(e) => {
                                    if strict_persistence {
                                        return Err(cog_core::SFError::Config(format!(
                                            "PostgresEntryStore init_table failed (strict_persistence=true): {}",
                                            e
                                        )));
                                    }
                                    warn!(
                                        "PostgresEntryStore init_table failed: {}. Summary layer will use memory fallback.",
                                        e
                                    );
                                }
                            }
                        }
                        None => {
                            if strict_persistence {
                                return Err(cog_core::SFError::Config(
                                    "No ExplainPool published by StoragePlugin; the summary layer of composite memory has no persistent entry store (strict_persistence=true)".into(),
                                ));
                            }
                            warn!("No ExplainPool published by StoragePlugin; summary layer will use memory fallback");
                        }
                    }
                    if let Err(e) = summary_backend.load().await {
                        if strict_persistence {
                            return Err(cog_core::SFError::Config(format!(
                                "Summary layer failed to load persisted entries (strict_persistence=true): {}",
                                e
                            )));
                        }
                        warn!("Summary layer failed to load persisted entries: {}", e);
                    }
                    composite = composite.with_summary_backend(Arc::new(summary_backend));

                    match pg_pool_explain {
                        Some(ref pool) => {
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
                        None => {
                            if strict_persistence {
                                return Err(cog_core::SFError::Config(
                                    "No ExplainPool published by StoragePlugin; the schema layer of composite memory has no persistent store (strict_persistence=true)".into(),
                                ));
                            }
                            warn!("No ExplainPool published by StoragePlugin; schema layer will use memory fallback");
                        }
                    }
                    info!("CompositeMemoryBackend enabled");
                    Arc::new(crate::MetricsInstrumentedMemoryBackend::new(
                        Arc::new(composite),
                        metrics_backend.clone(),
                    ))
                }
                // Non-durable by construction. Gated by the deployment config
                // rather than by strict_persistence: choosing this backend is a
                // deliberate "lose it on restart" decision, not a degradation
                // the strict flag is meant to catch.
                "memory" => {
                    info!("MemoryMemoryBackend enabled (in-process, not durable)");
                    Arc::new(crate::MetricsInstrumentedMemoryBackend::new(
                        Arc::new(crate::MemoryMemoryBackend::new()),
                        metrics_backend.clone(),
                    ))
                }
                other => {
                    return Err(cog_core::SFError::Config(format!(
                        "unknown memory.backend_type {other:?}; expected \"composite\" or \"memory\""
                    )));
                }
            };
            Some(backend)
        };

        // ── Reranker provider ──
        let reranker_provider: Option<Arc<dyn crate::RerankerProvider>> = if load_reranker_model {
            match crate::FastEmbedRerankerProvider::try_new() {
                Ok(p) => {
                    info!("FastEmbed BGE-Reranker-V2-M3 loaded");
                    Some(Arc::new(p))
                }
                Err(e) => {
                    warn!("Failed to load BGE-Reranker-V2-M3: {}", e);
                    None
                }
            }
        } else {
            info!(
                "Reranker model loading disabled (memory.load_reranker_model=false); \
                 no reranker published"
            );
            None
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
        ctx.publish_observable(crate::observable::global_observable());
        info!("MemoryPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // memory 是 cog-memory 自有配置段，自读 cogneva.json。
        let memory = crate::MemoryConfig::load()?;
        if !memory.enabled || !memory.auto_ingest {
            return Ok(());
        }

        let memory_backend = ctx.consume_service::<dyn cog_core::MemoryBackend>();
        let metrics_backend = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        let embed_provider = ctx.consume_service::<dyn cog_core::EmbeddingProvider>();
        let llm_provider = ctx.consume_service::<dyn cog_core::LlmClient>();
        let event_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            .map(|h| (*h).clone());

        if let Some(backend) = memory_backend {
            let extractor: Arc<dyn cog_core::MemoryExtractor> =
                if let Some(ref provider) = llm_provider {
                    let mut extractor = crate::LlmMemoryExtractor::new(provider.clone());
                    if let Some(ref embedder) = embed_provider {
                        extractor = extractor.with_embedder(embedder.clone());
                    }
                    Arc::new(extractor)
                } else {
                    Arc::new(crate::RuleBasedExtractor::new())
                };
            let mut ingestor =
                crate::MemoryIngestor::new(backend, extractor).with_config((&memory.ingest).into());
            if let Some(ref metrics) = metrics_backend {
                ingestor = ingestor.with_metrics(metrics.clone());
            }
            // 池状态来源由 supervisor 在 init 发布（init_all 先于 start_all），
            // 缺席时闸门退化为纯本地判据：上游断供仍会被拦住，只是要花掉阈值
            // 次尝试才知道。
            match ctx.consume_service::<dyn cog_core::LlmPoolStatusSource>() {
                Some(source) => ingestor = ingestor.with_pool_status_source(source),
                None => warn!(
                    "No LlmPoolStatusSource published; ingest pull gate falls back to local failure counting"
                ),
            }
            info!("Memory auto-ingest enabled");
            // 事件面开关与发布侧同开同关：开启后 AgentEnd 只上持久总线，
            // 摄取器必须从总线消费（ack 后完成），广播上不再有活体 AgentEnd。
            let mbc = ctx.config().multi_backend_consumer.clone();
            if mbc.enabled && mbc.events_on_bus {
                match ctx.consume::<cog_core::EventPlaneBackend>() {
                    Some(plane) => {
                        let stop_handle = ingestor.spawn_bus(plane.0.clone(), mbc.channel.clone());
                        match self.ingestor_stop.lock() {
                            Ok(mut slot) => *slot = Some(stop_handle),
                            Err(poisoned) => {
                                *poisoned.into_inner() = Some(stop_handle);
                            }
                        }
                        info!("Memory auto-ingest consuming from event plane bus");
                    }
                    None => {
                        if ctx.config().system.strict_persistence {
                            return Err(cog_core::SFError::Config(
                                "events_on_bus=true but EventPlaneBackend unavailable (strict_persistence=true)"
                                    .into(),
                            ));
                        }
                        warn!(
                            "events_on_bus=true but EventPlaneBackend unavailable; falling back to broadcast ingest"
                        );
                        if let Some(tx) = event_tx {
                            let stop_handle = ingestor.spawn(tx.subscribe());
                            match self.ingestor_stop.lock() {
                                Ok(mut slot) => *slot = Some(stop_handle),
                                Err(poisoned) => {
                                    *poisoned.into_inner() = Some(stop_handle);
                                }
                            }
                        }
                    }
                }
            } else if let Some(tx) = event_tx {
                // The ingestor task exits as soon as the returned stop handle
                // is dropped, so it must be kept alive for the plugin's whole
                // lifetime; `shutdown` later signals it through this handle.
                let stop_handle = ingestor.spawn(tx.subscribe());
                match self.ingestor_stop.lock() {
                    Ok(mut slot) => *slot = Some(stop_handle),
                    Err(poisoned) => {
                        *poisoned.into_inner() = Some(stop_handle);
                    }
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        let stop_handle = match self.ingestor_stop.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = stop_handle {
            // Signal the ingestor task to exit; dropping the handle would
            // also stop it, but an explicit signal keeps the intent clear.
            let _ = handle.send(()).await;
        }
        info!("MemoryPlugin shutdown");
        Ok(())
    }
}

/// Wrapper so `dyn RerankerProvider` can be stored in [`cog_core::PluginContext`].
pub struct RerankerProviderHolder(pub Arc<dyn crate::RerankerProvider>);

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "memory",
    // Layers initialise in parallel, so consuming storage's ExplainPool /
    // VectorBackend / ObjectBackend without declaring the dependency is a
    // race with storage's own init. Storage defines no dependencies, so this
    // edge cannot form a cycle.
    requires: &["storage"],
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
        cog_core::ConsumeSpec {
            type_name: "EventPlaneBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "LlmPoolStatusSource",
            required: false,
        },
    ],
    factory: || Box::new(MemoryPlugin::new()),
};
