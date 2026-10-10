//! Memory plugin — implements [`cog_core::SystemPlugin`].

use cog_core::EmbeddingProvider;
use std::sync::Arc;
use tracing::{info, warn};

/// The summary layer and the embedder that will be handed to it: what the repair
/// pass needs, and what is kept from `init` so it can run in `start`.
type EmbeddingRepair = (Arc<crate::VectorSummaryBackend>, Arc<dyn EmbeddingProvider>);

/// Memory plugin that self-assembles and publishes memory backend,
/// metrics backend, embedding provider, and reranker provider.
pub struct MemoryPlugin {
    initialized: bool,
    /// Stop handle for the auto-ingest background task. Must be held for the
    /// whole plugin lifetime: the ingestor stops on an explicit signal or
    /// when the event broadcast closes, and `shutdown` uses this handle to
    /// stop it cleanly.
    ingestor_stop: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<()>>>,
    /// The summary layer together with the embedder it should be given, kept from
    /// `init` so the repair pass can run once in `start`.
    ///
    /// `None` unless the composite backend was assembled with an entry store and a
    /// model was loaded: the pass exists for rows stored before a model was
    /// configured, so without one it has no vector to give and nothing to do.
    embedding_repair: std::sync::Mutex<Option<EmbeddingRepair>>,
}

impl MemoryPlugin {
    /// Create a plugin that will build all memory services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            ingestor_stop: std::sync::Mutex::new(None),
            embedding_repair: std::sync::Mutex::new(None),
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
            benchmark_isolation,
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
                crate::BenchmarkIsolation::new(
                    memory.ingest.benchmark_namespaces.clone(),
                    memory.ingest.benchmark_canary_markers.clone(),
                ),
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
                    // 两个 session 的成败各自独立：sparse 没起来只是少了混合检索的那一半，
                    // 说出来让读者知道该修什么，而不是让整条嵌入能力跟着一起消失。
                    match p.sparse_status() {
                        Ok(()) => info!(
                            "FastEmbed BGE-M3 sparse session loaded; hybrid retrieval has both \
                             halves"
                        ),
                        Err(reason) => warn!(
                            "FastEmbed BGE-M3 sparse session is absent ({reason}); dense \
                             embedding works, sparse requests will be refused"
                        ),
                    }
                    Some(Arc::new(p))
                }
                Err(e) => {
                    warn!("Failed to load BGE-M3 embedding model: {}", e);
                    None
                }
            }
        } else {
            // 关掉是因为权重不在镜像里：本地无缓存时会向 HuggingFace 发一次没有超时的
            // 拉取，离线集群里会把 init 挂死。部署侧把权重以只读卷挂上、用一个 env
            // 覆盖打开这个开关（`FASTEMBED_CACHE_DIR` 指名那份目录），确认权重就位再
            // 打开即恢复向量能力。dense 与 sparse 两个 session 一起实测峰值 RSS
            // 1.67GiB：两者读同一份权重文件，加第二个几乎不涨。
            info!(
                "Embedding model loading disabled (memory.load_embedding_model=false); \
                 published vectors stay unavailable"
            );
            None
        };

        // 回填通路的两个把手：它的层与它的模型。只有 composite 那一支且模型真的装载了
        // 才有值——其余各支没有「早于模型的旧行」这个前提，也就没有回填这回事。
        let mut embedding_repair: Option<EmbeddingRepair> = None;

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
                    let summary_layer = Arc::new(summary_backend);
                    composite = composite.with_summary_backend(summary_layer.clone());
                    if let Some(ref embedder) = embed_provider {
                        embedding_repair = Some((summary_layer, embedder.clone()));
                    }

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
        let reranker_provider: Option<Arc<dyn cog_core::RerankerProvider>> = if load_reranker_model
        {
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
        // 网关的 memory 写入路由消费的就是这一条，所以排除面必须挂在这里：
        // 这条管道与事件总线那条是两个入口，而「一份载荷从哪条路进来」与
        // 「它是什么数据」无关，只守一条等于只守一半。
        let default_ingestor: Arc<dyn cog_core::MemoryIngestor> = Arc::new(
            crate::IngestionPipeline::new(crate::RuleBasedExtractor::new())
                .with_isolation(benchmark_isolation.clone()),
        );
        ctx.publish_service(default_ingestor);
        info!(
            "MemoryPlugin default ingestor published; benchmark exclusion surface: {}",
            benchmark_isolation.describe()
        );
        ctx.publish_service(metrics_backend);
        info!("MemoryPlugin metrics backend published");
        if let Some(ref p) = embed_provider {
            ctx.publish_service(p.clone());
            info!("MemoryPlugin embed provider published");
        }
        if let Some(ref p) = reranker_provider {
            // 按 trait 对象发布（与 embed provider 同一条路）：消费方在 cog-wiki，
            // 它只依赖 cog-core，认不出本 crate 的具体类型。
            ctx.publish_service::<dyn cog_core::RerankerProvider>(p.clone());
            info!("MemoryPlugin reranker provider published");
        }

        // Observable publish (pin-style)
        ctx.publish_observable(crate::observable::global_observable());
        info!("MemoryPlugin observable published");

        *self
            .embedding_repair
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = embedding_repair;

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // memory 是 cog-memory 自有配置段，自读 cogneva.json。
        let memory = crate::MemoryConfig::load()?;
        let memory_backend = ctx.consume_service::<dyn cog_core::MemoryBackend>();
        let metrics_backend = ctx.consume_service::<dyn cog_core::MetricsBackend>();

        // 维护循环只管已经存下的记忆，与「要不要摄取新事件」无关：只要记忆开着就起。
        // 这是「低价值记忆自动衰减」唯一的生产驱动方——少了它，`decay` 没有调用点。
        if memory.enabled {
            if let (Some(backend), Some(metrics)) =
                (memory_backend.clone(), metrics_backend.clone())
            {
                // 衰减动的是共用条目存储，所以它是一个单写者角色：中介由持有共用库的
                // 插件在 `init` 里发布（每个插件的 init 都排在任一 start 之前）。
                // 取不到时为 None，循环按「没有共用库」自扫——但这条兜底的成因不止
                // 一种（发布方建中介失败也只 warn），所以拿到中介与否不是资格判据：
                // 够不够格由「它的条目存储是不是共享那份」决定，不够格的进程靠不启
                // 这条循环排除在候选集外。
                let role = ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>();
                let shutdown = ctx
                    .consume::<cog_core::ShutdownSignal>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                crate::maintenance::spawn_decay_loop(
                    backend,
                    metrics,
                    memory.maintenance.clone(),
                    role,
                    shutdown,
                );
            } else {
                warn!(
                    "Memory decay maintenance not started: memory backend or metrics backend unavailable"
                );
            }

            // 回填只在 `start` 里跑一次，且只有装载了模型的那一支才拿得到把手：
            // 「早于模型的旧行」这个前提本身由模型是否装载决定，所以判据不另设开关。
            // 它不注册成循环——它不是循环，跑完就停了，注册进存活表反而会在正常结束
            // 之后被读成「这条循环死了」。扫描范围是整张条目表，也就是 `load` 启动时
            // 已经在做的那次读，代价不是新的一类。
            if let Some((summary, embedder)) = self
                .embedding_repair
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                tokio::spawn(async move {
                    match summary.backfill_embeddings(embedder.as_ref()).await {
                        Ok(report) => info!(
                            scanned = report.scanned,
                            backfilled = report.backfilled,
                            already_embedded = report.already_embedded,
                            failed = report.failed,
                            "Summary embedding repair pass finished"
                        ),
                        Err(e) => warn!(
                            "Summary embedding repair pass could not read the entry store, so \
                             no row was given a vector: {e}"
                        ),
                    }
                });
            }
        }

        if !memory.enabled || !memory.auto_ingest {
            return Ok(());
        }

        let embed_provider = ctx.consume_service::<dyn cog_core::EmbeddingProvider>();
        let llm_provider = ctx.consume_service::<dyn cog_core::LlmClient>();
        let event_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            .map(|h| (*h).clone());

        if let Some(backend) = memory_backend {
            let extractor: Arc<dyn cog_core::MemoryExtractor> =
                if let Some(ref provider) = llm_provider {
                    let mut extractor = crate::LlmMemoryExtractor::new(provider.clone())
                        .with_input_budget_tokens(memory.ingest.extraction_input_budget_tokens);
                    if let Some(ref embedder) = embed_provider {
                        extractor = extractor.with_embedder(embedder.clone());
                    }
                    Arc::new(extractor)
                } else {
                    Arc::new(crate::RuleBasedExtractor::new())
                };
            let isolation = crate::BenchmarkIsolation::new(
                memory.ingest.benchmark_namespaces.clone(),
                memory.ingest.benchmark_canary_markers.clone(),
            );
            let mut ingestor = crate::MemoryIngestor::new(backend, extractor).with_config(
                crate::MemoryIngestorConfig {
                    benchmark_isolation: isolation.clone(),
                    ..(&memory.ingest).into()
                },
            );
            info!(
                "Memory ingest exclusion surface (event bus and reconcile path): {}",
                isolation.describe()
            );
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

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "memory",
    // Layers initialise in parallel, so consuming storage's ExplainPool /
    // VectorBackend / ObjectBackend without declaring the dependency is a
    // race with storage's own init. Storage defines no dependencies, so this
    // edge cannot form a cycle.
    requires: &["storage"],
    optional_requires: &[],
    factory: || Box::new(MemoryPlugin::new()),
};
