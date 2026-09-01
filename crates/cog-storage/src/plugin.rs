//! Storage plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

use cog_core::storage::{ConfigPool, ExplainPool, MessagesPool, RedisClient, UsersPool};

/// Storage plugin that creates and publishes PostgreSQL pools and Redis client.
pub struct StoragePlugin {
    initialized: bool,
}

impl StoragePlugin {
    /// Create the storage plugin.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for StoragePlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for StoragePlugin {
    fn name(&self) -> &'static str {
        "storage"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Snapshot all config values we need so we can drop the immutable borrow
        // before calling `ctx.publish` (which needs mutable borrow).
        let (
            db_url,
            strict_persistence,
            pg_max_connections,
            pg_min_connections,
            pg_acquire_timeout_secs,
            pg_idle_timeout_secs,
            redis_url,
            raw_logger_config,
            _tier_migrator_enabled,
            data_dir,
        ) = {
            let config = ctx.config();
            let db_url = std::env::var("COGNEVA_DATABASE_URL")
                .ok()
                .or_else(|| {
                    config
                        .providers
                        .db
                        .options
                        .get("connection")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .or_else(|| {
                    config
                        .providers
                        .pg
                        .options
                        .get("connection")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                });
            (
                db_url,
                config.system.strict_persistence,
                config.system.pg_max_connections,
                config.system.pg_min_connections,
                config.system.pg_acquire_timeout_secs,
                config.system.pg_idle_timeout_secs,
                config.dag_executor.redis_url.clone(),
                config.raw_logger.clone(),
                config.tier_migrator.enabled,
                config.app.data_dir.clone(),
            )
        };

        // ── PostgreSQL pools ──
        let (users_pool, messages_pool, config_pool, explain_pool) = if let Some(url) = db_url {
            let migrator = crate::migrate::Migrator::default();
            match migrator.run(&url).await {
                Ok(()) => info!("Database migrations applied successfully"),
                Err(e) => {
                    if strict_persistence {
                        return Err(cog_core::SFError::Config(format!(
                            "Database migration failed (strict_persistence=true): {}",
                            e
                        )));
                    }
                    warn!("Database migration failed: {}", e);
                }
            }

            let users_url = url.clone();
            let messages_url = url.clone();
            let config_url = url.clone();
            let explain_url = url.clone();

            async fn connect_pool(
                u: String,
                strict_persistence: bool,
                pg_max_connections: u32,
                pg_min_connections: u32,
                pg_acquire_timeout_secs: u64,
                pg_idle_timeout_secs: u64,
            ) -> Result<Option<sqlx::PgPool>, String> {
                match sqlx::postgres::PgPoolOptions::new()
                    .max_connections(pg_max_connections)
                    .min_connections(pg_min_connections)
                    .acquire_timeout(std::time::Duration::from_secs(pg_acquire_timeout_secs))
                    .idle_timeout(Some(std::time::Duration::from_secs(pg_idle_timeout_secs)))
                    .connect(&u)
                    .await
                {
                    Ok(pool) => {
                        info!(
                            "PostgreSQL pool created: {}",
                            u.rsplit('/').next().unwrap_or(&u)
                        );
                        Ok(Some(pool))
                    }
                    Err(e) => {
                        if strict_persistence {
                            return Err(format!(
                                "Failed to create PostgreSQL pool for {} (strict_persistence=true): {}",
                                u, e
                            ));
                        }
                        warn!(
                            "Failed to create PostgreSQL pool for {}: {}. Falling back to memory.",
                            u, e
                        );
                        Ok(None)
                    }
                }
            }

            let users_pool = connect_pool(
                users_url,
                strict_persistence,
                pg_max_connections,
                pg_min_connections,
                pg_acquire_timeout_secs,
                pg_idle_timeout_secs,
            )
            .await
            .map_err(cog_core::SFError::Config)?;
            let messages_pool = connect_pool(
                messages_url,
                strict_persistence,
                pg_max_connections,
                pg_min_connections,
                pg_acquire_timeout_secs,
                pg_idle_timeout_secs,
            )
            .await
            .map_err(cog_core::SFError::Config)?;
            let config_pool = connect_pool(
                config_url,
                strict_persistence,
                pg_max_connections,
                pg_min_connections,
                pg_acquire_timeout_secs,
                pg_idle_timeout_secs,
            )
            .await
            .map_err(cog_core::SFError::Config)?;
            let explain_pool = connect_pool(
                explain_url,
                strict_persistence,
                pg_max_connections,
                pg_min_connections,
                pg_acquire_timeout_secs,
                pg_idle_timeout_secs,
            )
            .await
            .map_err(cog_core::SFError::Config)?;

            // 所有池子指向同一个数据库；如果某个池子创建失败（连接抖动、
            // 超时），复用任意一个成功的池子，避免部分后端落盘、部分后端
            // 回退到内存的不一致状态。这是 self_evolution 任务持久化的关键。
            let any_pool = users_pool
                .as_ref()
                .or(messages_pool.as_ref())
                .or(config_pool.as_ref())
                .or(explain_pool.as_ref())
                .cloned();
            let users_pool = users_pool.or_else(|| any_pool.clone());
            let messages_pool = messages_pool.or_else(|| any_pool.clone());
            let config_pool = config_pool.or_else(|| any_pool.clone());
            let explain_pool = explain_pool.or_else(|| any_pool.clone());

            (users_pool, messages_pool, config_pool, explain_pool)
        } else {
            if strict_persistence {
                return Err(cog_core::SFError::Config(
                    "PostgreSQL DSN not configured (strict_persistence=true). Set COGNEVA_DATABASE_URL or providers.pg.options.connection".into()
                ));
            }
            warn!("PostgreSQL DSN not configured. Persistence backends will use memory fallback.");
            (None, None, None, None)
        };

        // ── GuardAuditRecorder (PostgreSQL) ──
        if let Some(ref pool) = explain_pool.clone() {
            let recorder = crate::PostgresAuditRecorder::new(pool.clone());
            match recorder.init_schema().await {
                Ok(()) => {
                    info!("PostgresAuditRecorder initialized");
                    let recorder_dyn: Arc<dyn cog_core::GuardAuditRecorder> = Arc::new(recorder);
                    ctx.publish_service(recorder_dyn);
                }
                Err(e) => {
                    if strict_persistence {
                        return Err(cog_core::SFError::Config(
                            format!("PostgresAuditRecorder init_schema failed (strict_persistence=true): {}", e)
                        ));
                    }
                    warn!(
                        "PostgresAuditRecorder init_schema failed: {}. Guard audit disabled.",
                        e
                    );
                }
            }
        }

        // ── Metrics backend (must happen before explain_pool is moved into ExplainPool) ──
        let metrics_backend: Arc<dyn cog_core::MetricsBackend> = if let Some(ref pool) =
            explain_pool.clone()
        {
            let backend = crate::PostgresMetricsBackend::new(pool.clone());
            match backend.init_schema().await {
                Ok(()) => {
                    info!("PostgresMetricsBackend initialized");
                    Arc::new(backend)
                }
                Err(e) => {
                    if strict_persistence {
                        return Err(cog_core::SFError::Config(
                            format!("PostgresMetricsBackend init_schema failed (strict_persistence=true): {}", e)
                        ));
                    }
                    warn!(
                        "PostgresMetricsBackend init_schema failed: {}. Falling back to memory.",
                        e
                    );
                    Arc::new(crate::MemoryMetricsBackend::new())
                }
            }
        } else {
            if strict_persistence {
                return Err(cog_core::SFError::Config(
                    "PostgreSQL pool not available for MetricsBackend (strict_persistence=true)"
                        .into(),
                ));
            }
            Arc::new(crate::MemoryMetricsBackend::new())
        };
        ctx.publish_service(metrics_backend);
        info!("StoragePlugin metrics backend published");

        // ── CheckpointStore (must happen before explain_pool is moved into ExplainPool) ──
        let snapshot_store: Arc<dyn cog_core::CheckpointStore> = {
            let snapshot_dir = format!("{}/snapshots", raw_logger_config.base_dir);
            let _ = tokio::fs::create_dir_all(&snapshot_dir).await;
            if let Some(ref pool) = explain_pool {
                let store = crate::PostgresSnapshotStore::new(pool.clone());
                match store.init_schema().await {
                    Ok(()) => {
                        info!("PostgresSnapshotStore initialized");
                        Arc::new(store)
                    }
                    Err(e) => {
                        warn!(
                            "PostgresSnapshotStore init_schema failed: {}. Falling back to file.",
                            e
                        );
                        Arc::new(crate::FileSnapshotStore::new(&snapshot_dir))
                    }
                }
            } else {
                Arc::new(crate::FileSnapshotStore::new(&snapshot_dir))
            }
        };
        ctx.publish_service(snapshot_store);
        info!("StoragePlugin checkpoint store published");

        // ── 审计流（审计 3.5：不可篡改哈希链，文件追加式）──
        let audit_path = format!("{}/audit/audit.jsonl", raw_logger_config.base_dir);
        match crate::audit_stream::FileAuditStream::open(&audit_path).await {
            Ok(stream) => {
                let stream: Arc<dyn cog_core::AuditStream> = Arc::new(stream);
                ctx.publish_service(stream);
                info!(path = %audit_path, "StoragePlugin audit stream published");
            }
            Err(e) => {
                // 链损坏是严重信号：非严格模式降级为无审计并告警，
                // 严格模式直接失败以便人工介入。
                if strict_persistence {
                    return Err(e);
                }
                warn!(error = %e, "audit stream unavailable; audit disabled");
            }
        }

        // ── State backend (for supervisor / orchestrator) ──
        let mut state_backend_pg = false;
        let state_backend: Arc<dyn cog_core::StateBackend> = if let Some(ref pool) = config_pool {
            let backend = crate::PostgresStateBackend::new(pool.clone());
            if let Err(e) = backend.init_schema().await {
                if strict_persistence {
                    return Err(cog_core::SFError::Config(format!(
                        "PostgresStateBackend init_schema failed (strict_persistence=true): {}",
                        e
                    )));
                }
                warn!(
                    "PostgresStateBackend init_schema failed: {}. Falling back to memory.",
                    e
                );
                Arc::new(crate::MemoryStateBackend::new())
            } else {
                info!("PostgresStateBackend initialized");
                state_backend_pg = true;
                Arc::new(backend)
            }
        } else {
            if strict_persistence {
                return Err(cog_core::SFError::Config(
                    "PostgreSQL pool not available for StateBackend (strict_persistence=true)"
                        .into(),
                ));
            }
            Arc::new(crate::MemoryStateBackend::new())
        };
        ctx.publish_service(state_backend.clone());
        info!("StoragePlugin state backend published");

        // ── Promotion ledger（晋级台账：配额/熔断/审计事实源，与 state
        //    backend 同一存储，避免双副本读到不同账本） ──
        let promotion_ledger: Arc<dyn cog_core::PromotionLedger> = if state_backend_pg {
            Arc::new(crate::PostgresStateBackend::new(
                config_pool.as_ref().expect("checked").clone(),
            ))
        } else {
            Arc::new(crate::MemoryStateBackend::new())
        };
        ctx.publish_service(promotion_ledger);
        info!("StoragePlugin promotion ledger published");

        // ── ObservabilityGateway (must happen before explain_pool is moved into ExplainPool) ──
        let observability_gateway: Arc<dyn cog_core::ObservabilityGateway> = if let Some(ref pool) =
            explain_pool
        {
            let gateway = crate::PostgresObservabilityGateway::new(pool.clone())
                .with_event_channel_capacity(
                    ctx.config().system.observability_event_channel_capacity,
                );
            match gateway.init_schema().await {
                Ok(()) => {
                    info!("PostgresObservabilityGateway initialized");
                    Arc::new(gateway)
                }
                Err(e) => {
                    warn!("PostgresObservabilityGateway init_schema failed: {}. Falling back to memory.", e);
                    Arc::new(crate::MemoryObservabilityGateway::new(
                        state_backend.clone(),
                    ))
                }
            }
        } else {
            Arc::new(crate::MemoryObservabilityGateway::new(
                state_backend.clone(),
            ))
        };
        ctx.publish_service(observability_gateway);
        info!("StoragePlugin observability gateway published");

        ctx.publish(Arc::new(UsersPool(users_pool)));
        ctx.publish(Arc::new(MessagesPool(messages_pool)));
        ctx.publish(Arc::new(ConfigPool(config_pool.clone())));
        ctx.publish(Arc::new(ExplainPool(explain_pool)));
        info!("StoragePlugin PostgreSQL pools published");

        // ── Vector backend ──
        let vector_backend: Arc<dyn cog_core::VectorBackend> =
            if let Ok(qdrant_url) = std::env::var("QDRANT_URL") {
                match crate::QdrantVectorBackend::try_new(&qdrant_url).await {
                    Ok(backend) => {
                        info!("QdrantVectorBackend connected");
                        Arc::new(backend)
                    }
                    Err(e) => {
                        if strict_persistence {
                            return Err(cog_core::SFError::Config(format!(
                            "QdrantVectorBackend connection failed (strict_persistence=true): {}",
                            e
                        )));
                        }
                        warn!(
                            "QdrantVectorBackend connection failed: {}. Falling back to memory.",
                            e
                        );
                        Arc::new(crate::MemoryVectorBackend::new())
                    }
                }
            } else {
                if strict_persistence {
                    return Err(cog_core::SFError::Config(
                        "QDRANT_URL not configured (strict_persistence=true)".into(),
                    ));
                }
                Arc::new(crate::MemoryVectorBackend::new())
            };
        ctx.publish_service(vector_backend);
        info!("StoragePlugin vector backend published");

        // ── Redis client ──
        let redis_client = redis::Client::open(redis_url.clone())
            .map_err(|e| cog_core::SFError::Config(format!("Redis client open failed: {}", e)))?;
        let redis_conn = redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| cog_core::SFError::Config(format!("Redis connection failed: {}", e)))?;
        info!("Redis connection established");
        ctx.publish(Arc::new(RedisClient(redis_client)));

        // ── AgentRegistry (Redis) ──
        let agent_registry = crate::RedisAgentRegistry::new(redis_conn)
            .with_ttl_seconds(ctx.config().agent.registration_ttl_secs);
        let agent_registry_dyn: Arc<dyn cog_core::AgentRegistry> = Arc::new(agent_registry);
        ctx.publish_service(agent_registry_dyn);
        info!("StoragePlugin agent registry published");

        // ── HookArchive (PostgreSQL) ──
        if let Some(ref pool) = config_pool {
            let archive = crate::PostgresHookArchive::new(pool.clone());
            match archive.init_schema().await {
                Ok(()) => {
                    info!("PostgresHookArchive initialized");
                    let archive_dyn: Arc<dyn cog_core::HookArchive> = Arc::new(archive);
                    ctx.publish_service(archive_dyn);
                }
                Err(e) => {
                    if strict_persistence {
                        return Err(cog_core::SFError::Config(format!(
                            "PostgresHookArchive init_schema failed (strict_persistence=true): {}",
                            e
                        )));
                    }
                    warn!(
                        "PostgresHookArchive init_schema failed: {}. Archive disabled.",
                        e
                    );
                }
            }
        }

        // ── Media backend (LiveKit) ──
        {
            let media_config = &ctx.config().providers.media;
            if media_config.enabled {
                let provider = media_config.provider.as_str();
                #[cfg(feature = "livekit")]
                if provider == "livekit" {
                    let api_key = std::env::var("LIVEKIT_API_KEY")
                        .ok()
                        .or_else(|| {
                            media_config
                                .options
                                .get("api_key")
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        })
                        .unwrap_or_default();
                    let api_secret = std::env::var("LIVEKIT_API_SECRET")
                        .ok()
                        .or_else(|| {
                            media_config
                                .options
                                .get("api_secret")
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        })
                        .unwrap_or_default();
                    let server_url = std::env::var("LIVEKIT_SERVER_URL")
                        .ok()
                        .or_else(|| {
                            media_config
                                .options
                                .get("server_url")
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        })
                        .unwrap_or_default();
                    if api_key.is_empty() || api_secret.is_empty() || server_url.is_empty() {
                        warn!("LiveKit media backend enabled but missing api_key, api_secret, or server_url");
                    } else {
                        let backend =
                            crate::LiveKitMediaBackend::new(cog_core::MediaBackendConfig {
                                api_key,
                                api_secret,
                                server_url,
                            });
                        info!("LiveKit media backend initialized");
                        let backend_dyn: Arc<dyn cog_core::MediaBackend> = Arc::new(backend);
                        ctx.publish_service(backend_dyn);
                    }
                }
                #[cfg(not(feature = "livekit"))]
                if provider == "livekit" {
                    warn!("LiveKit media backend requested but crate compiled without livekit feature");
                } else {
                    warn!(
                        "Unknown media provider: {}. Media backend disabled.",
                        provider
                    );
                }
            }
        }

        // ── RawLogger ──
        let raw_logger: Arc<dyn cog_core::RawLogger> = if !raw_logger_config.enabled {
            info!("Raw logger disabled (NoopRawLogger)");
            Arc::new(crate::NoopRawLogger::new())
        } else {
            let logger_config = cog_core::raw_logger::RawLoggerConfig {
                enabled: true,
                base_dir: raw_logger_config.base_dir.clone(),
                max_buffer_size: raw_logger_config.max_buffer_size,
                format: raw_logger_config.format,
                zstd_level: raw_logger_config.zstd_level,
            };
            match crate::FileRawLogger::new(
                logger_config,
                Arc::new(cog_protocol::convert::ProtoCodec),
            )
            .await
            {
                Ok(logger) => {
                    info!("FileRawLogger enabled");
                    Arc::new(logger)
                }
                Err(e) => {
                    warn!(
                        "Failed to create FileRawLogger: {}. Falling back to Noop.",
                        e
                    );
                    Arc::new(crate::NoopRawLogger::new())
                }
            }
        };
        ctx.publish_service(raw_logger);
        info!("StoragePlugin raw logger published");

        // ── TraceStore ──
        let trace_store: Arc<dyn cog_core::TraceStore> =
            match std::env::var("COGNEVA_TRACE_STORE_BACKEND").as_deref() {
                Ok("redis") => match crate::RedisBackend::new(&redis_url).await {
                    Ok(backend) => {
                        info!("RedisTraceStore initialized (hot tier)");
                        Arc::new(crate::RedisTraceStore::new(backend, None))
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create RedisTraceStore: {}. Falling back to FileTraceStore.",
                            e
                        );
                        let trace_dir = format!("{}/traces", raw_logger_config.base_dir);
                        let _ = tokio::fs::create_dir_all(&trace_dir).await;
                        Arc::new(crate::FileTraceStore::new(&trace_dir))
                    }
                },
                Ok("s3") => {
                    warn!(
                        "S3TraceStore requires S3 configuration. Falling back to FileTraceStore."
                    );
                    let trace_dir = format!("{}/traces", raw_logger_config.base_dir);
                    let _ = tokio::fs::create_dir_all(&trace_dir).await;
                    Arc::new(crate::FileTraceStore::new(&trace_dir))
                }
                _ => {
                    let trace_dir = format!("{}/traces", raw_logger_config.base_dir);
                    if let Err(e) = tokio::fs::create_dir_all(&trace_dir).await {
                        warn!("Failed to create trace dir {}: {}", trace_dir, e);
                    }
                    info!("FileTraceStore initialized at {}", trace_dir);
                    Arc::new(crate::FileTraceStore::new(&trace_dir))
                }
            };
        ctx.publish_service(trace_store);
        info!("StoragePlugin trace store published");

        // ── RawLogIndexStore ──
        let raw_log_index_store: Arc<dyn cog_core::RawLogIndexStore> =
            Arc::new(crate::MemoryRawLogIndexStore::new());
        ctx.publish_service(raw_log_index_store);
        info!("StoragePlugin raw log index store published");

        // ── ObjectBackend (singleton for all modules) ──
        let object_backend: Arc<dyn cog_core::ObjectBackend> = Arc::new(
            crate::FileObjectBackend::new(std::path::Path::new(&data_dir)),
        );
        ctx.publish_service(object_backend);
        info!("StoragePlugin object backend published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // ── Tier migrator (raw-log cold-tier migration) ──
        if ctx.config().tier_migrator.enabled {
            if let Some(store) = ctx.consume_service::<dyn cog_core::RawLogIndexStore>() {
                let cold_dir = format!("{}/cold", ctx.config().raw_logger.base_dir);
                let base_dir = ctx.config().raw_logger.base_dir.clone();
                let tier_config = ctx.config().tier_migrator.clone();
                let metrics = ctx.consume_service::<dyn cog_core::MetricsBackend>();
                let shutdown = ctx
                    .consume::<cog_core::ShutdownSignal>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::create_dir_all(&cold_dir).await {
                        warn!("Failed to create cold-tier dir {}: {}", cold_dir, e);
                    }
                    let object_backend: Arc<dyn cog_core::ObjectBackend> =
                        Arc::new(crate::FileObjectBackend::new(&cold_dir));
                    let mut migrator = crate::TierMigrator::new(
                        base_dir,
                        crate::tier_policy_from_config(&tier_config),
                        object_backend,
                        store,
                    );
                    if let Some(mb) = metrics {
                        migrator = migrator.with_metrics(mb);
                    }
                    let _handle = Arc::new(migrator).spawn(shutdown);
                    info!("TierMigrator started");
                });
            }
        } else {
            info!("TierMigrator disabled");
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("StoragePlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "storage",
    requires: &[],
    optional_requires: &[],
    provides: &[
        "RawLogger",
        "TraceStore",
        "StateBackend",
        "RedisClient",
        "VectorBackend",
        "AgentRegistry",
        "ObjectBackend",
        "CheckpointStore",
        "RawLogIndexStore",
        "GuardAuditRecorder",
        "MetricsBackend",
        "ObservabilityGateway",
        "HookArchive",
        "MediaBackend",
    ],
    consumes: &[],
    factory: || Box::new(StoragePlugin::new()),
};
