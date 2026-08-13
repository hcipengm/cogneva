//! Cogneva library — pure wiring and composition root.
//! This crate is both a library (for integration tests) and a binary.
//! All modules are declared here so tests can access them via `cogneva::`.

pub mod assembly;
pub mod bootstrap;
pub mod config_loader;
pub mod config_watcher;
pub mod daemon;
pub mod hot_reload;
pub mod pidfile;
pub mod platform;
pub mod plugin_registry;
pub mod shutdown_coordinator;
pub mod validate_config;

#[cfg(windows)]
pub mod windows_service;

/// Global shutdown coordinator reference for Windows Service STOP handling.
pub static SHUTDOWN: std::sync::OnceLock<shutdown_coordinator::ShutdownCoordinator> =
    std::sync::OnceLock::new();

use std::sync::Arc;
use tracing::warn;

/// Pure wiring logic: initialize components and connect them together.
pub async fn run_app() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = assembly::infra::load_and_normalize_config();
    // 解析 secret://env|file|vault 引用（审计 3.3）。
    config_loader::resolve_secret_refs(&mut config).await?;

    let ctx = cog_core::PluginContext::new(config.core.clone());
    ctx.publish(Arc::new(config.core.clone()));

    let (daemon, _pid_file) = assembly::infra::init_daemon_and_pidfile();

    let task_event_tx = tokio::sync::broadcast::channel::<cog_core::TaskEvent>(
        config.system.task_event_channel_capacity,
    )
    .0;
    ctx.publish(Arc::new(task_event_tx.clone()));

    // Create shutdown coordinator early (before plugin init)
    let shutdown =
        shutdown_coordinator::ShutdownCoordinator::new(config.core.system.shutdown_timeout_ms);
    let _ = SHUTDOWN.set(shutdown.clone());
    let shutdown = Arc::new(shutdown);

    let mut plugin_runner = plugin_registry::register_all()?;

    // Config-driven plugin filtering
    if let Some(ref enabled) = config.system.enabled_plugins {
        plugin_runner.retain(|name| enabled.contains(&name.to_string()));
    } else if !config.system.disabled_plugins.is_empty() {
        plugin_runner.retain(|name| !config.system.disabled_plugins.contains(&name.to_string()));
    }

    // Validate that filtering did not break required dependencies
    plugin_runner.validate_after_filter()?;

    plugin_runner.init_all(&ctx).await?;

    // Register shutdown hook now that RawLogger is available
    if let Some(raw_logger) = ctx.consume_service::<dyn cog_core::RawLogger>() {
        let logger = raw_logger.clone();
        shutdown.register_hook(move || {
            let l = logger.clone();
            async move {
                tracing::info!("Shutdown hook: flushing raw logger");
                if let Err(e) = l.shutdown().await {
                    tracing::warn!("Raw logger shutdown failed: {}", e);
                }
            }
        });
    }

    // Publish shutdown infrastructure so plugins can consume it
    let (shutdown_broadcast_tx, _shutdown_broadcast_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_signal = shutdown.signal();
    tokio::spawn({
        let sig = shutdown_signal.clone();
        let tx = shutdown_broadcast_tx.clone();
        async move {
            sig.wait().await;
            let _ = tx.send(());
        }
    });
    ctx.publish(shutdown.clone());
    ctx.publish(Arc::new(cog_core::ShutdownBroadcastTx(
        shutdown_broadcast_tx,
    )));
    ctx.publish(Arc::new(shutdown_signal));

    plugin_runner.start_all(&ctx).await?;

    // Wire the LLM client into the gateway's WebSocket chat handler. The llm
    // plugin is not topologically ordered before gateway, so the slot is
    // filled here after every plugin has initialised. The published client is
    // the hot-swappable wrapper, so later config-driven swaps stay effective.
    if let Some(gateway_state) = ctx.consume::<cog_gateway::GatewayState>() {
        let llm = ctx.consume_service::<dyn cog_core::LlmClient>();
        *gateway_state
            .llm_client
            .write()
            .unwrap_or_else(|e| e.into_inner()) = llm;
    }

    daemon.ready();

    // Spawn config hot-reload consumer (direct orchestration, not a plugin)
    let (config_watcher_opt, _notify_watcher) = assembly::infra::init_config_watcher();
    if let Some(watcher) = config_watcher_opt {
        let mut rx = watcher.subscribe();
        let log_handle = ctx
            .consume::<cog_observability::plugin::LogFilterHandleHolder>()
            .map(|h| h.0.clone());
        let supervisor_config_tx = ctx
            .consume::<cog_supervisor::plugin::SupervisorConfigTxHolder>()
            .expect("supervisor config tx")
            .0
            .clone();
        let gateway_state = ctx
            .consume::<cog_gateway::GatewayState>()
            .expect("gateway state");
        let llm_hot_swap = ctx.consume::<cog_llm::HotSwappableLlmClient>();
        let llm_http_client = ctx.consume_service::<dyn cog_core::HttpClient>();

        // llm_routing / tuning 已下沉 cog-llm（core Config 不再聚合）：
        // 初始与每轮热重载都经 cog-llm 自载器读 cogneva.json。
        let initial_llm_routing = cog_llm::LLMRoutingConfig::load()
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        tokio::spawn(async move {
            let mut active_llm_routing = initial_llm_routing;
            while rx.changed().await.is_ok() {
                let new_config = rx.borrow_and_update().clone();
                let mut applied = Vec::new();
                let mut need_restart = Vec::new();

                // 1. log_level
                if let Some(ref handle) = log_handle {
                    if let Ok(new_filter) =
                        tracing_subscriber::EnvFilter::try_new(&new_config.app.log_level)
                    {
                        if let Err(e) = handle.reload(new_filter) {
                            tracing::warn!("Failed to reload log filter: {}", e);
                        } else {
                            applied.push(format!("log_level={}", new_config.app.log_level));
                        }
                    }
                }

                // 2. gateway config
                let (gateway_applied, gateway_restart) =
                    crate::hot_reload::apply_gateway_config_update(
                        &gateway_state.config,
                        &new_config.gateway,
                        &gateway_state.request_timeout_secs,
                        &gateway_state.sandbox_task_timeout_secs,
                    );
                applied.extend(gateway_applied);
                need_restart.extend(gateway_restart.clone());
                if !gateway_restart.is_empty() {
                    tracing::error!(
                        "PORT CHANGE DETECTED: {}. These CANNOT be hot-reloaded. The process MUST be restarted.",
                        gateway_restart.join("; ")
                    );
                }

                // 3. supervisor intervals
                let new_supervisor_cfg: cog_supervisor::SupervisorConfig =
                    new_config.supervisor.clone().into();
                if let Err(e) = supervisor_config_tx.send(new_supervisor_cfg) {
                    tracing::warn!("Failed to send supervisor config reload: {}", e);
                } else {
                    applied.push("supervisor_intervals_updated".to_string());
                }

                // 4. LLM provider hot-swap
                let new_llm_routing = match cog_llm::LLMRoutingConfig::load() {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("llm_routing reload skipped: {}", e);
                        continue;
                    }
                };
                if active_llm_routing != new_llm_routing {
                    let new_stream_capacity = match cog_llm::TuningConfig::load() {
                        Ok(t) => t.stream_capacity,
                        Err(e) => {
                            tracing::warn!("tuning reload skipped: {}", e);
                            continue;
                        }
                    };
                    if let Some(ref llm_hot_swap) = llm_hot_swap {
                        let new_provider = match cog_llm::plugin::build_llm_provider(
                            new_stream_capacity,
                            new_config.system.anthropic_default_max_tokens,
                            &new_llm_routing,
                            llm_http_client.clone(),
                        ) {
                            Ok(p) => Some(p),
                            Err(e) => {
                                tracing::warn!("Failed to reload LLM provider graph: {}", e);
                                None
                            }
                        };
                        if let Some(new_provider) = new_provider {
                            llm_hot_swap.swap(new_provider).await;
                            active_llm_routing = new_llm_routing.clone();
                            applied.push("llm_provider_swapped".to_string());
                        }
                    } else {
                        need_restart
                            .push("llm_config_changed_but_no_provider_initialized".to_string());
                    }
                }

                // 5. metrics
                need_restart.push(format!(
                    "metrics_enabled={}, interval={}s (requires restart)",
                    new_config.metrics.enabled, new_config.metrics.interval_secs
                ));

                if !applied.is_empty() {
                    tracing::info!("Config hot-reloaded (applied): {}", applied.join("; "));
                }
                if !need_restart.is_empty() {
                    tracing::error!(
                        "Config changed but requires restart: {}",
                        need_restart.join("; ")
                    );
                }
            }
        });
    }

    let _signal_handle = shutdown.spawn_signal_listener();
    shutdown.signal().wait().await;
    warn!("Shutdown signal received, stopping plugins...");

    plugin_runner.shutdown_all().await?;
    shutdown.wait_for_shutdown().await;
    warn!("Cogneva shutdown complete");
    Ok(())
}
