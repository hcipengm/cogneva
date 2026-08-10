//! Observability plugin — implements [`cog_core::SystemPlugin`].

use chrono::Utc;
use cog_core::alerts::{AlertChannel, AlertEvent, AlertInstance, AlertSeverity, AlertState};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Observability plugin that self-assembles and publishes raw logger, metrics,
/// Loki, ClickHouse, Jaeger, and Elasticsearch services.
pub struct ObservabilityPlugin {
    initialized: bool,
    trace_collector: Option<Arc<crate::snapshot::TraceCollector>>,
    trace_tier_migrator: Option<Arc<crate::snapshot::TraceTierMigrator>>,
}

impl ObservabilityPlugin {
    /// Create a plugin that will build all observability services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            trace_collector: None,
            trace_tier_migrator: None,
        }
    }
}

impl Default for ObservabilityPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for ObservabilityPlugin {
    fn name(&self) -> &'static str {
        "observability"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Snapshot config values to drop immutable borrow before publishing.
        let (
            app_name,
            app_version,
            log_level,
            observability,
            metrics,
            tier_migrator_hot_days,
            tier_migrator_warm_days,
        ) = {
            let config = ctx.config();
            (
                config.app.name.clone(),
                config.app.version.clone(),
                config.app.log_level.clone(),
                // observability 是 cog-observability 自有配置段，自读 cogneva.json。
                crate::ObservabilityExportersConfig::load()?,
                config.metrics.clone(),
                (config.tier_migrator.hot_duration_secs / 86400) as u32,
                (config.tier_migrator.warm_duration_secs / 86400) as u32,
            )
        };

        // Consume HTTP client (published by NetPlugin).
        let http_client: Arc<dyn cog_core::HttpClient> = ctx
            .consume_service::<dyn cog_core::HttpClient>()
            .expect("http client")
            .clone();

        // ── Subscriber (global) ──
        let log_level = std::env::var("RUST_LOG").unwrap_or(log_level);
        let log_format = if log_level == "json" {
            crate::LogFormat::Json
        } else {
            crate::LogFormat::Pretty
        };
        let (jaeger_exporter, log_filter_handle) = if observability.jaeger.enabled {
            let exporter = crate::jaeger::init_jaeger_subscriber(
                &observability.jaeger.endpoint,
                &observability.jaeger.service_name,
                &log_level,
                log_format,
                Some(http_client.clone()),
            );
            (Some(exporter), None)
        } else {
            let handle = crate::init_subscriber(&log_level, log_format);
            (None, Some(handle))
        };
        if let Some(handle) = log_filter_handle {
            ctx.publish(Arc::new(LogFilterHandleHolder(handle)));
            info!("ObservabilityPlugin LogFilterHandle published");
        }

        // ── Raw logger ──
        let raw_logger = ctx
            .consume_service::<dyn cog_core::RawLogger>()
            .expect("raw logger")
            .clone();
        {
            let record = cog_core::RawRecord {
                meta: cog_core::RawMeta {
                    version: "1.0".into(),
                    stream: "system_raw".into(),
                    recorded_at: chrono::Utc::now(),
                    recorded_by: "cogneva".into(),
                    sequence: 0,
                    trace_id: uuid::Uuid::new_v4().to_string(),
                    span_id: None,
                },
                context: cog_core::RawContext::default(),
                payload: cog_core::RawPayload {
                    direction: "internal".into(),
                    transport: "system".into(),
                    format: Some("json".into()),
                    raw: serde_json::json!({
                        "event": "config_loaded",
                        "app_name": app_name,
                        "app_version": app_version,
                        "log_level": log_level,
                    }),
                },
            };
            if let Err(e) = raw_logger.write(record).await {
                warn!("RawLogger write failed (system_raw): {}", e);
            }
        }
        info!("ObservabilityPlugin raw logger consumed");

        // ── Metrics exporter ──
        if metrics.enabled {
            let ex: Arc<dyn cog_core::MetricsExporter> = Arc::new(crate::MetricsExporter::new());
            ctx.publish_service(ex);
            info!("ObservabilityPlugin metrics exporter published");
        } else {
            info!("Metrics exporter disabled by config");
        }

        // ── Loki ──
        if observability.loki.enabled {
            let client = crate::logs::LokiPushClient::new(&observability.loki.endpoint)
                .with_max_retries(observability.loki.max_retries)
                .with_timeout(observability.loki.timeout_secs)
                .with_label("service", &observability.jaeger.service_name)
                .with_client(http_client.clone());
            let client = Arc::new(client);
            let pusher = Arc::new(crate::logs::LokiBackgroundPusher::new(
                client.clone(),
                std::time::Duration::from_secs(observability.loki.flush_interval_sec),
                observability.loki.max_batch_size,
            ));
            ctx.publish(client.clone());
            ctx.publish(pusher.clone());
            info!("ObservabilityPlugin Loki client + pusher published");
        } else {
            info!("Loki push client disabled by config");
        }

        // ── ClickHouse ──
        if observability.clickhouse.enabled {
            let backend = crate::analytics::ClickHouseAnalyticsBackend::new(
                &observability.clickhouse.base_url,
                &observability.clickhouse.database,
            )
            .with_table(&observability.clickhouse.table)
            .with_auth(
                &observability.clickhouse.username,
                &observability.clickhouse.password,
            )
            .with_client(http_client.clone());
            let backend = Arc::new(backend);
            let buffer = Arc::new(crate::analytics::ClickHouseEventBuffer::new(
                backend.clone(),
                std::time::Duration::from_secs(observability.clickhouse.flush_interval_sec),
                observability.clickhouse.max_batch_size,
            ));
            ctx.publish(backend.clone());
            ctx.publish(buffer.clone());
            info!("ObservabilityPlugin ClickHouse backend + buffer published");
        } else {
            info!("ClickHouse analytics backend disabled by config");
        }

        // ── Jaeger exporter ──
        if let Some(exporter) = jaeger_exporter {
            ctx.publish(exporter);
            info!("ObservabilityPlugin Jaeger exporter published");
        }

        // ── Elasticsearch ──
        if observability.elasticsearch.enabled {
            let mut backend = crate::search_index::ElasticsearchBackend::new(
                &observability.elasticsearch.base_url,
            )
            .with_client(http_client.clone());
            if !observability.elasticsearch.api_key.is_empty() {
                backend = backend.with_api_key(&observability.elasticsearch.api_key);
            } else if !observability.elasticsearch.username.is_empty() {
                backend = backend.with_basic_auth(
                    &observability.elasticsearch.username,
                    &observability.elasticsearch.password,
                );
            }
            let backend: Arc<dyn cog_core::SearchBackend> = Arc::new(backend);
            ctx.publish_service(backend);
            info!("ObservabilityPlugin Elasticsearch backend published");
        } else {
            info!("Elasticsearch search backend disabled by config");
        }

        // ── Trace store ──
        let trace_store = ctx
            .consume_service::<dyn cog_core::TraceStore>()
            .expect("trace store")
            .clone();
        info!("ObservabilityPlugin trace store consumed");

        // ── Trace collector & replay engine ──
        let trace_collector = Arc::new(crate::snapshot::TraceCollector::new(trace_store.clone()));
        let replay_engine: Arc<dyn cog_core::ReplayEngine> =
            Arc::new(crate::snapshot::ReplayEngine::new(trace_store.clone()));
        ctx.publish(trace_collector.clone());
        ctx.publish_service(replay_engine.clone());
        info!("ObservabilityPlugin trace collector + replay engine published");

        // ── Trace tier migrator ──
        let trace_tier_migrator = Arc::new(crate::snapshot::TraceTierMigrator::new(
            trace_store.clone(),
            tier_migrator_hot_days,
            tier_migrator_warm_days,
        ));
        ctx.publish(trace_tier_migrator.clone());
        info!("ObservabilityPlugin trace tier migrator published");

        self.trace_collector = Some(trace_collector);
        self.trace_tier_migrator = Some(trace_tier_migrator);

        // Observable publish (pin-style)
        // 必须显式协变为 dyn Observable：publish_service 按静态类型 TypeId 注册，
        // 直接传具体类型会让网关 consume_all_services::<dyn Observable>() 拿不到，
        // D5 指标（接管台/events）永远为 0。
        let observable = crate::observable::global_observable();
        let as_observable: Arc<dyn cog_core::Observable> = observable.clone();
        ctx.publish_service(as_observable);
        info!("ObservabilityPlugin observable published");

        // Evolution metrics service for self-evolution pipeline.
        let evolution_metrics: Arc<dyn cog_core::EvolutionMetrics> = observable.clone();
        ctx.publish_service(evolution_metrics);
        info!("ObservabilityPlugin evolution metrics published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // ── Trace collector ──
        if let Some(ref trace_collector) = self.trace_collector {
            if let Some(event_tx) =
                ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            {
                let _handle = trace_collector.clone().spawn_collection_task(
                    (*event_tx).subscribe(),
                    ctx.consume::<cog_core::ShutdownSignal>()
                        .map(|s| (*s).clone())
                        .unwrap_or_default(),
                );
            }
        }

        // ── Trace tier migrator ──
        if let Some(ref trace_tier_migrator) = self.trace_tier_migrator {
            let trace_migrator = trace_tier_migrator.clone();
            let interval_secs = ctx.config().system.trace_migrator_interval_secs;
            let shutdown = ctx
                .consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if let Err(e) = trace_migrator.run_migration().await {
                                warn!("Trace tier migration failed: {}", e);
                            }
                        }
                        _ = shutdown.wait() => break,
                    }
                }
            });
        }

        // ── Alertmanager bridge ──
        let obs_cfg = crate::ObservabilityExportersConfig::load()?;
        if obs_cfg.alertmanager.enabled {
            let webhook_url = obs_cfg.alertmanager.webhook_url.clone();
            let timeout_secs = obs_cfg.alertmanager.timeout_secs;
            if !webhook_url.is_empty() {
                if let Some(http_client) = ctx.consume_service::<dyn cog_core::HttpClient>() {
                    if let Some(supervisor) = ctx.consume_service::<dyn cog_core::Supervisor>() {
                        let _ = spawn_alert_manager_bridge(
                            webhook_url,
                            timeout_secs,
                            &http_client,
                            &supervisor,
                        );
                        info!("Alertmanager webhook bridge started");
                    } else {
                        info!("Alertmanager bridge skipped: no Supervisor available");
                    }
                } else {
                    info!("Alertmanager bridge skipped: no HttpClient available");
                }
            } else {
                info!("Alertmanager bridge skipped: webhook_url is empty");
            }
        } else {
            info!("Alertmanager bridge disabled by config");
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("ObservabilityPlugin shutdown");
        Ok(())
    }
}

/// Convert a [`SupervisorEvent`] into [`AlertEvent`]s for Alertmanager webhook dispatch.
fn supervisor_event_to_alert_events(event: &cog_core::SupervisorEvent) -> Vec<AlertEvent> {
    let now = Utc::now();
    match event {
        cog_core::SupervisorEvent::AgentUnhealthy {
            agent_id,
            issue,
            timestamp,
        } => {
            let (severity, msg, value) = match issue {
                cog_core::HealthIssue::Suspect { missed_beats } => (
                    AlertSeverity::Warning,
                    format!("Agent suspect: missed {missed_beats} beats"),
                    *missed_beats as f64,
                ),
                cog_core::HealthIssue::Dead { .. } => (
                    AlertSeverity::Critical,
                    "Agent declared dead".to_string(),
                    1.0,
                ),
                cog_core::HealthIssue::Stuck { stuck_seconds } => (
                    AlertSeverity::Warning,
                    format!("Agent stuck for {stuck_seconds}s"),
                    *stuck_seconds as f64,
                ),
                cog_core::HealthIssue::StateBackendDead => (
                    AlertSeverity::Critical,
                    "Agent dead (state backend)".to_string(),
                    1.0,
                ),
            };
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            labels.insert("alert_type".into(), "agent_unhealthy".into());
            labels.insert("message".into(), msg);
            let inst = AlertInstance {
                rule_name: "agent_unhealthy".into(),
                labels,
                state: AlertState::Firing,
                severity,
                value,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::QuotaThresholdBreached {
            workspace_id,
            remaining,
            threshold: _,
            scheduler_paused,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("workspace_id".into(), workspace_id.clone());
            labels.insert("alert_type".into(), "quota_threshold_breached".into());
            labels.insert("scheduler_paused".into(), scheduler_paused.to_string());
            let inst = AlertInstance {
                rule_name: "quota_threshold_breached".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Critical,
                value: *remaining as f64,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::AgentResourceAlert {
            agent_id,
            metric,
            threshold,
            current,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            labels.insert("metric".into(), metric.clone());
            labels.insert("threshold".into(), threshold.to_string());
            let inst = AlertInstance {
                rule_name: "agent_resource_alert".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Warning,
                value: *current,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::TaskDeadLetter {
            task_id,
            agent_id,
            crew_id,
            retry_count,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("task_id".into(), task_id.clone());
            if let Some(aid) = agent_id {
                labels.insert("agent_id".into(), aid.clone());
            }
            if let Some(cid) = crew_id {
                labels.insert("crew_id".into(), cid.clone());
            }
            let inst = AlertInstance {
                rule_name: "task_dead_letter".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Critical,
                value: *retry_count as f64,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::SquadRespawnRequested {
            crew_id,
            reason,
            timestamp,
            ..
        } => {
            let mut labels = HashMap::new();
            labels.insert("crew_id".into(), crew_id.clone());
            labels.insert("reason".into(), reason.clone());
            let inst = AlertInstance {
                rule_name: "squad_respawn_requested".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Warning,
                value: 1.0,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: now,
            };
            vec![AlertEvent::Firing(inst)]
        }
        cog_core::SupervisorEvent::AgentRecovered {
            agent_id,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("agent_id".into(), agent_id.clone());
            let inst = AlertInstance {
                rule_name: "agent_unhealthy".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: 0.0,
                starts_at: *timestamp,
                ends_at: Some(now),
                updated_at: now,
            };
            vec![AlertEvent::Resolved(inst)]
        }
        cog_core::SupervisorEvent::QuotaRecovered {
            workspace_id,
            remaining,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("workspace_id".into(), workspace_id.clone());
            let inst = AlertInstance {
                rule_name: "quota_threshold_breached".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: *remaining as f64,
                starts_at: *timestamp,
                ends_at: Some(now),
                updated_at: now,
            };
            vec![AlertEvent::Resolved(inst)]
        }
        _ => vec![],
    }
}

/// Build an [`AlertManager`] and spawn the background bridge task that
/// forwards `SupervisorEvent`s to Alertmanager webhooks.
fn spawn_alert_manager_bridge(
    webhook_url: String,
    timeout_secs: u64,
    http_client: &Arc<dyn cog_core::HttpClient>,
    supervisor: &Arc<dyn cog_core::Supervisor>,
) -> Option<Arc<crate::alerts::AlertManager>> {
    let channel = AlertChannel::Webhook {
        url: webhook_url.clone(),
        headers: HashMap::new(),
    };
    let manager = Arc::new(
        crate::alerts::AlertManager::new(vec![], vec![channel])
            .with_timeout(timeout_secs)
            .with_client(http_client.clone()),
    );
    let mut alert_rx = supervisor.subscribe();
    let manager_for_task = manager.clone();
    tokio::spawn(async move {
        while let Ok(event) = alert_rx.recv().await {
            let alerts = supervisor_event_to_alert_events(&event);
            if !alerts.is_empty() {
                manager_for_task.notify(&alerts).await;
            }
        }
    });
    Some(manager)
}

/// Wrapper so [`LogFilterHandle`] can be stored in [`cog_core::PluginContext`].
pub struct LogFilterHandleHolder(pub crate::LogFilterHandle);

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "observability",
    requires: &["net", "storage"],
    optional_requires: &[],
    provides: &[
        "MetricsExporter",
        "RawLogger",
        "TraceCollector",
        "ReplayEngine",
        "TraceTierMigrator",
        "SearchBackend",
        "Observable",
        "LogFilterHandle",
        "EvolutionMetrics",
    ],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "HttpClient",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "RawLogger",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "TraceStore",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Supervisor",
            required: false,
        },
    ],
    factory: || Box::new(ObservabilityPlugin::new()),
};
