//! Observability plugin — implements [`cog_core::SystemPlugin`].

use chrono::Utc;
use cog_core::alerts::{AlertChannel, AlertEvent, AlertInstance, AlertSeverity, AlertState};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use crate::alert_store::{NewAlert, PostgresAlertStore};

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

        // ── Alert bridge: notification outlet + persistent state machine ──
        let obs_cfg = crate::ObservabilityExportersConfig::load()?;
        let alert_store = match std::env::var("COGNEVA_DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => match PostgresAlertStore::connect(&url).await {
                Ok(store) => match store.init_schema().await {
                    Ok(()) => {
                        info!("alert state machine persisted to PostgreSQL");
                        Some(Arc::new(store))
                    }
                    Err(e) => {
                        warn!(error = %e, "alert table init failed; alerts stay notification-only");
                        None
                    }
                },
                Err(e) => {
                    warn!(error = %e, "alert store connect failed; alerts stay notification-only");
                    None
                }
            },
            _ => {
                info!("COGNEVA_DATABASE_URL unset; alert state machine not persisted");
                None
            }
        };
        let webhook =
            if obs_cfg.alertmanager.enabled && !obs_cfg.alertmanager.webhook_url.is_empty() {
                Some((
                    obs_cfg.alertmanager.webhook_url.clone(),
                    obs_cfg.alertmanager.timeout_secs,
                ))
            } else {
                None
            };

        if alert_store.is_some() || webhook.is_some() {
            match ctx.consume_service::<dyn cog_core::Supervisor>() {
                Some(supervisor) => {
                    let http_client = ctx.consume_service::<dyn cog_core::HttpClient>();
                    if webhook.is_some() && http_client.is_none() {
                        info!("webhook configured but no HttpClient; alerts persist only");
                    }
                    let bridged =
                        spawn_alert_bridge(webhook, http_client, alert_store, &supervisor);
                    if bridged.is_some() {
                        info!("Alertmanager webhook bridge started");
                    }
                }
                None => info!("alert bridge skipped: no Supervisor available"),
            }
        } else {
            info!("alert bridge disabled: no webhook and no PostgreSQL store");
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
    // LLM 上游池全灭是"无人可用"级别的故障：不补可联通的上游，整条 LLM
    // 依赖链停摆，所以路由到通知出口（Critical），恢复时自动 resolve。
    match event {
        cog_core::SupervisorEvent::LlmUpstreamPoolDown {
            earliest_recovery_unix,
            unavailable,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("alert_type".into(), "llm_upstream_pool_down".into());
            labels.insert("unavailable_count".into(), unavailable.len().to_string());
            labels.insert(
                "earliest_recovery_unix".into(),
                earliest_recovery_unix.to_string(),
            );
            labels.insert("unavailable".into(), unavailable.join(","));
            // 文案统一由映射层写进 `message` 标签：通知出口与 PG 落盘共用同一份
            // 文本，避免两侧各写一遍导致措辞漂移。
            labels.insert(
                "message".into(),
                format!(
                    "All {} LLM upstreams unavailable (earliest recovery unix {})",
                    unavailable.len(),
                    earliest_recovery_unix
                ),
            );
            let inst = AlertInstance {
                rule_name: "llm_upstream_pool_down".into(),
                labels,
                state: AlertState::Firing,
                severity: AlertSeverity::Critical,
                value: unavailable.len() as f64,
                starts_at: *timestamp,
                ends_at: None,
                updated_at: Utc::now(),
            };
            return vec![AlertEvent::Firing(inst)];
        }
        cog_core::SupervisorEvent::LlmUpstreamPoolRecovered { timestamp } => {
            let mut labels = HashMap::new();
            labels.insert(
                "message".into(),
                "LLM upstream pool recovered; LLM-dependent tasks resumed".into(),
            );
            let inst = AlertInstance {
                rule_name: "llm_upstream_pool_down".into(),
                labels,
                state: AlertState::Resolved,
                severity: AlertSeverity::Info,
                value: 0.0,
                starts_at: *timestamp,
                ends_at: Some(Utc::now()),
                updated_at: Utc::now(),
            };
            return vec![AlertEvent::Resolved(inst)];
        }
        _ => {}
    }

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
            threshold,
            scheduler_paused,
            timestamp,
        } => {
            let mut labels = HashMap::new();
            labels.insert("workspace_id".into(), workspace_id.clone());
            labels.insert("alert_type".into(), "quota_threshold_breached".into());
            labels.insert("scheduler_paused".into(), scheduler_paused.to_string());
            labels.insert(
                "message".into(),
                format!(
                    "Quota breached: remaining={remaining}, threshold={}, paused={scheduler_paused}",
                    threshold
                ),
            );
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
            labels.insert(
                "message".into(),
                format!("Resource alert: {metric}={current:.2}, threshold={threshold:.2}"),
            );
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
            labels.insert(
                "message".into(),
                format!("Task sent to DLQ after {retry_count} retries"),
            );
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
            labels.insert(
                "message".into(),
                format!("Squad respawn requested: {reason}"),
            );
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
            labels.insert("message".into(), "Agent recovered".into());
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
            labels.insert(
                "message".into(),
                format!("Quota recovered: remaining={remaining}"),
            );
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

/// Spawn the alert bridge: subscribe to `SupervisorEvent`s, map each one once,
/// and feed both outlets — the notification outlet (Alertmanager webhook) and
/// the persistent state machine (PostgreSQL `alerts` table). One mapping result
/// drives both, so what the notification says and what is stored agree.
fn spawn_alert_bridge(
    webhook: Option<(String, u64)>,
    http_client: Option<Arc<dyn cog_core::HttpClient>>,
    store: Option<Arc<PostgresAlertStore>>,
    supervisor: &Arc<dyn cog_core::Supervisor>,
) -> Option<Arc<crate::alerts::AlertManager>> {
    let manager = match (webhook, http_client) {
        (Some((url, timeout_secs)), Some(client)) => {
            let channel = AlertChannel::Webhook {
                url,
                headers: HashMap::new(),
            };
            Some(Arc::new(
                crate::alerts::AlertManager::new(vec![], vec![channel])
                    .with_timeout(timeout_secs)
                    .with_client(client),
            ))
        }
        _ => None,
    };
    let mut alert_rx = supervisor.subscribe();
    let manager_for_task = manager.clone();
    tokio::spawn(async move {
        while let Ok(event) = alert_rx.recv().await {
            let alerts = supervisor_event_to_alert_events(&event);
            if alerts.is_empty() {
                continue;
            }
            if let Some(manager) = &manager_for_task {
                manager.notify(&alerts).await;
            }
            if let Some(store) = &store {
                persist_alerts(store, &alerts).await;
            }
        }
    });
    manager
}

/// Stable dedup key: re-evaluating the same condition must hit the same row in
/// the `alerts` table. Only labels that identify the alert instance take part;
/// values that change between evaluations (thresholds, messages) must stay out
/// or every tick would insert a new row. A resolve event has to compute the
/// same key as the firing event it closes.
fn alert_dedup_key(inst: &AlertInstance) -> String {
    let label = |key: &str| inst.labels.get(key).map(String::as_str).unwrap_or("");
    let identity = match inst.rule_name.as_str() {
        "agent_unhealthy" => label("agent_id").to_string(),
        "quota_threshold_breached" => label("workspace_id").to_string(),
        "agent_resource_alert" => format!("{}:{}", label("agent_id"), label("metric")),
        "task_dead_letter" => label("task_id").to_string(),
        "squad_respawn_requested" => label("crew_id").to_string(),
        _ => String::new(),
    };
    if identity.is_empty() {
        inst.rule_name.clone()
    } else {
        format!("{}:{}", inst.rule_name, identity)
    }
}

/// Human-readable alert text. The mapping layer writes the text into the
/// `message` label (single source of truth for wording); this only falls back
/// so a stored row never carries an empty message.
fn alert_message(inst: &AlertInstance) -> String {
    match inst.labels.get("message") {
        Some(text) if !text.is_empty() => text.clone(),
        _ => format!(
            "{} {}",
            inst.rule_name,
            match inst.state {
                AlertState::Pending => "pending",
                AlertState::Firing => "firing",
                AlertState::Resolved => "resolved",
            }
        ),
    }
}

/// Drive mapped alerts into the PostgreSQL state machine: `Firing` guarantees an
/// open `firing` row, `Resolved` closes the row with the same dedup key.
/// Re-evaluating is idempotent, so a restarting producer cannot pile up rows.
async fn persist_alerts(store: &PostgresAlertStore, alerts: &[AlertEvent]) {
    for event in alerts {
        let (condition, inst) = match event {
            AlertEvent::Firing(inst) => (true, inst),
            AlertEvent::Resolved(inst) => (false, inst),
        };
        let alert = NewAlert {
            rule: inst.rule_name.clone(),
            dedup_key: alert_dedup_key(inst),
            severity: inst.severity.as_str().to_string(),
            message: alert_message(inst),
            labels: serde_json::to_value(&inst.labels).unwrap_or_else(|_| serde_json::json!({})),
        };
        if let Err(e) = store.set_alert(condition, &alert).await {
            warn!(rule = %alert.rule, error = %e, "alert persistence failed");
        }
    }
}

/// Wrapper so [`crate::LogFilterHandle`] can be stored in [`cog_core::PluginContext`].
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

#[cfg(test)]
mod tests {
    use super::*;

    fn inst(rule: &str, pairs: &[(&str, &str)], state: AlertState) -> AlertInstance {
        AlertInstance {
            rule_name: rule.into(),
            labels: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            state,
            severity: AlertSeverity::Critical,
            value: 1.0,
            starts_at: Utc::now(),
            ends_at: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn dedup_key_survives_resolve_and_ignores_volatile_labels() {
        let firing = cog_core::SupervisorEvent::AgentUnhealthy {
            agent_id: "a1".into(),
            issue: cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
            timestamp: Utc::now(),
        };
        let recovered = cog_core::SupervisorEvent::AgentRecovered {
            agent_id: "a1".into(),
            timestamp: Utc::now(),
        };
        let key_of = |ev: &cog_core::SupervisorEvent| {
            let alerts = supervisor_event_to_alert_events(ev);
            let Some(AlertEvent::Firing(i) | AlertEvent::Resolved(i)) = alerts.first() else {
                panic!("事件必须映射出告警");
            };
            alert_dedup_key(i)
        };
        // 触发与恢复必须落同一行，否则 resolve 关不掉。
        assert_eq!(key_of(&firing), key_of(&recovered));
        assert_eq!(key_of(&firing), "agent_unhealthy:a1");
    }

    #[test]
    fn dedup_key_separates_resource_metric_and_pool_rule() {
        let resource = inst(
            "agent_resource_alert",
            &[("agent_id", "a1"), ("metric", "cpu")],
            AlertState::Firing,
        );
        assert_eq!(alert_dedup_key(&resource), "agent_resource_alert:a1:cpu");
        // 池全灭告警没有实例标签，去重键就是规则名（与网关侧写入同键）。
        let pool = inst("llm_upstream_pool_down", &[], AlertState::Firing);
        assert_eq!(alert_dedup_key(&pool), "llm_upstream_pool_down");
    }

    #[test]
    fn message_comes_from_label_with_rule_fallback() {
        let with_text = inst(
            "quota_threshold_breached",
            &[("message", "quota low")],
            AlertState::Firing,
        );
        assert_eq!(alert_message(&with_text), "quota low");
        let without = inst("agent_unhealthy", &[], AlertState::Resolved);
        assert_eq!(alert_message(&without), "agent_unhealthy resolved");
    }

    #[test]
    fn existing_alert_kinds_all_carry_message_and_identity() {
        // 存量告警必须与新告警一样具备落盘所需的文案与去重身份。
        let events = vec![
            cog_core::SupervisorEvent::AgentResourceAlert {
                agent_id: "a1".into(),
                metric: "cpu".into(),
                threshold: 0.9,
                current: 0.95,
                timestamp: Utc::now(),
            },
            cog_core::SupervisorEvent::TaskDeadLetter {
                task_id: "t1".into(),
                agent_id: None,
                crew_id: None,
                retry_count: 3,
                timestamp: Utc::now(),
            },
            cog_core::SupervisorEvent::SquadRespawnRequested {
                crew_id: "c1".into(),
                squad_id: None,
                reason: "unhealthy".into(),
                timestamp: Utc::now(),
            },
            cog_core::SupervisorEvent::QuotaThresholdBreached {
                workspace_id: "w1".into(),
                remaining: 1,
                threshold: 10,
                scheduler_paused: true,
                timestamp: Utc::now(),
            },
        ];
        for event in &events {
            let alerts = supervisor_event_to_alert_events(event);
            let Some(AlertEvent::Firing(i)) = alerts.first() else {
                panic!("事件必须映射出 firing 告警");
            };
            assert!(!alert_message(i).is_empty(), "文案不能为空");
            assert_ne!(
                alert_dedup_key(i),
                i.rule_name,
                "带实例标签的告警去重键要能区分实例"
            );
        }
    }
}
